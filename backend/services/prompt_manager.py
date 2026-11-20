"""
PromptManager: hot-reload prompts from config/prompts.json (watches mtime every 2s).
Supports distinct scene/background, baseline, and checklist placeholders,
and per-camera override prompts stored under "cameras": {name: str}.
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

from backend.core.config import PROMPTS_CONFIG_PATH, atomic_write_json, load_prompts_config
from backend.core.error_tracker import error_tracker


class PromptManager:
    RELOAD_INTERVAL = 2  # seconds

    def __init__(
        self,
        prompts_path: Path = PROMPTS_CONFIG_PATH,
        cameras_config_provider: Optional[Callable[[], list[dict]]] = None,
    ):
        self.path = Path(prompts_path)
        self._cameras_config_provider = cameras_config_provider
        
        # Load initial prompts from config file
        initial_cfg = load_prompts_config()
        self._master_scene_context: str = initial_cfg.get("master_scene_context", "")
        self._master: str = initial_cfg.get("master", "")
        self._followup: str = initial_cfg.get("followup", "")
        self._cam_overrides: dict[str, str] = initial_cfg.get("cameras", {})
        self._mtime: float = 0.0
        self._reload_sync()

    # ── Public ──────────────────────────────────────────────────────

    def get_prompt(
        self,
        cam_name: str,
        hour: Optional[int] = None,
        is_followup: bool = False,
        cycle: int = 1,
        interval_sec: float = 10.0,
        prev_observation: Optional[str] = None,
        prev_severity: Optional[str] = None,
    ) -> str:
        """Build the final prompt for cam_name with context injected."""
        if hour is None:
            hour = datetime.now().hour

        baseline = self._get_cam_context(cam_name, hour)
        routine_checklist, threat_checklist = self._get_cam_checklists(cam_name)
        master_scene = self._master_scene_context.strip() if self._master_scene_context else "Standard facility environment."

        baseline_block = (
            "CAMERA BASELINE — orientation only; use it to understand the scene, not as proof of an incident:\n"
            f"{baseline}\n" if baseline else "CAMERA BASELINE — (none provided)"
        )
        routine_block = (
            "VISUAL CHECKLIST B — ROUTINE ACTIVITIES\n"
            "If an item is visibly present, copy it into routine_flags; otherwise return []. Do not infer intent or risk.\n"
            f"{routine_checklist or '(none provided)'}"
        )
        # Site-neutral on purpose: what counts as a threat comes from the operator's checklist,
        # and how to weigh it comes from the template in config/prompts.json.
        threat_block = (
            "VISUAL CHECKLIST A — PRIORITY THREATS\n"
            "Copy an item into priority_flags when it is visibly happening in the frames; otherwise return []. "
            "Do not flag an item that is only possible or that the image is too unclear to show.\n"
            f"{threat_checklist or '(none provided)'}"
        )

        if is_followup:
            # The follow-up is told what the previous check saw, so 'evolution' can say whether
            # the situation resolved, held or got worse. Severity is still judged from these frames.
            prev_line = ""
            if prev_observation:
                prev_text = " ".join(str(prev_observation).split())[:300]
                prev_line = (
                    f"The previous check rated the scene {str(prev_severity or 'UNKNOWN').upper()} and observed: "
                    f"\"{prev_text}\"\n"
                )
            followup_info = (
                f"FOLLOW-UP CHECK #{cycle}, {interval_sec:.0f}s after the previous check.\n"
                f"{prev_line}"
                f"These frames cover the time since that check. Decide the severity from what these frames show now, "
                f"not from the earlier rating. In 'evolution', say how the situation has changed since the previous "
                f"check: resolved, unchanged, or worse. If the scene is now routine or empty, mark "
                f"\"safety\": \"OK\", \"severity\": \"LOW\"."
            )

            template = self._followup
            return (
                template
                .replace("{cam_name}", cam_name)
                .replace("{master_scene_context}", master_scene)
                .replace("{scene_background}", master_scene)
                .replace("{global_context}", master_scene)
                .replace("{normal_context}", f"{baseline_block}\n\n{threat_block}\n\n{routine_block}")
                .replace("{baseline_context}", baseline_block)
                .replace("{routine_checklist}", routine_block)
                .replace("{threat_checklist}", threat_block)
                .replace("{followup_context}", followup_info)
            )

        template = self._cam_overrides.get(cam_name) or self._master
        return (
            template
            .replace("{cam_name}", cam_name)
            .replace("{master_scene_context}", master_scene)
            .replace("{scene_background}", master_scene)
            .replace("{global_context}", master_scene)
            .replace("{normal_context}", f"{baseline_block}\n\n{threat_block}\n\n{routine_block}")
            .replace("{baseline_context}", baseline_block)
            .replace("{routine_checklist}", routine_block)
            .replace("{threat_checklist}", threat_block)
        )

    def get_followup_prompt(self, cam_name: str, hour: Optional[int] = None) -> str:
        return self.get_prompt(cam_name, hour=hour, is_followup=True)

    def get_master(self) -> str:
        return self._master

    def get_master_scene_context(self) -> str:
        return self._master_scene_context

    def get_cam_overrides(self) -> dict[str, str]:
        return dict(self._cam_overrides)

    def save(
        self,
        master: Optional[str] = None,
        master_scene_context: Optional[str] = None,
        followup: Optional[str] = None,
        cam_name: Optional[str] = None,
        cam_prompt: Optional[str] = None,
    ) -> None:
        """Update one or more fields and persist to config/prompts.json."""
        if master is not None:
            self._master = master
        if master_scene_context is not None:
            self._master_scene_context = master_scene_context
        if followup is not None:
            self._followup = followup
        if cam_name is not None:
            if cam_prompt:
                self._cam_overrides[cam_name] = cam_prompt
            else:
                self._cam_overrides.pop(cam_name, None)

        data = {
            "master_scene_context": self._master_scene_context,
            "master": self._master,
            "followup": self._followup,
            "cameras": self._cam_overrides,
        }
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            atomic_write_json(self.path, data)
            self._mtime = os.path.getmtime(self.path)
        except Exception as exc:
            error_tracker.capture_exception(
                exc,
                component="PromptManager",
                camera=cam_name,
                effect=f"Failed to save prompt configuration to {self.path}",
                severity="ERROR",
            )

    # ── Background watcher ──────────────────────────────────────────

    async def watch_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self.RELOAD_INTERVAL)
                self._reload_sync()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                error_tracker.capture_exception(
                    exc,
                    component="PromptManager",
                    effect="Error in prompts file watcher loop; continuing",
                    severity="WARNING",
                )

    # ── Internal ────────────────────────────────────────────────────

    def _reload_sync(self) -> None:
        if not self.path.exists():
            return
        try:
            mtime = os.path.getmtime(self.path)
            if mtime <= self._mtime:
                return
            with open(self.path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if "master_scene_context" in data:
                self._master_scene_context = data["master_scene_context"]
            if "master" in data:
                self._master = data["master"]
            if "followup" in data:
                self._followup = data["followup"]
            self._cam_overrides = data.get("cameras", {})
            self._mtime = mtime
            print("[PromptMgr] Prompts reloaded from disk")
        except Exception as exc:
            error_tracker.capture_exception(
                exc,
                component="PromptManager",
                effect=f"Failed to reload prompts from {self.path}; using in-memory prompts",
                severity="WARNING",
            )

    def _get_cam_config(self, cam_name: str) -> Optional[dict]:
        if not self._cameras_config_provider:
            return None
        try:
            cameras = self._cameras_config_provider()
            for c in cameras:
                if c.get("name") == cam_name:
                    return c
        except Exception as exc:
            error_tracker.capture_exception(
                exc,
                component="PromptManager",
                camera=cam_name,
                effect=f"Failed to retrieve camera config for {cam_name}",
                severity="WARNING",
            )
        return None

    def _get_cam_context(self, cam_name: str, hour: int) -> str:
        cam = self._get_cam_config(cam_name)
        if not cam:
            return ""
            
        is_night = not (6 <= hour < 21)
        if is_night and cam.get("night_context_enabled"):
            return cam.get("night_context", "").strip()
            
        return cam.get("normal_context", "").strip()

    def _get_cam_incident_rules_str(self, cam_name: str) -> str:
        """Legacy combined representation retained for compatibility."""
        routine, threat = self._get_cam_checklists(cam_name)
        parts = []
        if threat:
            parts.append(f"VISUAL CHECKLIST A:\n{threat}")
        if routine:
            parts.append(f"VISUAL CHECKLIST B:\n{routine}")
        return "\n\n".join(parts)

    def _get_cam_checklists(self, cam_name: str) -> tuple[str, str]:
        """Return (routine checklist, priority checklist) as display-ready lines."""
        cam = self._get_cam_config(cam_name)
        if not cam:
            return "", ""

        def format_items(value) -> str:
            values = value if isinstance(value, list) else str(value or "").splitlines()
            return "\n".join(
                f"  * {str(item).strip()}" for item in values if str(item).strip()
            )

        return format_items(cam.get("low_incidents")), format_items(cam.get("severe_incidents"))

