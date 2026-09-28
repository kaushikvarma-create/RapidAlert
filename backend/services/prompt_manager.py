"""
PromptManager: hot-reload prompts from config/prompts.json (watches mtime every 2s).
Supports a master prompt template with {normal_context} placeholder,
and per-camera override prompts stored under "cameras": {name: str}.
"""
from __future__ import annotations

import asyncio
import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

from backend.core.config import PROMPTS_CONFIG_PATH, load_prompts_config
from backend.core.error_tracker import error_tracker


class PromptManager:
    RELOAD_INTERVAL = 2  # seconds

    def __init__(
        self,
        prompts_path: Path = PROMPTS_CONFIG_PATH,
        cameras_config_provider: Optional[Callable[[], list[dict]]] = None,
    ):
        self.path = prompts_path
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
        prev_severity: Optional[str] = None,
        prev_observation: Optional[str] = None,
        cycle: int = 1,
        interval_sec: float = 10.0,
    ) -> str:
        """Build the final prompt for cam_name with context injected."""
        if hour is None:
            hour = datetime.now().hour

        normal_context = self._get_cam_context(cam_name, hour)
        ctx_str = ""
        if normal_context:
            ctx_str = (
                f"NORMAL CONTEXT for this camera: {normal_context}\n"
                "Flag any deviation from this normal context."
            )

        master_scene = self._master_scene_context.strip() if self._master_scene_context else "Standard facility environment."

        if is_followup:
            followup_info = (
                f"FOLLOW-UP CONTEXT:\n"
                f"This is follow-up check #{cycle} ({interval_sec:.0f}s after initial trigger)."
            )
            if prev_severity:
                followup_info += f" An earlier incident had severity: {prev_severity}."
            if prev_observation:
                followup_info += f" Earlier observation: \"{prev_observation}\"."
            followup_info += " Objectively determine whether this has resolved or is persisting."

            template = self._followup
            return (
                template
                .replace("{master_scene_context}", master_scene)
                .replace("{global_context}", master_scene)
                .replace("{normal_context}", ctx_str)
                .replace("{followup_context}", followup_info)
            )

        template = self._cam_overrides.get(cam_name) or self._master
        return (
            template
            .replace("{master_scene_context}", master_scene)
            .replace("{global_context}", master_scene)
            .replace("{normal_context}", ctx_str)
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
            with open(self.path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
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

    def _get_cam_context(self, cam_name: str, hour: int) -> str:
        if not self._cameras_config_provider:
            return ""
        try:
            cameras = self._cameras_config_provider()
            for c in cameras:
                if c.get("name") == cam_name:
                    if 6 <= hour < 21:
                        return c.get("normal_context_day", "").strip()
                    else:
                        return c.get("normal_context_night", "").strip()
        except Exception as exc:
            error_tracker.capture_exception(
                exc,
                component="PromptManager",
                camera=cam_name,
                effect=f"Failed to retrieve normal context for {cam_name}",
                severity="WARNING",
            )
        return ""
