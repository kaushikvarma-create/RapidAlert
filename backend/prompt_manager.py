"""
PromptManager: hot-reload prompts from prompts.json (watches mtime every 2s).
Supports a master prompt template with {normal_context} placeholder,
and per-camera override prompts stored under "cameras": {name: str}.
"""
import asyncio
import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

DEFAULT_MASTER = (
    "You are an expert CCTV surveillance AI.\n"
    "Analyse the provided temporal sequence of 4 CCTV frames capturing an incident window over the past 10 seconds:\n"
    "- Frame 1: Scene baseline (t -10s)\n"
    "- Frame 2: Developing activity (t -5s)\n"
    "- Frame 3: Immediate lead-up (t -2s)\n"
    "- Frame 4: Trigger moment (t 0s)\n\n"
    "{normal_context}\n\n"
    "Return EXACTLY this format, no extra text:\n"
    "OBSERVATION: <1-2 sentences describing the sequence of events and what changed>\n"
    "ACTIVITY: <ACTIVE|IDLE|UNKNOWN>\n"
    "WORKERS: <integer count of people in scene>\n"
    "MACHINERY: <comma-separated list or None>\n"
    "SAFETY: <OK|WARNING|DANGER>\n"
    "SEVERITY: <LOW|MEDIUM|HIGH|EXTREME>\n"
    "EVOLUTION: <concise summary of movement and changes across the sequence>\n\n"
    "CRITICAL: Do NOT use extended thinking, reasoning steps, or <think> tags. Output the final format immediately."
)

DEFAULT_FOLLOWUP = (
    "You are an expert CCTV surveillance AI.\n"
    "Analyse the provided temporal sequence of 4 CCTV frames capturing the scene follow-up window:\n"
    "- Frame 1: Sequence start\n"
    "- Frame 2: Mid-sequence progression\n"
    "- Frame 3: Recent status\n"
    "- Frame 4: Current outcome\n\n"
    "{normal_context}\n"
    "{followup_context}\n\n"
    "CRITICAL INSTRUCTIONS TO PREVENT FALSE POSITIVES & HALLUCINATION:\n"
    "- Objectively evaluate ONLY the visual evidence visible in these CURRENT 4 frames.\n"
    "- Do NOT carry forward or hallucinate hazards/severity from earlier triggers.\n"
    "- If earlier movement/activity has subsided, people have departed, or normal operations have resumed, you MUST mark:\n"
    "  SAFETY: OK\n"
    "  SEVERITY: LOW\n"
    "- Only output MEDIUM, HIGH, or DANGER if you directly observe an active violation, hazard, or aggressive motion in the CURRENT frames.\n\n"
    "Return EXACTLY this format, no extra text:\n"
    "OBSERVATION: <1-2 sentences stating current scene status, explicitly noting if earlier activity has resolved, stabilized, or continued>\n"
    "ACTIVITY: <ACTIVE|IDLE|UNKNOWN>\n"
    "WORKERS: <integer count of people in scene>\n"
    "MACHINERY: <comma-separated list or None>\n"
    "SAFETY: <OK|WARNING|DANGER>\n"
    "SEVERITY: <LOW|MEDIUM|HIGH|EXTREME>\n"
    "EVOLUTION: <concise summary of changes across the 4 frames>\n\n"
    "CRITICAL: Do NOT use extended thinking, reasoning steps, or <think> tags. Output the final format immediately."
)


class PromptManager:
    RELOAD_INTERVAL = 2  # seconds

    def __init__(
        self,
        prompts_path: Path,
        cameras_config_provider: Optional[Callable[[], list[dict]]] = None,
    ):
        self.path = prompts_path
        self._cameras_config_provider = cameras_config_provider
        self._master: str = DEFAULT_MASTER
        self._followup: str = DEFAULT_FOLLOWUP
        self._cam_overrides: dict[str, str] = {}
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
            return template.replace("{normal_context}", ctx_str).replace("{followup_context}", followup_info)

        template = self._cam_overrides.get(cam_name) or self._master
        return template.replace("{normal_context}", ctx_str)

    def get_followup_prompt(self, cam_name: str, hour: Optional[int] = None) -> str:
        return self.get_prompt(cam_name, hour=hour, is_followup=True)

    def get_master(self) -> str:
        return self._master

    def get_cam_overrides(self) -> dict[str, str]:
        return dict(self._cam_overrides)

    def save(
        self,
        master: Optional[str] = None,
        cam_name: Optional[str] = None,
        cam_prompt: Optional[str] = None,
    ) -> None:
        """Update one or more fields and persist to prompts.json."""
        if master is not None:
            self._master = master
        if cam_name is not None:
            if cam_prompt:
                self._cam_overrides[cam_name] = cam_prompt
            else:
                self._cam_overrides.pop(cam_name, None)

        data = {"master": self._master, "cameras": self._cam_overrides}
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.path, "w") as f:
            json.dump(data, f, indent=2)
        # Bump mtime tracker so watch_loop doesn't re-read what we just wrote
        try:
            self._mtime = os.path.getmtime(self.path)
        except OSError:
            pass

    # ── Background watcher ──────────────────────────────────────────

    async def watch_loop(self) -> None:
        while True:
            await asyncio.sleep(self.RELOAD_INTERVAL)
            self._reload_sync()

    # ── Internal ────────────────────────────────────────────────────

    def _reload_sync(self) -> None:
        if not self.path.exists():
            return
        try:
            mtime = os.path.getmtime(self.path)
            if mtime <= self._mtime:
                return
            with open(self.path) as f:
                data = json.load(f)
            self._master = data.get("master", DEFAULT_MASTER)
            self._followup = data.get("followup", DEFAULT_FOLLOWUP)
            self._cam_overrides = data.get("cameras", {})
            self._mtime = mtime
            print("[PromptMgr] Prompts reloaded from disk")
        except Exception as exc:
            print(f"[PromptMgr] Reload error: {exc}")

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
        except Exception:
            pass
        return ""
