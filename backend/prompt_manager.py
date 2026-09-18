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
    "You are a surveillance AI.\n"
    "Analyse the provided sequence of CCTV frames representing a 10-second temporal window from this camera.\n\n"
    "{normal_context}\n\n"
    "Return EXACTLY this format, no extra text:\n"
    "OBSERVATION: <one sentence describing the overall scene>\n"
    "ACTIVITY: <ACTIVE|IDLE|UNKNOWN>\n"
    "WORKERS: <integer count>\n"
    "MACHINERY: <comma-separated list or None>\n"
    "SAFETY: <OK|WARNING|DANGER>\n"
    "SEVERITY: <LOW|MEDIUM|HIGH>\n"
    "EVOLUTION: <explicitly describe the action or movement happening across the sequence of frames, or None if completely static>\n\n"
    "CRITICAL: Do NOT use extended thinking, reasoning steps, or <think> tags. Output the final JSON-like format immediately."
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
        self._cam_overrides: dict[str, str] = {}
        self._mtime: float = 0.0
        self._reload_sync()

    # ── Public ──────────────────────────────────────────────────────

    def get_prompt(self, cam_name: str, hour: Optional[int] = None) -> str:
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

        template = self._cam_overrides.get(cam_name) or self._master
        return template.replace("{normal_context}", ctx_str)

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
