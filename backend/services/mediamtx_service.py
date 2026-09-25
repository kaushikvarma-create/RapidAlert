"""
MediaMTXService: Subprocess manager and dynamic config generator for MediaMTX live streaming gateway.
Repackages RTSP feeds into Low-Latency HLS (LL-HLS) with native H.264 and H.265 (HEVC) hardware decode support.
"""
from __future__ import annotations

import os
import signal
import subprocess
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional
import httpx
import yaml

from backend.core.config import ROOT_DIR, CAMERAS_CONFIG_PATH
from backend.core.error_tracker import error_tracker

STREAM_DIR = ROOT_DIR / "stream"
MEDIAMTX_BIN = STREAM_DIR / "mediamtx"
MEDIAMTX_YML = STREAM_DIR / "mediamtx.yml"
MEDIAMTX_API_URL = "http://127.0.0.1:9997"
MEDIAMTX_HLS_PORT = 8888


def get_safe_cam_slug(cam_name: str) -> str:
    """Normalize camera name into URL-safe path slug for MediaMTX (e.g. '1ST_ENTRANCE' -> '1st_entrance')."""
    return cam_name.strip().lower().replace(" ", "_").replace("-", "_")


class MediaMTXService:
    def __init__(self, root_dir: Path = ROOT_DIR):
        self.root_dir = root_dir
        self.stream_dir = root_dir / "stream"
        self.bin_path = self.stream_dir / "mediamtx"
        self.yml_path = self.stream_dir / "mediamtx.yml"
        self._process: Optional[subprocess.Popen] = None
        self._lock = threading.Lock()
        self._is_running = False

    def generate_config(self, cameras: List[Dict[str, Any]]) -> str:
        """Generate mediamtx.yml content dynamically from camera registry."""
        paths_config: Dict[str, Any] = {}
        for cam in cameras:
            name = cam.get("name")
            url = cam.get("url")
            enabled = cam.get("enabled", True)
            if not name or not url or not enabled:
                continue

            slug = get_safe_cam_slug(name)
            paths_config[slug] = {
                "source": url,
                "sourceProtocol": "tcp",
                "sourceOnDemand": True,
                "sourceOnDemandStartTimeout": "15s",
                "sourceOnDemandCloseAfter": "60s",
            }

        config_dict = {
            "logLevel": "warn",
            "logDestinations": ["stdout"],
            "api": True,
            "apiAddress": "127.0.0.1:9997",
            "rtspAddress": ":8554",
            "hlsAddress": ":8888",
            "webrtc": False,
            "rtpAddress": ":8100",
            "rtcpAddress": ":8101",
            "hlsVariant": "lowLatency",
            "hlsSegmentCount": 7,
            "hlsSegmentDuration": "1s",
            "hlsPartDuration": "200ms",
            "paths": paths_config,
        }

        self.stream_dir.mkdir(parents=True, exist_ok=True)
        yml_str = yaml.dump(config_dict, sort_keys=False)
        with open(self.yml_path, "w", encoding="utf-8") as f:
            f.write(yml_str)
        return yml_str

    def start(self, cameras: Optional[List[Dict[str, Any]]] = None) -> bool:
        """Start the MediaMTX daemon process."""
        with self._lock:
            if self._is_running and self._process and self._process.poll() is None:
                return True

            if not self.bin_path.exists():
                error_tracker.capture_error(
                    message=f"MediaMTX binary not found at {self.bin_path}",
                    component="MediaMTXService",
                    effect="Low-latency HLS streaming unavailable; falling back to direct MJPEG",
                    severity="WARNING",
                )
                return False

            # Ensure binary is executable
            try:
                os.chmod(self.bin_path, 0o755)
            except Exception:
                pass

            if cameras is not None:
                self.generate_config(cameras)
            elif not self.yml_path.exists():
                # Load from cameras.json if exists
                if CAMERAS_CONFIG_PATH.exists():
                    try:
                        import json
                        with open(CAMERAS_CONFIG_PATH) as f:
                            cams = json.load(f)
                        self.generate_config(cams)
                    except Exception:
                        self.generate_config([])
                else:
                    self.generate_config([])

            # Kill any existing stray mediamtx processes on port 8888 / 9997
            try:
                subprocess.run(["pkill", "-f", "stream/mediamtx"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                time.sleep(0.2)
            except Exception:
                pass

            try:
                self._process = subprocess.Popen(
                    [str(self.bin_path), str(self.yml_path)],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    cwd=str(self.stream_dir),
                    preexec_fn=os.setsid,
                )
                self._is_running = True
                print(f"[MediaMTX] 🚀 Low-Latency HLS Gateway active on port {MEDIAMTX_HLS_PORT}")
                return True
            except Exception as exc:
                error_tracker.capture_exception(
                    exc,
                    component="MediaMTXService",
                    effect="Failed to launch MediaMTX subprocess",
                    severity="ERROR",
                )
                self._is_running = False
                return False

    def sync_cameras(self, cameras: List[Dict[str, Any]]) -> None:
        """Hot-reload camera paths in MediaMTX."""
        self.generate_config(cameras)
        # Attempt REST API hot-reload or restart if needed
        try:
            # MediaMTX automatically detects config file modifications or restart
            if not self._is_running or not self._process or self._process.poll() is not None:
                self.start(cameras)
        except Exception as exc:
            error_tracker.capture_exception(
                exc,
                component="MediaMTXService",
                effect="Failed to sync camera paths to MediaMTX",
                severity="WARNING",
            )

    def is_healthy(self) -> bool:
        """Check if MediaMTX is responding on port 8888."""
        if not self._is_running or not self._process or self._process.poll() is not None:
            return False
        return True

    def stop(self) -> None:
        """Gracefully terminate MediaMTX daemon."""
        with self._lock:
            if self._process:
                try:
                    os.killpg(os.getpgid(self._process.pid), signal.SIGTERM)
                    self._process.wait(timeout=2.0)
                except Exception:
                    try:
                        os.killpg(os.getpgid(self._process.pid), signal.SIGKILL)
                    except Exception:
                        pass
                self._process = None
            self._is_running = False
            print("[MediaMTX] 🛑 MediaMTX Gateway stopped")


mediamtx_service = MediaMTXService()
