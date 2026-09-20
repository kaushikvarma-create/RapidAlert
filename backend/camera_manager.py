"""
CameraManager: one daemon thread per enabled camera.
Each thread continuously reads RTSP frames → FrameStore.
Watches cameras.json for mtime changes; adds/removes streams dynamically.
"""
import threading
import time
import json
import os
from pathlib import Path
from typing import Dict, Optional

import cv2

os.environ.setdefault("OPENCV_FFMPEG_CAPTURE_OPTIONS", "rtsp_transport;tcp")

from frame_store import FrameStore
from nvidia_ingest import NvidiaStreamCapture, is_nvidia_available


class CameraThread(threading.Thread):
    RETRY_DELAY = 15  # seconds between reconnect attempts

    def __init__(
        self,
        cam_name: str,
        url: str,
        frame_store: FrameStore,
        stop_event: threading.Event,
        use_nvidia: bool = True,
    ):
        super().__init__(name=f"cam-{cam_name}", daemon=True)
        self.cam_name = cam_name
        self.url = url
        self.frame_store = frame_store
        self.stop_event = stop_event
        self.use_nvidia = use_nvidia and is_nvidia_available()
        self.connected = False

    def run(self) -> None:
        while not self.stop_event.is_set():
            cap = None
            is_hw = False

            if self.use_nvidia:
                try:
                    cap = NvidiaStreamCapture(self.url, width=1280, height=720)
                    if cap.isOpened():
                        is_hw = True
                        print(f"[CamMgr] ⚡ {self.cam_name} using NVIDIA Hardware Decoder (NVDEC)")
                    else:
                        cap.release()
                        cap = None
                except Exception as e:
                    print(f"[CamMgr] ⚠️  {self.cam_name} NVDEC init failed ({e}), falling back to OpenCV")
                    cap = None

            if cap is None:
                cap = cv2.VideoCapture(self.url, cv2.CAP_FFMPEG)
                cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)

            got_frame = False

            while not self.stop_event.is_set():
                if is_hw:
                    ok, frame = cap.read(timeout_sec=1.5)
                else:
                    ok, frame = cap.read()

                if ok and frame is not None:
                    self.frame_store.put(self.cam_name, frame)
                    if not got_frame:
                        got_frame = True
                        self.connected = True
                        mode = "NVDEC" if is_hw else "CPU-OpenCV"
                        print(f"[CamMgr] ✅ {self.cam_name} connected ({mode})")
                else:
                    if got_frame:
                        self.connected = False
                        print(
                            f"[CamMgr] ⚠  {self.cam_name} stream lost "
                            f"— retrying in {self.RETRY_DELAY}s"
                        )
                        break
                    time.sleep(0.05)

            if cap:
                cap.release()

            # Retry backoff
            if not self.stop_event.is_set():
                for _ in range(self.RETRY_DELAY * 10):
                    if self.stop_event.is_set():
                        break
                    time.sleep(0.1)

        self.frame_store.remove(self.cam_name)
        print(f"[CamMgr] ⛔ {self.cam_name} stopped")


class CameraManager:
    def __init__(self, config_path: Path, frame_store: FrameStore):
        self.config_path = config_path
        self.frame_store = frame_store
        self._threads: Dict[str, CameraThread] = {}
        self._stop_events: Dict[str, threading.Event] = {}
        self._config: list[dict] = []
        self._lock = threading.Lock()
        self._config_mtime: float = 0.0

    # ── Public API ──────────────────────────────────────────────────

    def get_active_cameras(self) -> list[str]:
        with self._lock:
            return list(self._threads.keys())

    def get_config(self) -> list[dict]:
        with self._lock:
            return list(self._config)

    def sync(self) -> bool:
        """Check cameras.json for changes and apply them. Returns True if changed."""
        try:
            mtime = os.path.getmtime(self.config_path)
        except OSError:
            return False

        if mtime <= self._config_mtime:
            return False

        self._config_mtime = mtime
        try:
            with open(self.config_path) as f:
                cameras: list[dict] = json.load(f)
        except Exception as e:
            print(f"[CamMgr] Config load error: {e}")
            return False

        with self._lock:
            self._config = cameras

        desired = {
            c["name"]: c["url"]
            for c in cameras
            if c.get("enabled", True)
        }
        with self._lock:
            current = {n: t.url for n, t in self._threads.items()}

        # Stop removed / URL-changed streams
        for name in list(current):
            if name not in desired or current[name] != desired[name]:
                print(f"[CamMgr] Removing {name}")
                self._stop_events[name].set()
                with self._lock:
                    self._threads.pop(name, None)
                    self._stop_events.pop(name, None)

        # Start new streams
        with self._lock:
            running = set(self._threads.keys())
        for name, url in desired.items():
            if name not in running:
                self._start_thread(name, url)

        return True

    def update_camera(self, cam: dict) -> None:
        """Upsert a single camera (add or replace)."""
        name = cam["name"]
        url = cam.get("url", "")
        enabled = cam.get("enabled", True)

        # Update in-memory config list
        with self._lock:
            existing = next((c for c in self._config if c["name"] == name), None)
            if existing:
                if not url:
                    url = existing.get("url", "")
                    cam["url"] = url
                existing.update(cam)
            else:
                self._config.append(dict(cam))

        # Stop if disabled or URL changed
        with self._lock:
            thread = self._threads.get(name)
        if thread:
            if not enabled or thread.url != url:
                self._stop_events[name].set()
                with self._lock:
                    self._threads.pop(name, None)
                    self._stop_events.pop(name, None)
            else:
                return  # same URL + enabled — no restart needed

        if enabled and url:
            self._start_thread(name, url)

    def remove_camera(self, name: str) -> None:
        ev = self._stop_events.get(name)
        if ev:
            ev.set()
        with self._lock:
            self._threads.pop(name, None)
            self._stop_events.pop(name, None)
            self._config = [c for c in self._config if c["name"] != name]

    def stop_all(self) -> None:
        for ev in list(self._stop_events.values()):
            ev.set()

    # ── Internal ────────────────────────────────────────────────────

    def _start_thread(self, name: str, url: str) -> None:
        print(f"[CamMgr] Starting {name}")
        ev = threading.Event()
        t = CameraThread(name, url, self.frame_store, ev)
        t.start()
        with self._lock:
            self._threads[name] = t
            self._stop_events[name] = ev
