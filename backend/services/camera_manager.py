"""
CameraManager: one daemon thread per enabled camera.
Each thread continuously reads RTSP frames → FrameStore.
Watches cameras.json for mtime changes; adds/removes streams dynamically.
"""
from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path
from typing import Dict, Optional

import cv2

os.environ["OPENCV_FFMPEG_CAPTURE_OPTIONS"] = "rtsp_transport;tcp|fflags;nobuffer|flags;low_delay|framedrop;1|max_delay;0|timeout;2000000"

from backend.services.frame_store import FrameStore
from backend.services.nvidia_ingest import NvidiaStreamCapture, is_nvidia_available
from backend.core.config import (
    CAMERA_RETRY_DELAY_SEC,
    CAMERA_READ_TIMEOUT_SEC,
    CAMERA_BUFFER_SIZE,
    DEFAULT_FRAME_WIDTH,
    DEFAULT_FRAME_HEIGHT,
)
from backend.core.error_tracker import error_tracker


class CameraThread(threading.Thread):
    RETRY_DELAY = CAMERA_RETRY_DELAY_SEC

    def __init__(
        self,
        cam_name: str,
        url: str,
        frame_store: FrameStore,
        stop_event: threading.Event,
        use_nvidia: bool = False,
    ):
        super().__init__(name=f"cam-{cam_name}", daemon=True)
        self.cam_name = cam_name
        self.url = url
        self.frame_store = frame_store
        self.stop_event = stop_event
        self.use_nvidia = use_nvidia and is_nvidia_available()
        self.connected = False
        self.connected_mode = "OFFLINE"

    def run(self) -> None:
        nvdec_consecutive_failures = 0
        MAX_NVDEC_RETRIES = 3

        while not self.stop_event.is_set():
            cap = None
            is_hw = False

            # Prefer Hardware NVDEC if available
            if self.use_nvidia and nvdec_consecutive_failures < MAX_NVDEC_RETRIES:
                for nvdec_attempt in range(1, MAX_NVDEC_RETRIES + 1):
                    if self.stop_event.is_set():
                        break
                    try:
                        cap = NvidiaStreamCapture(self.url, width=DEFAULT_FRAME_WIDTH, height=DEFAULT_FRAME_HEIGHT)
                        if cap.isOpened():
                            is_hw = True
                            print(f"[CamMgr] ⚡ {self.cam_name} using NVIDIA Hardware Decoder (NVDEC)")
                            break
                        else:
                            cap.release()
                            cap = None
                    except Exception as e:
                        error_tracker.capture_exception(
                            e,
                            component="CameraManager",
                            camera=self.cam_name,
                            effect=f"NVIDIA NVDEC hardware decode attempt {nvdec_attempt}/{MAX_NVDEC_RETRIES} failed for {self.cam_name}",
                            severity="WARNING",
                        )
                        cap = None
                    time.sleep(0.3 * nvdec_attempt)

                if cap is None or not is_hw:
                    nvdec_consecutive_failures += 1
                    print(f"[CamMgr] ⚠️ {self.cam_name} NVDEC hardware decode unavailable after {MAX_NVDEC_RETRIES} attempts; falling back to CPU OpenCV decoder")

            if cap is None:
                is_hw = False
                cap = cv2.VideoCapture(self.url, cv2.CAP_FFMPEG)
                cap.set(cv2.CAP_PROP_BUFFERSIZE, CAMERA_BUFFER_SIZE)

            got_frame = False
            failed_attempts = 0
            # Allow warm-up period: RTSP handshakes & keyframe acquisition take 1-4 seconds
            WARMUP_MAX_ATTEMPTS = 30 if is_hw else 15

            while not self.stop_event.is_set():
                if is_hw:
                    ok, frame = cap.read(timeout_sec=CAMERA_READ_TIMEOUT_SEC)
                else:
                    ok, frame = cap.read()

                if ok and frame is not None:
                    self.frame_store.put(self.cam_name, frame)
                    failed_attempts = 0
                    if not got_frame:
                        got_frame = True
                        self.connected = True
                        self.connected_mode = "NVDEC" if is_hw else "CPU-OpenCV"
                        if is_hw:
                            nvdec_consecutive_failures = 0
                        print(f"[CamMgr] ✅ {self.cam_name} connected ({self.connected_mode})")
                else:
                    failed_attempts += 1
                    # During initial handshake / warm-up, wait up to WARMUP_MAX_ATTEMPTS
                    if not got_frame:
                        if failed_attempts >= WARMUP_MAX_ATTEMPTS:
                            if is_hw:
                                nvdec_consecutive_failures += 1
                                print(f"[CamMgr] ⚠️ {self.cam_name} NVDEC keyframe acquisition timeout ({failed_attempts} attempts); retrying connection")
                            else:
                                print(f"[CamMgr] ⚠️ {self.cam_name} CPU RTSP stream timeout ({failed_attempts} attempts)")
                            break
                        time.sleep(0.12)
                        continue

                    # If stream was already active and drops for more than 5 seconds (~10 attempts)
                    if got_frame and failed_attempts >= 10:
                        self.connected = False
                        self.connected_mode = "DISCONNECTED"
                        error_tracker.capture_error(
                            message=f"RTSP stream connection lost for {self.cam_name}",
                            component="CameraManager",
                            camera=self.cam_name,
                            effect=f"Stream ingestion halted; entering reconnect backoff ({self.RETRY_DELAY}s)",
                            severity="WARNING",
                        )
                        break
                    time.sleep(0.05)

            if cap:
                cap.release()

            # If running on CPU, periodically allow retry of NVDEC on next cycle
            if not is_hw:
                nvdec_consecutive_failures = 0

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

    def get_camera_modes(self) -> dict[str, str]:
        with self._lock:
            return {name: thread.connected_mode for name, thread in self._threads.items()}

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
            error_tracker.capture_exception(
                e,
                component="CameraManager",
                effect=f"Failed to load cameras config from {self.config_path}; sync skipped",
                severity="ERROR",
            )
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
        t = CameraThread(name, url, self.frame_store, ev, use_nvidia=True)
        t.start()
        with self._lock:
            self._threads[name] = t
            self._stop_events[name] = ev
        time.sleep(0.18)
