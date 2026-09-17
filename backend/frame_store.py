"""
FrameStore: thread-safe frame buffer.
One slot per camera: (frame_bgr, timestamp).
get_snapshot_b64() encodes to JPEG + base64 for VLM submission.
"""
import threading
import time
import base64
from typing import Optional, Tuple

import cv2
import numpy as np


class FrameStore:
    def __init__(self, max_w: int = 1280, jpeg_quality: int = 82):
        self._store: dict[str, Tuple[np.ndarray, float]] = {}
        self._lock = threading.Lock()
        self.max_w = max_w
        self.jpeg_quality = jpeg_quality

    def put(self, cam_name: str, frame: np.ndarray) -> None:
        with self._lock:
            self._store[cam_name] = (frame, time.monotonic())

    def get_latest(self, cam_name: str) -> Optional[Tuple[np.ndarray, float]]:
        with self._lock:
            entry = self._store.get(cam_name)
            if entry is None:
                return None
            frame, ts = entry
            return frame.copy(), ts

    def get_snapshot_b64(
        self,
        cam_name: str,
        max_w: Optional[int] = None,
        quality: Optional[int] = None,
    ) -> Optional[str]:
        """Returns base64-encoded JPEG of the latest frame, or None if no frame yet."""
        entry = self.get_latest(cam_name)
        if entry is None:
            return None
        frame, _ = entry
        mw = max_w if max_w is not None else self.max_w
        q = quality if quality is not None else self.jpeg_quality
        h, w = frame.shape[:2]
        if w > mw:
            frame = cv2.resize(frame, (mw, int(h * mw / w)), interpolation=cv2.INTER_AREA)
        _, buf = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, q])
        return base64.b64encode(buf).decode()

    def list_cameras(self) -> list[str]:
        with self._lock:
            return list(self._store.keys())

    def remove(self, cam_name: str) -> None:
        with self._lock:
            self._store.pop(cam_name, None)

    def get_frame_age(self, cam_name: str) -> Optional[float]:
        """Returns seconds since last frame, or None."""
        with self._lock:
            entry = self._store.get(cam_name)
            if entry is None:
                return None
            return time.monotonic() - entry[1]
