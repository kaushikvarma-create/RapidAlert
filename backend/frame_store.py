"""
FrameStore: thread-safe frame buffer.
One slot per camera: (frame_bgr, timestamp).
get_snapshot_b64() encodes to JPEG + base64 for VLM submission.
"""
import threading
import time
import base64
import collections
from typing import Optional, Tuple

import cv2
import numpy as np


class FrameStore:
    def __init__(self, max_w: int = 1280, jpeg_quality: int = 82):
        self._store: dict[str, collections.deque[Tuple[np.ndarray, float]]] = collections.defaultdict(
            lambda: collections.deque(maxlen=150)
        )
        self._lock = threading.Lock()
        self.max_w = max_w
        self.jpeg_quality = jpeg_quality

    def put(self, cam_name: str, frame: np.ndarray) -> None:
        with self._lock:
            self._store[cam_name].append((frame, time.monotonic()))

    def get_latest(self, cam_name: str) -> Optional[Tuple[np.ndarray, float]]:
        with self._lock:
            q = self._store.get(cam_name)
            if not q:
                return None
            frame, ts = q[-1]
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
        return self._encode_frame(frame, max_w, quality)

    def get_temporal_snapshots_b64(
        self,
        cam_name: str,
        count: int = 4,
        span_sec: float = 10.0,
        max_w: Optional[int] = None,
        quality: Optional[int] = None,
    ) -> Optional[list[str]]:
        """Returns evenly sampled frames over the requested time span."""
        with self._lock:
            q = self._store.get(cam_name)
            if not q:
                return None
            # Extract items to list
            items = list(q)
            
        now = time.monotonic()
        target_start = now - span_sec
        
        # Filter to frames roughly within the window (plus some slack)
        window = [item for item in items if item[1] >= target_start - 2.0]
        
        if not window:
            window = [items[-1]] # Fallback to latest if nothing in window

        # If we have less than requested, just take what we have
        if len(window) <= count:
            selected = window
        else:
            # Evenly sample `count` frames from the window
            indices = np.linspace(0, len(window) - 1, count, dtype=int)
            selected = [window[i] for i in indices]

        return [self._encode_frame(f, max_w, quality) for f, _ in selected]

    def _encode_frame(
        self,
        frame: np.ndarray,
        max_w: Optional[int] = None,
        quality: Optional[int] = None,
    ) -> str:
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
            q = self._store.get(cam_name)
            if not q:
                return None
            return time.monotonic() - q[-1][1]
