"""
FrameStore: thread-safe frame buffer.
One slot per camera: (frame_bgr, timestamp).
get_snapshot_b64() encodes to JPEG + base64 for VLM submission.
"""
from __future__ import annotations

import base64
import collections
import threading
import time
from typing import Optional, Tuple

import cv2
import numpy as np

from backend.core.config import DEFAULT_FRAME_WIDTH, DEFAULT_JPEG_QUALITY
from backend.core.error_tracker import error_tracker


class FrameStore:
    def __init__(self, max_w: int = DEFAULT_FRAME_WIDTH, jpeg_quality: int = DEFAULT_JPEG_QUALITY):
        self._store: dict[str, collections.deque[Tuple[np.ndarray, float]]] = collections.defaultdict(
            lambda: collections.deque(maxlen=300)
        )
        self._latest_jpeg: dict[str, Tuple[float, bytes, str]] = {}
        self._lock = threading.Lock()
        self.max_w = max_w
        self.jpeg_quality = jpeg_quality

    def put(self, cam_name: str, frame: np.ndarray) -> None:
        if frame is None or frame.size == 0:
            return
        # Reject uninitialized YUV zeroed buffers (solid bright green: B<25, R<25, G>90 or pitch black: mean<3)
        mean_b, mean_g, mean_r = cv2.mean(frame)[:3]
        if mean_g > 90 and mean_r < 25 and mean_b < 25:
            return
        if mean_g < 3 and mean_r < 3 and mean_b < 3:
            return

        ts = time.monotonic()
        # Fast one-pass JPEG encode upon frame ingestion for zero-latency multi-client streaming
        try:
            h, w = frame.shape[:2]
            if w > 640:
                small = cv2.resize(frame, (640, int(h * 640 / w)), interpolation=cv2.INTER_LINEAR)
            else:
                small = frame
            ok, buf = cv2.imencode(".jpg", small, [cv2.IMWRITE_JPEG_QUALITY, 65, cv2.IMWRITE_JPEG_OPTIMIZE, 0])
            if ok:
                raw_bytes = buf.tobytes()
                b64_str = base64.b64encode(buf).decode()
                cached_tuple = (ts, raw_bytes, b64_str)
            else:
                cached_tuple = None
        except Exception:
            cached_tuple = None

        with self._lock:
            self._store[cam_name].append((frame, ts))
            if cached_tuple is not None:
                self._latest_jpeg[cam_name] = cached_tuple

    def get_latest(self, cam_name: str) -> Optional[Tuple[np.ndarray, float]]:
        with self._lock:
            q = self._store.get(cam_name)
            if not q:
                return None
            frame, ts = q[-1]
            return frame.copy(), ts

    def get_latest_cached_entry(self, cam_name: str) -> Optional[Tuple[float, str]]:
        """Returns (ts, b64_str) directly from cache with zero NumPy copy overhead."""
        with self._lock:
            entry = self._latest_jpeg.get(cam_name)
            if entry is not None:
                return entry[0], entry[2]
        return None

    def get_cached_snapshot_b64(self, cam_name: str) -> Optional[str]:
        """Returns pre-encoded base64 string with zero CPU overhead."""
        with self._lock:
            entry = self._latest_jpeg.get(cam_name)
            if entry is not None:
                return entry[2]
        return self.get_snapshot_b64(cam_name)

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
        return self._encode_frame(frame, max_w, quality, cam_name=cam_name)

    def get_snapshot_jpeg(
        self,
        cam_name: str,
        max_w: Optional[int] = None,
        quality: Optional[int] = None,
    ) -> Optional[bytes]:
        """Returns raw JPEG bytes of the latest frame with zero base64 overhead."""
        entry = self.get_latest(cam_name)
        if entry is None:
            return None
        frame, _ = entry
        try:
            mw = max_w if max_w is not None else self.max_w
            q = quality if quality is not None else self.jpeg_quality
            h, w = frame.shape[:2]
            if w > mw:
                frame = cv2.resize(frame, (mw, int(h * mw / w)), interpolation=cv2.INTER_AREA)
            ok, buf = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, q])
            if ok:
                return buf.tobytes()
        except Exception:
            pass
        return None

    def get_latest_jpeg_entry(
        self,
        cam_name: str,
        max_w: Optional[int] = None,
        quality: Optional[int] = None,
    ) -> Optional[Tuple[float, bytes]]:
        """Returns (timestamp, jpeg_bytes) of the latest frame."""
        with self._lock:
            cached = self._latest_jpeg.get(cam_name)
            if cached is not None and (max_w is None or max_w == 640 or max_w >= 640):
                return cached[0], cached[1]

        entry = self.get_latest(cam_name)
        if entry is None:
            return None
        frame, ts = entry
        try:
            mw = max_w if max_w is not None else self.max_w
            q = quality if quality is not None else self.jpeg_quality
            h, w = frame.shape[:2]
            if w > mw:
                frame = cv2.resize(frame, (mw, int(h * mw / w)), interpolation=cv2.INTER_AREA)
            ok, buf = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, q])
            if ok:
                return ts, buf.tobytes()
        except Exception:
            pass
        return None

    def get_pre_trigger_frames(
        self,
        cam_name: str,
        trigger_time: float,
        offsets: list[float] = [-5.0, -2.0, -0.5],
    ) -> list[np.ndarray]:
        """Finds frames closest to trigger_time + offset for each offset."""
        with self._lock:
            q = self._store.get(cam_name)
            if not q:
                return []
            items = list(q)

        results = []
        for offset in offsets:
            target_ts = trigger_time + offset
            # Find item with minimal absolute timestamp difference
            closest = min(items, key=lambda item: abs(item[1] - target_ts))
            results.append(closest[0].copy())
        return results

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
            items = list(q)
            
        now = time.monotonic()
        target_start = now - span_sec
        
        # Filter to frames roughly within the window (plus some slack)
        window = [item for item in items if item[1] >= target_start - 2.0]
        
        if not window:
            window = items
        if not window:
            return None

        # If we have fewer than requested, pad by repeating the oldest valid frame
        if len(window) < count:
            oldest = window[0]
            padded = [oldest] * (count - len(window)) + list(window)
            selected = padded
        elif len(window) == count:
            selected = window
        else:
            # Evenly sample `count` frames from the window
            indices = np.linspace(0, len(window) - 1, count, dtype=int)
            selected = [window[i] for i in indices]

        return [self._encode_frame(f, max_w, quality, cam_name=cam_name) for f, _ in selected]

    def encode_frames(
        self,
        frames: list[np.ndarray],
        max_w: Optional[int] = None,
        quality: Optional[int] = None,
    ) -> list[str]:
        return [self._encode_frame(f, max_w, quality) for f in frames]

    def _encode_frame(
        self,
        frame: np.ndarray,
        max_w: Optional[int] = None,
        quality: Optional[int] = None,
        cam_name: Optional[str] = None,
    ) -> str:
        try:
            mw = max_w if max_w is not None else self.max_w
            q = quality if quality is not None else self.jpeg_quality
            h, w = frame.shape[:2]
            if w > mw:
                frame = cv2.resize(frame, (mw, int(h * mw / w)), interpolation=cv2.INTER_AREA)
            ok, buf = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, q])
            if not ok:
                raise ValueError("cv2.imencode returned False")
            return base64.b64encode(buf).decode()
        except Exception as exc:
            error_tracker.capture_exception(
                exc,
                component="FrameStore",
                camera=cam_name,
                effect=f"Failed to encode JPEG frame for camera {cam_name}; returning empty base64",
                severity="WARNING",
            )
            return ""

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
