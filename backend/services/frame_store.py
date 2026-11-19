"""
FrameStore: thread-safe frame buffer.
One slot per camera: (frame_bgr, timestamp).
get_snapshot_b64() encodes to JPEG + base64 for VLM submission.
"""
from __future__ import annotations

import asyncio
import base64
import collections
import threading
import time
from typing import Optional, Tuple, Set

import cv2
import numpy as np

from backend.core.config import DEFAULT_FRAME_WIDTH, DEFAULT_JPEG_QUALITY, INGEST_FPS, FRAME_HISTORY_SEC
from backend.core.error_tracker import error_tracker


class FrameStore:
    def __init__(self, max_w: int = DEFAULT_FRAME_WIDTH, jpeg_quality: int = DEFAULT_JPEG_QUALITY):
        self._store: dict[str, collections.deque[Tuple[np.ndarray, float]]] = collections.defaultdict(
            lambda: collections.deque(maxlen=INGEST_FPS * FRAME_HISTORY_SEC)
        )
        self._frame_seq: dict[str, int] = collections.defaultdict(int)
        self._latest_jpeg: dict[str, Tuple[int, float, bytes, int, int]] = {}  # (seq, ts, bytes, w, q)
        # One mutex guards frame sequence state; each camera gets its own Condition on it so a
        # new frame wakes only that camera's stream readers. With a single shared Condition every
        # frame from any camera woke every MJPEG reader (18 cameras x 25fps = ~450 spurious
        # wake-ups per second per viewer), each of which then returned empty-handed.
        self._mutex = threading.RLock()
        self._condition = threading.Condition(self._mutex)
        self._cam_conditions: dict[str, threading.Condition] = {}
        self._lock = threading.Lock()
        self.max_w = max_w
        self.jpeg_quality = jpeg_quality

    def put(self, cam_name: str, frame: np.ndarray) -> None:
        if frame is None or frame.size == 0:
            return
        ts = time.monotonic()
        with self._mutex:
            # Keep the buffer time-based for every source: the hardware pipeline is already capped
            # at INGEST_FPS, but the CPU fallback decoder delivers the camera's full frame rate,
            # which would shrink a fixed-length buffer to a few seconds of history.
            q = self._store[cam_name]
            if q and ts - q[-1][1] < 0.75 / INGEST_FPS:
                return
            q.append((frame, ts))
            self._frame_seq[cam_name] += 1
            cond = self._cam_conditions.get(cam_name)
            if cond is not None:
                cond.notify_all()

    def get_frame_since(
        self,
        cam_name: str,
        last_seq: int,
        timeout: float = 1.0,
        max_w: Optional[int] = None,
        quality: Optional[int] = None,
    ) -> Optional[Tuple[int, bytes]]:
        """
        Thread-safe blocking wait for next camera frame sequence.
        Returns (seq, jpeg_bytes) immediately if new frame is ready, or waits up to timeout seconds.
        """
        mw = max_w if max_w is not None else 1280
        q = quality if quality is not None else 78

        with self._mutex:
            cond = self._cam_conditions.get(cam_name)
            if cond is None:
                cond = self._cam_conditions[cam_name] = threading.Condition(self._mutex)
            deadline = time.monotonic() + timeout
            cur_seq = self._frame_seq.get(cam_name, 0)
            while cur_seq <= last_seq:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                cond.wait(timeout=remaining)
                cur_seq = self._frame_seq.get(cam_name, 0)

            if cur_seq <= last_seq or cam_name not in self._store or not self._store[cam_name]:
                return None

            # Fast path: check if this sequence is already cached
            cached = self._latest_jpeg.get(cam_name)
            if cached is not None and cached[0] == cur_seq and cached[3] == mw and cached[4] == q:
                return cur_seq, cached[2]

            frame, ts = self._store[cam_name][-1]

        # Single-pass encode outside condition lock
        try:
            h, w = frame.shape[:2]
            if w > mw:
                resized = cv2.resize(frame, (mw, int(h * mw / w)), interpolation=cv2.INTER_LINEAR)
            else:
                resized = frame
            ok, buf = cv2.imencode(".jpg", resized, [cv2.IMWRITE_JPEG_QUALITY, q, cv2.IMWRITE_JPEG_OPTIMIZE, 0])
            if ok:
                raw_bytes = buf.tobytes()
                with self._condition:
                    self._latest_jpeg[cam_name] = (cur_seq, ts, raw_bytes, mw, q)
                return cur_seq, raw_bytes
        except Exception:
            pass
        return None

    def has_frames(self, cam_name: str) -> bool:
        """True if at least one frame is buffered. Use this instead of get_latest() for existence
        checks: get_latest() copies the whole frame."""
        with self._lock:
            return bool(self._store.get(cam_name))

    def get_latest(self, cam_name: str) -> Optional[Tuple[np.ndarray, float]]:
        with self._lock:
            q = self._store.get(cam_name)
            if not q:
                return None
            frame, ts = q[-1]
            return frame.copy(), ts

    def get_latest_cached_entry(self, cam_name: str) -> Optional[Tuple[float, str]]:
        """Returns (ts, b64_str) directly from cache or on-demand encode."""
        entry = self.get_latest_jpeg_entry(cam_name, max_w=320, quality=60)
        if entry is None:
            return None
        ts, raw_bytes = entry
        b64_str = base64.b64encode(raw_bytes).decode()
        return ts, b64_str

    def get_cached_snapshot_b64(self, cam_name: str) -> Optional[str]:
        """Returns snapshot base64 string."""
        entry = self.get_latest_cached_entry(cam_name)
        return entry[1] if entry else self.get_snapshot_b64(cam_name)

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
        """Returns raw JPEG bytes of the latest frame."""
        entry = self.get_latest_jpeg_entry(cam_name, max_w=max_w, quality=quality)
        return entry[1] if entry else None

    def get_latest_jpeg_entry(
        self,
        cam_name: str,
        max_w: Optional[int] = None,
        quality: Optional[int] = None,
    ) -> Optional[Tuple[float, bytes]]:
        """Returns (timestamp, jpeg_bytes) of the latest frame, with memoized cache."""
        mw = max_w if max_w is not None else 1280
        q = quality if quality is not None else 78

        with self._lock:
            q_store = self._store.get(cam_name)
            if not q_store:
                return None
            frame, ts = q_store[-1]
            cached = self._latest_jpeg.get(cam_name)
            if cached is not None and cached[0] == ts and cached[2] == mw and cached[3] == q:
                return ts, cached[1]

        # Fast encode on-demand outside lock to prevent contention
        try:
            h, w = frame.shape[:2]
            if w > mw:
                resized = cv2.resize(frame, (mw, int(h * mw / w)), interpolation=cv2.INTER_LINEAR)
            else:
                resized = frame
            ok, buf = cv2.imencode(".jpg", resized, [cv2.IMWRITE_JPEG_QUALITY, q, cv2.IMWRITE_JPEG_OPTIMIZE, 0])
            if ok:
                raw_bytes = buf.tobytes()
                with self._lock:
                    self._latest_jpeg[cam_name] = (ts, raw_bytes, mw, q)
                return ts, raw_bytes
        except Exception:
            pass
        return None

    def get_pre_trigger_frames_with_timestamps(
        self,
        cam_name: str,
        trigger_time: float,
        offsets: list[float] = [-5.0, -2.0, -0.5],
    ) -> list[tuple[np.ndarray, float]]:
        """Find frames closest to trigger offsets and return their capture timestamps."""
        with self._lock:
            q = self._store.get(cam_name)
            if not q:
                return []
            items = list(q)

        results = []
        for offset in offsets:
            target_ts = trigger_time + offset
            closest = min(items, key=lambda item: abs(item[1] - target_ts))
            results.append((closest[0].copy(), closest[1]))
        return results

    def get_pre_trigger_frames(
        self,
        cam_name: str,
        trigger_time: float,
        offsets: list[float] = [-5.0, -2.0, -0.5],
    ) -> list[np.ndarray]:
        return [frame for frame, _ in self.get_pre_trigger_frames_with_timestamps(cam_name, trigger_time, offsets)]

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
            
        selected = self._select_temporal(items, count, span_sec)
        if not selected:
            return None
        return [self._encode_frame(f, max_w, quality, cam_name=cam_name) for f, _ in selected]

    @staticmethod
    def _select_temporal(items: list, count: int, span_sec: float) -> list:
        """
        Pick `count` frames evenly spaced over exactly `span_sec`, ending at the newest frame:
        for count=4, span=10 the targets are t-10.0, t-6.67, t-3.33 and t-0, and the frame closest
        to each target is used.

        The previous version sampled by index over a window with 2 seconds of slack, so the frames
        handed to the model sat at about t-12, t-8, t-4, t-0 while their labels said
        t-10, t-6.5, t-3, t-0. If less than `span_sec` of history exists, the oldest frame repeats.
        """
        if not items:
            return []
        if count <= 1:
            return [items[-1]]
        newest_ts = items[-1][1]
        # Right after a (re)connect there is less history than asked for: spread the frames over
        # what exists rather than repeating the oldest one three times.
        span_sec = min(span_sec, newest_ts - items[0][1])
        targets =[newest_ts - span_sec * (count - 1 - i) / (count - 1) for i in range(count)]
        return [min(items, key=lambda item: abs(item[1] - t)) for t in targets]

    def get_temporal_snapshots_with_timestamps_b64(
        self,
        cam_name: str,
        count: int = 4,
        span_sec: float = 10.0,
        max_w: Optional[int] = None,
        quality: Optional[int] = None,
    ) -> Optional[list[tuple[str, float]]]:
        """Return encoded temporal frames with their monotonic capture timestamps."""
        with self._lock:
            q = self._store.get(cam_name)
            if not q:
                return None
            items = list(q)

        selected = self._select_temporal(items, count, span_sec)
        if not selected:
            return None
        return [(self._encode_frame(frame, max_w, quality, cam_name=cam_name), ts) for frame, ts in selected]

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
