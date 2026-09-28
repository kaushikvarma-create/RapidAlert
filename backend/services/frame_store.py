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

from backend.core.config import DEFAULT_FRAME_WIDTH, DEFAULT_JPEG_QUALITY
from backend.core.error_tracker import error_tracker


class FrameStore:
    def __init__(self, max_w: int = DEFAULT_FRAME_WIDTH, jpeg_quality: int = DEFAULT_JPEG_QUALITY):
        self._store: dict[str, collections.deque[Tuple[np.ndarray, float]]] = collections.defaultdict(
            lambda: collections.deque(maxlen=300)
        )
        self._latest_jpeg: dict[str, Tuple[float, bytes, int, int]] = {}
        self._subscribers: dict[str, Set[asyncio.Queue]] = collections.defaultdict(set)
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._lock = threading.Lock()
        self.max_w = max_w
        self.jpeg_quality = jpeg_quality

    def set_event_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        """Binds the FastAPI asyncio event loop for threadsafe stream push notifications."""
        self._loop = loop

    def subscribe(self, cam_name: str) -> asyncio.Queue:
        """Subscribes an active client stream to instant push frame events (zero polling latency)."""
        q: asyncio.Queue = asyncio.Queue(maxsize=2)
        with self._lock:
            self._subscribers[cam_name].add(q)
            cached = self._latest_jpeg.get(cam_name)
            if cached is not None:
                try:
                    q.put_nowait(cached[1])
                except Exception:
                    pass
        return q

    def unsubscribe(self, cam_name: str, q: asyncio.Queue) -> None:
        """Removes a client stream subscription."""
        with self._lock:
            if cam_name in self._subscribers:
                self._subscribers[cam_name].discard(q)

    def put(self, cam_name: str, frame: np.ndarray) -> None:
        if frame is None or frame.size == 0:
            return
        ts = time.monotonic()
        with self._lock:
            self._store[cam_name].append((frame, ts))
            subs = list(self._subscribers.get(cam_name, []))

        # If any client is actively streaming this camera, encode ONCE and push immediately
        if subs:
            try:
                ok, buf = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, 78])
                if ok:
                    raw_bytes = buf.tobytes()
                    with self._lock:
                        self._latest_jpeg[cam_name] = (ts, raw_bytes, self.max_w, 78)
                    if self._loop and self._loop.is_running():
                        for q in subs:
                            def _push(queue=q, data=raw_bytes):
                                if queue.full():
                                    try:
                                        queue.get_nowait()
                                    except Exception:
                                        pass
                                try:
                                    queue.put_nowait(data)
                                except Exception:
                                    pass
                            self._loop.call_soon_threadsafe(_push)
            except Exception:
                pass

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
