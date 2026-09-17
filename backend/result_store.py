"""
ResultStore: per-camera latest VLM result + rolling history.
Also tracks global metrics: analyses/sec and latency percentiles.
"""
import threading
import time
from collections import deque
from typing import Optional


class ResultStore:
    HISTORY_LEN = 50

    def __init__(self):
        self._latest: dict[str, dict] = {}
        self._history: dict[str, deque] = {}
        self._latencies: deque = deque(maxlen=100)
        self._analysis_times: deque = deque(maxlen=500)
        self._lock = threading.Lock()

    def put(self, cam_name: str, result: dict, latency: float) -> None:
        ts = time.time()
        record = dict(result, ts=ts, latency=round(latency, 2))
        with self._lock:
            self._latest[cam_name] = record
            if cam_name not in self._history:
                self._history[cam_name] = deque(maxlen=self.HISTORY_LEN)
            self._history[cam_name].append(record)
            self._latencies.append(latency)
            self._analysis_times.append(ts)

    def get_latest(self, cam_name: str) -> Optional[dict]:
        with self._lock:
            r = self._latest.get(cam_name)
            return dict(r) if r else None

    def get_all_latest(self) -> dict[str, dict]:
        with self._lock:
            return {k: dict(v) for k, v in self._latest.items()}

    def get_history(self, cam_name: str) -> list[dict]:
        with self._lock:
            h = self._history.get(cam_name)
            return list(h) if h else []

    def get_metrics(self) -> dict:
        with self._lock:
            now = time.time()
            recent = [t for t in self._analysis_times if now - t < 10]
            analyses_per_sec = round(len(recent) / 10, 2)

            lats = sorted(self._latencies)
            n = len(lats)
            p50 = round(lats[int(n * 0.50)], 2) if n else 0
            p95 = round(lats[min(int(n * 0.95), n - 1)], 2) if n else 0

            return {
                "analyses_per_sec": analyses_per_sec,
                "p50_latency": p50,
                "p95_latency": p95,
                "total_analyses": len(self._analysis_times),
            }
