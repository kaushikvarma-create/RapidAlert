"""
ResultStore: per-camera latest VLM result + rolling history.
Stores both single and concurrent multi-model comparisons + thumbnails.
Tracks global metrics: analyses/sec and latency percentiles.
"""
import threading
import time
from collections import deque
from typing import Optional, Union, List


class ResultStore:
    HISTORY_LEN = 50

    def __init__(self):
        self._latest: dict[str, dict] = {}
        self._latest_concurrent: dict[str, list[dict]] = {}
        self._latest_thumbnails: dict[str, list[str]] = {}
        self._history: dict[str, deque] = {}
        self._latencies: deque = deque(maxlen=100)
        self._e2e_latencies: deque = deque(maxlen=100)
        self._analysis_times: deque = deque(maxlen=500)
        self._lock = threading.Lock()

    def put(
        self,
        cam_name: str,
        results: Union[dict, List[dict]],
        latency: float,
        thumbnails_b64: Optional[List[str]] = None,
        e2e_latency: Optional[float] = None,
    ) -> None:
        ts = time.time()
        res_list = results if isinstance(results, list) else [results]
        primary = res_list[0] if res_list else {}
        record = dict(primary, ts=ts, latency=round(latency, 2))
        if e2e_latency is not None:
            record["e2e_latency"] = round(e2e_latency, 2)
            record["trigger_to_post"] = round(e2e_latency, 2)
        else:
            record["e2e_latency"] = None
            record["trigger_to_post"] = None

        with self._lock:
            self._latest[cam_name] = record
            self._latest_concurrent[cam_name] = res_list
            if thumbnails_b64:
                self._latest_thumbnails[cam_name] = thumbnails_b64
            if cam_name not in self._history:
                self._history[cam_name] = deque(maxlen=self.HISTORY_LEN)
            self._history[cam_name].append(record)
            self._latencies.append(latency)
            if e2e_latency is not None:
                self._e2e_latencies.append(e2e_latency)
            self._analysis_times.append(ts)

    def get_latest(self, cam_name: str) -> Optional[dict]:
        with self._lock:
            r = self._latest.get(cam_name)
            return dict(r) if r else None

    def get_all_latest(self) -> dict[str, dict]:
        with self._lock:
            return {k: dict(v) for k, v in self._latest.items()}

    def get_all_latest_concurrent(self) -> dict[str, dict]:
        """Returns full multi-model results and thumbnails for all cameras."""
        with self._lock:
            out = {}
            for cam in set(list(self._latest.keys()) + list(self._latest_concurrent.keys())):
                out[cam] = {
                    "results": list(self._latest_concurrent.get(cam, [])),
                    "thumbnails_b64": list(self._latest_thumbnails.get(cam, [])),
                }
            return out

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

            e2es = sorted(self._e2e_latencies)
            m = len(e2es)
            p50_e2e = round(e2es[int(m * 0.50)], 2) if m else 0
            p95_e2e = round(e2es[min(int(m * 0.95), m - 1)], 2) if m else 0

            return {
                "analyses_per_sec": analyses_per_sec,
                "p50_latency": p50,
                "p95_latency": p95,
                "p50_e2e_latency": p50_e2e,
                "p95_e2e_latency": p95_e2e,
                "total_analyses": len(self._analysis_times),
            }
