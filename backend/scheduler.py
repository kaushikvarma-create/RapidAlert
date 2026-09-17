"""
DeadlineScheduler: the core speed engine.

Architecture:
  - _feed_loop: always running, picks the most stale camera not currently
    in-flight and pushes it onto an asyncio.Queue.
  - _worker (N instances): pulls from the queue, calls VLM, stores result,
    broadcasts to WS.
  - _tune_loop: every TUNE_INTERVAL seconds, inspects p95 latency and
    adds/removes a worker to keep latency bounded.
  - _metrics_loop: broadcasts metrics every 5s.

Speed principle: the feed loop never sleeps longer than 5ms when work is
available. Workers start new jobs immediately after finishing. The semaphore
is replaced by explicit worker count (easier to auto-tune).
"""
import asyncio
import time
import traceback
from typing import Callable, Optional

from camera_manager import CameraManager
from frame_store import FrameStore
from vlm_client import VLMPool
from prompt_manager import PromptManager
from result_store import ResultStore
from alert_engine import AlertEngine
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from storage import StorageManager


class DeadlineScheduler:
    TUNE_INTERVAL = 15          # seconds between auto-tune ticks
    LATENCY_UP_THRESH = 4.0     # p95 below → add a worker
    LATENCY_DN_THRESH = 12.0    # p95 above → remove a worker
    # MIN_WORKERS is set in __init__ to max(2, n_vllm_endpoints)
    FEED_TICK = 0.005           # 5 ms — time between feed loop iterations
    IDLE_TICK = 0.05            # 50 ms — when no cameras are available
    QUEUE_CAP_PER_WORKER = 1    # max items queued per worker — prevents pile-up
                                # when cams > workers; each cam waits ≤1 cycle

    def __init__(
        self,
        camera_manager: CameraManager,
        frame_store: FrameStore,
        vlm_pool: VLMPool,
        prompt_manager: PromptManager,
        result_store: ResultStore,
        alert_engine: AlertEngine,
        initial_concurrency: int = 4,
        max_concurrency: int = 16,
        broadcast_fn: Optional[Callable] = None,
        storage=None,   # StorageManager | None
    ):
        self.camera_manager = camera_manager
        self.frame_store = frame_store
        self.vlm_pool = vlm_pool
        self.prompt_manager = prompt_manager
        self.result_store = result_store
        self.alert_engine = alert_engine
        self.broadcast_fn = broadcast_fn
        self.storage = storage

        # We want to fully utilize vLLM's internal continuous batching.
        # Throttling requests at the app level prevents vLLM from building
        # efficient batches. Pin workers to max_concurrency.
        self.MIN_WORKERS = max_concurrency
        self._concurrency = max_concurrency
        self._max_concurrency = max_concurrency
        self._running = False
        self._last_analyzed: dict[str, float] = {}
        self._in_flight: set[str] = set()
        self._workers: list[asyncio.Task] = []
        self._queue: Optional[asyncio.Queue] = None
        self._latencies: list[float] = []
        self._total_analyzed = 0

    # ── Properties ─────────────────────────────────────────────────

    @property
    def concurrency(self) -> int:
        return len(self._workers)

    @property
    def queue_depth(self) -> int:
        return self._queue.qsize() if self._queue else 0

    @property
    def in_flight_count(self) -> int:
        return len(self._in_flight)

    # ── Lifecycle ───────────────────────────────────────────────────

    async def start(self) -> None:
        self._queue = asyncio.Queue(maxsize=0)
        self._running = True
        self._workers = [
            asyncio.create_task(self._worker(i), name=f"vlm-worker-{i}")
            for i in range(self._concurrency)
        ]
        self._bg_tasks = [
            asyncio.create_task(self._feed_loop(), name="scheduler-feed"),
            asyncio.create_task(self._tune_loop(), name="scheduler-tune"),
            asyncio.create_task(self._metrics_loop(), name="scheduler-metrics")
        ]
        print(
            f"[Scheduler] ✅ Started — {self._concurrency} workers, "
            f"min={self.MIN_WORKERS}, max={self._max_concurrency}, "
            f"endpoints={len(self.vlm_pool.endpoints)}"
        )

    async def stop(self) -> None:
        self._running = False
        for w in self._workers:
            w.cancel()
        for t in self._bg_tasks:
            t.cancel()
        self._workers.clear()
        self._bg_tasks.clear()

    def set_concurrency_limit(self, n: int) -> None:
        """Manually set target concurrency (dashboard override)."""
        self._max_concurrency = max(self.MIN_WORKERS, n)

    # ── Feed loop ───────────────────────────────────────────────────

    async def _feed_loop(self) -> None:
        """Continuously picks the most stale eligible camera and queues it.

        Queue depth is capped at QUEUE_CAP_PER_WORKER × workers so cameras
        don't pile up waiting behind each other when there are more cameras
        than workers.  Without this, N cameras queued ahead of camera X means
        X waits N/workers × latency ≈ 15-18 s between analyses.
        """
        while self._running:
            cams = self.camera_manager.get_active_cameras()
            eligible = [
                c for c in cams
                if c not in self._in_flight
                and self.frame_store.get_latest(c) is not None
            ]

            # Don't flood the queue — workers should always have fresh work,
            # not be processing a backlog from several seconds ago.
            queue_cap = max(1, len(self._workers)) * self.QUEUE_CAP_PER_WORKER
            if eligible and self._queue.qsize() < queue_cap:
                now = time.monotonic()
                cam = max(
                    eligible,
                    key=lambda c: now - self._last_analyzed.get(c, 0),
                )
                self._in_flight.add(cam)
                await self._queue.put(cam)
                await asyncio.sleep(self.FEED_TICK)
            else:
                await asyncio.sleep(self.IDLE_TICK)

    # ── Worker ──────────────────────────────────────────────────────

    async def _worker(self, worker_id: int) -> None:
        while self._running:
            try:
                cam = await asyncio.wait_for(self._queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            try:
                await self._analyze(cam)
            except Exception:
                traceback.print_exc()
            finally:
                self._in_flight.discard(cam)
                self._queue.task_done()

    async def _analyze(self, cam_name: str) -> None:
        # Full-res frame for VLM
        frame_b64 = self.frame_store.get_snapshot_b64(cam_name)
        if frame_b64 is None:
            return

        # Smaller thumbnail for WS broadcast (saves bandwidth)
        thumb_b64 = self.frame_store.get_snapshot_b64(cam_name, max_w=320, quality=65)
        prompt = self.prompt_manager.get_prompt(cam_name)

        t0 = time.monotonic()
        result = await self.vlm_pool.analyze(cam_name, frame_b64, prompt)
        latency = time.monotonic() - t0

        self._latencies.append(latency)
        if len(self._latencies) > 100:
            del self._latencies[0]

        self._last_analyzed[cam_name] = time.monotonic()
        self._total_analyzed += 1
        self.result_store.put(cam_name, result, latency)

        # Persist to SQLite (non-blocking — storage uses thread-local conn)
        if self.storage:
            try:
                self.storage.save(result, latency=latency)
            except Exception as exc:
                print(f"[Scheduler] Storage error for {cam_name}: {exc}")

        # Alert check (only for significant events)
        await self.alert_engine.process(cam_name, result, thumbnail_b64=thumb_b64)

        # Broadcast result to dashboard
        if self.broadcast_fn:
            await self.broadcast_fn({
                "type": "result",
                "cam": cam_name,
                "result": result,
                "latency": round(latency, 2),
                "thumbnail_b64": thumb_b64,
            })

    # ── Auto-tune ───────────────────────────────────────────────────

    async def _tune_loop(self) -> None:
        while self._running:
            await asyncio.sleep(self.TUNE_INTERVAL)
            if len(self._latencies) < 5:
                continue

            recent = self._latencies[-20:]
            p95 = sorted(recent)[int(len(recent) * 0.95)]
            cam_count = max(1, len(self.camera_manager.get_active_cameras()))
            target_max = min(self._max_concurrency, cam_count * 2)

            if p95 < self.LATENCY_UP_THRESH and len(self._workers) < target_max:
                # Inference is fast — add a worker
                i = len(self._workers)
                w = asyncio.create_task(self._worker(i), name=f"vlm-worker-{i}")
                self._workers.append(w)
                print(
                    f"[Scheduler] ↑ Concurrency → {len(self._workers)}  "
                    f"(p95={p95:.1f}s)"
                )

            elif p95 > self.LATENCY_DN_THRESH and len(self._workers) > self.MIN_WORKERS:
                # Inference is slow — shed a worker
                old = self._workers.pop()
                old.cancel()
                print(
                    f"[Scheduler] ↓ Concurrency → {len(self._workers)}  "
                    f"(p95={p95:.1f}s)"
                )

    # ── Metrics broadcast ───────────────────────────────────────────

    async def _metrics_loop(self) -> None:
        while self._running:
            await asyncio.sleep(5)
            metrics = self.result_store.get_metrics()
            metrics.update({
                "concurrency": len(self._workers),
                "queue_depth": self.queue_depth,
                "in_flight": self.in_flight_count,
            })
            if self.broadcast_fn:
                await self.broadcast_fn({"type": "metrics", "data": metrics})
