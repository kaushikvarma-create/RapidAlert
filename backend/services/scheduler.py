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
from __future__ import annotations

import asyncio
import time
from typing import Callable, Optional, TYPE_CHECKING

from backend.services.camera_manager import CameraManager
from backend.services.frame_store import FrameStore
from backend.services.vlm_client import VLMPool
from backend.services.prompt_manager import PromptManager
from backend.services.result_store import ResultStore
from backend.services.alert_engine import AlertEngine
from backend.services.priority_queue import PriorityQueueAdapter
from backend.core.config import (
    DEFAULT_VLM_MODEL,
    DEFAULT_HEARTBEAT_SEC,
    DEFAULT_FOLLOWUP_INTERVAL,
    DEFAULT_FOLLOWUP_MAX_CYCLES,
    DEFAULT_PERSISTENT_FOLLOWUP,
    TUNE_INTERVAL_SEC,
    LATENCY_UP_THRESH_SEC,
    LATENCY_DN_THRESH_SEC,
    FEED_TICK_SEC,
    IDLE_TICK_SEC,
    QUEUE_CAP_PER_WORKER,
    TEMPORAL_THUMB_WIDTH,
    TEMPORAL_THUMB_QUALITY,
    HIGH_RES_FRAME_WIDTH,
    HIGH_RES_JPEG_QUALITY,
)
from backend.core.error_tracker import error_tracker

if TYPE_CHECKING:
    from backend.services.storage import StorageManager


class DeadlineScheduler:
    TUNE_INTERVAL = TUNE_INTERVAL_SEC
    LATENCY_UP_THRESH = LATENCY_UP_THRESH_SEC
    LATENCY_DN_THRESH = LATENCY_DN_THRESH_SEC
    FEED_TICK = FEED_TICK_SEC
    IDLE_TICK = IDLE_TICK_SEC
    QUEUE_CAP_PER_WORKER = QUEUE_CAP_PER_WORKER

    def __init__(
        self,
        camera_manager: CameraManager,
        frame_store: FrameStore,
        vlm_pool: VLMPool,
        prompt_manager: PromptManager,
        result_store: ResultStore,
        alert_engine: AlertEngine,
        initial_concurrency: int = 4,
        max_concurrency: int = 8,
        broadcast_fn: Optional[Callable] = None,
        storage=None,   # StorageManager | None
        default_heartbeat_sec: float = DEFAULT_HEARTBEAT_SEC,
    ):
        self.camera_manager = camera_manager
        self.frame_store = frame_store
        self.vlm_pool = vlm_pool
        self.prompt_manager = prompt_manager
        self.result_store = result_store
        self.alert_engine = alert_engine
        self.broadcast_fn = broadcast_fn
        self.storage = storage
        self.default_heartbeat_sec = float(default_heartbeat_sec)

        # Pin workers to max_concurrency
        self.MIN_WORKERS = max_concurrency
        self._concurrency = max_concurrency
        self._max_concurrency = max_concurrency
        self._running = False
        self._last_analyzed: dict[str, float] = {}
        self._in_flight: set[str] = set()
        self._workers: list[asyncio.Task] = []
        self._queue: Optional[asyncio.PriorityQueue] = None
        self._seq: int = 0
        self._total_analyzed: int = 0

        self._latencies: list[float] = []
        self.followup_interval_sec: float = DEFAULT_FOLLOWUP_INTERVAL
        self.persistent_followup: bool = DEFAULT_PERSISTENT_FOLLOWUP
        self.followup_max_cycles: int = DEFAULT_FOLLOWUP_MAX_CYCLES
        self.clip_retention_hours: float = 24.0
        self.clip_rolling_buffer_enabled: bool = True
        self._followup_tasks: set[asyncio.Task] = set()

    def get_cam_heartbeat_interval(self, cam_name: str) -> float:
        """Returns the mandatory analysis interval in seconds for the given camera."""
        cams = self.camera_manager.get_config()
        for c in cams:
            if c.get("name") == cam_name and c.get("heartbeat_sec") is not None:
                try:
                    return float(c["heartbeat_sec"])
                except (ValueError, TypeError) as exc:
                    error_tracker.capture_exception(
                        exc,
                        component="Scheduler",
                        camera=cam_name,
                        effect=f"Invalid heartbeat_sec in camera config for {cam_name}; using default {self.default_heartbeat_sec}s",
                        severity="WARNING",
                    )
        return float(self.default_heartbeat_sec)

    def _next_seq(self) -> int:
        self._seq += 1
        return self._seq

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
        self._queue = PriorityQueueAdapter()
        await self._queue.start()
        self._running = True
        self._workers = [
            asyncio.create_task(self._worker(i), name=f"vlm-worker-{i}")
            for i in range(self._concurrency)
        ]
        self._bg_tasks = [
            asyncio.create_task(self._feed_loop(), name="scheduler-feed"),
            asyncio.create_task(self._tune_loop(), name="scheduler-tune"),
            asyncio.create_task(self._metrics_loop(), name="scheduler-metrics"),
            asyncio.create_task(self._clip_pruner_loop(), name="scheduler-clip-pruner"),
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
        for ft in list(self._followup_tasks):
            ft.cancel()
        self._followup_tasks.clear()
        self._workers.clear()
        self._bg_tasks.clear()
        if self._queue:
            await self._queue.stop()

    def set_concurrency_limit(self, n: int) -> None:
        """Manually set target concurrency (dashboard override)."""
        self._max_concurrency = max(self.MIN_WORKERS, n)

    # ── Priority Tier Enqueueing ────────────────────────────────────

    async def queue_incident(self, incident: dict) -> None:
        """Enqueue a prioritized incident trigger (Tier-1 Follow-Up, Tier-2 Major, Tier-3 Minor)."""
        if not self._running or self._queue is None:
            return
        cam_name = incident.get("cam", "")
        # Prevent piling up identical camera jobs in queue
        if cam_name in self._in_flight:
            return
        self._in_flight.add(cam_name)
        tier = incident.get("tier", 2)
        # Tier-1 (Followup) and Tier-2 (Major Shift): Priority 0
        # Tier-3 (Minor Shift / Monitoring): Priority 1
        priority = 0 if tier <= 2 else 1
        await self._queue.put((priority, self._next_seq(), incident))
        tag = "Major" if tier == 2 else ("Minor" if tier == 3 else "Follow-Up")
        print(f"[Scheduler] 📥 Queued {tag} incident for {cam_name} (Tier-{tier}, Priority {priority})")

    # ── Feed loop (Heartbeat check for quiet cameras) ───────────────

    async def _feed_loop(self) -> None:
        """
        Background heartbeat loop: checks for cameras that have been quiet
        without an incident and queues a Tier-4 low-priority refresh.
        """
        while self._running:
            try:
                cams = self.camera_manager.get_active_cameras()
                now = time.monotonic()
                eligible = [
                    c for c in cams
                    if c not in self._in_flight
                    and self.frame_store.get_latest(c) is not None
                    and now - self._last_analyzed.get(c, 0) >= self.get_cam_heartbeat_interval(c)
                ]

                queue_cap = max(1, len(self._workers)) * self.QUEUE_CAP_PER_WORKER
                if eligible and self._queue.qsize() < queue_cap:
                    cam = max(
                        eligible,
                        key=lambda c: now - self._last_analyzed.get(c, 0),
                    )
                    self._in_flight.add(cam)
                    job = {"cam": cam, "is_heartbeat": True, "tier": 4}
                    # Tier-4 Routine Heartbeat: Priority 2 (yields to any T1/T2/T3)
                    await self._queue.put((2, self._next_seq(), job))
                    await asyncio.sleep(self.FEED_TICK)
                else:
                    await asyncio.sleep(self.IDLE_TICK)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                error_tracker.capture_exception(
                    exc,
                    component="Scheduler",
                    effect="Scheduler heartbeat feed loop encountered an error; pausing for 1s",
                    severity="WARNING",
                )
                await asyncio.sleep(1.0)

    # ── Worker ──────────────────────────────────────────────────────

    async def _worker(self, worker_id: int) -> None:
        while self._running:
            try:
                item = await asyncio.wait_for(self._queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break

            priority, seq, job = item
            cam = job.get("cam", "")
            try:
                await self._analyze_job(job)
            except Exception as exc:
                error_tracker.capture_exception(
                    exc,
                    component="Scheduler",
                    camera=cam,
                    effect=f"VLM analysis job failed for camera {cam}; item discarded from queue",
                    severity="ERROR",
                )
            finally:
                self._in_flight.discard(cam)
                self._queue.task_done()

    async def _analyze_job(self, job: dict) -> None:
        cam_name = job["cam"]
        is_incident = not job.get("is_heartbeat", False)
        is_followup = job.get("is_followup", False)
        drift_score = job.get("drift", 0.0)
        job_labels = job.get("labels")
        incident_id = job.get("incident_id")
        parent_id = job.get("parent_id")
        cycle = job.get("cycle", 1)
        prev_severity = job.get("prev_severity")
        prev_observation = job.get("prev_observation")
        followup_delay = float(job.get("followup_delay", self.followup_interval_sec))

        if is_incident and job.get("frames_b64"):
            frames_b64 = job["frames_b64"]
            thumbs_b64 = job.get("thumbs_b64", [])
            high_res_snap = (
                job.get("thumbnail_b64")
                or self.frame_store.get_snapshot_b64(cam_name, max_w=HIGH_RES_FRAME_WIDTH, quality=HIGH_RES_JPEG_QUALITY)
                or (thumbs_b64[-1] if thumbs_b64 else None)
            )
        else:
            # Heartbeat fallback: extract 4 temporal frames across 10s
            frames_b64 = self.frame_store.get_temporal_snapshots_b64(cam_name, count=4, span_sec=10.0, max_w=512)
            thumbs_b64 = self.frame_store.get_temporal_snapshots_b64(
                cam_name, count=4, span_sec=10.0, max_w=TEMPORAL_THUMB_WIDTH, quality=TEMPORAL_THUMB_QUALITY
            )
            high_res_snap = (
                self.frame_store.get_snapshot_b64(cam_name, max_w=HIGH_RES_FRAME_WIDTH, quality=HIGH_RES_JPEG_QUALITY)
                or (thumbs_b64[-1] if thumbs_b64 else None)
            )
            job_labels = ["t -10.0s", "t -6.5s", "t -3.0s", "t 0.0s (Current)"]

        if not frames_b64:
            return

        prompt = self.prompt_manager.get_prompt(
            cam_name=cam_name,
            is_followup=is_followup,
            prev_severity=prev_severity,
            prev_observation=prev_observation,
            cycle=cycle,
            interval_sec=followup_delay,
        )

        t0 = time.monotonic()
        res = await self.vlm_pool.analyze(cam_name, frames_b64, prompt, labels=job_labels)
        res["model"] = DEFAULT_VLM_MODEL
        latency = res.get("latency", time.monotonic() - t0)

        if is_incident:
            trigger_time = job.get("trigger_time", t0)
            e2e_latency = round(time.monotonic() - trigger_time, 2)
            res["e2e_latency"] = e2e_latency
            res["trigger_to_post"] = e2e_latency
            res["is_incident"] = True
            res["is_followup"] = is_followup
        else:
            e2e_latency = None
            res["e2e_latency"] = None
            res["trigger_to_post"] = None
            res["is_incident"] = False
            res["is_followup"] = False

        self._latencies.append(latency)
        if len(self._latencies) > 100:
            del self._latencies[0]

        self._last_analyzed[cam_name] = time.monotonic()
        self._total_analyzed += 1

        results = [res]

        # Store result in memory
        self.result_store.put(cam_name, results, latency, thumbnails_b64=thumbs_b64, e2e_latency=e2e_latency)

        # Alert check based on Cosmos 8B result with high-resolution frame
        latest_thumb = high_res_snap or (thumbs_b64[-1] if thumbs_b64 else None)
        alert = await self.alert_engine.process(
            cam_name=cam_name,
            result=res,
            thumbnail_b64=latest_thumb,
            thumbnails_b64=thumbs_b64,
            is_incident=is_incident,
            is_followup=is_followup,
            incident_id=incident_id,
            parent_id=parent_id,
            labels=job_labels,
            drift=drift_score,
            e2e_latency=e2e_latency,
            latency=latency,
            cycle=cycle,
            delay_sec=followup_delay,
        )

        # Persist to SQLite
        if self.storage:
            try:
                self.storage.save(
                    dict(res, cam=cam_name),
                    latency=latency,
                    e2e_latency=e2e_latency,
                    incident_id=alert.get("incident_id") if alert else incident_id,
                    parent_id=alert.get("parent_id") if alert else parent_id,
                    trigger_mode=alert.get("trigger_mode") if alert else ("TRIGGER" if is_incident else ("FOLLOWUP" if is_followup else "PERIODIC")),
                    clip_path=alert.get("clip_path") if alert else None,
                    labels=job_labels,
                )
            except Exception as exc:
                error_tracker.capture_exception(
                    exc,
                    component="Scheduler",
                    camera=cam_name,
                    effect=f"Failed to persist analysis result for {cam_name} to database",
                    severity="ERROR",
                )

        # 1. If this was an initial trigger event (not a follow-up), schedule follow-up cycle 1!
        if is_incident and not is_followup and alert:
            evt_id = alert["id"]
            inc_id = alert.get("incident_id")
            fu_task = asyncio.create_task(
                self._schedule_followup(
                    cam_name=cam_name,
                    incident_id=inc_id,
                    parent_id=evt_id,
                    drift_score=drift_score,
                    cycle=1,
                    prev_severity=res.get("severity", "MEDIUM"),
                    prev_observation=res.get("observation", ""),
                    delay_sec=self.followup_interval_sec,
                )
            )
            self._followup_tasks.add(fu_task)
            fu_task.add_done_callback(self._followup_tasks.discard)

        # 2. If this was a follow-up and persistent follow-up is enabled:
        elif is_followup and self.persistent_followup:
            current_sev = (res.get("severity") or "LOW").upper()
            current_safety = (res.get("safety") or "UNKNOWN").upper()
            is_elevated = (
                current_sev in ("MEDIUM", "HIGH", "EXTREME")
                or current_safety in ("WARNING", "DANGER")
            )
            if is_elevated and cycle < self.followup_max_cycles:
                next_cycle = cycle + 1
                fu_task = asyncio.create_task(
                    self._schedule_followup(
                        cam_name=cam_name,
                        incident_id=incident_id,
                        parent_id=parent_id,
                        drift_score=drift_score,
                        cycle=next_cycle,
                        prev_severity=current_sev,
                        prev_observation=res.get("observation", ""),
                        delay_sec=self.followup_interval_sec,
                    )
                )
                self._followup_tasks.add(fu_task)
                fu_task.add_done_callback(self._followup_tasks.discard)
                print(
                    f"[Scheduler] 🔄 Persistent follow-up: {cam_name} severity remains {current_sev} "
                    f"({current_safety}). Next follow-up #{next_cycle} scheduled in {self.followup_interval_sec:.1f}s."
                )
            elif not is_elevated:
                print(
                    f"[Scheduler] ✅ Scene resolved on {cam_name}: severity dropped to {current_sev} "
                    f"({current_safety}). Persistent follow-up concluded."
                )
                if self.broadcast_fn:
                    await self.broadcast_fn({
                        "type": "followup_status",
                        "cam": cam_name,
                        "active": False,
                        "cycle": cycle,
                        "resolved": True,
                        "severity": current_sev,
                    })
            elif cycle >= self.followup_max_cycles:
                print(
                    f"[Scheduler] 🛑 Persistent follow-up reached max cycles ({self.followup_max_cycles}) "
                    f"for {cam_name}."
                )
                if self.broadcast_fn:
                    await self.broadcast_fn({
                        "type": "followup_status",
                        "cam": cam_name,
                        "active": False,
                        "cycle": cycle,
                        "resolved": False,
                        "severity": current_sev,
                    })

        # Broadcast single Cosmos 8B result, thumbnails, drift score and incident flag to dashboard
        if self.broadcast_fn:
            await self.broadcast_fn({
                "type": "result_concurrent",
                "cam": cam_name,
                "results": results,
                "thumbnails_b64": thumbs_b64,
                "drift": drift_score,
                "is_incident": is_incident,
                "is_followup": is_followup,
                "cycle": cycle,
                "labels": job_labels,
                "latency": latency,
                "e2e_latency": e2e_latency,
                "trigger_to_post": e2e_latency,
            })

    async def _schedule_followup(
        self,
        cam_name: str,
        incident_id: str,
        parent_id: str,
        drift_score: float,
        cycle: int = 1,
        prev_severity: Optional[str] = None,
        prev_observation: Optional[str] = None,
        delay_sec: Optional[float] = None,
    ) -> None:
        """Schedules a high-priority follow-up temporal evaluation."""
        if delay_sec is None:
            delay_sec = self.followup_interval_sec
        try:
            print(f"[Scheduler] ⏳ Scheduled follow-up #{cycle} for {cam_name} (Parent: {parent_id}) in {delay_sec:.1f}s...")
            if self.broadcast_fn:
                await self.broadcast_fn({
                    "type": "followup_status",
                    "cam": cam_name,
                    "active": True,
                    "cycle": cycle,
                    "incident_id": incident_id,
                    "parent_id": parent_id,
                    "severity": prev_severity or "HIGH",
                    "delay_sec": delay_sec,
                })
            await asyncio.sleep(delay_sec)
            if not self._running or self._queue is None:
                return

            frames_b64 = self.frame_store.get_temporal_snapshots_b64(cam_name, count=4, span_sec=delay_sec, max_w=512)
            thumbs_b64 = self.frame_store.get_temporal_snapshots_b64(
                cam_name, count=4, span_sec=delay_sec, max_w=TEMPORAL_THUMB_WIDTH, quality=TEMPORAL_THUMB_QUALITY
            )
            high_res_snap = (
                self.frame_store.get_snapshot_b64(cam_name, max_w=HIGH_RES_FRAME_WIDTH, quality=HIGH_RES_JPEG_QUALITY)
                or (thumbs_b64[-1] if thumbs_b64 else None)
            )

            if not frames_b64:
                error_tracker.capture_error(
                    message=f"No temporal frames found for {cam_name} during follow-up #{cycle}",
                    component="Scheduler",
                    camera=cam_name,
                    effect=f"Follow-up #{cycle} evaluation skipped due to missing frames",
                    severity="WARNING",
                )
                return

            step = max(1.0, delay_sec / 4.0)
            followup_labels = [
                f"t +{step:.1f}s",
                f"t +{step * 2:.1f}s",
                f"t +{step * 3:.1f}s",
                f"t +{delay_sec:.1f}s (Outcome)",
            ]
            followup_job = {
                "cam": cam_name,
                "is_incident": True,
                "is_followup": True,
                "incident_id": incident_id,
                "parent_id": parent_id,
                "drift": drift_score,
                "cycle": cycle,
                "prev_severity": prev_severity,
                "prev_observation": prev_observation,
                "followup_delay": delay_sec,
                "frames_b64": frames_b64,
                "thumbs_b64": thumbs_b64,
                "thumbnail_b64": high_res_snap,
                "labels": followup_labels,
                "trigger_time": time.monotonic(),
            }
            await self._queue.put((0, self._next_seq(), followup_job))
            print(f"[Scheduler] 🔄 Queued follow-up #{cycle} for {cam_name} (Parent: {parent_id})")
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            error_tracker.capture_exception(
                exc,
                component="Scheduler",
                camera=cam_name,
                effect=f"Error running follow-up cycle #{cycle} for {cam_name}; follow-up cancelled",
                severity="ERROR",
            )

    # ── Auto-tune ───────────────────────────────────────────────────

    async def _tune_loop(self) -> None:
        while self._running:
            try:
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
            except asyncio.CancelledError:
                break
            except Exception as exc:
                error_tracker.capture_exception(
                    exc,
                    component="Scheduler",
                    effect="Auto-tuning loop encountered an error; continuing with current worker count",
                    severity="WARNING",
                )

    # ── Metrics broadcast ───────────────────────────────────────────

    async def _metrics_loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(5)
                metrics = self.result_store.get_metrics()
                shards_stats = self.vlm_pool.get_stats()
                is_mig = any(s.get("is_mig") for s in shards_stats)
                metrics.update({
                    "concurrency": len(self._workers),
                    "queue_depth": self.queue_depth,
                    "in_flight":   self.in_flight_count,
                    "vlm_shards":  shards_stats,
                    "vlm_mode":    "mig" if is_mig else "shared",
                    "is_mig":      is_mig,
                })
                if self.broadcast_fn:
                    await self.broadcast_fn({"type": "metrics", "data": metrics})
            except asyncio.CancelledError:
                break
            except Exception as exc:
                error_tracker.capture_exception(
                    exc,
                    component="Scheduler",
                    effect="Metrics broadcast loop failed to dispatch telemetry",
                    severity="WARNING",
                )

    async def _clip_pruner_loop(self) -> None:
        """Periodic background pruner for expired video clips (runs every 300s)."""
        while self._running:
            try:
                await asyncio.sleep(300)
                if self.storage and self.clip_rolling_buffer_enabled:
                    await asyncio.to_thread(self.storage.prune_expired_clips, self.clip_retention_hours)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                error_tracker.capture_exception(
                    exc,
                    component="Scheduler",
                    effect="Background clip pruner loop encountered an unexpected error",
                    severity="WARNING",
                )

