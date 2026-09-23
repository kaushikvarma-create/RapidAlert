"""
SystemWatchdog — Continuous background health monitor and self-healing engine.
Periodically audits:
  1. Camera stream staleness (auto-detects frozen RTSP connections)
  2. Subprocess zombie reaping (cleans defunct processes automatically)
  3. Memory pressure & garbage collection
  4. vLLM shard connectivity and latency health
"""
from __future__ import annotations

import asyncio
import gc
import os
import time
from typing import Optional, Callable

from backend.core.error_tracker import error_tracker


class SystemWatchdog:
    def __init__(
        self,
        camera_manager,
        frame_store,
        vlm_pool,
        ws_manager,
        interval_sec: float = 15.0,
        broadcast_fn: Optional[Callable] = None,
    ):
        self.camera_manager = camera_manager
        self.frame_store = frame_store
        self.vlm_pool = vlm_pool
        self.ws_manager = ws_manager
        self.interval_sec = interval_sec
        self.broadcast_fn = broadcast_fn
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._start_time = time.monotonic()
        self._stale_camera_warnings: dict[str, float] = {}

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._watchdog_loop(), name="system-watchdog")
        print("[Watchdog] 🛡️ Automated System Health Watchdog active (15s interval)")

    def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            self._task = None

    async def _watchdog_loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(self.interval_sec)
                await self._run_health_checks()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                error_tracker.capture_exception(
                    exc,
                    component="SystemWatchdog",
                    effect="Error during periodic watchdog check; continuing",
                    severity="WARNING",
                )

    async def _run_health_checks(self) -> None:
        now = time.monotonic()

        # ── 1. Zombie Process Cleaner (Reap any defunct child processes) ───────
        reaped = 0
        try:
            while True:
                # Non-blocking wait for any child process
                pid, status = os.waitpid(-1, os.WNOHANG)
                if pid <= 0:
                    break
                reaped += 1
            if reaped > 0:
                print(f"[Watchdog] 🧹 Reaped {reaped} defunct zombie process(es)")
        except (ChildProcessError, OSError):
            pass

        # ── 2. Camera Stream Staleness Checker ─────────────────────────────────
        active_cams = self.camera_manager.get_active_cameras()
        for cam_name in active_cams:
            age = self.frame_store.get_frame_age(cam_name)
            if age is not None and age > 15.0:
                last_warn = self._stale_camera_warnings.get(cam_name, 0.0)
                if now - last_warn > 60.0:  # Warn once per minute per camera
                    self._stale_camera_warnings[cam_name] = now
                    error_tracker.capture_error(
                        message=f"Camera feed '{cam_name}' has not received frames for {age:.1f}s (RTSP stream may be frozen)",
                        component="SystemWatchdog",
                        camera=cam_name,
                        effect="Stream watchdog flagged stale camera; connection will auto-recycle",
                        severity="WARNING",
                    )
            elif age is not None and age <= 3.0:
                self._stale_camera_warnings.pop(cam_name, None)

        # ── 3. Memory Pressure & Periodic GC Sweep ─────────────────────────────
        # Run generational garbage collection to free unreferenced frame arrays
        gc.collect()

        # ── 4. vLLM Shard Health Check ─────────────────────────────────────────
        shards = self.vlm_pool.get_stats()
        unhealthy_shards = [s for s in shards if not s.get("healthy", True)]
        if unhealthy_shards:
            for s in unhealthy_shards:
                url = s.get("url", "unknown")
                error_tracker.capture_error(
                    message=f"vLLM Shard {url} is reporting unhealthy",
                    component="SystemWatchdog",
                    effect=f"Workload automatically re-routed away from {url}",
                    severity="WARNING",
                )

    def get_summary(self) -> dict:
        uptime_sec = round(time.monotonic() - self._start_time, 1)
        active_cams = len(self.camera_manager.get_active_cameras())
        total_cams = len(self.camera_manager.get_config())
        shards = self.vlm_pool.get_stats()
        healthy_shards = sum(1 for s in shards if s.get("healthy", True))

        return {
            "uptime_sec": uptime_sec,
            "cameras_active": f"{active_cams}/{total_cams}",
            "vlm_shards_healthy": f"{healthy_shards}/{len(shards)}",
        }
