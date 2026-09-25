"""
SystemWatchdog — Continuous background health monitor, self-healing engine, and automated watchdog alerting.
Periodically audits:
  1. Camera stream staleness (auto-detects frozen RTSP connections and blackouts)
  2. Subprocess zombie reaping (cleans defunct processes automatically)
  3. Memory pressure & garbage collection
  4. vLLM shard connectivity, MIG health, and cluster outages
  5. Disk space availability
  6. Automated daily 24-hour health digest dispatch
"""
from __future__ import annotations

import asyncio
import gc
import os
import shutil
import time
from datetime import datetime
from typing import Optional, Callable

from backend.core.error_tracker import error_tracker
from backend.services.watchdog_emailer import WatchdogEmailer


class SystemWatchdog:
    def __init__(
        self,
        camera_manager,
        frame_store,
        vlm_pool,
        ws_manager,
        watchdog_emailer: Optional[WatchdogEmailer] = None,
        interval_sec: float = 15.0,
        broadcast_fn: Optional[Callable] = None,
    ):
        self.camera_manager = camera_manager
        self.frame_store = frame_store
        self.vlm_pool = vlm_pool
        self.ws_manager = ws_manager
        self.emailer = watchdog_emailer
        self.interval_sec = interval_sec
        self.broadcast_fn = broadcast_fn
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._start_time = time.monotonic()
        self._stale_camera_warnings: dict[str, float] = {}
        self._unhealthy_shard_warnings: dict[str, float] = {}
        
        # State tracking for critical alert incidents & resolutions
        self._camera_blackout_active = False
        self._vllm_cluster_down_active = False
        self._disk_critical_active = False
        self._last_digest_date: Optional[str] = None

    def set_emailer(self, emailer: WatchdogEmailer) -> None:
        self.emailer = emailer

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
                self._check_daily_digest_schedule()
                if self.emailer:
                    self.emailer.heartbeat_tick()
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
                pid, status = os.waitpid(-1, os.WNOHANG)
                if pid <= 0:
                    break
                reaped += 1
            if reaped > 0:
                print(f"[Watchdog] 🧹 Reaped {reaped} defunct zombie process(es)")
        except (ChildProcessError, OSError):
            pass

        # ── 2. Camera Stream Staleness & Blackout Checker ─────────────────────
        active_cams = self.camera_manager.get_active_cameras()
        total_cams = len(active_cams)
        stale_count = 0
        stale_cam_names = []

        for cam_name in active_cams:
            age = self.frame_store.get_frame_age(cam_name)
            if age is not None and age > 15.0:
                stale_count += 1
                stale_cam_names.append(cam_name)
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

        # Critical Camera Blackout Detection (>50% of feeds dead or frozen > 30s)
        if total_cams > 0 and stale_count >= max(2, total_cams // 2) and (now - self._start_time > 45.0):
            if not self._camera_blackout_active and self.emailer:
                self._camera_blackout_active = True
                self.emailer.send_critical_alert(
                    event_type="CAMERA_BLACKOUT",
                    title=f"Critical Camera Blackout ({stale_count}/{total_cams} Streams Frozen)",
                    message=f"{stale_count} of {total_cams} surveillance feeds are frozen or failing to ingest frames.",
                    details={
                        "Affected Cameras": ", ".join(stale_cam_names),
                        "Total Cameras Active": f"{total_cams - stale_count}/{total_cams}",
                        "Threshold": "Staleness > 15 seconds",
                    },
                    severity="CRITICAL",
                    remediation="Check network switch connectivity and RTSP camera stream URLs.",
                )
        elif self._camera_blackout_active and stale_count == 0:
            self._camera_blackout_active = False
            if self.emailer:
                self.emailer.send_resolution_alert(
                    event_type="CAMERA_BLACKOUT",
                    title=f"All {total_cams} Camera Feeds Restored & Streaming",
                    message="All CCTV camera feeds have recovered normal frame rates and NVDEC/OpenCV ingestion.",
                    details={"Active Cameras": total_cams},
                )

        # ── 3. Memory Pressure & Periodic GC Sweep ─────────────────────────────
        gc.collect()

        # ── 4. vLLM Shard & Cluster Outage Health Check ────────────────────────
        shards = self.vlm_pool.get_stats()
        unhealthy_shards = [s for s in shards if not s.get("healthy", False)]
        
        if unhealthy_shards:
            if now - self._start_time > 90.0:
                for s in unhealthy_shards:
                    url = s.get("url", "unknown")
                    last_warn = self._unhealthy_shard_warnings.get(url, 0.0)
                    if now - last_warn > 120.0:
                        self._unhealthy_shard_warnings[url] = now
                        error_tracker.capture_error(
                            message=f"vLLM Shard {url} is reporting unhealthy",
                            component="SystemWatchdog",
                            effect=f"Workload automatically re-routed away from {url}",
                            severity="WARNING",
                        )
                
                # Check for Complete Cluster Outage (0 healthy shards)
                if len(shards) > 0 and len(unhealthy_shards) == len(shards):
                    if not self._vllm_cluster_down_active and self.emailer:
                        self._vllm_cluster_down_active = True
                        self.emailer.send_critical_alert(
                            event_type="VLLM_CLUSTER_DOWN",
                            title="vLLM Inference Cluster Completely Offline",
                            message="All vLLM / MIG GPU inference instances are failing health checks. Real-time vision-language analysis is stalled.",
                            details={
                                "Total Shards": len(shards),
                                "Healthy Shards": "0",
                                "Shards Affected": ", ".join(s.get("url", "unknown") for s in shards),
                            },
                            severity="CRITICAL",
                            remediation="Inspect vLLM container / subprocess logs or run `./run.sh` restart.",
                        )
        else:
            if self._vllm_cluster_down_active:
                self._vllm_cluster_down_active = False
                if self.emailer:
                    self.emailer.send_resolution_alert(
                        event_type="VLLM_CLUSTER_DOWN",
                        title="vLLM Inference Shards Restored & Healthy",
                        message=f"vLLM inference cluster is back online. All {len(shards)} shards responding to health probes.",
                    )
            self._unhealthy_shard_warnings.clear()

        # ── 5. Disk Storage Check ──────────────────────────────────────────────
        try:
            total, used, free = shutil.disk_usage("/")
            free_pct = (free / total) * 100.0
            if free_pct < 5.0:
                if not self._disk_critical_active and self.emailer:
                    self._disk_critical_active = True
                    self.emailer.send_critical_alert(
                        event_type="STORAGE_CRITICAL",
                        title=f"Host Disk Space Critically Low ({free_pct:.1f}% Free)",
                        message=f"Available disk space is critically low ({free / (1024**3):.1f} GB remaining). Storage exhaustion risks crash.",
                        details={
                            "Free Space": f"{free / (1024**3):.2f} GB ({free_pct:.1f}%)",
                            "Total Space": f"{total / (1024**3):.2f} GB",
                        },
                        severity="CRITICAL",
                        remediation="Clear old recording clips in data/ or purge historical incident archives.",
                    )
            elif self._disk_critical_active and free_pct >= 10.0:
                self._disk_critical_active = False
                if self.emailer:
                    self.emailer.send_resolution_alert(
                        event_type="STORAGE_CRITICAL",
                        title="Disk Space Normalized",
                        message=f"Disk space has cleared. Free space is now at {free_pct:.1f}%.",
                    )
        except Exception:
            pass

    def _check_daily_digest_schedule(self) -> None:
        """Checks if the daily 24-hour health digest should be dispatched."""
        if not self.emailer:
            return

        now = datetime.now()
        today_str = now.strftime("%Y-%m-%d")

        # Check system config for daily digest trigger hour
        cfg = self.emailer._get_sys_config()
        if not cfg.get("daily_digest_enabled", True):
            return

        target_hour = cfg.get("daily_digest_hour", 8)  # Default: 08:00 AM IST

        if now.hour == target_hour and self._last_digest_date != today_str:
            self._last_digest_date = today_str
            print(f"[Watchdog] ⏰ Triggering scheduled 24-Hour System Health & Diagnostics Digest (Target: {target_hour:02d}:00 IST)...")
            self.emailer.send_daily_health_digest()

    def get_summary(self) -> dict:
        uptime_sec = round(time.monotonic() - self._start_time, 1)
        active_cams = len(self.camera_manager.get_active_cameras())
        total_cams = len(self.camera_manager.get_config())
        shards = self.vlm_pool.get_stats()
        healthy_shards = sum(1 for s in shards if s.get("healthy", False))

        return {
            "uptime_sec": uptime_sec,
            "cameras_active": f"{active_cams}/{total_cams}",
            "vlm_shards_healthy": f"{healthy_shards}/{len(shards)}",
            "camera_blackout": self._camera_blackout_active,
            "vllm_cluster_down": self._vllm_cluster_down_active,
            "disk_critical": self._disk_critical_active,
        }
