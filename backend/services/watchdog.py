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
import glob
import os
import shutil
import signal
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, Callable

from backend.core.config import DATABASE_PATH
from backend.core.error_tracker import error_tracker
from backend.services.container_manager import ContainerManager, container_manager
from backend.services.watchdog_emailer import WatchdogEmailer


class SystemWatchdog:
    def __init__(
        self,
        camera_manager,
        frame_store,
        vlm_pool,
        ws_manager,
        storage=None,
        watchdog_emailer: Optional[WatchdogEmailer] = None,
        container_mgr: Optional[ContainerManager] = None,
        db_path: Path = DATABASE_PATH,
        interval_sec: float = 15.0,
        broadcast_fn: Optional[Callable] = None,
    ):
        self.camera_manager = camera_manager
        self.frame_store = frame_store
        self.vlm_pool = vlm_pool
        self.ws_manager = ws_manager
        self.storage = storage
        self.emailer = watchdog_emailer
        self.container_mgr = container_mgr or container_manager
        self.db_path = db_path
        self.interval_sec = interval_sec
        self.broadcast_fn = broadcast_fn
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._start_time = time.monotonic()
        self._stale_camera_warnings: dict[str, float] = {}
        self._unhealthy_shard_warnings: dict[str, float] = {}
        self._last_process_sweep_ts: float = 0.0
        
        # State tracking for critical alert incidents & resolutions
        self._camera_blackout_active = False
        self._vllm_cluster_down_active = False
        self._disk_critical_active = False
        self._container_crashes_active: dict[str, bool] = {}
        self._container_restarts_session = 0
        self._last_digest_date: Optional[str] = None
        self._sanitized_count_session = 0

    def set_emailer(self, emailer: WatchdogEmailer) -> None:
        self.emailer = emailer

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        # Run immediate startup sanitation to reap any legacy zombies or dead launchers
        try:
            self.audit_and_sanitize_processes(startup=True)
        except Exception as exc:
            print(f"[Watchdog] ⚠️ Error during startup process sanitation: {exc}")
        self._task = asyncio.create_task(self._watchdog_loop(), name="system-watchdog")
        print("[Watchdog] 🛡️ Automated System Health & Process Sanitation Watchdog active (15s interval)")

    def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            self._task = None

    def _record_sanitation(self, evt: dict) -> None:
        """Persists process cleanup event into SQLite audit log."""
        try:
            if self.storage and hasattr(self.storage, "record_sanitation_event"):
                self.storage.record_sanitation_event(
                    pid=evt.get("pid", 0),
                    name=evt.get("name", "unknown"),
                    cmd=evt.get("cmd", ""),
                    rss_mb=evt.get("rss_mb", 0.0),
                    cpu_pct=evt.get("cpu_pct", 0.0),
                    reason=evt.get("reason", "Stale Process"),
                )
            else:
                conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS watchdog_sanitation_log (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        ts REAL NOT NULL,
                        timestamp TEXT NOT NULL,
                        pid INTEGER,
                        name TEXT,
                        cmd TEXT,
                        rss_mb REAL,
                        cpu_pct REAL,
                        reason TEXT
                    );
                    """
                )
                conn.execute(
                    """
                    INSERT INTO watchdog_sanitation_log (ts, timestamp, pid, name, cmd, rss_mb, cpu_pct, reason)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        time.time(),
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        evt.get("pid", 0),
                        evt.get("name", "unknown"),
                        evt.get("cmd", "")[:200],
                        evt.get("rss_mb", 0.0),
                        evt.get("cpu_pct", 0.0),
                        evt.get("reason", "Stale Process"),
                    ),
                )
                conn.commit()
                conn.close()
            self._sanitized_count_session += 1
        except Exception:
            pass

    def audit_and_sanitize_processes(self, startup: bool = False) -> list[dict]:
        """
        Inspects the Linux process table to detect, terminate, and reap:
          1. Zombie/defunct processes (state Z)
          2. Rogue crash reporters (apport-gtk / apport) and runaway crash dump files in /var/crash
          3. Stale/orphaned RapidAlert backend instances (uvicorn/run.sh) from prior sessions
          4. Orphaned multiprocessing spawn workers
        Persists every sanitation event into watchdog_sanitation_log for executive email reporting.
        """
        cleaned_events: list[dict] = []
        current_pid = os.getpid()
        current_ppid = os.getppid()

        # 1. Clean any runaway crash dumps in /var/crash
        try:
            for crash_file in glob.glob("/var/crash/_usr_bin_python3*") + glob.glob("/var/crash/*.crash"):
                try:
                    f_size_mb = os.path.getsize(crash_file) / (1024 * 1024)
                    os.unlink(crash_file)
                    evt = {
                        "pid": 0,
                        "name": os.path.basename(crash_file),
                        "cmd": crash_file,
                        "rss_mb": round(f_size_mb, 1),
                        "cpu_pct": 0.0,
                        "reason": f"Purged Runaway Crash Dump ({f_size_mb:.1f} MB)",
                    }
                    cleaned_events.append(evt)
                    self._record_sanitation(evt)
                    print(f"[Watchdog] 🧹 Removed crash dump file: {crash_file} ({f_size_mb:.1f} MB freed)")
                except Exception:
                    pass
        except Exception:
            pass

        # 2. Reaping immediate child zombies via waitpid
        try:
            while True:
                wpid, status = os.waitpid(-1, os.WNOHANG)
                if wpid <= 0:
                    break
                evt = {
                    "pid": wpid,
                    "name": "child_worker",
                    "cmd": "child_process",
                    "rss_mb": 0.0,
                    "cpu_pct": 0.0,
                    "reason": "Reaped Child Zombie Process",
                }
                cleaned_events.append(evt)
                self._record_sanitation(evt)
        except (ChildProcessError, OSError):
            pass

        # 3. Scan system /proc
        for p_dir in glob.glob("/proc/[0-9]*"):
            try:
                pid = int(os.path.basename(p_dir))
                if pid in (current_pid, current_ppid, 1):
                    continue

                # Read stat
                with open(f"{p_dir}/stat", "r") as f:
                    stat_content = f.read()
                l_paren = stat_content.find("(")
                r_paren = stat_content.rfind(")")
                if l_paren == -1 or r_paren == -1:
                    continue
                comm = stat_content[l_paren + 1 : r_paren]
                rest = stat_content[r_paren + 2 :].split()
                state = rest[0]
                ppid = int(rest[1])
                rss_pages = int(rest[21]) if len(rest) > 21 else 0
                rss_mb = (rss_pages * 4096) / (1024 * 1024)

                # Read cmdline
                try:
                    with open(f"{p_dir}/cmdline", "rb") as f:
                        cmdline = f.read().replace(b"\x00", b" ").decode("utf-8", errors="ignore").strip()
                except Exception:
                    cmdline = comm

                should_kill = False
                reason = ""

                # Rule A: Rogue apport crash reporter GUI
                if comm in ("apport-gtk", "apport") or "apport-gtk" in cmdline:
                    should_kill = True
                    reason = f"Rogue Apport Crash Daemon ({rss_mb:.1f} MB RAM)"

                # Rule B: Stale uvicorn backend process from a prior run
                elif ("uvicorn" in cmdline and "backend.main" in cmdline) and pid != current_pid:
                    should_kill = True
                    reason = "Stale Uvicorn Backend Instance"

                # Rule C: Stale launcher script
                elif ("bash ./run.sh" in cmdline or "bash run.sh" in cmdline) and pid not in (current_pid, current_ppid):
                    should_kill = True
                    reason = "Stale run.sh Launcher Script"

                # Rule D: Orphaned multiprocessing spawn worker
                elif "from multiprocessing.spawn import spawn_main" in cmdline and ppid in (1, 0):
                    should_kill = True
                    reason = "Orphaned Multiprocessing Spawn Worker"

                # Rule E: Zombie process whose parent is not us
                elif state == "Z":
                    if ppid != 1 and os.path.exists(f"/proc/{ppid}"):
                        try:
                            with open(f"/proc/{ppid}/cmdline", "rb") as f:
                                p_cmd = f.read().replace(b"\x00", b" ").decode("utf-8", errors="ignore")
                            if "run.sh" in p_cmd or "uvicorn" in p_cmd:
                                os.kill(ppid, signal.SIGTERM)
                                evt = {
                                    "pid": ppid,
                                    "name": "stale_parent",
                                    "cmd": p_cmd,
                                    "rss_mb": 0.0,
                                    "cpu_pct": 0.0,
                                    "reason": f"Parent of Zombie {pid} Terminated ({comm})",
                                }
                                cleaned_events.append(evt)
                                self._record_sanitation(evt)
                        except Exception:
                            pass

                if should_kill:
                    try:
                        os.kill(pid, signal.SIGTERM)
                        time.sleep(0.05)
                        if os.path.exists(f"/proc/{pid}"):
                            os.kill(pid, signal.SIGKILL)
                        evt = {
                            "pid": pid,
                            "name": comm,
                            "cmd": cmdline,
                            "rss_mb": round(rss_mb, 1),
                            "cpu_pct": 0.0,
                            "reason": reason,
                        }
                        cleaned_events.append(evt)
                        self._record_sanitation(evt)
                        print(f"[Watchdog] 🛡️ Terminated rogue/stale process: PID {pid} ({comm}) — {reason}")
                    except (ProcessLookupError, PermissionError):
                        pass

            except Exception:
                continue

        if cleaned_events:
            print(f"[Watchdog] 🧹 Process sanitation sweep complete: {len(cleaned_events)} rogue/stale process(es) cleared.")

        return cleaned_events

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

        # ── 1. Periodic Process Table Sanitation Sweep (Every 30s) ───────────
        if now - self._last_process_sweep_ts >= 30.0:
            self._last_process_sweep_ts = now
            try:
                self.audit_and_sanitize_processes(startup=False)
            except Exception as exc:
                print(f"[Watchdog] ⚠️ Error during process sanitation sweep: {exc}")

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

        # ── 5. Docker Container Crash Audit & Self-Healing Restarts ────────────
        await self._audit_and_heal_containers()

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

    async def _audit_and_heal_containers(self) -> None:
        """
        Audits all configured vLLM inference containers:
          1. Detects crashed, dead, stopped, or OOM-killed Docker containers.
          2. Detects unresponsive containers whose /health probe fails continuously.
          3. Triggers automated container restart with crash-loop backoff protection.
          4. Dispatches executive watchdog email alerts with full container logs & solving tools.
          5. Sends green resolution alerts once the container recovers and passes health checks.
        """
        now = time.monotonic()
        shards = self.vlm_pool.get_stats() if self.vlm_pool else []
        sys_cfg = self.container_mgr._get_sys_config()
        configured_endpoints = sys_cfg.get("vllm_endpoints", [])

        for idx, ep_cfg in enumerate(configured_endpoints):
            port = int(ep_cfg.get("url", f":{8000 + idx}").split(":")[-1])
            cname = ep_cfg.get("container_name") or f"rapidalert_vllm_{idx}"
            mig_uuid = ep_cfg.get("mig_uuid", "Shared")
            sm_count = ep_cfg.get("sm_count", 0)

            # Match shard stats
            matching_shard = next((s for s in shards if str(port) in str(s.get("url", ""))), None)
            is_healthy = matching_shard.get("healthy", False) if matching_shard else False

            # Inspect Docker container state
            cinfo = await self.container_mgr.inspect_container(cname)

            needs_restart = False
            crash_reason = ""

            # Check A: Missing container
            if not cinfo.get("exists", False):
                if now - self._start_time > 45.0:
                    needs_restart = True
                    crash_reason = f"Container '{cname}' is missing or deleted from Docker"

            # Check B: OOM Killed
            elif cinfo.get("oom_killed", False):
                needs_restart = True
                crash_reason = f"Linux Kernel OOM Killed (Out of Memory, Exit Code {cinfo.get('exit_code', -1)})"

            # Check C: Dead or exited
            elif not cinfo.get("running", False) or cinfo.get("status") in ("exited", "dead"):
                needs_restart = True
                crash_reason = f"Container terminated abruptly (Status: {cinfo.get('status')}, Exit Code: {cinfo.get('exit_code', -1)})"

            # Check D: Running in Docker, but unresponsive to HTTP health probes
            elif cinfo.get("running", True):
                if is_healthy:
                    self.container_mgr.clear_unresponsive_tick(cname)
                    # If this container had an active crash alert, resolve it
                    if self._container_crashes_active.get(cname, False):
                        self._container_crashes_active[cname] = False
                        if self.emailer:
                            self.emailer.send_resolution_alert(
                                event_type=f"CONTAINER_CRASH_{cname}",
                                title=f"Container {cname} Restored & Healthy on Port {port}",
                                message=(
                                    f"vLLM inference container '{cname}' on port {port} has completed model warmup "
                                    f"and is healthy. Shard load-balancer routing has been restored."
                                ),
                                details={
                                    "Container Name": cname,
                                    "Endpoint Port": port,
                                    "MIG Hardware Slice": f"{mig_uuid} ({sm_count} SMs)",
                                    "Model Loaded": ep_cfg.get("model", "unknown"),
                                    "Container Status": cinfo.get("status", "running"),
                                },
                            )
                else:
                    # Only flag unresponsive after warmup grace period
                    if now - self._start_time > self.container_mgr.warmup_grace_seconds:
                        tick = self.container_mgr.record_unresponsive_tick(cname)
                        # Require 3 consecutive unresponsive ticks (~45s) to avoid transient probe blips
                        if tick >= 3:
                            needs_restart = True
                            crash_reason = (
                                f"vLLM Engine Health Probe Timed Out / Stalled for {tick * int(self.interval_sec)}s "
                                f"(HTTP /health unresponsive despite Docker Status: {cinfo.get('status')})"
                            )

            if needs_restart:
                can_res, backoff_reason = self.container_mgr.can_restart(cname)
                if can_res:
                    # 1. Capture crash diagnostics & tail logs BEFORE restarting
                    logs = await self.container_mgr.get_container_logs(cname, lines=40)
                    hw = self.container_mgr.get_hardware_telemetry()
                    tools = self.container_mgr.get_solving_tools_guide(cname, port)

                    # 2. Execute self-healing restart
                    if cinfo.get("exists", False):
                        ok_restart, act_msg = await self.container_mgr.restart_container(cname)
                    else:
                        ok_restart, act_msg = await self.container_mgr.launch_container_from_config(idx, ep_cfg)

                    self._container_restarts_session += 1

                    # 3. Record in SQLite audit log
                    self._record_sanitation({
                        "pid": cinfo.get("pid", 0),
                        "name": cname,
                        "cmd": f"docker restart {cname}",
                        "rss_mb": 0.0,
                        "cpu_pct": 0.0,
                        "reason": f"Container Auto-Restart: {crash_reason}",
                    })

                    # 4. Capture in error tracker
                    error_tracker.capture_error(
                        message=f"Watchdog auto-restarted vLLM container {cname} ({crash_reason})",
                        component="ContainerWatchdog",
                        effect=act_msg,
                        severity="CRITICAL",
                    )

                    # 5. Dispatch Watchdog Email with diagnostics and solving tools
                    if self.emailer:
                        self._container_crashes_active[cname] = True
                        details = {
                            "MIG Hardware Slice": f"{mig_uuid} ({sm_count} SMs)",
                            "Model Configured": ep_cfg.get("model", "unknown"),
                            "Container Exit Code": cinfo.get("exit_code", -1),
                            "OOM Killed Flag": str(cinfo.get("oom_killed", False)),
                            "Host Memory": hw.get("ram_status", "unknown"),
                            "Host Swap": hw.get("swap_status", "unknown"),
                            "Host GPU Telemetry": hw.get("gpu_status", "unknown"),
                        }
                        self.emailer.send_container_crash_alert(
                            container_name=cname,
                            port=port,
                            crash_reason=crash_reason,
                            action_taken=act_msg,
                            details=details,
                            container_logs=logs,
                            solving_tools=tools,
                            cooldown_sec=300.0,
                        )
                else:
                    # In crash-loop backoff: log warning and suppress repeated restarts
                    last_warn = self._unhealthy_shard_warnings.get(f"{cname}_backoff", 0.0)
                    if now - last_warn > 180.0:
                        self._unhealthy_shard_warnings[f"{cname}_backoff"] = now
                        error_tracker.capture_error(
                            message=f"Container {cname} is in crash-loop backoff: {backoff_reason}",
                            component="ContainerWatchdog",
                            effect="Auto-restart temporarily paused to prevent tight loop",
                            severity="CRITICAL",
                        )

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
            "container_restarts_session": self._container_restarts_session,
            "container_watchdog": self.container_mgr.get_stats(),
        }
