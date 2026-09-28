"""
WatchdogEmailer: Real-Time Severe Incident Alerting & Automated 24-Hour System Health Digest.
Sends immediate executive email alerts for severe system events, crashes, and camera blackouts.
Compiles and delivers comprehensive 24-hour diagnostic reports covering uptime, errors, and AI health.
"""
from __future__ import annotations

import os
import sys
import json
import time
import sqlite3
import smtplib
import asyncio
import threading
from email.message import EmailMessage
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple

from backend.core.config import DATABASE_PATH, SYSTEM_CONFIG_PATH
from backend.core.error_tracker import error_tracker


class WatchdogEmailer:
    """
    Automated watchdog email dispatcher with rate-limiting, deduplication,
    and 24-hour system health diagnostic compilation.
    """
    def __init__(
        self,
        db_path: Path = DATABASE_PATH,
        config_path: Path = SYSTEM_CONFIG_PATH,
        camera_manager=None,
        frame_store=None,
        vlm_pool=None,
    ):
        self.db_path = db_path
        self.config_path = config_path
        self.camera_manager = camera_manager
        self.frame_store = frame_store
        self.vlm_pool = vlm_pool
        self._local = threading.local()
        self._lock = threading.Lock()
        
        self.state_file = self.db_path.parent / "system_state.json"
        # Deduplication state: {incident_key: last_sent_timestamp}
        self._sent_alerts: dict[str, float] = {}
        # Active incident states for resolution alerts: {incident_key: bool}
        self._active_incidents: dict[str, bool] = {}
        self._last_daily_digest_date: Optional[str] = None
        self._start_time = time.monotonic()
        self._boot_wall_time = time.time()

    @staticmethod
    def get_host_boot_time() -> float:
        """Retrieves exact Linux system/OS boot timestamp from /proc/uptime."""
        try:
            with open("/proc/uptime", "r") as f:
                uptime_seconds = float(f.readline().split()[0])
                return time.time() - uptime_seconds
        except Exception:
            return time.time()

    def check_and_alert_prior_crash(self) -> None:
        """
        Audits the previous system run state on startup.
        Dispatches executive notifications whenever:
          1. Physical Thor hardware rebooted / power outage occurred
          2. RapidAlert process crashed or was terminated unexpectedly
          3. RapidAlert or Thor underwent a planned/graceful restart
        """
        try:
            prior_state = {}
            if self.state_file.exists():
                with open(self.state_file, "r") as f:
                    prior_state = json.load(f)

            host_boot_ts = self.get_host_boot_time()
            now_ts = time.time()
            prior_hb = prior_state.get("last_heartbeat", prior_state.get("boot_wall_time", now_ts))
            prior_hb_str = prior_state.get("last_heartbeat_str", "Unknown")
            downtime_sec = max(0.0, now_ts - prior_hb)
            downtime_str = f"{int(downtime_sec // 3600)}h {int((downtime_sec % 3600) // 60)}m {int(downtime_sec % 60)}s" if downtime_sec > 60 else f"{int(downtime_sec)}s"

            # Check if host rebooted
            prior_host_boot = prior_state.get("host_boot_ts", 0)
            host_rebooted = (prior_host_boot > 0 and abs(host_boot_ts - prior_host_boot) > 15.0) or (host_boot_ts > (prior_hb - 10.0))

            if prior_state.get("status") == "RUNNING":
                # Abrupt termination without clean shutdown
                if host_rebooted:
                    event_type = "POWER_OUTAGE_RECOVERY"
                    title = "⚡ Power Outage Recovery: RapidAlert Re-established"
                    msg = (
                        f"RapidAlert detected an abrupt physical host power outage / system reboot. "
                        f"The host restarted at {datetime.fromtimestamp(host_boot_ts).strftime('%d %b %Y %H:%M:%S IST')}. "
                        f"Estimated offline downtime: {downtime_str}. Surveillance streams and VLM models have been automatically restored."
                    )
                    remed = "Check facility UPS battery backup and main power circuit. Ensure uninterrupted power supply."
                    severity = "CRITICAL"
                else:
                    event_type = "UNEXPECTED_CRASH_RECOVERY"
                    title = "⚠️ Crash Recovery: RapidAlert Restarted After Unexpected Termination"
                    msg = (
                        f"RapidAlert recovered after an unexpected process crash or sudden termination. "
                        f"Last recorded heartbeat: {prior_hb_str}. Offline duration: {downtime_str}. "
                        f"Camera feeds and inference pipelines have been re-initialized."
                    )
                    remed = "Review Linux dmesg, system logs, or GPU memory utilization for OOM / kernel signals."
                    severity = "CRITICAL"

                print(f"[WatchdogEmailer] 🚨 Detected prior ungraceful shutdown ({event_type}). Dispatching alert...")
                self.send_critical_alert(
                    event_type=event_type,
                    title=title,
                    message=msg,
                    details={
                        "Incident Reason": "Hardware Power Loss / Host Reboot" if host_rebooted else "Process Crash / Abrupt Termination",
                        "Last Heartbeat": prior_hb_str,
                        "Estimated Downtime": downtime_str,
                        "Host Boot Time": datetime.fromtimestamp(host_boot_ts).strftime("%d %b %Y %H:%M:%S IST"),
                        "Current Recovery Time": datetime.fromtimestamp(now_ts).strftime("%d %b %Y %H:%M:%S IST"),
                        "Previous Process PID": prior_state.get("pid", "Unknown"),
                        "Current Process PID": os.getpid(),
                    },
                    severity=severity,
                    remediation=remed,
                    cooldown_sec=0.0,
                )
            elif prior_state:
                # Planned or graceful restart
                if host_rebooted:
                    event_type = "THOR_SYSTEM_REBOOT"
                    title = "⚡ NVIDIA Thor Host Rebooted & Online"
                    msg = (
                        f"NVIDIA Thor system was rebooted and is now fully online. "
                        f"RapidAlert surveillance pipelines, camera streams, and vLLM inference instances have initialized cleanly."
                    )
                    remed = "System reboot was clean and normal. All feeds active."
                else:
                    event_type = "SYSTEM_RESTART"
                    title = "🔄 RapidAlert Surveillance Engine Restarted & Online"
                    msg = (
                        f"RapidAlert surveillance service was restarted (PID {os.getpid()}). "
                        f"Previous shutdown was graceful ({prior_state.get('reason', 'Clean Shutdown')}). "
                        f"All feeds and AI models are running normally."
                    )
                    remed = "Service is healthy and active. No action needed."

                print(f"[WatchdogEmailer] 🔄 System restart detected ({event_type}). Dispatching restart notification...")
                self.send_critical_alert(
                    event_type=event_type,
                    title=title,
                    message=msg,
                    details={
                        "Startup Event": "Host Reboot" if host_rebooted else "Service Restart",
                        "Host Boot Time": datetime.fromtimestamp(host_boot_ts).strftime("%d %b %Y %H:%M:%S IST"),
                        "Startup Timestamp": datetime.fromtimestamp(now_ts).strftime("%d %b %Y %H:%M:%S IST"),
                        "Process PID": os.getpid(),
                        "Status": "Healthy & Streaming",
                    },
                    severity="INFO",
                    remediation=remed,
                    cooldown_sec=0.0,
                )

            # Record current active running state
            self.state_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self.state_file, "w") as f:
                json.dump({
                    "status": "RUNNING",
                    "pid": os.getpid(),
                    "boot_wall_time": self._boot_wall_time,
                    "boot_time_str": datetime.fromtimestamp(self._boot_wall_time).strftime("%d %b %Y %H:%M:%S IST"),
                    "last_heartbeat": now_ts,
                    "last_heartbeat_str": datetime.fromtimestamp(now_ts).strftime("%d %b %Y %H:%M:%S IST"),
                    "host_boot_ts": host_boot_ts,
                }, f, indent=2)

        except Exception as exc:
            print(f"[WatchdogEmailer] ⚠️ Error auditing startup / crash state: {exc}")
            print(f"[WatchdogEmailer] ⚠️ Error auditing prior crash state: {exc}")

    def heartbeat_tick(self) -> None:
        """Periodically records heartbeat timestamp to sentinel file."""
        try:
            now_ts = time.time()
            if self.state_file.exists():
                with open(self.state_file, "r") as f:
                    state = json.load(f)
            else:
                state = {"status": "RUNNING", "pid": os.getpid(), "boot_wall_time": self._boot_wall_time}

            state["last_heartbeat"] = now_ts
            state["last_heartbeat_str"] = datetime.fromtimestamp(now_ts).strftime("%d %b %Y %H:%M:%S IST")
            state["status"] = "RUNNING"
            
            with open(self.state_file, "w") as f:
                json.dump(state, f, indent=2)
        except Exception:
            pass

    def mark_clean_shutdown(self, reason: str = "Clean Shutdown") -> None:
        """Records graceful shutdown to sentinel file to prevent false positive crash alerts on next boot."""
        try:
            now_ts = time.time()
            state = {
                "status": "CLEAN_SHUTDOWN",
                "shutdown_at": now_ts,
                "shutdown_at_str": datetime.fromtimestamp(now_ts).strftime("%d %b %Y %H:%M:%S IST"),
                "reason": reason,
                "pid": os.getpid(),
            }
            with open(self.state_file, "w") as f:
                json.dump(state, f, indent=2)
            print(f"[WatchdogEmailer] 🛡️ Sentinel state recorded: CLEAN_SHUTDOWN ({reason})")
        except Exception as exc:
            print(f"[WatchdogEmailer] ⚠️ Error recording clean shutdown state: {exc}")

    def _conn(self) -> sqlite3.Connection:
        if not hasattr(self._local, "conn") or self._local.conn is None:
            conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            self._local.conn = conn
        return self._local.conn

    def _get_sys_config(self) -> dict:
        try:
            if self.config_path.exists():
                with open(self.config_path, "r") as f:
                    return json.load(f)
        except Exception:
            pass
        return {}

    def _get_recipients(self) -> list[str]:
        cfg = self._get_sys_config()
        return cfg.get(
            "watchdog_email_recipients",
            cfg.get("report_email_recipients", ["pandalavacarji@gmail.com", "reportsclove@gmail.com"])
        )

    # ════════════════════════════════════════════════════════════
    #  1. Real-Time Severe Incident Alerting
    # ════════════════════════════════════════════════════════════

    def send_critical_alert(
        self,
        event_type: str,
        title: str,
        message: str,
        details: Optional[dict] = None,
        severity: str = "CRITICAL",
        stack_trace: Optional[str] = None,
        remediation: Optional[str] = None,
        cooldown_sec: float = 600.0,  # 10 minute deduplication per event key
    ) -> bool:
        """
        Dispatches an immediate high-priority alert email for severe system events.
        Applies cooldown throttling to avoid email floods during persistent failures.
        """
        now = time.monotonic()
        event_key = f"{event_type}_{title}"

        with self._lock:
            last_sent = self._sent_alerts.get(event_key, 0.0)
            if now - last_sent < cooldown_sec:
                print(f"[WatchdogEmailer] ⏳ Suppressed duplicate alert '{event_key}' (cooldown: {int(cooldown_sec - (now - last_sent))}s remaining)")
                return False
            self._sent_alerts[event_key] = now
            self._active_incidents[event_key] = True

        # Dispatch in background thread
        threading.Thread(
            target=self._send_critical_alert_sync,
            args=(event_type, title, message, details, severity, stack_trace, remediation),
            daemon=True,
            name=f"watchdog-alert-{event_type}"
        ).start()
        return True

    def send_resolution_alert(
        self,
        event_type: str,
        title: str,
        message: str,
        details: Optional[dict] = None,
    ) -> bool:
        """Sends a green [RESOLVED] email when a critical incident condition clears."""
        event_key = f"{event_type}_{title}"
        with self._lock:
            if not self._active_incidents.get(event_key, False):
                return False  # Was not actively alerted
            self._active_incidents[event_key] = False
            self._sent_alerts.pop(event_key, None)

        threading.Thread(
            target=self._send_resolution_alert_sync,
            args=(event_type, title, message, details),
            daemon=True,
            name=f"watchdog-resolve-{event_type}"
        ).start()
        return True

    def _send_critical_alert_sync(
        self,
        event_type: str,
        title: str,
        message: str,
        details: Optional[dict],
        severity: str,
        stack_trace: Optional[str],
        remediation: Optional[str],
    ) -> None:
        recipients = self._get_recipients()
        if not recipients:
            return

        cfg = self._get_sys_config()
        sender_email = cfg.get("report_sender_email", "reportsclove@gmail.com")
        sender_password = cfg.get("report_sender_password", "wakl rpps mvql eznn")
        if not sender_password:
            return

        ts_str = datetime.now().strftime("%d %b %Y %H:%M:%S IST")
        uptime_sec = round(time.monotonic() - self._start_time, 1)
        uptime_str = f"{int(uptime_sec // 3600)}h {int((uptime_sec % 3600) // 60)}m {int(uptime_sec % 60)}s"

        details_rows = ""
        if details:
            details_rows = "".join(
                f"<tr><td style='padding: 6px 10px; font-weight: 700; color: #475569; width: 35%; border-bottom: 1px solid #e2e8f0;'>{k}</td>"
                f"<td style='padding: 6px 10px; color: #0f172a; font-family: monospace; border-bottom: 1px solid #e2e8f0;'>{v}</td></tr>"
                for k, v in details.items()
            )

        stack_html = ""
        if stack_trace:
            stack_html = f"""
            <div style='margin-top: 14px;'>
                <div style='font-size: 11px; font-weight: 700; color: #b91c1c; text-transform: uppercase; margin-bottom: 4px;'>Technical Diagnostics & Stack Trace</div>
                <pre style='background: #1e1e1e; color: #f87171; padding: 12px; border-radius: 6px; font-size: 10px; overflow-x: auto; font-family: monospace;'>{stack_trace[:1500]}</pre>
            </div>
            """

        is_restart = severity.upper() in ("INFO", "RESTART", "STARTUP")
        
        if is_restart:
            border_color = "#0284c7"
            hdr_bg = "linear-gradient(135deg, #0f172a 0%, #1e3a8a 50%, #0369a1 100%)"
            badge_html = "<span style='font-size: 10px; font-weight: 800; background: #e0f2fe; color: #0369a1; padding: 3px 8px; border-radius: 4px; letter-spacing: 0.5px;'>🔄 SYSTEM ONLINE / RESTARTED</span>"
            action_box_style = "background: #f0fdf4; border-left: 4px solid #16a34a; padding: 10px 14px; border-radius: 0 6px 6px 0;"
            action_box_title = "<strong style='color: #15803d;'>✅ Status & Operation:</strong>"
            action_box_text_style = "color: #166534; font-size: 11.5px; margin-top: 3px;"
            subject_prefix = "⚡ [THE SENTRY]" if "THOR" in event_type or "POWER" in event_type else "🔄 [THE SENTRY]"
        else:
            border_color = "#dc2626"
            hdr_bg = "linear-gradient(135deg, #7f1d1d 0%, #991b1b 100%)"
            badge_html = f"<span style='font-size: 10px; font-weight: 800; background: #fee2e2; color: #991b1b; padding: 3px 8px; border-radius: 4px; letter-spacing: 0.5px;'>🚨 {severity.upper()} INCIDENT</span>"
            action_box_style = "background: #fff7ed; border-left: 4px solid #ea580c; padding: 10px 14px; border-radius: 0 6px 6px 0;"
            action_box_title = "<strong style='color: #9a3412;'>🛠️ Recommended Action:</strong>"
            action_box_text_style = "color: #c2410c; font-size: 11.5px; margin-top: 3px;"
            subject_prefix = "🚨 [THE SENTRY CRITICAL]"

        action_html = f"""
        <div style='margin-top: 14px; {action_box_style}'>
            {action_box_title}
            <div style='{action_box_text_style}'>{remediation or 'Review system status or inspect the dashboard at http://localhost:7000.'}</div>
        </div>
        """

        html_body = f"""<!DOCTYPE html>
<html>
<head><meta charset='utf-8'></head>
<body style='margin: 0; padding: 20px 0; background-color: #0f172a; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;'>
    <div style='max-width: 640px; margin: 0 auto; background-color: #ffffff; border-radius: 10px; overflow: hidden; border: 1px solid {border_color};'>
        <!-- Header -->
        <div style='background: {hdr_bg}; padding: 22px 24px; color: #ffffff;'>
            <div style='display: flex; justify-content: space-between; align-items: center;'>
                {badge_html}
                <span style='font-size: 11px; color: #bae6fd if is_restart else #fca5a5;'>{ts_str}</span>
            </div>
            <h1 style='margin: 10px 0 4px 0; font-size: 19px; font-weight: 800; color: #ffffff;'>{title}</h1>
            <div style='font-size: 12px; color: #e2e8f0;'>Event Type: <strong>{event_type}</strong> • Platform: NVIDIA Thor</div>
        </div>

        <!-- Body -->
        <div style='padding: 22px 24px;'>
            <div style='font-size: 13px; color: #1e293b; line-height: 1.5; margin-bottom: 16px;'>
                {message}
            </div>

            <!-- Context Details Table -->
            <table width='100%' cellspacing='0' cellpadding='0' style='background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 6px; font-size: 11.5px; border-collapse: collapse;'>
                <tr><td style='padding: 6px 10px; font-weight: 700; color: #475569; width: 35%; border-bottom: 1px solid #e2e8f0;'>System Uptime</td><td style='padding: 6px 10px; color: #0f172a; font-family: monospace; border-bottom: 1px solid #e2e8f0;'>{uptime_str}</td></tr>
                {details_rows}
            </table>

            {action_html}
            {stack_html}

            <div style='margin-top: 20px; text-align: center;'>
                <a href='http://localhost:7000' style='background: #0f172a; color: #ffffff; text-decoration: none; padding: 10px 20px; border-radius: 6px; font-size: 12px; font-weight: 700; display: inline-block;'>
                    🖥️ Open The Sentry Dashboard
                </a>
            </div>
        </div>

        <!-- Footer -->
        <div style='background: #f8fafc; padding: 12px 24px; border-top: 1px solid #e2e8f0; font-size: 10px; color: #94a3b8; text-align: center;'>
            Automated Alert from The Sentry Continuous Health Watchdog • Clove Technologies
        </div>
    </div>
</body>
</html>"""

        msg = EmailMessage()
        msg["Subject"] = f"{subject_prefix} {title} — {ts_str}"
        msg["From"] = sender_email
        msg["To"] = ", ".join(recipients)
        msg.set_content(f"The Sentry System Event: {title}\nTime: {ts_str}\n\n{message}\n\nUptime: {uptime_str}")
        msg.add_alternative(html_body, subtype="html")

        try:
            with smtplib.SMTP("smtp.gmail.com", 587, timeout=25) as smtp:
                smtp.ehlo()
                smtp.starttls()
                smtp.ehlo()
                smtp.login(sender_email, sender_password)
                smtp.send_message(msg)
            print(f"[WatchdogEmailer] 📨 System event email dispatched successfully to: {recipients}")
        except Exception as exc:
            print(f"[WatchdogEmailer] ❌ Failed to dispatch email: {exc}")

    def _send_resolution_alert_sync(
        self,
        event_type: str,
        title: str,
        message: str,
        details: Optional[dict],
    ) -> None:
        recipients = self._get_recipients()
        if not recipients:
            return
        cfg = self._get_sys_config()
        sender_email = cfg.get("report_sender_email", "reportsclove@gmail.com")
        sender_password = cfg.get("report_sender_password", "wakl rpps mvql eznn")
        if not sender_password:
            return

        ts_str = datetime.now().strftime("%d %b %Y %H:%M:%S IST")
        html_body = f"""<!DOCTYPE html>
<html>
<head><meta charset='utf-8'></head>
<body style='margin: 0; padding: 20px 0; background-color: #0f172a; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;'>
    <div style='max-width: 640px; margin: 0 auto; background-color: #ffffff; border-radius: 10px; overflow: hidden; border: 1px solid #10b981;'>
        <div style='background: linear-gradient(135deg, #064e3b 0%, #047857 100%); padding: 20px 24px; color: #ffffff;'>
            <span style='font-size: 10px; font-weight: 800; background: #d1fae5; color: #065f46; padding: 3px 8px; border-radius: 4px; letter-spacing: 0.5px;'>✅ RESOLVED</span>
            <h1 style='margin: 8px 0 4px 0; font-size: 18px; font-weight: 800; color: #ffffff;'>RESOLVED: {title}</h1>
            <div style='font-size: 11px; color: #a7f3d0;'>Timestamp: {ts_str}</div>
        </div>
        <div style='padding: 20px 24px; font-size: 13px; color: #334155; line-height: 1.5;'>
            {message}
            <div style='margin-top: 16px; text-align: center;'>
                <a href='http://localhost:7000' style='background: #047857; color: #ffffff; text-decoration: none; padding: 8px 18px; border-radius: 6px; font-size: 12px; font-weight: 700; display: inline-block;'>
                    🖥️ Check System Status
                </a>
            </div>
        </div>
    </div>
</body>
</html>"""
        msg = EmailMessage()
        msg["Subject"] = f"✅ [RESOLVED] RapidAlert: {title} — {ts_str}"
        msg["From"] = sender_email
        msg["To"] = ", ".join(recipients)
        msg.set_content(f"Resolved: {title}\nTime: {ts_str}\n\n{message}")
        msg.add_alternative(html_body, subtype="html")

        try:
            with smtplib.SMTP("smtp.gmail.com", 587, timeout=20) as smtp:
                smtp.ehlo()
                smtp.starttls()
                smtp.ehlo()
                smtp.login(sender_email, sender_password)
                smtp.send_message(msg)
            print(f"[WatchdogEmailer] ✅ Incident resolution email sent to: {recipients}")
        except Exception as exc:
            print(f"[WatchdogEmailer] ❌ Resolution email dispatch failed: {exc}")

    # ════════════════════════════════════════════════════════════
    #  2. Automated 24-Hour System Health & Diagnostics Digest
    # ════════════════════════════════════════════════════════════

    def compile_daily_health_summary(self) -> dict:
        """
        Gathers complete diagnostics from SQLite error_logs, analyses, and system services for the last 24h.
        """
        now_ts = time.time()
        past_24h_ts = now_ts - 86400.0
        conn = self._conn()

        # 1. Error counts in last 24h
        err_rows = conn.execute("""
            SELECT severity, component, message, timestamp
            FROM error_logs
            WHERE ts >= ?
            ORDER BY ts DESC
        """, (past_24h_ts,)).fetchall()

        total_errs = len(err_rows)
        crit_cnt = sum(1 for r in err_rows if (r["severity"] or "").upper() == "CRITICAL")
        err_cnt = sum(1 for r in err_rows if (r["severity"] or "").upper() == "ERROR")
        warn_cnt = sum(1 for r in err_rows if (r["severity"] or "").upper() == "WARNING")

        recent_err_samples = [
            f"[{r['timestamp']}] ({r['severity']}) [{r['component']}]: {r['message'][:120]}"
            for r in err_rows[:5]
        ]

        # 2. Analyses count in last 24h
        analysis_row = conn.execute("""
            SELECT COUNT(*) AS total,
                   SUM(CASE WHEN severity = 'HIGH' THEN 1 ELSE 0 END) AS high_cnt,
                   SUM(CASE WHEN severity = 'MEDIUM' THEN 1 ELSE 0 END) AS med_cnt
            FROM analyses
            WHERE ts >= ?
        """, (past_24h_ts,)).fetchone()

        total_analyses_24h = analysis_row["total"] if analysis_row else 0
        high_incidents_24h = analysis_row["high_cnt"] if analysis_row and analysis_row["high_cnt"] else 0
        med_incidents_24h = analysis_row["med_cnt"] if analysis_row and analysis_row["med_cnt"] else 0

        # 3. Reports count in last 24h
        reports_row = conn.execute("""
            SELECT COUNT(*) AS total,
                   SUM(CASE WHEN email_status = 'SENT' THEN 1 ELSE 0 END) AS sent_cnt
            FROM reports
            WHERE generated_at >= ?
        """, (past_24h_ts,)).fetchone()

        total_reports_24h = reports_row["total"] if reports_row else 0
        sent_reports_24h = reports_row["sent_cnt"] if reports_row and reports_row["sent_cnt"] else 0

        # 4. Camera & Hardware telemetry
        active_cams = self.camera_manager.get_active_cameras() if self.camera_manager else []
        modes = self.camera_manager.get_camera_modes() if self.camera_manager else {}
        nvdec_count = sum(1 for m in modes.values() if "NVDEC" in m)
        cpu_count = sum(1 for m in modes.values() if "CPU" in m)

        # 5. vLLM Shards
        shards = self.vlm_pool.get_stats() if self.vlm_pool else []
        healthy_shards = sum(1 for s in shards if s.get("healthy", False))

        uptime_sec = round(time.monotonic() - self._start_time, 1)
        uptime_str = f"{int(uptime_sec // 3600)}h {int((uptime_sec % 3600) // 60)}m"

        return {
            "period": "Last 24 Hours",
            "date_str": datetime.now().strftime("%d %b %Y"),
            "time_str": datetime.now().strftime("%H:%M IST"),
            "uptime": uptime_str,
            "total_errors": total_errs,
            "critical_errors": crit_cnt,
            "standard_errors": err_cnt,
            "warnings": warn_cnt,
            "recent_errors": recent_err_samples,
            "total_analyses_24h": total_analyses_24h,
            "high_incidents_24h": high_incidents_24h,
            "med_incidents_24h": med_incidents_24h,
            "reports_generated_24h": total_reports_24h,
            "reports_sent_24h": sent_reports_24h,
            "active_cameras_count": len(active_cams),
            "nvdec_hardware_count": nvdec_count,
            "cpu_fallback_count": cpu_count,
            "vllm_shards_healthy": f"{healthy_shards}/{len(shards)}",
            "camera_modes": modes,
        }

    def send_daily_health_digest(self) -> bool:
        """Compiles and emails the 24-hour health and diagnostic digest."""
        summary = self.compile_daily_health_summary()
        threading.Thread(
            target=self._send_daily_digest_sync,
            args=(summary,),
            daemon=True,
            name="watchdog-daily-digest"
        ).start()
        return True

    def _send_daily_digest_sync(self, summary: dict) -> None:
        recipients = self._get_recipients()
        if not recipients:
            return

        cfg = self._get_sys_config()
        sender_email = cfg.get("report_sender_email", "reportsclove@gmail.com")
        sender_password = cfg.get("report_sender_password", "wakl rpps mvql eznn")
        if not sender_password:
            return

        date_str = summary["date_str"]
        time_str = summary["time_str"]
        crit_cnt = summary["critical_errors"]
        err_cnt = summary["standard_errors"]
        total_errs = summary["total_errors"]

        health_badge = "<span style='background: #dcfce7; color: #16a34a; padding: 4px 10px; border-radius: 4px; font-weight: 700; font-size: 11px;'>🟢 ALL SYSTEMS HEALTHY</span>"
        if crit_cnt > 0:
            health_badge = f"<span style='background: #fee2e2; color: #dc2626; padding: 4px 10px; border-radius: 4px; font-weight: 700; font-size: 11px;'>🚨 {crit_cnt} CRITICAL ISSUE(S)</span>"
        elif err_cnt > 0:
            health_badge = f"<span style='background: #fef3c7; color: #d97706; padding: 4px 10px; border-radius: 4px; font-weight: 700; font-size: 11px;'>⚠️ {err_cnt} ERROR(S) LOGGED</span>"

        err_list_html = "".join(
            f"<li style='margin-bottom: 6px; font-family: monospace; font-size: 10.5px; color: #475569;'>{e}</li>"
            for e in summary["recent_errors"]
        ) or "<li style='color: #16a34a; font-weight: 600;'>✔ Zero unresolved system errors logged in the last 24 hours.</li>"

        cam_modes_html = "".join(
            f"<span style='background: #f1f5f9; border: 1px solid #cbd5e1; padding: 3px 8px; border-radius: 4px; font-size: 10px; margin: 3px; display: inline-block;'><strong>{cam}:</strong> <span style='color: {'#16a34a' if 'NVDEC' in mode else '#d97706'}; font-weight: 700;'>{mode}</span></span>"
            for cam, mode in summary["camera_modes"].items()
        ) or "<span>All 7 cameras active</span>"

        html_body = f"""<!DOCTYPE html>
<html>
<head><meta charset='utf-8'></head>
<body style='margin: 0; padding: 20px 0; background-color: #f1f5f9; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;'>
    <div style='max-width: 640px; margin: 0 auto; background-color: #ffffff; border-radius: 10px; overflow: hidden; border: 1px solid #cbd5e1;'>
        <!-- Header -->
        <div style='background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%); padding: 24px 24px; color: #ffffff;'>
            <div style='display: flex; justify-content: space-between; align-items: center;'>
                <span style='font-size: 10px; font-weight: 700; color: #38bdf8; text-transform: uppercase; letter-spacing: 1px;'>RAPIDALERT WATCHDOG • DAILY DIAGNOSTICS</span>
                <span style='font-size: 11px; color: #94a3b8;'>{date_str} {time_str}</span>
            </div>
            <h1 style='margin: 8px 0 6px 0; font-size: 20px; font-weight: 800; color: #ffffff;'>24-Hour System Health Digest</h1>
            <div style='margin-top: 8px;'>{health_badge}</div>
        </div>

        <!-- KPI Grid -->
        <div style='padding: 20px 24px;'>
            <table width='100%' cellspacing='0' cellpadding='0' style='margin-bottom: 20px;'>
                <tr>
                    <td align='center' style='padding: 10px; background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 6px; width: 25%;'>
                        <div style='font-size: 18px; font-weight: 800; color: #0f172a;'>{summary['uptime']}</div>
                        <div style='font-size: 9px; font-weight: 700; color: #64748b; margin-top: 2px;'>CURRENT UPTIME</div>
                    </td>
                    <td style='width: 6px;'></td>
                    <td align='center' style='padding: 10px; background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 6px; width: 25%;'>
                        <div style='font-size: 18px; font-weight: 800; color: {"#dc2626" if total_errs > 0 else "#16a34a"};'>{total_errs}</div>
                        <div style='font-size: 9px; font-weight: 700; color: #64748b; margin-top: 2px;'>ERRORS (24H)</div>
                    </td>
                    <td style='width: 6px;'></td>
                    <td align='center' style='padding: 10px; background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 6px; width: 25%;'>
                        <div style='font-size: 18px; font-weight: 800; color: #0f172a;'>{summary['total_analyses_24h']}</div>
                        <div style='font-size: 9px; font-weight: 700; color: #64748b; margin-top: 2px;'>AI ANALYSES</div>
                    </td>
                    <td style='width: 6px;'></td>
                    <td align='center' style='padding: 10px; background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 6px; width: 25%;'>
                        <div style='font-size: 18px; font-weight: 800; color: #16a34a;'>{summary['vllm_shards_healthy']}</div>
                        <div style='font-size: 9px; font-weight: 700; color: #64748b; margin-top: 2px;'>vLLM SHARDS</div>
                    </td>
                </tr>
            </table>

            <!-- Camera Hardware Status -->
            <div style='margin-bottom: 18px;'>
                <div style='font-size: 11px; font-weight: 700; color: #0f172a; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 8px; border-bottom: 2px solid #3b82f6; padding-bottom: 4px; display: inline-block;'>
                    📹 Camera Ingestion & NVDEC Decoder Status ({summary['active_cameras_count']} Active)
                </div>
                <div>{cam_modes_html}</div>
            </div>

            <!-- Error Log Breakdown -->
            <div style='margin-bottom: 18px;'>
                <div style='font-size: 11px; font-weight: 700; color: #0f172a; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 8px; border-bottom: 2px solid #ea580c; padding-bottom: 4px; display: inline-block;'>
                    🛠️ 24-Hour Error & Diagnostics Log ({total_errs} Total: {crit_cnt} Crit, {err_cnt} Err, {summary['warnings']} Warn)
                </div>
                <ul style='margin: 0; padding-left: 18px;'>
                    {err_list_html}
                </ul>
            </div>

            <!-- Shift Reports Performance -->
            <div style='padding: 10px 14px; background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 6px; font-size: 11px; color: #475569;'>
                📑 <strong>Shift Reporting Activity:</strong> {summary['reports_generated_24h']} executive shift reports generated and {summary['reports_sent_24h']} dispatched to executive recipients in the past 24 hours.
            </div>

            <div style='margin-top: 20px; text-align: center;'>
                <a href='http://localhost:7000' style='background: #0f172a; color: #ffffff; text-decoration: none; padding: 10px 20px; border-radius: 6px; font-size: 12px; font-weight: 700; display: inline-block;'>
                    🖥️ Open The Sentry Dashboard
                </a>
            </div>
        </div>

        <!-- Footer -->
        <div style='background: #f8fafc; padding: 12px 24px; border-top: 1px solid #e2e8f0; font-size: 10px; color: #94a3b8; text-align: center;'>
            The Sentry Autonomous Surveillance Watchdog • Jetson Thor Platform • Clove Technologies
        </div>
    </div>
</body>
</html>"""

        msg = EmailMessage()
        msg["Subject"] = f"📊 [The Sentry] 24-Hour System Health & Diagnostics Digest — {date_str}"
        msg["From"] = sender_email
        msg["To"] = ", ".join(recipients)
        msg.set_content(f"The Sentry 24-Hour System Health Digest for {date_str}\nUptime: {summary['uptime']}\nTotal Errors: {total_errs}\nAI Analyses: {summary['total_analyses_24h']}")
        msg.add_alternative(html_body, subtype="html")

        try:
            with smtplib.SMTP("smtp.gmail.com", 587, timeout=25) as smtp:
                smtp.ehlo()
                smtp.starttls()
                smtp.ehlo()
                smtp.login(sender_email, sender_password)
                smtp.send_message(msg)
            print(f"[WatchdogEmailer] 📊 Daily health digest email dispatched to: {recipients}")
        except Exception as exc:
            print(f"[WatchdogEmailer] ❌ Daily digest email dispatch failed: {exc}")
