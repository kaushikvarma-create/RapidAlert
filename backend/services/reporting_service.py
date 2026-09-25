"""
ReportingService: Automated Shift Reporting & Email Delivery for RapidAlert.
Generates executive-grade shift reports at 06:00 (Night Shift) and 18:00 (Day Shift).
Compiles metrics directly from SQLite (analyses.db), summarizes camera routines via VLM,
builds professional PDF reports using WeasyPrint, and delivers executive emails via Gmail SMTP.
Maintains a strict rolling buffer of 69 reports (~1.15 months of twice-daily reports, <20 MB).
"""
from __future__ import annotations

import os
import sys
import json
import time
import base64
import smtplib
import sqlite3
import asyncio
import threading
from email.message import EmailMessage
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple

import weasyprint
from jinja2 import Environment

from backend.core.config import DATABASE_PATH, SYSTEM_CONFIG_PATH
from backend.core.error_tracker import error_tracker
from backend.core.shutdown_logger import log_system_event

MAX_REPORTS_BUFFER = 69  # Exactly 69 shift reports (~34.5 days of twice-daily shifts)

ZONE_MAP = {
    "1ST_ENTRANCE": "1F Corridor & Turnstiles",
    "1ST_EXIT": "1F Corridor Exit",
    "1ST_OUT": "Lobby & Lift Area",
    "ADMIN_CABIN": "Executive Admin Cabin",
    "PARKING": "Basement Parking Zone",
    "RECEPTION": "Main Front Reception Desk",
    "SERVER_ENTRY": "Server Room Corridor"
}


class ReportingService:
    def __init__(
        self,
        db_path: Path = DATABASE_PATH,
        config_path: Path = SYSTEM_CONFIG_PATH,
        vlm_pool=None,
        frame_store=None,
    ):
        self.db_path = db_path
        self.config_path = config_path
        self.vlm_pool = vlm_pool
        self.frame_store = frame_store
        self._local = threading.local()
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._lock = threading.Lock()
        self._last_scheduled_slot: Optional[str] = None
        self.reports_dir = Path(db_path).parent / "reports"
        self.reports_dir.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _conn(self) -> sqlite3.Connection:
        if not hasattr(self._local, "conn") or self._local.conn is None:
            conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            self._local.conn = conn
        return self._local.conn

    def _init_schema(self) -> None:
        try:
            conn = self._conn()
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS reports (
                    id               INTEGER PRIMARY KEY AUTOINCREMENT,
                    title            TEXT    NOT NULL,
                    shift_type       TEXT    NOT NULL,
                    period_start     REAL    NOT NULL,
                    period_end       REAL    NOT NULL,
                    generated_at     REAL    NOT NULL,
                    pdf_path         TEXT    NOT NULL,
                    summary_json     TEXT    NOT NULL,
                    email_status     TEXT    NOT NULL,
                    email_recipients TEXT,
                    error_message    TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_reports_generated ON reports(generated_at);
            """)
            conn.commit()
        except Exception as exc:
            error_tracker.capture_exception(
                exc,
                component="ReportingService",
                effect="Failed to initialize reports SQLite schema",
                severity="ERROR",
            )

    def _get_sys_config(self) -> dict:
        try:
            if self.config_path.exists():
                with open(self.config_path, "r") as f:
                    return json.load(f)
        except Exception:
            pass
        return {}

    def _enforce_rolling_buffer(self) -> int:
        """
        Enforces a rolling buffer of MAX_REPORTS_BUFFER (69 reports).
        Prunes older report records and removes their PDF files from disk.
        """
        pruned_count = 0
        try:
            conn = self._conn()
            # Fetch all rows beyond the top MAX_REPORTS_BUFFER
            rows = conn.execute("""
                SELECT id, pdf_path FROM reports
                ORDER BY generated_at DESC
                LIMIT -1 OFFSET ?
            """, (MAX_REPORTS_BUFFER,)).fetchall()

            if rows:
                for r in rows:
                    if r["pdf_path"]:
                        try:
                            p = Path(r["pdf_path"])
                            if p.exists():
                                p.unlink(missing_ok=True)
                        except Exception:
                            pass
                    conn.execute("DELETE FROM reports WHERE id = ?", (r["id"],))
                    pruned_count += 1
                conn.commit()
                print(f"[ReportService] 🧹 Pruned {pruned_count} old reports to maintain rolling {MAX_REPORTS_BUFFER}-report buffer.")
        except Exception as exc:
            error_tracker.capture_exception(
                exc,
                component="ReportingService",
                effect=f"Failed to enforce rolling {MAX_REPORTS_BUFFER} report buffer",
                severity="WARNING",
            )
        return pruned_count

    # ── VLM Summarization & Visual Vehicle Auditing ─────────────

    def _get_routine_summary(self, cam_name: str, events: list[dict]) -> str:
        if not events:
            return "Routine baseline surveillance active. Zero security or perimeter anomalies detected."

        obs_list = [f"- {e['ts']} ({e['sev']}): {e['obs']}" for e in events[:10]]
        prompt = (
            f"Analyze these CCTV activity events for camera '{cam_name}' and write a single, short, clear, professional sentence summarizing the routine activity during this shift. "
            f"Do not use bullet points, prefixes, or chatty greetings. Output ONLY the single sentence.\n"
            + "\n".join(obs_list)
        )

        try:
            import requests
            resp = requests.post(
                "http://localhost:8000/v1/chat/completions",
                json={
                    "model": "vrfai/Cosmos-Reason2-8B-NVFP4",
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.2,
                    "max_tokens": 60,
                },
                timeout=8,
            )
            if resp.ok:
                data = resp.json()
                text = data["choices"][0]["message"]["content"].strip().replace('"', "")
                if text:
                    return text
        except Exception:
            pass
        return "Standard operational movement and routine zone monitoring observed."

    def _get_parking_vehicle_count(self, latest_frame_bytes: Optional[bytes]) -> str:
        if not latest_frame_bytes:
            return "2W: 0\n4W: 0\nHV: 0"

        try:
            import requests
            import re
            b64_img = base64.b64encode(latest_frame_bytes).decode("utf-8")
            prompt = (
                "Carefully scan the ENTIRE image and count every single vehicle visible anywhere in the frame.\n"
                "Count the vehicles by category:\n"
                "- 2W (motorcycles, bikes, scooters)\n"
                "- 4W (cars, SUVs, vans)\n"
                "- HV (trucks, buses, heavy machinery)\n\n"
                "Return ONLY a valid JSON object in this exact format without any markdown:\n"
                "{\"2W\": 0, \"4W\": 0, \"HV\": 0}"
            )

            resp = requests.post(
                "http://localhost:8000/v1/chat/completions",
                json={
                    "model": "vrfai/Cosmos-Reason2-8B-NVFP4",
                    "messages": [{
                        "role": "user",
                        "content": [
                            {"type": "text", "text": prompt},
                            {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{b64_img}"}}
                        ]
                    }],
                    "temperature": 0.1,
                    "max_tokens": 50,
                },
                timeout=15,
            )
            if resp.ok:
                reply = resp.json()["choices"][0]["message"]["content"].strip()
                match = re.search(r"\{.*?\}", reply, re.DOTALL)
                if match:
                    d = json.loads(match.group(0))
                    return f"2W: {d.get('2W', 0)}\n4W: {d.get('4W', 0)}\nHV: {d.get('HV', 0)}"
        except Exception:
            pass
        return "2W: 0\n4W: 0\nHV: 0"

    # ── Report Generation ───────────────────────────────────────

    def generate_shift_report(
        self,
        shift_type: str = "AUTO",
        custom_start: Optional[float] = None,
        custom_end: Optional[float] = None,
        send_email: bool = True,
    ) -> Dict[str, Any]:
        """
        Generates executive PDF shift report from SQLite data and optionally dispatches via email.
        Maintains rolling 69-report buffer.
        """
        now = datetime.now()
        now_ts = time.time()

        # Determine shift time window
        if custom_start and custom_end:
            p_start, p_end = custom_start, custom_end
            start_dt = datetime.fromtimestamp(p_start)
            end_dt = datetime.fromtimestamp(p_end)
            shift_label = "CUSTOM"
        else:
            if shift_type == "NIGHT" or (shift_type == "AUTO" and now.hour < 12):
                # Night shift: yesterday 18:00 to today 06:00
                yesterday = now - timedelta(days=1)
                start_dt = yesterday.replace(hour=18, minute=0, second=0, microsecond=0)
                end_dt = now.replace(hour=6, minute=0, second=0, microsecond=0)
                shift_label = "NIGHT (18:00 – 06:00)"
            else:
                # Day shift: today 06:00 to today 18:00
                start_dt = now.replace(hour=6, minute=0, second=0, microsecond=0)
                end_dt = now.replace(hour=18, minute=0, second=0, microsecond=0)
                shift_label = "DAY (06:00 – 18:00)"
            
            p_start = start_dt.timestamp()
            p_end = end_dt.timestamp()

        # Fallback if testing before window has passed: cap p_end to current time
        if p_end > now_ts:
            p_end = now_ts

        time_window = f"{start_dt.strftime('%H:%M')} – {end_dt.strftime('%H:%M')} IST"
        date_str = start_dt.strftime("%d %b %Y")

        conn = self._conn()

        # Fetch active camera list
        cam_rows = conn.execute("""
            SELECT DISTINCT cam FROM analyses
            UNION
            SELECT name FROM (
                SELECT 'SERVER_ENTRY' AS name UNION SELECT '1ST_ENTRANCE' UNION
                SELECT 'PARKING' UNION SELECT '1ST_OUT' UNION
                SELECT 'ADMIN_CABIN' UNION SELECT '1ST_EXIT' UNION SELECT 'RECEPTION'
            )
        """).fetchall()
        all_cams = sorted(list(set(r[0] for r in cam_rows if r[0])))

        # Fetch analyses in period
        analyses_rows = conn.execute("""
            SELECT id, cam, ts, observation, activity, workers, machinery, safety, severity, incident_id
            FROM analyses
            WHERE ts >= ? AND ts <= ?
            ORDER BY ts ASC
        """, (p_start, p_end)).fetchall()

        events_by_cam: Dict[str, list[dict]] = {c: [] for c in all_cams}
        peak_occ_by_cam: Dict[str, int] = {c: 0 for c in all_cams}
        active_counts: Dict[str, int] = {c: 0 for c in all_cams}
        total_counts: Dict[str, int] = {c: 0 for c in all_cams}

        high_events = []
        med_events = []

        # Temp cache for evidence JPEGs for PDF embedding
        evidence_tmp_dir = self.reports_dir / "evidence_cache"
        evidence_tmp_dir.mkdir(parents=True, exist_ok=True)

        for row in analyses_rows:
            c = row["cam"]
            if c not in events_by_cam:
                events_by_cam[c] = []
                peak_occ_by_cam[c] = 0
                active_counts[c] = 0
                total_counts[c] = 0

            total_counts[c] += 1
            act = (row["activity"] or "IDLE").upper()
            if act in ("ACTIVE", "MODERATE"):
                active_counts[c] += 1

            # Parse worker count
            w_str = str(row["workers"] or "0").split()[0]
            try:
                w_c = int(w_str)
            except Exception:
                w_c = 0
            if w_c > peak_occ_by_cam[c]:
                peak_occ_by_cam[c] = w_c

            sev = (row["severity"] or "LOW").upper()
            ev_item = {
                "id": row["id"],
                "ts": datetime.fromtimestamp(row["ts"]).strftime("%H:%M:%S"),
                "mtime": row["ts"],
                "cam": c,
                "type": act if act != "UNKNOWN" else "MONITORING",
                "sev": sev,
                "obs": row["observation"] or "Routine scene observation",
                "incident_id": row["incident_id"],
                "img_path": None,
            }

            events_by_cam[c].append(ev_item)
            if sev == "HIGH":
                high_events.append(ev_item)
            elif sev == "MEDIUM":
                med_events.append(ev_item)

        # Attach image evidence for priority incidents with multi-stage resolution
        priority_events = high_events + med_events
        priority_events.sort(key=lambda x: (0 if x["sev"] == "HIGH" else 1, x["mtime"]))
        
        # Deduplicate and cap priority events in PDF
        selected_events = []
        seen_mins = set()
        for ev in priority_events:
            min_key = f"{ev['cam']}_{datetime.fromtimestamp(ev['mtime']).strftime('%Y%m%d_%H%M')}"
            if min_key not in seen_mins:
                seen_mins.add(min_key)
                
                # Multi-stage image frame lookup:
                # 1. Exact incident_id match in incident_frames
                f_row = None
                if ev.get("incident_id"):
                    f_row = conn.execute("""
                        SELECT frame_data FROM incident_frames
                        WHERE incident_id = ?
                        LIMIT 1
                    """, (ev["incident_id"],)).fetchone()

                # 2. Approximate timestamp & camera match in incident_frames
                if not f_row:
                    f_row = conn.execute("""
                        SELECT frame_data FROM incident_frames
                        WHERE cam = ? AND ABS(ts - ?) < 5.0
                        ORDER BY ABS(ts - ?) ASC
                        LIMIT 1
                    """, (ev["cam"], ev["mtime"], ev["mtime"])).fetchone()

                # 3. Fallback to latest cached frame from FrameStore if available
                img_data = f_row["frame_data"] if f_row and f_row["frame_data"] else None
                if not img_data and self.frame_store:
                    entry = self.frame_store.get_latest_cached_entry(ev["cam"])
                    if entry:
                        try:
                            img_data = base64.b64decode(entry[1])
                        except Exception:
                            pass

                if img_data:
                    img_file = evidence_tmp_dir / f"ev_{ev['id']}.jpg"
                    with open(img_file, "wb") as f:
                        f.write(img_data)
                    ev["img_path"] = str(img_file.resolve())

                selected_events.append(ev)

        selected_events.sort(key=lambda x: x["mtime"])

        # Latest parking image for vehicle categorization (multi-stage resolution)
        parking_img_bytes = None
        if self.frame_store:
            p_entry = self.frame_store.get_latest_cached_entry("PARKING")
            if p_entry:
                try:
                    parking_img_bytes = base64.b64decode(p_entry[1])
                except Exception:
                    pass

        if not parking_img_bytes:
            # Query recent frame from incident_frames table
            p_row = conn.execute("""
                SELECT frame_data FROM incident_frames
                WHERE cam = 'PARKING'
                ORDER BY ts DESC
                LIMIT 1
            """).fetchone()
            if p_row and p_row["frame_data"]:
                parking_img_bytes = p_row["frame_data"]

        # Compile general info rows per camera
        general_info = []
        routines = {}
        for c in all_cams:
            zone = ZONE_MAP.get(c, "Surveillance Zone")
            p_occ = peak_occ_by_cam.get(c, 0)
            tot = total_counts.get(c, 0)
            act_cnt = active_counts.get(c, 0)
            act_pct = f"{int((act_cnt / tot) * 100)}%" if tot > 0 else "0%"

            veh_str = "–"
            if c == "PARKING":
                veh_str = self._get_parking_vehicle_count(parking_img_bytes)

            in_cnt = "–"
            out_cnt = "–"
            general_info.append([c, zone, in_cnt, out_cnt, str(p_occ), veh_str, act_pct, "OK"])

            # Routine summary via LLM
            routines[c] = self._get_routine_summary(c, events_by_cam.get(c, []))

        # Overall Stats
        stats = {
            "total_analyses": len(analyses_rows),
            "high_count": len(high_events),
            "med_count": len(med_events),
            "peak_workers": max(peak_occ_by_cam.values()) if peak_occ_by_cam else 0,
            "active_cameras_count": sum(1 for c in all_cams if total_counts.get(c, 0) > 0),
            "total_cameras_count": len(all_cams),
        }

        # Render PDF via Jinja2 + WeasyPrint
        template_file = Path(__file__).resolve().parent.parent / "templates" / "report_template.html"
        if not template_file.exists():
            raise FileNotFoundError(f"Report template not found at {template_file}")

        with open(template_file, "r") as f:
            template_str = f.read()

        env = Environment()
        template = env.from_string(template_str)

        sys_config = self._get_sys_config()
        title = sys_config.get("report_title", "CLOVE HQ SITE INTELLIGENCE REPORT")

        html_content = template.render(
            settings={"title": title},
            date_str=date_str,
            time_window=time_window,
            shift_name=shift_label,
            stats=stats,
            general_info=general_info,
            all_events=selected_events[:20],
            routines=routines,
        )

        pdf_filename = f"Clove_HQ_Shift_Report_{now.strftime('%Y%m%d_%H%M%S')}.pdf"
        output_pdf_path = self.reports_dir / pdf_filename

        weasyprint.HTML(string=html_content).write_pdf(str(output_pdf_path))
        print(f"[ReportService] ✅ Executive PDF report compiled: {output_pdf_path}")

        # Build summary JSON
        exec_highlights = []
        for c, desc in routines.items():
            if "normal" not in desc.lower() or len(exec_highlights) < 3:
                exec_highlights.append(f"{c}: {desc}")

        summary_data = {
            "generated_at": now.strftime("%b %d, %Y %H:%M:%S"),
            "shift": shift_label,
            "time_window": time_window,
            "stats": stats,
            "exec_highlights": exec_highlights[:5],
            "safety_compliance": "✔ Standard safety compliance observed across all operational zones." if stats["high_count"] == 0 else f"⚠ {stats['high_count']} high severity incident(s) flagged during shift.",
            "active_cameras": all_cams,
        }

        # Email dispatch
        email_status = "DISABLED"
        email_err = None
        recipients = sys_config.get("report_email_recipients", ["pandalavacarji@gmail.com", "reportsclove@gmail.com"])

        if send_email:
            email_status, email_err = self._send_email_smtp(
                pdf_path=output_pdf_path,
                summary=summary_data,
                date_str=date_str,
                timeframe_str=time_window,
                shift_label=shift_label,
                recipients=recipients,
            )

        # Store record in SQLite reports table
        report_id = None
        try:
            cur = conn.execute("""
                INSERT INTO reports (
                    title, shift_type, period_start, period_end, generated_at,
                    pdf_path, summary_json, email_status, email_recipients, error_message
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                title,
                shift_label,
                p_start,
                p_end,
                now_ts,
                str(output_pdf_path.resolve()),
                json.dumps(summary_data),
                email_status,
                ", ".join(recipients) if recipients else "",
                email_err,
            ))
            conn.commit()
            report_id = cur.lastrowid
        except Exception as exc:
            error_tracker.capture_exception(
                exc,
                component="ReportingService",
                effect="Failed to record shift report in SQLite",
                severity="WARNING",
            )

        # Enforce rolling 69-report buffer
        self._enforce_rolling_buffer()

        log_system_event(
            event_type="ShiftReportGenerated",
            reason=f"{shift_label} Shift Report Compiled & Dispatched",
            uptime_sec=0,
            details={
                "Report ID": report_id,
                "PDF": pdf_filename,
                "Email Status": email_status,
                "Recipients": ", ".join(recipients),
                "Total Analyses": stats["total_analyses"],
                "High Incidents": stats["high_count"],
            },
            action_required="None. Report available in Dashboard.",
        )

        return {
            "id": report_id,
            "status": "ok",
            "shift": shift_label,
            "pdf_path": str(output_pdf_path),
            "pdf_name": pdf_filename,
            "email_status": email_status,
            "summary": summary_data,
        }

    # ── Email Transport (Gmail SMTP) ───────────────────────────

    def _send_email_smtp(
        self,
        pdf_path: Path,
        summary: dict,
        date_str: str,
        timeframe_str: str,
        shift_label: str,
        recipients: list[str],
    ) -> Tuple[str, Optional[str]]:
        if not recipients:
            return "DISABLED", "No recipient emails configured"

        sys_config = self._get_sys_config()
        sender_email = sys_config.get("report_sender_email", "reportsclove@gmail.com")
        sender_password = sys_config.get("report_sender_password", "wakl rpps mvql eznn")

        if not sender_password:
            return "FAILED", "Sender password not configured in system.json"

        try:
            stats = summary.get("stats", {})
            highlights = summary.get("exec_highlights", [])
            high_cnt = stats.get("high_count", 0)
            med_cnt = stats.get("med_count", 0)

            exec_bullets_html = "".join(
                f"<li style='margin-bottom: 8px; color: #334155; line-height: 1.4;'><strong style='color: #0f172a;'>{h.split(':')[0]}:</strong> {h.split(':', 1)[1] if ':' in h else h}</li>"
                for h in highlights
            ) or "<li style='color: #64748b;'>Routine baseline surveillance maintained across all active camera zones.</li>"

            safety_html = (
                "<li style='color: #16a34a; font-weight: 600;'>✔ Zero high-severity security or perimeter breaches detected during this shift.</li>"
                if high_cnt == 0
                else f"<li style='color: #dc2626; font-weight: 700;'>🚨 {high_cnt} high-severity anomaly event(s) recorded. Immediate review recommended.</li>"
            )

            html_body = f"""<!DOCTYPE html>
<html>
<head><meta charset='utf-8'></head>
<body style='margin: 0; padding: 20px 0; background-color: #f1f5f9; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;'>
    <div style='max-width: 620px; margin: 0 auto; background-color: #ffffff; border-radius: 10px; overflow: hidden; border: 1px solid #cbd5e1;'>
        <div style='background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%); padding: 24px 22px; color: #ffffff;'>
            <div style='font-size: 10px; font-weight: 700; color: #38bdf8; text-transform: uppercase; letter-spacing: 1px;'>CLOVE HQ SURVEILLANCE • RAPIDALERT</div>
            <h1 style='margin: 6px 0 10px 0; font-size: 18px; font-weight: 800; color: #ffffff;'>{shift_label} Site Intelligence Report</h1>
            <div style='font-size: 11px; color: #94a3b8;'>
                <span style='background: rgba(255,255,255,0.12); color: #f8fafc; padding: 3px 8px; border-radius: 4px; margin-right: 6px;'>📅 {date_str}</span>
                <span style='background: rgba(56,189,248,0.2); color: #38bdf8; padding: 3px 8px; border-radius: 4px;'>⏱ {timeframe_str}</span>
            </div>
        </div>
        <div style='padding: 20px 22px;'>
            <table width='100%' cellspacing='0' cellpadding='0' style='margin-bottom: 20px;'>
                <tr>
                    <td align='center' style='padding: 10px; background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 6px; width: 25%;'>
                        <div style='font-size: 18px; font-weight: 800; color: #0f172a;'>{stats.get('total_analyses', 0)}</div>
                        <div style='font-size: 9px; font-weight: 700; color: #64748b; margin-top: 2px;'>ANALYSES</div>
                    </td>
                    <td style='width: 6px;'></td>
                    <td align='center' style='padding: 10px; background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 6px; width: 25%;'>
                        <div style='font-size: 18px; font-weight: 800; color: {"#dc2626" if high_cnt > 0 else "#16a34a"};'>{high_cnt}</div>
                        <div style='font-size: 9px; font-weight: 700; color: #64748b; margin-top: 2px;'>HIGH SEV</div>
                    </td>
                    <td style='width: 6px;'></td>
                    <td align='center' style='padding: 10px; background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 6px; width: 25%;'>
                        <div style='font-size: 18px; font-weight: 800; color: #d97706;'>{med_cnt}</div>
                        <div style='font-size: 9px; font-weight: 700; color: #64748b; margin-top: 2px;'>MEDIUM</div>
                    </td>
                    <td style='width: 6px;'></td>
                    <td align='center' style='padding: 10px; background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 6px; width: 25%;'>
                        <div style='font-size: 18px; font-weight: 800; color: #16a34a;'>{stats.get('active_cameras_count', 0)}</div>
                        <div style='font-size: 9px; font-weight: 700; color: #64748b; margin-top: 2px;'>FEEDS ACTIVE</div>
                    </td>
                </tr>
            </table>

            <div style='margin-bottom: 18px;'>
                <div style='font-size: 11px; font-weight: 700; color: #0f172a; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 8px; border-bottom: 2px solid #3b82f6; padding-bottom: 4px; display: inline-block;'>
                    📌 Shift Highlights & Zone Activity
                </div>
                <ul style='margin: 0; padding-left: 18px; font-size: 12px;'>
                    {exec_bullets_html}
                </ul>
            </div>

            <div style='margin-bottom: 18px;'>
                <div style='font-size: 11px; font-weight: 700; color: #0f172a; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 8px; border-bottom: 2px solid #10b981; padding-bottom: 4px; display: inline-block;'>
                    🛡️ Safety Compliance Status
                </div>
                <ul style='margin: 0; padding-left: 18px; font-size: 12px;'>
                    {safety_html}
                </ul>
            </div>

            <div style='padding: 12px 14px; background-color: #f8fafc; border: 1px dashed #cbd5e1; border-radius: 6px; font-size: 11px; color: #64748b;'>
                📎 The comprehensive PDF report containing detailed zone metrics, vehicle counts, and visual evidence thumbnails is attached to this email.
            </div>
        </div>
        <div style='background: #f8fafc; padding: 12px 22px; border-top: 1px solid #e2e8f0; font-size: 10px; color: #94a3b8; text-align: center;'>
            Generated automatically by RapidAlert AI Surveillance Engine • Jetson Thor Platform
        </div>
    </div>
</body>
</html>"""

            msg = EmailMessage()
            msg["Subject"] = f"[RapidAlert] {shift_label} Site Intelligence Report — {date_str}"
            msg["From"] = sender_email
            msg["To"] = ", ".join(recipients)
            msg.set_content("Please review the attached RapidAlert Site Intelligence Shift Report.")
            msg.add_alternative(html_body, subtype="html")

            with open(pdf_path, "rb") as f:
                pdf_bytes = f.read()

            msg.add_attachment(
                pdf_bytes,
                maintype="application",
                subtype="pdf",
                filename=pdf_path.name,
            )

            with smtplib.SMTP("smtp.gmail.com", 587, timeout=30) as smtp:
                smtp.ehlo()
                smtp.starttls()
                smtp.ehlo()
                smtp.login(sender_email, sender_password)
                smtp.send_message(msg)

            print(f"[ReportService] 📧 Email successfully dispatched to: {recipients}")
            return "SENT", None
        except Exception as exc:
            err_msg = str(exc)
            error_tracker.capture_exception(
                exc,
                component="ReportingService",
                effect=f"Failed to send shift report email to {recipients}",
                severity="WARNING",
            )
            print(f"[ReportService] ❌ Email dispatch failed: {err_msg}")
            return "FAILED", err_msg

    # ── Background Scheduler Loop ──────────────────────────────

    async def _scheduler_loop(self) -> None:
        """
        Monitors system clock and triggers automated shift reports at 06:00 and 18:00 IST.
        """
        print("[ReportService] ⏰ Shift report scheduler started (Scheduled triggers: 06:00 AM & 06:00 PM IST)")
        while self._running:
            try:
                now = datetime.now()
                # Target hours: 6 (06:00) and 18 (18:00)
                if now.hour in (6, 18) and now.minute == 0:
                    slot_id = f"{now.strftime('%Y%m%d')}_{now.hour:02d}"
                    if self._last_scheduled_slot != slot_id:
                        self._last_scheduled_slot = slot_id
                        shift_name = "NIGHT" if now.hour == 6 else "DAY"
                        print(f"[ReportService] 🚀 Triggering automated {shift_name} Shift Report ({slot_id})...")
                        
                        # Run in background executor to avoid blocking event loop
                        loop = asyncio.get_running_loop()
                        await loop.run_in_executor(
                            None,
                            self.generate_shift_report,
                            shift_name,
                            None,
                            None,
                            True,
                        )
                await asyncio.sleep(20)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                error_tracker.capture_exception(
                    exc,
                    component="ReportingService",
                    effect="Error in shift report background scheduler loop",
                    severity="WARNING",
                )
                await asyncio.sleep(30)

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._scheduler_loop())

    def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()

    def get_reports_list(self, limit: int = 100) -> list[dict]:
        conn = self._conn()
        rows = conn.execute("""
            SELECT id, title, shift_type, period_start, period_end, generated_at,
                   pdf_path, summary_json, email_status, email_recipients, error_message
            FROM reports
            ORDER BY generated_at DESC
            LIMIT ?
        """, (limit,)).fetchall()

        out = []
        for r in rows:
            pdf_p = Path(r["pdf_path"])
            sum_data = {}
            try:
                sum_data = json.loads(r["summary_json"])
            except Exception:
                pass
            
            p_start_str = datetime.fromtimestamp(r["period_start"]).strftime("%H:%M")
            p_end_str = datetime.fromtimestamp(r["period_end"]).strftime("%H:%M")
            time_frame = f"{p_start_str} – {p_end_str} IST"

            out.append({
                "id": r["id"],
                "title": r["title"],
                "shift_type": r["shift_type"],
                "period_start": r["period_start"],
                "period_end": r["period_end"],
                "time_frame": time_frame,
                "generated_at": r["generated_at"],
                "generated_at_str": datetime.fromtimestamp(r["generated_at"]).strftime("%b %d, %Y %H:%M"),
                "pdf_name": pdf_p.name,
                "pdf_exists": pdf_p.exists(),
                "file_size_kb": round(pdf_p.stat().st_size / 1024, 1) if pdf_p.exists() else 0,
                "email_status": r["email_status"],
                "email_recipients": r["email_recipients"],
                "summary": sum_data,
                "error_message": r["error_message"],
            })
        return out

    def get_buffer_stats(self) -> dict:
        conn = self._conn()
        row = conn.execute("SELECT COUNT(*) AS count FROM reports").fetchone()
        stored_count = row["count"] if row else 0

        # Calculate approximate total disk space
        total_bytes = 0
        try:
            for p in self.reports_dir.glob("*.pdf"):
                total_bytes += p.stat().st_size
        except Exception:
            pass

        return {
            "stored_count": stored_count,
            "max_buffer": MAX_REPORTS_BUFFER,
            "buffer_coverage_days": round(MAX_REPORTS_BUFFER / 2, 1),
            "disk_usage_mb": round(total_bytes / (1024 * 1024), 2),
            "average_pdf_kb": round((total_bytes / 1024) / max(1, stored_count), 1),
        }
