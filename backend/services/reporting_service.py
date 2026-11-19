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

from backend.core.config import DATABASE_PATH, SYSTEM_CONFIG_PATH, CAMERAS_CONFIG_PATH, DEFAULT_VLM_MODEL
from backend.core.error_tracker import error_tracker
from backend.core.shutdown_logger import log_system_event

# Reports are emailed on three cadences (see REPORT_SLOT_HOURS below): every 6h, every 12h and
# every 24h = 7 reports a day. 210 keeps about a month of them.
MAX_REPORTS_BUFFER = 210
REPORTS_PER_DAY = 7

# Report clock: a slot every 6 hours. At each slot every window whose length divides the slot
# hour (counted from 06:00) is due, so: 6h report at 00/06/12/18, 12h report at 06/18, 24h at 06.
REPORT_SLOT_HOURS = (0, 6, 12, 18)
REPORT_ANCHOR_HOUR = 6
DEFAULT_REPORT_WINDOWS_HOURS = (6, 12, 24)
REPORT_CATCHUP_MIN = 10   # a slot can still be served this many minutes late (e.g. after a restart)

# Report branding (all overridable in system.json: report_site_name, report_provider_name,
# report_title, report_site_logo, report_provider_logo).
DEFAULT_SITE_NAME = "Alekhya Rise"
DEFAULT_PROVIDER_NAME = "Clove Technologies"
ASSETS_DIR = Path(__file__).resolve().parent.parent / "templates" / "assets"


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

    def _get_vlm_model(self) -> str:
        cfg = self._get_sys_config()
        return cfg.get("vllm_model") or cfg.get("primary_model") or DEFAULT_VLM_MODEL

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

        # Goes through _llm_text, which turns the model's "thinking" output off. The direct call
        # used before did not, so with a 60-token limit the report printed the start of the
        # model's reasoning ("Thinking Process: 1. Analyze the Request...") as the camera summary.
        text = self._llm_text(prompt, max_tokens=80, timeout=15.0).replace('"', "").strip()
        if text and not text.lower().startswith("thinking"):
            return text
        return "Standard operational movement and routine zone monitoring observed."

    def _llm_text(self, prompt: str, max_tokens: int = 260, timeout: float = 45.0) -> str:
        """Plain text completion against the first configured vLLM endpoint that answers."""
        import requests
        cfg = self._get_sys_config()
        urls = [e.get("url") for e in cfg.get("vllm_endpoints", []) if e.get("url")] or ["http://localhost:8000"]
        for url in urls:
            try:
                resp = requests.post(
                    f"{url}/v1/chat/completions",
                    json={
                        "model": self._get_vlm_model(),
                        "messages": [{"role": "user", "content": prompt}],
                        "temperature": 0.2,
                        "max_tokens": max_tokens,
                        "chat_template_kwargs": {"enable_thinking": False},
                    },
                    timeout=timeout,
                )
                if resp.ok:
                    text = (resp.json()["choices"][0]["message"]["content"] or "").strip()
                    if text:
                        return text
            except Exception:
                continue
        return ""

    def _get_written_summary(
        self,
        period_label: str,
        time_window: str,
        stats: dict,
        notable_events: list[dict],
        routines: Dict[str, str],
        total_counts: Dict[str, int],
    ) -> str:
        """
        One written paragraph for the whole reporting period, built only from what was recorded.
        Falls back to a plain factual paragraph if the model cannot be reached.
        """
        busiest = sorted(total_counts.items(), key=lambda kv: -kv[1])[:3]
        quiet = [c for c, n in total_counts.items() if n == 0]
        fallback_parts = [
            f"During {time_window}, {stats['total_analyses']} scene analyses were recorded across "
            f"{stats['active_cameras_count']} of {stats['total_cameras_count']} cameras.",
            (f"{stats['high_count']} high-severity and {stats['med_count']} medium-severity event(s) were flagged."
             if (stats['high_count'] or stats['med_count'])
             else "No high- or medium-severity events were flagged."),
        ]
        if busiest and busiest[0][1] > 0:
            fallback_parts.append("Most activity was recorded on " + ", ".join(c for c, n in busiest if n > 0) + ".")
        if quiet:
            fallback_parts.append(f"{len(quiet)} camera(s) recorded no analyses in this period: " + ", ".join(quiet[:6]) + ".")
        fallback = " ".join(fallback_parts)

        event_lines = [f"- {e['ts']} {e['cam']} [{e['sev']}]: {e['obs'][:220]}" for e in notable_events[:12]]
        cam_lines = [
            f"- {c}: {total_counts.get(c, 0)} analyses. {routines.get(c, '')[:200]}"
            for c, _ in sorted(total_counts.items(), key=lambda kv: -kv[1])[:20]
        ]
        prompt = (
            "You are writing the summary paragraph of an automated CCTV monitoring report.\n"
            f"Reporting period: {period_label}, {time_window}.\n"
            f"Totals: {stats['total_analyses']} analyses; {stats['high_count']} high-severity events; "
            f"{stats['med_count']} medium-severity events; {stats['active_cameras_count']} of "
            f"{stats['total_cameras_count']} cameras reporting.\n\n"
            "Flagged events (time, camera, severity, observation):\n"
            + ("\n".join(event_lines) if event_lines else "- none")
            + "\n\nPer-camera activity:\n"
            + ("\n".join(cam_lines) if cam_lines else "- none")
            + "\n\nWrite 4 to 6 plain sentences. State the overall level of activity, describe each flagged "
            "event with its camera and time (or say clearly that there were none), and name the busiest and "
            "the quiet cameras. Use only the information above; do not add causes, advice or details that are "
            "not listed. No headings, no bullet points, no greeting."
        )
        text = self._llm_text(prompt, max_tokens=320).replace("\n", " ").strip()
        return text if len(text) > 40 else fallback

    def _get_camera_key_frames(
        self,
        conn: sqlite3.Connection,
        cam_name: str,
        p_start: float,
        p_end: float,
        evidence_tmp_dir: Path,
        cam_events: list[dict],
    ) -> list[dict]:
        """
        Pulls up to 2 key visual frames for a specific camera:
        - Prioritizes Alert / Incident frames (HIGH/MEDIUM severity, safety warning/danger).
        - If clean routine shift, pulls 2 routine/checkpoint frames across the shift.
        - Tags each image with an explicit badge (e.g. '🚨 ALERT FRAME (10:15:22)' vs '📷 ROUTINE CHECKPOINT (07:00:00)').
        """
        key_frames = []
        seen_ts = []

        # 1. First priority: Check alert events for this camera
        alert_events = [e for e in cam_events if e.get("sev") in ("HIGH", "MEDIUM")]
        alert_events.sort(key=lambda x: (0 if x.get("sev") == "HIGH" else 1, x["mtime"]))

        for ev in alert_events:
            if len(key_frames) >= 2:
                break
            if any(abs(ev["mtime"] - t) < 10.0 for t in seen_ts):
                continue

            img_data = None
            if ev.get("img_path") and Path(ev["img_path"]).exists():
                key_frames.append({
                    "img_path": ev["img_path"],
                    "badge_text": f"ALERT FRAME ({ev['ts']})",
                    "badge_class": "badge-alert",
                    "caption": (ev["obs"][:65] + "...") if len(ev["obs"]) > 65 else ev["obs"],
                    "time_str": ev["ts"],
                })
                seen_ts.append(ev["mtime"])
                continue

            if ev.get("incident_id"):
                f_row = conn.execute("SELECT frame_data FROM incident_frames WHERE incident_id = ? LIMIT 1", (ev["incident_id"],)).fetchone()
                if f_row and f_row["frame_data"]:
                    img_data = f_row["frame_data"]

            if not img_data:
                f_row = conn.execute("SELECT frame_data FROM incident_frames WHERE cam = ? AND ABS(ts - ?) < 6.0 LIMIT 1", (cam_name, ev["mtime"])).fetchone()
                if f_row and f_row["frame_data"]:
                    img_data = f_row["frame_data"]

            if img_data:
                img_file = evidence_tmp_dir / f"kf_alert_{cam_name}_{int(ev['mtime'])}.jpg"
                with open(img_file, "wb") as f:
                    f.write(img_data)
                key_frames.append({
                    "img_path": str(img_file.resolve()),
                    "badge_text": f"ALERT FRAME ({ev['ts']})",
                    "badge_class": "badge-alert",
                    "caption": (ev["obs"][:65] + "...") if len(ev["obs"]) > 65 else ev["obs"],
                    "time_str": ev["ts"],
                })
                seen_ts.append(ev["mtime"])

        # 2. Check incident_frames table for any frames during shift period
        if len(key_frames) < 2:
            rows = conn.execute("""
                SELECT frame_data, ts FROM incident_frames
                WHERE cam = ? AND ts >= ? AND ts <= ?
                ORDER BY ts ASC
            """, (cam_name, p_start, p_end)).fetchall()

            if rows:
                candidates = [rows[0]]
                if len(rows) > 1:
                    candidates.append(rows[-1])
                for r in candidates:
                    if len(key_frames) >= 2:
                        break
                    ts_val = r["ts"]
                    if any(abs(ts_val - t) < 15.0 for t in seen_ts):
                        continue
                    ts_str = datetime.fromtimestamp(ts_val).strftime("%H:%M:%S")
                    img_file = evidence_tmp_dir / f"kf_chk_{cam_name}_{int(ts_val)}.jpg"
                    with open(img_file, "wb") as f:
                        f.write(r["frame_data"])
                    key_frames.append({
                        "img_path": str(img_file.resolve()),
                        "badge_text": f"ROUTINE CHECKPOINT ({ts_str})",
                        "badge_class": "badge-routine",
                        "caption": "Normal operational zone status",
                        "time_str": ts_str,
                    })
                    seen_ts.append(ts_val)

        # 3. Fallback to FrameStore cached entry / live snapshot if still under 2 frames
        if len(key_frames) < 2 and self.frame_store:
            entry = self.frame_store.get_latest_cached_entry(cam_name)
            if entry:
                try:
                    ts_val, b64_snap = entry
                    img_data = base64.b64decode(b64_snap)
                    ts_str = datetime.fromtimestamp(ts_val).strftime("%H:%M:%S")
                    img_file = evidence_tmp_dir / f"kf_live_{cam_name}_{int(ts_val)}.jpg"
                    with open(img_file, "wb") as f:
                        f.write(img_data)
                    key_frames.append({
                        "img_path": str(img_file.resolve()),
                        "badge_text": f"BASELINE MONITOR ({ts_str})",
                        "badge_class": "badge-routine",
                        "caption": "Active zone baseline surveillance",
                        "time_str": ts_str,
                    })
                except Exception:
                    pass

        return key_frames

    # ── Report Generation ───────────────────────────────────────

    def generate_shift_report(
        self,
        shift_type: str = "AUTO",
        custom_start: Optional[float] = None,
        custom_end: Optional[float] = None,
        send_email: bool = True,
        label: Optional[str] = None,
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
            shift_label = label or "CUSTOM"
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

        if start_dt.date() == end_dt.date():
            time_window = f"{start_dt.strftime('%H:%M')} – {end_dt.strftime('%H:%M')} IST"
        else:  # window crosses midnight: show both dates so a 24h report is unambiguous
            time_window = f"{start_dt.strftime('%d %b %H:%M')} – {end_dt.strftime('%d %b %H:%M')} IST"
        date_str = start_dt.strftime("%d %b %Y")

        conn = self._conn()

        # Fetch active camera list
        # Cameras in the report = cameras configured now + cameras that reported in this period.
        # (It used to be a fixed list of seven office camera names plus every camera that had
        # ever produced an analysis, so a different site would be reported as mostly dead feeds.)
        configured_cams: list[str] = []
        zone_by_cam: Dict[str, str] = {}
        try:
            with open(CAMERAS_CONFIG_PATH, "r", encoding="utf-8") as f:
                for c in json.load(f):
                    if not c.get("name") or not c.get("enabled", True):
                        continue
                    configured_cams.append(c["name"])
                    # "Area" column: first sentence of the camera's own description, if it has one.
                    ctx = (c.get("normal_context") or "").strip().split(". ")[0].strip().rstrip(".")
                    zone_by_cam[c["name"]] = (ctx[:70] + "…") if len(ctx) > 70 else ctx
        except Exception:
            pass
        cam_rows = conn.execute(
            "SELECT DISTINCT cam FROM analyses WHERE ts >= ? AND ts <= ?", (p_start, p_end)
        ).fetchall()
        all_cams = sorted(set(configured_cams) | set(r[0] for r in cam_rows if r[0]))

        # Fetch analyses in period
        analyses_rows = conn.execute("""
            SELECT id, cam, ts, observation, activity, workers, machinery, safety, severity, incident_id, trigger_mode
            FROM analyses
            WHERE ts >= ? AND ts <= ?
            ORDER BY ts ASC
        """, (p_start, p_end)).fetchall()

        events_by_cam: Dict[str, list[dict]] = {c: [] for c in all_cams}
        peak_occ_by_cam: Dict[str, int] = {c: 0 for c in all_cams}
        active_counts: Dict[str, int] = {c: 0 for c in all_cams}
        total_counts: Dict[str, int] = {c: 0 for c in all_cams}
        person_counts: Dict[str, list[int]] = {c: [] for c in all_cams}   # per-check person counts
        motion_counts: Dict[str, int] = {c: 0 for c in all_cams}          # drift-triggered checks
        alert_counts: Dict[str, int] = {c: 0 for c in all_cams}           # MEDIUM / HIGH
        hour_events: Dict[int, int] = {}                                  # hour bucket -> motion events
        hour_alerts: Dict[int, int] = {}

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
                person_counts[c] = []
                motion_counts[c] = 0
                alert_counts[c] = 0

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
            person_counts[c].append(w_c)

            sev = (row["severity"] or "LOW").upper()
            mode = (row["trigger_mode"] or "PERIODIC").upper()
            hour_bucket = int((row["ts"] - p_start) // 3600)
            if mode == "TRIGGER":
                motion_counts[c] += 1
                hour_events[hour_bucket] = hour_events.get(hour_bucket, 0) + 1
            if sev in ("HIGH", "MEDIUM"):
                alert_counts[c] += 1
                hour_alerts[hour_bucket] = hour_alerts.get(hour_bucket, 0) + 1
            ev_item = {
                "id": row["id"],
                "ts": datetime.fromtimestamp(row["ts"]).strftime("%H:%M:%S"),
                "mtime": row["ts"],
                "cam": c,
                "type": {"TRIGGER": "Motion event", "FOLLOWUP": "Follow-up check"}.get(mode, "Routine check"),
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
                f_row = None
                if ev.get("incident_id"):
                    f_row = conn.execute("""
                        SELECT frame_data FROM incident_frames
                        WHERE incident_id = ?
                        LIMIT 1
                    """, (ev["incident_id"],)).fetchone()

                if not f_row:
                    f_row = conn.execute("""
                        SELECT frame_data FROM incident_frames
                        WHERE cam = ? AND ABS(ts - ?) < 5.0
                        ORDER BY ABS(ts - ?) ASC
                        LIMIT 1
                    """, (ev["cam"], ev["mtime"], ev["mtime"])).fetchone()

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

        # Per-camera figures. Nothing here depends on a camera's name, so the report works for
        # whatever cameras are configured. "Peak persons" is the 95th percentile of the per-check
        # person counts rather than the single largest number the model ever wrote (one miscount,
        # e.g. 87, used to set the figure). The Activity Rate and Vehicles columns were removed.
        def _p95(values: list[int]) -> int:
            if not values:
                return 0
            ordered = sorted(values)
            return ordered[min(len(ordered) - 1, int(len(ordered) * 0.95))]

        routines = {}
        zone_activity = []
        general_info = []
        peak_by_cam: Dict[str, int] = {}
        for c in all_cams:
            zone = zone_by_cam.get(c, "") or "Surveillance Zone"
            peak_by_cam[c] = _p95(person_counts.get(c, []))

            # Camera, zone, In, Out, peak persons. In/Out have no data source and stay blank.
            general_info.append([c, zone, "–", "–", str(peak_by_cam[c])])

            # One-sentence routine summary for this camera
            c_events = events_by_cam.get(c, [])
            rout_desc = self._get_routine_summary(c, c_events)
            routines[c] = rout_desc

            # Pull 2 key visual frames tagged per camera
            c_keyframes = self._get_camera_key_frames(conn, c, p_start, p_end, evidence_tmp_dir, c_events)
            zone_activity.append({
                "cam": c,
                "zone": zone,
                "summary": rout_desc,
                "peak_occ": peak_by_cam[c],
                "key_frames": c_keyframes,
            })

        # Overall Stats
        sanitation_shift_count = 0
        try:
            r = conn.execute(
                "SELECT COUNT(*) as cnt FROM watchdog_sanitation_log WHERE ts >= ? AND ts <= ?",
                (p_start, p_end),
            ).fetchone()
            sanitation_shift_count = r["cnt"] if r else 0
        except Exception:
            pass

        stats = {
            "total_analyses": len(analyses_rows),
            "high_count": len(high_events),
            "med_count": len(med_events),
            "peak_workers": max(peak_by_cam.values()) if peak_by_cam else 0,
            "motion_events": sum(motion_counts.values()),
            "active_cameras_count": sum(1 for c in all_cams if total_counts.get(c, 0) > 0),
            "total_cameras_count": len(all_cams),
            "sanitation_count": sanitation_shift_count,
        }

        written_summary = self._get_written_summary(
            shift_label, time_window, stats, selected_events, routines, total_counts
        )

        # Render PDF via Jinja2 + WeasyPrint
        template_file = Path(__file__).resolve().parent.parent / "templates" / "report_template.html"
        if not template_file.exists():
            raise FileNotFoundError(f"Report template not found at {template_file}")

        with open(template_file, "r") as f:
            template_str = f.read()

        env = Environment()
        template = env.from_string(template_str)

        sys_config = self._get_sys_config()
        site_name = sys_config.get("report_site_name", DEFAULT_SITE_NAME)
        provider_name = sys_config.get("report_provider_name", DEFAULT_PROVIDER_NAME)
        title = sys_config.get("report_title", "Site Monitoring Report")

        def _logo(key: str, default_file: str) -> str:
            path = Path(sys_config.get(key) or (ASSETS_DIR / default_file))
            return str(path.resolve()) if path.exists() else ""

        html_content = template.render(
            settings={
                "title": title,
                "site_name": site_name,
                "provider_name": provider_name,
                "site_logo": _logo("report_site_logo", "site_logo.png"),
                "provider_logo": _logo("report_provider_logo", "provider_logo.png"),
            },
            date_str=date_str,
            time_window=time_window,
            shift_name=shift_label,
            stats=stats,
            general_info=general_info,
            all_events=selected_events[:20],
            routines=routines,
            zone_activity=zone_activity,
            written_summary=written_summary,
        )

        site_slug = "".join(ch if ch.isalnum() else "_" for ch in site_name).strip("_") or "Site"
        pdf_filename = f"{site_slug}_Site_Report_{now.strftime('%Y%m%d_%H%M%S')}.pdf"
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
            "site_name": site_name,
            "provider_name": provider_name,
            "written_summary": written_summary,
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
            site_name = summary.get("site_name") or sys_config.get("report_site_name", DEFAULT_SITE_NAME)
            provider_name = summary.get("provider_name") or sys_config.get("report_provider_name", DEFAULT_PROVIDER_NAME)
            site_logo = Path(sys_config.get("report_site_logo") or (ASSETS_DIR / "site_logo.png"))
            provider_logo = Path(sys_config.get("report_provider_logo") or (ASSETS_DIR / "provider_logo.png"))
            site_logo_html = "<img src='cid:sitelogo' alt='' height='64' style='display:block;'>" if site_logo.exists() else ""
            provider_logo_html = (
                "<img src='cid:providerlogo' alt='' height='30' style='display:block; margin-left:auto;'>"
                if provider_logo.exists() else f"<b>{provider_name}</b>"
            )
            high_cnt = stats.get("high_count", 0)
            med_cnt = stats.get("med_count", 0)

            exec_bullets_html = "".join(
                f"<li style='margin-bottom: 8px; color: #334155; line-height: 1.4;'><strong style='color: #0f172a;'>{h.split(':')[0]}:</strong> {h.split(':', 1)[1] if ':' in h else h}</li>"
                for h in highlights
            ) or "<li style='color: #64748b;'>Routine baseline surveillance maintained across all active camera zones.</li>"

            import html as _html
            written_html = _html.escape(summary.get("written_summary") or "")

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
        <div style='padding: 14px 22px 12px 22px; background: #ffffff;'>
            <table width='100%' cellspacing='0' cellpadding='0'><tr>
                <td style='vertical-align: middle;'>{site_logo_html}</td>
                <td style='vertical-align: middle; text-align: right;'>
                    <div style='font-size: 8px; letter-spacing: 1px; color: #94a3b8; text-transform: uppercase; margin-bottom: 3px;'>Report by</div>
                    {provider_logo_html}
                </td>
            </tr></table>
        </div>
        <div style='background: linear-gradient(120deg, #1f2d1a 0%, #33502a 55%, #4f7d60 100%); padding: 22px 22px; color: #ffffff; border-bottom: 4px solid #d67b4c;'>
            <div style='font-size: 10px; font-weight: 700; color: #e6dc9a; text-transform: uppercase; letter-spacing: 1px;'>{site_name} • Site Monitoring</div>
            <h1 style='margin: 6px 0 10px 0; font-size: 18px; font-weight: 800; color: #ffffff;'>{shift_label} Site Report</h1>
            <div style='font-size: 11px; color: #cbd5c0;'>
                <span style='background: rgba(255,255,255,0.14); color: #f8fafc; padding: 3px 8px; border-radius: 4px; margin-right: 6px;'>{date_str}</span>
                <span style='background: rgba(214,123,76,0.3); color: #ffd2b8; padding: 3px 8px; border-radius: 4px;'>{timeframe_str}</span>
            </div>
        </div>
        <div style='padding: 20px 22px;'>
            <table width='100%' cellspacing='0' cellpadding='0' style='margin-bottom: 20px;'>
                <tr>
                    <td align='center' style='padding: 10px; background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 6px; width: 25%;'>
                        <div style='font-size: 18px; font-weight: 800; color: #0f172a;'>{stats.get('total_analyses', 0)}</div>
                        <div style='font-size: 9px; font-weight: 700; color: #64748b; margin-top: 2px;'>SCENE CHECKS</div>
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
                <div style='font-size: 11px; font-weight: 700; color: #0f172a; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 8px; border-bottom: 2px solid #ea6f3e; padding-bottom: 4px;'>
                    Summary of the period
                </div>
                <p style='margin: 0; font-size: 12.5px; line-height: 1.55; color: #1e293b;'>{written_html}</p>
            </div>

            <div style='margin-bottom: 18px;'>
                <div style='font-size: 11px; font-weight: 700; color: #0f172a; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 8px; border-bottom: 2px solid #3b82f6; padding-bottom: 4px; display: inline-block;'>
                    Highlights by camera
                </div>
                <ul style='margin: 0; padding-left: 18px; font-size: 12px;'>
                    {exec_bullets_html}
                </ul>
            </div>

            <div style='margin-bottom: 18px;'>
                <div style='font-size: 11px; font-weight: 700; color: #0f172a; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 8px; border-bottom: 2px solid #10b981; padding-bottom: 4px; display: inline-block;'>
                    Safety status
                </div>
                <ul style='margin: 0; padding-left: 18px; font-size: 12px;'>
                    {safety_html}
                </ul>
            </div>

            <div style='padding: 12px 14px; background-color: #f8fafc; border: 1px dashed #cbd5e1; border-radius: 6px; font-size: 11px; color: #64748b;'>
                The full PDF report, with per-camera figures and snapshots, is attached to this email.
            </div>
        </div>
        <div style='background: #f8fafc; padding: 12px 22px; border-top: 1px solid #e2e8f0; font-size: 10px; color: #94a3b8; text-align: center;'>
            {site_name} • Site Monitoring Report • {provider_name}
        </div>
    </div>
</body>
</html>"""

            msg = EmailMessage()
            msg["Subject"] = f"[{site_name}] {shift_label} Site Monitoring Report — {date_str}"
            msg["From"] = sender_email
            msg["To"] = ", ".join(recipients)
            msg.set_content(
                f"{site_name} site monitoring report for {timeframe_str}, {date_str}.\n\n"
                f"{summary.get('written_summary', '')}\n\nThe full report is attached as a PDF.\n\n{provider_name}"
            )
            msg.add_alternative(html_body, subtype="html")
            # Logos travel inside the email (referenced as cid:...), so they show without remote images.
            html_part = msg.get_payload()[1]
            for cid, logo in (("sitelogo", site_logo), ("providerlogo", provider_logo)):
                if logo.exists():
                    html_part.add_related(logo.read_bytes(), maintype="image", subtype="png", cid=f"<{cid}>")

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

    def _due_reports(self, now: datetime) -> list[tuple[str, float, float]]:
        """
        Reports due at the 6-hour slot `now` falls in, as (label, period_start, period_end).
        Slots are 00:00, 06:00, 12:00, 18:00. Every slot gets a 6-hour report; 06:00 and 18:00
        also get a 12-hour report; 06:00 also gets the 24-hour report. Which windows are sent is
        configurable with "report_windows_hours" in system.json (default [6, 12, 24]).
        """
        if now.hour not in REPORT_SLOT_HOURS or now.minute >= REPORT_CATCHUP_MIN:
            return []
        cfg = self._get_sys_config()
        if not cfg.get("reporting_enabled", True):
            return []
        try:
            windows = sorted({int(w) for w in cfg.get("report_windows_hours", DEFAULT_REPORT_WINDOWS_HOURS)})
        except (TypeError, ValueError):
            windows = list(DEFAULT_REPORT_WINDOWS_HOURS)
        slot_end = now.replace(minute=0, second=0, microsecond=0)
        due = []
        for hours in windows:
            if hours <= 0 or (now.hour - REPORT_ANCHOR_HOUR) % hours != 0:
                continue
            start = slot_end - timedelta(hours=hours)
            span = f"{start.strftime('%H:%M')} – {slot_end.strftime('%H:%M')}"
            if hours == 12:
                # Keeps the dashboard's existing Day / Night filters working for the 12-hour reports.
                label = f"{'NIGHT' if now.hour == 6 else 'DAY'} 12-HOUR ({span})"
            else:
                label = f"{hours}-HOUR ({span})"
            due.append((label, start.timestamp(), slot_end.timestamp()))
        return due

    def _already_reported(self, label: str, period_end: float) -> bool:
        """True if this exact report was already generated (survives restarts, unlike a flag)."""
        try:
            row = self._conn().execute(
                "SELECT 1 FROM reports WHERE shift_type = ? AND ABS(period_end - ?) < 60 LIMIT 1",
                (label, period_end),
            ).fetchone()
            return row is not None
        except Exception:
            return False

    def _run_due_reports(self, due: list[tuple[str, float, float]]) -> None:
        """Generate and email each due report, one after another (runs in a worker thread)."""
        for label, p_start, p_end in due:
            if self._already_reported(label, p_end):
                continue
            try:
                print(f"[ReportService] 🚀 Generating scheduled report: {label}")
                self.generate_shift_report("CUSTOM", p_start, p_end, True, label)
            except Exception as exc:
                error_tracker.capture_exception(
                    exc,
                    component="ReportingService",
                    effect=f"Scheduled report '{label}' failed",
                    severity="WARNING",
                )

    async def _scheduler_loop(self) -> None:
        """Checks the clock every 20s and sends the reports due at each 6-hour slot."""
        print("[ReportService] ⏰ Report scheduler started (6h at 00/06/12/18, 12h at 06/18, 24h at 06:00)")
        while self._running:
            try:
                due = self._due_reports(datetime.now())
                if due:
                    slot_id = f"{datetime.now().strftime('%Y%m%d_%H')}"
                    if self._last_scheduled_slot != slot_id:
                        self._last_scheduled_slot = slot_id
                        loop = asyncio.get_running_loop()
                        await loop.run_in_executor(None, self._run_due_reports, due)
                await asyncio.sleep(20)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                error_tracker.capture_exception(
                    exc,
                    component="ReportingService",
                    effect="Error in report scheduler loop",
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
            "buffer_coverage_days": round(MAX_REPORTS_BUFFER / REPORTS_PER_DAY, 1),
            "disk_usage_mb": round(total_bytes / (1024 * 1024), 2),
            "average_pdf_kb": round((total_bytes / 1024) / max(1, stored_count), 1),
        }
