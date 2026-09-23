"""
StorageManager: persists VLM analysis results to SQLite.
Each row = one analysis result for one camera.
Provides querying by camera, time range, severity, and safety.
"""
from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Optional

from backend.core.error_tracker import error_tracker


class StorageManager:
    DB_VERSION = 1

    def __init__(self, db_path: Path):
        self.db_path = db_path
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()  # one connection per thread
        self._init_schema()
        print(f"[Storage] DB → {db_path}")

    # ── Connection (thread-local) ────────────────────────────────
    def _conn(self) -> sqlite3.Connection:
        if not hasattr(self._local, "conn") or self._local.conn is None:
            conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA cache_size=-8000")
            self._local.conn = conn
        return self._local.conn

    # ── Schema ───────────────────────────────────────────────────
    def _init_schema(self) -> None:
        try:
            conn = self._conn()
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS analyses (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    cam          TEXT    NOT NULL,
                    ts           REAL    NOT NULL,
                    observation  TEXT,
                    activity     TEXT,
                    workers      TEXT,
                    machinery    TEXT,
                    safety       TEXT,
                    severity     TEXT,
                    latency      REAL,
                    e2e_latency  REAL,
                    incident_id  TEXT,
                    parent_id    TEXT,
                    trigger_mode TEXT,
                    clip_path    TEXT,
                    keywords     TEXT,
                    threat_level TEXT,
                    confidence   REAL,
                    labels       TEXT,
                    raw          TEXT,
                    error        INTEGER DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS meta (
                    key   TEXT PRIMARY KEY,
                    value TEXT
                );
            """)

            # Migration: Add missing columns if database exists from earlier version
            cursor = conn.execute("PRAGMA table_info(analyses)")
            existing_cols = {row["name"] for row in cursor.fetchall()}
            schema_additions = {
                "e2e_latency": "REAL",
                "incident_id": "TEXT",
                "parent_id": "TEXT",
                "trigger_mode": "TEXT",
                "clip_path": "TEXT",
                "keywords": "TEXT",
                "threat_level": "TEXT",
                "confidence": "REAL",
                "labels": "TEXT",
            }
            for col_name, col_type in schema_additions.items():
                if col_name not in existing_cols:
                    try:
                        conn.execute(f"ALTER TABLE analyses ADD COLUMN {col_name} {col_type}")
                        conn.commit()
                    except sqlite3.OperationalError as op_err:
                        if "duplicate column name" not in str(op_err).lower():
                            error_tracker.capture_exception(
                                op_err,
                                component="StorageManager",
                                effect=f"Failed to add column {col_name} to analyses table",
                                severity="WARNING",
                            )

            # Create indexes after all columns are confirmed to exist
            conn.executescript("""
                CREATE INDEX IF NOT EXISTS idx_analyses_cam       ON analyses(cam);
                CREATE INDEX IF NOT EXISTS idx_analyses_ts        ON analyses(ts);
                CREATE INDEX IF NOT EXISTS idx_analyses_severity  ON analyses(severity);
                CREATE INDEX IF NOT EXISTS idx_analyses_safety    ON analyses(safety);
                CREATE INDEX IF NOT EXISTS idx_analyses_incident  ON analyses(incident_id);
                CREATE INDEX IF NOT EXISTS idx_analyses_trigger   ON analyses(trigger_mode);
                CREATE INDEX IF NOT EXISTS idx_analyses_cam_ts    ON analyses(cam, ts);
            """)

            # Store DB version
            conn.execute(
                "INSERT OR IGNORE INTO meta (key, value) VALUES ('db_version', ?)",
                (str(self.DB_VERSION),),
            )
            conn.commit()
        except Exception as exc:
            error_tracker.capture_exception(
                exc,
                component="StorageManager",
                effect="Failed to initialize SQLite schema; historical persistence may be disabled",
                severity="CRITICAL",
            )

    # ── Write ────────────────────────────────────────────────────
    def save(
        self,
        result: dict,
        latency: float = 0.0,
        e2e_latency: Optional[float] = None,
        incident_id: Optional[str] = None,
        parent_id: Optional[str] = None,
        trigger_mode: Optional[str] = None,
        clip_path: Optional[str] = None,
        keywords: Optional[str] = None,
        threat_level: Optional[str] = None,
        confidence: Optional[float] = None,
        labels: Optional[list | str] = None,
    ) -> Optional[int]:
        """Insert one analysis result. Returns new row id or None if failed."""
        cam = result.get("cam", "")
        try:
            conn = self._conn()
            ts = result.get("ts") or time.time()
            e2e = e2e_latency if e2e_latency is not None else result.get("e2e_latency")
            inc_id = incident_id or result.get("incident_id")
            p_id = parent_id or result.get("parent_id")
            t_mode = trigger_mode or result.get("trigger_mode")
            c_path = clip_path or result.get("clip_path")
            kw = keywords or (", ".join(result.get("keywords", [])) if isinstance(result.get("keywords"), list) else result.get("keywords", ""))
            t_level = threat_level or result.get("threat_level") or result.get("severity")
            conf = confidence if confidence is not None else result.get("confidence")
            lbls = labels if isinstance(labels, str) else (", ".join(labels) if isinstance(labels, list) else result.get("labels", ""))

            cur = conn.execute(
                """
                INSERT INTO analyses
                  (cam, ts, observation, activity, workers, machinery,
                   safety, severity, latency, e2e_latency, incident_id,
                   parent_id, trigger_mode, clip_path, keywords,
                   threat_level, confidence, labels, error)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    cam,
                    ts,
                    result.get("observation", ""),
                    result.get("activity", "UNKNOWN"),
                    result.get("workers", "0"),
                    result.get("machinery", "None"),
                    result.get("safety", "UNKNOWN"),
                    result.get("severity", "LOW"),
                    round(latency, 3),
                    round(e2e, 3) if e2e is not None else None,
                    inc_id,
                    p_id,
                    t_mode,
                    c_path,
                    kw,
                    t_level,
                    round(conf, 3) if conf is not None else None,
                    lbls,
                    1 if result.get("error") else 0,
                ),
            )
            conn.commit()
            return cur.lastrowid
        except Exception as exc:
            error_tracker.capture_exception(
                exc,
                component="StorageManager",
                camera=cam,
                effect=f"Failed to persist analysis to SQLite for camera {cam}; row discarded",
                severity="ERROR",
            )
            return None

    # ── Read ─────────────────────────────────────────────────────
    def query(
        self,
        cam: Optional[str] = None,
        since_ts: Optional[float] = None,
        until_ts: Optional[float] = None,
        severity: Optional[str] = None,
        safety: Optional[str] = None,
        search: Optional[str] = None,
        incident_id: Optional[str] = None,
        trigger_mode: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict]:
        try:
            clauses = []
            params: list = []

            if cam:
                clauses.append("cam = ?")
                params.append(cam)
            if since_ts is not None:
                clauses.append("ts >= ?")
                params.append(since_ts)
            if until_ts is not None:
                clauses.append("ts <= ?")
                params.append(until_ts)
            if severity:
                clauses.append("severity = ?")
                params.append(severity.upper())
            if safety:
                clauses.append("safety = ?")
                params.append(safety.upper())
            if incident_id:
                clauses.append("incident_id = ?")
                params.append(incident_id)
            if trigger_mode:
                clauses.append("trigger_mode = ?")
                params.append(trigger_mode.upper())
            if search and search.strip():
                term = f"%{search.strip()}%"
                clauses.append(
                    "(observation LIKE ? OR activity LIKE ? OR machinery LIKE ? OR keywords LIKE ? OR incident_id LIKE ? OR labels LIKE ?)"
                )
                params.extend([term, term, term, term, term, term])

            where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
            params.extend([limit, offset])
            sql = f"SELECT * FROM analyses {where} ORDER BY ts DESC LIMIT ? OFFSET ?"
            conn = self._conn()
            rows = conn.execute(sql, params).fetchall()
            return [dict(r) for r in rows]
        except Exception as exc:
            error_tracker.capture_exception(
                exc,
                component="StorageManager",
                camera=cam,
                effect="Failed to query historical analysis rows from SQLite",
                severity="WARNING",
            )
            return []

    def get_latest_per_cam(self) -> list[dict]:
        """Latest analysis for every camera (for dashboard summary)."""
        try:
            conn = self._conn()
            rows = conn.execute("""
                SELECT a.*
                FROM analyses a
                INNER JOIN (
                    SELECT cam, MAX(ts) AS max_ts FROM analyses GROUP BY cam
                ) b ON a.cam = b.cam AND a.ts = b.max_ts
                ORDER BY a.cam
            """).fetchall()
            return [dict(r) for r in rows]
        except Exception as exc:
            error_tracker.capture_exception(
                exc,
                component="StorageManager",
                effect="Failed to query latest analysis per camera from SQLite",
                severity="WARNING",
            )
            return []

    def get_stats(self) -> dict:
        """Aggregate statistics across all stored analyses."""
        try:
            conn = self._conn()
            row = conn.execute("""
                SELECT
                  COUNT(*)                                           AS total,
                  COUNT(DISTINCT cam)                                AS cameras,
                  SUM(CASE WHEN severity='HIGH'   THEN 1 ELSE 0 END) AS high_count,
                  SUM(CASE WHEN severity='MEDIUM' THEN 1 ELSE 0 END) AS medium_count,
                  SUM(CASE WHEN safety='DANGER'   THEN 1 ELSE 0 END) AS danger_count,
                  AVG(latency)                                       AS avg_latency,
                  MIN(ts)                                            AS oldest_ts,
                  MAX(ts)                                            AS newest_ts
                FROM analyses
            """).fetchone()
            return dict(row) if row else {}
        except Exception as exc:
            error_tracker.capture_exception(
                exc,
                component="StorageManager",
                effect="Failed to query aggregate statistics from SQLite",
                severity="WARNING",
            )
            return {}

    def get_camera_summary(self, cam: str, hours: float = 24) -> dict:
        """Per-camera stats for the last N hours."""
        try:
            since = time.time() - hours * 3600
            conn = self._conn()
            row = conn.execute("""
                SELECT
                  COUNT(*)                                           AS total,
                  SUM(CASE WHEN severity='HIGH'   THEN 1 ELSE 0 END) AS high,
                  SUM(CASE WHEN severity='MEDIUM' THEN 1 ELSE 0 END) AS medium,
                  SUM(CASE WHEN safety='DANGER'   THEN 1 ELSE 0 END) AS danger,
                  AVG(latency)                                       AS avg_latency
                FROM analyses
                WHERE cam = ? AND ts >= ?
            """, (cam, since)).fetchone()
            return dict(row) if row else {}
        except Exception as exc:
            error_tracker.capture_exception(
                exc,
                component="StorageManager",
                camera=cam,
                effect=f"Failed to query summary for camera {cam} from SQLite",
                severity="WARNING",
            )
            return {}

    def count(self) -> int:
        try:
            conn = self._conn()
            return conn.execute("SELECT COUNT(*) FROM analyses").fetchone()[0]
        except Exception as exc:
            error_tracker.capture_exception(
                exc,
                component="StorageManager",
                effect="Failed to count analyses rows in SQLite",
                severity="WARNING",
            )
            return 0

    def search(
        self,
        query: Optional[str] = None,
        cam: Optional[str] = None,
        verdict: Optional[str] = None,
        severity: Optional[str] = None,
        safety: Optional[str] = None,
        tier: Optional[int] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict]:
        """Keyword and metadata search across stored analyses."""
        return self.query(
            cam=cam,
            severity=severity or verdict,
            safety=safety,
            search=query,
            limit=limit,
            offset=offset,
        )

    def prune_expired_clips(self, retention_hours: float) -> int:
        """
        Deletes video clip files on disk and nullifies clip_path in DB
        for incidents older than retention_hours.
        """
        if retention_hours <= 0:
            return 0
        cutoff_ts = time.time() - (retention_hours * 3600.0)
        pruned_count = 0
        try:
            conn = self._conn()
            rows = conn.execute(
                "SELECT id, clip_path FROM analyses WHERE clip_path IS NOT NULL AND ts < ?",
                (cutoff_ts,),
            ).fetchall()

            for r in rows:
                row_id = r["id"]
                clip_path_str = r["clip_path"]
                if clip_path_str:
                    try:
                        p = Path(clip_path_str)
                        if not p.is_absolute():
                            p = Path("data") / clip_path_str
                        if p.exists():
                            p.unlink()
                    except Exception as e:
                        pass
                conn.execute("UPDATE analyses SET clip_path = NULL WHERE id = ?", (row_id,))
                pruned_count += 1

            if pruned_count > 0:
                conn.commit()
                print(f"[Storage] 🧹 Pruned {pruned_count} expired video clips (retention: {retention_hours}h)")
        except Exception as exc:
            error_tracker.capture_exception(
                exc,
                component="StorageManager",
                effect=f"Failed to prune expired video clips older than {retention_hours}h",
                severity="WARNING",
            )
        return pruned_count

