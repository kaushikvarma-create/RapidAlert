"""
StorageManager: persists VLM analysis results to SQLite.
Each row = one analysis result for one camera.
Provides querying by camera, time range, severity, and safety.
"""
import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Optional


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
        conn = self._conn()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS analyses (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                cam         TEXT    NOT NULL,
                ts          REAL    NOT NULL,
                observation TEXT,
                activity    TEXT,
                workers     TEXT,
                machinery   TEXT,
                safety      TEXT,
                severity    TEXT,
                latency     REAL,
                raw         TEXT,
                error       INTEGER DEFAULT 0
            );

            CREATE INDEX IF NOT EXISTS idx_analyses_cam    ON analyses(cam);
            CREATE INDEX IF NOT EXISTS idx_analyses_ts     ON analyses(ts);
            CREATE INDEX IF NOT EXISTS idx_analyses_severity ON analyses(severity);
            CREATE INDEX IF NOT EXISTS idx_analyses_safety   ON analyses(safety);

            CREATE TABLE IF NOT EXISTS meta (
                key   TEXT PRIMARY KEY,
                value TEXT
            );
        """)
        # Add e2e_latency column if not present
        try:
            conn.execute("ALTER TABLE analyses ADD COLUMN e2e_latency REAL")
            conn.commit()
        except Exception:
            pass

        # Store DB version
        conn.execute(
            "INSERT OR IGNORE INTO meta (key, value) VALUES ('db_version', ?)",
            (str(self.DB_VERSION),),
        )
        conn.commit()

    # ── Write ────────────────────────────────────────────────────
    def save(self, result: dict, latency: float = 0.0, e2e_latency: Optional[float] = None) -> int:
        """Insert one analysis result. Returns new row id."""
        conn = self._conn()
        ts = result.get("ts") or time.time()
        e2e = e2e_latency if e2e_latency is not None else result.get("e2e_latency")
        cur = conn.execute(
            """
            INSERT INTO analyses
              (cam, ts, observation, activity, workers, machinery,
               safety, severity, latency, e2e_latency, error)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                result.get("cam", ""),
                ts,
                result.get("observation", ""),
                result.get("activity", "UNKNOWN"),
                result.get("workers", "0"),
                result.get("machinery", "None"),
                result.get("safety", "UNKNOWN"),
                result.get("severity", "LOW"),
                round(latency, 3),
                round(e2e, 3) if e2e is not None else None,
                1 if result.get("error") else 0,
            ),
        )
        conn.commit()
        return cur.lastrowid

    # ── Read ─────────────────────────────────────────────────────
    def query(
        self,
        cam: Optional[str] = None,
        since_ts: Optional[float] = None,
        until_ts: Optional[float] = None,
        severity: Optional[str] = None,
        safety: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict]:
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

        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        params.extend([limit, offset])
        sql = f"SELECT * FROM analyses {where} ORDER BY ts DESC LIMIT ? OFFSET ?"
        conn = self._conn()
        rows = conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]

    def get_latest_per_cam(self) -> list[dict]:
        """Latest analysis for every camera (for dashboard summary)."""
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

    def get_stats(self) -> dict:
        """Aggregate statistics across all stored analyses."""
        conn = self._conn()
        row = conn.execute("""
            SELECT
              COUNT(*)                                   AS total,
              COUNT(DISTINCT cam)                        AS cameras,
              SUM(CASE WHEN severity='HIGH'   THEN 1 ELSE 0 END) AS high_count,
              SUM(CASE WHEN severity='MEDIUM' THEN 1 ELSE 0 END) AS medium_count,
              SUM(CASE WHEN safety='DANGER'   THEN 1 ELSE 0 END) AS danger_count,
              AVG(latency)                               AS avg_latency,
              MIN(ts)                                    AS oldest_ts,
              MAX(ts)                                    AS newest_ts
            FROM analyses
        """).fetchone()
        return dict(row) if row else {}

    def get_camera_summary(self, cam: str, hours: float = 24) -> dict:
        """Per-camera stats for the last N hours."""
        since = time.time() - hours * 3600
        conn = self._conn()
        row = conn.execute("""
            SELECT
              COUNT(*)                                        AS total,
              SUM(CASE WHEN severity='HIGH'   THEN 1 ELSE 0 END) AS high,
              SUM(CASE WHEN severity='MEDIUM' THEN 1 ELSE 0 END) AS medium,
              SUM(CASE WHEN safety='DANGER'   THEN 1 ELSE 0 END) AS danger,
              AVG(latency)                                    AS avg_latency
            FROM analyses
            WHERE cam = ? AND ts >= ?
        """, (cam, since)).fetchone()
        return dict(row) if row else {}

    def count(self) -> int:
        conn = self._conn()
        return conn.execute("SELECT COUNT(*) FROM analyses").fetchone()[0]
