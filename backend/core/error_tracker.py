"""
RapidAlert Structured Error & Exception Tracker
Eliminates silent error swallowing by systematically recording:
- What error occurred (type and message)
- Where it occurred (component, file, line, function, camera)
- Downstream operational effects on the surveillance pipeline
- Full stack trace for root-cause diagnosis

Provides in-memory caching, SQLite persistence, structured logging, and WebSocket dispatch.
"""
from __future__ import annotations

import collections
import dataclasses
import datetime
import logging
import os
import sqlite3
import sys
import threading
import time
import traceback
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from backend.core.config import DATABASE_PATH, LOGS_DIR

# Set up file and console logger for system errors
logger = logging.getLogger("rapidalert.errors")
logger.setLevel(logging.INFO)

if not logger.handlers:
    _formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    # Console handler
    _ch = logging.StreamHandler(sys.stdout)
    _ch.setFormatter(_formatter)
    logger.addHandler(_ch)

    # File handler in logs/
    try:
        LOGS_DIR.mkdir(parents=True, exist_ok=True)
        _fh = logging.FileHandler(LOGS_DIR / "errors.log", encoding="utf-8")
        _fh.setFormatter(_formatter)
        logger.addHandler(_fh)
    except Exception:
        pass


@dataclasses.dataclass
class ErrorRecord:
    id: str
    timestamp: str
    ts: float
    component: str
    function: str
    file: str
    line: int
    camera: Optional[str]
    error_type: str
    message: str
    effect: str
    stack_trace: str
    severity: str = "ERROR"  # "CRITICAL", "ERROR", "WARNING"

    def to_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)


class ErrorTracker:
    """
    Centralized error and exception recording engine.
    Ensures no exception is bypassed without visibility into its location and effect.
    """

    def __init__(self, db_path: Path = DATABASE_PATH, maxlen: int = 300):
        self.db_path = db_path
        self._records: collections.deque[ErrorRecord] = collections.deque(maxlen=maxlen)
        self._lock = threading.Lock()
        self._broadcast_fn: Optional[Callable] = None
        self._watchdog_emailer = None
        self._seq = 0
        self._init_db()

    def set_broadcaster(self, fn: Callable) -> None:
        """Inject WebSocket broadcast function."""
        self._broadcast_fn = fn

    def set_watchdog_emailer(self, emailer: Any) -> None:
        """Inject WatchdogEmailer for automated severe incident alerting."""
        self._watchdog_emailer = emailer

    def _init_db(self) -> None:
        """Initialize errors table in SQLite database."""
        try:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            with sqlite3.connect(str(self.db_path)) as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS error_logs (
                        id TEXT PRIMARY KEY,
                        ts REAL,
                        timestamp TEXT,
                        component TEXT,
                        function TEXT,
                        file TEXT,
                        line INTEGER,
                        camera TEXT,
                        error_type TEXT,
                        message TEXT,
                        effect TEXT,
                        stack_trace TEXT,
                        severity TEXT
                    )
                """)
                conn.execute("CREATE INDEX IF NOT EXISTS idx_err_ts ON error_logs(ts)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_err_component ON error_logs(component)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_err_camera ON error_logs(camera)")
                conn.commit()
        except Exception as e:
            logger.error(f"[ErrorTracker] Failed to initialize SQLite errors table: {e}")

    def capture_exception(
        self,
        exc: BaseException,
        component: str,
        effect: str,
        camera: Optional[str] = None,
        severity: str = "ERROR",
        function: Optional[str] = None,
    ) -> ErrorRecord:
        """
        Record a caught exception with its origin and operational downstream effect.
        """
        with self._lock:
            self._seq += 1
            seq = self._seq

        now_ts = time.time()
        now_str = datetime.datetime.fromtimestamp(now_ts).strftime("%Y-%m-%d %H:%M:%S")
        err_id = f"ERR-{time.strftime('%Y%m%d%H%M%S')}-{seq:03d}"
        
        # Extract caller location from traceback
        tb = exc.__traceback__
        filename = ""
        lineno = 0
        func_name = function or ""

        if tb:
            # Walk to the deepest frame of the exception
            last_tb = tb
            while last_tb.tb_next:
                last_tb = last_tb.tb_next
            frame = last_tb.tb_frame
            full_file = frame.f_code.co_filename
            lineno = last_tb.tb_lineno
            if not func_name:
                func_name = frame.f_code.co_name
            # Keep relative path for readability
            try:
                filename = os.path.relpath(full_file, os.getcwd())
            except ValueError:
                filename = os.path.basename(full_file)
        else:
            # Inspect caller frame
            try:
                frame = sys._getframe(1)
                full_file = frame.f_code.co_filename
                lineno = frame.f_lineno
                if not func_name:
                    func_name = frame.f_code.co_name
                filename = os.path.basename(full_file)
            except Exception:
                filename = "unknown"

        st = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__)) if tb else ""
        error_type = exc.__class__.__name__
        msg = str(exc) or error_type

        record = ErrorRecord(
            id=err_id,
            timestamp=now_str,
            ts=now_ts,
            component=component,
            function=func_name,
            file=f"{filename}:{lineno}",
            line=lineno,
            camera=camera,
            error_type=error_type,
            message=msg,
            effect=effect,
            stack_trace=st,
            severity=severity.upper(),
        )

        # 1. Append to memory ring buffer
        with self._lock:
            self._records.append(record)

        # 2. Log structured output
        cam_info = f" [Cam: {camera}]" if camera else ""
        log_msg = (
            f"[{record.severity}] [{component}]{cam_info} {error_type}: {msg} at {record.file} in {func_name}()\n"
            f"    ↳ EFFECT: {effect}"
        )
        if record.severity == "CRITICAL":
            logger.critical(log_msg)
        elif record.severity == "WARNING":
            logger.warning(log_msg)
        else:
            logger.error(log_msg)

        # 3. Persist to SQLite
        self._persist_record(record)

        # 4. Asynchronous WebSocket notification if broadcaster is present
        if self._broadcast_fn:
            try:
                import asyncio
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    asyncio.create_task(self._broadcast_fn({
                        "type": "system_error",
                        "data": record.to_dict(),
                    }))
            except Exception:
                pass

        # 5. Automated Critical Watchdog Email Alert
        if record.severity in ("CRITICAL", "FATAL") and self._watchdog_emailer:
            try:
                self._watchdog_emailer.send_critical_alert(
                    event_type="FATAL_EXCEPTION",
                    title=f"Critical Exception in {component}",
                    message=f"A severe unhandled exception was captured in {component}: {record.message}",
                    details={
                        "Component": component,
                        "Function": func_name,
                        "File Location": record.file,
                        "Downstream Effect": record.effect,
                        "Camera Context": camera or "N/A",
                    },
                    severity=record.severity,
                    stack_trace=record.stack_trace,
                    remediation=f"Investigate {record.file} in {func_name}(). Check application logs for full cascade.",
                )
            except Exception:
                pass

        return record

    def capture_error(
        self,
        message: str,
        component: str,
        effect: str,
        camera: Optional[str] = None,
        severity: str = "ERROR",
        function: Optional[str] = None,
        error_type: str = "RuntimeError",
    ) -> ErrorRecord:
        """
        Record a system error that is not raised as a Python exception.
        """
        with self._lock:
            self._seq += 1
            seq = self._seq

        now_ts = time.time()
        now_str = datetime.datetime.fromtimestamp(now_ts).strftime("%Y-%m-%d %H:%M:%S")
        err_id = f"ERR-{time.strftime('%Y%m%d%H%M%S')}-{seq:03d}"

        try:
            frame = sys._getframe(1)
            full_file = frame.f_code.co_filename
            lineno = frame.f_lineno
            func_name = function or frame.f_code.co_name
            filename = os.path.basename(full_file)
        except Exception:
            filename = "unknown"
            lineno = 0
            func_name = function or "unknown"

        st = "".join(traceback.format_stack(limit=5))

        record = ErrorRecord(
            id=err_id,
            timestamp=now_str,
            ts=now_ts,
            component=component,
            function=func_name,
            file=f"{filename}:{lineno}",
            line=lineno,
            camera=camera,
            error_type=error_type,
            message=message,
            effect=effect,
            stack_trace=st,
            severity=severity.upper(),
        )

        with self._lock:
            self._records.append(record)

        cam_info = f" [Cam: {camera}]" if camera else ""
        log_msg = f"[{record.severity}] [{component}]{cam_info} {error_type}: {message} at {record.file} -> EFFECT: {effect}"
        if record.severity == "CRITICAL":
            logger.critical(log_msg)
        elif record.severity == "WARNING":
            logger.warning(log_msg)
        else:
            logger.error(log_msg)

        self._persist_record(record)

        if self._broadcast_fn:
            try:
                import asyncio
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    asyncio.create_task(self._broadcast_fn({
                        "type": "system_error",
                        "data": record.to_dict(),
                    }))
            except Exception:
                pass

        # Automated Critical Watchdog Email Alert
        if record.severity in ("CRITICAL", "FATAL") and self._watchdog_emailer:
            try:
                self._watchdog_emailer.send_critical_alert(
                    event_type="CRITICAL_FAULT",
                    title=f"Critical Fault in {component}",
                    message=f"A severe fault was recorded in {component}: {record.message}",
                    details={
                        "Component": component,
                        "Function": func_name,
                        "File Location": record.file,
                        "Downstream Effect": record.effect,
                        "Camera Context": camera or "N/A",
                    },
                    severity=record.severity,
                    stack_trace=record.stack_trace,
                    remediation=f"Inspect subsystem {component}. Check active processes and hardware status.",
                )
            except Exception:
                pass

        return record

    def _persist_record(self, r: ErrorRecord) -> None:
        """Write error record to SQLite in a thread-safe manner."""
        try:
            with sqlite3.connect(str(self.db_path)) as conn:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO error_logs
                    (id, ts, timestamp, component, function, file, line, camera, error_type, message, effect, stack_trace, severity)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        r.id,
                        r.ts,
                        r.timestamp,
                        r.component,
                        r.function,
                        r.file,
                        r.line,
                        r.camera,
                        r.error_type,
                        r.message,
                        r.effect,
                        r.stack_trace,
                        r.severity,
                    ),
                )
                conn.commit()
        except Exception as e:
            logger.error(f"[ErrorTracker] Failed to persist error to DB: {e}")

    def get_recent(
        self,
        limit: int = 50,
        component: Optional[str] = None,
        camera: Optional[str] = None,
        severity: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Retrieve recent error records with optional filtering."""
        with self._lock:
            items = list(self._records)

        filtered = items
        if component:
            c_low = component.lower()
            filtered = [r for r in filtered if r.component.lower() == c_low]
        if camera:
            cam_low = camera.lower()
            filtered = [r for r in filtered if r.camera and r.camera.lower() == cam_low]
        if severity:
            s_up = severity.upper()
            filtered = [r for r in filtered if r.severity == s_up]

        filtered = sorted(filtered, key=lambda x: x.ts, reverse=True)
        return [r.to_dict() for r in filtered[:limit]]

    def get_summary(self) -> Dict[str, Any]:
        """Aggregate error metrics across components, severities, and effects."""
        with self._lock:
            items = list(self._records)

        by_component: Dict[str, int] = collections.defaultdict(int)
        by_severity: Dict[str, int] = collections.defaultdict(int)
        by_effect: Dict[str, int] = collections.defaultdict(int)

        for r in items:
            by_component[r.component] += 1
            by_severity[r.severity] += 1
            # Truncate effect label for clean categorical grouping
            effect_key = r.effect[:60] + "..." if len(r.effect) > 60 else r.effect
            by_effect[effect_key] += 1

        return {
            "total_errors": len(items),
            "by_component": dict(by_component),
            "by_severity": dict(by_severity),
            "by_effect": dict(by_effect),
        }

    def get_errors(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Convenience alias for get_recent."""
        return self.get_recent(limit=limit)

    def count(self) -> int:
        """Returns the number of error records currently in memory."""
        with self._lock:
            return len(self._records)

    def clear(self) -> None:
        """Clear errors from memory and SQLite."""
        with self._lock:
            self._records.clear()
        try:
            with sqlite3.connect(str(self.db_path)) as conn:
                conn.execute("DELETE FROM error_logs")
                conn.commit()
        except Exception as e:
            logger.error(f"[ErrorTracker] Failed to clear error_logs: {e}")


# Global singleton instance
error_tracker = ErrorTracker()
