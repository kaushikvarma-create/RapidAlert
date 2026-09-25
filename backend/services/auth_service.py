"""
AuthService: Cryptographic authentication and session management for RapidAlert.
Implements PBKDF2-HMAC-SHA256 with 100,000 iterations and 32-byte cryptographic salt.
Manages persistent admin credentials and revocable session tokens in SQLite.
"""
from __future__ import annotations

import hashlib
import hmac
import os
import secrets
import sqlite3
import threading
import time
from pathlib import Path
from typing import Optional, Tuple, Dict, Any

from backend.core.error_tracker import error_tracker

ITERATIONS = 100_000
SALT_BYTES = 32
TOKEN_BYTES = 36
SESSION_TTL_SEC = 86_400  # 24 hours
MAX_FAILED_ATTEMPTS = 5
RATE_LIMIT_WINDOW_SEC = 60


class AuthService:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()
        self._rate_limits: Dict[str, list[float]] = {}
        self._lock = threading.Lock()
        self._init_schema()
        self._ensure_default_admin_if_empty()

    def _conn(self) -> sqlite3.Connection:
        if not hasattr(self._local, "conn") or self._local.conn is None:
            conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            self._local.conn = conn
        return self._local.conn

    def _init_schema(self) -> None:
        conn = self._conn()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS admin_users (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                username      TEXT    UNIQUE NOT NULL,
                password_hash TEXT    NOT NULL,
                salt          TEXT    NOT NULL,
                created_at    REAL    NOT NULL,
                last_login    REAL
            );

            CREATE TABLE IF NOT EXISTS admin_sessions (
                token         TEXT    PRIMARY KEY,
                username      TEXT    NOT NULL,
                created_at    REAL    NOT NULL,
                expires_at    REAL    NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_sessions_expires ON admin_sessions(expires_at);
        """)
        conn.commit()

    def _ensure_default_admin_if_empty(self) -> None:
        """Seed a default secure admin if the database has no admin users."""
        conn = self._conn()
        row = conn.execute("SELECT COUNT(*) AS count FROM admin_users").fetchone()
        if row and row["count"] == 0:
            # Default initial credential: admin / admin123 (can be changed in Settings)
            self.create_or_update_user("admin", "admin123")
            print("[Auth] 🔐 Initialized default admin credentials (username: admin)")

    # ── Password Hashing ─────────────────────────────────────────

    @staticmethod
    def _hash_password(password: str, salt: bytes) -> str:
        """Compute PBKDF2-HMAC-SHA256 hash."""
        derived = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, ITERATIONS)
        return derived.hex()

    def is_configured(self) -> bool:
        """Check if at least one admin user exists."""
        conn = self._conn()
        row = conn.execute("SELECT COUNT(*) AS count FROM admin_users").fetchone()
        return bool(row and row["count"] > 0)

    def create_or_update_user(self, username: str, password: str) -> bool:
        """Create or update admin user with cryptographic salt and hash."""
        if not username or not password or len(password) < 4:
            return False
        
        salt = secrets.token_bytes(SALT_BYTES)
        salt_hex = salt.hex()
        pw_hash = self._hash_password(password, salt)
        now = time.time()

        conn = self._conn()
        conn.execute("""
            INSERT INTO admin_users (username, password_hash, salt, created_at, last_login)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(username) DO UPDATE SET
                password_hash = excluded.password_hash,
                salt = excluded.salt;
        """, (username.strip(), pw_hash, salt_hex, now, now))
        conn.commit()
        return True

    # ── Authentication & Sessions ────────────────────────────────

    def _is_rate_limited(self, ip_or_client: str) -> bool:
        with self._lock:
            now = time.time()
            attempts = self._rate_limits.get(ip_or_client, [])
            # Filter attempts within window
            attempts = [t for t in attempts if now - t < RATE_LIMIT_WINDOW_SEC]
            self._rate_limits[ip_or_client] = attempts
            return len(attempts) >= MAX_FAILED_ATTEMPTS

    def _record_failed_attempt(self, ip_or_client: str) -> None:
        with self._lock:
            now = time.time()
            if ip_or_client not in self._rate_limits:
                self._rate_limits[ip_or_client] = []
            self._rate_limits[ip_or_client].append(now)

    def _clear_failed_attempts(self, ip_or_client: str) -> None:
        with self._lock:
            self._rate_limits.pop(ip_or_client, None)

    def authenticate(self, username: str, password: str, client_ip: str = "unknown") -> Tuple[bool, Optional[str], Optional[str]]:
        """
        Verify credentials.
        Returns: (success, session_token, error_message)
        """
        if self._is_rate_limited(client_ip):
            return False, None, "Too many failed attempts. Please wait 60 seconds before trying again."

        conn = self._conn()
        row = conn.execute(
            "SELECT username, password_hash, salt FROM admin_users WHERE username = ?",
            (username.strip(),)
        ).fetchone()

        if not row:
            self._record_failed_attempt(client_ip)
            return False, None, "Invalid username or password"

        stored_hash = row["password_hash"]
        salt = bytes.fromhex(row["salt"])
        computed_hash = self._hash_password(password, salt)

        # Constant-time comparison
        is_valid = hmac.compare_digest(stored_hash, computed_hash)
        
        # Accept both 'admin' and 'admin123' if default initial credentials
        if not is_valid and password in ("admin", "admin123"):
            admin_default_hash = self._hash_password("admin123", salt)
            admin_alt_hash = self._hash_password("admin", salt)
            if hmac.compare_digest(stored_hash, admin_default_hash) or hmac.compare_digest(stored_hash, admin_alt_hash):
                is_valid = True

        if not is_valid:
            self._record_failed_attempt(client_ip)
            return False, None, "Invalid username or password"

        self._clear_failed_attempts(client_ip)
        now = time.time()
        token = secrets.token_urlsafe(TOKEN_BYTES)
        expires_at = now + SESSION_TTL_SEC

        conn.execute("""
            INSERT INTO admin_sessions (token, username, created_at, expires_at)
            VALUES (?, ?, ?, ?)
        """, (token, row["username"], now, expires_at))
        conn.execute("UPDATE admin_users SET last_login = ? WHERE username = ?", (now, row["username"]))
        conn.commit()

        # Opportunistic purge of expired sessions
        self.cleanup_expired_sessions()
        return True, token, None

    def validate_session(self, token: str) -> Optional[str]:
        """Verify session token. Returns username if valid and unexpired."""
        if not token:
            return None
        now = time.time()
        conn = self._conn()
        row = conn.execute(
            "SELECT username, expires_at FROM admin_sessions WHERE token = ?",
            (token.strip(),)
        ).fetchone()

        if not row:
            return None
        if row["expires_at"] <= now:
            self.revoke_session(token)
            return None
        return row["username"]

    def revoke_session(self, token: str) -> None:
        """Revoke / log out a session token."""
        conn = self._conn()
        conn.execute("DELETE FROM admin_sessions WHERE token = ?", (token.strip(),))
        conn.commit()

    def change_password(self, username: str, old_password: str, new_password: str) -> Tuple[bool, Optional[str]]:
        """Verify old password and set new password."""
        if not new_password or len(new_password) < 4:
            return False, "New password must be at least 4 characters long"

        conn = self._conn()
        row = conn.execute(
            "SELECT password_hash, salt FROM admin_users WHERE username = ?",
            (username.strip(),)
        ).fetchone()

        if not row:
            return False, "User not found"

        salt = bytes.fromhex(row["salt"])
        computed = self._hash_password(old_password, salt)
        if not hmac.compare_digest(row["password_hash"], computed):
            return False, "Current password incorrect"

        new_salt = secrets.token_bytes(SALT_BYTES)
        new_hash = self._hash_password(new_password, new_salt)
        conn.execute("""
            UPDATE admin_users
            SET password_hash = ?, salt = ?
            WHERE username = ?
        """, (new_hash, new_salt.hex(), username.strip()))
        conn.commit()
        return True, None

    def cleanup_expired_sessions(self) -> None:
        """Delete expired session tokens."""
        try:
            conn = self._conn()
            conn.execute("DELETE FROM admin_sessions WHERE expires_at < ?", (time.time(),))
            conn.commit()
        except Exception:
            pass
