"""Shared audit & user-management layer backed by SQLite.

Provides:
- Self-service registration (name, email, role → UUID token)
- Token-based authentication for the web UI and FastAPI API
- Page-view logging
- API-call logging
- Session expiry (configurable, default 90 days)

The SQLite file lives alongside the output data (default ``logs/audit.db``)
and is created automatically on first use.  The ``logs/`` directory is
gitignored.

Usage::

    from renter_shield.audit import validate_token, log_page_view, log_api_call
    user = validate_token(token_string)  # returns dict or None
    log_page_view(user["id"], "investigator", "overview", {"jur": "nyc"})
    log_api_call(user["id"], "/investigator/jurisdictions", "GET")
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
_DB_DIR = Path(os.getenv("LI_AUDIT_DIR", "logs"))
_DB_PATH = _DB_DIR / "audit.db"
SESSION_EXPIRY_DAYS = int(os.getenv("LI_SESSION_EXPIRY_DAYS", "90"))

# Scopes that can be assigned at registration
VALID_SCOPES = {"renter", "investigator", "developer"}


# ---------------------------------------------------------------------------
# Database initialisation
# ---------------------------------------------------------------------------
def _get_db() -> sqlite3.Connection:
    """Return a connection to the audit database, creating tables if needed."""
    _DB_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(_DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            id          TEXT PRIMARY KEY,
            name        TEXT NOT NULL,
            email       TEXT NOT NULL,
            role        TEXT NOT NULL DEFAULT '',
            scope       TEXT NOT NULL,
            token       TEXT NOT NULL UNIQUE,
            registered_at TEXT NOT NULL,
            ip          TEXT DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS page_views (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id     TEXT NOT NULL REFERENCES users(id),
            scope       TEXT NOT NULL,
            page        TEXT NOT NULL,
            params      TEXT DEFAULT '',
            viewed_at   TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS api_calls (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id     TEXT NOT NULL REFERENCES users(id),
            path        TEXT NOT NULL,
            method      TEXT NOT NULL DEFAULT 'GET',
            called_at   TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_users_token ON users(token);
        CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
        CREATE INDEX IF NOT EXISTS idx_page_views_user ON page_views(user_id);
        CREATE INDEX IF NOT EXISTS idx_api_calls_user ON api_calls(user_id);
    """)
    _migrate_scope_check(conn)
    return conn


def _migrate_scope_check(conn: sqlite3.Connection) -> None:
    """Drop the legacy ``CHECK (scope IN ('renter', 'investigator'))`` constraint.

    Existing databases created before the ``developer`` scope was added carry a
    restrictive CHECK that rejects new scopes.  SQLite can't ALTER a constraint,
    so rebuild the table without it (Python's ``VALID_SCOPES`` enforces scopes).
    """
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'users'"
    ).fetchone()
    if not row or "CHECK" not in (row["sql"] or ""):
        return  # already migrated (no CHECK) or table absent

    conn.execute("PRAGMA foreign_keys=OFF")
    conn.executescript("""
        BEGIN;
        CREATE TABLE users_new (
            id          TEXT PRIMARY KEY,
            name        TEXT NOT NULL,
            email       TEXT NOT NULL,
            role        TEXT NOT NULL DEFAULT '',
            scope       TEXT NOT NULL,
            token       TEXT NOT NULL UNIQUE,
            registered_at TEXT NOT NULL,
            ip          TEXT DEFAULT ''
        );
        INSERT INTO users_new SELECT id, name, email, role, scope, token, registered_at, ip FROM users;
        DROP TABLE users;
        ALTER TABLE users_new RENAME TO users;
        CREATE INDEX IF NOT EXISTS idx_users_token ON users(token);
        CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
        COMMIT;
    """)
    conn.execute("PRAGMA foreign_keys=ON")


# Module-level connection (lazy)
_conn: sqlite3.Connection | None = None


def _db() -> sqlite3.Connection:
    global _conn  # noqa: PLW0603
    if _conn is None:
        _conn = _get_db()
    return _conn


# ---------------------------------------------------------------------------
# Registration & token management
# ---------------------------------------------------------------------------
def register_user(
    name: str,
    email: str,
    role: str,
    scope: str,
    ip: str = "",
) -> dict[str, Any]:
    """Create a new user and return their record (including token).

    If the email+scope combination already exists and the session hasn't
    expired, returns the existing record instead of creating a duplicate.
    """
    scope = scope.lower()
    if scope not in VALID_SCOPES:
        raise ValueError(f"Invalid scope: {scope}")

    db = _db()

    # Check for existing non-expired registration with same email+scope
    row = db.execute(
        "SELECT * FROM users WHERE email = ? AND scope = ? ORDER BY registered_at DESC LIMIT 1",
        (email.strip().lower(), scope),
    ).fetchone()

    if row:
        reg_time = datetime.fromisoformat(row["registered_at"])
        if datetime.now(timezone.utc) - reg_time < timedelta(days=SESSION_EXPIRY_DAYS):
            return dict(row)

    user_id = uuid.uuid4().hex[:12]
    token = uuid.uuid4().hex
    now = datetime.now(timezone.utc).isoformat()

    db.execute(
        "INSERT INTO users (id, name, email, role, scope, token, registered_at, ip) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (user_id, name.strip(), email.strip().lower(), role.strip(), scope, token, now, ip),
    )
    db.commit()

    return {
        "id": user_id,
        "name": name.strip(),
        "email": email.strip().lower(),
        "role": role.strip(),
        "scope": scope,
        "token": token,
        "registered_at": now,
        "ip": ip,
    }


def validate_token(token: str) -> dict[str, Any] | None:
    """Look up a token and return the user record if valid and not expired."""
    if not token:
        return None
    db = _db()
    row = db.execute("SELECT * FROM users WHERE token = ?", (token,)).fetchone()
    if not row:
        return None
    reg_time = datetime.fromisoformat(row["registered_at"])
    if datetime.now(timezone.utc) - reg_time > timedelta(days=SESSION_EXPIRY_DAYS):
        return None
    return dict(row)


def get_user_scope(token: str) -> str | None:
    """Return the scope for a valid token, or None."""
    user = validate_token(token)
    return user["scope"] if user else None


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def log_page_view(
    user_id: str,
    scope: str,
    page: str,
    params: dict | None = None,
) -> None:
    """Record a page view."""
    db = _db()
    db.execute(
        "INSERT INTO page_views (user_id, scope, page, params, viewed_at) VALUES (?, ?, ?, ?, ?)",
        (user_id, scope, page, str(params or {}), datetime.now(timezone.utc).isoformat()),
    )
    db.commit()


def log_api_call(
    user_id: str,
    path: str,
    method: str = "GET",
) -> None:
    """Record an API call."""
    db = _db()
    db.execute(
        "INSERT INTO api_calls (user_id, path, method, called_at) VALUES (?, ?, ?, ?)",
        (user_id, path, method, datetime.now(timezone.utc).isoformat()),
    )
    db.commit()
