"""Shared audit & user-management layer backed by SQLite.

Provides:
- Self-service registration (name, email, role → UUID token)
- Token-based authentication for both Streamlit apps and the FastAPI API
- Page-view logging for Streamlit sessions
- API-call logging
- Session expiry (configurable, default 7 days)

The SQLite file lives alongside the output data (default ``logs/audit.db``)
and is created automatically on first use.  The ``logs/`` directory is
gitignored.

Usage — Streamlit::

    from renter_shield.audit import require_registration, log_page_view
    user = require_registration("investigator")  # shows gate if not registered
    log_page_view(user["id"], "overview", {"jur": "nyc"})

Usage — FastAPI::

    from renter_shield.audit import validate_token, log_api_call
    user = validate_token(token_string)  # returns dict or None
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
SESSION_EXPIRY_DAYS = int(os.getenv("LI_SESSION_EXPIRY_DAYS", "7"))

# Scopes that can be assigned at registration
VALID_SCOPES = {"renter", "investigator"}


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
            scope       TEXT NOT NULL CHECK (scope IN ('renter', 'investigator')),
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
    return conn


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
    """Record a Streamlit page view."""
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


# ---------------------------------------------------------------------------
# Streamlit gate helper
# ---------------------------------------------------------------------------
def require_registration(scope: str) -> dict[str, Any] | None:
    """Streamlit registration gate.  Call at the top of the app.

    If the user is already registered (session state), returns the user dict.
    Otherwise, renders a registration form and returns None (caller should
    ``st.stop()`` if None).
    """
    import streamlit as st

    session_key = f"_audit_user_{scope}"

    # Already registered this session?
    if session_key in st.session_state:
        user = st.session_state[session_key]
        # Validate token is still valid (not expired)
        check = validate_token(user["token"])
        if check:
            return check
        # Expired — clear and re-register
        del st.session_state[session_key]

    # Show registration form
    st.title("🔐 Access Registration")

    tab_register, tab_token = st.tabs(["New registration", "I have a token"])

    with tab_token:
        token_input = st.text_input(
            "Paste your token",
            type="password",
            placeholder="e.g. a1b2c3d4...",
            key="_audit_token_input",
        )
        if st.button("Sign in", key="_audit_token_submit"):
            if token_input:
                user = validate_token(token_input.strip())
                if user and user.get("scope") == scope:
                    st.session_state[session_key] = user
                    st.rerun()
                elif user:
                    st.error(f"This token is for **{user['scope']}** access, not **{scope}**.")
                else:
                    st.error("Invalid or expired token.")
            else:
                st.error("Please paste your token.")

    with tab_register:
        if scope == "investigator":
            st.markdown(
                "This tool contains investigator-level data including owner "
                "identities, confidence tiers, and detailed score breakdowns. "
                "By registering, you agree that:\n\n"
                "- You will use this data only for legitimate investigative "
                "or research purposes.\n"
                "- You understand scores are algorithmic estimates and require "
                "independent verification.\n"
                "- Your access will be logged for audit purposes."
            )
        else:
            st.markdown(
                "Register to access property violation data. Your access will "
                "be logged for quality and security purposes."
            )

        with st.form("registration_form"):
            name = st.text_input("Full name", placeholder="e.g. Jane Doe")
            email = st.text_input("Email", placeholder="e.g. jane.doe@example.org")
            role = st.text_input(
                "Role / Organisation",
                placeholder="e.g. Investigator, Journalist, Renter",
            )
            agreed = st.checkbox(
                "I understand my access will be monitored and I will use "
                "this data responsibly."
            )
            submitted = st.form_submit_button("Register & Continue")

        if submitted:
            if not name or not email or not agreed:
                st.error("Please fill in your name, email, and agree to the terms.")
                return None
            if "@" not in email or "." not in email.split("@")[-1]:
                st.error("Please enter a valid email address.")
                return None

            user = register_user(name=name, email=email, role=role, scope=scope)
            st.session_state[session_key] = user

            # Show token for API access
            st.success("✅ Registered! You now have access.")
            st.info(
                f"**Your API token** (use as `X-API-Key` header for API calls):\n\n"
                f"`{user['token']}`\n\n"
                f"Save this — it won't be shown again. "
                f"Expires after {SESSION_EXPIRY_DAYS} days."
            )
            st.button("Continue to app →", key="_audit_continue")
            return None  # Let user see the token before proceeding

    return None
