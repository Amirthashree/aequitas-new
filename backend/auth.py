# backend/auth.py
# ─────────────────────────────────────────────────────────────────────────────
# Phase 8: Google OAuth helper + password auth utilities
# ─────────────────────────────────────────────────────────────────────────────

import os
import bcrypt
from google.oauth2 import id_token
from google.auth.transport import requests as google_requests

# ── Replace with your actual Google OAuth Client ID ───────────────────────────
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "YOUR_GOOGLE_CLIENT_ID")
# ─────────────────────────────────────────────────────────────────────────────


def verify_google_token(token: str) -> dict | None:
    """
    Validate a Google Sign-In ID token.

    Returns:
        { "google_id": str, "email": str, "name": str }
        or None if the token is invalid / expired.
    """
    try:
        info = id_token.verify_oauth2_token(
            token,
            google_requests.Request(),
            GOOGLE_CLIENT_ID,
        )
        return {
            "google_id": info["sub"],
            "email":     info.get("email", ""),
            "name":      info.get("name", ""),
        }
    except Exception:
        return None


def hash_password(plain: str) -> str:
    """Hash a plaintext password with bcrypt. Returns a UTF-8 string."""
    salt = bcrypt.gensalt()
    hashed = bcrypt.hashpw(plain.encode("utf-8"), salt)
    return hashed.decode("utf-8")


def check_password(plain: str, hashed: str) -> bool:
    """Verify a plaintext password against a bcrypt hash."""
    try:
        return bcrypt.checkpw(plain.encode("utf-8"), hashed.encode("utf-8"))
    except Exception:
        return False