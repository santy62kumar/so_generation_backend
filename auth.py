"""
auth.py
─────────────────────────────────────────────────────────────────────────────
Minimal username/password auth for the internal Database Manager UI.

- Credentials come from environment variables (.env): ADMIN_USERNAME, ADMIN_PASSWORD
- On successful login we issue a signed, expiring token (HMAC-SHA256 — no
  extra dependency like PyJWT needed).
- Every protected route depends on `get_current_user`, which validates the
  `Authorization: Bearer <token>` header.

To change the login credentials, just update your .env file — nothing in
code needs to change.
"""

import os
import time
import hmac
import hashlib
import base64

from fastapi import APIRouter, Header, HTTPException, Depends
from pydantic import BaseModel

# ── Config (from .env) ────────────────────────────────────────────────────────
SECRET_KEY = os.getenv("ADMIN_SECRET_KEY", "please-change-this-secret-key")
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "admin")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "changeme")
TOKEN_TTL_SECONDS = int(os.getenv("ADMIN_TOKEN_TTL", "28800"))  # 8 hours default


# ── Token helpers ──────────────────────────────────────────────────────────────
def _sign(payload_b64: str) -> str:
    return hmac.new(SECRET_KEY.encode(), payload_b64.encode(), hashlib.sha256).hexdigest()


def create_token(username: str) -> str:
    expires_at = int(time.time()) + TOKEN_TTL_SECONDS
    raw = f"{username}:{expires_at}"
    payload_b64 = base64.urlsafe_b64encode(raw.encode()).decode()
    signature = _sign(payload_b64)
    return f"{payload_b64}.{signature}"


def verify_token(token: str) -> str:
    try:
        payload_b64, signature = token.split(".")
    except ValueError:
        raise HTTPException(status_code=401, detail="Invalid session token")

    expected_signature = _sign(payload_b64)
    if not hmac.compare_digest(signature, expected_signature):
        raise HTTPException(status_code=401, detail="Invalid session token")

    try:
        username, expires_at = base64.urlsafe_b64decode(payload_b64.encode()).decode().split(":")
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid session token")

    if int(expires_at) < int(time.time()):
        raise HTTPException(status_code=401, detail="Session expired, please log in again")

    return username


def get_current_user(authorization: str = Header(default=None)) -> str:
    """FastAPI dependency — attach with `user: str = Depends(get_current_user)`."""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing or invalid Authorization header")
    token = authorization[len("Bearer "):]
    return verify_token(token)


# ── Router ─────────────────────────────────────────────────────────────────────
auth_router = APIRouter(prefix="/auth", tags=["auth"])


class LoginRequest(BaseModel):
    username: str
    password: str


class LoginResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int


@auth_router.post("/login", response_model=LoginResponse)
def login(payload: LoginRequest):
    valid_username = hmac.compare_digest(payload.username, ADMIN_USERNAME)
    valid_password = hmac.compare_digest(payload.password, ADMIN_PASSWORD)

    if not (valid_username and valid_password):
        raise HTTPException(status_code=401, detail="Invalid username or password")

    token = create_token(payload.username)
    return LoginResponse(access_token=token, expires_in=TOKEN_TTL_SECONDS)


@auth_router.get("/me")
def me(user: str = Depends(get_current_user)):  # simple whoami, mainly for debugging
    return {"username": user}