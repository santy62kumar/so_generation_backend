"""
app/api/auth.py
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

import base64
import hashlib
import hmac
import os
import time

from dotenv import load_dotenv
from fastapi import APIRouter, Depends, Header, HTTPException, Request
from pydantic import BaseModel, Field

from ..core.rate_limit import SlidingWindowLimiter, client_key, env_int

load_dotenv()

# ── Config (from .env) ────────────────────────────────────────────────────────
SECRET_KEY = os.getenv("ADMIN_SECRET_KEY", "")
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "")
_ADMIN_USERNAME_BYTES = ADMIN_USERNAME.encode()
_ADMIN_PASSWORD_BYTES = ADMIN_PASSWORD.encode()

if not ADMIN_USERNAME or not ADMIN_PASSWORD or len(SECRET_KEY) < 32:
    raise RuntimeError(
        "ADMIN_USERNAME, ADMIN_PASSWORD, and an ADMIN_SECRET_KEY of at least 32 characters are required."
    )

try:
    TOKEN_TTL_SECONDS = int(os.getenv("ADMIN_TOKEN_TTL", "28800"))
except ValueError as exc:
    raise RuntimeError("ADMIN_TOKEN_TTL must be a whole number of seconds.") from exc
if TOKEN_TTL_SECONDS <= 0:
    raise RuntimeError("ADMIN_TOKEN_TTL must be greater than zero.")

LOGIN_ATTEMPT_LIMIT = env_int("LOGIN_ATTEMPT_LIMIT", 5)
LOGIN_WINDOW_SECONDS = env_int("LOGIN_WINDOW_SECONDS", 300)

# Counts only *failed* attempts, and a success clears the bucket, so this uses
# check/record/reset rather than the plain request-counting dependency.
# ponytail: per-process; use shared storage if workers need one global budget.
_login_limiter = SlidingWindowLimiter("auth/login", LOGIN_ATTEMPT_LIMIT, LOGIN_WINDOW_SECONDS)


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
    if not hmac.compare_digest(signature.encode(), expected_signature.encode()):
        raise HTTPException(status_code=401, detail="Invalid session token")

    try:
        username, expires_at = base64.urlsafe_b64decode(payload_b64.encode()).decode().split(":")
        expires_at = int(expires_at)
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid session token")

    if expires_at < int(time.time()):
        raise HTTPException(status_code=401, detail="Session expired, please log in again")

    if not hmac.compare_digest(username.encode(), _ADMIN_USERNAME_BYTES):
        raise HTTPException(status_code=401, detail="Invalid session token")

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
    username: str = Field(min_length=1, max_length=200)
    password: str = Field(min_length=1, max_length=500)


class LoginResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int


@auth_router.post("/login", response_model=LoginResponse)
def login(payload: LoginRequest, request: Request):
    client = client_key(request)
    _login_limiter.check(client)

    # compare_digest raises TypeError on non-ASCII str, so compare bytes.
    valid_username = hmac.compare_digest(payload.username.encode(), _ADMIN_USERNAME_BYTES)
    valid_password = hmac.compare_digest(payload.password.encode(), _ADMIN_PASSWORD_BYTES)

    if not (valid_username and valid_password):
        _login_limiter.record(client)
        raise HTTPException(status_code=401, detail="Invalid username or password")

    _login_limiter.reset(client)
    token = create_token(payload.username)
    return LoginResponse(access_token=token, expires_in=TOKEN_TTL_SECONDS)


@auth_router.get("/me")
def me(user: str = Depends(get_current_user)):  # simple whoami, mainly for debugging
    return {"username": user}
