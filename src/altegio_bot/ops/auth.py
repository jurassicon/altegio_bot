from __future__ import annotations

import hashlib
import hmac as _hmac
import secrets
import time

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials

from altegio_bot.settings import settings

_security = HTTPBasic(auto_error=False)

SESSION_COOKIE = "ops_session"
SESSION_MAX_AGE = 8 * 3600  # 8 hours


def make_session_token(user: str, password: str) -> str:
    """Return an HMAC-SHA256-signed session token valid for SESSION_MAX_AGE."""
    ts = str(int(time.time()))
    msg = f"{user}:{ts}"
    sig = _hmac.new(password.encode(), msg.encode(), hashlib.sha256).hexdigest()
    return f"{msg}:{sig}"


def check_session_token(token: str, user: str, password: str) -> bool:
    """Return True if the token is valid and not expired."""
    try:
        tok_user, ts_str, sig = token.split(":", 2)
    except ValueError:
        return False
    if tok_user != user:
        return False
    try:
        ts = int(ts_str)
    except ValueError:
        return False
    if time.time() - ts > SESSION_MAX_AGE:
        return False
    msg = f"{tok_user}:{ts_str}"
    expected = _hmac.new(password.encode(), msg.encode(), hashlib.sha256).hexdigest()
    return secrets.compare_digest(sig, expected)


async def require_ops_auth(
    request: Request,
    credentials: HTTPBasicCredentials | None = Depends(_security),
) -> None:
    ops_token = settings.ops_token
    ops_user = settings.ops_user
    ops_pass = settings.ops_pass

    # No auth configured → allow (useful in dev)
    if not ops_token and not ops_user:
        return

    # Token via header or query param
    if ops_token:
        header_tok = request.headers.get("X-Ops-Token", "")
        query_tok = request.query_params.get("token", "")
        if header_tok and secrets.compare_digest(header_tok, ops_token):
            return
        if query_tok and secrets.compare_digest(query_tok, ops_token):
            return

    # Session cookie (set by the /ops/login form)
    if ops_user:
        # Use ops_secret as the signing key; fall back to ops_pass
        signing_key = settings.ops_secret or ops_pass
        session = request.cookies.get(SESSION_COOKIE, "")
        if session and check_session_token(session, ops_user, signing_key):
            return

    # HTTP Basic Auth (backward-compat for curl / scripts)
    if ops_user and credentials:
        user_ok = secrets.compare_digest(credentials.username.encode(), ops_user.encode())
        pass_ok = secrets.compare_digest(credentials.password.encode(), ops_pass.encode())
        if user_ok and pass_ok:
            return

    # Not authenticated:
    # – browser clients → redirect to the login form
    # – non-browser clients → return 401 + WWW-Authenticate
    if "text/html" in request.headers.get("accept", ""):
        next_path = str(request.url.path)
        raise HTTPException(
            status_code=302,
            headers={"Location": f"/ops/login?next={next_path}"},
        )
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Unauthorized",
        headers={"WWW-Authenticate": 'Basic realm="Ops"'},
    )
