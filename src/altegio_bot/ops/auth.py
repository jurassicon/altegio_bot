from __future__ import annotations

import hashlib
import hmac as _hmac
import secrets
import time
from urllib.parse import urlsplit

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


# ---------------------------------------------------------------------------
# §43.7 — a stricter contract for the actions that spend money and send messages
# ---------------------------------------------------------------------------
# `require_ops_auth` above is the cabinet's historical door, and it is left
# exactly as it is: it has a dev fallback ("nothing configured → allow"), it
# accepts a token in the query string and it accepts HTTP Basic, all of which
# earn their keep for a read-only dashboard, curl and existing scripts.
#
# None of that may guard a voucher purchase or a WhatsApp send. So the new
# controlling endpoints get their own dependency, and it differs in four ways:
#
# 1. **No dev fallback.** An unconfigured deployment refuses instead of allowing.
#    A machine nobody finished setting up must not be able to spend real money.
# 2. **Browser session only.** Not a query token — secrets do not belong in URLs,
#    in browser history or in access logs — and not HTTP Basic, because the whole
#    point of §43 is a supported operator process in a real browser, and because a
#    credential that travels on every request is exactly what CSRF exploits.
# 3. **CSRF, bound to the session.** A token derived from the presented cookie,
#    rendered into the page, required in a header. Another origin can make a
#    browser send the cookie; it cannot read it, so it cannot produce the header.
# 4. **A server-resolved principal.** Who acted is read from the session, never
#    from the payload. A request may not name its own actor.
#
# What this is NOT is a redesign of the project's authorisation. The existing
# dependency, the login form, the session cookie and every other route keep their
# behaviour; this adds one stricter door in front of the new sensitive actions.

CSRF_HEADER = "X-Ops-CSRF"
CSRF_DOMAIN = "altegio_bot/ops/voucher-mailing/csrf/v1"


class OpsSessionError(Exception):
    """The presented request is not an authorised browser session.

    Carries a stable reason code so an endpoint can answer with §42's PII-free
    vocabulary instead of prose, and a status so the browser does the right thing.
    """

    def __init__(self, reason: str, *, status_code: int = status.HTTP_403_FORBIDDEN) -> None:
        super().__init__(reason)
        self.reason = reason
        self.status_code = status_code


def _signing_key() -> str:
    """The key the session cookie is signed with, as `require_ops_auth` uses it."""
    return settings.ops_secret or settings.ops_pass


def session_fingerprint(token: str) -> str:
    """A keyed digest of a session token. Never the token.

    Keyed rather than a bare hash: a session token is low entropy in the sense
    that matters here — it is `user:timestamp:mac`, so an unkeyed digest of it
    would be reproducible by anyone who could guess the user and the second, and
    an audit trail of reproducible digests is a lookup table.

    What it is for is telling two sessions of the SAME account apart in an audit.
    It is not a credential and nothing accepts it as one.
    """
    return _hmac.new(
        _signing_key().encode(),
        (CSRF_DOMAIN + ":fingerprint:" + token).encode(),
        hashlib.sha256,
    ).hexdigest()


def csrf_token_for(session_token: str) -> str:
    """The CSRF token of one session.

    Derived from the session rather than stored, so there is no server-side CSRF
    state to expire, and so a token minted for one session is worthless with
    another. A cross-site page can cause the cookie to be sent but cannot read it,
    and therefore cannot compute this.
    """
    return _hmac.new(
        _signing_key().encode(),
        (CSRF_DOMAIN + ":token:" + session_token).encode(),
        hashlib.sha256,
    ).hexdigest()


def resolve_ops_session(request: Request) -> tuple[str, str]:
    """``(account, session_fingerprint)`` for an authorised browser session.

    Raises :class:`OpsSessionError` otherwise — including when Ops auth is not
    configured at all, which is the case the historical dependency lets through.
    """
    ops_user = settings.ops_user
    if not ops_user or not _signing_key():
        # Deliberately a refusal and not a fallback. See the note above.
        raise OpsSessionError("ops_session_unconfigured", status_code=status.HTTP_403_FORBIDDEN)

    token = request.cookies.get(SESSION_COOKIE, "")
    if not token or not check_session_token(token, ops_user, _signing_key()):
        raise OpsSessionError("ops_session_invalid", status_code=status.HTTP_401_UNAUTHORIZED)
    return ops_user, session_fingerprint(token)


def require_same_origin(request: Request) -> None:
    """Refuse a cross-site write.

    ``Origin`` is checked when the browser sent one, and `Referer` is used as the
    fallback for the browsers and proxies that only send that. A request with
    neither is refused rather than trusted: every browser sends one of them on a
    cross-origin POST, so "neither" is not a browser doing something ordinary.

    Compared against the host the request actually arrived on, so this keeps
    working behind the deployment's own proxy without a list of allowed hosts to
    drift out of date.
    """
    expected = (request.headers.get("host") or "").strip().casefold()
    if not expected:
        raise OpsSessionError("ops_origin_unknown")

    def host_of(value: str) -> str:
        return urlsplit(value.strip()).netloc.casefold()

    origin = request.headers.get("origin")
    if origin:
        if origin.strip().casefold() == "null" or host_of(origin) != expected:
            raise OpsSessionError("ops_origin_rejected")
        return
    referer = request.headers.get("referer")
    if referer:
        if host_of(referer) != expected:
            raise OpsSessionError("ops_origin_rejected")
        return
    raise OpsSessionError("ops_origin_missing")


def require_csrf(request: Request) -> None:
    """The header must be this session's CSRF token."""
    token = request.cookies.get(SESSION_COOKIE, "")
    presented = request.headers.get(CSRF_HEADER, "")
    if not token or not presented:
        raise OpsSessionError("ops_csrf_missing")
    if not secrets.compare_digest(presented, csrf_token_for(token)):
        raise OpsSessionError("ops_csrf_invalid")


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
