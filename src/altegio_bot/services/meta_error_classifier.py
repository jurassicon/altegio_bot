"""Pure, dependency-free classification of Meta/WhatsApp send errors.

These helpers decide whether a failed Meta send is transient (the circuit
breaker should close and the message should be retried/requeued) or permanent
(retrying will never succeed). They are intentionally free of database, network
and settings side effects so they can be shared by the outbox worker and the
WhatsApp inbox/operator-relay worker without import cycles.

Inputs may be a plain string (the canonical form stored on outbox rows and
returned by ``safe_send``), a ``BaseException`` (e.g. ``MetaCloudError`` which
carries structured ``status_code`` / ``meta_code`` / ``is_transient`` fields),
or ``None``. Structured attributes, when present, are preferred; otherwise the
helpers fall back to parsing the stringified error so legacy/stored errors keep
working.
"""

from __future__ import annotations

import re
from typing import Union

ErrorLike = Union[BaseException, str, None]

# Transient HTTP statuses Meta/WhatsApp returns during outages or rate limiting.
_TRANSIENT_HTTP_STATUS_CODES = frozenset({429, 500, 502, 503, 504})

# String/regex fallbacks. These match both ``MetaCloudError.__str__`` output
# (``status=500``/``status_code=500``, ``code=2``, ``is_transient=true``) and
# Python-dict / JSON reprs of a raw Meta error body.
_TRANSIENT_HTTP_STATUS_RE = re.compile(r"\bstatus(?:_code)?=(429|500|502|503|504)\b")
# The ``(?!\d)`` guard prevents ``"code":200`` / ``"code":230`` / ``"code":270``
# from matching the transient Meta code 2 when the classifier is handed a raw
# JSON/dict body instead of the sanitized ``code=2`` string form.
_TRANSIENT_META_CODE_RE = re.compile(r"""(?:(?:"code"|'code')\s*:\s*["']?2["']?(?!\d)|\bcode=2\b)""")
_TRANSIENT_FLAG_RE = re.compile(r"""(?:(?:"is_transient"|'is_transient')\s*:\s*true|\bis_transient=true\b)""")
_TRANSIENT_NETWORK_HINTS = (
    "timeout",
    "timed out",
    "connection error",
    "connect error",
    "connecterror",
    "connection reset",
    "connection refused",
    "connection aborted",
    "network is unreachable",
    "temporarily unavailable",
    "temporary failure",
)


def _coerce_error_text(err: ErrorLike) -> str:
    if err is None:
        return ""
    return str(err)


def is_token_expired_error(err: ErrorLike) -> bool:
    low = _coerce_error_text(err).lower()
    return ("access token" in low and "expired" in low) or "code=190" in low


def is_permanent_meta_template_error(err: ErrorLike) -> bool:
    """Return True for permanent Meta template validation errors (HTTP 400).

    These errors indicate a mis-configured template call; retrying will never succeed.
    """
    low = _coerce_error_text(err).lower()
    return any(
        marker in low
        for marker in (
            "#132000",
            "number of parameters does not match",
            "does not match the expected number of params",
            "required parameter is missing",
            "template does not exist",
            "template name does not exist",
            "does not exist in the translation",
            "template validation error",
            "code=132000",
            "code=132001",
            "code=132005",
            "code=132007",
            "code=132012",
            "code=132015",
            "code=132016",
        )
    )


def is_text_window_policy_error(err: ErrorLike) -> bool:
    """Return True for deterministic Meta policy/window errors.

    Only these errors trigger automatic template fallback after a failed text
    send inside an open 24h window.  Ambiguous errors (timeouts, 5xx, unknown)
    return False — the caller preserves normal retry behaviour to avoid
    duplicate-send risk when the text may have been accepted but the response
    was lost.
    """
    low = _coerce_error_text(err).lower()
    return any(
        marker in low
        for marker in (
            "131047",
            "24 hour",
            "24-hour",
            "outside the allowed window",
            "customer service window",
            "re-engagement message",
        )
    )


# Documented WhatsApp Cloud permanent rejections where the app is confident Meta
# did NOT accept the message. Deliberately conservative: anything not on this
# allowlist (or the vetted token/template/window classifiers) is treated as an
# indeterminate outcome, never a permanent failure.
_DETERMINISTIC_REJECTION_MARKERS = (
    "131026",  # message undeliverable (recipient cannot receive on WhatsApp)
    "131009",  # parameter value is not valid
    "131008",  # required parameter is missing
    "message undeliverable",
    "recipient phone number not in allowed list",  # sandbox allow-list rejection
    "not a valid whatsapp",
    "invalid recipient",
    "permission denied",
    "does not have permission",
    "unsupported message type",
    "unsupported request",
)


def is_deterministic_meta_rejection(err: ErrorLike) -> bool:
    """Return True only for errors that prove Meta did NOT accept the message.

    Composes the vetted permanent classifiers (token expiry, template validation,
    24h window policy) with an explicit allowlist of documented permanent
    rejection markers. Everything else — timeouts, 5xx, connection resets,
    ``Unexpected Meta response``, JSON decode failures, unknown exceptions — is
    intentionally NOT deterministic: the send may have been accepted, so the
    caller must treat it as ``unknown`` rather than ``failed``.
    """
    if is_token_expired_error(err):
        return True
    if is_permanent_meta_template_error(err) or is_text_window_policy_error(err):
        return True
    low = _coerce_error_text(err).lower()
    return any(marker in low for marker in _DETERMINISTIC_REJECTION_MARKERS)


def is_transient_provider_error(err: ErrorLike) -> bool:
    """Return True when a failed Meta send should close the circuit and retry.

    Token expiry, template-validation and 24h-window policy errors are permanent
    and always return False, regardless of any transient hint.
    """
    text = _coerce_error_text(err)
    if is_token_expired_error(text):
        return False
    if is_permanent_meta_template_error(text) or is_text_window_policy_error(text):
        return False

    # Prefer structured fields when the error object carries them
    # (e.g. providers.meta_cloud.MetaCloudError). Plain strings/exceptions
    # simply return None here and fall through to the string parsing below.
    if getattr(err, "is_transient", None) is True:
        return True
    status = getattr(err, "status_code", None)
    if isinstance(status, int) and status in _TRANSIENT_HTTP_STATUS_CODES:
        return True
    meta_code = getattr(err, "meta_code", None)
    if meta_code is not None and str(meta_code).strip() == "2":
        return True

    low = text.lower()
    if _TRANSIENT_HTTP_STATUS_RE.search(low):
        return True
    if _TRANSIENT_FLAG_RE.search(low):
        return True
    if _TRANSIENT_META_CODE_RE.search(low):
        return True
    return any(hint in low for hint in _TRANSIENT_NETWORK_HINTS)


def transient_error_reason(err: ErrorLike) -> tuple[str, str | None]:
    """Return a safe (kind, code) pair describing why an error is transient.

    Never returns raw bodies/PII — only the classification kind and, for HTTP
    or known Meta codes, the numeric code.
    """
    text = _coerce_error_text(err)
    low = text.lower()

    status = getattr(err, "status_code", None)
    if isinstance(status, int) and status in _TRANSIENT_HTTP_STATUS_CODES:
        return "http", str(status)
    m = _TRANSIENT_HTTP_STATUS_RE.search(low)
    if m:
        return "http", m.group(1)

    if getattr(err, "is_transient", None) is True:
        return "is_transient", None
    if _TRANSIENT_FLAG_RE.search(low):
        return "is_transient", None

    meta_code = getattr(err, "meta_code", None)
    if meta_code is not None and str(meta_code).strip() == "2":
        return "meta_code", "2"
    if _TRANSIENT_META_CODE_RE.search(low):
        return "meta_code", "2"

    return "network", None
