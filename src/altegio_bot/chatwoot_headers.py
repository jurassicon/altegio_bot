"""Settings-free helpers for the optional Chatwoot X-Forwarded-Proto header.

Kept deliberately free of any app dependency (no Settings, no httpx, no
ChatwootClient): the read-only ops probe imports this module in a minimal
environment that has only the CHATWOOT_* variables, where instantiating
Settings() would fail on unrelated required fields (DATABASE_URL,
ALTEGIO_WEBHOOK_SECRET). App-level callers read the configured value from
settings themselves and pass it in explicitly.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

_ALLOWED_FORWARDED_PROTOS = ("http", "https")


def normalize_forwarded_proto(value: str | None) -> str | None:
    """Validate a CHATWOOT_API_FORWARDED_PROTO value.

    Returns "http"/"https" (trimmed, lower-cased) or None when the header
    must not be sent. Invalid values are ignored with a warning so a typo
    can never silently change request semantics.
    """
    if value is None:
        return None
    cleaned = value.strip().lower()
    if not cleaned:
        return None
    if cleaned in _ALLOWED_FORWARDED_PROTOS:
        return cleaned
    logger.warning(
        "chatwoot: ignoring invalid CHATWOOT_API_FORWARDED_PROTO=%r (expected 'http' or 'https')",
        value,
    )
    return None


def forwarded_proto_header(value: str | None) -> dict[str, str]:
    """Optional X-Forwarded-Proto header for Chatwoot API requests.

    Returns {} when the value is empty/invalid — callers can always merge
    the result into their existing headers.
    """
    proto = normalize_forwarded_proto(value)
    if proto:
        return {"X-Forwarded-Proto": proto}
    return {}
