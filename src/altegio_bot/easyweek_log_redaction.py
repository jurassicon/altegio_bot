"""Keeping EasyWeek request URLs out of the logs, wherever they are made.

`httpx` writes one INFO line per request containing the full URL. For EasyWeek
that URL is not neutral: `GET /customers/{uuid}` names a specific human being,
and `?phone=%2B49...` is the number itself. A container log is not something a
number can be taken back out of.

The CLI already raised those loggers before doing anything. The web application
does not: it runs at INFO, `_silence_url_logging` is private to the canary
script, and copying it into an Ops endpoint would leave two half-rules that
drift apart. So the rule lives here once, and both callers ask for it by name.

Two layers, on purpose
----------------------
The level is lifted to WARNING so the request lines are not emitted at all, and
a scrubbing filter is attached so that a line which *does* get emitted — because
somebody lowered the level again with a debug flag, or a library reconfigured
logging on import — still cannot carry an identifier out.

What may be logged about a request
----------------------------------
The operation, the status, the attempt count, how long it took, and a stable
reason code. Not the URL, not the path, not the query, not a body, and not a
credential. Those are written by our own code at our own level, so nothing an
operator needs is lost by taking the transport's lines away.
"""

from __future__ import annotations

import logging
import re
from typing import Final

# The namespaces that log a request line. `httpx._client` is named explicitly
# rather than relied on to inherit: a handler attached directly to it would not
# see a filter placed only on its parent.
URL_LOGGING_NAMESPACES: Final = (
    "httpx",
    "httpx._client",
    "httpcore",
    "httpcore.http11",
    "httpcore.connection",
)

# A canonical UUID anywhere in a line — path segment, query value or bare word.
# EasyWeek addresses customers, bookings and orders by UUID, and any of them
# identifies a person once it is joined to something else.
_UUID_RE: Final = re.compile(
    r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b",
    re.IGNORECASE,
)
# `phone=` / `email=` query values, percent-encoded or not.
_CONTACT_QUERY_RE: Final = re.compile(r"((?:phone|email)=)[^&\s\"']+", re.IGNORECASE)
# A bare international number, however it reached the line.
_PHONE_RE: Final = re.compile(r"(?:\+|%2B)\d{8,15}", re.IGNORECASE)
# Credentials, in the shapes a header dump or an f-string would produce.
# Everything after the separator goes, up to a real delimiter — not just the
# first token: `Authorization: Bearer <key>` would otherwise keep the key and
# redact the word "Bearer".
_SECRET_RE: Final = re.compile(
    r"((?:authorization|api[-_]?key|x-api-key|workspace[-_]?slug)\s*[:=]\s*)[^,;\n\"']+",
    re.IGNORECASE,
)

REDACTED: Final = "<redacted>"


class EasyWeekUrlLogFilter(logging.Filter):
    """Scrub identifiers and credentials out of any record that carries a URL.

    Rewrites the formatted message rather than dropping the record: that a
    request happened, and what it answered, is exactly the part worth keeping.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            message = record.getMessage()
        except Exception:  # pragma: no cover - a broken record must not block a request
            return True
        scrubbed = _CONTACT_QUERY_RE.sub(rf"\1{REDACTED}", message)
        scrubbed = _SECRET_RE.sub(rf"\1{REDACTED}", scrubbed)
        scrubbed = _UUID_RE.sub(REDACTED, scrubbed)
        scrubbed = _PHONE_RE.sub(REDACTED, scrubbed)
        if scrubbed != message:
            record.msg = scrubbed
            record.args = ()
        return True


def redact_easyweek_url_logging() -> None:
    """Silence and scrub the transport loggers. Idempotent, safe to call often.

    Call it BEFORE the HTTP client is constructed, not merely before the
    request: a client can log while it connects.
    """
    scrubber = EasyWeekUrlLogFilter()
    for name in URL_LOGGING_NAMESPACES:
        target = logging.getLogger(name)
        if target.level < logging.WARNING:
            target.setLevel(logging.WARNING)
        if not any(isinstance(existing, EasyWeekUrlLogFilter) for existing in target.filters):
            target.addFilter(scrubber)


__all__ = [
    "EasyWeekUrlLogFilter",
    "REDACTED",
    "URL_LOGGING_NAMESPACES",
    "redact_easyweek_url_logging",
]
