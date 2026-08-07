"""What EasyWeek is allowed to do in PR-5, in one place.

Deliberately low-level: this module imports only ``models`` (which imports
nothing from the workers), so every consumer — the EasyWeek inbox worker that
PLANS jobs, the outbox worker that RENDERS them, and the campaign worker that
claims the execution jobs the outbox worker skips — can share one definition
without an import cycle.

Having one definition is the point. The allowlist previously lived as a tuple in
``easyweek_inbox_worker`` and again as a frozenset in ``outbox_worker``; two
copies of a security boundary drift, and the drift is silent until an EasyWeek
job of an unplanned type reaches a consumer that never heard of the restriction
and treats it as Altegio.

Also here: the static booking-page validator. It guards a value that reaches a
customer, and it is needed by the outbox worker without dragging in anything
from the workers package.
"""

from __future__ import annotations

from urllib.parse import urlsplit, urlunsplit

from altegio_bot.models.models import PROVIDER_EASYWEEK

RECORD_CREATED = "record_created"
RECORD_UPDATED = "record_updated"
RECORD_CANCELED = "record_canceled"

# A TEMPLATE CODE, deliberately not a job type.
#
# A first-time customer gets a different approved Meta template, which means a
# different `message_templates` row — but the same `record_created` job. Keeping
# it out of EASYWEEK_LIFECYCLE_JOB_TYPES is what makes that work: the allowlist,
# the domain-scope gate, the param builder and the preflight all key on
# `MessageJob.job_type`, so they keep seeing `record_created` and the seven-field
# contract stays correct. Only the row lookup — and therefore
# `meta_template_name` — differs.
RECORD_CREATED_NEW_CLIENT = "record_created_new_client"

# The ONLY job types EasyWeek may plan, render or send in this phase.
#
# Reminders, review_3d, repeat_10d, comeback_3d, newsletters, follow-up, promo
# and campaigns are Altegio-only for now. That is not an oversight to be relaxed
# by whoever next touches a worker: each of those paths calls something EasyWeek
# has no equivalent for — the Altegio API, an Altegio-keyed BOOKING_LINKS entry,
# a campaign runner built around Altegio client ids.
EASYWEEK_LIFECYCLE_JOB_TYPES: frozenset[str] = frozenset(
    {
        RECORD_CREATED,
        RECORD_UPDATED,
        RECORD_CANCELED,
    }
)


def normalize_provider(value: object | None, *, default: str) -> str:
    """Read a row's ``provider`` the same way everywhere.

    A column default fills NULL on INSERT; it does nothing for a row that
    already exists with an empty string, and nothing at all for a hand-built
    object in a test. So the read is normalized here rather than at each of the
    call sites that would otherwise each invent their own fallback.
    """
    if not isinstance(value, str):
        return default
    return value.strip() or default


def easyweek_job_type_allowed(provider: str, job_type: str) -> bool:
    """True when *job_type* is inside EasyWeek's phase-1 allowlist.

    Non-EasyWeek providers are always allowed through: this function bounds
    EasyWeek, it is not a general-purpose job-type filter.
    """
    if provider != PROVIDER_EASYWEEK:
        return True
    return job_type in EASYWEEK_LIFECYCLE_JOB_TYPES


def easyweek_job_type_error(provider: str, job_type: str) -> str | None:
    """A PII-free reason string, or ``None`` when the job type is allowed."""
    if easyweek_job_type_allowed(provider, job_type):
        return None
    return f"EasyWeek job type not enabled in this phase: {job_type}"


# ---------------------------------------------------------------------------
# Static booking page
# ---------------------------------------------------------------------------

# C0 controls, DEL, the C1 range, the Unicode line/paragraph separators, the
# BOM, and a plain interior space. A newline inside a Meta parameter is
# rejected by the API outright; the rest are the characters that let a URL
# render as one thing and resolve as another. Checked on the WHOLE string
# before parsing, because `urlsplit` silently strips several of them
# (CVE-2023-24329) and would hand back a clean-looking result for a hostile
# input.
_FORBIDDEN_URL_CHARS: frozenset[str] = (
    frozenset(chr(c) for c in range(0x20))
    | frozenset(chr(c) for c in range(0x7F, 0xA0))
    | {" ", "\u2028", "\u2029", "\ufeff"}
)


def booking_page_allowed_hosts() -> frozenset[str]:
    """Hosts the static booking page may live on, from configuration.

    Read at call time rather than import time so an operator can fix a typo and
    recreate the service without a code change, and so tests can set it.

    Empty until the approved Durlach host is confirmed. Empty means "nothing is
    allowed", not "everything is allowed" — see
    :func:`validate_static_booking_page`.
    """
    from altegio_bot.settings import settings

    raw = getattr(settings, "easyweek_booking_page_allowed_hosts", "") or ""
    return frozenset(host.strip().lower() for host in str(raw).split(",") if host.strip())


def validate_static_booking_page(raw: object | None) -> str | None:
    """Return the normalized static booking page, or ``None`` when unusable.

    ``EASYWEEK_BOOKING_PAGE_URL`` is not an internal setting: it is the link a
    customer taps after a cancellation, and the fallback whenever a per-booking
    manage link cannot be re-verified. "Non-empty string" was never enough of a
    check for that — ``javascript:alert(1)``, ``//evil.invalid/book`` and
    ``https://user:pw@host/`` are all non-empty.

    This is deliberately NOT :func:`extract_manage_link`: that validator proves a
    per-booking pair against a booking hash and pins one exact path shape. The
    static page has no hash and no fixed path, so reusing it would either reject
    every legitimate value or have to be loosened until it stopped protecting
    the manage link.

    The host is checked against ``EASYWEEK_BOOKING_PAGE_ALLOWED_HOSTS``, which
    is configuration rather than a constant here: the approved page is a
    property of the location, not of this code, and hardcoding a guess would
    either block the real value or bless the wrong one.

    An EMPTY allowlist rejects everything. That is the point: until an operator
    confirms the approved host, a typo in ``EASYWEEK_BOOKING_PAGE_URL`` would
    otherwise pass every syntactic check and go out as the link a customer taps
    after a cancellation. Failing closed stops the activation; failing open
    would ship the typo. Altegio is unaffected — it never reaches this
    validator.

    The returned string is rebuilt from the parsed components, so an
    unnormalised original never survives into a message.
    """
    if not isinstance(raw, str):
        return None

    candidate = raw.strip()
    if not candidate:
        return None
    if any(ch in _FORBIDDEN_URL_CHARS for ch in candidate):
        return None

    allowed_hosts = booking_page_allowed_hosts()
    if not allowed_hosts:
        return None

    # Every component access is inside the guard: `.port` on
    # "https://host:bad/" or "https://[oops/" raises ValueError lazily, at
    # attribute access, and an untrusted value must never become an exception
    # that escapes into the caller's error path.
    try:
        parts = urlsplit(candidate)
        if parts.scheme != "https":
            return None
        if not parts.hostname:
            return None
        if parts.username or parts.password:
            return None
        if parts.fragment:
            return None
        _ = parts.port
        # `hostname` is already lowercased and port-stripped by urlsplit, and
        # credentials/port are rejected above, so an exact match is enough.
        if parts.hostname not in allowed_hosts:
            return None
    except ValueError:
        return None

    return urlunsplit((parts.scheme, parts.netloc, parts.path, parts.query, ""))
