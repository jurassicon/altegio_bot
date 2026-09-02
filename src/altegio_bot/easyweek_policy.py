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

REMINDER_24H = "reminder_24h"
REMINDER_2H = "reminder_2h"

REVIEW_3D = "review_3d"

# PR-12. The two retention messages, and deliberately only those two.
REPEAT_10D = "repeat_10d"
COMEBACK_3D = "comeback_3d"

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

# The lifecycle job types: one notification per webhook delivery.
#
# Deliberately UNCHANGED by PR-8. Several gates key on this exact set — the
# seven-field param contract, the domain-scope check, the record loader — and
# widening it to include reminders would silently apply lifecycle rules to a job
# that has different ones (a reminder has a run_at in the future, a stale check,
# and a mandatory API guard).
EASYWEEK_LIFECYCLE_JOB_TYPES: frozenset[str] = frozenset(
    {
        RECORD_CREATED,
        RECORD_UPDATED,
        RECORD_CANCELED,
    }
)

# PR-8. Time-triggered rather than delivery-triggered, which is what makes them
# a separate set: they are planned once and sent hours or days later, so what
# was true at planning time has to be re-proven against the live EasyWeek API
# before the message goes out.
EASYWEEK_REMINDER_JOB_TYPES: frozenset[str] = frozenset(
    {
        REMINDER_24H,
        REMINDER_2H,
    }
)

# Everything EasyWeek may send TO A CUSTOMER. The allowlist below is keyed on
# this union so a new EasyWeek notification kind has exactly one place to be
# registered.
#
# Newsletters, follow-up, promo and campaigns remain Altegio-only. That is not
# an oversight to be relaxed by whoever next touches a worker: each of those
# paths calls something EasyWeek has no equivalent for — the Altegio API, an
# Altegio-keyed BOOKING_LINKS entry, a campaign runner built around Altegio
# client ids.
# PR-9. Earned by a proven `booking-succeeded`, sent three days after the
# appointment. Its own set because it is marketing rather than a lifecycle
# notification: it carries a different param contract, a different link and its
# own send fence, and it must never inherit the reminder or lifecycle rules.
EASYWEEK_REVIEW_JOB_TYPES: frozenset[str] = frozenset({REVIEW_3D})

# PR-12. Retention: the two messages that ask a customer to come back.
#
# Its OWN set, exactly like the review one, and for the same reason: these two
# are gated by their own planning flag and their own send fence, they carry
# their own param contracts, and their eligibility is decided by the proven
# `Client.easyweek_visits_total` counter rather than by anything the reminder or
# lifecycle rules know about.
#
# The set is closed at two members on purpose. Newsletters, newsletter
# follow-up, promo and campaign execution are NOT retention and must not be
# added here to "reuse the fence": each of them reaches an Altegio API, an
# Altegio-keyed link map or a campaign runner built around Altegio client ids,
# and a wider set here would silently let one of them through every EasyWeek
# gate downstream.
EASYWEEK_RETENTION_JOB_TYPES: frozenset[str] = frozenset({REPEAT_10D, COMEBACK_3D})

# The job types whose rendering DEPENDS on a complete service snapshot: a single
# RecordService with a title, a price, and a Record.total_cost that agrees with
# it. Lifecycle messages print the service line and the total; reminders print
# the service line. Both are fail-closed on an unknown value, because rendering
# flattens `None` into the literal "None" and an unknown price into "0.00".
#
# Stated as a POSITIVE allowlist rather than "everything except review". A
# negative test is fail-OPEN: the next EasyWeek job type added to the customer
# set would silently skip a guard it may well need. A type has to be listed here
# to get the guard, and listing it is a deliberate act.
#
# review_3d is deliberately absent. It renders exactly two parameters — the
# customer's name and the proven review link — so a booking with an unknown
# price still owes a perfectly sendable review, and failing it on a price it
# never prints would lose the review outright.
#
# The PR-12 retention types are absent for the same reason, and the reason is
# worth stating because `repeat_10d` DOES print a service. It prints the service
# TITLE and nothing else: no price, no total. So the price half of this guard
# would fail a perfectly sendable retention message over a number it never
# renders. The title itself is proven instead by the retention pre-send guard,
# which requires exactly one service carrying a non-blank one.
EASYWEEK_SERVICE_SNAPSHOT_JOB_TYPES: frozenset[str] = EASYWEEK_LIFECYCLE_JOB_TYPES | EASYWEEK_REMINDER_JOB_TYPES

EASYWEEK_CUSTOMER_JOB_TYPES: frozenset[str] = (
    EASYWEEK_LIFECYCLE_JOB_TYPES
    | EASYWEEK_REMINDER_JOB_TYPES
    | EASYWEEK_REVIEW_JOB_TYPES
    | EASYWEEK_RETENTION_JOB_TYPES
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
    """True when *job_type* is inside EasyWeek's customer-notification allowlist.

    Non-EasyWeek providers are always allowed through: this function bounds
    EasyWeek, it is not a general-purpose job-type filter.
    """
    if provider != PROVIDER_EASYWEEK:
        return True
    return job_type in EASYWEEK_CUSTOMER_JOB_TYPES


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

    A registry ``booking_page_url`` is the link a customer taps after a
    cancellation, and the fallback whenever a per-booking manage link cannot be
    re-verified. "Non-empty string" was never enough of a
    check for that — ``javascript:alert(1)``, ``//evil.invalid/book`` and
    ``https://user:pw@host/`` are all non-empty.

    This is deliberately NOT :func:`extract_manage_link`: that validator proves a
    per-booking pair against a booking hash and pins one exact path shape. The
    static page has no hash and no fixed path, so reusing it would either reject
    every legitimate value or have to be loosened until it stopped protecting
    the manage link.

    The ORIGIN is checked against ``EASYWEEK_BOOKING_PAGE_ALLOWED_HOSTS``, which
    is configuration rather than a constant here: the approved page is a
    property of the location, not of this code, and hardcoding a guess would
    either block the real value or bless the wrong one.

    Origin, not just hostname: a URL with no port and one with an explicit
    ``:443`` are the same https origin and are both accepted, but any other port
    is refused. ``https://allowed.host:4443/`` is a different service behind the
    same name, and this value ends up as a link a customer taps.

    An EMPTY allowlist rejects everything. That is the point: until an operator
    confirms the approved host, a typo in a registry booking page would
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
        # The allowlist is an ORIGIN check, and an origin is scheme + host +
        # PORT. `hostname` has the port stripped off, so matching on it alone
        # let `https://allowed.host:4443/` through — a different port is a
        # different service, and this link is what a customer taps after a
        # cancellation.
        #
        # `None` (no port given) and an explicit `:443` are the same origin for
        # https and are both accepted; anything else is refused. Accepting the
        # redundant `:443` costs nothing and avoids rejecting a URL that is
        # literally equivalent to the allowed one.
        if parts.port is not None and parts.port != 443:
            return None
        # `hostname` is already lowercased by urlsplit, and credentials and any
        # non-443 port are rejected above, so an exact match is enough.
        if parts.hostname not in allowed_hosts:
            return None
    except ValueError:
        return None

    return urlunsplit((parts.scheme, parts.netloc, parts.path, parts.query, ""))
