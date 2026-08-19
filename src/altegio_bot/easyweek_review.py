"""PR-9: the review link, and when a booking has earned a review request.

Two decisions live here, and nowhere else.

**The link.** ``review_url`` is the only trusted source, and it is trusted only
after being proven. Everything else that looks like a link on this booking is
the wrong one: ``Record.short_link`` is the MANAGE link, the registry booking
page is a static storefront, ``GOOGLE_MAPS_REVIEW_LINKS`` belongs to Altegio and
is keyed by an Altegio company id that shares an integer space with EasyWeek's
``location_id``. A review link is tapped by a customer, so a near-miss here is a
customer sent to another salon's review form.

The validator is deliberately NOT the manage-link one. They are different
contracts — ``/f/<hash>`` against ``/r/<hash>`` — and loosening one validator to
cover both would weaken the link that PR-4 already proves. What they DO share is
the hash: EasyWeek's own variable catalogue pairs ``booking_hash_id=40589417``
with ``booking_page=https://eyw.me/r/40589417`` and
``review_url=https://eyw.me/f/40589417``. So the hash inside the review URL must
equal the ``booking_hash_id`` this booking already proved; a review link naming
some other booking is refused rather than sent.

**The moment.** Three days after the appointment started, and only for a booking
that finished. Never earlier, and never retroactively: a review request for a
visit that happened weeks ago is not a reminder that slipped, it is marketing
nobody asked for. So a run time that is already in the past produces no job at
all rather than a late one.

Identity is the business fact — provider, booking, ``review_3d``, and the
appointment's start instant — hashed into a bounded key. A Resend, a second
succeeded delivery with a different payload hash, and a delivery retry all
describe the SAME earned review and must never produce a second message.

Imports nothing from the workers, so both the inbox planner and the outbox guard
can share one definition without a cycle.
"""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Final
from urllib.parse import urlsplit

from altegio_bot.easyweek_normalizer import MAX_BOOKING_HASH_ID_LEN, normalize_booking_hash_id

# How long after the appointment we ask. Three days is the product decision the
# Altegio path already uses; PR-9 keeps it rather than inventing a second one.
REVIEW_DELAY: Final = timedelta(days=3)

REVIEW_URL_SCHEME: Final = "https"
REVIEW_URL_HOST: Final = "eyw.me"
# The review path, and NOT `/r/` — that one is the manage link. Two contracts,
# two validators, on purpose.
REVIEW_URL_PREFIX: Final = "/f/"

# A URL field is attacker-influenced text that ends up in a customer's message.
# Bound it before parsing rather than after.
MAX_REVIEW_URL_LEN: Final = 512

# C0 controls, DEL, the C1 range, the Unicode line/paragraph separators, the
# BOM, and any interior space. Checked on the WHOLE string BEFORE parsing,
# because `urlsplit` silently strips several of them (CVE-2023-24329) and would
# hand back a clean-looking result for a hostile input.
_FORBIDDEN_URL_CHARS: Final[frozenset[str]] = (
    frozenset(chr(c) for c in range(0x20)) | frozenset(chr(c) for c in range(0x7F, 0xA0)) | {" ", " ", " ", "﻿"}
)

# The hash as it appears in the path: the same bounded, non-empty token shape
# `normalize_booking_hash_id` accepts, with no slashes and no dots.
_REVIEW_HASH_RE: Final = re.compile(r"[A-Za-z0-9_-]{1,%d}" % MAX_BOOKING_HASH_ID_LEN)

_KEY_PREFIX: Final = "easyweek_review"


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def validate_review_url(raw: object, *, booking_hash_id: object) -> str | None:
    """The proven review link, or ``None``. Never raises, never guesses.

    ``None`` is a suppression, not an error to work around: without a link we
    can prove, there is no review request to send.

    The returned string is rebuilt from validated components, so an
    unnormalised original can never survive into a message.
    """
    expected_hash = normalize_booking_hash_id(booking_hash_id)
    if expected_hash is None:
        # The booking never proved a hash, so nothing can be checked against it.
        return None

    if not isinstance(raw, str):
        return None
    # Trimming is allowed only at the edges; an interior space is refused below.
    candidate = raw.strip()
    if not candidate or len(candidate) > MAX_REVIEW_URL_LEN:
        return None
    if any(ch in _FORBIDDEN_URL_CHARS for ch in candidate):
        return None
    # Checked on the raw text, not on the parsed components: a trailing "?" or
    # "#" parses to an EMPTY query/fragment, which is falsy, so the component
    # tests below would wave it through. The documented review URL has neither.
    if "?" in candidate or "#" in candidate:
        return None

    # Every component access sits inside the guard: `.port` on
    # "https://eyw.me:bad/f/1" or "https://[oops/f/1" raises ValueError lazily,
    # at attribute access, and untrusted input must not become an exception in
    # the caller's error path.
    try:
        parts = urlsplit(candidate)
        if parts.scheme != REVIEW_URL_SCHEME:
            return None
        if parts.hostname != REVIEW_URL_HOST:
            return None
        if parts.username or parts.password:
            return None
        if parts.query or parts.fragment:
            return None
        # None (no port) and an explicit :443 are the same https origin; any
        # other port is a different service behind the same name.
        if parts.port is not None and parts.port != 443:
            return None
        path = parts.path
    except ValueError:
        return None

    if not path.startswith(REVIEW_URL_PREFIX):
        return None
    url_hash = path[len(REVIEW_URL_PREFIX) :]
    if not _REVIEW_HASH_RE.fullmatch(url_hash):
        return None
    if url_hash != expected_hash:
        # A well-formed review link for a DIFFERENT booking. Sending it would
        # ask this customer to review someone else's appointment.
        return None

    return f"{REVIEW_URL_SCHEME}://{REVIEW_URL_HOST}{REVIEW_URL_PREFIX}{url_hash}"


# ── PR-10: the link comes from OUR configuration, not from the payload ─────
#
# Production proved the premise wrong: a real `booking-succeeded` payload for
# the Durlach branch carries 88 root keys and `review_url`
# is not among them. `plan_review` therefore always returned None and PR-9
# could never fire. Revision 12 of the canonical plan authorises the change of
# SOURCE — and only the source. Everything else about PR-9 stands.
#
# The link is a Google review link, chosen by EasyWeek `company_id`, and it
# lives in its own variable. NOT in `EASYWEEK_LOCATION_MAP`, which gates
# lifecycle and reminders as a whole: a typo in a review link must not take
# booking confirmations down with it. NOT in `GOOGLE_MAPS_REVIEW_LINKS`, which
# is keyed by an Altegio company id sharing an integer space with EasyWeek's
# `location_id` — mixing them breaks provider isolation.

GOOGLE_REVIEW_SCHEME: Final = "https"
GOOGLE_REVIEW_HOST: Final = "g.page"
GOOGLE_REVIEW_PREFIX: Final = "/r/"
GOOGLE_REVIEW_SUFFIX: Final = "/review"

# The opaque place token between them. Bounded, and deliberately the same
# character class the rest of this module accepts.
_GOOGLE_TOKEN_RE: Final = re.compile(r"[A-Za-z0-9_-]{1,128}")

REVIEW_LINK_MISSING: Final = "review_link_missing"
REVIEW_LINK_INVALID: Final = "review_link_invalid"
REVIEW_LINK_CHANGED: Final = "review_link_changed"
REVIEW_LINKS_UNCONFIGURED: Final = "review_links_unconfigured"
REVIEW_LINKS_INVALID: Final = "review_links_invalid"


def validate_google_review_url(raw: object) -> str | None:
    """The proven Google review link, or ``None``. Never raises.

    Same discipline as :func:`validate_review_url`, minus the booking hash: a
    Google link identifies the SALON, not the booking, so there is nothing on
    the appointment to compare it against. What replaces that proof is the
    strictness of everything else — one host, one path shape, no query, no
    fragment, no credentials, no odd port.

    Only ``https://g.page/r/<token>/review`` is accepted. Other Google review
    forms (``search.google.com/local/writereview?placeid=…``) need a query
    string, which this contract forbids outright; supporting them is a separate
    plan change, not a loosened validator.

    The result is rebuilt from validated components, so an unnormalised
    original can never survive into a customer's message.
    """
    if not isinstance(raw, str):
        return None
    # Deliberately NOT trimmed. This value comes from our own configuration, so
    # stray whitespace is an operator typo worth surfacing, not something to
    # paper over — and trimming first would swallow a trailing TAB before the
    # control-character check below ever saw it.
    candidate = raw
    if not candidate or candidate != candidate.strip():
        return None
    if len(candidate) > MAX_REVIEW_URL_LEN:
        return None
    if any(ch in _FORBIDDEN_URL_CHARS for ch in candidate):
        return None
    # On the RAW text: a trailing "?" or "#" parses to an empty component,
    # which is falsy, so the component checks below would wave it through. PR-9
    # already shipped that bug once.
    if "?" in candidate or "#" in candidate:
        return None

    try:
        parts = urlsplit(candidate)
        if parts.scheme != GOOGLE_REVIEW_SCHEME:
            return None
        # `hostname` is already lowercased and IDNA-decoded by urlsplit; an
        # exact ASCII match rejects both `g.page.evil.com` and a Cyrillic
        # homoglyph that merely looks like `g.page`.
        if parts.hostname != GOOGLE_REVIEW_HOST:
            return None
        if parts.username or parts.password:
            return None
        if parts.query or parts.fragment:
            return None
        if parts.port is not None and parts.port != 443:
            return None
        path = parts.path
    except ValueError:
        return None

    if not path.startswith(GOOGLE_REVIEW_PREFIX) or not path.endswith(GOOGLE_REVIEW_SUFFIX):
        return None
    token = path[len(GOOGLE_REVIEW_PREFIX) : -len(GOOGLE_REVIEW_SUFFIX)]
    if not _GOOGLE_TOKEN_RE.fullmatch(token):
        # Catches the empty token, an extra path segment (the "/" is not in the
        # class) and any character outside it.
        return None

    return f"{GOOGLE_REVIEW_SCHEME}://{GOOGLE_REVIEW_HOST}{GOOGLE_REVIEW_PREFIX}{token}{GOOGLE_REVIEW_SUFFIX}"


@dataclass(frozen=True)
class GoogleReviewLinks:
    """Total parse result; invalid input never degrades to a partial map."""

    configured: bool
    valid: bool
    links: dict[int, str] = field(default_factory=dict)

    @property
    def ready(self) -> bool:
        return self.configured and self.valid and bool(self.links)

    @property
    def unavailable_reason(self) -> str | None:
        if not self.configured:
            return REVIEW_LINKS_UNCONFIGURED
        if not self.valid:
            return REVIEW_LINKS_INVALID
        return None


def parse_google_review_links(raw: object) -> GoogleReviewLinks:
    """Parse ``EASYWEEK_GOOGLE_REVIEW_LINKS``. Never raises.

    Fail-closed and all-or-nothing, like ``parse_allowed_service_categories``:
    one bad entry invalidates the whole map rather than silently shrinking it.
    A half-parsed map is the dangerous case — the branch whose entry was
    dropped would look like "no link configured" and go quietly unreviewed.
    """
    if raw is None:
        return GoogleReviewLinks(configured=False, valid=True)
    if not isinstance(raw, str):
        return GoogleReviewLinks(configured=True, valid=False)
    text = raw.strip()
    # `{}` and `{ }` are one intention written two ways; both mean "nothing
    # configured here yet", not "configured wrongly".
    if not text or "".join(text.split()) == "{}":
        return GoogleReviewLinks(configured=False, valid=True)

    # The hook fires ONLY for JSON objects, and it sees the duplicate keys
    # json.loads would otherwise silently collapse — two entries for one branch
    # mean the operator disagrees with themselves about where customers go.
    #
    # It is wrapped in a marker rather than returning the bare pair list: a
    # top-level JSON ARRAY never reaches the hook and would otherwise arrive
    # here as an ordinary list, indistinguishable from parsed pairs, and blow up
    # on unpacking. A parser fed untrusted configuration must not raise.
    def _object_marker(items: list[tuple[str, object]]) -> tuple[str, list[tuple[str, object]]]:
        return ("object", items)

    try:
        parsed = json.loads(text, object_pairs_hook=_object_marker)
    except (ValueError, TypeError, RecursionError):
        return GoogleReviewLinks(configured=True, valid=False)

    if not (isinstance(parsed, tuple) and len(parsed) == 2 and parsed[0] == "object"):
        # A JSON array, string, number or null — not the object this is.
        return GoogleReviewLinks(configured=True, valid=False)
    pairs: list[tuple[str, object]] = parsed[1]

    links: dict[int, str] = {}
    for key, value in pairs:
        company_id = _canonical_company_id(key)
        if company_id is None:
            return GoogleReviewLinks(configured=True, valid=False)
        if company_id in links:
            return GoogleReviewLinks(configured=True, valid=False)
        url = validate_google_review_url(value)
        if url is None:
            return GoogleReviewLinks(configured=True, valid=False)
        links[company_id] = url

    if not links:
        return GoogleReviewLinks(configured=True, valid=False)
    return GoogleReviewLinks(configured=True, valid=True, links=links)


def _canonical_company_id(key: object) -> int | None:
    """A positive integer company id, written as a JSON object key."""
    if isinstance(key, bool):
        return None
    if isinstance(key, int):
        value = key
    elif isinstance(key, str):
        candidate = key.strip()
        # ASCII digits only, and no sign: `str.isdigit()` accepts Arabic-Indic
        # and other Unicode digits, and a leading `+` would silently make a
        # second valid key for the same branch written differently. Softer than the
        # fail-closed discipline around it.
        if not candidate or not candidate.isascii() or not candidate.isdecimal():
            return None
        try:
            value = int(candidate)
        except ValueError:
            return None
    else:
        return None
    return value if value > 0 else None


def google_review_url_for_company(company_id: object, raw_config: object) -> tuple[str | None, str | None]:
    """``(url, reason)`` for this branch. Exactly one side is ever set.

    The reason codes are stable and carry no customer data, so they are safe in
    an event error, a log line and a preflight report alike.
    """
    parsed = parse_google_review_links(raw_config)
    if not parsed.configured or not parsed.valid:
        return None, parsed.unavailable_reason or REVIEW_LINKS_INVALID

    canonical = _canonical_company_id(company_id)
    if canonical is None:
        return None, REVIEW_LINK_MISSING
    url = parsed.links.get(canonical)
    if url is None:
        return None, REVIEW_LINK_MISSING
    return url, None


def review_run_at(starts_at: datetime) -> datetime:
    """When the review request is due, in UTC."""
    return _as_utc(starts_at) + REVIEW_DELAY


def review_moment_passed(starts_at: datetime, now: datetime) -> bool:
    """Is it already too late for this booking to earn a review at all?

    THE single definition, so the planner's "keep waiting for a fixed config"
    window and ``plan_review``'s own refusal cannot drift apart. Past this
    instant no configuration change can produce a job — asking for a review of
    a visit three days gone is unsolicited marketing, not a late reminder — so
    holding the event any longer only blocks its booking's lifecycle.
    """
    return review_run_at(starts_at) <= _as_utc(now)


def easyweek_review_dedupe_key(*, booking_uuid: uuid.UUID, starts_at: datetime) -> str:
    """Stable identity for "this booking, this appointment, one review".

    Keyed on the business fact rather than on the delivery: two succeeded
    payloads with different hashes describe the same earned review, and a
    delivery retry is a second attempt at one message, not a second message.
    """
    material = "|".join((_KEY_PREFIX, str(booking_uuid), "review_3d", _as_utc(starts_at).isoformat()))
    digest = hashlib.sha256(material.encode("utf-8")).hexdigest()[:40]
    return f"{_KEY_PREFIX}:review_3d:{digest}"


@dataclass(frozen=True)
class PlannedReview:
    """One earned review request: when it fires, and how it is identified."""

    run_at: datetime
    dedupe_key: str
    review_url: str


def plan_review(
    *,
    booking_uuid: uuid.UUID,
    starts_at: datetime | None,
    now: datetime,
    review_url: object,
    booking_hash_id: object,
    is_deleted: bool = False,
) -> PlannedReview | None:
    """The review this finished booking owes, or ``None``.

    ``review_url`` is the Google link resolved from
    ``EASYWEEK_GOOGLE_REVIEW_LINKS`` for this branch — NOT a payload field.
    EasyWeek does not send one (a real production record has 88 root keys and no
    ``review_url``), which is why PR-9 never fired.

    Total and side-effect free, so the planner, the tests and anyone reasoning
    about production all get the same answer. Eligibility that needs the
    database — the category snapshot, the client's opt-out, the record's tenancy
    — is the caller's job; what is decided here is the link, the moment and the
    identity.
    """
    if is_deleted or starts_at is None:
        return None

    # PR-10: the hash no longer sources the link, but it still has to exist.
    # It is how this booking proved its identity in PR-4, and a review request
    # for a booking we cannot identify has nothing to stand on.
    if normalize_booking_hash_id(booking_hash_id) is None:
        return None

    # The link now comes from our configuration, already resolved by the
    # caller; it is re-validated here so no path can plan an unproven URL.
    url = validate_google_review_url(review_url)
    if url is None:
        return None

    run_at = review_run_at(starts_at)
    if review_moment_passed(starts_at, now):
        # Not "slightly late": the appointment is at least three days gone, and
        # a review request for it now is unsolicited marketing.
        return None

    return PlannedReview(
        run_at=run_at,
        dedupe_key=easyweek_review_dedupe_key(booking_uuid=booking_uuid, starts_at=starts_at),
        review_url=url,
    )


def review_job_payload(
    *,
    booking_uuid: uuid.UUID,
    company_id: int,
    starts_at: datetime,
    review_url: str,
    source_event_id: int | None = None,
    source_payload_hash: str | None = None,
) -> dict[str, Any]:
    """The minimal technical payload a review job carries.

    No name, no phone, no e-mail, no service text, no price, no webhook body:
    everything customer-facing is re-read and re-proven at send time, and a
    payload is where data goes to rot.

    ``record_starts_at`` is the immutable instant this review was earned for. At
    send time it is compared against the current Record, so a booking that moved
    cannot deliver a review request for an appointment that no longer happened
    when we thought it did. The two source markers are audit only — they are
    never used to decide anything.
    """
    payload: dict[str, Any] = {
        "provider": "easyweek",
        "company_id": company_id,
        "booking_uuid": str(booking_uuid),
        "record_starts_at": _as_utc(starts_at).isoformat(),
        "review_url": review_url,
        "job_type": "review_3d",
    }
    if source_event_id is not None:
        payload["source_event_id"] = source_event_id
    if source_payload_hash is not None:
        payload["source_payload_hash"] = source_payload_hash
    return payload
