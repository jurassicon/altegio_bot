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
import re
import uuid
from dataclasses import dataclass
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


def review_run_at(starts_at: datetime) -> datetime:
    """When the review request is due, in UTC."""
    return _as_utc(starts_at) + REVIEW_DELAY


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

    Total and side-effect free, so the planner, the tests and anyone reasoning
    about production all get the same answer. Eligibility that needs the
    database — the category snapshot, the client's opt-out, the record's tenancy
    — is the caller's job; what is decided here is the link, the moment and the
    identity.
    """
    if is_deleted or starts_at is None:
        return None

    url = validate_review_url(review_url, booking_hash_id=booking_hash_id)
    if url is None:
        return None

    run_at = review_run_at(starts_at)
    if run_at <= _as_utc(now):
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
