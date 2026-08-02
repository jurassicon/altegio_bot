"""EasyWeek webhook payload → validated domain intent.

Pure, synchronous and side-effect free: it takes one captured ``easyweek_events``
row and either returns a fully validated :class:`NormalizedBooking`, or raises
:class:`NormalizationError` carrying a stable, PII-free error code.

Design rules come from ``docs/easyweek/INTEGRATION_PLAN.md`` §1.6 and are
deliberately stricter than the payload:

* **UUID-first.** Root ``uid`` is the authoritative booking identity. ``id``,
  ``booking_hash_id`` and ``location_uuid`` do not substitute for it.
* **Location isolation.** The numeric ``location_id`` is compared against the
  operator-configured location. A foreign location is rejected outright — the
  payload never gets to choose which location this bot owns.
* **Fail-closed manage links.** A URL is never synthesised. Only the exact pair
  ``booking_page`` + ``booking_hash_id`` forming ``https://eyw.me/r/<hash>`` is
  trusted; anything else clears the stored link rather than keeping an unproven
  one.
* **No prose parsing.** The event type comes from the ``event_hint`` recorded in
  our own URL, never from the localized ``booking_status``.

The payload shape is taken from real captured deliveries: every field lives at
the ROOT of the object, and some root keys literally contain dots
(``booking_attributes.booking_comment``) — they are flat keys, not nested
objects. Numbers (``id``, ``customer_id``, ``location_id``) arrive as JSON
numbers; ``uid``, ``booking_hash_id``, ``booking_page`` and the timestamps
arrive as strings.

Nothing here logs, and no error code ever embeds a payload value.
"""

from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Final
from urllib.parse import urlsplit

# ---------------------------------------------------------------------------
# Event mapping — exact trigger names only
# ---------------------------------------------------------------------------

CREATE: Final = "create"
UPDATE: Final = "update"
DELETE: Final = "delete"
IGNORE: Final = "ignore"

# The five real trigger names, confirmed by live capture. Short aliases
# ("created", "updated", "canceled") and our own internal verbs ("create",
# "update", "delete") are NOT accepted: early capture rows contain synthetic
# hints from smoke tests, and silently treating them as real bookings would
# process rows that never came from a real EasyWeek delivery.
_EVENT_HINT_MAP: Final[dict[str, str]] = {
    "booking-created": CREATE,
    "booking-updated": UPDATE,
    "booking-rescheduled": UPDATE,
    "booking-canceled": DELETE,
    # Captured for phase 2 (visits_total / review guard). Reaches a terminal
    # status with no Client, Record or MessageJob side effect.
    "booking-succeeded": IGNORE,
}

MANAGE_LINK_SCHEME: Final = "https"
MANAGE_LINK_HOST: Final = "eyw.me"
MANAGE_LINK_PREFIX: Final = "/r/"

# Mirrors records.easyweek_booking_hash_id (String(64)).
MAX_BOOKING_HASH_ID_LEN: Final = 64


class NormalizationError(Exception):
    """A deterministic, non-retryable rejection carrying a safe code.

    The code is a fixed identifier from the list below — never a payload value
    and never a driver/database exception string, both of which can contain a
    customer's phone, e-mail or name.
    """

    # Every code the normalizer can produce.
    INVALID_EVENT_HINT: Final = "invalid_event_hint"
    INVALID_PAYLOAD: Final = "invalid_payload"
    TRUNCATED_PAYLOAD: Final = "truncated_payload"
    MISSING_BOOKING_UUID: Final = "missing_booking_uuid"
    INVALID_BOOKING_UUID: Final = "invalid_booking_uuid"
    MISSING_BOOKING_ID: Final = "missing_booking_id"
    INVALID_LOCATION_ID: Final = "invalid_location_id"
    FOREIGN_LOCATION: Final = "foreign_location"
    INVALID_DATETIME: Final = "invalid_datetime"
    INVALID_MANAGE_LINK: Final = "invalid_manage_link"

    ALL_CODES: Final = frozenset(
        {
            INVALID_EVENT_HINT,
            INVALID_PAYLOAD,
            TRUNCATED_PAYLOAD,
            MISSING_BOOKING_UUID,
            INVALID_BOOKING_UUID,
            MISSING_BOOKING_ID,
            INVALID_LOCATION_ID,
            FOREIGN_LOCATION,
            INVALID_DATETIME,
            INVALID_MANAGE_LINK,
        }
    )

    def __init__(self, code: str) -> None:
        if code not in self.ALL_CODES:
            raise ValueError(f"unknown normalization error code: {code!r}")
        self.code = code
        # The message is the bare code: it ends up in logs, so it must not be
        # able to carry payload content.
        super().__init__(code)


@dataclass(frozen=True)
class ManageLink:
    """A manage link whose provenance was proven by the page/hash pair."""

    url: str
    hash_id: str


@dataclass(frozen=True)
class NormalizedBooking:
    """Everything the worker needs, already validated."""

    action: str  # CREATE | UPDATE | DELETE
    booking_uuid: uuid.UUID
    booking_id: int
    customer_id: int | None
    company_id: int
    starts_at: datetime | None
    ends_at: datetime | None
    duration_sec: int | None
    phone_e164: str | None
    display_name: str | None
    email: str | None
    staff_name: str | None
    comment: str | None
    service_id: int | None
    service_name: str | None
    # None means "no proven link in this delivery". Whether that clears the
    # stored link or preserves it is decided by ``manage_link_present``.
    manage_link: ManageLink | None
    # True when the delivery carried BOTH link fields (valid or not). A delivery
    # with neither field keeps the last proven link; a delivery that carried
    # them but failed validation must not leave a stale link in place.
    manage_link_present: bool


def map_event_hint(event_hint: str | None) -> str:
    """Return CREATE/UPDATE/DELETE/IGNORE for an exact EasyWeek trigger name."""
    if not isinstance(event_hint, str):
        raise NormalizationError(NormalizationError.INVALID_EVENT_HINT)
    action = _EVENT_HINT_MAP.get(event_hint.strip())
    if action is None:
        raise NormalizationError(NormalizationError.INVALID_EVENT_HINT)
    return action


def _require_int(value: Any, *, code: str) -> int:
    """Accept only a real JSON number (or an exact integral value).

    ``True``/``False`` are rejected: ``bool`` is an ``int`` subclass in Python,
    and a boolean silently becoming id ``1`` would attach a booking to whichever
    row happens to own that id.
    """
    if isinstance(value, bool) or value is None:
        raise NormalizationError(code)
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    raise NormalizationError(code)


def _optional_str(value: Any, *, limit: int | None = None) -> str | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    return text[:limit] if limit else text


def parse_iso_utc(value: Any, *, required: bool) -> datetime | None:
    """Parse an EasyWeek ISO timestamp into an aware UTC datetime.

    EasyWeek sends normal ISO-8601 with a real offset (``+0000`` / ``+02:00``),
    so the Altegio workaround for malformed offsets and its Europe/Belgrade
    assumption are deliberately NOT applied here.
    """
    if value is None or not isinstance(value, str) or not value.strip():
        if required:
            raise NormalizationError(NormalizationError.INVALID_DATETIME)
        return None

    text = value.strip()
    # `fromisoformat` accepts "+02:00" but not the compact "+0000" EasyWeek
    # uses for UTC, so normalise the compact form first.
    if len(text) >= 5 and (text[-5] in "+-") and text[-4:].isdigit():
        text = f"{text[:-5]}{text[-5]}{text[-4:-2]}:{text[-2:]}"
    elif text.endswith("Z"):
        text = f"{text[:-1]}+00:00"

    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        raise NormalizationError(NormalizationError.INVALID_DATETIME) from None

    if parsed.tzinfo is None:
        # A naive timestamp has no defensible interpretation here: guessing the
        # location's zone would silently shift every reminder by an hour.
        raise NormalizationError(NormalizationError.INVALID_DATETIME)
    return parsed.astimezone(timezone.utc)


def normalize_booking_hash_id(value: Any) -> str | None:
    """Return the hash as a bounded string, or None when unusable.

    Kept a string on purpose: it is the number from the manage link, and
    treating it as an integer would assume a purely numeric format and destroy
    leading zeros.
    """
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        text = str(value)
    elif isinstance(value, str):
        text = value.strip()
    else:
        return None
    if not text or len(text) > MAX_BOOKING_HASH_ID_LEN:
        return None
    return text


def extract_manage_link(payload: dict[str, Any]) -> tuple[ManageLink | None, bool]:
    """Validate the ``booking_page`` + ``booking_hash_id`` pair.

    Returns ``(link, present)``. ``present`` is True when the delivery carried
    either field at all — the caller uses it to tell "this event says nothing
    about the link" (keep the stored one) from "this event carried a link that
    did not verify" (clear the stored one).

    A URL is never synthesised from ``uid`` or from the hash. Only the exact
    shape ``https://eyw.me/r/<hash>`` is trusted, and the hash inside the URL
    must equal the normalized ``booking_hash_id``.
    """
    raw_page = payload.get("booking_page")
    raw_hash = payload.get("booking_hash_id")
    present = raw_page is not None or raw_hash is not None
    if not present:
        return None, False

    hash_id = normalize_booking_hash_id(raw_hash)
    if hash_id is None or not isinstance(raw_page, str):
        return None, True

    page = raw_page.strip()
    if not page:
        return None, True

    try:
        parts = urlsplit(page)
    except ValueError:
        return None, True

    if parts.scheme != MANAGE_LINK_SCHEME:
        return None, True
    # `hostname` lowercases and strips the port; comparing it to the bare host
    # while separately rejecting `port`/credentials closes the
    # "https://eyw.me:1234@evil/" family.
    if parts.hostname != MANAGE_LINK_HOST:
        return None, True
    if parts.port is not None or parts.username or parts.password:
        return None, True
    if parts.query or parts.fragment:
        return None, True
    if not parts.path.startswith(MANAGE_LINK_PREFIX):
        return None, True

    url_hash = parts.path[len(MANAGE_LINK_PREFIX) :]
    if url_hash != hash_id:
        return None, True

    # Rebuild rather than echo the input, so no unnormalised original survives.
    return ManageLink(url=f"https://{MANAGE_LINK_HOST}{MANAGE_LINK_PREFIX}{hash_id}", hash_id=hash_id), True


def _customer_phone(payload: dict[str, Any]) -> str | None:
    """E.164-ish normalisation. Never logged, never echoed into an error."""
    raw = payload.get("customer_phone")
    if not isinstance(raw, str):
        return None
    digits = "".join(ch for ch in raw if ch.isdigit())
    if not digits:
        return None
    return f"+{digits}"[:32]


def _display_name(payload: dict[str, Any]) -> str | None:
    for key in ("customer_full_name", "customer_name"):
        name = _optional_str(payload.get(key), limit=256)
        if name:
            return name
    first = _optional_str(payload.get("customer_first_name"))
    last = _optional_str(payload.get("customer_last_name"))
    joined = " ".join(part for part in (first, last) if part)
    return joined[:256] if joined else None


def normalize_event(
    *,
    event_hint: str | None,
    payload: Any,
    body_truncated: bool,
    expected_location_id: int,
) -> NormalizedBooking | None:
    """Validate one captured delivery.

    Returns ``None`` for ``booking-succeeded`` (terminal, no side effects).
    Raises :class:`NormalizationError` for every deterministic rejection.
    """
    action = map_event_hint(event_hint)
    if action == IGNORE:
        return None

    # A truncated body is not a payload we may reason about: the missing tail
    # could contain the very fields we validate.
    if body_truncated:
        raise NormalizationError(NormalizationError.TRUNCATED_PAYLOAD)
    if not isinstance(payload, dict) or not payload:
        raise NormalizationError(NormalizationError.INVALID_PAYLOAD)

    # The operator's configured location decides ownership, never the payload.
    if not isinstance(expected_location_id, int) or expected_location_id <= 0:
        raise NormalizationError(NormalizationError.INVALID_LOCATION_ID)
    location_id = _require_int(payload.get("location_id"), code=NormalizationError.INVALID_LOCATION_ID)
    if location_id != expected_location_id:
        raise NormalizationError(NormalizationError.FOREIGN_LOCATION)

    raw_uid = payload.get("uid")
    if raw_uid is None or (isinstance(raw_uid, str) and not raw_uid.strip()):
        raise NormalizationError(NormalizationError.MISSING_BOOKING_UUID)
    if not isinstance(raw_uid, str):
        raise NormalizationError(NormalizationError.INVALID_BOOKING_UUID)
    try:
        booking_uuid = uuid.UUID(raw_uid.strip())
    except (ValueError, AttributeError, TypeError):
        raise NormalizationError(NormalizationError.INVALID_BOOKING_UUID) from None

    booking_id = _require_int(payload.get("id"), code=NormalizationError.MISSING_BOOKING_ID)

    customer_id: int | None
    if payload.get("customer_id") is None:
        customer_id = None
    else:
        customer_id = _require_int(payload.get("customer_id"), code=NormalizationError.INVALID_PAYLOAD)

    # `booking_date_start` is the machine-readable source of truth; the
    # `*_formatted`, `date` and `time` fields are localized display strings.
    starts_at = parse_iso_utc(payload.get("booking_date_start"), required=False)
    ends_at = parse_iso_utc(payload.get("booking_date_end"), required=False)

    duration_sec: int | None = None
    raw_duration = payload.get("booking_duration")
    if raw_duration is not None and not isinstance(raw_duration, bool):
        if isinstance(raw_duration, int) or (isinstance(raw_duration, float) and raw_duration.is_integer()):
            minutes = int(raw_duration)
            if minutes >= 0:
                duration_sec = minutes * 60

    service_id: int | None = None
    raw_service_id = payload.get("service_id")
    if raw_service_id is not None and not isinstance(raw_service_id, bool):
        if isinstance(raw_service_id, int) or (isinstance(raw_service_id, float) and raw_service_id.is_integer()):
            service_id = int(raw_service_id)

    manage_link, manage_link_present = extract_manage_link(payload)

    return NormalizedBooking(
        action=action,
        booking_uuid=booking_uuid,
        booking_id=booking_id,
        customer_id=customer_id,
        company_id=expected_location_id,
        starts_at=starts_at,
        ends_at=ends_at,
        duration_sec=duration_sec,
        phone_e164=_customer_phone(payload),
        display_name=_display_name(payload),
        email=_optional_str(payload.get("customer_email"), limit=256),
        staff_name=_optional_str(payload.get("users_description"), limit=256)
        or _optional_str(payload.get("user_name"), limit=256),
        comment=_optional_str(payload.get("booking_attributes.booking_comment")),
        service_id=service_id,
        service_name=_optional_str(payload.get("service_name"), limit=256),
        manage_link=manage_link,
        manage_link_present=manage_link_present,
    )


def easyweek_job_dedupe_key(
    *,
    event_hint: str,
    booking_uuid: uuid.UUID,
    payload_hash: str | None,
    job_type: str,
) -> str:
    """Bounded, stable dedupe key for EasyWeek lifecycle jobs.

    Idempotency for a Resend cannot be built on a unique constraint over
    ``easyweek_events.payload_hash``: capture is research-grade and every
    delivery MUST stay its own row. Instead the *job* is keyed by what makes two
    deliveries the same business fact — provider, exact trigger, booking UUID
    and the canonical payload digest.

    Deliberately NOT the Altegio ``make_dedupe_key`` format: that one is keyed
    on ``run_at`` and is left byte-for-byte untouched.
    """
    digest_source = f"{event_hint}|{booking_uuid}|{payload_hash or ''}"
    digest = hashlib.sha256(digest_source.encode("utf-8")).hexdigest()[:32]
    # message_jobs.dedupe_key is String(128); this is comfortably inside it.
    return f"easyweek:{job_type}:{booking_uuid}:{digest}"
