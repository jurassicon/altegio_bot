"""EasyWeek webhook payload → validated domain intent.

Pure, synchronous and side-effect free: it takes one captured ``easyweek_events``
row and either returns a fully validated :class:`NormalizedBooking`, or raises
:class:`NormalizationError` carrying a stable, PII-free error code.

Design rules come from ``docs/easyweek/INTEGRATION_PLAN.md`` §1.6 and are
deliberately stricter than the payload:

* **UUID-first.** Root ``uid`` is the authoritative booking identity. ``id``,
  ``booking_hash_id`` and ``location_uuid`` do not substitute for it.
* **Location isolation.** The numeric ``location_id`` must belong to the strict
  registry and the payload's ``location_uuid`` must match the same entry. A
  foreign location or a mismatched pair is rejected before domain writes.
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
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Final, Mapping
from urllib.parse import urlsplit

from altegio_bot.easyweek_locations import EasyWeekLocation
from altegio_bot.easyweek_service_category import normalize_service_category

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

# ---------------------------------------------------------------------------
# Domain numeric bounds — the actual PostgreSQL column limits
# ---------------------------------------------------------------------------
# A JSON number is unbounded; a PostgreSQL column is not. Passing an oversized
# integer through would raise DataError at INSERT time, which the worker cannot
# tell from a transient database fault — so it would be retried forever instead
# of being rejected once. Every number is therefore range-checked HERE.
PG_INT_MIN: Final = -2147483648
PG_INT_MAX: Final = 2147483647
PG_BIGINT_MIN: Final = -9223372036854775808
PG_BIGINT_MAX: Final = 9223372036854775807

# Numeric(12, 2): ten integral digits plus two decimals.
MAX_MONEY: Final = Decimal("9999999999.99")
# The same ceiling expressed in the minor units the payload actually sends.
MAX_MONEY_CENTS: Final = 999999999999

# ---------------------------------------------------------------------------
# Price field syntax — see _price_to_decimal for the confirmed field semantics
# ---------------------------------------------------------------------------
# Digits only. No sign, no separator, no exponent, no currency, no surrounding
# space: a money amount is parsed, never interpreted.
_MINOR_UNITS_RE: Final = re.compile(r"[0-9]+")
# Matched separately so a negative amount reports "out of range" rather than
# "not a number" — it IS a number, just not one a booking total may hold.
_NEGATIVE_MINOR_UNITS_RE: Final = re.compile(r"-[0-9]+")
# The major-unit cross-check projection: "120.00", "120.0" and "120" all state
# the same amount, and the comparison that follows is numeric, not textual.
_MAJOR_UNITS_RE: Final = re.compile(r"[0-9]+(?:\.[0-9]{1,2})?")


def canonical_booking_uuid(payload: Any) -> uuid.UUID | None:
    """The delivery's booking UUID in canonical form, or ``None``.

    THE single definition of booking identity, shared by capture, the PR-4
    migration backfill, the claim ordering key and the normalizer — so that the
    row stored at capture, the row the claim serialises on, and the row the
    domain writes can never disagree about which booking a delivery belongs to.

    ``uuid.UUID`` accepts every textual form EasyWeek could plausibly send —
    lowercase, uppercase, braced, ``urn:uuid:``-prefixed and dash-less — and
    collapses them all to one value; ``.strip()`` covers surrounding whitespace.
    Two deliveries of the same booking therefore share a key no matter how the
    text was written.

    Returns ``None`` — never raises — for a missing, non-string or syntactically
    invalid ``uid``, and for any non-object payload. Capture must record such a
    delivery unchanged, and it must neither block other bookings nor be blocked;
    the deterministic rejection happens later, in the normalizer.
    """
    if not isinstance(payload, dict):
        return None
    raw = payload.get("uid")
    if not isinstance(raw, str):
        return None
    try:
        return uuid.UUID(raw.strip())
    except (ValueError, AttributeError, TypeError):
        return None


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
    LOCATION_IDENTITY_MISMATCH: Final = "location_identity_mismatch"
    INVALID_DATETIME: Final = "invalid_datetime"
    INVALID_MANAGE_LINK: Final = "invalid_manage_link"
    # The numeric booking id already belongs to a Record carrying a DIFFERENT
    # booking UUID. Raised by the worker, not by payload validation.
    IDENTITY_CONFLICT: Final = "identity_conflict"
    # A number is well-formed JSON but does not fit the domain column it is
    # destined for. Rejected here, deterministically, rather than surfacing
    # later as a DataError/InvalidOperation that would look transient and be
    # retried forever.
    INVALID_NUMERIC_RANGE: Final = "invalid_numeric_range"
    # The delivery's price fields do not describe the same amount — either the
    # major-unit projection disagrees with the authoritative storage value, or a
    # price is claimed by a field that is not authoritative. Distinct from
    # INVALID_PAYLOAD on purpose: each individual field is well-formed, and the
    # operator's next step is to look at the delivery, not at our parser.
    PRICE_FIELDS_CONFLICT: Final = "price_fields_conflict"
    # PR-11: the visit counter's own inputs. Separate codes from the lifecycle
    # ones because the operator's next step differs — these say "this succeeded
    # delivery cannot move the counter", not "this delivery is unusable".
    MISSING_CUSTOMER_ID: Final = "missing_customer_id"
    MISSING_VISITS_TOTAL: Final = "missing_visits_total"
    INVALID_VISITS_TOTAL: Final = "invalid_visits_total"
    VISITS_TOTAL_OUT_OF_RANGE: Final = "visits_total_out_of_range"

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
            LOCATION_IDENTITY_MISMATCH,
            INVALID_DATETIME,
            INVALID_MANAGE_LINK,
            IDENTITY_CONFLICT,
            INVALID_NUMERIC_RANGE,
            PRICE_FIELDS_CONFLICT,
            MISSING_CUSTOMER_ID,
            MISSING_VISITS_TOTAL,
            INVALID_VISITS_TOTAL,
            VISITS_TOTAL_OUT_OF_RANGE,
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
    # None means "no proven link in this delivery". Whether that clears the
    # stored link or preserves it is decided by ``manage_link_present``.
    manage_link: ManageLink | None
    # True when the delivery carried BOTH link fields (valid or not). A delivery
    # with neither field keeps the last proven link; a delivery that carried
    # them but failed validation must not leave a stale link in place.
    manage_link_present: bool

    # --- service / price (PR-5 renders from domain data, not from payload) ---
    service_id: int | None
    service_name: str | None
    service_quantity: int | None
    # Customer-facing description of the WHOLE service set, and how many
    # services the booking has. Confirmed root fields in the live capture.
    services_description: str | None
    services_count: int | None
    # Root-level machine category. It is normalized here but eligibility is
    # deliberately decided later from the persisted Record.raw snapshot.
    service_category: str | None
    # Booking-level total. `booking_price` is the authoritative storage value in
    # exact minor units ("12000" == 120.00); `booking_price_int` is NOT a cent
    # count and is never read. See `_price_to_decimal`.
    total_cost: Decimal | None

    # Logical names of the fields this delivery actually CARRIED. Patch
    # semantics: a field absent from a partial delivery must not blank the
    # value we already know, while a field that is present — including present
    # and empty — is authoritative for this booking.
    present_fields: frozenset[str]

    def carries(self, field: str) -> bool:
        return field in self.present_fields


def map_event_hint(event_hint: str | None) -> str:
    """Return CREATE/UPDATE/DELETE/IGNORE for an exact EasyWeek trigger name."""
    if not isinstance(event_hint, str):
        raise NormalizationError(NormalizationError.INVALID_EVENT_HINT)
    action = _EVENT_HINT_MAP.get(event_hint.strip())
    if action is None:
        raise NormalizationError(NormalizationError.INVALID_EVENT_HINT)
    return action


def _as_exact_int(value: Any) -> int | None:
    """Return an exact integer, or None when the value is not one.

    ``True``/``False`` are rejected: ``bool`` is an ``int`` subclass in Python,
    and a boolean silently becoming id ``1`` would attach a booking to whichever
    row happens to own that id. Numeric strings, NaN, Infinity and non-integral
    floats are rejected too — EasyWeek sends real JSON numbers for every id.
    """
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        # NaN and ±Infinity are not integral, so `is_integer()` already
        # excludes them; the explicit check documents the intent.
        if value != value or value in (float("inf"), float("-inf")):
            return None
        if value.is_integer():
            return int(value)
    return None


def _require_int(
    value: Any,
    *,
    code: str,
    minimum: int = PG_BIGINT_MIN,
    maximum: int = PG_BIGINT_MAX,
    range_code: str = NormalizationError.INVALID_NUMERIC_RANGE,
) -> int:
    """Exact integer within the destination column's range, or a safe rejection.

    ``code`` reports "this is not a number at all"; ``range_code`` reports "it
    is a number, but it does not fit". Both are fixed identifiers.
    """
    number = _as_exact_int(value)
    if number is None:
        raise NormalizationError(code)
    if number < minimum or number > maximum:
        raise NormalizationError(range_code)
    return number


def _require_positive_id(value: Any, *, code: str, maximum: int = PG_BIGINT_MAX) -> int:
    """A business identifier: exact, in range, and strictly positive.

    Every id in the confirmed captured payloads (``id``, ``customer_id``,
    ``location_id``, ``service_id``) is a positive number. A zero or negative
    id is not something EasyWeek produces, and accepting one would let a
    malformed delivery address an unrelated row.
    """
    return _require_int(value, code=code, minimum=1, maximum=maximum)


def _optional_int(value: Any) -> int | None:
    """A JSON number, or None. Never a bool, never a numeric string."""
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return None


def _price_minor_units(raw: str) -> int:
    """Exact minor units from the authoritative storage string.

    Deliberately syntactic and total: only ASCII digits are a price. A comma, a
    currency symbol, an exponent, surrounding space or any other localized form
    is rejected rather than coerced — the salon's locale must never decide what
    a customer is told they owe.
    """
    if _MINOR_UNITS_RE.fullmatch(raw):
        return int(raw)
    if _NEGATIVE_MINOR_UNITS_RE.fullmatch(raw):
        # Well-formed, but not an amount a booking total may hold: it would
        # render as a negative price to the customer.
        raise NormalizationError(NormalizationError.INVALID_NUMERIC_RANGE)
    raise NormalizationError(NormalizationError.INVALID_PAYLOAD)


def _assert_price_projection(value: Decimal, raw: Any) -> None:
    """The major-unit projection must describe the amount we already parsed."""
    if not isinstance(raw, str) or not _MAJOR_UNITS_RE.fullmatch(raw):
        raise NormalizationError(NormalizationError.INVALID_PAYLOAD)
    if Decimal(raw) != value:
        raise NormalizationError(NormalizationError.PRICE_FIELDS_CONFLICT)


def _price_to_decimal(payload: dict[str, Any]) -> Decimal | None:
    """Booking price as a money Decimal, or a deterministic rejection.

    The field semantics here come from confirmed production captures, which
    contradicted the documentation this parser was first written against. For a
    real price of 120.00 € EasyWeek sends::

        booking_price_int: 120           # MAJOR units, not cents
        booking_price: "12000"           # storage format: exact minor units
        booking_price_float: "120.00"    # major-unit projection
        booking_price_formatted: "€120.00"   # localized display text

    So:

    * ``booking_price`` is the single authoritative value, read as exact minor
      units with integer arithmetic — never through ``float``;
    * ``booking_price_float`` is a cross-check, not a source. When the delivery
      carries it, it must describe the same amount, or the delivery is refused;
    * ``booking_price_formatted`` is display text and is never parsed, not even
      as a fallback — it carries the salon's currency symbol and separator;
    * ``booking_price_int`` is NOT a cent count. Dividing it by 100 is what
      turned 120.00 € into 1.20 € in production, so it is not read at all.

    Presence semantics are unchanged and keyed on the authoritative field alone
    (see ``present_fields``): absent means "unchanged", an explicit ``null``
    means "cleared", and a real 0.00 stays a real price. A clear that another
    price field contradicts is a conflict, not a clear — we do not guess which
    half of the delivery to believe.

    The value is range-checked against ``Numeric(12, 2)`` BEFORE any Decimal
    arithmetic: ``quantize()`` on a 30-digit value raises
    ``decimal.InvalidOperation`` (the default context carries 28 digits), which
    would escape as an unexpected error and be retried forever instead of being
    rejected once.
    """
    projection_present = "booking_price_float" in payload
    projection = payload.get("booking_price_float")
    # A price claimed only by a non-authoritative field. Whatever produced it,
    # it is not the shape we confirmed, and guessing is exactly the failure this
    # PR exists to remove.
    claims_price_without_authority = projection_present and projection is not None

    if "booking_price" not in payload:
        if claims_price_without_authority:
            raise NormalizationError(NormalizationError.PRICE_FIELDS_CONFLICT)
        return None

    raw = payload.get("booking_price")
    if raw is None:
        if claims_price_without_authority:
            raise NormalizationError(NormalizationError.PRICE_FIELDS_CONFLICT)
        return None

    # Only the storage string. A JSON number here — including ``bool``, which is
    # an ``int`` subclass — is an unconfirmed shape, and reading it would revive
    # the ambiguity between major and minor units that caused the defect.
    if not isinstance(raw, str):
        raise NormalizationError(NormalizationError.INVALID_PAYLOAD)

    minor_units = _price_minor_units(raw)
    if minor_units > MAX_MONEY_CENTS:
        raise NormalizationError(NormalizationError.INVALID_NUMERIC_RANGE)
    value = (Decimal(minor_units) / Decimal(100)).quantize(Decimal("0.01"))

    if projection is not None:
        _assert_price_projection(value, projection)
    return value


def _optional_bounded_int(
    payload: dict[str, Any],
    key: str,
    *,
    minimum: int,
    maximum: int,
) -> int | None:
    """Bounded integer for an optional field; absent stays None, bad rejects."""
    if key not in payload or payload.get(key) is None:
        return None
    return _require_int(
        payload.get(key),
        code=NormalizationError.INVALID_PAYLOAD,
        minimum=minimum,
        maximum=maximum,
    )


def _optional_services_count(payload: dict[str, Any]) -> int | None:
    """Normalize the singular EasyWeek service count as optional proof.

    Unlike identifiers and persisted numeric domain fields, an unusable
    ``services_count`` is not a malformed event: it means only that notification
    eligibility is unproven. Zero remains available to the existing display
    snapshot but clears the separate eligibility proof. Presence tracking still
    records every explicit clear, while absent updates preserve previous proof.
    """
    if "services_count" not in payload:
        return None
    count = _as_exact_int(payload.get("services_count"))
    if count is None or count < 0 or count > PG_INT_MAX:
        return None
    return count


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

    # EVERY component access is inside the guard, not just urlsplit(). Reading
    # `.port` on "https://eyw.me:bad/r/1" or "https://[oops/r/1" raises
    # ValueError from urllib itself — lazily, at attribute access. Letting that
    # escape would turn an untrusted URL into an unexpected exception, skip the
    # deterministic path entirely, and leave the row stuck at the head of the
    # queue. Any parse failure is simply an untrusted pair.
    try:
        parts = urlsplit(page)
        if parts.scheme != MANAGE_LINK_SCHEME:
            return None, True
        # `hostname` lowercases and strips the port; comparing it to the bare
        # host while separately rejecting port/credentials closes the
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
    except ValueError:
        return None, True

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


@dataclass(frozen=True)
class SucceededBooking:
    """PR-9: a proven ``booking-succeeded`` delivery, and nothing more.

    Deliberately tiny, and deliberately NOT a :class:`NormalizedBooking`. A
    succeeded delivery is evidence that a visit finished — it is not a new
    version of the booking. Handing the lifecycle object back here would invite
    a caller to write its fields, and a succeeded payload must never rewrite the
    name, phone, price, service, or time that the lifecycle events proved.

    ``review_url`` is captured raw and read by nobody. PR-9 planned to prove it
    against the booking hash once the Record was in hand; PR-10 replaced the
    SOURCE of the review link with our own configuration after production
    showed EasyWeek does not send this field at all. The field and
    :func:`altegio_bot.easyweek_review.validate_review_url` are kept
    deliberately, not by oversight: the normalizer's job is to record the
    payload shape faithfully, and the validator remains the written contract
    for ``eyw.me`` links should EasyWeek ever start sending one. Neither is on
    any production path today.
    """

    booking_uuid: uuid.UUID
    booking_id: int
    company_id: int
    review_url: Any


def normalize_succeeded_event(
    *,
    event_hint: str | None,
    payload: Any,
    body_truncated: bool,
    location_registry: Mapping[int, EasyWeekLocation],
) -> SucceededBooking:
    """Validate one ``booking-succeeded`` delivery, or reject it deterministically.

    Applies exactly the integrity and isolation rules every other event gets —
    truncation, payload shape, the ``location_id`` + ``location_uuid`` pair as
    ONE registry identity — and then the identity a review needs: a canonical
    ``uid`` and the numeric booking id, both of which the caller matches against
    the Record before anything is planned.

    Separate from :func:`normalize_event` rather than folded into it: that
    function returns ``None`` for this trigger and every caller relies on that
    meaning "terminal, no side effects". Widening its contract would make a
    lifecycle path responsible for a marketing one.
    """
    if map_event_hint(event_hint) is not IGNORE:
        raise NormalizationError(NormalizationError.INVALID_EVENT_HINT)

    if body_truncated:
        raise NormalizationError(NormalizationError.TRUNCATED_PAYLOAD)
    if not isinstance(payload, dict) or not payload:
        raise NormalizationError(NormalizationError.INVALID_PAYLOAD)

    location_id = _require_positive_id(
        payload.get("location_id"),
        code=NormalizationError.INVALID_LOCATION_ID,
        maximum=PG_INT_MAX,
    )
    location = location_registry.get(location_id)
    if location is None:
        raise NormalizationError(NormalizationError.FOREIGN_LOCATION)

    raw_location_uuid = payload.get("location_uuid")
    try:
        payload_location_uuid = str(uuid.UUID(raw_location_uuid)) if isinstance(raw_location_uuid, str) else None
    except (ValueError, AttributeError, TypeError):
        payload_location_uuid = None
    if payload_location_uuid != location.location_uuid:
        raise NormalizationError(NormalizationError.LOCATION_IDENTITY_MISMATCH)

    raw_uid = payload.get("uid")
    if raw_uid is None or (isinstance(raw_uid, str) and not raw_uid.strip()):
        raise NormalizationError(NormalizationError.MISSING_BOOKING_UUID)
    if not isinstance(raw_uid, str):
        raise NormalizationError(NormalizationError.INVALID_BOOKING_UUID)
    booking_uuid = canonical_booking_uuid(payload)
    if booking_uuid is None:
        raise NormalizationError(NormalizationError.INVALID_BOOKING_UUID)

    booking_id = _require_positive_id(payload.get("id"), code=NormalizationError.MISSING_BOOKING_ID)

    return SucceededBooking(
        booking_uuid=booking_uuid,
        booking_id=booking_id,
        company_id=location_id,
        review_url=payload.get("review_url"),
    )


@dataclass(frozen=True)
class SucceededVisit:
    """PR-11: a proven ``booking-succeeded`` plus the two fields a counter needs.

    Built on top of :class:`SucceededBooking` rather than replacing it. The
    review path (PR-9/PR-10) must keep working on deliveries that carry no
    usable ``visits_total`` at all, so demanding one inside
    :func:`normalize_succeeded_event` would turn a counter problem into a lost
    review.
    """

    booking_uuid: uuid.UUID
    booking_id: int
    company_id: int
    customer_id: int
    visits_total: int


def _require_strict_int(value: Any, *, code: str) -> int:
    """A JSON integer and nothing else — not a bool, float, or numeric string.

    Stricter than :func:`_as_exact_int`, which accepts ``3.0`` because a price
    or a duration may legitimately arrive as an integral float. A visit count
    may not: ``3.0`` means the producer changed the field's type, ``"3"`` means
    it changed its encoding, and ``True`` means it is not a count at all.
    Coercing any of them would hide a contract change behind a plausible number,
    and this value is written to a client's row.
    """
    if isinstance(value, bool) or not isinstance(value, int):
        raise NormalizationError(code)
    return value


def normalize_succeeded_visit_event(
    *,
    event_hint: str | None,
    payload: Any,
    body_truncated: bool,
    location_registry: Mapping[int, EasyWeekLocation],
) -> SucceededVisit:
    """Validate one ``booking-succeeded`` as evidence for the visit counter.

    Runs the full succeeded contract first — trigger, truncation, payload shape,
    the ``location_id`` + ``location_uuid`` pair as ONE registry identity, the
    canonical ``uid`` and the numeric booking id — then adds the two fields the
    counter needs and nothing else.

    Everything else in the payload is deliberately ignored. A succeeded delivery
    proves a visit finished; it is not a newer version of the booking, so its
    name, phone, price, services, times and localised status must never reach a
    domain row.
    """
    succeeded = normalize_succeeded_event(
        event_hint=event_hint,
        payload=payload,
        body_truncated=body_truncated,
        location_registry=location_registry,
    )

    # The external customer id is matched against `Client.altegio_client_id` by
    # the caller. Without it there is no provable client, and finding one by
    # phone or name would be exactly the cross-provider guess this integration
    # forbids.
    customer_id = _require_positive_id(
        payload.get("customer_id"),
        code=NormalizationError.MISSING_CUSTOMER_ID,
    )

    if "visits_total" not in payload or payload.get("visits_total") is None:
        raise NormalizationError(NormalizationError.MISSING_VISITS_TOTAL)
    visits_total = _require_strict_int(
        payload.get("visits_total"),
        code=NormalizationError.INVALID_VISITS_TOTAL,
    )
    # A finished visit means the customer has at least this one. Zero or
    # negative contradicts the trigger that delivered it.
    if visits_total < 1 or visits_total > PG_INT_MAX:
        raise NormalizationError(NormalizationError.VISITS_TOTAL_OUT_OF_RANGE)

    return SucceededVisit(
        booking_uuid=succeeded.booking_uuid,
        booking_id=succeeded.booking_id,
        company_id=succeeded.company_id,
        customer_id=customer_id,
        visits_total=visits_total,
    )


def normalize_event(
    *,
    event_hint: str | None,
    payload: Any,
    body_truncated: bool,
    location_registry: Mapping[int, EasyWeekLocation],
) -> NormalizedBooking | None:
    """Validate one captured delivery.

    Returns ``None`` for ``booking-succeeded`` (terminal, no side effects).
    Raises :class:`NormalizationError` for every deterministic rejection.
    """
    action = map_event_hint(event_hint)

    # Payload integrity and location isolation are validated for EVERY event,
    # including the ones we ignore. Returning early for `booking-succeeded`
    # would hand a `processed` status to a truncated, empty or FOREIGN-location
    # delivery — a foreign booking must never be recorded as successfully
    # handled by this bot, whatever its trigger.
    if body_truncated:
        # The missing tail could contain the very fields we validate.
        raise NormalizationError(NormalizationError.TRUNCATED_PAYLOAD)
    if not isinstance(payload, dict) or not payload:
        raise NormalizationError(NormalizationError.INVALID_PAYLOAD)

    location_id = _require_positive_id(
        payload.get("location_id"),
        code=NormalizationError.INVALID_LOCATION_ID,
        maximum=PG_INT_MAX,  # clients.company_id / records.company_id are INTEGER
    )
    location = location_registry.get(location_id)
    if location is None:
        raise NormalizationError(NormalizationError.FOREIGN_LOCATION)

    raw_location_uuid = payload.get("location_uuid")
    try:
        payload_location_uuid = str(uuid.UUID(raw_location_uuid)) if isinstance(raw_location_uuid, str) else None
    except (ValueError, AttributeError, TypeError):
        payload_location_uuid = None
    if payload_location_uuid != location.location_uuid:
        raise NormalizationError(NormalizationError.LOCATION_IDENTITY_MISMATCH)

    if action == IGNORE:
        # `booking-succeeded` is captured for phase 2 (visits_total / review
        # guard) and produces no Client, Record or MessageJob. The booking UUID
        # is deliberately NOT required here: nothing is keyed by it on this
        # path, so demanding it would fail events we only need to retain.
        # Integrity and isolation above were still enforced.
        return None

    raw_uid = payload.get("uid")
    if raw_uid is None or (isinstance(raw_uid, str) and not raw_uid.strip()):
        raise NormalizationError(NormalizationError.MISSING_BOOKING_UUID)
    if not isinstance(raw_uid, str):
        raise NormalizationError(NormalizationError.INVALID_BOOKING_UUID)
    booking_uuid = canonical_booking_uuid(payload)
    if booking_uuid is None:
        raise NormalizationError(NormalizationError.INVALID_BOOKING_UUID)

    # records.altegio_record_id is BIGINT.
    booking_id = _require_positive_id(payload.get("id"), code=NormalizationError.MISSING_BOOKING_ID)

    # Presence matters as much as the value: a delivery that omits
    # `customer_id` must not unlink a client we already resolved (see
    # `present_fields` below). An explicit null is NOT treated as "unlink" —
    # the confirmed payloads never send one, so that semantics is unproven.
    customer_id: int | None
    if payload.get("customer_id") is None:
        customer_id = None
    else:
        # clients.altegio_client_id is BIGINT.
        customer_id = _require_positive_id(
            payload.get("customer_id"),
            code=NormalizationError.INVALID_PAYLOAD,
        )

    # `booking_date_start` is the machine-readable source of truth; the
    # `*_formatted`, `date` and `time` fields are localized display strings.
    starts_at = parse_iso_utc(payload.get("booking_date_start"), required=False)
    ends_at = parse_iso_utc(payload.get("booking_date_end"), required=False)

    # records.duration_sec is INTEGER, and the payload gives MINUTES, so the
    # minute value is bounded by INT_MAX/60 before multiplying.
    duration_minutes = _optional_bounded_int(
        payload,
        "booking_duration",
        minimum=0,
        maximum=PG_INT_MAX // 60,
    )
    duration_sec = None if duration_minutes is None else duration_minutes * 60

    # record_services.service_id is INTEGER and is a primary-key COMPONENT: it
    # selects WHICH service row the snapshot belongs to. It is identity, not a
    # patchable attribute, so the explicit-clear semantics that apply to title,
    # amount and price do NOT apply here.
    #
    # Absent  -> the known service identity is kept (the delivery said nothing).
    # Present as a valid positive id -> the normal service change.
    # Present as null/false/"12"/0/negative -> DETERMINISTIC REJECTION.
    #
    # The last case is fail-closed on purpose. No captured payload has ever sent
    # `service_id: null`, so its meaning is unproven: it could mean "the service
    # was removed" or it could be an upstream serialisation artefact. Guessing
    # either way is unsafe — silently keeping the old identity (the previous
    # behaviour) would attach a NEW title, amount and price to the OLD
    # service_id, and deleting the snapshot would destroy a proven one. Rejecting
    # leaves every domain row untouched and makes the payload visible to an
    # operator instead.
    if "service_id" not in payload:
        service_id = None
    else:
        service_id = _require_positive_id(
            payload.get("service_id"),
            code=NormalizationError.INVALID_PAYLOAD,
            maximum=PG_INT_MAX,
        )
    # record_services.amount is INTEGER.
    service_quantity = _optional_bounded_int(payload, "quantity", minimum=0, maximum=PG_INT_MAX)
    services_count = _optional_services_count(payload)
    total_cost = _price_to_decimal(payload)

    manage_link, manage_link_present = extract_manage_link(payload)

    # Which logical fields this delivery actually carried. `booking-updated`
    # legitimately omits fields the salon did not touch, and blanking a known
    # value because a partial delivery was silent would lose data we already
    # proved.
    present_fields = (
        frozenset(
            name
            for name, key in (
                ("phone_e164", "customer_phone"),
                ("email", "customer_email"),
                ("starts_at", "booking_date_start"),
                ("ends_at", "booking_date_end"),
                ("duration_sec", "booking_duration"),
                ("staff_name", "users_description"),
                ("comment", "booking_attributes.booking_comment"),
                ("service_id", "service_id"),
                ("service_name", "service_name"),
                ("service_quantity", "quantity"),
                ("services_description", "services_description"),
                ("services_count", "services_count"),
                ("service_category", "service_category"),
                # The authoritative price field, and the only one presence is
                # keyed on: a delivery that carries only display variants has
                # not proven a price. See `_price_to_decimal`.
                ("total_cost", "booking_price"),
                # Presence decides whether the client link may be rewritten.
                ("customer_id", "customer_id"),
            )
            if key in payload
        )
        | (
            # display_name is derived from several possible keys.
            frozenset({"display_name"})
            if any(
                key in payload
                for key in ("customer_full_name", "customer_name", "customer_first_name", "customer_last_name")
            )
            else frozenset()
        )
        | (frozenset({"staff_name"}) if "user_name" in payload else frozenset())
    )

    return NormalizedBooking(
        action=action,
        booking_uuid=booking_uuid,
        booking_id=booking_id,
        customer_id=customer_id,
        # The registry proves ownership, but the event itself selects the
        # provider-scoped tenant. Never substitute a process-global id here.
        company_id=location_id,
        starts_at=starts_at,
        ends_at=ends_at,
        duration_sec=duration_sec,
        phone_e164=_customer_phone(payload),
        display_name=_display_name(payload),
        email=_optional_str(payload.get("customer_email"), limit=256),
        staff_name=_optional_str(payload.get("users_description"), limit=256)
        or _optional_str(payload.get("user_name"), limit=256),
        comment=_optional_str(payload.get("booking_attributes.booking_comment")),
        manage_link=manage_link,
        manage_link_present=manage_link_present,
        service_id=service_id,
        service_name=_optional_str(payload.get("service_name"), limit=512),
        service_quantity=service_quantity,
        services_description=_optional_str(payload.get("services_description"), limit=512),
        services_count=services_count,
        service_category=(
            normalized_category.value
            if (normalized_category := normalize_service_category(payload.get("service_category"))) is not None
            else None
        ),
        total_cost=total_cost,
        present_fields=present_fields,
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
