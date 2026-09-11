"""Reading POS orders the way EasyWeek actually returns them (§35).

Scope lives in the request, not in the body
-------------------------------------------
The documented order listing takes ``location_uuid``, ``customer_uuid`` and
``staffer_uuid`` as filters. The observed order body carries **neither** a
top-level ``location_uuid`` nor a ``staffer_uuid`` — so a matcher requiring both
to be echoed back could never match a real order and would report every unknown
create as unresolved forever.

So the documented request proves the scope, and only what the response really
carries is checked locally: the unique marker, the created-at window, and the
customer when the body names one.

An unrecognised state is not "open"
-----------------------------------
Classification is an allowlist in both directions. A missing, null, unfamiliar
or self-contradictory status is ``ORDER_UNKNOWN`` — never ``ORDER_OPEN`` — and
an unknown state can neither prove a create nor authorise a payment. Refund and
cancellation are decided before payment, because an order that was paid and then
reverted is *refunded*, and calling it paid would hide the rollback this canary
exists to prove.

Completeness is proven, not assumed
-----------------------------------
A page is only trusted when its own metadata agrees with what was asked for:
``current_page`` must be the page requested, ``last_page`` must be consistent
across the walk, and ``per_page`` must be the fixed size. Missing, malformed or
repeated metadata makes the walk incomplete — a server that answers page 2 with
page 1 has not shown us the end of anything.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Awaitable, Callable, Final

from altegio_bot.easyweek_voucher_canary.artifact import REDACTED_CONTAINER_KEYS, matches_customer
from altegio_bot.easyweek_voucher_identity import SUPPORTED_VOUCHER_PRICE_MINOR

# Order classifications, decided from documented fields only.
ORDER_OPEN: Final = "open"
ORDER_PAID: Final = "paid"
ORDER_REFUNDED: Final = "refunded"
ORDER_CANCELLED: Final = "cancelled"
# A readable order whose state we do not recognise, or whose signals disagree.
# Distinct from MALFORMED, which means "this is not a readable order at all".
ORDER_UNKNOWN: Final = "unknown"
ORDER_MALFORMED: Final = "malformed"

# States a stage may act on. Everything else — including both fail-closed ones —
# blocks a create verification, a pay plan and a pay mutation.
ACTIONABLE_ORDER_STATES: Final = frozenset({ORDER_OPEN, ORDER_PAID, ORDER_REFUNDED, ORDER_CANCELLED})

# How a payment was proven. `account_paid_amount` is deliberately NOT one of
# these: it is an opaque bookkeeping figure and proves nothing about an order.
PAYMENT_PROOF_STATUS: Final = "documented_status_flag"
PAYMENT_PROOF_AMOUNTS: Final = "settled_order_amounts"
PAYMENT_PROOF_NONE: Final = "none"

# Stable, PII-free reasons a payable order is not proven.
CANARY_VOUCHER_LINE_UNPROVEN: Final = "canary_voucher_line_unproven"
CANARY_ORDER_TOTAL_UNPROVEN: Final = "canary_order_total_unproven"
CANARY_ORDER_EXTRA_ITEMS: Final = "canary_order_extra_items"

MAX_ORDER_PAGES: Final = 50
POS_PER_PAGE: Final = 100

_CUSTOMER_UUID_KEYS: Final = ("customer_uuid", "client_uuid", "recipient_uuid")

# Status vocabularies. Each is an allowlist; anything outside them all is
# ORDER_UNKNOWN rather than a guess.
_OPEN_STATUSES: Final = frozenset({"open"})
_PAID_STATUSES: Final = frozenset({"paid", "completed", "closed"})
_REFUNDED_STATUSES: Final = frozenset({"refunded", "reverted"})
_CANCELLED_STATUSES: Final = frozenset({"canceled", "cancelled"})

# Line collections an order may carry. Anything in one of these other than the
# single voucher line is out of scope for this canary.
_ITEM_COLLECTION_KEYS: Final = ("services", "goods", "products", "items")
_TOTAL_KEYS: Final = ("total", "subtotal", "amount_due")


def order_object(payload: object) -> dict[str, Any] | None:
    """The order object, from the envelope or from a ``data`` wrapper."""
    if not isinstance(payload, dict):
        return None
    inner = payload.get("data")
    if isinstance(inner, dict):
        return inner
    return payload


def rows(payload: object) -> list[Any]:
    """The row list of a page, from a bare list or a ``data`` envelope."""
    listed = payload.get("data") if isinstance(payload, dict) else payload
    return listed if isinstance(listed, list) else []


@dataclass(frozen=True)
class PageMeta:
    """Pagination metadata that agreed with the request that produced it."""

    current_page: int
    last_page: int
    per_page: int | None


def page_meta(payload: object, *, expected_page: int, expected_per_page: int | None) -> PageMeta | None:
    """Validated pagination metadata, or ``None`` when it cannot be trusted.

    ``None`` covers every way a page can fail to prove where it sits: no
    metadata at all, metadata that is not an object, non-integer counters, a
    ``current_page`` that is not the page we asked for — the repeated-page-one
    case — a ``last_page`` behind the current one, or a page size that is not
    the fixed one we requested.
    """
    if not isinstance(payload, dict):
        return None
    meta = payload.get("meta")
    if not isinstance(meta, dict):
        return None

    current = meta.get("current_page")
    last = meta.get("last_page")
    if type(current) is not int or type(last) is not int:
        return None
    # The server answering page 2 with page 1 has shown us nothing new, and a
    # walk that counted it would "finish" without ever reaching the end.
    if current != expected_page:
        return None
    if last < current or last < 1:
        return None

    per_page = meta.get("per_page")
    if per_page is not None:
        if type(per_page) is not int:
            return None
        if expected_per_page is not None and per_page != expected_per_page:
            return None
        return PageMeta(current_page=current, last_page=last, per_page=per_page)
    return PageMeta(current_page=current, last_page=last, per_page=None)


@dataclass(frozen=True)
class PagedWalk:
    """Every row of a listing, and whether the walk is PROVEN complete."""

    rows: tuple[Any, ...]
    complete: bool


async def walk_pages(
    fetch: Callable[[int], Awaitable[Any]],
    *,
    max_pages: int = MAX_ORDER_PAGES,
    expected_per_page: int | None = POS_PER_PAGE,
) -> PagedWalk:
    """Walk a paginated listing to a PROVEN end.

    ``complete`` is true only when every page carried metadata that agreed with
    its request and the walk reached the published ``last_page``. It is false
    when metadata is missing or malformed, when ``last_page`` moved mid-walk, or
    when the page ceiling was hit.

    A caller must treat an incomplete walk as unknown rather than as "there is
    nothing there" — that distinction is the whole reason an unresolved create
    is not closed by one quiet answer. An empty intermediate page is not an end
    marker either: the published ``last_page`` decides.
    """
    collected: list[Any] = []
    published: int | None = None

    for page in range(1, max_pages + 1):
        payload = await fetch(page)
        meta = page_meta(payload, expected_page=page, expected_per_page=expected_per_page)
        if meta is None:
            return PagedWalk(rows=tuple(collected), complete=False)
        if published is None:
            published = meta.last_page
        elif meta.last_page != published:
            # The list moved under the walk. Neither answer is trustworthy.
            return PagedWalk(rows=tuple(collected), complete=False)

        collected.extend(rows(payload))
        if page >= published:
            return PagedWalk(rows=tuple(collected), complete=True)

    return PagedWalk(rows=tuple(collected), complete=False)


def _true(value: object) -> bool:
    return value is True


def _exact_int(value: object) -> int | None:
    """Exact ``int`` only. ``True`` is not 1 here, and 15.0 is not 1500."""
    return value if type(value) is int else None


def classify_order(payload: object) -> tuple[str, str]:
    """``(order_state, payment_proof)`` from documented fields only.

    Every state is an allowlist. A status that is absent, null, not a string or
    simply unfamiliar yields ``ORDER_UNKNOWN``, and so does a body whose signals
    contradict each other — a cancelled order that also claims to be paid, or a
    settled invoice under an ``open`` status. Guessing "probably open" there is
    how an unknown state becomes a payment.

    Refund and cancellation are read before payment. A refunded order WAS paid,
    so those two are not a contradiction; anything else that overlaps is.
    """
    order = order_object(payload)
    if order is None or not isinstance(order.get("uuid"), str) or not order["uuid"]:
        return ORDER_MALFORMED, PAYMENT_PROOF_NONE

    raw_status = order.get("status")
    status_slug = raw_status.casefold().strip() if isinstance(raw_status, str) else None

    refunded = _true(order.get("is_reverted")) or _true(order.get("is_refunded")) or status_slug in _REFUNDED_STATUSES
    cancelled = (
        _true(order.get("is_canceled")) or _true(order.get("is_cancelled")) or status_slug in _CANCELLED_STATUSES
    )
    paid_flag = _true(order.get("is_paid")) or status_slug in _PAID_STATUSES
    open_flag = status_slug in _OPEN_STATUSES

    invoice = order.get("invoice")
    invoice = invoice if isinstance(invoice, dict) else order
    amount_due = _exact_int(invoice.get("amount_due"))
    amount_paid = _exact_int(invoice.get("amount_paid"))
    settled = amount_due == 0 and amount_paid == SUPPORTED_VOUCHER_PRICE_MINOR

    # Refund wins over payment; a refunded order having been paid is expected.
    # Reverted AND cancelled at once is not, and is not a state to act on.
    if refunded:
        return (ORDER_UNKNOWN if cancelled else ORDER_REFUNDED), PAYMENT_PROOF_NONE

    if cancelled:
        # A cancellation that also carries a payment — or still calls itself
        # open — is a body we do not understand well enough to act on.
        if paid_flag or settled or open_flag:
            return ORDER_UNKNOWN, PAYMENT_PROOF_NONE
        return ORDER_CANCELLED, PAYMENT_PROOF_NONE

    # Evidence of a payment outranks an "open" label. The two disagreeing is a
    # body we do not fully understand, but the safe reading of it is not the
    # generous one: calling it PAID refuses a second payment and keeps the
    # refund reachable, while calling it OPEN or UNKNOWN could take the money
    # twice or strand it.
    if paid_flag:
        return ORDER_PAID, PAYMENT_PROOF_STATUS
    if settled:
        # The second documented way to see a payment. The opaque
        # `account_paid_amount` is never consulted.
        return ORDER_PAID, PAYMENT_PROOF_AMOUNTS

    if open_flag:
        return ORDER_OPEN, PAYMENT_PROOF_NONE

    # Missing, null, non-string or unfamiliar status, with nothing else to go
    # on. NOT "open": an order we cannot name is an order we must not pay for.
    return ORDER_UNKNOWN, PAYMENT_PROOF_NONE


def payable_order_reasons(
    payload: object,
    *,
    expected_template_uuid: str,
    expected_price_minor: int,
) -> tuple[str, ...]:
    """Why this order is NOT the exact one-voucher order we may pay for.

    An empty tuple means the order carries exactly one voucher line for the
    confirmed template at the exact nominal and quantity, no other line items,
    and a total that agrees where the body publishes one.

    This gates the PAYMENT only. A refund never consults it: an already-paid
    order must stay refundable even when its voucher body turns out to be
    something nobody expected.
    """
    order = order_object(payload)
    if order is None:
        return (CANARY_VOUCHER_LINE_UNPROVEN,)

    reasons: list[str] = []

    vouchers = order.get("vouchers")
    single = order.get("voucher")
    lines: list[Any]
    if isinstance(vouchers, list):
        lines = list(vouchers)
    elif isinstance(single, dict):
        lines = [single]
    else:
        lines = []

    if len(lines) != 1 or not isinstance(lines[0], dict):
        reasons.append(CANARY_VOUCHER_LINE_UNPROVEN)
    else:
        line = lines[0]
        price = _exact_int(line.get("price"))
        quantity = _exact_int(line.get("quantity"))
        if (
            line.get("voucher_template_uuid") != expected_template_uuid
            or price != expected_price_minor
            or quantity != 1
        ):
            reasons.append(CANARY_VOUCHER_LINE_UNPROVEN)

    # Services, goods or any other line collection would make the payable sum
    # something other than the one voucher we planned for.
    for key in _ITEM_COLLECTION_KEYS:
        value = order.get(key)
        if isinstance(value, list) and value:
            reasons.append(CANARY_ORDER_EXTRA_ITEMS)
            break
        if isinstance(value, dict) and value:
            reasons.append(CANARY_ORDER_EXTRA_ITEMS)
            break

    # Totals are checked where the body publishes them. A total that is present
    # but wrong is a different order; a total that is absent is simply not part
    # of the observed contract yet, and absence alone does not block.
    invoice = order.get("invoice")
    invoice = invoice if isinstance(invoice, dict) else order
    for key in _TOTAL_KEYS:
        if key not in invoice:
            continue
        if _exact_int(invoice.get(key)) != expected_price_minor:
            reasons.append(CANARY_ORDER_TOTAL_UNPROVEN)
            break

    return tuple(dict.fromkeys(reasons))


def carries_customer_reference(order: dict[str, Any]) -> bool:
    """Whether this order body names a customer at all.

    The listing and the exact read do not carry the same fields, so "the body
    did not mention a customer" has to be distinguishable from "the body named
    the wrong one".
    """
    for key in _CUSTOMER_UUID_KEYS:
        if isinstance(order.get(key), str) and order[key]:
            return True
    for key in REDACTED_CONTAINER_KEYS:
        container = order.get(key)
        if isinstance(container, dict) and isinstance(container.get("uuid"), str):
            return True
    return False


def within_window(order: dict[str, Any], *, start: datetime, end: datetime) -> bool:
    raw = order.get("created_at")
    if not isinstance(raw, str) or not raw:
        return False
    try:
        created = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return False
    if created.tzinfo is None:
        return False
    return start <= created <= end


def matches_canary_order(
    order: Any,
    *,
    customer_uuid: str,
    marker: str,
    window_start: datetime,
    window_end: datetime,
) -> bool:
    """Is this listing row the order this canary created?

    Checked here: the exact unique marker, the bounded created-at window, and
    the customer WHEN the row names one. Deliberately not checked here: the
    location and the staffer, which the documented request already scoped and
    the observed body does not echo; and the voucher shape, which is what the
    canary is trying to learn and therefore cannot be a precondition for
    recognising its own order.
    """
    if not isinstance(order, dict):
        return False
    # The marker is unique to this canary scope and is the identity.
    if order.get("comment") != marker:
        return False
    if not within_window(order, start=window_start, end=window_end):
        return False
    if carries_customer_reference(order) and not matches_customer(order, customer_uuid):
        return False
    return True


@dataclass(frozen=True)
class MarkerMatch:
    """What a complete marker-scoped walk found."""

    count: int
    order_uuid: str | None
    complete: bool

    @property
    def resolved(self) -> bool:
        return self.complete and self.count == 1 and self.order_uuid is not None


async def find_marker_orders(
    reader: Any,
    *,
    location_uuid: str,
    customer_uuid: str,
    staffer_uuid: str,
    marker: str,
    window_start: datetime,
    window_end: datetime,
    max_pages: int = MAX_ORDER_PAGES,
) -> MarkerMatch:
    """Walk this customer's orders in this branch, completely, and match ours.

    No candidate UUID is logged or printed; only the single match, and only into
    the ledger where a payment and a refund need it.
    """

    async def fetch(page: int) -> Any:
        return await reader.list_location_orders(
            location_uuid=location_uuid,
            customer_uuid=customer_uuid,
            staffer_uuid=staffer_uuid,
            page=page,
        )

    walk = await walk_pages(fetch, max_pages=max_pages)
    matches: list[str] = []
    for row in walk.rows:
        if not matches_canary_order(
            row,
            customer_uuid=customer_uuid,
            marker=marker,
            window_start=window_start,
            window_end=window_end,
        ):
            continue
        found = row.get("uuid")
        if isinstance(found, str) and found:
            matches.append(found)

    unique = list(dict.fromkeys(matches))
    return MarkerMatch(
        count=len(unique),
        order_uuid=unique[0] if len(unique) == 1 else None,
        complete=walk.complete,
    )
