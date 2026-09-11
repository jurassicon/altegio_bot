"""Reading POS orders the way EasyWeek actually returns them (§35).

Scope lives in the request, not in the body
-------------------------------------------
The documented order listing takes ``location_uuid``, ``customer_uuid`` and
``staffer_uuid`` as filters. The observed order body carries **neither** a
top-level ``location_uuid`` nor a ``staffer_uuid`` — so an earlier version of
this matcher, which required both to be echoed back, could never match a real
order and would have reported every unknown create as unresolved forever.

The fix is to let the documented request prove the scope and to check locally
only what the response really carries: the unique marker, the created-at window,
the customer when the body names one, and whatever voucher facts happen to be
there. A voucher field this integration has not seen yet is a contract
observation, never evidence that the order belongs to somebody else.

Completeness comes from pagination metadata
-------------------------------------------
A first empty page is not proof of the end of a list when the API tells us the
last page number. ``meta.last_page`` is used where present; an inconsistent or
truncated walk stays UNKNOWN rather than being read as "nothing there".
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
ORDER_MALFORMED: Final = "malformed"

# How a payment was proven. `account_paid_amount` is deliberately NOT one of
# these: it is an opaque bookkeeping figure and proves nothing about an order.
PAYMENT_PROOF_STATUS: Final = "documented_status_flag"
PAYMENT_PROOF_AMOUNTS: Final = "settled_order_amounts"
PAYMENT_PROOF_NONE: Final = "none"

MAX_ORDER_PAGES: Final = 50

_CUSTOMER_UUID_KEYS: Final = ("customer_uuid", "client_uuid", "recipient_uuid")


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


def last_page(payload: object) -> int | None:
    """The API's own ``last_page``, if it published one."""
    if not isinstance(payload, dict):
        return None
    for container in (payload.get("meta"), payload):
        if isinstance(container, dict):
            value = container.get("last_page")
            if type(value) is int and value >= 1:
                return value
    return None


@dataclass(frozen=True)
class PagedWalk:
    """Every row of a listing, and whether the walk is known to be complete."""

    rows: tuple[Any, ...]
    complete: bool


async def walk_pages(
    fetch: Callable[[int], Awaitable[Any]],
    *,
    max_pages: int = MAX_ORDER_PAGES,
) -> PagedWalk:
    """Walk a listing to its end, preferring the API's own page count.

    ``complete`` is false when the page ceiling was hit or when the published
    ``last_page`` changed mid-walk. A caller must treat an incomplete walk as
    unknown rather than as "there is nothing there" — that distinction is the
    whole reason an unresolved create is not closed by a single quiet answer.
    """
    collected: list[Any] = []
    published: int | None = None

    for page in range(1, max_pages + 1):
        payload = await fetch(page)
        collected.extend(rows(payload))

        observed = last_page(payload)
        if observed is not None:
            if published is None:
                published = observed
            elif observed != published:
                # The list moved under the walk. Neither answer is trustworthy.
                return PagedWalk(rows=tuple(collected), complete=False)
            if page >= published:
                return PagedWalk(rows=tuple(collected), complete=True)
            continue

        # No metadata at all: an empty page is the only end marker available.
        if not rows(payload):
            return PagedWalk(rows=tuple(collected), complete=True)

    return PagedWalk(rows=tuple(collected), complete=False)


def _true(value: object) -> bool:
    return value is True


def classify_order(payload: object) -> tuple[str, str]:
    """``(order_state, payment_proof)`` from documented fields only.

    Refund and cancellation are decided before payment: an order that was paid
    and then reverted is *refunded*, and reporting it as paid would hide exactly
    the rollback the canary has to prove.
    """
    order = order_object(payload)
    if order is None or not isinstance(order.get("uuid"), str):
        return ORDER_MALFORMED, PAYMENT_PROOF_NONE

    status = order.get("status")
    status_slug = status.casefold() if isinstance(status, str) else ""

    if _true(order.get("is_reverted")) or _true(order.get("is_refunded")) or status_slug in {"refunded", "reverted"}:
        return ORDER_REFUNDED, PAYMENT_PROOF_NONE
    if _true(order.get("is_canceled")) or _true(order.get("is_cancelled")) or status_slug in {"canceled", "cancelled"}:
        return ORDER_CANCELLED, PAYMENT_PROOF_NONE

    if _true(order.get("is_paid")) or status_slug in {"paid", "completed", "closed"}:
        return ORDER_PAID, PAYMENT_PROOF_STATUS

    # A settled invoice is the second documented way to see a payment. The
    # opaque `account_paid_amount` is never consulted.
    invoice = order.get("invoice")
    invoice = invoice if isinstance(invoice, dict) else order
    amount_due = invoice.get("amount_due")
    amount_paid = invoice.get("amount_paid")
    if (
        type(amount_due) is int
        and amount_due == 0
        and type(amount_paid) is int
        and amount_paid == SUPPORTED_VOUCHER_PRICE_MINOR
    ):
        return ORDER_PAID, PAYMENT_PROOF_AMOUNTS

    return ORDER_OPEN, PAYMENT_PROOF_NONE


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
