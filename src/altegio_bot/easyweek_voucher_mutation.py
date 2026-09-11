"""The ONLY module allowed to mutate EasyWeek POS orders (INTEGRATION_PLAN §35).

Three endpoints, no fourth, and no way to add one without editing this file::

    POST /orders
    POST /orders/{order_uuid}/pay
    POST /orders/{order_uuid}/refund

``easyweek_client.EasyWeekClient`` stays GET-only and
``easyweek_voucher_calculation.EasyWeekVoucherCalculationClient`` still owns
``POST /orders/calculate`` and nothing else. Both of those answer "may this code
persist anything?" with "no", and they keep answering it. A grep for the three
paths above has exactly one hit: here.

``easyweek_migration.write_client`` is the cutover's mutation surface. It is not
reused: its natural identity, retry policy and reconciliation contract belong to
a booking migration, and binding a voucher canary to it would mean a migration
change could widen the canary.

Pinned request, not a builder
-----------------------------
``create_voucher_order`` does not assemble "an order". It assembles the one
order §35 authorises: the confirmed Karlsruhe location, one voucher line, the
confirmed template, price exactly 1500, quantity exactly 1, a non-personal
comment marker, and the runtime customer/staffer UUIDs the owner named in an
approved plan. There is no parameter for a service, a good, a discount, a
promocode, a second line, a different price or a bulk quantity, so no caller can
smuggle one in. ``pay`` takes one order UUID and one account UUID; ``refund``
takes one order UUID and sends the documented empty body.

One request, never repeated
---------------------------
Every method issues at most ONE network request and never retries — not on a
timeout, not on a transport failure, not on a 429, not on any 5xx, not on a 2xx
whose body cannot be read. Each of those leaves the outcome genuinely unknown:
EasyWeek publishes no idempotency key for these endpoints, so a second POST is a
coin flip on whether a real order, a real payment or a real refund happens
twice. They are raised as :class:`EasyWeekVoucherMutationUnknown`, which is
deliberately NOT a subclass of the generic retryable error, so that no "is it
retryable?" sweep can pick one up and repeat it.

A redirect is a refusal. ``follow_redirects`` is off and the client is always
built and owned here, because a 307/308 preserves method and body: following one
would re-send this POST, Authorization header included, wherever the response
pointed.

Nothing here logs or reprs a URL, a header, the Bearer key, the workspace slug,
a request body, a response body, a customer, an order UUID, a voucher code or a
voucher URL. A 4xx reports only its status, the operation and the names of
fields this client itself sent.
"""

from __future__ import annotations

import logging
import uuid as uuid_module
from dataclasses import dataclass, field
from types import TracebackType
from typing import Any, Final

import httpx

from altegio_bot.easyweek_client import (
    _DEFAULT_TIMEOUT,
    EasyWeekAuthError,
    EasyWeekConfigError,
    EasyWeekError,
    EasyWeekNotFoundError,
    EasyWeekPermanentError,
    _normalize_base_url,
    _unwrap_secret,
)
from altegio_bot.easyweek_voucher_identity import (
    EASYWEEK_VOUCHER_TEMPLATE_UUID,
    KARLSRUHE_LOCATION_UUID,
    SUPPORTED_VOUCHER_PRICE_MINOR,
    SUPPORTED_VOUCHER_QUANTITY,
)
from altegio_bot.settings import settings

logger = logging.getLogger("easyweek_voucher_mutation")

_PATH_ORDERS: Final = "orders"
_PATH_PAY: Final = "pay"
_PATH_REFUND: Final = "refund"

CREATE_OPERATION: Final = "create_voucher_order"
PAY_OPERATION: Final = "pay_voucher_order"
REFUND_OPERATION: Final = "refund_voucher_order"

# Every redirect status. None is followed; each is a fail-closed refusal.
_REDIRECT_STATUSES: Final = frozenset({301, 302, 303, 307, 308})

# The only field names a 4xx may echo back, per operation. Never a value, never
# the server's prose, never the body.
CREATE_REQUEST_FIELDS: Final = frozenset(
    {
        "location_uuid",
        "customer_uuid",
        "staffer_uuid",
        "comment",
        "vouchers",
        "voucher_template_uuid",
        "price",
        "quantity",
    }
)
PAY_REQUEST_FIELDS: Final = frozenset({"account_uuid"})
REFUND_REQUEST_FIELDS: Final = frozenset()

# A marker has to be findable in the EasyWeek dashboard and carry nothing about
# a person. Bounded and charset-pinned so it cannot become a free-text channel.
_MARKER_MAX_LENGTH: Final = 64
_MARKER_ALPHABET: Final = frozenset("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_")


class EasyWeekVoucherMutationUnknown(EasyWeekError):
    """The single request was sent and its effect is unknown. NEVER auto-retried.

    Deliberately not a subclass of
    :class:`~altegio_bot.easyweek_client.EasyWeekRetryableError`: "retryable"
    invites another attempt, and the whole point of this class is that another
    attempt could create a second order, take a second payment or issue a second
    refund. A caller has to name it, stop, and reconcile by reading.
    """


@dataclass(frozen=True)
class VoucherMutationResponse:
    """A 2xx answer whose body parsed as a JSON object.

    ``envelope`` is the complete parsed body — nothing unwrapped, nothing
    dropped — because the caller has to look for an order identity and a voucher
    artifact at whatever level EasyWeek actually puts them.

    ``repr=False`` on that field is not cosmetic: a dataclass repr reaches
    tracebacks, pytest output and log records, and this body may carry a customer
    subtree, a voucher code or a customer-facing URL.
    """

    http_status: int
    envelope: dict[str, Any] = field(repr=False)


def _canonical_uuid(value: object, *, label: str, operation: str) -> str:
    """Return *value* only when it already is a canonical lowercase UUID.

    Validated before any path is built. ``uuid.UUID(...)`` would accept braces,
    a urn prefix, uppercase and stray whitespace and then normalise them —
    accepting those would put an identity on the wire that the operator never
    typed. The offending value is never echoed into the error.
    """
    if not isinstance(value, str) or not value:
        raise EasyWeekPermanentError(f"{label} must be a canonical lowercase UUID", operation=operation)
    try:
        canonical = str(uuid_module.UUID(value))
    except (ValueError, AttributeError, TypeError):
        raise EasyWeekPermanentError(f"{label} must be a canonical lowercase UUID", operation=operation) from None
    if canonical != value:
        raise EasyWeekPermanentError(f"{label} must be a canonical lowercase UUID", operation=operation)
    return canonical


def _pinned(value: object, *, expected: str, label: str, operation: str) -> str:
    """Accept only the one confirmed literal for *label*.

    An equality check, not "is this a canonical UUID?": a well-formed UUID for
    another branch or another product is still a request §35 never authorised.
    """
    if not isinstance(value, str) or value != expected:
        raise EasyWeekPermanentError(f"{label} is not the confirmed canary identity", operation=operation)
    return expected


def _pinned_price(value: object, *, operation: str) -> int:
    """Accept only the exact supported nominal, as an exact ``int``.

    ``type(value) is int`` rather than ``isinstance``: ``True`` is an ``int`` to
    ``isinstance`` and would become a one-cent order. Floats and numeric strings
    are refused too — money that survived a float is money we cannot prove.
    """
    if type(value) is not int:
        raise EasyWeekPermanentError("price_minor must be an exact integer", operation=operation)
    if value != SUPPORTED_VOUCHER_PRICE_MINOR:
        raise EasyWeekPermanentError("price_minor is not the supported voucher nominal", operation=operation)
    return SUPPORTED_VOUCHER_PRICE_MINOR


def _safe_marker(value: object, *, operation: str) -> str:
    """A bounded, charset-pinned, non-personal reconciliation marker.

    The marker is the one thing this canary writes into a field a human will
    read, so it is the one place free text could enter an order. It is therefore
    restricted to a short slug: no spaces, no punctuation beyond ``-``/``_``, no
    unicode, nothing that could carry a name, a phone number or an injected
    instruction.
    """
    if not isinstance(value, str) or not value:
        raise EasyWeekPermanentError("marker must be a non-empty slug", operation=operation)
    if len(value) > _MARKER_MAX_LENGTH:
        raise EasyWeekPermanentError("marker is too long", operation=operation)
    if not set(value) <= _MARKER_ALPHABET:
        raise EasyWeekPermanentError("marker contains unsupported characters", operation=operation)
    return value


def _safe_validation_fields(response: httpx.Response, *, allowed: frozenset[str]) -> list[str]:
    """Names of fields WE sent that a 4xx complained about; never values.

    Laravel reports nested failures as dotted paths (``vouchers.0.price``). Each
    path is split and only segments in *allowed* survive, so an operator learns
    which field, never what was in it and never the server's prose.
    """
    if not allowed:
        return []
    try:
        payload: Any = response.json()
    except Exception:
        return []
    if not isinstance(payload, dict):
        return []

    keys: set[str] = set()
    for container in (payload.get("errors"), payload):
        if isinstance(container, dict):
            keys.update(key for key in container if isinstance(key, str))

    recognised: set[str] = set()
    for key in keys:
        for segment in key.split("."):
            if segment in allowed:
                recognised.add(segment)
    return sorted(recognised)


class EasyWeekVoucherMutationClient:
    """Creates, pays and refunds exactly one canary voucher order. Nothing else.

    Usage::

        async with EasyWeekVoucherMutationClient() as client:
            created = await client.create_voucher_order(
                location_uuid=KARLSRUHE_LOCATION_UUID,
                customer_uuid=...,
                staffer_uuid=...,
                voucher_template_uuid=EASYWEEK_VOUCHER_TEMPLATE_UUID,
                price_minor=SUPPORTED_VOUCHER_PRICE_MINOR,
                marker=...,
            )

    ``transport`` and ``timeout`` are the ONLY injection points, and neither can
    change redirect policy or introduce a retry. There is deliberately no
    ``http_client`` parameter: a client built elsewhere with
    ``follow_redirects=True`` would let a 307 replay a mutation.
    """

    def __init__(
        self,
        *,
        api_key: str | None = None,
        workspace_slug: str | None = None,
        base_url: str | None = None,
        timeout: httpx.Timeout | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        key = _unwrap_secret(api_key if api_key is not None else settings.easyweek_api_key)
        slug = _unwrap_secret(workspace_slug if workspace_slug is not None else settings.easyweek_workspace_slug)
        if not (isinstance(key, str) and key.strip()):
            raise EasyWeekConfigError("EASYWEEK_API_KEY is not configured")
        if not (isinstance(slug, str) and slug.strip()):
            raise EasyWeekConfigError("EASYWEEK_WORKSPACE_SLUG is not configured")

        self._api_key = key.strip()
        self._workspace_slug = slug.strip()
        self._base_url = _normalize_base_url(base_url if base_url is not None else settings.easyweek_api_base_url)
        self._client = httpx.AsyncClient(
            timeout=timeout or _DEFAULT_TIMEOUT,
            follow_redirects=False,
            transport=transport,
        )

    # -- lifecycle ---------------------------------------------------------

    async def aclose(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> EasyWeekVoucherMutationClient:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.aclose()

    def __repr__(self) -> str:
        # No key, no slug, no headers, no URL. A repr lands in tracebacks and
        # incident tickets, and none of those needs the endpoint.
        return "<EasyWeekVoucherMutationClient>"

    __str__ = __repr__

    # -- internals ---------------------------------------------------------

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._api_key}",
            "Workspace": self._workspace_slug,
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    async def _post_once(
        self,
        *path_segments: str,
        body: dict[str, Any] | None,
        operation: str,
        allowed_fields: frozenset[str],
    ) -> VoucherMutationResponse:
        """Issue exactly one POST and classify its outcome. Never repeats.

        The URL is assembled from vetted constants plus already-validated UUIDs,
        so no caller can supply a URL, a path segment or an endpoint. The body is
        whatever the calling method pinned; ``None`` sends no JSON body at all,
        which is what the documented refund expects.
        """
        url = "/".join([self._base_url, *path_segments])

        try:
            response = await self._client.post(url, headers=self._headers(), json=body)
        except httpx.TimeoutException:
            logger.error("easyweek_voucher_mutation: %s timed out — outcome UNKNOWN, no retry", operation)
            raise EasyWeekVoucherMutationUnknown(
                "mutation timed out; outcome unknown",
                operation=operation,
                attempts=1,
            ) from None
        except httpx.HTTPError as exc:
            # Only the exception CLASS: an httpx message can embed the full URL.
            logger.error(
                "easyweek_voucher_mutation: %s transport failure error_type=%s — outcome UNKNOWN, no retry",
                operation,
                type(exc).__name__,
            )
            raise EasyWeekVoucherMutationUnknown(
                "mutation transport failure; outcome unknown",
                operation=operation,
                attempts=1,
            ) from None

        status = response.status_code
        logger.info("easyweek_voucher_mutation: %s status=%s attempts=1", operation, status)

        if status in _REDIRECT_STATUSES:
            # `Location` is neither read nor logged. A 307/308 preserves method
            # and body, so following one would be a second mutation carrying the
            # Authorization header wherever the response pointed. There is no
            # reading of a redirect this canary is allowed to act on — but the
            # request DID leave, so the outcome is unknown, not a clean refusal.
            logger.error("easyweek_voucher_mutation: %s redirect refused status=%s", operation, status)
            raise EasyWeekVoucherMutationUnknown(
                "mutation endpoint answered with a redirect; not followed",
                operation=operation,
                status_code=status,
                attempts=1,
            )

        if 200 <= status < 300:
            try:
                payload: Any = response.json()
            except Exception:
                payload = None
            if not isinstance(payload, dict):
                # A 2xx we cannot read is NOT a failure we can record: the write
                # probably landed and we have nothing to reconcile against.
                logger.error(
                    "easyweek_voucher_mutation: %s unreadable 2xx body — outcome UNKNOWN, no retry",
                    operation,
                )
                raise EasyWeekVoucherMutationUnknown(
                    "mutation succeeded with an unreadable body; outcome unknown",
                    operation=operation,
                    status_code=status,
                    attempts=1,
                )
            return VoucherMutationResponse(http_status=status, envelope=payload)

        if status == 429 or 500 <= status < 600:
            # NOT retried, and 429 is no exception here. Elsewhere a 429 is a
            # safe retry because the limiter refuses before the handler runs; a
            # mutation canary cannot afford to assume which side of the handler
            # the refusal happened on.
            logger.error("easyweek_voucher_mutation: %s status=%s — outcome UNKNOWN, no retry", operation, status)
            raise EasyWeekVoucherMutationUnknown(
                "mutation outcome unknown",
                operation=operation,
                status_code=status,
                attempts=1,
            )

        # Permanent 4xx: the server rejected the request before acting.
        fields = _safe_validation_fields(response, allowed=allowed_fields)
        named = ",".join(fields) if fields else "no recognised field named"
        logger.error("easyweek_voucher_mutation: %s rejected status=%s fields=%s", operation, status, named)
        if status in (401, 403):
            raise EasyWeekAuthError(
                "authentication or authorization failed",
                operation=operation,
                status_code=status,
                attempts=1,
            )
        if status == 404:
            raise EasyWeekNotFoundError("resource not found", operation=operation, status_code=status, attempts=1)
        raise EasyWeekPermanentError(
            "mutation rejected as invalid: " + named,
            operation=operation,
            status_code=status,
            attempts=1,
        )

    # -- the three public mutations ---------------------------------------

    async def create_voucher_order(
        self,
        *,
        location_uuid: str,
        customer_uuid: str,
        staffer_uuid: str,
        voucher_template_uuid: str,
        price_minor: int,
        marker: str,
    ) -> VoucherMutationResponse:
        """``POST /orders`` — one open POS order with exactly one voucher line.

        The location, the template, the price and the quantity are pinned
        literals; the customer and staffer come from an owner-approved plan and
        are validated as canonical UUIDs. Services, goods, discounts, promocodes,
        a second line and a bulk quantity have no parameter and no place in the
        body, so they cannot appear by accident or by argument.
        """
        pinned_location = _pinned(
            location_uuid, expected=KARLSRUHE_LOCATION_UUID, label="location_uuid", operation=CREATE_OPERATION
        )
        pinned_template = _pinned(
            voucher_template_uuid,
            expected=EASYWEEK_VOUCHER_TEMPLATE_UUID,
            label="voucher_template_uuid",
            operation=CREATE_OPERATION,
        )
        canonical_customer = _canonical_uuid(customer_uuid, label="customer_uuid", operation=CREATE_OPERATION)
        canonical_staffer = _canonical_uuid(staffer_uuid, label="staffer_uuid", operation=CREATE_OPERATION)
        exact_price = _pinned_price(price_minor, operation=CREATE_OPERATION)
        safe_marker = _safe_marker(marker, operation=CREATE_OPERATION)

        body: dict[str, Any] = {
            "location_uuid": pinned_location,
            "customer_uuid": canonical_customer,
            "staffer_uuid": canonical_staffer,
            "comment": safe_marker,
            "vouchers": [
                {
                    "voucher_template_uuid": pinned_template,
                    "price": exact_price,
                    "quantity": SUPPORTED_VOUCHER_QUANTITY,
                }
            ],
        }
        return await self._post_once(
            _PATH_ORDERS,
            body=body,
            operation=CREATE_OPERATION,
            allowed_fields=CREATE_REQUEST_FIELDS,
        )

    async def pay_voucher_order(self, *, order_uuid: str, account_uuid: str) -> VoucherMutationResponse:
        """``POST /orders/{uuid}/pay`` — one payment, on one account, once.

        The body is exactly ``{"account_uuid": ...}`` and nothing else. The
        documented endpoint takes no amount, and it should not: the sum is
        already fixed by the exact open order and its one voucher line, so a
        second place to state it would be a second place to state it *wrongly* —
        and a canary that could name an arbitrary sum would be a payment tool.

        The order UUID must come from the ledger and the account UUID from the
        same approved plan.
        """
        canonical_order = _canonical_uuid(order_uuid, label="order_uuid", operation=PAY_OPERATION)
        canonical_account = _canonical_uuid(account_uuid, label="account_uuid", operation=PAY_OPERATION)

        return await self._post_once(
            _PATH_ORDERS,
            canonical_order,
            _PATH_PAY,
            body={"account_uuid": canonical_account},
            operation=PAY_OPERATION,
            allowed_fields=PAY_REQUEST_FIELDS,
        )

    async def refund_voucher_order(self, *, order_uuid: str) -> VoucherMutationResponse:
        """``POST /orders/{uuid}/refund`` — the documented refund, no body.

        Deliberately parameterless beyond the order: there is no partial amount,
        no reason string and no free text, because none of those is documented
        and every one of them would be a way to put something into a real
        financial record.
        """
        canonical_order = _canonical_uuid(order_uuid, label="order_uuid", operation=REFUND_OPERATION)
        return await self._post_once(
            _PATH_ORDERS,
            canonical_order,
            _PATH_REFUND,
            body=None,
            operation=REFUND_OPERATION,
            allowed_fields=REFUND_REQUEST_FIELDS,
        )
