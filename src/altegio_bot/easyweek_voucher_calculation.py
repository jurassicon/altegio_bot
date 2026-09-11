"""Non-persistent EasyWeek voucher calculation transport (evidence only).

This module exists so that exactly one grep answers "who may POST to EasyWeek
for a campaign?". It owns one documented, officially non-persistent endpoint::

    POST /orders/calculate

and nothing else. Deliberately absent, and deliberately not addable here: the
persistent order endpoint, its pay and refund actions, voucher-template writes,
order/account/staffer reads, and any generic ``request``/``post`` escape hatch.
A caller cannot hand this client a URL, a body, a quantity, a discount, a
promocode, a customer or a staffer.

Literal-pinned scope
--------------------
The public method does not accept "some canonical UUID" and "some positive
price". It accepts exactly the confirmed Karlsruhe location, exactly the
confirmed voucher template and exactly the supported €15 nominal — the literals
in :mod:`altegio_bot.easyweek_voucher_identity`. A syntactically valid but
different identity, or a different price, is refused before the wire. The
operator flow still reads the price from the fresh template first and only
reaches this method once ``cost == value == 1500`` is proven; this transport is
the second, independent fence, not the first one.

No redirect, ever
-----------------
The client always creates and owns its own ``httpx.AsyncClient`` with
``follow_redirects=False``. There is no parameter through which a caller could
supply a pre-built client, because a client built with ``follow_redirects=True``
would let a ``307``/``308`` answer replay this POST — Authorization header and
body included — against the *persistent* order endpoint. A 3xx is therefore a
typed, fail-closed refusal after the single request, and ``Location`` is never
read.

One POST, ever
--------------
``/orders/calculate`` is documented as non-persistent, but "documented" is not
"proven for this workspace". This client issues exactly one POST per call and
never retries — not on a timeout, not on a transport failure, not on a 429, not
on any 5xx. Each of those leaves the outcome uninterpretable, and a second POST
would trade a clean unknown for a second unexplained server-side event. Those
outcomes are raised as :class:`EasyWeekCalculationUncertain`, which is
deliberately NOT a subclass of the generic retryable error: no "is it
retryable?" sweep can pick it up by accident, and no caller may re-run the
command automatically after one.

Nothing here logs or reprs a secret, a header, a URL or a response body.
"""

from __future__ import annotations

import logging
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
    EasyWeekProtocolError,
    _normalize_base_url,
    _unwrap_secret,
)
from altegio_bot.easyweek_voucher_identity import (
    EASYWEEK_VOUCHER_TEMPLATE_UUID,
    EASYWEEK_WORKSPACE_SLUG,
    KARLSRUHE_LOCATION_UUID,
    SUPPORTED_VOUCHER_PRICE_MINOR,
    SUPPORTED_VOUCHER_QUANTITY,
)
from altegio_bot.settings import settings

logger = logging.getLogger("easyweek_voucher_calculation")

# The only relative path this module may ever build. Both segments are
# constants: no caller-supplied path part exists, so no input can retarget it.
_PATH_ORDERS: Final = "orders"
_PATH_CALCULATE: Final = "calculate"
CALCULATE_OPERATION: Final = "calculate_voucher_order"

# Re-exported so a reader of this module sees the pinned line without following
# an import. The transport accepts nothing else.
VOUCHER_QUANTITY: Final = SUPPORTED_VOUCHER_QUANTITY

# Every redirect status. None of them is followed; each is a fail-closed refusal.
_REDIRECT_STATUSES: Final = frozenset({301, 302, 303, 307, 308})

# Field names that may be echoed back from a 422. Nothing outside this set —
# and never a value, a message or a body — reaches an operator.
CALCULATE_REQUEST_FIELDS: Final = frozenset(
    {
        "location_uuid",
        "vouchers",
        "voucher_template_uuid",
        "price",
        "quantity",
    }
)


class EasyWeekCalculationUncertain(EasyWeekError):
    """The single POST left an outcome we cannot interpret. Never auto-retried.

    Not a subclass of :class:`~altegio_bot.easyweek_client.EasyWeekRetryableError`
    on purpose: "retryable" invites another attempt, and this class exists to
    say the opposite. A caller has to name it and stop.
    """


@dataclass(frozen=True)
class VoucherCalculationResult:
    """A 2xx answer whose envelope was readable. Amounts are NOT judged here.

    ``envelope`` is the COMPLETE parsed JSON object, exactly as the server sent
    it — nothing is unwrapped and nothing is dropped. An earlier version handed
    back only the inner ``data`` object, which silently discarded any
    ``order_uuid``/``status`` sitting on the outer envelope: precisely the
    signals this evidence path exists to catch.

    It carries ``repr=False`` because a dataclass repr lands in tracebacks,
    pytest output and log records, and this field may hold a voucher artifact,
    a customer subtree or a URL.
    """

    http_status: int
    envelope: dict[str, Any] = field(repr=False)


def _pinned_identity(value: object, *, expected: str, label: str) -> str:
    """Accept only the one confirmed literal for *label*.

    Deliberately an equality check rather than "is this a canonical UUID?". A
    well-formed UUID for another branch or another product would still be a
    request this evidence path has no permission to make, and the difference
    between "syntactically valid" and "the one we proved" is the whole fence.
    The offending value is never echoed into the error.
    """
    if not isinstance(value, str) or value != expected:
        raise EasyWeekPermanentError(
            f"{label} is not the confirmed voucher-scope identity",
            operation=CALCULATE_OPERATION,
        )
    return expected


def _pinned_price_minor(value: object) -> int:
    """Accept only the exact supported nominal, as an exact ``int``.

    ``type(value) is int`` rather than ``isinstance``: ``True`` is an ``int`` to
    ``isinstance`` and would silently become a one-cent price. Floats and
    numeric strings are refused too — money that survived a float is money we
    cannot prove.

    Zero, negative and merely-different prices are all refused here, not only in
    the domain layer: production evidence shows EasyWeek accepts ``0`` and
    ``1499`` and returns a happily calculated invoice, so the refusal has to sit
    where the request is built.
    """
    if type(value) is not int:
        raise EasyWeekPermanentError("price_minor must be an exact integer", operation=CALCULATE_OPERATION)
    if value != SUPPORTED_VOUCHER_PRICE_MINOR:
        raise EasyWeekPermanentError(
            "price_minor is not the supported voucher nominal",
            operation=CALCULATE_OPERATION,
        )
    return SUPPORTED_VOUCHER_PRICE_MINOR


def _safe_validation_fields(response: httpx.Response) -> list[str]:
    """Names of fields WE sent that a 422 complained about; never values.

    Laravel reports nested failures as dotted paths (``vouchers.0.price``). Each
    path is split and only segments in :data:`CALCULATE_REQUEST_FIELDS` survive,
    so an operator learns which field, never what was in it and never the
    server's prose. Numeric indices are dropped with everything else.
    """
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
            if segment in CALCULATE_REQUEST_FIELDS:
                recognised.add(segment)
    return sorted(recognised)


class EasyWeekVoucherCalculationClient:
    """POSTs one documented non-persistent voucher calculation, and nothing else.

    Usage::

        async with EasyWeekVoucherCalculationClient() as client:
            result = await client.calculate_single_voucher(
                location_uuid=KARLSRUHE_LOCATION_UUID,
                voucher_template_uuid=EASYWEEK_VOUCHER_TEMPLATE_UUID,
                price_minor=SUPPORTED_VOUCHER_PRICE_MINOR,
            )

    ``transport`` and ``timeout`` are the ONLY injection points, and neither can
    change redirect policy: the unit suite drives an ``httpx.MockTransport`` and
    never touches the network. There is deliberately no ``http_client``
    parameter — see the module docstring.
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
        if slug.strip() != EASYWEEK_WORKSPACE_SLUG:
            raise EasyWeekConfigError("EASYWEEK_WORKSPACE_SLUG is not the confirmed voucher workspace")

        self._api_key = key.strip()
        self._workspace_slug = EASYWEEK_WORKSPACE_SLUG
        self._base_url = _normalize_base_url(base_url if base_url is not None else settings.easyweek_api_base_url)

        # Always built here, always owned here. A caller cannot supply a client,
        # so a caller cannot turn redirects back on and let a 307 replay this
        # POST against the persistent order endpoint.
        self._client = httpx.AsyncClient(
            timeout=timeout or _DEFAULT_TIMEOUT,
            follow_redirects=False,
            transport=transport,
        )

    # -- lifecycle ---------------------------------------------------------

    async def aclose(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> EasyWeekVoucherCalculationClient:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.aclose()

    def __repr__(self) -> str:
        # No key, no slug, no headers and no URL: a repr lands in logs and
        # tracebacks, and the base URL is the one thing an operator never needs
        # from a repr but an incident report should never carry either.
        return "<EasyWeekVoucherCalculationClient>"

    __str__ = __repr__

    # -- internals ---------------------------------------------------------

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._api_key}",
            "Workspace": self._workspace_slug,
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    # -- the one public operation -----------------------------------------

    async def calculate_single_voucher(
        self,
        *,
        location_uuid: str,
        voucher_template_uuid: str,
        price_minor: int,
    ) -> VoucherCalculationResult:
        """``POST /orders/calculate`` for exactly one voucher line, exactly once.

        All three arguments must equal the confirmed literals; they exist as
        arguments so that a caller states its intent explicitly and a test can
        prove the refusal, not so that a caller can choose. The body is
        assembled here from those validated scalars — there is no parameter that
        accepts a dict, so no caller can smuggle a discount, a promocode, a
        customer, a staffer, an account, goods, services or a second line in.

        Outcomes, all of them terminal after a single request:

        ==============================  ======================================
        2xx with a readable envelope    :class:`VoucherCalculationResult`
        2xx, non-JSON/non-object        :class:`EasyWeekProtocolError`
        2xx without ``invoice``         :class:`EasyWeekProtocolError`
        301/302/303/307/308             :class:`EasyWeekPermanentError`
        401 / 403                       :class:`EasyWeekAuthError`
        404                             :class:`EasyWeekNotFoundError`
        422                             :class:`EasyWeekPermanentError` + fields
        other 4xx                       :class:`EasyWeekPermanentError`
        429 / any 5xx                   :class:`EasyWeekCalculationUncertain`
        timeout / transport error       :class:`EasyWeekCalculationUncertain`
        ==============================  ======================================

        429 is uncertain rather than retryable on purpose. Elsewhere a 429 is a
        safe retry because the limiter refuses the request before the handler
        runs — but this call exists to prove that nothing was persisted, and a
        second POST would make that proof weaker, not stronger.
        """
        pinned_location = _pinned_identity(location_uuid, expected=KARLSRUHE_LOCATION_UUID, label="location_uuid")
        pinned_template = _pinned_identity(
            voucher_template_uuid,
            expected=EASYWEEK_VOUCHER_TEMPLATE_UUID,
            label="voucher_template_uuid",
        )
        exact_price = _pinned_price_minor(price_minor)

        body: dict[str, Any] = {
            "location_uuid": pinned_location,
            "vouchers": [
                {
                    "voucher_template_uuid": pinned_template,
                    "price": exact_price,
                    "quantity": SUPPORTED_VOUCHER_QUANTITY,
                }
            ],
        }
        url = "/".join([self._base_url, _PATH_ORDERS, _PATH_CALCULATE])

        try:
            response = await self._client.post(url, headers=self._headers(), json=body)
        except httpx.TimeoutException:
            logger.error("easyweek_voucher_calculation: timeout — outcome UNKNOWN, no second POST")
            raise EasyWeekCalculationUncertain(
                "calculation timed out; outcome unknown",
                operation=CALCULATE_OPERATION,
                attempts=1,
            ) from None
        except httpx.HTTPError as exc:
            # Only the exception CLASS: an httpx message can embed the full URL.
            logger.error(
                "easyweek_voucher_calculation: transport failure error_type=%s — outcome UNKNOWN, no second POST",
                type(exc).__name__,
            )
            raise EasyWeekCalculationUncertain(
                "calculation transport failure; outcome unknown",
                operation=CALCULATE_OPERATION,
                attempts=1,
            ) from None

        status = response.status_code
        logger.info("easyweek_voucher_calculation: status=%s attempts=1", status)

        if status in _REDIRECT_STATUSES:
            # `Location` is neither read nor logged. A 307/308 preserves method
            # and body, so following one here would be a second POST — possibly
            # at the persistent order endpoint — carrying the Authorization
            # header. There is no interpretation of a redirect that this
            # evidence path is allowed to act on.
            logger.error("easyweek_voucher_calculation: redirect refused status=%s", status)
            raise EasyWeekPermanentError(
                "calculation endpoint answered with a redirect; not followed",
                operation=CALCULATE_OPERATION,
                status_code=status,
                attempts=1,
            )

        if 200 <= status < 300:
            return VoucherCalculationResult(http_status=status, envelope=self._readable_envelope(response))

        if status == 429 or 500 <= status < 600:
            logger.error(
                "easyweek_voucher_calculation: status=%s — outcome UNKNOWN, no second POST",
                status,
            )
            raise EasyWeekCalculationUncertain(
                "calculation outcome unknown",
                operation=CALCULATE_OPERATION,
                status_code=status,
                attempts=1,
            )

        if status in (401, 403):
            raise EasyWeekAuthError(
                "authentication or authorization failed",
                operation=CALCULATE_OPERATION,
                status_code=status,
                attempts=1,
            )
        if status == 404:
            raise EasyWeekNotFoundError(
                "resource not found",
                operation=CALCULATE_OPERATION,
                status_code=status,
                attempts=1,
            )
        if status == 422:
            fields = _safe_validation_fields(response)
            logger.error(
                "easyweek_voucher_calculation: rejected status=422 fields=%s",
                ",".join(fields) if fields else "unnamed",
            )
            raise EasyWeekPermanentError(
                "calculation rejected as invalid: " + (",".join(fields) if fields else "no recognised field named"),
                operation=CALCULATE_OPERATION,
                status_code=status,
                attempts=1,
            )
        raise EasyWeekPermanentError(
            "permanent client error",
            operation=CALCULATE_OPERATION,
            status_code=status,
            attempts=1,
        )

    @staticmethod
    def _readable_envelope(response: httpx.Response) -> dict[str, Any]:
        """Return the COMPLETE 2xx body, once it is known to carry an invoice.

        Nothing is unwrapped: the domain projection needs every level of the
        envelope, because ``order_uuid`` and ``status`` may sit on the outer
        object, inside ``data`` or inside ``invoice``, and a value on a level
        this transport had discarded would have been a persistence signal lost.

        A 200 whose body is not JSON, not an object, or carries no ``invoice``
        at any supported level is a contract problem, not a calculation:
        reporting it as a success would let an empty page from a proxy read as
        proven evidence. The body is never echoed into the error.
        """
        try:
            payload: Any = response.json()
        except Exception:
            raise EasyWeekProtocolError(
                "calculation response body is not valid JSON",
                operation=CALCULATE_OPERATION,
                status_code=response.status_code,
            ) from None

        if not isinstance(payload, dict):
            raise EasyWeekProtocolError(
                "calculation response is not a JSON object",
                operation=CALCULATE_OPERATION,
                status_code=response.status_code,
            )

        inner = payload.get("data")
        carries_invoice = "invoice" in payload or (isinstance(inner, dict) and "invoice" in inner)
        if not carries_invoice:
            raise EasyWeekProtocolError(
                "calculation response carries no invoice",
                operation=CALCULATE_OPERATION,
                status_code=response.status_code,
            )
        return payload
