"""Non-persistent EasyWeek voucher calculation transport (evidence only).

This module exists so that exactly one grep answers "who may POST to EasyWeek
for a campaign?". It owns one documented, officially non-persistent endpoint::

    POST /orders/calculate

and nothing else. Deliberately absent, and deliberately not addable here:
``POST /orders``, ``POST /orders/{uuid}/pay``, ``POST /orders/{uuid}/refund``,
voucher-template writes, order/account/staffer reads, and any generic
``request``/``post`` escape hatch. A caller cannot hand this client a URL, a
body, a quantity, a discount, a promocode, a customer or a staffer.

Why a separate module
---------------------
``easyweek_client.EasyWeekClient`` is GET-only by construction (plan §1.6 p.8)
and stays that way. ``easyweek_migration.write_client`` is the cutover's
mutation surface; binding campaign evidence to it would let a migration change
widen the campaign path. So the transport *policy* is reused by importing the
pinned origin, timeout and typed errors from the read client, while the request
surface is defined here and only here.

One POST, ever
--------------
``/orders/calculate`` is documented as non-persistent, but "documented" is not
"proven for this workspace". This client therefore issues exactly one POST per
call and never retries — not on a timeout, not on a transport failure, not on a
429, not on any 5xx. Each of those leaves the outcome uninterpretable, and a
second POST would trade a clean unknown for a second unexplained server-side
event. Those outcomes are raised as :class:`EasyWeekCalculationUncertain`, which
is deliberately NOT a subclass of the generic retryable error: no "is it
retryable?" sweep can pick it up by accident.

Nothing here logs a secret, a header, a URL or a response body.
"""

from __future__ import annotations

import logging
import uuid as uuid_module
from dataclasses import dataclass
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
from altegio_bot.settings import settings

logger = logging.getLogger("easyweek_voucher_calculation")

# The only relative path this module may ever build. Both segments are
# constants: no caller-supplied path part exists, so no input can retarget it.
_PATH_ORDERS: Final = "orders"
_PATH_CALCULATE: Final = "calculate"
CALCULATE_OPERATION: Final = "calculate_voucher_order"

# The supported contract is one voucher line. Quantity is not a parameter: a
# caller that could pass it could also preview a bulk purchase, and no bulk
# evidence exists.
VOUCHER_QUANTITY: Final = 1

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

    The transport proves only that the server answered with a JSON object that
    carries an ``invoice`` key. Whether the invoice is a supported calculation
    is a domain question, answered by
    ``campaigns.easyweek_voucher_contract``.
    """

    http_status: int
    payload: dict[str, Any]


def _canonical_lowercase_uuid(value: object, *, label: str) -> str:
    """Return *value* only when it already is a canonical lowercase UUID.

    Checked BEFORE the request is built. ``uuid.UUID(...)`` would happily accept
    braces, urn prefixes, uppercase and stray whitespace and then normalise
    them; accepting those would mean the wire carries an identity the caller
    never typed. The offending value is never echoed into the error.
    """
    if not isinstance(value, str) or not value:
        raise EasyWeekPermanentError(f"{label} must be a canonical lowercase UUID", operation=CALCULATE_OPERATION)
    try:
        canonical = str(uuid_module.UUID(value))
    except (ValueError, AttributeError, TypeError):
        raise EasyWeekPermanentError(
            f"{label} must be a canonical lowercase UUID", operation=CALCULATE_OPERATION
        ) from None
    if canonical != value:
        raise EasyWeekPermanentError(f"{label} must be a canonical lowercase UUID", operation=CALCULATE_OPERATION)
    return canonical


def _exact_positive_minor_amount(value: object) -> int:
    """Return a strictly positive exact ``int`` price in minor units.

    ``type(value) is int`` rather than ``isinstance``: ``True`` is an ``int`` to
    ``isinstance`` and would silently become a one-cent price. Floats and
    numeric strings are refused too — money that survived a float is money we
    cannot prove.

    Zero and negative prices are refused by the transport, not merely by the
    domain layer: production evidence shows EasyWeek accepts both and returns a
    happily-calculated invoice, so the refusal has to sit where the request is
    built.
    """
    if type(value) is not int:
        raise EasyWeekPermanentError("price_minor must be an exact integer", operation=CALCULATE_OPERATION)
    if value <= 0:
        raise EasyWeekPermanentError("price_minor must be strictly positive", operation=CALCULATE_OPERATION)
    return value


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
                location_uuid=...,
                voucher_template_uuid=...,
                price_minor=...,
            )

    ``transport``, ``http_client`` and ``timeout`` exist for dependency
    injection: the unit suite drives an ``httpx.MockTransport`` and never
    touches the network.
    """

    def __init__(
        self,
        *,
        api_key: str | None = None,
        workspace_slug: str | None = None,
        base_url: str | None = None,
        timeout: httpx.Timeout | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
        http_client: httpx.AsyncClient | None = None,
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

        if http_client is not None:
            self._client = http_client
            self._owns_client = False
        else:
            self._client = httpx.AsyncClient(
                timeout=timeout or _DEFAULT_TIMEOUT,
                # A redirect would re-send the Authorization header to whatever
                # host the response named.
                follow_redirects=False,
                transport=transport,
            )
            self._owns_client = True

    # -- lifecycle ---------------------------------------------------------

    async def aclose(self) -> None:
        if self._owns_client:
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
        # No key, no slug, no headers: a repr lands in logs and tracebacks.
        return f"<EasyWeekVoucherCalculationClient base_url={self._base_url!r}>"

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

        The body is assembled here from validated scalars — there is no
        parameter that accepts a dict, so no caller can smuggle a discount, a
        promocode, a customer, a staffer, an account, goods, services or a
        second line into it.

        Outcomes, all of them terminal after a single POST:

        ==========================  ==========================================
        2xx with a readable object  :class:`VoucherCalculationResult`
        2xx, non-JSON/non-object    :class:`EasyWeekProtocolError`
        2xx without ``invoice``     :class:`EasyWeekProtocolError`
        401 / 403                   :class:`EasyWeekAuthError`
        404                         :class:`EasyWeekNotFoundError`
        422                         :class:`EasyWeekPermanentError` + field names
        other 4xx                   :class:`EasyWeekPermanentError`
        429 / any 5xx               :class:`EasyWeekCalculationUncertain`
        timeout / transport error   :class:`EasyWeekCalculationUncertain`
        ==========================  ==========================================

        429 is uncertain rather than retryable on purpose. Elsewhere a 429 is a
        safe retry because the limiter refuses the request before the handler
        runs — but this call exists to prove that nothing was persisted, and a
        second POST would make that proof weaker, not stronger.
        """
        canonical_location = _canonical_lowercase_uuid(location_uuid, label="location_uuid")
        canonical_template = _canonical_lowercase_uuid(voucher_template_uuid, label="voucher_template_uuid")
        exact_price = _exact_positive_minor_amount(price_minor)

        body: dict[str, Any] = {
            "location_uuid": canonical_location,
            "vouchers": [
                {
                    "voucher_template_uuid": canonical_template,
                    "price": exact_price,
                    "quantity": VOUCHER_QUANTITY,
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

        if 200 <= status < 300:
            return VoucherCalculationResult(http_status=status, payload=self._readable_object(response))

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
    def _readable_object(response: httpx.Response) -> dict[str, Any]:
        """Unwrap a 2xx body to the object that must carry ``invoice``.

        A 200 whose body is not JSON, not an object, or has no ``invoice`` is a
        contract problem, not a calculation: reporting it as a success would let
        an empty page from a proxy read as proven evidence. The body is never
        echoed into the error.
        """
        try:
            payload: Any = response.json()
        except Exception:
            raise EasyWeekProtocolError(
                "calculation response body is not valid JSON",
                operation=CALCULATE_OPERATION,
                status_code=response.status_code,
            ) from None

        if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
            payload = payload["data"]
        if not isinstance(payload, dict):
            raise EasyWeekProtocolError(
                "calculation response is not a JSON object",
                operation=CALCULATE_OPERATION,
                status_code=response.status_code,
            )
        if "invoice" not in payload:
            raise EasyWeekProtocolError(
                "calculation response carries no invoice",
                operation=CALCULATE_OPERATION,
                status_code=response.status_code,
            )
        return payload
