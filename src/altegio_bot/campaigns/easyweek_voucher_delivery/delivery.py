"""The one Meta request this canary may make (§36).

Deliberately not the shared provider
------------------------------------
``MetaCloudProvider`` is the right tool for campaign traffic: it caches senders,
logs what it sent, and lives behind a worker that retries. Every one of those is
wrong here. The parameter list contains a bearer secret, so it must never reach
a log line; and the whole safety model rests on the request happening at most
once, so it must not sit behind anything whose job is to try again.

So this module owns its own client, sends one POST, and classifies the answer
into three outcomes and no fourth.

Accepted is not delivered
-------------------------
A 2xx with a message id means Meta took responsibility for the message. It does
not mean a phone displayed it. ``delivered`` and ``read`` are only ever written
by a webhook naming this exact message id.

Doubt is not failure
--------------------
A timeout, a reset, a 429, a 5xx, a 409, an unreadable 2xx or a 2xx without a
message id all mean the same thing: the message may be on its way to a real
person and we cannot prove otherwise. They are UNKNOWN — never retried, never
refunded automatically, always escalated to a human. Only a refusal Meta itself
attributes to the request, before acting on it, is a rejection — and even that
does not re-open the send, because the ledger allows one attempt per lifetime.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from types import TracebackType
from typing import Any, Final

import httpx

from altegio_bot.settings import settings

logger = logging.getLogger("easyweek_voucher_delivery")

# Outcome vocabulary. A caller maps these to ledger states; nothing else.
DELIVERY_ACCEPTED: Final = "provider_accepted"
DELIVERY_UNKNOWN: Final = "unknown"
DELIVERY_REJECTED: Final = "rejected"

# Stable, PII-free reasons. None of them carries a body, a number or a parameter.
REASON_TIMEOUT: Final = "meta_timeout"
REASON_TRANSPORT: Final = "meta_transport_failure"
REASON_UNREADABLE: Final = "meta_unreadable_response"
REASON_NO_MESSAGE_ID: Final = "meta_no_message_id"
REASON_RATE_LIMITED: Final = "meta_rate_limited"
REASON_CONFLICT: Final = "meta_conflict"
REASON_SERVER_ERROR: Final = "meta_server_error"
REASON_REDIRECT: Final = "meta_redirect_refused"
REASON_REJECTED: Final = "meta_rejected_request"

# Statuses whose refusal Meta decided before acting on the message. Everything
# else in the 4xx range stays unknown: a 409 may mean the message already
# exists, and a 429 may have been counted after the handler ran.
_PROVEN_REJECTION_STATUSES: Final = frozenset({400, 401, 403, 404, 422})
_REDIRECT_STATUSES: Final = frozenset({301, 302, 303, 307, 308})

_DEFAULT_TIMEOUT: Final = httpx.Timeout(connect=5.0, read=20.0, write=10.0, pool=5.0)


@dataclass(frozen=True)
class DeliveryOutcome:
    """What one send attempt established, and nothing more.

    ``repr`` deliberately carries no payload: this object reaches exception
    handlers and log records.
    """

    outcome: str
    reason: str | None = None
    provider_message_id: str | None = None
    http_status: int | None = None

    @property
    def accepted(self) -> bool:
        return self.outcome == DELIVERY_ACCEPTED

    @property
    def unknown(self) -> bool:
        return self.outcome == DELIVERY_UNKNOWN

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "outcome": self.outcome,
            "reason": self.reason,
            "http_status": self.http_status,
            "provider_message_recorded": self.provider_message_id is not None,
        }


def _message_id(payload: object) -> str | None:
    """The one message id Meta returned, or ``None``.

    Exactly one: we sent exactly one message, and a response describing several
    is a response about something we do not understand.
    """
    if not isinstance(payload, dict):
        return None
    messages = payload.get("messages")
    if not isinstance(messages, list) or len(messages) != 1:
        return None
    first = messages[0]
    if not isinstance(first, dict):
        return None
    identifier = first.get("id")
    if not isinstance(identifier, str) or not identifier.strip():
        return None
    return identifier.strip()


class VoucherDeliveryClient:
    """One POST to Meta's messages endpoint, and no second one.

    The client is created and owned here with ``follow_redirects=False``: a
    307 preserves method and body, so following one would re-send the voucher
    code, Authorization header and all, wherever the response pointed.
    """

    def __init__(
        self,
        *,
        access_token: str | None = None,
        graph_url: str | None = None,
        api_version: str | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
        timeout: httpx.Timeout | None = None,
    ) -> None:
        self._token = (access_token if access_token is not None else settings.whatsapp_access_token).strip()
        self._graph_url = (graph_url if graph_url is not None else settings.whatsapp_graph_url).strip().rstrip("/")
        self._api_version = (api_version if api_version is not None else settings.whatsapp_api_version).strip()
        self._client = httpx.AsyncClient(
            timeout=timeout or _DEFAULT_TIMEOUT,
            transport=transport,
            follow_redirects=False,
        )

    async def __aenter__(self) -> "VoucherDeliveryClient":
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._client.aclose()

    def configured(self) -> bool:
        return bool(self._token and self._graph_url and self._api_version)

    async def send_voucher_template(
        self,
        *,
        phone_number_id: str,
        to_e164: str,
        template_name: str,
        language: str,
        params: list[str],
    ) -> DeliveryOutcome:
        """Send the approved voucher template once. Never repeats.

        ``params`` contains the voucher code in slot two. It is used to build
        the request body and is never logged, stored or attached to the returned
        outcome — not even its length.
        """
        url = f"{self._graph_url}/{self._api_version}/{phone_number_id}/messages"
        payload = {
            "messaging_product": "whatsapp",
            "to": to_e164.lstrip("+"),
            "type": "template",
            "template": {
                "name": template_name,
                "language": {"code": language},
                "components": [
                    {
                        "type": "body",
                        "parameters": [{"type": "text", "text": value} for value in params],
                    }
                ],
            },
        }

        try:
            response = await self._client.post(
                url,
                headers={"Authorization": f"Bearer {self._token}"},
                json=payload,
            )
        except httpx.TimeoutException:
            # The request may have been delivered and the answer lost.
            logger.error("easyweek_voucher_delivery: send timed out — outcome UNKNOWN, no retry")
            return DeliveryOutcome(outcome=DELIVERY_UNKNOWN, reason=REASON_TIMEOUT)
        except httpx.HTTPError as exc:
            # Only the exception CLASS: an httpx message can embed the full URL.
            logger.error(
                "easyweek_voucher_delivery: transport failure error_type=%s — outcome UNKNOWN, no retry",
                type(exc).__name__,
            )
            return DeliveryOutcome(outcome=DELIVERY_UNKNOWN, reason=REASON_TRANSPORT)

        status = response.status_code
        logger.info("easyweek_voucher_delivery: send status=%s attempts=1", status)

        if status in _REDIRECT_STATUSES:
            # Not followed, but the request did leave this process.
            return DeliveryOutcome(outcome=DELIVERY_UNKNOWN, reason=REASON_REDIRECT, http_status=status)

        if 200 <= status < 300:
            try:
                body: Any = response.json()
            except Exception:
                body = None
            identifier = _message_id(body)
            if identifier is None:
                # A 2xx we cannot tie to a message id is not a failure we may
                # record: the message probably went out and we have nothing to
                # match a webhook against.
                logger.error("easyweek_voucher_delivery: 2xx without a usable message id — UNKNOWN")
                return DeliveryOutcome(
                    outcome=DELIVERY_UNKNOWN,
                    reason=REASON_NO_MESSAGE_ID,
                    http_status=status,
                )
            return DeliveryOutcome(
                outcome=DELIVERY_ACCEPTED,
                provider_message_id=identifier,
                http_status=status,
            )

        if status == 429:
            return DeliveryOutcome(outcome=DELIVERY_UNKNOWN, reason=REASON_RATE_LIMITED, http_status=status)
        if status == 409:
            return DeliveryOutcome(outcome=DELIVERY_UNKNOWN, reason=REASON_CONFLICT, http_status=status)
        if 500 <= status < 600:
            return DeliveryOutcome(outcome=DELIVERY_UNKNOWN, reason=REASON_SERVER_ERROR, http_status=status)

        if status in _PROVEN_REJECTION_STATUSES and _carries_meta_error(response):
            # Meta's own error envelope on a status it decides before acting.
            # A rejection still does not re-open the send: the ledger allows one
            # attempt for the lifetime of the row.
            logger.error("easyweek_voucher_delivery: send rejected status=%s", status)
            return DeliveryOutcome(outcome=DELIVERY_REJECTED, reason=REASON_REJECTED, http_status=status)

        logger.error("easyweek_voucher_delivery: unattributable status=%s — UNKNOWN", status)
        return DeliveryOutcome(outcome=DELIVERY_UNKNOWN, reason=REASON_UNREADABLE, http_status=status)


def _carries_meta_error(response: httpx.Response) -> bool:
    """Did Meta answer, rather than something in front of it?

    Only the SHAPE is inspected — an ``error`` object in a JSON body. No code,
    message, trace id or subcode is read, kept or reported: a Meta error message
    can quote the request it refused.
    """
    if "json" not in response.headers.get("content-type", "").casefold():
        return False
    try:
        payload: Any = response.json()
    except Exception:
        return False
    return isinstance(payload, dict) and isinstance(payload.get("error"), dict)


__all__ = [
    "DELIVERY_ACCEPTED",
    "DELIVERY_REJECTED",
    "DELIVERY_UNKNOWN",
    "DeliveryOutcome",
    "VoucherDeliveryClient",
]
