"""Altegio Record Update API client.

Confirmed Altegio endpoints used here:
  GET /record/{location_id}/{record_id}  — fetch appointment (fresh state for PUT)
  PUT /record/{location_id}/{record_id}  — update appointment (full payload required)

PUT /record requires a full payload. Partial PUT returns 422 with a list of
required fields. The payload is built verbatim from the GET response with only
the fields that we intentionally change (``services`` for price override and
``comment`` for the audit trail). send_sms is always forced to False so that
Altegio does not send a customer SMS for a bot-triggered price update.

Authorization: Bearer {partner_token},{user_token}
"""

from __future__ import annotations

import logging
from typing import Any

import httpx

from altegio_bot.settings import settings

logger = logging.getLogger(__name__)


class AltegioRecordUpdateError(Exception):
    """Raised when fetching or updating an Altegio appointment record fails."""


def _auth_header() -> str:
    return f"Bearer {settings.altegio_partner_token},{settings.altegio_user_token}"


def _headers() -> dict[str, str]:
    return {
        "Authorization": _auth_header(),
        "Accept": settings.altegio_api_accept,
        "Content-Type": "application/json",
    }


def _sanitize_error(message: str) -> str:
    sanitized = message
    for token in (settings.altegio_partner_token, settings.altegio_user_token):
        if token:
            sanitized = sanitized.replace(token, "[redacted]")
    return sanitized


async def fetch_altegio_record_for_update(
    *,
    location_id: int,
    record_id: int,
    timeout_sec: float = 20.0,
) -> dict[str, Any]:
    """Fetch a single Altegio appointment for use in a subsequent PUT.

    Returns the ``data`` dict from the Altegio GET response. The caller must
    pass this dict unchanged to ``update_altegio_record_price_and_comment`` so
    the full PUT payload is built from fresh, authoritative Altegio data.

    Raises AltegioRecordUpdateError on HTTP, network, JSON, or shape errors.
    Tokens are redacted before any error message is stored or logged.
    """
    base = settings.altegio_api_base_url.rstrip("/")
    url = f"{base}/record/{location_id}/{record_id}"

    try:
        async with httpx.AsyncClient(timeout=timeout_sec) as client:
            resp = await client.get(url, headers=_headers())
            resp.raise_for_status()
    except httpx.HTTPStatusError as exc:
        status = exc.response.status_code
        raise AltegioRecordUpdateError(
            f"GET /record HTTP {status}: location_id={location_id} record_id={record_id}"
        ) from exc
    except httpx.HTTPError as exc:
        err = _sanitize_error(str(exc))
        raise AltegioRecordUpdateError(
            f"GET /record network error: location_id={location_id} record_id={record_id}: {err}"
        ) from exc

    try:
        payload = resp.json()
    except Exception as exc:
        raise AltegioRecordUpdateError(
            f"GET /record invalid JSON: location_id={location_id} record_id={record_id}"
        ) from exc

    if not isinstance(payload, dict):
        raise AltegioRecordUpdateError(
            f"GET /record unexpected payload type {type(payload).__name__}: "
            f"location_id={location_id} record_id={record_id}"
        )

    data = payload.get("data")
    if not isinstance(data, dict):
        raise AltegioRecordUpdateError(
            f"GET /record unexpected data type {type(data).__name__}: location_id={location_id} record_id={record_id}"
        )

    return data


def normalize_record_client_for_put(record_data: dict[str, Any]) -> dict[str, Any]:
    """Extract only the client fields required by Altegio PUT /record.

    The full client object from GET contains many extra fields; only phone,
    name, and email are needed and accepted by the PUT endpoint.  Sending
    extra fields (id, display_name, …) has caused 422 errors in some Altegio
    account configurations.

    Field policy (matches smoke-tested PUT payload, May 2026):
      phone  — included only when truthy; empty string treated as absent.
      email  — included only when truthy; empty string treated as absent.
      name   — always included (Altegio requires this field); falls back to
               ``display_name`` when ``name`` is empty/absent, then to ``""``
               so the PUT never sends a missing ``name`` key.
    """
    client = record_data.get("client") or {}
    result: dict[str, Any] = {}

    phone = client.get("phone")
    if phone:
        result["phone"] = phone

    # name falls back to display_name to restore smoke-tested payload behaviour
    result["name"] = client.get("name") or client.get("display_name") or ""

    email = client.get("email")
    if email:
        result["email"] = email

    return result


def _value_or_zero(value: object) -> float:
    """Return ``float(value)`` if *value* is not None, else 0.0.

    Avoids the ``or 0`` anti-pattern where a legitimate zero price (free
    service) would silently fall through the truthiness check and be
    replaced by 0 from the ``or`` branch rather than preserved as-is.
    """
    if value is None:
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def build_minimal_service_for_put(
    svc: dict[str, Any],
    *,
    override_cost: float | None = None,
    override_first_cost: float | None = None,
    override_discount: float | None = None,
) -> dict[str, Any]:
    """Build a minimal service dict for Altegio PUT /record.

    Only id, first_cost, discount, and cost are included — the confirmed
    minimal structure from smoke testing (May 2026).  Extra fields
    (title, cost_to_pay, manual_cost, cost_per_unit, assistants, amount, …)
    are stripped to avoid PUT 422 errors on certain Altegio configurations.

    Altegio PUT /record field semantics:
      cost        — final charged price in EUR (after discount applied)
      first_cost  — original list price in EUR before discount
      discount    — discount amount in EUR (NOT a percentage)

    Pass override_* kwargs to change specific price fields from the source
    service; omitted overrides fall back to the source service values.
    ``_value_or_zero`` is used instead of ``or 0`` so that a legitimate zero
    price is preserved rather than silently replaced via truthiness.
    """
    return {
        "id": svc["id"],
        "first_cost": override_first_cost if override_first_cost is not None else _value_or_zero(svc.get("first_cost")),
        "discount": override_discount if override_discount is not None else _value_or_zero(svc.get("discount")),
        "cost": override_cost if override_cost is not None else _value_or_zero(svc.get("cost")),
    }


def _build_put_payload(
    record_data: dict[str, Any],
    *,
    new_services: list[dict[str, Any]],
    new_comment: str,
) -> dict[str, Any]:
    """Build a full PUT /record payload from a GET /record response.

    Altegio PUT /record requires the full appointment object; partial payloads
    return 422. All fields are taken verbatim from the GET response except
    ``services`` (price override) and ``comment`` (audit trail). ``send_sms``
    is always set to False so Altegio does not generate an SMS notification for
    this bot-triggered update.
    """
    staff = record_data.get("staff") or {}
    staff_id = record_data.get("staff_id") or staff.get("id")

    return {
        "staff_id": staff_id,
        "services": new_services,
        "client": normalize_record_client_for_put(record_data),
        "save_if_busy": record_data.get("save_if_busy", 1),
        "datetime": record_data.get("datetime", ""),
        "seance_length": record_data.get("seance_length") or record_data.get("length"),
        "send_sms": False,
        "comment": new_comment,
        "sms_remain_hours": record_data.get("sms_remain_hours"),
        "email_remain_hours": record_data.get("email_remain_hours"),
        "attendance": record_data.get("attendance", 0),
        "api_id": record_data.get("api_id"),
        "custom_color": record_data.get("custom_color"),
        "record_labels": record_data.get("record_labels") or [],
    }


async def update_altegio_record_price_and_comment(
    *,
    location_id: int,
    record_id: int,
    record_data: dict[str, Any],
    new_services: list[dict[str, Any]],
    new_comment: str,
    timeout_sec: float = 20.0,
) -> dict[str, Any]:
    """PUT /record/{location_id}/{record_id} — update service price and comment.

    Sends a full PUT payload built from ``record_data`` (the fresh GET /record
    response), with ``services`` replaced by ``new_services`` and ``comment``
    replaced by ``new_comment``. Price and comment are sent in one request to
    minimise race conditions.

    ``send_sms`` is forced False to prevent Altegio from triggering an SMS
    notification for this bot-originated price-override PUT.

    Raises AltegioRecordUpdateError on HTTP, network, JSON, shape errors, or
    when Altegio returns ``success != true``. Tokens are redacted from all
    error messages before storage or logging.
    """
    base = settings.altegio_api_base_url.rstrip("/")
    url = f"{base}/record/{location_id}/{record_id}"

    put_payload = _build_put_payload(
        record_data,
        new_services=new_services,
        new_comment=new_comment,
    )

    try:
        async with httpx.AsyncClient(timeout=timeout_sec) as client:
            resp = await client.put(url, headers=_headers(), json=put_payload)
            resp.raise_for_status()
    except httpx.HTTPStatusError as exc:
        status = exc.response.status_code
        err = _sanitize_error(str(exc))
        raise AltegioRecordUpdateError(
            f"PUT /record HTTP {status}: location_id={location_id} record_id={record_id}: {err}"
        ) from exc
    except httpx.HTTPError as exc:
        err = _sanitize_error(str(exc))
        raise AltegioRecordUpdateError(
            f"PUT /record network error: location_id={location_id} record_id={record_id}: {err}"
        ) from exc

    try:
        data = resp.json()
    except Exception as exc:
        raise AltegioRecordUpdateError(
            f"PUT /record invalid JSON: location_id={location_id} record_id={record_id}"
        ) from exc

    if not isinstance(data, dict):
        raise AltegioRecordUpdateError(
            f"PUT /record unexpected response type {type(data).__name__}: "
            f"location_id={location_id} record_id={record_id}"
        )

    if data.get("success") is not True:
        raise AltegioRecordUpdateError(
            f"PUT /record unsuccessful response: location_id={location_id} record_id={record_id}: {data!r}"
        )

    return data
