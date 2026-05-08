"""Apply a promo discount program to a booked Altegio visit.

Altegio endpoint (UNCONFIRMED — source: developer discussion, not OpenAPI spec):
  POST /visit/loyalty/apply_discount_program/{location_id}/{card_id}/{program_id}
  Expected request body: {"record_id": <altegio_record_id>}
  Expected response:     {"success": true, ...}  or HTTP error

The endpoint is guarded by promo_apply_discount_api_verified=False (default).
Do NOT enable promo_apply_discount_api_verified in production until the endpoint
shape is confirmed against the Altegio OpenAPI spec and smoke-tested.

Confirmed Altegio endpoints used elsewhere in this project (NOT this module):
  POST   /loyalty/cards/{location_id}           — issue card
  DELETE /loyalty/cards/{location_id}/{card_id} — delete card
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone

import httpx
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.models.models import Client, OutboxMessage, PromoLead, Record, RecordService
from altegio_bot.settings import settings

logger = logging.getLogger(__name__)


class PromoDiscountApplyError(Exception):
    """Raised when the Altegio apply_discount_program API call fails."""


@dataclass
class PromoDiscountApplyResult:
    applied: bool
    raw: dict


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _auth_header() -> str:
    return f"Bearer {settings.altegio_partner_token},{settings.altegio_user_token}"


def _headers() -> dict[str, str]:
    return {
        "Authorization": _auth_header(),
        "Accept": settings.altegio_api_accept,
        "Content-Type": "application/json",
    }


async def apply_promo_discount_to_visit(
    *,
    location_id: int,
    card_id: int,
    program_id: int | str,
    record_id: int | str,
) -> PromoDiscountApplyResult:
    """Apply a loyalty discount program to a visit via Altegio API.

    UNCONFIRMED endpoint — see module docstring. Requires
    promo_apply_discount_api_verified=True before calling.

    Raises PromoDiscountApplyError on HTTP, network, JSON, or shape failures.
    """
    base = settings.altegio_api_base_url.rstrip("/")
    url = f"{base}/visit/loyalty/apply_discount_program/{location_id}/{card_id}/{program_id}"

    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            resp = await client.post(
                url,
                headers=_headers(),
                json={"record_id": int(record_id)},
            )
            resp.raise_for_status()
    except httpx.HTTPError as exc:
        raise PromoDiscountApplyError(
            f"apply_discount_program HTTP error: location={location_id} card={card_id} program={program_id}: {exc}"
        ) from exc

    try:
        data = resp.json()
    except Exception as exc:
        raise PromoDiscountApplyError(f"apply_discount_program invalid JSON: location={location_id}: {exc}") from exc

    if not isinstance(data, dict):
        raise PromoDiscountApplyError(
            f"apply_discount_program unexpected response shape {type(data).__name__}: {data!r}"
        )

    return PromoDiscountApplyResult(applied=True, raw=data)


def _phone_variants(phone_e164: str) -> list[str]:
    digits = re.sub(r"\D+", "", phone_e164)
    variants = {phone_e164, digits, f"+{digits}"}
    return [v for v in variants if v]


def get_promo_allowed_service_ids() -> set[int]:
    """Parse promo_allowed_service_ids setting into a set of int service IDs."""
    raw = (settings.promo_allowed_service_ids or "").strip()
    if not raw:
        return set()
    result: set[int] = set()
    for part in raw.split(","):
        part = part.strip()
        if part:
            try:
                result.add(int(part))
            except ValueError:
                logger.warning("promo_discount: invalid service_id in promo_allowed_service_ids: %r", part)
    return result


async def find_applicable_promo_lead_for_record(
    session: AsyncSession,
    *,
    phone_e164: str,
    now: datetime,
) -> PromoLead | None:
    """Return the most recent active PromoLead eligible for discount application.

    Filters:
    - phone_e164 matches the booking client
    - campaign_name == settings.promo_campaign_name
    - status in ('issued', 'booked')
    - expires_at > now
    - loyalty_card_id IS NOT NULL
    - location_id IS NOT NULL
    - discount_program_id IS NOT NULL
    - meta.loyalty_card_issued == true
    - meta.promo_card_deleted_at IS NULL (card not yet cleaned up)
    """
    if not phone_e164:
        return None

    campaign = settings.promo_campaign_name
    stmt = (
        select(PromoLead)
        .where(PromoLead.phone_e164 == phone_e164)
        .where(PromoLead.campaign_name == campaign)
        .where(PromoLead.status.in_(["issued", "booked"]))
        .where(PromoLead.expires_at > now)
        .where(PromoLead.loyalty_card_id.is_not(None))
        .where(PromoLead.location_id.is_not(None))
        .where(PromoLead.discount_program_id.is_not(None))
        .where(PromoLead.meta["loyalty_card_issued"].astext == "true")
        .where(PromoLead.meta["promo_card_deleted_at"].astext.is_(None))
        .order_by(PromoLead.created_at.desc())
        .limit(1)
    )
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def _has_prior_attended_visits(
    session: AsyncSession,
    phone_e164: str,
    exclude_record_id: int,
) -> bool:
    """Return True if the client has prior attended visits, excluding the current record.

    Uses only locally synced records (Client + Record tables).
    TODO: A full Altegio CRM API check is deferred to a future PR.
    """
    variants = _phone_variants(phone_e164)
    stmt = (
        select(Record.id)
        .join(Client, Client.id == Record.client_id)
        .where(Client.phone_e164.in_(variants))
        .where(Record.is_deleted.is_(False))
        .where(or_(Record.attendance == 1, Record.visit_attendance == 1))
        .where(Record.id != exclude_record_id)
        .limit(1)
    )
    result = await session.execute(stmt)
    return result.scalar_one_or_none() is not None


async def _get_record_service_ids(session: AsyncSession, record_pk: int) -> set[int]:
    """Return the set of service_ids for a record."""
    stmt = select(RecordService.service_id).where(RecordService.record_id == record_pk)
    result = await session.execute(stmt)
    return set(result.scalars().all())


def _build_discount_applied_reply() -> str:
    return (
        "Gute Nachricht 🎁\n\n"
        "Ihr Neukundenrabatt wurde Ihrer Buchung zugeordnet.\n\n"
        "Bitte beachten Sie: In der Online-Buchung und in der ersten Bestätigung\n"
        "können noch reguläre Preise angezeigt werden. Unser Team sieht den Rabatt\n"
        "in Ihrer Buchung.\n\n"
        "Wir freuen uns auf Ihren Besuch 💙"
    )


async def try_apply_promo_discount(
    session: AsyncSession,
    record: Record,
    company_id: int,
) -> None:
    """Attempt to apply a promo discount to a newly booked Altegio visit.

    Called from inbox_worker.handle_event after a record create/update webhook.
    Fail-closed: controlled failures are recorded in PromoLead.meta and do not
    propagate as exceptions. Unexpected exceptions propagate to the caller so
    the event can be retried.

    Flow:
    1. Feature gate check (promo_apply_discount_enabled).
    2. Resolve client phone from record.
    3. Find matching PromoLead.
    4. Idempotency guard (already applied to this record).
    5. Service allowlist check (promo_allowed_service_ids).
    6. New-client guard (no prior attended visits, local DB only).
    7. Transition issued → booked (booking confirmed).
    8. API gate check (promo_apply_discount_api_verified).
    9. Call Altegio apply_discount_program API.
    10. Update PromoLead status → applied.
    11. Create OutboxMessage for customer reply.
    """
    cfg = settings

    if not cfg.promo_apply_discount_enabled:
        return

    now = _utcnow()

    if record.client_id is None:
        return

    client = await session.get(Client, record.client_id)
    if client is None or not client.phone_e164:
        return

    phone_e164 = client.phone_e164

    # ── 3. Find matching PromoLead ────────────────────────────────────────────
    lead = await find_applicable_promo_lead_for_record(
        session,
        phone_e164=phone_e164,
        now=now,
    )
    if lead is None:
        return

    meta = lead.meta or {}

    # ── 4. Idempotency guard ──────────────────────────────────────────────────
    if lead.status == "applied":
        if meta.get("discount_apply_altegio_record_id") == record.altegio_record_id:
            logger.info(
                "promo_discount: skip duplicate webhook record=%s lead_id=%s (already applied)",
                record.altegio_record_id,
                lead.id,
            )
            return
        if meta.get("discount_applied_at"):
            logger.info(
                "promo_discount: skip lead_id=%s already applied at %s",
                lead.id,
                meta.get("discount_applied_at"),
            )
            return

    # ── 5. Service allowlist check ────────────────────────────────────────────
    allowed_service_ids = get_promo_allowed_service_ids()
    if not allowed_service_ids:
        err = "promo_allowed_service_ids empty — discount not applied automatically"
        lead.meta = {**meta, "apply_error": err, "apply_skip_reason": err}
        logger.info("promo_discount: skip lead_id=%s %s", lead.id, err)
        return

    record_service_ids = await _get_record_service_ids(session, record.id)
    if not record_service_ids.intersection(allowed_service_ids):
        err = f"no allowed service in record: record_services={record_service_ids} allowed={allowed_service_ids}"
        lead.meta = {**meta, "apply_error": err, "apply_skip_reason": err}
        logger.info("promo_discount: skip lead_id=%s service not allowed", lead.id)
        return

    # ── 6. New-client guard ───────────────────────────────────────────────────
    # TODO: This check uses local DB only and may miss visits not yet synced
    # from Altegio. A full CRM API history check is deferred to a future PR.
    if await _has_prior_attended_visits(session, phone_e164, exclude_record_id=record.id):
        err = "client has prior attended visits — discount not applied"
        lead.meta = {**meta, "apply_error": err, "apply_skip_reason": err}
        logger.info("promo_discount: skip lead_id=%s prior visited client", lead.id)
        return

    # ── 7. Transition issued → booked ─────────────────────────────────────────
    if lead.status == "issued":
        meta = {
            **meta,
            "booked_at": now.isoformat(),
            "booked_record_id": record.id,
            "booked_altegio_record_id": record.altegio_record_id,
        }
        lead.status = "booked"
        lead.meta = meta
        lead.record_id = record.id
        lead.altegio_record_id = record.altegio_record_id
        logger.info(
            "promo_discount: issued→booked lead_id=%s record_id=%s",
            lead.id,
            record.id,
        )

    # ── 8. API gate ───────────────────────────────────────────────────────────
    if not cfg.promo_apply_discount_api_verified:
        err = "promo_apply_discount_api_verified=False — discount apply blocked until endpoint is verified"
        lead.meta = {
            **meta,
            "discount_apply_error": err,
            "discount_apply_attempted_at": now.isoformat(),
        }
        logger.warning("promo_discount: api not verified, blocking apply for lead_id=%s", lead.id)
        return

    # ── 9. Validate required fields ───────────────────────────────────────────
    location_id = lead.location_id
    card_id_raw = lead.loyalty_card_id
    program_id_raw = lead.discount_program_id
    altegio_record_id = record.altegio_record_id

    if not location_id or not card_id_raw or not program_id_raw or not altegio_record_id:
        err = (
            f"missing required fields: location_id={location_id} "
            f"card_id={card_id_raw} program_id={program_id_raw} "
            f"altegio_record_id={altegio_record_id}"
        )
        lead.status = "apply_failed"
        lead.meta = {
            **meta,
            "discount_apply_error": err,
            "discount_apply_attempted_at": now.isoformat(),
        }
        logger.warning("promo_discount: missing fields lead_id=%s %s", lead.id, err)
        return

    try:
        card_id = int(card_id_raw)
    except (ValueError, TypeError) as exc:
        err = f"invalid card_id: {exc}"
        lead.status = "apply_failed"
        lead.meta = {
            **meta,
            "discount_apply_error": err,
            "discount_apply_attempted_at": now.isoformat(),
        }
        logger.warning("promo_discount: invalid card_id lead_id=%s %s", lead.id, err)
        return

    program_id: int | str = program_id_raw

    # ── 10. Call Altegio API ──────────────────────────────────────────────────
    try:
        api_result = await apply_promo_discount_to_visit(
            location_id=location_id,
            card_id=card_id,
            program_id=program_id,
            record_id=altegio_record_id,
        )
    except PromoDiscountApplyError as exc:
        err = str(exc)
        lead.status = "apply_failed"
        lead.meta = {
            **meta,
            "discount_apply_error": err,
            "discount_apply_attempted_at": now.isoformat(),
        }
        logger.warning("promo_discount: API failed lead_id=%s: %s", lead.id, exc)
        return

    # ── 11. Update PromoLead → applied ─────────────────────────────────────────
    lead.status = "applied"
    lead.applied_at = now
    lead.record_id = record.id
    lead.altegio_record_id = record.altegio_record_id
    lead.meta = {
        **meta,
        "discount_applied_at": now.isoformat(),
        "discount_apply_result": api_result.raw,
        "discount_apply_record_id": record.id,
        "discount_apply_altegio_record_id": record.altegio_record_id,
        "discount_apply_location_id": location_id,
        "discount_apply_card_id": card_id,
        "discount_apply_program_id": program_id,
    }
    logger.info(
        "promo_discount: applied lead_id=%s record_id=%s card_id=%s",
        lead.id,
        record.id,
        card_id,
    )

    # ── 12. Queue customer WhatsApp reply ──────────────────────────────────────
    body = _build_discount_applied_reply()
    session.add(
        OutboxMessage(
            company_id=company_id,
            client_id=record.client_id,
            record_id=record.id,
            job_id=None,
            sender_id=None,
            phone_e164=phone_e164,
            template_code="wa_promo_discount_applied",
            language="de",
            body=body,
            status="queued",
            provider_message_id=None,
            scheduled_at=now,
            sent_at=None,
            message_source="bot",
            meta={
                "source": "promo_discount_apply",
                "lead_id": lead.id,
                "record_id": record.id,
                "altegio_record_id": record.altegio_record_id,
            },
        )
    )
