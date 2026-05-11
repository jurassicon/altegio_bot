"""Apply a promo discount to a booked Altegio visit.

Two apply modes are supported (see Settings.promo_apply_mode):

record_price_override (default, confirmed working — smoke-tested May 2026):
  GET /record/{location_id}/{record_id}   — fetch fresh record
  PUT /record/{location_id}/{record_id}   — update service price + audit comment
  Simple case  (1 record for client that day, 1 allowed service):
    price override applied automatically, customer notification queued.
  Complex case (multiple records same day or multiple allowed services):
    admin comment written with manual-review marker, lead set to booked,
    no automatic price change and no customer notification.

legacy loyalty-program path (NOT used for automatic apply — kept for
backward compatibility with existing direct-wrapper tests and smoke scripts):
  POST /visit/loyalty/apply_discount_program/{location_id}/{card_id}/{program_id}
  UNCONFIRMED endpoint — source: developer discussion, not OpenAPI spec.

Confirmed Altegio endpoints used elsewhere in this project (NOT this module):
  POST   /loyalty/cards/{location_id}           — issue card
  DELETE /loyalty/cards/{location_id}/{card_id} — delete card
"""

from __future__ import annotations

import logging
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import httpx
from sqlalchemy import or_, select
from sqlalchemy.exc import IntegrityError as SAIntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.altegio_record_update import (
    AltegioRecordUpdateError,
    fetch_altegio_record_for_update,
    update_altegio_record_price_and_comment,
)
from altegio_bot.models.models import Client, MessageJob, PromoLead, Record, RecordService
from altegio_bot.settings import settings

_LOCAL_TZ = ZoneInfo("Europe/Belgrade")

# Regex matching both promo comment markers:
#   [PromoLead:<id>]         — simple automatic price override
#   [PromoLead:<id>:manual]  — complex manual-review annotation
_PROMO_MARKER_RE = re.compile(r"\[PromoLead:\d+(?::\w+)?\]")

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
    if not settings.promo_apply_discount_api_verified:
        raise PromoDiscountApplyError(
            "apply_promo_discount_api_verified=False — API call blocked until endpoint is verified"
        )

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

    if data.get("success") is not True:
        raise PromoDiscountApplyError(
            f"apply_discount_program unsuccessful response:"
            f" location={location_id} card={card_id} program={program_id}: {data!r}"
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


def is_promo_origin_comment(comment: str | None) -> bool:
    """Return True if ``comment`` contains a promo price-override marker.

    Markers written by this module:
      [PromoLead:<id>]         — simple automatic price override (applied)
      [PromoLead:<id>:manual]  — complex manual-review annotation (booked)

    Used by inbox_worker to suppress the normal record_updated customer
    notification when the update was triggered by our own promo PUT.
    """
    if not comment:
        return False
    return bool(_PROMO_MARKER_RE.search(comment))


async def _count_same_day_records_for_client(
    session: AsyncSession,
    *,
    client_id: int | None,
    company_id: int,
    reference_starts_at: datetime | None,
) -> int:
    """Count non-deleted records for the client on the same local calendar day.

    The local timezone is Europe/Belgrade, matching the rest of the project.

    Returns 2 (forces complex case) when ``client_id`` or
    ``reference_starts_at`` is None so the caller treats an unknown date
    context as ambiguous and does not auto-apply a price override.
    """
    if client_id is None or reference_starts_at is None:
        return 2  # unknown → force complex/manual case (fail-closed)

    reference_date = reference_starts_at.astimezone(_LOCAL_TZ).date()

    stmt = (
        select(Record.id, Record.starts_at)
        .where(Record.client_id == client_id)
        .where(Record.company_id == company_id)
        .where(Record.is_deleted.is_(False))
    )
    result = await session.execute(stmt)
    rows = result.all()

    count = 0
    for _, starts_at in rows:
        if starts_at is None:
            continue
        if starts_at.astimezone(_LOCAL_TZ).date() == reference_date:
            count += 1

    return count


def _build_simple_apply_comment(
    lead: PromoLead,
    *,
    original_cost: float,
    new_cost: float,
    discount_amount: float,
) -> str:
    """Build the audit comment appended to the record on a successful simple apply.

    The closing ``[PromoLead:<id>]`` token is the idempotency marker — its
    presence in the record comment prevents a duplicate PUT on retry.
    """
    campaign = settings.promo_campaign_name
    return (
        f"Promo {campaign}: Neukundenrabatt {discount_amount:g} € automatisch angewendet.\n"
        f"PromoLead ID: {lead.id}\n"
        f"Code: {lead.secret_code}\n"
        f"Original price: {original_cost:g} €\n"
        f"New price: {new_cost:g} €\n"
        f"[PromoLead:{lead.id}]"
    )


def _build_manual_review_comment(lead: PromoLead) -> str:
    """Build the admin annotation for the complex / manual-review case.

    The closing ``[PromoLead:<id>:manual]`` token is the idempotency marker.
    No customer notification is sent when this marker is used.
    """
    campaign = settings.promo_campaign_name
    discount_amount = settings.promo_discount_amount
    return (
        f"Promo {campaign}: Neukundenrabatt {discount_amount:g} € reserviert.\n"
        f"Bitte manuell prüfen/anwenden.\n"
        f"PromoLead ID: {lead.id}\n"
        f"Code: {lead.secret_code}\n"
        f"[PromoLead:{lead.id}:manual]"
    )


async def _apply_via_record_price_override(
    session: AsyncSession,
    record: Record,
    lead: PromoLead,
    client: Client,
    phone_e164: str,
    now: datetime,
    cfg: object,
) -> None:
    """Apply promo discount by modifying the service price on the Altegio record.

    Simple case (1 same-day record, 1 allowed service):
      - Fetches fresh record via GET /record.
      - Builds modified services list with discounted price.
      - Sends price + audit comment in one PUT /record.
      - Transitions lead → applied, queues customer notification.

    Complex case (multiple same-day records OR multiple allowed services):
      - Fetches fresh record via GET /record.
      - Sends admin annotation comment in one PUT /record (no price change).
      - Transitions lead → booked with manual_review_required=True.
      - No customer notification is created.

    Both cases are idempotent: if the record comment already contains a promo
    marker the function returns without a second PUT.
    """
    meta = lead.meta or {}

    location_id = lead.location_id
    altegio_record_id = record.altegio_record_id

    if not location_id or not altegio_record_id:
        err = f"missing required fields: location_id={location_id} altegio_record_id={altegio_record_id}"
        lead.status = "apply_failed"
        lead.meta = {**meta, "discount_apply_error": err, "discount_apply_attempted_at": now.isoformat()}
        logger.warning("promo_discount: missing fields lead_id=%s %s", lead.id, err)
        return

    # ── Idempotency: local comment check (no API call) ────────────────────────
    if is_promo_origin_comment(record.comment):
        logger.info("promo_discount: promo marker in local comment, skip PUT lead_id=%s", lead.id)
        return

    # ── Attendance guard: do not price-override attended / completed records ──
    if (record.attendance or 0) == 1 or (record.visit_attendance or 0) == 1:
        err = "record already attended — price override skipped"
        lead.meta = {**meta, "apply_skip_reason": err}
        logger.info("promo_discount: skip lead_id=%s %s", lead.id, err)
        return

    # ── Determine simple vs complex case ──────────────────────────────────────
    allowed_service_ids = get_promo_allowed_service_ids()
    record_service_ids = await _get_record_service_ids(session, record.id)
    matching_service_ids = record_service_ids.intersection(allowed_service_ids)

    same_day_count = await _count_same_day_records_for_client(
        session,
        client_id=record.client_id,
        company_id=record.company_id,
        reference_starts_at=record.starts_at,
    )

    is_simple = same_day_count == 1 and len(matching_service_ids) == 1

    # ── Fetch fresh Altegio record ────────────────────────────────────────────
    try:
        altegio_data = await fetch_altegio_record_for_update(
            location_id=location_id,
            record_id=altegio_record_id,
        )
    except AltegioRecordUpdateError as exc:
        err = str(exc)
        lead.status = "apply_failed"
        lead.meta = {**meta, "discount_apply_error": err, "discount_apply_attempted_at": now.isoformat()}
        logger.warning("promo_discount: GET /record failed lead_id=%s: %s", lead.id, exc)
        return

    # ── Idempotency: fresh Altegio comment check ──────────────────────────────
    if is_promo_origin_comment(altegio_data.get("comment")):
        logger.info("promo_discount: promo marker in Altegio comment, skip PUT lead_id=%s", lead.id)
        lead.meta = {**meta, "apply_skip_reason": "promo_marker_already_in_altegio_comment"}
        return

    # ── Attendance re-check against fresh Altegio data ───────────────────────
    fresh_attendance = int(altegio_data.get("attendance") or 0)
    fresh_visit_attendance = int(altegio_data.get("visit_attendance") or 0)
    if fresh_attendance == 1 or fresh_visit_attendance == 1:
        err = "record already attended in Altegio — price override skipped"
        lead.meta = {**meta, "apply_skip_reason": err}
        logger.info("promo_discount: skip lead_id=%s %s (fresh attendance check)", lead.id, err)
        return

    existing_comment = altegio_data.get("comment") or ""
    altegio_services = altegio_data.get("services") or []

    if is_simple:
        # ── Simple case: automatic price override ─────────────────────────────
        service_id = next(iter(matching_service_ids))

        target_svc: dict | None = None
        for svc in altegio_services:
            if isinstance(svc, dict) and svc.get("id") == service_id:
                target_svc = svc
                break

        if target_svc is None:
            err = f"allowed service id={service_id} not found in fresh Altegio record services"
            lead.status = "apply_failed"
            lead.meta = {**meta, "discount_apply_error": err, "discount_apply_attempted_at": now.isoformat()}
            logger.warning("promo_discount: service missing in Altegio record lead_id=%s", lead.id)
            return

        try:
            original_cost = float(target_svc.get("cost") or target_svc.get("manual_cost") or 0)
        except (TypeError, ValueError):
            original_cost = 0.0

        discount_amount = float(cfg.promo_discount_amount)  # type: ignore[union-attr]
        new_cost = max(0.0, original_cost - discount_amount)

        new_services: list[dict] = []
        for svc in altegio_services:
            if isinstance(svc, dict) and svc.get("id") == service_id:
                modified = dict(svc)
                modified["first_cost"] = original_cost
                modified["cost"] = new_cost
                new_services.append(modified)
            else:
                new_services.append(svc)

        append_note = _build_simple_apply_comment(
            lead, original_cost=original_cost, new_cost=new_cost, discount_amount=discount_amount
        )
        new_comment = (existing_comment + "\n" + append_note).strip() if existing_comment else append_note

        try:
            put_result = await update_altegio_record_price_and_comment(
                location_id=location_id,
                record_id=altegio_record_id,
                record_data=altegio_data,
                new_services=new_services,
                new_comment=new_comment,
            )
        except AltegioRecordUpdateError as exc:
            err = str(exc)
            lead.status = "apply_failed"
            lead.meta = {**meta, "discount_apply_error": err, "discount_apply_attempted_at": now.isoformat()}
            logger.warning("promo_discount: PUT /record failed lead_id=%s: %s", lead.id, exc)
            return

        # Extract Altegio-computed discount percentage (percentage, not our € amount)
        returned_discount: float | None = None
        put_data = put_result.get("data") or {}
        for svc in put_data.get("services") or []:
            if isinstance(svc, dict) and svc.get("id") == service_id:
                raw = svc.get("discount")
                if raw is not None:
                    try:
                        returned_discount = float(raw)
                    except (TypeError, ValueError):
                        pass
                break

        lead.status = "applied"
        lead.applied_at = now
        lead.record_id = record.id
        lead.altegio_record_id = record.altegio_record_id
        lead.meta = {
            **meta,
            "discount_apply_method": "record_price_override",
            "original_cost": original_cost,
            "discounted_cost": new_cost,
            "discount_amount": discount_amount,
            "altegio_record_update_status": "success",
            "discount_apply_attempted_at": now.isoformat(),
            **({"altegio_returned_discount": returned_discount} if returned_discount is not None else {}),
        }

        await _ensure_promo_discount_notification_job(session, lead, client, record, phone_e164, now)
        logger.info(
            "promo_discount: price_override applied lead_id=%s record_id=%s original=%.2f new=%.2f",
            lead.id,
            record.id,
            original_cost,
            new_cost,
        )

    else:
        # ── Complex case: manual review ───────────────────────────────────────
        skip_reason = "multiple_records_same_day" if same_day_count != 1 else "multiple_allowed_services_in_record"

        append_note = _build_manual_review_comment(lead)
        new_comment = (existing_comment + "\n" + append_note).strip() if existing_comment else append_note

        try:
            await update_altegio_record_price_and_comment(
                location_id=location_id,
                record_id=altegio_record_id,
                record_data=altegio_data,
                new_services=list(altegio_services),
                new_comment=new_comment,
            )
        except AltegioRecordUpdateError as exc:
            err = str(exc)
            lead.status = "apply_failed"
            lead.meta = {**meta, "discount_apply_error": err, "discount_apply_attempted_at": now.isoformat()}
            logger.warning("promo_discount: complex PUT /record failed lead_id=%s: %s", lead.id, exc)
            return

        lead.status = "booked"
        lead.record_id = record.id
        lead.altegio_record_id = record.altegio_record_id
        lead.meta = {
            **meta,
            "manual_review_required": True,
            "discount_apply_skip_reason": skip_reason,
            "discount_apply_attempted_at": now.isoformat(),
        }
        logger.info(
            "promo_discount: complex case, manual review required lead_id=%s skip_reason=%s",
            lead.id,
            skip_reason,
        )


def _build_notification_body() -> str:
    return (
        "Gute Nachricht 🎁\n\n"
        "Ihr Neukundenrabatt wurde Ihrer Buchung zugeordnet.\n\n"
        "Bitte beachten Sie: In der Online-Buchung und in der ersten Bestätigung "
        "können noch reguläre Preise angezeigt werden. Unser Team sieht den Rabatt "
        "in Ihrer Buchung.\n\n"
        "Wir freuen uns auf Ihren Besuch 💙"
    )


async def _ensure_promo_discount_notification_job(
    session: AsyncSession,
    lead: PromoLead,
    client: Client,
    record: Record,
    phone_e164: str,
    now: datetime,
) -> None:
    """Idempotent: create or find the customer notification MessageJob for this lead.

    Reads existing job by dedupe_key before inserting to prevent duplicate-key errors
    on webhook retries where the Altegio apply already succeeded.

    Writes to lead.meta (without overwriting existing apply metadata):
      customer_notification          = 'queued'
      customer_notification_job_id   = <job.id>
      customer_notification_created_at = <now ISO>
      customer_notification_dedupe_key = <dedupe_key>
    """
    dedupe_key = f"promo_discount_applied:{lead.id}"

    existing = (
        await session.execute(select(MessageJob).where(MessageJob.dedupe_key == dedupe_key))
    ).scalar_one_or_none()

    current_meta = lead.meta or {}

    if existing is not None:
        lead.meta = {
            **current_meta,
            "customer_notification": "queued",
            "customer_notification_job_id": existing.id,
            "customer_notification_created_at": now.isoformat(),
            "customer_notification_dedupe_key": dedupe_key,
        }
        logger.info(
            "promo_discount: notification job already exists job_id=%s lead_id=%s",
            existing.id,
            lead.id,
        )
        return

    notification_body = _build_notification_body()

    try:
        async with session.begin_nested():
            job = MessageJob(
                company_id=lead.company_id,
                client_id=client.id,
                record_id=record.id,
                job_type="promo_discount_applied",
                run_at=now,
                dedupe_key=dedupe_key,
                payload={
                    "body": notification_body,
                    "phone_e164": phone_e164,
                    "promo_lead_id": lead.id,
                },
            )
            session.add(job)
            await session.flush()
    except SAIntegrityError:
        # Concurrent handler inserted the same job between our SELECT and INSERT.
        # Savepoint was rolled back; re-read the winner and update meta.
        existing = (
            await session.execute(select(MessageJob).where(MessageJob.dedupe_key == dedupe_key))
        ).scalar_one_or_none()
        if existing is None:
            raise
        lead.meta = {
            **current_meta,
            "customer_notification": "queued",
            "customer_notification_job_id": existing.id,
            "customer_notification_created_at": now.isoformat(),
            "customer_notification_dedupe_key": dedupe_key,
        }
        logger.warning(
            "promo_discount: concurrent insert detected, using existing job_id=%s lead_id=%s",
            existing.id,
            lead.id,
        )
        return

    lead.meta = {
        **current_meta,
        "customer_notification": "queued",
        "customer_notification_job_id": job.id,
        "customer_notification_created_at": now.isoformat(),
        "customer_notification_dedupe_key": dedupe_key,
    }
    logger.info(
        "promo_discount: queued notification job_id=%s lead_id=%s",
        job.id,
        lead.id,
    )


async def find_applicable_promo_lead_for_record(
    session: AsyncSession,
    *,
    company_id: int,
    phone_e164: str,
    now: datetime,
    record: Record,
    for_update: bool = False,
    expected_lead_id: int | None = None,
) -> PromoLead | None:
    """Return the most recent active PromoLead eligible for discount application.

    Filters:
    - company_id matches the booking company (prevents cross-location mismatch)
    - phone_e164 matches the booking client
    - campaign_name == settings.promo_campaign_name
    - status in ('issued', 'booked')
    - expires_at > now
    - loyalty_card_id IS NOT NULL
    - location_id IS NOT NULL
    - discount_program_id IS NOT NULL
    - meta.loyalty_card_issued == true
    - meta.promo_card_deleted_at IS NULL (card not yet cleaned up)

    Booked-lead rebinding guard:
    - A booked lead is only returned when it references the same record as the current
      booking (lead.record_id == record.id OR lead.altegio_record_id == record.altegio_record_id).
    - A booked lead bound to a different record is silently skipped to prevent
      re-attributing the promo to a different booking.

    for_update=True locks and refreshes the row for post-I/O revalidation.
    expected_lead_id restricts revalidation to the same candidate found before I/O.
    """
    if not phone_e164:
        return None

    campaign = settings.promo_campaign_name
    stmt = (
        select(PromoLead)
        .where(PromoLead.company_id == company_id)
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

    if expected_lead_id is not None:
        stmt = stmt.where(PromoLead.id == expected_lead_id)

    if for_update:
        stmt = stmt.with_for_update().execution_options(populate_existing=True)

    result = await session.execute(stmt)
    lead = result.scalar_one_or_none()

    if lead is not None and lead.status == "booked":
        same_record = (lead.record_id is not None and record.id is not None and lead.record_id == record.id) or (
            lead.altegio_record_id is not None
            and record.altegio_record_id is not None
            and lead.altegio_record_id == record.altegio_record_id
        )
        if not same_record:
            logger.warning(
                "promo_discount: booked lead_id=%s bound to record_id=%s/%s, skipping current record_id=%s/%s",
                lead.id,
                lead.record_id,
                lead.altegio_record_id,
                record.id,
                record.altegio_record_id,
            )
            return None

    return lead


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


async def try_apply_promo_discount(
    session: AsyncSession,
    record: Record,
    company_id: int,
    *,
    booking_created_at: datetime | None = None,
    booking_created_at_resolver: Callable[[], Awaitable[datetime | None]] | None = None,
) -> None:
    """Attempt to apply a promo discount to a newly created Altegio visit.

    Called from inbox_worker.handle_event on record create webhooks only.
    Update webhooks are intentionally ignored to avoid applying promo to
    bookings created before the promo was issued.

    booking_created_at must be the confirmed booking creation time from Altegio
    (not the webhook received time). If it is not supplied directly,
    booking_created_at_resolver may be provided; it is called lazily only after
    local PromoLead, service allowlist, and prior-visit checks pass. If the
    timestamp is still None or earlier than PromoLead.issued_at, the discount is
    skipped (fail-closed) to prevent applying a promo to a booking that predates
    the promo campaign.

    Fail-closed: controlled failures are recorded in PromoLead.meta and do not
    propagate as exceptions. Unexpected exceptions propagate to the caller so
    the event can be retried.

    Flow:
    1.  Feature gate check (promo_apply_discount_enabled).
    2.  Resolve client phone from record.
    3.  Find matching PromoLead (filtered by company_id + phone).
    4.  Service allowlist check (promo_allowed_service_ids).
    5.  New-client guard (no prior attended visits, local DB only).
    6.  Resolve booking_created_at lazily, revalidate/lock the same PromoLead,
        re-run mutable local guards, then guard timestamp.
    7.  Transition issued → booked (booking confirmed).
    8.  API gate check (promo_apply_discount_api_verified).
    9.  Route by promo_apply_mode:
          'record_price_override' → _apply_via_record_price_override() (GET+PUT /record).
          other                   → legacy loyalty-program path (steps 10-12).
    10. Validate required fields for loyalty-program path.
    11. Call Altegio apply_discount_program API (legacy path).
    12. Update PromoLead status → applied (legacy path).
    13. Queue a customer WhatsApp notification (MessageJob, job_type='promo_discount_applied').
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

    async def _passes_mutable_local_guards(current_lead: PromoLead) -> bool:
        current_meta = current_lead.meta or {}

        # Service allowlist check. This is intentionally re-run after any
        # awaited timestamp lookup because local record services can change.
        allowed_service_ids = get_promo_allowed_service_ids()
        if not allowed_service_ids:
            err = "promo_allowed_service_ids empty — discount not applied automatically"
            current_lead.meta = {**current_meta, "apply_error": err, "apply_skip_reason": err}
            logger.info("promo_discount: skip lead_id=%s %s", current_lead.id, err)
            return False

        record_service_ids = await _get_record_service_ids(session, record.id)
        if not record_service_ids.intersection(allowed_service_ids):
            err = f"no allowed service in record: record_services={record_service_ids} allowed={allowed_service_ids}"
            current_lead.meta = {**current_meta, "apply_error": err, "apply_skip_reason": err}
            logger.info("promo_discount: skip lead_id=%s service not allowed", current_lead.id)
            return False

        # New-client guard. This is local DB only and is also re-run after
        # timestamp lookup because another sync can add attended visits.
        if await _has_prior_attended_visits(session, phone_e164, exclude_record_id=record.id):
            err = "client has prior attended visits — discount not applied"
            current_lead.meta = {**current_meta, "apply_error": err, "apply_skip_reason": err}
            logger.info("promo_discount: skip lead_id=%s prior visited client", current_lead.id)
            return False

        return True

    # ── 3. Find matching PromoLead ────────────────────────────────────────────
    lead = await find_applicable_promo_lead_for_record(
        session,
        company_id=company_id,
        phone_e164=phone_e164,
        now=now,
        record=record,
    )
    if lead is None:
        return

    meta = lead.meta or {}

    # ── 4–5. Mutable local guards ─────────────────────────────────────────────
    # TODO: The prior-visit check uses local DB only and may miss visits not yet
    # synced from Altegio. A full CRM API history check is deferred to a future PR.
    if not await _passes_mutable_local_guards(lead):
        return

    candidate_lead_id = lead.id

    # ── 6. Booking created timestamp guard ────────────────────────────────────
    # Resolve only after cheap local checks. A missing or pre-promo timestamp
    # means the booking may predate this promo campaign, so apply stays blocked.
    if booking_created_at is None and booking_created_at_resolver is not None:
        booking_created_at = await booking_created_at_resolver()
        now = _utcnow()
        lead = await find_applicable_promo_lead_for_record(
            session,
            company_id=company_id,
            phone_e164=phone_e164,
            now=now,
            record=record,
            for_update=True,
            expected_lead_id=candidate_lead_id,
        )
        if lead is None:
            logger.info(
                "promo_discount: lead no longer applicable after booking_created_at lookup record_id=%s",
                record.id,
            )
            return
        if not await _passes_mutable_local_guards(lead):
            return
        meta = lead.meta or {}

    if booking_created_at is None:
        err = "missing booking created timestamp"
        lead.meta = {**meta, "apply_skip_reason": err}
        logger.info("promo_discount: skip lead_id=%s %s", lead.id, err)
        return

    if booking_created_at < lead.issued_at:
        err = "booking predates promo lead"
        lead.meta = {
            **meta,
            "apply_skip_reason": err,
            "booking_created_at": booking_created_at.isoformat(),
            "promo_issued_at": lead.issued_at.isoformat(),
        }
        logger.info("promo_discount: skip lead_id=%s %s", lead.id, err)
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

    # ── 9. Route by apply mode ────────────────────────────────────────────────
    if cfg.promo_apply_mode == "record_price_override":
        await _apply_via_record_price_override(session, record, lead, client, phone_e164, now, cfg)
        return

    # ── Legacy loyalty-program path ───────────────────────────────────────────
    # ── 10. Validate required fields ─────────────────────────────────────────
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

    # ── 11. Call Altegio API (legacy loyalty-program path) ───────────────────
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

    # ── 12. Update PromoLead → applied ───────────────────────────────────────
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

    await _ensure_promo_discount_notification_job(session, lead, client, record, phone_e164, now)
    logger.info(
        "promo_discount: applied lead_id=%s record_id=%s card_id=%s",
        lead.id,
        record.id,
        card_id,
    )
