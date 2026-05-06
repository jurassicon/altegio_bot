from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import or_, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.altegio_records import (
    client_has_future_appointments,
    count_attended_client_visits,
)
from altegio_bot.campaigns.followup import check_followup_final_eligibility
from altegio_bot.campaigns.runner import (
    CAMPAIGN_EXECUTION_JOB_TYPE,
    FOLLOWUP_JOB_TYPE,
    recompute_campaign_run_stats,
)
from altegio_bot.db import SessionLocal
from altegio_bot.message_planner import (
    COMEBACK_3D_DELAY,
    COMEBACK_3D_SOURCE_CANCELLED_AT_KEY,
    MAX_VISITS_FOR_REVIEW,
)
from altegio_bot.meta_templates import (
    NEWSLETTER_FOLLOWUP_TEMPLATE,
    NEWSLETTER_MONTHLY_TEMPLATE,
    TEMPLATE_LANGUAGE,
    UNIVERSAL_JOB_TYPES,
    build_template_params,
    requires_image_header,
    resolve_meta_template,
)
from altegio_bot.models.models import (
    CampaignRecipient,
    CampaignRun,
    Client,
    ContactRateLimit,
    MessageJob,
    MessageTemplate,
    OutboxMessage,
    Record,
    RecordService,
)
from altegio_bot.perf import perf_log
from altegio_bot.providers.base import WhatsAppProvider
from altegio_bot.providers.dummy import safe_send, safe_send_template
from altegio_bot.settings import settings
from altegio_bot.template_validation import validate_template_params
from altegio_bot.whatsapp_routing import pick_sender_code_for_record, pick_sender_id

logger = logging.getLogger("outbox_worker")

MIN_SECONDS_BETWEEN_MESSAGES = 30
DEFAULT_LANGUAGE = "de"
PAST_RECORD_GRACE_MINUTES = 5

PRE_APPOINTMENT_JOB_TYPES = (
    "record_created",
    "record_updated",
    "reminder_24h",
    "reminder_2h",
)

MARKETING_JOB_TYPES = (
    "review_3d",
    "repeat_10d",
    "comeback_3d",
    "newsletter_new_clients_monthly",
    "newsletter_new_clients_followup",
)

WA_131026_SUPPRESSIBLE_JOB_TYPES: tuple[str, ...] = (
    "review_3d",
    "repeat_10d",
    "comeback_3d",
    "newsletter_new_clients_monthly",
    "newsletter_new_clients_followup",
)

TOKEN_EXPIRED_RETRY_SECONDS = 60
STOP_WORKER_ON_TOKEN_EXPIRED_ENV = "STOP_WORKER_ON_TOKEN_EXPIRED"
_TOKEN_EXPIRED = False

# Maximum number of Altegio API guard retries (for repeat_10d / review_3d
# pre-send checks). This counter is stored in job.payload["_api_guard_attempts"]
# and is intentionally separate from the ``attempts`` field, which counts only
# real WhatsApp send attempts. This prevents transient Altegio API outages from
# consuming the send-attempt budget.
MAX_API_GUARD_ATTEMPTS = 5

COMEBACK_3D_MISSING_SOURCE_REASON = "Skipped: source record missing for comeback_3d"


def _resolve_template_header_image_url(template_name: str) -> str | None:
    """Return the configured image URL for templates that have an IMAGE HEADER component.

    Returns None both when the template does not require a header and when the
    URL is not configured — callers must check ``requires_image_header`` first
    to distinguish the two cases.
    """
    if template_name == NEWSLETTER_MONTHLY_TEMPLATE:
        return settings.meta_newsletter_monthly_header_image_url.strip() or None
    if template_name == NEWSLETTER_FOLLOWUP_TEMPLATE:
        return settings.meta_newsletter_followup_header_image_url.strip() or None
    return None


def _missing_required_header_error(template_name: str) -> str:
    return (
        f"Template {template_name} requires image header but image URL is not configured. "
        "Set META_NEWSLETTER_MONTHLY_HEADER_IMAGE_URL or "
        "META_NEWSLETTER_FOLLOWUP_HEADER_IMAGE_URL in .env."
    )


COMEBACK_3D_MISSING_SOURCE_TIME_REASON = "Skipped: source record starts_at missing for comeback_3d"
COMEBACK_3D_MISSING_CANCEL_TIME_REASON = "Skipped: source cancellation time is missing for comeback_3d guard"
COMEBACK_3D_ALREADY_RETURNED_REASON = "Skipped: client already returned within comeback_3d window"

_COMEBACK_3D_CANCELLED_AT_PAYLOAD_KEYS = (
    COMEBACK_3D_SOURCE_CANCELLED_AT_KEY,
    "source_canceled_at",
    "cancelled_at",
    "canceled_at",
    "deleted_at",
    "event_received_at",
)

STALE_PROCESSING_MINUTES = 10

DEFAULT_LANGUAGE_BY_COMPANY = {
    758285: "de",
    1271200: "de",
}

BOOKING_LINKS = {
    758285: "https://n813709.alteg.io/",
    1271200: "https://n813709.alteg.io/",
}

GOOGLE_MAPS_REVIEW_LINKS: dict[int, str] = {
    758285: "https://g.page/r/CdOqDUWhxCAbEBM/review",
    1271200: "https://g.page/r/CWd7fy4dua5kEBM/review",
}

PRE_APPOINTMENT_NOTES_DE = (
    "\n\nWichtige Hinweise vor dem Termin:\n"
    "• Bitte pünktlich kommen — ab 15 Min. Verspätung können wir "
    "nicht garantieren, dass der Termin stattfindet.\n"
    "• Wimpern bitte sauber: ohne Mascara, ohne geklebte Wimpern.\n"
    "• Falls Sie schon eine Kundenkarte haben, bitte mitbringen.\n"
    "• Auffüllen: ab 3. Woche 60 €, ab 4. Woche 70 €, ab 5. Woche "
    "keine Auffüllung (Neuauflage).\n"
    "• Zahlung: bar oder mit Karte.\n"
)

SUCCESS_OUTBOX_STATUSES = ("sent", "delivered", "read")


def _stop_worker_on_token_expired() -> bool:
    return os.getenv(STOP_WORKER_ON_TOKEN_EXPIRED_ENV, "0").strip() == "1"


def _mark_token_expired() -> None:
    global _TOKEN_EXPIRED
    _TOKEN_EXPIRED = True


def _token_expired() -> bool:
    return _TOKEN_EXPIRED


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _record_is_in_past(record: Record | None, *, job_type: str) -> bool:
    if job_type not in PRE_APPOINTMENT_JOB_TYPES:
        return False

    if record is None or record.starts_at is None:
        return False

    cutoff = utcnow() - timedelta(minutes=PAST_RECORD_GRACE_MINUTES)
    return record.starts_at < cutoff


def _record_attended(record: Record | None) -> bool:
    if record is None:
        return False

    attendance = getattr(record, "attendance", 0) or 0
    visit_attendance = getattr(record, "visit_attendance", 0) or 0
    return bool(attendance == 1 or visit_attendance == 1)


def _job_type_allows_131026_suppression(job_type: str) -> bool:
    """Вернуть True только для маркетинговых job types с разрешенным pre-send suppression."""
    return job_type in WA_131026_SUPPRESSIBLE_JOB_TYPES


async def _find_success_outbox(
    session: AsyncSession,
    job_id: int,
) -> OutboxMessage | None:
    stmt = (
        select(OutboxMessage)
        .where(OutboxMessage.job_id == job_id)
        .where(OutboxMessage.status.in_(SUCCESS_OUTBOX_STATUSES))
        .order_by(OutboxMessage.id.desc())
        .limit(1)
    )
    res = await session.execute(stmt)
    return res.scalar_one_or_none()


async def _find_existing_outbox(
    session: AsyncSession,
    job_id: int,
) -> OutboxMessage | None:
    stmt = select(OutboxMessage).where(OutboxMessage.job_id == job_id).order_by(OutboxMessage.id.desc()).limit(1)

    res = await session.execute(stmt)
    return res.scalar_one_or_none()


def _retry_delay_seconds(attempt: int) -> int:
    base = 30
    delay = base * (2 ** (attempt - 1))
    return min(delay, 15 * 60)


def _get_api_guard_attempts(job: MessageJob) -> int:
    """Return the current API guard retry count from job.payload."""
    return int((getattr(job, "payload", None) or {}).get("_api_guard_attempts", 0))


def _handle_api_guard_error(job: MessageJob, exc: Exception) -> None:
    """Increment the API guard counter and requeue or permanently fail the job.

    The guard counter lives in ``job.payload["_api_guard_attempts"]`` so it
    does not share the ``attempts`` budget with actual WhatsApp send attempts.
    """
    payload = dict(getattr(job, "payload", None) or {})
    count = int(payload.get("_api_guard_attempts", 0)) + 1
    payload["_api_guard_attempts"] = count
    job.payload = payload

    if count >= MAX_API_GUARD_ATTEMPTS:
        job.status = "failed"
        job.locked_at = None
        job.last_error = f"Altegio API error (max guard attempts): {exc}"
        return

    delay = _retry_delay_seconds(count)
    job.status = "queued"
    job.locked_at = None
    job.run_at = utcnow() + timedelta(seconds=delay)
    job.last_error = f"Altegio API error: {exc}"


def _fmt_money(value: Decimal | None) -> str:
    if value is None:
        return "0.00"
    return f"{value:.2f}"


def _fmt_date(dt: datetime | None) -> str:
    if dt is None:
        return ""
    tz = ZoneInfo(settings.ops_local_tz)
    return dt.astimezone(tz).strftime("%d.%m.%Y")


def _fmt_time(dt: datetime | None) -> str:
    if dt is None:
        return ""
    tz = ZoneInfo(settings.ops_local_tz)
    return dt.astimezone(tz).strftime("%H:%M")


async def _lock_next_jobs(
    session: AsyncSession,
    batch_size: int,
) -> list[MessageJob]:
    now = utcnow()

    stmt = (
        select(MessageJob)
        .where(MessageJob.status == "queued")
        .where(MessageJob.job_type != CAMPAIGN_EXECUTION_JOB_TYPE)
        .where(MessageJob.run_at <= now)
        .order_by(MessageJob.run_at.asc())
        .limit(batch_size)
        .with_for_update(skip_locked=True)
    )
    res = await session.execute(stmt)
    jobs = list(res.scalars().all())

    for job in jobs:
        job.status = "processing"
        job.locked_at = now

    return jobs


async def _requeue_processing_jobs(
    session: AsyncSession,
    job_ids: list[int],
) -> None:
    if not job_ids:
        return

    stmt = (
        update(MessageJob)
        .where(MessageJob.id.in_(job_ids))
        .where(MessageJob.status == "processing")
        .values(status="queued", locked_at=None)
    )
    await session.execute(stmt)


async def _requeue_stale_processing_jobs(session: AsyncSession) -> int:
    cutoff = utcnow() - timedelta(minutes=STALE_PROCESSING_MINUTES)

    stmt = (
        update(MessageJob)
        .where(MessageJob.status == "processing")
        .where(MessageJob.locked_at.is_not(None))
        .where(MessageJob.locked_at < cutoff)
        .values(
            status="queued",
            locked_at=None,
            run_at=utcnow(),
            last_error="Recovered: stale processing job",
        )
    )
    res = await session.execute(stmt)
    return int(getattr(res, "rowcount", 0) or 0)


async def _apply_rate_limit(
    session: AsyncSession,
    phone_e164: str,
) -> datetime | None:
    now = utcnow()

    await session.execute(
        text(
            """
            INSERT INTO contact_rate_limits (phone_e164, next_allowed_at)
            VALUES (:phone_e164, :next_allowed_at)
            ON CONFLICT (phone_e164) DO NOTHING;
            """
        ),
        {"phone_e164": phone_e164, "next_allowed_at": now},
    )

    stmt = select(ContactRateLimit).where(ContactRateLimit.phone_e164 == phone_e164).with_for_update()

    res = await session.execute(stmt)
    rl = res.scalar_one()

    if rl.next_allowed_at > now:
        return rl.next_allowed_at

    rl.next_allowed_at = now + timedelta(seconds=MIN_SECONDS_BETWEEN_MESSAGES)
    return None


async def _load_record(
    session: AsyncSession,
    job: MessageJob,
) -> Record | None:
    if job.record_id is None:
        return None
    return await session.get(Record, job.record_id)


def _as_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc)


def _parse_payload_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return _as_utc(value)

    if not isinstance(value, str):
        return None

    raw = value.strip()
    if not raw:
        return None

    try:
        return _as_utc(datetime.fromisoformat(raw.replace("Z", "+00:00")))
    except ValueError:
        return None


def _resolve_comeback_cancelled_at(job: MessageJob, record: Record | None) -> datetime | None:
    payload = getattr(job, "payload", None) or {}
    for key in _COMEBACK_3D_CANCELLED_AT_PAYLOAD_KEYS:
        resolved = _parse_payload_datetime(payload.get(key))
        if resolved is not None:
            return resolved

    created_at = getattr(job, "created_at", None)
    if isinstance(created_at, datetime):
        return _as_utc(created_at)

    run_at = getattr(job, "run_at", None)
    if isinstance(run_at, datetime):
        return _as_utc(run_at) - COMEBACK_3D_DELAY

    return None


async def _client_returned_since(
    session: AsyncSession,
    company_id: int,
    altegio_client_id: int,
    since: datetime,
    *,
    exclude_record_id: int | None = None,
) -> bool:
    stmt = (
        select(Record.id)
        .where(Record.company_id == company_id)
        .where(Record.altegio_client_id == altegio_client_id)
        .where(Record.is_deleted.is_(False))
        .where(or_(Record.confirmed.is_(None), Record.confirmed != 0))
        .where(Record.starts_at.is_not(None))
        .where(Record.starts_at > _as_utc(since))
        .where(Record.starts_at <= utcnow())
        .limit(1)
    )
    if exclude_record_id is not None:
        stmt = stmt.where(Record.id != exclude_record_id)

    res = await session.execute(stmt)
    return res.scalar_one_or_none() is not None


def _pick_language(company_id: int, client: Client | None) -> str:
    return DEFAULT_LANGUAGE_BY_COMPANY.get(company_id, DEFAULT_LANGUAGE)


async def _load_template(
    session: AsyncSession,
    *,
    company_id: int,
    template_code: str,
    language: str,
) -> tuple[MessageTemplate | None, str]:
    """Look up the active MessageTemplate for *company_id* / *template_code*.

    Lookup order (deterministic, always order_by id ASC where no unique match):

    Phase 1 — company-specific rows (always executed):
      1. company_id + code + requested language  (exact)
      2. company_id + code + DEFAULT_LANGUAGE    (only when language ≠ DEFAULT_LANGUAGE)
      3. company_id + code (any language)

    Phase 2 — cross-company fallback (ONLY for universal template codes):
      4. code + requested language  (any company, id ASC)
      5. code + DEFAULT_LANGUAGE    (any company, only when language ≠ DEFAULT_LANGUAGE)
      6. code (any company, any language, id ASC)

    Phase 2 is reached only when Phase 1 finds nothing AND *template_code* is in
    UNIVERSAL_JOB_TYPES.  Universal templates (review_3d, repeat_10d, comeback_3d,
    newsletter_new_clients_monthly, newsletter_new_clients_followup) have no address
    footer and are stored in message_templates under a single canonical company_id
    but shared by all branches.

    Phase 2 is intentionally SKIPPED for branch-specific codes (record_created,
    record_updated, record_canceled, reminder_24h, reminder_2h).  Those templates
    contain branch-specific address footers; silently using another branch's row
    would produce incorrect text and mislead the recipient about the salon location.

    Mirrors the fallback already present in the preview endpoint get_template_text().
    """
    # ------------------------------------------------------------------
    # Phase 1: company-specific rows (existing priority, unchanged)
    # ------------------------------------------------------------------
    base = (
        select(MessageTemplate)
        .where(MessageTemplate.company_id == company_id)
        .where(MessageTemplate.code == template_code)
        .where(MessageTemplate.is_active.is_(True))
    )

    stmt = base.where(MessageTemplate.language == language).limit(1)
    res = await session.execute(stmt)
    tmpl = res.scalar_one_or_none()
    if tmpl is not None:
        return tmpl, language

    if language != DEFAULT_LANGUAGE:
        stmt = base.where(MessageTemplate.language == DEFAULT_LANGUAGE).limit(1)
        res = await session.execute(stmt)
        tmpl = res.scalar_one_or_none()
        if tmpl is not None:
            return tmpl, DEFAULT_LANGUAGE

    stmt = base.order_by(MessageTemplate.id.asc()).limit(1)
    res = await session.execute(stmt)
    tmpl = res.scalar_one_or_none()
    if tmpl is not None:
        return tmpl, tmpl.language

    # ------------------------------------------------------------------
    # Phase 2: cross-company fallback — universal templates only.
    # Branch-specific codes (record_*, reminder_*) intentionally skip this
    # to prevent accidentally serving the wrong branch's address footer.
    # ------------------------------------------------------------------
    if template_code not in UNIVERSAL_JOB_TYPES:
        return None, language

    cross = (
        select(MessageTemplate).where(MessageTemplate.code == template_code).where(MessageTemplate.is_active.is_(True))
    )

    stmt = cross.where(MessageTemplate.language == language).order_by(MessageTemplate.id.asc()).limit(1)
    res = await session.execute(stmt)
    tmpl = res.scalar_one_or_none()
    if tmpl is not None:
        logger.info(
            "_load_template cross-company fallback: company=%s code=%s language=%s"
            " → using template_id=%s from company=%s",
            company_id,
            template_code,
            language,
            tmpl.id,
            tmpl.company_id,
        )
        return tmpl, language

    if language != DEFAULT_LANGUAGE:
        stmt = cross.where(MessageTemplate.language == DEFAULT_LANGUAGE).order_by(MessageTemplate.id.asc()).limit(1)
        res = await session.execute(stmt)
        tmpl = res.scalar_one_or_none()
        if tmpl is not None:
            logger.info(
                "_load_template cross-company fallback: company=%s code=%s"
                " → DEFAULT_LANGUAGE fallback template_id=%s from company=%s",
                company_id,
                template_code,
                tmpl.id,
                tmpl.company_id,
            )
            return tmpl, DEFAULT_LANGUAGE

    stmt = cross.order_by(MessageTemplate.id.asc()).limit(1)
    res = await session.execute(stmt)
    tmpl = res.scalar_one_or_none()
    if tmpl is not None:
        logger.info(
            "_load_template cross-company fallback: company=%s code=%s"
            " → any-language fallback template_id=%s from company=%s language=%s",
            company_id,
            template_code,
            tmpl.id,
            tmpl.company_id,
            tmpl.language,
        )
        return tmpl, tmpl.language

    return None, language


async def _load_client(
    session: AsyncSession,
    job: MessageJob,
    record: Record | None,
) -> Client | None:
    if job.client_id is not None:
        return await session.get(Client, job.client_id)

    if record is not None and record.client_id is not None:
        return await session.get(Client, record.client_id)

    return None


async def _is_new_client_for_record(
    session: AsyncSession,
    *,
    company_id: int,
    client_id: int | None,
    record_id: int | None,
    record_starts_at: datetime | None,
) -> bool:
    if client_id is None or record_id is None or record_starts_at is None:
        return False

    stmt = (
        select(Record.id)
        .where(Record.company_id == company_id)
        .where(Record.client_id == client_id)
        .where(Record.id != record_id)
        .where(Record.starts_at.is_not(None))
        .where(Record.starts_at < record_starts_at)
        .limit(1)
    )
    res = await session.execute(stmt)
    prev_id = res.scalar_one_or_none()
    return prev_id is None


async def _render_message(
    session: AsyncSession,
    *,
    company_id: int,
    template_code: str,
    record: Record | None,
    client: Client | None,
) -> tuple[str, int, str, dict[str, Any]]:
    language = _pick_language(company_id, client)

    tmpl, used_lang = await _load_template(
        session,
        company_id=company_id,
        template_code=template_code,
        language=language,
    )
    if tmpl is None:
        raise ValueError(f"Template not found: company={company_id} code={template_code}")

    services_text = ""
    primary_service = ""
    total_cost = Decimal("0.00")

    if record is not None:
        svc_stmt = (
            select(RecordService).where(RecordService.record_id == record.id).order_by(RecordService.service_id.asc())
        )
        svc_res = await session.execute(svc_stmt)
        services = list(svc_res.scalars().all())

        if services:
            primary_service = services[0].title or ""

        lines: list[str] = []
        for svc in services:
            lines.append(f"{svc.title} — {_fmt_money(svc.cost_to_pay)}€")
            if svc.cost_to_pay is not None:
                total_cost += svc.cost_to_pay

        services_text = "\n".join(lines)

    unsubscribe_link = ""
    booking_link = BOOKING_LINKS.get(company_id, "")

    sender_code = "default"
    if record is not None:
        sender_code = await pick_sender_code_for_record(
            session=session,
            company_id=company_id,
            record_id=record.id,
        )

    sender_id = await pick_sender_id(
        session=session,
        company_id=company_id,
        sender_code=sender_code,
    )
    if sender_id is None:
        raise ValueError(f"No active sender for company={company_id} code={sender_code}")

    pre_appointment_notes = ""
    if template_code == "record_created" and record is not None and used_lang == "de":
        is_new = await _is_new_client_for_record(
            session=session,
            company_id=company_id,
            client_id=record.client_id,
            record_id=record.id,
            record_starts_at=record.starts_at,
        )
        if is_new:
            pre_appointment_notes = PRE_APPOINTMENT_NOTES_DE

    ctx: dict[str, Any] = {
        "client_name": (client.display_name if client else ""),
        "staff_name": (record.staff_name if record else ""),
        "date": _fmt_date(record.starts_at if record else None),
        "time": _fmt_time(record.starts_at if record else None),
        "services": services_text,
        "primary_service": primary_service,
        "total_cost": _fmt_money(total_cost),
        "short_link": (record.short_link if record else ""),
        "unsubscribe_link": unsubscribe_link,
        "booking_link": booking_link,
        "sender_id": sender_id,
        "sender_code": sender_code,
        "pre_appointment_notes": pre_appointment_notes,
    }

    if template_code == "review_3d":
        ctx["short_link"] = GOOGLE_MAPS_REVIEW_LINKS.get(
            company_id,
            ctx["short_link"],
        )

    body = tmpl.body
    return body, sender_id, used_lang, ctx


async def _load_job(
    session: AsyncSession,
    job_id: int,
) -> MessageJob | None:
    stmt = select(MessageJob).where(MessageJob.id == job_id).with_for_update(skip_locked=True)

    res = await session.execute(stmt)
    job = res.scalar_one_or_none()
    if job is not None:
        return job

    exists_stmt = select(MessageJob.id).where(MessageJob.id == job_id)
    exists_res = await session.execute(exists_stmt)
    exists_id = exists_res.scalar_one_or_none()

    if exists_id is None:
        raise RuntimeError(f"MessageJob not found: id={job_id}")

    logger.info("Skip job_id=%s (locked)", job_id)
    return None


def _is_token_expired_error(err: str) -> bool:
    low = err.lower()
    return "access token" in low and "expired" in low


async def _count_131026_failures(
    session: AsyncSession,
    phone: str,
    window_days: int,
) -> int:
    """Count Meta 131026 undeliverable events for phone within window.

    Real production pattern:
    - outbox_messages.status stays 'sent' (Meta accepted the API call)
    - delivery webhook arrives later with statuses[0].status='failed'
      and statuses[0].errors[0].code=131026
    - The webhook worker does NOT downgrade 'sent' to 'failed' because
      'failed' has rank 0 which is lower than 'sent' rank 3.
    So om.status is NOT checked here — we rely solely on the webhook
    payload in whatsapp_events.
    """
    window_start = utcnow() - timedelta(days=window_days)
    result = await session.execute(
        text(
            "SELECT COUNT(*) FROM outbox_messages om "
            "WHERE om.phone_e164 = :phone "
            "  AND om.sent_at >= :window_start "
            "  AND om.provider_message_id IS NOT NULL "
            "  AND om.message_source = 'bot' "
            "  AND EXISTS ( "
            "    SELECT 1 FROM whatsapp_events we WHERE "
            "      payload "
            "        #>> '{entry,0,changes,0,value,statuses,0,id}' "
            "        = om.provider_message_id "
            "      AND payload "
            "        #>> "
            "        '{entry,0,changes,0,value,statuses,0,status}' "
            "        = 'failed' "
            "      AND payload "
            "        #>> "
            "        '{entry,0,changes,0,value,statuses,0,errors,0,code}' "
            "        = '131026' "
            "    LIMIT 1 "
            "  )"
        ),
        {"phone": phone, "window_start": window_start},
    )
    return result.scalar_one()


async def process_job_in_session(
    session: AsyncSession,
    job_id: int,
    provider: WhatsAppProvider,
) -> int | None:
    """Process one job inside *session*.

    Returns the ``campaign_run_id`` when a campaign message is successfully
    sent so the caller can trigger a post-commit stats recompute.
    """
    campaign_run_id: int | None = None
    with perf_log("outbox_worker", "process_job", job_id=job_id) as ctx:
        campaign_run_id = await _process_job_in_session_inner(session, job_id, provider, ctx)
    return campaign_run_id


async def _process_job_in_session_inner(
    session: AsyncSession,
    job_id: int,
    provider: WhatsAppProvider,
    ctx: dict[str, Any],
) -> int | None:
    job = await _load_job(session, job_id)
    if job is None:
        return None

    ctx.update(
        company_id=job.company_id,
        record_id=job.record_id,
        client_id=job.client_id,
        job_type=job.job_type,
    )

    campaign_run_id = await _run_job_logic(session, job, provider)
    ctx.update(outcome=job.status)
    return campaign_run_id


async def _run_job_logic(
    session: AsyncSession,
    job: MessageJob,
    provider: WhatsAppProvider,
) -> int | None:
    """Process one outbox job.

    Returns the ``campaign_run_id`` (int) when a campaign message is
    successfully sent so the caller can trigger a best-effort recompute
    after the transaction commits.  Returns ``None`` for every other
    outcome (non-campaign job, send failure, guard skip, etc.).
    """
    # Safety guard: orchestrator jobs must never reach outbox_worker.
    # _lock_next_jobs() already excludes them, but if somehow an execution job
    # arrives here (e.g. via direct process_job_in_session call), requeue it so
    # campaign_worker can pick it up, rather than letting it fail with "No phone_e164".
    if job.job_type == CAMPAIGN_EXECUTION_JOB_TYPE:
        logger.error(
            "outbox_worker received campaign execution job_id=%s — requeuing for campaign_worker",
            job.id,
        )
        job.status = "queued"
        job.locked_at = None
        return

    success = await _find_success_outbox(session, job.id)
    if success is not None:
        logger.info(
            "Skip job_id=%s (already sent outbox_id=%s)",
            job.id,
            getattr(success, "id", None),
        )
        job.status = "done"
        job.locked_at = None
        job.last_error = None
        return

    attempts = getattr(job, "attempts", 0)
    max_attempts = getattr(job, "max_attempts", 5)

    if attempts >= max_attempts:
        job.status = "failed"
        job.locked_at = None
        job.last_error = "Max attempts reached"
        return

    record = await _load_record(session, job)
    if _record_is_in_past(record, job_type=job.job_type):
        job.status = "canceled"
        job.locked_at = None
        job.last_error = "Skipped: record starts_at is in the past"
        return

    client = await _load_client(session, job, record)

    # Follow-up final eligibility guard: re-check current recipient/client state
    # before the actual send.  Catches changes that happened during the 14-day
    # delay between job creation and delivery (read, booking, opt-out, future record).
    if job.job_type == FOLLOWUP_JOB_TYPE:
        _fu_payload = getattr(job, "payload", None) or {}
        _fu_recipient_id = _fu_payload.get("campaign_recipient_id")
        _fu_run_id = _fu_payload.get("campaign_run_id")

        # Fail-closed: every followup job must reference a valid recipient.
        if _fu_recipient_id is None:
            job.status = "canceled"
            job.locked_at = None
            job.last_error = "Follow-up skipped: missing campaign_recipient_id"
            logger.warning(
                "followup job without campaign_recipient_id job_id=%s — canceled (fail-closed)",
                job.id,
            )
            return None

        _fu_recipient = await session.get(CampaignRecipient, int(_fu_recipient_id))
        # Fail-closed: recipient referenced in payload must exist in DB.
        if _fu_recipient is None:
            job.status = "canceled"
            job.locked_at = None
            job.last_error = f"Follow-up skipped: campaign_recipient_id={_fu_recipient_id} not found"
            logger.warning(
                "followup job: recipient_id=%s not found job_id=%s — canceled (fail-closed)",
                _fu_recipient_id,
                job.id,
            )
            return None

        # Fail-closed: campaign_run_id is mandatory; attribution_start is unreliable without it.
        if _fu_run_id is None:
            job.status = "canceled"
            job.locked_at = None
            job.last_error = "Follow-up skipped: missing campaign_run_id"
            logger.warning(
                "followup job without campaign_run_id job_id=%s — canceled (fail-closed)",
                job.id,
            )
            return None

        _fu_run = await session.get(CampaignRun, int(_fu_run_id))
        if _fu_run is None:
            job.status = "canceled"
            job.locked_at = None
            job.last_error = f"Follow-up skipped: campaign_run_id={_fu_run_id} not found"
            logger.warning(
                "followup job: run_id=%s not found job_id=%s — canceled (fail-closed)",
                _fu_run_id,
                job.id,
            )
            return None

        _guard = await check_followup_final_eligibility(session, _fu_recipient, _fu_run, utcnow())
        if not _guard.eligible:
            if _guard.booked_after_at is not None and _fu_recipient.booked_after_at is None:
                _fu_recipient.booked_after_at = _guard.booked_after_at
            _fu_recipient.followup_status = _guard.followup_status or "followup_skipped"
            job.status = "canceled"
            job.locked_at = None
            job.last_error = _guard.skip_reason
            logger.info(
                "followup guard skipped job_id=%s recipient_id=%s reason=%r",
                job.id,
                _fu_recipient_id,
                _guard.skip_reason,
            )
            return None

    if client is not None:
        opted_out = bool(getattr(client, "wa_opted_out", False))
        if opted_out and job.job_type in MARKETING_JOB_TYPES:
            job.status = "canceled"
            job.locked_at = None
            job.last_error = "Skipped: client unsubscribed"
            return

    if record is not None and job.job_type in ("review_3d", "repeat_10d"):
        if not _record_attended(record):
            job.status = "canceled"
            job.locked_at = None
            job.last_error = "Skipped: record is not attended"
            return

    if job.job_type == "review_3d":
        altegio_cid = getattr(client, "altegio_client_id", None) if client is not None else None
        if altegio_cid is None:
            job.status = "canceled"
            job.locked_at = None
            job.last_error = "Skipped: no altegio_client_id for review_3d"
            return

        try:
            attended = await count_attended_client_visits(
                company_id=job.company_id,
                altegio_client_id=altegio_cid,
            )
        except Exception as exc:
            logger.warning(
                "review_3d guard: Altegio API failed job_id=%s altegio_client_id=%s guard_attempt=%d: %s",
                job.id,
                altegio_cid,
                _get_api_guard_attempts(job) + 1,
                exc,
            )
            _handle_api_guard_error(job, exc)
            return

        if attended > MAX_VISITS_FOR_REVIEW:
            job.status = "canceled"
            job.locked_at = None
            job.last_error = f"Skipped: client has >{MAX_VISITS_FOR_REVIEW} attended visits (Altegio API)"
            return

    if job.job_type == "repeat_10d":
        altegio_cid = getattr(client, "altegio_client_id", None) if client is not None else None
        if altegio_cid is None:
            job.status = "canceled"
            job.locked_at = None
            job.last_error = "Skipped: no altegio_client_id for repeat_10d"
            return

        try:
            has_future_appointment = await client_has_future_appointments(
                company_id=job.company_id,
                altegio_client_id=altegio_cid,
            )
        except Exception as exc:
            logger.warning(
                "repeat_10d guard: Altegio API failed job_id=%s altegio_client_id=%s guard_attempt=%d: %s",
                job.id,
                altegio_cid,
                _get_api_guard_attempts(job) + 1,
                exc,
            )
            _handle_api_guard_error(job, exc)
            return

        if has_future_appointment:
            job.status = "canceled"
            job.locked_at = None
            job.last_error = "Skipped: client already has a future appointment (Altegio API)"
            return

        if record is not None and record.starts_at is not None:
            returned = await _client_returned_since(
                session,
                job.company_id,
                int(altegio_cid),
                record.starts_at,
                exclude_record_id=int(record.id),
            )
            if returned:
                job.status = "canceled"
                job.locked_at = None
                job.last_error = "Skipped: client already returned within repeat_10d window"
                return

    if record is not None and getattr(record, "is_deleted", False):
        allow_deleted = job.job_type in ("record_canceled", "comeback_3d")
        if not allow_deleted:
            job.status = "canceled"
            job.locked_at = None
            job.last_error = "Skipped: record is deleted"
            return

    if job.job_type == "comeback_3d":
        if record is None:
            job.status = "canceled"
            job.locked_at = None
            job.last_error = COMEBACK_3D_MISSING_SOURCE_REASON
            return

        if record.starts_at is None:
            job.status = "canceled"
            job.locked_at = None
            job.last_error = COMEBACK_3D_MISSING_SOURCE_TIME_REASON
            return

        if not record.is_deleted:
            job.status = "canceled"
            job.locked_at = None
            job.last_error = "Skipped: record is not deleted"
            return

        altegio_cid_comeback = getattr(client, "altegio_client_id", None) if client is not None else None
        if altegio_cid_comeback is None:
            altegio_cid_comeback = getattr(record, "altegio_client_id", None)
        if altegio_cid_comeback is None:
            job.status = "canceled"
            job.locked_at = None
            job.last_error = "Skipped: no altegio_client_id for comeback_3d"
            return

        comeback_cancelled_at = _resolve_comeback_cancelled_at(job, record)
        if comeback_cancelled_at is None:
            job.status = "canceled"
            job.locked_at = None
            job.last_error = COMEBACK_3D_MISSING_CANCEL_TIME_REASON
            return

        if client is not None:
            future_stmt = (
                select(Record.id)
                .where(Record.company_id == job.company_id)
                .where(Record.client_id == client.id)
                .where(Record.is_deleted.is_(False))
                .where(Record.starts_at > utcnow())
                .limit(1)
            )
            future_res = await session.execute(future_stmt)
            if future_res.scalar_one_or_none() is not None:
                job.status = "canceled"
                job.locked_at = None
                job.last_error = "Skipped: client already has a future appointment"
                return

            cutoff_30d = utcnow() - timedelta(days=30)
            sent_stmt = (
                select(OutboxMessage.id)
                .where(OutboxMessage.company_id == job.company_id)
                .where(OutboxMessage.client_id == client.id)
                .where(OutboxMessage.template_code == "comeback_3d")
                .where(OutboxMessage.status.in_(SUCCESS_OUTBOX_STATUSES))
                .where(OutboxMessage.sent_at > cutoff_30d)
                .limit(1)
            )
            sent_res = await session.execute(sent_stmt)
            if sent_res.scalar_one_or_none() is not None:
                job.status = "canceled"
                job.locked_at = None
                job.last_error = "Skipped: comeback_3d already sent in the last 30 days"
                return

        comeback_returned = await _client_returned_since(
            session,
            job.company_id,
            int(altegio_cid_comeback),
            comeback_cancelled_at,
            exclude_record_id=int(record.id),
        )
        if comeback_returned:
            job.status = "canceled"
            job.locked_at = None
            job.last_error = COMEBACK_3D_ALREADY_RETURNED_REASON
            return

    # Effective phone: local client takes priority; CRM-only campaign jobs store phone in payload.
    phone = client.phone_e164 if client else None
    if phone is None:
        phone = (getattr(job, "payload", None) or {}).get("phone_e164")
    if not phone:
        job.status = "failed"
        job.locked_at = None
        job.last_error = "No phone_e164"
        return

    if settings.wa_131026_suppression_enabled and _job_type_allows_131026_suppression(job.job_type):
        n_fail = await _count_131026_failures(
            session,
            phone,
            settings.wa_131026_suppression_window_days,
        )
        if n_fail >= settings.wa_131026_suppression_threshold:
            _wd = settings.wa_131026_suppression_window_days
            reason = f"suppressed_131026: repeated undeliverable ({n_fail} in {_wd}d)"
            out = OutboxMessage(
                company_id=job.company_id,
                client_id=(client.id if client else None),
                record_id=(record.id if record else None),
                job_id=job.id,
                sender_id=None,
                phone_e164=phone,
                template_code=job.job_type,
                language=DEFAULT_LANGUAGE,
                body="",
                status="canceled",
                error=reason,
                provider_message_id=None,
                scheduled_at=job.run_at,
                sent_at=utcnow(),
                meta={
                    "suppression_code": "131026",
                    "threshold": (settings.wa_131026_suppression_threshold),
                    "window_days": _wd,
                    "matched_failures": n_fail,
                },
            )
            session.add(out)
            job.status = "canceled"
            job.locked_at = None
            job.last_error = reason
            logger.info(
                "Suppressed 131026 job_id=%s phone=%s failures=%d window=%dd",
                job.id,
                phone,
                n_fail,
                _wd,
            )
            return

    delay_until = await _apply_rate_limit(session, phone)
    if delay_until is not None:
        job.status = "queued"
        job.locked_at = None
        job.run_at = delay_until
        return

    try:
        body, sender_id, lang, msg_ctx = await _render_message(
            session=session,
            company_id=job.company_id,
            template_code=job.job_type,
            record=record,
            client=client,
        )
    except Exception as exc:
        job.status = "failed"
        job.locked_at = None
        job.last_error = f"Template render error: {exc}"
        return

    _job_payload = getattr(job, "payload", None) or {}
    loyalty_card_text = _job_payload.get("loyalty_card_text", "")

    if loyalty_card_text:
        msg_ctx["loyalty_card_text"] = loyalty_card_text

    # CRM-only newsletter jobs (client is None): populate client_name from payload.contact_name.
    # Without this, msg_ctx["client_name"] stays "" and Meta rejects the template call with
    # "Required parameter is missing" because template param #1 must not be empty.
    # Local-client jobs are unaffected: client is not None so _render_message already set client_name.
    if client is None and _job_payload.get("contact_name"):
        msg_ctx["client_name"] = _job_payload["contact_name"]

    # Effective contact_name for Chatwoot mirror: prefer local client, then payload (CRM-only).
    contact_name = client.display_name if client else _job_payload.get("contact_name")

    attempts = getattr(job, "attempts", 0) + 1
    setattr(job, "attempts", attempts)

    send_mode = settings.whatsapp_send_mode.strip().lower()
    use_template = send_mode in ("template", "auto")

    meta_template_name: str | None = None
    template_params: list[str] = []

    if use_template:
        is_new = bool(msg_ctx.get("pre_appointment_notes", ""))
        meta_template_name = resolve_meta_template(
            job.company_id,
            job.job_type,
            is_new_client=is_new,
        )
        if meta_template_name is None:
            job.status = "failed"
            job.locked_at = None
            job.last_error = f"No Meta template for company={job.company_id} job_type={job.job_type}"
            logger.error(
                "No Meta template for company=%s job_type=%s; failing job_id=%s (send_mode=%s)",
                job.company_id,
                job.job_type,
                job.id,
                send_mode,
            )
            return
        template_params = build_template_params(meta_template_name, msg_ctx)

        # Resolve image header URL for newsletter templates before preflight so
        # a missing URL fails fast with a clear error (no blank-header send).
        header_image_url: str | None = None
        if requires_image_header(meta_template_name):
            header_image_url = _resolve_template_header_image_url(meta_template_name)
            if not header_image_url:
                err_msg = _missing_required_header_error(meta_template_name)
                logger.error(
                    "Missing header image URL template=%s job_id=%s",
                    meta_template_name,
                    job.id,
                )
                job.status = "failed"
                job.locked_at = None
                job.last_error = err_msg
                return

        preflight_err = validate_template_params(meta_template_name, template_params)
        if preflight_err is not None:
            logger.error(
                "Preflight validation failed: %s job_id=%s template=%s",
                preflight_err,
                job.id,
                meta_template_name,
            )
            out = OutboxMessage(
                company_id=job.company_id,
                client_id=(client.id if client else None),
                record_id=(record.id if record else None),
                job_id=job.id,
                sender_id=sender_id,
                phone_e164=phone,
                template_code=job.job_type,
                language=lang,
                body=body,
                status="failed",
                error=preflight_err,
                provider_message_id=None,
                scheduled_at=job.run_at,
                sent_at=utcnow(),
                meta={
                    "send_type": "template",
                    "template": meta_template_name,
                    "params": template_params,
                    "lang": TEMPLATE_LANGUAGE,
                    "validation": "local_preflight_failure",
                },
            )
            session.add(out)
            job.status = "failed"
            job.locked_at = None
            job.last_error = preflight_err
            return

        final_body = body
        for i, val in enumerate(template_params):
            placeholder = f"{{{{{i + 1}}}}}"
            final_body = final_body.replace(placeholder, str(val))
        try:
            final_body = final_body.format(**msg_ctx)
        except Exception:
            pass

    else:
        final_body = body
        try:
            final_body = final_body.format(**msg_ctx)
        except Exception:
            pass

    if use_template:
        assert meta_template_name is not None
        msg_id, err = await safe_send_template(
            provider=provider,
            sender_id=sender_id,
            phone=phone,
            template_name=meta_template_name,
            language=TEMPLATE_LANGUAGE,
            params=template_params,
            fallback_text=final_body,
            contact_name=contact_name,
            company_id=job.company_id,
            header_image_url=header_image_url,
        )
        send_meta: dict[str, Any] = {
            "send_type": "template",
            "template": meta_template_name,
            "params": template_params,
            "lang": TEMPLATE_LANGUAGE,
        }
        if header_image_url:
            send_meta["header_image_url"] = header_image_url
    else:
        msg_id, err = await safe_send(
            provider=provider,
            sender_id=sender_id,
            phone=phone,
            text=final_body,
            contact_name=contact_name,
            company_id=job.company_id,
        )
        send_meta = {"send_type": "text"}

    if err is not None:
        out = OutboxMessage(
            company_id=job.company_id,
            client_id=(client.id if client else None),
            record_id=(record.id if record else None),
            job_id=job.id,
            sender_id=sender_id,
            phone_e164=phone,
            template_code=job.job_type,
            language=lang,
            body=final_body,
            status="failed",
            error=err,
            provider_message_id=msg_id,
            scheduled_at=job.run_at,
            sent_at=utcnow(),
            meta=send_meta,
        )
        session.add(out)

        if _is_token_expired_error(err):
            _mark_token_expired()
            job.status = "queued"
            job.locked_at = None
            job.run_at = utcnow() + timedelta(seconds=TOKEN_EXPIRED_RETRY_SECONDS)
            job.last_error = f"Send blocked: {err}"
            return

        job.last_error = f"Send failed: {err}"

        max_attempts = getattr(job, "max_attempts", 5)
        if attempts >= max_attempts:
            job.status = "failed"
            job.locked_at = None
            return

        delay = _retry_delay_seconds(attempts)
        job.status = "queued"
        job.locked_at = None
        job.run_at = utcnow() + timedelta(seconds=delay)
        return

    now_sent = utcnow()
    out = OutboxMessage(
        company_id=job.company_id,
        client_id=(client.id if client else None),
        record_id=(record.id if record else None),
        job_id=job.id,
        sender_id=sender_id,
        phone_e164=phone,
        template_code=job.job_type,
        language=lang,
        body=final_body,
        status="sent",
        error=None,
        provider_message_id=msg_id,
        scheduled_at=job.run_at,
        sent_at=now_sent,
        meta=send_meta,
    )
    session.add(out)

    # Backfill CampaignRecipient → OutboxMessage link if this is a campaign
    # job.  Flush first so out.id is populated by the RETURNING clause.
    campaign_recipient_id = _job_payload.get("campaign_recipient_id")
    if campaign_recipient_id is not None:
        await session.flush()
        recipient = await session.get(CampaignRecipient, int(campaign_recipient_id))
        if recipient is not None:
            if recipient.outbox_message_id is None:
                recipient.outbox_message_id = out.id
            if recipient.provider_message_id is None and msg_id:
                recipient.provider_message_id = msg_id
            if recipient.sent_at is None:
                recipient.sent_at = now_sent

    job.status = "done"
    job.locked_at = None
    job.last_error = None

    logger.info(
        "Outbox sent job_id=%s outbox_id=%s sender_id=%s phone=%s send_type=%s template=%s",
        job.id,
        getattr(out, "id", None),
        sender_id,
        phone,
        send_meta.get("send_type"),
        send_meta.get("template"),
    )

    # Signal to the caller that this campaign run needs a post-commit
    # stats recompute.  Only returned when the job is a campaign message
    # (payload contains campaign_run_id) and the send succeeded.
    _campaign_run_id = _job_payload.get("campaign_run_id")
    if _campaign_run_id is not None:
        return int(_campaign_run_id)
    return None


async def _try_recompute_campaign_run_stats(run_id: int) -> None:
    """Best-effort recompute of a campaign run's stats after a send.

    Opens its own session so any failure is completely isolated from the
    already-committed outbox send.  All exceptions are caught and logged
    as warnings — they never propagate to the caller.
    """
    try:
        async with SessionLocal() as session:
            async with session.begin():
                await recompute_campaign_run_stats(session, run_id)
        logger.info("auto-recompute ok run_id=%s", run_id)
    except Exception as exc:
        logger.warning(
            "auto-recompute failed run_id=%s (best-effort, ignored): %s",
            run_id,
            exc,
        )


async def process_job(
    job_id: int,
    provider: WhatsAppProvider,
    *,
    _pending_recomputes: set[int] | None = None,
) -> None:
    """Process one outbox job in its own session/transaction.

    *_pending_recomputes* is an optional set supplied by batch callers
    (e.g. ``run_loop``).  When provided, a successful campaign send adds
    its ``campaign_run_id`` to the set instead of triggering recompute
    immediately — the caller is responsible for deduplicating and calling
    :func:`_try_recompute_campaign_run_stats` once per unique run_id
    after the whole batch is done.

    When *_pending_recomputes* is ``None`` (the default), the function
    is self-contained: recompute fires right after the commit, which is
    correct for any caller that processes a single job at a time.
    """
    campaign_run_id: int | None = None
    async with SessionLocal() as session:
        async with session.begin():
            campaign_run_id = await process_job_in_session(
                session=session,
                job_id=job_id,
                provider=provider,
            )
    if campaign_run_id is not None:
        if _pending_recomputes is not None:
            # Deferred / batch mode: caller will flush once per unique run.
            _pending_recomputes.add(campaign_run_id)
        else:
            # Self-contained mode: recompute immediately after this commit.
            await _try_recompute_campaign_run_stats(campaign_run_id)


async def run_loop(
    provider: WhatsAppProvider,
    batch_size: int = 50,
    poll_sec: float = 1.0,
) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logger.info("Outbox worker started")

    while True:
        async with SessionLocal() as session:
            async with session.begin():
                recovered = await _requeue_stale_processing_jobs(session)
                if recovered:
                    logger.warning(
                        "Recovered stale processing jobs: %s",
                        recovered,
                    )

        job_ids: list[int] = []

        async with SessionLocal() as session:
            async with session.begin():
                jobs = await _lock_next_jobs(session, batch_size)
                job_ids = [j.id for j in jobs]

        if not job_ids:
            await asyncio.sleep(poll_sec)
            continue

        # Collect campaign run_ids across the whole cycle so that
        # recompute is called once per unique run, not once per message.
        pending: set[int] = set()

        for idx, jid in enumerate(job_ids):
            await process_job(
                job_id=jid,
                provider=provider,
                _pending_recomputes=pending,
            )

            if _token_expired() and _stop_worker_on_token_expired():
                remaining = job_ids[idx + 1 :]
                if remaining:
                    async with SessionLocal() as session:
                        async with session.begin():
                            await _requeue_processing_jobs(session, remaining)

                logger.error(
                    "Stopping outbox worker: access token expired (requeued %s jobs)",
                    len(remaining),
                )
                # Still recompute for runs that did get sent this cycle.
                for run_id in pending:
                    await _try_recompute_campaign_run_stats(run_id)
                return

        # End of cycle: one recompute per unique campaign run.
        for run_id in pending:
            await _try_recompute_campaign_run_stats(run_id)


async def run_once(
    session_maker: Any,
    *,
    provider: Any,
    limit: int = 10,
    company_id: int | None = None,
) -> int:
    from sqlalchemy import func, select

    async with session_maker() as session:
        async with session.begin():
            await _requeue_stale_processing_jobs(session)

        stmt = (
            select(MessageJob.id)
            .where(MessageJob.status == "queued")
            .where(MessageJob.job_type != CAMPAIGN_EXECUTION_JOB_TYPE)
            .where(MessageJob.run_at <= func.now())
            .order_by(MessageJob.run_at.asc(), MessageJob.id.asc())
            .limit(limit)
        )
        if company_id is not None:
            stmt = stmt.where(MessageJob.company_id == company_id)

        res = await session.execute(stmt)
        ids = list(res.scalars().all())

        campaign_run_ids: set[int] = set()
        for job_id in ids:
            run_id = await process_job_in_session(
                session,
                int(job_id),
                provider=provider,
            )
            if run_id is not None:
                campaign_run_ids.add(run_id)

        await session.commit()

        # Best-effort recompute for each campaign run that got at least one
        # new sent message.  Each call opens its own session so recompute
        # failures cannot affect the committed sends above.
        for run_id in campaign_run_ids:
            await _try_recompute_campaign_run_stats(run_id)

        return len(ids)
