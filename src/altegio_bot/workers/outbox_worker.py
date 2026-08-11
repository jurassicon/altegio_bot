from __future__ import annotations

import asyncio
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import or_, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.altegio_records import (
    client_has_any_future_record,
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
from altegio_bot.delivery_retry_identity import (
    DELIVERY_RETRY_JOB_TYPES,
    claims_delivery_retry,
    delivery_retry_audit,
    resolve_retry_chain_members,
    resolve_retry_identity,
    resolve_retry_reference,
    retry_outbox_audit_mismatch,
)
from altegio_bot.easyweek_branches import (
    PRE_APPOINTMENT_NOTES_DE,
    BranchProfile,
    branch_profile_for_slug,
    branch_template_contract_error,
)
from altegio_bot.easyweek_locations import EasyWeekLocation, configured_easyweek_locations
from altegio_bot.easyweek_normalizer import extract_manage_link
from altegio_bot.easyweek_policy import (
    EASYWEEK_LIFECYCLE_JOB_TYPES,
    RECORD_CREATED,
    RECORD_CREATED_NEW_CLIENT,
    easyweek_job_type_error,
    normalize_provider,
    validate_static_booking_page,
)
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
    build_lifecycle_template_params,
    build_template_params,
    requires_image_header,
    resolve_meta_template,
)
from altegio_bot.models.models import (
    PROVIDER_ALTEGIO,
    PROVIDER_EASYWEEK,
    CampaignRecipient,
    CampaignRun,
    Client,
    ContactRateLimit,
    MessageJob,
    MessageTemplate,
    OutboxMessage,
    PromoLead,
    Record,
    RecordService,
    WhatsAppSender,
)
from altegio_bot.perf import perf_log
from altegio_bot.promo_discount_apply import process_promo_apply_existing_booking_job
from altegio_bot.providers.base import WhatsAppProvider
from altegio_bot.providers.dummy import safe_send, safe_send_template
from altegio_bot.services import meta_circuit
from altegio_bot.services.meta_error_classifier import (
    is_permanent_meta_template_error,
    is_text_window_policy_error,
    is_token_expired_error,
    is_transient_provider_error,
    transient_error_reason,
)
from altegio_bot.settings import settings
from altegio_bot.template_validation import validate_lifecycle_template_params, validate_template_params
from altegio_bot.whatsapp_routing import pick_sender_code_for_record, pick_sender_id
from altegio_bot.whatsapp_window import is_whatsapp_customer_window_open
from altegio_bot.workers.promo_lead_handler import (
    PROMO_APPLY_EXISTING_BOOKING_JOB_TYPE,
    PROMO_CARD_BOOKING_REMINDER_JOB_TYPE,
    PROMO_ELIGIBILITY_CHECK_JOB_TYPE,
    process_promo_eligibility_check_job,
)

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

# Recurring marketing campaign job types subject to the stricter 90-day
# suppression (any prior 131026/131049 failure or suppressed_* row blocks the
# next send). Repeatedly hitting a known-undeliverable number across recurring
# blasts is wasteful and risky for the WABA.
#
# promo_card_booking_reminder is intentionally NOT here: it is a single
# lifecycle nudge tied to one issued promo card (not a recurring campaign) and
# it runs on a SEPARATE send path (_process_promo_card_booking_reminder) which
# never reaches the _run_job_logic 90-day marketing block. It keeps the standard
# 14-day 131026 threshold guard via WA_131026_SUPPRESSIBLE_JOB_TYPES below.
MARKETING_JOB_TYPES = (
    "review_3d",
    "repeat_10d",
    "comeback_3d",
    "newsletter_new_clients_monthly",
    "newsletter_new_clients_followup",
)

MARKETING_TRANSIENT_CAP_JOB_TYPES = (
    *MARKETING_JOB_TYPES,
    "promo_card_booking_reminder",
    FOLLOWUP_JOB_TYPE,
)

DELIVERY_DEADLINE_JOB_TYPES = DELIVERY_RETRY_JOB_TYPES

_DELIVERED_READ_STATUSES = ("delivered", "read")
_DEADLINE_ALREADY_PASSED = datetime(1970, 1, 1, tzinfo=timezone.utc)
_MARKETING_TRANSIENT_RETRY_CAP = timedelta(hours=24)
_ORIGINAL_RUN_AT_KEY = "_original_run_at"

WA_131026_SUPPRESSIBLE_JOB_TYPES: tuple[str, ...] = (
    "review_3d",
    "repeat_10d",
    "comeback_3d",
    "newsletter_new_clients_monthly",
    "newsletter_new_clients_followup",
    "promo_card_booking_reminder",
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

# Separate counter for the follow-up live Altegio guard stored in job.payload.
# Independent from _api_guard_attempts (which serves review_3d / repeat_10d / comeback_3d)
# and from job.attempts (which counts actual WhatsApp send attempts).
_FOLLOWUP_LIVE_GUARD_ATTEMPTS_KEY = "_followup_live_guard_attempts"
MAX_FOLLOWUP_LIVE_GUARD_ATTEMPTS = 10

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
_REMINDER_STALE_TOLERANCE_SECONDS = 60

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


def _followup_live_guard_delay_seconds(attempt: int) -> int:
    """Retry delay for the follow-up live Altegio guard: 1 min / 5 min / 25 min / 1 h+."""
    if attempt <= 1:
        return 60
    if attempt == 2:
        return 300
    if attempt == 3:
        return 1500
    return 3600


def _get_followup_live_guard_attempts(job: MessageJob) -> int:
    """Return the current follow-up live guard retry count from job.payload."""
    return int((getattr(job, "payload", None) or {}).get(_FOLLOWUP_LIVE_GUARD_ATTEMPTS_KEY, 0))


def _handle_followup_live_guard_error(job: MessageJob, exc: Exception) -> None:
    """Increment the follow-up live guard counter and requeue or permanently fail the job.

    Uses a dedicated payload counter so transient Altegio outages do not consume
    the job.attempts budget (which is reserved for real WhatsApp send attempts).
    """
    payload = dict(getattr(job, "payload", None) or {})
    count = int(payload.get(_FOLLOWUP_LIVE_GUARD_ATTEMPTS_KEY, 0)) + 1
    payload[_FOLLOWUP_LIVE_GUARD_ATTEMPTS_KEY] = count
    job.payload = payload

    if count >= MAX_FOLLOWUP_LIVE_GUARD_ATTEMPTS:
        job.status = "failed"
        job.locked_at = None
        job.last_error = f"Follow-up delayed: Altegio future-record check failed (max attempts): {exc}"
        return

    delay = _followup_live_guard_delay_seconds(count)
    job.status = "queued"
    job.locked_at = None
    job.run_at = utcnow() + timedelta(seconds=delay)
    job.last_error = f"Follow-up delayed: Altegio future-record check failed: {exc}"


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
        select(MessageJob)
        .where(MessageJob.status == "processing")
        .where(MessageJob.locked_at.is_not(None))
        .where(MessageJob.locked_at < cutoff)
        .order_by(MessageJob.id.asc())
        .with_for_update(skip_locked=True)
    )
    jobs = list((await session.execute(stmt)).scalars().all())

    recovered = 0
    now = utcnow()
    for job in jobs:
        # A stale legacy retry is not trusted merely because it once reached
        # `processing`. Re-prove it before making it sendable again; otherwise
        # recovery would undo the presend safety boundary on every worker boot.
        if claims_delivery_retry(job):
            reference = resolve_retry_reference(job)
            if reference.reference is None:
                guard_reason = f"Canceled: {reference.error or 'delivery_retry_reference_unproven'}"
            else:
                record = await _load_record(session, job)
                guard_reason = await _delivery_retry_presend_guard(session, job, record)
            if guard_reason is not None:
                job.status = "canceled"
                job.locked_at = None
                job.updated_at = now
                job.last_error = guard_reason
                continue

        job.status = "queued"
        job.locked_at = None
        job.run_at = now
        job.updated_at = now
        job.last_error = "Recovered: stale processing job"
        recovered += 1

    return recovered


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


def _check_reminder_stale(
    job: MessageJob,
    record: Record | None,
) -> tuple[bool, str | None]:
    """Return (True, reason) when a reminder job is stale after reschedule.

    New jobs carry an immutable ``record_starts_at`` in payload.  Legacy
    jobs (without the field) fall back to ``job.run_at + offset``.
    Both paths use a 60-second tolerance to absorb microsecond noise.
    """
    if record is None or record.starts_at is None:
        return False, None

    current_utc = _as_utc(record.starts_at)
    payload = getattr(job, "payload", None) or {}
    raw_starts_at = payload.get("record_starts_at")

    if raw_starts_at is not None:
        payload_dt = _parse_payload_datetime(raw_starts_at)
        if payload_dt is None:
            return True, "Skipped: malformed reminder record_starts_at"
        delta = abs((payload_dt - current_utc).total_seconds())
        if delta > _REMINDER_STALE_TOLERANCE_SECONDS:
            return True, "Skipped: stale reminder after record reschedule"
        return False, None

    # Legacy fallback: no record_starts_at in payload.
    # run_at may have been shifted by rate-limit/retry, so only cancel when
    # the job is pristine (never retried, no prior error).
    run_at = getattr(job, "run_at", None)
    if run_at is None:
        return False, None

    attempts = getattr(job, "attempts", 0) or 0
    last_error = getattr(job, "last_error", None)
    if attempts > 0 or last_error is not None:
        logger.warning(
            "legacy reminder without record_starts_at cannot be safely"
            " stale-checked after retry/rate-limit job_id=%s attempts=%s",
            getattr(job, "id", None),
            attempts,
        )
        return False, None

    run_at_utc = _as_utc(run_at)
    if job.job_type == "reminder_24h":
        expected = run_at_utc + timedelta(hours=24)
    else:
        expected = run_at_utc + timedelta(hours=2)

    delta = abs((expected - current_utc).total_seconds())
    if delta > _REMINDER_STALE_TOLERANCE_SECONDS:
        return (
            True,
            "Skipped: stale legacy reminder after record reschedule",
        )
    return False, None


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
    provider: str = PROVIDER_ALTEGIO,
) -> tuple[MessageTemplate | None, str]:
    """Look up the active MessageTemplate for *provider* / *company_id* / *code*.

    ``provider`` bounds EVERY phase below, including the cross-company fallback.
    EasyWeek's ``company_id`` is the numeric EasyWeek ``:location_id`` and shares
    an integer space with Altegio company ids, so a provider-blind query could
    serve an Altegio body — with an Altegio address footer — to an EasyWeek
    customer. The column has a server default of ``'altegio'``, but a default is
    not a filter: it only decides what NEW rows get, so the predicate is explicit.

    For EasyWeek there is deliberately NO cross-company phase at all (see below).

    Lookup order — every step is ``order_by(id ASC).limit(1)``. Nothing enforces
    one row per (provider, company_id, code, language), so without the explicit
    order the winner would be whatever the planner returned first and could
    change between two runs over identical data.

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
        .where(MessageTemplate.provider == provider)
        .where(MessageTemplate.company_id == company_id)
        .where(MessageTemplate.code == template_code)
        .where(MessageTemplate.is_active.is_(True))
    )

    stmt = base.where(MessageTemplate.language == language).order_by(MessageTemplate.id.asc()).limit(1)
    res = await session.execute(stmt)
    tmpl = res.scalar_one_or_none()
    if tmpl is not None:
        return tmpl, language

    if language != DEFAULT_LANGUAGE:
        stmt = base.where(MessageTemplate.language == DEFAULT_LANGUAGE).order_by(MessageTemplate.id.asc()).limit(1)
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
    # EasyWeek has exactly ONE location, so "another company's row" can only be a
    # different tenant — never a legitimate shared template. There is no
    # cross-company phase for it at any code.
    if provider != PROVIDER_ALTEGIO:
        return None, language

    if template_code not in UNIVERSAL_JOB_TYPES:
        return None, language

    cross = (
        select(MessageTemplate)
        .where(MessageTemplate.provider == provider)
        .where(MessageTemplate.code == template_code)
        .where(MessageTemplate.is_active.is_(True))
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
    provider: str | None = None,
) -> bool:
    """True when this booking is the customer's first at *company_id*.

    ``provider`` is optional and defaults to no filter, which keeps every
    existing Altegio call site on byte-identical SQL. EasyWeek passes it: the
    two CRMs share one integer space for ``company_id``, so without the
    predicate an Altegio booking could count as a "previous visit" and quietly
    downgrade a first-time EasyWeek customer to the ordinary confirmation.
    """
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
    if provider is not None:
        stmt = stmt.where(Record.provider == provider)
    res = await session.execute(stmt)
    prev_id = res.scalar_one_or_none()
    return prev_id is None


def _easyweek_domain_scope_error(
    job: MessageJob,
    record: Record | None,
    client: Client | None,
    *,
    provider: str,
) -> str | None:
    """Prove the loaded Record and Client really belong to this EasyWeek job.

    ``record_id`` and ``client_id`` are plain BIGINTs into tables that hold BOTH
    CRMs' rows. A job pointing at the wrong id — a mis-planned job, an id reused
    after a restore, a hand-edited row — would render an Altegio customer's
    appointment into an EasyWeek message and send it from the EasyWeek number.
    Nothing downstream re-checks this: ``_render_message`` trusts the objects it
    is handed, and the provider predicates added in PR-5 bound the TEMPLATE and
    the SENDER, not the domain rows.

    ``RecordService`` has no provider column of its own — it is scoped only
    through ``record_id``. So proving the Record here is exactly what makes the
    service snapshot read later trustworthy.

    Returns a reason on mismatch, ``None`` when the trio is consistent. The
    reason names the broken invariant and never the values behind it: the
    offending row belongs to another tenant, and this string is written to
    ``job.last_error`` and to the log.
    """
    if record is None:
        return "EasyWeek lifecycle job has no record"
    if client is None:
        return "EasyWeek lifecycle job has no client"
    if record.provider != provider:
        return "Record belongs to a different provider than the job"
    if client.provider != provider:
        return "Client belongs to a different provider than the job"
    if record.company_id != job.company_id:
        return "Record belongs to a different company than the job"
    if client.company_id != job.company_id:
        return "Client belongs to a different company than the job"
    if record.client_id is None:
        return "Record has no client"
    if job.client_id is not None and job.client_id != record.client_id:
        return "Job client_id does not match record client_id"
    if client.id != record.client_id:
        return "Loaded client is not the record's client"
    return None


def _easyweek_service_snapshot_error(
    record: Record,
    services: list[RecordService],
) -> str | None:
    """Refuse to invent a service name or a price the booking does not have.

    PR-4 deliberately keeps three states apart: a real ``Decimal("0.00")``, an
    unknown ``NULL``, and an authoritative clear. Rendering flattens all of them
    — ``f"{svc.title}"`` turns ``None`` into the literal string ``"None"`` and
    ``_fmt_money(None)`` turns an unknown price into ``0.00`` — and the preflight
    then waves both through, because they are non-empty strings. A customer would
    read "None — 0.00€" for a booking whose price nobody knows.

    So for EasyWeek lifecycle v1 the contract is fail-closed: render only a
    snapshot that is complete and self-consistent. ``Decimal("0.00")`` stays a
    perfectly valid price — this rejects UNKNOWN, not zero.

    The single-row shape is PR-4's, not an assumption: the payload carries one
    flat service, and ``_sync_service_snapshot`` deletes any row whose
    ``service_id`` the delivery did not name.

    Reasons carry no title and no amount — an inconsistent snapshot may hold
    another tenant's data, and this string reaches ``job.last_error``.
    """
    if len(services) != 1:
        return f"EasyWeek service snapshot must hold exactly one service, found {len(services)}"

    svc = services[0]
    if svc.title is None or not svc.title.strip():
        return "EasyWeek service snapshot has no service title"
    if svc.cost_to_pay is None:
        return "EasyWeek service snapshot has no price"
    if record.total_cost is None:
        return "EasyWeek record has no total_cost"
    if record.total_cost != svc.cost_to_pay:
        # PR-4 keeps these identical by construction, so a divergence means the
        # snapshot was written by something else — guessing which one is right
        # would be inventing a price.
        return "EasyWeek record total_cost and service cost_to_pay disagree"
    return None


def _easyweek_owned_branch(
    company_id: int | None,
) -> tuple[EasyWeekLocation | None, BranchProfile | None, str | None]:
    """Resolve one job to the registry entry and profile selected by its slug."""
    registry = configured_easyweek_locations()
    if not registry.configured:
        return None, None, "EasyWeek registry is not configured; refusing to send."
    if not registry.valid:
        return None, None, "EasyWeek registry is invalid; refusing to send."

    location = registry.locations.get(company_id) if company_id is not None else None
    if location is None:
        return (
            None,
            None,
            f"EasyWeek company {company_id} is not in the configured registry; refusing to send.",
        )

    profile = branch_profile_for_slug(location.name)
    if profile is None:
        return (
            location,
            None,
            f"EasyWeek company {company_id} has no source-controlled branch profile; refusing to send.",
        )
    if location.meta_template_prefix != profile.meta_template_prefix:
        return (
            location,
            None,
            f"EasyWeek company {company_id} registry prefix does not match its branch profile; refusing to send.",
        )
    return location, profile, None


def easyweek_job_ownership_error(company_id: int | None) -> str | None:
    """Fail-closed proof that the configured registry still owns this company.

    The registry is the EasyWeek tenant boundary. A queued job outlives the
    configuration that created it, so membership has to be re-proven at send
    time: a branch removed from ``EASYWEEK_LOCATION_MAP``, an empty registry and
    a malformed one must all stop the job rather than let an already-verified
    ``Record.short_link`` carry it through.

    The branch must also resolve to a source-controlled profile, so a company
    whose content and template prefix nobody has approved cannot send at all.

    Returns a short, stable reason (no registry contents, no URLs, no payload)
    or ``None`` when the job may proceed.
    """
    _location, _profile, error = _easyweek_owned_branch(company_id)
    return error


def easyweek_effective_booking_link(record: Record | None, template_code: str, *, company_id: int) -> str:
    """The ONLY link an EasyWeek lifecycle message may carry.

    ``record_canceled`` always gets the static booking page, even when a
    verified manage link exists: the call to action after a cancellation is
    "book again", and pointing the customer at the management page of a booking
    that no longer exists is both useless and confusing.

    ``record_created`` / ``record_updated`` may use ``Record.short_link``, but
    only after the stored pair is re-verified HERE, at send time, by the very
    same validator the normalizer used — :func:`extract_manage_link`. Re-using it
    rather than re-implementing a check is the point: a second, looser copy would
    drift, and this is the last gate before a URL reaches a customer. A link is
    never synthesised from the booking UUID, the numeric id or the hash.

    The static page comes from the registry entry selected by ``company_id`` and
    is validated too, by :func:`validate_static_booking_page`. An unknown
    location, invalid registry or unusable URL yields "" here, and the caller
    fails the job locally rather than sending a blank or cross-branch link.

    Anything that does not verify falls back to the static page.
    """
    # Ownership FIRST, before any link is considered. A proven `short_link` on
    # the Record is not authorisation to send: the registry is the tenant
    # boundary, and a branch that was removed from it — or a registry that is
    # empty or malformed — must not yield a customer-facing URL just because an
    # older, then-valid manage link is still stored on the row.
    location, _profile, ownership_error = _easyweek_owned_branch(company_id)
    if ownership_error is not None or location is None:
        return ""

    static_page = validate_static_booking_page(location.booking_page_url) or ""
    if record is None or template_code == "record_canceled":
        return static_page

    link, _present = extract_manage_link(
        {
            "booking_page": record.short_link,
            "booking_hash_id": record.easyweek_booking_hash_id,
        }
    )
    if link is None:
        return static_page
    return link.url


async def _render_message(
    session: AsyncSession,
    *,
    company_id: int,
    template_code: str,
    record: Record | None,
    client: Client | None,
    provider: str = PROVIDER_ALTEGIO,
) -> tuple[str, int, str, dict[str, Any]]:
    is_easyweek = provider == PROVIDER_EASYWEEK
    language = (
        (settings.easyweek_default_language or DEFAULT_LANGUAGE).strip() or DEFAULT_LANGUAGE
        if is_easyweek
        else _pick_language(company_id, client)
    )

    # A first-time EasyWeek customer gets a different approved Meta template,
    # which means a different DB row — but the SAME `record_created` job.
    #
    # That distinction is the whole design. `build_lifecycle_template_params`
    # and its preflight key on `MessageJob.job_type`, which never changes, so
    # the seven-field contract keeps holding; only the row that is looked up —
    # and therefore `meta_template_name` — differs. Giving the new-client
    # variant its own job type would have needed a second param contract and a
    # second entry in every allowlist; giving it its own template code needs
    # neither.
    #
    # Decided BEFORE the lookup, because it selects which row to load. The
    # Altegio path is untouched: it keeps one row plus `{pre_appointment_notes}`
    # and decides after loading, where `used_lang` is known.
    lookup_code = template_code
    if is_easyweek and template_code == RECORD_CREATED and record is not None:
        is_new_client = await _is_new_client_for_record(
            session=session,
            company_id=company_id,
            client_id=record.client_id,
            record_id=record.id,
            record_starts_at=record.starts_at,
            provider=PROVIDER_EASYWEEK,
        )
        if is_new_client:
            lookup_code = RECORD_CREATED_NEW_CLIENT

    tmpl, used_lang = await _load_template(
        session,
        company_id=company_id,
        template_code=lookup_code,
        language=language,
        provider=provider,
    )
    if tmpl is None and lookup_code != template_code:
        # The new-client row is optional. A location that has not seeded it yet
        # still sends the ordinary confirmation rather than failing a booking
        # over a variant that is a nicety, not a requirement.
        logger.info(
            "EasyWeek new-client template missing; falling back company=%s code=%s",
            company_id,
            lookup_code,
        )
        lookup_code = template_code
        tmpl, used_lang = await _load_template(
            session,
            company_id=company_id,
            template_code=template_code,
            language=language,
            provider=provider,
        )
    if tmpl is None:
        raise ValueError(f"Template not found: provider={provider} company={company_id} code={template_code}")

    services_text = ""
    primary_service = ""
    total_cost = Decimal("0.00")

    if record is not None:
        svc_stmt = (
            select(RecordService).where(RecordService.record_id == record.id).order_by(RecordService.service_id.asc())
        )
        svc_res = await session.execute(svc_stmt)
        services = list(svc_res.scalars().all())

        if is_easyweek and template_code in EASYWEEK_LIFECYCLE_JOB_TYPES:
            # BEFORE the loop below, which is what would flatten an unknown
            # title into "None" and an unknown price into "0.00".
            snapshot_err = _easyweek_service_snapshot_error(record, services)
            if snapshot_err is not None:
                raise ValueError(snapshot_err)

        if services:
            primary_service = services[0].title or ""

        lines: list[str] = []
        for svc in services:
            lines.append(f"{svc.title} — {_fmt_money(svc.cost_to_pay)}€")
            if svc.cost_to_pay is not None:
                total_cost += svc.cost_to_pay

        services_text = "\n".join(lines)

    unsubscribe_link = ""
    if is_easyweek:
        # EasyWeek has no BOOKING_LINKS entry and must never borrow one: that map
        # is keyed by Altegio company id and holds Altegio salon pages.
        booking_link = easyweek_effective_booking_link(record, template_code, company_id=company_id)
    else:
        booking_link = BOOKING_LINKS.get(company_id, "")

    sender_code = "default"
    # `service_sender_rules` is NOT provider-scoped, so an EasyWeek service id
    # could match an Altegio rule and route the message to the wrong number.
    # Until that table gets its own provider-aware design, EasyWeek stays on the
    # default sender — safe, and explicit rather than accidental.
    if record is not None and not is_easyweek:
        sender_code = await pick_sender_code_for_record(
            session=session,
            company_id=company_id,
            record_id=record.id,
        )

    sender_id = await pick_sender_id(
        session=session,
        company_id=company_id,
        sender_code=sender_code,
        provider=provider,
    )
    if sender_id is None:
        raise ValueError(f"No active sender for provider={provider} company={company_id} code={sender_code}")

    pre_appointment_notes = ""
    # PRE_APPOINTMENT_NOTES_DE is KitiLash/Altegio copy — lash-extension prep
    # instructions written for that salon. It has no business being appended to
    # another CRM's booking confirmation, and PR-5 does not invent an EasyWeek
    # equivalent: an empty string is the honest value until someone writes one.
    if not is_easyweek and template_code == "record_created" and record is not None and used_lang == "de":
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

    if is_easyweek:
        # From the SAME row whose body/language/code were just used, so the name
        # and the text can never come from two different templates.
        ctx["meta_template_name"] = tmpl.meta_template_name
        # The code that was ACTUALLY selected — `record_created_new_client` is a
        # distinct approved template, not a variant — so the branch guard below
        # validates the row that will really be sent.
        ctx["easyweek_template_code"] = lookup_code
        # An EasyWeek lifecycle message links only via `booking_link`; the raw
        # `short_link` is unverified at this point and must not leak into a
        # parameter slot.
        ctx["short_link"] = booking_link

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


# Backwards-compatible private aliases for the shared error classifier.
# Internal call sites and tests reference these underscore names.
_is_token_expired_error = is_token_expired_error
_is_permanent_meta_template_error = is_permanent_meta_template_error
_is_text_window_policy_error = is_text_window_policy_error
_is_transient_provider_error = is_transient_provider_error
_transient_error_reason = transient_error_reason


def _decrement_send_attempt(job: MessageJob) -> None:
    job.attempts = max(0, int(getattr(job, "attempts", 0) or 0) - 1)


def _original_run_at(job: MessageJob) -> datetime | None:
    payload = getattr(job, "payload", None) or {}
    parsed = _parse_payload_datetime(payload.get(_ORIGINAL_RUN_AT_KEY))
    if parsed is not None:
        return parsed
    run_at = getattr(job, "run_at", None)
    if run_at is None:
        return None
    return _as_utc(run_at)


def _anchor_run_at(job: MessageJob, original_outbox: OutboxMessage | None) -> datetime | None:
    if original_outbox is not None and original_outbox.scheduled_at is not None:
        return _as_utc(original_outbox.scheduled_at)
    return _original_run_at(job)


def _retry_deadline_at(
    job: MessageJob,
    record: Record | None,
    *,
    original_outbox: OutboxMessage | None = None,
) -> datetime | None:
    job_type = getattr(job, "job_type", None)

    if job_type in MARKETING_TRANSIENT_CAP_JOB_TYPES:
        anchor = _anchor_run_at(job, original_outbox)
        return anchor + _MARKETING_TRANSIENT_RETRY_CAP if anchor is not None else None

    if job_type not in DELIVERY_DEADLINE_JOB_TYPES:
        return None

    starts_at = None
    if record is not None and record.starts_at is not None:
        starts_at = _as_utc(record.starts_at)
    if starts_at is None:
        return _DEADLINE_ALREADY_PASSED

    if job_type in ("record_created", "record_updated"):
        return starts_at - timedelta(minutes=30)
    if job_type == "record_canceled":
        return starts_at - timedelta(minutes=15)
    if job_type == "reminder_2h":
        return starts_at - timedelta(minutes=15)
    if job_type == "reminder_24h":
        deadline = starts_at - timedelta(hours=3)
        anchor = _anchor_run_at(job, original_outbox)
        if anchor is not None:
            deadline = min(deadline, anchor + timedelta(hours=6))
        return deadline

    return None


def _schedule_retry_or_cancel(
    job: MessageJob,
    record: Record | None,
    delay_seconds: int,
    reason: str,
    *,
    original_outbox: OutboxMessage | None = None,
) -> bool:
    payload = dict(getattr(job, "payload", None) or {})
    if _ORIGINAL_RUN_AT_KEY not in payload and getattr(job, "run_at", None) is not None:
        payload[_ORIGINAL_RUN_AT_KEY] = _as_utc(job.run_at).isoformat()
        job.payload = payload

    next_run_at = utcnow() + timedelta(seconds=delay_seconds)
    deadline = _retry_deadline_at(job, record, original_outbox=original_outbox)
    if deadline is not None and next_run_at > deadline:
        job.status = "canceled"
        job.locked_at = None
        job.updated_at = utcnow()
        job.last_error = f"Retry deadline exceeded for {job.job_type}"
        return False

    job.status = "queued"
    job.run_at = next_run_at
    job.locked_at = None
    job.updated_at = utcnow()
    job.last_error = reason
    return True


def _deadline_passed_for_send(job: MessageJob, record: Record | None) -> bool:
    if job.job_type not in DELIVERY_DEADLINE_JOB_TYPES:
        return False
    payload = getattr(job, "payload", None) or {}
    if _ORIGINAL_RUN_AT_KEY not in payload:
        return False
    deadline = _retry_deadline_at(job, record)
    if deadline is None or deadline == _DEADLINE_ALREADY_PASSED:
        return False
    return utcnow() > deadline


async def _delivery_retry_chain_has_success(
    session: AsyncSession,
    original_outbox_id: int,
) -> bool:
    """True when the canonical chain rooted at *original_outbox_id* has landed.

    Membership is PROVEN, not inferred from the dedupe prefix. Sharing a key
    prefix says only that a row was named after this root; it says nothing about
    who wrote it or which chain its payload points at. A corrupted or
    hand-written row with a delivered outbox could otherwise declare the whole
    chain successful — suppressing a legitimate failed-callback and cancelling
    correct queued retries.

    So a candidate counts only when its own payload/namespace prove a reference
    to THIS root and its identity matches the identity proven from the root
    itself, via :func:`resolve_retry_chain_members` — the same definition of
    membership every other retry decision uses.

    The delivered ``OutboxMessage`` is checked too, not just the job that owns
    it. Belonging to a proven member is what puts a row in the chain; its own
    company, record, effective client and template code then have to agree with
    the tenant that chain belongs to, and its retry audit has to corroborate
    this root and this attempt — by the SAME predicate the callback resolver
    applies, :func:`retry_outbox_audit_mismatch`. Without that last check the
    identical row could be rejected as unproven when a status arrived for it and
    accepted here as proof the chain had landed, which is enough to cancel a
    correct queued retry and lose a notification.

    The audit requirement applies to retry rows only. The ROOT is the original
    send, not a retry: it carries no such audit and must not be asked for one,
    so the early root check below stays exactly as it is.
    """
    orig_stmt = (
        select(OutboxMessage.id)
        .where(OutboxMessage.id == original_outbox_id)
        .where(OutboxMessage.status.in_(_DELIVERED_READ_STATUSES))
        .limit(1)
    )
    if (await session.execute(orig_stmt)).scalar_one_or_none() is not None:
        return True

    chain = await resolve_retry_chain_members(session, original_outbox_id)
    if chain.identity is None:
        # The root is not a provable chain root, so nothing can be a proven
        # member of it. Fail closed: the presend guard refuses this job anyway,
        # and claiming success here would cancel siblings on unproven grounds.
        return False
    if not chain.members:
        return False

    delivered_stmt = (
        select(OutboxMessage)
        .where(OutboxMessage.job_id.in_(chain.job_ids))
        .where(OutboxMessage.status.in_(_DELIVERED_READ_STATUSES))
    )
    delivered_rows = list((await session.execute(delivered_stmt)).scalars().all())
    if not delivered_rows:
        return False

    attempt_by_job_id = {int(member.job.id): member.reference.attempt_number for member in chain.members}
    for row in delivered_rows:
        if chain.identity.outbox_mismatch_field(row) is not None:
            continue
        attempt_number = attempt_by_job_id.get(int(row.job_id)) if row.job_id is not None else None
        if attempt_number is None:
            continue
        if retry_outbox_audit_mismatch(
            row,
            original_outbox_id=original_outbox_id,
            attempt_number=attempt_number,
        ):
            continue
        return True
    return False


async def _delivery_retry_presend_guard(
    session: AsyncSession,
    job: MessageJob,
    record: Record | None,
) -> str | None:
    """Re-prove a retry job's chain before it is allowed to send.

    Independent of the callback that created it, and on purpose: this guard has
    to hold for a row the callback never wrote — one left behind by an older
    buggy build, hand-inserted, or restored from a backup. A well-formed retry
    payload carrying ``provider='altegio'`` on an EasyWeek chain is exactly the
    shape that must not survive to a send, and the callback repair alone cannot
    see it.

    Runs before the template lookup, the sender routing, the rate limit and any
    Meta or text call.
    """
    reference = resolve_retry_reference(job)
    if reference.reference is None:
        return f"Canceled: {reference.error or 'delivery_retry_reference_unproven'}"
    original_outbox_id = reference.reference.original_outbox_id

    if await _delivery_retry_chain_has_success(session, original_outbox_id):
        return "Canceled: delivery retry chain already succeeded"

    original_outbox = await session.get(OutboxMessage, original_outbox_id)
    if original_outbox is None:
        return "Retry deadline exceeded or original outbox missing for delivery retry"

    original_job: MessageJob | None = None
    if original_outbox.job_id is not None:
        original_job = await session.get(MessageJob, original_outbox.job_id)

    resolution = await resolve_retry_identity(
        session,
        anchor_outbox=original_outbox,
        original_job=original_job,
        job_type=job.job_type,
    )
    if resolution.identity is None:
        return f"Canceled: delivery retry identity unproven ({resolution.error})"

    mismatch = resolution.identity.mismatch_field(job)
    if mismatch is not None:
        return f"Canceled: delivery retry {mismatch} does not match the proven chain identity"

    deadline = _retry_deadline_at(job, record, original_outbox=original_outbox)
    if deadline is not None and utcnow() > deadline:
        return f"Retry deadline exceeded for {job.job_type}"
    return None


async def _pause_for_closed_meta_circuit(session: AsyncSession, job: MessageJob, record: Record | None) -> bool:
    if not settings.meta_circuit_breaker_enabled:
        return False
    if not await meta_circuit.should_pause_meta_sends(session=session):
        return False

    requeued = _schedule_retry_or_cancel(
        job,
        record,
        settings.meta_circuit_pause_requeue_delay_seconds,
        "Meta circuit closed: send paused until Meta recovers",
    )
    logger.info(
        "Meta circuit closed; %s job_id=%s job_type=%s record_id=%s company_id=%s delay_seconds=%s",
        "requeued" if requeued else "canceled",
        job.id,
        job.job_type,
        job.record_id,
        job.company_id,
        settings.meta_circuit_pause_requeue_delay_seconds,
    )
    return True


async def _handle_transient_meta_error(
    job: MessageJob,
    record: Record | None,
    err: str,
) -> None:
    error_kind, error_code = _transient_error_reason(err)
    await meta_circuit.close_meta_circuit(
        reason="transient_send_error",
        error_kind=error_kind,
        error_code=error_code,
        next_probe_at=utcnow() + timedelta(seconds=settings.meta_circuit_probe_initial_delay_seconds),
    )
    requeued = _schedule_retry_or_cancel(
        job,
        record,
        settings.meta_circuit_pause_requeue_delay_seconds,
        "Meta circuit closed: send paused until Meta recovers",
    )
    logger.warning(
        "Transient Meta error closed circuit; %s job_id=%s job_type=%s record_id=%s "
        "company_id=%s error_kind=%s error_code=%s",
        "requeued" if requeued else "canceled",
        job.id,
        job.job_type,
        job.record_id,
        job.company_id,
        error_kind,
        error_code,
    )


def _get_24h_whitelist() -> frozenset[str]:
    """Return the set of job types eligible for text-inside-24h routing."""
    raw = settings.bot_template_text_inside_24h_job_types
    return frozenset(t.strip() for t in raw.split(",") if t.strip())


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


async def _marketing_suppression_reason(
    session: AsyncSession,
    phone: str,
    cooldown_days: int,
) -> str | None:
    """Stricter suppression for marketing/follow-up jobs.

    Returns a 'suppressed_131026' / 'suppressed_131049' reason string if the phone
    has ANY undeliverable/suppression history within ``cooldown_days``:
      * a 131026 or 131049 WhatsApp delivery-failure webhook on a bot message;
      * a previous canceled outbox row stamped suppressed_131026 / suppressed_131049.

    A single occurrence is enough (conservative for marketing). Best-effort: never
    raises (returns None on error) so a query problem cannot block the send
    pipeline — the transactional 131026 threshold guard and the live Altegio guard
    still apply.
    """
    try:
        window_start = utcnow() - timedelta(days=cooldown_days)
        codes: set[str] = set()

        fail_rows = await session.execute(
            text(
                "SELECT DISTINCT (we.payload #>> "
                "  '{entry,0,changes,0,value,statuses,0,errors,0,code}') AS code "
                "FROM outbox_messages om "
                "JOIN whatsapp_events we ON "
                "  we.payload #>> '{entry,0,changes,0,value,statuses,0,id}' "
                "    = om.provider_message_id "
                "WHERE om.phone_e164 = :phone "
                "  AND om.sent_at >= :ws "
                "  AND om.provider_message_id IS NOT NULL "
                "  AND om.message_source = 'bot' "
                "  AND we.payload #>> '{entry,0,changes,0,value,statuses,0,status}' "
                "      = 'failed' "
                "  AND (we.payload #>> '{entry,0,changes,0,value,statuses,0,errors,0,code}') "
                "      IN ('131026', '131049')"
            ),
            {"phone": phone, "ws": window_start},
        )
        for row in fail_rows:
            if row[0]:
                codes.add(str(row[0]))

        supp_rows = await session.execute(
            text(
                "SELECT meta ->> 'suppression_code' AS code, error "
                "FROM outbox_messages "
                "WHERE phone_e164 = :phone "
                "  AND status = 'canceled' "
                "  AND sent_at >= :ws "
                "  AND ( (meta ->> 'suppression_code') IN ('131026', '131049') "
                "        OR error LIKE 'suppressed_131026%' "
                "        OR error LIKE 'suppressed_131049%' )"
            ),
            {"phone": phone, "ws": window_start},
        )
        for code, error in supp_rows:
            blob = f"{code or ''} {error or ''}"
            if "131049" in blob:
                codes.add("131049")
            if "131026" in blob:
                codes.add("131026")

        if "131026" in codes:
            return "suppressed_131026: previous marketing undeliverable/suppression"
        if "131049" in codes:
            return "suppressed_131049: previous ecosystem engagement failure"
        return None
    except Exception as exc:  # pragma: no cover - defensive, never block the pipeline
        logger.warning("marketing suppression check failed phone=%s: %s", phone, exc)
        return None


async def _mark_followup_recipient_suppressed(
    session: AsyncSession,
    job: Any,
    status: str,
) -> None:
    """Persist a suppression followup_status on the follow-up recipient (Ops visibility)."""
    try:
        rid = (job.payload or {}).get("campaign_recipient_id")
        if rid is None:
            return
        recipient = await session.get(CampaignRecipient, int(rid))
        if recipient is not None:
            recipient.followup_status = status
    except Exception:  # pragma: no cover - best-effort
        return


async def _update_promo_lead_notification_meta(
    session: AsyncSession,
    promo_lead_id: Any,
    status: str,
    *,
    error: str | None = None,
    provider_message_id: str | None = None,
    now: datetime,
    job_id: int,
) -> None:
    """Best-effort update of PromoLead.meta notification status. Never raises."""
    if promo_lead_id is None:
        return
    try:
        lead = await session.get(PromoLead, int(promo_lead_id))
        if lead is None:
            logger.warning(
                "promo_discount_applied: PromoLead not found promo_lead_id=%s job_id=%s",
                promo_lead_id,
                job_id,
            )
            return
        meta = lead.meta or {}
        if status == "sent":
            lead.meta = {
                **meta,
                "customer_notification": "sent",
                "customer_notification_sent_at": now.isoformat(),
                "customer_notification_provider_message_id": provider_message_id,
            }
        elif status == "failed":
            update: dict[str, Any] = {
                **meta,
                "customer_notification": "failed",
                "customer_notification_failed_at": now.isoformat(),
            }
            if error is not None:
                update["customer_notification_error"] = error
            lead.meta = update
        elif status == "retrying":
            if error is not None:
                lead.meta = {**meta, "customer_notification_last_error": error}
    except Exception as exc:
        logger.warning(
            "promo_discount_applied: could not update PromoLead meta job_id=%s: %s",
            job_id,
            exc,
        )


_ID_RE = re.compile(r"^\d+$")


def _parse_int_payload_id(value: Any, field_name: str) -> tuple[int | None, str | None]:
    """Parse a job-payload field expected to be a positive integer id.

    Returns ``(int_value, None)`` on success.
    Returns ``(None, None)`` when *value* is ``None`` (field absent).
    Returns ``(None, error_str)`` when *value* is present but invalid.

    Accepted: positive ``int`` (not ``bool``), digit-only ``str`` (``'1'``,
    ``'42'``, ``'001'``).  Rejected: ``bool``, ``float``, empty / signed /
    decimal string, ``list``, ``dict``, ``0``, ``'0'``.
    """
    if value is None:
        return None, None

    if isinstance(value, bool):
        return None, f"Follow-up skipped: invalid {field_name}={value!r}"

    if isinstance(value, int):
        if value <= 0:
            return None, f"Follow-up skipped: invalid {field_name}={value!r}"
        return value, None

    if isinstance(value, str):
        if not _ID_RE.fullmatch(value):
            return None, f"Follow-up skipped: invalid {field_name}={value!r}"
        parsed = int(value)
        if parsed <= 0:
            return None, f"Follow-up skipped: invalid {field_name}={value!r}"
        return parsed, None

    return None, f"Follow-up skipped: invalid {field_name}={value!r}"


def _parse_positive_int_id(value: Any, field_name: str) -> tuple[int | None, str | None]:
    """Strict positive-int parser for any job-payload id field.

    Context-neutral variant of ``_parse_int_payload_id`` — same contract
    and same rejection rules, but with a generic error prefix suitable for
    handlers other than follow-up campaign jobs.

    Returns ``(int_value, None)`` on success.
    Returns ``(None, None)`` when *value* is ``None`` (field absent).
    Returns ``(None, error_str)`` when *value* is present but invalid.

    Accepted: positive ``int`` (not ``bool``), digit-only ``str``
    (e.g. ``'1'``, ``'42'``, ``'001'``).
    Rejected: ``bool``, ``float``, ``0``, negative int, ``''``,
    strings with whitespace / signs / decimal points, ``list``, ``dict``.

    Note: ``bool`` is checked *before* ``int`` because ``bool`` is a
    subclass of ``int`` in Python — ``isinstance(True, int)`` is ``True``.
    Without the explicit guard ``int(True) == 1``, allowing a boolean
    payload to silently resolve to id 1.
    """
    if value is None:
        return None, None

    if isinstance(value, bool):
        return None, f"invalid {field_name}={value!r}: bool not accepted as id"

    if isinstance(value, int):
        if value <= 0:
            return None, f"invalid {field_name}={value!r}: must be positive"
        return value, None

    if isinstance(value, str):
        if not _ID_RE.fullmatch(value):
            return None, f"invalid {field_name}={value!r}: must be digit-only string"
        parsed = int(value)
        if parsed <= 0:
            return None, f"invalid {field_name}={value!r}: must be positive"
        return parsed, None

    return None, f"invalid {field_name}={value!r}: unsupported type {type(value).__name__!r}"


async def _backfill_campaign_recipient_after_send(
    session: AsyncSession,
    job_type: str,
    job_id: int,
    payload: dict[str, Any],
    outbox_id: int,
    now_sent: datetime,
    provider_message_id: str | None = None,
) -> None:
    """Update CampaignRecipient tracking fields after a successful send."""
    campaign_recipient_id = payload.get("campaign_recipient_id")
    if campaign_recipient_id is None:
        return

    _rcid_int, _rcid_err = _parse_int_payload_id(campaign_recipient_id, "campaign_recipient_id")
    if _rcid_err is not None:
        logger.warning(
            "campaign backfill: %s job_id=%s — skipping",
            _rcid_err,
            job_id,
        )
        return
    recipient = await session.get(CampaignRecipient, _rcid_int)
    if recipient is None:
        logger.warning(
            "campaign backfill: recipient_id=%s not found job_id=%s — skipping",
            campaign_recipient_id,
            job_id,
        )
        return
    if job_type == FOLLOWUP_JOB_TYPE:
        recipient.followup_outbox_id = outbox_id
        recipient.followup_sent_at = now_sent
        if getattr(recipient, "followup_status", None) not in {"delivered", "read"}:
            recipient.followup_status = "sent"
    else:
        if recipient.outbox_message_id is None:
            recipient.outbox_message_id = outbox_id
        if recipient.provider_message_id is None and provider_message_id:
            recipient.provider_message_id = provider_message_id
        if recipient.sent_at is None:
            recipient.sent_at = now_sent


def _phone_digits(phone: str | None) -> str:
    """Return only digits from *phone* for normalized comparison."""
    if not phone:
        return ""
    return "".join(c for c in phone if c.isdigit())


_PROMO_REMINDER_BODY_TEMPLATE = (
    "Hallo 😊\n\n"
    "Ihre Sommer-Aktion ist aktiviert, aber wir sehen noch keinen passenden Termin für den Rabatt.\n\n"
    "Bitte buchen Sie rechtzeitig, damit Ihr Rabatt von {discount_amount}€ nicht verfällt.\n\n"
    "Gültig bis: {expires_at_display}\n"
    "Termin buchen: {booking_link}\n\n"
    "Liebe Grüße\n"
    "KitiLash\n\n"
    "Antworten Sie mit STOP, um keine Marketing-Nachrichten mehr zu erhalten."
)


def _render_promo_reminder_body(
    discount_amount: str,
    expires_at_display: str,
    booking_link: str,
) -> str:
    return _PROMO_REMINDER_BODY_TEMPLATE.format(
        discount_amount=discount_amount,
        expires_at_display=expires_at_display,
        booking_link=booking_link,
    )


async def _process_promo_card_booking_reminder(
    session: AsyncSession,
    job: MessageJob,
    provider: WhatsAppProvider,
) -> None:
    """Send promo card booking reminder via Meta WhatsApp template.

    Full eligibility re-check is performed before the send to cancel stale jobs.
    Includes 131026 suppression, normalized opt-out, and active-card validation.
    """
    from altegio_bot.meta_templates import PROMO_CARD_BOOKING_REMINDER_TEMPLATE
    from altegio_bot.workers.promo_lead_handler import _expires_display

    now = utcnow()
    payload = getattr(job, "payload", None) or {}

    # Fix 5 (strict): reject bool/float/whitespace-padded strings that
    # bare int() would silently accept (int(True)==1, int(1.5)==1, int(' 1 ')==1).
    raw_id = payload.get("promo_lead_id")
    promo_lead_id, _id_err = _parse_positive_int_id(raw_id, "promo_lead_id")
    if promo_lead_id is None:
        job.status = "failed"
        job.locked_at = None
        if _id_err is not None:
            job.last_error = f"promo_card_booking_reminder: invalid promo_lead_id={raw_id!r}"
        else:
            job.last_error = "promo_card_booking_reminder: missing promo_lead_id in payload"
        return

    lead = await session.get(PromoLead, promo_lead_id)
    if lead is None:
        job.status = "failed"
        job.locked_at = None
        job.last_error = f"promo_card_booking_reminder: PromoLead {promo_lead_id} not found"
        return

    # Fix 2: full eligibility re-check before send
    def _cancel(reason: str) -> None:
        job.status = "canceled"
        job.locked_at = None
        job.last_error = reason

    if lead.status != "issued":
        _cancel("promo_card_booking_reminder: lead no longer issued")
        logger.info("promo_card_booking_reminder: cancelling job_id=%s lead status=%s", job.id, lead.status)
        return

    if lead.issued_at is None:
        _cancel("promo_card_booking_reminder: lead no longer issued")
        return

    if lead.expires_at is None or lead.expires_at <= now:
        _cancel("promo_card_booking_reminder: promo lead expired")
        return

    if lead.applied_at is not None:
        _cancel("promo_card_booking_reminder: lead already applied")
        return

    if lead.used_at is not None:
        _cancel("promo_card_booking_reminder: lead already applied")
        return

    if lead.cancelled_at is not None:
        _cancel("promo_card_booking_reminder: lead no longer issued")
        return

    # Fix 2: active-card check
    lead_meta = lead.meta or {}
    if (
        not lead.loyalty_card_id
        or not lead.loyalty_card_number
        or not lead.location_id
        or not lead.discount_program_id
        or str(lead_meta.get("loyalty_card_issued", "")).lower() != "true"
        or str(lead_meta.get("card_issue_failed", "")).lower() == "true"
    ):
        _cancel("promo_card_booking_reminder: active loyalty card missing")
        return

    if lead_meta.get("booking_reminder_sent_at"):
        _cancel("promo_card_booking_reminder: booking reminder already sent")
        return

    if lead_meta.get("manual_review_required"):
        _cancel("promo_card_booking_reminder: manual review required")
        return

    phone = lead.phone_e164

    # Fix 4: normalized opt-out check — any Client row with matching digits
    phone_digits = _phone_digits(phone)
    opted_out_rows = (await session.execute(select(Client).where(Client.wa_opted_out.is_(True)))).scalars().all()
    if any(_phone_digits(c.phone_e164) == phone_digits for c in opted_out_rows):
        _cancel("promo_card_booking_reminder: phone opted out")
        logger.info("promo_card_booking_reminder: opted-out job_id=%s phone=%s", job.id, phone)
        return

    if await _pause_for_closed_meta_circuit(session, job, None):
        return

    delay_until = await _apply_rate_limit(session, phone)
    if delay_until is not None:
        job.status = "queued"
        job.locked_at = None
        job.run_at = delay_until
        return

    sender_id = await pick_sender_id(session=session, company_id=lead.company_id, sender_code="default")
    if sender_id is None:
        job.status = "failed"
        job.locked_at = None
        job.last_error = "promo_card_booking_reminder: no active sender for company"
        return

    # Fix 3: 131026 suppression
    if settings.wa_131026_suppression_enabled:
        n_fail = await _count_131026_failures(
            session,
            phone,
            settings.wa_131026_suppression_window_days,
        )
        if n_fail >= settings.wa_131026_suppression_threshold:
            _wd = settings.wa_131026_suppression_window_days
            reason = f"suppressed_131026: repeated undeliverable ({n_fail} in {_wd}d)"
            session.add(
                OutboxMessage(
                    company_id=lead.company_id,
                    client_id=None,
                    record_id=None,
                    job_id=job.id,
                    sender_id=None,
                    phone_e164=phone,
                    template_code=PROMO_CARD_BOOKING_REMINDER_JOB_TYPE,
                    language=TEMPLATE_LANGUAGE,
                    body="",
                    status="canceled",
                    error=reason,
                    provider_message_id=None,
                    scheduled_at=job.run_at,
                    sent_at=now,
                    meta={
                        "suppression_code": "131026",
                        "threshold": settings.wa_131026_suppression_threshold,
                        "window_days": _wd,
                        "matched_failures": n_fail,
                        "source": "promo_booking_reminder",
                        "promo_lead_id": promo_lead_id,
                    },
                    message_source="bot",
                )
            )
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

    template_name = PROMO_CARD_BOOKING_REMINDER_TEMPLATE
    booking_link = BOOKING_LINKS.get(lead.company_id, settings.promo_booking_url)
    # Fix 9: use promo funnel expiry display logic
    discount_amount = str(int(lead.discount_amount)) if lead.discount_amount else ""
    expires_at_display = _expires_display(lead.expires_at)
    template_params = [discount_amount, expires_at_display, booking_link]

    # Fix 7: rendered body for ops visibility
    body = _render_promo_reminder_body(discount_amount, expires_at_display, booking_link)

    preflight_err = validate_template_params(template_name, template_params)
    if preflight_err is not None:
        session.add(
            OutboxMessage(
                company_id=lead.company_id,
                client_id=None,
                record_id=None,
                job_id=job.id,
                sender_id=sender_id,
                phone_e164=phone,
                template_code=PROMO_CARD_BOOKING_REMINDER_JOB_TYPE,
                language=TEMPLATE_LANGUAGE,
                body=body,
                status="failed",
                error=preflight_err,
                provider_message_id=None,
                scheduled_at=job.run_at,
                sent_at=now,
                meta={
                    "send_type": "template",
                    "template": template_name,
                    "params": template_params,
                    "validation": "local_preflight_failure",
                    "source": "promo_booking_reminder",
                    "promo_lead_id": promo_lead_id,
                },
                message_source="bot",
            )
        )
        job.status = "failed"
        job.locked_at = None
        job.last_error = preflight_err
        return

    attempts = getattr(job, "attempts", 0) + 1
    setattr(job, "attempts", attempts)

    msg_id, err = await safe_send_template(
        provider=provider,
        sender_id=sender_id,
        phone=phone,
        template_name=template_name,
        language=TEMPLATE_LANGUAGE,
        params=template_params,
        tenant_provider=normalize_provider(getattr(job, "provider", None), default=PROVIDER_ALTEGIO),
        company_id=lead.company_id,
    )

    out_meta_base: dict[str, Any] = {
        "send_type": "template",
        "template": template_name,
        "params": template_params,
        "source": "promo_booking_reminder",
        "promo_lead_id": promo_lead_id,
    }

    if err is not None:
        if settings.meta_circuit_breaker_enabled and _is_transient_provider_error(err):
            _decrement_send_attempt(job)
            await _handle_transient_meta_error(job, None, err)
            return

        session.add(
            OutboxMessage(
                company_id=lead.company_id,
                client_id=None,
                record_id=None,
                job_id=job.id,
                sender_id=sender_id,
                phone_e164=phone,
                template_code=PROMO_CARD_BOOKING_REMINDER_JOB_TYPE,
                language=TEMPLATE_LANGUAGE,
                body=body,
                status="failed",
                error=err,
                provider_message_id=msg_id,
                scheduled_at=job.run_at,
                sent_at=now,
                meta=out_meta_base,
                message_source="bot",
            )
        )
        max_attempts = getattr(job, "max_attempts", 5)
        if _is_token_expired_error(err):
            _mark_token_expired()
            job.status = "queued"
            job.locked_at = None
            job.run_at = now + timedelta(seconds=TOKEN_EXPIRED_RETRY_SECONDS)
            job.last_error = f"Send blocked: {err}"
        elif _is_permanent_meta_template_error(err) or attempts >= max_attempts:
            job.status = "failed"
            job.locked_at = None
            job.last_error = f"Send failed: {err}"
        else:
            job.status = "queued"
            job.locked_at = None
            job.run_at = now + timedelta(seconds=_retry_delay_seconds(attempts))
            job.last_error = f"Send failed: {err}"
        return

    out = OutboxMessage(
        company_id=lead.company_id,
        client_id=None,
        record_id=None,
        job_id=job.id,
        sender_id=sender_id,
        phone_e164=phone,
        template_code=PROMO_CARD_BOOKING_REMINDER_JOB_TYPE,
        language=TEMPLATE_LANGUAGE,
        body=body,
        status="sent",
        error=None,
        provider_message_id=msg_id,
        scheduled_at=job.run_at,
        sent_at=now,
        meta=out_meta_base,
        message_source="bot",
    )
    session.add(out)
    await session.flush()

    # Fix 8: rich meta on success
    lead.meta = {
        **lead_meta,
        "booking_reminder_sent_at": now.isoformat(),
        "booking_reminder_outbox_id": out.id,
        "booking_reminder_provider_message_id": msg_id,
        "booking_reminder_template": template_name,
    }
    job.status = "done"
    job.locked_at = None
    job.last_error = None
    logger.info(
        "promo_card_booking_reminder: sent job_id=%s phone=%s outbox_id=%s",
        job.id,
        phone,
        out.id,
    )


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
    with perf_log("outbox_worker", "outbox.load_job", job_id=job_id) as _lj_ctx:
        job = await _load_job(session, job_id)
        if job is not None:
            _lj_ctx.update(job_type=job.job_type, company_id=job.company_id)
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
    # The CRM this job belongs to. Read FIRST — before routing, before any row is
    # loaded, before any external call — and threaded through template loading,
    # sender routing and Meta-name resolution: every place where an Altegio and an
    # EasyWeek row could otherwise collide on a numeric company_id. The normalized
    # read keeps hand-built test jobs and any legacy row without the column on the
    # Altegio path, exactly as before.
    job_provider = normalize_provider(getattr(job, "provider", None), default=PROVIDER_ALTEGIO)

    # Retry syntax/reference/type is a routing boundary. It runs before campaign
    # and promo dispatch so a malformed or legacy row cannot be interpreted as
    # a different local command, much less reach an API or Meta. Full
    # identity/domain/deadline proof remains immediately before the send path.
    if claims_delivery_retry(job):
        reference = resolve_retry_reference(job)
        if reference.reference is None:
            job.status = "canceled"
            job.locked_at = None
            job.updated_at = utcnow()
            job.last_error = f"Canceled: {reference.error or 'delivery_retry_reference_unproven'}"
            return None

    # Phase-1 allowlist, checked before ANYTHING else acts on this job.
    #
    # It has to be here rather than deeper down: the campaign branch below
    # requeues, the promo branches hand the job to their own handlers, and the
    # marketing paths call the live Altegio API — all of which would have already
    # happened by the time a later guard ran. An EasyWeek `reminder_24h` has no
    # template, no Altegio client id and no reason to exist yet; the only correct
    # outcome is a deterministic terminal failure that nobody retries.
    job_type_err = easyweek_job_type_error(job_provider, job.job_type)
    if job_type_err is not None:
        job.status = "failed"
        job.locked_at = None
        job.last_error = job_type_err
        logger.error(
            "EasyWeek job type not enabled: job_id=%s company=%s job_type=%s",
            job.id,
            job.company_id,
            job.job_type,
        )
        return None

    # EasyWeek registry ownership. Deliberately HERE: after the provider is
    # known and the phase-1 allowlist has run, but before the template, Record,
    # Client, sender, rate limit, render and every external call. The registry
    # is the tenant boundary, so membership has to be proven before anything
    # acts on the job — a branch removed from `EASYWEEK_LOCATION_MAP` must not
    # keep sending just because its jobs were queued while it was still there.
    #
    # Failing locally and terminally is the point: a retry cannot fix a
    # configuration that no longer claims this company.
    if job_provider == PROVIDER_EASYWEEK:
        ownership_err = easyweek_job_ownership_error(job.company_id)
        if ownership_err is not None:
            job.status = "failed"
            job.locked_at = None
            job.last_error = ownership_err
            logger.error(
                "EasyWeek job company is not owned by the configured registry: job_id=%s company=%s job_type=%s",
                job.id,
                job.company_id,
                job.job_type,
            )
            return None

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

    if job.job_type == PROMO_ELIGIBILITY_CHECK_JOB_TYPE:
        await process_promo_eligibility_check_job(session, job, provider)
        return None

    if job.job_type == PROMO_APPLY_EXISTING_BOOKING_JOB_TYPE:
        await process_promo_apply_existing_booking_job(session, job)
        return None

    if job.job_type == PROMO_CARD_BOOKING_REMINDER_JOB_TYPE:
        await _process_promo_card_booking_reminder(session, job, provider)
        return None

    with perf_log(
        "outbox_worker",
        "outbox.load_record",
        job_id=job.id,
        job_type=job.job_type,
        company_id=job.company_id,
    ):
        record = await _load_record(session, job)
    if _record_is_in_past(record, job_type=job.job_type):
        job.status = "canceled"
        job.locked_at = None
        job.last_error = "Skipped: record starts_at is in the past"
        return

    if job.job_type in ("reminder_24h", "reminder_2h"):
        _stale, _stale_err = _check_reminder_stale(job, record)
        if _stale:
            job.status = "canceled"
            job.locked_at = None
            job.last_error = _stale_err
            return

    if _deadline_passed_for_send(job, record):
        job.status = "canceled"
        job.locked_at = None
        job.updated_at = utcnow()
        job.last_error = f"Retry deadline exceeded for {job.job_type}"
        return

    if claims_delivery_retry(job):
        guard_reason = await _delivery_retry_presend_guard(session, job, record)
        if guard_reason is not None:
            job.status = "canceled"
            job.locked_at = None
            job.updated_at = utcnow()
            job.last_error = guard_reason
            return

    with perf_log(
        "outbox_worker",
        "outbox.load_client",
        job_id=job.id,
        job_type=job.job_type,
        company_id=job.company_id,
    ):
        client = await _load_client(session, job, record)

    # Everything after this point — the phone, the body, the params, the Meta
    # call — treats `record` and `client` as belonging to this job. For EasyWeek
    # that has to be PROVEN, not assumed, and proven here: this is the last
    # point at which no customer-facing value has been built yet.
    if job_provider == PROVIDER_EASYWEEK and job.job_type in EASYWEEK_LIFECYCLE_JOB_TYPES:
        scope_err = _easyweek_domain_scope_error(job, record, client, provider=job_provider)
        if scope_err is not None:
            job.status = "failed"
            job.locked_at = None
            job.last_error = f"EasyWeek domain scope violation: {scope_err}"
            logger.error(
                "EasyWeek domain scope violation: %s job_id=%s company=%s code=%s",
                scope_err,
                job.id,
                job.company_id,
                job.job_type,
            )
            return None

    # Follow-up final eligibility guard (DB phase): re-check current recipient/client state
    # before the actual send.  Catches changes that happened during the 14-day delay between
    # job creation and delivery (read, booking, opt-out).
    # NOTE: the live Altegio future-record check runs AFTER the 131026 suppression guard so
    # that a locally known-undeliverable phone short-circuits before we hit an external API.
    _fu_recipient: CampaignRecipient | None = None
    _fu_recipient_id: int | None = None
    _fu_altegio_cid: int | None = None

    if job.job_type == FOLLOWUP_JOB_TYPE:
        _fu_payload = getattr(job, "payload", None) or {}
        _fu_recipient_id = _fu_payload.get("campaign_recipient_id")
        _fu_run_id = _fu_payload.get("campaign_run_id")

        if _fu_recipient_id is None:
            job.status = "canceled"
            job.locked_at = None
            job.last_error = "Follow-up skipped: missing campaign_recipient_id"
            logger.warning(
                "followup job without campaign_recipient_id job_id=%s — canceled (fail-closed)",
                job.id,
            )
            return None

        _fu_recipient_id_int, _fu_rid_err = _parse_int_payload_id(_fu_recipient_id, "campaign_recipient_id")
        if _fu_rid_err is not None:
            job.status = "canceled"
            job.locked_at = None
            job.last_error = _fu_rid_err
            logger.warning(
                "followup job: %s job_id=%s — canceled (fail-closed)",
                _fu_rid_err,
                job.id,
            )
            return None

        _fu_recipient = await session.get(CampaignRecipient, _fu_recipient_id_int)
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

        _fu_run_id_int, _fu_ruid_err = _parse_int_payload_id(_fu_run_id, "campaign_run_id")
        if _fu_ruid_err is not None:
            job.status = "canceled"
            job.locked_at = None
            job.last_error = _fu_ruid_err
            logger.warning(
                "followup job: %s job_id=%s — canceled (fail-closed)",
                _fu_ruid_err,
                job.id,
            )
            return None

        _fu_run = await session.get(CampaignRun, _fu_run_id_int)
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

        # Resolve altegio_client_id (recipient row first, then loaded client row as fallback).
        # The actual live API call happens after the 131026 suppression check below.
        _fu_altegio_cid = getattr(_fu_recipient, "altegio_client_id", None)
        if _fu_altegio_cid is None and client is not None:
            _fu_altegio_cid = getattr(client, "altegio_client_id", None)

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
                    **delivery_retry_audit(job),
                },
            )
            session.add(out)
            job.status = "canceled"
            job.locked_at = None
            job.last_error = reason
            # Keep follow-up recipient terminal state consistent with the marketing
            # suppression branch below: a follow-up canceled here must become
            # suppressed_131026, not stay followup_queued (Ops/reports/repair rely
            # on the terminal followup_status).
            if job.job_type == FOLLOWUP_JOB_TYPE:
                await _mark_followup_recipient_suppressed(session, job, "suppressed_131026")
            logger.info(
                "Suppressed 131026 job_id=%s phone=%s failures=%d window=%dd",
                job.id,
                phone,
                n_fail,
                _wd,
            )
            return

    # Stricter marketing suppression (broader than the 131026 threshold above):
    # ANY prior 131026/131049 failure OR prior suppressed_* row within the longer
    # marketing cooldown blocks marketing/follow-up sends. Scoped to
    # MARKETING_JOB_TYPES — transactional reminders keep the threshold rule only.
    if settings.marketing_suppression_enabled and job.job_type in MARKETING_JOB_TYPES:
        supp_reason = await _marketing_suppression_reason(
            session,
            phone,
            settings.marketing_suppression_cooldown_days,
        )
        if supp_reason:
            supp_code = "131049" if "131049" in supp_reason else "131026"
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
                error=supp_reason,
                provider_message_id=None,
                scheduled_at=job.run_at,
                sent_at=utcnow(),
                meta={
                    "suppression_code": supp_code,
                    "marketing_suppression": True,
                    "cooldown_days": settings.marketing_suppression_cooldown_days,
                    **delivery_retry_audit(job),
                },
            )
            session.add(out)
            job.status = "canceled"
            job.locked_at = None
            job.last_error = supp_reason
            if job.job_type == FOLLOWUP_JOB_TYPE:
                await _mark_followup_recipient_suppressed(session, job, f"suppressed_{supp_code}")
            logger.info(
                "Marketing-suppressed job_id=%s phone=%s code=%s cooldown=%dd",
                job.id,
                phone,
                supp_code,
                settings.marketing_suppression_cooldown_days,
            )
            return

    # Live Altegio guard (follow-up only): check for any non-deleted future record.
    # Runs here — AFTER the 131026 suppression guard — so a locally known-undeliverable
    # phone short-circuits before we call an external API.
    # Only reached when the DB guard passed (recipient eligible) and 131026 did not fire.
    if job.job_type == FOLLOWUP_JOB_TYPE:
        if _fu_altegio_cid is None:
            # Cannot perform the live future-record check without an Altegio client id.
            # Fail permanently rather than silently skipping the guard (fail-closed).
            job.status = "failed"
            job.locked_at = None
            job.last_error = "Follow-up failed: missing Altegio client id for live future-record check"
            _fu_recipient.followup_status = "followup_failed"
            # Keep followup_message_job_id pointing at this job for audit trail.
            logger.error(
                "followup live guard: no altegio_client_id job_id=%s recipient_id=%s — failing job",
                job.id,
                _fu_recipient_id,
            )
            return None

        try:
            _fu_has_future = await client_has_any_future_record(
                company_id=job.company_id,
                altegio_client_id=int(_fu_altegio_cid),
            )
        except Exception as exc:
            logger.warning(
                "followup live guard: Altegio API failed job_id=%s altegio_client_id=%s attempt=%d: %s",
                job.id,
                _fu_altegio_cid,
                _get_followup_live_guard_attempts(job) + 1,
                exc,
            )
            _handle_followup_live_guard_error(job, exc)
            return None

        if _fu_has_future:
            _fu_recipient.followup_status = "skipped_future_record"
            _fu_recipient.followup_message_job_id = None
            job.status = "canceled"
            job.locked_at = None
            job.last_error = "Follow-up skipped: future Altegio record exists"
            logger.info(
                "followup live guard: future record found job_id=%s recipient_id=%s altegio_client_id=%s",
                job.id,
                _fu_recipient_id,
                _fu_altegio_cid,
            )
            return None

    if await _pause_for_closed_meta_circuit(session, job, record):
        return None

    with perf_log(
        "outbox_worker",
        "outbox.apply_rate_limit",
        job_id=job.id,
        job_type=job.job_type,
        company_id=job.company_id,
        phone_e164=phone,
    ) as _rl_ctx:
        delay_until = await _apply_rate_limit(session, phone)
        _rl_ctx.update(delayed=delay_until is not None)
        if delay_until is not None:
            _rl_ctx.update(next_allowed_at=delay_until.isoformat())
    if delay_until is not None:
        job.status = "queued"
        job.locked_at = None
        job.run_at = delay_until
        return

    # ── promo_discount_applied: free-form text, no MessageTemplate or Meta template ──
    if job.job_type == "promo_discount_applied":
        _pd_payload = getattr(job, "payload", None) or {}
        _pd_promo_lead_id = _pd_payload.get("promo_lead_id")  # extracted early for reconciliation
        _pd_body = _pd_payload.get("body", "")
        if not _pd_body:
            job.status = "failed"
            job.locked_at = None
            job.last_error = "promo_discount_applied: missing body in payload"
            _pd_now = utcnow()
            await _update_promo_lead_notification_meta(
                session,
                _pd_promo_lead_id,
                "failed",
                error=job.last_error,
                now=_pd_now,
                job_id=job.id,
            )
            return None
        _pd_attempts = getattr(job, "attempts", 0) + 1
        setattr(job, "attempts", _pd_attempts)
        _pd_sender_id = await pick_sender_id(
            session=session,
            company_id=job.company_id,
            sender_code="default",
        )
        if _pd_sender_id is None:
            job.status = "failed"
            job.locked_at = None
            job.last_error = "promo_discount_applied: no active sender for company"
            _pd_now = utcnow()
            await _update_promo_lead_notification_meta(
                session,
                _pd_promo_lead_id,
                "failed",
                error=job.last_error,
                now=_pd_now,
                job_id=job.id,
            )
            return None

        contact_name = client.display_name if client else None
        with perf_log(
            "outbox_worker",
            "outbox.meta_send",
            job_id=job.id,
            job_type=job.job_type,
            company_id=job.company_id,
            sender_id=_pd_sender_id,
            phone_e164=phone,
            template_code=job.job_type,
            send_mode="text",
        ) as _pd_ms_ctx:
            msg_id, err = await safe_send(
                provider=provider,
                sender_id=_pd_sender_id,
                phone=phone,
                text=_pd_body,
                contact_name=contact_name,
                tenant_provider=job_provider,
                company_id=job.company_id,
            )
            _pd_ms_ctx.update(provider_message_id=msg_id)
            if err is not None:
                _pd_ms_ctx.update(send_error=err)
        _pd_now = utcnow()
        _pd_send_meta: dict[str, Any] = {"send_type": "text", **delivery_retry_audit(job)}

        if err is not None:
            if settings.meta_circuit_breaker_enabled and _is_transient_provider_error(err):
                _decrement_send_attempt(job)
                await _handle_transient_meta_error(job, record, err)
                await _update_promo_lead_notification_meta(
                    session,
                    _pd_promo_lead_id,
                    "retrying",
                    error="Meta circuit closed: send paused until Meta recovers",
                    now=_pd_now,
                    job_id=job.id,
                )
                return None

            with perf_log(
                "outbox_worker",
                "outbox.insert_outbox",
                job_id=job.id,
                job_type=job.job_type,
                company_id=job.company_id,
                phone_e164=phone,
                template_code=job.job_type,
                outbox_status="failed",
            ):
                session.add(
                    OutboxMessage(
                        company_id=job.company_id,
                        client_id=(client.id if client else None),
                        record_id=(record.id if record else None),
                        job_id=job.id,
                        sender_id=_pd_sender_id,
                        phone_e164=phone,
                        template_code=job.job_type,
                        language="de",
                        body=_pd_body,
                        status="failed",
                        error=err,
                        provider_message_id=msg_id,
                        scheduled_at=job.run_at,
                        sent_at=_pd_now,
                        meta=_pd_send_meta,
                    )
                )
            if _is_token_expired_error(err):
                _mark_token_expired()
                job.status = "queued"
                job.locked_at = None
                job.run_at = _pd_now + timedelta(seconds=TOKEN_EXPIRED_RETRY_SECONDS)
                job.last_error = f"Send blocked: {err}"
            elif _pd_attempts >= max_attempts:
                job.status = "failed"
                job.locked_at = None
                job.last_error = f"Send failed: {err}"
                await _update_promo_lead_notification_meta(
                    session,
                    _pd_promo_lead_id,
                    "failed",
                    error=err,
                    now=_pd_now,
                    job_id=job.id,
                )
            else:
                job.last_error = f"Send failed: {err}"
                job.status = "queued"
                job.locked_at = None
                job.run_at = _pd_now + timedelta(seconds=_retry_delay_seconds(_pd_attempts))
                await _update_promo_lead_notification_meta(
                    session,
                    _pd_promo_lead_id,
                    "retrying",
                    error=err,
                    now=_pd_now,
                    job_id=job.id,
                )
            return None

        with perf_log(
            "outbox_worker",
            "outbox.insert_outbox",
            job_id=job.id,
            job_type=job.job_type,
            company_id=job.company_id,
            phone_e164=phone,
            template_code=job.job_type,
            outbox_status="sent",
        ):
            out = OutboxMessage(
                company_id=job.company_id,
                client_id=(client.id if client else None),
                record_id=(record.id if record else None),
                job_id=job.id,
                sender_id=_pd_sender_id,
                phone_e164=phone,
                template_code=job.job_type,
                language="de",
                body=_pd_body,
                status="sent",
                error=None,
                provider_message_id=msg_id,
                scheduled_at=job.run_at,
                sent_at=_pd_now,
                meta=_pd_send_meta,
            )
            session.add(out)
        job.status = "done"
        job.locked_at = None
        job.last_error = None
        logger.info("promo_discount_applied: sent job_id=%s phone=%s", job.id, phone)

        await _update_promo_lead_notification_meta(
            session,
            _pd_promo_lead_id,
            "sent",
            provider_message_id=msg_id,
            now=_pd_now,
            job_id=job.id,
        )
        return None

    try:
        with perf_log(
            "outbox_worker",
            "outbox.render_message",
            job_id=job.id,
            job_type=job.job_type,
            company_id=job.company_id,
            template_code=job.job_type,
            record_id=getattr(record, "id", None),
            client_id=getattr(client, "id", None),
        ):
            body, sender_id, lang, msg_ctx = await _render_message(
                session=session,
                company_id=job.company_id,
                template_code=job.job_type,
                record=record,
                client=client,
                provider=job_provider,
            )
    except Exception as exc:
        job.status = "failed"
        job.locked_at = None
        job.last_error = f"Template render error: {exc}"
        return

    # Validate the DB row selected by `_render_message` for every send mode.
    # Text sends carry the rendered body rather than the Meta template name, so
    # allowing them to skip this proof would still let a Rastatt row containing
    # Durlach content reach the customer. Delivery retries enter this same path.
    if job_provider == PROVIDER_EASYWEEK:
        _location, profile, ownership_error = _easyweek_owned_branch(job.company_id)
        if ownership_error is not None or profile is None:
            job.status = "failed"
            job.locked_at = None
            job.last_error = ownership_error or "EasyWeek branch ownership could not be proven."
            logger.error(
                "EasyWeek branch ownership changed during render; failing job_id=%s company=%s",
                job.id,
                job.company_id,
            )
            return

        selected_name = (msg_ctx.get("meta_template_name") or "").strip()
        selected_code = str(msg_ctx.get("easyweek_template_code") or job.job_type)
        branch_error = branch_template_contract_error(
            profile=profile,
            template_code=selected_code,
            resolved_name=selected_name,
            resolved_body=body,
        )
        if branch_error is not None:
            job.status = "failed"
            job.locked_at = None
            job.last_error = branch_error
            logger.error(
                "EasyWeek template does not belong to the job's branch; failing job_id=%s company=%s branch=%s",
                job.id,
                job.company_id,
                profile.slug,
            )
            return

    # The language Meta is actually told the template is in.
    #
    # For Altegio this stays the global TEMPLATE_LANGUAGE: every approved
    # kitilash_* template is registered in `de`, and `lang` may legitimately
    # differ from it after a same-company fallback without meaning the Meta
    # template changed. Nothing about that policy moves here.
    #
    # For EasyWeek it must be the language of the row `_load_template` actually
    # returned. `body` and `meta_template_name` already come from that row; if
    # the request said `de` while the row was `en`, Meta rejects the send —
    # after the request is spent — for a mismatch we could see locally.
    _is_easyweek_lifecycle = job_provider == PROVIDER_EASYWEEK and job.job_type in EASYWEEK_LIFECYCLE_JOB_TYPES
    effective_template_language = TEMPLATE_LANGUAGE
    if _is_easyweek_lifecycle:
        effective_template_language = (lang or "").strip()
        if not effective_template_language:
            job.status = "failed"
            job.locked_at = None
            job.last_error = f"EasyWeek template has no language: company={job.company_id} code={job.job_type}"
            logger.error(
                "EasyWeek template row has a blank language; failing job_id=%s company=%s code=%s",
                job.id,
                job.company_id,
                job.job_type,
            )
            return

    # The language written to every OutboxMessage row for this job.
    #
    # For EasyWeek it is the SAME normalized string Meta was handed, so the audit
    # trail cannot disagree with what was actually sent — a `lang` of `" de "`
    # would otherwise be stored raw next to a `de` request, and a later
    # investigation would be comparing two different-looking values.
    #
    # Altegio keeps storing `lang` (which may be a same-company fallback
    # language) while sending TEMPLATE_LANGUAGE. That divergence is deliberate
    # and pre-existing: the row records which template TEXT was used, the request
    # records how the Meta template is registered.
    outbox_language = effective_template_language if _is_easyweek_lifecycle else lang

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
        if job_provider == PROVIDER_EASYWEEK:
            # DB-first: the name sent to Meta is ONLY the value from the row
            # rendered above, never a freshly derived replacement. The branch
            # guard before this block has already proved that DB value equals
            # the source-controlled name approved for the selected branch/code.
            meta_template_name = (msg_ctx.get("meta_template_name") or "").strip() or None
            if meta_template_name is None:
                job.status = "failed"
                job.locked_at = None
                job.last_error = (
                    f"No meta_template_name on EasyWeek template: company={job.company_id} code={job.job_type}"
                )
                logger.error(
                    "EasyWeek template has no meta_template_name; failing job_id=%s company=%s code=%s",
                    job.id,
                    job.company_id,
                    job.job_type,
                )
                return

            template_params = build_lifecycle_template_params(job.job_type, msg_ctx)
        else:
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

        # Keyed by CODE for EasyWeek: its Meta name is unknown to the Python
        # rules, so a name-keyed check would fall through to the generic
        # "non-empty" path and let a wrong-arity param list reach Meta.
        if job_provider == PROVIDER_EASYWEEK:
            preflight_err = validate_lifecycle_template_params(job.job_type, template_params)
        else:
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
                language=outbox_language,
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
                    "lang": effective_template_language,
                    "validation": "local_preflight_failure",
                    **delivery_retry_audit(job),
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

    # ── Bot text-inside-24h routing ───────────────────────────────────────────
    # Decision is made here at send time (not at job-creation time) so that a
    # window that opened after the job was planned is always honoured.
    _24h_eligible = (
        settings.bot_template_text_inside_24h_enabled
        and use_template
        and job.job_type in _get_24h_whitelist()
        and bool(final_body.strip())
    )
    _wa_window_open: bool | None = None
    _last_inbound_at: datetime | None = None
    _wa_window_check_error: str | None = None
    _text_send_error: str | None = None
    _use_template_fallback = False

    if _24h_eligible:
        # Resolve sender phone_number_id so we only count inbound events from
        # the same WhatsApp sender number, preventing a false-open from a
        # customer message delivered to a different WA number in the same system.
        _sender_phone_number_id: str | None = None
        try:
            _sender_obj = await session.get(WhatsAppSender, sender_id)
            if _sender_obj is not None:
                _sender_phone_number_id = _sender_obj.phone_number_id
            else:
                logger.warning(
                    "wa_window_check: WhatsAppSender id=%s not found, skipping 24h text routing job_id=%s",
                    sender_id,
                    job.id,
                )
                _wa_window_open = False
                _wa_window_check_error = f"sender_not_found:id={sender_id}"
        except Exception as exc:
            logger.warning(
                "wa_window_check: sender lookup failed job_id=%s sender_id=%s: %s",
                job.id,
                sender_id,
                exc,
            )
            _wa_window_open = False
            _wa_window_check_error = f"sender_lookup_failed:{exc}"

        if _wa_window_open is None:
            # Sender loaded OK — perform the actual window check.
            # Wrapped in try/except: this feature is an optimization and must
            # never block critical appointment notifications.  On any failure,
            # fail open to the legacy template path.
            try:
                with perf_log(
                    "outbox_worker",
                    "outbox.wa_window_check",
                    job_id=job.id,
                    job_type=job.job_type,
                    company_id=job.company_id,
                    phone_e164=phone,
                ) as _wc_ctx:
                    _wa_window_open, _last_inbound_at = await is_whatsapp_customer_window_open(
                        session=session,
                        phone_e164=phone,
                        now=utcnow(),
                        phone_number_id=_sender_phone_number_id,
                    )
                    _wc_ctx.update(window_open=_wa_window_open)
                logger.info(
                    "wa_window_check job_id=%s job_type=%s company_id=%s "
                    "phone_e164=%s window_open=%s last_inbound_at=%s",
                    job.id,
                    job.job_type,
                    job.company_id,
                    phone,
                    _wa_window_open,
                    _last_inbound_at.isoformat() if _last_inbound_at else None,
                )
            except Exception as exc:
                logger.warning(
                    "wa_window_check: failed, falling back to template "
                    "job_id=%s job_type=%s company_id=%s phone_e164=%s: %s",
                    job.id,
                    job.job_type,
                    job.company_id,
                    phone,
                    exc,
                )
                _wa_window_open = False
                _last_inbound_at = None
                _wa_window_check_error = str(exc)

        if _wa_window_open:
            with perf_log(
                "outbox_worker",
                "outbox.text_inside_24h_send",
                job_id=job.id,
                job_type=job.job_type,
                company_id=job.company_id,
                sender_id=sender_id,
                phone_e164=phone,
            ) as _ts_ctx:
                _text_msg_id, _text_err = await safe_send(
                    provider=provider,
                    sender_id=sender_id,
                    phone=phone,
                    text=final_body,
                    contact_name=contact_name,
                    tenant_provider=job_provider,
                    company_id=job.company_id,
                )
                _ts_ctx.update(provider_message_id=_text_msg_id)
                if _text_err is not None:
                    _ts_ctx.update(send_error=_text_err)

            if _text_err is None:
                # Text send succeeded — record and return early.
                logger.info(
                    "text_inside_24h: sent as text job_id=%s job_type=%s "
                    "company_id=%s phone_e164=%s provider_message_id=%s",
                    job.id,
                    job.job_type,
                    job.company_id,
                    phone,
                    _text_msg_id,
                )
                _now_sent = utcnow()
                _last_inbound_iso = _last_inbound_at.isoformat() if _last_inbound_at else None
                with perf_log(
                    "outbox_worker",
                    "outbox.insert_outbox",
                    job_id=job.id,
                    job_type=job.job_type,
                    company_id=job.company_id,
                    phone_e164=phone,
                    template_code=job.job_type,
                    outbox_status="sent",
                ):
                    _text_out = OutboxMessage(
                        company_id=job.company_id,
                        client_id=(client.id if client else None),
                        record_id=(record.id if record else None),
                        job_id=job.id,
                        sender_id=sender_id,
                        phone_e164=phone,
                        template_code=job.job_type,
                        language=outbox_language,
                        body=final_body,
                        status="sent",
                        error=None,
                        provider_message_id=_text_msg_id,
                        scheduled_at=job.run_at,
                        sent_at=_now_sent,
                        meta={
                            "send_type": "text",
                            "original_send_type": "template",
                            "text_inside_24h": True,
                            "text_inside_24h_eligible": True,
                            "original_template": meta_template_name,
                            "original_template_language": effective_template_language,
                            "original_template_params": template_params,
                            "wa_window_open": True,
                            "last_meta_inbound_at": _last_inbound_iso,
                            "route_reason": "customer_service_window_open",
                            **delivery_retry_audit(job),
                        },
                    )
                    session.add(_text_out)
                if _job_payload.get("campaign_recipient_id") is not None:
                    await session.flush()
                await _backfill_campaign_recipient_after_send(
                    session=session,
                    job_type=job.job_type,
                    job_id=job.id,
                    payload=_job_payload,
                    outbox_id=_text_out.id,
                    now_sent=_now_sent,
                    provider_message_id=_text_msg_id,
                )
                job.status = "done"
                job.locked_at = None
                job.last_error = None
                _campaign_run_id = _job_payload.get("campaign_run_id")
                return int(_campaign_run_id) if _campaign_run_id is not None else None

            # Text send failed — decide whether to fall back or preserve retry behaviour.
            _text_send_error = _text_err
            if settings.bot_template_text_inside_24h_fallback_enabled and _is_text_window_policy_error(_text_err):
                # Deterministic policy error: fall back to template send below.
                _use_template_fallback = True
                logger.info(
                    "text_inside_24h: policy error, falling back to template job_id=%s job_type=%s err=%r",
                    job.id,
                    job.job_type,
                    _text_err,
                )
            else:
                # Ambiguous error (timeout, 5xx, unknown): do NOT send template —
                # the text may have been accepted but the response was lost.
                logger.warning(
                    "text_inside_24h: ambiguous error, no automatic fallback job_id=%s job_type=%s err=%r",
                    job.id,
                    job.job_type,
                    _text_err,
                )
                if settings.meta_circuit_breaker_enabled and _is_transient_provider_error(_text_err):
                    _decrement_send_attempt(job)
                    await _handle_transient_meta_error(job, record, _text_err)
                    return None

                _last_inbound_iso = _last_inbound_at.isoformat() if _last_inbound_at else None
                with perf_log(
                    "outbox_worker",
                    "outbox.insert_outbox",
                    job_id=job.id,
                    job_type=job.job_type,
                    company_id=job.company_id,
                    phone_e164=phone,
                    template_code=job.job_type,
                    outbox_status="failed",
                ):
                    session.add(
                        OutboxMessage(
                            company_id=job.company_id,
                            client_id=(client.id if client else None),
                            record_id=(record.id if record else None),
                            job_id=job.id,
                            sender_id=sender_id,
                            phone_e164=phone,
                            template_code=job.job_type,
                            language=outbox_language,
                            body=final_body,
                            status="failed",
                            error=_text_err,
                            provider_message_id=None,
                            scheduled_at=job.run_at,
                            sent_at=utcnow(),
                            meta={
                                "send_type": "text",
                                "text_inside_24h": True,
                                "wa_window_open": True,
                                "last_meta_inbound_at": _last_inbound_iso,
                                "route_reason": "customer_service_window_open",
                                **delivery_retry_audit(job),
                            },
                        )
                    )
                if _is_token_expired_error(_text_err):
                    _mark_token_expired()
                    job.status = "queued"
                    job.locked_at = None
                    job.run_at = utcnow() + timedelta(seconds=TOKEN_EXPIRED_RETRY_SECONDS)
                    job.last_error = f"Send blocked: {_text_err}"
                    return None
                job.last_error = f"Send failed: {_text_err}"
                if attempts >= max_attempts:
                    job.status = "failed"
                    job.locked_at = None
                    return None
                delay = _retry_delay_seconds(attempts)
                job.status = "queued"
                job.locked_at = None
                job.run_at = utcnow() + timedelta(seconds=delay)
                return None

    # ── Regular template send (or template fallback after policy error) ────────
    with perf_log(
        "outbox_worker",
        "outbox.meta_send",
        job_id=job.id,
        job_type=job.job_type,
        company_id=job.company_id,
        sender_id=sender_id,
        phone_e164=phone,
        template_code=job.job_type,
        send_mode=send_mode,
    ) as _ms_ctx:
        if use_template:
            assert meta_template_name is not None
            if _use_template_fallback:
                with perf_log(
                    "outbox_worker",
                    "outbox.template_fallback",
                    job_id=job.id,
                    job_type=job.job_type,
                    company_id=job.company_id,
                    sender_id=sender_id,
                    phone_e164=phone,
                    text_send_error=_text_send_error,
                ) as _fb_ctx:
                    msg_id, err = await safe_send_template(
                        provider=provider,
                        sender_id=sender_id,
                        phone=phone,
                        template_name=meta_template_name,
                        language=effective_template_language,
                        params=template_params,
                        fallback_text=final_body,
                        contact_name=contact_name,
                        tenant_provider=job_provider,
                        company_id=job.company_id,
                        header_image_url=header_image_url,
                    )
                    _fb_ctx.update(provider_message_id=msg_id)
                    if err is not None:
                        _fb_ctx.update(send_error=err)
            else:
                msg_id, err = await safe_send_template(
                    provider=provider,
                    sender_id=sender_id,
                    phone=phone,
                    template_name=meta_template_name,
                    language=effective_template_language,
                    params=template_params,
                    fallback_text=final_body,
                    contact_name=contact_name,
                    tenant_provider=job_provider,
                    company_id=job.company_id,
                    header_image_url=header_image_url,
                )
            send_meta: dict[str, Any] = {
                "send_type": "template_fallback" if _use_template_fallback else "template",
                "template": meta_template_name,
                "params": template_params,
                "lang": effective_template_language,
            }
            if header_image_url:
                send_meta["header_image_url"] = header_image_url
            if _24h_eligible:
                _last_inbound_iso = _last_inbound_at.isoformat() if _last_inbound_at else None
                send_meta["text_inside_24h_eligible"] = True
                send_meta["wa_window_open"] = bool(_wa_window_open)
                send_meta["last_meta_inbound_at"] = _last_inbound_iso
                send_meta["original_template"] = meta_template_name
                send_meta["original_template_language"] = effective_template_language
                if _wa_window_check_error:
                    send_meta["text_inside_24h"] = False
                    send_meta["wa_window_check_error"] = _wa_window_check_error
                    send_meta["route_reason"] = "wa_window_check_failed"
                elif _use_template_fallback:
                    send_meta["text_inside_24h"] = True
                    send_meta["text_send_error"] = _text_send_error
                    send_meta["fallback_reason"] = "text_send_failed"
                    send_meta["route_reason"] = "text_send_policy_error_fallback"
                else:
                    send_meta["text_inside_24h"] = False
                    send_meta["route_reason"] = "customer_service_window_closed"
        else:
            msg_id, err = await safe_send(
                provider=provider,
                sender_id=sender_id,
                phone=phone,
                text=final_body,
                contact_name=contact_name,
                tenant_provider=job_provider,
                company_id=job.company_id,
            )
            send_meta = {"send_type": "text"}
        _ms_ctx.update(provider_message_id=msg_id)
        if err is not None:
            _ms_ctx.update(send_error=err)

    _retry_audit = delivery_retry_audit(job)
    if _retry_audit:
        retry_payload = getattr(job, "payload", None) or {}
        send_meta.update(_retry_audit)
        send_meta["delivery_retry_reason"] = "original_delivery_failed"
        send_meta["original_provider_message_id"] = retry_payload.get("delivery_retry_of_provider_message_id")

    if err is not None:
        if settings.meta_circuit_breaker_enabled and _is_transient_provider_error(err):
            _decrement_send_attempt(job)
            await _handle_transient_meta_error(job, record, err)
            return

        with perf_log(
            "outbox_worker",
            "outbox.insert_outbox",
            job_id=job.id,
            job_type=job.job_type,
            company_id=job.company_id,
            phone_e164=phone,
            template_code=job.job_type,
            outbox_status="failed",
        ):
            out = OutboxMessage(
                company_id=job.company_id,
                client_id=(client.id if client else None),
                record_id=(record.id if record else None),
                job_id=job.id,
                sender_id=sender_id,
                phone_e164=phone,
                template_code=job.job_type,
                language=outbox_language,
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

        if use_template and _is_permanent_meta_template_error(err):
            job.status = "failed"
            job.locked_at = None
            return

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

    with perf_log(
        "outbox_worker",
        "outbox.insert_outbox",
        job_id=job.id,
        job_type=job.job_type,
        company_id=job.company_id,
        phone_e164=phone,
        template_code=job.job_type,
        outbox_status="sent",
    ):
        now_sent = utcnow()
        out = OutboxMessage(
            company_id=job.company_id,
            client_id=(client.id if client else None),
            record_id=(record.id if record else None),
            job_id=job.id,
            sender_id=sender_id,
            phone_e164=phone,
            template_code=job.job_type,
            language=outbox_language,
            body=final_body,
            status="sent",
            error=None,
            provider_message_id=msg_id,
            scheduled_at=job.run_at,
            sent_at=now_sent,
            meta=send_meta,
        )
        session.add(out)
        if _job_payload.get("campaign_recipient_id") is not None:
            await session.flush()
        await _backfill_campaign_recipient_after_send(
            session=session,
            job_type=job.job_type,
            job_id=job.id,
            payload=_job_payload,
            outbox_id=out.id,
            now_sent=now_sent,
            provider_message_id=msg_id,
        )

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


def _resolve_poll_sec(
    explicit: float | None,
    settings_value: float,
) -> float:
    return settings_value if explicit is None else explicit


async def run_loop(
    provider: WhatsAppProvider,
    batch_size: int = 50,
    poll_sec: float | None = None,
) -> None:
    effective_poll_sec = _resolve_poll_sec(poll_sec, settings.outbox_worker_poll_sec)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logger.info(
        "Outbox worker started. batch_size=%s poll=%ss",
        batch_size,
        effective_poll_sec,
    )

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
            await asyncio.sleep(effective_poll_sec)
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
