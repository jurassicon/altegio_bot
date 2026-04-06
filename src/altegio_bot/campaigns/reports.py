"""Отчёты по кампаниям рассылки.

Два типа отчётов:
  1. Отчёт по конкретному run (run_report).
  2. Monthly dashboard по филиалам (monthly_dashboard).

Правила подсчёта воронки доставки (кумулятивно):
  provider_accepted = sent + delivered + read
    (все, что прошло у провайдера, независимо от текущего статуса)
  delivered          = delivered + read
    (read ⊆ delivered — если прочитали, значит доставили)
  read               = только read

Monthly dashboard включает только runs с mode='send-real'.
Preview runs в дашборд не попадают.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.models.models import CampaignRecipient, CampaignRun, OutboxMessage

# Отображение company_id → название
COMPANIES: dict[int, str] = {758285: "Karlsruhe", 1271200: "Rastatt"}

# Статусы OutboxMessage, считающиеся «принятыми провайдером»
_PROVIDER_ACCEPTED_STATUSES = {"sent", "delivered", "read"}
# Статусы OutboxMessage, считающиеся «доставленными»
_DELIVERED_STATUSES = {"delivered", "read"}


def _pct(num: int, den: int) -> float:
    """Безопасное деление для процентов."""
    return round(num / den, 4) if den else 0.0


async def run_report(session: AsyncSession, run_id: int) -> dict[str, Any]:
    """Полный отчёт по одному CampaignRun."""
    run = await session.get(CampaignRun, run_id)
    if run is None:
        raise ValueError(f"CampaignRun {run_id} not found")

    # Подсчёт получателей по статусам и причинам
    stmt = (
        select(
            CampaignRecipient.status,
            CampaignRecipient.excluded_reason,
            func.count(CampaignRecipient.id).label("cnt"),
        )
        .where(CampaignRecipient.campaign_run_id == run_id)
        .group_by(CampaignRecipient.status, CampaignRecipient.excluded_reason)
    )
    rows = (await session.execute(stmt)).all()

    status_counts: dict[str, int] = {}
    reason_counts: dict[str, int] = {}
    for status, reason, cnt in rows:
        status_counts[status] = status_counts.get(status, 0) + cnt
        if reason:
            reason_counts[reason] = reason_counts.get(reason, 0) + cnt

    # Attribution из outbox_messages (свежие данные с кумулятивным подсчётом)
    attr = await _fetch_attribution(session, run_id)

    total = run.total_clients_seen
    eligible = run.candidates_count
    queued = run.queued_count or status_counts.get("queued", 0)

    return {
        "run_id": run.id,
        "campaign_code": run.campaign_code,
        "mode": run.mode,
        "status": run.status,
        "period_start": _iso(run.period_start),
        "period_end": _iso(run.period_end),
        "source_preview_run_id": run.source_preview_run_id,
        "created_at": _iso(run.created_at),
        "completed_at": _iso(run.completed_at),
        # Сегментация
        "total_found": total,
        "eligible": eligible,
        "excluded": {
            "total": total - eligible,
            "opted_out": run.excluded_opted_out,
            "no_phone": run.excluded_no_phone,
            "invalid_phone": run.excluded_invalid_phone,
            "no_whatsapp": run.excluded_no_whatsapp,
            "multiple_records_in_period": run.excluded_multiple_records,
            "no_confirmed_record_in_period": run.excluded_no_confirmed_record,
            "has_records_before_period": run.excluded_has_records_before,
            "by_reason": reason_counts,
        },
        # Loyalty
        "cards_deleted": run.cards_deleted_count,
        "cards_issued": run.cards_issued_count,
        "cleanup_failed": run.cleanup_failed_count,
        # Доставка (кумулятивно: если прочитали — это и delivered, и provider_accepted)
        "queued": queued,
        "provider_accepted": run.provider_accepted_count or attr["provider_accepted"],
        "delivered": run.delivered_count or attr["delivered"],
        "read": run.read_count or attr["read"],
        # Атрибуция (из полей CampaignRecipient)
        "replied": run.replied_count or attr["replied"],
        "booked_after_campaign": run.booked_after_count or attr["booked_after"],
        "opted_out_after_campaign": run.opted_out_after_count,
        # Follow-up
        "followup_enabled": run.followup_enabled,
        "followup_delay_days": run.followup_delay_days,
        "followup_policy": run.followup_policy,
        "followup_stats": attr["followup"],
    }


async def _fetch_attribution(session: AsyncSession, run_id: int) -> dict[str, Any]:
    """Получить свежие данные атрибуции JOIN с outbox_messages.

    Воронка считается кумулятивно:
      provider_accepted >= delivered >= read
    Это важно: если сообщение уже «read», оно тоже считается в delivered
    и provider_accepted — иначе предыдущие шаги воронки занижаются.
    """
    # Сырые статусы из outbox_messages
    stmt = (
        select(
            OutboxMessage.status,
            func.count(OutboxMessage.id).label("cnt"),
        )
        .join(CampaignRecipient, CampaignRecipient.outbox_message_id == OutboxMessage.id)
        .where(CampaignRecipient.campaign_run_id == run_id)
        .group_by(OutboxMessage.status)
    )
    rows = (await session.execute(stmt)).all()
    raw: dict[str, int] = {status: int(cnt) for status, cnt in rows}

    # Кумулятивный подсчёт
    provider_accepted = sum(raw.get(s, 0) for s in _PROVIDER_ACCEPTED_STATUSES)
    delivered = sum(raw.get(s, 0) for s in _DELIVERED_STATUSES)
    read = raw.get("read", 0)

    # Follow-up статусы
    fu_stmt = (
        select(
            CampaignRecipient.followup_status,
            func.count(CampaignRecipient.id).label("cnt"),
        )
        .where(CampaignRecipient.campaign_run_id == run_id)
        .where(CampaignRecipient.followup_status.is_not(None))
        .group_by(CampaignRecipient.followup_status)
    )
    fu_rows = (await session.execute(fu_stmt)).all()
    fu_statuses = {s: int(c) for s, c in fu_rows}

    # Подсчёт replied и booked из CampaignRecipient
    attr_stmt = select(
        func.count(CampaignRecipient.id).filter(CampaignRecipient.replied_at.is_not(None)).label("replied"),
        func.count(CampaignRecipient.id).filter(CampaignRecipient.booked_after_at.is_not(None)).label("booked"),
    ).where(CampaignRecipient.campaign_run_id == run_id)
    attr_row = (await session.execute(attr_stmt)).one()

    return {
        "provider_accepted": provider_accepted,
        "delivered": delivered,
        "read": read,
        "replied": int(attr_row.replied or 0),
        "booked_after": int(attr_row.booked or 0),
        "followup": fu_statuses,
    }


async def monthly_dashboard(
    session: AsyncSession,
    *,
    year: int,
    month: int,
    company_ids: list[int] | None = None,
) -> dict[str, Any]:
    """Monthly dashboard по всем филиалам за указанный месяц.

    Учитываются только runs с mode='send-real' и status='completed'.
    Preview runs в дашборд не попадают.
    """
    month_start = datetime(year, month, 1, tzinfo=timezone.utc)
    next_month = month + 1 if month < 12 else 1
    next_year = year if month < 12 else year + 1
    month_end = datetime(next_year, next_month, 1, tzinfo=timezone.utc)

    cids_filter = company_ids if company_ids else list(COMPANIES.keys())

    runs_stmt = (
        select(CampaignRun)
        .where(CampaignRun.period_start >= month_start)
        .where(CampaignRun.period_start < month_end)
        .where(CampaignRun.status == "completed")
        # Только реальные рассылки; preview не считаем в дашборде
        .where(CampaignRun.mode == "send-real")
        .order_by(CampaignRun.created_at.desc())
    )
    all_runs = (await session.execute(runs_stmt)).scalars().all()

    # Фильтруем по компаниям (company_ids — JSON-массив)
    runs = [r for r in all_runs if any(cid in (r.company_ids or []) for cid in cids_filter)]

    # Группируем по company_id
    per_company: dict[int, list[CampaignRun]] = {}
    for run in runs:
        for cid in run.company_ids or []:
            if cid in cids_filter:
                per_company.setdefault(cid, []).append(run)

    company_reports = []
    for cid in cids_filter:
        cid_runs = per_company.get(cid, [])
        totals = _aggregate_company(cid_runs)
        company_reports.append(
            {
                "company_id": cid,
                "company_name": COMPANIES.get(cid, str(cid)),
                "runs_count": len(cid_runs),
                "run_ids": [r.id for r in cid_runs],
                "totals": totals,
            }
        )

    return {
        "year": year,
        "month": month,
        "period": f"{year}-{month:02d}",
        "companies": company_reports,
    }


def _aggregate_company(runs: list[CampaignRun]) -> dict[str, Any]:
    """Агрегировать метрики по списку runs одной компании."""
    if not runs:
        return _empty_totals()

    total_found = sum(r.total_clients_seen for r in runs)
    eligible = sum(r.candidates_count for r in runs)
    sent = sum(r.queued_count for r in runs)
    delivered = sum(r.delivered_count for r in runs)
    read = sum(r.read_count for r in runs)
    replied = sum(r.replied_count for r in runs)
    booked = sum(r.booked_after_count for r in runs)
    opted_out = sum(r.opted_out_after_count for r in runs)
    invalid = sum(r.excluded_invalid_phone for r in runs)
    no_wa = sum(r.excluded_no_whatsapp for r in runs)

    problematic = invalid + no_wa

    return {
        "new_clients_found": total_found,
        "eligible": eligible,
        "sent": sent,
        "delivered": delivered,
        "read": read,
        "replied": replied,
        "booked_after_30d": booked,
        "opted_out": opted_out,
        "problematic_phones_pct": _pct(problematic, eligible),
        # sent / eligible
        "coverage_rate": _pct(sent, eligible),
        # booked / sent
        "booking_conversion": _pct(booked, sent),
    }


def _empty_totals() -> dict[str, Any]:
    return {
        "new_clients_found": 0,
        "eligible": 0,
        "sent": 0,
        "delivered": 0,
        "read": 0,
        "replied": 0,
        "booked_after_30d": 0,
        "opted_out": 0,
        "problematic_phones_pct": 0.0,
        "coverage_rate": 0.0,
        "booking_conversion": 0.0,
    }


def _iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return dt.isoformat()
