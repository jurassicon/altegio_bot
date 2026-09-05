"""Local proofs shared by handover plan, apply and verify; never persist PII."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.easyweek_locations import configured_easyweek_locations
from altegio_bot.easyweek_migration.manifest import MigrationManifest
from altegio_bot.easyweek_migration.reminder_handover import CANCEL_REASON
from altegio_bot.easyweek_policy import EASYWEEK_REMINDER_JOB_TYPES
from altegio_bot.easyweek_service_category import evaluate_service_category
from altegio_bot.models.models import Client, EasyWeekMigrationLedger, MessageJob, Record
from altegio_bot.settings import settings


def digest(value: Any) -> str:
    def encode(item: Any) -> str:
        if isinstance(item, datetime):
            return item.astimezone(timezone.utc).isoformat()
        return str(item)

    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), default=encode).encode()).hexdigest()


def columns(row: Any, *, exclude: tuple[str, ...] = ()) -> dict[str, Any]:
    return {column.key: getattr(row, column.key) for column in row.__table__.columns if column.key not in exclude}


def configuration_digest() -> str:
    # Include credentials only in the hash. Changing credentials or the runtime
    # domain policy invalidates the live proof without exposing either in a file.
    names = (
        "easyweek_enabled",
        "easyweek_processing_enabled",
        "easyweek_notifications_enabled",
        "easyweek_reminders_enabled",
        "easyweek_reminder_api_guard_enabled",
        "easyweek_allowed_service_categories",
        "easyweek_location_map",
        "easyweek_workspace_slug",
        "easyweek_api_base_url",
    )
    values = {name: getattr(settings, name, None) for name in names}
    values["api_key"] = settings.easyweek_api_key.get_secret_value()
    return digest(values)


def configuration_ready() -> bool:
    # Notification fences may stay closed during the migration. Freeze their
    # actual values; do not ask the operator to open sending just to plan jobs.
    return bool(
        settings.easyweek_enabled and settings.easyweek_processing_enabled and configured_easyweek_locations().ready
    )


async def wave_entries(session: AsyncSession, companies: tuple[int, ...], runs: tuple[str, ...]) -> list[Any]:
    return list(
        (
            await session.scalars(
                select(EasyWeekMigrationLedger)
                .where(
                    EasyWeekMigrationLedger.source_company_id.in_(companies), EasyWeekMigrationLedger.run_id.in_(runs)
                )
                .order_by(EasyWeekMigrationLedger.id)
                .execution_options(populate_existing=True)
            )
        ).all()
    )


async def candidate_fingerprint(session: AsyncSession, companies: tuple[int, ...], runs: tuple[str, ...]) -> str:
    entries = await wave_entries(session, companies, runs)
    material = []
    for entry in entries:
        # Source and target identities, not company-wide Record enumeration.
        predicates = [
            (Record.provider == "altegio")
            & (Record.company_id == entry.source_company_id)
            & (Record.altegio_record_id == entry.source_record_id)
        ]
        from altegio_bot.easyweek_migration.reminder_handover import canonical_uuid

        booking_uuid = canonical_uuid(entry.target_booking_uuid)
        if booking_uuid is not None:
            predicates.append(Record.easyweek_booking_uuid == booking_uuid)
        records = list(
            (
                await session.scalars(
                    select(Record).where(or_(*predicates)).order_by(Record.id).execution_options(populate_existing=True)
                )
            ).all()
        )
        record_ids = [record.id for record in records]
        clients = list(
            (
                await session.scalars(
                    select(Client)
                    .where(Client.id.in_([r.client_id for r in records]))
                    .order_by(Client.id)
                    .execution_options(populate_existing=True)
                )
            ).all()
        )
        jobs = list(
            (
                await session.scalars(
                    select(MessageJob)
                    .where(MessageJob.record_id.in_(record_ids), MessageJob.job_type.in_(EASYWEEK_REMINDER_JOB_TYPES))
                    .order_by(MessageJob.id)
                    .execution_options(populate_existing=True)
                )
            ).all()
        )
        material.append(
            [columns(entry), [columns(r) for r in records], [columns(c) for c in clients], [columns(j) for j in jobs]]
        )
    return digest(material)


async def row_evidence(session: AsyncSession, entry: Any, source: Record, target: Record) -> dict[str, str]:
    clients = list(
        (
            await session.scalars(
                select(Client)
                .where(Client.id.in_([source.client_id, target.client_id]))
                .order_by(Client.id)
                .execution_options(populate_existing=True)
            )
        ).all()
    )
    jobs = list(
        (
            await session.scalars(
                select(MessageJob)
                .where(MessageJob.record_id == source.id, MessageJob.job_type.in_(EASYWEEK_REMINDER_JOB_TYPES))
                .order_by(MessageJob.id)
                .execution_options(populate_existing=True)
            )
        ).all()
    )
    # Only the fields this handover itself changes are omitted, so replay can
    # prove the same frozen facts after its own successful cancellation.
    return {
        "ledger": digest(
            columns(entry, exclude=("reminders_handed_over_at", "reminder_handover_plan_digest", "updated_at"))
        ),
        "source": digest(columns(source)),
        "target": digest(columns(target)),
        "clients": digest([columns(client) for client in clients]),
        "source_jobs": digest(
            [columns(job, exclude=("status", "last_error", "locked_at", "updated_at")) for job in jobs]
        ),
    }


async def local_refusal(
    session: AsyncSession, entry: Any, source: Record, target: Record, manifest: MigrationManifest | None = None
) -> str | None:
    if entry.source_provider != "altegio" or entry.target_provider != "easyweek":
        return "provider_mismatch"
    if manifest is not None:
        branch = manifest.branch(entry.source_company_id)
        if branch is None or branch.staff_scope(source.staff_id) != "selected":
            return "staff_scope_unproven"
    eligibility = evaluate_service_category(
        record_raw=target.raw, allowed_categories_raw=settings.easyweek_allowed_service_categories
    )
    if not eligibility.allowed:
        return "ownership_unproven"
    client = await session.get(Client, target.client_id) if target.client_id is not None else None
    if client is None or client.provider != "easyweek" or client.company_id != target.company_id:
        return "target_client_unproven"
    duplicates = list(
        (
            await session.scalars(
                select(EasyWeekMigrationLedger.id).where(
                    EasyWeekMigrationLedger.target_booking_uuid == entry.target_booking_uuid,
                    EasyWeekMigrationLedger.id != entry.id,
                )
            )
        ).all()
    )
    if duplicates:
        return "ledger_duplicate_target"
    for record in (source, target):
        jobs = list(
            (
                await session.scalars(
                    select(MessageJob)
                    .where(MessageJob.record_id == record.id, MessageJob.job_type.in_(EASYWEEK_REMINDER_JOB_TYPES))
                    .execution_options(populate_existing=True)
                )
            ).all()
        )
        for job in jobs:
            if job.provider != record.provider:
                return "provider_mismatch"
            if job.company_id != record.company_id:
                return "company_mismatch"
            if job.client_id != record.client_id:
                return "target_client_unproven" if record is target else "source_client_mismatch"
            if record is source and job.status == "canceled" and job.last_error == CANCEL_REASON:
                continue
            if job.run_at is None or not job.dedupe_key or not isinstance(job.payload, dict):
                return "reminder_identity_mismatch"
    return None
