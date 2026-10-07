"""Bounded read-only list preparation and atomic operator-confirmed application.

The plan contains only the necessary identity snapshot; no raw provider payload
is retained. Browser confirmation names an opaque plan, never customer identities.
All provider reads finish before the application transaction starts.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import uuid
from datetime import timedelta
from typing import Any

from sqlalchemy import func, or_, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.campaigns.easyweek_customer_history import _history_row
from altegio_bot.campaigns.easyweek_manual_identity import (
    IDENTITY_CONFLICT,
    KARLSRUHE_COMPANY_ID,
    ensure_local_identity,
    local_identity,
    lock_identity,
)
from altegio_bot.campaigns.easyweek_manual_recipient import (
    CUSTOMER_UNPROVEN,
    PHONE_UNUSABLE,
    PREVIEW_FROZEN,
    ROWS_AMBIGUOUS,
    RUN_NOT_EDITABLE,
    RUN_NOT_FOUND,
    ProvenCustomer,
    _active,
    _frozen,
    _preview_blocker,
    apply_proven_recipient,
    prove_customer,
)
from altegio_bot.easyweek_client import CUSTOMER_BOOKINGS_PER_PAGE
from altegio_bot.models.models import CampaignRecipient, CampaignRun, Client, EasyWeekManualRecipientPlan
from altegio_bot.utils import utcnow
from altegio_bot.webhooks.common import normalize_phone_candidate

MANUAL_POLICY = "altegio_visit_zero_easyweek_bookings"
HISTORY_NONEMPTY = "manual_recipient_history_nonempty"
HISTORY_UNPROVEN = "manual_recipient_history_unproven"
ATTESTATION_REQUIRED = "manual_batch_attestation_required"
INPUT_LIMIT = "manual_batch_input_limit"
RATE_LIMIT = "manual_batch_rate_limit"
PLAN_INVALID = "manual_batch_plan_invalid"
PLAN_EXPIRED = "manual_batch_plan_expired"
PLAN_CHANGED = "manual_batch_plan_changed"
READ_TIMEOUT = "manual_batch_timeout"
MAX_INPUT_BYTES = 16384
MAX_INPUT_LINES = 200
MAX_CONTACTS = 100
MAX_CONCURRENT_READS = 2
CONTACT_TIMEOUT_SECONDS = 15
LIST_TIMEOUT_SECONDS = 90
PLAN_TTL = timedelta(minutes=15)


async def _check_contacts(session_maker, *, run_id, phones, reader):
    """Same bounded, ordered read phase for preparation and confirmation.

    Cancel and join every sibling before returning on timeout or failure. No
    provider task may outlive this phase and overlap the write transaction.
    """
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_READS)

    async def one(phone):
        async with semaphore:
            try:
                async with asyncio.timeout(CONTACT_TIMEOUT_SECONDS):
                    result = await _check_one(
                        session_maker, run_id=run_id, company_id=KARLSRUHE_COMPANY_ID, phone=phone, reader=reader
                    )
            except TimeoutError:
                result = {"status": "rejected", "reason": READ_TIMEOUT}
            return phone, result

    tasks = [asyncio.create_task(one(phone)) for phone in phones]
    try:
        async with asyncio.timeout(LIST_TIMEOUT_SECONDS):
            return await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def prove_zero_booking_history(reader: Any, *, customer_uuid: str) -> str | None:
    """Zero means a complete, structurally valid zero-row history response.

    A nonzero total with validated rows is enough to exclude; do not spend API
    budget walking a history that can never qualify. An absent/malformed response
    is uncertainty, never zero. Every booking status excludes equally.
    """
    try:
        identity = uuid.UUID(customer_uuid)
        body = await reader.list_customer_bookings(customer_uuid, page=1, per_page=CUSTOMER_BOOKINGS_PER_PAGE)
        if not isinstance(body, dict) or not isinstance(body.get("data"), list):
            return HISTORY_UNPROVEN
        meta = body.get("meta")
        if not isinstance(meta, dict):
            return HISTORY_UNPROVEN
        fields = [meta.get(key) for key in ("current_page", "last_page", "per_page", "total")]
        if any(type(value) is not int for value in fields):
            return HISTORY_UNPROVEN
        current, last, per_page, total = fields
        if (
            current != 1
            or total < 0
            or per_page != CUSTOMER_BOOKINGS_PER_PAGE
            or last != max(1, (total + per_page - 1) // per_page)
            or len(body["data"]) != min(total, per_page)
        ):
            return HISTORY_UNPROVEN
        if total == 0:
            return None
        for row in body["data"]:
            if _history_row(row, customer_uuid=identity)[0] is None:
                return HISTORY_UNPROVEN
        return HISTORY_NONEMPTY
    except Exception:  # noqa: BLE001 - stable refusal, never raw provider errors
        return HISTORY_UNPROVEN


def _digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()


def _principal(value: str) -> str:
    return _digest(value)


def _failure(reason: str) -> dict[str, Any]:
    return {"ok": False, "reason": reason}


async def _run_signature(session: AsyncSession, run: CampaignRun) -> str:
    rows = list(
        (
            await session.scalars(
                select(CampaignRecipient)
                .where(CampaignRecipient.campaign_run_id == run.id)
                .order_by(CampaignRecipient.id)
            )
        ).all()
    )
    columns = CampaignRecipient.__table__.columns
    return _digest(
        {
            "run": [
                run.provider,
                run.company_ids,
                run.campaign_code,
                run.period_start,
                run.period_end,
                run.status,
                run.mode,
            ],
            "recipients": [{column.name: getattr(row, column.name) for column in columns} for row in rows],
        }
    )


async def _local_signature(session: AsyncSession, *, phone: str, customer_uuid: str) -> str:
    rows = list(
        (
            await session.scalars(
                select(Client)
                .where(
                    or_(
                        Client.phone_e164 == phone,
                        (Client.provider == "easyweek") & (Client.easyweek_customer_uuid == uuid.UUID(customer_uuid)),
                    )
                )
                .order_by(Client.id)
            )
        ).all()
    )
    return _digest(
        [
            [
                row.id,
                row.provider,
                row.company_id,
                row.altegio_client_id,
                row.easyweek_customer_uuid,
                row.phone_e164,
                row.wa_opted_out,
                row.wa_opted_out_at,
            ]
            for row in rows
        ]
    )


async def _matching_rows(session: AsyncSession, *, run_id: int, client: Client | None, proven: ProvenCustomer):
    selectors = [
        CampaignRecipient.easyweek_customer_uuid == uuid.UUID(proven.uuid),
        CampaignRecipient.phone_e164 == proven.phone,
    ]
    if client is not None:
        selectors.append(CampaignRecipient.client_id == client.id)
    return list(
        (
            await session.scalars(
                select(CampaignRecipient).where(CampaignRecipient.campaign_run_id == run_id, or_(*selectors))
            )
        ).all()
    )


async def _check_one(session_maker, *, run_id: int, company_id: int, phone: str, reader) -> dict[str, Any]:
    async with session_maker() as session:
        _, blocker = await local_identity(session, company_id=company_id, phone=phone)
        if blocker:
            return {"status": "rejected", "reason": blocker}
    proven, reason = await prove_customer(reader, phone=phone)
    if proven is None:
        return {"status": "rejected", "reason": reason or CUSTOMER_UNPROVEN}
    async with session_maker() as session:
        client, blocker = await local_identity(session, company_id=company_id, phone=phone, customer_uuid=proven.uuid)
        if blocker:
            return {"status": "rejected", "reason": blocker}
        rows = await _matching_rows(session, run_id=run_id, client=client, proven=proven)
        if (
            (rows and client is None)
            or len(rows) > 1
            or any(
                row.recipient_basis not in ("earned_first_visit", "operator_manual_selection")
                or row.easyweek_customer_uuid not in (None, uuid.UUID(proven.uuid))
                or row.company_id != company_id
                or (client is not None and row.client_id != client.id)
                for row in rows
            )
        ):
            return {"status": "rejected", "reason": ROWS_AMBIGUOUS}
        signature = await _local_signature(session, phone=phone, customer_uuid=proven.uuid)
        active = bool(rows and _active(rows[0]))
    if not active:
        reason = await prove_zero_booking_history(reader, customer_uuid=proven.uuid)
        if reason:
            return {"status": "rejected", "reason": reason}
        # Restoring a removed earned row retains its earned proof, which cannot
        # coexist with a zero-history manual policy; use the ordinary editor.
        if rows and rows[0].recipient_basis == "earned_first_visit" and rows[0].excluded_reason == "manual_removed":
            return {"status": "rejected", "reason": ROWS_AMBIGUOUS}
    return {
        "status": "already_present" if active else "addable",
        "reason": None,
        "phone": phone,
        "uuid": proven.uuid,
        "name": proven.first_name,
        "local_signature": signature,
    }


async def _rate_limited(session: AsyncSession, operator_digest: str) -> bool:
    count = await session.scalar(
        select(func.count())
        .select_from(EasyWeekManualRecipientPlan)
        .where(
            EasyWeekManualRecipientPlan.operator_digest == operator_digest,
            EasyWeekManualRecipientPlan.created_at > utcnow() - timedelta(minutes=1),
        )
    )
    return int(count or 0) >= 3


async def check_manual_recipients(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    run_id: int,
    phones: str,
    reader: Any,
    operator: str,
    session_fingerprint: str,
    prior_altegio_visit_confirmed: bool,
    assign_karlsruhe_confirmed: bool,
) -> dict[str, Any]:
    if prior_altegio_visit_confirmed is not True or assign_karlsruhe_confirmed is not True:
        return _failure(ATTESTATION_REQUIRED)
    if not operator or not session_fingerprint:
        return _failure(PLAN_INVALID)
    if not isinstance(phones, str) or len(phones.encode()) > MAX_INPUT_BYTES:
        return _failure(INPUT_LIMIT)
    lines = phones.splitlines()
    if not lines or len(lines) > MAX_INPUT_LINES:
        return _failure(INPUT_LIMIT)
    operator_digest = _principal(operator)
    async with session_maker() as session:
        run = await session.get(CampaignRun, run_id)
        if run is None:
            return _failure(RUN_NOT_FOUND)
        blocker = await _preview_blocker(session, run)
        if blocker:
            return _failure(blocker)
        if run.company_ids != [KARLSRUHE_COMPANY_ID] or run.campaign_code != "new_clients_monthly":
            return _failure(RUN_NOT_EDITABLE)
        if await _frozen(session, run_id):
            return _failure(PREVIEW_FROZEN)
        if await _rate_limited(session, operator_digest):
            return _failure(RATE_LIMIT)
        run_signature = await _run_signature(session, run)
    public_rows: list[dict[str, Any]] = []
    unique: dict[str, int] = {}
    for line, value in enumerate(lines, 1):
        if not value.strip():
            continue
        phone = normalize_phone_candidate(value)
        if phone is None:
            public_rows.append({"line": line, "phone": None, "status": "rejected", "reason": PHONE_UNUSABLE})
        elif phone in unique:
            public_rows.append(
                {"line": line, "phone": phone, "status": "duplicate", "reason": "manual_batch_duplicate"}
            )
        else:
            unique[phone] = line
    if len(unique) > MAX_CONTACTS or (not unique and not public_rows):
        return _failure(INPUT_LIMIT)
    if not unique:
        return {
            "ok": True,
            "reason": None,
            "plan_id": None,
            "expires_at": None,
            "eligible_count": 0,
            "already_present_count": 0,
            "rejected_count": len(public_rows),
            "duplicate_count": 0,
            "rows": public_rows,
        }
    # Reserve the rate budget before HTTP, including attempts that time out.
    # The empty, undisclosed plan cannot be confirmed and contains no contacts.
    now = utcnow()
    plan_uuid = uuid.uuid4()
    async with session_maker() as session:
        async with session.begin():
            key = int.from_bytes(
                hashlib.sha256(("manual-batch:" + operator_digest).encode()).digest()[:8], "big", signed=True
            )
            await session.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": key})
            if await _rate_limited(session, operator_digest):
                return _failure(RATE_LIMIT)
            # Expired verification material has no remaining authority.
            await session.execute(
                update(EasyWeekManualRecipientPlan)
                .where(
                    EasyWeekManualRecipientPlan.expires_at <= now,
                    EasyWeekManualRecipientPlan.applied_at.is_(None),
                )
                .values(payload={})
            )
            session.add(
                EasyWeekManualRecipientPlan(
                    id=plan_uuid,
                    run_id=run_id,
                    operator_digest=operator_digest,
                    session_digest=_principal(session_fingerprint),
                    policy=MANUAL_POLICY,
                    proof_digest=_digest({}),
                    payload={},
                    created_at=now,
                    expires_at=now + PLAN_TTL,
                )
            )
    try:
        checked = await _check_contacts(session_maker, run_id=run_id, phones=unique, reader=reader)
    except TimeoutError:
        return _failure(READ_TIMEOUT)
    accepted = []
    for phone, result in checked:
        public_rows.append(
            {"line": unique[phone], "phone": phone, "status": result["status"], "reason": result["reason"]}
        )
        if result["status"] != "rejected":
            accepted.append(result)
    public_rows.sort(key=lambda row: row["line"])
    payload = {
        "rows": accepted,
        "run_signature": run_signature,
        "prior_altegio_visit_confirmed": True,
        "assign_karlsruhe_confirmed": True,
        "input_digest": _digest(list(unique)),
    }
    async with session_maker() as session:
        async with session.begin():
            run = await session.get(CampaignRun, run_id)
            if run is None or await _run_signature(session, run) != run_signature or await _frozen(session, run_id):
                return _failure(PLAN_CHANGED)
            plan = await session.get(EasyWeekManualRecipientPlan, plan_uuid)
            plan.payload = payload
            plan.proof_digest = _digest(payload)
            plan_id = str(plan.id)
    return {
        "ok": True,
        "reason": None,
        "plan_id": plan_id,
        "expires_at": (now + PLAN_TTL).isoformat(),
        "eligible_count": sum(row["status"] == "addable" for row in public_rows),
        "already_present_count": sum(row["status"] == "already_present" for row in public_rows),
        "rejected_count": sum(row["status"] == "rejected" for row in public_rows),
        "duplicate_count": sum(row["status"] == "duplicate" for row in public_rows),
        "rows": public_rows,
    }


class _Abort(Exception):
    pass


def _plan_error(plan, *, run_id, operator, session_fingerprint, confirmed_count) -> str | None:
    if (
        plan is None
        or plan.run_id != run_id
        or plan.policy != MANUAL_POLICY
        or not hmac.compare_digest(plan.operator_digest, _principal(operator))
        or not hmac.compare_digest(plan.session_digest, _principal(session_fingerprint))
    ):
        return PLAN_INVALID
    if plan.applied_at is not None:
        if type(confirmed_count) is not int or confirmed_count != (plan.result or {}).get("added_count"):
            return PLAN_INVALID
        return None
    if utcnow() >= plan.expires_at:
        return PLAN_EXPIRED
    if not isinstance(plan.payload, dict) or not hmac.compare_digest(plan.proof_digest, _digest(plan.payload)):
        return PLAN_INVALID
    rows = plan.payload.get("rows", [])
    if (
        type(confirmed_count) is not int
        or confirmed_count <= 0
        or confirmed_count != sum(row["status"] == "addable" for row in rows)
    ):
        return PLAN_INVALID
    return None


async def confirm_manual_recipients(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    run_id: int,
    plan_id: str,
    confirmed_count: int,
    reader: Any,
    operator: str,
    session_fingerprint: str,
) -> dict[str, Any]:
    try:
        identity = uuid.UUID(plan_id)
    except (ValueError, TypeError, AttributeError):
        return _failure(PLAN_INVALID)
    async with session_maker() as session:
        plan = await session.get(EasyWeekManualRecipientPlan, identity)
        error = _plan_error(
            plan,
            run_id=run_id,
            operator=operator,
            session_fingerprint=session_fingerprint,
            confirmed_count=confirmed_count,
        )
        if error:
            return _failure(error)
        if plan.applied_at is not None:
            return dict(plan.result)
        payload = plan.payload
        proof_digest = plan.proof_digest
    # Repeat every accepted identity and policy read. No DB write transaction is
    # open during these calls, and the browser cannot substitute a subset.
    current = []
    live_changed = False
    timed_out = False
    try:
        checked_rows = await _check_contacts(
            session_maker, run_id=run_id, phones=[row["phone"] for row in payload["rows"]], reader=reader
        )
        for row, (phone, checked) in zip(payload["rows"], checked_rows, strict=True):
            timed_out |= checked.get("reason") == READ_TIMEOUT
            live_changed |= phone != row["phone"] or checked != row
            current.append(checked)
    except TimeoutError:
        timed_out = True
    from altegio_bot.campaigns.runner import lock_editable_preview, recompute_snapshot_counters

    try:
        async with session_maker() as session:
            async with session.begin():
                plan = await session.scalar(
                    select(EasyWeekManualRecipientPlan)
                    .where(EasyWeekManualRecipientPlan.id == identity)
                    .with_for_update()
                )
                error = _plan_error(
                    plan,
                    run_id=run_id,
                    operator=operator,
                    session_fingerprint=session_fingerprint,
                    confirmed_count=confirmed_count,
                )
                if error:
                    raise _Abort(error)
                if plan.applied_at is not None:
                    return dict(plan.result)
                if timed_out:
                    raise _Abort(READ_TIMEOUT)
                if live_changed or plan.proof_digest != proof_digest:
                    raise _Abort(PLAN_CHANGED)
                try:
                    run = await lock_editable_preview(session, run_id)
                except ValueError:
                    raise _Abort(PREVIEW_FROZEN) from None
                if (
                    await _preview_blocker(session, run)
                    or await _run_signature(session, run) != payload["run_signature"]
                ):
                    raise _Abort(PLAN_CHANGED)
                # Stable lock order across previews; recheck all identities
                # before touching any of them, and roll back all on failure.
                for row in sorted(current, key=lambda item: item["uuid"]):
                    await lock_identity(session, phone=row["phone"], customer_uuid=row["uuid"])
                for row in current:
                    _, reason = await local_identity(
                        session,
                        company_id=KARLSRUHE_COMPANY_ID,
                        phone=row["phone"],
                        customer_uuid=row["uuid"],
                        lock=True,
                    )
                    if (
                        reason
                        or await _local_signature(session, phone=row["phone"], customer_uuid=row["uuid"])
                        != row["local_signature"]
                    ):
                        raise _Abort(reason or IDENTITY_CONFLICT)
                added = 0
                recipient_ids = []
                for row in current:
                    if row["status"] == "already_present":
                        continue
                    client, reason = await ensure_local_identity(
                        session,
                        company_id=KARLSRUHE_COMPANY_ID,
                        phone=row["phone"],
                        customer_uuid=row["uuid"],
                        first_name=row["name"],
                        assign_karlsruhe=True,
                    )
                    if client is None:
                        raise _Abort(reason or IDENTITY_CONFLICT)
                    outcome = await apply_proven_recipient(
                        session,
                        run=run,
                        company_id=KARLSRUHE_COMPANY_ID,
                        client=client,
                        proven=ProvenCustomer(uuid=row["uuid"], phone=row["phone"], first_name=row["name"]),
                    )
                    if not outcome.ok or outcome.recipient_basis != "operator_manual_selection":
                        raise _Abort(outcome.reason or ROWS_AMBIGUOUS)
                    recipient = await session.get(CampaignRecipient, outcome.recipient_id)
                    recipient.manual_policy = MANUAL_POLICY
                    recipient.manual_policy_checked_at = utcnow()
                    recipient.manual_operator_attested_at = plan.created_at
                    added += 1
                    recipient_ids.append(recipient.id)
                await recompute_snapshot_counters(session, run)
                result = {
                    "ok": True,
                    "reason": None,
                    "plan_id": str(identity),
                    "added_count": added,
                    "recipient_ids": recipient_ids,
                    "unchanged_count": sum(row["status"] == "already_present" for row in current),
                }
                plan.applied_at = utcnow()
                plan.result = result
                # Replays need only the result; erase retained contact data.
                plan.payload = {}
                return result
    except _Abort as exc:
        return _failure(str(exc))
