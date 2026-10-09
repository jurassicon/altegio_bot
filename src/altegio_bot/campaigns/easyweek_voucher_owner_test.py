"""One-ever, browser-approved free gift test, executed by the voucher worker.

This isolated singleton owns no campaign recipients and has no delivery path.
The production mailing fence must stay CLOSED. Each mutation is claimed once;
restart, configuration rotation and an ambiguous response never replenish it.
Only safe facts survive provider reads: no code, phone or raw response is stored.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from altegio_bot.campaigns.easyweek_voucher_production.account import (
    account_fingerprint,
    expected_account_fingerprint,
    prove_current_account,
)
from altegio_bot.campaigns.easyweek_voucher_production.baseline import prove_production_baseline
from altegio_bot.campaigns.easyweek_voucher_production.issuer import pinned_issuer, prove_issuer_membership
from altegio_bot.campaigns.easyweek_voucher_production.operations import OpsPrincipal
from altegio_bot.easyweek_client import EasyWeekClient
from altegio_bot.easyweek_migration.customer_api import read_customer_card
from altegio_bot.easyweek_voucher_canary.artifact import matches_customer
from altegio_bot.easyweek_voucher_canary.orders import (
    ORDER_OPEN,
    ORDER_PAID,
    canonical_uuid,
    classify_order,
    find_marker_orders,
    order_object,
    paid_order_amounts_proven,
    payable_order_reasons,
    rows,
)
from altegio_bot.easyweek_voucher_identity import (
    EASYWEEK_WORKSPACE_CURRENCY,
    EASYWEEK_WORKSPACE_SLUG,
    EASYWEEK_WORKSPACE_UUID,
    KARLSRUHE_LOCATION_UUID,
)
from altegio_bot.easyweek_voucher_mutation import EasyWeekVoucherMutationClient
from altegio_bot.easyweek_voucher_production_contract import GIFT_PRODUCTION_CONTRACT
from altegio_bot.models.models import EasyWeekVoucherOwnerTest
from altegio_bot.settings import settings
from altegio_bot.utils import utcnow

SCOPE = "easyweek_owner_gift_test_v1"
CONTRACT = GIFT_PRODUCTION_CONTRACT
TTL = timedelta(minutes=10)
READ_TIMEOUT = 90
_RUNNING = {"create_running", "pay_running"}
_QUEUED = {"create_queued", "pay_queued"}


class OwnerTestError(Exception):
    """A stable local reason, never a remote exception or response."""

    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(reason)


def _key() -> bytes:
    value = settings.ops_secret or settings.ops_pass
    if not value:
        raise OwnerTestError("owner_test_signing_unconfigured")
    return value.encode()


def _digest(value: Any) -> str:
    material = json.dumps(value, sort_keys=True, separators=(",", ":"))
    return hmac.new(_key(), (SCOPE + ":" + material).encode(), hashlib.sha256).hexdigest()


def _configuration() -> tuple[str, str, str]:
    if not settings.easyweek_voucher_owner_test_enabled:
        raise OwnerTestError("owner_test_disabled")
    if settings.easyweek_voucher_production_mailing_enabled:
        raise OwnerTestError("owner_test_requires_closed_mailing_fence")
    if not settings.easyweek_voucher_production_executor_enabled:
        raise OwnerTestError("owner_test_executor_disabled")
    if not settings.ops_user:
        raise OwnerTestError("owner_test_signing_unconfigured")
    _key()
    customer = canonical_uuid(settings.easyweek_voucher_owner_test_customer_uuid.strip())
    if customer is None:
        raise OwnerTestError("owner_test_customer_unconfigured")
    issuer = pinned_issuer(settings.easyweek_voucher_production_mailing_staffer_uuid)
    if not issuer.pinned or issuer.uuid is None:
        raise OwnerTestError("owner_test_issuer_unproven")
    account = canonical_uuid(settings.easyweek_voucher_production_mailing_account_uuid.strip())
    if account is None or account_fingerprint(account) != expected_account_fingerprint(CONTRACT):
        raise OwnerTestError("owner_test_account_unproven")
    return customer, issuer.uuid, account


def _require_principal(principal: OpsPrincipal) -> None:
    if principal.account != settings.ops_user or not principal.session_fingerprint:
        raise OwnerTestError("owner_test_principal_mismatch")


@asynccontextmanager
async def _reader(reader: Any = None):
    if reader is not None:
        yield reader
    else:
        async with EasyWeekClient(max_attempts=1) as client:
            yield client


async def _prove(reader: Any, configured: tuple[str, str, str]) -> str:
    customer, issuer, account = configured
    try:
        async with asyncio.timeout(READ_TIMEOUT):
            workspace = await reader.get_workspace()
            if not isinstance(workspace, dict) or any(
                workspace.get(key) != expected
                for key, expected in (
                    ("uuid", EASYWEEK_WORKSPACE_UUID),
                    ("slug", EASYWEEK_WORKSPACE_SLUG),
                    ("currency", EASYWEEK_WORKSPACE_CURRENCY),
                )
            ):
                raise OwnerTestError("owner_test_workspace_unproven")
            locations = await reader.list_locations()
            if (
                sum(isinstance(row, dict) and row.get("uuid") == KARLSRUHE_LOCATION_UUID for row in rows(locations))
                != 1
            ):
                raise OwnerTestError("owner_test_location_unproven")
            baseline = prove_production_baseline(
                await reader.get_voucher_template(CONTRACT.template_uuid), contract=CONTRACT
            )
            if not baseline.proven:
                raise OwnerTestError("owner_test_product_unproven")
            if not await prove_current_account(
                reader, account_uuid=account, location_uuid=KARLSRUHE_LOCATION_UUID, contract=CONTRACT
            ):
                raise OwnerTestError("owner_test_account_unproven")
            membership = await prove_issuer_membership(
                reader, location_uuid=KARLSRUHE_LOCATION_UUID, issuer_uuid=issuer
            )
            if not membership.proven:
                raise OwnerTestError("owner_test_issuer_unproven")
            card = read_customer_card(await reader.get_customer(customer))
            if card.uuid != customer:
                raise OwnerTestError("owner_test_customer_unproven")
            # UUID selects the one explicitly server-configured owner contact.
            # Its phone is bound privately so changing it invalidates all plans.
            return _digest(
                {
                    "customer": customer,
                    "phone": card.phone,
                    "issuer": issuer,
                    "account": account,
                    "contract": CONTRACT.digest_material(),
                    "template_digest": baseline.digest,
                }
            )
    except OwnerTestError:
        raise
    except Exception:
        raise OwnerTestError("owner_test_live_read_failed") from None


def _safe(row: EasyWeekVoucherOwnerTest | None) -> dict[str, Any]:
    reasons = []
    try:
        _configuration()
    except OwnerTestError as exc:
        reasons.append(exc.reason)
    state = row.state if row else "new"
    stopped = bool(row and row.stopped)
    available = []
    if not reasons and not stopped:
        if state == "new" and not (row and row.create_attempted):
            available.append("create")
        elif state == "open" and not (row and row.pay_attempted):
            available.append("pay")
    if row is not None:
        if row.create_attempted and state not in _RUNNING | _QUEUED:
            available.append("reconcile")
        if not stopped:
            available.append("stop")
    return {
        "scope": SCOPE,
        "status_known": True,
        "state": state,
        "status": state,
        "stopped": stopped,
        "available_actions": available,
        "reasons": reasons,
        "reason": row.reason if row else None,
        "count": 1,
        "nominal_minor": 1000,
        "issue_minor": 0,
        "currency": "EUR",
        "account_label": CONTRACT.payment_account_label,
        "create_attempted": bool(row and row.create_attempted),
        "pay_attempted": bool(row and row.pay_attempted),
        "order_observed": bool(row and row.order_uuid),
        "code_present": bool(row and row.code_present),
        "operation_status": "running" if state in _RUNNING else "queued" if state in _QUEUED else None,
        "delivery_authorized": False,
    }


def _event(row: EasyWeekVoucherOwnerTest, action: str, principal: OpsPrincipal | None = None) -> None:
    row.updated_at = utcnow()
    entry = {"action": action, "at": row.updated_at.isoformat()}
    if principal:
        entry.update(principal.as_safe_dict())
    row.audit = [*row.audit, entry]


async def get_status(session_factory: Any) -> dict[str, Any]:
    async with session_factory() as session:
        return _safe(await session.get(EasyWeekVoucherOwnerTest, 1))


async def _locked(session: Any) -> EasyWeekVoucherOwnerTest | None:
    return await session.scalar(
        select(EasyWeekVoucherOwnerTest).where(EasyWeekVoucherOwnerTest.id == 1).with_for_update()
    )


def _allowed(row: EasyWeekVoucherOwnerTest, stage: str) -> None:
    if row.stopped:
        raise OwnerTestError("owner_test_stopped")
    expected = "new" if stage == "create" else "open"
    attempted = row.create_attempted if stage == "create" else row.pay_attempted
    if row.state != expected or attempted:
        raise OwnerTestError("owner_test_stage_unavailable")


async def offer(session_factory: Any, *, principal: OpsPrincipal, stage: str, reader: Any = None) -> dict[str, Any]:
    if stage not in {"create", "pay"}:
        raise OwnerTestError("owner_test_stage_unavailable")
    _require_principal(principal)
    configured = _configuration()
    async with _reader(reader) as client:
        binding = await _prove(client, configured)
    now = utcnow()
    async with session_factory() as session, session.begin():
        await session.execute(
            insert(EasyWeekVoucherOwnerTest)
            .values(
                id=1,
                customer_uuid=UUID(configured[0]),
                binding_digest=binding,
                marker="EWOWNERGIFT-" + uuid4().hex,
                state="new",
                stopped=False,
                create_attempted=False,
                pay_attempted=False,
                code_present=False,
                audit=[],
                created_at=now,
                updated_at=now,
            )
            .on_conflict_do_nothing(index_elements=["id"])
        )
        row = await _locked(session)
        assert row is not None
        _allowed(row, stage)
        if row.binding_digest != binding or str(row.customer_uuid) != configured[0]:
            raise OwnerTestError("owner_test_configuration_drift")
        plan = {
            "id": uuid4().hex,
            "stage": stage,
            "binding": binding,
            "principal": principal.account,
            "session": principal.session_fingerprint,
            "expires_at": (now + TTL).isoformat(),
            "count": 1,
            "nominal_minor": 1000,
            "issue_minor": 0,
            "order": str(row.order_uuid) if row.order_uuid else None,
        }
        row.approval = {**plan, "signature": _digest(plan), "consumed": False}
        _event(row, "offer_" + stage, principal)
        return {
            **_safe(row),
            "ready": True,
            "approval_id": plan["id"],
            "stage": stage,
            "expires_at": plan["expires_at"],
        }


def _check_plan(row: EasyWeekVoucherOwnerTest, *, principal: OpsPrincipal | None = None) -> dict[str, Any]:
    plan = dict(row.approval or {})
    signature = plan.pop("signature", "")
    plan.pop("consumed", None)
    if not isinstance(signature, str) or not hmac.compare_digest(signature, _digest(plan)):
        raise OwnerTestError("owner_test_approval_invalid")
    if plan.get("stage") not in {"create", "pay"} or plan.get("binding") != row.binding_digest:
        raise OwnerTestError("owner_test_approval_invalid")
    if plan["stage"] == "pay" and plan.get("order") != (str(row.order_uuid) if row.order_uuid else None):
        raise OwnerTestError("owner_test_approval_invalid")
    if principal and (
        plan.get("principal") != principal.account or plan.get("session") != principal.session_fingerprint
    ):
        raise OwnerTestError("owner_test_principal_mismatch")
    try:
        expiry = datetime.fromisoformat(plan["expires_at"])
        if expiry <= utcnow():
            raise ValueError
    except (KeyError, TypeError, ValueError):
        raise OwnerTestError("owner_test_approval_expired") from None
    return plan


async def confirm(
    session_factory: Any,
    *,
    principal: OpsPrincipal,
    approval_id: str,
    confirmed_count: int,
    confirmed_nominal_minor: int,
    confirmed_issue_minor: int,
) -> dict[str, Any]:
    _require_principal(principal)
    configured = _configuration()
    if any(type(value) is not int for value in (confirmed_count, confirmed_nominal_minor, confirmed_issue_minor)) or (
        confirmed_count,
        confirmed_nominal_minor,
        confirmed_issue_minor,
    ) != (1, 1000, 0):
        raise OwnerTestError("owner_test_amount_unconfirmed")
    async with session_factory() as session, session.begin():
        row = await _locked(session)
        if row is None or not row.approval or row.approval.get("id") != approval_id:
            raise OwnerTestError("owner_test_approval_invalid")
        plan = _check_plan(row, principal=principal)
        if str(row.customer_uuid) != configured[0]:
            raise OwnerTestError("owner_test_configuration_drift")
        if row.stopped:
            raise OwnerTestError("owner_test_stopped")
        if row.approval.get("consumed"):
            return {**_safe(row), "accepted": True}
        _allowed(row, plan["stage"])
        row.approval = {**row.approval, "consumed": True}
        row.state = plan["stage"] + "_queued"
        _event(row, "confirm_" + plan["stage"], principal)
        return {**_safe(row), "accepted": True}


async def stop(session_factory: Any, *, principal: OpsPrincipal) -> dict[str, Any]:
    _require_principal(principal)
    async with session_factory() as session, session.begin():
        row = await _locked(session)
        if row is None:
            raise OwnerTestError("owner_test_not_started")
        row.stopped = True
        if row.state in _QUEUED or row.state == "new":
            row.state = "blocked"
        row.reason = "owner_test_stopped"
        _event(row, "stop", principal)
        return _safe(row)


def _order_proof(payload: object, customer: str, order_uuid: str | None = None) -> tuple[str, str, bool]:
    order = order_object(payload)
    if order is None or canonical_uuid(order.get("uuid")) is None or not matches_customer(order, customer):
        raise OwnerTestError("owner_test_order_identity_unproven")
    if order_uuid and order.get("uuid") != order_uuid:
        raise OwnerTestError("owner_test_order_identity_unproven")
    if "location_uuid" in order and order["location_uuid"] != KARLSRUHE_LOCATION_UUID:
        raise OwnerTestError("owner_test_order_identity_unproven")
    if payable_order_reasons(
        order, expected_template_uuid=CONTRACT.template_uuid, expected_price_minor=0, expected_value_minor=1000
    ):
        raise OwnerTestError("owner_test_order_product_unproven")
    state, _ = classify_order(order, expected_price_minor=0)
    vouchers = order.get("vouchers")
    code = bool(
        isinstance(vouchers, list)
        and len(vouchers) == 1
        and isinstance(vouchers[0], dict)
        and isinstance(vouchers[0].get("code"), str)
        and vouchers[0]["code"].strip()
    )
    if state == ORDER_PAID and (not code or not paid_order_amounts_proven(order, expected_price_minor=0)):
        raise OwnerTestError("owner_test_paid_artifact_unproven")
    if state not in {ORDER_OPEN, ORDER_PAID}:
        raise OwnerTestError("owner_test_order_state_unproven")
    return str(order["uuid"]), state, code


async def interrupt_running(session_factory: Any) -> None:
    """Single executor startup: a dead attempt never returns to its queue."""
    async with session_factory() as session, session.begin():
        row = await _locked(session)
        if row and row.state in _RUNNING:
            row.state = "unknown"
            row.reason = "owner_test_execution_interrupted"
            _event(row, "interrupted")


async def _finish(
    session_factory: Any,
    *,
    state: str,
    reason: str | None = None,
    order_uuid: str | None = None,
    code_present: bool = False,
) -> dict[str, Any]:
    async with session_factory() as session, session.begin():
        row = await _locked(session)
        assert row is not None
        row.state = state
        row.reason = reason
        if order_uuid:
            row.order_uuid = UUID(order_uuid)
        row.code_present = code_present
        _event(row, "result_" + state)
        return _safe(row)


async def execute_next(session_factory: Any, *, reader: Any = None, writer: Any = None) -> dict[str, Any] | None:
    """The dedicated executor's one-shot entry point. Never called by HTTP."""
    async with session_factory() as session, session.begin():
        row = await _locked(session)
        if row is None or row.state not in _QUEUED:
            return None
        try:
            configured = _configuration()
            plan = _check_plan(row)
            stage = plan["stage"]
            if row.stopped or row.state != stage + "_queued":
                raise OwnerTestError("owner_test_stopped")
            if str(row.customer_uuid) != configured[0]:
                raise OwnerTestError("owner_test_configuration_drift")
            if row.create_attempted if stage == "create" else row.pay_attempted:
                raise OwnerTestError("owner_test_stage_unavailable")
        except OwnerTestError as exc:
            row.state, row.reason = "blocked", exc.reason
            _event(row, "refused")
            return _safe(row)
        row.state = stage + "_running"
        _event(row, "claimed_" + stage)
        binding, marker = row.binding_digest, row.marker
        order_uuid = str(row.order_uuid) if row.order_uuid else None

    attempted = False
    try:
        async with _reader(reader) as client:
            if await _prove(client, configured) != binding:
                raise OwnerTestError("owner_test_configuration_drift")
            if stage == "pay":
                if order_uuid is None:
                    raise OwnerTestError("owner_test_order_identity_unproven")
                _, state, _ = _order_proof(await client.get_order(order_uuid), configured[0], order_uuid)
                if state != ORDER_OPEN:
                    raise OwnerTestError("owner_test_unexpected_paid_account_unproven")
            # STOP and expiry are checked after all slow reads and immediately
            # before the durable mutation claim. STOP after this boundary cannot
            # cancel a request already in flight; the truthful result is retained.
            async with session_factory() as session, session.begin():
                row = await _locked(session)
                assert row is not None
                _check_plan(row)
                if row.stopped or row.state != stage + "_running":
                    raise OwnerTestError("owner_test_stopped")
                if _configuration() != configured:
                    raise OwnerTestError("owner_test_configuration_drift")
                setattr(row, stage + "_attempted", True)
                _event(row, "attempt_" + stage)
            attempted = True
            async with _writer(writer) as mutation:
                if stage == "create":
                    response = await mutation.create_voucher_order(
                        location_uuid=KARLSRUHE_LOCATION_UUID,
                        customer_uuid=configured[0],
                        staffer_uuid=configured[1],
                        voucher_template_uuid=CONTRACT.template_uuid,
                        price_minor=0,
                        marker=marker,
                        product_contract_version=CONTRACT.version,
                    )
                    body = order_object(response.envelope)
                    order_uuid = canonical_uuid(body.get("uuid")) if body else None
                    if order_uuid is None:
                        raise OwnerTestError("owner_test_order_identity_unproven")
                    # Persist only the returned identity before readback, so a
                    # failing GET never erases the exact reconciliation target.
                    async with session_factory() as session, session.begin():
                        row = await _locked(session)
                        assert row is not None
                        row.order_uuid = UUID(order_uuid)
                else:
                    await mutation.pay_voucher_order(order_uuid=order_uuid, account_uuid=configured[2])
            exact_uuid, state, code = _order_proof(await client.get_order(order_uuid), configured[0], order_uuid)
            if stage == "create" and state == ORDER_PAID:
                return await _finish(
                    session_factory,
                    state="blocked",
                    reason="owner_test_unexpected_paid_account_unproven",
                    order_uuid=exact_uuid,
                    code_present=code,
                )
            if stage == "pay" and state != ORDER_PAID:
                raise OwnerTestError("owner_test_payment_unproven")
            return await _finish(session_factory, state=state, order_uuid=exact_uuid, code_present=code)
    except OwnerTestError as exc:
        return await _finish(session_factory, state="unknown" if attempted else "blocked", reason=exc.reason)
    except Exception:
        return await _finish(
            session_factory,
            state="unknown" if attempted else "blocked",
            reason="owner_test_external_outcome_unknown" if attempted else "owner_test_live_read_failed",
        )


@asynccontextmanager
async def _writer(writer: Any = None):
    if writer is not None:
        yield writer
    else:
        async with EasyWeekVoucherMutationClient() as client:
            yield client


async def reconcile(session_factory: Any, *, principal: OpsPrincipal, reader: Any = None) -> dict[str, Any]:
    """GET-only readback, available after STOP; never replenishes an attempt."""
    _require_principal(principal)
    async with session_factory() as session:
        row = await session.get(EasyWeekVoucherOwnerTest, 1)
        if row is None or not row.create_attempted or row.state in _RUNNING | _QUEUED:
            raise OwnerTestError("owner_test_reconcile_unavailable")
        customer, marker = str(row.customer_uuid), row.marker
        exact_uuid = str(row.order_uuid) if row.order_uuid else None
        created_at, pay_attempted, revision = row.created_at, row.pay_attempted, row.updated_at
    try:
        async with asyncio.timeout(READ_TIMEOUT), _reader(reader) as client:
            if exact_uuid is None:
                match = await find_marker_orders(
                    client,
                    location_uuid=KARLSRUHE_LOCATION_UUID,
                    customer_uuid=customer,
                    marker=marker,
                    window_start=created_at - timedelta(minutes=5),
                    window_end=utcnow() + timedelta(minutes=5),
                    max_pages=5,
                )
                if not match.complete or match.count != 1 or match.order_uuid is None:
                    raise OwnerTestError("owner_test_reconcile_unproven")
                exact_uuid = match.order_uuid
            exact_uuid, state, code = _order_proof(await client.get_order(exact_uuid), customer, exact_uuid)
    except OwnerTestError:
        raise
    except Exception:
        raise OwnerTestError("owner_test_live_read_failed") from None
    async with session_factory() as session, session.begin():
        row = await _locked(session)
        assert row is not None
        if row.state in _RUNNING | _QUEUED or row.updated_at != revision:
            raise OwnerTestError("owner_test_reconcile_unavailable")
        row.order_uuid = UUID(exact_uuid)
        row.code_present = code
        if state == ORDER_PAID:
            row.state = "paid" if pay_attempted else "blocked"
            row.reason = None if pay_attempted else "owner_test_unexpected_paid_account_unproven"
        else:
            row.state = "unknown" if pay_attempted else "open"
            row.reason = "owner_test_payment_unproven" if pay_attempted else None
        _event(row, "reconcile", principal)
        return _safe(row)
