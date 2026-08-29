"""PR-11.1: the cutover end to end, against PostgreSQL.

The Altegio API is stubbed (the source of truth for *what exists* is not what is
under test here) and the EasyWeek write client is driven by a MockTransport, but
the ledger, its unique constraint, the gate and the whole runner are real.

The sequence the runbook prescribes is proven as a sequence, not as isolated
units: inventory → dry-run → canary → reconcile → bulk → dry-run → delta →
reconcile. That ordering is the product; testing each step alone would miss the
one property that matters most, which is that running it twice creates nothing
twice.
"""

from __future__ import annotations

import json
import logging

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.easyweek_migration import ledger as ledger_module
from altegio_bot.easyweek_migration import runner as runner_module
from altegio_bot.easyweek_migration.customers import CustomerDirectory
from altegio_bot.easyweek_migration.cutover import parse_cutover
from altegio_bot.easyweek_migration.gates import ApplyGateError
from altegio_bot.easyweek_migration.manifest import KARLSRUHE_COMPANY_ID, RASTATT_COMPANY_ID, parse_manifest
from altegio_bot.easyweek_migration.runner import (
    MODE_APPLY,
    MODE_DRY_RUN,
    MODE_INVENTORY,
    MODE_RECONCILE,
    MODE_ROLLBACK_DRY_RUN,
    RunInputs,
    new_run_id,
    run_apply,
    run_inventory_or_dry_run,
    run_reconcile,
    run_rollback,
)
from altegio_bot.easyweek_migration.write_client import (
    EasyWeekMigrationWriteClient,
    RateLimiter,
)
from altegio_bot.models.models import EasyWeekMigrationLedger, MessageJob, OutboxMessage
from altegio_bot.settings import settings
from altegio_bot.tests.test_easyweek_migration_planning import (
    CUSTOMER_PHONE,
    CUSTOMER_UUID,
    KA_LOCATION_UUID,
    RA_SERVICE_ID,
    RA_STAFF_ID,
    manifest_text,
    record,
)

CUTOVER = "2026-09-01T00:00:00Z"
KA_RECORD_A = 900001
KA_RECORD_B = 900002
RA_RECORD_A = 910001

CREATED_UUIDS = {
    KA_RECORD_A: "aaaaaaaa-0000-4000-8000-000000000001",
    KA_RECORD_B: "aaaaaaaa-0000-4000-8000-000000000002",
    RA_RECORD_A: "bbbbbbbb-0000-4000-8000-000000000001",
}


@pytest.fixture(autouse=True)
def _production_flags(monkeypatch: pytest.MonkeyPatch) -> None:
    """The exact flag state the runbook demands before an apply."""
    monkeypatch.setattr(settings, "easyweek_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_processing_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_notifications_enabled", False, raising=False)
    monkeypatch.setattr(settings, "easyweek_reviews_enabled", False, raising=False)
    monkeypatch.setattr(settings, "easyweek_review_send_enabled", False, raising=False)
    monkeypatch.setattr(settings, "easyweek_reminders_enabled", False, raising=False)
    monkeypatch.setattr(settings, "easyweek_visit_counter_enabled", True, raising=False)


@pytest.fixture
def source(monkeypatch: pytest.MonkeyPatch) -> dict[int, list[dict]]:
    """Stub the Altegio API. Karlsruhe and Rastatt each return their own rows.

    A Durlach entry is deliberately impossible to add: the fetch is keyed by the
    company ids the manifest names, and Durlach has none.
    """
    rows: dict[int, list[dict]] = {
        KARLSRUHE_COMPANY_ID: [record(id=KA_RECORD_A), record(id=KA_RECORD_B, date="2026-09-11 10:00:00")],
        RASTATT_COMPANY_ID: [
            record(
                id=RA_RECORD_A,
                staff_id=RA_STAFF_ID,
                services=[{"id": RA_SERVICE_ID, "cost": 90.0, "cost_to_pay": 90.0}],
            )
        ],
    }

    async def _fetch(*, company_id, window, timeout_sec=30.0, client=None):
        return list(rows.get(company_id, []))

    monkeypatch.setattr(runner_module, "fetch_company_records", _fetch)
    return rows


@pytest_asyncio.fixture
async def session_local(session_maker: async_sessionmaker[AsyncSession]) -> async_sessionmaker[AsyncSession]:
    return session_maker


def make_inputs(mode: str, **overrides) -> RunInputs:
    manifest = parse_manifest(manifest_text())
    assert manifest.valid
    kwargs = {
        "mode": mode,
        "run_id": new_run_id(),
        "cutover": parse_cutover(CUTOVER),
        "manifest": manifest,
        "directory": CustomerDirectory(valid=True, by_phone={CUSTOMER_PHONE: [CUSTOMER_UUID]}),
        "apply_requested": mode == MODE_APPLY,
        "native_notifications_confirmed": mode == MODE_APPLY,
        "cutover_supplied": True,
    }
    kwargs.update(overrides)
    return RunInputs(**kwargs)


class RecordingTransport:
    """Counts every request that actually left, and answers per source record."""

    def __init__(self, *, fail_with: dict[int, object] | None = None) -> None:
        self.requests: list[httpx.Request] = []
        self.fail_with = fail_with or {}
        self.cancelled: list[str] = []
        self.bookings: dict[str, dict] = {}

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)

        if request.method == "GET":
            uuid = request.url.path.rsplit("/", 1)[-1]
            booking = self.bookings.get(uuid)
            if booking is None:
                return httpx.Response(404)
            return httpx.Response(200, json=booking)

        if request.url.path.endswith("set-booking-cancel"):
            uuid = request.url.path.split("/")[-2]
            self.cancelled.append(uuid)
            return httpx.Response(200, json={})

        body = json.loads(request.content.decode())
        record_id = int(body["comment"].rsplit(":", 1)[-1])
        failure = self.fail_with.get(record_id)
        if isinstance(failure, Exception):
            raise failure
        if isinstance(failure, int):
            return httpx.Response(failure, json={"error": "no"})

        uuid = CREATED_UUIDS[record_id]
        self.bookings[uuid] = {
            "uuid": uuid,
            "comment": body["comment"],
            "start_time": body["start_time"],
            "is_canceled": False,
            "is_completed": False,
        }
        return httpx.Response(201, json={"uuid": uuid})

    @property
    def mutations(self) -> int:
        return sum(1 for r in self.requests if r.method == "POST")


def make_write_client(transport: RecordingTransport) -> EasyWeekMigrationWriteClient:
    async def _sleep(_delay: float) -> None:
        return None

    return EasyWeekMigrationWriteClient(
        api_key="test-key",
        workspace_slug="test-slug",
        transport=httpx.MockTransport(transport),
        sleep=_sleep,
        rate_limiter=RateLimiter(requests_per_minute=6000, sleep=_sleep),
    )


async def ledger_rows(session_local) -> list[EasyWeekMigrationLedger]:
    async with session_local() as session:
        return list(
            (
                await session.execute(
                    select(EasyWeekMigrationLedger).order_by(
                        EasyWeekMigrationLedger.source_company_id, EasyWeekMigrationLedger.source_record_id
                    )
                )
            )
            .scalars()
            .all()
        )


async def message_side_effects(session_local) -> tuple[int, int]:
    async with session_local() as session:
        jobs = (await session.execute(select(func.count()).select_from(MessageJob))).scalar_one()
        outbox = (await session.execute(select(func.count()).select_from(OutboxMessage))).scalar_one()
    return jobs, outbox


# ---------------------------------------------------------------------------
# Read-only modes
# ---------------------------------------------------------------------------


async def test_inventory_and_dry_run_write_nothing_anywhere(session_local, source):
    transport = RecordingTransport()

    async with session_local() as session:
        inventory = await run_inventory_or_dry_run(session, make_inputs(MODE_INVENTORY))
    async with session_local() as session:
        dry_run = await run_inventory_or_dry_run(session, make_inputs(MODE_DRY_RUN))

    assert inventory.as_safe_dict()["totals"]["ready"] == 3
    assert dry_run.as_safe_dict()["totals"]["ready"] == 3
    assert dry_run.mutations_attempted == 0
    # No EasyWeek request of any kind, and no ledger row.
    assert transport.requests == []
    assert await ledger_rows(session_local) == []
    assert await message_side_effects(session_local) == (0, 0)


async def test_the_dry_run_report_is_machine_readable_and_pii_free(session_local, source):
    async with session_local() as session:
        report = await run_inventory_or_dry_run(session, make_inputs(MODE_DRY_RUN))

    blob = report.to_json()
    parsed = json.loads(blob)
    assert parsed["mode"] == MODE_DRY_RUN
    assert parsed["cutover_at"] == "2026-09-01T00:00:00Z"
    assert set(parsed["totals"]) == {
        "ready",
        "created",
        "already_migrated",
        "blocked",
        "uncertain",
        "failed",
        "skipped",
    }
    assert set(parsed["by_company"]) == {str(KARLSRUHE_COMPANY_ID), str(RASTATT_COMPANY_ID)}
    assert CUSTOMER_PHONE not in blob
    assert CUSTOMER_UUID not in blob


async def test_durlach_never_appears_in_a_report(session_local, source):
    async with session_local() as session:
        report = await run_inventory_or_dry_run(session, make_inputs(MODE_DRY_RUN))
    assert "308697" not in report.as_safe_dict()["by_company"]


async def test_past_cancelled_and_completed_source_rows_are_skipped_not_migrated(session_local, source):
    source[KARLSRUHE_COMPANY_ID].extend(
        [
            record(id=900010, date="2026-08-01 10:00:00"),  # before cutover
            record(id=900011, confirmed=0),  # cancelled
            record(id=900012, attendance=1),  # completed
        ]
    )
    async with session_local() as session:
        report = await run_inventory_or_dry_run(session, make_inputs(MODE_DRY_RUN))
    totals = report.as_safe_dict()["totals"]
    assert totals["ready"] == 3
    assert totals["skipped"] == 3


# ---------------------------------------------------------------------------
# Apply
# ---------------------------------------------------------------------------


async def test_apply_without_the_gate_makes_no_mutation(session_local, source):
    """No verified dry-run id, no native attestation — nothing leaves."""
    transport = RecordingTransport()
    inputs = make_inputs(MODE_APPLY, native_notifications_confirmed=False, verified_dry_run_id=None)

    async with make_write_client(transport) as client:
        with pytest.raises(ApplyGateError):
            await run_apply(session_local, inputs, write_client=client)

    assert transport.mutations == 0
    assert await ledger_rows(session_local) == []


@pytest.mark.parametrize(
    "flag",
    ["easyweek_notifications_enabled", "easyweek_reviews_enabled"],
)
async def test_apply_is_blocked_while_a_customer_message_flag_is_on(session_local, source, monkeypatch, flag):
    monkeypatch.setattr(settings, flag, True, raising=False)
    transport = RecordingTransport()

    async with session_local() as session:
        plan = await run_inventory_or_dry_run(session, make_inputs(MODE_DRY_RUN))
    inputs = make_inputs(MODE_APPLY, verified_dry_run_id=plan.plan_digest)

    async with make_write_client(transport) as client:
        with pytest.raises(ApplyGateError):
            await run_apply(session_local, inputs, write_client=client)

    assert transport.mutations == 0


async def test_apply_creates_the_ready_bookings_and_records_them(session_local, source):
    transport = RecordingTransport()
    async with session_local() as session:
        plan = await run_inventory_or_dry_run(session, make_inputs(MODE_DRY_RUN))
    inputs = make_inputs(MODE_APPLY, verified_dry_run_id=plan.plan_digest)

    async with make_write_client(transport) as client:
        report = await run_apply(session_local, inputs, write_client=client)

    assert report.as_safe_dict()["totals"]["created"] == 3
    assert transport.mutations == 3

    rows = await ledger_rows(session_local)
    assert [row.status for row in rows] == ["created", "created", "created"]
    assert all(row.target_booking_uuid for row in rows)
    assert all(row.attempts == 1 for row in rows)
    # The whole point of PR-11.1: a schedule row, not a conversation.
    assert await message_side_effects(session_local) == (0, 0)


async def test_the_created_booking_carries_a_stable_pii_free_marker(session_local, source):
    transport = RecordingTransport()
    async with session_local() as session:
        plan = await run_inventory_or_dry_run(session, make_inputs(MODE_DRY_RUN))

    async with make_write_client(transport) as client:
        await run_apply(
            session_local, make_inputs(MODE_APPLY, verified_dry_run_id=plan.plan_digest), write_client=client
        )

    bodies = [json.loads(r.content.decode()) for r in transport.requests if r.method == "POST"]
    comments = {body["comment"] for body in bodies}
    assert f"altegio-migration:{KARLSRUHE_COMPANY_ID}:{KA_RECORD_A}" in comments
    assert all(CUSTOMER_PHONE not in comment for comment in comments)


async def test_a_second_apply_creates_no_duplicate(session_local, source):
    """The property the ledger exists for. Same source, second run, zero writes."""
    transport = RecordingTransport()
    async with session_local() as session:
        plan = await run_inventory_or_dry_run(session, make_inputs(MODE_DRY_RUN))

    async with make_write_client(transport) as client:
        await run_apply(
            session_local, make_inputs(MODE_APPLY, verified_dry_run_id=plan.plan_digest), write_client=client
        )
    first_round = transport.mutations

    # Re-plan (the ledger now knows) and apply again.
    async with session_local() as session:
        second_plan = await run_inventory_or_dry_run(session, make_inputs(MODE_DRY_RUN))
    async with make_write_client(transport) as client:
        second = await run_apply(
            session_local, make_inputs(MODE_APPLY, verified_dry_run_id=second_plan.plan_digest), write_client=client
        )

    assert transport.mutations == first_round == 3
    assert second.as_safe_dict()["totals"]["already_migrated"] == 3
    assert second.as_safe_dict()["totals"]["created"] == 0
    assert len(await ledger_rows(session_local)) == 3


async def test_the_ledger_source_identity_is_unique(session_local):
    """A second row for one source booking is a database error, not a policy."""
    async with session_local() as session:
        async with session.begin():
            claimed = await ledger_module.claim_for_apply(
                session,
                run_id="run-a",
                source_company_id=KARLSRUHE_COMPANY_ID,
                source_record_id=KA_RECORD_A,
                source_fingerprint="fp",
            )
            assert claimed

    async with session_local() as session:
        async with session.begin():
            await ledger_module.record_created(
                session,
                run_id="run-a",
                source_company_id=KARLSRUHE_COMPANY_ID,
                source_record_id=KA_RECORD_A,
                target_booking_uuid=CREATED_UUIDS[KA_RECORD_A],
            )

    # A `created` row can never be re-claimed, so no second POST is possible.
    async with session_local() as session:
        async with session.begin():
            again = await ledger_module.claim_for_apply(
                session,
                run_id="run-b",
                source_company_id=KARLSRUHE_COMPANY_ID,
                source_record_id=KA_RECORD_A,
                source_fingerprint="fp",
            )
    assert again is False
    assert len(await ledger_rows(session_local)) == 1


async def test_a_timeout_becomes_uncertain_and_halts_the_run(session_local, source):
    """No blind retry, and no further writes while the last outcome is unknown."""
    timeout = httpx.ReadTimeout("timed out", request=httpx.Request("POST", "https://my.easyweek.io/"))
    transport = RecordingTransport(fail_with={KA_RECORD_A: timeout})

    async with session_local() as session:
        plan = await run_inventory_or_dry_run(session, make_inputs(MODE_DRY_RUN))
    async with make_write_client(transport) as client:
        report = await run_apply(
            session_local, make_inputs(MODE_APPLY, verified_dry_run_id=plan.plan_digest), write_client=client
        )

    safe = report.as_safe_dict()
    assert safe["totals"]["uncertain"] == 1
    assert safe["totals"]["created"] == 0
    # Exactly ONE POST: the timed-out one was not repeated, and the run stopped.
    assert transport.mutations == 1
    assert any("reconcile" in err for err in safe["errors"])

    rows = await ledger_rows(session_local)
    uncertain = [row for row in rows if row.status == "uncertain"]
    assert len(uncertain) == 1
    assert uncertain[0].target_booking_uuid is None


async def test_an_uncertain_row_blocks_the_next_apply_until_reconciled(session_local, source):
    timeout = httpx.ReadTimeout("timed out", request=httpx.Request("POST", "https://my.easyweek.io/"))
    transport = RecordingTransport(fail_with={KA_RECORD_A: timeout})
    async with session_local() as session:
        plan = await run_inventory_or_dry_run(session, make_inputs(MODE_DRY_RUN))
    async with make_write_client(transport) as client:
        await run_apply(
            session_local, make_inputs(MODE_APPLY, verified_dry_run_id=plan.plan_digest), write_client=client
        )

    async with session_local() as session:
        replan = await run_inventory_or_dry_run(session, make_inputs(MODE_DRY_RUN))
    blocked = [row for row in replan.blocked_rows if row["source_record_id"] == KA_RECORD_A]
    assert blocked and blocked[0]["reason"] == "ledger_uncertain_needs_reconcile"


async def test_reconcile_reports_an_unresolvable_uncertain_row_without_guessing(session_local, source):
    timeout = httpx.ReadTimeout("timed out", request=httpx.Request("POST", "https://my.easyweek.io/"))
    transport = RecordingTransport(fail_with={KA_RECORD_A: timeout})
    async with session_local() as session:
        plan = await run_inventory_or_dry_run(session, make_inputs(MODE_DRY_RUN))
    async with make_write_client(transport) as client:
        await run_apply(
            session_local, make_inputs(MODE_APPLY, verified_dry_run_id=plan.plan_digest), write_client=client
        )

    async with make_write_client(transport) as client:
        report = await run_reconcile(session_local, make_inputs(MODE_RECONCILE), write_client=client)

    safe = report.as_safe_dict()
    assert safe["reason_codes"]["uncertain_unresolved"] == 1
    # It stays uncertain. Neither "it worked" nor "it did not" was proven.
    rows = await ledger_rows(session_local)
    assert [row.status for row in rows if row.source_record_id == KA_RECORD_A] == ["uncertain"]


async def test_a_permanent_4xx_fails_the_row_and_leaves_the_others_alone(session_local, source):
    transport = RecordingTransport(fail_with={KA_RECORD_A: 422})
    async with session_local() as session:
        plan = await run_inventory_or_dry_run(session, make_inputs(MODE_DRY_RUN))
    async with make_write_client(transport) as client:
        report = await run_apply(
            session_local, make_inputs(MODE_APPLY, verified_dry_run_id=plan.plan_digest), write_client=client
        )

    safe = report.as_safe_dict()
    assert safe["totals"]["failed"] == 1
    assert safe["totals"]["created"] == 2  # the run continued
    # One attempt for the rejected row: a permanent 4xx is never retried.
    posts = [r for r in transport.requests if r.method == "POST"]
    assert len(posts) == 3


async def test_a_failed_row_may_be_retried_once_the_cause_is_fixed(session_local, source):
    transport = RecordingTransport(fail_with={KA_RECORD_A: 422})
    async with session_local() as session:
        plan = await run_inventory_or_dry_run(session, make_inputs(MODE_DRY_RUN))
    async with make_write_client(transport) as client:
        await run_apply(
            session_local, make_inputs(MODE_APPLY, verified_dry_run_id=plan.plan_digest), write_client=client
        )

    healthy = RecordingTransport()
    async with session_local() as session:
        replan = await run_inventory_or_dry_run(session, make_inputs(MODE_DRY_RUN))
    async with make_write_client(healthy) as client:
        report = await run_apply(
            session_local, make_inputs(MODE_APPLY, verified_dry_run_id=replan.plan_digest), write_client=client
        )

    assert report.as_safe_dict()["totals"]["created"] == 1
    assert healthy.mutations == 1


# ---------------------------------------------------------------------------
# The prescribed sequence: canary → reconcile → bulk → dry-run → delta
# ---------------------------------------------------------------------------


async def test_the_full_canary_bulk_delta_sequence(session_local, source):
    transport = RecordingTransport()

    # 1. dry-run
    async with session_local() as session:
        plan = await run_inventory_or_dry_run(session, make_inputs(MODE_DRY_RUN))
    assert plan.as_safe_dict()["totals"]["ready"] == 3

    # 2. canary: exactly one booking.
    async with make_write_client(transport) as client:
        canary = await run_apply(
            session_local,
            make_inputs(MODE_APPLY, verified_dry_run_id=plan.plan_digest, limit=1),
            write_client=client,
        )
    assert canary.as_safe_dict()["totals"]["created"] == 1
    assert transport.mutations == 1
    # The rest are reported as still ready, not silently dropped.
    assert canary.as_safe_dict()["totals"]["ready"] == 2

    # 3. reconciliation of the canary: nothing uncertain.
    async with make_write_client(transport) as client:
        recon = await run_reconcile(session_local, make_inputs(MODE_RECONCILE), write_client=client)
    assert recon.as_safe_dict()["totals"].get("uncertain", 0) == 0

    # 4. bulk apply of the remainder.
    async with session_local() as session:
        plan2 = await run_inventory_or_dry_run(session, make_inputs(MODE_DRY_RUN))
    async with make_write_client(transport) as client:
        bulk = await run_apply(
            session_local, make_inputs(MODE_APPLY, verified_dry_run_id=plan2.plan_digest), write_client=client
        )
    assert bulk.as_safe_dict()["totals"]["created"] == 2
    assert bulk.as_safe_dict()["totals"]["already_migrated"] == 1

    # 5. a new booking appears in Altegio after the bulk — the delta case.
    source[KARLSRUHE_COMPANY_ID].append(record(id=900003, date="2026-09-20 09:00:00"))
    CREATED_UUIDS[900003] = "aaaaaaaa-0000-4000-8000-000000000003"

    # 6. the old digest no longer matches: the gate refuses a stale plan.
    async with make_write_client(transport) as client:
        with pytest.raises(ApplyGateError):
            await run_apply(
                session_local, make_inputs(MODE_APPLY, verified_dry_run_id=plan2.plan_digest), write_client=client
            )

    # 7. re-run dry-run, then delta apply.
    async with session_local() as session:
        plan3 = await run_inventory_or_dry_run(session, make_inputs(MODE_DRY_RUN))
    async with make_write_client(transport) as client:
        delta = await run_apply(
            session_local, make_inputs(MODE_APPLY, verified_dry_run_id=plan3.plan_digest), write_client=client
        )
    assert delta.as_safe_dict()["totals"]["created"] == 1
    assert transport.mutations == 4  # 1 canary + 2 bulk + 1 delta, never more

    # 8. final reconciliation.
    async with make_write_client(transport) as client:
        final = await run_reconcile(session_local, make_inputs(MODE_RECONCILE), write_client=client)
    assert final.as_safe_dict()["totals"]["created"] == 4
    assert await message_side_effects(session_local) == (0, 0)


# ---------------------------------------------------------------------------
# Rollback
# ---------------------------------------------------------------------------


async def test_rollback_is_read_only_by_default(session_local, source):
    transport = RecordingTransport()
    async with session_local() as session:
        plan = await run_inventory_or_dry_run(session, make_inputs(MODE_DRY_RUN))
    apply_inputs = make_inputs(MODE_APPLY, verified_dry_run_id=plan.plan_digest)
    async with make_write_client(transport) as client:
        await run_apply(session_local, apply_inputs, write_client=client)

    async with make_write_client(transport) as client:
        report = await run_rollback(
            session_local,
            make_inputs(MODE_ROLLBACK_DRY_RUN, rollback_run_id=apply_inputs.run_id, rollback_confirmed=False),
            write_client=client,
        )

    assert report.as_safe_dict()["reason_codes"]["rollback_eligible"] == 3
    assert transport.cancelled == []
    assert report.mutations_attempted == 0
    rows = await ledger_rows(session_local)
    assert all(row.status == "created" for row in rows)


async def test_rollback_only_touches_its_own_run(session_local, source):
    transport = RecordingTransport()
    async with session_local() as session:
        plan = await run_inventory_or_dry_run(session, make_inputs(MODE_DRY_RUN))
    apply_inputs = make_inputs(MODE_APPLY, verified_dry_run_id=plan.plan_digest)
    async with make_write_client(transport) as client:
        await run_apply(session_local, apply_inputs, write_client=client)

    async with make_write_client(transport) as client:
        report = await run_rollback(
            session_local,
            make_inputs(MODE_ROLLBACK_DRY_RUN, rollback_run_id="some-other-run", rollback_confirmed=False),
            write_client=client,
        )
    assert report.as_safe_dict()["reason_codes"] == {}


async def test_a_confirmed_rollback_cancels_and_records(session_local, source):
    transport = RecordingTransport()
    async with session_local() as session:
        plan = await run_inventory_or_dry_run(session, make_inputs(MODE_DRY_RUN))
    apply_inputs = make_inputs(MODE_APPLY, verified_dry_run_id=plan.plan_digest)
    async with make_write_client(transport) as client:
        await run_apply(session_local, apply_inputs, write_client=client)

    async with make_write_client(transport) as client:
        await run_rollback(
            session_local,
            make_inputs(MODE_ROLLBACK_DRY_RUN, rollback_run_id=apply_inputs.run_id, rollback_confirmed=True),
            write_client=client,
        )

    assert len(transport.cancelled) == 3
    rows = await ledger_rows(session_local)
    assert all(row.status == "rolled_back" for row in rows)
    # The target uuid survives: an operator must still be able to say what was cancelled.
    assert all(row.target_booking_uuid for row in rows)


async def test_a_booking_edited_by_hand_after_migration_is_never_rolled_back(session_local, source):
    transport = RecordingTransport()
    async with session_local() as session:
        plan = await run_inventory_or_dry_run(session, make_inputs(MODE_DRY_RUN))
    apply_inputs = make_inputs(MODE_APPLY, verified_dry_run_id=plan.plan_digest)
    async with make_write_client(transport) as client:
        await run_apply(session_local, apply_inputs, write_client=client)

    # Somebody rewrote the comment in the EasyWeek UI.
    edited = CREATED_UUIDS[KA_RECORD_A]
    transport.bookings[edited]["comment"] = "moved to Thursday, called the client"

    async with make_write_client(transport) as client:
        report = await run_rollback(
            session_local,
            make_inputs(MODE_ROLLBACK_DRY_RUN, rollback_run_id=apply_inputs.run_id, rollback_confirmed=True),
            write_client=client,
        )

    codes = report.as_safe_dict()["reason_codes"]
    assert codes["rollback_target_modified_after_migration"] == 1
    assert edited not in transport.cancelled
    assert len(transport.cancelled) == 2


# ---------------------------------------------------------------------------
# Log hygiene
# ---------------------------------------------------------------------------


async def test_normal_logs_carry_no_pii(session_local, source, caplog):
    transport = RecordingTransport()
    with caplog.at_level(logging.INFO):
        async with session_local() as session:
            plan = await run_inventory_or_dry_run(session, make_inputs(MODE_DRY_RUN))
        async with make_write_client(transport) as client:
            await run_apply(
                session_local, make_inputs(MODE_APPLY, verified_dry_run_id=plan.plan_digest), write_client=client
            )

    text = "\n".join(record.getMessage() for record in caplog.records)
    assert CUSTOMER_PHONE not in text
    assert CUSTOMER_UUID not in text
    assert "test-key" not in text
    # Not even the target identifiers: a log line is not a report, and the
    # report is where identifiers belong.
    assert KA_LOCATION_UUID not in text
    assert CREATED_UUIDS[KA_RECORD_A] not in text


async def test_a_crashed_claim_is_never_re_sent(session_local, source):
    """The crash hole: a process that died around its POST leaves `pending`.

    A `pending` row may correspond to a booking that exists, so it is not
    re-claimable and not re-classified as ready. Only `blocked` and `failed` —
    where nothing was sent — may be tried again.
    """
    async with session_local() as session:
        async with session.begin():
            assert await ledger_module.claim_for_apply(
                session,
                run_id="crashed-run",
                source_company_id=KARLSRUHE_COMPANY_ID,
                source_record_id=KA_RECORD_A,
                source_fingerprint="fp",
            )
    # ... and the process dies here, having possibly sent the POST.

    async with session_local() as session:
        async with session.begin():
            reclaimed = await ledger_module.claim_for_apply(
                session,
                run_id="next-run",
                source_company_id=KARLSRUHE_COMPANY_ID,
                source_record_id=KA_RECORD_A,
                source_fingerprint="fp",
            )
    assert reclaimed is False

    transport = RecordingTransport()
    async with session_local() as session:
        plan = await run_inventory_or_dry_run(session, make_inputs(MODE_DRY_RUN))
    blocked = [row for row in plan.blocked_rows if row["source_record_id"] == KA_RECORD_A]
    assert blocked and blocked[0]["reason"] == "ledger_uncertain_needs_reconcile"

    async with make_write_client(transport) as client:
        report = await run_apply(
            session_local, make_inputs(MODE_APPLY, verified_dry_run_id=plan.plan_digest), write_client=client
        )
    # The other two bookings still go through; only the crashed one is held.
    assert report.as_safe_dict()["totals"]["created"] == 2
    assert KA_RECORD_A not in {row["source_record_id"] for row in report.created_rows}


async def test_reconcile_surfaces_a_crashed_claim(session_local, source):
    async with session_local() as session:
        async with session.begin():
            await ledger_module.claim_for_apply(
                session,
                run_id="crashed-run",
                source_company_id=KARLSRUHE_COMPANY_ID,
                source_record_id=KA_RECORD_A,
                source_fingerprint="fp",
            )

    transport = RecordingTransport()
    async with make_write_client(transport) as client:
        report = await run_reconcile(session_local, make_inputs(MODE_RECONCILE), write_client=client)

    assert report.as_safe_dict()["reason_codes"]["uncertain_unresolved"] == 1
