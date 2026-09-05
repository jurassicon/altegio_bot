"""Regression proofs for the operator's frozen wave, using real PostgreSQL."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from sqlalchemy import event, select, text, update
from sqlalchemy.exc import DBAPIError

from altegio_bot.easyweek_migration import reminder_handover_db as db
from altegio_bot.easyweek_migration.reminder_handover import (
    SnapshotError,
    confirmation_phrase,
    freeze_plan,
    read_apply_report,
    read_snapshot,
    write_apply_report,
    write_snapshot,
)
from altegio_bot.models.models import Client, EasyWeekMigrationLedger, MessageJob, Record
from altegio_bot.scripts import easyweek_reminder_handover as cli
from altegio_bot.settings import settings
from altegio_bot.tests import test_easyweek_reminder_handover_db as h
from altegio_bot.tests.test_easyweek_reminder_handover import handover_row, owed, plan_with

registry = h.registry
seeded = h.seeded


async def plan(session_maker, seeded, *, runs=("run-1",), client=None):
    async with session_maker() as session:
        return await db.build_plan(
            session,
            manifest=h.wave_manifest(),
            company_ids=(h.COMPANY,),
            run_ids=runs,
            client=client or h.FakeBookings(h.booking_body(seeded["starts"])),
            sleep=h._no_sleep,
        )


async def test_postgres_enforces_read_only_before_first_select(engine, session_maker, seeded):
    statements = []

    def observe(conn, cursor, statement, parameters, context, many):
        statements.append(statement)

    event.listen(engine.sync_engine, "before_cursor_execute", observe)
    try:
        async with session_maker() as session:
            result = await db.build_plan(
                session,
                manifest=h.wave_manifest(),
                company_ids=(h.COMPANY,),
                run_ids=("run-1",),
                client=h.FakeBookings(h.booking_body(seeded["starts"])),
            )
            assert result.cutover_ready
            assert statements[0] == "SET TRANSACTION READ ONLY"
            assert await session.scalar(text("SHOW transaction_read_only")) == "on"
            with pytest.raises(DBAPIError):
                await session.execute(text("UPDATE records SET is_deleted = true"))
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", observe)


async def test_other_run_is_neither_read_from_api_nor_locked(session_maker, seeded):
    await h.add_migrated_pair(
        session_maker, source_record_id=900002, booking_uuid=h.BOOKING_TWO, starts_at=seeded["starts"]
    )
    client = h.FakeBookings(h.booking_body(seeded["starts"]))
    planned = await plan(session_maker, seeded, client=client)
    assert planned.ledger_rows_seen == 1
    assert client.calls == [str(h.BOOKING)]
    async with session_maker() as blocker:
        async with blocker.begin():
            await blocker.execute(
                select(EasyWeekMigrationLedger).where(EasyWeekMigrationLedger.run_id == "run-2").with_for_update()
            )
            result = await asyncio.wait_for(h.run_apply(session_maker, planned), timeout=3)
            assert result.halted is None
    async with session_maker() as session:
        other = await session.scalar(select(EasyWeekMigrationLedger).where(EasyWeekMigrationLedger.run_id == "run-2"))
        assert other.reminders_handed_over_at is None


@pytest.mark.parametrize("change", ["category", "client", "staff", "provider"])
async def test_local_refusals_spend_no_api_request(change, session_maker, seeded):
    async with session_maker() as session, session.begin():
        target = await session.get(Record, seeded["target_pk"])
        source = await session.get(Record, seeded["source_pk"])
        if change == "category":
            target.raw = {"easyweek": {"service_category": "Nagelservice", "services_count": 1}}
        elif change == "client":
            target.client_id = 10
        elif change == "staff":
            source.staff_id = 5003
        else:
            entry = await session.scalar(select(EasyWeekMigrationLedger))
            entry.target_provider = "altegio"
    client = h.FakeBookings(h.booking_body(seeded["starts"]))
    result = await plan(session_maker, seeded, client=client)
    assert not result.cutover_ready and result.eligible_refusals
    assert client.calls == []


async def test_plan_detects_changes_during_live_walk(session_maker, seeded):
    class ChangingApi:
        async def get_booking(self, booking_uuid):
            async with session_maker() as session, session.begin():
                await session.execute(update(Client).where(Client.id == 1).values(display_name="changed privately"))
            return h.booking_body(seeded["starts"])

    result = await plan(session_maker, seeded, client=ChangingApi())
    assert result.candidate_set_changed
    assert not result.cutover_ready
    assert (await h.run_apply(session_maker, result)).halted is not None


@pytest.mark.parametrize(
    "change", ["client", "target_client", "source_identity", "source_payload", "ledger", "category"]
)
async def test_frozen_local_fact_drift_rolls_back(change, session_maker, seeded):
    old = await h.add_job(
        session_maker,
        provider="altegio",
        record_pk=seeded["source_pk"],
        job_type="reminder_24h",
        status="queued",
        dedupe_key="source-old",
    )
    planned = await plan(session_maker, seeded)
    async with session_maker() as session, session.begin():
        if change == "client":
            client = await session.get(Client, 1)
            client.phone_e164 = "+490000000000"
        elif change == "target_client":
            target = await session.get(Record, seeded["target_pk"])
            target.client_id = 10
        elif change == "source_identity":
            source = await session.get(Record, seeded["source_pk"])
            source.altegio_record_id += 1
        elif change == "source_payload":
            job = await session.get(MessageJob, old)
            job.payload = {"changed": True}
        elif change == "ledger":
            entry = await session.scalar(select(EasyWeekMigrationLedger))
            entry.source_fingerprint = "b" * 64
        else:
            target = await session.get(Record, seeded["target_pk"])
            target.raw = {}
    result = await h.run_apply(session_maker, planned)
    assert result.halted
    assert [(job.id, job.status) for job in await h.jobs(session_maker)] == [(old, "queued")]
    async with session_maker() as session:
        assert (await session.scalar(select(EasyWeekMigrationLedger))).reminders_handed_over_at is None


async def test_configuration_drift_blocks_apply(session_maker, seeded, monkeypatch):
    planned = await plan(session_maker, seeded)
    monkeypatch.setattr(settings, "easyweek_allowed_service_categories", '["Nagelservice"]')
    assert (await h.run_apply(session_maker, planned)).halted == "configuration_changed"


@pytest.mark.parametrize("status", ["queued", "processing", "done"])
async def test_wrong_client_never_counts_as_target_coverage(status, session_maker, seeded):
    planned = await plan(session_maker, seeded)
    item = planned.rows[0].obligations[0]
    job_id = await h.add_job(
        session_maker,
        provider="easyweek",
        record_pk=seeded["target_pk"],
        job_type=item.job_type,
        status=status,
        dedupe_key=item.dedupe_key,
    )
    async with session_maker() as session, session.begin():
        job = await session.get(MessageJob, job_id)
        job.client_id = 10
    assert not (await plan(session_maker, seeded)).cutover_ready
    assert (await h.run_apply(session_maker, planned)).halted is not None


async def test_two_actual_concurrent_applies_have_one_mutation_set(session_maker, seeded):
    planned = await plan(session_maker, seeded)
    results = await asyncio.gather(h.run_apply(session_maker, planned), h.run_apply(session_maker, planned))
    assert all(result.halted is None for result in results)
    assert sorted(len(result.created_job_ids) for result in results) == [0, 2]
    assert len(await h.jobs(session_maker)) == 2


@pytest.mark.parametrize("timeout_kind", ["lock", "statement"])
async def test_database_timeouts_rollback(timeout_kind, session_maker, seeded):
    planned = await plan(session_maker, seeded)
    async with session_maker() as blocker, blocker.begin():
        await blocker.execute(select(Record).where(Record.id == seeded["target_pk"]).with_for_update())
        async with session_maker() as session, session.begin():
            result = await db.apply_plan(
                session,
                freeze_plan(planned),
                lock_timeout_ms=50 if timeout_kind == "lock" else 5000,
                statement_timeout_ms=50 if timeout_kind == "statement" else 5000,
            )
        assert result.halted == f"database_{timeout_kind}_timeout"
    assert await h.jobs(session_maker) == []


@pytest.mark.parametrize("expired", ["snapshot", "boundary"])
async def test_clock_is_rechecked_after_real_lock_wait(expired, session_maker, seeded, monkeypatch):
    planned = await plan(session_maker, seeded)
    from altegio_bot.easyweek_migration import reminder_handover_db

    now = datetime.now(timezone.utc)
    if expired == "snapshot":
        planned.created_at = now - timedelta(seconds=3599)
    else:
        now = planned.rows[0].obligations[0].run_at - timedelta(seconds=1)
    entered = asyncio.Event()
    original = reminder_handover_db._lock_scope

    async def locking(*args):
        entered.set()
        return await original(*args)

    monkeypatch.setattr(reminder_handover_db, "_lock_scope", locking)
    async with session_maker() as blocker:
        await blocker.execute(select(Record).where(Record.id == seeded["target_pk"]).with_for_update())
        task = asyncio.create_task(h.run_apply(session_maker, planned, now=now))
        try:
            await asyncio.wait_for(entered.wait(), timeout=3)
            await asyncio.sleep(1.1)
            await blocker.rollback()
            result = await asyncio.wait_for(task, timeout=3)
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
    assert result.halted == ("snapshot_expired" if expired == "snapshot" else "reminder_boundary_passed")
    assert await h.jobs(session_maker) == []


async def test_replay_report_can_be_read_and_verified(session_maker, seeded, tmp_path):
    planned = await plan(session_maker, seeded)
    await h.run_apply(session_maker, planned)
    result = await h.run_apply(session_maker, planned)
    frozen = freeze_plan(planned)
    report = result.apply_report(frozen, applied_at=datetime.now(timezone.utc))
    path = write_apply_report(report, tmp_path / "repeat.json")
    restored = read_apply_report(path, frozen=frozen)
    async with session_maker() as session, session.begin():
        await session.execute(text("SET TRANSACTION READ ONLY"))
        assert (await db.verify_handover(session, frozen, restored))["passed"]


async def test_early_plan_failure_invalidates_old_snapshot(tmp_path, monkeypatch):
    snapshot = tmp_path / "plan.json"
    snapshot.write_text("previous authority")
    args = cli.build_parser().parse_args(
        [
            "plan",
            "--company-id",
            "758285",
            "--run-id",
            "run-1",
            "--manifest",
            str(tmp_path / "missing.json"),
            "--snapshot",
            str(snapshot),
        ]
    )
    assert await cli._run(args) == 1
    assert not snapshot.exists()


@pytest.mark.parametrize("field", ["manifest_digest", "configuration_digest", "candidate_fingerprint", "run_ids"])
def test_wave_authority_is_in_digest(field, tmp_path):
    planned = plan_with(handover_row(obligations=owed(48)))
    path = write_snapshot(planned, tmp_path / "plan.json")
    data = json.loads(path.read_text())
    data["wave"][field] = ["run-other"] if field == "run_ids" else "b" * 64
    path.write_text(json.dumps(data))
    with pytest.raises(SnapshotError):
        read_snapshot(path)


def test_ci_cannot_skip_required_handover_proofs():
    workflow = Path(__file__).resolve().parents[3] / ".github/workflows/ci_deploy.yml"
    body = workflow.read_text()
    step = body.split("- name: Run required reminder handover tests\n", 1)[1].split("      - name:", 1)[0]
    assert 'REQUIRE_PG_CONCURRENCY: "1"' in step
    for suffix in ("", "_db", "_safety"):
        assert f"src/altegio_bot/tests/test_easyweek_reminder_handover{suffix}.py" in step
    assert all(fragment not in step for fragment in ("continue-on-error", "if:", " -k ", " -m ", "|| true"))


@pytest.mark.parametrize("gate", ["flag", "environment", "digest", "confirmation", "manifest", "run"])
async def test_incomplete_apply_permission_never_opens_write_session(
    gate, session_maker, seeded, tmp_path, monkeypatch
):
    planned = await plan(session_maker, seeded)
    snapshot = write_snapshot(planned, tmp_path / "plan.json")
    manifest = tmp_path / "manifest.json"
    manifest.write_text(h.manifest_json())
    args = cli.build_parser().parse_args(
        [
            "apply",
            "--company-id",
            str(h.COMPANY),
            "--run-id",
            "run-1",
            "--manifest",
            str(manifest),
            "--snapshot",
            str(snapshot),
            "--apply",
            "--plan-digest",
            planned.digest(),
            "--confirm",
            confirmation_phrase(planned.digest()),
        ]
    )
    monkeypatch.setenv(cli.APPLY_ENV_FLAG, "true")
    if gate == "flag":
        args.apply = False
    elif gate == "environment":
        monkeypatch.delenv(cli.APPLY_ENV_FLAG)
    elif gate == "digest":
        args.plan_digest = "b" * 64
    elif gate == "confirmation":
        args.confirm = "wrong"
    elif gate == "manifest":
        manifest.write_text("{}")
    else:
        args.run_id = ["other-run"]

    def forbidden():
        pytest.fail("unauthorised apply opened a database session")

    monkeypatch.setattr(cli, "SessionLocal", forbidden)
    assert await cli._run(args) == 1


async def test_live_verify_uses_scoped_runtime_proof(session_maker, seeded):
    planned = await plan(session_maker, seeded)
    async with session_maker() as session:
        good = h.FakeBookings(h.booking_body(seeded["starts"]))
        assert await db.verify_live_scope(session, freeze_plan(planned), client=good)
        assert good.calls == [str(h.BOOKING)]
        moved = h.FakeBookings(h.booking_body(seeded["starts"] + timedelta(hours=1)))
        assert not await db.verify_live_scope(session, freeze_plan(planned), client=moved)


def test_recomputed_digest_cannot_authorise_missing_obligations(tmp_path):
    from altegio_bot.easyweek_migration.handover_evidence import digest

    planned = plan_with(handover_row(obligations=owed(48)))
    path = write_snapshot(planned, tmp_path / "plan.json")
    data = json.loads(path.read_text())
    data["rows"][0]["obligations"] = []
    data["obligation_outcomes"] = {}
    data["readiness"]["coverage_ready"] = True
    data.pop("plan_digest")
    data["plan_digest"] = digest(data)
    path.write_text(json.dumps(data))
    with pytest.raises(SnapshotError, match="snapshot_obligations_incomplete"):
        read_snapshot(path)


async def test_cli_plan_apply_verify_with_real_postgres(session_maker, seeded, tmp_path, monkeypatch, capsys):
    manifest = tmp_path / "manifest.json"
    manifest.write_text(h.manifest_json())
    common = [
        "--company-id",
        str(h.COMPANY),
        "--run-id",
        "run-1",
        "--manifest",
        str(manifest),
        "--snapshot",
        str(tmp_path / "plan.json"),
        "--apply-report",
        str(tmp_path / "report.json"),
    ]

    class Api(h.FakeBookings):
        async def aclose(self):
            pass

    api = Api(h.booking_body(seeded["starts"]))

    def create_client(*, max_attempts):
        assert max_attempts == 1
        return api

    monkeypatch.setattr(cli, "EasyWeekClient", create_client)
    monkeypatch.setattr(cli, "SessionLocal", session_maker)
    parser = cli.build_parser()
    assert await cli._run(parser.parse_args(["plan", *common])) == 0
    frozen = read_snapshot(tmp_path / "plan.json")
    assert api.calls == [str(h.BOOKING)]
    monkeypatch.setenv(cli.APPLY_ENV_FLAG, "true")
    assert (
        await cli._run(
            parser.parse_args(
                [
                    "apply",
                    *common,
                    "--apply",
                    "--plan-digest",
                    frozen.digest,
                    "--confirm",
                    confirmation_phrase(frozen.digest),
                ]
            )
        )
        == 0
    )
    assert api.calls == [str(h.BOOKING)], "apply is strictly network-free"
    capsys.readouterr()
    assert await cli._run(parser.parse_args(["verify", *common])) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["passed"] and report["api_guard_ready"]
    assert report["uncovered_obligations"] == 0 and report["scope_drift"] is None
    assert api.calls == [str(h.BOOKING), str(h.BOOKING)]
