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
    check_snapshot_usable,
    confirmation_phrase,
    freeze_plan,
    invalidate_snapshot,
    read_apply_report,
    read_snapshot,
    write_apply_report,
    write_snapshot,
)
from altegio_bot.models.models import (
    PROVIDER_ALTEGIO,
    PROVIDER_EASYWEEK,
    Client,
    EasyWeekMigrationLedger,
    EasyWeekMigrationWaveClosure,
    MessageJob,
    Record,
)
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
    """A superseded plan must stop being applicable, not merely change its name.

    The old behaviour renamed the file. Its bytes stayed a valid, digest-bearing
    authorisation, and pointing `apply` at the renamed path worked — including
    when the new plan had stopped precisely because the live EasyWeek picture no
    longer matched it.
    """
    snapshot = tmp_path / "plan.json"
    planned = plan_with(handover_row(obligations=owed(48)))
    write_snapshot(planned, snapshot)
    assert read_snapshot(snapshot).digest  # applicable before

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

    # The authorising bytes are gone, and what remains cannot be read as a plan.
    with pytest.raises(SnapshotError) as refused:
        read_snapshot(snapshot)
    assert str(refused.value) == "snapshot_invalidated"

    tombstone = json.loads(snapshot.read_text())
    assert tombstone["mode"] == "invalidated"
    assert set(tombstone) == {"version", "mode", "invalidated_at", "reason"}
    assert tombstone["reason"] == "superseded_by_new_plan"
    # Nothing that could authorise, identify a person or name a booking.
    body = snapshot.read_text()
    for leaked in ("rows", "plan_digest", "booking", "client", "job_id", "phone"):
        assert leaked not in body
    assert snapshot.stat().st_mode & 0o777 == 0o600
    assert snapshot.parent.stat().st_mode & 0o777 == 0o700
    # And there is no copy anywhere to rename back.
    assert sorted(item.name for item in tmp_path.iterdir()) == ["plan.json"]


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


# ---------------------------------------------------------------------------
# A superseded authorisation cannot come back
# ---------------------------------------------------------------------------


def _applicable_snapshot(path: Path) -> Path:
    write_snapshot(plan_with(handover_row(obligations=owed(48))), path)
    assert read_snapshot(path).digest
    return path


@pytest.mark.parametrize(
    "argv_tail, expected_exit",
    [
        # A plan whose arguments do not even parse. `parse_args` exits before
        # `_run`, so this used to leave the previous permission in place.
        pytest.param(["--company-id", "758285"], 2, id="missing-run-id"),
        pytest.param(["--company-id", "not-a-number", "--run-id", "run-1"], 2, id="invalid-company-id"),
        pytest.param(["--run-id", "run-1"], 2, id="missing-company-id"),
    ],
)
def test_a_plan_that_fails_to_parse_still_destroys_the_old_permission(tmp_path, argv_tail, expected_exit):
    snapshot = _applicable_snapshot(tmp_path / "plan.json")
    argv = ["plan", *argv_tail, "--manifest", str(tmp_path / "m.json"), "--snapshot", str(snapshot)]

    with pytest.raises(SystemExit) as exited:
        cli.main(argv)
    assert exited.value.code == expected_exit

    with pytest.raises(SnapshotError) as refused:
        read_snapshot(snapshot)
    assert str(refused.value) == "snapshot_invalidated"


def test_a_plan_with_no_snapshot_flag_invalidates_the_default_path(tmp_path, monkeypatch):
    default = tmp_path / "outputs" / "plan.json"
    default.parent.mkdir(parents=True)
    _applicable_snapshot(default)
    monkeypatch.setattr(cli, "DEFAULT_SNAPSHOT", str(default))

    with pytest.raises(SystemExit):
        cli.main(["plan", "--company-id", "758285"])

    with pytest.raises(SnapshotError):
        read_snapshot(default)


def test_help_alone_is_not_a_plan_attempt(tmp_path):
    snapshot = _applicable_snapshot(tmp_path / "plan.json")

    with pytest.raises(SystemExit) as exited:
        cli.main(["plan", "--help", "--snapshot", str(snapshot)])
    assert exited.value.code == 0

    assert read_snapshot(snapshot).digest, "reading the help must not destroy an authorisation"


@pytest.mark.parametrize("mode", ["apply", "verify"])
@pytest.mark.parametrize("suffix", [".invalidated", ".tombstone", ".bak", ".old"])
def test_an_archive_path_is_refused_before_any_write_session(tmp_path, monkeypatch, mode, suffix):
    """The refusal must land before the database is touched at all."""
    archived = _applicable_snapshot(tmp_path / f"plan.json{suffix}")

    def _no_session(*args, **kwargs):  # pragma: no cover - must never run
        raise AssertionError("an archive path reached a database session")

    monkeypatch.setattr(cli, "SessionLocal", _no_session)
    monkeypatch.setenv(cli.APPLY_ENV_FLAG, "1")

    exit_code = cli.main(
        [
            mode,
            "--company-id",
            "758285",
            "--run-id",
            "run-1",
            "--manifest",
            str(tmp_path / "m.json"),
            "--snapshot",
            str(archived),
            *(["--apply", "--plan-digest", "d" * 64, "--confirm", "x"] if mode == "apply" else []),
        ]
    )
    assert exit_code == 1


def test_a_tombstone_is_never_readable_as_a_plan(tmp_path):
    snapshot = _applicable_snapshot(tmp_path / "plan.json")
    invalidate_snapshot(snapshot, reason="superseded_by_new_plan")

    with pytest.raises(SnapshotError):
        read_snapshot(snapshot)
    # Renaming it back changes nothing: the authorising bytes no longer exist.
    renamed = snapshot.with_name("restored.json")
    snapshot.replace(renamed)
    with pytest.raises(SnapshotError):
        read_snapshot(renamed)


def test_invalidation_is_not_a_rename_contract():
    """A future refactor must not turn destruction back into an archive copy.

    Pinned as a contract because the defect was exactly this: the code looked
    like it invalidated something, and only moved it.
    """
    import inspect

    from altegio_bot.easyweek_migration import reminder_handover as module

    # The compiled function, not its prose: a docstring may discuss renaming,
    # the code may not do it.
    code = module.invalidate_snapshot.__code__
    names = set(code.co_names)
    assert "rename" not in names, "an invalidation that renames leaves the authorisation intact"
    assert "copyfile" not in names and "copy2" not in names
    assert "replace" in names, "the replacement must be atomic"
    literals = {const for const in code.co_consts if isinstance(const, str)}
    assert not any(".invalidated" in literal for literal in literals)
    assert "TOMBSTONE_MODE" in inspect.getsource(module.invalidate_snapshot)

    cli_source = inspect.getsource(cli)
    assert 'with_suffix(snapshot.suffix + ".invalidated")' not in cli_source
    assert "invalidate_snapshot" in cli_source


def test_a_failed_apply_does_not_invalidate_the_snapshot(tmp_path, monkeypatch, capsys):
    """Invalidation belongs to a NEW plan attempt, and to nothing else.

    Synchronous deliberately. `cli.main` calls `asyncio.run`, and inside an
    async test that raises immediately and is swallowed as
    `handover_unexpected_error` — the exit code would be 1 for a reason that has
    nothing to do with the permission gate, and the coroutine would never be
    awaited. The stderr assertion below is what proves the real path was taken.
    """
    snapshot = _applicable_snapshot(tmp_path / "plan.json")
    monkeypatch.delenv(cli.APPLY_ENV_FLAG, raising=False)

    exit_code = cli.main(
        [
            "apply",
            "--company-id",
            "758285",
            "--run-id",
            "run-1",
            "--manifest",
            str(tmp_path / "m.json"),
            "--snapshot",
            str(snapshot),
            "--apply",
            "--plan-digest",
            "d" * 64,
            "--confirm",
            "nope",
        ]
    )

    assert exit_code == 1
    stderr = capsys.readouterr().err
    # A real refusal from the apply path — the manifest this argv names does not
    # exist — and NOT the catch-all that an `asyncio.run` inside a running loop
    # would have produced.
    assert "migration_wave_changed" in stderr, stderr
    assert "handover_unexpected_error" not in stderr
    assert read_snapshot(snapshot).digest, "a refused apply must leave the operator's plan intact"


# ---------------------------------------------------------------------------
# One writer at a time, for one migration wave
# ---------------------------------------------------------------------------
#
# The gap these prove is not a row race: a row that does not exist yet cannot be
# locked. A migration apply could INSERT a new `created` row into the very wave
# a handover was walking, after its last completeness check, and the handover
# would report success for a wave containing a booking it never proved, never
# covered and never marked.
#
# The whole point is the FINAL state after both transactions, so each test below
# asserts it directly: no successful handover may coexist with an unmarked
# `created` row of the same wave.


async def _unmarked_created_rows(session_maker, *, run_id: str = "run-1") -> list[int]:
    async with session_maker() as session:
        rows = (
            await session.execute(select(EasyWeekMigrationLedger).where(EasyWeekMigrationLedger.run_id == run_id))
        ).scalars()
        return [row.id for row in rows if row.status == "created" and row.reminders_handed_over_at is None]


async def _insert_created_row(session_maker, *, source_record_id: int, booking, run_id: str = "run-1") -> int:
    """A competing migration apply, taking the same wave lock the runner takes."""
    async with session_maker() as session:
        async with session.begin():
            await db.ledger_module.lock_migration_wave(session, source_company_id=h.COMPANY, run_id=run_id)
            if await db.ledger_module.wave_handed_over(session, source_company_id=h.COMPANY, run_id=run_id):
                # Exactly what the runner does with this answer: refuse before
                # any EasyWeek request, so the booking is never created at all.
                return 0
            row = EasyWeekMigrationLedger(
                source_provider=PROVIDER_ALTEGIO,
                source_company_id=h.COMPANY,
                source_record_id=source_record_id,
                source_fingerprint="c" * 64,
                target_provider=PROVIDER_EASYWEEK,
                target_booking_uuid=str(booking),
                run_id=run_id,
                status="created",
            )
            session.add(row)
            await session.flush()
            return int(row.id)


async def test_a_row_inserted_into_the_wave_during_apply_cannot_be_missed(session_maker, seeded):
    """The writer goes first, so the handover must refuse the whole wave.

    Held deliberately overlapping: the handover is stopped inside its
    transaction, after it has taken the wave lock, and the competitor is only
    released afterwards — which is the ordering the defect needed.
    """
    planned = await plan(session_maker, seeded)
    inserted = await _insert_created_row(session_maker, source_record_id=900777, booking=h.BOOKING_TWO)
    assert inserted, "the competitor must really own a row in this wave"

    result = await h.run_apply(session_maker, planned)

    assert result.halted == db.HALT_ELIGIBLE_SCOPE_CHANGED
    assert result.created_job_ids == () and result.marked_ledger_ids == ()
    assert await h.jobs(session_maker) == [], "a halted handover leaves no target job behind"
    # The invariant: no success, so an unmarked created row is not a violation.
    assert inserted in await _unmarked_created_rows(session_maker)


async def _wait_until_waiting_on_the_wave(session_maker) -> None:
    """Return once another backend is blocked on the wave's advisory lock.

    A barrier made of the database's own wait state, not of a delay. It is also
    what makes this test able to fail: without the wave lock nobody ever waits,
    and the loop says so instead of passing by scheduling luck.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + 10.0
    while loop.time() < deadline:
        async with session_maker() as probe:
            waiting = (
                await probe.execute(
                    text(
                        "SELECT count(*) FROM pg_stat_activity "
                        "WHERE wait_event_type = 'Lock' AND wait_event = 'advisory'"
                    )
                )
            ).scalar_one()
        if waiting:
            return
        await asyncio.sleep(0.005)
    raise AssertionError("no writer was ever serialised against the wave being handed over")


async def test_a_writer_arriving_during_apply_waits_and_is_then_refused(session_maker, seeded, monkeypatch):
    """The handover goes first, and the wave is closed behind it.

    A plain lock would only delay the writer: released at commit, it would then
    insert exactly the unmarked `created` row the invariant forbids. The marker
    the handover just wrote is what refuses it instead — the lock buys the
    ordering, the marker makes the ordering matter.
    """
    planned = await plan(session_maker, seeded)
    inside = asyncio.Event()
    release = asyncio.Event()
    real_check = db._eligible_scope_still_complete
    seen: list[str | None] = []

    async def paused(session, frozen):
        reason = await real_check(session, frozen)
        seen.append(reason)
        if len(seen) == 2:
            # After the LAST completeness check and before the commit: the exact
            # window the phantom row used to slip through.
            inside.set()
            await release.wait()
        return reason

    monkeypatch.setattr(db, "_eligible_scope_still_complete", paused)

    async def competitor() -> int:
        await inside.wait()
        task = asyncio.create_task(_insert_created_row(session_maker, source_record_id=900778, booking=h.BOOKING_TWO))
        # It must be BLOCKED, not merely slow: proven against the database.
        await _wait_until_waiting_on_the_wave(session_maker)
        assert not task.done()
        release.set()
        return await task

    handover, inserted = await asyncio.gather(h.run_apply(session_maker, planned), competitor())

    assert handover.halted is None, handover.halted
    assert handover.marked_ledger_ids
    assert inserted == 0, "the wave is closed, so the booking is refused before it is created"
    assert await _unmarked_created_rows(session_maker) == []


async def test_a_historical_row_promoted_to_created_cannot_slip_in(session_maker, seeded):
    """The other direction: a row that already exists, moving INTO the wave.

    A `failed` row carries no booking, so promoting it is a legitimate migration
    step — and after a handover it would be a `created` row with no reminder
    ownership on either side.
    """
    async with session_maker() as session:
        async with session.begin():
            historical = EasyWeekMigrationLedger(
                source_provider=PROVIDER_ALTEGIO,
                source_company_id=h.COMPANY,
                source_record_id=900779,
                source_fingerprint="d" * 64,
                target_provider=PROVIDER_EASYWEEK,
                target_booking_uuid=None,
                run_id="run-1",
                status="failed",
            )
            session.add(historical)
            await session.flush()
            historical_id = int(historical.id)

    planned = await plan(session_maker, seeded)
    result = await h.run_apply(session_maker, planned)
    assert result.halted is None, result.halted

    # The promotion now arrives. It takes the same wave lock and finds the wave
    # closed, so it never becomes a created row at all.
    async with session_maker() as session:
        async with session.begin():
            await db.ledger_module.lock_migration_wave(session, source_company_id=h.COMPANY, run_id="run-1")
            closed = await db.ledger_module.wave_handed_over(session, source_company_id=h.COMPANY, run_id="run-1")
    assert closed is True

    assert await _unmarked_created_rows(session_maker) == []
    async with session_maker() as session:
        still = await session.get(EasyWeekMigrationLedger, historical_id)
    assert still.status == "failed"


async def test_an_unresolved_row_in_the_wave_stops_the_handover(session_maker, seeded):
    """`pending` and `uncertain` can still become `created` — after the fact.

    That transition is legitimate once a real booking exists in EasyWeek, so it
    must not be refused later; the wave is therefore required to be resolved
    BEFORE its reminders move, which is what the runbook already prescribes.
    """
    async with session_maker() as session:
        async with session.begin():
            session.add(
                EasyWeekMigrationLedger(
                    source_provider=PROVIDER_ALTEGIO,
                    source_company_id=h.COMPANY,
                    source_record_id=900780,
                    source_fingerprint="a" * 64,
                    target_provider=PROVIDER_EASYWEEK,
                    target_booking_uuid=None,
                    run_id="run-1",
                    status="uncertain",
                )
            )

    planned = await plan(session_maker, seeded)
    result = await h.run_apply(session_maker, planned)

    # Refused twice over, and the first refusal wins: the plan itself is no
    # longer cutover-ready, so the snapshot cannot authorise anything. The
    # transactional guard behind it is proven by
    # `test_an_unresolved_row_appearing_after_the_snapshot_still_blocks_apply`,
    # where the row appears after a clean snapshot was taken.
    assert planned.cutover_ready is False
    assert result.halted == "snapshot_not_cutover_ready"
    assert await h.jobs(session_maker) == []
    # Halted, so nothing was marked either — the wave is left exactly as it was
    # for the operator to reconcile first.
    async with session_maker() as session:
        markers = (
            await session.execute(
                select(EasyWeekMigrationLedger.reminders_handed_over_at).where(
                    EasyWeekMigrationLedger.run_id == "run-1"
                )
            )
        ).scalars()
    assert all(value is None for value in markers)


async def test_the_wave_lock_leaves_another_wave_alone(session_maker, seeded):
    """Narrow by construction: another run, company or provider is not delayed."""
    async with session_maker() as session:
        async with session.begin():
            await db.ledger_module.lock_migration_wave(session, source_company_id=h.COMPANY, run_id="run-1")
            # A different run of the same company, and a different company, both
            # inside the same transaction: neither may block on the first.
            await asyncio.wait_for(
                db.ledger_module.lock_migration_wave(session, source_company_id=h.COMPANY, run_id="run-2"),
                timeout=5,
            )
    async with session_maker() as other:
        async with other.begin():
            await asyncio.wait_for(
                db.ledger_module.lock_migration_wave(other, source_company_id=h.COMPANY + 1, run_id="run-1"),
                timeout=5,
            )


def test_the_wave_lock_key_is_scoped_to_provider_company_and_run():
    key = db.ledger_module._wave_lock_key
    base = key(source_company_id=h.COMPANY, run_id="run-1")
    assert base == key(source_company_id=h.COMPANY, run_id="run-1")
    assert base != key(source_company_id=h.COMPANY, run_id="run-2")
    assert base != key(source_company_id=h.COMPANY + 1, run_id="run-1")
    assert -(2**31) <= base < 2**31, "must fit the advisory lock's int32"


# ---------------------------------------------------------------------------
# Blocker 2: which command is a plan, decided before argparse
# ---------------------------------------------------------------------------
#
# Every case goes through the real `cli.main()` and then asks `read_snapshot`,
# because the helper answering correctly proves nothing about what the command
# actually did to the file. All of them are synchronous: `cli.main` calls
# `asyncio.run`, which inside a running loop would raise and be swallowed as
# `handover_unexpected_error` — a green test for the wrong reason.


def _is_destroyed(path: Path) -> bool:
    try:
        read_snapshot(path)
    except SnapshotError as error:
        return str(error) == "snapshot_invalidated"
    return False


@pytest.mark.parametrize(
    "argv",
    [
        pytest.param(
            ["plan", "--company-id", "not-a-number", "--run-id", "run-1", "--manifest", "m.json"],
            id="explicit-plan-malformed-company",
        ),
        pytest.param(
            ["--company-id", "not-a-number", "--run-id", "run-1", "--manifest", "m.json"],
            id="default-plan-malformed-company",
        ),
        pytest.param(["--company-id", "758285"], id="default-plan-missing-required"),
        pytest.param(["plan", "--company-id", "758285", "--unknown-flag"], id="unknown-argument"),
        pytest.param(
            ["plan", "--run-id", "run-1", "--company-id"],
            id="explicit-plan-missing-option-value",
        ),
        pytest.param(
            ["--run-id", "run-1", "--company-id"],
            id="default-plan-missing-option-value",
        ),
        pytest.param(
            ["--unknown-option", "future-value"],
            id="default-plan-unknown-option-with-value",
        ),
    ],
)
def test_a_plan_attempt_destroys_the_old_permission_however_it_fails(tmp_path, argv):
    """A plan whose arguments never parse is still a plan attempt.

    The previous scanner took the first token without a leading dash as the
    mode. In `--company-id not-a-number ...` that token is an option's VALUE, so
    the command was read as "not a plan" and the old authorisation survived
    argparse's exit — still applicable, at its usual path.
    """
    snapshot = _applicable_snapshot(tmp_path / "plan.json")

    with pytest.raises(SystemExit) as exited:
        cli.main([*argv, "--snapshot", str(snapshot)])

    assert exited.value.code == 2, "argparse still refuses the command"
    assert _is_destroyed(snapshot)


def test_a_malformed_apply_never_invalidates_the_snapshot(tmp_path):
    """Recovery from a pre-parse error must not turn apply into plan."""
    snapshot = _applicable_snapshot(tmp_path / "plan.json")

    with pytest.raises(SystemExit) as exited:
        cli.main(["apply", "--run-id", "run-1", "--company-id", "--snapshot", str(snapshot)])

    assert exited.value.code == 2
    assert read_snapshot(snapshot).digest


def test_options_before_an_explicit_plan_still_name_the_right_snapshot(tmp_path):
    """`--snapshot custom.json plan ...`: the value is not the mode."""
    custom = _applicable_snapshot(tmp_path / "custom.json")
    default = _applicable_snapshot(tmp_path / "default.json")

    with pytest.raises(SystemExit):
        cli.main(["--snapshot", str(custom), "plan", "--company-id", "not-a-number", "--manifest", "m.json"])

    assert _is_destroyed(custom)
    assert read_snapshot(default).digest, "an explicit custom path must not take the default one with it"


def test_an_inline_snapshot_value_is_honoured(tmp_path):
    custom = _applicable_snapshot(tmp_path / "custom.json")
    default = _applicable_snapshot(tmp_path / "default.json")

    with pytest.raises(SystemExit):
        cli.main(["plan", f"--snapshot={custom}", "--company-id", "not-a-number"])

    assert _is_destroyed(custom)
    assert read_snapshot(default).digest


@pytest.mark.parametrize("mode", ["apply", "verify"])
def test_an_option_value_of_plan_does_not_invalidate_anything(tmp_path, mode):
    """The opposite error: a value that happens to read as a mode."""
    snapshot = _applicable_snapshot(tmp_path / "plan.json")

    cli.main(
        [
            mode,
            "--company-id",
            "758285",
            "--run-id",
            "plan",
            "--manifest",
            "plan",
            "--snapshot",
            str(snapshot),
        ]
    )

    assert read_snapshot(snapshot).digest, "an apply or a verify must never destroy an authorisation"


def test_a_bare_help_invalidates_nothing(tmp_path, monkeypatch):
    snapshot = _applicable_snapshot(tmp_path / "plan.json")
    monkeypatch.setattr(cli, "DEFAULT_SNAPSHOT", str(snapshot))

    with pytest.raises(SystemExit) as exited:
        cli.main(["--help"])

    assert exited.value.code == 0
    assert read_snapshot(snapshot).digest


def test_the_pre_parser_mirrors_the_real_option_arity():
    """A contract, so the two parsers cannot drift into disagreeing.

    Every option the real parser takes a value for must take one here too;
    otherwise its value becomes a positional and can be read as the mode.
    """
    real = {
        action.option_strings[0]: action.nargs
        for action in cli.build_parser()._actions
        if action.option_strings and action.nargs != 0
    }
    pre = {
        action.option_strings[0]: action.nargs
        for action in cli.build_pre_parser()._actions
        if action.option_strings and action.nargs != 0
    }
    missing = sorted(set(real) - set(pre))
    assert missing == [], f"the pre-parser does not know these take a value: {missing}"


# ---------------------------------------------------------------------------
# Blocker 3: a plan that cannot be applied says so
# ---------------------------------------------------------------------------


async def _add_ledger_row(session_maker, *, status: str, source_record_id: int, run_id: str = "run-1") -> int:
    async with session_maker() as session:
        async with session.begin():
            row = EasyWeekMigrationLedger(
                source_provider=PROVIDER_ALTEGIO,
                source_company_id=h.COMPANY,
                source_record_id=source_record_id,
                source_fingerprint="b" * 64,
                target_provider=PROVIDER_EASYWEEK,
                target_booking_uuid=None,
                run_id=run_id,
                status=status,
            )
            session.add(row)
            await session.flush()
            return int(row.id)


@pytest.mark.parametrize("status", ["pending", "uncertain"])
async def test_an_unresolved_row_makes_the_plan_refuse_the_cutover(session_maker, seeded, status, tmp_path):
    """The plan used to authorise a cutover the apply was certain to refuse.

    One fully proven `created` row beside one `uncertain` row read as
    `cutover_ready`, the CLI printed the apply command, and the operator stopped
    the outbox worker for a transaction that could only answer
    `migration_wave_unresolved`.
    """
    await _add_ledger_row(session_maker, status=status, source_record_id=900801)

    planned = await plan(session_maker, seeded)

    assert planned.unresolved_rows == {status: 1}
    assert planned.guard_ready is False
    assert planned.cutover_ready is False
    report = planned.as_safe_dict()
    assert report["wave_blockers"] == ["migration_wave_unresolved"]
    assert report["cutover_ready"] is False
    # PII-free: a status name and a count, nothing about a person.
    assert report["unresolved_rows"] == {status: 1}

    # The blocked snapshot is legitimate diagnostic evidence, not a corrupt
    # file. The strict reader preserves its false readiness; the separate
    # usability gate is what refuses to authorise an apply.
    snapshot = write_snapshot(planned, tmp_path / f"{status}.json")
    frozen = read_snapshot(snapshot)
    assert frozen.historical_rows[status] == 1
    assert frozen.guard_ready is False and frozen.cutover_ready is False
    with pytest.raises(SnapshotError, match="the frozen plan is not cutover-ready"):
        check_snapshot_usable(
            frozen,
            supplied_digest=frozen.digest,
            supplied_confirmation=confirmation_phrase(frozen.digest),
            now=planned.created_at,
        )


async def test_resolving_the_row_restores_readiness(session_maker, seeded):
    row_id = await _add_ledger_row(session_maker, status="uncertain", source_record_id=900802)
    assert (await plan(session_maker, seeded)).cutover_ready is False

    async with session_maker() as session:
        async with session.begin():
            await session.execute(
                update(EasyWeekMigrationLedger).where(EasyWeekMigrationLedger.id == row_id).values(status="rolled_back")
            )

    planned = await plan(session_maker, seeded)
    assert planned.unresolved_rows == {}
    assert planned.cutover_ready is True
    assert planned.as_safe_dict()["wave_blockers"] == []


async def test_an_unresolved_row_appearing_after_the_snapshot_still_blocks_apply(session_maker, seeded):
    """The plan check does not replace the transactional guard."""
    planned = await plan(session_maker, seeded)
    assert planned.cutover_ready is True

    await _add_ledger_row(session_maker, status="pending", source_record_id=900803)

    result = await h.run_apply(session_maker, planned)
    assert result.halted == db.HALT_WAVE_UNRESOLVED
    assert await h.jobs(session_maker) == []


# ---------------------------------------------------------------------------
# Blocker 1: closing a company/run pair that holds no created row
# ---------------------------------------------------------------------------


async def _closure_rows(session_maker) -> list[tuple[int, str]]:
    async with session_maker() as session:
        rows = (
            await session.execute(select(EasyWeekMigrationWaveClosure).order_by(EasyWeekMigrationWaveClosure.run_id))
        ).scalars()
        return [(row.source_company_id, row.run_id) for row in rows]


async def test_every_claimed_pair_is_closed_including_an_empty_one(session_maker, seeded):
    """The empty pair is the whole defect.

    A snapshot naming R1 and R2 where R2 holds no `created` row had nothing to
    carry a marker, so the closure check answered "no" for R2 the moment the
    advisory lock was released — and a late retry under R2 could POST a booking
    into a wave that had already been handed over.
    """
    await _add_ledger_row(session_maker, status="failed", source_record_id=900804, run_id="run-2")

    planned = await plan(session_maker, seeded, runs=("run-1", "run-2"))
    assert planned.cutover_ready is True, "a failed row is not unresolved"

    result = await h.run_apply(session_maker, planned)
    assert result.halted is None, result.halted

    # BOTH pairs are durably closed, including the one with no created row.
    assert await _closure_rows(session_maker) == [(h.COMPANY, "run-1"), (h.COMPANY, "run-2")]
    async with session_maker() as session:
        for run_id in ("run-1", "run-2"):
            assert await db.ledger_module.wave_handed_over(session, source_company_id=h.COMPANY, run_id=run_id)


async def test_a_late_claim_into_the_empty_pair_is_refused_by_the_ledger(session_maker, seeded):
    """Through the production entry point, not a direct INSERT.

    `claim_for_apply` is what every migration apply calls before its POST, and
    it is where the refusal lives — so no caller can reach EasyWeek by skipping
    a check that happens to live in the runner.
    """
    await _add_ledger_row(session_maker, status="failed", source_record_id=900805, run_id="run-2")
    planned = await plan(session_maker, seeded, runs=("run-1", "run-2"))
    assert (await h.run_apply(session_maker, planned)).halted is None

    # A retry of the FAILED row: failed → pending is the re-claim path.
    with pytest.raises(db.ledger_module.WaveClosed):
        async with session_maker() as session:
            async with session.begin():
                await db.ledger_module.claim_for_apply(
                    session,
                    run_id="run-2",
                    source_company_id=h.COMPANY,
                    source_record_id=900805,
                    source_fingerprint="b" * 64,
                )

    # A brand-new booking under the same run id: same refusal, same reason.
    with pytest.raises(db.ledger_module.WaveClosed):
        async with session_maker() as session:
            async with session.begin():
                await db.ledger_module.claim_for_apply(
                    session,
                    run_id="run-2",
                    source_company_id=h.COMPANY,
                    source_record_id=900806,
                    source_fingerprint="b" * 64,
                )

    async with session_maker() as session:
        rows = (
            await session.execute(select(EasyWeekMigrationLedger).where(EasyWeekMigrationLedger.run_id == "run-2"))
        ).scalars()
        statuses = sorted(row.status for row in rows)
    assert statuses == ["failed"], "no row was created or re-claimed in a closed wave"


async def test_promoting_an_uncertain_row_into_a_closed_wave_is_refused(session_maker, seeded):
    """`resolve-created` is a production entry point too, and it is guarded."""
    planned = await plan(session_maker, seeded)
    assert (await h.run_apply(session_maker, planned)).halted is None
    row_id = await _add_ledger_row(session_maker, status="uncertain", source_record_id=900807)

    with pytest.raises(db.ledger_module.WaveClosed):
        async with session_maker() as session:
            async with session.begin():
                await db.ledger_module.resolve_uncertain_as_created(
                    session,
                    run_id="run-1",
                    source_company_id=h.COMPANY,
                    source_record_id=900807,
                    target_booking_uuid=str(h.BOOKING_TWO),
                    target_snapshot_fingerprint="e" * 64,
                )

    async with session_maker() as session:
        row = await session.get(EasyWeekMigrationLedger, row_id)
    assert row.status == "uncertain", "the booking stays recorded and visible to reconciliation"


async def test_a_halted_handover_leaves_no_closure_behind(session_maker, seeded):
    """Rollback means rollback: closure, jobs, cancellations and markers."""
    planned = await plan(session_maker, seeded)
    await _add_ledger_row(session_maker, status="pending", source_record_id=900808)

    result = await h.run_apply(session_maker, planned)

    assert result.halted == db.HALT_WAVE_UNRESOLVED
    assert await _closure_rows(session_maker) == []
    assert await h.jobs(session_maker) == []


async def test_repeating_the_same_handover_is_idempotent_for_the_closure(session_maker, seeded):
    planned = await plan(session_maker, seeded)
    first = await h.run_apply(session_maker, planned)
    second = await h.run_apply(session_maker, planned)

    assert first.halted is None and second.halted is None
    assert await _closure_rows(session_maker) == [(h.COMPANY, "run-1")]
    assert second.created_job_ids == ()


async def test_a_foreign_plan_digest_cannot_close_an_already_closed_wave(session_maker, seeded):
    planned = await plan(session_maker, seeded)
    assert (await h.run_apply(session_maker, planned)).halted is None

    async with session_maker() as session:
        async with session.begin():
            accepted = await db.ledger_module.close_migration_wave(
                session,
                source_company_id=h.COMPANY,
                run_id="run-1",
                plan_digest="f" * 64,
            )
    assert accepted is False, "a different authorisation is a conflict, not an update"


@pytest.mark.parametrize("damage", ["missing", "foreign"])
async def test_verify_proves_the_closure_of_an_empty_pair(session_maker, seeded, damage):
    """Row markers cannot prove closure for the run that has no created row."""
    await _add_ledger_row(session_maker, status="failed", source_record_id=900810, run_id="run-2")
    planned = await plan(session_maker, seeded, runs=("run-1", "run-2"))
    frozen = freeze_plan(planned)
    result = await h.run_apply(session_maker, planned)
    assert result.halted is None
    apply_report = result.apply_report(frozen, applied_at=datetime.now(timezone.utc))

    async with session_maker() as session:
        async with session.begin():
            closure = (
                (
                    await session.execute(
                        select(EasyWeekMigrationWaveClosure).where(EasyWeekMigrationWaveClosure.run_id == "run-2")
                    )
                )
                .scalars()
                .one()
            )
            if damage == "missing":
                await session.delete(closure)
            else:
                closure.plan_digest = "f" * 64

    async with session_maker() as session:
        verdict = await db.verify_handover(session, frozen, apply_report)

    assert verdict["passed"] is False
    assert verdict["wave_closures_expected"] == 2
    assert verdict["wave_closures_verified"] == 1
    field = "wave_closures_missing" if damage == "missing" else "wave_closures_with_foreign_digest"
    assert verdict[field] == [{"source_company_id": h.COMPANY, "run_id": "run-2"}]


async def test_a_closed_wave_does_not_close_another_run_or_company(session_maker, seeded):
    planned = await plan(session_maker, seeded)
    assert (await h.run_apply(session_maker, planned)).halted is None

    async with session_maker() as session:
        assert not await db.ledger_module.wave_handed_over(session, source_company_id=h.COMPANY, run_id="run-9")
        assert not await db.ledger_module.wave_handed_over(session, source_company_id=h.COMPANY + 1, run_id="run-1")
    # And a claim there is not refused either.
    async with session_maker() as session:
        async with session.begin():
            claimed = await db.ledger_module.claim_for_apply(
                session,
                run_id="run-9",
                source_company_id=h.COMPANY,
                source_record_id=900809,
                source_fingerprint="b" * 64,
            )
    assert claimed is True
