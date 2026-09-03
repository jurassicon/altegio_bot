"""PostgreSQL contract for the reconcile snapshot and its restore.

The rollback this replaces was not executable: it said "deploy the previous code,
then run reconcile --apply", but the reconcile command only exists in the version
being rolled back, and an ordinary apply writes the NEW contract rather than
restoring the old rows. Nothing recorded what the rows had been.

So the apply now records a snapshot before its first mutation, and this suite
drives the whole cycle it enables:

    snapshot → apply → restore dry-run → restore apply → restore again

plus the states where a restore must STOP instead of writing.
"""

from __future__ import annotations

import ast
import json
import os
import stat
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.easyweek_branches import BRANCH_PROFILES, branch_template_contract
from altegio_bot.models.models import (
    PROVIDER_ALTEGIO,
    PROVIDER_EASYWEEK,
    MessageTemplate,
    WhatsAppSender,
)
from altegio_bot.scripts import reconcile_easyweek_templates as cli
from altegio_bot.scripts import restore_easyweek_templates as restore
from altegio_bot.settings import settings
from altegio_bot.tests.easyweek_approved_meta_fixtures import approved_set

pytestmark = pytest.mark.asyncio

DURLACH_ID = 999501
RASTATT_ID = 999502
KARLSRUHE_ID = 999503
DURLACH_UUID = "dddddddd-eeee-4fff-8000-000000000001"
RASTATT_UUID = "dddddddd-eeee-4fff-8000-000000000002"
KARLSRUHE_UUID = "dddddddd-eeee-4fff-8000-000000000003"
BOOKING_HOST = "book.kitilash.invalid"
PHONE_NUMBER_ID = "shared-bot-phone-number-id"
CODES = ("review_3d", "repeat_10d", "comeback_3d")

API_LOCATIONS = [
    {"uuid": DURLACH_UUID, "name": "KitiLash Durlach"},
    {"uuid": RASTATT_UUID, "name": "KitiLash Rastatt"},
    {"uuid": KARLSRUHE_UUID, "name": "KitiLash Karlsruhe"},
]
ALL_APPROVED = [*approved_set("du"), *approved_set("ra"), *approved_set("ka")]

OLD_BODY = "OLD BODY THAT THE PREVIOUS CODE WROTE"
OLD_NAME = "kitilash_du_review_3d_v1"


@pytest.fixture(autouse=True)
def _registry(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "easyweek_default_language", "de", raising=False)
    monkeypatch.setattr(settings, "meta_wa_phone_number_id", PHONE_NUMBER_ID, raising=False)
    monkeypatch.setattr(settings, "easyweek_booking_page_allowed_hosts", BOOKING_HOST, raising=False)
    monkeypatch.setattr(
        settings,
        "easyweek_location_map",
        json.dumps(
            {
                "durlach": _entry(DURLACH_ID, DURLACH_UUID, "du"),
                "rastatt": _entry(RASTATT_ID, RASTATT_UUID, "ra"),
                "karlsruhe": _entry(KARLSRUHE_ID, KARLSRUHE_UUID, "ka"),
            }
        ),
        raising=False,
    )


def _entry(location_id: int, location_uuid: str, prefix: str) -> dict[str, Any]:
    return {
        "location_id": location_id,
        "location_uuid": location_uuid,
        "meta_template_prefix": prefix,
        "booking_page_url": f"https://{BOOKING_HOST}/{prefix}",
    }


class _FakeLocations:
    async def __aenter__(self) -> "_FakeLocations":
        return self

    async def __aexit__(self, *_exc: object) -> None:
        return None

    async def list_locations(self) -> list[dict[str, Any]]:
        return API_LOCATIONS


class _FakeMeta:
    async def __aenter__(self) -> "_FakeMeta":
        return self

    async def __aexit__(self, *_exc: object) -> None:
        return None

    async def list_templates(self) -> list[dict[str, Any]]:
        return ALL_APPROVED


@pytest_asyncio.fixture
async def db(session_maker: async_sessionmaker[AsyncSession]) -> AsyncSession:
    async with session_maker() as session:
        yield session


async def _apply(
    db: AsyncSession,
    snapshot: Path | None,
    *,
    branches: tuple[str, ...] = ("durlach",),
    codes: tuple[str, ...] = CODES,
) -> cli.ReconcileReport:
    report = await cli.run_reconcile(
        db,
        branches=branches,
        codes=codes,
        apply=True,
        snapshot_path=snapshot,
        client_factory=_FakeLocations,
        meta_client_factory=_FakeMeta,
    )
    await db.flush()
    return report


async def _restore(db: AsyncSession, snapshot: Path, *, apply: bool = False) -> restore.RestoreReport:
    report = await restore.run_restore(db, snapshot_path=snapshot, apply=apply)
    await db.flush()
    return report


async def _rows(db: AsyncSession) -> list[MessageTemplate]:
    return list((await db.execute(select(MessageTemplate).order_by(MessageTemplate.id))).scalars().all())


async def _states(db: AsyncSession) -> set[tuple]:
    return {
        (row.id, row.provider, row.company_id, row.code, row.language, row.body, row.meta_template_name, row.is_active)
        for row in await _rows(db)
    }


async def _row(db: AsyncSession, *, company_id: int, code: str) -> MessageTemplate | None:
    stmt = (
        select(MessageTemplate)
        .where(MessageTemplate.provider == PROVIDER_EASYWEEK)
        .where(MessageTemplate.company_id == company_id)
        .where(MessageTemplate.code == code)
    )
    return (await db.execute(stmt)).scalars().first()


def _contract(slug: str, code: str) -> str:
    return branch_template_contract(BRANCH_PROFILES[slug], code).raw_body


async def _seed_old_review(db: AsyncSession, *, is_active: bool = True) -> MessageTemplate:
    """One pre-existing row, holding what the previous code wrote."""
    row = MessageTemplate(
        provider=PROVIDER_EASYWEEK,
        company_id=DURLACH_ID,
        code="review_3d",
        language="de",
        body=OLD_BODY,
        meta_template_name=OLD_NAME,
        is_active=is_active,
    )
    db.add(row)
    await db.flush()
    return row


# ===========================================================================
# The snapshot artefact
# ===========================================================================


async def test_apply_requires_a_snapshot_path() -> None:
    """A write with no recorded previous state cannot be rolled back."""
    with pytest.raises(SystemExit):
        cli._parse_args(["--branch", "durlach", "--code", "review_3d", "--apply"])

    args = cli._parse_args(["--branch", "durlach", "--code", "review_3d", "--apply", "--snapshot", "/tmp/s.json"])
    assert args.snapshot == "/tmp/s.json"


async def test_a_dry_run_needs_no_snapshot(db: AsyncSession, tmp_path: Path) -> None:
    report = await cli.run_reconcile(
        db,
        branches=("durlach",),
        codes=CODES,
        apply=False,
        client_factory=_FakeLocations,
        meta_client_factory=_FakeMeta,
    )
    assert report.snapshot_written is None
    assert list(tmp_path.iterdir()) == []


async def test_the_snapshot_records_existing_and_absent_rows(db: AsyncSession, tmp_path: Path) -> None:
    await _seed_old_review(db)
    path = tmp_path / "snap.json"

    await _apply(db, path)

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert cli.SNAPSHOT_VERSION == 2
    assert payload["snapshot_version"] == cli.SNAPSHOT_VERSION
    assert payload["scope"] == {"branches": ["durlach"], "codes": sorted(CODES), "language": "de"}

    by_code = {row["code"]: row for row in payload["rows"]}
    assert by_code["review_3d"]["existed"] is True
    assert by_code["review_3d"]["body"] == OLD_BODY
    assert by_code["review_3d"]["meta_template_name"] == OLD_NAME
    assert by_code["review_3d"]["is_active"] is True
    for absent in ("repeat_10d", "comeback_3d"):
        assert by_code[absent]["existed"] is False
        assert "body" not in by_code[absent]
    for code, entry in by_code.items():
        assert entry["expected_after"] == {
            "body": _contract("durlach", code),
            "meta_template_name": f"kitilash_du_{code}_v1",
            "is_active": True,
        }


async def test_complete_evidence_is_saved_before_the_first_write(
    db: AsyncSession, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    old = await _seed_old_review(db)
    before = await _states(db)
    path = tmp_path / "snap.json"
    apply_templates = cli.apply_templates

    async def checked_apply(session, **kwargs):
        evidence = restore.load_snapshot(path)
        assert await _states(session) == before
        assert next(row for row in evidence["rows"] if row["existed"])["id"] == old.id
        assert all(row["expected_after"]["is_active"] is True for row in evidence["rows"])
        return await apply_templates(session, **kwargs)

    monkeypatch.setattr(cli, "apply_templates", checked_apply)
    report = await _apply(db, path)
    assert report.mutations_attempted == 3


async def test_no_op_plan_and_snapshot_preserve_the_exact_contract(db: AsyncSession, tmp_path: Path) -> None:
    await _apply(db, tmp_path / "first.json")
    row = await _row(db, company_id=DURLACH_ID, code="review_3d")
    row.meta_template_name = f" {OLD_NAME} "
    await db.flush()
    path = tmp_path / "exact.json"
    report = await _apply(db, path)
    assert report.mutations_attempted == 1, "a normalised name match is not an exact expected-after state"
    assert row.meta_template_name == OLD_NAME
    again = await _apply(db, tmp_path / "noop.json")
    assert again.mutations_attempted == 0
    for entry in restore.load_snapshot(tmp_path / "noop.json")["rows"]:
        assert entry["expected_after"] == {key: entry[key] for key in restore.STATE_FIELDS}
    await _restore(db, path, apply=True)
    assert row.meta_template_name == f" {OLD_NAME} ", "rollback preserves the exact previous value"


async def test_the_snapshot_is_owner_only_and_survives_the_container(db: AsyncSession, tmp_path: Path) -> None:
    """It lands on a host path, so it outlives a --rm container."""
    path = tmp_path / "snap.json"
    await _apply(db, path)

    assert path.exists()
    assert stat.S_IMODE(os.stat(path).st_mode) == cli.SNAPSHOT_MODE


async def test_the_snapshot_is_never_silently_overwritten(db: AsyncSession, tmp_path: Path) -> None:
    """Overwriting would destroy the only record of an earlier apply."""
    path = tmp_path / "snap.json"
    await _apply(db, path)
    original = path.read_text(encoding="utf-8")

    with pytest.raises(cli.ReconcileError) as excinfo:
        await _apply(db, path)

    assert cli.ERROR_SNAPSHOT_EXISTS in str(excinfo.value)
    assert path.read_text(encoding="utf-8") == original


async def test_an_unwritable_snapshot_path_blocks_before_any_db_change(db: AsyncSession, tmp_path: Path) -> None:
    """Proven writable BEFORE the mutation, not discovered during an incident."""
    await _seed_old_review(db)
    blocked = tmp_path / "nowhere"
    blocked.write_text("not a directory")

    with pytest.raises(Exception):
        await _apply(db, blocked / "snap.json")

    row = await _row(db, company_id=DURLACH_ID, code="review_3d")
    assert row.body == OLD_BODY, "the database is untouched when the artefact cannot be written"
    assert await _row(db, company_id=DURLACH_ID, code="repeat_10d") is None


# ===========================================================================
# The full cycle
# ===========================================================================


async def test_snapshot_apply_restore_returns_the_previous_rows(db: AsyncSession, tmp_path: Path) -> None:
    """The cycle the runbook now documents, end to end."""
    old = await _seed_old_review(db)
    old_id = old.id
    path = tmp_path / "snap.json"

    apply_report = await _apply(db, path)
    assert apply_report.snapshot_written == str(path)
    assert apply_report.mutations_attempted == 3
    assert (await _row(db, company_id=DURLACH_ID, code="review_3d")).body == _contract("durlach", "review_3d")
    assert len(await _rows(db)) == 3

    dry = await _restore(db, path)
    assert dry.mutations_attempted == 0
    assert {plan.action for plan in dry.plans} == {restore.ACTION_RESTORE, restore.ACTION_DEACTIVATE}
    assert (await _row(db, company_id=DURLACH_ID, code="review_3d")).body == _contract("durlach", "review_3d")

    applied = await _restore(db, path, apply=True)
    assert applied.mutations_attempted == 3
    assert applied.blockers == []

    restored = await _row(db, company_id=DURLACH_ID, code="review_3d")
    assert restored.id == old_id, "the same row, not a re-created one"
    assert restored.body == OLD_BODY
    assert restored.meta_template_name == OLD_NAME
    assert restored.is_active is True
    for created in ("repeat_10d", "comeback_3d"):
        row = await _row(db, company_id=DURLACH_ID, code=created)
        assert row is not None, "rows created by the apply are deactivated, never deleted"
        assert row.is_active is False


async def test_a_second_restore_is_a_safe_no_op(db: AsyncSession, tmp_path: Path) -> None:
    await _seed_old_review(db)
    path = tmp_path / "snap.json"
    await _apply(db, path)
    await _restore(db, path, apply=True)
    before = {(row.id, row.body, row.meta_template_name, row.is_active) for row in await _rows(db)}

    again = await _restore(db, path, apply=True)

    assert again.mutations_attempted == 0
    assert {plan.action for plan in again.plans} == {restore.ACTION_UNCHANGED}
    assert {(row.id, row.body, row.meta_template_name, row.is_active) for row in await _rows(db)} == before


async def test_an_inactive_previous_row_is_restored_inactive(db: AsyncSession, tmp_path: Path) -> None:
    """`is_active` is part of the previous state, not a detail to normalise."""
    await _seed_old_review(db, is_active=False)
    path = tmp_path / "snap.json"

    await _apply(db, path)
    assert (await _row(db, company_id=DURLACH_ID, code="review_3d")).is_active is True

    await _restore(db, path, apply=True)

    row = await _row(db, company_id=DURLACH_ID, code="review_3d")
    assert row.is_active is False
    assert row.body == OLD_BODY
    assert (await _restore(db, path, apply=True)).mutations_attempted == 0


@pytest.mark.parametrize("old_name", [None, ""])
async def test_nullable_previous_names_are_restored_exactly(
    db: AsyncSession, tmp_path: Path, old_name: str | None
) -> None:
    old = await _seed_old_review(db, is_active=False)
    old.meta_template_name = old_name
    await db.flush()
    path = tmp_path / "snap.json"
    await _apply(db, path)
    await _restore(db, path, apply=True)
    assert old.meta_template_name == old_name
    assert old.is_active is False
    assert (await _restore(db, path, apply=True)).mutations_attempted == 0
    old.meta_template_name = "" if old_name is None else None
    await db.flush()
    assert (await _restore(db, path, apply=True)).blockers == [restore.BLOCK_ROW_CHANGED_SINCE]


async def test_cli_restore_commits_the_cycle_and_repeats_as_no_op(
    db: AsyncSession,
    session_maker: async_sessionmaker[AsyncSession],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture,
) -> None:
    await _seed_old_review(db, is_active=False)
    path = tmp_path / "snap.json"
    await _apply(db, path)
    await db.commit()
    before = await _states(db)
    await db.rollback()
    monkeypatch.setattr(restore, "SessionLocal", session_maker)
    capsys.readouterr()
    assert await restore.main(["--snapshot", str(path)]) == 0
    assert ast.literal_eval(capsys.readouterr().out.strip())["mutations_attempted"] == 0
    async with session_maker() as verify:
        assert await _states(verify) == before
    assert await restore.main(["--snapshot", str(path), "--apply"]) == 0
    assert ast.literal_eval(capsys.readouterr().out.strip())["mutations_attempted"] == 3
    async with session_maker() as verify:
        after = await _states(verify)
        row = await _row(verify, company_id=DURLACH_ID, code="review_3d")
        assert row.body == OLD_BODY and row.is_active is False and row.meta_template_name == OLD_NAME
        assert all(row.is_active is False for row in await _rows(verify))
    assert await restore.main(["--snapshot", str(path), "--apply"]) == 0
    assert ast.literal_eval(capsys.readouterr().out.strip())["mutations_attempted"] == 0
    async with session_maker() as verify:
        assert await _states(verify) == after


async def test_cli_rolls_back_all_flushed_writes_on_error(
    db: AsyncSession,
    session_maker: async_sessionmaker[AsyncSession],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture,
) -> None:
    await _seed_old_review(db)
    path = tmp_path / "snap.json"
    await _apply(db, path)
    await db.commit()
    before = await _states(db)
    await db.rollback()
    run_restore = restore.run_restore

    async def fail_after_sql(session, **kwargs):
        report = await run_restore(session, **kwargs)
        assert report.mutations_attempted == 3
        await session.flush()  # Real UPDATEs inside the real CLI transaction.
        assert await _states(session) != before
        raise RuntimeError("injected failure after SQL writes")

    monkeypatch.setattr(restore, "SessionLocal", session_maker)
    monkeypatch.setattr(restore, "run_restore", fail_after_sql)
    capsys.readouterr()
    assert await restore.main(["--snapshot", str(path), "--apply"]) == 1
    assert ast.literal_eval(capsys.readouterr().out.strip())["error"] == restore.ERROR_UNEXPECTED
    async with session_maker() as verify:
        assert await _states(verify) == before


# ===========================================================================
# When the restore must STOP
# ===========================================================================


@pytest.mark.parametrize(
    ("code", "edit"),
    [
        ("review_3d", {"meta_template_name": "manual_review_v2"}),
        ("review_3d", {"is_active": False}),
        ("comeback_3d", {"meta_template_name": "manual_comeback_v2"}),
        ("comeback_3d", {"is_active": False, "meta_template_name": "manual_comeback_v2"}),
        ("comeback_3d", {"is_active": False, "body": "MANUAL INACTIVE BODY"}),
    ],
)
async def test_manual_edits_block_the_entire_cli_restore(
    db: AsyncSession,
    session_maker: async_sessionmaker[AsyncSession],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture,
    code: str,
    edit: dict,
) -> None:
    await _seed_old_review(db)
    path = tmp_path / "snap.json"
    await _apply(db, path)
    await db.commit()
    row = await _row(db, company_id=DURLACH_ID, code=code)
    for key, value in edit.items():
        setattr(row, key, value)
    await db.commit()
    before = await _states(db)
    await db.rollback()

    monkeypatch.setattr(restore, "SessionLocal", session_maker)
    capsys.readouterr()
    for flags in ([], ["--apply"]):
        assert await restore.main(["--snapshot", str(path), *flags]) == 1
        report = ast.literal_eval(capsys.readouterr().out.strip())
        assert report["blockers"] == [restore.BLOCK_ROW_CHANGED_SINCE]
        assert report["mutations_attempted"] == 0
        async with session_maker() as verify:
            assert await _states(verify) == before


@pytest.mark.parametrize("location_map", ["", "{invalid", "{}"])
async def test_registry_failure_cannot_bypass_the_changed_state_check(
    db: AsyncSession, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, location_map: str
) -> None:
    await _seed_old_review(db)
    path = tmp_path / "snap.json"
    await _apply(db, path)
    row = await _row(db, company_id=DURLACH_ID, code="review_3d")
    row.body = "MANUAL EDIT DURING A CONFIGURATION INCIDENT"
    await db.flush()
    before = await _states(db)
    monkeypatch.setattr(settings, "easyweek_location_map", location_map)

    report = await _restore(db, path, apply=True)

    assert report.blockers == [restore.BLOCK_ROW_CHANGED_SINCE]
    assert report.mutations_attempted == 0
    assert await _states(db) == before


async def test_frozen_evidence_allows_restore_without_a_current_registry(
    db: AsyncSession, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    await _seed_old_review(db)
    path = tmp_path / "snap.json"
    await _apply(db, path)
    monkeypatch.setattr(settings, "easyweek_location_map", "{invalid")
    report = await _restore(db, path, apply=True)
    assert report.blockers == []
    assert report.mutations_attempted == 3
    assert (await _restore(db, path, apply=True)).mutations_attempted == 0


async def test_a_row_changed_after_the_apply_blocks_the_restore(db: AsyncSession, tmp_path: Path) -> None:
    """Somebody's later edit is not ours to discard."""
    await _seed_old_review(db)
    path = tmp_path / "snap.json"
    await _apply(db, path)

    edited = await _row(db, company_id=DURLACH_ID, code="review_3d")
    edited.body = "A LATER MANUAL EDIT"
    await db.flush()

    report = await _restore(db, path, apply=True)

    assert restore.BLOCK_ROW_CHANGED_SINCE in report.blockers
    assert report.mutations_attempted == 0
    assert (await _row(db, company_id=DURLACH_ID, code="review_3d")).body == "A LATER MANUAL EDIT"
    # And the whole restore stopped, not just that row.
    assert (await _row(db, company_id=DURLACH_ID, code="repeat_10d")).is_active is True


async def test_a_created_row_edited_afterwards_blocks_the_restore(db: AsyncSession, tmp_path: Path) -> None:
    path = tmp_path / "snap.json"
    await _apply(db, path)

    created = await _row(db, company_id=DURLACH_ID, code="repeat_10d")
    created.body = "SOMEBODY ELSE'S TEXT"
    await db.flush()

    report = await _restore(db, path, apply=True)

    assert restore.BLOCK_ROW_CHANGED_SINCE in report.blockers
    assert (await _row(db, company_id=DURLACH_ID, code="repeat_10d")).is_active is True


async def test_a_vanished_row_blocks_rather_than_being_recreated(db: AsyncSession, tmp_path: Path) -> None:
    await _seed_old_review(db)
    path = tmp_path / "snap.json"
    await _apply(db, path)

    row = await _row(db, company_id=DURLACH_ID, code="review_3d")
    await db.delete(row)
    await db.flush()

    report = await _restore(db, path, apply=True)

    assert restore.BLOCK_ROW_VANISHED in report.blockers
    assert report.mutations_attempted == 0


async def test_duplicated_rows_block_the_restore(db: AsyncSession, tmp_path: Path) -> None:
    await _seed_old_review(db)
    path = tmp_path / "snap.json"
    await _apply(db, path)
    db.add(
        MessageTemplate(
            provider=PROVIDER_EASYWEEK,
            company_id=DURLACH_ID,
            code="review_3d",
            language="de",
            body="A SECOND ROW",
            meta_template_name=OLD_NAME,
            is_active=False,
        )
    )
    await db.flush()

    report = await _restore(db, path, apply=True)

    assert restore.BLOCK_ROW_DUPLICATED in report.blockers
    assert report.mutations_attempted == 0


@pytest.mark.parametrize(
    ("mutate", "expected"),
    [
        pytest.param(lambda d: d.update(snapshot_version=999), restore.BLOCK_SNAPSHOT_VERSION, id="version"),
        pytest.param(lambda d: d.update(snapshot_version=1), restore.BLOCK_SNAPSHOT_VERSION, id="legacy_v1"),
        pytest.param(lambda d: d.update(rows=[]), restore.BLOCK_SNAPSHOT_SHAPE, id="empty"),
        pytest.param(
            lambda d: d["rows"][0].update(provider=PROVIDER_ALTEGIO),
            restore.BLOCK_SNAPSHOT_SCOPE,
            id="other_provider",
        ),
        pytest.param(
            lambda d: d["rows"][0].update(code="reminder_24h"),
            restore.BLOCK_SNAPSHOT_SCOPE,
            id="out_of_scope_code",
        ),
        pytest.param(lambda d: d["rows"][0].pop("body", None), restore.BLOCK_SNAPSHOT_SHAPE, id="missing_body"),
        pytest.param(
            lambda d: d["rows"][0].pop("expected_after"), restore.BLOCK_SNAPSHOT_SHAPE, id="no_expected_after"
        ),
        pytest.param(
            lambda d: d["rows"][0]["expected_after"].pop("body"),
            restore.BLOCK_SNAPSHOT_SHAPE,
            id="no_after_body",
        ),
        pytest.param(
            lambda d: d["rows"][0]["expected_after"].pop("meta_template_name"),
            restore.BLOCK_SNAPSHOT_SHAPE,
            id="no_after_name",
        ),
        pytest.param(
            lambda d: d["rows"][0]["expected_after"].pop("is_active"),
            restore.BLOCK_SNAPSHOT_SHAPE,
            id="no_after_activity",
        ),
        pytest.param(
            lambda d: d["rows"][0]["expected_after"].update(is_active="true"),
            restore.BLOCK_SNAPSHOT_SHAPE,
            id="invalid_after_activity",
        ),
        pytest.param(
            lambda d: d["rows"][0]["expected_after"].update(body=None),
            restore.BLOCK_SNAPSHOT_SHAPE,
            id="invalid_after_body",
        ),
        pytest.param(
            lambda d: d["rows"][0]["expected_after"].update(meta_template_name=None),
            restore.BLOCK_SNAPSHOT_SHAPE,
            id="invalid_after_name",
        ),
        pytest.param(
            lambda d: d["rows"][0]["expected_after"].update(is_active=False),
            restore.BLOCK_SNAPSHOT_SHAPE,
            id="unproven_inactive_after",
        ),
        pytest.param(
            lambda d: d["rows"][0].update(is_active="false"),
            restore.BLOCK_SNAPSHOT_SHAPE,
            id="invalid_before_activity",
        ),
        pytest.param(lambda d: d["rows"].append(d["rows"][0]), restore.BLOCK_SNAPSHOT_SHAPE, id="duplicate_key"),
    ],
)
async def test_an_unsuitable_snapshot_is_refused(db: AsyncSession, tmp_path: Path, mutate, expected: str) -> None:
    """A snapshot this command cannot fully understand is never partly applied."""
    await _seed_old_review(db)
    path = tmp_path / "snap.json"
    await _apply(db, path)
    payload = json.loads(path.read_text(encoding="utf-8"))
    # Ensure the row we mutate is the one that existed.
    payload["rows"].sort(key=lambda row: not row.get("existed", False))
    mutate(payload)
    broken = tmp_path / "broken.json"
    broken.write_text(json.dumps(payload, ensure_ascii=False))
    before = await _states(db)

    with pytest.raises(cli.ReconcileError) as excinfo:
        await _restore(db, broken, apply=True)

    assert expected in str(excinfo.value)
    assert await _states(db) == before


async def test_an_unreadable_snapshot_is_refused(db: AsyncSession, tmp_path: Path) -> None:
    broken = tmp_path / "broken.json"
    broken.write_text("{not json")

    with pytest.raises(cli.ReconcileError) as excinfo:
        await _restore(db, broken, apply=True)

    assert restore.BLOCK_SNAPSHOT_UNREADABLE in str(excinfo.value)


# ===========================================================================
# Scope
# ===========================================================================


async def test_the_restore_touches_nothing_outside_the_snapshot(db: AsyncSession, tmp_path: Path) -> None:
    await _seed_old_review(db)
    db.add_all(
        [
            MessageTemplate(
                provider=PROVIDER_EASYWEEK,
                company_id=RASTATT_ID,
                code="review_3d",
                language="de",
                body="RASTATT UNTOUCHED",
                meta_template_name="kitilash_ra_review_3d_v1",
                is_active=True,
            ),
            MessageTemplate(
                provider=PROVIDER_EASYWEEK,
                company_id=DURLACH_ID,
                code="reminder_24h",
                language="de",
                body="REMINDER UNTOUCHED",
                meta_template_name="kitilash_du_reminder_24h_v1",
                is_active=True,
            ),
            MessageTemplate(
                provider=PROVIDER_EASYWEEK,
                company_id=DURLACH_ID,
                code="review_3d",
                language="en",
                body="ENGLISH UNTOUCHED",
                meta_template_name="kitilash_du_review_3d_v1",
                is_active=True,
            ),
            MessageTemplate(
                provider=PROVIDER_ALTEGIO,
                company_id=DURLACH_ID,
                code="review_3d",
                language="de",
                body="ALTEGIO UNTOUCHED",
                meta_template_name="kitilash_ka_review_3d_v1",
                is_active=True,
            ),
        ]
    )
    await db.flush()
    path = tmp_path / "snap.json"
    await _apply(db, path)

    await _restore(db, path, apply=True)

    untouched = {
        (row.provider, row.company_id, row.code, row.language): row.body
        for row in await _rows(db)
        if row.body.endswith("UNTOUCHED")
    }
    assert untouched == {
        (PROVIDER_EASYWEEK, RASTATT_ID, "review_3d", "de"): "RASTATT UNTOUCHED",
        (PROVIDER_EASYWEEK, DURLACH_ID, "reminder_24h", "de"): "REMINDER UNTOUCHED",
        (PROVIDER_EASYWEEK, DURLACH_ID, "review_3d", "en"): "ENGLISH UNTOUCHED",
        (PROVIDER_ALTEGIO, DURLACH_ID, "review_3d", "de"): "ALTEGIO UNTOUCHED",
    }


async def test_the_restore_never_touches_a_sender(db: AsyncSession, tmp_path: Path) -> None:
    """Template rollback is not a sender decision."""
    db.add(
        WhatsAppSender(
            provider=PROVIDER_EASYWEEK,
            company_id=DURLACH_ID,
            sender_code="default",
            phone_number_id=PHONE_NUMBER_ID,
            is_active=True,
        )
    )
    await db.flush()
    path = tmp_path / "snap.json"
    await _apply(db, path)

    await _restore(db, path, apply=True)

    senders = list((await db.execute(select(WhatsAppSender))).scalars().all())
    assert len(senders) == 1
    assert senders[0].is_active is True
    assert senders[0].phone_number_id == PHONE_NUMBER_ID


async def test_the_restore_report_does_not_claim_a_meta_proof(db: AsyncSession, tmp_path: Path) -> None:
    """A restored body is the old body — not evidence it matches Meta today."""
    await _seed_old_review(db)
    path = tmp_path / "snap.json"
    await _apply(db, path)

    payload = (await _restore(db, path)).as_safe_dict()

    assert payload["send_authorized"] is False
    assert "not a Meta contract proof" in payload["note"]
    assert "not a send authorization" in payload["note"]
