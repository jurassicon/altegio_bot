"""Required PG proof for separated gift amounts and non-destructive rollback."""

from __future__ import annotations

import pytest

from altegio_bot.tests.test_easyweek_voucher_10eur_migration import (
    _current_batch,
    _historical_approval,
    _rows,
)
from altegio_bot.tests.test_easyweek_voucher_10eur_migration import (
    product_migration_db as _product_migration_db,
)
from altegio_bot.tests.test_easyweek_voucher_production_migration import (
    _alembic_ok,
    _execute,
    _fetch,
    _refused,
    _run_alembic,
    _seed_batch,
    _seed_run_and_recipients,
)

PARENT = "e7c2a4f19b86"
REVISION = "b4d7f1c90ae2"
BATCH = "easyweek_voucher_production_batches"
APPROVAL = "easyweek_voucher_production_approvals"
product_migration_db = _product_migration_db


async def test_owner_test_migration_preserves_singleton_evidence_and_refuses_downgrade(product_migration_db):
    db = product_migration_db
    _alembic_ok("upgrade", REVISION, db_url=db)
    _alembic_ok("upgrade", "c9e7a34d6210", db_url=db)
    # Empty rollback is reversible; even an unconsumed offer is not empty.
    _alembic_ok("downgrade", REVISION, db_url=db)
    _alembic_ok("upgrade", "c9e7a34d6210", db_url=db)
    table = "easyweek_voucher_owner_test"
    insert = f"""INSERT INTO {table}
        (id, customer_uuid, binding_digest, marker, state, stopped,
         create_attempted, pay_attempted, code_present, audit, created_at, updated_at)
        VALUES (:id, '11111111-1111-4111-8111-111111111111', :binding, :marker,
                'new', false, false, false, false, '[]'::jsonb, now(), now())"""
    await _execute(db, insert, {"id": 1, "binding": "a" * 64, "marker": "synthetic-owner-gift"})
    assert "ck_ew_owner_test_singleton" in await _refused(
        db, insert, {"id": 2, "binding": "b" * 64, "marker": "synthetic-second-gift"}
    )
    assert "ck_ew_owner_test_attempts" in await _refused(db, f"UPDATE {table} SET pay_attempted=true")
    assert "ck_ew_owner_test_state" in await _refused(db, f"UPDATE {table} SET state='delivered'")
    before = await _rows(db, table)
    result = _run_alembic("downgrade", REVISION, db_url=db)
    assert result.returncode != 0 and "owner gift test evidence exists" in result.stderr
    assert await _rows(db, table) == before
    assert await _fetch(db, "SELECT version_num FROM alembic_version") == [("c9e7a34d6210",)]


async def test_gift_upgrade_preserves_preview_history_and_backfills_actual_paid_prices(product_migration_db):
    db = product_migration_db
    _alembic_ok("upgrade", PARENT, db_url=db)
    old_run, _, old_batch = await _seed_batch(db, recipient_count=1)
    run, _ = await _seed_run_and_recipients(db, count=2, phone_offset=100)
    paid_batch = await _current_batch(db, run_id=run, count=2)
    approval_id = await _historical_approval(db, run_id=old_run)
    before = {table: await _rows(db, table) for table in (BATCH, APPROVAL, "campaign_runs", "campaign_recipients")}
    before_batch = await _fetch(db, f"SELECT row_to_json(t)::jsonb FROM {BATCH} t ORDER BY id")
    before_approval = await _fetch(db, f"SELECT row_to_json(t)::jsonb FROM {APPROVAL} t ORDER BY id")

    _alembic_ok("upgrade", REVISION, db_url=db)
    assert await _fetch(
        db, f"SELECT id, voucher_issue_price_minor, total_issue_price_minor FROM {BATCH} ORDER BY id"
    ) == [
        (old_batch, 1500, 1500),
        (paid_batch, 1000, 2000),
    ]
    assert await _fetch(db, f"SELECT id, stage_issue_price_minor, batch_issue_price_minor FROM {APPROVAL}") == [
        (approval_id, 1000, 1000),
    ]
    after_batch = await _fetch(db, f"SELECT row_to_json(t)::jsonb FROM {BATCH} t ORDER BY id")
    after_approval = await _fetch(db, f"SELECT row_to_json(t)::jsonb FROM {APPROVAL} t ORDER BY id")
    assert [
        ({k: v for k, v in row.items() if k not in {"voucher_issue_price_minor", "total_issue_price_minor"}},)
        for (row,) in after_batch
    ] == before_batch
    assert [
        ({k: v for k, v in row.items() if k not in {"stage_issue_price_minor", "batch_issue_price_minor"}},)
        for (row,) in after_approval
    ] == before_approval
    for table in ("campaign_runs", "campaign_recipients"):
        assert await _rows(db, table) == before[table]
    # Old schemas may not acquire zero issue prices after the backfill.
    assert "issue_price_contract" in await _refused(
        db,
        f"UPDATE {BATCH} SET voucher_issue_price_minor=0, total_issue_price_minor=0 WHERE id=:id",
        {"id": paid_batch},
    )
    assert "approval_issue_price" in await _refused(db, f"UPDATE {APPROVAL} SET stage_issue_price_minor=0")
    _alembic_ok("downgrade", PARENT, db_url=db)
    for table in before:
        assert await _rows(db, table) == before[table]
    _alembic_ok("upgrade", REVISION, db_url=db)


@pytest.mark.parametrize("gift_kind", ["batch", "unconsumed_approval"])
async def test_gift_checks_and_downgrade_refuse_before_any_ddl(product_migration_db, gift_kind):
    db = product_migration_db
    _alembic_ok("upgrade", PARENT, db_url=db)
    run, _ = await _seed_run_and_recipients(db, count=1)
    if gift_kind == "batch":
        target_id = await _current_batch(db, run_id=run, count=1)
    else:
        target_id = await _historical_approval(db, run_id=run)
    _alembic_ok("upgrade", REVISION, db_url=db)
    # Construct a valid new-version row from synthetic historical seed material;
    # no production row is ever rewritten by the migration itself.
    table = BATCH if gift_kind == "batch" else APPROVAL
    money = (
        "voucher_issue_price_minor=0, total_issue_price_minor=0"
        if gift_kind == "batch"
        else "stage_issue_price_minor=0, batch_issue_price_minor=0"
    )
    await _execute(
        db,
        f"UPDATE {table} SET request_schema_version='4', "
        "product_contract_version='easyweek-production-gift-10eur-v1', " + money + " WHERE id=:id",
        {"id": target_id},
    )
    bad_changes = (
        (
            "voucher_issue_price_minor=1, total_issue_price_minor=1",
            "total_exposure_minor=0",
            "recipient_count=0, approved_recipient_count=0, total_exposure_minor=0, approved_exposure_minor=0",
            "request_schema_version='3'",
        )
        if gift_kind == "batch"
        else (
            "stage_issue_price_minor=1",
            "batch_issue_price_minor=1000",
            "stage_amount_minor=0",
            "stage_target_count=0",
            "batch_recipient_count=0",
            "request_schema_version='3'",
        )
    )
    for change in bad_changes:
        assert "CheckViolationError" in await _refused(
            db, f"UPDATE {table} SET {change} WHERE id=:id", {"id": target_id}
        )
    before = await _rows(db, table)
    result = _run_alembic("downgrade", PARENT, db_url=db)
    assert result.returncode != 0 and "gift contract downgrade refused" in result.stderr
    assert await _rows(db, table) == before
    assert await _fetch(db, "SELECT version_num FROM alembic_version") == [(REVISION,)]
    assert await _fetch(
        db,
        "SELECT count(*) FROM information_schema.columns WHERE table_name=:table "
        "AND column_name='voucher_issue_price_minor'",
        {"table": BATCH},
    ) == [(1,)]
