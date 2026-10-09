"""Required PostgreSQL migration proof: preserve previews/history, refuse lossy rollback."""

from __future__ import annotations

import os
import uuid

import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from altegio_bot.easyweek_voucher_production_contract import CURRENT_PRODUCTION_CONTRACT as CURRENT
from altegio_bot.models.models import VOUCHER_PRODUCTION_SCOPE
from altegio_bot.tests.easyweek_voucher_delivery_fixtures import seed_recipient
from altegio_bot.tests.test_easyweek_voucher_production_migration import (
    _BATCH_INSERT,
    _ITEM_INSERT,
    _alembic_ok,
    _execute,
    _fetch,
    _refused,
    _run_alembic,
    _seed_batch,
    _seed_run_and_recipients,
)

PARENT = "d8b4e6a29c13"
REVISION = "e7c2a4f19b86"
BATCH = "easyweek_voucher_production_batches"
APPROVAL = "easyweek_voucher_production_approvals"


@pytest_asyncio.fixture
async def product_migration_db():
    server = os.environ.get("ALTEGIO_MIGTEST_DATABASE_URL") or os.environ.get("DATABASE_URL")
    assert server, "10 EUR migration proof requires the isolated PostgreSQL DATABASE_URL"
    name = "altegio_10eur_mig_" + uuid.uuid4().hex[:12]
    admin = create_async_engine(server, isolation_level="AUTOCOMMIT")
    async with admin.connect() as conn:
        await conn.execute(text(f'CREATE DATABASE "{name}"'))
    try:
        yield server.rsplit("/", 1)[0] + "/" + name
    finally:
        async with admin.connect() as conn:
            await conn.execute(
                text("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = :name"), {"name": name}
            )
            await conn.execute(text(f'DROP DATABASE "{name}"'))
        await admin.dispose()


async def _rows(db, table):
    return await _fetch(db, f"SELECT row_to_json(t)::text FROM {table} t ORDER BY id")


async def _current_batch(db, *, run_id, count):
    statement = (
        _BATCH_INSERT.replace(
            "(batch_scope, request_schema_version,",
            "(batch_scope, product_contract_version, message_contract_code, request_schema_version,",
        )
        .replace("VALUES (:scope, '1', '2026-09-27-43'", "VALUES (:scope, :product, :message, '3', :baseline")
        .replace("'55555555-5555-4555-8555-555555555555'", ":template")
    )
    await _execute(
        db,
        statement,
        {
            "scope": VOUCHER_PRODUCTION_SCOPE,
            "run": run_id,
            "count": count,
            "price": 1000,
            "total": count * 1000,
            "approved_count": count,
            "approved_total": count * 1000,
            "product": CURRENT.version,
            "message": CURRENT.message_code,
            "baseline": CURRENT.baseline_version,
            "template": CURRENT.template_uuid,
        },
    )
    return (await _fetch(db, f"SELECT id FROM {BATCH} WHERE campaign_run_id = :run", {"run": run_id}))[0][0]


async def _historical_approval(db, *, run_id):
    """Seed revision e7's physical schema, independent of future ORM columns."""
    await _execute(
        db,
        f"""
        INSERT INTO {APPROVAL} (
            batch_scope, request_schema_version, product_contract_version, provider, company_id, stage,
            principal, session_fingerprint, identification_limit, campaign_run_id, target_slots, target_slot_count,
            stage_target_count, stage_amount_minor, batch_recipient_count, batch_exposure_minor,
            campaign_period_start, campaign_period_end, plan_digest, plan_issued_at, expires_at,
            issuer_pinned, issuer_membership_proven, runtime_identity_bound, baseline_version, frozen_digest, status
        ) VALUES (
            :scope, '3', :product, 'easyweek', 322579, 'freeze',
            'synthetic-operator', :fingerprint, 'shared_ops_account', :run, '[1]'::jsonb, 1,
            1, 1000, 1, 1000, '2026-10-07T00:00:00+00', '2026-10-08T00:00:00+00',
            :digest, '2026-10-07T00:00:00+00', '2026-10-07T00:30:00+00',
            true, true, true, :baseline, :frozen, 'pending'
        )
    """,
        {
            "scope": VOUCHER_PRODUCTION_SCOPE,
            "product": CURRENT.version,
            "fingerprint": "b" * 64,
            "run": run_id,
            "digest": "c" * 64,
            "baseline": CURRENT.baseline_version,
            "frozen": "d" * 64,
        },
    )
    return (await _fetch(db, f"SELECT id FROM {APPROVAL} WHERE campaign_run_id=:run", {"run": run_id}))[0][0]


async def test_upgrade_preserves_16_earned_18_manual_preview_and_all_historical_bytes(product_migration_db):
    db = product_migration_db
    _alembic_ok("upgrade", PARENT, db_url=db)
    # Seed an actual complete earned proof, then duplicate its captured evidence
    # into distinct synthetic preview recipients. This test exercises storage,
    # never claims those duplicates are a live eligible mailing.
    engine = create_async_engine(db)
    try:
        earned_run, earned_id = await seed_recipient(async_sessionmaker(engine, expire_on_commit=False))
    finally:
        await engine.dispose()
    run, recipients = await _seed_run_and_recipients(db, count=34, phone_offset=300)
    for recipient in recipients[:16]:
        await _execute(
            db,
            "UPDATE campaign_recipients r SET recipient_basis='earned_first_visit', easyweek_customer_uuid=NULL, "
            "source_easyweek_event_id=e.source_easyweek_event_id, source_record_id=e.source_record_id, "
            "source_booking_uuid=e.source_booking_uuid, source_visits_total=e.source_visits_total, "
            "source_visits_total_updated_at=e.source_visits_total_updated_at "
            "FROM campaign_recipients e WHERE r.id=:id AND e.id=:earned",
            {"id": recipient, "earned": earned_id},
        )
    for recipient in recipients[16:]:
        await _execute(
            db,
            "UPDATE campaign_recipients SET manual_policy='altegio_visit_zero_easyweek_bookings', "
            "manual_policy_checked_at=now(), manual_operator_attested_at=now(), auto_excluded_reason='synthetic_audit' "
            "WHERE id=:id",
            {"id": recipient},
        )
    old_run, old_recipients, old_batch = await _seed_batch(db, recipient_count=1, phone_offset=900)
    await _execute(
        db,
        _ITEM_INSERT,
        {
            "batch": old_batch,
            "batch_count": 1,
            "slot": 1,
            "run": old_run,
            "recipient": old_recipients[0],
            "customer": str(uuid.uuid4()),
            "value": 1500,
            "quantity": 1,
            "marker": "historical-contract-proof",
        },
    )
    await _execute(
        db,
        "UPDATE easyweek_voucher_production_batch_items SET voucher_code_hmac=:mac, "
        "hmac_key_id='synthetic-historical-key' WHERE batch_id=:id",
        {"mac": "a" * 64, "id": old_batch},
    )
    old_items = await _rows(db, "easyweek_voucher_production_batch_items")
    old_row = (await _fetch(db, f"SELECT row_to_json(t)::jsonb FROM {BATCH} t WHERE id=:id", {"id": old_batch}))[0][0]
    previews_before = await _rows(db, "campaign_runs")
    recipients_before = await _rows(db, "campaign_recipients")
    _alembic_ok("upgrade", REVISION, db_url=db)
    assert old_items == await _rows(db, "easyweek_voucher_production_batch_items")
    assert previews_before == await _rows(db, "campaign_runs")
    assert recipients_before == await _rows(db, "campaign_recipients")
    after = (await _fetch(db, f"SELECT row_to_json(t)::jsonb FROM {BATCH} t WHERE id=:id", {"id": old_batch}))[0][0]
    assert {k: v for k, v in after.items() if k not in {"product_contract_version", "message_contract_code"}} == old_row
    assert after["voucher_unit_price_minor"] == 1500
    assert after["product_contract_version"] == "easyweek-production-15eur-v1"
    _alembic_ok("downgrade", PARENT, db_url=db)
    assert (await _fetch(db, f"SELECT row_to_json(t)::jsonb FROM {BATCH} t WHERE id=:id", {"id": old_batch}))[0][
        0
    ] == old_row
    _alembic_ok("upgrade", REVISION, db_url=db)
    assert old_items == await _rows(db, "easyweek_voucher_production_batch_items")
    assert recipients_before == await _rows(db, "campaign_recipients")
    assert earned_run != run
    new_batch = await _current_batch(db, run_id=run, count=34)
    assert (
        await _fetch(
            db,
            f"SELECT total_exposure_minor, approved_exposure_minor, approved_recipient_count FROM {BATCH} WHERE id=:id",
            {"id": new_batch},
        )
    ) == [(34000, 34000, 34)]


async def test_new_money_contract_and_entitlement_are_enforced_and_downgrade_refuses_before_ddl(product_migration_db):
    db = product_migration_db
    _alembic_ok("upgrade", REVISION, db_url=db)
    _, old_recipients, old_batch = await _seed_batch(db, recipient_count=1, phone_offset=50)
    run, recipients = await _seed_run_and_recipients(db, count=1, phone_offset=200)
    current = await _current_batch(db, run_id=run, count=1)
    for sql in (
        "voucher_unit_price_minor=1500, total_exposure_minor=1500, approved_exposure_minor=1500",
        "request_schema_version='2'",
        "product_contract_version='easyweek-production-15eur-v1'",
        "message_contract_code='new_client_voucher'",
        "voucher_template_uuid='49bc000c-c3a6-47c7-bdfd-b8ccd3ae2677'",
    ):
        assert "ck_ew_voucher_production_batch_unit_price" in await _refused(
            db, f"UPDATE {BATCH} SET {sql} WHERE id=:id", {"id": current}
        )
    customer = "00000000-0000-4000-8000-000000000001"
    old_run = (await _fetch(db, f"SELECT campaign_run_id FROM {BATCH} WHERE id=:id", {"id": old_batch}))[0][0]
    old_item = {
        "batch": old_batch,
        "batch_count": 1,
        "slot": 1,
        "run": old_run,
        "recipient": old_recipients[0],
        "customer": customer,
        "value": 1500,
        "quantity": 1,
        "marker": "old-entitlement",
    }
    await _execute(db, _ITEM_INSERT, old_item)
    new_item = {**old_item, "batch": current, "run": run, "recipient": recipients[0], "marker": "new-entitlement"}
    assert "fk_ew_voucher_production_item_batch_price" in await _refused(
        db, _ITEM_INSERT, {**new_item, "customer": str(uuid.uuid4())}
    )
    assert "uq_ew_voucher_production_item_entitlement" in await _refused(db, _ITEM_INSERT, {**new_item, "value": 1000})
    await _execute(db, _ITEM_INSERT, {**new_item, "value": 1000, "customer": str(uuid.uuid4())})
    before = await _rows(db, BATCH)
    result = _run_alembic("downgrade", PARENT, db_url=db)
    assert result.returncode != 0 and "10 EUR downgrade refused" in result.stderr
    assert before == await _rows(db, BATCH)
    assert (await _fetch(db, "SELECT version_num FROM alembic_version")) == [(REVISION,)]
    assert (
        await _fetch(
            db,
            "SELECT count(*) FROM information_schema.columns WHERE table_name=:table "
            "AND column_name='product_contract_version'",
            {"table": APPROVAL},
        )
    ) == [(1,)]


async def test_unconsumed_new_approval_alone_blocks_lossy_downgrade(product_migration_db):
    db = product_migration_db
    _alembic_ok("upgrade", REVISION, db_url=db)
    run, _recipients = await _seed_run_and_recipients(db, count=1)
    approval_id = await _historical_approval(db, run_id=run)
    before = await _rows(db, APPROVAL)
    assert "ck_ew_voucher_production_approval_product" in await _refused(
        db,
        f"UPDATE {APPROVAL} SET product_contract_version='easyweek-production-15eur-v1' WHERE id=:id",
        {"id": approval_id},
    )
    for change in ("batch_exposure_minor=1500", "stage_amount_minor=1500", "batch_recipient_count=2"):
        assert "ck_ew_voucher_production_approval_product_amount" in await _refused(
            db, f"UPDATE {APPROVAL} SET {change} WHERE id=:id", {"id": approval_id}
        )
    result = _run_alembic("downgrade", PARENT, db_url=db)
    assert result.returncode != 0 and "10 EUR downgrade refused" in result.stderr
    assert before == await _rows(db, APPROVAL)
    assert (await _fetch(db, "SELECT version_num FROM alembic_version")) == [(REVISION,)]
