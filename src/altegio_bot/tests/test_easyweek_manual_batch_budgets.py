"""Check and confirmation share a bounded provider-read budget (review R5)."""

import asyncio
import uuid

import pytest

from altegio_bot.campaigns import easyweek_manual_batch as bulk
from altegio_bot.tests.test_easyweek_manual_batch import Reader, check, confirm, counts, preview


async def prepared_reads(session_maker, run_id, count):
    reader = Reader()
    reader.cards = {
        f"+491510001{i:03d}": {
            "uuid": str(uuid.uuid5(uuid.NAMESPACE_URL, f"https://example.invalid/review-r5/{i}")),
            "phone": f"+491510001{i:03d}",
            "first_name": "Synthetic",
        }
        for i in range(count)
    }
    proven = {}
    for phone in reader.cards:
        proven[phone] = await bulk._check_one(
            session_maker, run_id=run_id, company_id=322579, phone=phone, reader=reader
        )
    return reader, proven


async def test_r5_maximum_unchanged_list_fits_the_same_check_and_confirm_budget(
    session_maker, configuration, monkeypatch
):
    run_id = await preview(session_maker)
    reader, proven = await prepared_reads(session_maker, run_id, bulk.MAX_CONTACTS)
    # Scale 100 x one-second remote reads and a 90-second list budget by 0.04.
    # Local proof contents are real; only remote latency is scaled. No wall-time
    # speed assertion: success plus observed bounded overlap is the contract.
    monkeypatch.setattr(bulk, "LIST_TIMEOUT_SECONDS", 3.6)
    monkeypatch.setattr(bulk, "CONTACT_TIMEOUT_SECONDS", 0.6)
    active = peak = 0

    async def delayed(*args, phone, **kwargs):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        try:
            await asyncio.sleep(0.04)
            return dict(proven[phone])
        finally:
            active -= 1

    monkeypatch.setattr(bulk, "_check_one", delayed)
    before = await counts(session_maker)
    plan = await check(session_maker, run_id, reader, phones="\n".join(proven))
    assert plan["ok"] and plan["eligible_count"] == bulk.MAX_CONTACTS
    assert await counts(session_maker) == before
    peak = 0
    result = await confirm(session_maker, run_id, plan, reader)
    assert result["ok"], result
    assert result["added_count"] == bulk.MAX_CONTACTS
    assert peak == bulk.MAX_CONCURRENT_READS and active == 0


@pytest.mark.parametrize("timeout_kind", ["list", "contact"])
async def test_r5_confirmation_timeout_is_not_drift_and_applies_nothing(
    session_maker, configuration, monkeypatch, timeout_kind
):
    run_id = await preview(session_maker)
    plan = await check(session_maker, run_id)
    before = await counts(session_maker)
    monkeypatch.setattr(bulk, "LIST_TIMEOUT_SECONDS", 0.01 if timeout_kind == "list" else 1)
    monkeypatch.setattr(bulk, "CONTACT_TIMEOUT_SECONDS", 0.01 if timeout_kind == "contact" else 1)

    async def delayed(*args, **kwargs):
        await asyncio.sleep(10)

    monkeypatch.setattr(bulk, "_check_one", delayed)
    result = await confirm(session_maker, run_id, plan)
    assert result == {"ok": False, "reason": "manual_batch_timeout"}
    assert await counts(session_maker) == before
