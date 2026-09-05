"""One canary proves ONE mutation contract — through the real runner.

A canary is evidence about a request shape. `POST /bookings` and
`POST /bookings/cart` are two different shapes, sent to two different endpoints,
answered with two different bodies and read back by two different projections,
and a proof of one says nothing whatsoever about the other.

The defect these tests exist for was quiet: the runner built every binding with
the default contract kind, so a cart canary was filed under `single` and a
single canary licensed a plan containing carts. Neither showed up anywhere —
the report said `licensed`, and the write went ahead.

Everything here goes through the public runner (`run_inventory_or_dry_run`,
`run_canary`, `run_apply`, `run_reconcile`), never through `build_binding`
directly: the binding is not the thing that has to be right, the runner's use of
it is.
"""

from __future__ import annotations

import json
import pathlib

import pytest
import pytest_asyncio
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.easyweek_migration import classify as classify_module
from altegio_bot.easyweek_migration.bindings import MUTATION_CART_TWO, MUTATION_SINGLE
from altegio_bot.easyweek_migration.classify import BLOCK_CONTRACT_UNSUPPORTED, BLOCK_CUSTOM_PRICE, BLOCKED
from altegio_bot.easyweek_migration.gates import (
    GATE_CANARY_PROOF_MISSING,
    GATE_CART_CANARY_PROOF_MISSING,
    ApplyGateError,
)
from altegio_bot.easyweek_migration.manifest import KARLSRUHE_COMPANY_ID, parse_manifest
from altegio_bot.easyweek_migration.runner import (
    MODE_APPLY,
    MODE_CANARY,
    MODE_DRY_RUN,
    MODE_RECONCILE,
    run_apply,
    run_canary,
    run_inventory_or_dry_run,
    run_reconcile,
)
from altegio_bot.models.models import EasyWeekMigrationCanaryProof
from altegio_bot.tests.easyweek_migration_harness import (
    KA_RECORD_A,
    KA_RECORD_B,
    RecordingTransport,
    apply_production_flags,
    ledger_rows,
    license_bulk,
    make_inputs,
    make_write_client,
    manifest_json,
    run_dry_run,
    stub_altegio_source,
)
from altegio_bot.tests.test_easyweek_migration_planning import KA_SERVICE_ID, KA_STAFF_ID, record

# The second half of a cart, mapped to a catalogue entry the fixture already
# serves: same location, same master, standard price, standard duration.
CART_ALTEGIO_SERVICE_ID = 6007
CART_SERVICE_UUID = "aaaaaaaa-1111-4111-8111-00000000ca01"
KA_RECORD_CART = 900077
KA_RECORD_CUSTOM_PRICE = 900078


@pytest.fixture(autouse=True)
def _production_flags(monkeypatch: pytest.MonkeyPatch) -> None:
    apply_production_flags(monkeypatch)


@pytest.fixture
def source(monkeypatch: pytest.MonkeyPatch) -> dict[int, list[dict]]:
    return stub_altegio_source(monkeypatch)


@pytest_asyncio.fixture
async def session_local(session_maker: async_sessionmaker[AsyncSession]) -> async_sessionmaker[AsyncSession]:
    return session_maker


def cart_manifest_json() -> str:
    """The harness manifest plus the mapping the cart's second service needs."""
    payload = json.loads(manifest_json())
    payload["branches"][str(KARLSRUHE_COMPANY_ID)]["services"][str(CART_ALTEGIO_SERVICE_ID)] = {
        "easyweek_service_uuid": CART_SERVICE_UUID,
        "catalog_duration_minutes": 60,
        "catalog_price": "90.00",
        "catalog_service_name": "Mascara Auffüllen",
        "catalog_currency": "EUR",
    }
    return json.dumps(payload)


def cart_manifest():
    parsed = parse_manifest(cart_manifest_json())
    assert parsed.valid, parsed.reason
    return parsed


def cart_source_row() -> dict:
    """One Altegio booking with two different standard services, 60 + 60."""
    return record(
        id=KA_RECORD_CART,
        staff_id=KA_STAFF_ID,
        date="2026-09-12 10:00:00",
        services=[
            {"id": KA_SERVICE_ID, "cost": 90.0, "cost_to_pay": 90.0, "amount": 1},
            {"id": CART_ALTEGIO_SERVICE_ID, "cost": 90.0, "cost_to_pay": 90.0, "amount": 1},
        ],
        seance_length=120 * 60,
    )


def cart_inputs(mode: str, **overrides):
    return make_inputs(mode, manifest=cart_manifest(), **overrides)


def plan_carts_as_ready(monkeypatch: pytest.MonkeyPatch) -> None:
    """Let the PLANNER carry a cart booking, while the write path stays shut.

    While the classifier refuses carts, one never reaches the licensing code at
    all — so the licensing code could be wrong in either direction and no test
    would notice. This widens the classifier's set, and ONLY the classifier's:
    the runner keeps its own refusal, so these tests exercise the real gate, the
    real proof lookup and the real per-contract binding without ever putting a
    cart POST on the wire.

    That the runner refuses independently is itself part of the design, and
    `test_the_write_path_refuses_the_cart_contract_on_its_own` pins it.
    """
    monkeypatch.setattr(
        classify_module,
        "SUPPORTED_MUTATION_KINDS",
        frozenset({MUTATION_SINGLE, MUTATION_CART_TWO}),
    )


async def stored_proofs(session_local) -> list[EasyWeekMigrationCanaryProof]:
    async with session_local() as session:
        return list((await session.execute(select(EasyWeekMigrationCanaryProof))).scalars())


async def license_single(session_local, transport, *, manifest=None):
    """Run the real single-booking canary and return its report."""
    overrides = {"manifest": manifest} if manifest is not None else {}
    async with session_local() as session:
        plan = await run_inventory_or_dry_run(session, make_inputs(MODE_DRY_RUN, **overrides))
    async with make_write_client(transport) as client:
        report = await run_canary(
            session_local,
            make_inputs(
                MODE_CANARY,
                verified_dry_run_id=plan.plan_digest,
                canary_company_id=KARLSRUHE_COMPANY_ID,
                canary_record_id=KA_RECORD_A,
                **overrides,
            ),
            write_client=client,
        )
    assert report.as_safe_dict()["totals"]["created"] == 1, report.errors
    return plan, report


# ---------------------------------------------------------------------------
# A dry-run and an apply say the same thing
# ---------------------------------------------------------------------------


async def test_a_cart_booking_is_blocked_in_the_dry_run_not_at_write_time(session_local, source):
    source[KARLSRUHE_COMPANY_ID].append(cart_source_row())

    async with session_local() as session:
        plan = await run_inventory_or_dry_run(session, cart_inputs(MODE_DRY_RUN))

    blocked = [row for row in plan.blocked_rows if row["source_record_id"] == KA_RECORD_CART]
    assert len(blocked) == 1
    assert blocked[0]["reason"] == BLOCK_CONTRACT_UNSUPPORTED
    # Visible as a candidate — the operator can see the booking exists and why
    # it is not migrating — and counted as blocked, never as ready.
    assert plan.as_safe_dict()["totals"]["ready"] == 3


async def test_the_apply_repeats_the_dry_runs_verdict_and_posts_nothing_for_it(session_local, source):
    source[KARLSRUHE_COMPANY_ID].append(cart_source_row())
    transport = RecordingTransport()

    manifest = cart_manifest()
    plan, _canary = await license_single(session_local, transport, manifest=manifest)

    async with session_local() as session:
        confirm = await run_inventory_or_dry_run(session, cart_inputs(MODE_DRY_RUN))
    async with make_write_client(transport) as client:
        report = await run_apply(
            session_local,
            cart_inputs(MODE_APPLY, verified_dry_run_id=confirm.plan_digest),
            write_client=client,
        )

    dry = [row for row in confirm.blocked_rows if row["source_record_id"] == KA_RECORD_CART]
    applied = [row for row in report.blocked_rows if row["source_record_id"] == KA_RECORD_CART]
    assert [row["reason"] for row in dry] == [BLOCK_CONTRACT_UNSUPPORTED]
    assert [row["reason"] for row in applied] == [BLOCK_CONTRACT_UNSUPPORTED]
    # No cart request ever left, and the row has no ledger claim of any kind.
    assert not any(request.url.path.endswith("/bookings/cart") for request in transport.requests)
    assert not any(row.source_record_id == KA_RECORD_CART for row in await ledger_rows(session_local))
    assert plan.plan_digest is not None


# ---------------------------------------------------------------------------
# A proof names the contract it proved
# ---------------------------------------------------------------------------


async def test_the_canary_files_its_proof_under_the_contract_it_executed(session_local, source):
    transport = RecordingTransport()
    _plan, report = await license_single(session_local, transport)

    assert report.canary_binding is not None
    assert report.canary_binding["contract_kind"] == MUTATION_SINGLE
    proofs = await stored_proofs(session_local)
    assert [proof.contract_kind for proof in proofs] == [MUTATION_SINGLE]


async def test_a_cart_canary_is_never_filed_as_a_single_proof(session_local, source, monkeypatch):
    plan_carts_as_ready(monkeypatch)
    source[KARLSRUHE_COMPANY_ID].append(cart_source_row())
    transport = RecordingTransport()

    async with session_local() as session:
        plan = await run_inventory_or_dry_run(session, cart_inputs(MODE_DRY_RUN))
    async with make_write_client(transport) as client:
        report = await run_canary(
            session_local,
            cart_inputs(
                MODE_CANARY,
                verified_dry_run_id=plan.plan_digest,
                canary_company_id=KARLSRUHE_COMPANY_ID,
                canary_record_id=KA_RECORD_CART,
            ),
            write_client=client,
        )

    # The wave this canary would have proven is named after the CART contract,
    # taken from the decision it selected rather than from a default. This is
    # the defect itself: filed as `single`, this attempt would have sat in the
    # same slot as a single-booking proof and licensed single bulk applies.
    assert report.canary_binding is not None
    assert report.canary_binding["contract_kind"] == MUTATION_CART_TWO

    # And it still wrote nothing, because the write path refuses the contract.
    assert report.errors == [BLOCK_CONTRACT_UNSUPPORTED]
    assert transport.requests == []
    assert await stored_proofs(session_local) == []


async def test_the_write_path_refuses_the_cart_contract_on_its_own(session_local, source, monkeypatch):
    """The second refusal, pinned: opening the planner alone changes nothing."""
    plan_carts_as_ready(monkeypatch)
    source[KARLSRUHE_COMPANY_ID].append(cart_source_row())
    transport = RecordingTransport()

    async with session_local() as session:
        plan = await run_inventory_or_dry_run(session, cart_inputs(MODE_DRY_RUN))
    assert not any(row["source_record_id"] == KA_RECORD_CART for row in plan.blocked_rows)

    async with make_write_client(transport) as client:
        report = await run_canary(
            session_local,
            cart_inputs(
                MODE_CANARY,
                verified_dry_run_id=plan.plan_digest,
                canary_company_id=KARLSRUHE_COMPANY_ID,
                canary_record_id=KA_RECORD_CART,
            ),
            write_client=client,
        )

    assert report.as_safe_dict()["totals"]["created"] == 0
    assert report.mutations_attempted == 0
    assert transport.requests == []
    assert not any(row.source_record_id == KA_RECORD_CART for row in await ledger_rows(session_local))


# ---------------------------------------------------------------------------
# Neither contract licenses the other
# ---------------------------------------------------------------------------


async def test_a_cart_proof_does_not_license_a_single_bulk_apply(session_local, source):
    transport = RecordingTransport()
    await license_single(session_local, transport)

    # The very proof that licensed this wave, re-filed under the other contract.
    # Nothing else changes: same manifest, same cutover, same branch, same wave.
    async with session_local() as session:
        async with session.begin():
            await session.execute(update(EasyWeekMigrationCanaryProof).values(contract_kind=MUTATION_CART_TWO))

    async with session_local() as session:
        confirm = await run_inventory_or_dry_run(session, make_inputs(MODE_DRY_RUN))
    before = len(transport.requests)
    with pytest.raises(ApplyGateError) as excinfo:
        async with make_write_client(transport) as client:
            await run_apply(
                session_local,
                make_inputs(MODE_APPLY, verified_dry_run_id=confirm.plan_digest),
                write_client=client,
            )

    assert GATE_CANARY_PROOF_MISSING in excinfo.value.failures
    assert transport.requests[before:] == []


async def test_a_single_proof_does_not_license_a_plan_containing_a_cart(session_local, source, monkeypatch):
    plan_carts_as_ready(monkeypatch)
    source[KARLSRUHE_COMPANY_ID].append(cart_source_row())
    transport = RecordingTransport()
    manifest = cart_manifest()

    # A perfectly good single canary — and a plan that is no longer only singles.
    await license_single(session_local, transport, manifest=manifest)

    async with session_local() as session:
        confirm = await run_inventory_or_dry_run(session, cart_inputs(MODE_DRY_RUN))
    # The canary already migrated one single booking, so what is left to apply
    # is one single, one Rastatt single — and the cart.
    assert confirm.as_safe_dict()["totals"]["ready"] == 3
    assert not any(row["source_record_id"] == KA_RECORD_CART for row in confirm.blocked_rows)

    before = len(transport.requests)
    with pytest.raises(ApplyGateError) as excinfo:
        async with make_write_client(transport) as client:
            await run_apply(
                session_local,
                cart_inputs(MODE_APPLY, verified_dry_run_id=confirm.plan_digest),
                write_client=client,
            )

    # Its own code, so the report cannot be read as "no canary at all" next to a
    # verified single canary.
    assert GATE_CART_CANARY_PROOF_MISSING in excinfo.value.failures
    assert GATE_CANARY_PROOF_MISSING not in excinfo.value.failures
    # And not one booking of EITHER contract was created — the mixed plan is
    # refused whole, not written down to the licensed half.
    assert transport.requests[before:] == []


def test_the_two_refusals_are_distinct_stable_and_pii_free():
    assert GATE_CANARY_PROOF_MISSING == "canary_proof_missing_or_stale"
    assert GATE_CART_CANARY_PROOF_MISSING == "cart_canary_proof_missing_or_stale"
    assert GATE_CANARY_PROOF_MISSING != GATE_CART_CANARY_PROOF_MISSING
    for code in (GATE_CANARY_PROOF_MISSING, GATE_CART_CANARY_PROOF_MISSING):
        for leaked in ("phone", "@", "+49", "Testkundin"):
            assert leaked not in code


def test_an_unknown_contract_kind_is_fail_closed():
    from altegio_bot.easyweek_migration.canary import CanaryVerdict
    from altegio_bot.easyweek_migration.gates import CANARY_PROOF_FAILURES

    # Nothing invents a reason code out of an unrecognised kind, and nothing
    # treats it as licensed either: it falls back to the generic refusal.
    assert CANARY_PROOF_FAILURES.get("something_new") is None
    verdict = CanaryVerdict(licensed=False, contract_kind="something_new")
    assert CANARY_PROOF_FAILURES.get(verdict.contract_kind, GATE_CANARY_PROOF_MISSING) == GATE_CANARY_PROOF_MISSING


# ---------------------------------------------------------------------------
# Continuing a wave checks the contract too
# ---------------------------------------------------------------------------


async def test_reconcile_refuses_a_scope_whose_proof_is_for_another_contract(session_local, source):
    transport = RecordingTransport()
    await license_single(session_local, transport)

    async with session_local() as session:
        confirm = await run_inventory_or_dry_run(session, make_inputs(MODE_DRY_RUN))
    async with make_write_client(transport) as client:
        await run_apply(
            session_local,
            make_inputs(MODE_APPLY, verified_dry_run_id=confirm.plan_digest),
            write_client=client,
        )

    async with session_local() as session:
        async with session.begin():
            await session.execute(update(EasyWeekMigrationCanaryProof).values(contract_kind=MUTATION_CART_TWO))

    before = len(transport.requests)
    async with make_write_client(transport) as client:
        report = await run_reconcile(session_local, make_inputs(MODE_RECONCILE), write_client=client)

    # The wave is no longer proven for the contract its rows were written with,
    # so the reconciliation refuses to reach a verdict about them.
    assert report.scope is not None
    assert report.scope["scope_proven"] is False
    assert report.scope["contract_kind"] == MUTATION_SINGLE
    assert transport.requests[before:] == []


async def test_a_blocked_cart_row_never_becomes_a_ledger_claim(session_local, source):
    source[KARLSRUHE_COMPANY_ID].append(cart_source_row())
    transport = RecordingTransport()
    await license_single(session_local, transport, manifest=cart_manifest())

    async with session_local() as session:
        confirm = await run_inventory_or_dry_run(session, cart_inputs(MODE_DRY_RUN))
    async with make_write_client(transport) as client:
        report = await run_apply(
            session_local,
            cart_inputs(MODE_APPLY, verified_dry_run_id=confirm.plan_digest),
            write_client=client,
        )

    rows = await ledger_rows(session_local)
    assert all(row.source_record_id != KA_RECORD_CART for row in rows)
    assert report.as_safe_dict()["totals"]["blocked"] >= 1
    assert all(row["outcome"] != BLOCKED for row in report.created_rows)


# ---------------------------------------------------------------------------
# The custom-price canary (plan §30.12)
# ---------------------------------------------------------------------------
#
# A second live canary asked whether a booking can carry a price other than the
# service's own. It cannot, not through anything this migration may use: the
# EasyWeek booking kept the catalogue price of €120, and the €150 that was
# wanted could only be expressed as a SEPARATE POS order beside it. That order
# changed neither the service price nor the booking total — it is a till
# receipt, not the appointment's price.
#
# So a source booking whose price differs from the catalogue has no proven
# target representation, and `custom_price_unsupported` stays a refusal. These
# tests hold that shut from both ends: the refusal happens before any request,
# and the write path knows nothing about orders at all.


def custom_price_source_row() -> dict:
    """One Altegio booking sold for more than the service costs."""
    return record(
        id=KA_RECORD_CUSTOM_PRICE,
        staff_id=KA_STAFF_ID,
        date="2026-09-13 10:00:00",
        services=[{"id": KA_SERVICE_ID, "cost": 150.0, "cost_to_pay": 150.0, "amount": 1}],
    )


async def test_a_custom_source_price_is_refused_before_anything_is_posted(session_local, source):
    source[KARLSRUHE_COMPANY_ID].append(custom_price_source_row())
    transport = RecordingTransport()
    await license_single(session_local, transport)

    async with session_local() as session:
        confirm = await run_inventory_or_dry_run(session, make_inputs(MODE_DRY_RUN))
    before = len(transport.requests)
    async with make_write_client(transport) as client:
        report = await run_apply(
            session_local,
            make_inputs(MODE_APPLY, verified_dry_run_id=confirm.plan_digest),
            write_client=client,
        )

    planned = [row for row in confirm.blocked_rows if row["source_record_id"] == KA_RECORD_CUSTOM_PRICE]
    applied = [row for row in report.blocked_rows if row["source_record_id"] == KA_RECORD_CUSTOM_PRICE]
    assert [row["reason"] for row in planned] == [BLOCK_CUSTOM_PRICE]
    assert [row["reason"] for row in applied] == [BLOCK_CUSTOM_PRICE]
    # Not one POST for it — a refusal, not a booking created and then repaired.
    assert not any(
        request.method == "POST" and str(KA_RECORD_CUSTOM_PRICE) in request.content.decode()
        for request in transport.requests[before:]
    )
    assert not any(row.source_record_id == KA_RECORD_CUSTOM_PRICE for row in await ledger_rows(session_local))


async def test_the_migration_write_path_never_calls_an_order_endpoint(session_local, source):
    """Structural: a POS order is not part of migrating an appointment.

    The custom-price canary ended with a real POS order in the workspace, and
    the tempting conclusion was that the migration could create one too. It must
    not: the order does not change what the customer is booked for, it would be
    a second financial record nobody reconciles, and nothing reads it back.
    """
    transport = RecordingTransport()
    await license_single(session_local, transport)

    async with session_local() as session:
        confirm = await run_inventory_or_dry_run(session, make_inputs(MODE_DRY_RUN))
    async with make_write_client(transport) as client:
        await run_apply(
            session_local,
            make_inputs(MODE_APPLY, verified_dry_run_id=confirm.plan_digest),
            write_client=client,
        )

    # Every request a full wave made, by path shape. Anything outside this list
    # is a capability the migration acquired without anybody deciding to.
    for request in transport.requests:
        path = request.url.path
        assert "order" not in path.lower(), path
        assert "pos" not in path.lower().split("/"), path
        assert "payment" not in path.lower(), path

    source_text = pathlib.Path("src/altegio_bot/easyweek_migration/write_client.py").read_text(encoding="utf-8").lower()
    for forbidden in ('"orders"', "/orders", '"pos"', "/pos", '"payments"', "/payments"):
        assert forbidden not in source_text, forbidden


async def test_the_readback_still_checks_the_ordered_services_own_price(session_local, source):
    """A booking whose order line was repriced afterwards fails its readback.

    This is the other half of the same evidence: because a custom price can only
    live on an order line, the proof that a migrated booking is UNTOUCHED has to
    read that line's own price — not the booking total, which the canary showed
    stays at the catalogue value either way.
    """
    transport = RecordingTransport()
    await license_single(session_local, transport)

    async with session_local() as session:
        confirm = await run_inventory_or_dry_run(session, make_inputs(MODE_DRY_RUN))
    async with make_write_client(transport) as client:
        await run_apply(
            session_local,
            make_inputs(MODE_APPLY, verified_dry_run_id=confirm.plan_digest),
            write_client=client,
        )

    # Reprice one migrated booking's order line, leaving the total alone —
    # exactly what the POS canary produced.
    row = next(row for row in await ledger_rows(session_local) if row.target_booking_uuid)
    booking = transport.bookings[row.target_booking_uuid]
    booking["ordered_services"][0]["price"] = booking["ordered_services"][0]["price"] + 6000

    async with make_write_client(transport) as client:
        report = await run_reconcile(
            session_local,
            make_inputs(MODE_RECONCILE, final=True),
            write_client=client,
        )

    assert report.completeness is not None
    assert report.completeness["passed"] is False


# ---------------------------------------------------------------------------
# Source quantity, at the last possible moment
# ---------------------------------------------------------------------------


async def test_a_quantity_that_changes_under_the_plan_stops_the_write(session_local, source):
    """`amount` is part of the identity, so changing it invalidates the plan.

    The plan is built once and walked for many minutes. If a master edits the
    booking to two units in those minutes, the reviewed plan no longer describes
    it — and two units have no proven target representation at all. The last look
    at the source has to catch that BEFORE the POST, not after.
    """
    transport = RecordingTransport()
    await license_bulk(session_local, transport)

    plan = await run_dry_run(session_local)
    changed = dict(record(id=KA_RECORD_B, date="2026-09-11 10:00:00"))
    changed["services"] = [{"id": KA_SERVICE_ID, "cost": 90.0, "cost_to_pay": 90.0, "amount": 2}]
    source["live_changes"][(KARLSRUHE_COMPANY_ID, KA_RECORD_B)] = changed

    before = transport.post_count_for(KA_RECORD_B)
    async with make_write_client(transport) as client:
        report = await run_apply(
            session_local,
            make_inputs(MODE_APPLY, verified_dry_run_id=plan.plan_digest),
            write_client=client,
        )

    refused = [row for row in report.blocked_rows if row["source_record_id"] == KA_RECORD_B]
    assert refused, report.as_safe_dict()["totals"]
    assert transport.post_count_for(KA_RECORD_B) == before
    assert not any(
        row.source_record_id == KA_RECORD_B and row.status == "created" for row in await ledger_rows(session_local)
    )
