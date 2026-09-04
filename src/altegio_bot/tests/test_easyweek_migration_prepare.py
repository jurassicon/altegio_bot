"""The preparation stage end to end: propose, report, and hand over.

The stage exists because the preparation, not the migration, is what took an
afternoon. So most of what is checked here is that it produces the artefacts the
EXISTING migrator already reads — a manifest, a customer directory, a verified
dry-run id it obtained itself — and that it refuses to decide anything a person
should decide.

The other half is the reporting contract. Two files, deliberately different: a
machine report with no names, numbers or e-mail addresses in it at all, and an
operator report that has all three because a person cannot confirm the creation
of a customer shown to them as a hash.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pytest

from altegio_bot.easyweek_migration import prepare as prepare_module
from altegio_bot.easyweek_migration.customer_decisions import (
    STATE_BLOCKED,
    STATE_CONFIRMED,
    STATE_PENDING,
    CustomerDecisionStore,
)
from altegio_bot.easyweek_migration.customer_overrides import CustomerOverrideError
from altegio_bot.easyweek_migration.cutover import parse_cutover
from altegio_bot.easyweek_migration.manifest import KARLSRUHE_COMPANY_ID, inventory_manifest, parse_manifest
from altegio_bot.easyweek_migration.mapping_proposal import (
    BASELINE_FIELDS,
    PROPOSAL_ALREADY_MAPPED,
    PROPOSAL_AMBIGUOUS,
    PROPOSAL_BASELINE_DRIFT,
    PROPOSAL_BASELINE_INCOMPLETE,
    PROPOSAL_NO_CANDIDATE,
    PROPOSAL_STAFF_UNAVAILABLE,
    PROPOSAL_UNIQUE_NAME,
    STAFF_AVAILABILITY_ABSENT,
    STAFF_AVAILABILITY_PROVEN,
    STAFF_AVAILABILITY_UNSTATED,
    MappingAgreement,
    collect_source_services,
    manifest_service_patch,
    merge_manifest_services,
    proposal_digest,
    propose_service_mapping,
    read_service_staff_uuids,
)
from altegio_bot.easyweek_migration.prepare import (
    BLOCK_NAME_NOT_SPLIT,
    FILE_CUSTOMER_DIRECTORY,
    FILE_MANIFEST_PROPOSED,
    FILE_OPERATOR_REVIEW,
    MODE_PREPARE,
    ConfirmRequest,
    ConfirmTarget,
    PrepareError,
    PrepareInputs,
    apply_confirmations,
    build_preparation_snapshot,
    mapping_pending_digest,
    run_prepare,
)
from altegio_bot.easyweek_migration.service_catalog import build_catalog_snapshot
from altegio_bot.scripts import easyweek_migration_prepare as cli
from altegio_bot.tests.easyweek_migration_harness import apply_production_flags, manifest_json
from altegio_bot.tests.test_easyweek_migration_customer_lookup import PHONE, UUID_A, card, page
from altegio_bot.tests.test_easyweek_migration_planning import (
    KA_LOCATION_UUID,
    KA_SERVICE_ID,
    KA_SERVICE_UUID,
    KA_STAFF_ID,
    KA_STAFF_UUID,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
CUTOVER = "2026-09-01T00:00:00Z"
OTHER_SERVICE_UUID = "99999999-9999-4999-8999-999999999999"
SECOND_SERVICE_ID = 6009
# A master the manifest does not map to any EasyWeek uuid.
KA_OTHER_STAFF_ID = 5099
# A second master in the wave, with an EasyWeek uuid of her own.
KA_SECOND_STAFF_ID = 5011
KA_SECOND_STAFF_UUID = "aaaa1111-2222-4333-8444-555566667777"


def catalog_row(
    uuid: str = KA_SERVICE_UUID,
    *,
    name: str = "Wimpernverlängerung 2D",
    price: int = 9000,
    minutes: int = 60,
    staff: list[str] | None = None,
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "uuid": uuid,
        "name": name,
        "currency": "EUR",
        "price": price,
        "duration": {"value": minutes, "label": "minutes"},
    }
    if staff is not None:
        row["employees"] = staff
    return row


# The manifest fixture freezes this identity for KA_SERVICE_ID. A catalogue row
# built from it is what "unchanged existing mapping" looks like; changing one
# field of it is what drift looks like.
BASELINE_NAME = "Mascara Effekt"
BASELINE_PRICE_MINOR = 9000
BASELINE_MINUTES = 60


def baseline_catalog_row(**overrides: Any) -> dict[str, Any]:
    row = catalog_row(
        KA_SERVICE_UUID,
        name=BASELINE_NAME,
        price=BASELINE_PRICE_MINOR,
        minutes=BASELINE_MINUTES,
    )
    row.update(overrides)
    return row


def source_record(
    record_id: int = 900001,
    *,
    service_id: int = KA_SERVICE_ID,
    service_name: str = "Wimpernverlängerung 2D",
    phone: str = PHONE,
    client: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "id": record_id,
        "date": "2026-09-10 12:00:00",
        "staff_id": KA_STAFF_ID,
        "seance_length": 3600,
        "client": client if client is not None else {"phone": phone, "first_name": "Testkundin", "id": 42},
        "services": [{"id": service_id, "title": service_name, "cost": 90.0, "cost_to_pay": 90.0}],
    }


class FakePrepareClient:
    """Catalogue pages plus customer lookups. GET only; no create method at all."""

    def __init__(
        self,
        catalog: list[dict[str, Any]] | None = None,
        customers: dict[str, Any] | None = None,
    ) -> None:
        self.catalog = catalog if catalog is not None else [catalog_row()]
        self.customers = customers or {}
        self.calls: list[str] = []

    async def list_location_services(self, location_uuid: str, *, page: int = 1, **kwargs: Any):
        self.calls.append("catalog")
        assert page == 1, "this fixture serves a single-page catalogue"
        return {
            "data": self.catalog,
            "meta": {"current_page": 1, "last_page": 1, "total": len(self.catalog)},
        }

    async def list_customers(self, *, params: dict[str, Any]) -> dict[str, Any]:
        self.calls.append("customers")
        answer = self.customers.get(params["phone"], page([]))
        if isinstance(answer, Exception):
            raise answer
        return answer

    async def get_customer(self, customer_uuid: str) -> dict[str, Any]:  # pragma: no cover - unused here
        raise AssertionError("prepare must not read a card by uuid")


@pytest.fixture
def state_dir(tmp_path: Path) -> Path:
    return tmp_path / "state"


@pytest.fixture(autouse=True)
def registry(monkeypatch: pytest.MonkeyPatch) -> None:
    apply_production_flags(monkeypatch)


def make_inputs(state_dir: Path, *, manifest_text: str | None = None, mode: str = MODE_PREPARE) -> PrepareInputs:
    text = manifest_text or manifest_json()
    manifest = inventory_manifest(text)
    assert manifest.valid, manifest.reason
    return PrepareInputs(
        mode=mode,
        run_id="run-prepare",
        state_dir=state_dir,
        manifest=manifest,
        manifest_json=json.loads(text),
        altegio_company_id=KARLSRUHE_COMPANY_ID,
        cutover=parse_cutover(CUTOVER),
        horizon_days=30,
    )


def stub_source(monkeypatch: pytest.MonkeyPatch, records: list[dict[str, Any]]) -> None:
    async def _fetch(*args: Any, **kwargs: Any) -> list[dict[str, Any]]:
        return list(records)

    monkeypatch.setattr(prepare_module, "fetch_company_records", _fetch)


def manifest_without_service() -> str:
    payload = json.loads(manifest_json())
    payload["branches"][str(KARLSRUHE_COMPANY_ID)]["services"] = {}
    return json.dumps(payload)


# ---------------------------------------------------------------------------
# Service proposals
# ---------------------------------------------------------------------------


def propose(
    catalog_rows: list[dict[str, Any]],
    *,
    records: list[dict[str, Any]] | None = None,
    branch: Any = None,
    staff: dict[str, Any] | None = None,
    staff_ids: set[int] | None = None,
):
    """Build proposals the way the shared snapshot builder does.

    ``branch`` defaults to a branch that maps the STAFF but no services, which is
    what a fresh service proposal needs: availability is judged through the
    Altegio-id → EasyWeek-uuid staff mapping, so a branch without it would make
    every verdict `unstated` for the wrong reason. Pass `mapped_branch()` to
    exercise an existing mapping.
    """
    if branch is None:
        branch = inventory_manifest(manifest_without_service()).branch(KARLSRUHE_COMPANY_ID)
    snapshot = build_catalog_snapshot(KA_LOCATION_UUID, catalog_rows)
    services = collect_source_services(records or [source_record()], staff_ids=staff_ids or {KA_STAFF_ID})
    return propose_service_mapping(
        altegio_company_id=KARLSRUHE_COMPANY_ID,
        source_services=services,
        catalog=snapshot,
        catalog_staff=staff or {},
        branch=branch,
    )


async def snapshot_for(inputs: PrepareInputs, client: Any = None) -> Any:
    """The same live snapshot the CLI builds before every confirm."""
    return await build_preparation_snapshot(inputs, write_client=client or FakePrepareClient())


async def confirm_customer(inputs: PrepareInputs, review: Any, phone: str, *, client: Any = None) -> dict[str, Any]:
    """Confirm one customer the way an operator does: identifier plus digest."""
    digest = next(row["review_digest"] for row in review.operator["customers"] if row["phone"] == phone)
    return apply_confirmations(
        inputs,
        ConfirmRequest(confirm_customers=(ConfirmTarget(identifier=phone, review_digest=digest),)),
        snapshot=await snapshot_for(inputs, client),
    )


def load_decisions(state_dir: Path) -> Any:
    store = CustomerDecisionStore(state_dir)
    with store:
        return store.load()


def mapped_branch() -> Any:
    """The Karlsruhe branch with its reviewed service baseline in place."""
    return parse_manifest(manifest_json()).branch(KARLSRUHE_COMPANY_ID)


def test_an_exact_name_match_is_proposed_with_its_numbers() -> None:
    [proposal] = propose([catalog_row()])

    assert proposal.status == PROPOSAL_UNIQUE_NAME
    assert proposal.actionable is True
    candidate = proposal.chosen
    assert candidate.easyweek_service_uuid == KA_SERVICE_UUID
    assert candidate.price_text == "90.00"
    assert candidate.duration_minutes == 60
    assert candidate.currency == "EUR"


def test_the_proposal_states_the_source_id_and_the_booking_count() -> None:
    records = [source_record(900001), source_record(900002)]
    [proposal] = propose([catalog_row()], records=records)
    shown = proposal.as_operator_dict()

    assert shown["altegio_service_id"] == KA_SERVICE_ID
    assert shown["altegio_service_name"] == "Wimpernverlängerung 2D"
    assert shown["booking_count"] == 2
    assert shown["target"]["easyweek_service_name"] == "Wimpernverlängerung 2D"


def test_a_near_miss_is_not_a_match() -> None:
    """One letter off in a salon catalogue is a different service."""
    [proposal] = propose([catalog_row(name="Wimpernverlängerung 3D")])

    assert proposal.status == PROPOSAL_NO_CANDIDATE
    assert proposal.actionable is False
    assert proposal.chosen is None


def test_two_catalogue_entries_with_one_name_are_ambiguous() -> None:
    [proposal] = propose([catalog_row(), catalog_row(OTHER_SERVICE_UUID, price=12000)])

    assert proposal.status == PROPOSAL_AMBIGUOUS
    assert proposal.actionable is False
    assert len(proposal.candidates) == 2, "the operator sees both, and picks neither by default"


def test_a_case_or_accent_difference_still_matches() -> None:
    [proposal] = propose([catalog_row(name="WIMPERNVERLÄNGERUNG   2D")])

    assert proposal.status == PROPOSAL_UNIQUE_NAME


def test_a_service_the_master_cannot_perform_is_never_actionable() -> None:
    proposals = propose(
        [catalog_row(staff=["other-uuid"])],
        staff={KA_SERVICE_UUID: frozenset({"other-uuid"})},
    )

    assert proposals[0].chosen.staff_availability == STAFF_AVAILABILITY_ABSENT
    assert proposals[0].actionable is False


def test_availability_is_proven_when_the_catalogue_names_the_master() -> None:
    proposals = propose(
        [catalog_row(staff=[KA_STAFF_UUID])],
        staff={KA_SERVICE_UUID: frozenset({KA_STAFF_UUID})},
    )

    assert proposals[0].chosen.staff_availability == STAFF_AVAILABILITY_PROVEN
    assert proposals[0].chosen.required_staff_uuids == (KA_STAFF_UUID,)


def test_a_catalogue_that_states_no_staff_says_so_rather_than_passing() -> None:
    """Silence is not a claim, and it is not invented into one."""
    proposals = propose([catalog_row()], staff={KA_SERVICE_UUID: None})

    assert proposals[0].chosen.staff_availability == STAFF_AVAILABILITY_UNSTATED
    assert proposals[0].actionable is True, "unprovable is not refused, it is flagged"


def test_a_master_with_no_easyweek_uuid_leaves_availability_unstated() -> None:
    """We cannot check coverage for somebody the manifest cannot name."""
    unmapped = source_record()
    unmapped["staff_id"] = KA_OTHER_STAFF_ID
    proposals = propose(
        [catalog_row(staff=[KA_STAFF_UUID])],
        records=[unmapped],
        staff={KA_SERVICE_UUID: frozenset({KA_STAFF_UUID})},
        staff_ids={KA_OTHER_STAFF_ID},
    )

    candidate = proposals[0].chosen
    assert candidate.staff_availability == STAFF_AVAILABILITY_UNSTATED
    assert candidate.unmapped_staff_ids == (KA_OTHER_STAFF_ID,)


def test_the_staff_reader_distinguishes_absent_from_empty() -> None:
    assert read_service_staff_uuids({"uuid": "x"}) is None
    assert read_service_staff_uuids({"uuid": "x", "employees": []}) == frozenset()
    assert read_service_staff_uuids({"uuid": "x", "employees": [{"uuid": "a"}]}) == frozenset({"a"})
    assert read_service_staff_uuids({"uuid": "x", "employees": [17]}) is None


def test_an_unchanged_existing_mapping_is_not_re_proposed() -> None:
    [proposal] = propose([baseline_catalog_row()], branch=mapped_branch())

    assert proposal.status == PROPOSAL_ALREADY_MAPPED
    assert proposal.existing_uuid == KA_SERVICE_UUID
    assert proposal.drift_fields == ()
    assert proposal.settled is True


def test_a_mapped_uuid_missing_from_the_catalogue_is_flagged() -> None:
    [proposal] = propose([catalog_row(OTHER_SERVICE_UUID)], branch=mapped_branch())

    assert proposal.status == "conflicts_with_manifest"
    assert proposal.actionable is False
    assert proposal.settled is False


# ---------------------------------------------------------------------------
# Agreeing a mapping, and what reaches the manifest
# ---------------------------------------------------------------------------


def test_only_a_confirmed_proposal_reaches_the_manifest() -> None:
    [proposal] = propose([catalog_row()])
    agreement = MappingAgreement()

    assert manifest_service_patch([proposal], agreement) == {}

    agreement.confirm(proposal)
    patch = manifest_service_patch([proposal], agreement)

    assert patch[str(KA_SERVICE_ID)] == {
        "easyweek_service_uuid": KA_SERVICE_UUID,
        "catalog_duration_minutes": 60,
        "catalog_price": "90.00",
        "catalog_service_name": "wimpernverlängerung 2d",
        "catalog_currency": "EUR",
    }


def test_an_ambiguous_proposal_cannot_be_confirmed() -> None:
    [proposal] = propose([catalog_row(), catalog_row(OTHER_SERVICE_UUID)])

    with pytest.raises(ValueError):
        MappingAgreement().confirm(proposal)


def test_an_agreement_is_reused_and_does_not_re_ask() -> None:
    [proposal] = propose([catalog_row()])
    agreement = MappingAgreement()
    agreement.confirm(proposal)

    [again] = propose([catalog_row()])
    assert agreement.agreed(again) is True


def test_an_agreement_lapses_when_the_target_moves() -> None:
    [proposal] = propose([catalog_row()])
    agreement = MappingAgreement()
    agreement.confirm(proposal)

    [moved] = propose([catalog_row(price=12000)])
    assert agreement.agreed(moved) is False, "the price is part of what was agreed"


def test_the_merge_is_additive_and_never_repoints_an_earlier_wave() -> None:
    original = json.loads(manifest_json())
    merged = merge_manifest_services(
        original,
        altegio_company_id=KARLSRUHE_COMPANY_ID,
        patch={str(KA_SERVICE_ID): {"easyweek_service_uuid": OTHER_SERVICE_UUID}},
    )

    branch = merged["branches"][str(KARLSRUHE_COMPANY_ID)]["services"]
    assert branch[str(KA_SERVICE_ID)]["easyweek_service_uuid"] == KA_SERVICE_UUID
    assert original == json.loads(manifest_json()), "the input is not mutated"


def test_the_merged_manifest_still_parses_strictly() -> None:
    stripped = json.loads(manifest_without_service())
    [proposal] = propose([catalog_row()])
    agreement = MappingAgreement()
    agreement.confirm(proposal)

    merged = merge_manifest_services(
        stripped,
        altegio_company_id=KARLSRUHE_COMPANY_ID,
        patch=manifest_service_patch([proposal], agreement),
    )
    parsed = parse_manifest(json.dumps(merged))

    assert parsed.valid, parsed.reason
    assert parsed.branch(KARLSRUHE_COMPANY_ID).service_uuid(KA_SERVICE_ID) == KA_SERVICE_UUID


# ---------------------------------------------------------------------------
# The read-only preparation pass
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_prepare_writes_the_three_artefacts_the_migrator_reads(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stub_source(monkeypatch, [source_record()])
    client = FakePrepareClient(customers={PHONE: page([card()])})

    result = await run_prepare(make_inputs(state_dir), write_client=client)

    for name in (FILE_MANIFEST_PROPOSED, FILE_CUSTOMER_DIRECTORY, FILE_OPERATOR_REVIEW):
        assert (state_dir / name).exists(), name
    assert result.machine["customers"]["ready"] == 1


@pytest.mark.asyncio
async def test_prepare_never_creates_a_customer(state_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """There is no code path from prepare to a mutation; the client has no method."""
    stub_source(monkeypatch, [source_record()])
    client = FakePrepareClient()

    await run_prepare(make_inputs(state_dir), write_client=client)

    assert not hasattr(client, "create_customer")
    assert "customers" in client.calls


@pytest.mark.asyncio
async def test_an_absent_customer_becomes_a_pending_decision(state_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    stub_source(monkeypatch, [source_record()])
    result = await run_prepare(make_inputs(state_dir), write_client=FakePrepareClient())

    assert result.machine["customers"]["pending_confirmation"] == 1
    assert result.machine["ready"]["all_clear"] is False


@pytest.mark.asyncio
async def test_an_undetermined_lookup_is_reported_as_a_technical_block(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from altegio_bot.easyweek_client import EasyWeekRetryableError

    stub_source(monkeypatch, [source_record()])
    client = FakePrepareClient(customers={PHONE: EasyWeekRetryableError("t", operation="list_customers")})

    result = await run_prepare(make_inputs(state_dir), write_client=client)

    assert result.machine["ready"]["blocked_by_technical_error"] == 1
    assert result.machine["customers"]["pending_confirmation"] == 0, "an error is not an absence"


@pytest.mark.asyncio
async def test_readiness_is_reported_in_separate_lines_not_one_word(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stub_source(monkeypatch, [source_record()])
    result = await run_prepare(make_inputs(state_dir), write_client=FakePrepareClient())

    ready = result.machine["ready"]
    assert set(ready) == {
        "customers_ready",
        "mapping_ready",
        "records_ready",
        "records_needing_manual_work",
        "blocked_by_technical_error",
        "manual_corrections_stale",
        "all_clear",
    }


@pytest.mark.asyncio
async def test_a_per_booking_override_is_reported_as_manual_work_not_prepared_away(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    discounted = source_record()
    discounted["services"][0]["cost_to_pay"] = 0.0
    stub_source(monkeypatch, [discounted])

    result = await run_prepare(make_inputs(state_dir), write_client=FakePrepareClient())

    assert result.machine["source"]["records_needing_manual_work"] == {"custom_price_unsupported": 1}
    assert result.machine["customers"]["states"] == {}, "its customer is not proposed for creation"


@pytest.mark.asyncio
async def test_the_machine_report_carries_no_personal_data(state_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    stub_source(
        monkeypatch,
        [source_record(client={"phone": PHONE, "first_name": "Testkundin", "email": "k@example.invalid", "id": 42})],
    )
    result = await run_prepare(make_inputs(state_dir), write_client=FakePrepareClient())
    blob = json.dumps(result.machine, ensure_ascii=False)

    for secret in (PHONE, "Testkundin", "k@example.invalid", "Wimpernverlängerung"):
        assert secret not in blob, secret


@pytest.mark.asyncio
async def test_the_operator_report_carries_what_a_person_needs_to_decide(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stub_source(monkeypatch, [source_record()])
    result = await run_prepare(
        make_inputs(state_dir, manifest_text=manifest_without_service()), write_client=FakePrepareClient()
    )
    blob = json.dumps(result.operator, ensure_ascii=False)

    assert PHONE in blob and "Testkundin" in blob
    assert "Wimpernverlängerung 2D" in blob
    assert "does NOT transfer visit history" in blob
    assert result.operator["timezone"] == "Europe/Berlin"
    assert result.operator["warning"].startswith("CONTAINS PERSONAL DATA")


@pytest.mark.asyncio
async def test_the_operator_report_lists_every_booking_with_its_local_times(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The list an operator checks against the Altegio screen."""
    discounted = source_record(900002)
    discounted["services"][0]["cost_to_pay"] = 0.0
    stub_source(monkeypatch, [source_record(900001), discounted])

    # The full manifest, so the discounted row reaches the PRICE check rather
    # than stopping at the missing mapping one step earlier.
    result = await run_prepare(make_inputs(state_dir), write_client=FakePrepareClient())
    rows = {row["altegio_record_id"]: row for row in result.operator["records"]}

    assert set(rows) == {900001, 900002}
    ready = rows[900001]
    # 12:00 local on 10 September is CEST — the salon's clock, and the instant.
    assert ready["starts_at_local"] == "2026-09-10T12:00:00+02:00"
    assert ready["ends_at_local"] == "2026-09-10T13:00:00+02:00"
    assert ready["starts_at_utc"] == "2026-09-10T10:00:00Z"
    assert ready["duration_minutes"] == 60
    assert ready["altegio_service_name"] == "Wimpernverlängerung 2D"
    assert ready["price"] == "90.0"
    assert ready["altegio_staff_id"] == KA_STAFF_ID

    assert rows[900002]["block_reason"] == "custom_price_unsupported"
    assert rows[900002]["price_to_pay"] == "0.0"


@pytest.mark.asyncio
async def test_an_unresolvable_local_time_is_reported_not_approximated(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The spring gap and the autumn fold have no single instant to print."""
    ambiguous = source_record()
    ambiguous["date"] = "2026-10-25 02:30:00"
    stub_source(monkeypatch, [ambiguous])

    result = await run_prepare(make_inputs(state_dir), write_client=FakePrepareClient())
    [row] = result.operator["records"]

    assert row["starts_at_local"] is None
    assert row["starts_at_utc"] is None


@pytest.mark.asyncio
async def test_the_operator_report_is_not_world_readable(state_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    stub_source(monkeypatch, [source_record()])
    await run_prepare(make_inputs(state_dir), write_client=FakePrepareClient())

    assert (os.stat(state_dir / FILE_OPERATOR_REVIEW).st_mode & 0o077) == 0
    assert (os.stat(state_dir).st_mode & 0o077) == 0


@pytest.mark.asyncio
async def test_prepare_refuses_an_unproven_branch_identity(state_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from altegio_bot.settings import settings

    stub_source(monkeypatch, [source_record()])
    monkeypatch.setattr(settings, "easyweek_location_map", "{}", raising=False)

    with pytest.raises(PrepareError):
        await run_prepare(make_inputs(state_dir), write_client=FakePrepareClient())


@pytest.mark.asyncio
async def test_prepare_is_idempotent_for_unchanged_data(state_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Re-running must not re-ask about a customer already confirmed."""
    stub_source(monkeypatch, [source_record()])
    inputs = make_inputs(state_dir)

    first = await run_prepare(inputs, write_client=FakePrepareClient())
    await confirm_customer(inputs, first, PHONE)
    result = await run_prepare(inputs, write_client=FakePrepareClient())

    assert result.machine["customers"]["pending_confirmation"] == 0
    assert result.machine["customers"]["confirmed_not_yet_created"] == 1


@pytest.mark.asyncio
async def test_the_catalogue_is_read_once_per_run(state_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """One walk: a second one costs rate budget and can disagree with the first."""
    stub_source(monkeypatch, [source_record()])
    client = FakePrepareClient()

    await run_prepare(make_inputs(state_dir), write_client=client)

    assert client.calls.count("catalog") == 1


@pytest.mark.asyncio
async def test_staff_availability_is_read_from_the_same_catalogue_rows(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stub_source(monkeypatch, [source_record()])
    client = FakePrepareClient(catalog=[catalog_row(staff=["someone-else"])])

    result = await run_prepare(make_inputs(state_dir, manifest_text=manifest_without_service()), write_client=client)

    [proposal] = result.machine["mapping"]["proposals"]
    assert proposal["staff_availability"] == STAFF_AVAILABILITY_ABSENT
    assert proposal["actionable"] is False
    assert result.machine["ready"]["mapping_ready"] is False


@pytest.mark.asyncio
async def test_a_confirmed_mapping_reaches_the_proposed_manifest(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stub_source(monkeypatch, [source_record()])
    inputs = make_inputs(state_dir, manifest_text=manifest_without_service())
    result = await run_prepare(inputs, write_client=FakePrepareClient())

    apply_confirmations(
        inputs,
        ConfirmRequest(
            confirm_all_services=True,
            expected_mapping_digest=result.machine["mapping"]["pending_digest"],
        ),
        snapshot=await snapshot_for(inputs),
    )

    merged = json.loads((state_dir / FILE_MANIFEST_PROPOSED).read_text())
    services = merged["branches"][str(KARLSRUHE_COMPANY_ID)]["services"]
    assert services[str(KA_SERVICE_ID)]["easyweek_service_uuid"] == KA_SERVICE_UUID
    assert parse_manifest(json.dumps(merged)).valid


@pytest.mark.asyncio
async def test_a_mapping_batch_confirmation_refuses_a_moved_list(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stub_source(monkeypatch, [source_record()])
    inputs = make_inputs(state_dir, manifest_text=manifest_without_service())
    await run_prepare(inputs, write_client=FakePrepareClient())

    with pytest.raises(PrepareError):
        apply_confirmations(
            inputs,
            ConfirmRequest(confirm_all_services=True, expected_mapping_digest="stale"),
            snapshot=await snapshot_for(inputs),
        )

    assert (
        not (state_dir / FILE_MANIFEST_PROPOSED).exists()
        or json.loads((state_dir / FILE_MANIFEST_PROPOSED).read_text())["branches"][str(KARLSRUHE_COMPANY_ID)][
            "services"
        ]
        == {}
    )


# ---------------------------------------------------------------------------
# The CLI's permission split
# ---------------------------------------------------------------------------


def test_creating_customers_needs_both_halves_of_the_permission(monkeypatch: pytest.MonkeyPatch) -> None:
    args = cli.build_parser().parse_args(
        [
            "create-customers",
            "--manifest",
            "m.json",
            "--company-id",
            str(KARLSRUHE_COMPANY_ID),
            "--cutover-at",
            CUTOVER,
        ]
    )

    monkeypatch.delenv(cli.CREATE_ENV_FLAG, raising=False)
    assert cli._create_permitted(args) is False

    monkeypatch.setenv(cli.CREATE_ENV_FLAG, "true")
    assert cli._create_permitted(args) is False, "the environment alone is not authorisation"

    args.authorise_customer_create = True
    assert cli._create_permitted(args) is True

    monkeypatch.setenv(cli.CREATE_ENV_FLAG, "false")
    assert cli._create_permitted(args) is False, "the flag alone is not authorisation either"


def test_the_prepare_cli_cannot_migrate_a_booking() -> None:
    parser = cli.build_parser()
    actions = {action.dest for action in parser._actions}

    assert "apply" not in actions
    assert "verified_dry_run_id" not in actions
    assert "canary_record_id" not in actions


def test_the_migrator_cli_cannot_create_a_customer() -> None:
    from altegio_bot.scripts import easyweek_migration as migrator

    actions = {action.dest for action in migrator.build_parser()._actions}
    assert "authorise_customer_create" not in actions


# ---------------------------------------------------------------------------
# Handing the verified dry-run id over
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_verified_dry_run_id_comes_from_the_report_this_run_produced(
    state_dir: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Never "the newest file in the report directory" — that is how an operator
    approves one plan and applies another."""

    class Report:
        plan_digest = "digest-of-this-very-run"
        outcomes = {"ready": 3, "blocked": 1}
        errors: list[str] = []

    async def _dry_run(_session: Any, _inputs: Any, **_kwargs: Any) -> Report:
        return Report()

    class Session:
        async def __aenter__(self) -> Session:
            return self

        async def __aexit__(self, *exc: Any) -> None:
            return None

    monkeypatch.setattr(cli, "run_inventory_or_dry_run", _dry_run)
    monkeypatch.setattr(cli, "SessionLocal", Session)

    directory = state_dir / FILE_CUSTOMER_DIRECTORY
    directory.parent.mkdir(parents=True, exist_ok=True)
    directory.write_text(json.dumps([{"uuid": UUID_A, "phone": PHONE, "first_name": "Testkundin"}]))

    manifest_path = tmp_path / "manifest.proposed.json"
    manifest_path.write_text(manifest_json())
    args = cli.build_parser().parse_args(
        [
            "verify-dry-run",
            "--manifest",
            str(manifest_path),
            "--company-id",
            str(KARLSRUHE_COMPANY_ID),
            "--cutover-at",
            CUTOVER,
            "--state-dir",
            str(state_dir),
        ]
    )

    assert await cli._verify_dry_run(args, make_inputs(state_dir)) == 0

    printed = json.loads(capsys.readouterr().out)
    assert printed["verified_dry_run_id"] == "digest-of-this-very-run"
    assert printed["ready_rows"] == 3

    handover = printed["next_command_after_a_clean_canary"]
    assert "--verified-dry-run-id digest-of-this-very-run" in handover
    assert str(manifest_path) in handover
    assert str(directory) in handover
    assert "--confirm-easyweek-native-notifications-disabled" in handover, "the attestation is still required"


@pytest.mark.asyncio
async def test_verifying_without_a_prepared_directory_refuses(state_dir: Path, tmp_path: Path) -> None:
    manifest_path = tmp_path / "manifest.proposed.json"
    manifest_path.write_text(manifest_json())
    args = cli.build_parser().parse_args(
        [
            "verify-dry-run",
            "--manifest",
            str(manifest_path),
            "--company-id",
            str(KARLSRUHE_COMPANY_ID),
            "--cutover-at",
            CUTOVER,
            "--state-dir",
            str(state_dir),
        ]
    )

    assert await cli._verify_dry_run(args, make_inputs(state_dir)) == 1


@pytest.mark.asyncio
async def test_the_handover_command_parses_with_the_migrators_own_parser(
    state_dir: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """A handover command that does not parse is a handover that fails at apply."""
    import shlex

    from altegio_bot.scripts import easyweek_migration as migrator

    class Report:
        plan_digest = "d"
        outcomes: dict[str, int] = {}
        errors: list[str] = []

    class Session:
        async def __aenter__(self) -> Session:
            return self

        async def __aexit__(self, *exc: Any) -> None:
            return None

    async def _dry_run(_session: Any, _inputs: Any, **_kwargs: Any) -> Report:
        return Report()

    monkeypatch.setattr(cli, "run_inventory_or_dry_run", _dry_run)
    monkeypatch.setattr(cli, "SessionLocal", Session)

    directory = state_dir / FILE_CUSTOMER_DIRECTORY
    directory.parent.mkdir(parents=True, exist_ok=True)
    directory.write_text(json.dumps([{"uuid": UUID_A, "phone": PHONE, "first_name": "T"}]))
    manifest_path = tmp_path / "manifest.proposed.json"
    manifest_path.write_text(manifest_json())

    args = cli.build_parser().parse_args(
        [
            "verify-dry-run",
            "--manifest",
            str(manifest_path),
            "--company-id",
            str(KARLSRUHE_COMPANY_ID),
            "--cutover-at",
            CUTOVER,
            "--state-dir",
            str(state_dir),
        ]
    )
    await cli._verify_dry_run(args, make_inputs(state_dir))
    handover = json.loads(capsys.readouterr().out)["next_command_after_a_clean_canary"]

    tokens = shlex.split(handover)
    parsed = migrator.build_parser().parse_args(tokens[tokens.index("-m") + 2 :])

    assert parsed.mode == "apply"
    assert parsed.apply is True
    assert parsed.verified_dry_run_id == "d"
    assert parsed.confirm_native_notifications_disabled is True


# ---------------------------------------------------------------------------
# Prepare and confirm must see the same wave (review finding 1)
# ---------------------------------------------------------------------------


def out_of_scope_records() -> list[dict[str, Any]]:
    """One booking before the cutover and one already cancelled.

    Both are out of the wave, and the confirm path used to collect services from
    every fetched booking — so a service only these rows use could be proposed,
    digested and confirmed although nothing in the wave uses it.
    """
    past = source_record(900010, service_id=SECOND_SERVICE_ID, service_name="Alte Leistung")
    past["date"] = "2026-08-20 12:00:00"
    cancelled = source_record(900011, service_id=SECOND_SERVICE_ID, service_name="Alte Leistung")
    cancelled["deleted"] = True
    return [past, cancelled]


@pytest.mark.asyncio
async def test_prepare_proposes_only_services_the_wave_actually_uses(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stub_source(monkeypatch, [source_record(), *out_of_scope_records()])

    result = await run_prepare(
        make_inputs(state_dir, manifest_text=manifest_without_service()), write_client=FakePrepareClient()
    )

    proposed = {row["altegio_service_id"] for row in result.machine["mapping"]["proposals"]}
    assert proposed == {KA_SERVICE_ID}, "the pre-cutover and cancelled rows are not this wave"
    assert result.machine["source"]["records_in_scope"] == 1
    assert result.machine["source"]["records_fetched"] == 3


@pytest.mark.asyncio
async def test_confirm_rebuilds_the_same_wave_and_the_same_digests(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Unchanged inputs, identical proposals — item digests and list digest."""
    stub_source(monkeypatch, [source_record(), *out_of_scope_records()])
    inputs = make_inputs(state_dir, manifest_text=manifest_without_service())

    review = await run_prepare(inputs, write_client=FakePrepareClient())
    snapshot = await snapshot_for(inputs)

    reviewed = {row["altegio_service_id"]: row["review_digest"] for row in result_proposals(review)}
    rebuilt = {proposal.source.altegio_service_id: proposal_digest(proposal) for proposal in snapshot.proposals}
    assert reviewed == rebuilt
    assert (
        mapping_pending_digest(list(snapshot.proposals), MappingAgreement())
        == (review.machine["mapping"]["pending_digest"])
    )
    assert {row["phone"]: row["review_digest"] for row in review.operator["customers"]} == {
        phone: record.shown_digest for phone, record in snapshot.customer_proposals.items()
    }


def result_proposals(review: Any) -> list[dict[str, Any]]:
    return review.operator["service_mapping"]


@pytest.mark.asyncio
async def test_confirm_reads_the_catalogue_once_and_reverifies_branch_identity(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from altegio_bot.settings import settings

    stub_source(monkeypatch, [source_record()])
    inputs = make_inputs(state_dir, manifest_text=manifest_without_service())

    client = FakePrepareClient()
    await build_preparation_snapshot(inputs, write_client=client)
    assert client.calls.count("catalog") == 1

    monkeypatch.setattr(settings, "easyweek_location_map", "{}", raising=False)
    with pytest.raises(PrepareError):
        await build_preparation_snapshot(inputs, write_client=FakePrepareClient())


@pytest.mark.asyncio
async def test_confirm_cannot_make_an_unavailable_service_actionable(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The catalogue withholds the service from the master who books it."""
    stub_source(monkeypatch, [source_record()])
    inputs = make_inputs(state_dir, manifest_text=manifest_without_service())
    client = FakePrepareClient(catalog=[catalog_row(staff=["somebody-else"])])

    review = await run_prepare(inputs, write_client=client)
    [shown] = review.machine["mapping"]["proposals"]
    assert shown["actionable"] is False
    assert shown["staff_availability"] == STAFF_AVAILABILITY_ABSENT

    snapshot = await snapshot_for(inputs, FakePrepareClient(catalog=[catalog_row(staff=["somebody-else"])]))
    with pytest.raises(PrepareError):
        apply_confirmations(
            inputs,
            ConfirmRequest(
                confirm_services=(ConfirmTarget(identifier=str(KA_SERVICE_ID), review_digest=shown["review_digest"]),)
            ),
            snapshot=snapshot,
        )

    merged = json.loads((state_dir / FILE_MANIFEST_PROPOSED).read_text())
    assert merged["branches"][str(KARLSRUHE_COMPANY_ID)]["services"] == {}


# ---------------------------------------------------------------------------
# Availability is per service, not per wave (review finding 3)
# ---------------------------------------------------------------------------


def two_master_records() -> list[dict[str, Any]]:
    """Master A books the service; master B is in the wave and books another."""
    a = source_record(900001)
    b = source_record(900002, service_id=SECOND_SERVICE_ID, service_name="Zweite Leistung")
    b["staff_id"] = KA_SECOND_STAFF_ID
    return [a, b]


def two_master_branch() -> Any:
    payload = json.loads(manifest_without_service())
    branch = payload["branches"][str(KARLSRUHE_COMPANY_ID)]
    branch["selected_altegio_staff_ids"] = [KA_STAFF_ID, KA_SECOND_STAFF_ID]
    branch["deferred_altegio_staff_ids"] = []
    branch["staff"][str(KA_SECOND_STAFF_ID)] = KA_SECOND_STAFF_UUID
    return inventory_manifest(json.dumps(payload)).branch(KARLSRUHE_COMPANY_ID)


def test_another_selected_masters_access_is_not_evidence() -> None:
    """A books it; the catalogue offers it only to B. That proves nothing."""
    proposals = propose(
        [catalog_row(staff=[KA_SECOND_STAFF_UUID])],
        records=two_master_records(),
        branch=two_master_branch(),
        staff={KA_SERVICE_UUID: frozenset({KA_SECOND_STAFF_UUID})},
        staff_ids={KA_STAFF_ID, KA_SECOND_STAFF_ID},
    )
    by_id = {proposal.source.altegio_service_id: proposal for proposal in proposals}

    candidate = by_id[KA_SERVICE_ID].chosen
    assert candidate.staff_availability == STAFF_AVAILABILITY_ABSENT
    assert candidate.required_staff_uuids == (KA_STAFF_UUID,), "A books it, so A is who must be covered"
    assert by_id[KA_SERVICE_ID].actionable is False


def test_every_master_who_books_the_service_must_be_covered() -> None:
    """Both masters book it; the catalogue names only one. Not proven."""
    shared = two_master_records()
    shared[1]["services"][0]["id"] = KA_SERVICE_ID
    shared[1]["services"][0]["title"] = "Wimpernverlängerung 2D"

    [proposal] = propose(
        [catalog_row(staff=[KA_STAFF_UUID])],
        records=shared,
        branch=two_master_branch(),
        staff={KA_SERVICE_UUID: frozenset({KA_STAFF_UUID})},
        staff_ids={KA_STAFF_ID, KA_SECOND_STAFF_ID},
    )

    assert proposal.chosen.required_staff_uuids == tuple(sorted((KA_STAFF_UUID, KA_SECOND_STAFF_UUID)))
    assert proposal.chosen.staff_availability == STAFF_AVAILABILITY_ABSENT


def test_availability_is_proven_when_every_actual_master_is_covered() -> None:
    shared = two_master_records()
    shared[1]["services"][0]["id"] = KA_SERVICE_ID
    shared[1]["services"][0]["title"] = "Wimpernverlängerung 2D"

    [proposal] = propose(
        [catalog_row(staff=[KA_STAFF_UUID, KA_SECOND_STAFF_UUID])],
        records=shared,
        branch=two_master_branch(),
        staff={KA_SERVICE_UUID: frozenset({KA_STAFF_UUID, KA_SECOND_STAFF_UUID})},
        staff_ids={KA_STAFF_ID, KA_SECOND_STAFF_ID},
    )

    assert proposal.chosen.staff_availability == STAFF_AVAILABILITY_PROVEN
    assert proposal.actionable is True


def test_a_catalogue_naming_extra_masters_is_still_proven() -> None:
    """Coverage is a superset test, not an equality one."""
    [proposal] = propose(
        [catalog_row(staff=[KA_STAFF_UUID, KA_SECOND_STAFF_UUID, "a-third-master"])],
        staff={KA_SERVICE_UUID: frozenset({KA_STAFF_UUID, KA_SECOND_STAFF_UUID, "a-third-master"})},
    )

    assert proposal.chosen.staff_availability == STAFF_AVAILABILITY_PROVEN


# ---------------------------------------------------------------------------
# Existing mappings drift (review finding 4)
# ---------------------------------------------------------------------------


DRIFTS = {
    "catalog_service_name": {"name": "Mascara Effekt XL"},
    "catalog_currency": {"currency": "CHF"},
    "catalog_price": {"price": 9500},
    "catalog_duration_minutes": {"duration": {"value": 90, "label": "minutes"}},
}


def mapped_source() -> list[dict[str, Any]]:
    """A booking whose service the manifest already maps."""
    return [source_record(service_name=BASELINE_NAME)]


def test_an_unchanged_existing_mapping_needs_no_confirmation() -> None:
    [proposal] = propose([baseline_catalog_row()], records=mapped_source(), branch=mapped_branch())

    assert proposal.status == PROPOSAL_ALREADY_MAPPED
    assert proposal.settled is True
    assert proposal.drift_fields == ()


@pytest.mark.parametrize("field", sorted(DRIFTS))
def test_each_drifted_baseline_field_is_reported_as_a_conflict(field: str) -> None:
    """A matching UUID over a moved service is not a matching service."""
    [proposal] = propose([baseline_catalog_row(**DRIFTS[field])], records=mapped_source(), branch=mapped_branch())

    assert proposal.status == PROPOSAL_BASELINE_DRIFT
    assert proposal.drift_fields == (field,)
    assert proposal.settled is False
    assert proposal.actionable is False, "drift is not something a confirmation resolves"


def test_a_drifted_mapping_names_the_changed_field_to_the_operator() -> None:
    [proposal] = propose(
        [baseline_catalog_row(**DRIFTS["catalog_price"])], records=mapped_source(), branch=mapped_branch()
    )
    shown = proposal.as_operator_dict()

    assert shown["drift_fields"] == ["catalog_price"]
    assert shown["existing_manifest_baseline"]["catalog_price"] == "90.00"
    assert shown["target"] is None, "a drifted mapping offers no target to accept"
    assert shown["candidates"][0]["price"] == "95.00", "and the live value is shown next to it"


@pytest.mark.asyncio
@pytest.mark.parametrize("field", sorted(DRIFTS))
async def test_drift_makes_the_wave_not_ready_and_rewrites_no_manifest(
    field: str, state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stub_source(monkeypatch, mapped_source())
    inputs = make_inputs(state_dir)
    client = FakePrepareClient(catalog=[baseline_catalog_row(**DRIFTS[field])])

    result = await run_prepare(inputs, write_client=client)

    assert result.machine["ready"]["mapping_ready"] is False
    assert [row["altegio_service_id"] for row in result.machine["mapping"]["drift"]] == [KA_SERVICE_ID]

    merged = json.loads((state_dir / FILE_MANIFEST_PROPOSED).read_text())
    entry = merged["branches"][str(KARLSRUHE_COMPANY_ID)]["services"][str(KA_SERVICE_ID)]
    assert entry["catalog_service_name"] == BASELINE_NAME, "the reviewed baseline is never overwritten"
    assert entry["catalog_price"] == "90.00"
    assert entry["catalog_duration_minutes"] == 60


@pytest.mark.asyncio
async def test_an_unchanged_existing_mapping_reports_the_wave_mapping_ready(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stub_source(monkeypatch, mapped_source())
    client = FakePrepareClient(catalog=[baseline_catalog_row()])

    result = await run_prepare(make_inputs(state_dir), write_client=client)

    assert result.machine["ready"]["mapping_ready"] is True
    assert result.machine["mapping"]["drift"] == []


def test_a_mapping_without_a_reviewed_baseline_is_not_readiness() -> None:
    """Mapped, present, and nothing recorded to compare it against."""
    payload = json.loads(manifest_json())
    entry = payload["branches"][str(KARLSRUHE_COMPANY_ID)]["services"][str(KA_SERVICE_ID)]
    entry.pop("catalog_service_name")
    entry.pop("catalog_currency")
    branch = inventory_manifest(json.dumps(payload)).branch(KARLSRUHE_COMPANY_ID)

    [proposal] = propose([baseline_catalog_row()], records=mapped_source(), branch=branch)

    assert proposal.status == PROPOSAL_BASELINE_INCOMPLETE
    assert proposal.settled is False
    assert proposal.actionable is False


# ---------------------------------------------------------------------------
# The digest covers everything shown (review finding 5)
# ---------------------------------------------------------------------------


def digest_of(catalog_rows: list[dict[str, Any]], records: list[dict[str, Any]] | None = None) -> str:
    [proposal] = propose(catalog_rows, records=records)
    return proposal_digest(proposal)


BASE_DIGEST_INPUTS: dict[str, Any] = {}


def test_the_review_payload_and_the_digest_are_the_same_structure() -> None:
    """One canonical payload, so a shown field cannot escape the digest."""
    [proposal] = propose([catalog_row()])
    shown = proposal.as_operator_dict()

    rebuilt = dict(shown)
    rebuilt.pop("review_digest")
    assert rebuilt == proposal.review_payload()
    assert shown["review_digest"] == proposal_digest(proposal)


def test_a_changed_source_price_changes_the_digest() -> None:
    before = digest_of([catalog_row()])
    dearer = source_record()
    dearer["services"][0]["cost"] = 95.0
    dearer["services"][0]["cost_to_pay"] = 95.0

    assert digest_of([catalog_row()], [dearer]) != before


def test_a_changed_source_duration_changes_the_digest() -> None:
    before = digest_of([catalog_row()])
    longer = source_record()
    # The service line's own length is what the review shows as the observed
    # source duration, so that is the field the digest has to follow.
    longer["services"][0]["seance_length"] = 5400

    assert digest_of([catalog_row()], [longer]) != before


@pytest.mark.parametrize(
    "override",
    [
        {"uuid": OTHER_SERVICE_UUID},
        # Same service under the normalisation, different spelling on screen.
        # It stays a unique-name match, so this isolates the DISPLAYED name.
        {"name": "WIMPERNVERLÄNGERUNG 2D"},
        {"currency": "CHF"},
        {"price": 9500},
        {"duration": {"value": 90, "label": "minutes"}},
    ],
    ids=["target_uuid", "target_name", "target_currency", "target_price", "target_duration"],
)
def test_a_changed_target_attribute_changes_the_digest(override: dict[str, Any]) -> None:
    before = digest_of([catalog_row()])
    row = catalog_row()
    row.update(override)

    assert digest_of([row]) != before


def test_a_changed_availability_verdict_changes_the_digest() -> None:
    """The evidence a verdict rests on is part of what was agreed."""
    permissive = propose([catalog_row(staff=[KA_STAFF_UUID])], staff={KA_SERVICE_UUID: frozenset({KA_STAFF_UUID})})
    silent = propose([catalog_row()], staff={KA_SERVICE_UUID: None})

    assert proposal_digest(permissive[0]) != proposal_digest(silent[0])


def test_reordering_equivalent_inputs_does_not_change_the_digest() -> None:
    """The digest follows the data, not the order the API returned rows in."""
    two = [source_record(900001), source_record(900002)]
    forward = digest_of([catalog_row(), catalog_row(OTHER_SERVICE_UUID, name="Andere")], two)
    reversed_rows = digest_of([catalog_row(OTHER_SERVICE_UUID, name="Andere"), catalog_row()], list(reversed(two)))

    assert forward == reversed_rows


def test_the_digest_carries_no_timestamp_or_sequence_field() -> None:
    [proposal] = propose([catalog_row()])
    blob = json.dumps(proposal.review_payload())

    for unstable in ("timestamp", "fetched_at", "run_id", "page", "received", "_at"):
        assert unstable not in blob, unstable


# ---------------------------------------------------------------------------
# Reporting hygiene and the contracts that must keep holding (findings 9, 10)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_neither_the_report_nor_the_logs_carry_personal_data(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    booking_uuid = "cafe0000-0000-4000-8000-000000000123"
    record = source_record(client={"phone": PHONE, "first_name": "Testkundin", "email": "k@example.invalid", "id": 42})
    record["easyweek_booking_uuid"] = booking_uuid
    stub_source(monkeypatch, [record])

    with caplog.at_level("INFO"):
        result = await run_prepare(make_inputs(state_dir), write_client=FakePrepareClient())

    blobs = [json.dumps(result.machine, ensure_ascii=False), caplog.text]
    for blob in blobs:
        for secret in (PHONE, "Testkundin", "k@example.invalid", booking_uuid, "Wimpernverlängerung"):
            assert secret not in blob, secret


@pytest.mark.asyncio
async def test_the_merge_stays_additive_after_a_confirmation(state_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A previous wave's reviewed mapping is never re-pointed by this one."""
    both = [
        source_record(service_name=BASELINE_NAME),
        source_record(900002, service_id=SECOND_SERVICE_ID, service_name="Zweite Leistung"),
    ]
    stub_source(monkeypatch, both)
    inputs = make_inputs(state_dir)
    catalog = [baseline_catalog_row(), catalog_row(OTHER_SERVICE_UUID, name="Zweite Leistung")]

    review = await run_prepare(inputs, write_client=FakePrepareClient(catalog=catalog))
    shown = {row["altegio_service_id"]: row["review_digest"] for row in review.operator["service_mapping"]}

    apply_confirmations(
        inputs,
        ConfirmRequest(
            confirm_services=(ConfirmTarget(identifier=str(SECOND_SERVICE_ID), review_digest=shown[SECOND_SERVICE_ID]),)
        ),
        snapshot=await snapshot_for(inputs, FakePrepareClient(catalog=catalog)),
    )

    services = json.loads((state_dir / FILE_MANIFEST_PROPOSED).read_text())["branches"][str(KARLSRUHE_COMPANY_ID)][
        "services"
    ]
    assert services[str(KA_SERVICE_ID)]["easyweek_service_uuid"] == KA_SERVICE_UUID, "untouched"
    assert services[str(SECOND_SERVICE_ID)]["easyweek_service_uuid"] == OTHER_SERVICE_UUID


@pytest.mark.asyncio
async def test_a_service_confirmation_needs_the_digest_the_operator_saw(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stub_source(monkeypatch, [source_record()])
    inputs = make_inputs(state_dir, manifest_text=manifest_without_service())
    review = await run_prepare(inputs, write_client=FakePrepareClient())
    [shown] = review.operator["service_mapping"]

    with pytest.raises(PrepareError):
        apply_confirmations(
            inputs,
            ConfirmRequest(confirm_services=(ConfirmTarget(identifier=str(KA_SERVICE_ID), review_digest="wrong"),)),
            snapshot=await snapshot_for(inputs),
        )
    assert (
        json.loads((state_dir / FILE_MANIFEST_PROPOSED).read_text())["branches"][str(KARLSRUHE_COMPANY_ID)]["services"]
        == {}
    )

    apply_confirmations(
        inputs,
        ConfirmRequest(
            confirm_services=(ConfirmTarget(identifier=str(KA_SERVICE_ID), review_digest=shown["review_digest"]),)
        ),
        snapshot=await snapshot_for(inputs),
    )
    assert (
        json.loads((state_dir / FILE_MANIFEST_PROPOSED).read_text())["branches"][str(KARLSRUHE_COMPANY_ID)]["services"][
            str(KA_SERVICE_ID)
        ]["easyweek_service_uuid"]
        == KA_SERVICE_UUID
    )


@pytest.mark.asyncio
async def test_a_service_confirmation_with_a_stale_digest_is_refused(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Reviewed at one price, confirmed after the catalogue moved."""
    stub_source(monkeypatch, [source_record()])
    inputs = make_inputs(state_dir, manifest_text=manifest_without_service())
    review = await run_prepare(inputs, write_client=FakePrepareClient())
    [shown] = review.operator["service_mapping"]

    moved = FakePrepareClient(catalog=[catalog_row(price=12000)])
    with pytest.raises(PrepareError):
        apply_confirmations(
            inputs,
            ConfirmRequest(
                confirm_services=(ConfirmTarget(identifier=str(KA_SERVICE_ID), review_digest=shown["review_digest"]),)
            ),
            snapshot=await snapshot_for(inputs, moved),
        )

    assert (
        json.loads((state_dir / FILE_MANIFEST_PROPOSED).read_text())["branches"][str(KARLSRUHE_COMPANY_ID)]["services"]
        == {}
    )


def test_no_fuzzy_matching_survived_the_rewrite() -> None:
    """Still exact canonical-name equality, and nothing else."""
    assert propose([catalog_row(name="Wimpernverlangerung 2D")])[0].status == PROPOSAL_NO_CANDIDATE
    assert propose([catalog_row(name="Wimpern 2D")])[0].status == PROPOSAL_NO_CANDIDATE
    assert propose([catalog_row(name="WIMPERNVERLÄNGERUNG   2D")])[0].status == PROPOSAL_UNIQUE_NAME


def test_every_frozen_baseline_field_has_a_drift_check() -> None:
    """A new manifest baseline field must not arrive without a drift check.

    ``BASELINE_FIELDS`` is the manifest's frozen identity. The UUID is checked by
    presence in the catalogue rather than by comparison, so it is the one
    exception; every other field has to appear in the drift parametrisation
    above, or a service could move under that field unnoticed.
    """
    compared = set(BASELINE_FIELDS) - {"easyweek_service_uuid"}

    assert compared == set(DRIFTS), "add the new baseline field to DRIFTS"


# ---------------------------------------------------------------------------
# A manual correction survives the next rebuild (plan §30.9)
# ---------------------------------------------------------------------------


FULL_NAME_ONLY = {"phone": PHONE, "name": "Anna Maria Schmidt", "id": 42}


def unsplit_record(record_id: int = 900001) -> dict[str, Any]:
    """A booking whose customer has a full name and no given name."""
    return source_record(record_id, client=dict(FULL_NAME_ONLY))


async def correct(inputs: PrepareInputs, **fields: str) -> dict[str, Any]:
    return apply_confirmations(
        inputs,
        ConfirmRequest(correct_phone=PHONE, **fields),
        snapshot=await snapshot_for(inputs),
    )


@pytest.mark.asyncio
async def test_a_correction_survives_a_process_exit_and_a_fresh_rebuild(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The defect, end to end.

    prepare blocks on source_name_not_split -> operator corrects -> the process
    exits -> a fresh confirm rebuilds the live snapshot -> the corrected proposal
    is still there, reviewable, and its digest is accepted.
    """
    stub_source(monkeypatch, [unsplit_record()])
    inputs = make_inputs(state_dir)

    blocked = await run_prepare(inputs, write_client=FakePrepareClient())
    [shown] = blocked.operator["customers"]
    assert shown["blocked_reason"] == BLOCK_NAME_NOT_SPLIT
    assert shown["manually_corrected"] is False

    await correct(inputs, correct_first_name="Anna Maria", correct_last_name="Schmidt")

    # A completely fresh rebuild — the process could have exited in between.
    rebuilt = await run_prepare(inputs, write_client=FakePrepareClient())
    [review] = rebuilt.operator["customers"]

    assert review["first_name"] == "Anna Maria"
    assert review["last_name"] == "Schmidt"
    assert review["state"] == STATE_PENDING
    assert review["blocked_reason"] is None
    assert review["manually_corrected"] is True
    assert rebuilt.machine["customers"]["manual_corrections_applied"] == 1

    outcome = apply_confirmations(
        inputs,
        ConfirmRequest(confirm_customers=(ConfirmTarget(identifier=PHONE, review_digest=review["review_digest"]),)),
        snapshot=await snapshot_for(inputs),
    )

    assert outcome["customer_states"] == {STATE_CONFIRMED: 1}


@pytest.mark.asyncio
async def test_a_second_prepare_does_not_discard_the_override(state_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    stub_source(monkeypatch, [unsplit_record()])
    inputs = make_inputs(state_dir)
    await run_prepare(inputs, write_client=FakePrepareClient())
    await correct(inputs, correct_first_name="Anna Maria")

    for _ in range(3):
        result = await run_prepare(inputs, write_client=FakePrepareClient())

    [review] = result.operator["customers"]
    assert review["first_name"] == "Anna Maria"
    assert review["manually_corrected"] is True


@pytest.mark.asyncio
async def test_the_corrected_digest_differs_from_the_one_before_the_correction(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A digest taken before the correction must not confirm the corrected row."""
    stub_source(monkeypatch, [unsplit_record()])
    inputs = make_inputs(state_dir)
    before = await run_prepare(inputs, write_client=FakePrepareClient())
    stale_digest = before.operator["customers"][0]["review_digest"]

    await correct(inputs, correct_first_name="Anna Maria")
    after = await run_prepare(inputs, write_client=FakePrepareClient())

    assert after.operator["customers"][0]["review_digest"] != stale_digest
    with pytest.raises(PrepareError):
        apply_confirmations(
            inputs,
            ConfirmRequest(confirm_customers=(ConfirmTarget(identifier=PHONE, review_digest=stale_digest),)),
            snapshot=await snapshot_for(inputs),
        )
    assert load_decisions(state_dir).get(PHONE).state == STATE_PENDING


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutate",
    [
        pytest.param(lambda rec: rec["client"].update({"id": 99}), id="source_customer_changed"),
        pytest.param(lambda rec: rec["client"].update({"phone": "+4915199999999"}), id="phone_identity_changed"),
        pytest.param(lambda rec: rec.update({"id": 900777}), id="linked_bookings_changed"),
    ],
)
async def test_a_moved_source_makes_the_correction_stale_rather_than_applied(
    mutate: Any, state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The correction was evidence about a person the source no longer describes."""
    record = unsplit_record()
    stub_source(monkeypatch, [record])
    inputs = make_inputs(state_dir)
    await run_prepare(inputs, write_client=FakePrepareClient())
    await correct(inputs, correct_first_name="Anna Maria")

    moved = unsplit_record()
    mutate(moved)
    stub_source(monkeypatch, [moved])
    result = await run_prepare(inputs, write_client=FakePrepareClient())

    reviews = {row["phone"]: row for row in result.operator["customers"]}
    target = reviews.get(moved["client"]["phone"], reviews.get(PHONE))
    assert target["state"] == STATE_BLOCKED
    assert target["correction_stale"] is True or target["first_name"] is None
    assert result.machine["ready"]["all_clear"] is False


@pytest.mark.asyncio
async def test_a_stale_correction_is_never_applied_to_another_customer(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A number that changed hands must not inherit somebody else's correction."""
    stub_source(monkeypatch, [unsplit_record()])
    inputs = make_inputs(state_dir)
    await run_prepare(inputs, write_client=FakePrepareClient())
    await correct(inputs, correct_first_name="Anna Maria")

    somebody_else = source_record(900002, client={"phone": PHONE, "name": "Bea Weber", "id": 77})
    stub_source(monkeypatch, [somebody_else])
    result = await run_prepare(inputs, write_client=FakePrepareClient())

    [review] = result.operator["customers"]
    assert review["first_name"] != "Anna Maria", "a name match is not identity"
    assert review["correction_stale"] is True
    assert review["state"] == STATE_BLOCKED
    assert result.machine["customers"]["manual_corrections_stale"] == 1


@pytest.mark.asyncio
async def test_a_correction_never_leaks_into_the_machine_report(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    stub_source(monkeypatch, [unsplit_record()])
    inputs = make_inputs(state_dir)
    await run_prepare(inputs, write_client=FakePrepareClient())

    with caplog.at_level("INFO"):
        await correct(inputs, correct_first_name="Anna Maria", correct_email="anna@example.invalid")
        result = await run_prepare(inputs, write_client=FakePrepareClient())

    for blob in (json.dumps(result.machine, ensure_ascii=False), caplog.text):
        for secret in (PHONE, "Anna Maria", "Schmidt", "anna@example.invalid"):
            assert secret not in blob, secret


@pytest.mark.asyncio
async def test_a_correction_that_sets_nothing_is_refused(state_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    stub_source(monkeypatch, [unsplit_record()])
    inputs = make_inputs(state_dir)
    await run_prepare(inputs, write_client=FakePrepareClient())

    with pytest.raises(PrepareError):
        await correct(inputs)


@pytest.mark.asyncio
async def test_the_override_store_is_not_world_readable(state_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    stub_source(monkeypatch, [unsplit_record()])
    inputs = make_inputs(state_dir)
    await run_prepare(inputs, write_client=FakePrepareClient())
    await correct(inputs, correct_first_name="Anna Maria")

    assert (os.stat(state_dir / "customer_overrides.json").st_mode & 0o077) == 0


@pytest.mark.asyncio
async def test_a_corrupt_override_store_is_refused_not_ignored(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Ignoring it would silently discard a correction and re-block the customer."""
    stub_source(monkeypatch, [unsplit_record()])
    inputs = make_inputs(state_dir)
    await run_prepare(inputs, write_client=FakePrepareClient())
    await correct(inputs, correct_first_name="Anna Maria")
    (state_dir / "customer_overrides.json").write_text("{ not json")

    with pytest.raises(CustomerOverrideError):
        await run_prepare(inputs, write_client=FakePrepareClient())


@pytest.mark.asyncio
async def test_an_override_store_from_the_future_is_refused(state_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    stub_source(monkeypatch, [unsplit_record()])
    inputs = make_inputs(state_dir)
    await run_prepare(inputs, write_client=FakePrepareClient())
    (state_dir / "customer_overrides.json").write_text(json.dumps({"version": 99, "overrides": []}))

    with pytest.raises(CustomerOverrideError):
        await run_prepare(inputs, write_client=FakePrepareClient())


@pytest.mark.asyncio
async def test_the_decision_store_version_is_unchanged_by_this_feature(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Corrections live in their own file: no terminal or in-flight state moves."""
    from altegio_bot.easyweek_migration.customer_decisions import STORE_VERSION

    stub_source(monkeypatch, [unsplit_record()])
    inputs = make_inputs(state_dir)
    await run_prepare(inputs, write_client=FakePrepareClient())
    await correct(inputs, correct_first_name="Anna Maria")

    stored = json.loads((state_dir / "customer_decisions.json").read_text())
    assert stored["version"] == STORE_VERSION == 1


# ---------------------------------------------------------------------------
# A mapping is inherited across waves; permission is not (plan §30.9)
# ---------------------------------------------------------------------------


def wave_b_branch() -> Any:
    """Wave B: a different master, on the manifest wave A already filled in."""
    payload = json.loads(manifest_json())
    branch = payload["branches"][str(KARLSRUHE_COMPANY_ID)]
    branch["selected_altegio_staff_ids"] = [KA_SECOND_STAFF_ID]
    branch["deferred_altegio_staff_ids"] = [KA_STAFF_ID]
    branch["staff"][str(KA_SECOND_STAFF_ID)] = KA_SECOND_STAFF_UUID
    return inventory_manifest(json.dumps(payload)).branch(KARLSRUHE_COMPANY_ID)


def wave_b_record() -> dict[str, Any]:
    record = source_record(service_name=BASELINE_NAME)
    record["staff_id"] = KA_SECOND_STAFF_ID
    return record


def test_an_inherited_mapping_the_new_master_may_not_use_is_not_settled() -> None:
    """Wave A mapped S for master A; wave B's master B may not perform it."""
    [proposal] = propose(
        [baseline_catalog_row(staff=[KA_STAFF_UUID])],
        records=[wave_b_record()],
        branch=wave_b_branch(),
        staff={KA_SERVICE_UUID: frozenset({KA_STAFF_UUID})},
        staff_ids={KA_SECOND_STAFF_ID},
    )

    assert proposal.status == PROPOSAL_STAFF_UNAVAILABLE
    assert proposal.settled is False
    assert proposal.actionable is False, "no automatic confirmation"
    assert proposal.drift_fields == (), "the baseline itself is intact"
    assert proposal.chosen is None


def test_an_inherited_mapping_the_new_master_may_use_stays_settled() -> None:
    [proposal] = propose(
        [baseline_catalog_row(staff=[KA_STAFF_UUID, KA_SECOND_STAFF_UUID])],
        records=[wave_b_record()],
        branch=wave_b_branch(),
        staff={KA_SERVICE_UUID: frozenset({KA_STAFF_UUID, KA_SECOND_STAFF_UUID})},
        staff_ids={KA_SECOND_STAFF_ID},
    )

    assert proposal.status == PROPOSAL_ALREADY_MAPPED
    assert proposal.settled is True


def test_an_inherited_mapping_with_an_unstated_catalogue_keeps_current_semantics() -> None:
    """UNSTATED is not turned into invented evidence, and not into a refusal."""
    [proposal] = propose(
        [baseline_catalog_row()],
        records=[wave_b_record()],
        branch=wave_b_branch(),
        staff={KA_SERVICE_UUID: None},
        staff_ids={KA_SECOND_STAFF_ID},
    )

    assert proposal.status == PROPOSAL_ALREADY_MAPPED
    assert proposal.candidates[0].staff_availability == STAFF_AVAILABILITY_UNSTATED


def test_the_check_uses_only_masters_who_actually_perform_the_service() -> None:
    """Not the union of the wave: a bystander's access is still not evidence."""
    both = json.loads(manifest_json())
    branch_payload = both["branches"][str(KARLSRUHE_COMPANY_ID)]
    branch_payload["selected_altegio_staff_ids"] = [KA_STAFF_ID, KA_SECOND_STAFF_ID]
    branch_payload["deferred_altegio_staff_ids"] = []
    branch_payload["staff"][str(KA_SECOND_STAFF_ID)] = KA_SECOND_STAFF_UUID
    branch = inventory_manifest(json.dumps(both)).branch(KARLSRUHE_COMPANY_ID)

    [proposal] = propose(
        [baseline_catalog_row(staff=[KA_STAFF_UUID])],
        records=[wave_b_record()],
        branch=branch,
        staff={KA_SERVICE_UUID: frozenset({KA_STAFF_UUID})},
        staff_ids={KA_STAFF_ID, KA_SECOND_STAFF_ID},
    )

    assert proposal.chosen is None
    assert proposal.status == PROPOSAL_STAFF_UNAVAILABLE
    assert proposal.candidates[0].required_staff_uuids == (KA_SECOND_STAFF_UUID,)


@pytest.mark.asyncio
async def test_an_unavailable_inherited_mapping_blocks_the_wave(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    payload = json.loads(manifest_json())
    branch = payload["branches"][str(KARLSRUHE_COMPANY_ID)]
    branch["selected_altegio_staff_ids"] = [KA_SECOND_STAFF_ID]
    branch["deferred_altegio_staff_ids"] = [KA_STAFF_ID]
    branch["staff"][str(KA_SECOND_STAFF_ID)] = KA_SECOND_STAFF_UUID

    stub_source(monkeypatch, [wave_b_record()])
    inputs = make_inputs(state_dir, manifest_text=json.dumps(payload))
    client = FakePrepareClient(catalog=[baseline_catalog_row(staff=[KA_STAFF_UUID])])

    result = await run_prepare(inputs, write_client=client)

    assert result.machine["ready"]["mapping_ready"] is False
    assert result.machine["ready"]["all_clear"] is False
    [shown] = result.machine["mapping"]["proposals"]
    assert shown["status"] == PROPOSAL_STAFF_UNAVAILABLE
    assert shown["settled"] is False
    assert shown["actionable"] is False


@pytest.mark.asyncio
async def test_an_unavailable_inherited_mapping_cannot_be_confirmed(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    payload = json.loads(manifest_json())
    branch = payload["branches"][str(KARLSRUHE_COMPANY_ID)]
    branch["selected_altegio_staff_ids"] = [KA_SECOND_STAFF_ID]
    branch["deferred_altegio_staff_ids"] = [KA_STAFF_ID]
    branch["staff"][str(KA_SECOND_STAFF_ID)] = KA_SECOND_STAFF_UUID

    stub_source(monkeypatch, [wave_b_record()])
    inputs = make_inputs(state_dir, manifest_text=json.dumps(payload))
    catalog = [baseline_catalog_row(staff=[KA_STAFF_UUID])]
    review = await run_prepare(inputs, write_client=FakePrepareClient(catalog=catalog))
    [shown] = review.operator["service_mapping"]

    with pytest.raises(PrepareError):
        apply_confirmations(
            inputs,
            ConfirmRequest(
                confirm_services=(ConfirmTarget(identifier=str(KA_SERVICE_ID), review_digest=shown["review_digest"]),)
            ),
            snapshot=await snapshot_for(inputs, FakePrepareClient(catalog=catalog)),
        )


# ---------------------------------------------------------------------------
# The booking's actual duration (plan §30.9)
# ---------------------------------------------------------------------------


def stretched(minutes: int) -> dict[str, Any]:
    """A booking hand-stretched past its service's catalogue length."""
    record = source_record()
    record["seance_length"] = minutes * 60
    return record


def test_the_actual_booking_duration_comes_from_the_top_level_field() -> None:
    """The service line says 60; the appointment is 90. The appointment wins."""
    record = stretched(90)
    record["services"][0]["seance_length"] = 3600

    row = prepare_module.operator_record_row(record, block_reason=None)

    assert row["duration_minutes"] == 90
    assert row["service_line_duration_minutes"] == 60, "kept separately, not replaced"


def test_a_changed_booking_duration_changes_the_digest() -> None:
    """A stretched slot must not inherit an agreement made about a standard one."""
    before = digest_of([catalog_row()], [stretched(60)])

    assert digest_of([catalog_row()], [stretched(90)]) != before


def test_the_booking_duration_and_the_service_line_duration_are_both_digested() -> None:
    record = stretched(90)
    record["services"][0]["seance_length"] = 3600
    [proposal] = propose([catalog_row()], records=[record])
    payload = proposal.review_payload()

    assert payload["observed_booking_durations_minutes"] == [90]
    assert payload["observed_source_durations_minutes"] == [60]


def test_a_fractional_duration_is_reported_as_unknown_not_rounded() -> None:
    record = source_record()
    record["seance_length"] = 3630  # 60.5 minutes

    assert prepare_module.operator_record_row(record, block_reason=None)["duration_minutes"] is None


@pytest.mark.parametrize(
    ("reason", "expected"),
    [
        (None, prepare_module.CLASS_AUTOMATIC),
        ("multi_service_unsupported", prepare_module.CLASS_CART_CANDIDATE),
        ("custom_duration_unsupported", prepare_module.CLASS_MANUAL_ADJUSTMENT),
        ("custom_price_unsupported", prepare_module.CLASS_MANUAL_ADJUSTMENT),
        ("duration_unknown", prepare_module.CLASS_BLOCKED_UNPROVEN),
        ("price_malformed", prepare_module.CLASS_BLOCKED_UNPROVEN),
        ("source_status_unrecognised", prepare_module.CLASS_BLOCKED_UNPROVEN),
        ("staff_not_in_wave_scope", prepare_module.CLASS_FULLY_MANUAL),
        ("service_mapping_missing", prepare_module.CLASS_AUTOMATIC),
    ],
)
def test_api_limits_and_unprovable_data_are_classified_apart(reason: str | None, expected: str) -> None:
    """ "The API cannot express this" is not "the data cannot be proven"."""
    assert prepare_module.handling_class(reason) == expected


@pytest.mark.asyncio
async def test_a_stretched_booking_is_manual_adjustment_not_an_automatic_path(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Nothing claims EasyWeek can take a custom duration; it needs a person."""
    stub_source(monkeypatch, [stretched(90)])

    result = await run_prepare(make_inputs(state_dir), write_client=FakePrepareClient())

    assert result.machine["source"]["records_needing_manual_work"] == {"custom_duration_unsupported": 1}
    [row] = result.operator["records"]
    assert row["handling"] == prepare_module.CLASS_MANUAL_ADJUSTMENT
    assert row["duration_minutes"] == 90


def test_no_code_path_claims_easyweek_supports_a_custom_duration() -> None:
    """The vocabulary names operator work, never a capability we have not proven."""
    source = (REPO_ROOT / "src" / "altegio_bot" / "easyweek_migration" / "prepare.py").read_text()

    assert "cart_candidate" in source
    assert "no canary has proven any of those" in source
    for claim in ("supports custom duration", "custom duration is supported", "cart API is available"):
        assert claim not in source
