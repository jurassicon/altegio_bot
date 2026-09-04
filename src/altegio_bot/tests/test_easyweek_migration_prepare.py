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
from altegio_bot.easyweek_migration.cutover import parse_cutover
from altegio_bot.easyweek_migration.manifest import KARLSRUHE_COMPANY_ID, inventory_manifest, parse_manifest
from altegio_bot.easyweek_migration.mapping_proposal import (
    PROPOSAL_ALREADY_MAPPED,
    PROPOSAL_AMBIGUOUS,
    PROPOSAL_NO_CANDIDATE,
    PROPOSAL_UNIQUE_NAME,
    STAFF_AVAILABILITY_ABSENT,
    STAFF_AVAILABILITY_PROVEN,
    STAFF_AVAILABILITY_UNSTATED,
    MappingAgreement,
    collect_source_services,
    manifest_service_patch,
    merge_manifest_services,
    propose_service_mapping,
    read_service_staff_uuids,
)
from altegio_bot.easyweek_migration.prepare import (
    FILE_CUSTOMER_DIRECTORY,
    FILE_MANIFEST_PROPOSED,
    FILE_OPERATOR_REVIEW,
    MODE_PREPARE,
    ConfirmRequest,
    PrepareError,
    PrepareInputs,
    apply_confirmations,
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

CUTOVER = "2026-09-01T00:00:00Z"
OTHER_SERVICE_UUID = "99999999-9999-4999-8999-999999999999"
SECOND_SERVICE_ID = 6009


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
    branch = manifest.branch(KARLSRUHE_COMPANY_ID)
    return PrepareInputs(
        mode=mode,
        run_id="run-prepare",
        state_dir=state_dir,
        manifest=manifest,
        manifest_json=json.loads(text),
        altegio_company_id=KARLSRUHE_COMPANY_ID,
        cutover=parse_cutover(CUTOVER),
        horizon_days=30,
        selected_staff_uuids=frozenset(
            uuid for staff_id, uuid in branch.staff.items() if staff_id in branch.selected_staff_ids
        ),
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
    selected: set[str] | None = None,
):
    snapshot = build_catalog_snapshot(KA_LOCATION_UUID, catalog_rows)
    services = collect_source_services(records or [source_record()], staff_ids={KA_STAFF_ID})
    return propose_service_mapping(
        altegio_company_id=KARLSRUHE_COMPANY_ID,
        source_services=services,
        catalog=snapshot,
        catalog_staff=staff or {},
        selected_staff_uuids=selected or set(),
        branch=branch,
    )


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
        selected={KA_STAFF_UUID},
    )

    assert proposals[0].chosen.staff_availability == STAFF_AVAILABILITY_ABSENT
    assert proposals[0].actionable is False


def test_availability_is_proven_when_the_catalogue_names_the_master() -> None:
    proposals = propose(
        [catalog_row(staff=[KA_STAFF_UUID])],
        staff={KA_SERVICE_UUID: frozenset({KA_STAFF_UUID})},
        selected={KA_STAFF_UUID},
    )

    assert proposals[0].chosen.staff_availability == STAFF_AVAILABILITY_PROVEN


def test_a_catalogue_that_states_no_staff_says_so_rather_than_passing() -> None:
    """Silence is not a claim, and it is not invented into one."""
    proposals = propose([catalog_row()], staff={KA_SERVICE_UUID: None}, selected={KA_STAFF_UUID})

    assert proposals[0].chosen.staff_availability == STAFF_AVAILABILITY_UNSTATED
    assert proposals[0].actionable is True, "unprovable is not refused, it is flagged"


def test_the_staff_reader_distinguishes_absent_from_empty() -> None:
    assert read_service_staff_uuids({"uuid": "x"}) is None
    assert read_service_staff_uuids({"uuid": "x", "employees": []}) == frozenset()
    assert read_service_staff_uuids({"uuid": "x", "employees": [{"uuid": "a"}]}) == frozenset({"a"})
    assert read_service_staff_uuids({"uuid": "x", "employees": [17]}) is None


def test_an_already_mapped_service_is_not_re_proposed() -> None:
    manifest = parse_manifest(manifest_json())
    [proposal] = propose([catalog_row()], branch=manifest.branch(KARLSRUHE_COMPANY_ID))

    assert proposal.status == PROPOSAL_ALREADY_MAPPED
    assert proposal.existing_uuid == KA_SERVICE_UUID


def test_a_mapped_uuid_missing_from_the_catalogue_is_flagged() -> None:
    manifest = parse_manifest(manifest_json())
    [proposal] = propose([catalog_row(OTHER_SERVICE_UUID)], branch=manifest.branch(KARLSRUHE_COMPANY_ID))

    assert proposal.status == "conflicts_with_manifest"
    assert proposal.actionable is False


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

    await run_prepare(inputs, write_client=FakePrepareClient())
    apply_confirmations(inputs, ConfirmRequest(confirm_customers=(PHONE,)))
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
        proposals=propose([catalog_row()]),
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
            proposals=propose([catalog_row()]),
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
