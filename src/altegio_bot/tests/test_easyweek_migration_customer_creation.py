"""Confirming and creating customers: the only write this stage can perform.

A customer card is cheap to create and expensive to create twice. A duplicate
sits on top of somebody's visit history, and the next booking lands on whichever
of the two a lookup happens to return. So the rules under test here are all
variations on one theme — a ``POST`` needs a proven absence, a specific
confirmation, and a durable marker written before the request goes out.

The other half is consent. These commands run in Docker without a terminal, and
an EOF must never look like a yes. Nothing in this module reads stdin, and one of
the tests below proves that by taking stdin away entirely.
"""

from __future__ import annotations

import json
import os
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from altegio_bot.easyweek_client import EasyWeekAuthError, EasyWeekPermanentError, EasyWeekRetryableError
from altegio_bot.easyweek_migration.customer_decisions import (
    LEGACY_EVIDENCE_MISSING,
    STATE_BLOCKED,
    STATE_CONFIRMED,
    STATE_CREATED,
    STATE_IN_FLIGHT,
    STATE_PENDING,
    STATE_SKIPPED,
    CustomerDecision,
    CustomerDecisionStore,
    DecisionSet,
    DecisionStoreError,
    DecisionStoreLocked,
)
from altegio_bot.easyweek_migration.cutover import parse_cutover
from altegio_bot.easyweek_migration.manifest import KARLSRUHE_COMPANY_ID, parse_manifest
from altegio_bot.easyweek_migration.prepare import (
    BLOCK_ALREADY_EXISTS,
    BLOCK_CREATE_REJECTED,
    BLOCK_CREATE_UNCERTAIN,
    BLOCK_CREATE_UNVERIFIED,
    BLOCK_LOOKUP_UNDETERMINED,
    BLOCK_NAME_NOT_SPLIT,
    BLOCK_SHARED_PHONE,
    FILE_CUSTOMER_DIRECTORY,
    MODE_CREATE_CUSTOMERS,
    ConfirmRequest,
    ConfirmTarget,
    PreparationSnapshot,
    PrepareError,
    PrepareInputs,
    SourceCustomer,
    apply_confirmations,
    build_customer_directory_payload,
    build_customer_request,
    collect_source_customers,
    pending_digest,
    run_create_customers,
)
from altegio_bot.easyweek_migration.service_catalog import CatalogSnapshot
from altegio_bot.easyweek_migration.write_client import EasyWeekUncertainMutation
from altegio_bot.tests.test_easyweek_migration_customer_lookup import (
    PHONE,
    UUID_A,
    UUID_B,
    FakeCustomerReads,
    card,
    page,
)
from altegio_bot.tests.test_easyweek_migration_planning import KA_LOCATION_UUID, manifest_text

OTHER_PHONE = "+4915199999999"


def decision(phone: str = PHONE, **overrides: Any) -> CustomerDecision:
    base = {
        "phone": phone,
        "first_name": "Testkundin",
        "last_name": None,
        "email": None,
        "linked_record_count": 2,
        "source_label": "",
        "source_identity": "a" * 64,
        "source_record_ids": (900001, 900002),
        "lookup_outcome": "absent",
    }
    base.update(overrides)
    state = base.get("state", STATE_PENDING)
    base.setdefault("review_state", STATE_BLOCKED if state == STATE_BLOCKED else STATE_PENDING)
    base.setdefault("intended_action", "none" if state == STATE_BLOCKED else "create_customer")
    return CustomerDecision(**base).with_digest()  # type: ignore[arg-type]


@pytest.fixture
def state_dir(tmp_path: Path) -> Path:
    return tmp_path / "prepare-state"


@pytest.fixture
def inputs(state_dir: Path) -> PrepareInputs:
    manifest = parse_manifest(manifest_text())
    assert manifest.valid
    return PrepareInputs(
        mode=MODE_CREATE_CUSTOMERS,
        run_id="run-test",
        state_dir=state_dir,
        manifest=manifest,
        manifest_json=json.loads(manifest_text()),
        altegio_company_id=KARLSRUHE_COMPANY_ID,
        cutover=parse_cutover("2026-09-01T00:00:00Z"),
        horizon_days=30,
        create_allowed=True,
    )


def seed(state_dir: Path, *records: CustomerDecision) -> None:
    store = CustomerDecisionStore(state_dir)
    with store:
        decisions = DecisionSet()
        for record in records:
            decisions.records[record.phone] = record
        store.save(decisions)


def load(state_dir: Path) -> DecisionSet:
    store = CustomerDecisionStore(state_dir)
    with store:
        return store.load()


def source_for(record: CustomerDecision, *, client_id: object = 42) -> SourceCustomer:
    """The Altegio-side evidence behind one decision, as the live read saw it.

    A correction binds to this, not to the decision — so a test that corrects a
    customer has to say what the source proves about them.
    """
    return SourceCustomer(
        phone=record.phone,
        first_name=record.first_name,
        last_name=record.last_name,
        full_name=record.source_label or None,
        email=record.email,
        record_ids=list(range(900001, 900001 + record.linked_record_count)),
        source_client_ids={client_id},
    )


def live_snapshot(
    *records: CustomerDecision,
    proposals: tuple[Any, ...] = (),
    sources: dict[str, SourceCustomer] | None = None,
) -> PreparationSnapshot:
    """A snapshot whose live data says exactly what these records say.

    The confirm path re-derives every customer from a fresh read, so a test that
    seeds the decision store has to state what that read would return. Passing
    the same records is "nothing changed"; passing different ones is "the source
    moved under the operator", which is its own test below.
    """
    manifest = parse_manifest(manifest_text())
    return PreparationSnapshot(
        branch=manifest.branch(KARLSRUHE_COMPANY_ID),
        records=(),
        in_scope=(),
        operator_records=(),
        manual={},
        ready_now=0,
        catalog=CatalogSnapshot(location_uuid=KA_LOCATION_UUID, services=()),
        catalog_staff={},
        proposals=proposals,
        customer_sources=sources if sources is not None else {r.phone: source_for(r) for r in records},
        customer_lookups={},
        customer_proposals={record.phone: record for record in records},
    )


class FakeCreateClient(FakeCustomerReads):
    """Lookups plus a scripted `create_customer`, counting every POST."""

    def __init__(self, pages: list[Any] | None = None, cards: dict[str, Any] | None = None, create: Any = None) -> None:
        super().__init__(pages=pages, cards=cards)
        self.create_answer = create
        self.posts: list[dict[str, Any]] = []

    async def create_customer(self, body: dict[str, Any]) -> dict[str, Any]:
        self.posts.append(dict(body))
        answer = self.create_answer
        if isinstance(answer, Exception):
            raise answer
        if callable(answer):
            return answer(body)
        return answer or {}


# ---------------------------------------------------------------------------
# The decision store
# ---------------------------------------------------------------------------


def test_a_decision_survives_a_restart(state_dir: Path) -> None:
    seed(state_dir, decision())
    assert load(state_dir).get(PHONE).state == STATE_PENDING


def test_a_truncated_store_is_refused_not_treated_as_empty(state_dir: Path) -> None:
    """An empty store would forget which customers already exist."""
    seed(state_dir, decision())
    (state_dir / "customer_decisions.json").write_text("{ this is not json")

    with pytest.raises(DecisionStoreError):
        load(state_dir)


def test_two_runs_cannot_hold_the_state_directory(state_dir: Path) -> None:
    first = CustomerDecisionStore(state_dir)
    first.acquire()
    try:
        with pytest.raises(DecisionStoreLocked):
            CustomerDecisionStore(state_dir).acquire()
    finally:
        first.release()

    CustomerDecisionStore(state_dir).acquire()  # released cleanly


def test_the_store_and_its_directory_are_not_world_readable(state_dir: Path) -> None:
    seed(state_dir, decision())
    assert (os.stat(state_dir / "customer_decisions.json").st_mode & 0o077) == 0
    assert (os.stat(state_dir).st_mode & 0o077) == 0


def test_unchanged_data_does_not_re_ask_for_a_confirmation() -> None:
    decisions = DecisionSet()
    decisions.upsert_proposal(decision())
    decisions.set_state(PHONE, STATE_CONFIRMED)

    decisions.upsert_proposal(decision())

    assert decisions.get(PHONE).state == STATE_CONFIRMED


def test_changed_data_takes_the_confirmation_away() -> None:
    decisions = DecisionSet()
    decisions.upsert_proposal(decision())
    decisions.set_state(PHONE, STATE_CONFIRMED)

    decisions.upsert_proposal(decision(first_name="Andere"))

    record = decisions.get(PHONE)
    assert record.state == STATE_PENDING
    assert record.first_name == "Andere"


def test_a_created_customer_is_never_re_proposed() -> None:
    decisions = DecisionSet()
    decisions.upsert_proposal(decision())
    decisions.set_state(PHONE, STATE_CREATED, customer_uuid=UUID_A)

    decisions.upsert_proposal(decision(first_name="Andere"))

    assert decisions.get(PHONE).state == STATE_CREATED


def test_a_deliberate_skip_is_not_re_opened() -> None:
    decisions = DecisionSet()
    decisions.upsert_proposal(decision())
    decisions.set_state(PHONE, STATE_SKIPPED)

    decisions.upsert_proposal(decision())

    assert decisions.get(PHONE).state == STATE_SKIPPED


# ---------------------------------------------------------------------------
# Confirming
# ---------------------------------------------------------------------------


def confirm_target(record: CustomerDecision) -> ConfirmTarget:
    """What an operator copies out of the review: the phone and its digest."""
    return ConfirmTarget(identifier=record.phone, review_digest=record.shown_digest)


def test_a_confirmation_names_one_customer(inputs: PrepareInputs, state_dir: Path) -> None:
    mine, other = decision(), decision(OTHER_PHONE)
    seed(state_dir, mine, other)

    apply_confirmations(
        inputs,
        ConfirmRequest(confirm_customers=(confirm_target(mine),)),
        snapshot=live_snapshot(mine, other),
    )

    decisions = load(state_dir)
    assert decisions.get(PHONE).state == STATE_CONFIRMED
    assert decisions.get(OTHER_PHONE).state == STATE_PENDING


def test_a_single_confirmation_without_a_digest_is_impossible_to_express() -> None:
    """The type itself carries the digest, so there is no unbound form to send."""
    with pytest.raises(TypeError):
        ConfirmTarget(identifier=PHONE)  # type: ignore[call-arg]


def test_a_single_confirmation_with_a_wrong_digest_is_refused(inputs: PrepareInputs, state_dir: Path) -> None:
    record = decision()
    seed(state_dir, record)

    with pytest.raises(PrepareError):
        apply_confirmations(
            inputs,
            ConfirmRequest(confirm_customers=(ConfirmTarget(identifier=PHONE, review_digest="wrong"),)),
            snapshot=live_snapshot(record),
        )

    assert load(state_dir).get(PHONE).state == STATE_PENDING


def test_a_single_confirmation_with_a_stale_digest_is_refused(inputs: PrepareInputs, state_dir: Path) -> None:
    """The operator read one proposal; the live source now says another."""
    reviewed = decision(first_name="Testkundin")
    seed(state_dir, reviewed)
    moved = decision(first_name="Andere")

    with pytest.raises(PrepareError):
        apply_confirmations(
            inputs,
            ConfirmRequest(confirm_customers=(confirm_target(reviewed),)),
            snapshot=live_snapshot(moved),
        )

    record = load(state_dir).get(PHONE)
    assert record.state == STATE_PENDING
    assert record.first_name == "Testkundin", "a refusal changes nothing at all"


def test_a_refused_confirmation_leaves_no_partial_change(inputs: PrepareInputs, state_dir: Path) -> None:
    """One bad digest in a command stops the whole command, skips included."""
    good, bad = decision(), decision(OTHER_PHONE)
    seed(state_dir, good, bad)

    with pytest.raises(PrepareError):
        apply_confirmations(
            inputs,
            ConfirmRequest(
                confirm_customers=(
                    confirm_target(good),
                    ConfirmTarget(identifier=OTHER_PHONE, review_digest="wrong"),
                ),
                skip_customers=(),
            ),
            snapshot=live_snapshot(good, bad),
        )

    decisions = load(state_dir)
    assert decisions.get(PHONE).state == STATE_PENDING
    assert decisions.get(OTHER_PHONE).state == STATE_PENDING


def test_a_confirmation_for_somebody_no_longer_in_the_wave_is_refused(inputs: PrepareInputs, state_dir: Path) -> None:
    record = decision()
    seed(state_dir, record)

    with pytest.raises(PrepareError):
        apply_confirmations(
            inputs,
            ConfirmRequest(confirm_customers=(confirm_target(record),)),
            snapshot=live_snapshot(),
        )

    assert load(state_dir).get(PHONE).state == STATE_PENDING


def test_a_batch_confirmation_is_bound_to_the_printed_list(inputs: PrepareInputs, state_dir: Path) -> None:
    mine, other = decision(), decision(OTHER_PHONE)
    seed(state_dir, mine, other)
    printed = pending_digest(load(state_dir))

    apply_confirmations(
        inputs,
        ConfirmRequest(confirm_all_pending=True, expected_pending_digest=printed),
        snapshot=live_snapshot(mine, other),
    )

    assert all(record.state == STATE_CONFIRMED for record in load(state_dir).records.values())


def test_a_batch_confirmation_refuses_a_list_that_moved(inputs: PrepareInputs, state_dir: Path) -> None:
    """A yes to one list is not a yes to a longer one."""
    mine = decision()
    seed(state_dir, mine)
    printed = pending_digest(load(state_dir))
    other = decision(OTHER_PHONE)
    seed(state_dir, mine, other)

    with pytest.raises(PrepareError):
        apply_confirmations(
            inputs,
            ConfirmRequest(confirm_all_pending=True, expected_pending_digest=printed),
            snapshot=live_snapshot(mine, other),
        )

    assert all(record.state == STATE_PENDING for record in load(state_dir).records.values())


def test_a_batch_confirmation_without_a_digest_is_refused(inputs: PrepareInputs, state_dir: Path) -> None:
    record = decision()
    seed(state_dir, record)

    with pytest.raises(PrepareError):
        apply_confirmations(inputs, ConfirmRequest(confirm_all_pending=True), snapshot=live_snapshot(record))


def test_a_confirmation_against_stale_stored_data_is_refused(inputs: PrepareInputs, state_dir: Path) -> None:
    """A stored record inconsistent with its own digest is never acted on."""
    stale = replace(decision(), first_name="Andere")  # digest still describes the old name
    seed(state_dir, stale)

    with pytest.raises(PrepareError):
        apply_confirmations(
            inputs,
            ConfirmRequest(confirm_customers=(confirm_target(stale),)),
            snapshot=live_snapshot(stale),
        )

    assert load(state_dir).get(PHONE).state == STATE_PENDING


def test_a_correction_returns_the_record_to_pending(inputs: PrepareInputs, state_dir: Path) -> None:
    record = decision(
        first_name=None,
        source_label="Anna Maria Schmidt",
        state=STATE_BLOCKED,
        blocked_reason=BLOCK_NAME_NOT_SPLIT,
    )
    seed(state_dir, record)

    apply_confirmations(
        inputs,
        ConfirmRequest(correct_phone=PHONE, correct_first_name="Anna Maria", correct_last_name="Schmidt"),
        snapshot=live_snapshot(record),
    )

    corrected = load(state_dir).get(PHONE)
    assert corrected.state == STATE_PENDING
    assert (corrected.first_name, corrected.last_name) == ("Anna Maria", "Schmidt")
    assert corrected.matches_shown()
    assert corrected.shown_digest != record.shown_digest, "a correction invalidates the old agreement"


def test_correcting_and_confirming_the_same_customer_is_refused(inputs: PrepareInputs, state_dir: Path) -> None:
    """Self-contradictory: the correction resets exactly what the confirm sets."""
    record = decision()
    seed(state_dir, record)

    with pytest.raises(PrepareError):
        apply_confirmations(
            inputs,
            ConfirmRequest(
                confirm_customers=(confirm_target(record),),
                correct_phone=PHONE,
                correct_first_name="Andere",
            ),
            snapshot=live_snapshot(record),
        )

    assert load(state_dir).get(PHONE).first_name == "Testkundin"


def test_a_skip_is_recorded_and_creates_nothing(inputs: PrepareInputs, state_dir: Path) -> None:
    record = decision()
    seed(state_dir, record)
    apply_confirmations(inputs, ConfirmRequest(skip_customers=(PHONE,)), snapshot=live_snapshot(record))

    assert load(state_dir).get(PHONE).state == STATE_SKIPPED


def test_confirming_never_reads_stdin(inputs: PrepareInputs, state_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Docker without a TTY: an EOF is not consent, and cannot be mistaken for it."""

    class Exploding:
        def read(self, *args: Any) -> str:
            raise AssertionError("stdin was read")

        readline = read
        __iter__ = read

    monkeypatch.setattr("sys.stdin", Exploding())
    record = decision()
    seed(state_dir, record)

    apply_confirmations(
        inputs,
        ConfirmRequest(confirm_customers=(confirm_target(record),)),
        snapshot=live_snapshot(record),
    )
    assert load(state_dir).get(PHONE).state == STATE_CONFIRMED


# ---------------------------------------------------------------------------
# The request body
# ---------------------------------------------------------------------------


def test_the_minimal_request_is_a_real_number_and_a_real_first_name() -> None:
    assert build_customer_request(decision()) == {"phone": PHONE, "first_name": "Testkundin"}


def test_nothing_optional_is_invented() -> None:
    body = build_customer_request(decision(email=None, last_name=None))

    assert "email" not in body
    assert "last_name" not in body
    assert set(body) == {"phone", "first_name"}


def test_known_optional_fields_are_carried_through() -> None:
    body = build_customer_request(decision(last_name="Schmidt", email="a@example.invalid"))

    assert body["last_name"] == "Schmidt"
    assert body["email"] == "a@example.invalid"


def test_a_request_without_a_first_name_is_refused() -> None:
    with pytest.raises(PrepareError):
        build_customer_request(decision(first_name=None))


def test_a_full_name_is_never_split_automatically() -> None:
    grouped = collect_source_customers([{"id": 1, "client": {"phone": PHONE, "name": "Anna Maria Schmidt"}}])

    person = grouped[PHONE]
    assert person.first_name is None
    assert person.full_name == "Anna Maria Schmidt"


def test_one_person_is_processed_once_with_all_their_records() -> None:
    grouped = collect_source_customers(
        [
            {"id": 1, "client": {"phone": PHONE, "first_name": "Testkundin", "id": 42}},
            {"id": 2, "client": {"phone": " +49 151 12345678 ", "first_name": "Testkundin", "id": 42}},
        ]
    )

    assert list(grouped) == [PHONE]
    assert grouped[PHONE].linked_record_count == 2
    assert grouped[PHONE].shares_phone is False


def test_two_source_customers_on_one_number_are_not_merged() -> None:
    grouped = collect_source_customers(
        [
            {"id": 1, "client": {"phone": PHONE, "first_name": "Anna", "id": 42}},
            {"id": 2, "client": {"phone": PHONE, "first_name": "Bea", "id": 43}},
        ]
    )

    assert grouped[PHONE].shares_phone is True


# ---------------------------------------------------------------------------
# Creating
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_creation_needs_its_own_permission(inputs: PrepareInputs, state_dir: Path) -> None:
    seed(state_dir, decision(state=STATE_CONFIRMED))
    client = FakeCreateClient()

    with pytest.raises(PrepareError):
        await run_create_customers(replace(inputs, create_allowed=False), write_client=client)
    assert client.posts == []


@pytest.mark.asyncio
async def test_only_confirmed_customers_are_created(inputs: PrepareInputs, state_dir: Path) -> None:
    seed(state_dir, decision(), decision(OTHER_PHONE, state=STATE_CONFIRMED))
    client = FakeCreateClient(
        pages=[page([])],
        cards={UUID_A: {"data": card(UUID_A, phone=OTHER_PHONE)}},
        create=lambda body: {"data": {"uuid": UUID_A, **body}},
    )

    result = await run_create_customers(inputs, write_client=client)

    assert [body["phone"] for body in client.posts] == [OTHER_PHONE]
    assert load(state_dir).get(PHONE).state == STATE_PENDING
    assert result.machine["halted"] is None


@pytest.mark.asyncio
async def test_existence_is_re_checked_immediately_before_the_post(inputs: PrepareInputs, state_dir: Path) -> None:
    """A card created in the UI five minutes ago must stop the POST."""
    seed(state_dir, decision(state=STATE_CONFIRMED))
    client = FakeCreateClient(pages=[page([card()])])

    await run_create_customers(inputs, write_client=client)

    assert client.posts == []
    record = load(state_dir).get(PHONE)
    assert record.state == STATE_BLOCKED
    assert record.blocked_reason == BLOCK_ALREADY_EXISTS
    assert record.customer_uuid == UUID_A


@pytest.mark.asyncio
async def test_an_undetermined_recheck_stops_the_run(inputs: PrepareInputs, state_dir: Path) -> None:
    seed(state_dir, decision(state=STATE_CONFIRMED))
    client = FakeCreateClient(pages=[EasyWeekRetryableError("timeout", operation="list_customers")])

    result = await run_create_customers(inputs, write_client=client)

    assert client.posts == []
    assert result.machine["halted"] == BLOCK_LOOKUP_UNDETERMINED
    assert result.exit_code == 1


@pytest.mark.asyncio
async def test_a_creation_is_proven_by_reading_the_workspace(inputs: PrepareInputs, state_dir: Path) -> None:
    seed(state_dir, decision(state=STATE_CONFIRMED))
    client = FakeCreateClient(
        pages=[page([])],
        cards={UUID_A: {"data": card()}},
        create=lambda body: {"data": {"uuid": UUID_A, **body}},
    )

    await run_create_customers(inputs, write_client=client)

    record = load(state_dir).get(PHONE)
    assert record.state == STATE_CREATED
    assert record.customer_uuid == UUID_A


@pytest.mark.asyncio
async def test_a_post_whose_card_cannot_be_read_back_is_not_recorded_as_created(
    inputs: PrepareInputs, state_dir: Path
) -> None:
    seed(state_dir, decision(state=STATE_CONFIRMED))
    client = FakeCreateClient(
        pages=[page([])],
        cards={UUID_A: EasyWeekRetryableError("timeout", operation="get_customer")},
        create=lambda body: {"data": {"uuid": UUID_A, **body}},
    )

    result = await run_create_customers(inputs, write_client=client)

    assert result.machine["halted"] == BLOCK_CREATE_UNVERIFIED
    assert load(state_dir).get(PHONE).state == STATE_IN_FLIGHT
    assert build_customer_directory_payload(load(state_dir)) == []


@pytest.mark.asyncio
async def test_a_card_read_back_with_another_number_is_not_accepted(inputs: PrepareInputs, state_dir: Path) -> None:
    seed(state_dir, decision(state=STATE_CONFIRMED))
    client = FakeCreateClient(
        pages=[page([])],
        cards={UUID_A: {"data": card(UUID_A, phone=OTHER_PHONE)}},
        create=lambda body: {"data": {"uuid": UUID_A, **body}},
    )

    result = await run_create_customers(inputs, write_client=client)

    assert result.machine["halted"] == BLOCK_CREATE_UNVERIFIED
    assert load(state_dir).get(PHONE).state == STATE_IN_FLIGHT


@pytest.mark.asyncio
async def test_a_2xx_without_a_uuid_is_uncertain_not_success(inputs: PrepareInputs, state_dir: Path) -> None:
    seed(state_dir, decision(state=STATE_CONFIRMED))
    client = FakeCreateClient(pages=[page([])], create={"data": {"first_name": "Testkundin"}})

    result = await run_create_customers(inputs, write_client=client)

    assert result.machine["halted"] == BLOCK_CREATE_UNVERIFIED
    assert load(state_dir).get(PHONE).state == STATE_IN_FLIGHT


@pytest.mark.asyncio
async def test_an_uncertain_post_is_never_retried(inputs: PrepareInputs, state_dir: Path) -> None:
    seed(state_dir, decision(state=STATE_CONFIRMED), decision(OTHER_PHONE, state=STATE_CONFIRMED))
    client = FakeCreateClient(
        pages=[page([])],
        create=EasyWeekUncertainMutation("timeout", operation="create_customer"),
    )

    result = await run_create_customers(inputs, write_client=client)

    assert len(client.posts) == 1, "one POST, and the run stops"
    assert result.machine["halted"] == BLOCK_CREATE_UNCERTAIN
    assert load(state_dir).get(PHONE).state == STATE_IN_FLIGHT


@pytest.mark.asyncio
async def test_the_in_flight_marker_is_written_before_the_request(inputs: PrepareInputs, state_dir: Path) -> None:
    """A crash mid-request must leave evidence that a POST may exist."""
    seen: dict[str, Any] = {}
    seed(state_dir, decision(state=STATE_CONFIRMED))

    def _capture(body: dict[str, Any]) -> dict[str, Any]:
        stored = json.loads((state_dir / "customer_decisions.json").read_text())
        seen["state"] = stored["decisions"][0]["state"]
        raise EasyWeekUncertainMutation("boom", operation="create_customer")

    client = FakeCreateClient(pages=[page([])], create=_capture)
    await run_create_customers(inputs, write_client=client)

    assert seen["state"] == STATE_IN_FLIGHT


@pytest.mark.asyncio
async def test_a_restart_reconciles_in_flight_by_reading_not_by_posting(inputs: PrepareInputs, state_dir: Path) -> None:
    seed(state_dir, decision(state=STATE_IN_FLIGHT))
    client = FakeCreateClient(pages=[page([card()])])

    result = await run_create_customers(inputs, write_client=client)

    assert client.posts == [], "the previous POST landed; a second one is the duplicate"
    assert load(state_dir).get(PHONE).state == STATE_CREATED
    assert load(state_dir).get(PHONE).customer_uuid == UUID_A
    assert result.machine["halted"] is None


@pytest.mark.asyncio
async def test_a_restart_retries_only_when_the_workspace_proves_absence(inputs: PrepareInputs, state_dir: Path) -> None:
    seed(state_dir, decision(state=STATE_IN_FLIGHT))
    client = FakeCreateClient(
        pages=[page([])],
        cards={UUID_A: {"data": card()}},
        create=lambda body: {"data": {"uuid": UUID_A, **body}},
    )

    await run_create_customers(inputs, write_client=client)

    assert len(client.posts) == 1
    assert load(state_dir).get(PHONE).state == STATE_CREATED


@pytest.mark.asyncio
async def test_a_restart_that_still_cannot_tell_creates_nothing(inputs: PrepareInputs, state_dir: Path) -> None:
    seed(state_dir, decision(state=STATE_IN_FLIGHT), decision(OTHER_PHONE, state=STATE_CONFIRMED))
    client = FakeCreateClient(pages=[EasyWeekAuthError("denied", operation="list_customers")])

    result = await run_create_customers(inputs, write_client=client)

    assert client.posts == []
    assert result.machine["halted"] == BLOCK_CREATE_UNCERTAIN
    assert load(state_dir).get(OTHER_PHONE).state == STATE_CONFIRMED


@pytest.mark.asyncio
async def test_a_restart_finding_two_cards_stops_for_a_person(inputs: PrepareInputs, state_dir: Path) -> None:
    seed(state_dir, decision(state=STATE_IN_FLIGHT))
    client = FakeCreateClient(pages=[page([card(UUID_A), card(UUID_B)])])

    await run_create_customers(inputs, write_client=client)

    record = load(state_dir).get(PHONE)
    assert record.state == STATE_BLOCKED
    assert record.blocked_reason == "customer_ambiguous"


@pytest.mark.asyncio
async def test_a_uniqueness_conflict_never_edits_the_contact_details(inputs: PrepareInputs, state_dir: Path) -> None:
    """The answer to "that phone is taken" is to look at who has it."""
    seed(state_dir, decision(email="taken@example.invalid", state=STATE_CONFIRMED))
    client = FakeCreateClient(
        pages=[page([])],
        create=EasyWeekPermanentError("422", operation="create_customer", status_code=422),
    )

    await run_create_customers(inputs, write_client=client)

    record = load(state_dir).get(PHONE)
    assert record.state == STATE_BLOCKED
    assert record.blocked_reason == BLOCK_CREATE_REJECTED
    assert record.email == "taken@example.invalid"
    assert record.phone == PHONE
    assert len(client.posts) == 1


@pytest.mark.asyncio
async def test_an_auth_failure_keeps_the_confirmation_and_names_itself(inputs: PrepareInputs, state_dir: Path) -> None:
    """The key cannot write; nothing is wrong with the customer's data."""
    from altegio_bot.easyweek_migration.prepare import BLOCK_CREATE_ACCESS_DENIED

    seed(state_dir, decision(state=STATE_CONFIRMED))
    client = FakeCreateClient(
        pages=[page([])],
        create=EasyWeekAuthError("denied", operation="create_customer", status_code=403),
    )

    result = await run_create_customers(inputs, write_client=client)

    assert result.machine["halted"] == BLOCK_CREATE_ACCESS_DENIED
    assert load(state_dir).get(PHONE).state == STATE_CONFIRMED, "not in flight: the server did nothing"


@pytest.mark.asyncio
async def test_a_creation_run_cannot_start_while_another_holds_the_lock(inputs: PrepareInputs, state_dir: Path) -> None:
    seed(state_dir, decision(state=STATE_CONFIRMED))
    holder = CustomerDecisionStore(state_dir)
    holder.acquire()
    try:
        with pytest.raises(DecisionStoreLocked):
            await run_create_customers(inputs, write_client=FakeCreateClient())
    finally:
        holder.release()


@pytest.mark.asyncio
async def test_the_directory_only_holds_customers_that_were_proven(inputs: PrepareInputs, state_dir: Path) -> None:
    seed(
        state_dir,
        decision(state=STATE_CONFIRMED),
        decision(OTHER_PHONE, state=STATE_PENDING),
    )
    client = FakeCreateClient(
        pages=[page([])],
        cards={UUID_A: {"data": card()}},
        create=lambda body: {"data": {"uuid": UUID_A, **body}},
    )

    await run_create_customers(inputs, write_client=client)
    written = json.loads((state_dir / FILE_CUSTOMER_DIRECTORY).read_text())

    assert [row["phone"] for row in written] == [PHONE]
    assert written[0]["uuid"] == UUID_A


def test_a_found_but_unaddressable_card_still_reaches_the_directory() -> None:
    """ "Cannot address them" and "not in EasyWeek" call for opposite actions."""
    decisions = DecisionSet()
    decisions.records[PHONE] = decision(
        first_name=None,
        state=STATE_BLOCKED,
        blocked_reason="customer_first_name_missing",
        customer_uuid=UUID_A,
    )

    [row] = build_customer_directory_payload(decisions)
    assert row["uuid"] == UUID_A
    assert row["first_name"] is None


def test_an_ambiguous_customer_never_reaches_the_directory() -> None:
    """Two cards on one number: putting either in would pick a winner."""
    decisions = DecisionSet()
    decisions.records[PHONE] = decision(state=STATE_BLOCKED, blocked_reason="customer_ambiguous", customer_uuid=None)

    assert build_customer_directory_payload(decisions) == []


def test_a_shared_phone_is_never_offered_for_creation(state_dir: Path) -> None:
    seed(state_dir, decision(state=STATE_BLOCKED, blocked_reason=BLOCK_SHARED_PHONE))
    record = load(state_dir).get(PHONE)

    assert record.state == STATE_BLOCKED
    assert record.creatable is False


def test_the_safe_shape_of_a_decision_carries_no_person(state_dir: Path) -> None:
    safe = decision(first_name="Testkundin", email="a@example.invalid").as_safe_dict()
    blob = json.dumps(safe)

    for secret in (PHONE, "Testkundin", "a@example.invalid"):
        assert secret not in blob
    assert safe["has_first_name"] is True


# ---------------------------------------------------------------------------
# Every field a customer review shows is inside the digest (review finding 5)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "change",
    [
        {"first_name": "Andere"},
        {"last_name": "Schmidt"},
        {"email": "neu@example.invalid"},
        {"linked_record_count": 3},
        {"phone": OTHER_PHONE},
    ],
    ids=["first_name", "last_name", "email", "linked_records", "phone"],
)
def test_changing_any_shown_customer_field_changes_the_digest(change: dict[str, Any]) -> None:
    assert decision(**change).shown_digest != decision().shown_digest


def test_the_customer_digest_covers_exactly_what_is_presented() -> None:
    """The presentation IS the digest input; nothing shown sits outside it."""
    record = decision(last_name="Schmidt", email="k@example.invalid")

    assert set(record.presentation()) == {
        "phone",
        "first_name",
        "last_name",
        "email",
        "linked_record_count",
        "source_identity",
        "source_record_ids",
        "source_record_count",
        "lookup_outcome",
        "easyweek_customer_uuid",
        "review_state",
        "intended_action",
        "blocked_reason",
        "correction_applied",
        "correction_stale",
        "evidence_current",
    }
    assert record.matches_shown()
    assert replace(record, linked_record_count=99).matches_shown() is False


@pytest.mark.parametrize(
    ("old_lookup", "old_reason"),
    [
        ("found", BLOCK_ALREADY_EXISTS),
        ("ambiguous", "customer_ambiguous"),
        ("undetermined", BLOCK_LOOKUP_UNDETERMINED),
    ],
)
def test_a_blocked_lookup_digest_never_authorises_a_later_absent_customer(
    old_lookup: str,
    old_reason: str,
    inputs: PrepareInputs,
    state_dir: Path,
) -> None:
    reviewed = decision(
        state=STATE_BLOCKED,
        lookup_outcome=old_lookup,
        blocked_reason=old_reason,
        customer_uuid=UUID_A if old_lookup == "found" else None,
    )
    fresh = decision()
    assert reviewed.shown_digest != fresh.shown_digest
    seed(state_dir, reviewed)

    with pytest.raises(PrepareError):
        apply_confirmations(
            inputs,
            ConfirmRequest(confirm_customers=(confirm_target(reviewed),)),
            snapshot=live_snapshot(fresh),
        )

    assert load(state_dir).get(PHONE).state == STATE_BLOCKED


@pytest.mark.parametrize(
    "change",
    [
        {"customer_uuid": UUID_B},
        {"source_identity": "b" * 64},
        {"source_record_ids": (900001, 900003)},
        {"lookup_outcome": "undetermined"},
        {"intended_action": "none"},
    ],
    ids=["customer_uuid", "source_identity", "source_records", "lookup", "intended_action"],
)
def test_customer_decision_evidence_changes_the_review_and_batch_digest(change: dict[str, Any]) -> None:
    before = decision(customer_uuid=UUID_A)
    after_values = {"customer_uuid": UUID_A, **change}
    after = decision(**after_values)

    assert before.shown_digest != after.shown_digest
    assert pending_digest(DecisionSet(records={PHONE: before})) != pending_digest(DecisionSet(records={PHONE: after}))


def test_an_unchanged_pending_create_accepts_its_current_digest(inputs: PrepareInputs, state_dir: Path) -> None:
    reviewed = decision()
    seed(state_dir, reviewed)

    apply_confirmations(
        inputs,
        ConfirmRequest(confirm_customers=(confirm_target(reviewed),)),
        snapshot=live_snapshot(reviewed),
    )

    assert load(state_dir).get(PHONE).state == STATE_CONFIRMED


def test_a_legacy_confirmed_decision_loses_write_authority(state_dir: Path) -> None:
    legacy = decision(state=STATE_CONFIRMED).to_json()
    for key in (
        "source_identity",
        "source_record_ids",
        "lookup_outcome",
        "review_state",
        "intended_action",
        "correction_applied",
        "correction_stale",
        "evidence_current",
    ):
        legacy.pop(key)
    state_dir.mkdir(parents=True)
    (state_dir / "customer_decisions.json").write_text(
        json.dumps({"version": 1, "decisions": [legacy]}),
        encoding="utf-8",
    )

    loaded = load(state_dir).get(PHONE)

    assert loaded is not None
    assert loaded.state == STATE_BLOCKED
    assert loaded.creatable is False
    assert loaded.shown_digest == ""


@pytest.mark.asyncio
async def test_a_legacy_in_flight_absence_requires_fresh_review_before_retry(
    inputs: PrepareInputs,
    state_dir: Path,
) -> None:
    legacy = decision(state=STATE_IN_FLIGHT).to_json()
    for key in (
        "source_identity",
        "source_record_ids",
        "lookup_outcome",
        "review_state",
        "intended_action",
        "correction_applied",
        "correction_stale",
        "evidence_current",
    ):
        legacy.pop(key)
    state_dir.mkdir(parents=True)
    (state_dir / "customer_decisions.json").write_text(
        json.dumps({"version": 1, "decisions": [legacy]}),
        encoding="utf-8",
    )
    client = FakeCreateClient(pages=[page([])])

    await run_create_customers(inputs, write_client=client)

    stored = load(state_dir).get(PHONE)
    assert client.posts == []
    assert stored is not None
    assert stored.state == STATE_BLOCKED
    assert stored.blocked_reason == LEGACY_EVIDENCE_MISSING
    assert stored.evidence_current is False


def test_a_legacy_created_marker_survives_v2_rewrite_and_reload(state_dir: Path) -> None:
    legacy = decision(state=STATE_CREATED, customer_uuid=UUID_A).to_json()
    for key in (
        "source_identity",
        "source_record_ids",
        "lookup_outcome",
        "review_state",
        "intended_action",
        "correction_applied",
        "correction_stale",
        "evidence_current",
    ):
        legacy.pop(key)
    state_dir.mkdir(parents=True)
    (state_dir / "customer_decisions.json").write_text(
        json.dumps({"version": 1, "decisions": [legacy]}),
        encoding="utf-8",
    )

    store = CustomerDecisionStore(state_dir)
    with store:
        records = store.load()
        store.save(records)

    preserved = load(state_dir).get(PHONE)
    assert preserved is not None
    assert preserved.state == STATE_CREATED
    assert preserved.customer_uuid == UUID_A
    assert preserved.evidence_current is False


def test_a_changed_shown_field_cancels_an_existing_agreement(inputs: PrepareInputs, state_dir: Path) -> None:
    """Confirmed at one set of values; the source then moved."""
    reviewed = decision()
    seed(state_dir, reviewed)
    apply_confirmations(
        inputs,
        ConfirmRequest(confirm_customers=(confirm_target(reviewed),)),
        snapshot=live_snapshot(reviewed),
    )
    assert load(state_dir).get(PHONE).state == STATE_CONFIRMED

    moved = decision(email="neu@example.invalid")
    apply_confirmations(inputs, ConfirmRequest(), snapshot=live_snapshot(moved))

    record = load(state_dir).get(PHONE)
    assert record.state == STATE_PENDING, "the agreement was about the old values"
    assert record.email == "neu@example.invalid"


def test_unchanged_data_keeps_the_agreement_through_a_confirm_run(inputs: PrepareInputs, state_dir: Path) -> None:
    reviewed = decision()
    seed(state_dir, reviewed)
    apply_confirmations(
        inputs,
        ConfirmRequest(confirm_customers=(confirm_target(reviewed),)),
        snapshot=live_snapshot(reviewed),
    )

    apply_confirmations(inputs, ConfirmRequest(), snapshot=live_snapshot(reviewed))

    assert load(state_dir).get(PHONE).state == STATE_CONFIRMED


def test_a_confirmation_is_refused_when_only_the_live_data_moved(inputs: PrepareInputs, state_dir: Path) -> None:
    """The store and the operator agree; the workspace does not. Fail closed."""
    stored = decision()
    seed(state_dir, stored)
    live = decision(linked_record_count=7)

    with pytest.raises(PrepareError):
        apply_confirmations(
            inputs,
            ConfirmRequest(confirm_customers=(confirm_target(stored),)),
            snapshot=live_snapshot(live),
        )

    assert load(state_dir).get(PHONE).state == STATE_PENDING


def test_a_digest_the_operator_never_reviewed_is_refused(inputs: PrepareInputs, state_dir: Path) -> None:
    """The hole the review found, approached from the other side.

    Here the operator supplies a digest matching the CURRENT live data
    perfectly — obtained from somewhere other than the review they were handed:
    a second machine's run, a colleague, an error message. The reviewed decision
    on disk is still the older one, so this would be a yes to a proposal nobody
    actually read.
    """
    reviewed = decision()
    seed(state_dir, reviewed)
    live = decision(linked_record_count=7)

    with pytest.raises(PrepareError):
        apply_confirmations(
            inputs,
            ConfirmRequest(confirm_customers=(confirm_target(live),)),
            snapshot=live_snapshot(live),
        )

    assert load(state_dir).get(PHONE).state == STATE_PENDING


@pytest.mark.parametrize("state", [STATE_CREATED, STATE_IN_FLIGHT])
def test_a_created_or_in_flight_customer_cannot_be_corrected(
    state: str, inputs: PrepareInputs, state_dir: Path
) -> None:
    """A created card exists and an in-flight one may. Neither is editable here.

    Correcting either would rewrite a real customer, or reopen a decision whose
    POST may already have landed — the duplicate-card path the whole stage is
    built to prevent.
    """
    record = decision(state=state, customer_uuid=UUID_A)
    seed(state_dir, record)

    with pytest.raises(PrepareError):
        apply_confirmations(
            inputs,
            ConfirmRequest(correct_phone=PHONE, correct_first_name="Andere"),
            snapshot=live_snapshot(record),
        )

    unchanged = load(state_dir).get(PHONE)
    assert unchanged.state == state
    assert unchanged.first_name == "Testkundin"


def test_a_correction_is_refused_when_the_source_no_longer_proves_the_customer(
    inputs: PrepareInputs, state_dir: Path
) -> None:
    """No source evidence, nothing to bind the correction to."""
    record = decision()
    seed(state_dir, record)

    with pytest.raises(PrepareError):
        apply_confirmations(
            inputs,
            ConfirmRequest(correct_phone=PHONE, correct_first_name="Andere"),
            snapshot=live_snapshot(record, sources={}),
        )

    assert load(state_dir).get(PHONE).first_name == "Testkundin"
