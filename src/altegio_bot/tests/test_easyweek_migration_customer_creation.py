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
    PrepareError,
    PrepareInputs,
    apply_confirmations,
    build_customer_directory_payload,
    build_customer_request,
    collect_source_customers,
    pending_digest,
    run_create_customers,
)
from altegio_bot.easyweek_migration.write_client import EasyWeekUncertainMutation
from altegio_bot.tests.test_easyweek_migration_customer_lookup import (
    PHONE,
    UUID_A,
    UUID_B,
    FakeCustomerReads,
    card,
    page,
)
from altegio_bot.tests.test_easyweek_migration_planning import manifest_text

OTHER_PHONE = "+4915199999999"


def decision(phone: str = PHONE, **overrides: Any) -> CustomerDecision:
    base = {
        "phone": phone,
        "first_name": "Testkundin",
        "last_name": None,
        "email": None,
        "linked_record_count": 2,
        "source_label": "",
    }
    base.update(overrides)
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


def test_a_confirmation_names_one_customer(inputs: PrepareInputs, state_dir: Path) -> None:
    seed(state_dir, decision(), decision(OTHER_PHONE))
    apply_confirmations(inputs, ConfirmRequest(confirm_customers=(PHONE,)))

    decisions = load(state_dir)
    assert decisions.get(PHONE).state == STATE_CONFIRMED
    assert decisions.get(OTHER_PHONE).state == STATE_PENDING


def test_a_batch_confirmation_is_bound_to_the_printed_list(inputs: PrepareInputs, state_dir: Path) -> None:
    seed(state_dir, decision(), decision(OTHER_PHONE))
    printed = pending_digest(load(state_dir))

    apply_confirmations(
        inputs,
        ConfirmRequest(confirm_all_pending=True, expected_pending_digest=printed),
    )

    assert all(record.state == STATE_CONFIRMED for record in load(state_dir).records.values())


def test_a_batch_confirmation_refuses_a_list_that_moved(inputs: PrepareInputs, state_dir: Path) -> None:
    """A yes to one list is not a yes to a longer one."""
    seed(state_dir, decision())
    printed = pending_digest(load(state_dir))
    seed(state_dir, decision(), decision(OTHER_PHONE))

    with pytest.raises(PrepareError):
        apply_confirmations(inputs, ConfirmRequest(confirm_all_pending=True, expected_pending_digest=printed))

    assert all(record.state == STATE_PENDING for record in load(state_dir).records.values())


def test_a_batch_confirmation_without_a_digest_is_refused(inputs: PrepareInputs, state_dir: Path) -> None:
    seed(state_dir, decision())

    with pytest.raises(PrepareError):
        apply_confirmations(inputs, ConfirmRequest(confirm_all_pending=True))


def test_a_confirmation_against_stale_data_is_refused(inputs: PrepareInputs, state_dir: Path) -> None:
    stale = replace(decision(), first_name="Andere")  # digest still describes the old name
    seed(state_dir, stale)

    outcome = apply_confirmations(inputs, ConfirmRequest(confirm_customers=(PHONE,)))

    assert load(state_dir).get(PHONE).state == STATE_PENDING
    assert {"reason": "decision_stale"} in outcome["refused"]


def test_a_correction_returns_the_record_to_pending(inputs: PrepareInputs, state_dir: Path) -> None:
    seed(
        state_dir,
        decision(
            first_name=None, source_label="Anna Maria Schmidt", state=STATE_BLOCKED, blocked_reason=BLOCK_NAME_NOT_SPLIT
        ),
    )

    apply_confirmations(
        inputs,
        ConfirmRequest(correct_phone=PHONE, correct_first_name="Anna Maria", correct_last_name="Schmidt"),
    )

    record = load(state_dir).get(PHONE)
    assert record.state == STATE_PENDING
    assert (record.first_name, record.last_name) == ("Anna Maria", "Schmidt")
    assert record.matches_shown()


def test_a_skip_is_recorded_and_creates_nothing(inputs: PrepareInputs, state_dir: Path) -> None:
    seed(state_dir, decision())
    apply_confirmations(inputs, ConfirmRequest(skip_customers=(PHONE,)))

    assert load(state_dir).get(PHONE).state == STATE_SKIPPED


def test_confirming_never_reads_stdin(inputs: PrepareInputs, state_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Docker without a TTY: an EOF is not consent, and cannot be mistaken for it."""

    class Exploding:
        def read(self, *args: Any) -> str:
            raise AssertionError("stdin was read")

        readline = read
        __iter__ = read

    monkeypatch.setattr("sys.stdin", Exploding())
    seed(state_dir, decision())

    apply_confirmations(inputs, ConfirmRequest(confirm_customers=(PHONE,)))
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
