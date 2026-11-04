"""§46.2 — the follow-up preview, and everything it must refuse.

What these tests are really about
---------------------------------
The preview's whole value is that it says no for the right reason. So almost
every test here states one fact about one person and asserts the exact reason
code that fact produces — never just "not included", because "not included for
the wrong reason" is a defect that would survive a weaker assertion and show up
later as somebody included for the wrong reason.

Strict network isolation
------------------------
Nothing in this module may reach the network. ``respx`` is armed with
``assert_all_mocked=True`` around every path that could make a request, so an
unexpected call fails the test instead of silently depending on absent
credentials. The two seams a preview legitimately has — the EasyWeek booking
read and the Meta template listing — are passed in as fakes, and the tests that
matter most assert that NEITHER of them was used where it must not be.
"""

from __future__ import annotations

import uuid as uuid_module
from datetime import datetime, timedelta
from typing import Any

import pytest
import respx
from pydantic import SecretStr
from sqlalchemy import func, select

from altegio_bot.campaigns.easyweek_customer_history import CUSTOMER_BOOKINGS_PER_PAGE
from altegio_bot.campaigns.easyweek_voucher_followup import followup_template_contract as message
from altegio_bot.campaigns.easyweek_voucher_followup import preview as preview_module
from altegio_bot.campaigns.easyweek_voucher_followup.eligibility import (
    ENTITLEMENT_INVALID_STATUSES,
    entitlement_verdict,
)
from altegio_bot.campaigns.easyweek_voucher_followup.failure_policy import (
    ADMISSIBLE_CLASS_FLOOR,
    ADMISSIBLE_CODE_FLOOR,
    CURRENT_FAILURE_POLICY,
    FailureFact,
    FailurePolicy,
)
from altegio_bot.campaigns.easyweek_voucher_followup.groups import SourceEvidence, resolve_group
from altegio_bot.campaigns.easyweek_voucher_followup.identity import (
    BATCH_SCHEMA_UNSUPPORTED,
    BATCH_SCOPE_UNSUPPORTED,
    BATCH_TERMINAL_STOP,
    CONSENT_PENDING_INBOUND,
    CONSENT_REFUSED,
    CONSENT_UNKNOWN,
    CONTRACT_VERSION,
    CUSTOMER_REPLIED,
    ENTITLEMENT_INVALID,
    ENTITLEMENT_UNPROVEN,
    EVIDENCE_UNPROVEN,
    FAILURE_POLICY_UNPROVEN,
    FOLLOWUP_GROUP_DELIVERED,
    FOLLOWUP_GROUP_FAILED,
    FOLLOWUP_GROUP_READ,
    FOLLOWUP_ROW_INCLUDED,
    FOLLOWUP_ROW_UNKNOWN,
    FUTURE_BOOKING_ACTIVE,
    GROUPS_REQUIRED,
    HISTORY_UNPROVEN,
    IDENTITY_CONFLICT,
    INTERVAL_NOT_ELAPSED,
    INTERVAL_REQUIRED,
    NEW_BOOKING_AFTER_ATTEMPT,
    SENDER_UNPROVEN,
    SOURCE_ATTEMPT_UNPROVEN,
    TEMPLATE_UNPROVEN,
    canonical_groups,
)
from altegio_bot.campaigns.easyweek_voucher_production import ledger as production_ledger
from altegio_bot.models.models import (
    CONSENT_COMMAND_NONE,
    CONSENT_COMMAND_STOP,
    CONSENT_SOURCE_INBOUND,
    CONSENT_STATE_OPTED_OUT,
    DELIVERY_FAILURE_CLASS_PERMANENT,
    DELIVERY_FAILURE_CLASS_TRANSIENT,
    DELIVERY_FAILURE_CLASS_UNKNOWN,
    DELIVERY_FAILURE_MATCH_CONFIRMED,
    DELIVERY_FAILURE_MATCH_CONFLICTING,
    DELIVERY_FAILURE_MATCH_UNMATCHED,
    DELIVERY_FAILURE_MATCH_UNPROVEN,
    DELIVERY_FAILURE_REASON_API_REJECTED,
    DELIVERY_FAILURE_REASON_CALLBACK_FAILED,
    DELIVERY_FAILURE_SOURCE_CALLBACK,
    DELIVERY_FAILURE_SOURCE_META_API,
    FOLLOWUP_GROUP_UNPROVEN,
    INBOUND_AUTHENTICITY_SIGNED,
    INBOUND_AUTHENTICITY_UNVERIFIED,
    RECIPIENT_BASIS_MANUAL,
    VOUCHER_PRODUCTION_ITEM_AMBIGUOUS,
    VOUCHER_PRODUCTION_ITEM_PROVIDER_ACCEPTED,
    VOUCHER_PRODUCTION_ITEM_SEND_REJECTED,
    VOUCHER_PRODUCTION_ITEM_SEND_UNKNOWN,
    Client,
    EasyWeekVoucherFollowupPreview,
    EasyWeekVoucherFollowupPreviewRow,
    EasyWeekVoucherProductionBatch,
    EasyWeekVoucherProductionBatchAttempt,
    EasyWeekVoucherProductionBatchItem,
    EasyWeekVoucherProductionDeliveryFailure,
    WhatsAppConsentSuppression,
    WhatsAppInboundReceipt,
)
from altegio_bot.settings import settings
from altegio_bot.tests.easyweek_voucher_followup_fixtures import (
    ATTEMPTED_AT,
    NOW,
    SENDER_DIGEST,
    TemplateReader,
    person,
    seed_sender_and_template,
    seed_source_batch,
)

THIRTY_DAYS = 30 * 24 * 60 * 60
FOURTEEN_DAYS = 14 * 24 * 60 * 60


# ---------------------------------------------------------------------------
# Fakes for the two seams
# ---------------------------------------------------------------------------


class FakeHistoryReader:
    """The EasyWeek customer read, answered from a fixture.

    It counts its calls, which several tests assert on: a preview that reads
    bookings for somebody it had already refused locally would be spending a
    request — and an EasyWeek rate budget — on an answer it cannot use.
    """

    def __init__(
        self,
        *,
        bookings: dict[str, list[dict[str, Any]]] | None = None,
        customer_error: Exception | None = None,
        history_error: Exception | None = None,
        truncate: bool = False,
        wrong_customer: str | None = None,
    ) -> None:
        self.bookings = bookings or {}
        self.customer_error = customer_error
        self.history_error = history_error
        self.truncate = truncate
        self.wrong_customer = wrong_customer
        self.customer_calls: list[str] = []
        self.history_calls: list[tuple[str, int]] = []

    async def get_booking(self, booking_uuid: str) -> dict[str, Any]:  # pragma: no cover - unused here
        raise AssertionError("the follow-up preview must not read a source booking directly")

    async def get_customer(self, customer_uuid: str) -> dict[str, Any]:
        self.customer_calls.append(customer_uuid)
        if self.customer_error is not None:
            raise self.customer_error
        return {"uuid": self.wrong_customer or customer_uuid}

    async def list_customer_bookings(
        self, customer_uuid: str, page: int, per_page: int = CUSTOMER_BOOKINGS_PER_PAGE
    ) -> dict[str, Any]:
        self.history_calls.append((customer_uuid, page))
        if self.history_error is not None:
            raise self.history_error
        rows = list(self.bookings.get(customer_uuid, []))
        total = len(rows)
        if self.truncate:
            # A declared total the pages never add up to: exactly the shape §46.5
            # says must become unknown rather than "no bookings".
            total = total + 1
        last = max(1, (total + CUSTOMER_BOOKINGS_PER_PAGE - 1) // CUSTOMER_BOOKINGS_PER_PAGE)
        start = (page - 1) * CUSTOMER_BOOKINGS_PER_PAGE
        return {
            "data": rows[start : start + CUSTOMER_BOOKINGS_PER_PAGE],
            "meta": {
                "current_page": page,
                "last_page": last,
                "per_page": CUSTOMER_BOOKINGS_PER_PAGE,
                "total": total,
            },
        }


def booking(
    *,
    index: int,
    customer_uuid: str,
    start: datetime,
    canceled: bool = False,
    completed: bool = True,
) -> dict[str, Any]:
    return {
        "uuid": f"{index:08d}-7777-4777-8777-777777777777",
        "customer": {"uuid": customer_uuid},
        "is_canceled": canceled,
        "is_completed": completed,
        "start_time": start.isoformat(),
    }


@pytest.fixture
def binding_key(monkeypatch: pytest.MonkeyPatch) -> None:
    """The voucher MAC key the fixtures' artifacts were bound under."""
    monkeypatch.setattr(settings, "easyweek_voucher_delivery_hmac_key", SecretStr("k" * 48), raising=False)
    monkeypatch.setattr(settings, "easyweek_voucher_delivery_hmac_key_id", "test-key-1", raising=False)


@pytest.fixture
def no_network() -> Any:
    """Every HTTP request in this test is a defect, including a 'harmless' one.

    ``assert_all_mocked=True`` with no routes registered: the first request of
    any kind raises. Deliberately not left to absent credentials — a deployment
    with credentials would then behave differently from the test that passed.
    """
    with respx.mock(assert_all_called=False, assert_all_mocked=True) as mock:
        yield mock


async def build(
    session_maker: Any,
    *,
    batch_id: int,
    groups: tuple[str, ...] = (FOLLOWUP_GROUP_DELIVERED,),
    interval: int = FOURTEEN_DAYS,
    reader: Any = None,
    templates: Any = None,
    now: datetime = NOW,
    policy: Any = CURRENT_FAILURE_POLICY,
    operator: str = "operator@example.test:fp",
) -> preview_module.PreviewOutcome:
    return await preview_module.create_preview(
        session_maker,
        spec=preview_module.PreviewSpec(
            batch_id=batch_id,
            groups=groups,
            min_interval_seconds=interval,
            operator_principal=operator,
        ),
        booking_reader=reader if reader is not None else FakeHistoryReader(),
        template_reader=templates if templates is not None else TemplateReader(),
        policy=policy,
        now=now,
    )


def reason_for(outcome: preview_module.PreviewOutcome, slot: int) -> str | None:
    for row in outcome.rows:
        if row.slot == slot:
            return row.reason
    raise AssertionError(f"slot {slot} is not in the preview")


def decision_for(outcome: preview_module.PreviewOutcome, slot: int) -> str:
    for row in outcome.rows:
        if row.slot == slot:
            return row.decision
    raise AssertionError(f"slot {slot} is not in the preview")


# ---------------------------------------------------------------------------
# The group selection itself (§46.3)
# ---------------------------------------------------------------------------


def test_group_selection_is_never_implicitly_everything() -> None:
    """An empty or unrecognised selection is empty, and empty is refused."""
    assert canonical_groups([]) == ()
    assert canonical_groups(None) == ()
    assert canonical_groups("read") == ()
    assert canonical_groups(["nope"]) == ()
    # And an unknown name alongside a real one does not widen the real one.
    assert canonical_groups(["nope", FOLLOWUP_GROUP_READ]) == (FOLLOWUP_GROUP_READ,)


def test_group_selection_is_canonical_and_deduplicated() -> None:
    """The stored selection does not depend on the order a form submitted."""
    one = canonical_groups([FOLLOWUP_GROUP_FAILED, FOLLOWUP_GROUP_READ, FOLLOWUP_GROUP_READ])
    two = canonical_groups([FOLLOWUP_GROUP_READ, FOLLOWUP_GROUP_FAILED])
    assert one == two == (FOLLOWUP_GROUP_READ, FOLLOWUP_GROUP_FAILED)
    assert len(set(one)) == len(one)


def _evidence(**changes: Any) -> SourceEvidence:
    base: dict[str, Any] = {
        "item_id": 1,
        "slot": 1,
        "status": "delivered",
        "send_attempt_count": 1,
        "send_attempted_at": ATTEMPTED_AT,
        "delivered_at": ATTEMPTED_AT + timedelta(minutes=1),
        "read_at": None,
        "item_message_id": "wamid.1",
        "attempt_message_id": "wamid.1",
        "attempt_sender_digest": SENDER_DIGEST,
        "attempt_recipient_digest": "r" * 64,
        "confirmed_failures": (),
    }
    base.update(changes)
    return SourceEvidence(**base)


def test_read_outranks_delivered_and_a_late_failure() -> None:
    """Read is the strongest evidence and nothing demotes it."""
    failure = FailureFact(
        match_state=DELIVERY_FAILURE_MATCH_CONFIRMED,
        failure_class=DELIVERY_FAILURE_CLASS_TRANSIENT,
        meta_error_code=131016,
        reason_code=DELIVERY_FAILURE_REASON_CALLBACK_FAILED,
    )
    verdict = resolve_group(_evidence(read_at=ATTEMPTED_AT + timedelta(minutes=2), confirmed_failures=(failure,)))
    assert verdict.group == FOLLOWUP_GROUP_READ


def test_delivered_outranks_a_contradictory_late_failure() -> None:
    """Somebody whose device showed the voucher did not have a failed send."""
    failure = FailureFact(
        match_state=DELIVERY_FAILURE_MATCH_CONFIRMED,
        failure_class=DELIVERY_FAILURE_CLASS_TRANSIENT,
        meta_error_code=131016,
        reason_code=DELIVERY_FAILURE_REASON_CALLBACK_FAILED,
    )
    verdict = resolve_group(_evidence(confirmed_failures=(failure,)))
    assert verdict.group == FOLLOWUP_GROUP_DELIVERED


@pytest.mark.parametrize(
    "changes",
    [
        {"delivered_at": None, "read_at": None},
        {"delivered_at": None, "status": VOUCHER_PRODUCTION_ITEM_SEND_UNKNOWN},
        {"delivered_at": None, "status": VOUCHER_PRODUCTION_ITEM_SEND_REJECTED},
        {"delivered_at": None, "status": VOUCHER_PRODUCTION_ITEM_PROVIDER_ACCEPTED},
    ],
)
def test_nothing_without_evidence_becomes_unread(changes: dict[str, Any]) -> None:
    """Accepted-only, unknown and a local refusal are not a group."""
    verdict = resolve_group(_evidence(**changes))
    assert verdict.group is None
    assert verdict.reason == EVIDENCE_UNPROVEN


@pytest.mark.parametrize(
    "changes,expected",
    [
        ({"send_attempted_at": None, "send_attempt_count": 0}, SOURCE_ATTEMPT_UNPROVEN),
        ({"send_attempt_count": 0}, SOURCE_ATTEMPT_UNPROVEN),
        ({"attempt_recipient_digest": None}, SOURCE_ATTEMPT_UNPROVEN),
        ({"attempt_sender_digest": None}, SOURCE_ATTEMPT_UNPROVEN),
        ({"attempt_message_id": "wamid.other"}, SOURCE_ATTEMPT_UNPROVEN),
    ],
)
def test_an_unproven_attempt_has_no_group(changes: dict[str, Any], expected: str) -> None:
    """No attempt, no frozen route, or two message ids: no group, with a reason."""
    verdict = resolve_group(_evidence(**changes))
    assert (verdict.group, verdict.reason) == (None, expected)


def test_a_foreign_sender_is_refused_by_name() -> None:
    """A slot sent from another line is not part of this send geography."""
    verdict = resolve_group(_evidence(attempt_sender_digest="z" * 64), expected_sender_digest=SENDER_DIGEST)
    assert (verdict.group, verdict.reason) == (None, SENDER_UNPROVEN)


# ---------------------------------------------------------------------------
# The failure admissibility policy (§46.3)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "fact",
    [
        FailureFact(DELIVERY_FAILURE_MATCH_CONFIRMED, DELIVERY_FAILURE_CLASS_TRANSIENT, 131016, "meta_callback_failed"),
        FailureFact(DELIVERY_FAILURE_MATCH_CONFIRMED, DELIVERY_FAILURE_CLASS_PERMANENT, 131026, "meta_api_rejected"),
        FailureFact(DELIVERY_FAILURE_MATCH_CONFIRMED, DELIVERY_FAILURE_CLASS_UNKNOWN, None, "meta_api_rejected"),
        FailureFact(DELIVERY_FAILURE_MATCH_UNMATCHED, DELIVERY_FAILURE_CLASS_TRANSIENT, 131016, "meta_callback_failed"),
        FailureFact(
            DELIVERY_FAILURE_MATCH_CONFLICTING, DELIVERY_FAILURE_CLASS_TRANSIENT, 131016, "meta_callback_failed"
        ),
        FailureFact(DELIVERY_FAILURE_MATCH_UNPROVEN, DELIVERY_FAILURE_CLASS_TRANSIENT, 131016, "meta_callback_failed"),
        None,
    ],
)
def test_the_shipped_policy_admits_nothing(fact: FailureFact | None) -> None:
    """v1 is a decision not to decide, and it says so for every input."""
    assert not CURRENT_FAILURE_POLICY.admits_anything
    assert CURRENT_FAILURE_POLICY.refusal_for(fact) == FAILURE_POLICY_UNPROVEN


def test_no_policy_version_may_admit_permanent_or_unknown() -> None:
    """The floor is enforced at construction, so a lowered policy cannot ship."""
    assert ADMISSIBLE_CLASS_FLOOR == frozenset({DELIVERY_FAILURE_CLASS_TRANSIENT})
    for bad in (DELIVERY_FAILURE_CLASS_PERMANENT, DELIVERY_FAILURE_CLASS_UNKNOWN):
        with pytest.raises(ValueError):
            FailurePolicy(version="bad", admissible_classes=frozenset({bad}))


def test_no_policy_version_may_admit_the_unspecific_zero_code() -> None:
    """``0`` identifies nothing a policy could reason about."""
    assert 0 not in ADMISSIBLE_CODE_FLOOR
    with pytest.raises(ValueError):
        FailurePolicy(
            version="bad",
            admissible_classes=frozenset({DELIVERY_FAILURE_CLASS_TRANSIENT}),
            admissible_meta_error_codes=frozenset({0}),
            min_wait_after_attempt_seconds=THIRTY_DAYS,
        )


def test_a_policy_that_admits_anything_must_state_a_waiting_time() -> None:
    """§46.3 forbids inventing one, so the absence is an error, not a zero."""
    with pytest.raises(ValueError):
        FailurePolicy(
            version="bad",
            admissible_classes=frozenset({DELIVERY_FAILURE_CLASS_TRANSIENT}),
            admissible_meta_error_codes=frozenset({131016}),
        )


def test_a_future_policy_takes_the_longer_of_the_two_floors() -> None:
    """Neither the operator's interval nor the policy's wait softens the other."""
    policy = FailurePolicy(
        version="v2",
        admissible_classes=frozenset({DELIVERY_FAILURE_CLASS_TRANSIENT}),
        admissible_meta_error_codes=frozenset({131016}),
        min_wait_after_attempt_seconds=THIRTY_DAYS,
    )
    assert policy.required_interval_seconds(FOURTEEN_DAYS) == THIRTY_DAYS
    assert policy.required_interval_seconds(60 * THIRTY_DAYS) == 60 * THIRTY_DAYS


# ---------------------------------------------------------------------------
# The message contract (§46.4)
# ---------------------------------------------------------------------------


def test_the_follow_up_has_its_own_contract_and_does_not_touch_the_first() -> None:
    from altegio_bot.campaigns.easyweek_voucher_production import template_contract as original

    assert message.FOLLOWUP_TEMPLATE_CODE != original.VOUCHER_TEMPLATE_CODE
    assert message.FOLLOWUP_META_TEMPLATE_NAME != original.VOUCHER_META_TEMPLATE_NAME
    assert message.FOLLOWUP_TEMPLATE_BODY != original.VOUCHER_TEMPLATE_BODY


def test_the_text_is_safe_for_the_failure_group() -> None:
    """It may not claim the first message was received or read."""
    body = message.FOLLOWUP_TEMPLATE_BODY.lower()
    for claim in ("geschrieben", "gesendet", "erhalten", "gelesen", "nachricht von uns"):
        assert claim not in body, claim
    # And every group maps to this one explicitly: no implicit fallback.
    assert set(message.GROUP_TEMPLATE_CODES) == {
        FOLLOWUP_GROUP_READ,
        FOLLOWUP_GROUP_DELIVERED,
        FOLLOWUP_GROUP_FAILED,
    }
    assert message.template_code_for_groups([FOLLOWUP_GROUP_FAILED]) == message.FOLLOWUP_TEMPLATE_CODE
    assert message.template_code_for_groups([]) is None


def test_the_text_explains_how_to_unsubscribe() -> None:
    assert "STOP" in message.FOLLOWUP_TEMPLATE_BODY
    assert "СТОП" in message.FOLLOWUP_TEMPLATE_BODY


def test_the_safe_example_leaks_nothing() -> None:
    example = message.safe_example()
    assert "[Vorname]" in example and "[Buchungslink]" in example
    assert "{" not in example and "}" not in example
    # No parameter name survives as a value, and no code-shaped string appears.
    for forbidden in ("+49", "wamid", "voucher_code"):
        assert forbidden not in example


@pytest.mark.parametrize(
    "changes",
    [
        {"status": "PENDING"},
        {"category": "UTILITY"},
        {"language": "ru"},
        {"parameter_format": None},
        {"components": [{"type": "BODY", "text": "etwas anderes"}]},
        {"components": [{"type": "HEADER", "text": "x"}, {"type": "BODY", "text": message.positional_body()}]},
        {"components": [{"type": "BODY", "text": message.positional_body() + " {{3}}"}]},
    ],
)
def test_a_meta_template_that_differs_at_all_is_unproven(changes: dict[str, Any]) -> None:
    from altegio_bot.tests.easyweek_voucher_followup_fixtures import approved_meta_templates

    listing = approved_meta_templates()
    listing[0].update(changes)
    proof = message.prove_meta_templates(listing)
    assert (proof.proven, proof.reason) == (False, TEMPLATE_UNPROVEN)


def test_two_matching_meta_rows_are_an_ambiguity_not_a_choice() -> None:
    from altegio_bot.tests.easyweek_voucher_followup_fixtures import approved_meta_templates

    listing = approved_meta_templates() + approved_meta_templates()
    assert message.prove_meta_templates(listing).proven is False


def test_an_empty_meta_listing_is_unproven() -> None:
    assert message.prove_meta_templates([]).proven is False


# ---------------------------------------------------------------------------
# The request's own parameters (§46.3)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("interval", [0, -1, -FOURTEEN_DAYS])
def test_a_missing_or_negative_interval_forbids_the_preview(interval: int) -> None:
    spec = preview_module.PreviewSpec(
        batch_id=1, groups=(FOLLOWUP_GROUP_READ,), min_interval_seconds=interval, operator_principal="o"
    )
    assert preview_module.spec_blocker(spec) == INTERVAL_REQUIRED


def test_an_empty_group_selection_forbids_the_preview() -> None:
    spec = preview_module.PreviewSpec(batch_id=1, groups=(), min_interval_seconds=FOURTEEN_DAYS, operator_principal="o")
    assert preview_module.spec_blocker(spec) == GROUPS_REQUIRED


def test_a_non_canonical_group_tuple_is_refused_rather_than_repaired() -> None:
    """The caller canonicalises; the contract does not silently accept noise."""
    spec = preview_module.PreviewSpec(
        batch_id=1,
        groups=(FOLLOWUP_GROUP_FAILED, FOLLOWUP_GROUP_READ),
        min_interval_seconds=FOURTEEN_DAYS,
        operator_principal="o",
    )
    assert preview_module.spec_blocker(spec) == GROUPS_REQUIRED


# ---------------------------------------------------------------------------
# The source batch gate (§46.1)
# ---------------------------------------------------------------------------


async def test_a_delivered_recipient_is_included(session_maker, binding_key, no_network) -> None:
    """The ordinary good case, so every refusal below means something."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    outcome = await build(session_maker, batch_id=batch_id)
    assert outcome.created is True
    assert (outcome.included, outcome.excluded, outcome.unknown) == (1, 0, 0)
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED
    assert reason_for(outcome, 1) is None


async def test_the_schema_of_the_historical_15_eur_contract_is_refused(session_maker, binding_key, no_network) -> None:
    """Schemas 1 and 2 keep their own older rules and are not reinterpreted."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}], schema_version="2")
    outcome = await build(session_maker, batch_id=batch_id)
    assert (outcome.created, outcome.reason) == (False, BATCH_SCHEMA_UNSUPPORTED)


async def test_a_manually_composed_batch_is_refused_by_name(session_maker, binding_key, no_network) -> None:
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(
        session_maker, slots=[{"state": "delivered"}], recipient_basis=RECIPIENT_BASIS_MANUAL
    )
    outcome = await build(session_maker, batch_id=batch_id)
    assert (outcome.created, outcome.reason) == (False, BATCH_SCOPE_UNSUPPORTED)


async def test_a_terminal_stop_forbids_creating_the_preview(session_maker, binding_key, no_network) -> None:
    """Checked on the server, not by hiding a button."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    await production_ledger.request_stop(session_maker, batch_id=batch_id, requested_by="operator")
    outcome = await build(session_maker, batch_id=batch_id)
    assert (outcome.created, outcome.reason) == (False, BATCH_TERMINAL_STOP)
    async with session_maker() as session:
        stored = (await session.execute(select(func.count()).select_from(EasyWeekVoucherFollowupPreview))).scalar_one()
    assert stored == 0


async def test_an_unproven_template_blocks_the_whole_preview(session_maker, binding_key, no_network) -> None:
    """No approved text, no proposal — and no free WhatsApp text instead."""
    await seed_sender_and_template(session_maker, with_followup_template=False)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    outcome = await build(session_maker, batch_id=batch_id)
    assert (outcome.created, outcome.reason) == (False, TEMPLATE_UNPROVEN)


async def test_a_local_row_alone_does_not_prove_the_template(session_maker, binding_key, no_network) -> None:
    """The live Meta read must show APPROVED in THIS evaluation."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    outcome = await build(session_maker, batch_id=batch_id, templates=TemplateReader([]))
    assert (outcome.created, outcome.reason) == (False, TEMPLATE_UNPROVEN)


async def test_a_batch_without_an_active_sender_is_refused(session_maker, binding_key, no_network) -> None:
    await seed_sender_and_template(session_maker, with_followup_template=True)
    async with session_maker() as session:
        async with session.begin():
            from altegio_bot.models.models import WhatsAppSender

            sender = (await session.execute(select(WhatsAppSender))).scalar_one()
            sender.is_active = False
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    outcome = await build(session_maker, batch_id=batch_id)
    assert (outcome.created, outcome.reason) == (False, SENDER_UNPROVEN)


# ---------------------------------------------------------------------------
# Groups and the interval, end to end
# ---------------------------------------------------------------------------


async def test_a_proven_group_the_operator_did_not_select_is_out_of_scope(
    session_maker, binding_key, no_network
) -> None:
    """Not excluded — simply not part of this preview."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(
        session_maker, slots=[{"state": "read", "index": 1}, {"state": "delivered", "index": 2}]
    )
    outcome = await build(session_maker, batch_id=batch_id, groups=(FOLLOWUP_GROUP_READ,))
    slots = {row.slot for row in outcome.rows}
    assert slots == {1}
    assert outcome.included == 1


async def test_an_unprovable_slot_is_reported_as_unknown_whatever_was_selected(
    session_maker, binding_key, no_network
) -> None:
    """§46.6 asks the page to account for the whole original mailing."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(
        session_maker,
        slots=[{"state": "delivered", "index": 1}, {"attempted": False, "index": 2}],
    )
    outcome = await build(session_maker, batch_id=batch_id, groups=(FOLLOWUP_GROUP_DELIVERED,))
    assert decision_for(outcome, 2) == FOLLOWUP_ROW_UNKNOWN
    assert reason_for(outcome, 2) == SOURCE_ATTEMPT_UNPROVEN
    assert outcome.unknown == 1
    async with session_maker() as session:
        row = (
            await session.execute(
                select(EasyWeekVoucherFollowupPreviewRow).where(EasyWeekVoucherFollowupPreviewRow.source_slot == 2)
            )
        ).scalar_one()
        assert row.group_code == FOLLOWUP_GROUP_UNPROVEN
        # No attempt row at all, so there is no frozen route to record.
        assert row.recipient_digest is None


async def test_the_interval_is_measured_from_the_proven_attempt(session_maker, binding_key, no_network) -> None:
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    # One second before the boundary.
    just_short = ATTEMPTED_AT + timedelta(seconds=FOURTEEN_DAYS - 1)
    outcome = await build(session_maker, batch_id=batch_id, now=just_short)
    assert reason_for(outcome, 1) == INTERVAL_NOT_ELAPSED
    # And exactly at it: the operator's number means what it says.
    exact = ATTEMPTED_AT + timedelta(seconds=FOURTEEN_DAYS)
    outcome = await build(session_maker, batch_id=batch_id, now=exact)
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED


async def test_a_later_callback_time_is_not_the_interval_anchor(session_maker, binding_key, no_network) -> None:
    """A delivery receipt much later must not restart the clock."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(
        session_maker,
        slots=[{"state": "delivered", "delivered_at": ATTEMPTED_AT + timedelta(days=13)}],
    )
    exact = ATTEMPTED_AT + timedelta(seconds=FOURTEEN_DAYS)
    outcome = await build(session_maker, batch_id=batch_id, now=exact)
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED


# ---------------------------------------------------------------------------
# The confirmed-failure group, end to end
# ---------------------------------------------------------------------------


async def _add_failure(
    session_maker,
    *,
    item_slot: int,
    match_state: str = DELIVERY_FAILURE_MATCH_CONFIRMED,
    failure_class: str = DELIVERY_FAILURE_CLASS_TRANSIENT,
    meta_error_code: int | None = 131016,
    source: str = DELIVERY_FAILURE_SOURCE_CALLBACK,
    fact_key: str = "fact-1",
) -> None:
    async with session_maker() as session:
        async with session.begin():
            item = (
                await session.execute(
                    select(EasyWeekVoucherProductionBatchItem).where(
                        EasyWeekVoucherProductionBatchItem.slot == item_slot
                    )
                )
            ).scalar_one()
            session.add(
                EasyWeekVoucherProductionDeliveryFailure(
                    fact_key=fact_key,
                    source=source,
                    reason_code=(
                        DELIVERY_FAILURE_REASON_CALLBACK_FAILED
                        if source == DELIVERY_FAILURE_SOURCE_CALLBACK
                        else DELIVERY_FAILURE_REASON_API_REJECTED
                    ),
                    match_state=match_state,
                    failure_class=failure_class,
                    authenticity=INBOUND_AUTHENTICITY_SIGNED,
                    item_id=int(item.id),
                    batch_id=int(item.batch_id),
                    slot=int(item.slot),
                    intent_uuid=item.outbound_intent_uuid,
                    provider_message_id=item.provider_message_id,
                    observed_sender_digest=SENDER_DIGEST,
                    observed_recipient_digest="r" * 64,
                    route_proven=True,
                    meta_error_code=meta_error_code,
                    observed_at=ATTEMPTED_AT + timedelta(minutes=1),
                    evidence={},
                )
            )


async def test_a_confirmed_failure_is_excluded_while_no_policy_admits_it(
    session_maker, binding_key, no_network
) -> None:
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "accepted"}])
    await _add_failure(session_maker, item_slot=1)
    outcome = await build(session_maker, batch_id=batch_id, groups=(FOLLOWUP_GROUP_FAILED,))
    assert outcome.created is True
    assert reason_for(outcome, 1) == FAILURE_POLICY_UNPROVEN
    assert outcome.included == 0


@pytest.mark.parametrize(
    "match_state",
    [
        DELIVERY_FAILURE_MATCH_UNMATCHED,
        DELIVERY_FAILURE_MATCH_CONFLICTING,
        DELIVERY_FAILURE_MATCH_UNPROVEN,
    ],
)
async def test_only_a_confirmed_fact_reaches_the_failure_group(
    session_maker, binding_key, no_network, match_state: str
) -> None:
    """An unmatched, conflicting or unproven fact says nothing about this slot."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "accepted"}])
    if match_state != DELIVERY_FAILURE_MATCH_UNMATCHED:
        await _add_failure(session_maker, item_slot=1, match_state=match_state)
    outcome = await build(session_maker, batch_id=batch_id, groups=(FOLLOWUP_GROUP_FAILED,))
    assert reason_for(outcome, 1) == EVIDENCE_UNPROVEN


async def test_one_inadmissible_reason_among_several_vetoes_the_row(session_maker, binding_key) -> None:
    """A permanent block hiding behind a transient retry is still a block."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "accepted"}])
    await _add_failure(session_maker, item_slot=1, fact_key="fact-a")
    await _add_failure(
        session_maker,
        item_slot=1,
        failure_class=DELIVERY_FAILURE_CLASS_PERMANENT,
        meta_error_code=131026,
        source=DELIVERY_FAILURE_SOURCE_META_API,
        fact_key="fact-b",
    )
    permissive = FailurePolicy(
        version="test-v2",
        admissible_classes=frozenset({DELIVERY_FAILURE_CLASS_TRANSIENT}),
        admissible_meta_error_codes=frozenset({131016}),
        min_wait_after_attempt_seconds=60,
    )
    outcome = await build(session_maker, batch_id=batch_id, groups=(FOLLOWUP_GROUP_FAILED,), policy=permissive)
    assert reason_for(outcome, 1) == FAILURE_POLICY_UNPROVEN


async def test_a_proven_policy_can_admit_a_named_transient_reason(session_maker, binding_key) -> None:
    """The mechanism works, so v1's zero is a decision and not a dead branch."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "accepted"}])
    await _add_failure(session_maker, item_slot=1)
    permissive = FailurePolicy(
        version="test-v2",
        admissible_classes=frozenset({DELIVERY_FAILURE_CLASS_TRANSIENT}),
        admissible_meta_error_codes=frozenset({131016}),
        min_wait_after_attempt_seconds=60,
    )
    outcome = await build(session_maker, batch_id=batch_id, groups=(FOLLOWUP_GROUP_FAILED,), policy=permissive)
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED
    async with session_maker() as session:
        stored = (await session.execute(select(EasyWeekVoucherFollowupPreview))).scalar_one()
        assert stored.failure_policy_version == "test-v2"


# ---------------------------------------------------------------------------
# Consent (§46.4)
# ---------------------------------------------------------------------------


async def _suppress(session_maker, *, address: str, uuid: str | None = None) -> None:
    async with session_maker() as session:
        async with session.begin():
            session.add(
                WhatsAppConsentSuppression(
                    wa_address=address,
                    state=CONSENT_STATE_OPTED_OUT,
                    consent_version=1,
                    decision_command=CONSENT_COMMAND_STOP,
                    decision_source=CONSENT_SOURCE_INBOUND,
                    decision_authenticity=INBOUND_AUTHENTICITY_SIGNED,
                    decided_observed_at=ATTEMPTED_AT + timedelta(days=1),
                    decided_event_at=ATTEMPTED_AT + timedelta(days=1),
                    opted_out_at=ATTEMPTED_AT + timedelta(days=1),
                    banned_easyweek_customer_uuid=uuid_module.UUID(uuid) if uuid else None,
                    banned_uuid_recorded_at=(ATTEMPTED_AT + timedelta(days=1)) if uuid else None,
                )
            )


async def test_a_proven_stop_excludes_the_recipient(session_maker, binding_key, no_network) -> None:
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    await _suppress(session_maker, address=person(1)["phone"])
    outcome = await build(session_maker, batch_id=batch_id)
    assert reason_for(outcome, 1) == CONSENT_REFUSED


async def test_a_stop_bound_to_the_uuid_excludes_even_from_another_number(
    session_maker, binding_key, no_network
) -> None:
    """A refusal is the person's, not the number's."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    await _suppress(session_maker, address="+4915100000000", uuid=person(1)["customer_uuid"])
    outcome = await build(session_maker, batch_id=batch_id)
    assert reason_for(outcome, 1) == CONSENT_REFUSED


async def test_a_card_flag_on_another_branch_still_excludes(session_maker, binding_key, no_network) -> None:
    """``wa_opted_out`` on ANY matching card, including an Altegio one."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    async with session_maker() as session:
        async with session.begin():
            session.add(
                Client(
                    company_id=999,
                    altegio_client_id=4242,
                    display_name="Same person, Altegio card",
                    phone_e164=person(1)["phone"],
                    raw={},
                    wa_opted_out=True,
                )
            )
    outcome = await build(session_maker, batch_id=batch_id)
    assert reason_for(outcome, 1) == CONSENT_REFUSED


async def test_pending_inbound_holds_the_recipient(session_maker, binding_key, no_network) -> None:
    """A busy worker queue is not a reason to write to somebody."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    async with session_maker() as session:
        async with session.begin():
            session.add(
                WhatsAppInboundReceipt(
                    receipt_key="receipt-pending-1",
                    message_index=0,
                    wa_address=person(1)["phone"],
                    command=CONSENT_COMMAND_NONE,
                    authenticity=INBOUND_AUTHENTICITY_SIGNED,
                    route_allowed=True,
                    observed_at=ATTEMPTED_AT - timedelta(days=1),
                    resolved_at=None,
                )
            )
    outcome = await build(session_maker, batch_id=batch_id)
    assert reason_for(outcome, 1) == CONSENT_PENDING_INBOUND


async def test_an_unnormalisable_number_is_unknown_not_permission(session_maker, binding_key, no_network) -> None:
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    async with session_maker() as session:
        async with session.begin():
            from altegio_bot.models.models import CampaignRecipient

            recipient = (await session.execute(select(CampaignRecipient))).scalar_one()
            recipient.phone_e164 = "nonsense"
    outcome = await build(session_maker, batch_id=batch_id)
    # The identity check catches it first, and the answer is still not "allowed".
    assert reason_for(outcome, 1) in {IDENTITY_CONFLICT, CONSENT_UNKNOWN}
    assert decision_for(outcome, 1) != FOLLOWUP_ROW_INCLUDED


async def test_a_missing_ledger_row_with_a_stored_card_flag_still_excludes(
    session_maker, binding_key, no_network
) -> None:
    """The historical opt-out that never reached the suppression table."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered", "card_opted_out": True}])
    async with session_maker() as session:
        suppressions = (
            await session.execute(select(func.count()).select_from(WhatsAppConsentSuppression))
        ).scalar_one()
    assert suppressions == 0
    outcome = await build(session_maker, batch_id=batch_id)
    assert reason_for(outcome, 1) == CONSENT_REFUSED


# ---------------------------------------------------------------------------
# Replies (§46.5)
# ---------------------------------------------------------------------------


async def _receipt(
    session_maker,
    *,
    address: str,
    key: str,
    observed: datetime,
    authenticity: str = INBOUND_AUTHENTICITY_SIGNED,
    route_allowed: bool | None = True,
    resolved: bool = True,
) -> None:
    async with session_maker() as session:
        async with session.begin():
            session.add(
                WhatsAppInboundReceipt(
                    receipt_key=key,
                    message_index=0,
                    wa_address=address,
                    command=CONSENT_COMMAND_NONE,
                    authenticity=authenticity,
                    route_allowed=route_allowed,
                    observed_at=observed,
                    resolved_at=observed if resolved else None,
                    resolution="noop" if resolved else None,
                )
            )


async def test_a_proven_reply_after_the_attempt_excludes(session_maker, binding_key, no_network) -> None:
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    await _receipt(
        session_maker,
        address=person(1)["phone"],
        key="reply-1",
        observed=ATTEMPTED_AT + timedelta(hours=2),
    )
    outcome = await build(session_maker, batch_id=batch_id)
    assert reason_for(outcome, 1) == CUSTOMER_REPLIED


async def test_a_reply_before_the_attempt_does_not_exclude(session_maker, binding_key, no_network) -> None:
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    await _receipt(
        session_maker,
        address=person(1)["phone"],
        key="reply-old",
        observed=ATTEMPTED_AT - timedelta(days=3),
    )
    outcome = await build(session_maker, batch_id=batch_id)
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED


@pytest.mark.parametrize(
    "changes",
    [
        {"authenticity": INBOUND_AUTHENTICITY_UNVERIFIED},
        {"route_allowed": False, "resolved": True},
    ],
)
async def test_an_unproven_or_foreign_reply_is_not_a_reply(
    session_maker, binding_key, no_network, changes: dict[str, Any]
) -> None:
    """An unsigned delivery and a refused sender route are not this conversation."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    await _receipt(
        session_maker,
        address=person(1)["phone"],
        key="reply-weak",
        observed=ATTEMPTED_AT + timedelta(hours=2),
        **changes,
    )
    outcome = await build(session_maker, batch_id=batch_id)
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED


async def test_a_chatwoot_mirror_produces_no_receipt_and_no_reply(session_maker, binding_key, no_network) -> None:
    """Only the Meta ingress writes receipts, which is why a mirror cannot count."""
    from altegio_bot.models.models import WhatsAppEvent
    from altegio_bot.whatsapp_consent import is_chatwoot_sourced

    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    for dedupe_key, payload in (
        ("chatwoot:1", {}),
        ("chatwoot_out:1", {}),
        ("meta:1", {"_chatwoot": {"conversation_id": 1}}),
        ("meta:2", {"_chatwoot_operator_relay": {"by": "operator"}}),
    ):
        event = WhatsAppEvent(dedupe_key=dedupe_key, payload=payload, query={}, headers={}, status="processed")
        assert is_chatwoot_sourced(event, payload) is True
    outcome = await build(session_maker, batch_id=batch_id)
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED


# ---------------------------------------------------------------------------
# Bookings (§46.5)
# ---------------------------------------------------------------------------


async def test_a_new_booking_after_the_attempt_excludes_even_when_cancelled(
    session_maker, binding_key, no_network
) -> None:
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    reader = FakeHistoryReader(
        bookings={
            who["customer_uuid"]: [
                booking(index=1, customer_uuid=who["customer_uuid"], start=ATTEMPTED_AT - timedelta(days=5)),
                booking(
                    index=2,
                    customer_uuid=who["customer_uuid"],
                    start=ATTEMPTED_AT + timedelta(days=2),
                    canceled=True,
                    completed=False,
                ),
            ]
        }
    )
    outcome = await build(session_maker, batch_id=batch_id, reader=reader)
    assert reason_for(outcome, 1) == NEW_BOOKING_AFTER_ATTEMPT


async def test_an_active_future_booking_excludes(session_maker, binding_key, no_network) -> None:
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    reader = FakeHistoryReader(
        bookings={
            who["customer_uuid"]: [
                booking(
                    index=3,
                    customer_uuid=who["customer_uuid"],
                    start=NOW + timedelta(days=3),
                    completed=False,
                )
            ]
        }
    )
    outcome = await build(session_maker, batch_id=batch_id, reader=reader)
    assert reason_for(outcome, 1) == FUTURE_BOOKING_ACTIVE


async def test_the_earning_visit_alone_does_not_exclude(session_maker, binding_key, no_network) -> None:
    """§46.5 is explicit that the past visit must not read as a reason to stay silent."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    reader = FakeHistoryReader(
        bookings={
            who["customer_uuid"]: [
                booking(index=1, customer_uuid=who["customer_uuid"], start=ATTEMPTED_AT - timedelta(days=10))
            ]
        }
    )
    outcome = await build(session_maker, batch_id=batch_id, reader=reader)
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED


async def test_a_second_completed_visit_does_not_fail_the_proof(session_maker, binding_key, no_network) -> None:
    """The PR-15 'exactly one completed visit' rule must not leak in here.

    Two completed visits BEFORE the attempt is an ordinary history for somebody
    who earned a voucher and came back before the mailing went out. The read is
    shared with PR-15; its verdict is not.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    reader = FakeHistoryReader(
        bookings={
            who["customer_uuid"]: [
                booking(index=1, customer_uuid=who["customer_uuid"], start=ATTEMPTED_AT - timedelta(days=30)),
                booking(index=2, customer_uuid=who["customer_uuid"], start=ATTEMPTED_AT - timedelta(days=10)),
            ]
        }
    )
    outcome = await build(session_maker, batch_id=batch_id, reader=reader)
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED


async def test_incomplete_pagination_is_unknown_never_an_empty_history(session_maker, binding_key, no_network) -> None:
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    reader = FakeHistoryReader(bookings={person(1)["customer_uuid"]: []}, truncate=True)
    outcome = await build(session_maker, batch_id=batch_id, reader=reader)
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
    assert reason_for(outcome, 1) == HISTORY_UNPROVEN


async def test_a_timeout_is_unknown_and_reported_as_retryable(session_maker, binding_key, no_network) -> None:
    from altegio_bot.easyweek_client import EasyWeekRetryableError

    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    reader = FakeHistoryReader(history_error=EasyWeekRetryableError("timeout", operation="list_customer_bookings"))
    outcome = await build(session_maker, batch_id=batch_id, reader=reader)
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
    assert outcome.retryable_uncertainty is True


async def test_a_card_that_resolves_to_somebody_else_is_unknown(session_maker, binding_key, no_network) -> None:
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    reader = FakeHistoryReader(wrong_customer="99999999-1111-4111-8111-111111111111")
    outcome = await build(session_maker, batch_id=batch_id, reader=reader)
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN


async def test_a_missing_booking_reader_is_unknown_not_an_empty_history(session_maker, binding_key, no_network) -> None:
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    outcome = await preview_module.create_preview(
        session_maker,
        spec=preview_module.PreviewSpec(
            batch_id=batch_id,
            groups=(FOLLOWUP_GROUP_DELIVERED,),
            min_interval_seconds=FOURTEEN_DAYS,
            operator_principal="o",
        ),
        booking_reader=None,
        template_reader=TemplateReader(),
        now=NOW,
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
    assert reason_for(outcome, 1) == HISTORY_UNPROVEN


async def test_the_booking_read_is_not_spent_on_a_row_already_refused(session_maker, binding_key, no_network) -> None:
    """The external step is the last one, so a local refusal costs no request."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    await _suppress(session_maker, address=person(1)["phone"])
    reader = FakeHistoryReader()
    outcome = await build(session_maker, batch_id=batch_id, reader=reader)
    assert reason_for(outcome, 1) == CONSENT_REFUSED
    assert reader.customer_calls == []
    assert reader.history_calls == []
    assert outcome.booking_reads == 0


# ---------------------------------------------------------------------------
# Identity and the original entitlement (§46.1, §46.4)
# ---------------------------------------------------------------------------


async def test_a_number_that_moved_to_another_card_is_an_identity_conflict(
    session_maker, binding_key, no_network
) -> None:
    """§46.1: the addressee is not whoever owns a matching number now."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    async with session_maker() as session:
        async with session.begin():
            card = (
                await session.execute(select(Client).where(Client.easyweek_customer_uuid.is_not(None)).limit(1))
            ).scalar_one()
            card.phone_e164 = "+4915100099999"
    outcome = await build(session_maker, batch_id=batch_id)
    assert reason_for(outcome, 1) == IDENTITY_CONFLICT


async def test_a_changed_campaign_number_breaks_the_frozen_route(session_maker, binding_key, no_network) -> None:
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    async with session_maker() as session:
        async with session.begin():
            from altegio_bot.models.models import CampaignRecipient

            recipient = (await session.execute(select(CampaignRecipient))).scalar_one()
            recipient.phone_e164 = "+4915100088888"
            card = (
                await session.execute(select(Client).where(Client.easyweek_customer_uuid.is_not(None)).limit(1))
            ).scalar_one()
            card.phone_e164 = "+4915100088888"
    outcome = await build(session_maker, batch_id=batch_id)
    assert reason_for(outcome, 1) == IDENTITY_CONFLICT


async def test_a_changed_customer_uuid_is_an_identity_conflict(session_maker, binding_key, no_network) -> None:
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    async with session_maker() as session:
        async with session.begin():
            card = (
                await session.execute(select(Client).where(Client.easyweek_customer_uuid.is_not(None)).limit(1))
            ).scalar_one()
            card.easyweek_customer_uuid = uuid_module.UUID("77777777-1111-4111-8111-111111111111")
    outcome = await build(session_maker, batch_id=batch_id)
    assert reason_for(outcome, 1) == IDENTITY_CONFLICT


@pytest.mark.parametrize("status", sorted(ENTITLEMENT_INVALID_STATUSES))
def test_an_invalid_entitlement_status_excludes(status: str) -> None:
    """Refunded, cleaned by hand, or a refund in flight: all excluded.

    Asserted against the rule itself rather than through a seeded row, because
    §45's own CHECKs make most of these states unreachable for a slot that was
    sent at all — ``refunded`` requires no acceptance, no delivery and no read.
    That is a good thing and the reason this branch is defence in depth: it
    answers for a historical row written before those constraints existed, and
    for a state a later stage could introduce.
    """
    verdict = entitlement_verdict(
        batch_id=1,
        slot=1,
        status=status,
        voucher_code_hmac="a" * 64,
        hmac_key_id="test-key-1",
        target_order_uuid="00000001-3333-4333-8333-333333333333",
        configured_key_id="test-key-1",
    )
    assert (verdict.proven, verdict.reason) == (False, ENTITLEMENT_INVALID)


async def test_an_unsettled_slot_is_excluded_end_to_end(session_maker, binding_key, no_network) -> None:
    """The one invalid status a delivered slot can really hold."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(
        session_maker, slots=[{"state": "delivered", "status": VOUCHER_PRODUCTION_ITEM_AMBIGUOUS}]
    )
    outcome = await build(session_maker, batch_id=batch_id)
    assert reason_for(outcome, 1) == ENTITLEMENT_INVALID


async def test_a_rotated_mac_key_leaves_the_entitlement_unproven(session_maker, binding_key, no_network) -> None:
    """Nothing is wrong with the voucher; what is gone is the proof."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}], hmac_key_id="an-older-key")
    outcome = await build(session_maker, batch_id=batch_id)
    assert reason_for(outcome, 1) == ENTITLEMENT_UNPROVEN


@pytest.mark.parametrize(
    "changes",
    [
        {"voucher_code_hmac": None},
        {"voucher_code_hmac": ""},
        {"hmac_key_id": None},
        {"target_order_uuid": None},
        {"configured_key_id": None},
        {"hmac_key_id": "an-older-key"},
    ],
)
def test_a_binding_that_cannot_be_re_proven_is_unproven(changes: dict[str, Any]) -> None:
    """Never "invalid" — the voucher may be perfectly fine; the PROOF is gone."""
    kwargs: dict[str, Any] = {
        "batch_id": 1,
        "slot": 1,
        "status": "delivered",
        "voucher_code_hmac": "a" * 64,
        "hmac_key_id": "test-key-1",
        "target_order_uuid": "00000001-3333-4333-8333-333333333333",
        "configured_key_id": "test-key-1",
    }
    kwargs.update(changes)
    verdict = entitlement_verdict(**kwargs)
    assert (verdict.proven, verdict.reason) == (False, ENTITLEMENT_UNPROVEN)


async def test_a_voucher_with_no_individual_dates_is_not_forbidden(session_maker, binding_key, no_network) -> None:
    """§45.4: provider-managed validity, and silence is the ordinary shape.

    The fixtures store no activation or expiry anywhere, which is exactly the
    state of every real artifact under this contract. A preview that treated
    that as a new prohibition would contradict the section outright.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    outcome = await build(session_maker, batch_id=batch_id)
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED


# ---------------------------------------------------------------------------
# The snapshot itself (§46.5)
# ---------------------------------------------------------------------------


async def test_a_stored_snapshot_records_the_whole_contract(session_maker, binding_key, no_network) -> None:
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    outcome = await build(session_maker, batch_id=batch_id, operator="ops-one:fp1")
    async with session_maker() as session:
        stored = (await session.execute(select(EasyWeekVoucherFollowupPreview))).scalar_one()
    assert stored.version == 1
    assert tuple(stored.selected_groups) == (FOLLOWUP_GROUP_DELIVERED,)
    assert stored.min_interval_seconds == FOURTEEN_DAYS
    assert stored.contract_version == CONTRACT_VERSION
    assert stored.failure_policy_version == CURRENT_FAILURE_POLICY.version
    assert stored.template_code == message.FOLLOWUP_TEMPLATE_CODE
    assert tuple(stored.template_parameter_order) == message.FOLLOWUP_TEMPLATE_FIELDS
    assert stored.template_body_digest == message.body_digest()
    assert stored.template_meta_verified is True
    assert stored.operator_principal == "ops-one:fp1"
    assert stored.valid_until > stored.checked_at
    assert stored.composition_digest == outcome.composition_digest
    # And the source links are unambiguous, with no personal data among them.
    assert stored.source_frozen_digest == "f" * 64
    assert stored.stop_generation == 0


async def test_re_viewing_a_snapshot_changes_nothing(session_maker, binding_key, no_network) -> None:
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    await build(session_maker, batch_id=batch_id)
    async with session_maker() as session:
        first = await preview_module.latest_preview(session, batch_id=batch_id)
        second = await preview_module.latest_preview(session, batch_id=batch_id)
        count = (await session.execute(select(func.count()).select_from(EasyWeekVoucherFollowupPreview))).scalar_one()
    assert first is not None and second is not None
    assert (first.version, first.composition_digest) == (second.version, second.composition_digest)
    assert count == 1


async def test_a_recalculation_adds_a_version_and_leaves_the_old_one_alone(
    session_maker, binding_key, no_network
) -> None:
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    first = await build(session_maker, batch_id=batch_id)
    second = await build(session_maker, batch_id=batch_id, interval=THIRTY_DAYS)
    assert (first.version, second.version) == (1, 2)
    assert first.composition_digest != second.composition_digest
    async with session_maker() as session:
        old = await preview_module.latest_preview(session, batch_id=batch_id, version=1)
        current = await preview_module.latest_preview(session, batch_id=batch_id)
    assert old is not None and current is not None
    assert old.min_interval_seconds == FOURTEEN_DAYS
    assert old.composition_digest == first.composition_digest
    assert current.version == 2


@pytest.mark.parametrize(
    "changes",
    [
        {"groups": (FOLLOWUP_GROUP_READ, FOLLOWUP_GROUP_DELIVERED)},
        {"interval": THIRTY_DAYS},
    ],
)
async def test_changing_the_terms_changes_the_digest(
    session_maker, binding_key, no_network, changes: dict[str, Any]
) -> None:
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    base = await build(session_maker, batch_id=batch_id)
    other = await build(session_maker, batch_id=batch_id, **changes)
    assert base.composition_digest != other.composition_digest


async def test_the_digest_is_deterministic_for_the_same_composition(session_maker, binding_key, no_network) -> None:
    """Same people, same terms, same digest — whatever order they were read in."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(
        session_maker, slots=[{"state": "delivered", "index": 1}, {"state": "delivered", "index": 2}]
    )
    first = await build(session_maker, batch_id=batch_id)
    second = await build(session_maker, batch_id=batch_id)
    assert first.included == second.included == 2
    assert first.composition_digest == second.composition_digest
    assert first.version != second.version


async def test_a_policy_version_change_changes_the_digest(session_maker, binding_key, no_network) -> None:
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    base = await build(session_maker, batch_id=batch_id)
    renamed = FailurePolicy(version="followup-policy-v1b")
    other = await build(session_maker, batch_id=batch_id, policy=renamed)
    assert base.composition_digest != other.composition_digest


async def test_one_version_cannot_hold_the_same_person_twice(session_maker, binding_key, no_network) -> None:
    """The dedupe contract PR-22.3 will claim against, enforced by the database.

    Within one batch §45 already forbids two slots for one customer, so the
    duplicate this guards against cannot be produced by the preview as it
    stands. That is exactly why the guarantee belongs in the schema rather than
    in a loop: a later stage that composed a version from more than one source
    must not be able to write the same person in twice, and this proves
    PostgreSQL itself refuses it.
    """
    from sqlalchemy.exc import IntegrityError

    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    await build(session_maker, batch_id=batch_id)
    async with session_maker() as session:
        row = (await session.execute(select(EasyWeekVoucherFollowupPreviewRow))).scalar_one()
        clone = dict(
            preview_id=row.preview_id,
            source_batch_id=row.source_batch_id,
            source_item_id=row.source_item_id + 1000,
            source_slot=row.source_slot + 1000,
            campaign_recipient_id=row.campaign_recipient_id,
            easyweek_customer_uuid=row.easyweek_customer_uuid,
            source_entitlement_digest="e" * 64,
            recipient_digest=row.recipient_digest,
            source_attempted_at=row.source_attempted_at,
            group_code=row.group_code,
            decision=FOLLOWUP_ROW_INCLUDED,
            reason_code=None,
            evidence={},
        )
    async with session_maker() as session:
        with pytest.raises(IntegrityError):
            async with session.begin():
                session.add(EasyWeekVoucherFollowupPreviewRow(**clone))
    # And the same for one entitlement under two customers.
    clone["easyweek_customer_uuid"] = uuid_module.UUID("55555555-1111-4111-8111-111111111111")
    clone["source_entitlement_digest"] = row.source_entitlement_digest
    async with session_maker() as session:
        with pytest.raises(IntegrityError):
            async with session.begin():
                session.add(EasyWeekVoucherFollowupPreviewRow(**clone))


async def test_a_second_preview_does_not_block_the_same_recipient(session_maker, binding_key, no_network) -> None:
    """Dedupe is scoped to one version, so a repeat preview starts fresh."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    first = await build(session_maker, batch_id=batch_id)
    second = await build(session_maker, batch_id=batch_id)
    third = await build(session_maker, batch_id=batch_id)
    assert (first.included, second.included, third.included) == (1, 1, 1)


async def test_a_stale_snapshot_reports_itself_as_stale(session_maker, binding_key, no_network) -> None:
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    await build(session_maker, batch_id=batch_id)
    async with session_maker() as session:
        stored = await preview_module.latest_preview(session, batch_id=batch_id)
    assert stored is not None
    assert stored.is_fresh(now=NOW) is True
    assert stored.is_fresh(now=NOW + timedelta(days=2)) is False


# ---------------------------------------------------------------------------
# What a preview must never do (§46 change boundaries)
# ---------------------------------------------------------------------------


async def _side_effect_counters(session_maker) -> dict[str, Any]:
    """Everything a send would have moved, so a preview can be shown not to."""
    from altegio_bot.models.models import MessageJob, OutboxMessage

    async with session_maker() as session:
        items = list(
            (
                await session.execute(
                    select(
                        EasyWeekVoucherProductionBatchItem.slot,
                        EasyWeekVoucherProductionBatchItem.status,
                        EasyWeekVoucherProductionBatchItem.send_attempt_count,
                        EasyWeekVoucherProductionBatchItem.provider_message_id,
                        EasyWeekVoucherProductionBatchItem.voucher_code_hmac,
                        EasyWeekVoucherProductionBatchItem.hmac_key_id,
                        EasyWeekVoucherProductionBatchItem.target_order_uuid,
                        EasyWeekVoucherProductionBatchItem.delivered_at,
                        EasyWeekVoucherProductionBatchItem.read_at,
                        EasyWeekVoucherProductionBatchItem.updated_at,
                    ).order_by(EasyWeekVoucherProductionBatchItem.slot)
                )
            ).all()
        )
        jobs = (await session.execute(select(func.count()).select_from(MessageJob))).scalar_one()
        outbox = (await session.execute(select(func.count()).select_from(OutboxMessage))).scalar_one()
        attempts = (
            await session.execute(select(func.count()).select_from(EasyWeekVoucherProductionBatchAttempt))
        ).scalar_one()
    return {"items": items, "jobs": jobs, "outbox": outbox, "attempts": attempts}


@pytest.mark.parametrize(
    "scenario",
    ["included", "excluded_by_stop", "unknown_by_timeout", "refused_by_template", "repeated"],
)
async def test_no_preview_path_sends_spends_or_mutates_anything(
    session_maker, binding_key, no_network, scenario: str
) -> None:
    """Zero Meta sends, zero jobs, zero voucher mutations, zero spent attempts.

    Across every outcome a preview can have, including the error paths and a
    repeat, because the invariant is about the PATH and not about the verdict.
    ``no_network`` is armed, so any HTTP request at all — a send, a mutation, a
    stray template read — fails the test rather than being counted afterwards.
    """
    await seed_sender_and_template(session_maker, with_followup_template=scenario != "refused_by_template")
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    if scenario == "excluded_by_stop":
        await _suppress(session_maker, address=person(1)["phone"])

    before = await _side_effect_counters(session_maker)
    reader: Any = FakeHistoryReader()
    if scenario == "unknown_by_timeout":
        from altegio_bot.easyweek_client import EasyWeekRetryableError

        reader = FakeHistoryReader(history_error=EasyWeekRetryableError("t", operation="list_customer_bookings"))

    await build(session_maker, batch_id=batch_id, reader=reader)
    if scenario == "repeated":
        await build(session_maker, batch_id=batch_id, reader=reader)
        await build(session_maker, batch_id=batch_id, reader=reader)

    after = await _side_effect_counters(session_maker)
    assert after["items"] == before["items"]
    assert (after["jobs"], after["outbox"], after["attempts"]) == (
        before["jobs"],
        before["outbox"],
        before["attempts"],
    )


async def test_a_preview_does_not_touch_the_primary_ledger_or_its_stop(session_maker, binding_key, no_network) -> None:
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    async with session_maker() as session:
        before = (await session.execute(select(EasyWeekVoucherProductionBatch))).scalar_one()
        snapshot = (before.status, before.frozen_digest, before.recipient_count, before.updated_at)
    await build(session_maker, batch_id=batch_id)
    async with session_maker() as session:
        after = (await session.execute(select(EasyWeekVoucherProductionBatch))).scalar_one()
    assert (after.status, after.frozen_digest, after.recipient_count, after.updated_at) == snapshot
    assert await production_ledger.terminal_stop_active(session_maker, batch_id=batch_id) is False


# ---------------------------------------------------------------------------
# The re-check after the network (§46.5)
# ---------------------------------------------------------------------------


class StoppingReader(FakeHistoryReader):
    """A booking read during which the person says STOP.

    The one interleaving §46.5 names explicitly: the external read takes time,
    and consent is exactly the kind of fact that moves while it does. A preview
    that did not re-check would store this person as eligible on evidence that
    was already false by the time it committed.
    """

    def __init__(self, session_maker: Any, *, address: str) -> None:
        super().__init__(bookings={})
        self.session_maker = session_maker
        self.address = address

    async def list_customer_bookings(self, customer_uuid: str, page: int, per_page: int = 50) -> dict[str, Any]:
        await _suppress(self.session_maker, address=self.address)
        return await super().list_customer_bookings(customer_uuid, page, per_page)


async def test_a_stop_during_the_booking_read_is_caught_before_the_snapshot(
    session_maker, binding_key, no_network
) -> None:
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    reader = StoppingReader(session_maker, address=person(1)["phone"])
    outcome = await build(session_maker, batch_id=batch_id, reader=reader)
    assert outcome.created is True
    assert reason_for(outcome, 1) == CONSENT_REFUSED
    assert outcome.included == 0
    async with session_maker() as session:
        stored = (
            await session.execute(
                select(func.count())
                .select_from(EasyWeekVoucherFollowupPreviewRow)
                .where(EasyWeekVoucherFollowupPreviewRow.decision == FOLLOWUP_ROW_INCLUDED)
            )
        ).scalar_one()
    assert stored == 0


class ReplyingReader(FakeHistoryReader):
    """A booking read during which the person replies."""

    def __init__(self, session_maker: Any, *, address: str) -> None:
        super().__init__(bookings={})
        self.session_maker = session_maker
        self.address = address

    async def list_customer_bookings(self, customer_uuid: str, page: int, per_page: int = 50) -> dict[str, Any]:
        await _receipt(
            self.session_maker,
            address=self.address,
            key="reply-during-read",
            observed=ATTEMPTED_AT + timedelta(days=1),
        )
        return await super().list_customer_bookings(customer_uuid, page, per_page)


async def test_a_reply_during_the_booking_read_is_caught_before_the_snapshot(
    session_maker, binding_key, no_network
) -> None:
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    reader = ReplyingReader(session_maker, address=person(1)["phone"])
    outcome = await build(session_maker, batch_id=batch_id, reader=reader)
    assert reason_for(outcome, 1) == CUSTOMER_REPLIED


class StoppingBatchReader(FakeHistoryReader):
    """A booking read during which the ORIGINAL mailing is stopped for good."""

    def __init__(self, session_maker: Any, *, batch_id: int) -> None:
        super().__init__(bookings={})
        self.session_maker = session_maker
        self.batch_id = batch_id

    async def list_customer_bookings(self, customer_uuid: str, page: int, per_page: int = 50) -> dict[str, Any]:
        await production_ledger.request_stop(self.session_maker, batch_id=self.batch_id, requested_by="operator")
        return await super().list_customer_bookings(customer_uuid, page, per_page)


async def test_a_terminal_stop_during_the_read_refuses_the_whole_save(session_maker, binding_key, no_network) -> None:
    """Not a note on a stored snapshot: no snapshot at all."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    reader = StoppingBatchReader(session_maker, batch_id=batch_id)
    outcome = await build(session_maker, batch_id=batch_id, reader=reader)
    assert (outcome.created, outcome.reason) == (False, BATCH_TERMINAL_STOP)
    async with session_maker() as session:
        stored = (await session.execute(select(func.count()).select_from(EasyWeekVoucherFollowupPreview))).scalar_one()
    assert stored == 0


async def test_concurrent_previews_each_get_their_own_version(session_maker, binding_key, no_network) -> None:
    """Two operators pressing the check at once, on real PostgreSQL.

    Neither may lose its proposal and neither may overwrite the other's, so the
    only correct outcome is two versions — or one refusal that writes nothing,
    if the unique constraint catches a genuine race.
    """
    import asyncio

    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    results = await asyncio.gather(
        build(session_maker, batch_id=batch_id, operator="ops-one:fp1"),
        build(session_maker, batch_id=batch_id, operator="ops-two:fp2"),
        return_exceptions=True,
    )
    created = [r for r in results if isinstance(r, preview_module.PreviewOutcome) and r.created]
    async with session_maker() as session:
        versions = sorted((await session.execute(select(EasyWeekVoucherFollowupPreview.version))).scalars().all())
    assert len(versions) == len(created) >= 1
    assert versions == sorted(set(versions))


# ---------------------------------------------------------------------------
# The two external seams, and what must happen without them
# ---------------------------------------------------------------------------


async def test_no_meta_read_is_spent_when_the_local_row_is_missing(session_maker, binding_key, no_network) -> None:
    """The cheap half first, so a branch without a row costs no request."""
    await seed_sender_and_template(session_maker, with_followup_template=False)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    templates = TemplateReader()
    outcome = await build(session_maker, batch_id=batch_id, templates=templates)
    assert (outcome.created, outcome.reason) == (False, TEMPLATE_UNPROVEN)
    assert templates.calls == 0


async def test_the_meta_read_happens_with_no_session_open(session_maker, binding_key, no_network) -> None:
    """§46.5: nothing external may run while this holds a database handle.

    Asserted from inside the seam itself, which is the only place that can say
    it: the template listing probes the follow-up preview tables with
    ``FOR UPDATE NOWAIT`` on its own connection. If the preview were holding a
    transaction over this call, the probe would block or fail.
    """
    from sqlalchemy import text

    probed: list[bool] = []

    class ProbingTemplateReader(TemplateReader):
        async def list_meta_templates(self) -> list[dict[str, Any]]:
            async with session_maker() as probe:
                async with probe.begin():
                    await probe.execute(text("SELECT 1 FROM easyweek_voucher_followup_previews FOR UPDATE NOWAIT"))
                    await probe.execute(text("SELECT pg_advisory_xact_lock(hashtext('followup-probe'))"))
            probed.append(True)
            return await super().list_meta_templates()

    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    outcome = await build(session_maker, batch_id=batch_id, templates=ProbingTemplateReader())
    assert outcome.created is True
    assert probed == [True]


async def test_the_booking_read_happens_with_no_session_open(session_maker, binding_key, no_network) -> None:
    """The same assertion for the one read that costs an EasyWeek request."""
    from sqlalchemy import text

    probed: list[bool] = []

    class ProbingReader(FakeHistoryReader):
        async def list_customer_bookings(self, customer_uuid: str, page: int, per_page: int = 50) -> dict[str, Any]:
            async with session_maker() as probe:
                async with probe.begin():
                    await probe.execute(text("SELECT 1 FROM easyweek_voucher_production_batch_items FOR UPDATE NOWAIT"))
            probed.append(True)
            return await super().list_customer_bookings(customer_uuid, page, per_page)

    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    outcome = await build(session_maker, batch_id=batch_id, reader=ProbingReader())
    assert outcome.created is True
    assert probed == [True]
