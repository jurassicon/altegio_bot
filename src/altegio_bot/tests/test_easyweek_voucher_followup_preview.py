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
from altegio_bot.campaigns.easyweek_voucher_followup.historical_consent import scan_historical_refusals
from altegio_bot.campaigns.easyweek_voucher_followup.identity import (
    BATCH_SCHEMA_UNSUPPORTED,
    BATCH_TERMINAL_STOP,
    BOOKING_CAPTURE_UNPROVEN,
    BOOKING_CREATION_UNPROVEN,
    CONSENT_PENDING_INBOUND,
    CONSENT_REFUSED,
    CONSENT_UNKNOWN,
    CONTRACT_VERSION,
    CUSTOMER_REPLIED,
    ENTITLEMENT_INVALID,
    ENTITLEMENT_SETTLEMENT_UNPROVEN,
    ENTITLEMENT_UNPROVEN,
    ENTITLEMENT_UNREADABLE,
    EVIDENCE_UNPROVEN,
    FAILURE_POLICY_UNPROVEN,
    FOLLOWUP_GROUP_DELIVERED,
    FOLLOWUP_GROUP_FAILED,
    FOLLOWUP_GROUP_READ,
    FOLLOWUP_ROW_EXCLUDED,
    FOLLOWUP_ROW_INCLUDED,
    FOLLOWUP_ROW_UNKNOWN,
    FUTURE_BOOKING_ACTIVE,
    GROUP_MOVED_OUT_OF_SELECTION,
    GROUPS_REQUIRED,
    HISTORICAL_CONSENT_UNPROVEN,
    HISTORICAL_STOP_UNACCOUNTED,
    HISTORY_UNPROVEN,
    IDENTITY_CONFLICT,
    INTERVAL_NOT_ELAPSED,
    INTERVAL_REQUIRED,
    NEW_BOOKING_AFTER_ATTEMPT,
    SENDER_UNPROVEN,
    SOURCE_ATTEMPT_UNPROVEN,
    STATUS_EVIDENCE_UNPROVEN,
    TEMPLATE_UNPROVEN,
    canonical_groups,
)
from altegio_bot.campaigns.easyweek_voucher_production import ledger as production_ledger
from altegio_bot.models.models import (
    CONSENT_COMMAND_NONE,
    CONSENT_COMMAND_START,
    CONSENT_COMMAND_STOP,
    CONSENT_SOURCE_INBOUND,
    CONSENT_STATE_OPTED_IN,
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
    DELIVERY_RECEIPT_KIND_READ,
    DELIVERY_RECEIPT_MATCH_CONFIRMED,
    DELIVERY_RECEIPT_MATCH_UNMATCHED,
    FOLLOWUP_GROUP_UNPROVEN,
    INBOUND_AUTHENTICITY_SIGNED,
    INBOUND_AUTHENTICITY_UNVERIFIED,
    PROVIDER_EASYWEEK,
    RECIPIENT_BASIS_EARNED,
    RECIPIENT_BASIS_MANUAL,
    VOUCHER_PRODUCTION_ITEM_AMBIGUOUS,
    VOUCHER_PRODUCTION_ITEM_PROVIDER_ACCEPTED,
    VOUCHER_PRODUCTION_ITEM_SEND_REJECTED,
    VOUCHER_PRODUCTION_ITEM_SEND_UNKNOWN,
    CampaignRecipient,
    Client,
    EasyWeekVoucherFollowupPreview,
    EasyWeekVoucherFollowupPreviewRow,
    EasyWeekVoucherProductionBatch,
    EasyWeekVoucherProductionBatchAttempt,
    EasyWeekVoucherProductionBatchItem,
    EasyWeekVoucherProductionDeliveryFailure,
    EasyWeekVoucherProductionDeliveryReceipt,
    WhatsAppConsentSuppression,
    WhatsAppInboundReceipt,
)
from altegio_bot.settings import settings
from altegio_bot.tests.easyweek_voucher_followup_fixtures import (
    ATTEMPTED_AT,
    COMPANY_ID,
    NOW,
    PHONE_NUMBER_ID,
    SECOND_COMPANY_ID,
    SENDER_DIGEST,
    UNCONFIGURED_COMPANY_ID,
    TemplateReader,
    location_map,
    order_uuid_for,
    person,
    seed_booking_cancelled,
    seed_booking_created,
    seed_booking_created_without_a_date,
    seed_sender_and_template,
    seed_source_batch,
    voucher_code_for,
    voucher_order,
)
from altegio_bot.utils import utcnow

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
        orders: dict[str, dict[str, Any]] | None = None,
        order_error: Exception | None = None,
        order_changes: dict[str, Any] | None = None,
        batch_id: int = 1,
        schema_version: str = "4",
        # slot -> which synthetic person occupies it, when a test seeded a
        # batch whose slots are not the first N people.
        people: dict[int, int] | None = None,
    ) -> None:
        self.bookings = bookings or {}
        self.customer_error = customer_error
        self.history_error = history_error
        self.truncate = truncate
        self.wrong_customer = wrong_customer
        self.orders = orders or {}
        self.order_error = order_error
        self.order_changes = order_changes or {}
        self.batch_id = batch_id
        self.schema_version = schema_version
        self.people = people or {}
        self.customer_calls: list[str] = []
        self.history_calls: list[tuple[str, int]] = []
        self.order_calls: list[str] = []

    async def get_booking(self, booking_uuid: str) -> dict[str, Any]:  # pragma: no cover - unused here
        raise AssertionError("the follow-up preview must not read a source booking directly")

    async def get_order(self, order_uuid: str) -> dict[str, Any]:
        """The ORIGINAL order, as §46.6's artifact proof reads it back."""
        self.order_calls.append(order_uuid)
        if self.order_error is not None:
            raise self.order_error
        override = self.orders.get(order_uuid)
        if override is not None:
            return override
        head = order_uuid.split("-", 1)[0]
        batch_id, slot = int(head[:4]), int(head[4:])
        who = person(self.people.get(slot, slot))
        return voucher_order(
            order_uuid=order_uuid,
            marker=f"marker-{batch_id}-{slot}",
            customer_uuid=who["customer_uuid"],
            code=voucher_code_for(slot),
            schema_version=self.schema_version,
            **self.order_changes,
        )

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
    """The voucher MAC key the fixtures' artifacts were bound under.

    The configured branch registry comes with it, because every preview needs
    one: the booking CREATION capture judges a delivery against the configured
    branches of this workspace, and a deployment without a registry cannot
    establish the branch scope at all.
    """
    monkeypatch.setattr(settings, "easyweek_voucher_delivery_hmac_key", SecretStr("k" * 48), raising=False)
    monkeypatch.setattr(settings, "easyweek_voucher_delivery_hmac_key_id", "test-key-1", raising=False)
    monkeypatch.setattr(settings, "easyweek_location_map", location_map(), raising=False)


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
    order_reader: Any = None,
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
        order_reader=order_reader,
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
        # The §46.3 proof, present by default so that a test about something
        # else is not silently a test about a missing receipt.
        "delivered_receipt_proven": True,
        "read_receipt_proven": True,
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


@pytest.mark.parametrize("schema", ["3", "4"])
async def test_a_manual_production_slot_is_supported(session_maker, binding_key, no_network, schema: str) -> None:
    """§46.2 puts schema 3/4 production batches in scope, not one basis.

    A manually selected recipient of a production mailing received the same
    approved message from the same sender as an earned one. What differs is
    which link proves their identity — the operator-proven customer uuid on the
    campaign row rather than the booking the voucher was earned on — and that is
    checked per slot.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(
        session_maker,
        slots=[{"state": "delivered", "basis": RECIPIENT_BASIS_MANUAL}],
        schema_version=schema,
    )
    outcome = await build(session_maker, batch_id=batch_id, reader=FakeHistoryReader(schema_version=schema))
    assert outcome.created is True, outcome.reason
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED


async def test_a_mixed_production_batch_judges_each_slot_on_its_own_basis(
    session_maker, binding_key, no_network
) -> None:
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(
        session_maker,
        slots=[
            {"state": "delivered", "index": 1, "basis": RECIPIENT_BASIS_EARNED},
            {"state": "delivered", "index": 2, "basis": RECIPIENT_BASIS_MANUAL},
        ],
    )
    async with session_maker() as session:
        header = (await session.execute(select(EasyWeekVoucherProductionBatch))).scalar_one()
        assert header.recipient_basis == "mixed"
    outcome = await build(session_maker, batch_id=batch_id)
    assert outcome.created is True, outcome.reason
    assert outcome.included == 2


async def test_a_manual_slot_with_a_substituted_uuid_is_an_identity_conflict(
    session_maker, binding_key, no_network
) -> None:
    """A manual row is proven by its uuid, and a matching phone is not a proof."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(
        session_maker, slots=[{"state": "delivered", "basis": RECIPIENT_BASIS_MANUAL}]
    )
    async with session_maker() as session:
        async with session.begin():
            recipient = (await session.execute(select(CampaignRecipient))).scalar_one()
            recipient.easyweek_customer_uuid = uuid_module.UUID("44444444-1111-4111-8111-111111111111")
    outcome = await build(session_maker, batch_id=batch_id)
    assert reason_for(outcome, 1) == IDENTITY_CONFLICT


async def test_a_slot_whose_basis_moved_is_an_identity_conflict(session_maker, binding_key, no_network) -> None:
    """The slot was composed under one basis; the campaign row now says another."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    async with session_maker() as session:
        async with session.begin():
            item = (await session.execute(select(EasyWeekVoucherProductionBatchItem))).scalar_one()
            item.recipient_basis = RECIPIENT_BASIS_MANUAL
            item.source_booking_uuid = None
            item.source_proof_digest = None
    outcome = await build(session_maker, batch_id=batch_id)
    assert reason_for(outcome, 1) == IDENTITY_CONFLICT


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


async def test_a_booking_created_after_the_attempt_excludes_even_when_cancelled(
    session_maker, binding_key, no_network
) -> None:
    """§46.5's first rule, on the CREATION date.

    The appointment is in the past and the booking was cancelled, so neither
    the appointment-time rule nor the future-booking rule says anything. What
    excludes is that the person made it after the mailing went out.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    new_booking = booking(
        index=2,
        customer_uuid=who["customer_uuid"],
        start=ATTEMPTED_AT + timedelta(days=2),
        canceled=True,
        completed=False,
    )
    await seed_booking_created(
        session_maker,
        booking_uuid=new_booking["uuid"],
        numeric_customer_id=who["numeric_id"],
        created_at=ATTEMPTED_AT + timedelta(days=1),
    )
    await seed_booking_cancelled(
        session_maker,
        booking_uuid=new_booking["uuid"],
        numeric_customer_id=who["numeric_id"],
    )
    reader = FakeHistoryReader(
        bookings={
            who["customer_uuid"]: [
                booking(index=1, customer_uuid=who["customer_uuid"], start=ATTEMPTED_AT - timedelta(days=5)),
                new_booking,
            ]
        }
    )
    outcome = await build(session_maker, batch_id=batch_id, reader=reader)
    assert reason_for(outcome, 1) == NEW_BOOKING_AFTER_ATTEMPT


async def test_a_booking_created_before_the_attempt_is_not_new(session_maker, binding_key, no_network) -> None:
    """The first review reproduction: made in advance, appointed after, cancelled.

    Nothing happened since the mailing. Reading the appointment time as the
    creation time called this a new booking and excluded the person wrongly.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    old_booking = booking(
        index=3,
        customer_uuid=who["customer_uuid"],
        start=ATTEMPTED_AT + timedelta(days=3),
        canceled=True,
        completed=False,
    )
    await seed_booking_created(
        session_maker,
        booking_uuid=old_booking["uuid"],
        numeric_customer_id=who["numeric_id"],
        created_at=ATTEMPTED_AT - timedelta(days=10),
        # Heard about late, which must not become the creation time.
        received_at=ATTEMPTED_AT + timedelta(hours=1),
    )
    reader = FakeHistoryReader(bookings={who["customer_uuid"]: [old_booking]})
    outcome = await build(session_maker, batch_id=batch_id, reader=reader)
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED


async def test_a_booking_created_after_the_attempt_for_a_past_visit_excludes(
    session_maker, binding_key, no_network
) -> None:
    """The second review reproduction, the other way round.

    The appointment is before the anchor, so an appointment-time rule left this
    person included — although they booked, and came, after the mailing.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    retro = booking(
        index=4,
        customer_uuid=who["customer_uuid"],
        start=ATTEMPTED_AT - timedelta(hours=2),
    )
    await seed_booking_created(
        session_maker,
        booking_uuid=retro["uuid"],
        numeric_customer_id=who["numeric_id"],
        created_at=ATTEMPTED_AT + timedelta(days=1),
    )
    reader = FakeHistoryReader(bookings={who["customer_uuid"]: [retro]})
    outcome = await build(session_maker, batch_id=batch_id, reader=reader)
    assert reason_for(outcome, 1) == NEW_BOOKING_AFTER_ATTEMPT


async def test_a_creation_exactly_on_the_boundary_is_not_after_it(session_maker, binding_key, no_network) -> None:
    """Created at the same instant as the attempt is not created after it."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    edge = booking(index=5, customer_uuid=who["customer_uuid"], start=ATTEMPTED_AT - timedelta(days=1))
    await seed_booking_created(
        session_maker,
        booking_uuid=edge["uuid"],
        numeric_customer_id=who["numeric_id"],
        created_at=ATTEMPTED_AT,
    )
    reader = FakeHistoryReader(bookings={who["customer_uuid"]: [edge]})
    outcome = await build(session_maker, batch_id=batch_id, reader=reader)
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED


async def test_a_booking_after_the_attempt_with_no_creation_evidence_is_unknown(
    session_maker, binding_key, no_network
) -> None:
    """Neither verdict is available, so neither is claimed."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    unknown_origin = booking(
        index=6,
        customer_uuid=who["customer_uuid"],
        start=ATTEMPTED_AT + timedelta(days=2),
        canceled=True,
        completed=False,
    )
    reader = FakeHistoryReader(bookings={who["customer_uuid"]: [unknown_origin]})
    outcome = await build(session_maker, batch_id=batch_id, reader=reader)
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
    assert reason_for(outcome, 1) == BOOKING_CREATION_UNPROVEN


async def test_a_reassigned_booking_keeps_its_creation_with_whoever_made_it(
    session_maker, binding_key, no_network
) -> None:
    """A later reassignment does not undo the fact that this person booked.

    Review round 2, finding 2B. The creation delivery names a customer; that is
    the historical fact. Dropping it because a LATER delivery of the same
    booking names somebody else credited nobody with the booking and let the
    person who made it back into the audience.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    moved = booking(
        index=7,
        customer_uuid=who["customer_uuid"],
        start=ATTEMPTED_AT + timedelta(days=2),
        canceled=True,
        completed=False,
    )
    await seed_booking_created(
        session_maker,
        booking_uuid=moved["uuid"],
        numeric_customer_id=who["numeric_id"],
        created_at=ATTEMPTED_AT - timedelta(days=4),
    )
    # Another delivery of the SAME booking names somebody else.
    await seed_booking_cancelled(
        session_maker,
        booking_uuid=moved["uuid"],
        numeric_customer_id=who["numeric_id"] + 1,
    )
    reader = FakeHistoryReader(bookings={who["customer_uuid"]: [moved]})
    outcome = await build(session_maker, batch_id=batch_id, reader=reader)
    # Created BEFORE the attempt and cancelled, so nothing excludes: the
    # creation is attributed, it is simply not new.
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED


async def test_a_booking_created_after_the_attempt_then_moved_away_still_excludes(
    session_maker, binding_key, no_network
) -> None:
    """Review round 2, finding 2B, the reproduction exactly as it was reported.

    The person booked after the mailing; the booking was then reassigned, so
    their live history is empty. The capture still attributes the creation to
    them, and that is engagement since the mailing.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    moved = booking(index=7, customer_uuid=person(2)["customer_uuid"], start=ATTEMPTED_AT + timedelta(days=9))
    await seed_booking_created(
        session_maker,
        booking_uuid=moved["uuid"],
        numeric_customer_id=who["numeric_id"],
        created_at=ATTEMPTED_AT + timedelta(days=1),
    )
    await seed_booking_cancelled(
        session_maker,
        booking_uuid=moved["uuid"],
        numeric_customer_id=who["numeric_id"] + 1,
    )
    reader = FakeHistoryReader(bookings={who["customer_uuid"]: []})
    outcome = await build(session_maker, batch_id=batch_id, reader=reader)
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_EXCLUDED
    assert reason_for(outcome, 1) == NEW_BOOKING_AFTER_ATTEMPT


async def test_a_booking_moved_in_is_not_the_new_holders_creation(session_maker, binding_key, no_network) -> None:
    """The other direction: a creation is never credited to the new owner."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    moved = booking(
        index=7,
        customer_uuid=who["customer_uuid"],
        start=ATTEMPTED_AT + timedelta(days=2),
        canceled=True,
        completed=False,
    )
    # Created by somebody else, then moved to this person.
    await seed_booking_created(
        session_maker,
        booking_uuid=moved["uuid"],
        numeric_customer_id=who["numeric_id"] + 5,
        created_at=ATTEMPTED_AT - timedelta(days=4),
    )
    await seed_booking_cancelled(
        session_maker,
        booking_uuid=moved["uuid"],
        numeric_customer_id=who["numeric_id"],
    )
    reader = FakeHistoryReader(bookings={who["customer_uuid"]: [moved]})
    outcome = await build(session_maker, batch_id=batch_id, reader=reader)
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
    assert reason_for(outcome, 1) == BOOKING_CREATION_UNPROVEN


async def test_a_manual_card_without_a_numeric_id_is_provable(session_maker, binding_key, no_network) -> None:
    """Review round 2, finding 5: the production shape of a manual card.

    ``ensure_local_identity`` creates a manual recipient's card with the proven
    uuid and ``altegio_client_id=None``. That is ordinary data, not damage, and
    demanding a numeric bridge from it refused somebody for the way their card
    was made. With a complete empty live history by the proven uuid there is
    nothing to date and nothing that could have left their list.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(
        session_maker,
        slots=[{"state": "delivered", "basis": RECIPIENT_BASIS_MANUAL, "numeric_id": None}],
    )
    async with session_maker() as session:
        card = (
            await session.execute(select(Client).where(Client.easyweek_customer_uuid.is_not(None)).limit(1))
        ).scalar_one()
        assert card.altegio_client_id is None
    reader = FakeHistoryReader(bookings={person(1)["customer_uuid"]: []})
    outcome = await build(session_maker, batch_id=batch_id, reader=reader)
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED


async def test_a_manual_card_without_a_numeric_id_still_sees_its_own_bookings(
    session_maker, binding_key, no_network
) -> None:
    """The history check is not weakened: a real new booking still excludes."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(
        session_maker,
        slots=[{"state": "delivered", "basis": RECIPIENT_BASIS_MANUAL, "numeric_id": None}],
    )
    who = person(1)
    fresh = booking(
        index=4,
        customer_uuid=who["customer_uuid"],
        start=ATTEMPTED_AT - timedelta(days=1),
        canceled=True,
        completed=False,
    )
    await seed_booking_created(
        session_maker,
        booking_uuid=fresh["uuid"],
        numeric_customer_id=who["numeric_id"],
        created_at=ATTEMPTED_AT + timedelta(days=2),
    )
    reader = FakeHistoryReader(bookings={who["customer_uuid"]: [fresh]})
    outcome = await build(session_maker, batch_id=batch_id, reader=reader)
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_EXCLUDED
    assert reason_for(outcome, 1) == NEW_BOOKING_AFTER_ATTEMPT


async def test_a_manual_card_without_a_numeric_id_is_unknown_while_a_booking_moved(
    session_maker, binding_key, no_network
) -> None:
    """And it stays honest: a reassignment is what an empty list cannot show."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(
        session_maker,
        slots=[{"state": "delivered", "basis": RECIPIENT_BASIS_MANUAL, "numeric_id": None}],
    )
    who = person(1)
    stray = booking(index=8, customer_uuid=person(3)["customer_uuid"], start=ATTEMPTED_AT + timedelta(days=5))
    await seed_booking_created(
        session_maker,
        booking_uuid=stray["uuid"],
        numeric_customer_id=4242,
        created_at=ATTEMPTED_AT + timedelta(days=1),
    )
    await seed_booking_cancelled(session_maker, booking_uuid=stray["uuid"], numeric_customer_id=4343)
    reader = FakeHistoryReader(bookings={who["customer_uuid"]: []})
    outcome = await build(session_maker, batch_id=batch_id, reader=reader)
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
    assert reason_for(outcome, 1) == BOOKING_CAPTURE_UNPROVEN


async def test_an_old_appointment_with_no_creation_evidence_is_unknown(session_maker, binding_key, no_network) -> None:
    """Review round 2, finding 2A: an old appointment is not an old booking.

    A booking appointed before the attempt used to skip the creation check
    altogether, so a booking entered after the mailing to record a visit that
    already happened — exactly the engagement §46.5 is about — came out
    included whenever its creation webhook was missing or invalid.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    backdated = booking(index=5, customer_uuid=who["customer_uuid"], start=ATTEMPTED_AT - timedelta(days=2))
    reader = FakeHistoryReader(bookings={who["customer_uuid"]: [backdated]})
    outcome = await build(session_maker, batch_id=batch_id, reader=reader)
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
    assert reason_for(outcome, 1) == BOOKING_CREATION_UNPROVEN


async def test_an_old_appointment_created_after_the_attempt_excludes(session_maker, binding_key, no_network) -> None:
    """And with the creation proven, the backdated booking excludes."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    backdated = booking(index=5, customer_uuid=who["customer_uuid"], start=ATTEMPTED_AT - timedelta(days=2))
    await seed_booking_created(
        session_maker,
        booking_uuid=backdated["uuid"],
        numeric_customer_id=who["numeric_id"],
        created_at=ATTEMPTED_AT + timedelta(days=3),
    )
    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: [backdated]}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_EXCLUDED
    assert reason_for(outcome, 1) == NEW_BOOKING_AFTER_ATTEMPT


async def test_a_creation_older_than_the_report_window_is_still_read(session_maker, binding_key, no_network) -> None:
    """Review round 2, finding 2C: the evidence was in the database, unread.

    The attribution report scans from one day before the earliest send, because
    a conversion is a booking created AFTER it. The follow-up asks a second
    question about the same bookings — when was this one made — and the answer
    may be long before the mailing. With the report's floor the creation sat
    unread and an ordinary cancelled booking made ten days earlier turned the
    recipient unknown.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    old = booking(
        index=6,
        customer_uuid=who["customer_uuid"],
        start=ATTEMPTED_AT + timedelta(days=3),
        canceled=True,
        completed=False,
    )
    made = ATTEMPTED_AT - timedelta(days=10)
    await seed_booking_created(
        session_maker,
        booking_uuid=old["uuid"],
        numeric_customer_id=who["numeric_id"],
        created_at=made,
        # The delivery arrived when the booking was made, which is what put it
        # outside the report's own window. Not faked late.
        received_at=made,
    )
    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: [old]}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED


async def test_an_unconfigured_branch_registry_has_no_creation_evidence(session_maker, monkeypatch, no_network) -> None:
    """Without a registry the branch scope of the question is unknown."""
    monkeypatch.setattr(settings, "easyweek_voucher_delivery_hmac_key", SecretStr("k" * 48), raising=False)
    monkeypatch.setattr(settings, "easyweek_voucher_delivery_hmac_key_id", "test-key-1", raising=False)
    monkeypatch.setattr(settings, "easyweek_location_map", "{}", raising=False)
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    outcome = await build(session_maker, batch_id=batch_id)
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
    assert reason_for(outcome, 1) == BOOKING_CAPTURE_UNPROVEN


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
    earned = booking(index=1, customer_uuid=who["customer_uuid"], start=ATTEMPTED_AT - timedelta(days=10))
    # Its creation is proven and older than the attempt. Without that the row is
    # honestly unknown, because an old APPOINTMENT does not establish an old
    # booking — which is the whole point of keeping the two dates apart.
    await seed_booking_created(
        session_maker,
        booking_uuid=earned["uuid"],
        numeric_customer_id=who["numeric_id"],
        created_at=ATTEMPTED_AT - timedelta(days=12),
    )
    reader = FakeHistoryReader(bookings={who["customer_uuid"]: [earned]})
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
    visits = [
        booking(index=1, customer_uuid=who["customer_uuid"], start=ATTEMPTED_AT - timedelta(days=30)),
        booking(index=2, customer_uuid=who["customer_uuid"], start=ATTEMPTED_AT - timedelta(days=10)),
    ]
    for visit, made in zip(visits, (timedelta(days=40), timedelta(days=12))):
        await seed_booking_created(
            session_maker,
            booking_uuid=visit["uuid"],
            numeric_customer_id=who["numeric_id"],
            created_at=ATTEMPTED_AT - made,
        )
    reader = FakeHistoryReader(bookings={who["customer_uuid"]: visits})
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


async def test_no_configured_reader_proves_neither_artifact_nor_history(session_maker, binding_key, no_network) -> None:
    """With no EasyWeek read at all, both external proofs are unknown."""
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
    # The artifact proof comes first, so its reason is the one reported.
    assert reason_for(outcome, 1) == ENTITLEMENT_UNREADABLE


async def test_a_missing_booking_reader_is_unknown_not_an_empty_history(session_maker, binding_key, no_network) -> None:
    """The artifact proves out and the booking history cannot be read at all."""
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
        order_reader=FakeHistoryReader(),
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
        target_order_uuid=order_uuid_for(1, 1),
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
        "target_order_uuid": order_uuid_for(1, 1),
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


# ---------------------------------------------------------------------------
# The historical opt-out backlog (§46.4.4)
# ---------------------------------------------------------------------------


async def _stored_event(
    session_maker,
    *,
    dedupe_key: str,
    address: str,
    text: str,
    status: str = "processed",
    received: datetime | None = None,
    payload: dict[str, Any] | None = None,
) -> int:
    """One stored WhatsApp delivery, exactly as the ingress would have kept it.

    No receipt is written, which is the whole point: an event stored before
    PR-22.1 existed has none, and every source the follow-up consults is silent
    about it.
    """
    from altegio_bot.models.models import WhatsAppEvent

    body = payload or {
        "entry": [
            {
                "changes": [
                    {
                        "value": {
                            "metadata": {"phone_number_id": PHONE_NUMBER_ID},
                            "messages": [
                                {
                                    "from": address.lstrip("+"),
                                    "id": f"wamid.{dedupe_key}",
                                    "type": "text",
                                    "text": {"body": text},
                                    "timestamp": str(int((received or ATTEMPTED_AT).timestamp())),
                                }
                            ],
                        }
                    }
                ]
            }
        ]
    }
    async with session_maker() as session:
        async with session.begin():
            event = WhatsAppEvent(
                dedupe_key=dedupe_key,
                payload=body,
                query={},
                headers={},
                status=status,
                received_at=received or ATTEMPTED_AT,
            )
            session.add(event)
            await session.flush()
            return int(event.id)


async def test_an_unaccounted_cyrillic_stop_blocks_the_recipient(session_maker, binding_key, no_network) -> None:
    """The reproduction: a stored «СТОП» with no receipt, flag or suppression.

    Every source PR-22.1 built reads clean — the ledger holds nothing, the card
    is not flagged, the campaign row is not opted out, there is no pending
    inbound — and the person has nevertheless told us to stop.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    await _stored_event(
        session_maker,
        dedupe_key="legacy-1",
        address=person(1)["phone"],
        text="СТОП",
    )
    async with session_maker() as session:
        # Proof that the finding is about a real gap and not about a seeded
        # refusal: nothing else in the database refuses this person.
        suppressions = (
            await session.execute(select(func.count()).select_from(WhatsAppConsentSuppression))
        ).scalar_one()
        receipts = (await session.execute(select(func.count()).select_from(WhatsAppInboundReceipt))).scalar_one()
        flagged = (
            await session.execute(select(func.count()).select_from(Client).where(Client.wa_opted_out.is_(True)))
        ).scalar_one()
    assert (suppressions, receipts, flagged) == (0, 0, 0)

    outcome = await build(session_maker, batch_id=batch_id)
    assert outcome.created is True
    assert reason_for(outcome, 1) == HISTORICAL_STOP_UNACCOUNTED
    assert outcome.included == 0


async def test_a_latin_stop_in_the_backlog_blocks_too(session_maker, binding_key, no_network) -> None:
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    await _stored_event(session_maker, dedupe_key="legacy-2", address=person(1)["phone"], text="stop please")
    outcome = await build(session_maker, batch_id=batch_id)
    assert reason_for(outcome, 1) == HISTORICAL_STOP_UNACCOUNTED


async def test_a_truncated_backlog_scan_is_unknown_for_everybody(session_maker, binding_key, no_network) -> None:
    """A partial answer about a backlog is not an answer."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    await _stored_event(session_maker, dedupe_key="noise-1", address="+4915100777777", text="guten Tag")
    await _stored_event(session_maker, dedupe_key="noise-2", address="+4915100777778", text="guten Tag")
    async with session_maker() as session:
        scan = await scan_historical_refusals(session, addresses=[person(1)["phone"]], limit=1)
    assert scan.complete is False
    assert scan.truncated is True
    assert any("truncated" in gap for gap in scan.gaps)

    outcome = await preview_module.create_preview(
        session_maker,
        spec=preview_module.PreviewSpec(
            batch_id=batch_id,
            groups=(FOLLOWUP_GROUP_DELIVERED,),
            min_interval_seconds=FOURTEEN_DAYS,
            operator_principal="o",
        ),
        booking_reader=FakeHistoryReader(),
        template_reader=TemplateReader(),
        now=NOW,
        historical_event_limit=1,
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
    assert reason_for(outcome, 1) == HISTORICAL_CONSENT_UNPROVEN


async def test_an_empty_suppression_table_is_not_proof_of_consent(session_maker, binding_key, no_network) -> None:
    """The sentence the runbook insists on, as an executable check.

    The backlog holds this person's refusal and the suppression table is empty.
    An implementation that read "no rows, therefore allowed" would include them.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    await _stored_event(session_maker, dedupe_key="legacy-3", address=person(1)["phone"], text="стоп")
    outcome = await build(session_maker, batch_id=batch_id)
    assert outcome.included == 0
    assert reason_for(outcome, 1) == HISTORICAL_STOP_UNACCOUNTED


async def test_an_accounted_historical_stop_does_not_block_twice(session_maker, binding_key, no_network) -> None:
    """A stored STOP the ledger already refuses is accounted for.

    The consent aggregate excludes that person on its own, with its own reason;
    the backlog scan has nothing to add and must not invent a second one.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    await _stored_event(session_maker, dedupe_key="legacy-4", address=person(1)["phone"], text="стоп")
    await _suppress(session_maker, address=person(1)["phone"])
    outcome = await build(session_maker, batch_id=batch_id)
    assert reason_for(outcome, 1) == CONSENT_REFUSED


async def test_a_correctly_processed_history_does_not_block(session_maker, binding_key, no_network) -> None:
    """An event with receipts was accounted for when it arrived."""
    from altegio_bot.models.models import WhatsAppEvent

    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    event_id = await _stored_event(
        session_maker,
        dedupe_key="handled-1",
        address=person(1)["phone"],
        text="hallo",
        received=ATTEMPTED_AT - timedelta(days=2),
    )
    async with session_maker() as session:
        async with session.begin():
            session.add(
                WhatsAppInboundReceipt(
                    receipt_key="handled-1:0",
                    whatsapp_event_id=event_id,
                    message_index=0,
                    wa_address=person(1)["phone"],
                    command=CONSENT_COMMAND_NONE,
                    authenticity=INBOUND_AUTHENTICITY_SIGNED,
                    route_allowed=True,
                    observed_at=ATTEMPTED_AT - timedelta(days=2),
                    resolved_at=ATTEMPTED_AT - timedelta(days=2),
                    resolution="noop",
                )
            )
    # And the event itself is excluded from the backlog, so the scan is cheap.
    async with session_maker() as session:
        scan = await scan_historical_refusals(session, addresses=[person(1)["phone"]])
    assert scan.complete is True
    assert scan.scanned_events == 0
    assert scan.unaccounted == frozenset()
    outcome = await build(session_maker, batch_id=batch_id)
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED
    assert WhatsAppEvent is not None  # the import is the point of the seed above


async def test_a_chatwoot_mirror_in_the_backlog_is_not_a_second_refusal(session_maker, binding_key, no_network) -> None:
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    await _stored_event(session_maker, dedupe_key="chatwoot:9", address=person(1)["phone"], text="стоп")
    outcome = await build(session_maker, batch_id=batch_id)
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED


async def test_a_refused_route_in_the_backlog_does_not_block(session_maker, binding_key, no_network) -> None:
    """An ``ignored`` delivery named no route this deployment accepts."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    await _stored_event(
        session_maker,
        dedupe_key="legacy-5",
        address=person(1)["phone"],
        text="стоп",
        status="ignored",
    )
    outcome = await build(session_maker, batch_id=batch_id)
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED


async def test_a_historical_start_is_not_read_as_permission(session_maker, binding_key, no_network) -> None:
    """A START in the backlog neither blocks nor clears a refusal."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    await _stored_event(session_maker, dedupe_key="legacy-6", address=person(1)["phone"], text="стоп")
    await _stored_event(
        session_maker,
        dedupe_key="legacy-7",
        address=person(1)["phone"],
        text="start",
        received=ATTEMPTED_AT + timedelta(days=1),
    )
    outcome = await build(session_maker, batch_id=batch_id)
    assert reason_for(outcome, 1) == HISTORICAL_STOP_UNACCOUNTED


async def _other_branch_card(session_maker, *, customer_uuid: str, phone: str, company_id: int = 777) -> None:
    """A second EasyWeek card of the SAME person, at another configured branch.

    The ordinary shape of a workspace with more than one salon, and the data the
    §46.4.2–3 veto is written for: one person, two cards, two numbers.
    """
    async with session_maker() as session:
        async with session.begin():
            session.add(
                Client(
                    provider=PROVIDER_EASYWEEK,
                    company_id=company_id,
                    easyweek_customer_uuid=uuid_module.UUID(customer_uuid),
                    altegio_client_id=None,
                    display_name="Same person, other branch",
                    phone_e164=phone,
                    raw={},
                )
            )


async def _proven_opt_in(session_maker, *, address: str, stop_at: datetime, start_at: datetime) -> tuple[str, str]:
    """A refusal, then the proven newer opt-in the EXISTING contract honours.

    Written through ``apply_consent_decision`` rather than by hand, so these
    tests rest on the real START rules — signed, strictly newer, orderable —
    instead of a row a test invented.
    """
    from altegio_bot.whatsapp_consent import apply_consent_decision, lock_consent_address

    outcomes: list[str] = []
    for command, moment in ((CONSENT_COMMAND_STOP, stop_at), (CONSENT_COMMAND_START, start_at)):
        async with session_maker() as session:
            async with session.begin():
                await lock_consent_address(session, address)
                decision = await apply_consent_decision(
                    session,
                    wa_address=address,
                    command=command,
                    authenticity=INBOUND_AUTHENTICITY_SIGNED,
                    source=CONSENT_SOURCE_INBOUND,
                    observed_at=moment,
                    event_at=moment,
                )
                outcomes.append(str(decision.state))
    return outcomes[0], outcomes[1]


async def test_a_stored_stop_from_another_card_of_the_same_person_blocks(
    session_maker, binding_key, no_network
) -> None:
    """Review round 2, finding 1A: the veto follows the PERSON, not the number.

    The mailing went to number A. Another card of the same proven uuid, at
    another branch, carries number B, and the backlog holds a refusal sent from
    B. The scan found it — B is one of the numbers the batch's recipients are
    known by — and the preview then asked about A alone and included them.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    other_number = "+4915100654321"
    await _other_branch_card(session_maker, customer_uuid=who["customer_uuid"], phone=other_number)
    await _stored_event(session_maker, dedupe_key="legacy-uuid-1", address=other_number, text="стоп")
    outcome = await build(session_maker, batch_id=batch_id)
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_EXCLUDED
    assert reason_for(outcome, 1) == HISTORICAL_STOP_UNACCOUNTED


async def test_a_stored_stop_from_a_stranger_does_not_block(session_maker, binding_key, no_network) -> None:
    """Another number belonging to another uuid is another person."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    stranger = "+4915100654399"
    async with session_maker() as session:
        async with session.begin():
            session.add(
                Client(
                    provider=PROVIDER_EASYWEEK,
                    company_id=778,
                    easyweek_customer_uuid=uuid_module.UUID(person(5)["customer_uuid"]),
                    altegio_client_id=None,
                    display_name="Somebody else",
                    phone_e164=stranger,
                    raw={},
                )
            )
    await _stored_event(session_maker, dedupe_key="legacy-uuid-2", address=stranger, text="стоп")
    outcome = await build(session_maker, batch_id=batch_id)
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED


async def test_a_proven_newer_start_lifts_a_stored_stop(session_maker, binding_key, no_network) -> None:
    """Review round 2, finding 1B: an answered refusal is not an unaccounted one.

    The customer said stop, then resubscribed under the existing START contract
    and the ledger honoured it. Both events happened before the mailing. The
    backlog scan re-imposed the old refusal on every preview because the ledger
    no longer reads ``opted_out``.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    stop_at = ATTEMPTED_AT - timedelta(days=20)
    await _stored_event(
        session_maker,
        dedupe_key="legacy-order-1",
        address=who["phone"],
        text="стоп",
        received=stop_at,
    )
    _, after_start = await _proven_opt_in(
        session_maker,
        address=who["phone"],
        stop_at=stop_at,
        start_at=ATTEMPTED_AT - timedelta(days=10),
    )
    assert after_start == CONSENT_STATE_OPTED_IN
    outcome = await build(session_maker, batch_id=batch_id)
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED


async def test_a_stop_newer_than_the_opt_in_still_blocks(session_maker, binding_key, no_network) -> None:
    """The order is the writer's own, and a later refusal wins."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    await _proven_opt_in(
        session_maker,
        address=who["phone"],
        stop_at=ATTEMPTED_AT - timedelta(days=20),
        start_at=ATTEMPTED_AT - timedelta(days=15),
    )
    await _stored_event(
        session_maker,
        dedupe_key="legacy-order-2",
        address=who["phone"],
        text="стоп",
        received=ATTEMPTED_AT - timedelta(days=5),
    )
    outcome = await build(session_maker, batch_id=batch_id)
    assert reason_for(outcome, 1) == HISTORICAL_STOP_UNACCOUNTED


async def test_a_stop_at_the_same_moment_as_the_opt_in_still_blocks(session_maker, binding_key, no_network) -> None:
    """A tie is not an order, and an unordered pair falls to the refusal."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    opt_in_at = ATTEMPTED_AT - timedelta(days=15)
    await _proven_opt_in(
        session_maker,
        address=who["phone"],
        stop_at=ATTEMPTED_AT - timedelta(days=20),
        start_at=opt_in_at,
    )
    await _stored_event(
        session_maker,
        dedupe_key="legacy-order-3",
        address=who["phone"],
        text="стоп",
        received=opt_in_at,
    )
    outcome = await build(session_maker, batch_id=batch_id)
    assert reason_for(outcome, 1) == HISTORICAL_STOP_UNACCOUNTED


async def test_a_stop_with_no_stated_time_cannot_be_superseded(session_maker, binding_key, no_network) -> None:
    """Unknown on one side means the order has no answer, so the refusal stands."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    await _proven_opt_in(
        session_maker,
        address=who["phone"],
        stop_at=ATTEMPTED_AT - timedelta(days=20),
        start_at=ATTEMPTED_AT - timedelta(days=10),
    )
    await _stored_event(
        session_maker,
        dedupe_key="legacy-order-4",
        address=who["phone"],
        text="стоп",
        payload={
            "entry": [
                {
                    "changes": [
                        {
                            "value": {
                                "metadata": {"phone_number_id": PHONE_NUMBER_ID},
                                "messages": [
                                    {
                                        "from": who["phone"].lstrip("+"),
                                        "id": "wamid.no-time",
                                        "type": "text",
                                        "text": {"body": "стоп"},
                                    }
                                ],
                            }
                        }
                    ]
                }
            ]
        },
    )
    outcome = await build(session_maker, batch_id=batch_id)
    assert reason_for(outcome, 1) == HISTORICAL_STOP_UNACCOUNTED


async def test_an_unsigned_start_never_lifts_a_stored_stop(session_maker, binding_key, no_network) -> None:
    """The existing START contract decides, and it refuses an unverified one."""
    from altegio_bot.whatsapp_consent import apply_consent_decision, lock_consent_address

    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    stop_at = ATTEMPTED_AT - timedelta(days=20)
    await _stored_event(
        session_maker,
        dedupe_key="legacy-order-5",
        address=who["phone"],
        text="стоп",
        received=stop_at,
    )
    async with session_maker() as session:
        async with session.begin():
            await lock_consent_address(session, who["phone"])
            await apply_consent_decision(
                session,
                wa_address=who["phone"],
                command=CONSENT_COMMAND_STOP,
                authenticity=INBOUND_AUTHENTICITY_SIGNED,
                source=CONSENT_SOURCE_INBOUND,
                observed_at=stop_at,
                event_at=stop_at,
            )
    async with session_maker() as session:
        async with session.begin():
            await lock_consent_address(session, who["phone"])
            refused = await apply_consent_decision(
                session,
                wa_address=who["phone"],
                command=CONSENT_COMMAND_START,
                authenticity=INBOUND_AUTHENTICITY_UNVERIFIED,
                source=CONSENT_SOURCE_INBOUND,
                observed_at=ATTEMPTED_AT - timedelta(days=10),
                event_at=ATTEMPTED_AT - timedelta(days=10),
            )
    assert refused.applied is False
    # The ledger still refuses, so the aggregate excludes before the backlog
    # scan is even consulted.
    outcome = await build(session_maker, batch_id=batch_id)
    assert reason_for(outcome, 1) == CONSENT_REFUSED


async def test_a_start_on_another_number_does_not_lift_this_one(session_maker, binding_key, no_network) -> None:
    """A START proven for one number is not a subscription on another."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    other_number = "+4915100654322"
    await _other_branch_card(session_maker, customer_uuid=who["customer_uuid"], phone=other_number)
    await _stored_event(session_maker, dedupe_key="legacy-order-6", address=other_number, text="стоп")
    # The opt-in belongs to the OTHER number of the same person.
    await _proven_opt_in(
        session_maker,
        address=who["phone"],
        stop_at=ATTEMPTED_AT - timedelta(days=20),
        start_at=ATTEMPTED_AT - timedelta(days=10),
    )
    outcome = await build(session_maker, batch_id=batch_id)
    assert reason_for(outcome, 1) == HISTORICAL_STOP_UNACCOUNTED


async def _uuid_bound_opt_in(
    session_maker,
    *,
    address: str,
    customer_uuid: str,
    stop_at: datetime,
    start_at: datetime,
) -> None:
    """A refusal once recorded against *address* and bound to the uuid, then lifted.

    The shape that keeps a dropped number reachable: the card has moved on, no
    card carries this number any more, and the ban travelled through
    ``banned_easyweek_customer_uuid`` when it was applied. The row now reads
    ``opted_in`` — a proven newer START answered it — so nothing refuses, and the
    link it carries is still the only thing that connects the old number to this
    person.
    """
    async with session_maker() as session:
        async with session.begin():
            session.add(
                WhatsAppConsentSuppression(
                    wa_address=address,
                    state=CONSENT_STATE_OPTED_IN,
                    consent_version=2,
                    decision_command=CONSENT_COMMAND_START,
                    decision_source=CONSENT_SOURCE_INBOUND,
                    decision_authenticity=INBOUND_AUTHENTICITY_SIGNED,
                    decided_observed_at=start_at,
                    decided_event_at=start_at,
                    # No ``opted_out_at``: the ban is not in force any more, and
                    # the schema pairs that column with the ``opted_out`` state.
                    last_stop_observed_at=stop_at,
                    last_stop_event_at=stop_at,
                    last_start_observed_at=start_at,
                    last_start_event_at=start_at,
                    banned_easyweek_customer_uuid=uuid_module.UUID(customer_uuid),
                    banned_uuid_recorded_at=stop_at,
                )
            )


async def test_a_stored_stop_on_a_number_the_card_dropped_still_blocks(session_maker, binding_key, no_network) -> None:
    """Review round 3, finding 1: the scan must be asked what the veto will ask.

    The mailing went to the person's current number. Their old number is carried
    by no card at all — it survives only in the consent ledger, through the uuid
    the ban was bound to when it was applied there. A raw «стоп» with no stated
    time of its own is sitting on that old number with no receipt, so nothing has
    accounted for it, and no opt-in can be ordered against it.

    The veto asked about the whole aggregate and would have found it. The SCAN
    was handed a set assembled from the cards, never saw the event, and so had
    nothing to report.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    who = person(1)
    dropped = "+4915100111222"
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    await _uuid_bound_opt_in(
        session_maker,
        address=dropped,
        customer_uuid=who["customer_uuid"],
        stop_at=ATTEMPTED_AT - timedelta(days=40),
        start_at=ATTEMPTED_AT - timedelta(days=30),
    )
    await _stored_event(
        session_maker,
        dedupe_key="dropped-number-stop",
        address=dropped,
        text="стоп",
        payload={
            "entry": [
                {
                    "changes": [
                        {
                            "value": {
                                "metadata": {"phone_number_id": PHONE_NUMBER_ID},
                                "messages": [
                                    {
                                        "from": dropped.lstrip("+"),
                                        "id": "wamid.dropped-number",
                                        "type": "text",
                                        "text": {"body": "стоп"},
                                    }
                                ],
                            }
                        }
                    ]
                }
            ]
        },
    )
    # The scan is now asked about the dropped number, because the aggregate is.
    async with session_maker() as session:
        scan = await scan_historical_refusals(
            session,
            addresses=await preview_module._candidate_addresses(session, batch_id=batch_id),
        )
    assert scan.complete is True
    assert scan.blocks(dropped) is True
    outcome = await build(session_maker, batch_id=batch_id)
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_EXCLUDED
    assert reason_for(outcome, 1) == HISTORICAL_STOP_UNACCOUNTED


async def test_a_proven_newer_start_on_the_dropped_number_lifts_it(session_maker, binding_key, no_network) -> None:
    """The positive control: the same shape, with the order provable.

    The refusal on the dropped number states its own time, and the opt-in in
    force on that same number is strictly newer. §46.4.7 lets exactly that lift
    it, so the person is sendable — and the widening that found the number is
    still only a ban extension.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    who = person(1)
    dropped = "+4915100111333"
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    stop_at = ATTEMPTED_AT - timedelta(days=40)
    await _uuid_bound_opt_in(
        session_maker,
        address=dropped,
        customer_uuid=who["customer_uuid"],
        stop_at=stop_at,
        start_at=ATTEMPTED_AT - timedelta(days=30),
    )
    await _stored_event(
        session_maker,
        dedupe_key="dropped-number-stop-dated",
        address=dropped,
        text="стоп",
        received=stop_at,
    )
    outcome = await build(session_maker, batch_id=batch_id)
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED


async def test_the_dropped_number_scan_stays_bounded_and_fails_closed(session_maker, binding_key, no_network) -> None:
    """The widened question does not widen the scan's licence to conclude."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    who = person(1)
    dropped = "+4915100111444"
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    await _uuid_bound_opt_in(
        session_maker,
        address=dropped,
        customer_uuid=who["customer_uuid"],
        stop_at=ATTEMPTED_AT - timedelta(days=40),
        start_at=ATTEMPTED_AT - timedelta(days=30),
    )
    await _stored_event(session_maker, dedupe_key="noise-a", address="+4915100777771", text="guten Tag")
    await _stored_event(session_maker, dedupe_key="noise-b", address="+4915100777772", text="guten Tag")
    async with session_maker() as session:
        scan = await scan_historical_refusals(session, addresses=[dropped], limit=1)
    assert (scan.complete, scan.truncated, scan.scanned_events) == (False, True, 1)
    outcome = await preview_module.create_preview(
        session_maker,
        spec=preview_module.PreviewSpec(
            batch_id=batch_id,
            groups=(FOLLOWUP_GROUP_DELIVERED,),
            min_interval_seconds=FOURTEEN_DAYS,
            operator_principal="operator@example.test:fp",
        ),
        booking_reader=FakeHistoryReader(),
        template_reader=TemplateReader(),
        now=NOW,
        historical_event_limit=1,
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
    assert reason_for(outcome, 1) == HISTORICAL_CONSENT_UNPROVEN


async def test_a_historical_refusal_is_not_answered_by_an_opt_in_elsewhere(
    session_maker, binding_key, no_network
) -> None:
    """Review round 3, finding 2, through a real preview.

    The campaign snapshot refuses the number this slot writes to. A proven
    opt-in exists for the same person on ANOTHER number of theirs. §46.4.7 lets
    an opt-in lift the refusal on the number it arrived from; collapsing the
    opt-ins into one high-water mark for the person let B's resubscribe answer
    A's refusal and put A back in the audience.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    who = person(1)
    other = "+4915100222555"
    batch_id, _ = await seed_source_batch(
        session_maker,
        slots=[{"state": "delivered", "recipient_opted_out": True}],
    )
    # Newer than the snapshot by the database's own clock, which is what
    # stamps ``CampaignRecipient.created_at``.
    await _uuid_bound_opt_in(
        session_maker,
        address=other,
        customer_uuid=who["customer_uuid"],
        stop_at=ATTEMPTED_AT - timedelta(days=40),
        start_at=utcnow() + timedelta(hours=1),
    )
    outcome = await build(session_maker, batch_id=batch_id)
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_EXCLUDED
    assert reason_for(outcome, 1) == CONSENT_REFUSED


async def test_a_proven_opt_in_on_the_same_number_does_answer_its_snapshot(
    session_maker, binding_key, no_network
) -> None:
    """The control: on its own number, a newer proven opt-in still answers it."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    who = person(1)
    batch_id, _ = await seed_source_batch(
        session_maker,
        slots=[{"state": "delivered", "recipient_opted_out": True}],
    )
    await _uuid_bound_opt_in(
        session_maker,
        address=who["phone"],
        customer_uuid=who["customer_uuid"],
        stop_at=ATTEMPTED_AT - timedelta(days=40),
        start_at=utcnow() + timedelta(hours=1),
    )
    outcome = await build(session_maker, batch_id=batch_id)
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED


async def test_the_backlog_scan_writes_nothing_at_all(session_maker, binding_key, no_network) -> None:
    """§46.4.4 keeps the projection a separate explicit operation."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    await _stored_event(session_maker, dedupe_key="legacy-8", address=person(1)["phone"], text="стоп")
    before = await _side_effect_counters(session_maker)
    await build(session_maker, batch_id=batch_id)
    await build(session_maker, batch_id=batch_id)
    after = await _side_effect_counters(session_maker)
    async with session_maker() as session:
        suppressions = (
            await session.execute(select(func.count()).select_from(WhatsAppConsentSuppression))
        ).scalar_one()
        receipts = (await session.execute(select(func.count()).select_from(WhatsAppInboundReceipt))).scalar_one()
        flagged = (
            await session.execute(select(func.count()).select_from(Client).where(Client.wa_opted_out.is_(True)))
        ).scalar_one()
    assert after["items"] == before["items"]
    # No suppression written, no receipt backfilled, no card flag projected.
    assert (suppressions, receipts, flagged) == (0, 0, 0)


async def test_the_stored_snapshot_records_the_backlog_scan(session_maker, binding_key, no_network) -> None:
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    await build(session_maker, batch_id=batch_id)
    async with session_maker() as session:
        stored = (await session.execute(select(EasyWeekVoucherFollowupPreview))).scalar_one()
    facts = stored.evidence["historical_consent"]
    assert facts["complete"] is True
    assert facts["unaccounted_count"] == 0
    assert "scanned_events" in facts


# ---------------------------------------------------------------------------
# Proven delivered/read (§46.3)
# ---------------------------------------------------------------------------


def _status(**changes: Any) -> dict[str, Any]:
    """One status callback as the worker hands it to the evidence writer."""
    base: dict[str, Any] = {
        "provider_message_id": "wamid.b1s1",
        "phone_number_id": PHONE_NUMBER_ID,
        "recipient_id": person(1)["phone"],
        "event_at": ATTEMPTED_AT + timedelta(minutes=1),
    }
    base.update(changes)
    return base


@pytest.mark.parametrize("kind", ["delivered", "read"])
async def test_a_signed_callback_on_the_frozen_route_leaves_proof(
    session_maker, binding_key, no_network, kind: str
) -> None:
    from altegio_bot.campaigns.easyweek_voucher_production import delivery_evidence

    await seed_sender_and_template(session_maker, with_followup_template=True)
    await seed_source_batch(session_maker, slots=[{"state": kind, "receipt_proven": False}])
    async with session_maker() as session:
        async with session.begin():
            outcome = await delivery_evidence.record_status_receipt(
                session, status=_status(), kind=kind, signature_verified=True
            )
    assert outcome.stored is True
    async with session_maker() as session:
        rows = list((await session.execute(select(EasyWeekVoucherProductionDeliveryReceipt))).scalars())
    assert [row.kind for row in rows] == [kind]
    assert rows[0].authenticity == INBOUND_AUTHENTICITY_SIGNED
    assert rows[0].route_proven is True


@pytest.mark.parametrize(
    "changes,signed",
    [
        ({}, False),
        ({"phone_number_id": "999999999999999"}, True),
        ({"recipient_id": "+4915100099999"}, True),
        ({"phone_number_id": None, "recipient_id": None}, True),
        ({"provider_message_id": ""}, True),
    ],
)
async def test_a_callback_without_the_three_proofs_leaves_none(
    session_maker, binding_key, no_network, changes: dict[str, Any], signed: bool
) -> None:
    """An unverified signature, a foreign sender, a foreign recipient, or a
    callback that names no route at all. None of them is evidence, and the
    stricter check is not bypassable by omitting the fields."""
    from altegio_bot.campaigns.easyweek_voucher_production import delivery_evidence

    await seed_sender_and_template(session_maker, with_followup_template=True)
    await seed_source_batch(session_maker, slots=[{"state": "delivered", "receipt_proven": False}])
    async with session_maker() as session:
        async with session.begin():
            outcome = await delivery_evidence.record_status_receipt(
                session, status=_status(**changes), kind="delivered", signature_verified=signed
            )
    assert outcome.stored is False
    assert outcome.reason is not None
    async with session_maker() as session:
        stored = (
            await session.execute(select(func.count()).select_from(EasyWeekVoucherProductionDeliveryReceipt))
        ).scalar_one()
    assert stored == 0


async def test_the_same_callback_twice_stores_one_proof(session_maker, binding_key, no_network) -> None:
    from altegio_bot.campaigns.easyweek_voucher_production import delivery_evidence

    await seed_sender_and_template(session_maker, with_followup_template=True)
    await seed_source_batch(session_maker, slots=[{"state": "delivered", "receipt_proven": False}])
    for _ in range(3):
        async with session_maker() as session:
            async with session.begin():
                await delivery_evidence.record_status_receipt(
                    session, status=_status(), kind="delivered", signature_verified=True
                )
    async with session_maker() as session:
        stored = (
            await session.execute(select(func.count()).select_from(EasyWeekVoucherProductionDeliveryReceipt))
        ).scalar_one()
    assert stored == 1


async def test_a_historical_status_without_proof_is_unknown(session_maker, binding_key, no_network) -> None:
    """The reproduction: the slot says delivered and nothing proves it.

    Never demoted to another group either — that would state something weaker
    than the mailing's own record and stronger than what is proven.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered", "receipt_proven": False}])
    outcome = await build(session_maker, batch_id=batch_id)
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
    assert reason_for(outcome, 1) == STATUS_EVIDENCE_UNPROVEN


async def test_a_historical_read_without_proof_is_unknown(session_maker, binding_key, no_network) -> None:
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "read", "receipt_proven": False}])
    outcome = await build(session_maker, batch_id=batch_id, groups=(FOLLOWUP_GROUP_READ, FOLLOWUP_GROUP_DELIVERED))
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
    assert reason_for(outcome, 1) == STATUS_EVIDENCE_UNPROVEN


async def test_a_proof_for_another_slots_route_does_not_carry(session_maker, binding_key, no_network) -> None:
    """A proof is bound to the route the attempt froze, not to the batch."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(
        session_maker,
        slots=[
            {"state": "delivered", "index": 1},
            {"state": "delivered", "index": 2, "receipt_proven": False},
        ],
    )
    outcome = await build(session_maker, batch_id=batch_id)
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED
    assert reason_for(outcome, 2) == STATUS_EVIDENCE_UNPROVEN


async def test_the_status_path_still_advances_a_slot_without_proof(session_maker, binding_key, no_network) -> None:
    """§46.3 does not weaken the MAILING's own record.

    An unsigned callback still moves the slot — dropping a status about a
    message already sent would corrupt the history of a mailing that happened —
    and simply leaves no proof for a later send to rest on.
    """
    from altegio_bot.campaigns.easyweek_voucher_production import ledger as production_ledger_module

    await seed_sender_and_template(session_maker, with_followup_template=True)
    await seed_source_batch(session_maker, slots=[{"state": "accepted", "receipt_proven": False}])
    async with session_maker() as session:
        async with session.begin():
            result = await production_ledger_module.apply_webhook_transition(
                session, provider_message_id="wamid.b1s1", status="delivered"
            )
    assert result.applied is True
    async with session_maker() as session:
        item = (await session.execute(select(EasyWeekVoucherProductionBatchItem))).scalar_one()
        receipts = (
            await session.execute(select(func.count()).select_from(EasyWeekVoucherProductionDeliveryReceipt))
        ).scalar_one()
    assert item.status == "delivered"
    assert item.delivered_at is not None
    assert receipts == 0


# ---------------------------------------------------------------------------
# The ORIGINAL artifact, read and proven (§46.6)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("schema", ["3", "4"])
async def test_a_correct_original_artifact_proves_out(session_maker, binding_key, no_network, schema: str) -> None:
    """Both fixed products, each under its OWN contract.

    Schema 3 is the paid €10 product and schema 4 the free gift certificate;
    their prices and settlement differ, and §46.6 forbids applying one's
    conditions to the other.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}], schema_version=schema)
    reader = FakeHistoryReader(schema_version=schema)
    outcome = await build(session_maker, batch_id=batch_id, reader=reader)
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED
    assert reader.order_calls == [order_uuid_for(batch_id, 1)]
    async with session_maker() as session:
        row = (await session.execute(select(EasyWeekVoucherFollowupPreviewRow))).scalar_one()
    # The stored row keeps the shape facts, and the verified binding among them.
    assert row.evidence["artifact"]["binding_verified"] is True
    assert row.evidence["artifact"]["schema_version"] == schema


async def test_a_schema_4_order_does_not_prove_a_schema_3_slot(session_maker, binding_key, no_network) -> None:
    """The historical contract keeps its own conditions."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}], schema_version="3")
    # The order describes the FREE gift product while the slot is the paid one.
    reader = FakeHistoryReader(schema_version="4")
    outcome = await build(session_maker, batch_id=batch_id, reader=reader)
    assert decision_for(outcome, 1) != FOLLOWUP_ROW_INCLUDED


@pytest.mark.parametrize(
    "changes,expected",
    [
        ({"status": "refunded"}, ENTITLEMENT_INVALID),
        ({"status": "cancelled"}, ENTITLEMENT_INVALID),
        ({"status": "something_new"}, ENTITLEMENT_UNPROVEN),
    ],
)
async def test_an_order_that_says_the_voucher_is_gone_excludes(
    session_maker, binding_key, no_network, changes: dict[str, Any], expected: str
) -> None:
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    reader = FakeHistoryReader(order_changes=changes)
    outcome = await build(session_maker, batch_id=batch_id, reader=reader)
    assert reason_for(outcome, 1) == expected


@pytest.mark.parametrize(
    "voucher_changes",
    [
        {"is_expired": True},
        {"status": "expired"},
        {"expires_at": "2026-09-15T00:00:00+00:00"},
        {"valid_until": "2026-09-15T00:00:00+00:00"},
    ],
)
async def test_an_explicitly_expired_artifact_excludes(
    session_maker, binding_key, no_network, voucher_changes: dict[str, Any]
) -> None:
    """The provider saying so, which is a different fact from it saying nothing."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    order = voucher_order(
        order_uuid=order_uuid_for(batch_id, 1),
        marker=f"marker-{batch_id}-1",
        customer_uuid=who["customer_uuid"],
        code=voucher_code_for(1),
    )
    order["vouchers"][0].update(voucher_changes)
    reader = FakeHistoryReader(orders={order_uuid_for(batch_id, 1): order})
    outcome = await build(session_maker, batch_id=batch_id, reader=reader)
    assert reason_for(outcome, 1) == ENTITLEMENT_INVALID


@pytest.mark.parametrize("schema", ["3", "4"])
async def test_an_open_unpaid_order_is_not_settled(session_maker, binding_key, no_network, schema: str) -> None:
    """Review round 2, finding 4: a code that exists is not a settled order.

    Everything else about the body is right — the source bindings, the voucher
    line, the real MAC — and the order was never paid for. The check read the
    state and refused only ``refunded``/``cancelled``/``unknown``, so an
    ``open``, ``is_paid: false`` order went through and the recipient was
    included.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}], schema_version=schema)
    who = person(1)
    order = voucher_order(
        order_uuid=order_uuid_for(batch_id, 1),
        marker=f"marker-{batch_id}-1",
        customer_uuid=who["customer_uuid"],
        code=voucher_code_for(1),
        schema_version=schema,
        status="open",
    )
    order["is_paid"] = False
    reader = FakeHistoryReader(orders={order_uuid_for(batch_id, 1): order}, schema_version=schema)
    outcome = await build(session_maker, batch_id=batch_id, reader=reader)
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
    assert reason_for(outcome, 1) == ENTITLEMENT_SETTLEMENT_UNPROVEN


@pytest.mark.parametrize("schema", ["3", "4"])
async def test_a_correctly_settled_order_of_either_schema_proves_out(
    session_maker, binding_key, no_network, schema: str
) -> None:
    """Each schema keeps its own price, and a correct one still proves out."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}], schema_version=schema)
    reader = FakeHistoryReader(schema_version=schema)
    outcome = await build(session_maker, batch_id=batch_id, reader=reader)
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED
    async with session_maker() as session:
        row = (await session.execute(select(EasyWeekVoucherFollowupPreviewRow))).scalar_one()
    assert row.evidence["artifact"]["settlement_proven"] is True


async def test_a_paid_order_whose_totals_contradict_the_contract_is_not_settled(
    session_maker, binding_key, no_network
) -> None:
    """Review round 2, finding 4, second half: the amounts are proven too."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}], schema_version="3")
    who = person(1)
    order = voucher_order(
        order_uuid=order_uuid_for(batch_id, 1),
        marker=f"marker-{batch_id}-1",
        customer_uuid=who["customer_uuid"],
        code=voucher_code_for(1),
        schema_version="3",
    )
    # Paid, and for a different amount than this schema's contract names.
    order["total"] = 9999
    reader = FakeHistoryReader(orders={order_uuid_for(batch_id, 1): order}, schema_version="3")
    outcome = await build(session_maker, batch_id=batch_id, reader=reader)
    assert reason_for(outcome, 1) == ENTITLEMENT_SETTLEMENT_UNPROVEN


async def test_a_paid_order_publishing_no_total_at_all_is_not_settled(session_maker, binding_key, no_network) -> None:
    """A missing amount is not an amount that agrees."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}], schema_version="3")
    who = person(1)
    order = voucher_order(
        order_uuid=order_uuid_for(batch_id, 1),
        marker=f"marker-{batch_id}-1",
        customer_uuid=who["customer_uuid"],
        code=voucher_code_for(1),
        schema_version="3",
    )
    order.pop("total")
    order.pop("subtotal")
    reader = FakeHistoryReader(orders={order_uuid_for(batch_id, 1): order}, schema_version="3")
    outcome = await build(session_maker, batch_id=batch_id, reader=reader)
    assert reason_for(outcome, 1) == ENTITLEMENT_SETTLEMENT_UNPROVEN


async def test_a_free_issue_funded_from_an_account_is_not_a_free_issue(session_maker, binding_key, no_network) -> None:
    """Schema 4's zero totals are necessary and never sufficient."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    order = voucher_order(
        order_uuid=order_uuid_for(batch_id, 1),
        marker=f"marker-{batch_id}-1",
        customer_uuid=who["customer_uuid"],
        code=voucher_code_for(1),
    )
    order["account_paid_amount"] = 1000
    reader = FakeHistoryReader(orders={order_uuid_for(batch_id, 1): order})
    outcome = await build(session_maker, batch_id=batch_id, reader=reader)
    assert reason_for(outcome, 1) == ENTITLEMENT_SETTLEMENT_UNPROVEN


async def test_a_zero_order_that_is_merely_open_is_not_a_gift_already_given(
    session_maker, binding_key, no_network
) -> None:
    """The case zero amounts cannot distinguish: a draft and a settled gift."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    order = voucher_order(
        order_uuid=order_uuid_for(batch_id, 1),
        marker=f"marker-{batch_id}-1",
        customer_uuid=who["customer_uuid"],
        code=voucher_code_for(1),
        status="open",
    )
    order["is_paid"] = False
    order["amount_due"] = 0
    order["amount_paid"] = 0
    reader = FakeHistoryReader(orders={order_uuid_for(batch_id, 1): order})
    outcome = await build(session_maker, batch_id=batch_id, reader=reader)
    assert reason_for(outcome, 1) == ENTITLEMENT_SETTLEMENT_UNPROVEN


async def test_an_artifact_with_no_individual_dates_still_proves_out(session_maker, binding_key, no_network) -> None:
    """§45.4: silence about dates is the ordinary shape of a correct voucher."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    order = voucher_order(
        order_uuid=order_uuid_for(batch_id, 1),
        marker=f"marker-{batch_id}-1",
        customer_uuid=who["customer_uuid"],
        code=voucher_code_for(1),
    )
    assert "expires_at" not in order["vouchers"][0]
    assert "valid_until" not in order["vouchers"][0]
    reader = FakeHistoryReader(orders={order_uuid_for(batch_id, 1): order})
    outcome = await build(session_maker, batch_id=batch_id, reader=reader)
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED


async def test_another_order_under_the_same_uuid_is_not_this_artifact(session_maker, binding_key, no_network) -> None:
    """A 200 is not proof the body describes what was asked about."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    other = voucher_order(
        order_uuid="99999999-3333-4333-8333-333333333333",
        marker=f"marker-{batch_id}-1",
        customer_uuid=who["customer_uuid"],
        code=voucher_code_for(1),
    )
    reader = FakeHistoryReader(orders={order_uuid_for(batch_id, 1): other})
    outcome = await build(session_maker, batch_id=batch_id, reader=reader)
    assert reason_for(outcome, 1) == ENTITLEMENT_UNREADABLE


async def test_a_rewritten_marker_is_not_this_phases_order(session_maker, binding_key, no_network) -> None:
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    reader = FakeHistoryReader(order_changes={"comment": "somebody else's order"})
    outcome = await build(session_maker, batch_id=batch_id, reader=reader)
    assert reason_for(outcome, 1) == ENTITLEMENT_UNPROVEN


async def test_a_code_that_does_not_match_the_stored_mac_is_unproven(session_maker, binding_key, no_network) -> None:
    """The real HMAC check, not a key-id comparison.

    Everything else about the order is correct: the right uuid, the right
    marker, the right customer, the right voucher line. Only the code differs,
    and a verdict that only compared key identifiers would include this person.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    reader = FakeHistoryReader(order_changes={})
    who = person(1)
    order = voucher_order(
        order_uuid=order_uuid_for(batch_id, 1),
        marker=f"marker-{batch_id}-1",
        customer_uuid=who["customer_uuid"],
        code="FOLLOWUP-TEST-CODE-9999",
    )
    reader.orders[order_uuid_for(batch_id, 1)] = order
    outcome = await build(session_maker, batch_id=batch_id, reader=reader)
    assert reason_for(outcome, 1) == ENTITLEMENT_UNPROVEN
    async with session_maker() as session:
        row = (await session.execute(select(EasyWeekVoucherFollowupPreviewRow))).scalar_one()
    assert row.evidence["artifact"]["binding_verified"] is False


async def test_the_same_key_id_with_a_different_key_is_unproven(
    session_maker, binding_key, no_network, monkeypatch
) -> None:
    """A MAC made under another secret, with the identifier unchanged."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    # Same id, different secret: the stored MAC was computed with the first.
    monkeypatch.setattr(settings, "easyweek_voucher_delivery_hmac_key", SecretStr("q" * 48), raising=False)
    outcome = await build(session_maker, batch_id=batch_id)
    assert reason_for(outcome, 1) == ENTITLEMENT_UNPROVEN


async def test_a_rotated_key_id_is_unproven_before_any_read(session_maker, binding_key, no_network) -> None:
    """A binding written under a key this deployment no longer holds.

    Caught locally, so the preview spends no request on it.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered", "hmac_key_id": "an-older-key"}])
    reader = FakeHistoryReader()
    outcome = await build(session_maker, batch_id=batch_id, reader=reader)
    assert reason_for(outcome, 1) == ENTITLEMENT_UNPROVEN
    assert reader.order_calls == []


@pytest.mark.parametrize("failure", ["timeout", "unreadable"])
async def test_an_unreadable_order_is_unknown_not_valid(session_maker, binding_key, no_network, failure: str) -> None:
    from altegio_bot.easyweek_client import EasyWeekRetryableError

    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    error: Exception = (
        EasyWeekRetryableError("timeout", operation="get_order")
        if failure == "timeout"
        else RuntimeError("truncated body")
    )
    reader = FakeHistoryReader(order_error=error)
    outcome = await build(session_maker, batch_id=batch_id, reader=reader)
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
    assert reason_for(outcome, 1) == ENTITLEMENT_UNREADABLE


async def test_the_artifact_proof_makes_no_mutation_and_no_send(session_maker, binding_key, no_network) -> None:
    """§46.6: no CREATE, no PAY, no refund, no replacement code, no send."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    before = await _side_effect_counters(session_maker)

    class MutationRefusingReader(FakeHistoryReader):
        async def create_order(self, *args: Any, **kwargs: Any) -> Any:  # pragma: no cover
            raise AssertionError("the preview must not create an order")

        async def pay_order(self, *args: Any, **kwargs: Any) -> Any:  # pragma: no cover
            raise AssertionError("the preview must not pay an order")

        async def refund_order(self, *args: Any, **kwargs: Any) -> Any:  # pragma: no cover
            raise AssertionError("the preview must not refund an order")

    outcome = await build(session_maker, batch_id=batch_id, reader=MutationRefusingReader())
    assert outcome.created is True
    after = await _side_effect_counters(session_maker)
    assert after["items"] == before["items"]
    assert (after["jobs"], after["outbox"], after["attempts"]) == (
        before["jobs"],
        before["outbox"],
        before["attempts"],
    )


async def test_the_order_read_happens_with_no_session_open(session_maker, binding_key, no_network) -> None:
    """The MAC check opens its own session only after the order came back."""
    from sqlalchemy import text

    probed: list[bool] = []

    class ProbingOrderReader(FakeHistoryReader):
        async def get_order(self, order_uuid: str) -> dict[str, Any]:
            async with session_maker() as probe:
                async with probe.begin():
                    await probe.execute(text("SELECT 1 FROM easyweek_voucher_production_batch_items FOR UPDATE NOWAIT"))
            probed.append(True)
            return await super().get_order(order_uuid)

    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    outcome = await build(session_maker, batch_id=batch_id, reader=ProbingOrderReader())
    assert outcome.created is True
    assert probed == [True]


# ---------------------------------------------------------------------------
# The full re-check before the snapshot (§46.5)
# ---------------------------------------------------------------------------


class _DuringRead(FakeHistoryReader):
    """A booking read that performs one local change while it is in flight.

    The window §46.5 is about: the external proof takes time, and the local
    facts behind the composition can move while it does.
    """

    def __init__(self, session_maker: Any, change: Any, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.session_maker = session_maker
        self.change = change
        self.fired = False

    async def list_customer_bookings(self, customer_uuid: str, page: int, per_page: int = 50) -> dict[str, Any]:
        if not self.fired:
            self.fired = True
            await self.change(self.session_maker)
        return await super().list_customer_bookings(customer_uuid, page, per_page)


async def _arrive_read_callback(session_maker: Any) -> None:
    """A proven ``read`` lands for slot 1 while the bookings are being read."""
    from altegio_bot.campaigns.easyweek_voucher_production import delivery_evidence
    from altegio_bot.campaigns.easyweek_voucher_production import ledger as production_ledger_module

    async with session_maker() as session:
        async with session.begin():
            await production_ledger_module.apply_webhook_transition(
                session, provider_message_id="wamid.b1s1", status="read"
            )
            await delivery_evidence.record_status_receipt(
                session,
                status=_status(event_at=ATTEMPTED_AT + timedelta(minutes=5)),
                kind="read",
                signature_verified=True,
            )


async def test_a_late_read_is_excluded_when_only_delivered_was_selected(session_maker, binding_key, no_network) -> None:
    """The review reproduction.

    The operator asked for «доставлено, прочтение не подтверждено». While the
    bookings were being read the read receipt arrived, so this person is no
    longer in that group — and the snapshot must not store them as if they were.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    reader = _DuringRead(session_maker, _arrive_read_callback)
    outcome = await build(session_maker, batch_id=batch_id, groups=(FOLLOWUP_GROUP_DELIVERED,), reader=reader)
    assert outcome.created is True
    assert outcome.included == 0
    assert reason_for(outcome, 1) == GROUP_MOVED_OUT_OF_SELECTION
    async with session_maker() as session:
        row = (await session.execute(select(EasyWeekVoucherFollowupPreviewRow))).scalar_one()
    assert row.decision == FOLLOWUP_ROW_EXCLUDED


async def test_a_late_read_moves_between_two_selected_groups(session_maker, binding_key, no_network) -> None:
    """§46.5 allows the move when both groups were chosen and share the text.

    The row stays included and the stored group is the ACTUAL one, so the
    snapshot does not claim a status the evidence no longer supports.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    reader = _DuringRead(session_maker, _arrive_read_callback)
    outcome = await build(
        session_maker,
        batch_id=batch_id,
        groups=(FOLLOWUP_GROUP_READ, FOLLOWUP_GROUP_DELIVERED),
        reader=reader,
    )
    assert outcome.included == 1
    async with session_maker() as session:
        row = (await session.execute(select(EasyWeekVoucherFollowupPreviewRow))).scalar_one()
    assert row.decision == FOLLOWUP_ROW_INCLUDED
    assert row.group_code == FOLLOWUP_GROUP_READ


async def _move_the_number(session_maker: Any) -> None:
    async with session_maker() as session:
        async with session.begin():
            card = (
                await session.execute(select(Client).where(Client.easyweek_customer_uuid.is_not(None)).limit(1))
            ).scalar_one()
            card.phone_e164 = "+4915100055555"


async def test_an_address_that_moves_during_the_read_is_caught(session_maker, binding_key, no_network) -> None:
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    reader = _DuringRead(session_maker, _move_the_number)
    outcome = await build(session_maker, batch_id=batch_id, reader=reader)
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
    assert reason_for(outcome, 1) == IDENTITY_CONFLICT


async def _adopt_the_uuid(session_maker: Any) -> None:
    async with session_maker() as session:
        async with session.begin():
            card = (
                await session.execute(select(Client).where(Client.easyweek_customer_uuid.is_not(None)).limit(1))
            ).scalar_one()
            card.easyweek_customer_uuid = uuid_module.UUID("33333333-1111-4111-8111-111111111111")


async def test_a_uuid_binding_that_moves_during_the_read_is_caught(session_maker, binding_key, no_network) -> None:
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    reader = _DuringRead(session_maker, _adopt_the_uuid)
    outcome = await build(session_maker, batch_id=batch_id, reader=reader)
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
    assert reason_for(outcome, 1) == IDENTITY_CONFLICT


async def _disable_the_sender(session_maker: Any) -> None:
    from altegio_bot.models.models import WhatsAppSender

    async with session_maker() as session:
        async with session.begin():
            sender = (await session.execute(select(WhatsAppSender))).scalar_one()
            sender.is_active = False


async def test_a_sender_disabled_during_the_read_refuses_the_save(session_maker, binding_key, no_network) -> None:
    """A contract fact of the whole preview, so the save is refused."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    reader = _DuringRead(session_maker, _disable_the_sender)
    outcome = await build(session_maker, batch_id=batch_id, reader=reader)
    assert (outcome.created, outcome.reason) == (False, SENDER_UNPROVEN)
    async with session_maker() as session:
        stored = (await session.execute(select(func.count()).select_from(EasyWeekVoucherFollowupPreview))).scalar_one()
    assert stored == 0


async def _repoint_the_sender(session_maker: Any) -> None:
    from altegio_bot.models.models import WhatsAppSender

    async with session_maker() as session:
        async with session.begin():
            sender = (await session.execute(select(WhatsAppSender))).scalar_one()
            sender.phone_number_id = "222222222222222"


async def test_a_sender_repointed_during_the_read_refuses_the_save(session_maker, binding_key, no_network) -> None:
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    reader = _DuringRead(session_maker, _repoint_the_sender)
    outcome = await build(session_maker, batch_id=batch_id, reader=reader)
    assert (outcome.created, outcome.reason) == (False, SENDER_UNPROVEN)


async def _disable_the_template(session_maker: Any) -> None:
    from altegio_bot.models.models import MessageTemplate

    async with session_maker() as session:
        async with session.begin():
            row = (
                await session.execute(
                    select(MessageTemplate).where(MessageTemplate.code == message.FOLLOWUP_TEMPLATE_CODE)
                )
            ).scalar_one()
            row.is_active = False


async def test_a_template_disabled_during_the_read_refuses_the_save(session_maker, binding_key, no_network) -> None:
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    reader = _DuringRead(session_maker, _disable_the_template)
    outcome = await build(session_maker, batch_id=batch_id, reader=reader)
    assert (outcome.created, outcome.reason) == (False, TEMPLATE_UNPROVEN)
    async with session_maker() as session:
        stored = (await session.execute(select(func.count()).select_from(EasyWeekVoucherFollowupPreview))).scalar_one()
    assert stored == 0


async def _rewrite_the_template_body(session_maker: Any) -> None:
    from altegio_bot.models.models import MessageTemplate

    async with session_maker() as session:
        async with session.begin():
            row = (
                await session.execute(
                    select(MessageTemplate).where(MessageTemplate.code == message.FOLLOWUP_TEMPLATE_CODE)
                )
            ).scalar_one()
            row.body = "Hallo {client_name}, etwas ganz anderes: {booking_link}."


async def test_a_template_body_changed_during_the_read_refuses_the_save(session_maker, binding_key, no_network) -> None:
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    reader = _DuringRead(session_maker, _rewrite_the_template_body)
    outcome = await build(session_maker, batch_id=batch_id, reader=reader)
    assert (outcome.created, outcome.reason) == (False, TEMPLATE_UNPROVEN)


async def test_a_row_that_loses_eligibility_is_not_replaced(session_maker, binding_key, no_network) -> None:
    """§46.5: the place of an excluded row is filled by nobody.

    Two slots are selected and one of them says stop during the read. The
    stored composition is smaller — never topped up from the group the operator
    did not choose, and never from the unknown rows.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(
        session_maker,
        slots=[
            {"state": "delivered", "index": 1},
            {"state": "delivered", "index": 2},
            {"state": "read", "index": 3},
        ],
    )

    async def stop_the_first(maker: Any) -> None:
        await _suppress(maker, address=person(1)["phone"])

    reader = _DuringRead(session_maker, stop_the_first)
    outcome = await build(session_maker, batch_id=batch_id, groups=(FOLLOWUP_GROUP_DELIVERED,), reader=reader)
    assert outcome.included == 1
    assert reason_for(outcome, 1) == CONSENT_REFUSED
    assert decision_for(outcome, 2) == FOLLOWUP_ROW_INCLUDED
    # Slot 3 is in a group nobody selected, so it is not part of this preview.
    assert {row.slot for row in outcome.rows} == {1, 2}


async def test_the_rechecks_of_stop_consent_and_reply_still_hold(session_maker, binding_key, no_network) -> None:
    """The three the first round already had, kept alongside the new ones."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])

    async def stop_during(maker: Any) -> None:
        await _suppress(maker, address=person(1)["phone"])

    outcome = await build(session_maker, batch_id=batch_id, reader=_DuringRead(session_maker, stop_during))
    assert reason_for(outcome, 1) == CONSENT_REFUSED

    async def reply_during(maker: Any) -> None:
        await _receipt(
            maker,
            address=person(2)["phone"],
            key="reply-during-recheck",
            observed=ATTEMPTED_AT + timedelta(days=1),
        )

    other_batch, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered", "index": 2}])
    second = await build(
        session_maker,
        batch_id=other_batch,
        reader=_DuringRead(session_maker, reply_during, people={1: 2}),
    )
    assert reason_for(second, 1) == CUSTOMER_REPLIED


async def test_the_database_itself_requires_each_part_of_a_receipt(session_maker, binding_key, no_network) -> None:
    """A stored CONFIRMED receipt IS the proof, enforced by Postgres.

    Three shapes the database refuses outright, so no writer can produce a
    receipt that is not evidence: a confirmed row with no slot, an unmatched row
    that names one, and a row whose signature was never verified.
    """
    from sqlalchemy.exc import IntegrityError

    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "read"}])
    async with session_maker() as session:
        item = (
            await session.execute(
                select(EasyWeekVoucherProductionBatchItem).where(
                    EasyWeekVoucherProductionBatchItem.batch_id == batch_id
                )
            )
        ).scalar_one()
        item_id = int(item.id)

    def receipt(**changes: Any) -> EasyWeekVoucherProductionDeliveryReceipt:
        base: dict[str, Any] = {
            "fact_key": f"check-{uuid_module.uuid4().hex}"[:64],
            "kind": DELIVERY_RECEIPT_KIND_READ,
            "authenticity": INBOUND_AUTHENTICITY_SIGNED,
            "match_state": DELIVERY_RECEIPT_MATCH_CONFIRMED,
            "item_id": item_id,
            "batch_id": batch_id,
            "slot": 1,
            "provider_message_id": f"wamid.check-{uuid_module.uuid4().hex[:8]}",
            "observed_sender_digest": "a" * 64,
            "observed_recipient_digest": "b" * 64,
            "route_proven": True,
            "observed_at": NOW,
            "evidence": {},
        }
        base.update(changes)
        return EasyWeekVoucherProductionDeliveryReceipt(**base)

    refused = [
        # Confirmed, with no slot named.
        {"item_id": None, "batch_id": None, "slot": None},
        # Confirmed, with no route proven.
        {"route_proven": False},
        # Unmatched, yet naming a slot.
        {"match_state": DELIVERY_RECEIPT_MATCH_UNMATCHED, "route_proven": False},
        # Never signed.
        {"authenticity": INBOUND_AUTHENTICITY_UNVERIFIED},
    ]
    for changes in refused:
        async with session_maker() as session:
            with pytest.raises(IntegrityError):
                async with session.begin():
                    session.add(receipt(**changes))
    # And the honest unmatched shape is accepted.
    async with session_maker() as session:
        async with session.begin():
            session.add(
                receipt(
                    match_state=DELIVERY_RECEIPT_MATCH_UNMATCHED,
                    item_id=None,
                    batch_id=None,
                    slot=None,
                    route_proven=False,
                )
            )


async def test_an_unmatched_receipt_is_evidence_for_nobody(session_maker, binding_key, no_network) -> None:
    """It names no slot, so no audience may rest on it."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "read", "receipt_proven": False}])
    async with session_maker() as session:
        async with session.begin():
            session.add(
                EasyWeekVoucherProductionDeliveryReceipt(
                    fact_key="unmatched-not-evidence",
                    kind=DELIVERY_RECEIPT_KIND_READ,
                    authenticity=INBOUND_AUTHENTICITY_SIGNED,
                    match_state=DELIVERY_RECEIPT_MATCH_UNMATCHED,
                    provider_message_id=f"wamid.b{batch_id}s1",
                    observed_sender_digest="a" * 64,
                    observed_recipient_digest="b" * 64,
                    route_proven=False,
                    observed_at=NOW,
                    evidence={},
                )
            )
    outcome = await build(session_maker, batch_id=batch_id, groups=(FOLLOWUP_GROUP_READ,))
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
    assert reason_for(outcome, 1) == STATUS_EVIDENCE_UNPROVEN


@pytest.mark.parametrize(
    ("offset_seconds", "expected"),
    [(-1, FOLLOWUP_ROW_INCLUDED), (0, FOLLOWUP_ROW_INCLUDED), (1, FOLLOWUP_ROW_EXCLUDED)],
)
async def test_the_creation_boundary_is_strictly_after_the_attempt(
    session_maker, binding_key, no_network, offset_seconds: int, expected: str
) -> None:
    """«После исходной попытки» means strictly after, to the second.

    A booking created at the very instant of the attempt is not engagement since
    the attempt; one created a second later is.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    row = booking(
        index=9,
        customer_uuid=who["customer_uuid"],
        start=ATTEMPTED_AT - timedelta(days=1),
        canceled=True,
        completed=False,
    )
    await seed_booking_created(
        session_maker,
        booking_uuid=row["uuid"],
        numeric_customer_id=who["numeric_id"],
        created_at=ATTEMPTED_AT + timedelta(seconds=offset_seconds),
    )
    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: [row]}),
    )
    assert decision_for(outcome, 1) == expected


async def test_a_repeated_creation_delivery_is_one_booking(session_maker, binding_key, no_network) -> None:
    """A Resend of the same creation is the same booking, not a second one."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    row = booking(index=10, customer_uuid=who["customer_uuid"], start=ATTEMPTED_AT - timedelta(days=2))
    made = ATTEMPTED_AT - timedelta(days=3)
    for _ in range(3):
        await seed_booking_created(
            session_maker,
            booking_uuid=row["uuid"],
            numeric_customer_id=who["numeric_id"],
            created_at=made,
        )
    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: [row]}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED


async def test_a_cancellation_delivered_before_its_creation_still_attributes_it(
    session_maker, binding_key, no_network
) -> None:
    """Delivery order is not business order, and the creation still stands.

    The cancellation arrives first, the creation afterwards, and both name the
    same customer. The booking was created after the attempt, so the person is
    excluded — §46.5 counts a booking that was later cancelled.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    row = booking(
        index=11,
        customer_uuid=who["customer_uuid"],
        start=ATTEMPTED_AT + timedelta(days=4),
        canceled=True,
        completed=False,
    )
    await seed_booking_cancelled(
        session_maker,
        booking_uuid=row["uuid"],
        numeric_customer_id=who["numeric_id"],
        received_at=ATTEMPTED_AT + timedelta(days=1),
    )
    await seed_booking_created(
        session_maker,
        booking_uuid=row["uuid"],
        numeric_customer_id=who["numeric_id"],
        created_at=ATTEMPTED_AT + timedelta(hours=6),
        received_at=ATTEMPTED_AT + timedelta(days=2),
    )
    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: [row]}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_EXCLUDED
    assert reason_for(outcome, 1) == NEW_BOOKING_AFTER_ATTEMPT


async def test_a_truncated_creation_scan_is_unknown_for_everybody(
    session_maker, binding_key, no_network, monkeypatch
) -> None:
    """A partial pass over the stored deliveries is not an answer about anybody."""
    from altegio_bot.campaigns.easyweek_voucher_production import booking_attribution

    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    row = booking(index=12, customer_uuid=who["customer_uuid"], start=ATTEMPTED_AT - timedelta(days=2))
    for index, made in enumerate((3, 4, 5)):
        await seed_booking_created(
            session_maker,
            booking_uuid=booking(index=20 + index, customer_uuid=who["customer_uuid"], start=NOW)["uuid"],
            numeric_customer_id=who["numeric_id"],
            created_at=ATTEMPTED_AT - timedelta(days=made),
        )
    monkeypatch.setattr(booking_attribution, "MAX_SCANNED_EVENTS", 1, raising=False)
    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: [row]}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
    assert reason_for(outcome, 1) == BOOKING_CAPTURE_UNPROVEN
    async with session_maker() as session:
        stored = (
            (
                await session.execute(
                    select(EasyWeekVoucherFollowupPreview).order_by(EasyWeekVoucherFollowupPreview.id.desc())
                )
            )
            .scalars()
            .first()
        )
    assert stored.evidence["booking_creation_capture"]["truncated"] is True


# ---------------------------------------------------------------------------
# Who created the booking (§46.5, review round 3)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("made_after_the_attempt", [False, True])
async def test_a_booking_in_their_history_created_by_somebody_else_is_unknown(
    session_maker, binding_key, no_network, made_after_the_attempt: bool
) -> None:
    """Review round 3, finding 3: the creation names an owner, so compare it.

    The booking sits in this person's CURRENT history and its one creation
    delivery named a different customer. Nothing moved it as far as the capture
    can see, so the old rule — distrust only a booking whose deliveries
    contradict each other — credited the creation to whoever holds it now. Their
    own engagement, invented from somebody else's visit to the booking page.

    Checked for a foreign creation both before and after the attempt: neither
    may be attributed, and with no independent reason to exclude the answer is
    unknown rather than either verdict.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    theirs_now = booking(
        index=30,
        customer_uuid=who["customer_uuid"],
        start=ATTEMPTED_AT - timedelta(days=1),
        canceled=True,
        completed=False,
    )
    await seed_booking_created(
        session_maker,
        booking_uuid=theirs_now["uuid"],
        numeric_customer_id=who["numeric_id"] + 77,
        created_at=ATTEMPTED_AT + (timedelta(days=2) if made_after_the_attempt else -timedelta(days=9)),
    )
    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: [theirs_now]}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
    assert reason_for(outcome, 1) == BOOKING_CREATION_UNPROVEN


async def test_two_creations_of_one_booking_do_not_vanish_into_an_empty_history(
    session_maker, binding_key, no_network
) -> None:
    """Review round 3, finding 3: a disputed booking is not a booking that did not happen.

    Two creation deliveries of one booking uuid name two different owners, so
    the capture can attribute neither. It used to drop the booking entirely —
    correct for a conversion report, and here it turned an empty live history
    into «nothing has happened since the mailing» for somebody one of those
    deliveries says did book.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    disputed = booking(index=31, customer_uuid=person(4)["customer_uuid"], start=NOW + timedelta(days=6))
    for owner, made in ((who["numeric_id"], timedelta(days=1)), (who["numeric_id"] + 5, timedelta(days=2))):
        await seed_booking_created(
            session_maker,
            booking_uuid=disputed["uuid"],
            numeric_customer_id=owner,
            created_at=ATTEMPTED_AT + made,
        )
    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: []}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
    assert reason_for(outcome, 1) == BOOKING_CAPTURE_UNPROVEN
    async with session_maker() as session:
        stored = (await session.execute(select(EasyWeekVoucherFollowupPreview))).scalar_one()
    assert stored.evidence["booking_creation_capture"]["disputed_bookings"] == 1


async def test_an_owner_change_with_no_proven_creation_is_unknown(session_maker, binding_key, no_network) -> None:
    """Lifecycle deliveries from two customers and no creation at all.

    The capture can say that something happened to a booking and that two
    customers were named on it, and nothing about who made it or when. That is
    a dispute, not an absence.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    moved = booking(index=32, customer_uuid=person(4)["customer_uuid"], start=NOW + timedelta(days=4))
    await seed_booking_cancelled(
        session_maker,
        booking_uuid=moved["uuid"],
        numeric_customer_id=who["numeric_id"],
        received_at=ATTEMPTED_AT + timedelta(days=1),
    )
    await seed_booking_cancelled(
        session_maker,
        booking_uuid=moved["uuid"],
        numeric_customer_id=who["numeric_id"] + 9,
        received_at=ATTEMPTED_AT + timedelta(days=2),
    )
    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: []}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
    assert reason_for(outcome, 1) == BOOKING_CAPTURE_UNPROVEN


async def test_a_strangers_booking_says_nothing_about_this_recipient(session_maker, binding_key, no_network) -> None:
    """The control: somebody else's perfectly ordinary booking changes nothing."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    stranger = booking(index=33, customer_uuid=person(6)["customer_uuid"], start=NOW + timedelta(days=5))
    await seed_booking_created(
        session_maker,
        booking_uuid=stranger["uuid"],
        numeric_customer_id=who["numeric_id"] + 404,
        created_at=ATTEMPTED_AT + timedelta(days=1),
    )
    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: []}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED


async def test_the_proven_creator_of_that_booking_is_still_excluded(session_maker, binding_key, no_network) -> None:
    """The other control: an undisputed creation of theirs still excludes them."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    theirs = booking(index=34, customer_uuid=person(6)["customer_uuid"], start=NOW + timedelta(days=5))
    await seed_booking_created(
        session_maker,
        booking_uuid=theirs["uuid"],
        numeric_customer_id=who["numeric_id"],
        created_at=ATTEMPTED_AT + timedelta(days=1),
    )
    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: []}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_EXCLUDED
    assert reason_for(outcome, 1) == NEW_BOOKING_AFTER_ATTEMPT


async def test_an_active_future_booking_excludes_even_with_a_disputed_creation(
    session_maker, binding_key, no_network
) -> None:
    """The appointment rule needs no creation evidence, so it still decides."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    ahead = booking(index=35, customer_uuid=who["customer_uuid"], start=NOW + timedelta(days=3), completed=False)
    for owner in (who["numeric_id"], who["numeric_id"] + 3):
        await seed_booking_created(
            session_maker,
            booking_uuid=ahead["uuid"],
            numeric_customer_id=owner,
            created_at=ATTEMPTED_AT + timedelta(days=1),
        )
    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: [ahead]}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_EXCLUDED
    assert reason_for(outcome, 1) == FUTURE_BOOKING_ACTIVE


async def test_a_manual_card_is_unaffected_by_the_owner_comparison(session_maker, binding_key, no_network) -> None:
    """The ordinary manual card keeps working: there is no id to compare with."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(
        session_maker,
        slots=[{"state": "delivered", "basis": RECIPIENT_BASIS_MANUAL, "numeric_id": None}],
    )
    who = person(1)
    theirs = booking(
        index=36,
        customer_uuid=who["customer_uuid"],
        start=ATTEMPTED_AT - timedelta(days=2),
        canceled=True,
        completed=False,
    )
    await seed_booking_created(
        session_maker,
        booking_uuid=theirs["uuid"],
        numeric_customer_id=4711,
        created_at=ATTEMPTED_AT - timedelta(days=4),
    )
    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: [theirs]}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED


# ---------------------------------------------------------------------------
# One snapshot for the historical and the live consent read (round 4, fix 1)
# ---------------------------------------------------------------------------


async def _raw_undated_stop(session_maker, *, dedupe_key: str, address: str) -> None:
    """A stored «стоп» with no stated time, which no opt-in can be ordered against."""
    await _stored_event(
        session_maker,
        dedupe_key=dedupe_key,
        address=address,
        text="стоп",
        payload={
            "entry": [
                {
                    "changes": [
                        {
                            "value": {
                                "metadata": {"phone_number_id": PHONE_NUMBER_ID},
                                "messages": [
                                    {
                                        "from": address.lstrip("+"),
                                        "id": f"wamid.{dedupe_key}",
                                        "type": "text",
                                        "text": {"body": "стоп"},
                                    }
                                ],
                            }
                        }
                    ]
                }
            ]
        },
    )


async def test_a_card_appearing_after_the_addresses_were_chosen_is_not_missed(
    session_maker, binding_key, no_network, monkeypatch
) -> None:
    """Review round 4, finding 1a — a deterministic race on two connections.

    A card of this person's uuid appears, carrying a number with an unprojected
    «стоп», AFTER the address set was chosen and before the first consent read.
    Under READ COMMITTED the scan had already been asked a narrower question
    while the fingerprint the row stored already described the world with the
    new number in it — so the re-check before the write compared equal and
    reported nothing.

    With the whole first phase on one snapshot the fingerprint describes exactly
    the world the row was judged in, and the change is found where it belongs:
    by the re-check, on its own connection, before the version is written.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    appeared = "+4915100333111"

    original = preview_module._candidate_addresses

    async def racing(session, *, batch_id: int):
        chosen = await original(session, batch_id=batch_id)
        # A genuinely independent connection, committed while the preview's own
        # transaction is open.
        async with session_maker() as other:
            async with other.begin():
                other.add(
                    Client(
                        provider=PROVIDER_EASYWEEK,
                        company_id=771,
                        easyweek_customer_uuid=uuid_module.UUID(who["customer_uuid"]),
                        altegio_client_id=None,
                        display_name="Card that appeared mid-read",
                        phone_e164=appeared,
                        raw={},
                    )
                )
        await _raw_undated_stop(session_maker, dedupe_key="race-a-stop", address=appeared)
        return chosen

    monkeypatch.setattr(preview_module, "_candidate_addresses", racing)
    outcome = await build(session_maker, batch_id=batch_id)
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
    assert reason_for(outcome, 1) == CONSENT_UNKNOWN


async def test_without_the_race_the_same_batch_is_included(session_maker, binding_key, no_network) -> None:
    """The baseline the race is measured against."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    outcome = await build(session_maker, batch_id=batch_id)
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED


async def test_a_start_landing_between_the_scan_and_the_decision_keeps_the_row_out(
    session_maker, binding_key, no_network, monkeypatch
) -> None:
    """Review round 4, finding 1b — the other interleaving, same mechanism.

    The scan reads the stored «стоп» as accounted for, because the ledger
    refuses this number at that moment. A proven signed START is then applied on
    another connection, and under READ COMMITTED the consent decision a moment
    later allowed the send — while a separate undated «стоп» remained on that
    number, one no opt-in can be ordered against. The refusal had been accounted
    for by a ledger state that no longer existed.
    """
    from altegio_bot.whatsapp_consent import apply_consent_decision, lock_consent_address

    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    stop_at = ATTEMPTED_AT - timedelta(days=20)
    # The refusal the scan will see in force...
    async with session_maker() as session:
        async with session.begin():
            await lock_consent_address(session, who["phone"])
            await apply_consent_decision(
                session,
                wa_address=who["phone"],
                command=CONSENT_COMMAND_STOP,
                authenticity=INBOUND_AUTHENTICITY_SIGNED,
                source=CONSENT_SOURCE_INBOUND,
                observed_at=stop_at,
                event_at=stop_at,
            )
    # ...and a separate raw refusal on the same number whose own time is unknown.
    await _raw_undated_stop(session_maker, dedupe_key="race-b-stop", address=who["phone"])

    original = preview_module.scan_historical_refusals

    async def racing(session, **kwargs):
        scan = await original(session, **kwargs)
        async with session_maker() as other:
            async with other.begin():
                await lock_consent_address(other, who["phone"])
                lifted = await apply_consent_decision(
                    other,
                    wa_address=who["phone"],
                    command=CONSENT_COMMAND_START,
                    authenticity=INBOUND_AUTHENTICITY_SIGNED,
                    source=CONSENT_SOURCE_INBOUND,
                    observed_at=ATTEMPTED_AT - timedelta(days=10),
                    event_at=ATTEMPTED_AT - timedelta(days=10),
                )
        assert lifted.applied, lifted.reason
        return scan

    monkeypatch.setattr(preview_module, "scan_historical_refusals", racing)
    outcome = await build(session_maker, batch_id=batch_id)
    assert decision_for(outcome, 1) != FOLLOWUP_ROW_INCLUDED
    assert reason_for(outcome, 1) == CONSENT_REFUSED


async def test_a_proven_newer_start_before_the_preview_does_open_the_row(
    session_maker, binding_key, no_network
) -> None:
    """The positive control: the same shape, resolved before the preview runs.

    A strictly newer proven START on the same number, and the stored refusal
    dated so the two can be ordered. Nothing races, the refusal is answered, and
    the person is sendable.
    """
    from altegio_bot.whatsapp_consent import apply_consent_decision, lock_consent_address

    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    stop_at = ATTEMPTED_AT - timedelta(days=20)
    await _stored_event(
        session_maker,
        dedupe_key="race-control-stop",
        address=who["phone"],
        text="стоп",
        received=stop_at,
    )
    for command, moment in (
        (CONSENT_COMMAND_STOP, stop_at),
        (CONSENT_COMMAND_START, ATTEMPTED_AT - timedelta(days=10)),
    ):
        async with session_maker() as session:
            async with session.begin():
                await lock_consent_address(session, who["phone"])
                await apply_consent_decision(
                    session,
                    wa_address=who["phone"],
                    command=command,
                    authenticity=INBOUND_AUTHENTICITY_SIGNED,
                    source=CONSENT_SOURCE_INBOUND,
                    observed_at=moment,
                    event_at=moment,
                )
    outcome = await build(session_maker, batch_id=batch_id)
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED


async def test_a_scan_that_was_asked_less_than_the_veto_needs_is_unknown(
    session_maker, binding_key, no_network, monkeypatch
) -> None:
    """The coverage invariant, asserted rather than assumed.

    Both address sets come from one resolver inside one snapshot, so this holds
    by construction. It is checked anyway: a future change that asked the scan a
    narrower question than the veto is taken against must read as unknown rather
    than as silence.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])

    async def narrower(session, *, batch_id: int):
        return set()

    monkeypatch.setattr(preview_module, "_candidate_addresses", narrower)
    outcome = await build(session_maker, batch_id=batch_id)
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
    assert reason_for(outcome, 1) == HISTORICAL_CONSENT_UNPROVEN


# ---------------------------------------------------------------------------
# An incomplete creation, and whose booking it is (round 4, fixes 2–5)
# ---------------------------------------------------------------------------


async def _proven_other_card(
    session_maker,
    *,
    numeric_id: int,
    customer_uuid: str,
    phone: str,
    company_id: int = COMPANY_ID,
) -> None:
    """A card of this workspace carrying BOTH a numeric id and a proven uuid.

    The link a creation delivery's numeric id is resolved back through. UUID
    first and no phone involved: the number is only there because the column
    exists.

    ``company_id`` is which branch holds the card, because that is what the
    reverse lookup's scope is about: one numeric id is unique per branch, so a
    second card claiming it lives in another branch — configured, and then the
    claim competes, or not, and then it says nothing here at all.
    """
    async with session_maker() as session:
        async with session.begin():
            session.add(
                Client(
                    provider=PROVIDER_EASYWEEK,
                    company_id=int(company_id),
                    easyweek_customer_uuid=uuid_module.UUID(customer_uuid),
                    altegio_client_id=int(numeric_id),
                    display_name="Other proven customer",
                    phone_e164=phone,
                    raw={},
                )
            )


async def test_an_undated_creation_still_names_whose_booking_it_is(session_maker, binding_key, no_network) -> None:
    """Review round 4, finding 2: the proven half of an incomplete creation.

    The branch, the booking identity and the customer are all proven and the
    instant is missing. The delivery used to be dropped whole, so the scan never
    learned that a creation of this booking named this customer — and with the
    person's current history empty, that silence read as "nothing has happened
    since the mailing".
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    contested = booking(index=40, customer_uuid=person(7)["customer_uuid"], start=NOW + timedelta(days=4))
    await seed_booking_created_without_a_date(
        session_maker,
        booking_uuid=contested["uuid"],
        numeric_customer_id=who["numeric_id"],
        received_at=ATTEMPTED_AT + timedelta(days=1),
    )
    # A later delivery of the SAME booking names somebody else.
    await seed_booking_cancelled(
        session_maker,
        booking_uuid=contested["uuid"],
        numeric_customer_id=who["numeric_id"] + 8,
        received_at=ATTEMPTED_AT + timedelta(days=2),
    )
    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: []}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
    assert reason_for(outcome, 1) == BOOKING_CAPTURE_UNPROVEN


@pytest.mark.parametrize("broken", ["foreign_location", "unreadable_uuid", "arbitrary_payload"])
async def test_an_unproven_undated_delivery_widens_nothing(session_maker, binding_key, no_network, broken: str) -> None:
    """A delivery that proves none of the three facts extends no ban."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    target = booking(index=41, customer_uuid=person(7)["customer_uuid"], start=NOW + timedelta(days=4))
    if broken == "foreign_location":
        await seed_booking_created_without_a_date(
            session_maker,
            booking_uuid=target["uuid"],
            numeric_customer_id=who["numeric_id"],
            company_id=999_111,
            received_at=ATTEMPTED_AT + timedelta(days=1),
        )
    elif broken == "unreadable_uuid":
        await seed_booking_created_without_a_date(
            session_maker,
            booking_uuid="not-a-uuid-at-all",
            numeric_customer_id=who["numeric_id"],
            received_at=ATTEMPTED_AT + timedelta(days=1),
        )
    else:
        await _arbitrary_lifecycle_payload(session_maker, received_at=ATTEMPTED_AT + timedelta(days=1))
    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: []}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED


async def _arbitrary_lifecycle_payload(session_maker, *, received_at: datetime) -> None:
    from altegio_bot.models.models import EasyWeekEvent

    async with session_maker() as session:
        async with session.begin():
            session.add(
                EasyWeekEvent(
                    event_hint="booking-created",
                    query={},
                    headers={},
                    payload={"something": "else entirely"},
                    received_at=received_at,
                    status="processed",
                )
            )


@pytest.mark.parametrize("made_after_the_attempt", [False, True])
async def test_a_manual_card_does_not_inherit_a_creation_proven_to_be_anothers(
    session_maker, binding_key, no_network, made_after_the_attempt: bool
) -> None:
    """Review round 4, finding 3: resolve the creation's owner the other way.

    A manual recipient's card carries a proven uuid and no numeric id by design,
    so the forward comparison is unavailable — and that absence used to let the
    creation be credited to them. The creation's own numeric id resolves back,
    through a card of this workspace, to a DIFFERENT proven uuid, and that is a
    positive link to somebody else.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(
        session_maker,
        slots=[{"state": "delivered", "basis": RECIPIENT_BASIS_MANUAL, "numeric_id": None}],
    )
    who = person(1)
    other = person(8)
    await _proven_other_card(
        session_maker,
        numeric_id=other["numeric_id"],
        customer_uuid=other["customer_uuid"],
        phone="+4915100444222",
    )
    theirs_now = booking(
        index=42,
        customer_uuid=who["customer_uuid"],
        start=ATTEMPTED_AT - timedelta(days=1),
        canceled=True,
        completed=False,
    )
    await seed_booking_created(
        session_maker,
        booking_uuid=theirs_now["uuid"],
        numeric_customer_id=other["numeric_id"],
        created_at=ATTEMPTED_AT + (timedelta(days=2) if made_after_the_attempt else -timedelta(days=9)),
    )
    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: [theirs_now]}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
    assert reason_for(outcome, 1) == BOOKING_CREATION_UNPROVEN


async def test_a_manual_card_with_a_complete_empty_history_still_works(session_maker, binding_key, no_network) -> None:
    """The control the reverse lookup must not break."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(
        session_maker,
        slots=[{"state": "delivered", "basis": RECIPIENT_BASIS_MANUAL, "numeric_id": None}],
    )
    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={person(1)["customer_uuid"]: []}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED


async def test_a_booking_already_observed_before_the_attempt_blocks_nobody(
    session_maker, binding_key, no_network
) -> None:
    """Review round 4, finding 4: observation proves existence, not creation.

    A cancellation of somebody else's booking was stored weeks before this
    mailing went out. Whatever that booking's creation date turns out to be, the
    delivery proves it existed by then — so it cannot be a booking made since
    the attempt, and it has no business holding a recipient as a possible one.
    No creation date is invented to reach that conclusion.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(
        session_maker,
        slots=[{"state": "delivered", "basis": RECIPIENT_BASIS_MANUAL, "numeric_id": None}],
    )
    who = person(1)
    older = booking(index=43, customer_uuid=person(9)["customer_uuid"], start=NOW + timedelta(days=7))
    await seed_booking_cancelled(
        session_maker,
        booking_uuid=older["uuid"],
        numeric_customer_id=who["numeric_id"] + 31,
        received_at=ATTEMPTED_AT - timedelta(days=20),
    )
    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: []}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED


async def test_a_booking_first_seen_after_the_attempt_still_guards(session_maker, binding_key, no_network) -> None:
    """And the window that really is open stays closed: unknown, as before."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(
        session_maker,
        slots=[{"state": "delivered", "basis": RECIPIENT_BASIS_MANUAL, "numeric_id": None}],
    )
    who = person(1)
    fresh = booking(index=44, customer_uuid=person(9)["customer_uuid"], start=NOW + timedelta(days=7))
    for owner, day in ((who["numeric_id"] + 31, 1), (who["numeric_id"] + 32, 2)):
        await seed_booking_cancelled(
            session_maker,
            booking_uuid=fresh["uuid"],
            numeric_customer_id=owner,
            received_at=ATTEMPTED_AT + timedelta(days=day),
        )
    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: []}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
    assert reason_for(outcome, 1) == BOOKING_CAPTURE_UNPROVEN


async def test_a_proven_new_booking_of_their_own_survives_a_separate_dispute(
    session_maker, binding_key, no_network
) -> None:
    """Review round 4, finding 5: a proven fact is not weakened by a gap.

    This person provably created a booking after the attempt. A different
    booking elsewhere in the window cannot be attributed to anybody — and that
    used to turn a provably-booked recipient into «не установлено», which both
    hides a real exclusion and invites someone to go looking for the reason.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    theirs = booking(index=45, customer_uuid=person(9)["customer_uuid"], start=NOW + timedelta(days=5))
    await seed_booking_created(
        session_maker,
        booking_uuid=theirs["uuid"],
        numeric_customer_id=who["numeric_id"],
        created_at=ATTEMPTED_AT + timedelta(days=1),
    )
    # And a separate booking nothing can attribute, first seen after the attempt.
    disputed = booking(index=46, customer_uuid=person(9)["customer_uuid"], start=NOW + timedelta(days=6))
    for owner, day in ((who["numeric_id"], 1), (who["numeric_id"] + 12, 2)):
        await seed_booking_created(
            session_maker,
            booking_uuid=disputed["uuid"],
            numeric_customer_id=owner,
            created_at=ATTEMPTED_AT + timedelta(days=day),
        )
    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: []}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_EXCLUDED
    assert reason_for(outcome, 1) == NEW_BOOKING_AFTER_ATTEMPT


async def test_a_dispute_without_a_proven_booking_of_their_own_stays_unknown(
    session_maker, binding_key, no_network
) -> None:
    """The control for that order: the dispute alone is no reason to exclude."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    disputed = booking(index=47, customer_uuid=person(9)["customer_uuid"], start=NOW + timedelta(days=6))
    for owner, day in ((who["numeric_id"], 1), (who["numeric_id"] + 12, 2)):
        await seed_booking_created(
            session_maker,
            booking_uuid=disputed["uuid"],
            numeric_customer_id=owner,
            created_at=ATTEMPTED_AT + timedelta(days=day),
        )
    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: []}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
    assert reason_for(outcome, 1) == BOOKING_CAPTURE_UNPROVEN


# ---------------------------------------------------------------------------
# Whose booking it is, and how old (round 5, fixes 1–4)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("dated_first", [True, False])
async def test_an_undated_second_creation_does_not_settle_the_owner_conflict(
    session_maker, binding_key, no_network, dated_first: bool
) -> None:
    """Review round 5, finding 1: a missing date is a second gap, not an answer.

    Two creation deliveries of one booking name two different customers, and
    one of them states no usable instant. WHO created a booking is a fact its
    creation delivery states even when it could not state WHEN, so the two are
    in conflict about the owner — but the undated delivery used to leave the
    aggregation before any comparison was made, and the booking came out
    cleanly attributed to whichever creation happened to carry a date.

    Checked in both delivery orders, because neither of them is the business
    order and the answer may not depend on which arrived first. With this
    person's current history empty, the wrong answer is the dangerous one:
    «nothing has happened since the mailing» about a booking one of those two
    deliveries says is theirs.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    contested = booking(index=50, customer_uuid=person(7)["customer_uuid"], start=NOW + timedelta(days=5))
    made = ATTEMPTED_AT - timedelta(days=11)

    async def dated() -> None:
        await seed_booking_created(
            session_maker,
            booking_uuid=contested["uuid"],
            numeric_customer_id=who["numeric_id"],
            created_at=made,
        )

    async def undated() -> None:
        await seed_booking_created_without_a_date(
            session_maker,
            booking_uuid=contested["uuid"],
            numeric_customer_id=who["numeric_id"] + 61,
            received_at=made + timedelta(minutes=5),
        )

    for seed in (dated, undated) if dated_first else (undated, dated):
        await seed()

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: []}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
    assert reason_for(outcome, 1) == BOOKING_CAPTURE_UNPROVEN


@pytest.mark.parametrize("dated_first", [True, False])
async def test_a_repeated_creation_that_only_adds_the_instant_is_still_usable(
    session_maker, binding_key, no_network, dated_first: bool
) -> None:
    """The control the owner check must not break.

    The same booking, the same owner, twice — once without the instant and once
    with it. That is a repeat of one creation with the date filled in, not a
    disagreement, so it stays usable evidence and the recipient is provable.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    theirs = booking(
        index=51,
        customer_uuid=who["customer_uuid"],
        start=ATTEMPTED_AT - timedelta(days=2),
        canceled=True,
        completed=False,
    )
    made = ATTEMPTED_AT - timedelta(days=12)

    async def dated() -> None:
        await seed_booking_created(
            session_maker,
            booking_uuid=theirs["uuid"],
            numeric_customer_id=who["numeric_id"],
            created_at=made,
        )

    async def undated() -> None:
        await seed_booking_created_without_a_date(
            session_maker,
            booking_uuid=theirs["uuid"],
            numeric_customer_id=who["numeric_id"],
            received_at=made + timedelta(minutes=5),
        )

    for seed in (dated, undated) if dated_first else (undated, dated):
        await seed()

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: [theirs]}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED


async def test_an_early_sighting_does_not_hide_a_creator_conflict(session_maker, binding_key, no_network) -> None:
    """Review round 5, finding 1: the louder problem is not settled by the quieter one.

    A cancellation of this booking was stored before the mailing went out, so
    the booking provably existed by then. Its creation deliveries then claim a
    LATER instant and name two different owners — a record that contradicts
    itself, and an early sighting answers an incomplete record, never a
    self-contradictory one. The conflict stands and the recipient stays unknown.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    contested = booking(index=52, customer_uuid=person(7)["customer_uuid"], start=NOW + timedelta(days=5))
    await seed_booking_cancelled(
        session_maker,
        booking_uuid=contested["uuid"],
        numeric_customer_id=who["numeric_id"],
        received_at=ATTEMPTED_AT - timedelta(days=18),
    )
    for owner, day in ((who["numeric_id"], 3), (who["numeric_id"] + 62, 4)):
        await seed_booking_created(
            session_maker,
            booking_uuid=contested["uuid"],
            numeric_customer_id=owner,
            created_at=ATTEMPTED_AT + timedelta(days=day),
        )

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: []}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
    assert reason_for(outcome, 1) == BOOKING_CAPTURE_UNPROVEN


@pytest.mark.parametrize("made_after_the_attempt", [False, True])
async def test_a_numeric_id_two_workspace_cards_claim_is_not_an_absent_link(
    session_maker, binding_key, no_network, monkeypatch: pytest.MonkeyPatch, made_after_the_attempt: bool
) -> None:
    """Review round 5, finding 2: ambiguous is not absent.

    The recipient's card is a manual one — a proven uuid and no numeric id, the
    ordinary shape — so the creation's own numeric id has to be read back to an
    owner. Two cards of this workspace claim that number against two different
    proven uuids, which is a POSITIVE claim that the creation may be somebody
    else's. Dropping such an id from the lookup made it indistinguishable from
    a number nobody bridges at all, and the rule that follows an absent link is
    the permissive one: the booking was credited to whoever holds it now.

    Checked for a creation before and after the attempt, because the permissive
    rule was wrong in both directions — it invented an old booking for one and
    somebody else's engagement for the other. With no other proven ground,
    both are unknown.

    The companion control, a creation resolving UNAMBIGUOUSLY to another
    person, is covered by the round-4 test above.
    """
    monkeypatch.setattr(settings, "easyweek_location_map", location_map(second_branch=True), raising=False)
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(
        session_maker,
        slots=[{"state": "delivered", "basis": RECIPIENT_BASIS_MANUAL, "numeric_id": None}],
    )
    who = person(1)
    claimed = person(8)["numeric_id"]
    await _proven_other_card(
        session_maker,
        numeric_id=claimed,
        customer_uuid=person(8)["customer_uuid"],
        phone="+4915100444333",
    )
    await _proven_other_card(
        session_maker,
        numeric_id=claimed,
        customer_uuid=person(9)["customer_uuid"],
        phone="+4915100444334",
        company_id=SECOND_COMPANY_ID,
    )
    theirs_now = booking(
        index=53,
        customer_uuid=who["customer_uuid"],
        start=ATTEMPTED_AT - timedelta(days=1),
        canceled=True,
        completed=False,
    )
    await seed_booking_created(
        session_maker,
        booking_uuid=theirs_now["uuid"],
        numeric_customer_id=claimed,
        created_at=ATTEMPTED_AT + (timedelta(days=2) if made_after_the_attempt else -timedelta(days=9)),
    )

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: [theirs_now]}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
    assert reason_for(outcome, 1) == BOOKING_CREATION_UNPROVEN


async def test_a_card_outside_the_configured_branches_makes_no_conflict(session_maker, binding_key, no_network) -> None:
    """And a card nobody configured competes with nothing.

    The same number, claimed by a second card in a branch that is not in the
    registry. The lookup is scoped to the configured branches, so the link
    inside this workspace stays unambiguous and the creation is still proven to
    be another person's — which is what keeps an unrelated tenant's numbering
    from making this mailing's bookings unanswerable.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(
        session_maker,
        slots=[{"state": "delivered", "basis": RECIPIENT_BASIS_MANUAL, "numeric_id": None}],
    )
    who = person(1)
    maker = person(8)
    await _proven_other_card(
        session_maker,
        numeric_id=maker["numeric_id"],
        customer_uuid=maker["customer_uuid"],
        phone="+4915100444335",
    )
    await _proven_other_card(
        session_maker,
        numeric_id=maker["numeric_id"],
        customer_uuid=person(9)["customer_uuid"],
        phone="+4915100444336",
        company_id=UNCONFIGURED_COMPANY_ID,
    )
    await _handed_on_booking(session_maker, index=54, maker_numeric_id=maker["numeric_id"])

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: []}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED


async def _handed_on_booking(session_maker, *, index: int, maker_numeric_id: int) -> dict[str, Any]:
    """A booking somebody made after the attempt and then handed to a third person.

    The shape that used to leave every bridge-less recipient unknown: a
    reassignment the live read cannot show, of a booking created inside the
    window. Who MADE it is stated by its own creation delivery.
    """
    handed_on = booking(index=index, customer_uuid=person(9)["customer_uuid"], start=NOW + timedelta(days=8))
    await seed_booking_created(
        session_maker,
        booking_uuid=handed_on["uuid"],
        numeric_customer_id=maker_numeric_id,
        created_at=ATTEMPTED_AT + timedelta(days=2),
    )
    await seed_booking_cancelled(
        session_maker,
        booking_uuid=handed_on["uuid"],
        numeric_customer_id=maker_numeric_id + 70,
        received_at=ATTEMPTED_AT + timedelta(days=3),
    )
    return handed_on


async def test_a_handover_of_somebody_elses_proven_booking_blocks_nobody(
    session_maker, binding_key, no_network
) -> None:
    """Review round 5, finding 3: a proven creator settles a reassignment too.

    The recipient's card is manual, so the live set is the whole account of
    their bookings and a booking that LEFT it is the one thing it cannot show —
    which is why a reassignment inside the window used to leave them unknown.
    But this booking's own creation delivery names a customer whose number
    resolves, through a card of this workspace, to a different proven uuid. It
    is an event in somebody else's history, and the recipient stays provable.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(
        session_maker,
        slots=[{"state": "delivered", "basis": RECIPIENT_BASIS_MANUAL, "numeric_id": None}],
    )
    who = person(1)
    maker = person(8)
    await _proven_other_card(
        session_maker,
        numeric_id=maker["numeric_id"],
        customer_uuid=maker["customer_uuid"],
        phone="+4915100444337",
    )
    await _handed_on_booking(session_maker, index=55, maker_numeric_id=maker["numeric_id"])

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: []}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED


async def test_a_handover_with_no_resolvable_creator_still_leaves_it_unknown(
    session_maker, binding_key, no_network
) -> None:
    """The window that really is open stays closed: no card, no answer."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(
        session_maker,
        slots=[{"state": "delivered", "basis": RECIPIENT_BASIS_MANUAL, "numeric_id": None}],
    )
    who = person(1)
    await _handed_on_booking(session_maker, index=56, maker_numeric_id=person(8)["numeric_id"])

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: []}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
    assert reason_for(outcome, 1) == BOOKING_CAPTURE_UNPROVEN


async def test_a_handover_elsewhere_does_not_stop_their_own_booking_excluding_them(
    session_maker, binding_key, no_network
) -> None:
    """And the exclusion the live read can see on its own still holds."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(
        session_maker,
        slots=[{"state": "delivered", "basis": RECIPIENT_BASIS_MANUAL, "numeric_id": None}],
    )
    who = person(1)
    maker = person(8)
    await _proven_other_card(
        session_maker,
        numeric_id=maker["numeric_id"],
        customer_uuid=maker["customer_uuid"],
        phone="+4915100444338",
    )
    await _handed_on_booking(session_maker, index=57, maker_numeric_id=maker["numeric_id"])
    ahead = booking(index=58, customer_uuid=who["customer_uuid"], start=NOW + timedelta(days=4))

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: [ahead]}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_EXCLUDED
    assert reason_for(outcome, 1) == FUTURE_BOOKING_ACTIVE


async def test_their_own_undated_booking_heard_of_before_the_attempt_is_not_new(
    session_maker, binding_key, no_network
) -> None:
    """Review round 5, finding 4: the existence proof reaches a VISIBLE booking.

    Their own cancelled booking, still in their complete current history. Its
    creation delivery proves the branch, the booking identity and the owner and
    states no usable instant — so the capture cannot date it, and the rule that
    every visible booking needs a proven creation held the recipient for ever.

    The delivery was stored before the mailing went out, which proves the
    booking existed by then. That is a lower bound on its existence and not a
    creation date, and it is exactly the fact the rule needs: nothing new is
    hiding here. No date is invented to reach it.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    theirs = booking(
        index=59,
        customer_uuid=who["customer_uuid"],
        start=ATTEMPTED_AT - timedelta(days=3),
        canceled=True,
        completed=False,
    )
    await seed_booking_created_without_a_date(
        session_maker,
        booking_uuid=theirs["uuid"],
        numeric_customer_id=who["numeric_id"],
        received_at=ATTEMPTED_AT - timedelta(days=15),
    )

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: [theirs]}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED


async def test_an_undated_booking_first_heard_of_after_the_attempt_is_still_unknown(
    session_maker, binding_key, no_network
) -> None:
    """The first control: a sighting after the send proves nothing about before it."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    theirs = booking(
        index=60,
        customer_uuid=who["customer_uuid"],
        start=ATTEMPTED_AT - timedelta(days=3),
        canceled=True,
        completed=False,
    )
    await seed_booking_created_without_a_date(
        session_maker,
        booking_uuid=theirs["uuid"],
        numeric_customer_id=who["numeric_id"],
        received_at=ATTEMPTED_AT + timedelta(days=2),
    )

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: [theirs]}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
    assert reason_for(outcome, 1) == BOOKING_CAPTURE_UNPROVEN


async def test_an_old_appointment_without_an_early_delivery_proves_nothing(
    session_maker, binding_key, no_network
) -> None:
    """The second control: the appointment time is not an existence proof.

    A booking in their history whose appointment was months ago and about which
    the capture holds nothing at all. A backdated booking recording a visit that
    already happened has exactly this shape, and it is engagement since the
    mailing — so the old date settles nothing and the answer stays unknown.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    stale = booking(
        index=61,
        customer_uuid=who["customer_uuid"],
        start=ATTEMPTED_AT - timedelta(days=90),
        canceled=False,
        completed=True,
    )

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: [stale]}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
    assert reason_for(outcome, 1) == BOOKING_CREATION_UNPROVEN


async def test_a_visible_booking_whose_creators_disagree_stays_unknown(session_maker, binding_key, no_network) -> None:
    """The third control: an early sighting does not settle a contradiction.

    Same booking, same early delivery, and now two creation deliveries naming
    two owners. The record does not merely lack a fact; it asserts two that
    cannot both hold, and that is not something an earlier sighting answers.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    theirs = booking(
        index=62,
        customer_uuid=who["customer_uuid"],
        start=ATTEMPTED_AT - timedelta(days=3),
        canceled=True,
        completed=False,
    )
    await seed_booking_created_without_a_date(
        session_maker,
        booking_uuid=theirs["uuid"],
        numeric_customer_id=who["numeric_id"],
        received_at=ATTEMPTED_AT - timedelta(days=15),
    )
    await seed_booking_created(
        session_maker,
        booking_uuid=theirs["uuid"],
        numeric_customer_id=who["numeric_id"] + 63,
        created_at=ATTEMPTED_AT - timedelta(days=14),
    )

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: [theirs]}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
    assert reason_for(outcome, 1) == BOOKING_CAPTURE_UNPROVEN


async def test_an_active_future_booking_still_excludes_despite_the_early_sighting(
    session_maker, binding_key, no_network
) -> None:
    """The fourth control: a slot booked ahead is a reason of its own."""
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    ahead = booking(index=63, customer_uuid=who["customer_uuid"], start=NOW + timedelta(days=6))
    await seed_booking_created_without_a_date(
        session_maker,
        booking_uuid=ahead["uuid"],
        numeric_customer_id=who["numeric_id"],
        received_at=ATTEMPTED_AT - timedelta(days=15),
    )

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: [ahead]}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_EXCLUDED
    assert reason_for(outcome, 1) == FUTURE_BOOKING_ACTIVE
