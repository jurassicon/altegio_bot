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
from altegio_bot.easyweek_voucher_identity import KARLSRUHE_LOCATION_UUID
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
    SECOND_LOCATION_UUID,
    SENDER_DIGEST,
    UNCONFIGURED_COMPANY_ID,
    TemplateReader,
    location_map,
    order_uuid_for,
    person,
    seed_booking_cancelled,
    seed_booking_created,
    seed_booking_created_without_a_customer,
    seed_booking_created_without_a_date,
    seed_booking_lifecycle,
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


async def _handed_on_booking(
    session_maker,
    *,
    index: int,
    maker_numeric_id: int,
    company_id: int = COMPANY_ID,
    location_uuid: str = KARLSRUHE_LOCATION_UUID,
) -> dict[str, Any]:
    """A booking somebody made after the attempt and then handed to a third person.

    The shape that used to leave every bridge-less recipient unknown: a
    reassignment the live read cannot show, of a booking created inside the
    window. Who MADE it is stated by its own creation delivery, and WHERE is
    what decides what that delivery's numeric id means.
    """
    handed_on = booking(index=index, customer_uuid=person(9)["customer_uuid"], start=NOW + timedelta(days=8))
    await seed_booking_created(
        session_maker,
        booking_uuid=handed_on["uuid"],
        numeric_customer_id=maker_numeric_id,
        created_at=ATTEMPTED_AT + timedelta(days=2),
        company_id=company_id,
        location_uuid=location_uuid,
    )
    await seed_booking_cancelled(
        session_maker,
        booking_uuid=handed_on["uuid"],
        numeric_customer_id=maker_numeric_id + 70,
        received_at=ATTEMPTED_AT + timedelta(days=3),
        company_id=company_id,
        location_uuid=location_uuid,
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


# ---------------------------------------------------------------------------
# Which branch proved it, and what is actually disputed (round 6, fixes 1–4)
# ---------------------------------------------------------------------------


async def test_a_number_matching_only_in_another_branch_does_not_name_the_creator(
    session_maker, binding_key, no_network, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Review round 6, finding 1: a numeric id means one thing per branch.

    The booking was created in one configured branch and handed on inside the
    window, and this recipient's card carries no numeric id — so the question
    is whether the creation can be read back to a person. The only card holding
    those digits is in ANOTHER configured branch, where the same number is a
    different customer record entirely.

    That card used to establish the creator, which turned an unanswerable
    reassignment into «nothing has happened since the mailing» for somebody the
    capture knows nothing about. The branch of the card now has to be the
    branch of the creation, and without one the slot stays unknown exactly as
    it did before any card existed.
    """
    monkeypatch.setattr(settings, "easyweek_location_map", location_map(second_branch=True), raising=False)
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(
        session_maker,
        slots=[{"state": "delivered", "basis": RECIPIENT_BASIS_MANUAL, "numeric_id": None}],
    )
    who = person(1)
    maker = person(8)
    # The card is in the mailing's own branch; the booking was made in the other.
    await _proven_other_card(
        session_maker,
        numeric_id=maker["numeric_id"],
        customer_uuid=maker["customer_uuid"],
        phone="+4915100555111",
    )
    await _handed_on_booking(
        session_maker,
        index=70,
        maker_numeric_id=maker["numeric_id"],
        company_id=SECOND_COMPANY_ID,
        location_uuid=SECOND_LOCATION_UUID,
    )

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: []}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
    assert reason_for(outcome, 1) == BOOKING_CAPTURE_UNPROVEN


async def test_a_card_in_the_creations_own_branch_still_names_the_creator(
    session_maker, binding_key, no_network, monkeypatch: pytest.MonkeyPatch
) -> None:
    """And the control: in the right branch the same card still answers.

    One card, one number, one branch — the branch the booking was made in. The
    creation is positively somebody else's, so the handover is an event in that
    person's history and this recipient stays provable.
    """
    monkeypatch.setattr(settings, "easyweek_location_map", location_map(second_branch=True), raising=False)
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
        phone="+4915100555112",
        company_id=SECOND_COMPANY_ID,
    )
    await _handed_on_booking(
        session_maker,
        index=71,
        maker_numeric_id=maker["numeric_id"],
        company_id=SECOND_COMPANY_ID,
        location_uuid=SECOND_LOCATION_UUID,
    )

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: []}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED


@pytest.mark.parametrize("with_the_unconfigured_card", [False, True])
async def test_only_an_unconfigured_card_carries_this_persons_number(
    session_maker, binding_key, no_network, with_the_unconfigured_card: bool
) -> None:
    """Review round 6, finding 1: an unconfigured card bridges nothing.

    A card in a branch nobody configured is another workspace's row, or one
    from the Altegio era. It used to supply this recipient's numeric bridge for
    the booking question, and a bridge proven in a branch no creation of this
    mailing can come from answers nothing while silently switching the rules
    the slot is judged by.

    Asserted against the control that has no such card at all: the decision
    must be the same one, and it is the control's rules that are right.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(
        session_maker,
        slots=[{"state": "delivered", "basis": RECIPIENT_BASIS_MANUAL, "numeric_id": None}],
    )
    who = person(1)
    if with_the_unconfigured_card:
        await _proven_other_card(
            session_maker,
            numeric_id=person(8)["numeric_id"],
            customer_uuid=who["customer_uuid"],
            phone="+4915100555113",
            company_id=UNCONFIGURED_COMPANY_ID,
        )
    theirs = booking(
        index=72,
        customer_uuid=who["customer_uuid"],
        start=ATTEMPTED_AT - timedelta(days=1),
        canceled=True,
        completed=False,
    )
    await seed_booking_created(
        session_maker,
        booking_uuid=theirs["uuid"],
        numeric_customer_id=person(12)["numeric_id"],
        created_at=ATTEMPTED_AT - timedelta(days=9),
    )

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: [theirs]}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED


@pytest.mark.parametrize("with_the_strangers_card", [False, True])
async def test_an_unconfigured_card_sharing_the_number_is_no_conflict(
    session_maker, binding_key, no_network, with_the_strangers_card: bool
) -> None:
    """Review round 6, finding 1: and it contradicts nothing either.

    This recipient's number is proven on their own card, in the mailing's own
    branch. A stranger's card in a branch nobody configured carries the same
    digits against a different uuid — which used to be read as «two people
    claim this number» and refused the whole booking question as an identity
    conflict.

    Asserted against the control without that card: the same decision, because
    a row outside the configured branches is not a competing claim about
    anything here.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    if with_the_strangers_card:
        await _proven_other_card(
            session_maker,
            numeric_id=who["numeric_id"],
            customer_uuid=person(8)["customer_uuid"],
            phone="+4915100555114",
            company_id=UNCONFIGURED_COMPANY_ID,
        )

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: []}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED


@pytest.mark.parametrize("dated", [True, False])
@pytest.mark.parametrize("shape", ["bridged_other_number", "manual_proven_other", "manual_unresolvable"])
async def test_losing_the_creation_instant_does_not_change_the_answer(
    session_maker, binding_key, no_network, monkeypatch: pytest.MonkeyPatch, shape: str, dated: bool
) -> None:
    """Review round 6, finding 2: a missing date is not a licence.

    One booking, visible in this person's complete current history, heard of
    weeks before the mailing, and run twice: once with a usable creation
    instant and once without. The early sighting answers whether anything could
    have appeared since the send — it says nothing about whose booking it is,
    so it may not be applied before the owner is checked.

    Each shape is asserted with the SAME expectation for both runs, which is
    the property at stake: a row that reads unknown with a usable instant must
    not become admissible by losing it. Two of the shapes are a known
    contradiction with this recipient's ownership and read unknown; the third
    establishes nothing either way and follows the ordinary rule for a
    bridge-less card, in both runs alike.
    """
    monkeypatch.setattr(settings, "easyweek_location_map", location_map(second_branch=True), raising=False)
    await seed_sender_and_template(session_maker, with_followup_template=True)
    manual = shape != "bridged_other_number"
    batch_id, _ = await seed_source_batch(
        session_maker,
        slots=[
            {"state": "delivered", "basis": RECIPIENT_BASIS_MANUAL, "numeric_id": None}
            if manual
            else {"state": "delivered"}
        ],
    )
    who = person(1)
    maker = person(8)
    if shape == "manual_proven_other":
        # Positively linked, in the branch the creation was delivered by.
        await _proven_other_card(
            session_maker,
            numeric_id=maker["numeric_id"],
            customer_uuid=maker["customer_uuid"],
            phone="+4915100555115",
        )
    elif shape == "manual_unresolvable":
        # Claimed elsewhere only: another configured branch and an
        # unconfigured one. Neither speaks for the creation's own branch.
        await _proven_other_card(
            session_maker,
            numeric_id=maker["numeric_id"],
            customer_uuid=maker["customer_uuid"],
            phone="+4915100555116",
            company_id=SECOND_COMPANY_ID,
        )
        await _proven_other_card(
            session_maker,
            numeric_id=maker["numeric_id"],
            customer_uuid=person(13)["customer_uuid"],
            phone="+4915100555117",
            company_id=UNCONFIGURED_COMPANY_ID,
        )
    theirs = booking(
        index=73,
        customer_uuid=who["customer_uuid"],
        start=ATTEMPTED_AT - timedelta(days=2),
        canceled=True,
        completed=False,
    )
    heard_of = ATTEMPTED_AT - timedelta(days=16)
    if dated:
        await seed_booking_created(
            session_maker,
            booking_uuid=theirs["uuid"],
            numeric_customer_id=maker["numeric_id"],
            created_at=ATTEMPTED_AT - timedelta(days=20),
            received_at=heard_of,
        )
    else:
        await seed_booking_created_without_a_date(
            session_maker,
            booking_uuid=theirs["uuid"],
            numeric_customer_id=maker["numeric_id"],
            received_at=heard_of,
        )

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: [theirs]}),
    )
    if shape == "manual_unresolvable":
        assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED
    else:
        assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
        assert reason_for(outcome, 1) in {BOOKING_CREATION_UNPROVEN, BOOKING_CAPTURE_UNPROVEN}


@pytest.mark.parametrize("undated", ["none", "first", "second"])
async def test_two_creations_in_two_branches_stay_in_conflict(
    session_maker, binding_key, no_network, monkeypatch: pytest.MonkeyPatch, undated: str
) -> None:
    """Review round 6, finding 2: the branch is half of what a number means.

    Two creation deliveries of one booking uuid, from two different configured
    branches, naming the same numeric customer id. They are not one person
    twice: the id is unique per branch, so this is a contradiction about where
    the booking was made and therefore about whose it is.

    Checked with both instants present, with the first missing and with the
    second missing. A creation delivery that could not state WHEN still states
    WHERE, so losing an instant must not dissolve the disagreement, and the
    delivery order must not decide it either.
    """
    monkeypatch.setattr(settings, "easyweek_location_map", location_map(second_branch=True), raising=False)
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    contested = booking(
        index=74,
        customer_uuid=who["customer_uuid"],
        start=ATTEMPTED_AT - timedelta(days=2),
        canceled=True,
        completed=False,
    )
    made = ATTEMPTED_AT - timedelta(days=20)
    heard_of = ATTEMPTED_AT - timedelta(days=19)
    branches = (
        (COMPANY_ID, KARLSRUHE_LOCATION_UUID, undated == "first"),
        (SECOND_COMPANY_ID, SECOND_LOCATION_UUID, undated == "second"),
    )
    for position, (company_id, location_uuid, drop_the_date) in enumerate(branches):
        when = heard_of + timedelta(minutes=position)
        if drop_the_date:
            await seed_booking_created_without_a_date(
                session_maker,
                booking_uuid=contested["uuid"],
                numeric_customer_id=who["numeric_id"],
                received_at=when,
                company_id=company_id,
                location_uuid=location_uuid,
            )
        else:
            await seed_booking_created(
                session_maker,
                booking_uuid=contested["uuid"],
                numeric_customer_id=who["numeric_id"],
                created_at=made,
                received_at=when,
                company_id=company_id,
                location_uuid=location_uuid,
            )

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: [contested]}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
    assert reason_for(outcome, 1) == BOOKING_CAPTURE_UNPROVEN


async def test_their_own_undated_booking_with_a_consistent_branch_is_still_fine(
    session_maker, binding_key, no_network, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The positive control for both halves: owner and branch agreeing.

    Their own cancelled booking, visible in their complete history, with one
    undated creation naming their own number in the branch their card proves it
    in, stored before the mailing. Nothing contradicts anything, the booking
    provably existed by then, and the recipient is provable — with a second
    branch configured, so the branch really is a variable here.
    """
    monkeypatch.setattr(settings, "easyweek_location_map", location_map(second_branch=True), raising=False)
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    theirs = booking(
        index=75,
        customer_uuid=who["customer_uuid"],
        start=ATTEMPTED_AT - timedelta(days=2),
        canceled=True,
        completed=False,
    )
    await seed_booking_created_without_a_date(
        session_maker,
        booking_uuid=theirs["uuid"],
        numeric_customer_id=who["numeric_id"],
        received_at=ATTEMPTED_AT - timedelta(days=17),
    )

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: [theirs]}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED


async def _anothers_old_booking_with_two_dates(session_maker, *, index: int, maker_numeric_id: int) -> None:
    """Somebody else's old booking whose creations agree on WHO and not on WHEN."""
    theirs = booking(index=index, customer_uuid=person(14)["customer_uuid"], start=ATTEMPTED_AT - timedelta(days=40))
    for days in (60, 55):
        await seed_booking_created(
            session_maker,
            booking_uuid=theirs["uuid"],
            numeric_customer_id=maker_numeric_id,
            created_at=ATTEMPTED_AT - timedelta(days=days),
            received_at=ATTEMPTED_AT - timedelta(days=30),
        )


async def test_a_date_dispute_about_anothers_booking_leaves_both_recipients_alone(
    session_maker, binding_key, no_network
) -> None:
    """Review round 6, finding 3: separate WHOSE it is from WHEN it was made.

    Two manual recipients, both provable, and then a stranger's old booking
    whose two creation deliveries agree about its creator and disagree about
    its instant. The owner is not in doubt at all — the date is — and the owner
    is a person neither recipient is.

    An unattributable booking used to be unattributable to everybody, so that
    one gap held every bridge-less recipient in the batch open at once. Both
    are asserted provable before the deliveries exist and after, which is what
    makes the gap's reach the thing under test.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(
        session_maker,
        slots=[
            {"state": "delivered", "basis": RECIPIENT_BASIS_MANUAL, "numeric_id": None},
            {"state": "delivered", "index": 2, "basis": RECIPIENT_BASIS_MANUAL, "numeric_id": None},
        ],
    )
    reader = FakeHistoryReader(bookings={person(1)["customer_uuid"]: [], person(2)["customer_uuid"]: []})

    before = await build(session_maker, batch_id=batch_id, reader=reader)
    assert decision_for(before, 1) == FOLLOWUP_ROW_INCLUDED
    assert decision_for(before, 2) == FOLLOWUP_ROW_INCLUDED

    maker = person(8)
    await _proven_other_card(
        session_maker,
        numeric_id=maker["numeric_id"],
        customer_uuid=maker["customer_uuid"],
        phone="+4915100555118",
    )
    await _anothers_old_booking_with_two_dates(session_maker, index=76, maker_numeric_id=maker["numeric_id"])

    after = await build(session_maker, batch_id=batch_id, reader=reader)
    assert decision_for(after, 1) == FOLLOWUP_ROW_INCLUDED
    assert decision_for(after, 2) == FOLLOWUP_ROW_INCLUDED


@pytest.mark.parametrize("handed_on", [False, True])
async def test_a_single_undated_creation_by_another_leaves_them_alone(
    session_maker, binding_key, no_network, handed_on: bool
) -> None:
    """The same, with the thinnest evidence there is: one undated creation.

    It names a creator and no instant. The creator is positively somebody
    else's in the branch that delivered it, so the booking is that person's and
    its missing date is that person's gap. Checked with and without a later
    delivery handing the booking on, because a reassignment does not move a
    creation and must not move the gap either.
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
        phone="+4915100555119",
    )
    target = booking(index=77, customer_uuid=person(14)["customer_uuid"], start=NOW + timedelta(days=9))
    await seed_booking_created_without_a_date(
        session_maker,
        booking_uuid=target["uuid"],
        numeric_customer_id=maker["numeric_id"],
        received_at=ATTEMPTED_AT + timedelta(days=2),
    )
    if handed_on:
        await seed_booking_cancelled(
            session_maker,
            booking_uuid=target["uuid"],
            numeric_customer_id=maker["numeric_id"] + 70,
            received_at=ATTEMPTED_AT + timedelta(days=3),
        )

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: []}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED


async def test_a_creator_that_really_cannot_be_established_still_leaves_it_unknown(
    session_maker, binding_key, no_network
) -> None:
    """And the window that really is open stays closed.

    The same undated creation, and no card of the creation's branch bridges the
    number it names. Nothing says whose booking it is, it was first heard of
    after the mailing, and this recipient's own list cannot show a booking that
    left it — so it could be theirs, and the answer is unknown.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(
        session_maker,
        slots=[{"state": "delivered", "basis": RECIPIENT_BASIS_MANUAL, "numeric_id": None}],
    )
    who = person(1)
    target = booking(index=78, customer_uuid=person(14)["customer_uuid"], start=NOW + timedelta(days=9))
    await seed_booking_created_without_a_date(
        session_maker,
        booking_uuid=target["uuid"],
        numeric_customer_id=person(8)["numeric_id"],
        received_at=ATTEMPTED_AT + timedelta(days=2),
    )
    await seed_booking_cancelled(
        session_maker,
        booking_uuid=target["uuid"],
        numeric_customer_id=person(8)["numeric_id"] + 70,
        received_at=ATTEMPTED_AT + timedelta(days=3),
    )

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: []}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
    assert reason_for(outcome, 1) == BOOKING_CAPTURE_UNPROVEN


async def test_a_future_active_booking_excludes_beside_anothers_date_dispute(
    session_maker, binding_key, no_network
) -> None:
    """Narrowing the gap does not narrow the rules: a slot booked ahead excludes."""
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
        phone="+4915100555120",
    )
    await _anothers_old_booking_with_two_dates(session_maker, index=79, maker_numeric_id=maker["numeric_id"])
    ahead = booking(index=80, customer_uuid=who["customer_uuid"], start=NOW + timedelta(days=5))

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: [ahead]}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_EXCLUDED
    assert reason_for(outcome, 1) == FUTURE_BOOKING_ACTIVE


async def test_a_creation_dated_after_a_delivery_that_predates_the_attempt_is_unknown(
    session_maker, binding_key, no_network
) -> None:
    """Review round 6, finding 4: one creation can still contradict the capture.

    A cancellation of this person's booking was stored weeks before the mailing
    went out, which proves the booking existed by then. Its one creation
    delivery states an instant AFTER the mailing. Both cannot be true, and the
    half this phase acted on was the harsher one: the person was excluded for a
    new booking the capture's own earlier row says was already there.

    No date is invented and no sighting is read as a creation date. The answer
    is simply that the capture's facts about this booking disagree.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    theirs = booking(
        index=81,
        customer_uuid=who["customer_uuid"],
        start=ATTEMPTED_AT - timedelta(days=1),
        canceled=True,
        completed=False,
    )
    await seed_booking_cancelled(
        session_maker,
        booking_uuid=theirs["uuid"],
        numeric_customer_id=who["numeric_id"],
        received_at=ATTEMPTED_AT - timedelta(days=14),
    )
    await seed_booking_created(
        session_maker,
        booking_uuid=theirs["uuid"],
        numeric_customer_id=who["numeric_id"],
        created_at=ATTEMPTED_AT + timedelta(days=3),
    )

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: [theirs]}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
    assert reason_for(outcome, 1) == BOOKING_CAPTURE_UNPROVEN


async def test_a_late_creation_delivery_with_an_earlier_date_is_ordinary(
    session_maker, binding_key, no_network
) -> None:
    """The control: a webhook may arrive after the cancellation it precedes.

    The cancellation is stored first and before the mailing, the creation
    arrives afterwards and states a truthful earlier instant. That is an
    allowed delivery order for this provider and nothing about it contradicts
    anything — the booking was made before the mailing and the recipient is
    provable.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    theirs = booking(
        index=82,
        customer_uuid=who["customer_uuid"],
        start=ATTEMPTED_AT - timedelta(days=1),
        canceled=True,
        completed=False,
    )
    await seed_booking_cancelled(
        session_maker,
        booking_uuid=theirs["uuid"],
        numeric_customer_id=who["numeric_id"],
        received_at=ATTEMPTED_AT - timedelta(days=14),
    )
    await seed_booking_created(
        session_maker,
        booking_uuid=theirs["uuid"],
        numeric_customer_id=who["numeric_id"],
        created_at=ATTEMPTED_AT - timedelta(days=25),
        received_at=ATTEMPTED_AT + timedelta(days=3),
    )

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: [theirs]}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED


async def test_another_independently_proven_new_booking_still_excludes(session_maker, binding_key, no_network) -> None:
    """And a booking whose own facts agree still excludes on its own.

    The contradicted booking is beside a second one this person provably made
    after the attempt and that nothing had heard of before it. A gap about the
    first is not a reason to stop reporting the second.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    contradicted = booking(
        index=83,
        customer_uuid=who["customer_uuid"],
        start=ATTEMPTED_AT - timedelta(days=1),
        canceled=True,
        completed=False,
    )
    await seed_booking_cancelled(
        session_maker,
        booking_uuid=contradicted["uuid"],
        numeric_customer_id=who["numeric_id"],
        received_at=ATTEMPTED_AT - timedelta(days=14),
    )
    await seed_booking_created(
        session_maker,
        booking_uuid=contradicted["uuid"],
        numeric_customer_id=who["numeric_id"],
        created_at=ATTEMPTED_AT + timedelta(days=3),
    )
    genuinely_new = booking(index=84, customer_uuid=who["customer_uuid"], start=ATTEMPTED_AT - timedelta(days=2))
    await seed_booking_created(
        session_maker,
        booking_uuid=genuinely_new["uuid"],
        numeric_customer_id=who["numeric_id"],
        created_at=ATTEMPTED_AT + timedelta(days=1),
    )

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: [contradicted, genuinely_new]}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_EXCLUDED
    assert reason_for(outcome, 1) == NEW_BOOKING_AFTER_ATTEMPT


# ---------------------------------------------------------------------------
# Which branch a number was proven in, and which pairs an event really named
# (round 7, fixes 1-7)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("dated", [True, False])
@pytest.mark.parametrize("same_digits", [True, False])
async def test_a_bridge_proven_in_one_branch_answers_nothing_in_another(
    session_maker, binding_key, no_network, monkeypatch: pytest.MonkeyPatch, dated: bool, same_digits: bool
) -> None:
    """Review round 7, finding 1: a proven number is proven in ONE branch.

    This recipient's card carries a numeric id, and it carries it in the
    mailing's own branch. The booking appeared in the OTHER configured branch
    after the attempt and was handed on, so it is not in their current history
    and its first owner there is nobody this deployment can name.

    Having a bridge used to switch the slot to the rules that assume the bridge
    can see everything, and a booking that left a list the bridge cannot read
    then went unnoticed in either direction: the same digits in the other
    branch are a different customer record, and different digits say nothing
    about who the recipient is there. Both are unknown.
    """
    monkeypatch.setattr(settings, "easyweek_location_map", location_map(second_branch=True), raising=False)
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    digits = who["numeric_id"] if same_digits else person(8)["numeric_id"]
    handed_on = booking(index=90, customer_uuid=person(9)["customer_uuid"], start=NOW + timedelta(days=8))
    if dated:
        await seed_booking_created(
            session_maker,
            booking_uuid=handed_on["uuid"],
            numeric_customer_id=digits,
            created_at=ATTEMPTED_AT + timedelta(days=2),
            company_id=SECOND_COMPANY_ID,
            location_uuid=SECOND_LOCATION_UUID,
        )
    else:
        await seed_booking_created_without_a_date(
            session_maker,
            booking_uuid=handed_on["uuid"],
            numeric_customer_id=digits,
            received_at=ATTEMPTED_AT + timedelta(days=2),
            company_id=SECOND_COMPANY_ID,
            location_uuid=SECOND_LOCATION_UUID,
        )
    await seed_booking_cancelled(
        session_maker,
        booking_uuid=handed_on["uuid"],
        numeric_customer_id=digits + 70,
        received_at=ATTEMPTED_AT + timedelta(days=3),
        company_id=SECOND_COMPANY_ID,
        location_uuid=SECOND_LOCATION_UUID,
    )

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: []}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
    assert reason_for(outcome, 1) == BOOKING_CAPTURE_UNPROVEN


async def test_a_delivery_that_named_nobody_is_still_somebodys_booking(session_maker, binding_key, no_network) -> None:
    """Review round 7, finding 1: a number cannot answer for a delivery with none.

    A cancellation of a booking whose creation was never captured, and which
    names no customer at all. Comparing this recipient's proven number against
    the customers such a booking named compared it against nothing and came out
    negative, so a booking the capture cannot attribute to anybody at all
    stopped being their problem. It could be theirs, and the row is unknown.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    async with session_maker() as session:
        async with session.begin():
            from altegio_bot.models.models import EasyWeekEvent

            session.add(
                EasyWeekEvent(
                    event_hint="booking-canceled",
                    query={},
                    headers={},
                    payload={
                        "uid": booking(index=88, customer_uuid=person(9)["customer_uuid"], start=NOW)["uuid"],
                        "location_id": COMPANY_ID,
                        "location_uuid": KARLSRUHE_LOCATION_UUID,
                    },
                    received_at=ATTEMPTED_AT + timedelta(days=2),
                    status="processed",
                )
            )

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: []}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
    assert reason_for(outcome, 1) == BOOKING_CAPTURE_UNPROVEN


async def test_a_proven_creator_in_the_other_branch_settles_it_for_a_bridged_card(
    session_maker, binding_key, no_network, monkeypatch: pytest.MonkeyPatch
) -> None:
    """And the control: the gap closes as soon as somebody else is proven.

    The same handover in the other branch, and this time a card of THAT branch
    carries the number its creation named against a different proven uuid. The
    booking is demonstrably somebody else's and the recipient stays provable —
    a bridge proven elsewhere neither helps nor hinders that answer.
    """
    monkeypatch.setattr(settings, "easyweek_location_map", location_map(second_branch=True), raising=False)
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    maker = person(8)
    await _proven_other_card(
        session_maker,
        numeric_id=maker["numeric_id"],
        customer_uuid=maker["customer_uuid"],
        phone="+4915100666101",
        company_id=SECOND_COMPANY_ID,
    )
    await _handed_on_booking(
        session_maker,
        index=91,
        maker_numeric_id=maker["numeric_id"],
        company_id=SECOND_COMPANY_ID,
        location_uuid=SECOND_LOCATION_UUID,
    )

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: []}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED


async def test_their_own_proven_new_booking_still_excludes_them_after_a_handover(
    session_maker, binding_key, no_network, monkeypatch: pytest.MonkeyPatch
) -> None:
    """And a booking they provably made in their own branch still excludes.

    Created inside the window under the number their own card proves, in the
    branch that card is in, and then handed on. The handover does not undo the
    visit to the booking page, and the gap the other branch would leave does
    not weaken a fact that is proven.
    """
    monkeypatch.setattr(settings, "easyweek_location_map", location_map(second_branch=True), raising=False)
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    await _handed_on_booking(session_maker, index=92, maker_numeric_id=who["numeric_id"])

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: []}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_EXCLUDED
    assert reason_for(outcome, 1) == NEW_BOOKING_AFTER_ATTEMPT


@pytest.mark.parametrize("creation_first", [True, False])
@pytest.mark.parametrize("dated", [True, False])
async def test_one_number_in_two_branches_is_two_customers(
    session_maker, binding_key, no_network, monkeypatch: pytest.MonkeyPatch, dated: bool, creation_first: bool
) -> None:
    """Review round 7, finding 2: the capture kept numbers, not pairs.

    One booking, created in one configured branch under a number and then
    cancelled in the other under the SAME number. Those digits are two
    different customer records, so the deliveries disagree about whose booking
    it is — and the capture, holding a set of bare numbers, saw one customer
    twice and reported no reassignment at all. With the booking gone from the
    recipient's list and no creator established anywhere, the matching digits
    were the only thing saying nothing had left it.

    Both delivery orders, because neither proves a business order, and with and
    without a creation instant, because losing the instant must not settle it
    either.
    """
    monkeypatch.setattr(settings, "easyweek_location_map", location_map(second_branch=True), raising=False)
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(
        session_maker,
        slots=[{"state": "delivered", "basis": RECIPIENT_BASIS_MANUAL, "numeric_id": None}],
    )
    who = person(1)
    digits = person(8)["numeric_id"]
    moved = booking(index=93, customer_uuid=person(9)["customer_uuid"], start=NOW + timedelta(days=6))

    async def _creation() -> None:
        if dated:
            await seed_booking_created(
                session_maker,
                booking_uuid=moved["uuid"],
                numeric_customer_id=digits,
                created_at=ATTEMPTED_AT + timedelta(days=2),
            )
        else:
            await seed_booking_created_without_a_date(
                session_maker,
                booking_uuid=moved["uuid"],
                numeric_customer_id=digits,
                received_at=ATTEMPTED_AT + timedelta(days=2),
            )

    async def _cancellation() -> None:
        await seed_booking_cancelled(
            session_maker,
            booking_uuid=moved["uuid"],
            numeric_customer_id=digits,
            received_at=ATTEMPTED_AT + timedelta(days=3),
            company_id=SECOND_COMPANY_ID,
            location_uuid=SECOND_LOCATION_UUID,
        )

    for step in (_creation, _cancellation) if creation_first else (_cancellation, _creation):
        await step()

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: []}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
    assert reason_for(outcome, 1) == BOOKING_CAPTURE_UNPROVEN


async def test_the_same_pair_twice_in_one_branch_is_no_handover(
    session_maker, binding_key, no_network, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The control that keeps the pair from crying wolf.

    The same branch and the same number in both deliveries: one customer
    record, named twice, and nothing about the booking has moved. A booking
    nothing moved, outside this person's list, hides nothing from it.
    """
    monkeypatch.setattr(settings, "easyweek_location_map", location_map(second_branch=True), raising=False)
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(
        session_maker,
        slots=[{"state": "delivered", "basis": RECIPIENT_BASIS_MANUAL, "numeric_id": None}],
    )
    who = person(1)
    digits = person(8)["numeric_id"]
    stayed = booking(index=94, customer_uuid=person(9)["customer_uuid"], start=NOW + timedelta(days=6))
    await seed_booking_created(
        session_maker,
        booking_uuid=stayed["uuid"],
        numeric_customer_id=digits,
        created_at=ATTEMPTED_AT + timedelta(days=2),
    )
    await seed_booking_cancelled(
        session_maker,
        booking_uuid=stayed["uuid"],
        numeric_customer_id=digits,
        received_at=ATTEMPTED_AT + timedelta(days=3),
    )

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: []}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED


async def test_two_branches_proven_to_be_one_person_move_nothing(
    session_maker, binding_key, no_network, monkeypatch: pytest.MonkeyPatch
) -> None:
    """And a branch of its own proves no change of person.

    Two different numbers in two branches, both bound by cards of this
    workspace to ONE proven uuid — somebody who has a record in each branch.
    The creation is positively that person's, the recipient is somebody else,
    and a booking of theirs moving between their own two records is not a gap
    in this recipient's history.
    """
    monkeypatch.setattr(settings, "easyweek_location_map", location_map(second_branch=True), raising=False)
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(
        session_maker,
        slots=[{"state": "delivered", "basis": RECIPIENT_BASIS_MANUAL, "numeric_id": None}],
    )
    who = person(1)
    maker = person(8)
    here, there = maker["numeric_id"], maker["numeric_id"] + 5
    await _proven_other_card(
        session_maker,
        numeric_id=here,
        customer_uuid=maker["customer_uuid"],
        phone="+4915100666102",
    )
    await _proven_other_card(
        session_maker,
        numeric_id=there,
        customer_uuid=maker["customer_uuid"],
        phone="+4915100666102",
        company_id=SECOND_COMPANY_ID,
    )
    moved = booking(index=95, customer_uuid=person(9)["customer_uuid"], start=NOW + timedelta(days=6))
    await seed_booking_created(
        session_maker,
        booking_uuid=moved["uuid"],
        numeric_customer_id=here,
        created_at=ATTEMPTED_AT + timedelta(days=2),
    )
    await seed_booking_cancelled(
        session_maker,
        booking_uuid=moved["uuid"],
        numeric_customer_id=there,
        received_at=ATTEMPTED_AT + timedelta(days=3),
        company_id=SECOND_COMPANY_ID,
        location_uuid=SECOND_LOCATION_UUID,
    )

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: []}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED


async def test_a_creation_dated_after_the_booking_was_first_heard_of_is_unknown(
    session_maker, binding_key, no_network
) -> None:
    """Review round 7, finding 3: the capture's own earlier row bounds the date.

    The attempt went out, a cancellation of this booking was stored the next
    day, and the booking's one creation delivery then claims it was made the
    day after that. A booking cannot have been created after this deployment
    was already told about it, and the half the phase acted on was the one that
    excludes somebody.

    No date is invented and the arrival time is not written as a creation date.
    The answer is that the capture's facts about this booking disagree.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    theirs = booking(
        index=96,
        customer_uuid=who["customer_uuid"],
        start=ATTEMPTED_AT - timedelta(days=1),
        canceled=True,
        completed=False,
    )
    await seed_booking_cancelled(
        session_maker,
        booking_uuid=theirs["uuid"],
        numeric_customer_id=who["numeric_id"],
        received_at=ATTEMPTED_AT + timedelta(days=1),
    )
    await seed_booking_created(
        session_maker,
        booking_uuid=theirs["uuid"],
        numeric_customer_id=who["numeric_id"],
        created_at=ATTEMPTED_AT + timedelta(days=2),
        received_at=ATTEMPTED_AT + timedelta(days=4),
    )

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: [theirs]}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
    assert reason_for(outcome, 1) == BOOKING_CAPTURE_UNPROVEN


async def test_a_creation_dated_after_the_preview_itself_is_unknown(session_maker, binding_key, no_network) -> None:
    """The same rule at the other end: a booking made in the future.

    One creation delivery, stored inside the window, stating an instant later
    than the preview is being computed at. Nothing can be created after the
    moment it is being asked about, so the claim is not a proven creation and
    the row says the capture contradicts itself.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    theirs = booking(
        index=97,
        customer_uuid=who["customer_uuid"],
        start=ATTEMPTED_AT - timedelta(days=1),
        canceled=True,
        completed=False,
    )
    await seed_booking_created(
        session_maker,
        booking_uuid=theirs["uuid"],
        numeric_customer_id=who["numeric_id"],
        created_at=NOW + timedelta(days=2),
        received_at=ATTEMPTED_AT + timedelta(days=1),
    )

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: [theirs]}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
    assert reason_for(outcome, 1) == BOOKING_CAPTURE_UNPROVEN


async def test_a_creation_dated_at_its_own_first_sighting_still_excludes(
    session_maker, binding_key, no_network
) -> None:
    """The boundary control: equal instants contradict nothing.

    The creation delivery arrived at the instant it says the booking was made,
    which is the ordinary case for a prompt webhook. It is a proven creation
    after the attempt and it excludes, as §46.5 requires.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    made = ATTEMPTED_AT + timedelta(days=1)
    theirs = booking(index=98, customer_uuid=who["customer_uuid"], start=ATTEMPTED_AT - timedelta(days=2))
    await seed_booking_created(
        session_maker,
        booking_uuid=theirs["uuid"],
        numeric_customer_id=who["numeric_id"],
        created_at=made,
        received_at=made,
    )

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: [theirs]}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_EXCLUDED
    assert reason_for(outcome, 1) == NEW_BOOKING_AFTER_ATTEMPT


async def test_a_late_delivery_of_an_old_creation_inside_the_window_is_ordinary(
    session_maker, binding_key, no_network
) -> None:
    """And the delivery order this provider is allowed stays ordinary.

    The cancellation was stored after the attempt, the creation arrived weeks
    later still, and it states a truthful instant from before the mailing.
    Nothing contradicts anything: the booking was made before the attempt and
    the recipient is provable.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    theirs = booking(
        index=99,
        customer_uuid=who["customer_uuid"],
        start=ATTEMPTED_AT - timedelta(days=1),
        canceled=True,
        completed=False,
    )
    await seed_booking_cancelled(
        session_maker,
        booking_uuid=theirs["uuid"],
        numeric_customer_id=who["numeric_id"],
        received_at=ATTEMPTED_AT + timedelta(days=1),
    )
    await seed_booking_created(
        session_maker,
        booking_uuid=theirs["uuid"],
        numeric_customer_id=who["numeric_id"],
        created_at=ATTEMPTED_AT - timedelta(days=20),
        received_at=NOW - timedelta(days=1),
    )

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: [theirs]}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED


@pytest.mark.parametrize("ground", ["another_new_booking", "a_booking_ahead"])
async def test_an_independent_ground_still_excludes_beside_a_contradicted_date(
    session_maker, binding_key, no_network, ground: str
) -> None:
    """A gap about one booking never silences a proven fact about another.

    Beside the booking whose creation postdates its own first sighting there is
    either a second booking this person provably made after the attempt, or an
    active appointment still ahead. Each excludes for its own reason.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    contradicted = booking(
        index=100,
        customer_uuid=who["customer_uuid"],
        start=ATTEMPTED_AT - timedelta(days=1),
        canceled=True,
        completed=False,
    )
    await seed_booking_cancelled(
        session_maker,
        booking_uuid=contradicted["uuid"],
        numeric_customer_id=who["numeric_id"],
        received_at=ATTEMPTED_AT + timedelta(days=1),
    )
    await seed_booking_created(
        session_maker,
        booking_uuid=contradicted["uuid"],
        numeric_customer_id=who["numeric_id"],
        created_at=ATTEMPTED_AT + timedelta(days=2),
        received_at=ATTEMPTED_AT + timedelta(days=4),
    )
    rows = [contradicted]
    if ground == "another_new_booking":
        proven_new = booking(index=101, customer_uuid=who["customer_uuid"], start=ATTEMPTED_AT - timedelta(days=3))
        await seed_booking_created(
            session_maker,
            booking_uuid=proven_new["uuid"],
            numeric_customer_id=who["numeric_id"],
            created_at=ATTEMPTED_AT + timedelta(days=1),
        )
        rows.append(proven_new)
        expected = NEW_BOOKING_AFTER_ATTEMPT
    else:
        rows.append(booking(index=102, customer_uuid=who["customer_uuid"], start=NOW + timedelta(days=3)))
        expected = FUTURE_BOOKING_ACTIVE

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: rows}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_EXCLUDED
    assert reason_for(outcome, 1) == expected


async def test_an_early_creation_that_named_no_customer_still_dates_the_booking(
    session_maker, binding_key, no_network
) -> None:
    """Review round 7, finding 4: keep the half such a delivery does prove.

    An early ``booking-created`` whose branch and booking identity are both
    proven and whose customer is missing. It names no owner, so it credits
    nobody and is no conversion — but it does prove that this booking existed
    when it was stored. The delivery used to be dropped whole, so a later
    complete creation claiming an instant days afterwards looked like a new
    booking, and the recipient was excluded for engagement the capture's own
    earlier row contradicts.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    theirs = booking(
        index=103,
        customer_uuid=who["customer_uuid"],
        start=ATTEMPTED_AT - timedelta(days=1),
        canceled=True,
        completed=False,
    )
    await seed_booking_created_without_a_customer(
        session_maker,
        booking_uuid=theirs["uuid"],
        received_at=ATTEMPTED_AT + timedelta(days=1),
        created_at=ATTEMPTED_AT + timedelta(days=3),
    )
    await seed_booking_created(
        session_maker,
        booking_uuid=theirs["uuid"],
        numeric_customer_id=who["numeric_id"],
        created_at=ATTEMPTED_AT + timedelta(days=3),
        received_at=ATTEMPTED_AT + timedelta(days=5),
    )

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: [theirs]}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
    assert reason_for(outcome, 1) == BOOKING_CAPTURE_UNPROVEN


@pytest.mark.parametrize("broken", ["foreign_branch", "unreadable_uuid", "arbitrary_payload"])
async def test_a_delivery_that_proves_no_identity_dates_nothing(
    session_maker, binding_key, no_network, broken: str
) -> None:
    """The negative controls: half of nothing is still nothing.

    A delivery from a branch nobody configured, one whose booking identity is
    unreadable, and one whose payload proves nothing at all. None of them says
    anything about this booking, so the complete creation that follows is the
    only evidence there is and it excludes.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    theirs = booking(
        index=104,
        customer_uuid=who["customer_uuid"],
        start=ATTEMPTED_AT - timedelta(days=1),
        canceled=True,
        completed=False,
    )
    if broken == "foreign_branch":
        await seed_booking_created_without_a_customer(
            session_maker,
            booking_uuid=theirs["uuid"],
            received_at=ATTEMPTED_AT + timedelta(days=1),
            company_id=UNCONFIGURED_COMPANY_ID,
        )
    elif broken == "unreadable_uuid":
        await seed_booking_created_without_a_customer(
            session_maker,
            booking_uuid="not-a-uuid-at-all",
            received_at=ATTEMPTED_AT + timedelta(days=1),
        )
    else:
        await _arbitrary_lifecycle_payload(session_maker, received_at=ATTEMPTED_AT + timedelta(days=1))
    await seed_booking_created(
        session_maker,
        booking_uuid=theirs["uuid"],
        numeric_customer_id=who["numeric_id"],
        created_at=ATTEMPTED_AT + timedelta(days=3),
        received_at=ATTEMPTED_AT + timedelta(days=5),
    )

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: [theirs]}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_EXCLUDED
    assert reason_for(outcome, 1) == NEW_BOOKING_AFTER_ATTEMPT


async def test_a_creation_that_named_only_a_booking_blocks_nobody(session_maker, binding_key, no_network) -> None:
    """And it is not a dispute of its own either.

    The only delivery about this booking proves a branch and an identity. There
    is no owner to doubt and no instant to disbelieve: it is not evidence about
    any person, and a partial row must not hold a whole batch open.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    await seed_booking_created_without_a_customer(
        session_maker,
        booking_uuid=booking(index=105, customer_uuid=person(9)["customer_uuid"], start=NOW)["uuid"],
        received_at=ATTEMPTED_AT + timedelta(days=2),
        created_at=ATTEMPTED_AT + timedelta(days=2),
    )

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: []}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED


async def test_another_branchs_card_with_the_same_digits_is_no_identity_conflict(
    session_maker, binding_key, no_network, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Review round 7, finding 5: the reverse identity check is per branch too.

    This recipient's own card proves their number in the mailing's branch. A
    different customer's card, in the other configured branch, carries the same
    digits against a different proven uuid — which is not a competing claim at
    all, because that number means one person here and another there.

    It used to read as «two people claim this number» and refuse the whole
    booking question as an identity conflict, with a completely empty history.
    """
    monkeypatch.setattr(settings, "easyweek_location_map", location_map(second_branch=True), raising=False)
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    await _proven_other_card(
        session_maker,
        numeric_id=who["numeric_id"],
        customer_uuid=person(8)["customer_uuid"],
        phone="+4915100666103",
        company_id=SECOND_COMPANY_ID,
    )

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: []}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED


async def test_two_numbers_claiming_one_person_is_still_a_conflict(
    session_maker, binding_key, no_network, monkeypatch: pytest.MonkeyPatch
) -> None:
    """And the ambiguity that is real still refuses.

    Cards of two configured branches bind two DIFFERENT numbers to this one
    proven uuid, so which number the capture's deliveries are about is
    genuinely unknown. That is an identity problem, it is reported as one, and
    reading the other direction per branch does not soften it.

    This is the shape the schema actually permits: one card per branch per
    person and one card per branch per number, so two cards of a single branch
    claiming one number cannot exist in the database at all — the conflicting
    state of the reverse lookup is defence in depth against a historical row,
    not a shape a test can seed.
    """
    monkeypatch.setattr(settings, "easyweek_location_map", location_map(second_branch=True), raising=False)
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    await _proven_other_card(
        session_maker,
        numeric_id=who["numeric_id"] + 11,
        customer_uuid=who["customer_uuid"],
        phone=who["phone"],
        company_id=SECOND_COMPANY_ID,
    )

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: []}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
    assert reason_for(outcome, 1) == IDENTITY_CONFLICT


async def test_a_frozen_cards_substituted_uuid_still_refuses_across_branches(
    session_maker, binding_key, no_network, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The other refusal the per-branch reading must not touch.

    The card the freeze bound now names a different person. That is a
    substitution, it is refused, and a second branch carrying the same digits
    neither excuses nor explains it.
    """
    monkeypatch.setattr(settings, "easyweek_location_map", location_map(second_branch=True), raising=False)
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    await _proven_other_card(
        session_maker,
        numeric_id=who["numeric_id"],
        customer_uuid=person(8)["customer_uuid"],
        phone="+4915100666104",
        company_id=SECOND_COMPANY_ID,
    )
    async with session_maker() as session:
        async with session.begin():
            card = (
                await session.execute(
                    select(Client)
                    .where(Client.easyweek_customer_uuid == uuid_module.UUID(who["customer_uuid"]))
                    .where(Client.company_id == COMPANY_ID)
                )
            ).scalar_one()
            card.easyweek_customer_uuid = uuid_module.UUID("66666666-1111-4111-8111-111111111111")

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: []}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
    assert reason_for(outcome, 1) == IDENTITY_CONFLICT


async def _a_booking_created_in_both_branches(
    session_maker,
    *,
    index: int,
    here: int,
    there: int,
    start: datetime,
    holder_uuid: str | None = None,
) -> dict[str, Any]:
    """One booking uuid, two creation deliveries, two branches.

    The pairs are what the deliveries really named: (this branch, ``here``) and
    (the other branch, ``there``). A cartesian product of the two branches and
    the two numbers would invent two more customers nothing observed.
    """
    contested = booking(
        index=index,
        customer_uuid=holder_uuid or person(9)["customer_uuid"],
        start=start,
    )
    await seed_booking_created(
        session_maker,
        booking_uuid=contested["uuid"],
        numeric_customer_id=here,
        created_at=ATTEMPTED_AT + timedelta(days=2),
    )
    await seed_booking_created(
        session_maker,
        booking_uuid=contested["uuid"],
        numeric_customer_id=there,
        created_at=ATTEMPTED_AT + timedelta(days=2),
        company_id=SECOND_COMPANY_ID,
        location_uuid=SECOND_LOCATION_UUID,
    )
    return contested


@pytest.mark.parametrize("shape", ["one_stranger", "two_strangers", "one_unlinked", "might_be_theirs"])
async def test_what_the_observed_pairs_prove_decides_whose_dispute_it_is(
    session_maker, binding_key, no_network, monkeypatch: pytest.MonkeyPatch, shape: str
) -> None:
    """Review round 7, finding 6: resolve the pairs, not the product.

    Two creation deliveries of one booking name two branches, and each names a
    number in its own. Both pairs are read back through cards of this
    workspace, and what they prove decides whose gap this is:

    * both to one stranger, or to two different strangers — somebody else's
      booking either way, and this manual recipient is left alone;
    * one of them to nobody — nothing is established, and the booking could be
      theirs;
    * one of them to the recipient — it could be their own, and the dispute is
      precisely about which.

    The branches used to disagree and that was the end of it: no owner could be
    named at all, so a stranger's contradiction held every bridge-less
    recipient open.
    """
    monkeypatch.setattr(settings, "easyweek_location_map", location_map(second_branch=True), raising=False)
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(
        session_maker,
        slots=[{"state": "delivered", "basis": RECIPIENT_BASIS_MANUAL, "numeric_id": None}],
    )
    who = person(1)
    here, there = person(8)["numeric_id"], person(10)["numeric_id"]
    await _proven_other_card(
        session_maker,
        numeric_id=here,
        customer_uuid=person(8)["customer_uuid"],
        phone="+4915100666105",
    )
    if shape != "one_unlinked":
        owner_there = {
            "one_stranger": person(8),
            "two_strangers": person(11),
            "might_be_theirs": who,
        }[shape]
        await _proven_other_card(
            session_maker,
            numeric_id=there,
            customer_uuid=owner_there["customer_uuid"],
            phone="+4915100666106",
            company_id=SECOND_COMPANY_ID,
        )
    await _a_booking_created_in_both_branches(
        session_maker,
        index=106,
        here=here,
        there=there,
        start=NOW + timedelta(days=5),
    )

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: []}),
    )
    if shape in {"one_stranger", "two_strangers"}:
        assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED
    else:
        assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
        assert reason_for(outcome, 1) == BOOKING_CAPTURE_UNPROVEN


async def test_a_two_branch_dispute_in_their_own_history_stays_unknown(
    session_maker, binding_key, no_network, monkeypatch: pytest.MonkeyPatch
) -> None:
    """And the same booking inside their list is their problem after all.

    Both pairs are proven to be one stranger's, and the booking is nevertheless
    in this recipient's current history — so something about it is wrong, and
    «nothing new since the attempt» cannot be asserted over a list that
    contains it. Fail-closed.
    """
    monkeypatch.setattr(settings, "easyweek_location_map", location_map(second_branch=True), raising=False)
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(
        session_maker,
        slots=[{"state": "delivered", "basis": RECIPIENT_BASIS_MANUAL, "numeric_id": None}],
    )
    who = person(1)
    maker = person(8)
    here, there = maker["numeric_id"], maker["numeric_id"] + 9
    for number, company in ((here, COMPANY_ID), (there, SECOND_COMPANY_ID)):
        await _proven_other_card(
            session_maker,
            numeric_id=number,
            customer_uuid=maker["customer_uuid"],
            phone="+4915100666107",
            company_id=company,
        )
    contested = await _a_booking_created_in_both_branches(
        session_maker,
        index=107,
        here=here,
        there=there,
        start=ATTEMPTED_AT - timedelta(days=2),
        holder_uuid=who["customer_uuid"],
    )

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: [contested]}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
    assert reason_for(outcome, 1) == BOOKING_CAPTURE_UNPROVEN


async def test_the_owner_lookup_stays_inside_one_statements_parameter_limit(
    session_maker, binding_key, no_network
) -> None:
    """Review round 7, finding 7: one query, a bounded number of parameters.

    The reverse owner lookup asks about every (branch, number) pair the capture
    named, and an old mailing's branch can easily have named tens of thousands
    of them. A single statement carrying one parameter per number crossed the
    wire protocol's own ceiling at 32 766 ids and came back to the operator as
    «the database is unavailable» — an infrastructure answer to an ordinary
    amount of history.

    The proven card sits at the very TOP of the range asked about, so a
    chunking that quietly dropped its last batch would lose it, and an empty
    answer substituted for the error would too.
    """
    from altegio_bot.campaigns.easyweek_voucher_followup import booking_evidence
    from altegio_bot.easyweek_locations import configured_easyweek_locations

    maker = person(8)
    await _proven_other_card(
        session_maker,
        numeric_id=maker["numeric_id"],
        customer_uuid=maker["customer_uuid"],
        phone="+4915100666108",
    )
    wanted = {(COMPANY_ID, number) for number in range(1, 32_766)}
    wanted.add((COMPANY_ID, maker["numeric_id"]))
    assert len(wanted) == 32_766

    async with session_maker() as session:
        owners = await booking_evidence._creation_owner_uuids(
            session,
            wanted=wanted,
            registry=configured_easyweek_locations(),
        )

    assert owners.owner_in(COMPANY_ID, maker["numeric_id"]) == uuid_module.UUID(maker["customer_uuid"])
    assert owners.owner_in(COMPANY_ID, 1) is None
    assert owners.is_conflicting(COMPANY_ID, maker["numeric_id"]) is False


async def test_a_truncated_scan_never_asks_the_owner_lookup(
    session_maker, binding_key, no_network, monkeypatch: pytest.MonkeyPatch
) -> None:
    """And a pass that already has no answer asks no further question.

    A truncated scan is unknown for everybody, so the per-pair owner lookup can
    only be work with nothing to decide — and with a capture large enough to
    truncate, it was also the query that failed first, turning the planned
    incomplete result into a 503. The lookup is made unavailable here, which is
    how the test proves the truncated pass no longer reaches it.
    """
    from altegio_bot.campaigns.easyweek_voucher_followup import booking_evidence
    from altegio_bot.campaigns.easyweek_voucher_production import booking_attribution

    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    row = booking(index=108, customer_uuid=who["customer_uuid"], start=ATTEMPTED_AT - timedelta(days=2))
    for offset, made in enumerate((3, 4, 5)):
        await seed_booking_created(
            session_maker,
            booking_uuid=booking(index=110 + offset, customer_uuid=who["customer_uuid"], start=NOW)["uuid"],
            numeric_customer_id=who["numeric_id"],
            created_at=ATTEMPTED_AT - timedelta(days=made),
        )

    async def refuse(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("a truncated scan must not ask the owner lookup")

    monkeypatch.setattr(booking_attribution, "MAX_SCANNED_EVENTS", 1, raising=False)
    monkeypatch.setattr(booking_evidence, "_creation_owner_uuids", refuse)

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: [row]}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
    assert reason_for(outcome, 1) == BOOKING_CAPTURE_UNPROVEN


# ---------------------------------------------------------------------------
# What one delivery proves, kept apart from what it leaves open
# (round 8, fixes 1-4)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("cancellation_first", [False, True])
async def test_a_nameless_cancellation_from_another_branch_can_still_be_a_move(
    session_maker, binding_key, no_network, monkeypatch: pytest.MonkeyPatch, cancellation_first: bool
) -> None:
    """Review round 8, finding 1: the branch survives a missing customer id.

    The booking was created in one configured branch under a number nothing
    can resolve, and a cancellation of the same booking arrived from the OTHER
    configured branch naming no customer at all. The pair cannot be formed, so
    the capture reported no reassignment — and a manual recipient with a
    completely empty history came out `included`, although the booking may be
    theirs and may have left their list unseen.

    The same delivery WITH a number already read as a move, which is what made
    the gap visible: the missing field decided the answer. Both delivery
    orders, because neither proves a business order.
    """
    monkeypatch.setattr(settings, "easyweek_location_map", location_map(second_branch=True), raising=False)
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(
        session_maker,
        slots=[{"state": "delivered", "basis": RECIPIENT_BASIS_MANUAL, "numeric_id": None}],
    )
    who = person(1)
    moved = booking(index=120, customer_uuid=person(9)["customer_uuid"], start=NOW + timedelta(days=6))

    async def _creation() -> None:
        await seed_booking_created(
            session_maker,
            booking_uuid=moved["uuid"],
            numeric_customer_id=person(8)["numeric_id"],
            created_at=ATTEMPTED_AT + timedelta(days=2),
        )

    async def _cancellation() -> None:
        await seed_booking_lifecycle(
            session_maker,
            booking_uuid=moved["uuid"],
            received_at=ATTEMPTED_AT + timedelta(days=3),
            company_id=SECOND_COMPANY_ID,
            location_uuid=SECOND_LOCATION_UUID,
        )

    for step in (_cancellation, _creation) if cancellation_first else (_creation, _cancellation):
        await step()

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: []}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
    assert reason_for(outcome, 1) == BOOKING_CAPTURE_UNPROVEN


async def test_a_nameless_cancellation_elsewhere_about_an_old_booking_is_no_gap(
    session_maker, binding_key, no_network, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The control that keeps the branch from crying wolf: an old creation.

    The identical shape with the creation dated BEFORE the mailing. Whatever
    happened to that booking in the other branch, it was not made since the
    attempt, and §46.5's creation rule has nothing to ask about it.
    """
    monkeypatch.setattr(settings, "easyweek_location_map", location_map(second_branch=True), raising=False)
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(
        session_maker,
        slots=[{"state": "delivered", "basis": RECIPIENT_BASIS_MANUAL, "numeric_id": None}],
    )
    who = person(1)
    old = booking(index=121, customer_uuid=person(9)["customer_uuid"], start=NOW + timedelta(days=6))
    await seed_booking_created(
        session_maker,
        booking_uuid=old["uuid"],
        numeric_customer_id=person(8)["numeric_id"],
        created_at=ATTEMPTED_AT - timedelta(days=20),
    )
    await seed_booking_lifecycle(
        session_maker,
        booking_uuid=old["uuid"],
        received_at=ATTEMPTED_AT + timedelta(days=3),
        company_id=SECOND_COMPANY_ID,
        location_uuid=SECOND_LOCATION_UUID,
    )

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: []}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED


async def test_a_booking_of_their_own_does_not_answer_for_the_one_that_left(
    session_maker, binding_key, no_network, monkeypatch: pytest.MonkeyPatch
) -> None:
    """And a non-empty history does not close the gap either.

    Their list holds one booking of their own, made long before the mailing
    and provably so. The booking that may have left it is still unaccounted
    for, so the row stays unknown rather than being answered by what remains.
    """
    monkeypatch.setattr(settings, "easyweek_location_map", location_map(second_branch=True), raising=False)
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(
        session_maker,
        slots=[{"state": "delivered", "basis": RECIPIENT_BASIS_MANUAL, "numeric_id": None}],
    )
    who = person(1)
    theirs = booking(
        index=122,
        customer_uuid=who["customer_uuid"],
        start=ATTEMPTED_AT - timedelta(days=2),
        canceled=True,
        completed=False,
    )
    await seed_booking_created(
        session_maker,
        booking_uuid=theirs["uuid"],
        numeric_customer_id=person(12)["numeric_id"],
        created_at=ATTEMPTED_AT - timedelta(days=30),
    )
    moved = booking(index=123, customer_uuid=person(9)["customer_uuid"], start=NOW + timedelta(days=6))
    await seed_booking_created(
        session_maker,
        booking_uuid=moved["uuid"],
        numeric_customer_id=person(8)["numeric_id"],
        created_at=ATTEMPTED_AT + timedelta(days=2),
    )
    await seed_booking_lifecycle(
        session_maker,
        booking_uuid=moved["uuid"],
        received_at=ATTEMPTED_AT + timedelta(days=3),
        company_id=SECOND_COMPANY_ID,
        location_uuid=SECOND_LOCATION_UUID,
    )

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: [theirs]}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
    assert reason_for(outcome, 1) == BOOKING_CAPTURE_UNPROVEN


async def test_a_proven_creator_settles_a_nameless_cancellation_elsewhere(
    session_maker, binding_key, no_network, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The control: a proven foreign creator closes it, as it always has.

    The creation's own number resolves, through a card of its own branch, to a
    different proven uuid. Whatever the other branch did to that booking, it
    was never this recipient's.
    """
    monkeypatch.setattr(settings, "easyweek_location_map", location_map(second_branch=True), raising=False)
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
        phone="+4915100777101",
    )
    moved = booking(index=124, customer_uuid=person(9)["customer_uuid"], start=NOW + timedelta(days=6))
    await seed_booking_created(
        session_maker,
        booking_uuid=moved["uuid"],
        numeric_customer_id=maker["numeric_id"],
        created_at=ATTEMPTED_AT + timedelta(days=2),
    )
    await seed_booking_lifecycle(
        session_maker,
        booking_uuid=moved["uuid"],
        received_at=ATTEMPTED_AT + timedelta(days=3),
        company_id=SECOND_COMPANY_ID,
        location_uuid=SECOND_LOCATION_UUID,
    )

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: []}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED


@pytest.mark.parametrize("named", [False, True])
async def test_what_a_cancellation_from_the_creations_own_branch_proves(
    session_maker, binding_key, no_network, monkeypatch: pytest.MonkeyPatch, named: bool
) -> None:
    """Review round 9, finding 1: one branch proves no owner.

    The cancellation comes from the branch the creation itself named. With the
    creation's own number on it, that is the same customer record twice: a
    repeat, nothing moved, and nothing for a recipient's own list to be hiding.

    With no number on it the branch agrees and the person does not: a numeric
    customer id is unique per branch, so a branch holds many of them, and
    "somewhere in this branch" is not "the same customer". The round-8 revision
    of this test expected `included` for that shape, which was the defect — the
    delivery leaves the booking's custody unproven wherever it came from.
    """
    monkeypatch.setattr(settings, "easyweek_location_map", location_map(second_branch=True), raising=False)
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(
        session_maker,
        slots=[{"state": "delivered", "basis": RECIPIENT_BASIS_MANUAL, "numeric_id": None}],
    )
    who = person(1)
    digits = person(8)["numeric_id"]
    stayed = booking(index=125, customer_uuid=person(9)["customer_uuid"], start=NOW + timedelta(days=6))
    await seed_booking_created(
        session_maker,
        booking_uuid=stayed["uuid"],
        numeric_customer_id=digits,
        created_at=ATTEMPTED_AT + timedelta(days=2),
    )
    await seed_booking_lifecycle(
        session_maker,
        booking_uuid=stayed["uuid"],
        received_at=ATTEMPTED_AT + timedelta(days=3),
        numeric_customer_id=digits if named else None,
    )

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: []}),
    )
    if named:
        assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED
    else:
        assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
        assert reason_for(outcome, 1) == BOOKING_CAPTURE_UNPROVEN


async def _partial_and_full_creation(
    session_maker,
    *,
    booking_uuid: str,
    numeric_id: int,
    full_at: datetime,
    partial_at: datetime,
    partial_company_id: int = COMPANY_ID,
    partial_location_uuid: str = KARLSRUHE_LOCATION_UUID,
    partial_first: bool = False,
) -> None:
    """One booking uuid, one complete creation and one that named no customer.

    The partial delivery proves its branch and states an instant; it names
    nobody, so it credits nobody. Those two facts are still facts about this
    booking, and a complete creation of the same booking has to agree with
    them.
    """

    async def _full() -> None:
        await seed_booking_created(
            session_maker,
            booking_uuid=booking_uuid,
            numeric_customer_id=numeric_id,
            created_at=full_at,
            received_at=NOW - timedelta(days=2),
        )

    async def _partial() -> None:
        await seed_booking_created_without_a_customer(
            session_maker,
            booking_uuid=booking_uuid,
            received_at=NOW - timedelta(days=1),
            created_at=partial_at,
            company_id=partial_company_id,
            location_uuid=partial_location_uuid,
        )

    for step in (_partial, _full) if partial_first else (_full, _partial):
        await step()


@pytest.mark.parametrize("partial_first", [False, True])
@pytest.mark.parametrize("made_after_the_attempt", [False, True])
async def test_a_partial_creation_from_another_branch_contradicts_the_full_one(
    session_maker,
    binding_key,
    no_network,
    monkeypatch: pytest.MonkeyPatch,
    made_after_the_attempt: bool,
    partial_first: bool,
) -> None:
    """Review round 8, finding 2: two creations, one of them without an owner.

    Both deliveries state the same instant and name two different configured
    branches, which cannot both be where this booking was made. The complete
    creation used to decide alone, in both directions: the booking read as
    this person's engagement after the mailing, or as their quiet old booking,
    depending only on the date the complete delivery happened to carry.

    The partial delivery still names nobody and counts as no conversion. What
    it proves — a branch — is enough to know that the record contradicts
    itself, and with no independent ground the row is unknown.
    """
    monkeypatch.setattr(settings, "easyweek_location_map", location_map(second_branch=True), raising=False)
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    made = ATTEMPTED_AT + timedelta(days=2) if made_after_the_attempt else ATTEMPTED_AT - timedelta(days=20)
    contested = booking(
        index=126,
        customer_uuid=who["customer_uuid"],
        start=ATTEMPTED_AT - timedelta(days=1),
        canceled=True,
        completed=False,
    )
    await _partial_and_full_creation(
        session_maker,
        booking_uuid=contested["uuid"],
        numeric_id=who["numeric_id"],
        full_at=made,
        partial_at=made,
        partial_company_id=SECOND_COMPANY_ID,
        partial_location_uuid=SECOND_LOCATION_UUID,
        partial_first=partial_first,
    )

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: [] if made_after_the_attempt else [contested]}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
    assert reason_for(outcome, 1) == BOOKING_CAPTURE_UNPROVEN


@pytest.mark.parametrize("partial_first", [False, True])
@pytest.mark.parametrize("full_is_the_older", [False, True])
async def test_two_creation_instants_straddling_the_send_are_a_contradiction(
    session_maker, binding_key, no_network, full_is_the_older: bool, partial_first: bool
) -> None:
    """Review round 8, finding 2: and the same when only the instants differ.

    One configured branch, two creation deliveries of one booking, and two
    explicit instants — one before the mailing and one after it. Both
    deliveries were stored later than both instants, so nothing about their
    arrival settles anything.

    Whichever instant the COMPLETE delivery carried used to be the answer: the
    earlier one left the person `included`, the later one excluded them for a
    booking made since the mailing. The two claims cannot both be true and
    neither is proven, so the row is unknown.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    older, newer = ATTEMPTED_AT - timedelta(days=20), ATTEMPTED_AT + timedelta(days=2)
    contested = booking(
        index=127,
        customer_uuid=who["customer_uuid"],
        start=ATTEMPTED_AT - timedelta(days=1),
        canceled=True,
        completed=False,
    )
    await _partial_and_full_creation(
        session_maker,
        booking_uuid=contested["uuid"],
        numeric_id=who["numeric_id"],
        full_at=older if full_is_the_older else newer,
        partial_at=newer if full_is_the_older else older,
        partial_first=partial_first,
    )

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: [contested] if full_is_the_older else []}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
    assert reason_for(outcome, 1) == BOOKING_CAPTURE_UNPROVEN


@pytest.mark.parametrize("partial_first", [False, True])
async def test_a_partial_creation_that_agrees_is_an_ordinary_repeat(
    session_maker, binding_key, no_network, partial_first: bool
) -> None:
    """The control: agreeing with the complete delivery contradicts nothing.

    The same branch and the same instant, stated twice, once without an owner.
    That is a repeat, the booking stays usable evidence, and this person's own
    booking made after the mailing still excludes them.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    made = ATTEMPTED_AT + timedelta(days=2)
    theirs = booking(index=128, customer_uuid=who["customer_uuid"], start=ATTEMPTED_AT - timedelta(days=1))
    await _partial_and_full_creation(
        session_maker,
        booking_uuid=theirs["uuid"],
        numeric_id=who["numeric_id"],
        full_at=made,
        partial_at=made,
        partial_first=partial_first,
    )

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: [theirs]}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_EXCLUDED
    assert reason_for(outcome, 1) == NEW_BOOKING_AFTER_ATTEMPT


@pytest.mark.parametrize("unusable", ["unconfigured_branch", "unreadable_uuid", "arbitrary_payload"])
async def test_a_partial_creation_that_proves_nothing_contradicts_nothing(
    session_maker, binding_key, no_network, unusable: str
) -> None:
    """The negative controls: no branch or no booking, no contradiction.

    A partial creation from a branch nobody configured, one whose booking
    identity is unreadable, and one whose payload proves nothing at all. Each
    carries an instant that disagrees with the complete creation, and none of
    them is about a booking of this workspace — so the complete creation is
    still the only evidence there is, and it excludes.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    theirs = booking(index=129, customer_uuid=who["customer_uuid"], start=ATTEMPTED_AT - timedelta(days=1))
    await seed_booking_created(
        session_maker,
        booking_uuid=theirs["uuid"],
        numeric_customer_id=who["numeric_id"],
        created_at=ATTEMPTED_AT + timedelta(days=2),
        received_at=NOW - timedelta(days=2),
    )
    if unusable == "unconfigured_branch":
        await seed_booking_created_without_a_customer(
            session_maker,
            booking_uuid=theirs["uuid"],
            received_at=NOW - timedelta(days=1),
            created_at=ATTEMPTED_AT - timedelta(days=30),
            company_id=UNCONFIGURED_COMPANY_ID,
        )
    elif unusable == "unreadable_uuid":
        await seed_booking_created_without_a_customer(
            session_maker,
            booking_uuid="not-a-uuid-at-all",
            received_at=NOW - timedelta(days=1),
            created_at=ATTEMPTED_AT - timedelta(days=30),
        )
    else:
        await _arbitrary_lifecycle_payload(session_maker, received_at=NOW - timedelta(days=1))

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: [theirs]}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_EXCLUDED
    assert reason_for(outcome, 1) == NEW_BOOKING_AFTER_ATTEMPT


async def test_a_lone_partial_creation_blocks_neither_recipient_of_the_batch(
    session_maker, binding_key, no_network
) -> None:
    """Review round 8, finding 2: and alone it is still nobody's problem.

    One partial creation, outside both recipients' histories, with nothing
    else about that booking anywhere. It names no owner and no second delivery
    disagrees with it, so there is nothing in dispute — and a whole batch may
    not be held open on a row whose owner nobody ever claimed.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(
        session_maker,
        slots=[
            {"state": "delivered", "basis": RECIPIENT_BASIS_MANUAL, "numeric_id": None},
            {"state": "delivered", "index": 2, "basis": RECIPIENT_BASIS_MANUAL, "numeric_id": None},
        ],
    )
    await seed_booking_created_without_a_customer(
        session_maker,
        booking_uuid=booking(index=130, customer_uuid=person(9)["customer_uuid"], start=NOW)["uuid"],
        received_at=ATTEMPTED_AT + timedelta(days=2),
        created_at=ATTEMPTED_AT + timedelta(days=2),
    )

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={person(1)["customer_uuid"]: [], person(2)["customer_uuid"]: []}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED
    assert decision_for(outcome, 2) == FOLLOWUP_ROW_INCLUDED


async def test_an_update_naming_another_holder_does_not_name_the_creator(
    session_maker, binding_key, no_network, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Review round 8, finding 3: who holds a booking is not who made it.

    No creation of this booking was ever captured, and a cancellation naming
    no customer arrived from a branch this recipient has no card in — so
    whether the booking was theirs is open, and the row is unknown. Adding a
    `booking-updated` that names a DIFFERENT customer in their own branch used
    to close it as `included`: the delivery was read as an answer about the
    creator.

    It is not one. An update names a later holder, and a booking this person
    made and then handed on is exactly the case the gap is about.
    """
    monkeypatch.setattr(settings, "easyweek_location_map", location_map(second_branch=True), raising=False)
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    gone = booking(index=131, customer_uuid=person(9)["customer_uuid"], start=NOW + timedelta(days=5))
    await seed_booking_lifecycle(
        session_maker,
        booking_uuid=gone["uuid"],
        received_at=ATTEMPTED_AT + timedelta(days=2),
        company_id=SECOND_COMPANY_ID,
        location_uuid=SECOND_LOCATION_UUID,
    )
    await seed_booking_lifecycle(
        session_maker,
        booking_uuid=gone["uuid"],
        event_hint="booking-updated",
        numeric_customer_id=person(8)["numeric_id"],
        received_at=ATTEMPTED_AT + timedelta(days=3),
    )

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: []}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
    assert reason_for(outcome, 1) == BOOKING_CAPTURE_UNPROVEN


async def test_a_partial_creation_elsewhere_with_a_late_cancellation_here_is_unknown(
    session_maker, binding_key, no_network, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The second shape of the same question: a creation that named nobody.

    The only creation delivery of this booking came from the other configured
    branch and named no customer, and the cancellation that followed in this
    branch names somebody else. Nothing says who made it, this recipient's own
    number is proven in one branch only, and their list cannot show a booking
    that left it.
    """
    monkeypatch.setattr(settings, "easyweek_location_map", location_map(second_branch=True), raising=False)
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    gone = booking(index=132, customer_uuid=person(9)["customer_uuid"], start=NOW + timedelta(days=5))
    await seed_booking_created_without_a_customer(
        session_maker,
        booking_uuid=gone["uuid"],
        received_at=ATTEMPTED_AT + timedelta(days=2),
        created_at=ATTEMPTED_AT + timedelta(days=1),
        company_id=SECOND_COMPANY_ID,
        location_uuid=SECOND_LOCATION_UUID,
    )
    await seed_booking_lifecycle(
        session_maker,
        booking_uuid=gone["uuid"],
        numeric_customer_id=person(8)["numeric_id"],
        received_at=ATTEMPTED_AT + timedelta(days=4),
    )

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: []}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
    assert reason_for(outcome, 1) == BOOKING_CAPTURE_UNPROVEN


async def test_two_updates_for_two_holders_without_a_creation_stay_unknown(
    session_maker, binding_key, no_network
) -> None:
    """And two holders in a row answer the creator question no better than one.

    Two `booking-updated` deliveries naming two different customers of this
    branch, and no creation anywhere. The booking passed through two pairs of
    hands after the mailing and nothing says whose it was first.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    gone = booking(index=133, customer_uuid=person(9)["customer_uuid"], start=NOW + timedelta(days=5))
    for offset, holder in ((2, person(8)), (3, person(10))):
        await seed_booking_lifecycle(
            session_maker,
            booking_uuid=gone["uuid"],
            event_hint="booking-updated",
            numeric_customer_id=holder["numeric_id"],
            received_at=ATTEMPTED_AT + timedelta(days=offset),
        )

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: []}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
    assert reason_for(outcome, 1) == BOOKING_CAPTURE_UNPROVEN


async def test_a_proven_foreign_creator_answers_the_holder_question(
    session_maker, binding_key, no_network, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The control: the creator, when it is proven, still settles everything.

    The booking's own creation delivery names a number that resolves, through
    a card of its branch, to a different proven uuid. Whoever held it
    afterwards, it was never this recipient's booking.
    """
    monkeypatch.setattr(settings, "easyweek_location_map", location_map(second_branch=True), raising=False)
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    maker = person(8)
    await _proven_other_card(
        session_maker,
        numeric_id=maker["numeric_id"],
        customer_uuid=maker["customer_uuid"],
        phone="+4915100777102",
    )
    gone = booking(index=134, customer_uuid=person(9)["customer_uuid"], start=NOW + timedelta(days=5))
    await seed_booking_created(
        session_maker,
        booking_uuid=gone["uuid"],
        numeric_customer_id=maker["numeric_id"],
        created_at=ATTEMPTED_AT + timedelta(days=1),
    )
    await seed_booking_lifecycle(
        session_maker,
        booking_uuid=gone["uuid"],
        event_hint="booking-updated",
        numeric_customer_id=person(10)["numeric_id"],
        received_at=ATTEMPTED_AT + timedelta(days=3),
    )

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: []}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED


async def test_a_booking_already_known_before_the_attempt_answers_it_too(
    session_maker, binding_key, no_network
) -> None:
    """The other control: a booking this deployment had already heard of.

    No creation was captured and the holders disagree, but a valid delivery
    about this booking was stored before the mailing went out — so whatever it
    is, it is not a booking made since. Nothing contradicts that, and no
    creation date is invented to say it.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    known = booking(index=135, customer_uuid=person(9)["customer_uuid"], start=NOW + timedelta(days=5))
    await seed_booking_lifecycle(
        session_maker,
        booking_uuid=known["uuid"],
        received_at=ATTEMPTED_AT - timedelta(days=10),
    )
    await seed_booking_lifecycle(
        session_maker,
        booking_uuid=known["uuid"],
        event_hint="booking-updated",
        numeric_customer_id=person(8)["numeric_id"],
        received_at=ATTEMPTED_AT + timedelta(days=3),
    )

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: []}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED


@pytest.mark.parametrize("ground", ["their_own_new_booking", "a_booking_ahead"])
async def test_an_independent_ground_still_excludes_beside_a_holder_question(
    session_maker, binding_key, no_network, ground: str
) -> None:
    """And a proven fact about them still excludes beside the open question.

    The unaccounted booking with its two holders sits beside either a booking
    this person provably made after the mailing or an appointment still ahead.
    Each excludes for its own reason, and neither is weakened by a gap about
    somebody else's record.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    gone = booking(index=136, customer_uuid=person(9)["customer_uuid"], start=NOW + timedelta(days=5))
    for offset, holder in ((2, person(8)), (3, person(10))):
        await seed_booking_lifecycle(
            session_maker,
            booking_uuid=gone["uuid"],
            event_hint="booking-updated",
            numeric_customer_id=holder["numeric_id"],
            received_at=ATTEMPTED_AT + timedelta(days=offset),
        )
    rows = []
    if ground == "their_own_new_booking":
        theirs = booking(index=137, customer_uuid=who["customer_uuid"], start=ATTEMPTED_AT - timedelta(days=1))
        await seed_booking_created(
            session_maker,
            booking_uuid=theirs["uuid"],
            numeric_customer_id=who["numeric_id"],
            created_at=ATTEMPTED_AT + timedelta(days=1),
        )
        rows.append(theirs)
        expected = NEW_BOOKING_AFTER_ATTEMPT
    else:
        rows.append(booking(index=138, customer_uuid=who["customer_uuid"], start=NOW + timedelta(days=3)))
        expected = FUTURE_BOOKING_ACTIVE

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: rows}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_EXCLUDED
    assert reason_for(outcome, 1) == expected


@pytest.mark.parametrize("over_the_cap", [False, True])
async def test_the_scan_is_complete_at_its_cap_and_truncated_one_past_it(
    session_maker, binding_key, no_network, monkeypatch: pytest.MonkeyPatch, over_the_cap: bool
) -> None:
    """The row cap's own boundary, asserted at the cap rather than near it.

    Exactly as many stored deliveries as the cap allows is a COMPLETE pass and
    a real answer about this person; one more is the planned incomplete result,
    unknown for everybody. The cap itself is unchanged — it is lowered here so
    the boundary can be crossed in an ordinary test run.
    """
    from altegio_bot.campaigns.easyweek_voucher_production import booking_attribution

    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    theirs = booking(
        index=139,
        customer_uuid=who["customer_uuid"],
        start=ATTEMPTED_AT - timedelta(days=1),
        canceled=True,
        completed=False,
    )
    await seed_booking_created(
        session_maker,
        booking_uuid=theirs["uuid"],
        numeric_customer_id=who["numeric_id"],
        created_at=ATTEMPTED_AT - timedelta(days=9),
    )
    for offset in range(2):
        await seed_booking_created(
            session_maker,
            booking_uuid=booking(index=140 + offset, customer_uuid=person(9)["customer_uuid"], start=NOW)["uuid"],
            numeric_customer_id=person(9)["numeric_id"],
            created_at=ATTEMPTED_AT - timedelta(days=8 - offset),
        )
    if over_the_cap:
        await seed_booking_created(
            session_maker,
            booking_uuid=booking(index=142, customer_uuid=person(9)["customer_uuid"], start=NOW)["uuid"],
            numeric_customer_id=person(9)["numeric_id"],
            created_at=ATTEMPTED_AT - timedelta(days=5),
        )
    monkeypatch.setattr(booking_attribution, "MAX_SCANNED_EVENTS", 3, raising=False)

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: [theirs]}),
    )
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
    capture = stored.evidence["booking_creation_capture"]
    if over_the_cap:
        assert capture["truncated"] is True
        assert capture["scanned_events"] == 3
        assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
        assert reason_for(outcome, 1) == BOOKING_CAPTURE_UNPROVEN
    else:
        assert capture["truncated"] is False
        assert capture["scanned_events"] == 3
        assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED


# ---------------------------------------------------------------------------
# An unproven owner stays unproven, wherever the decision is made
# (round 9, fixes 1-3)
# ---------------------------------------------------------------------------


async def _a_booking_with_a_nameless_cancellation(
    session_maker,
    *,
    index: int,
    shape: str,
    cancellation_branch: int,
    cancellation_location: str,
    cancellation_first: bool,
    holder_uuid: str | None = None,
    maker_numeric_id: int | None = None,
) -> dict[str, Any]:
    """A booking whose creator nothing resolves and whose custody is unproven.

    The creation names a numeric customer id no card of this workspace
    bridges, so WHO made it is open. The cancellation names no customer at
    all, so who holds it now is open too — and the branch it came from does
    not close that, because a branch holds many customer records.
    """
    row = booking(
        index=index,
        customer_uuid=holder_uuid or person(9)["customer_uuid"],
        start=ATTEMPTED_AT - timedelta(days=1),
        canceled=True,
        completed=False,
    )
    digits = maker_numeric_id if maker_numeric_id is not None else person(8)["numeric_id"]

    async def _creation() -> None:
        if shape == "undated":
            await seed_booking_created_without_a_date(
                session_maker,
                booking_uuid=row["uuid"],
                numeric_customer_id=digits,
                received_at=ATTEMPTED_AT - timedelta(days=15),
            )
            return
        await seed_booking_created(
            session_maker,
            booking_uuid=row["uuid"],
            numeric_customer_id=digits,
            created_at=(
                ATTEMPTED_AT + timedelta(days=2) if shape == "new_dated" else ATTEMPTED_AT - timedelta(days=20)
            ),
        )

    async def _cancellation() -> None:
        await seed_booking_lifecycle(
            session_maker,
            booking_uuid=row["uuid"],
            received_at=ATTEMPTED_AT + timedelta(days=3),
            company_id=cancellation_branch,
            location_uuid=cancellation_location,
        )

    for step in (_cancellation, _creation) if cancellation_first else (_creation, _cancellation):
        await step()
    return row


@pytest.mark.parametrize("cancellation_first", [False, True])
@pytest.mark.parametrize("branch", ["same", "another"])
@pytest.mark.parametrize("shape", ["new_dated", "old_dated", "undated"])
async def test_an_unproven_custody_reaches_a_visible_bookings_attribution(
    session_maker,
    binding_key,
    no_network,
    monkeypatch: pytest.MonkeyPatch,
    shape: str,
    branch: str,
    cancellation_first: bool,
) -> None:
    """Review round 9, finding 1: the same doubt, applied to every decision.

    A manual recipient with no numeric bridge SEES this booking in their
    current history. Its creation names a customer id nothing resolves, and a
    cancellation of it named no customer at all — so neither who made it nor
    who holds it is established.

    The permissive rule for a card with no bridge — "a booking nothing moved
    belongs to whoever created it" — was still being applied, because the only
    thing it asked was whether a delivery had named a DIFFERENT customer
    record. None had: the delivery that could have named one named nobody. So
    a booking made after the mailing excluded them, and the same booking with
    an old date, or with no date at all, left them `included`.

    Three shapes, both branches and both delivery orders, and the answer is
    the same in all of them: nothing about this booking is established, and
    nothing else proves anything about this person.
    """
    monkeypatch.setattr(settings, "easyweek_location_map", location_map(second_branch=True), raising=False)
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(
        session_maker,
        slots=[{"state": "delivered", "basis": RECIPIENT_BASIS_MANUAL, "numeric_id": None}],
    )
    who = person(1)
    row = await _a_booking_with_a_nameless_cancellation(
        session_maker,
        index=160,
        shape=shape,
        cancellation_branch=COMPANY_ID if branch == "same" else SECOND_COMPANY_ID,
        cancellation_location=KARLSRUHE_LOCATION_UUID if branch == "same" else SECOND_LOCATION_UUID,
        cancellation_first=cancellation_first,
        holder_uuid=who["customer_uuid"],
    )

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: [row]}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
    assert reason_for(outcome, 1) == (BOOKING_CAPTURE_UNPROVEN if shape == "undated" else BOOKING_CREATION_UNPROVEN)


@pytest.mark.parametrize("branch", ["same", "another"])
@pytest.mark.parametrize("shape", ["new_dated", "old_dated", "undated"])
async def test_what_an_unproven_custody_outside_their_list_changes(
    session_maker, binding_key, no_network, monkeypatch: pytest.MonkeyPatch, shape: str, branch: str
) -> None:
    """And the same shape with an empty history, where it does and does not.

    The booking is not in their list at all, which is the one thing a list
    cannot show about a booking that left it — so a booking made AFTER the
    mailing whose custody nobody can account for leaves the row unknown,
    whichever branch the nameless cancellation came from. That branch used to
    matter and does not: the branch that created a booking holds every other
    customer of that branch too.

    And the limit of it, in the same test: a booking made before the mailing,
    or one this deployment had provably already heard of by then, is not a
    booking made since, whoever holds it now. §46.5's creation rule has
    nothing to ask about it and the recipient stays provable.
    """
    monkeypatch.setattr(settings, "easyweek_location_map", location_map(second_branch=True), raising=False)
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(
        session_maker,
        slots=[{"state": "delivered", "basis": RECIPIENT_BASIS_MANUAL, "numeric_id": None}],
    )
    who = person(1)
    await _a_booking_with_a_nameless_cancellation(
        session_maker,
        index=161,
        shape=shape,
        cancellation_branch=COMPANY_ID if branch == "same" else SECOND_COMPANY_ID,
        cancellation_location=KARLSRUHE_LOCATION_UUID if branch == "same" else SECOND_LOCATION_UUID,
        cancellation_first=False,
    )

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: []}),
    )
    if shape == "new_dated":
        assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
        assert reason_for(outcome, 1) == BOOKING_CAPTURE_UNPROVEN
    else:
        # Made twenty days before the mailing, or first heard of a fortnight
        # before it. Either way not a booking made since.
        assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED


@pytest.mark.parametrize("shape", ["new_dated", "old_dated"])
async def test_their_own_proven_creation_still_decides_through_a_nameless_delivery(
    session_maker, binding_key, no_network, shape: str
) -> None:
    """The control: a creator that IS proven still answers for the booking.

    The creation names the number this recipient's own card proves, in the
    branch that card is in, so whose booking it is was never in doubt — only
    who holds it now, which §46.5's creation rule does not ask about. Made
    after the mailing it excludes them; made before it, it leaves them
    provable.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    row = await _a_booking_with_a_nameless_cancellation(
        session_maker,
        index=162,
        shape=shape,
        cancellation_branch=COMPANY_ID,
        cancellation_location=KARLSRUHE_LOCATION_UUID,
        cancellation_first=False,
        holder_uuid=who["customer_uuid"],
        maker_numeric_id=who["numeric_id"],
    )

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: [row]}),
    )
    if shape == "new_dated":
        assert decision_for(outcome, 1) == FOLLOWUP_ROW_EXCLUDED
        assert reason_for(outcome, 1) == NEW_BOOKING_AFTER_ATTEMPT
    else:
        assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED


async def test_a_proven_foreign_creator_still_answers_through_a_nameless_delivery(
    session_maker, binding_key, no_network, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The other control: proven to be somebody else's, and still is.

    The creation's number resolves, through a card of its own branch, to a
    different proven uuid. The nameless cancellation leaves that person's
    booking in doubt, not this recipient's.
    """
    monkeypatch.setattr(settings, "easyweek_location_map", location_map(second_branch=True), raising=False)
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
        phone="+4915100888101",
    )
    await _a_booking_with_a_nameless_cancellation(
        session_maker,
        index=163,
        shape="new_dated",
        cancellation_branch=SECOND_COMPANY_ID,
        cancellation_location=SECOND_LOCATION_UUID,
        cancellation_first=False,
    )

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: []}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED


async def _a_stranger_and_a_nameless_second_creation(
    session_maker,
    *,
    index: int,
    stranger_numeric_id: int,
    second_numeric_id: int | None = None,
) -> dict[str, Any]:
    """One booking, a creation proven to be a stranger's, and a second creation.

    The second delivery names the OTHER configured branch and the same instant.
    Where it names no customer, nothing says whose creation that one was — and
    the two deliveries cannot both be right about where the booking was made.
    """
    contested = booking(index=index, customer_uuid=person(13)["customer_uuid"], start=NOW + timedelta(days=4))
    made = ATTEMPTED_AT + timedelta(days=2)
    await seed_booking_created(
        session_maker,
        booking_uuid=contested["uuid"],
        numeric_customer_id=stranger_numeric_id,
        created_at=made,
    )
    if second_numeric_id is None:
        await seed_booking_created_without_a_customer(
            session_maker,
            booking_uuid=contested["uuid"],
            received_at=ATTEMPTED_AT + timedelta(days=3),
            created_at=made,
            company_id=SECOND_COMPANY_ID,
            location_uuid=SECOND_LOCATION_UUID,
        )
    else:
        await seed_booking_created(
            session_maker,
            booking_uuid=contested["uuid"],
            numeric_customer_id=second_numeric_id,
            created_at=made,
            received_at=ATTEMPTED_AT + timedelta(days=3),
            company_id=SECOND_COMPANY_ID,
            location_uuid=SECOND_LOCATION_UUID,
        )
    return contested


@pytest.mark.parametrize("bridged", [False, True])
async def test_a_contradicting_creation_that_named_nobody_keeps_the_owner_open(
    session_maker, binding_key, no_network, monkeypatch: pytest.MonkeyPatch, bridged: bool
) -> None:
    """Review round 9, finding 2: a known owner answers for its own delivery.

    One booking uuid, two creation deliveries, the same instant and two
    different configured branches. The first names a customer whose card
    proves a stranger's uuid; the second names no customer at all.

    The proven stranger was allowed to answer for both: the owner question
    came out "demonstrably somebody else's", the contradiction about where the
    booking was made was set aside with it, and recipients with completely
    empty histories came out `included` — a bridged one and a manual one
    alike. Whose the second creation was is simply not established, and
    nothing else says anything about these recipients.
    """
    monkeypatch.setattr(settings, "easyweek_location_map", location_map(second_branch=True), raising=False)
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(
        session_maker,
        slots=[{"state": "delivered"}]
        if bridged
        else [{"state": "delivered", "basis": RECIPIENT_BASIS_MANUAL, "numeric_id": None}],
    )
    who = person(1)
    maker = person(8)
    await _proven_other_card(
        session_maker,
        numeric_id=maker["numeric_id"],
        customer_uuid=maker["customer_uuid"],
        phone="+4915100888102",
    )
    await _a_stranger_and_a_nameless_second_creation(
        session_maker,
        index=164,
        stranger_numeric_id=maker["numeric_id"],
    )

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: []}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
    assert reason_for(outcome, 1) == BOOKING_CAPTURE_UNPROVEN


@pytest.mark.parametrize("second", ["a_number_with_no_card", "the_recipient", "the_same_stranger", "another_stranger"])
async def test_what_the_second_creation_names_decides_the_batchs_answer(
    session_maker, binding_key, no_network, monkeypatch: pytest.MonkeyPatch, second: str
) -> None:
    """And the controls either side of it, by what the second delivery proves.

    The same two-branch contradiction with the second creation naming a
    customer: one nobody has a card for, the recipient themselves, the same
    stranger again, and a second stranger. Only the last two establish that
    this booking was somebody else's however the branches disagree; the first
    two leave it open, and the recipient with the empty history stays unknown.
    """
    monkeypatch.setattr(settings, "easyweek_location_map", location_map(second_branch=True), raising=False)
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(
        session_maker,
        slots=[{"state": "delivered", "basis": RECIPIENT_BASIS_MANUAL, "numeric_id": None}],
    )
    who = person(1)
    maker, other = person(8), person(11)
    await _proven_other_card(
        session_maker,
        numeric_id=maker["numeric_id"],
        customer_uuid=maker["customer_uuid"],
        phone="+4915100888103",
    )
    second_numeric = {
        "a_number_with_no_card": person(10)["numeric_id"],
        "the_recipient": person(10)["numeric_id"] + 1,
        "the_same_stranger": person(10)["numeric_id"] + 2,
        "another_stranger": person(10)["numeric_id"] + 3,
    }[second]
    if second != "a_number_with_no_card":
        await _proven_other_card(
            session_maker,
            numeric_id=second_numeric,
            customer_uuid={
                "the_recipient": who["customer_uuid"],
                "the_same_stranger": maker["customer_uuid"],
                "another_stranger": other["customer_uuid"],
            }[second],
            phone="+4915100888104",
            company_id=SECOND_COMPANY_ID,
        )
    await _a_stranger_and_a_nameless_second_creation(
        session_maker,
        index=165,
        stranger_numeric_id=maker["numeric_id"],
        second_numeric_id=second_numeric,
    )

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: []}),
    )
    if second in {"the_same_stranger", "another_stranger"}:
        assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED
    else:
        assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
        assert reason_for(outcome, 1) == BOOKING_CAPTURE_UNPROVEN


async def _an_undated_creation_and_a_late_claim(
    session_maker,
    *,
    index: int,
    claimed: datetime,
    holder_uuid: str | None = None,
) -> dict[str, Any]:
    """An owned creation with no instant, and a later one claiming an instant.

    The first delivery was stored before the mailing, which proves the booking
    existed by then. The second names no customer and states a single instant.
    """
    row = booking(
        index=index,
        customer_uuid=holder_uuid or person(9)["customer_uuid"],
        start=ATTEMPTED_AT - timedelta(days=1),
        canceled=True,
        completed=False,
    )
    await seed_booking_created_without_a_date(
        session_maker,
        booking_uuid=row["uuid"],
        numeric_customer_id=person(8)["numeric_id"],
        received_at=ATTEMPTED_AT - timedelta(days=12),
    )
    await seed_booking_created_without_a_customer(
        session_maker,
        booking_uuid=row["uuid"],
        received_at=NOW - timedelta(days=1),
        created_at=claimed,
    )
    return row


@pytest.mark.parametrize("visible", [False, True])
@pytest.mark.parametrize("claimed", ["after_the_attempt", "after_the_preview"])
async def test_an_impossible_instant_withdraws_the_early_sighting(
    session_maker, binding_key, no_network, claimed: str, visible: bool
) -> None:
    """Review round 9, finding 3: a disputed booking has a chronology too.

    A creation delivery naming an owner and no instant was stored before the
    mailing, which proves this booking existed by then. A later creation of
    the same booking names no customer and claims it was made after the
    mailing — or after the moment this preview is being computed at. Both
    claims cannot hold, and the early sighting was accepted as settling the
    question anyway, so the row came out `included`.

    The same contradiction on a booking with an OWNER on the second delivery
    already read as unknown, because such a booking is attributed and its
    chronology was checked. A disputed one was not. Checked inside their
    current history and outside it; no arrival time is read as a creation
    date.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(
        session_maker,
        slots=[{"state": "delivered", "basis": RECIPIENT_BASIS_MANUAL, "numeric_id": None}],
    )
    who = person(1)
    row = await _an_undated_creation_and_a_late_claim(
        session_maker,
        index=166,
        claimed=ATTEMPTED_AT + timedelta(days=3) if claimed == "after_the_attempt" else NOW + timedelta(days=3),
        holder_uuid=who["customer_uuid"] if visible else None,
    )

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: [row] if visible else []}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
    assert reason_for(outcome, 1) == BOOKING_CAPTURE_UNPROVEN


@pytest.mark.parametrize("shape", ["a_consistent_old_instant", "no_instant_at_all"])
async def test_an_early_sighting_still_settles_a_record_that_agrees(
    session_maker, binding_key, no_network, shape: str
) -> None:
    """The control: nothing is contradicted, so the sighting still answers.

    A booking outside this recipient's list, heard of weeks before the
    mailing, whose second creation delivery either states a truthful older
    instant — an ordinary late delivery — or states none at all. Neither claim
    fights the capture's own earlier row, so whoever made it, it is not a
    booking made since the mailing, and the recipient is provable. No creation
    date is invented to say so.

    Deliberately outside their list, because that is where this proof is
    consulted: a booking IN somebody's list still needs its creation
    attributed to them, and a record one of whose creations named nobody
    cannot clear that bar — which the paragraph above, and
    :func:`test_a_disputed_bookings_claimed_instants_are_checked_against_its_own_sighting`,
    are about.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(
        session_maker,
        slots=[{"state": "delivered", "basis": RECIPIENT_BASIS_MANUAL, "numeric_id": None}],
    )
    who = person(1)
    row = booking(
        index=167,
        customer_uuid=person(9)["customer_uuid"],
        start=ATTEMPTED_AT - timedelta(days=1),
        canceled=True,
        completed=False,
    )
    await seed_booking_created_without_a_date(
        session_maker,
        booking_uuid=row["uuid"],
        numeric_customer_id=person(8)["numeric_id"],
        received_at=ATTEMPTED_AT - timedelta(days=12),
    )
    if shape == "a_consistent_old_instant":
        await seed_booking_created_without_a_customer(
            session_maker,
            booking_uuid=row["uuid"],
            received_at=NOW - timedelta(days=1),
            created_at=ATTEMPTED_AT - timedelta(days=30),
        )
    else:
        await seed_booking_created_without_a_customer(
            session_maker,
            booking_uuid=row["uuid"],
            received_at=NOW - timedelta(days=1),
        )

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: []}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED


@pytest.mark.parametrize("ground", ["their_own_new_booking", "a_booking_ahead"])
async def test_an_independent_ground_still_excludes_beside_an_impossible_instant(
    session_maker, binding_key, no_network, ground: str
) -> None:
    """And a proven fact about them still excludes beside the contradiction.

    The contradicted booking sits in their history next to either a booking
    they provably made after the mailing or an appointment still ahead. Each
    excludes for its own reason, whatever the other record cannot settle.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    row = await _an_undated_creation_and_a_late_claim(
        session_maker,
        index=168,
        claimed=ATTEMPTED_AT + timedelta(days=3),
        holder_uuid=who["customer_uuid"],
    )
    rows = [row]
    if ground == "their_own_new_booking":
        theirs = booking(index=169, customer_uuid=who["customer_uuid"], start=ATTEMPTED_AT - timedelta(days=2))
        await seed_booking_created(
            session_maker,
            booking_uuid=theirs["uuid"],
            numeric_customer_id=who["numeric_id"],
            created_at=ATTEMPTED_AT + timedelta(days=1),
        )
        rows.append(theirs)
        expected = NEW_BOOKING_AFTER_ATTEMPT
    else:
        rows.append(booking(index=170, customer_uuid=who["customer_uuid"], start=NOW + timedelta(days=4)))
        expected = FUTURE_BOOKING_ACTIVE

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: rows}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_EXCLUDED
    assert reason_for(outcome, 1) == expected


def test_a_disputed_bookings_claimed_instants_are_checked_against_its_own_sighting() -> None:
    """Review round 9, finding 3, at the rule itself.

    The end-to-end reproductions above reach this rule through a dispute whose
    second delivery also leaves its owner open, so the rule is pinned here on
    its own: what makes the early sighting unusable is the claimed instant,
    not the missing owner.
    """
    from altegio_bot.campaigns.easyweek_voucher_followup import booking_evidence
    from altegio_bot.campaigns.easyweek_voucher_production.booking_attribution import (
        DISPUTE_CREATION_UNDATED,
        DisputedBooking,
    )

    anchor = ATTEMPTED_AT
    seen = ATTEMPTED_AT - timedelta(days=12)

    def dispute(*claimed: datetime) -> DisputedBooking:
        return DisputedBooking(
            booking_uuid=uuid_module.UUID("00000171-7777-4777-8777-777777777777"),
            reason=DISPUTE_CREATION_UNDATED,
            customer_ids=frozenset({person(8)["numeric_id"]}),
            creation_customer_ids=frozenset({person(8)["numeric_id"]}),
            created_at=tuple(claimed),
            creation_company_ids=frozenset({COMPANY_ID}),
            creation_pairs=frozenset({(COMPANY_ID, person(8)["numeric_id"])}),
            customer_pairs=frozenset({(COMPANY_ID, person(8)["numeric_id"])}),
            first_observed_at=seen,
        )

    # Nothing claimed, or a claim the sighting agrees with: the sighting answers.
    assert booking_evidence._dispute_predates(dispute(), anchor=anchor, now=NOW) is True
    assert booking_evidence._dispute_predates(dispute(seen - timedelta(days=5)), anchor=anchor, now=NOW) is True
    # A claim later than the booking was first heard of, and one later than the
    # preview itself: the record contradicts itself and proves nothing.
    assert booking_evidence._dispute_predates(dispute(anchor + timedelta(days=3)), anchor=anchor, now=NOW) is False
    assert booking_evidence._dispute_predates(dispute(NOW + timedelta(days=3)), anchor=anchor, now=NOW) is False


# ---------------------------------------------------------------------------
# A repeat that agrees is not a second claim (round 10)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("repeat", ["none", "undated", "an_old_instant"])
async def test_a_consistent_partial_repeat_keeps_their_own_booking_provable(
    session_maker, binding_key, no_network, repeat: str
) -> None:
    """Review round 10: an ownerless repeat of a creation is not a new claim.

    Their own cancelled booking sits in their complete current history. Its
    creation delivery names the number their own card proves, in the branch
    that card is in, and states no instant — and it was stored weeks before
    the mailing, which proves the booking existed by then. That is the shape
    §46.5 settles: whose it is was never in doubt, and nothing new is hiding
    there.

    A second creation delivery of the same booking, from the same branch,
    naming no customer, used to withdraw all of that. The refusal did not ask
    whether the delivery disagreed with anything: it leaves the owner
    unstated, and the owner of a creation in that branch was already stated by
    a delivery that could not be about a different booking or a different
    branch. Whether the repeat carries a consistent older instant or none at
    all, the answer is the one the record without it gives.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    theirs = booking(
        index=180,
        customer_uuid=who["customer_uuid"],
        start=ATTEMPTED_AT - timedelta(days=2),
        canceled=True,
        completed=False,
    )
    await seed_booking_created_without_a_date(
        session_maker,
        booking_uuid=theirs["uuid"],
        numeric_customer_id=who["numeric_id"],
        received_at=ATTEMPTED_AT - timedelta(days=16),
    )
    if repeat != "none":
        await seed_booking_created_without_a_customer(
            session_maker,
            booking_uuid=theirs["uuid"],
            received_at=NOW - timedelta(days=1),
            created_at=ATTEMPTED_AT - timedelta(days=25) if repeat == "an_old_instant" else None,
        )

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: [theirs]}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED


@pytest.mark.parametrize("disagreement", ["after_the_sighting", "after_the_preview", "a_second_instant"])
async def test_a_partial_repeat_that_disagrees_still_refuses(
    session_maker, binding_key, no_network, disagreement: str
) -> None:
    """And the control that keeps the exemption honest: it must AGREE.

    The same shape with a repeat that fights the record instead of repeating
    it — an instant later than the booking was first heard of, one later than
    the preview itself, and two ownerless deliveries claiming two different
    instants. None of them is a repeat of anything, and the row stays unknown.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    theirs = booking(
        index=181,
        customer_uuid=who["customer_uuid"],
        start=ATTEMPTED_AT - timedelta(days=2),
        canceled=True,
        completed=False,
    )
    await seed_booking_created_without_a_date(
        session_maker,
        booking_uuid=theirs["uuid"],
        numeric_customer_id=who["numeric_id"],
        received_at=ATTEMPTED_AT - timedelta(days=16),
    )
    claims = {
        "after_the_sighting": [ATTEMPTED_AT + timedelta(days=3)],
        "after_the_preview": [NOW + timedelta(days=3)],
        "a_second_instant": [ATTEMPTED_AT - timedelta(days=25), ATTEMPTED_AT - timedelta(days=30)],
    }[disagreement]
    for offset, claimed in enumerate(claims):
        await seed_booking_created_without_a_customer(
            session_maker,
            booking_uuid=theirs["uuid"],
            received_at=NOW - timedelta(days=2 - offset),
            created_at=claimed,
        )

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: [theirs]}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
    assert reason_for(outcome, 1) == BOOKING_CAPTURE_UNPROVEN


@pytest.mark.parametrize("repeated", [False, True])
async def test_a_consistent_partial_repeat_leaves_a_strangers_booking_theirs(
    session_maker, binding_key, no_network, repeated: bool
) -> None:
    """Review round 10: and it does not reopen somebody else's booking either.

    An undated creation made after the mailing, positively a stranger's — the
    number it names resolves through a card of its own branch to a different
    proven uuid — and absent from both recipients' histories. That is settled:
    the gap is that person's and nobody else's.

    An ownerless repeat of it, from the same branch, used to hold the whole
    batch open: a manual recipient and a bridged one, neither of whom the
    capture says anything about. Two independent recipients are asserted
    before and after the repeat exists.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(
        session_maker,
        slots=[
            {"state": "delivered", "basis": RECIPIENT_BASIS_MANUAL, "numeric_id": None},
            {"state": "delivered", "index": 2},
        ],
    )
    maker = person(8)
    await _proven_other_card(
        session_maker,
        numeric_id=maker["numeric_id"],
        customer_uuid=maker["customer_uuid"],
        phone="+4915100999101",
    )
    target = booking(index=182, customer_uuid=person(13)["customer_uuid"], start=NOW + timedelta(days=7))
    await seed_booking_created_without_a_date(
        session_maker,
        booking_uuid=target["uuid"],
        numeric_customer_id=maker["numeric_id"],
        received_at=ATTEMPTED_AT + timedelta(days=2),
    )
    if repeated:
        await seed_booking_created_without_a_customer(
            session_maker,
            booking_uuid=target["uuid"],
            received_at=ATTEMPTED_AT + timedelta(days=3),
        )

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={person(1)["customer_uuid"]: [], person(2)["customer_uuid"]: []}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED
    assert decision_for(outcome, 2) == FOLLOWUP_ROW_INCLUDED


@pytest.mark.parametrize("shape", ["partial_only", "partial_then_full", "full_then_partial", "two_full_only"])
async def test_what_confirms_the_owner_of_the_other_branchs_creation(
    session_maker, binding_key, no_network, monkeypatch: pytest.MonkeyPatch, shape: str
) -> None:
    """Review round 10: which branch an ownerless creation came from decides.

    A complete creation in one configured branch names a customer whose card
    proves a stranger's uuid. A creation of the same booking from the OTHER
    configured branch names nobody — and that one cannot be a repeat of the
    first: two branches cannot both be where a booking was made, so what it
    leaves unstated is a second, unanswered claim about who created it. The
    row is unknown, and must stay so.

    When a complete delivery from that second branch then confirms the same
    stranger — the same person, through that branch's own card — the branch
    has an owner and the ownerless delivery adds no candidate the capture
    cannot already name. Checked with the ownerless delivery arriving in the
    middle and last, against the control that has only the two complete ones.
    """
    monkeypatch.setattr(settings, "easyweek_location_map", location_map(second_branch=True), raising=False)
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(
        session_maker,
        slots=[{"state": "delivered", "basis": RECIPIENT_BASIS_MANUAL, "numeric_id": None}],
    )
    who = person(1)
    maker = person(8)
    here, there = maker["numeric_id"], maker["numeric_id"] + 4
    for number, company in ((here, COMPANY_ID), (there, SECOND_COMPANY_ID)):
        await _proven_other_card(
            session_maker,
            numeric_id=number,
            customer_uuid=maker["customer_uuid"],
            phone="+4915100999102",
            company_id=company,
        )
    contested = booking(index=183, customer_uuid=person(13)["customer_uuid"], start=NOW + timedelta(days=7))
    made = ATTEMPTED_AT + timedelta(days=2)
    await seed_booking_created(
        session_maker,
        booking_uuid=contested["uuid"],
        numeric_customer_id=here,
        created_at=made,
    )

    async def _partial() -> None:
        await seed_booking_created_without_a_customer(
            session_maker,
            booking_uuid=contested["uuid"],
            received_at=ATTEMPTED_AT + timedelta(days=3),
            created_at=made,
            company_id=SECOND_COMPANY_ID,
            location_uuid=SECOND_LOCATION_UUID,
        )

    async def _full_there() -> None:
        await seed_booking_created(
            session_maker,
            booking_uuid=contested["uuid"],
            numeric_customer_id=there,
            created_at=made,
            received_at=ATTEMPTED_AT + timedelta(days=4),
            company_id=SECOND_COMPANY_ID,
            location_uuid=SECOND_LOCATION_UUID,
        )

    steps = {
        "partial_only": (_partial,),
        "partial_then_full": (_partial, _full_there),
        "full_then_partial": (_full_there, _partial),
        "two_full_only": (_full_there,),
    }[shape]
    for step in steps:
        await step()

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: []}),
    )
    if shape == "partial_only":
        assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
        assert reason_for(outcome, 1) == BOOKING_CAPTURE_UNPROVEN
    else:
        assert decision_for(outcome, 1) == FOLLOWUP_ROW_INCLUDED


async def test_two_owners_in_one_branch_are_not_masked_by_a_consistent_repeat(
    session_maker, binding_key, no_network
) -> None:
    """A repeat agrees with ONE record, and there is no one record here.

    Two complete creation deliveries of this booking, from one branch, naming
    two different customers — a creators' conflict — and an ownerless delivery
    from that same branch beside them. The branch has owners, plural, and the
    repeat cannot pick between them: the row stays unknown and the louder
    problem is not hidden behind the quieter one.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    contested = booking(
        index=184,
        customer_uuid=who["customer_uuid"],
        start=ATTEMPTED_AT - timedelta(days=2),
        canceled=True,
        completed=False,
    )
    made = ATTEMPTED_AT - timedelta(days=20)
    for owner in (who["numeric_id"], person(8)["numeric_id"]):
        await seed_booking_created(
            session_maker,
            booking_uuid=contested["uuid"],
            numeric_customer_id=owner,
            created_at=made,
            received_at=ATTEMPTED_AT - timedelta(days=18),
        )
    await seed_booking_created_without_a_customer(
        session_maker,
        booking_uuid=contested["uuid"],
        received_at=NOW - timedelta(days=1),
        created_at=made,
    )

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: [contested]}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
    assert reason_for(outcome, 1) == BOOKING_CAPTURE_UNPROVEN


@pytest.mark.parametrize("branch", ["same", "another"])
@pytest.mark.parametrize("visible", [False, True])
async def test_a_nameless_lifecycle_delivery_still_counts_beside_a_consistent_repeat(
    session_maker, binding_key, no_network, monkeypatch: pytest.MonkeyPatch, visible: bool, branch: str
) -> None:
    """And the doubt about who HOLDS it is untouched by this exemption.

    The creation question is answered — an undated creation names a customer
    and an ownerless delivery from the same branch repeats it — and then a
    cancellation names nobody at all. Who created the booking and who holds it
    now are two questions, and the second is still open: inside their current
    history and outside it, from the creation's own branch and from the other.
    """
    monkeypatch.setattr(settings, "easyweek_location_map", location_map(second_branch=True), raising=False)
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(
        session_maker,
        slots=[{"state": "delivered", "basis": RECIPIENT_BASIS_MANUAL, "numeric_id": None}],
    )
    who = person(1)
    row = booking(
        index=185,
        customer_uuid=who["customer_uuid"] if visible else person(13)["customer_uuid"],
        start=ATTEMPTED_AT - timedelta(days=2),
        canceled=True,
        completed=False,
    )
    await seed_booking_created(
        session_maker,
        booking_uuid=row["uuid"],
        numeric_customer_id=person(8)["numeric_id"],
        created_at=ATTEMPTED_AT + timedelta(days=2),
    )
    await seed_booking_created_without_a_customer(
        session_maker,
        booking_uuid=row["uuid"],
        received_at=ATTEMPTED_AT + timedelta(days=3),
        created_at=ATTEMPTED_AT + timedelta(days=2),
    )
    await seed_booking_lifecycle(
        session_maker,
        booking_uuid=row["uuid"],
        received_at=ATTEMPTED_AT + timedelta(days=4),
        company_id=COMPANY_ID if branch == "same" else SECOND_COMPANY_ID,
        location_uuid=KARLSRUHE_LOCATION_UUID if branch == "same" else SECOND_LOCATION_UUID,
    )

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: [row] if visible else []}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_UNKNOWN
    assert reason_for(outcome, 1) == (BOOKING_CREATION_UNPROVEN if visible else BOOKING_CAPTURE_UNPROVEN)


@pytest.mark.parametrize("ground", ["their_own_new_booking", "a_booking_ahead"])
async def test_an_independent_ground_still_excludes_beside_a_consistent_repeat(
    session_maker, binding_key, no_network, ground: str
) -> None:
    """And a proven fact about this person still excludes beside the repeat.

    Their old undated booking with its ownerless repeat is provable and
    excludes nobody. Next to it is either a booking they provably made after
    the mailing or an appointment still ahead, and each excludes on its own
    ground.
    """
    await seed_sender_and_template(session_maker, with_followup_template=True)
    batch_id, _ = await seed_source_batch(session_maker, slots=[{"state": "delivered"}])
    who = person(1)
    old = booking(
        index=186,
        customer_uuid=who["customer_uuid"],
        start=ATTEMPTED_AT - timedelta(days=2),
        canceled=True,
        completed=False,
    )
    await seed_booking_created_without_a_date(
        session_maker,
        booking_uuid=old["uuid"],
        numeric_customer_id=who["numeric_id"],
        received_at=ATTEMPTED_AT - timedelta(days=16),
    )
    await seed_booking_created_without_a_customer(
        session_maker,
        booking_uuid=old["uuid"],
        received_at=NOW - timedelta(days=1),
    )
    rows = [old]
    if ground == "their_own_new_booking":
        fresh = booking(index=187, customer_uuid=who["customer_uuid"], start=ATTEMPTED_AT - timedelta(days=3))
        await seed_booking_created(
            session_maker,
            booking_uuid=fresh["uuid"],
            numeric_customer_id=who["numeric_id"],
            created_at=ATTEMPTED_AT + timedelta(days=1),
        )
        rows.append(fresh)
        expected = NEW_BOOKING_AFTER_ATTEMPT
    else:
        rows.append(booking(index=188, customer_uuid=who["customer_uuid"], start=NOW + timedelta(days=5)))
        expected = FUTURE_BOOKING_ACTIVE

    outcome = await build(
        session_maker,
        batch_id=batch_id,
        reader=FakeHistoryReader(bookings={who["customer_uuid"]: rows}),
    )
    assert decision_for(outcome, 1) == FOLLOWUP_ROW_EXCLUDED
    assert reason_for(outcome, 1) == expected
