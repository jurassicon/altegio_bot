"""Synthetic seeds and transports for the §46.7–46.8 follow-up SEND tests.

Every identity here is obviously fabricated. No production phone number,
customer uuid, order, message id, voucher code, template or sender belongs in
this repository, and the send path never reads one anyway: what it reads is the
ledger's own evidence and the stored preview, which these helpers build.

What is real in the suites that use this
----------------------------------------
The tables, their CHECK constraints and their partial unique indexes; the
approval's signature and TTL; the claim transaction and its consent address
locks; the run's lease; the terminal stop and its tombstones; the executor loop;
the callback routing. Those are the subject.

What is faked is the outside world — EasyWeek's read-only API and Meta's message
endpoint — and nothing else. :class:`CountingSender` is the important one: it
counts every POST, so "at most one external attempt per recipient" and "no
background transport retries" are assertions about a number rather than about a
reading of the code.
"""

from __future__ import annotations

import uuid as uuid_module
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.campaigns.easyweek_voucher_delivery.delivery import (
    DELIVERY_ACCEPTED,
    DELIVERY_REJECTED,
    DELIVERY_UNKNOWN,
    DeliveryOutcome,
)
from altegio_bot.campaigns.easyweek_voucher_followup import preview as preview_module
from altegio_bot.campaigns.easyweek_voucher_followup import send_dispatch as dispatch_module
from altegio_bot.campaigns.easyweek_voucher_followup import send_ledger as ledger_module
from altegio_bot.campaigns.easyweek_voucher_production.operations import OpsPrincipal
from altegio_bot.models.models import (
    FOLLOWUP_GROUP_DELIVERED,
    EasyWeekVoucherFollowupSend,
)
from altegio_bot.settings import settings
from altegio_bot.tests.easyweek_voucher_followup_fixtures import (
    NOW,
    TemplateReader,
    seed_sender_and_template,
    seed_source_batch,
)

FOURTEEN_DAYS = 14 * 24 * 60 * 60

# The operator these suites act as. A synthetic account and a synthetic session
# fingerprint; neither is a credential and nothing accepts them as one.
OPERATOR = "ops-operator"
OTHER_OPERATOR = "ops-second-operator"


def principal(account: str = OPERATOR, *, fingerprint: str = "a" * 64) -> OpsPrincipal:
    return OpsPrincipal(account=account, session_fingerprint=fingerprint)


@pytest.fixture
def send_fence(monkeypatch: pytest.MonkeyPatch) -> None:
    """The follow-up's OWN send fence, open, and an executor said to be running.

    Deliberately two separate settings, and deliberately NOT the production
    mailing's: a test that opened ``..._MAILING_ENABLED`` and expected a
    follow-up to go out would be testing the bypass §46.8 forbids. The
    production fence is pinned CLOSED here so every suite proves the two are
    independent.
    """
    monkeypatch.setattr(settings, "easyweek_voucher_followup_send_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_voucher_followup_executor_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_voucher_production_mailing_enabled", False, raising=False)


@pytest.fixture
def primary_fence_only(monkeypatch: pytest.MonkeyPatch) -> None:
    """The PRIMARY mailing permitted and the follow-up's own fence closed.

    The exact configuration §46.8 says must still refuse every follow-up action.
    """
    monkeypatch.setattr(settings, "easyweek_voucher_production_mailing_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_voucher_production_executor_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_voucher_followup_send_enabled", False, raising=False)
    monkeypatch.setattr(settings, "easyweek_voucher_followup_executor_enabled", False, raising=False)


@dataclass
class CountingSender:
    """Meta's message endpoint, answered from a fixture and COUNTED.

    One POST per call and no retry anywhere — the real client has none either,
    which is what the counting proves rather than assumes. ``answers`` lets a
    test script a sequence of outcomes; the default is an acceptance with a
    synthetic message id per call.

    It also records the PARAMETERS it was given, so a test can prove the
    approved BODY was built in the approved order — and that neither a voucher
    code nor anything else unexpected was in it.
    """

    answers: list[DeliveryOutcome] = field(default_factory=list)
    calls: list[dict[str, Any]] = field(default_factory=list)
    raise_on_call: Exception | None = None

    async def send_voucher_template(
        self,
        *,
        phone_number_id: str,
        to_e164: str,
        template_name: str,
        language: str,
        params: list[str],
    ) -> DeliveryOutcome:
        self.calls.append(
            {
                "phone_number_id": phone_number_id,
                "to_e164": to_e164,
                "template_name": template_name,
                "language": language,
                "params": list(params),
            }
        )
        if self.raise_on_call is not None:
            raise self.raise_on_call
        if self.answers:
            return self.answers.pop(0)
        return DeliveryOutcome(
            outcome=DELIVERY_ACCEPTED,
            provider_message_id=f"wamid.followup.{len(self.calls)}",
            http_status=200,
        )

    @property
    def count(self) -> int:
        return len(self.calls)


def accepted(message_id: str) -> DeliveryOutcome:
    return DeliveryOutcome(outcome=DELIVERY_ACCEPTED, provider_message_id=message_id, http_status=200)


def unknown(reason: str = "meta_timeout") -> DeliveryOutcome:
    return DeliveryOutcome(outcome=DELIVERY_UNKNOWN, reason=reason)


def rejected() -> DeliveryOutcome:
    return DeliveryOutcome(outcome=DELIVERY_REJECTED, reason="meta_rejected_request", http_status=400)


class StubTransports(dispatch_module.Transports):
    """The two transports a run uses, supplied by the test.

    Subclasses the class the deployed code constructs, so only what it yields is
    replaced and none of the authorisation path changes with it.
    """

    def __init__(self, *, reader: Any = None, sender: Any = None, templates: Any = None) -> None:
        self.reader_obj = reader
        self.sender_obj = sender
        # The per-attempt Meta template proof runs for real; only its transport
        # is replaced. Defaulting to an APPROVED listing keeps every suite that
        # is not about the template itself describing a healthy deployment.
        self.templates_obj = templates if templates is not None else TemplateReader()

    def template_reader(self) -> Any:
        return self.templates_obj

    def reader(self) -> Any:  # noqa: D102 - the base class documents it
        import contextlib

        @contextlib.asynccontextmanager
        async def _reader() -> Any:
            yield self.reader_obj

        return _reader()

    def sender(self) -> Any:  # noqa: D102 - the base class documents it
        import contextlib

        @contextlib.asynccontextmanager
        async def _sender() -> Any:
            if self.sender_obj is None:
                raise AssertionError("this test did not expect a Meta request to be made")
            yield self.sender_obj

        return _sender()


async def seed_ready_batch(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    slots: list[dict[str, Any]] | None = None,
    phone_number_id: str | None = None,
) -> int:
    """One finished €10 production batch whose slots are ready for a follow-up.

    The default is the ordinary good case: two recipients whose first message was
    delivered and never read, from the branch's own sender, over a proven route.
    """
    from altegio_bot.models.models import PROVIDER_EASYWEEK, WhatsAppSender
    from altegio_bot.tests.easyweek_voucher_followup_fixtures import PHONE_NUMBER_ID

    # Idempotent, because a test that seeds a SECOND original mailing for the
    # same branch needs the one sender and the one template row, not a duplicate
    # of each — the deployment has exactly one of both.
    async with session_maker() as session:
        existing = (
            await session.execute(
                select(WhatsAppSender.id)
                .where(WhatsAppSender.provider == PROVIDER_EASYWEEK)
                .where(WhatsAppSender.sender_code == "default")
                .limit(1)
            )
        ).first()
    if existing is None:
        await seed_sender_and_template(
            session_maker,
            with_followup_template=True,
            phone_number_id=phone_number_id or PHONE_NUMBER_ID,
        )
    batch_id, _recipients = await seed_source_batch(
        session_maker,
        slots=slots if slots is not None else [{"state": "delivered"}, {"state": "delivered"}],
    )
    return batch_id


async def build_preview(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    batch_id: int,
    reader: Any,
    groups: tuple[str, ...] = (FOLLOWUP_GROUP_DELIVERED,),
    interval: int = FOURTEEN_DAYS,
    now: datetime = NOW,
) -> preview_module.PreviewOutcome:
    """One stored preview version, through the real PR-22.2 path.

    Deliberately the real one: the send's offer reads a stored snapshot, and a
    hand-written row would let the two drift apart.
    """
    outcome = await preview_module.create_preview(
        session_maker,
        spec=preview_module.PreviewSpec(
            batch_id=int(batch_id),
            groups=groups,
            min_interval_seconds=int(interval),
            operator_principal=f"{OPERATOR}:fp",
        ),
        booking_reader=reader,
        template_reader=TemplateReader(),
        now=now,
    )
    assert outcome.created, outcome.reason
    return outcome


async def offer(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    batch_id: int,
    actor: OpsPrincipal | None = None,
    now: datetime = NOW,
) -> ledger_module.PlanOffer:
    return await dispatch_module.offer_send(
        session_maker,
        principal=actor or principal(),
        batch_id=int(batch_id),
        template_reader=TemplateReader(),
        now=now,
    )


async def confirm(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    plan: ledger_module.PlanOffer,
    actor: OpsPrincipal | None = None,
    count: int | None = None,
    signature: str | None = None,
    now: datetime = NOW,
) -> ledger_module.ConfirmOutcome:
    assert plan.approval is not None
    return await dispatch_module.confirm_send(
        session_maker,
        approval_id=plan.approval.id,
        presented_signature=signature if signature is not None else plan.approval.plan_signature,
        principal=actor or principal(),
        confirmed_count=count if count is not None else plan.approval.approved_recipient_count,
        template_reader=TemplateReader(),
        now=now,
    )


async def rows_of(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    run_id: int,
) -> dict[int, EasyWeekVoucherFollowupSend]:
    """``{slot: row}`` for one run, as the ORM sees it. For assertions only."""
    async with session_maker() as session:
        rows = (
            await session.execute(
                select(EasyWeekVoucherFollowupSend).where(EasyWeekVoucherFollowupSend.run_id == int(run_id))
            )
        ).scalars()
        return {int(row.source_slot): row for row in rows}


async def all_sends(session_maker: async_sessionmaker[AsyncSession]) -> list[EasyWeekVoucherFollowupSend]:
    async with session_maker() as session:
        return list(
            (await session.execute(select(EasyWeekVoucherFollowupSend).order_by(EasyWeekVoucherFollowupSend.id)))
            .scalars()
            .all()
        )


def status_payload(
    *,
    provider_message_id: str,
    kind: str,
    phone_number_id: str | None = None,
    recipient_phone: str | None = None,
    event_at: datetime | None = None,
    error_code: object = None,
) -> dict[str, Any]:
    """One Meta status callback in the shape the inbound worker hands on.

    The route is given as the raw values Meta sends — a sender id and a
    recipient number — because that is exactly what the callback carries and
    what the stored digests have to be compared against.
    """
    from altegio_bot.tests.easyweek_voucher_followup_fixtures import PHONE_NUMBER_ID

    payload: dict[str, Any] = {
        "provider_message_id": provider_message_id,
        "status": kind,
        "phone_number_id": phone_number_id if phone_number_id is not None else PHONE_NUMBER_ID,
        "recipient_id": recipient_phone,
        "event_at": event_at or (NOW + timedelta(minutes=1)),
    }
    if error_code is not None:
        payload["error_code"] = error_code
    return payload


def person_phone(index: int) -> str:
    from altegio_bot.tests.easyweek_voucher_followup_fixtures import person

    return str(person(index)["phone"])


def person_uuid(index: int) -> uuid_module.UUID:
    from altegio_bot.tests.easyweek_voucher_followup_fixtures import person

    return uuid_module.UUID(str(person(index)["customer_uuid"]))


__all__ = [
    "FOURTEEN_DAYS",
    "OPERATOR",
    "OTHER_OPERATOR",
    "CountingSender",
    "StubTransports",
    "accepted",
    "all_sends",
    "build_preview",
    "confirm",
    "offer",
    "person_phone",
    "person_uuid",
    "primary_fence_only",
    "principal",
    "rejected",
    "rows_of",
    "seed_ready_batch",
    "send_fence",
    "status_payload",
    "unknown",
]
