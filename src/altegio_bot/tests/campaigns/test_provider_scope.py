"""PR-13 provider identity and fail-closed campaign boundaries."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

import altegio_bot.campaigns.configuration as configuration
import altegio_bot.campaigns.followup as followup
import altegio_bot.campaigns.runner as runner
import altegio_bot.workers.campaign_worker as campaign_worker
import altegio_bot.workers.outbox_worker as outbox_worker
from altegio_bot.campaigns.configuration import resolve_campaign_readiness
from altegio_bot.campaigns.contracts import ClientCandidate, ClientSnapshot
from altegio_bot.campaigns.easyweek_eligibility import REGISTRY_UNAVAILABLE
from altegio_bot.campaigns.easyweek_segment import SEGMENT_SOURCE
from altegio_bot.campaigns.provider import (
    CAMPAIGN_EXECUTION_NOT_AUTHORIZED,
    CAMPAIGN_IDENTITY_MISMATCH,
    CAMPAIGN_PROVIDER_MISMATCH,
    CAMPAIGN_PROVIDER_UNKNOWN,
    EASYWEEK_CAMPAIGN_SEGMENT_NOT_IMPLEMENTED,
    CampaignProviderRefusal,
    campaign_dedupe_key,
    require_campaign_execution_provider,
)
from altegio_bot.campaigns.runner import RunParams, retry_recipient_job
from altegio_bot.easyweek_locations import EasyWeekLocation, EasyWeekLocationRegistry
from altegio_bot.models.models import (
    CampaignRecipient,
    CampaignRun,
    Client,
    MessageJob,
    MessageTemplate,
    OutboxMessage,
    WhatsAppSender,
)

COMPANY_ID = 758285
NOW = datetime(2026, 8, 1, tzinfo=timezone.utc)


async def _persist_delivery_identity(
    session,
    *,
    company_ids: object,
    job_company_id: int = COMPANY_ID,
    recipient_company_id: int | None = None,
    recipient_client_id: int | None = None,
    job_client_id: int | None = None,
    recipient_phone: object = "+4915111111111",
    payload_phone: object = "+4915111111111",
    payload_ids: str = "both",
    job_type: str = "newsletter_new_clients_monthly",
    suffix: str = "identity",
) -> tuple[CampaignRun, CampaignRecipient, MessageJob]:
    run = CampaignRun(
        provider="altegio",
        campaign_code="new_clients_monthly",
        mode="send-real",
        company_ids=company_ids,
        period_start=NOW,
        period_end=datetime(2026, 9, 1, tzinfo=timezone.utc),
        status="running",
    )
    session.add(run)
    await session.flush()
    recipient = CampaignRecipient(
        provider="altegio",
        campaign_run_id=run.id,
        company_id=recipient_company_id if recipient_company_id is not None else job_company_id,
        client_id=recipient_client_id,
        phone_e164=recipient_phone,
        status="queued",
    )
    session.add(recipient)
    await session.flush()
    payload: dict[str, object] = {"kind": job_type}
    if payload_ids in {"both", "run_only"}:
        payload["campaign_run_id"] = run.id
    if payload_ids in {"both", "recipient_only"}:
        payload["campaign_recipient_id"] = recipient.id
    if payload_phone is not None:
        payload["phone_e164"] = payload_phone
    job = MessageJob(
        provider="altegio",
        company_id=job_company_id,
        client_id=job_client_id,
        job_type=job_type,
        run_at=NOW,
        status="processing",
        attempts=0,
        max_attempts=5,
        dedupe_key=f"altegio:test:{suffix}:{run.id}:{job_company_id}",
        payload=payload,
    )
    session.add(job)
    await session.flush()
    if job_type == runner.FOLLOWUP_JOB_TYPE:
        recipient.followup_message_job_id = job.id
    else:
        recipient.message_job_id = job.id
    return run, recipient, job


async def _persist_campaign_client(
    session,
    *,
    provider: str = "altegio",
    company_id: int = COMPANY_ID,
    external_id: int = 9001,
    phone_e164: str | None = "+4915222222222",
) -> Client:
    client = Client(
        provider=provider,
        company_id=company_id,
        altegio_client_id=external_id,
        phone_e164=phone_e164,
        raw={},
    )
    session.add(client)
    await session.flush()
    return client


def _campaign_delivery_probes(monkeypatch):
    reached_delivery = AsyncMock(return_value=object())
    send_template = AsyncMock(side_effect=AssertionError("Meta must not run"))
    send_text = AsyncMock(side_effect=AssertionError("Meta must not run"))
    monkeypatch.setattr(outbox_worker, "_find_success_outbox", reached_delivery)
    monkeypatch.setattr(outbox_worker, "safe_send_template", send_template)
    monkeypatch.setattr(outbox_worker, "safe_send", send_text)
    return reached_delivery, send_template, send_text


def _params(provider: str, *, mode: str) -> RunParams:
    return RunParams(
        provider=provider,
        company_id=COMPANY_ID,
        location_id=COMPANY_ID,
        period_start=NOW,
        period_end=datetime(2026, 9, 1, tzinfo=timezone.utc),
        mode=mode,  # type: ignore[arg-type]
        card_type_id="card-type",
    )


def test_campaign_dedupe_is_provider_scoped() -> None:
    common = {
        "job_type": "newsletter_new_clients_monthly",
        "company_id": COMPANY_ID,
        "record_id": 1,
        "run_at_iso": NOW.isoformat(),
    }
    altegio = campaign_dedupe_key(provider="altegio", **common)
    easyweek = campaign_dedupe_key(provider="easyweek", **common)

    assert altegio != easyweek
    assert altegio.startswith("altegio:")
    assert easyweek.startswith("easyweek:")


def test_unknown_and_easyweek_execution_providers_fail_closed() -> None:
    with pytest.raises(CampaignProviderRefusal) as unknown:
        require_campaign_execution_provider("other")
    assert unknown.value.reason == CAMPAIGN_PROVIDER_UNKNOWN

    with pytest.raises(CampaignProviderRefusal) as easyweek:
        require_campaign_execution_provider("easyweek")
    assert easyweek.value.reason == EASYWEEK_CAMPAIGN_SEGMENT_NOT_IMPLEMENTED


@pytest.mark.asyncio
async def test_easyweek_preview_with_unavailable_registry_fails_pii_free(
    session_maker,
    monkeypatch,
) -> None:
    discover = AsyncMock(side_effect=AssertionError("Altegio discovery must not run"))
    monkeypatch.setattr(runner, "SessionLocal", session_maker)
    monkeypatch.setattr(runner, "_find_candidates", discover)

    run = await runner.run_preview(_params("easyweek", mode="preview"))

    assert run.provider == "easyweek"
    assert run.status == "failed"
    assert run.meta["last_error"] == REGISTRY_UNAVAILABLE
    discover.assert_not_awaited()
    async with session_maker() as session:
        assert await session.scalar(select(func.count()).select_from(CampaignRecipient)) == 0
        assert await session.scalar(select(func.count()).select_from(MessageJob)) == 0
        assert await session.scalar(select(func.count()).select_from(OutboxMessage)) == 0


@pytest.mark.asyncio
async def test_easyweek_send_real_creates_no_run_recipient_job_or_outbox(
    session_maker,
    monkeypatch,
) -> None:
    monkeypatch.setattr(runner, "SessionLocal", session_maker)

    with pytest.raises(CampaignProviderRefusal) as exc_info:
        await runner.enqueue_send_real(_params("easyweek", mode="send-real"))
    assert exc_info.value.reason == EASYWEEK_CAMPAIGN_SEGMENT_NOT_IMPLEMENTED

    async with session_maker() as session:
        for model in (CampaignRun, CampaignRecipient, MessageJob, OutboxMessage):
            assert await session.scalar(select(func.count()).select_from(model)) == 0


@pytest.mark.asyncio
async def test_altegio_preview_recipient_and_execution_job_get_provider_explicitly(
    session_maker,
    monkeypatch,
) -> None:
    candidate = ClientCandidate(
        client=ClientSnapshot(
            id=1,
            provider="altegio",
            company_id=1,
            altegio_client_id=1,
            display_name="Client 1",
            phone_e164="+10000000001",
            wa_opted_out=False,
        ),
        total_records_in_period=1,
        confirmed_records_in_period=1,
        lash_records_in_period=1,
        confirmed_lash_records_in_period=1,
        service_titles_in_period=["Lashes"],
        records_before_period=0,
    )
    monkeypatch.setattr(runner, "SessionLocal", session_maker)
    monkeypatch.setattr(runner, "_find_candidates", AsyncMock(return_value=[candidate]))

    preview = await runner.run_preview(_params("altegio", mode="preview"))
    queued = await runner.enqueue_send_real(_params("altegio", mode="send-real"))

    async with session_maker() as session:
        recipient = await session.scalar(
            select(CampaignRecipient).where(CampaignRecipient.campaign_run_id == preview.id)
        )
        job = await session.scalar(
            select(MessageJob).where(MessageJob.payload.contains({"campaign_run_id": queued.id}))
        )

    assert preview.provider == "altegio"
    assert recipient is not None and recipient.provider == preview.provider
    assert queued.provider == "altegio"
    assert job is not None and job.provider == queued.provider
    assert job.payload["provider"] == queued.provider
    assert job.dedupe_key.startswith("altegio:")


@pytest.mark.asyncio
async def test_postgres_rejects_recipient_provider_different_from_run(session_maker) -> None:
    async with session_maker() as session:
        run = CampaignRun(
            provider="altegio",
            campaign_code="new_clients_monthly",
            mode="preview",
            company_ids=[COMPANY_ID],
            period_start=NOW,
            period_end=datetime(2026, 9, 1, tzinfo=timezone.utc),
            status="completed",
        )
        session.add(run)
        await session.flush()
        session.add(
            CampaignRecipient(
                provider="easyweek",
                campaign_run_id=run.id,
                company_id=COMPANY_ID,
                status="candidate",
            )
        )
        with pytest.raises(IntegrityError):
            await session.flush()
        await session.rollback()


@pytest.mark.asyncio
async def test_sender_and_template_lookup_do_not_cross_provider_on_numeric_collision(
    session_maker,
    monkeypatch,
) -> None:
    location = EasyWeekLocation(
        name="karlsruhe",
        location_id=COMPANY_ID,
        location_uuid="8395fab6-7ee8-4702-88d9-fd78f92539c1",
        meta_template_prefix="ka",
        booking_page_url="https://kitilash.easyweek.de/",
    )
    monkeypatch.setattr(
        configuration,
        "configured_easyweek_locations",
        lambda: EasyWeekLocationRegistry(configured=True, valid=True, locations={COMPANY_ID: location}),
    )
    monkeypatch.setattr(configuration, "validate_static_booking_page", lambda value: value)

    async with session_maker() as session:
        async with session.begin():
            altegio_sender = WhatsAppSender(
                provider="altegio",
                company_id=COMPANY_ID,
                sender_code="campaign",
                phone_number_id="altegio-phone",
                is_active=True,
            )
            easyweek_sender = WhatsAppSender(
                provider="easyweek",
                company_id=COMPANY_ID,
                sender_code="campaign",
                phone_number_id="easyweek-phone",
                is_active=True,
            )
            altegio_template = MessageTemplate(
                provider="altegio",
                company_id=COMPANY_ID,
                code="newsletter_new_clients_monthly",
                language="de",
                body="altegio",
                meta_template_name="altegio_template",
                is_active=True,
            )
            easyweek_template = MessageTemplate(
                provider="easyweek",
                company_id=COMPANY_ID,
                code="newsletter_new_clients_monthly",
                language="de",
                body="easyweek",
                meta_template_name="easyweek_template",
                is_active=True,
            )
            session.add_all([altegio_sender, easyweek_sender, altegio_template, easyweek_template])
        readiness = await resolve_campaign_readiness(
            session,
            provider="easyweek",
            company_id=COMPANY_ID,
            sender_code="campaign",
            template_code="newsletter_new_clients_monthly",
        )

    assert readiness.sender_id == easyweek_sender.id
    assert readiness.sender_id != altegio_sender.id
    assert readiness.template_id == easyweek_template.id
    assert readiness.meta_template_name == "easyweek_template"
    assert readiness.ready_for_send is False
    assert readiness.segment_source == SEGMENT_SOURCE
    assert EASYWEEK_CAMPAIGN_SEGMENT_NOT_IMPLEMENTED not in readiness.reasons
    assert CAMPAIGN_EXECUTION_NOT_AUTHORIZED in readiness.reasons
    assert readiness.live_guard == "easyweek_customer_booking_history_reproof"


@pytest.mark.asyncio
async def test_client_lookup_stays_inside_provider_with_numeric_collision(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            session.add(
                Client(
                    provider="easyweek",
                    company_id=1,
                    altegio_client_id=1,
                    display_name="EasyWeek collision",
                    phone_e164="+10000000001",
                    raw={},
                )
            )
        matches = (
            (
                await session.execute(
                    select(Client).where(
                        Client.provider == "altegio",
                        Client.company_id == 1,
                        Client.altegio_client_id == 1,
                    )
                )
            )
            .scalars()
            .all()
        )

    assert len(matches) == 1
    assert matches[0].display_name == "Client 1"


@pytest.mark.asyncio
async def test_retry_refuses_run_recipient_job_provider_mismatch(
    session_maker,
    monkeypatch,
) -> None:
    monkeypatch.setattr(runner, "SessionLocal", session_maker)
    async with session_maker() as session:
        async with session.begin():
            run = CampaignRun(
                provider="altegio",
                campaign_code="new_clients_monthly",
                mode="send-real",
                company_ids=[COMPANY_ID],
                period_start=NOW,
                period_end=datetime(2026, 9, 1, tzinfo=timezone.utc),
                status="failed",
            )
            session.add(run)
            await session.flush()
            job = MessageJob(
                provider="easyweek",
                company_id=COMPANY_ID,
                job_type="newsletter_new_clients_monthly",
                run_at=NOW,
                status="failed",
                attempts=0,
                max_attempts=5,
                dedupe_key="easyweek:mismatch",
                payload={"campaign_run_id": run.id},
            )
            session.add(job)
            await session.flush()
            recipient = CampaignRecipient(
                provider="altegio",
                campaign_run_id=run.id,
                company_id=COMPANY_ID,
                message_job_id=job.id,
                status="queued",
            )
            session.add(recipient)
            await session.flush()
            recipient_id = recipient.id

    result = await retry_recipient_job(recipient_id)
    assert result == {"outcome": CAMPAIGN_PROVIDER_MISMATCH}


@pytest.mark.asyncio
async def test_seeded_easyweek_resume_retry_and_followup_refuse_before_loyalty(
    session_maker,
    monkeypatch,
) -> None:
    loyalty = MagicMock(side_effect=AssertionError("Altegio loyalty must not be constructed"))
    monkeypatch.setattr(runner, "SessionLocal", session_maker)
    monkeypatch.setattr(followup, "SessionLocal", session_maker)
    monkeypatch.setattr(runner, "AltegioLoyaltyClient", loyalty)

    async with session_maker() as session:
        async with session.begin():
            run = CampaignRun(
                provider="easyweek",
                campaign_code="new_clients_monthly",
                mode="send-real",
                company_ids=[COMPANY_ID],
                location_id=COMPANY_ID,
                card_type_id="not-used",
                period_start=NOW,
                period_end=datetime(2026, 9, 1, tzinfo=timezone.utc),
                status="failed",
                followup_enabled=True,
                followup_policy="unread_only",
                followup_template_name="not-used",
            )
            session.add(run)
            await session.flush()
            job = MessageJob(
                provider="easyweek",
                company_id=COMPANY_ID,
                job_type="newsletter_new_clients_monthly",
                run_at=NOW,
                status="failed",
                attempts=0,
                max_attempts=5,
                dedupe_key="easyweek:seeded-retry",
                payload={"campaign_run_id": run.id},
            )
            session.add(job)
            await session.flush()
            recipient = CampaignRecipient(
                provider="easyweek",
                campaign_run_id=run.id,
                company_id=COMPANY_ID,
                message_job_id=job.id,
                status="delivered",
            )
            session.add(recipient)
            await session.flush()
            run_id = run.id
            recipient_id = recipient.id

    with pytest.raises(CampaignProviderRefusal) as resume_error:
        await runner.resume_send_real(run_id)
    assert resume_error.value.reason == EASYWEEK_CAMPAIGN_SEGMENT_NOT_IMPLEMENTED

    async with session_maker() as session:
        with pytest.raises(CampaignProviderRefusal) as plan_error:
            await followup.plan_followup(session, run_id)
    assert plan_error.value.reason == EASYWEEK_CAMPAIGN_SEGMENT_NOT_IMPLEMENTED

    with pytest.raises(CampaignProviderRefusal) as execute_error:
        await followup.execute_followup(run_id)
    assert execute_error.value.reason == EASYWEEK_CAMPAIGN_SEGMENT_NOT_IMPLEMENTED

    retry = await runner.retry_recipient_job(recipient_id)
    assert retry == {"outcome": EASYWEEK_CAMPAIGN_SEGMENT_NOT_IMPLEMENTED}
    loyalty.assert_not_called()


@pytest.mark.asyncio
async def test_partial_campaign_identity_stays_blocked(session_maker, monkeypatch) -> None:
    reached_delivery = AsyncMock(return_value=object())
    monkeypatch.setattr(outbox_worker, "_find_success_outbox", reached_delivery)

    async with session_maker() as session:
        async with session.begin():
            for payload_ids in ("run_only", "recipient_only"):
                _run, _recipient, job = await _persist_delivery_identity(
                    session,
                    company_ids=[COMPANY_ID],
                    payload_ids=payload_ids,
                    suffix=payload_ids,
                )
                await outbox_worker._run_job_logic(session, job, provider=MagicMock())
                assert job.status == "failed"
                assert job.last_error is not None

    reached_delivery.assert_not_awaited()


@pytest.mark.asyncio
async def test_campaign_client_id_mismatch_is_blocked(session_maker, monkeypatch) -> None:
    reached_delivery, send_template, send_text = _campaign_delivery_probes(monkeypatch)

    async with session_maker() as session:
        async with session.begin():
            client_a = await _persist_campaign_client(session, external_id=9101)
            client_b = await _persist_campaign_client(session, external_id=9102)
            _run, _recipient, job = await _persist_delivery_identity(
                session,
                company_ids=[COMPANY_ID],
                recipient_client_id=client_a.id,
                job_client_id=client_b.id,
                suffix="client-mismatch",
            )
            await outbox_worker._run_job_logic(session, job, provider=MagicMock())
            assert job.status == "failed"
            assert job.last_error == CAMPAIGN_IDENTITY_MISMATCH
            assert job.attempts == 0
            assert await session.scalar(select(func.count()).select_from(OutboxMessage)) == 0

    reached_delivery.assert_not_awaited()
    send_template.assert_not_awaited()
    send_text.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("link_kind", ["other", "missing"])
async def test_campaign_job_link_mismatch_is_blocked(
    session_maker,
    monkeypatch,
    link_kind,
) -> None:
    reached_delivery, send_template, send_text = _campaign_delivery_probes(monkeypatch)

    async with session_maker() as session:
        async with session.begin():
            client = await _persist_campaign_client(session, external_id=9201)
            _run, recipient, job = await _persist_delivery_identity(
                session,
                company_ids=[COMPANY_ID],
                recipient_client_id=client.id,
                job_client_id=client.id,
                suffix=f"job-link-{link_kind}",
            )
            if link_kind == "other":
                other_job = MessageJob(
                    provider="altegio",
                    company_id=COMPANY_ID,
                    client_id=client.id,
                    job_type="newsletter_new_clients_monthly",
                    run_at=NOW,
                    status="queued",
                    attempts=0,
                    max_attempts=5,
                    dedupe_key="altegio:test:other-primary-job",
                    payload={"kind": "newsletter_new_clients_monthly"},
                )
                session.add(other_job)
                await session.flush()
                recipient.message_job_id = other_job.id
            else:
                recipient.message_job_id = None

            await outbox_worker._run_job_logic(session, job, provider=MagicMock())
            assert job.status == "failed"
            assert job.last_error == CAMPAIGN_IDENTITY_MISMATCH
            assert job.attempts == 0
            assert await session.scalar(select(func.count()).select_from(OutboxMessage)) == 0

    reached_delivery.assert_not_awaited()
    send_template.assert_not_awaited()
    send_text.assert_not_awaited()


@pytest.mark.asyncio
async def test_valid_primary_campaign_identity_passes(session_maker, monkeypatch) -> None:
    reached_delivery, send_template, send_text = _campaign_delivery_probes(monkeypatch)

    async with session_maker() as session:
        async with session.begin():
            client = await _persist_campaign_client(session, external_id=9301)
            _run, recipient, job = await _persist_delivery_identity(
                session,
                company_ids=[COMPANY_ID],
                recipient_client_id=client.id,
                job_client_id=client.id,
                suffix="valid-primary",
            )
            await outbox_worker._run_job_logic(session, job, provider=MagicMock())
            assert recipient.message_job_id == job.id
            assert job.status == "done"
            assert job.last_error is None
            assert job.attempts == 0

    reached_delivery.assert_awaited_once()
    send_template.assert_not_awaited()
    send_text.assert_not_awaited()


@pytest.mark.asyncio
async def test_followup_uses_followup_message_job_id(session_maker, monkeypatch) -> None:
    reached_delivery, send_template, send_text = _campaign_delivery_probes(monkeypatch)

    async with session_maker() as session:
        async with session.begin():
            client = await _persist_campaign_client(session, external_id=9401)
            primary_job = MessageJob(
                provider="altegio",
                company_id=COMPANY_ID,
                client_id=client.id,
                job_type="newsletter_new_clients_monthly",
                run_at=NOW,
                status="done",
                attempts=1,
                max_attempts=5,
                dedupe_key="altegio:test:followup-primary",
                payload={"kind": "newsletter_new_clients_monthly"},
            )
            session.add(primary_job)
            await session.flush()
            _run, recipient, followup_job = await _persist_delivery_identity(
                session,
                company_ids=[COMPANY_ID],
                recipient_client_id=client.id,
                job_client_id=client.id,
                job_type=runner.FOLLOWUP_JOB_TYPE,
                suffix="valid-followup",
            )
            recipient.message_job_id = primary_job.id

            await outbox_worker._run_job_logic(session, followup_job, provider=MagicMock())
            assert recipient.message_job_id == primary_job.id
            assert recipient.followup_message_job_id == followup_job.id
            assert followup_job.status == "done"
            assert followup_job.last_error is None
            assert followup_job.attempts == 0

    reached_delivery.assert_awaited_once()
    send_template.assert_not_awaited()
    send_text.assert_not_awaited()


@pytest.mark.asyncio
async def test_followup_job_link_mismatch_is_blocked(session_maker, monkeypatch) -> None:
    reached_delivery, send_template, send_text = _campaign_delivery_probes(monkeypatch)

    async with session_maker() as session:
        async with session.begin():
            client = await _persist_campaign_client(session, external_id=9501)
            _run, recipient, followup_job = await _persist_delivery_identity(
                session,
                company_ids=[COMPANY_ID],
                recipient_client_id=client.id,
                job_client_id=client.id,
                job_type=runner.FOLLOWUP_JOB_TYPE,
                suffix="followup-link-mismatch",
            )
            recipient.followup_message_job_id = None

            await outbox_worker._run_job_logic(session, followup_job, provider=MagicMock())
            assert followup_job.status == "canceled"
            assert followup_job.last_error == CAMPAIGN_IDENTITY_MISMATCH
            assert followup_job.attempts == 0
            assert await session.scalar(select(func.count()).select_from(OutboxMessage)) == 0

    reached_delivery.assert_not_awaited()
    send_template.assert_not_awaited()
    send_text.assert_not_awaited()


@pytest.mark.asyncio
async def test_foreign_provider_client_is_blocked(session_maker, monkeypatch) -> None:
    reached_delivery, send_template, send_text = _campaign_delivery_probes(monkeypatch)

    async with session_maker() as session:
        async with session.begin():
            client = await _persist_campaign_client(
                session,
                provider="easyweek",
                external_id=9601,
            )
            _run, _recipient, job = await _persist_delivery_identity(
                session,
                company_ids=[COMPANY_ID],
                recipient_client_id=client.id,
                job_client_id=client.id,
                suffix="foreign-client",
            )
            await outbox_worker._run_job_logic(session, job, provider=MagicMock())
            assert job.status == "failed"
            assert job.last_error == CAMPAIGN_IDENTITY_MISMATCH
            assert job.attempts == 0
            assert await session.scalar(select(func.count()).select_from(OutboxMessage)) == 0

    reached_delivery.assert_not_awaited()
    send_template.assert_not_awaited()
    send_text.assert_not_awaited()


@pytest.mark.asyncio
async def test_client_company_mismatch_is_blocked(session_maker, monkeypatch) -> None:
    reached_delivery, send_template, send_text = _campaign_delivery_probes(monkeypatch)

    async with session_maker() as session:
        async with session.begin():
            client = await _persist_campaign_client(
                session,
                company_id=COMPANY_ID + 1,
                external_id=9701,
            )
            _run, _recipient, job = await _persist_delivery_identity(
                session,
                company_ids=[COMPANY_ID],
                recipient_client_id=client.id,
                job_client_id=client.id,
                suffix="client-company",
            )
            await outbox_worker._run_job_logic(session, job, provider=MagicMock())
            assert job.status == "failed"
            assert job.last_error == CAMPAIGN_IDENTITY_MISMATCH
            assert job.attempts == 0
            assert await session.scalar(select(func.count()).select_from(OutboxMessage)) == 0

    reached_delivery.assert_not_awaited()
    send_template.assert_not_awaited()
    send_text.assert_not_awaited()


@pytest.mark.asyncio
async def test_crm_only_matching_phone_passes(session_maker, monkeypatch) -> None:
    reached_delivery, send_template, send_text = _campaign_delivery_probes(monkeypatch)

    async with session_maker() as session:
        async with session.begin():
            _run, recipient, job = await _persist_delivery_identity(
                session,
                company_ids=[COMPANY_ID],
                recipient_phone="+49 151 33333333",
                payload_phone="49151-33333333",
                suffix="crm-phone-match",
            )
            await outbox_worker._run_job_logic(session, job, provider=MagicMock())
            assert recipient.client_id is None
            assert job.client_id is None
            assert job.status == "done"
            assert job.last_error is None

    reached_delivery.assert_awaited_once()
    send_template.assert_not_awaited()
    send_text.assert_not_awaited()


@pytest.mark.asyncio
async def test_local_client_payload_phone_fallback_matches_recipient(session_maker, monkeypatch) -> None:
    reached_delivery, send_template, send_text = _campaign_delivery_probes(monkeypatch)

    async with session_maker() as session:
        async with session.begin():
            client = await _persist_campaign_client(
                session,
                external_id=9801,
                phone_e164=None,
            )
            _run, _recipient, job = await _persist_delivery_identity(
                session,
                company_ids=[COMPANY_ID],
                recipient_client_id=client.id,
                job_client_id=client.id,
                recipient_phone="+4915166666666",
                payload_phone="49 151 66666666",
                suffix="local-phone-fallback",
            )
            await outbox_worker._run_job_logic(session, job, provider=MagicMock())
            assert job.status == "done"
            assert job.last_error is None

    reached_delivery.assert_awaited_once()
    send_template.assert_not_awaited()
    send_text.assert_not_awaited()


@pytest.mark.asyncio
async def test_local_client_payload_phone_fallback_mismatch_is_blocked(
    session_maker,
    monkeypatch,
) -> None:
    reached_delivery, send_template, send_text = _campaign_delivery_probes(monkeypatch)

    async with session_maker() as session:
        async with session.begin():
            client = await _persist_campaign_client(
                session,
                external_id=9802,
                phone_e164=None,
            )
            _run, _recipient, job = await _persist_delivery_identity(
                session,
                company_ids=[COMPANY_ID],
                recipient_client_id=client.id,
                job_client_id=client.id,
                recipient_phone="+4915166666666",
                payload_phone="+4915177777777",
                suffix="local-phone-fallback-mismatch",
            )
            await outbox_worker._run_job_logic(session, job, provider=MagicMock())
            assert job.status == "failed"
            assert job.last_error == CAMPAIGN_IDENTITY_MISMATCH
            assert job.attempts == 0
            assert await session.scalar(select(func.count()).select_from(OutboxMessage)) == 0

    reached_delivery.assert_not_awaited()
    send_template.assert_not_awaited()
    send_text.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("recipient_phone", "payload_phone"),
    [
        ("+4915144444444", "+4915155555555"),
        (None, "+4915144444444"),
        ("+4915144444444", None),
        ("+49invalid", "+49invalid"),
    ],
    ids=["different", "missing-recipient", "missing-payload", "invalid"],
)
async def test_crm_only_phone_mismatch_is_blocked(
    session_maker,
    monkeypatch,
    recipient_phone,
    payload_phone,
) -> None:
    reached_delivery, send_template, send_text = _campaign_delivery_probes(monkeypatch)

    async with session_maker() as session:
        async with session.begin():
            _run, _recipient, job = await _persist_delivery_identity(
                session,
                company_ids=[COMPANY_ID],
                recipient_phone=recipient_phone,
                payload_phone=payload_phone,
                suffix="crm-phone-mismatch",
            )
            await outbox_worker._run_job_logic(session, job, provider=MagicMock())
            assert job.status == "failed"
            assert job.last_error == CAMPAIGN_IDENTITY_MISMATCH
            assert job.attempts == 0
            assert await session.scalar(select(func.count()).select_from(OutboxMessage)) == 0

    reached_delivery.assert_not_awaited()
    send_template.assert_not_awaited()
    send_text.assert_not_awaited()


@pytest.mark.asyncio
async def test_multi_company_run_jobs_pass_identity_guard(session_maker, monkeypatch) -> None:
    reached_delivery = AsyncMock(return_value=object())
    monkeypatch.setattr(outbox_worker, "_find_success_outbox", reached_delivery)

    async with session_maker() as session:
        async with session.begin():
            run = CampaignRun(
                provider="altegio",
                campaign_code="new_clients_monthly",
                mode="send-real",
                company_ids=[COMPANY_ID, COMPANY_ID + 1],
                period_start=NOW,
                period_end=datetime(2026, 9, 1, tzinfo=timezone.utc),
                status="running",
            )
            session.add(run)
            await session.flush()
            jobs: list[MessageJob] = []
            for company_id in run.company_ids:
                recipient = CampaignRecipient(
                    provider="altegio",
                    campaign_run_id=run.id,
                    company_id=company_id,
                    phone_e164=f"+491511111{company_id}",
                    status="queued",
                )
                session.add(recipient)
                await session.flush()
                job = MessageJob(
                    provider="altegio",
                    company_id=company_id,
                    job_type="newsletter_new_clients_monthly",
                    run_at=NOW,
                    status="processing",
                    attempts=0,
                    max_attempts=5,
                    dedupe_key=f"altegio:test:multi:{run.id}:{company_id}",
                    payload={
                        "campaign_run_id": run.id,
                        "campaign_recipient_id": recipient.id,
                        "phone_e164": recipient.phone_e164,
                    },
                )
                session.add(job)
                await session.flush()
                recipient.message_job_id = job.id
                jobs.append(job)

            for job in jobs:
                await outbox_worker._run_job_logic(session, job, provider=MagicMock())

            assert [job.status for job in jobs] == ["done", "done"]

    assert reached_delivery.await_count == 2


@pytest.mark.asyncio
async def test_company_outside_run_scope_is_blocked(session_maker, monkeypatch) -> None:
    reached_delivery = AsyncMock(return_value=object())
    monkeypatch.setattr(outbox_worker, "_find_success_outbox", reached_delivery)

    async with session_maker() as session:
        async with session.begin():
            _run, _recipient, job = await _persist_delivery_identity(
                session,
                company_ids=[COMPANY_ID + 1],
                suffix="outside",
            )
            await outbox_worker._run_job_logic(session, job, provider=MagicMock())
            assert job.status == "failed"
            assert job.last_error == CAMPAIGN_IDENTITY_MISMATCH

    reached_delivery.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "company_ids",
    [
        [],
        [str(COMPANY_ID)],
        [True],
        [float(COMPANY_ID)],
        None,
        {"company_id": COMPANY_ID},
        [COMPANY_ID, COMPANY_ID],
    ],
    ids=["empty", "string-id", "bool", "float", "null", "non-list", "duplicate"],
)
async def test_malformed_company_scope_is_blocked(
    session_maker,
    monkeypatch,
    company_ids,
) -> None:
    reached_delivery = AsyncMock(return_value=object())
    monkeypatch.setattr(outbox_worker, "_find_success_outbox", reached_delivery)

    async with session_maker() as session:
        async with session.begin():
            _run, _recipient, job = await _persist_delivery_identity(
                session,
                company_ids=company_ids,
                suffix=f"malformed-{type(company_ids).__name__}",
            )
            await outbox_worker._run_job_logic(session, job, provider=MagicMock())
            assert job.status == "failed"
            assert job.last_error == CAMPAIGN_IDENTITY_MISMATCH

    reached_delivery.assert_not_awaited()


@pytest.mark.asyncio
async def test_recipient_company_mismatch_is_blocked(session_maker, monkeypatch) -> None:
    reached_delivery = AsyncMock(return_value=object())
    monkeypatch.setattr(outbox_worker, "_find_success_outbox", reached_delivery)

    async with session_maker() as session:
        async with session.begin():
            _run, _recipient, job = await _persist_delivery_identity(
                session,
                company_ids=[COMPANY_ID, COMPANY_ID + 1],
                recipient_company_id=COMPANY_ID + 1,
                suffix="recipient-company",
            )
            await outbox_worker._run_job_logic(session, job, provider=MagicMock())
            assert job.status == "failed"
            assert job.last_error == CAMPAIGN_IDENTITY_MISMATCH

    reached_delivery.assert_not_awaited()


@pytest.mark.asyncio
async def test_legacy_standalone_altegio_job_is_preserved(session_maker, monkeypatch) -> None:
    reached_delivery = AsyncMock(return_value=object())
    monkeypatch.setattr(outbox_worker, "_find_success_outbox", reached_delivery)

    async with session_maker() as session:
        async with session.begin():
            job = MessageJob(
                provider="altegio",
                company_id=COMPANY_ID,
                job_type="newsletter_new_clients_monthly",
                run_at=NOW,
                status="processing",
                attempts=0,
                max_attempts=5,
                dedupe_key="altegio:test:legacy-standalone",
                payload={"kind": "newsletter_new_clients_monthly"},
            )
            session.add(job)
            await session.flush()
            await outbox_worker._run_job_logic(session, job, provider=MagicMock())
            assert job.status == "done"
            assert job.last_error is None

    reached_delivery.assert_awaited_once()


@pytest.mark.asyncio
async def test_late_easyweek_jobs_are_terminal_before_attempts_or_external_calls(
    session_maker,
    monkeypatch,
) -> None:
    send_template = AsyncMock(side_effect=AssertionError("Meta must not run"))
    send_text = AsyncMock(side_effect=AssertionError("Meta must not run"))
    monkeypatch.setattr(outbox_worker, "safe_send_template", send_template)
    monkeypatch.setattr(outbox_worker, "safe_send", send_text)
    execute = AsyncMock(side_effect=AssertionError("campaign runner must not run"))
    monkeypatch.setattr(campaign_worker, "execute_queued_send_real", execute)

    async with session_maker() as session:
        async with session.begin():
            newsletter = MessageJob(
                provider="easyweek",
                company_id=COMPANY_ID,
                job_type="newsletter_new_clients_monthly",
                run_at=NOW,
                status="processing",
                attempts=0,
                max_attempts=5,
                dedupe_key="easyweek:late-newsletter",
                payload={},
            )
            execution = MessageJob(
                provider="easyweek",
                company_id=COMPANY_ID,
                job_type=runner.CAMPAIGN_EXECUTION_JOB_TYPE,
                run_at=NOW,
                status="processing",
                attempts=0,
                max_attempts=1,
                dedupe_key="easyweek:late-execution",
                payload={"campaign_run_id": 999},
            )
            session.add_all([newsletter, execution])
            await session.flush()
            await outbox_worker._run_job_logic(session, newsletter, provider=MagicMock())
            await campaign_worker.process_job_in_session(session, execution.id)

        assert newsletter.status == "failed"
        assert newsletter.last_error == EASYWEEK_CAMPAIGN_SEGMENT_NOT_IMPLEMENTED
        assert newsletter.attempts == 0
        assert execution.status == "failed"
        assert execution.last_error == EASYWEEK_CAMPAIGN_SEGMENT_NOT_IMPLEMENTED
        assert execution.attempts == 0
        assert await session.scalar(select(func.count()).select_from(OutboxMessage)) == 0

    send_template.assert_not_awaited()
    send_text.assert_not_awaited()
    execute.assert_not_awaited()
