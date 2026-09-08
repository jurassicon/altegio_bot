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
from altegio_bot.campaigns.provider import (
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
async def test_easyweek_preview_persists_only_pii_free_refusal(
    session_maker,
    monkeypatch,
) -> None:
    discover = AsyncMock(side_effect=AssertionError("Altegio discovery must not run"))
    monkeypatch.setattr(runner, "SessionLocal", session_maker)
    monkeypatch.setattr(runner, "_find_candidates", discover)

    run = await runner.run_preview(_params("easyweek", mode="preview"))

    assert run.provider == "easyweek"
    assert run.status == "failed"
    assert run.meta["last_error"] == EASYWEEK_CAMPAIGN_SEGMENT_NOT_IMPLEMENTED
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
    assert EASYWEEK_CAMPAIGN_SEGMENT_NOT_IMPLEMENTED in readiness.reasons


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
