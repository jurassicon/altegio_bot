"""Tests: auto-hide preview after a reconciled successful send-real.

The preview run is hidden (meta['hidden']=True) only when the send-real
run is fully reconciled, i.e., recompute_campaign_run_stats is called and
no recipient remains in an in-flight or problematic state:
  candidate | cleanup_failed | card_issued | queued

This means:
  - Auto-hide is NOT triggered at send-real finalization time (jobs only
    queued, outbox has not yet confirmed delivery).
  - Auto-hide IS triggered when recompute finds all recipients advanced
    past the in-flight states (typically provider_accepted or better).
  - Hidden preview stays in DB, is accessible via direct link, and is
    absent from the default list unless include_hidden=true.

Coverage:
  1.  send-real finalization with queued recipients: preview NOT hidden
  2.  recompute with provider_accepted recipients: preview IS hidden
  3.  hidden_reason and hidden_by_run_id recorded correctly
  4.  recompute with outbox still queued: preview NOT hidden
  5.  recompute with cleanup_failed recipient: preview NOT hidden
  6.  failed send-real: preview NOT hidden
  7.  partial failure: preview NOT hidden
  8.  hidden preview absent from default list
  9.  hidden preview visible with include_hidden=true
  10. hidden preview accessible via direct GET /runs/{id}
  11. auto-hide is idempotent (second recompute preserves first metadata)
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

import altegio_bot.campaigns.runner as runner_module
import altegio_bot.ops.campaigns_api as campaigns_api_module
import altegio_bot.ops.router as ops_router_module
from altegio_bot.campaigns.runner import RunParams, run_send_real
from altegio_bot.campaigns.segment import ClientCandidate, ClientSnapshot
from altegio_bot.main import app
from altegio_bot.models.models import (
    CampaignRecipient,
    CampaignRun,
    MessageJob,
    OutboxMessage,
)
from altegio_bot.ops.auth import require_ops_auth

COMPANY = 758285
LOCATION = 1
CARD_TYPE = "type-abc"
PERIOD_START = datetime(2026, 3, 1, tzinfo=timezone.utc)
PERIOD_END = datetime(2026, 4, 1, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _make_params(
    source_preview_run_id: int | None = None,
) -> RunParams:
    return RunParams(
        company_id=COMPANY,
        location_id=LOCATION,
        period_start=PERIOD_START,
        period_end=PERIOD_END,
        mode="send-real",
        card_type_id=CARD_TYPE,
        source_preview_run_id=source_preview_run_id,
    )


def _eligible_candidate(
    phone_e164: str = "+4915111111111",
) -> ClientCandidate:
    snapshot = ClientSnapshot(
        id=None,
        company_id=COMPANY,
        altegio_client_id=9001,
        display_name="Test Kunde",
        phone_e164=phone_e164,
        wa_opted_out=False,
    )
    return ClientCandidate(
        client=snapshot,
        total_records_in_period=1,
        confirmed_records_in_period=1,
        lash_records_in_period=1,
        confirmed_lash_records_in_period=1,
        service_titles_in_period=["Wimpern"],
        records_before_period=0,
        local_client_found=False,
        excluded_reason=None,
    )


class _LoyaltyOK:
    """Mock loyalty: always issues cards successfully."""

    def __init__(self) -> None:
        self.issued: list[dict] = []

    async def get_card_types(self, location_id):
        return [{"id": CARD_TYPE}]

    async def issue_card(
        self,
        location_id,
        *,
        loyalty_card_number,
        loyalty_card_type_id,
        phone,
    ):
        self.issued.append({"number": loyalty_card_number, "phone": phone})
        return {"loyalty_card_number": loyalty_card_number, "id": "card-ok"}

    async def delete_card(self, location_id, card_id):
        pass

    async def aclose(self):
        pass


class _LoyaltyFail:
    """Mock loyalty: always raises on issue_card."""

    async def get_card_types(self, location_id):
        return [{"id": CARD_TYPE}]

    async def issue_card(
        self,
        location_id,
        *,
        loyalty_card_number,
        loyalty_card_type_id,
        phone,
    ):
        raise RuntimeError("Altegio API: card already exists")

    async def delete_card(self, location_id, card_id):
        pass

    async def aclose(self):
        pass


class _LoyaltyPartial:
    """Mock loyalty: fails on the second issue_card call."""

    def __init__(self) -> None:
        self.call_count = 0

    async def get_card_types(self, location_id):
        return [{"id": CARD_TYPE}]

    async def issue_card(
        self,
        location_id,
        *,
        loyalty_card_number,
        loyalty_card_type_id,
        phone,
    ):
        self.call_count += 1
        if self.call_count >= 2:
            raise RuntimeError("Altegio API: duplicate card")
        return {
            "loyalty_card_number": loyalty_card_number,
            "id": f"card-{self.call_count}",
        }

    async def delete_card(self, location_id, card_id):
        pass

    async def aclose(self):
        pass


async def _create_preview_run(session_maker) -> int:
    """Insert a completed preview CampaignRun and return its id."""
    async with session_maker() as session:
        async with session.begin():
            preview = CampaignRun(
                campaign_code="new_clients_monthly",
                mode="preview",
                company_ids=[COMPANY],
                period_start=PERIOD_START,
                period_end=PERIOD_END,
                status="completed",
                candidates_count=1,
                meta={},
            )
            session.add(preview)
            await session.flush()
            return preview.id


async def _create_reconciled_send_real(
    session_maker,
    *,
    preview_run_id: int,
    outbox_status: str = "sent",
) -> int:
    """Create a completed send-real run with one recipient and outbox row.

    Recipient starts in 'queued' status with message_job_id set.
    OutboxMessage linked via job_id only (production gap pattern).

    After recompute:
    - outbox_status 'sent' -> recipient advances to provider_accepted.
    - outbox_status 'queued' -> recipient stays 'queued' (blocking).
    """
    now = datetime.now(timezone.utc)
    async with session_maker() as session:
        async with session.begin():
            run = CampaignRun(
                campaign_code="new_clients_monthly",
                mode="send-real",
                company_ids=[COMPANY],
                period_start=PERIOD_START,
                period_end=PERIOD_END,
                status="completed",
                source_preview_run_id=preview_run_id,
                total_clients_seen=1,
                candidates_count=1,
                queued_count=1,
                cards_issued_count=1,
                meta={},
            )
            session.add(run)
            await session.flush()
            run_id = run.id

            job = MessageJob(
                company_id=COMPANY,
                job_type="newsletter_new_clients_monthly",
                run_at=now,
                status="done",
                dedupe_key=f"test-autohide-job-{run_id}",
                payload={"campaign_run_id": run_id},
            )
            session.add(job)
            await session.flush()
            job_id = job.id

            outbox = OutboxMessage(
                company_id=COMPANY,
                job_id=job_id,
                phone_e164="+4915111111111",
                template_code="newsletter_new_clients_monthly",
                language="de",
                body="Test body",
                status=outbox_status,
                scheduled_at=now,
            )
            session.add(outbox)

            # Recipient has job_id but no outbox_message_id (gap resolved
            # by recompute)
            recipient = CampaignRecipient(
                campaign_run_id=run_id,
                company_id=COMPANY,
                altegio_client_id=9001,
                phone_e164="+4915111111111",
                display_name="Test Kunde",
                status="queued",
                loyalty_card_id="CARD-001",
                message_job_id=job_id,
                outbox_message_id=None,
            )
            session.add(recipient)

    return run_id


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
def runner_ok(session_maker, monkeypatch) -> _LoyaltyOK:
    loyalty = _LoyaltyOK()
    monkeypatch.setattr(runner_module, "SessionLocal", session_maker)
    monkeypatch.setattr(runner_module, "AltegioLoyaltyClient", lambda: loyalty)
    return loyalty


@pytest_asyncio.fixture
def runner_fail(session_maker, monkeypatch) -> _LoyaltyFail:
    loyalty = _LoyaltyFail()
    monkeypatch.setattr(runner_module, "SessionLocal", session_maker)
    monkeypatch.setattr(runner_module, "AltegioLoyaltyClient", lambda: loyalty)
    return loyalty


@pytest_asyncio.fixture
def runner_partial(session_maker, monkeypatch) -> _LoyaltyPartial:
    loyalty = _LoyaltyPartial()
    monkeypatch.setattr(runner_module, "SessionLocal", session_maker)
    monkeypatch.setattr(runner_module, "AltegioLoyaltyClient", lambda: loyalty)
    return loyalty


@pytest_asyncio.fixture
async def http_client(session_maker, monkeypatch) -> AsyncGenerator[AsyncClient, None]:
    monkeypatch.setattr(ops_router_module, "SessionLocal", session_maker)
    monkeypatch.setattr(campaigns_api_module, "SessionLocal", session_maker)
    monkeypatch.setitem(app.dependency_overrides, require_ops_auth, lambda: None)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        yield client


# ---------------------------------------------------------------------------
# 1. Send-real finalization with queued recipients: preview NOT hidden
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_send_real_finalization_does_not_hide_preview(runner_ok, session_maker) -> None:
    """After send-real completes (jobs queued), preview must NOT be hidden.

    At finalization time jobs are only queued — outbox has not yet
    confirmed delivery.  Auto-hide must not trigger here.
    """
    preview_run_id = await _create_preview_run(session_maker)
    candidate = _eligible_candidate()

    with patch.object(
        runner_module,
        "_load_candidates_from_preview_snapshot",
        new=AsyncMock(return_value=[candidate]),
    ):
        send_real = await run_send_real(_make_params(source_preview_run_id=preview_run_id))

    assert send_real.status == "completed"
    assert not send_real.meta.get("partial_failure")
    assert "last_error" not in send_real.meta

    async with session_maker() as session:
        preview = await session.get(CampaignRun, preview_run_id)

    assert not (preview.meta or {}).get("hidden"), (
        "Preview must NOT be hidden right after send-real finalization "
        "(jobs only queued, outbox not yet processed). "
        f"meta={preview.meta!r}"
    )


# ---------------------------------------------------------------------------
# 2. Recompute with provider_accepted recipients: preview IS hidden
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_recompute_hides_preview_when_reconciled(http_client, session_maker) -> None:
    """Recompute hides preview when all recipients reach provider_accepted.

    Setup: outbox.status='sent' -> after _sync_recipient_statuses the
    recipient advances from 'queued' to 'provider_accepted'.
    'provider_accepted' is not in _BLOCKING_HIDE_STATUSES -> auto-hide.
    """
    preview_run_id = await _create_preview_run(session_maker)
    send_real_id = await _create_reconciled_send_real(
        session_maker,
        preview_run_id=preview_run_id,
        outbox_status="sent",
    )

    response = await http_client.post(f"/ops/campaigns/runs/{send_real_id}/recompute")
    assert response.status_code == 200

    async with session_maker() as session:
        preview = await session.get(CampaignRun, preview_run_id)

    assert (preview.meta or {}).get("hidden") is True, (
        f"Expected preview to be hidden after reconciled recompute, got meta={preview.meta!r}"
    )


# ---------------------------------------------------------------------------
# 3. hidden_reason and hidden_by_run_id recorded correctly
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_recompute_records_reason_and_run_id(http_client, session_maker) -> None:
    """meta must carry hidden_reason and hidden_by_run_id after auto-hide."""
    preview_run_id = await _create_preview_run(session_maker)
    send_real_id = await _create_reconciled_send_real(
        session_maker,
        preview_run_id=preview_run_id,
        outbox_status="sent",
    )

    await http_client.post(f"/ops/campaigns/runs/{send_real_id}/recompute")

    async with session_maker() as session:
        preview = await session.get(CampaignRun, preview_run_id)

    assert preview.meta.get("hidden_reason") == ("auto_hidden_after_successful_send_real"), (
        f"Unexpected hidden_reason: {preview.meta.get('hidden_reason')!r}"
    )
    assert preview.meta.get("hidden_by_run_id") == send_real_id, (
        f"Expected hidden_by_run_id={send_real_id}, got {preview.meta.get('hidden_by_run_id')!r}"
    )


# ---------------------------------------------------------------------------
# 4. Recompute with outbox still queued: preview NOT hidden
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_recompute_with_queued_outbox_does_not_hide_preview(http_client, session_maker) -> None:
    """When outbox is still 'queued', recipient stays 'queued' -> no hide.

    _sync_recipient_statuses does not map outbox 'queued' to a recipient
    advance. The recipient stays in the blocking 'queued' state.
    """
    preview_run_id = await _create_preview_run(session_maker)
    send_real_id = await _create_reconciled_send_real(
        session_maker,
        preview_run_id=preview_run_id,
        outbox_status="queued",
    )

    response = await http_client.post(f"/ops/campaigns/runs/{send_real_id}/recompute")
    assert response.status_code == 200

    async with session_maker() as session:
        preview = await session.get(CampaignRun, preview_run_id)

    assert not (preview.meta or {}).get("hidden"), (
        "Preview must NOT be hidden when outbox is still queued "
        f"(recipient stays in blocking state). meta={preview.meta!r}"
    )


# ---------------------------------------------------------------------------
# 5. Recompute with cleanup_failed recipient: preview NOT hidden
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_recompute_with_cleanup_failed_does_not_hide_preview(http_client, session_maker) -> None:
    """cleanup_failed recipient blocks auto-hide regardless of meta flags."""
    async with session_maker() as session:
        async with session.begin():
            preview = CampaignRun(
                campaign_code="new_clients_monthly",
                mode="preview",
                company_ids=[COMPANY],
                period_start=PERIOD_START,
                period_end=PERIOD_END,
                status="completed",
                candidates_count=1,
                meta={},
            )
            session.add(preview)
            await session.flush()
            preview_run_id = preview.id

            # send-real: completed but has a cleanup_failed recipient.
            # No partial_failure in meta — tests the direct status check.
            run = CampaignRun(
                campaign_code="new_clients_monthly",
                mode="send-real",
                company_ids=[COMPANY],
                period_start=PERIOD_START,
                period_end=PERIOD_END,
                status="completed",
                source_preview_run_id=preview_run_id,
                total_clients_seen=1,
                candidates_count=1,
                queued_count=0,
                cards_issued_count=0,
                meta={},
            )
            session.add(run)
            await session.flush()
            send_real_id = run.id

            session.add(
                CampaignRecipient(
                    campaign_run_id=send_real_id,
                    company_id=COMPANY,
                    altegio_client_id=9001,
                    phone_e164="+4915111111111",
                    display_name="Test Kunde",
                    status="cleanup_failed",
                    excluded_reason="cleanup_failed",
                )
            )

    response = await http_client.post(f"/ops/campaigns/runs/{send_real_id}/recompute")
    assert response.status_code == 200

    async with session_maker() as session:
        preview = await session.get(CampaignRun, preview_run_id)

    assert not (preview.meta or {}).get("hidden"), (
        f"cleanup_failed recipient must block auto-hide. meta={preview.meta!r}"
    )


# ---------------------------------------------------------------------------
# 6. Failed send-real: preview NOT hidden
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_failed_send_real_does_not_hide_preview(runner_fail, session_maker) -> None:
    """When the send-real fails (status='failed'), preview stays visible."""
    preview_run_id = await _create_preview_run(session_maker)
    candidate = _eligible_candidate()

    with patch.object(
        runner_module,
        "_load_candidates_from_preview_snapshot",
        new=AsyncMock(return_value=[candidate]),
    ):
        send_real = await run_send_real(_make_params(source_preview_run_id=preview_run_id))

    assert send_real.status == "failed", f"Expected send-real to fail, got {send_real.status!r}"

    async with session_maker() as session:
        preview = await session.get(CampaignRun, preview_run_id)

    assert not (preview.meta or {}).get("hidden"), f"Failed send-real must NOT hide preview. meta={preview.meta!r}"


# ---------------------------------------------------------------------------
# 7. Partial failure: preview NOT hidden
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_partial_failure_does_not_hide_preview(runner_partial, session_maker) -> None:
    """Partial failure (completed + partial_failure) must not hide preview."""
    preview_run_id = await _create_preview_run(session_maker)
    c1 = _eligible_candidate(phone_e164="+4915111111111")
    c2 = _eligible_candidate(phone_e164="+4915222222222")

    with patch.object(
        runner_module,
        "_load_candidates_from_preview_snapshot",
        new=AsyncMock(return_value=[c1, c2]),
    ):
        send_real = await run_send_real(_make_params(source_preview_run_id=preview_run_id))

    assert send_real.status == "completed"
    assert send_real.meta.get("partial_failure") is True

    async with session_maker() as session:
        preview = await session.get(CampaignRun, preview_run_id)

    assert not (preview.meta or {}).get("hidden"), f"Partial failure must NOT hide preview. meta={preview.meta!r}"


# ---------------------------------------------------------------------------
# 8. Hidden preview absent from default list
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_hidden_preview_absent_from_default_list(http_client, session_maker) -> None:
    """A hidden preview must not appear in GET /ops/campaigns/runs."""
    async with session_maker() as session:
        async with session.begin():
            preview = CampaignRun(
                campaign_code="new_clients_monthly",
                mode="preview",
                company_ids=[COMPANY],
                period_start=PERIOD_START,
                period_end=PERIOD_END,
                status="completed",
                candidates_count=0,
                meta={
                    "hidden": True,
                    "hidden_reason": ("auto_hidden_after_successful_send_real"),
                    "hidden_by_run_id": 999,
                },
            )
            session.add(preview)
            await session.flush()
            hidden_id = preview.id

    response = await http_client.get("/ops/campaigns/runs")
    assert response.status_code == 200
    data = response.json()

    ids = [item["id"] for item in data["items"]]
    assert hidden_id not in ids, f"Hidden preview run_id={hidden_id} must not appear in default list, got ids={ids}"


# ---------------------------------------------------------------------------
# 9. Hidden preview visible with include_hidden=true
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_hidden_preview_visible_with_include_hidden(http_client, session_maker) -> None:
    """GET /ops/campaigns/runs?include_hidden=true must show hidden preview."""
    async with session_maker() as session:
        async with session.begin():
            preview = CampaignRun(
                campaign_code="new_clients_monthly",
                mode="preview",
                company_ids=[COMPANY],
                period_start=PERIOD_START,
                period_end=PERIOD_END,
                status="completed",
                candidates_count=0,
                meta={
                    "hidden": True,
                    "hidden_reason": ("auto_hidden_after_successful_send_real"),
                },
            )
            session.add(preview)
            await session.flush()
            hidden_id = preview.id

    response = await http_client.get("/ops/campaigns/runs?include_hidden=true")
    assert response.status_code == 200
    data = response.json()

    ids = [item["id"] for item in data["items"]]
    assert hidden_id in ids, f"Hidden preview run_id={hidden_id} must appear when include_hidden=true, got ids={ids}"


# ---------------------------------------------------------------------------
# 10. Hidden preview accessible via direct GET /runs/{id}
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_hidden_preview_accessible_by_direct_link(http_client, session_maker) -> None:
    """GET /ops/campaigns/runs/{id} must return 200 for a hidden preview."""
    async with session_maker() as session:
        async with session.begin():
            preview = CampaignRun(
                campaign_code="new_clients_monthly",
                mode="preview",
                company_ids=[COMPANY],
                period_start=PERIOD_START,
                period_end=PERIOD_END,
                status="completed",
                candidates_count=0,
                meta={
                    "hidden": True,
                    "hidden_reason": ("auto_hidden_after_successful_send_real"),
                },
            )
            session.add(preview)
            await session.flush()
            hidden_id = preview.id

    response = await http_client.get(f"/ops/campaigns/runs/{hidden_id}")
    assert response.status_code == 200, f"Expected 200 for hidden preview direct link, got {response.status_code}"
    data = response.json()
    assert data["id"] == hidden_id
    assert data["hidden"] is True


# ---------------------------------------------------------------------------
# 11. Auto-hide is idempotent
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_auto_hide_is_idempotent(http_client, session_maker) -> None:
    """Second call to _auto_hide_preview_run must not overwrite first data."""
    preview_run_id = await _create_preview_run(session_maker)
    send_real_id = await _create_reconciled_send_real(
        session_maker,
        preview_run_id=preview_run_id,
        outbox_status="sent",
    )

    # First recompute: auto-hides preview
    resp = await http_client.post(f"/ops/campaigns/runs/{send_real_id}/recompute")
    assert resp.status_code == 200

    async with session_maker() as session:
        preview_first = await session.get(CampaignRun, preview_run_id)

    assert preview_first.meta.get("hidden") is True
    first_hidden_by = preview_first.meta.get("hidden_by_run_id")

    # Call _auto_hide_preview_run directly with a different run id
    from altegio_bot.campaigns.runner import _auto_hide_preview_run

    async with session_maker() as session:
        async with session.begin():
            await _auto_hide_preview_run(
                session,
                preview_run_id,
                send_real_run_id=9999,
            )

    async with session_maker() as session:
        preview_second = await session.get(CampaignRun, preview_run_id)

    assert preview_second.meta.get("hidden") is True
    assert preview_second.meta.get("hidden_by_run_id") == first_hidden_by, (
        "Idempotent call must not overwrite hidden_by_run_id. "
        f"Expected {first_hidden_by}, "
        f"got {preview_second.meta.get('hidden_by_run_id')!r}"
    )
