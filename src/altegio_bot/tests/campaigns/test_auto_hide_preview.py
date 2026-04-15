"""Tests: auto-hide preview run after a successful send-real.

After a clean send-real (status='completed', no last_error, no
partial_failure), the source preview run must be automatically hidden:
  - meta['hidden'] = True
  - meta['hidden_reason'] = 'auto_hidden_after_successful_send_real'
  - meta['hidden_by_run_id'] = send_real_run.id

Hidden preview:
  - absent from the default campaigns list (GET /ops/campaigns/runs)
  - still accessible via direct link (GET /ops/campaigns/runs/{id})
  - visible when include_hidden=true is passed to the list endpoint

Auto-hide must NOT trigger for:
  - failed send-real (status='failed', meta['last_error'] set)
  - partial failure (status='completed', meta['partial_failure']=True)

Coverage:
  1. clean send-real sets meta['hidden']=True on source preview
  2. hidden_reason and hidden_by_run_id recorded in preview meta
  3. hidden preview absent from default list
  4. hidden preview visible when include_hidden=true
  5. hidden preview accessible via direct GET /runs/{id}
  6. failed send-real does NOT hide preview
  7. partial failure does NOT hide preview
  8. auto-hide is idempotent (second run on same preview stays hidden)
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
from altegio_bot.models.models import CampaignRun
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
# 1. Clean send-real sets meta['hidden']=True on source preview
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_clean_send_real_hides_source_preview(runner_ok, session_maker) -> None:
    """After a clean send-real, meta['hidden']==True on the source preview."""
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

    assert preview is not None
    assert preview.meta is not None
    assert preview.meta.get("hidden") is True, (
        f'Expected meta["hidden"]=True after clean send-real, got meta={preview.meta!r}'
    )


# ---------------------------------------------------------------------------
# 2. hidden_reason and hidden_by_run_id recorded in preview meta
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_auto_hide_records_reason_and_run_id(runner_ok, session_maker) -> None:
    """meta must carry hidden_reason and hidden_by_run_id after auto-hide."""
    preview_run_id = await _create_preview_run(session_maker)
    candidate = _eligible_candidate()

    with patch.object(
        runner_module,
        "_load_candidates_from_preview_snapshot",
        new=AsyncMock(return_value=[candidate]),
    ):
        send_real = await run_send_real(_make_params(source_preview_run_id=preview_run_id))

    async with session_maker() as session:
        preview = await session.get(CampaignRun, preview_run_id)

    assert preview.meta.get("hidden_reason") == ("auto_hidden_after_successful_send_real"), (
        f"Unexpected hidden_reason: {preview.meta.get('hidden_reason')!r}"
    )
    assert preview.meta.get("hidden_by_run_id") == send_real.id, (
        f"Expected hidden_by_run_id={send_real.id}, got {preview.meta.get('hidden_by_run_id')!r}"
    )


# ---------------------------------------------------------------------------
# 3. Hidden preview absent from default campaigns list
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_hidden_preview_absent_from_default_list(http_client, session_maker, monkeypatch) -> None:
    """A hidden preview must not appear in GET /ops/campaigns/runs."""
    # Create a preview run with hidden=True in meta
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
    assert hidden_id not in ids, f"Hidden preview run_id={hidden_id} must not appear in default list, but got ids={ids}"


# ---------------------------------------------------------------------------
# 4. Hidden preview visible with include_hidden=true
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
                meta={"hidden": True, "hidden_reason": ("auto_hidden_after_successful_send_real")},
            )
            session.add(preview)
            await session.flush()
            hidden_id = preview.id

    response = await http_client.get("/ops/campaigns/runs?include_hidden=true")
    assert response.status_code == 200
    data = response.json()

    ids = [item["id"] for item in data["items"]]
    assert hidden_id in ids, f"Hidden preview run_id={hidden_id} must appear when include_hidden=true, but ids={ids}"


# ---------------------------------------------------------------------------
# 5. Hidden preview accessible via direct GET /runs/{id}
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
                meta={"hidden": True, "hidden_reason": ("auto_hidden_after_successful_send_real")},
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
# 6. Failed send-real does NOT hide preview
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_failed_send_real_does_not_hide_preview(runner_fail, session_maker) -> None:
    """When the send-real fails (status='failed'), preview must stay visible."""
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

    assert not (preview.meta or {}).get("hidden"), f"Failed send-real must NOT hide preview, but meta={preview.meta!r}"


# ---------------------------------------------------------------------------
# 7. Partial failure does NOT hide preview
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
    assert send_real.meta.get("partial_failure") is True, f"Expected partial_failure, got meta={send_real.meta!r}"

    async with session_maker() as session:
        preview = await session.get(CampaignRun, preview_run_id)

    assert not (preview.meta or {}).get("hidden"), f"Partial failure must NOT hide preview, but meta={preview.meta!r}"


# ---------------------------------------------------------------------------
# 8. Auto-hide is idempotent
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_auto_hide_is_idempotent(runner_ok, session_maker) -> None:
    """Running auto-hide on an already-hidden preview does not raise."""
    preview_run_id = await _create_preview_run(session_maker)
    candidate = _eligible_candidate()

    # First send-real: auto-hides preview
    with patch.object(
        runner_module,
        "_load_candidates_from_preview_snapshot",
        new=AsyncMock(return_value=[candidate]),
    ):
        first_run = await run_send_real(_make_params(source_preview_run_id=preview_run_id))

    assert first_run.status == "completed"

    async with session_maker() as session:
        preview_after_first = await session.get(CampaignRun, preview_run_id)

    assert preview_after_first.meta.get("hidden") is True

    # Directly call _auto_hide_preview_run again — must be idempotent
    from altegio_bot.campaigns.runner import _auto_hide_preview_run

    async with session_maker() as session:
        async with session.begin():
            await _auto_hide_preview_run(
                session,
                preview_run_id,
                send_real_run_id=9999,
            )

    # hidden_by_run_id must NOT be overwritten (idempotent: returns early)
    async with session_maker() as session:
        preview_after_second = await session.get(CampaignRun, preview_run_id)

    assert preview_after_second.meta.get("hidden") is True
    assert preview_after_second.meta.get("hidden_by_run_id") == first_run.id, (
        "Idempotent call must not overwrite hidden_by_run_id from the first successful run"
    )
