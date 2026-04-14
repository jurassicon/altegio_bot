"""Тесты: управление preview snapshot (delete / remove / add recipient).

Сценарии:
  1. delete_preview_run — success: status → 'deleted'.
  2. delete_preview — forbidden when referenced by send-real.
  3. remove_recipient — status='skipped', excluded_reason='manual_removed'.
  4. counters recompute after manual remove.
  5. add recipient — success (mocked CRM).
  6. duplicate add → 409.
  7. send-real from edited preview uses edited snapshot (manual_removed пропускается).
  8. manual_removed reason appears in /recipients JSON.
  9. deleted run исчезает из /runs по умолчанию, но виден при include_deleted=true.
 10. delete_preview_run — forbidden for running preview (race condition guard).
 11. remove_recipient — forbidden for running preview.
 12. add_recipient — forbidden for running preview (via HTTP).
 13. summary flags: running preview not marked editable/deletable/discardable.
 14. summary flags: used-as-source preview not marked editable/deletable/discardable.
 15. discard_preview_run — success: status → 'discarded'.
 16. discard_preview_run — forbidden for running preview (race condition guard).
 17. discard_preview_run — forbidden when referenced by send-real.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

import altegio_bot.campaigns.runner as runner_module
import altegio_bot.ops.campaigns_api as campaigns_api_module
import altegio_bot.ops.router as ops_router_module
from altegio_bot.campaigns.runner import (
    delete_preview_run,
    discard_preview_run,
    remove_recipient_from_preview,
)
from altegio_bot.main import app
from altegio_bot.models.models import CampaignRecipient, CampaignRun
from altegio_bot.ops.auth import require_ops_auth

NOW = datetime(2026, 4, 14, tzinfo=timezone.utc)
COMPANY_ID = 758285


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def http_client(session_maker, monkeypatch) -> AsyncClient:
    monkeypatch.setattr(ops_router_module, "SessionLocal", session_maker)
    monkeypatch.setattr(campaigns_api_module, "SessionLocal", session_maker)
    monkeypatch.setattr(runner_module, "SessionLocal", session_maker)
    monkeypatch.setitem(app.dependency_overrides, require_ops_auth, lambda: None)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        yield client


async def _make_preview_run(
    session_maker,
    *,
    status: str = "completed",
    company_ids: list | None = None,
) -> int:
    """Создать тестовый preview CampaignRun и вернуть его id."""
    async with session_maker() as session:
        async with session.begin():
            run = CampaignRun(
                campaign_code="new_clients_monthly",
                mode="preview",
                company_ids=company_ids or [COMPANY_ID],
                period_start=datetime(2026, 3, 1, tzinfo=timezone.utc),
                period_end=datetime(2026, 4, 1, tzinfo=timezone.utc),
                status=status,
                total_clients_seen=3,
                candidates_count=2,
                excluded_opted_out=1,
                meta={},
            )
            session.add(run)
            await session.flush()
            return run.id


async def _make_send_real_run(
    session_maker,
    *,
    source_preview_run_id: int,
    status: str = "completed",
) -> int:
    """Создать send-real run, ссылающийся на preview."""
    async with session_maker() as session:
        async with session.begin():
            run = CampaignRun(
                campaign_code="new_clients_monthly",
                mode="send-real",
                company_ids=[COMPANY_ID],
                period_start=datetime(2026, 3, 1, tzinfo=timezone.utc),
                period_end=datetime(2026, 4, 1, tzinfo=timezone.utc),
                status=status,
                source_preview_run_id=source_preview_run_id,
                meta={},
            )
            session.add(run)
            await session.flush()
            return run.id


async def _make_recipient(
    session_maker,
    *,
    run_id: int,
    phone: str = "+4915100000001",
    excluded_reason: str | None = None,
    status: str | None = None,
    client_id: int | None = None,
) -> int:
    """Создать CampaignRecipient и вернуть его id."""
    rec_status = status or ("skipped" if excluded_reason else "candidate")
    async with session_maker() as session:
        async with session.begin():
            r = CampaignRecipient(
                campaign_run_id=run_id,
                company_id=COMPANY_ID,
                client_id=client_id,
                phone_e164=phone,
                display_name="Test User",
                status=rec_status,
                excluded_reason=excluded_reason,
                total_records_in_period=1,
                confirmed_records_in_period=1,
                lash_records_in_period=1,
                confirmed_lash_records_in_period=1,
                records_before_period=0,
                records_after_period=0,
                service_titles_in_period=["Lash"],
                is_opted_out=False,
                local_client_found=True,
            )
            session.add(r)
            await session.flush()
            return r.id


# ---------------------------------------------------------------------------
# 1. delete_preview_run — success
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_delete_preview_run_success(session_maker, monkeypatch) -> None:
    """delete_preview_run переводит status в 'deleted' и записывает deleted_at в meta."""
    monkeypatch.setattr(runner_module, "SessionLocal", session_maker)

    run_id = await _make_preview_run(session_maker, status="completed")

    await delete_preview_run(run_id)

    async with session_maker() as session:
        run = await session.get(CampaignRun, run_id)

    assert run is not None
    assert run.status == "deleted"
    assert "deleted_at" in (run.meta or {})


# ---------------------------------------------------------------------------
# 2. delete_preview — forbidden when referenced by send-real
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_delete_preview_forbidden_when_referenced(session_maker, monkeypatch) -> None:
    """delete_preview_run запрещено, если preview используется как source_preview_run_id."""
    monkeypatch.setattr(runner_module, "SessionLocal", session_maker)

    preview_id = await _make_preview_run(session_maker, status="completed")
    await _make_send_real_run(session_maker, source_preview_run_id=preview_id)

    with pytest.raises(ValueError, match="send-real"):
        await delete_preview_run(preview_id)

    # Статус не изменился
    async with session_maker() as session:
        run = await session.get(CampaignRun, preview_id)
    assert run.status == "completed"


# ---------------------------------------------------------------------------
# 3. remove_recipient — soft-exclude
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_remove_recipient_from_preview(session_maker, monkeypatch) -> None:
    """remove_recipient_from_preview → status='skipped', excluded_reason='manual_removed'."""
    monkeypatch.setattr(runner_module, "SessionLocal", session_maker)

    run_id = await _make_preview_run(session_maker, status="completed")
    recipient_id = await _make_recipient(session_maker, run_id=run_id, status="candidate")

    r = await remove_recipient_from_preview(run_id, recipient_id)

    assert r.status == "skipped"
    assert r.excluded_reason == "manual_removed"
    assert "manually_removed_at" in (r.meta or {})


# ---------------------------------------------------------------------------
# 4. counters recompute after manual remove
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_counters_recompute_after_manual_remove(session_maker, monkeypatch) -> None:
    """После manual remove counters run пересчитываются корректно."""
    monkeypatch.setattr(runner_module, "SessionLocal", session_maker)

    # Run с 3 получателями: 2 eligible, 1 opted_out
    run_id = await _make_preview_run(
        session_maker,
        status="completed",
    )
    # Установить точные счётчики вручную
    async with session_maker() as session:
        async with session.begin():
            run = await session.get(CampaignRun, run_id)
            run.total_clients_seen = 3
            run.candidates_count = 2
            run.excluded_opted_out = 1

    r1 = await _make_recipient(session_maker, run_id=run_id, phone="+491001", status="candidate")
    await _make_recipient(session_maker, run_id=run_id, phone="+491002", status="candidate")
    await _make_recipient(
        session_maker,
        run_id=run_id,
        phone="+491003",
        excluded_reason="opted_out",
    )

    # Убрать первого eligible
    await remove_recipient_from_preview(run_id, r1)

    async with session_maker() as session:
        run = await session.get(CampaignRun, run_id)

    # total_clients_seen не уменьшается (manual_removed не удаляет запись)
    assert run.total_clients_seen == 3
    # candidates_count уменьшился на 1
    assert run.candidates_count == 1
    # opted_out counter сохранился
    assert run.excluded_opted_out == 1


# ---------------------------------------------------------------------------
# 5. add recipient — success (mocked CRM)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_add_recipient_success(http_client: AsyncClient, session_maker) -> None:
    """POST /runs/{id}/recipients/add добавляет eligible клиента в snapshot."""
    # session_maker уже проброшен через http_client fixture (monkeypatch)
    run_id = await _make_preview_run(session_maker, status="completed")

    # Mock: CRM возвращает один eligible клиент
    with (
        patch(
            "altegio_bot.ops.campaigns_api.get_client_crm_records",
            new_callable=AsyncMock,
        ) as mock_crm,
        patch(
            "altegio_bot.ops.campaigns_api.classify_crm_records",
        ) as mock_classify,
        patch(
            "altegio_bot.ops.campaigns_api.check_lash_services",
            new_callable=AsyncMock,
        ) as mock_lash,
    ):
        from altegio_bot.campaigns.altegio_crm import CrmRecord

        rec = CrmRecord(
            crm_id=1001,
            starts_at=datetime(2026, 3, 15, tzinfo=timezone.utc),
            confirmed=1,
            deleted=False,
            attendance=1,
            service_ids=[99],
            service_titles=["Lashes Classic"],
        )
        mock_crm.return_value = [rec]
        mock_classify.return_value = ([rec], 0, 0)
        mock_lash.return_value = (1, 1, frozenset([99]), frozenset([99]))

        resp = await http_client.post(
            f"/ops/campaigns/runs/{run_id}/recipients/add",
            json={"phone": "+491728079002", "altegio_client_id": 9999},
        )

    assert resp.status_code == 201, resp.text
    data = resp.json()
    assert data["recipient"]["phone_e164"] == "+491728079002"
    assert data["recipient"]["status"] == "candidate"
    assert data["recipient"]["excluded_reason"] is None

    # Проверить, что recompute обновил candidates_count
    async with session_maker() as session:
        run = await session.get(CampaignRun, run_id)
    assert run.candidates_count == 1


# ---------------------------------------------------------------------------
# 6. duplicate add → 409
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_duplicate_add_rejected(http_client: AsyncClient, session_maker) -> None:
    """Повторное добавление того же phone → 409 Conflict."""
    run_id = await _make_preview_run(session_maker, status="completed")
    await _make_recipient(session_maker, run_id=run_id, phone="+491728079002", status="candidate")

    with (
        patch("altegio_bot.ops.campaigns_api.get_client_crm_records", new_callable=AsyncMock),
        patch("altegio_bot.ops.campaigns_api.classify_crm_records", return_value=([], 0, 0)),
        patch("altegio_bot.ops.campaigns_api.check_lash_services", new_callable=AsyncMock),
    ):
        resp = await http_client.post(
            f"/ops/campaigns/runs/{run_id}/recipients/add",
            json={"phone": "+491728079002", "altegio_client_id": 9999},
        )

    assert resp.status_code == 409
    assert "+491728079002" in resp.text


# ---------------------------------------------------------------------------
# 7. send-real from edited preview: manual_removed recipient skipped
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_send_real_snapshot_skips_manual_removed(session_maker, monkeypatch) -> None:
    """Snapshot, загруженный для send-real, не включает manual_removed получателей в eligible."""
    monkeypatch.setattr(runner_module, "SessionLocal", session_maker)

    run_id = await _make_preview_run(session_maker, status="completed")

    # 2 eligible + 1 manual_removed
    await _make_recipient(session_maker, run_id=run_id, phone="+491001", status="candidate")
    await _make_recipient(session_maker, run_id=run_id, phone="+491002", status="candidate")
    manual_id = await _make_recipient(session_maker, run_id=run_id, phone="+491003", status="candidate")
    await remove_recipient_from_preview(run_id, manual_id)

    # Загрузить snapshot через _load_candidates_from_preview_snapshot
    async with session_maker() as session:
        candidates = await runner_module._load_candidates_from_preview_snapshot(session, run_id)

    eligible = [c for c in candidates if c.is_eligible]
    excluded = [c for c in candidates if not c.is_eligible]

    assert len(eligible) == 2, f"expected 2 eligible, got {len(eligible)}"
    assert len(excluded) == 1
    assert excluded[0].excluded_reason == "manual_removed"
    # Phone manual_removed клиента не в eligible
    eligible_phones = {c.client.phone_e164 for c in eligible}
    assert "+491003" not in eligible_phones


# ---------------------------------------------------------------------------
# 8. manual_removed reason in /recipients JSON
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_manual_removed_in_recipients_json(http_client: AsyncClient, session_maker) -> None:
    """GET /runs/{id}/recipients возвращает excluded_reason='manual_removed' для убранных."""
    # monkeypatch уже установлен через http_client fixture (SessionLocal пробрасывается там)
    run_id = await _make_preview_run(session_maker, status="completed")
    r_id = await _make_recipient(session_maker, run_id=run_id, phone="+491999", status="candidate")

    # Soft-remove напрямую через API endpoint
    resp = await http_client.post(f"/ops/campaigns/runs/{run_id}/recipients/{r_id}/remove")
    assert resp.status_code == 200

    # Проверить в /recipients JSON
    list_resp = await http_client.get(f"/ops/campaigns/runs/{run_id}/recipients")
    assert list_resp.status_code == 200
    items = list_resp.json()["items"]
    matched = [i for i in items if i["id"] == r_id]
    assert matched, "recipient should still appear in list"
    assert matched[0]["excluded_reason"] == "manual_removed"
    assert matched[0]["status"] == "skipped"


# ---------------------------------------------------------------------------
# 9. deleted run invisible from /runs (visible with include_deleted=true)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_deleted_run_hidden_from_list(http_client: AsyncClient, session_maker, monkeypatch) -> None:
    """Soft-deleted run не виден в /runs по умолчанию, но виден при include_deleted=true."""
    monkeypatch.setattr(runner_module, "SessionLocal", session_maker)

    run_id = await _make_preview_run(session_maker, status="completed")
    await delete_preview_run(run_id)

    # Default: deleted runs hidden
    resp = await http_client.get("/ops/campaigns/runs")
    assert resp.status_code == 200
    ids_default = [r["id"] for r in resp.json()["items"]]
    assert run_id not in ids_default, "deleted run must not appear in default list"

    # With include_deleted=true: visible
    resp2 = await http_client.get("/ops/campaigns/runs?include_deleted=true")
    assert resp2.status_code == 200
    ids_with_deleted = [r["id"] for r in resp2.json()["items"]]
    assert run_id in ids_with_deleted, "deleted run must appear when include_deleted=true"


# ---------------------------------------------------------------------------
# 10. delete_preview_run forbidden for running preview (race condition guard)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_delete_preview_forbidden_for_running(session_maker, monkeypatch) -> None:
    """delete_preview_run запрещено для running preview — race condition с run_preview()."""
    monkeypatch.setattr(runner_module, "SessionLocal", session_maker)

    run_id = await _make_preview_run(session_maker, status="running")

    with pytest.raises(ValueError, match="completed"):
        await delete_preview_run(run_id)

    async with session_maker() as session:
        run = await session.get(CampaignRun, run_id)
    assert run.status == "running", "status must remain 'running'"


# ---------------------------------------------------------------------------
# 11. remove_recipient forbidden for running preview
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_remove_recipient_forbidden_for_running(session_maker, monkeypatch) -> None:
    """remove_recipient_from_preview запрещено для running preview."""
    monkeypatch.setattr(runner_module, "SessionLocal", session_maker)

    run_id = await _make_preview_run(session_maker, status="running")
    recipient_id = await _make_recipient(session_maker, run_id=run_id, status="candidate")

    with pytest.raises(ValueError, match="completed"):
        await remove_recipient_from_preview(run_id, recipient_id)

    async with session_maker() as session:
        r = await session.get(CampaignRecipient, recipient_id)
    assert r.status == "candidate", "recipient status must not change"


# ---------------------------------------------------------------------------
# 12. add_recipient forbidden for running preview (via HTTP 400)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_add_recipient_forbidden_for_running(http_client: AsyncClient, session_maker) -> None:
    """POST /runs/{id}/recipients/add → 400 для running preview."""
    run_id = await _make_preview_run(session_maker, status="running")

    resp = await http_client.post(
        f"/ops/campaigns/runs/{run_id}/recipients/add",
        json={"phone": "+491234567890", "altegio_client_id": 9999},
    )

    assert resp.status_code == 400, resp.text
    assert "completed" in resp.text.lower()


# ---------------------------------------------------------------------------
# 13. summary flags: running preview NOT marked editable/deletable/discardable
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_summary_flags_running_not_editable(http_client: AsyncClient, session_maker) -> None:
    """GET /runs/{id} → все action-флаги False для running preview."""
    run_id = await _make_preview_run(session_maker, status="running")

    resp = await http_client.get(f"/ops/campaigns/runs/{run_id}")
    assert resp.status_code == 200, resp.text
    data = resp.json()

    assert data["is_snapshot_editable"] is False, "running preview must not be marked editable"
    assert data["is_deletable"] is False, "running preview must not be marked deletable"
    # discard during running is a race condition — must also be False
    assert data["is_discardable"] is False, "running preview must not be marked discardable"


# ---------------------------------------------------------------------------
# 14. summary flags: used-as-source preview NOT marked editable/deletable/discardable
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_summary_flags_used_source_not_editable(http_client: AsyncClient, session_maker) -> None:
    """GET /runs/{id} → все action-флаги False если уже used_as_source."""
    preview_id = await _make_preview_run(session_maker, status="completed")
    await _make_send_real_run(session_maker, source_preview_run_id=preview_id)

    resp = await http_client.get(f"/ops/campaigns/runs/{preview_id}")
    assert resp.status_code == 200, resp.text
    data = resp.json()

    assert data["is_snapshot_editable"] is False, "used-as-source preview must not be editable"
    assert data["is_deletable"] is False, "used-as-source preview must not be deletable"
    assert data["is_discardable"] is False, "used-as-source preview must not be discardable"


# ---------------------------------------------------------------------------
# 15. discard_preview_run — success
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_discard_preview_run_success(session_maker, monkeypatch) -> None:
    """discard_preview_run переводит status в 'discarded' и записывает discarded_at в meta."""
    monkeypatch.setattr(runner_module, "SessionLocal", session_maker)

    run_id = await _make_preview_run(session_maker, status="completed")

    await discard_preview_run(run_id)

    async with session_maker() as session:
        run = await session.get(CampaignRun, run_id)

    assert run is not None
    assert run.status == "discarded"
    assert "discarded_at" in (run.meta or {})


# ---------------------------------------------------------------------------
# 16. discard_preview_run — forbidden for running preview (race condition guard)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_discard_preview_forbidden_for_running(session_maker, monkeypatch) -> None:
    """discard_preview_run запрещено для running preview — race condition с run_preview()."""
    monkeypatch.setattr(runner_module, "SessionLocal", session_maker)

    run_id = await _make_preview_run(session_maker, status="running")

    with pytest.raises(ValueError, match="completed"):
        await discard_preview_run(run_id)

    async with session_maker() as session:
        run = await session.get(CampaignRun, run_id)
    assert run.status == "running", "status must remain 'running'"


# ---------------------------------------------------------------------------
# 17. discard_preview_run — forbidden when referenced by send-real
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_discard_preview_forbidden_when_referenced(session_maker, monkeypatch) -> None:
    """discard_preview_run запрещено, если preview используется как source_preview_run_id."""
    monkeypatch.setattr(runner_module, "SessionLocal", session_maker)

    preview_id = await _make_preview_run(session_maker, status="completed")
    await _make_send_real_run(session_maker, source_preview_run_id=preview_id)

    with pytest.raises(ValueError, match="send-real"):
        await discard_preview_run(preview_id)

    async with session_maker() as session:
        run = await session.get(CampaignRun, preview_id)
    assert run.status == "completed"
