"""§38.9: the boundary between an APPROVED backlog and ordinary new traffic.

Steps 55-57 used to rest on an operator comparing two printed lists. Between
the approval and the bulk opening the EasyWeek inbox worker keeps planning
pair jobs, and a job that appeared in that window would be released by the
bulk fence without ever having been approved — while looking exactly like the
ones that were.

Two independent controls close it, and both are proven here:

* the producer is paused, which is only safe because capture does not depend
  on it — the HTTP endpoint writes and commits its own ``easyweek_events`` row
  and the worker is a separate consumer of ``captured`` rows;
* the final audit is bound to the approved ``release_set_digest``, so a set
  that changed anyway fails closed instead of being opened.

Everything here runs against a real database. Nothing sends.
"""

from __future__ import annotations

import json
import uuid
from contextlib import ExitStack
from datetime import timedelta
from decimal import Decimal
from typing import Any
from unittest.mock import patch

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select

import altegio_bot.db as app_db
import altegio_bot.webhooks.easyweek as ew_webhook
from altegio_bot.easyweek_multi_service import (
    WebhookServicePair,
    clear_multi_service_catalog_cache,
    multi_service_job_payload,
    prove_exactly_two_service_snapshot,
    record_raw_with_multi_service_snapshot,
)
from altegio_bot.easyweek_multi_service_rollout import MULTI_SERVICE_RELEASE_SET_CHANGED
from altegio_bot.easyweek_service_category import record_raw_with_services_count
from altegio_bot.main import app
from altegio_bot.models.models import PROVIDER_ALTEGIO, PROVIDER_EASYWEEK, Client, EasyWeekEvent, MessageJob, Record
from altegio_bot.scripts.easyweek_multi_service_release_audit import run_release_audit
from altegio_bot.settings import settings
from altegio_bot.tests.easyweek_fixtures import TEST_BOOKING_UUID, TEST_LOCATION_ID, TEST_LOCATION_UUID
from altegio_bot.utils import utcnow
from altegio_bot.workers import easyweek_inbox_worker as inbox

pytestmark = pytest.mark.asyncio

SECRET = "release-boundary-secret"
URL = "/webhooks/easyweek"
_BRANCH_SLUG = "release-boundary-branch"
_PREFIX = "rb"
_TOTAL = Decimal("80.00")
_ALLOWED_CATEGORY = "Fixture Category"


@pytest.fixture(autouse=True)
def _configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    from altegio_bot.easyweek_branches import BRANCH_PROFILES, BranchProfile

    monkeypatch.setattr(
        settings,
        "easyweek_location_map",
        json.dumps(
            {
                _BRANCH_SLUG: {
                    "location_id": TEST_LOCATION_ID,
                    "location_uuid": TEST_LOCATION_UUID,
                    "meta_template_prefix": _PREFIX,
                    "booking_page_url": "https://booking.example.invalid/test",
                }
            }
        ),
        raising=False,
    )
    monkeypatch.setitem(
        BRANCH_PROFILES,
        _BRANCH_SLUG,
        BranchProfile(
            slug=_BRANCH_SLUG,
            api_name="Release boundary fixture",
            meta_template_prefix=_PREFIX,
            content=BRANCH_PROFILES["durlach"].content,
        ),
    )
    monkeypatch.setattr(settings, "easyweek_allowed_service_categories", json.dumps([_ALLOWED_CATEGORY]), raising=False)
    monkeypatch.setattr(settings, "easyweek_notifications_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_reminders_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_reminder_api_guard_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_multi_service_notifications_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_resource_shadow_proof_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_multi_service_send_enabled", False, raising=False)
    monkeypatch.setattr(settings, "easyweek_multi_service_canary_job_id", "", raising=False)
    clear_multi_service_catalog_cache()


# ---------------------------------------------------------------------------
# Capture, with the consumer stopped
# ---------------------------------------------------------------------------


def _capture_env(session_maker) -> ExitStack:
    stack = ExitStack()
    stack.enter_context(patch.object(settings, "easyweek_enabled", True))
    stack.enter_context(patch.object(settings, "easyweek_webhook_secret", SECRET))
    stack.enter_context(patch.object(ew_webhook, "SessionLocal", session_maker))
    return stack


def _delivery(index: int) -> dict[str, Any]:
    return {
        "id": 1811630 + index,
        "uid": str(uuid.UUID(int=index + 1)),
        "customer_phone": "+4915000000000",
        "booking_date_start": "2026-11-10T14:00:00+0000",
    }


async def test_deliveries_are_captured_while_the_inbox_worker_is_not_running(session_maker) -> None:
    """The whole basis for pausing the producer at step 56a.

    No worker runs anywhere in this test. Every delivery still lands as a
    durable ``captured`` row, and only after that does the endpoint answer
    200 — so EasyWeek never sees a delivery the database did not keep.
    """
    with _capture_env(session_maker):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            for index in range(3):
                response = await client.post(
                    f"{URL}?event=booking-created&token={SECRET}",
                    content=json.dumps(_delivery(index)),
                    headers={"Content-Type": "application/json"},
                )
                assert response.status_code == 200
                assert response.json() == {"ok": True}

    async with session_maker() as session:
        rows = list((await session.execute(select(EasyWeekEvent).order_by(EasyWeekEvent.id))).scalars())
    assert len(rows) == 3
    assert [row.status for row in rows] == ["captured"] * 3


async def test_the_paused_backlog_is_consumed_in_capture_order_once_the_worker_returns(
    session_maker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Step 58a: no catch-up mode, no bulk replay — the ordinary queue resumes."""
    with _capture_env(session_maker):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            for index in range(3):
                await client.post(
                    f"{URL}?event=booking-created&token={SECRET}",
                    content=json.dumps(_delivery(index)),
                    headers={"Content-Type": "application/json"},
                )

    monkeypatch.setattr(app_db, "SessionLocal", session_maker, raising=False)
    monkeypatch.setattr(inbox, "SessionLocal", session_maker, raising=False)
    monkeypatch.setattr(settings, "easyweek_processing_enabled", True, raising=False)

    claimed: list[int] = []
    async with session_maker() as session:
        async with session.begin():
            first = await inbox.claim_next_event(session)
            assert first is not None
            claimed.append(first.id)
    async with session_maker() as session:
        async with session.begin():
            second = await inbox.claim_next_event(session)
            assert second is not None
            claimed.append(second.id)

    assert claimed == sorted(claimed), "the paused backlog is drained oldest-first"


async def test_the_capture_endpoint_does_not_depend_on_the_inbox_worker() -> None:
    """Source-level: capture owns its own session, commit and durability."""
    import inspect

    source = inspect.getsource(ew_webhook)
    assert "easyweek_inbox_worker" not in source
    assert "session.commit()" in source
    assert "EasyWeekEvent(" in source
    # The endpoint answers 200 only after a successful commit, and 503 when the
    # row could not be written — that is what makes a paused consumer lossless.
    assert "status_code=503" in source


# ---------------------------------------------------------------------------
# The approved release set, and the digest that binds it
# ---------------------------------------------------------------------------


def _line(line_uuid: str, name: str, price: int, duration: int) -> dict[str, Any]:
    return {
        "uuid": line_uuid,
        "name": name,
        "currency": "EUR",
        "price": price,
        "original_price": price,
        "discount": 0,
        "quantity": 1,
        "duration": {"value": duration, "label": "minutes"},
        "original_duration": {"value": duration, "label": "minutes"},
    }


def _booking() -> dict[str, Any]:
    return {
        "uuid": TEST_BOOKING_UUID,
        "location_uuid": TEST_LOCATION_UUID,
        "currency": "EUR",
        "order": {"subtotal": 8000, "total": 8000},
        "ordered_services": [
            _line("11111111-1111-4111-8111-111111111111", "Fixture Service", 3500, 30),
            _line("22222222-2222-4222-8222-222222222222", "Second Fixture Service", 4500, 45),
        ],
    }


def _catalog() -> list[dict[str, Any]]:
    return [
        {
            "uuid": "aaaaaaaa-1111-4111-8111-111111111111",
            "name": "Fixture Service",
            "currency": "EUR",
            "price": 3500,
            "duration": {"value": 30, "label": "minutes"},
            "category": {"name": _ALLOWED_CATEGORY},
        },
        {
            "uuid": "aaaaaaaa-2222-4222-8222-222222222222",
            "name": "Second Fixture Service",
            "currency": "EUR",
            "price": 4500,
            "duration": {"value": 45, "label": "minutes"},
            "category": {"name": _ALLOWED_CATEGORY},
        },
    ]


def _snapshot():
    return prove_exactly_two_service_snapshot(
        webhook=WebhookServicePair(
            booking_uuid=uuid.UUID(TEST_BOOKING_UUID),
            location_uuid=TEST_LOCATION_UUID,
            service_name="Fixture Service",
            service_related="Second Fixture Service",
            services_description="Fixture Service, Second Fixture Service",
            services_count=2,
            quantity=2,
            booking_currency="EUR",
            total_cost=_TOTAL,
        ),
        booking_payload=_booking(),
        catalog_rows=_catalog(),
    )


async def _seed_approved_backlog(session) -> tuple[Client, Record, Any, MessageJob]:
    client = Client(
        provider=PROVIDER_EASYWEEK,
        company_id=TEST_LOCATION_ID,
        altegio_client_id=7300002,
        display_name="Release boundary fixture",
        phone_e164="+49000000000",
        raw={},
    )
    session.add(client)
    await session.flush()

    snapshot = _snapshot()
    record = Record(
        provider=PROVIDER_EASYWEEK,
        company_id=TEST_LOCATION_ID,
        altegio_record_id=4200001,
        easyweek_booking_uuid=uuid.UUID(TEST_BOOKING_UUID),
        client_id=client.id,
        starts_at=utcnow() + timedelta(days=3),
        total_cost=_TOTAL,
        is_deleted=False,
        raw=record_raw_with_multi_service_snapshot(record_raw_with_services_count({}, 2), snapshot),
    )
    session.add(record)
    await session.flush()

    approved = MessageJob(
        provider=PROVIDER_EASYWEEK,
        company_id=TEST_LOCATION_ID,
        record_id=record.id,
        client_id=client.id,
        job_type="record_created",
        run_at=utcnow() - timedelta(minutes=1),
        status="queued",
        dedupe_key="release-boundary-approved",
        payload=multi_service_job_payload(snapshot),
    )
    session.add(approved)
    await session.flush()
    return client, record, snapshot, approved


async def _plan_one_more_pair_job(session, client: Client, record: Record, snapshot: Any) -> MessageJob:
    """What the inbox worker would do if it were still running."""
    job = MessageJob(
        provider=PROVIDER_EASYWEEK,
        company_id=TEST_LOCATION_ID,
        record_id=record.id,
        client_id=client.id,
        job_type="record_updated",
        run_at=utcnow() - timedelta(minutes=1),
        status="queued",
        dedupe_key="release-boundary-late-arrival",
        payload=multi_service_job_payload(snapshot),
    )
    session.add(job)
    await session.flush()
    return job


async def test_the_digest_is_stable_across_reruns_of_an_unchanged_queue(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed_approved_backlog(session)

    async with session_maker() as session:
        first = await run_release_audit(session)
    async with session_maker() as session:
        second = await run_release_audit(session)

    assert first.release_set_digest == second.release_set_digest
    assert len(first.release_set_digest) == 64


async def test_a_job_planned_after_the_approval_cannot_enter_the_approved_release(session_maker) -> None:
    """THE TOCTOU property, stated as a machine check rather than an eyeball."""
    async with session_maker() as session:
        async with session.begin():
            client, record, snapshot, approved = await _seed_approved_backlog(session)

    async with session_maker() as session:
        approved_report = await run_release_audit(session)
    approved_digest = approved_report.release_set_digest
    assert approved_report.bulk_ready is True
    assert approved_report.provider_candidate_job_ids == [approved.id]

    # The producer was still running: one more pair job lands, unapproved.
    async with session_maker() as session:
        async with session.begin():
            late = await _plan_one_more_pair_job(session, client, record, snapshot)

    async with session_maker() as session:
        final = await run_release_audit(session, expected_release_digest=approved_digest)

    assert final.release_digest_error == MULTI_SERVICE_RELEASE_SET_CHANGED
    assert final.reasons[MULTI_SERVICE_RELEASE_SET_CHANGED] == 1
    assert final.bulk_ready is False, "the bulk opening cannot proceed on a set nobody approved"
    assert late.id in final.provider_candidate_job_ids
    assert final.release_set_digest != approved_digest


async def test_the_unchanged_approved_set_passes_its_digest_check(session_maker) -> None:
    """Positive control: the binding must not block a rollout that is correct."""
    async with session_maker() as session:
        async with session.begin():
            await _seed_approved_backlog(session)

    async with session_maker() as session:
        approved = await run_release_audit(session)

    async with session_maker() as session:
        final = await run_release_audit(session, expected_release_digest=approved.release_set_digest)

    assert final.release_digest_error is None
    assert final.bulk_ready is True


async def test_other_providers_and_job_families_never_enter_the_digest(session_maker) -> None:
    """Altegio, single-service, review, retention, campaign and voucher rows."""
    async with session_maker() as session:
        async with session.begin():
            client, record, snapshot, _approved = await _seed_approved_backlog(session)
            baseline_payload = multi_service_job_payload(snapshot)
            for index, (provider, job_type, payload) in enumerate(
                (
                    (PROVIDER_ALTEGIO, "record_created", baseline_payload),
                    (PROVIDER_EASYWEEK, "record_updated", {}),
                    (PROVIDER_EASYWEEK, "review_3d", baseline_payload),
                    (PROVIDER_EASYWEEK, "repeat_10d", baseline_payload),
                    (PROVIDER_EASYWEEK, "comeback_3d", baseline_payload),
                    (PROVIDER_ALTEGIO, "followup_14d", baseline_payload),
                )
            ):
                session.add(
                    MessageJob(
                        provider=provider,
                        company_id=TEST_LOCATION_ID,
                        record_id=record.id,
                        client_id=client.id,
                        job_type=job_type,
                        run_at=utcnow() - timedelta(minutes=1),
                        status="queued",
                        dedupe_key=f"release-boundary-other-{index}",
                        payload=dict(payload),
                    )
                )

    async with session_maker() as session:
        report = await run_release_audit(session)

    assert report.open_pair_jobs == 1, "only the EasyWeek pair job is in the release set"
    assert len(report.provider_candidate_job_ids) == 1


async def test_the_digest_check_never_writes_and_never_names_a_customer(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed_approved_backlog(session)

    async with session_maker() as session:
        report = await run_release_audit(session, expected_release_digest="0" * 64)
        assert not session.dirty
        assert not session.new
        assert not session.deleted

    text = str(report.as_safe_dict())
    for forbidden in (TEST_BOOKING_UUID, TEST_LOCATION_UUID, "+49000000000", "Fixture Service", "80.00"):
        assert forbidden not in text, forbidden
    assert report.release_digest_error == MULTI_SERVICE_RELEASE_SET_CHANGED
