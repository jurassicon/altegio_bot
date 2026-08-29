"""PR-11.1: the fence between a plan and the first mutation.

Two things are proven here, and they are the two that matter most:

1. **No path reaches EasyWeek without the whole gate.** Every single missing
   precondition is tested on its own, because a gate that passes when one of six
   conditions is met is not a gate.
2. **The mutation client's uncertain contract.** A timeout is never retried, a
   429 is, and a permanent 4xx is not.
"""

from __future__ import annotations

import httpx
import pytest

from altegio_bot.easyweek_client import (
    EasyWeekAuthError,
    EasyWeekPermanentError,
    EasyWeekRetryableError,
)
from altegio_bot.easyweek_migration.customers import CustomerDirectory
from altegio_bot.easyweek_migration.gates import (
    GATE_APPLY_FLAG_MISSING,
    GATE_CANARY_NOTIFICATION_OBSERVED,
    GATE_CAPTURE_DISABLED,
    GATE_CUSTOMER_DIRECTORY_INVALID,
    GATE_CUTOVER_MISSING,
    GATE_DRY_RUN_ID_MISMATCH,
    GATE_DRY_RUN_ID_MISSING,
    GATE_MANIFEST_INVALID,
    GATE_NATIVE_NOTIFICATIONS_UNCONFIRMED,
    GATE_NOTIFICATIONS_ENABLED,
    GATE_PROCESSING_DISABLED,
    GATE_REVIEWS_ENABLED,
    ApplyGateError,
    EffectiveBotSettings,
    evaluate_apply_gate,
    read_effective_settings,
    require_apply_gate,
)
from altegio_bot.easyweek_migration.manifest import parse_manifest
from altegio_bot.easyweek_migration.write_client import (
    DEFAULT_REQUESTS_PER_MINUTE,
    EasyWeekMigrationWriteClient,
    EasyWeekUncertainMutation,
    RateLimiter,
    build_booking_request,
)
from altegio_bot.settings import settings
from altegio_bot.tests.test_easyweek_migration_planning import manifest_text

DIGEST = "a" * 64
BOOKING_UUID = "99999999-9999-4999-8999-999999999999"


def production_settings(**overrides) -> EffectiveBotSettings:
    """The state the runbook puts production into before a cutover apply."""
    base = {
        "easyweek_enabled": True,
        "easyweek_processing_enabled": True,
        "easyweek_notifications_enabled": False,
        "easyweek_reviews_enabled": False,
        "easyweek_review_send_enabled": False,
        "easyweek_reminders_enabled": False,
        "easyweek_visit_counter_enabled": True,
    }
    base.update(overrides)
    return EffectiveBotSettings(**base)


@pytest.fixture
def manifest():
    return parse_manifest(manifest_text())


@pytest.fixture
def directory():
    return CustomerDirectory(valid=True, by_phone={"+4915112345678": [BOOKING_UUID]})


def gate(*, manifest, directory, effective=None, **overrides):
    kwargs = {
        "apply_requested": True,
        "native_notifications_confirmed": True,
        "cutover_supplied": True,
        "verified_dry_run_id": DIGEST,
        "computed_plan_digest": DIGEST,
        "manifest": manifest,
        "directory": directory,
        "canary_notification_observed": False,
        "effective": effective or production_settings(),
    }
    kwargs.update(overrides)
    return evaluate_apply_gate(**kwargs)


# ---------------------------------------------------------------------------
# The gate
# ---------------------------------------------------------------------------


def test_the_full_production_state_passes(manifest, directory):
    result = gate(manifest=manifest, directory=directory)
    assert result.passed
    assert result.failures == []
    require_apply_gate(result)  # does not raise


def test_apply_without_the_apply_flag_is_refused(manifest, directory):
    result = gate(manifest=manifest, directory=directory, apply_requested=False)
    assert not result.passed
    assert GATE_APPLY_FLAG_MISSING in result.failures


def test_apply_is_blocked_when_bot_notifications_are_enabled(manifest, directory):
    result = gate(
        manifest=manifest,
        directory=directory,
        effective=production_settings(easyweek_notifications_enabled=True),
    )
    assert not result.passed
    assert GATE_NOTIFICATIONS_ENABLED in result.failures
    with pytest.raises(ApplyGateError):
        require_apply_gate(result)


def test_apply_is_blocked_when_reviews_are_enabled(manifest, directory):
    result = gate(
        manifest=manifest,
        directory=directory,
        effective=production_settings(easyweek_reviews_enabled=True),
    )
    assert not result.passed
    assert GATE_REVIEWS_ENABLED in result.failures


def test_apply_is_blocked_without_the_native_notification_attestation(manifest, directory):
    """The one condition no code can verify — so it must be stated explicitly."""
    result = gate(manifest=manifest, directory=directory, native_notifications_confirmed=False)
    assert not result.passed
    assert GATE_NATIVE_NOTIFICATIONS_UNCONFIRMED in result.failures


def test_processing_and_webhook_capture_must_stay_ON(manifest, directory):
    """Turning capture off would lose the migration's own events forever (§1.3)."""
    no_processing = gate(
        manifest=manifest, directory=directory, effective=production_settings(easyweek_processing_enabled=False)
    )
    no_capture = gate(manifest=manifest, directory=directory, effective=production_settings(easyweek_enabled=False))
    assert GATE_PROCESSING_DISABLED in no_processing.failures
    assert GATE_CAPTURE_DISABLED in no_capture.failures


def test_the_visit_counter_may_stay_on(manifest, directory):
    """It sends nothing; it records a fact EasyWeek already stated."""
    result = gate(
        manifest=manifest, directory=directory, effective=production_settings(easyweek_visit_counter_enabled=True)
    )
    assert result.passed


def test_apply_is_blocked_without_a_verified_dry_run_id(manifest, directory):
    result = gate(manifest=manifest, directory=directory, verified_dry_run_id=None)
    assert GATE_DRY_RUN_ID_MISSING in result.failures


def test_a_plan_that_changed_since_the_review_is_refused(manifest, directory):
    """New or cancelled source bookings move the digest — re-review, do not proceed."""
    result = gate(manifest=manifest, directory=directory, computed_plan_digest="b" * 64)
    assert GATE_DRY_RUN_ID_MISMATCH in result.failures


def test_apply_is_blocked_without_a_cutover(manifest, directory):
    result = gate(manifest=manifest, directory=directory, cutover_supplied=False)
    assert GATE_CUTOVER_MISSING in result.failures


def test_invalid_configuration_stops_apply_before_any_mutation(manifest, directory):
    bad_manifest = parse_manifest("{not json")
    bad_directory = CustomerDirectory(valid=False, reason="customer_directory_empty")
    result = gate(manifest=bad_manifest, directory=bad_directory)
    assert GATE_MANIFEST_INVALID in result.failures
    assert GATE_CUSTOMER_DIRECTORY_INVALID in result.failures


def test_one_unexpected_canary_notification_halts_every_later_apply(manifest, directory):
    result = gate(manifest=manifest, directory=directory, canary_notification_observed=True)
    assert not result.passed
    assert GATE_CANARY_NOTIFICATION_OBSERVED in result.failures


def test_every_failure_is_reported_at_once(manifest, directory):
    """Three problems should cost one run, not three."""
    result = gate(
        manifest=manifest,
        directory=directory,
        apply_requested=False,
        native_notifications_confirmed=False,
        verified_dry_run_id=None,
        effective=production_settings(easyweek_reviews_enabled=True),
    )
    assert {
        GATE_APPLY_FLAG_MISSING,
        GATE_NATIVE_NOTIFICATIONS_UNCONFIRMED,
        GATE_DRY_RUN_ID_MISSING,
        GATE_REVIEWS_ENABLED,
    } <= set(result.failures)


def test_the_gate_records_the_effective_settings_it_judged(manifest, directory):
    result = gate(manifest=manifest, directory=directory)
    assert result.effective_settings["EASYWEEK_NOTIFICATIONS_ENABLED"] is False
    assert result.effective_settings["EASYWEEK_PROCESSING_ENABLED"] is True


def test_effective_settings_are_read_from_the_running_process(monkeypatch):
    monkeypatch.setattr(settings, "easyweek_notifications_enabled", True, raising=False)
    assert read_effective_settings().easyweek_notifications_enabled is True


# ---------------------------------------------------------------------------
# The mutation client
# ---------------------------------------------------------------------------


def make_client(handler, *, max_attempts=2, sleeps=None):
    async def _sleep(delay: float) -> None:
        if sleeps is not None:
            sleeps.append(delay)

    return EasyWeekMigrationWriteClient(
        api_key="test-key",
        workspace_slug="test-slug",
        transport=httpx.MockTransport(handler),
        sleep=_sleep,
        rate_limiter=RateLimiter(requests_per_minute=6000, sleep=_sleep),
        max_attempts=max_attempts,
    )


def booking_body():
    return build_booking_request(
        location_uuid="11111111-1111-4111-8111-111111111111",
        staff_uuid="33333333-3333-4333-8333-333333333333",
        service_uuid="44444444-4444-4444-8444-444444444444",
        customer_uuid="77777777-7777-4777-8777-777777777777",
        starts_at_utc_iso="2026-09-10T12:00:00Z",
        duration_minutes=60,
        comment="altegio-migration:758285:900001",
    )


async def test_a_successful_post_returns_the_created_uuid():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert request.url.path.endswith("/bookings")
        return httpx.Response(201, json={"uuid": BOOKING_UUID})

    async with make_client(handler) as client:
        created = await client.create_booking(booking_body())
    assert created.booking_uuid == BOOKING_UUID
    assert created.attempts == 1


async def test_a_timeout_after_post_is_uncertain_and_never_retried():
    """The single most important branch: the booking may exist. Do not send again."""
    calls: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        raise httpx.ReadTimeout("timed out", request=request)

    async with make_client(handler) as client:
        with pytest.raises(EasyWeekUncertainMutation):
            await client.create_booking(booking_body())
    assert len(calls) == 1


async def test_a_transport_disconnect_is_uncertain_too():
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection reset", request=request)

    async with make_client(handler) as client:
        with pytest.raises(EasyWeekUncertainMutation):
            await client.create_booking(booking_body())


async def test_a_2xx_without_a_usable_uuid_is_uncertain_not_success():
    """Something probably exists; we cannot name it, so we cannot claim it."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(201, json={"ok": True})

    async with make_client(handler) as client:
        with pytest.raises(EasyWeekUncertainMutation):
            await client.create_booking(booking_body())


async def test_429_is_retried_with_bounded_backoff():
    statuses = [429, 201]
    sleeps: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        status = statuses.pop(0)
        if status == 201:
            return httpx.Response(201, json={"uuid": BOOKING_UUID})
        return httpx.Response(429, headers={"Retry-After": "1"})

    async with make_client(handler, sleeps=sleeps) as client:
        created = await client.create_booking(booking_body())
    assert created.attempts == 2
    assert any(delay > 0 for delay in sleeps)


async def test_a_transient_5xx_is_retried_then_gives_up_as_retryable():
    calls: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        return httpx.Response(503)

    async with make_client(handler) as client:
        with pytest.raises(EasyWeekRetryableError):
            await client.create_booking(booking_body())
    assert len(calls) == 2  # bounded, not unbounded


async def test_a_permanent_4xx_is_never_retried():
    calls: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        return httpx.Response(422, json={"error": "bad"})

    async with make_client(handler) as client:
        with pytest.raises(EasyWeekPermanentError):
            await client.create_booking(booking_body())
    assert len(calls) == 1


async def test_an_auth_failure_is_never_retried():
    calls: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        return httpx.Response(403)

    async with make_client(handler) as client:
        with pytest.raises(EasyWeekAuthError):
            await client.create_booking(booking_body())
    assert len(calls) == 1


async def test_the_rate_limiter_paces_below_the_easyweek_budget():
    """60/min is the ceiling; the cutover deliberately sits well under it."""
    assert DEFAULT_REQUESTS_PER_MINUTE < 60

    now = [0.0]
    slept: list[float] = []

    async def _sleep(delay: float) -> None:
        slept.append(delay)
        now[0] += delay

    limiter = RateLimiter(requests_per_minute=60, sleep=_sleep, monotonic=lambda: now[0])
    await limiter.acquire()
    await limiter.acquire()
    assert slept and slept[0] == pytest.approx(1.0)


def test_the_request_body_carries_only_proven_identifiers():
    body = booking_body()
    assert set(body) == {
        "location_uuid",
        "staff_uuid",
        "customer_uuid",
        "start_time",
        "duration",
        "services",
        "comment",
    }
    # No name, no phone, no free-text service label.
    assert body["comment"].startswith("altegio-migration:")


def test_the_client_repr_never_carries_the_api_key():
    client = EasyWeekMigrationWriteClient(
        api_key="super-secret-key",
        workspace_slug="slug",
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json={})),
    )
    assert "super-secret-key" not in repr(client)
    assert "super-secret-key" not in str(client)
