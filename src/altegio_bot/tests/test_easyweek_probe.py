"""Tests for the read-only EasyWeek operator probe (PR-2).

The probe is the operator's only way to discover the real Durlach
``EASYWEEK_LOCATION_UUID``, and it runs against production data — so its output
must be an allowlist projection, never a redaction pass over a raw response.

These tests drive the probe through ``httpx.MockTransport`` (no real network) and
assert with unique sentinels that no customer field, note, order total, header or
API key can reach stdout/stderr, even when upstream adds new nested fields.
"""

from __future__ import annotations

import json
from typing import Any

import httpx

import altegio_bot.scripts.easyweek_probe as probe
from altegio_bot.easyweek_client import EasyWeekClient

KEY = "SENTINEL_PROBEKEY_aaa111"
SLUG = "SENTINEL_PROBESLUG_aaa222"
CUSTOMER_NAME = "SENTINEL_PROBENAME_aaa333"
CUSTOMER_PHONE = "SENTINEL_PROBEPHONE_aaa444"
CUSTOMER_EMAIL = "SENTINEL_PROBEEMAIL_aaa555"
NOTES = "SENTINEL_PROBENOTES_aaa666"
ORDER_MARKER = "SENTINEL_PROBEORDER_aaa777"
FUTURE_FIELD = "SENTINEL_PROBEFUTURE_aaa888"

ALL_SENTINELS = (
    KEY,
    SLUG,
    CUSTOMER_NAME,
    CUSTOMER_PHONE,
    CUSTOMER_EMAIL,
    NOTES,
    ORDER_MARKER,
    FUTURE_FIELD,
)

BASE = "https://api.example.test/api/public/v2"
BOOKING_UUID = "123e4567-e89b-12d3-a456-426614174000"
DURLACH_UUID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

_LOCATIONS: list[dict[str, Any]] = [
    {
        "uuid": DURLACH_UUID,
        "name": "Durlach",
        "timezone": "Europe/Berlin",
        "address": {"country": "DE", "city": "Karlsruhe", "street": "Pfinztalstr.", "house": "9"},
        # An unexpected nested field must not leak just because it is present.
        "internal_notes": NOTES,
        "owner": {"email": CUSTOMER_EMAIL},
    },
    {"uuid": "ffffffff-1111-2222-3333-444444444444", "name": "Other", "timezone": "Europe/Berlin"},
]

_BOOKING: dict[str, Any] = {
    "uuid": BOOKING_UUID,
    "location_uuid": DURLACH_UUID,
    "start_time": "2026-08-01T10:00:00Z",
    "end_time": "2026-08-01T11:00:00Z",
    "start_time_local": "2026-08-01T12:00:00",
    "end_time_local": "2026-08-01T13:00:00",
    "timezone": "Europe/Berlin",
    "status": {"type": "CONFIRMED"},
    "is_canceled": False,
    "is_completed": False,
    "customer": {"name": CUSTOMER_NAME, "phone": CUSTOMER_PHONE, "email": CUSTOMER_EMAIL},
    "notes": NOTES,
    "comment": NOTES,
    "order": {"total": 4200, "marker": ORDER_MARKER},
    "ordered_services": [{"name": "Wimpernverlängerung", "price": 4200, "secret": ORDER_MARKER}],
    # A field upstream might add tomorrow: must not appear in the summary.
    "brand_new_upstream_field": FUTURE_FIELD,
}


def _install_client(monkeypatch, handler, **overrides: Any) -> None:
    """Point the probe at a MockTransport-backed client."""

    def _factory(*args: Any, **kwargs: Any) -> EasyWeekClient:
        return EasyWeekClient(
            api_key=KEY,
            workspace_slug=SLUG,
            base_url=BASE,
            transport=httpx.MockTransport(handler),
            **overrides,
        )

    monkeypatch.setattr(probe, "EasyWeekClient", _factory)


def _ok_handler(request: httpx.Request) -> httpx.Response:
    path = request.url.path
    if path.endswith("/ping"):
        return httpx.Response(200, json={"ping": "pong", "version": "v12.32.3"})
    if path.endswith("/locations"):
        return httpx.Response(200, json=_LOCATIONS)
    if "/bookings/" in path:
        return httpx.Response(200, json=_BOOKING)
    return httpx.Response(404)  # pragma: no cover


def _assert_no_sentinels(text: str) -> None:
    for sentinel in ALL_SENTINELS:
        assert sentinel not in text, f"{sentinel!r} leaked into probe output"


# ===========================================================================
# Happy path
# ===========================================================================


def test_probe_prints_safe_ping_and_locations(monkeypatch, capsys) -> None:
    _install_client(monkeypatch, _ok_handler)

    exit_code = probe.main(["--redact-pii"])

    assert exit_code == probe.EXIT_OK
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["ok"] is True
    assert payload["ping"] == {"ok": True, "version": "v12.32.3"}
    assert payload["locations"]["count"] == 2

    # The operator gets exactly what they need to identify Durlach.
    durlach = next(item for item in payload["locations"]["items"] if item["name"] == "Durlach")
    assert durlach["uuid"] == DURLACH_UUID
    assert durlach["timezone"] == "Europe/Berlin"
    assert durlach["address"]["city"] == "Karlsruhe"

    # Nothing else from the location object may appear.
    assert set(durlach) <= {"uuid", "name", "timezone", "address"}
    _assert_no_sentinels(captured.out + captured.err)


def test_probe_booking_summary_is_allowlisted(monkeypatch, capsys) -> None:
    _install_client(monkeypatch, _ok_handler)

    exit_code = probe.main(["--booking-uuid", BOOKING_UUID])

    assert exit_code == probe.EXIT_OK
    captured = capsys.readouterr()
    booking = json.loads(captured.out)["booking"]

    # Useful, safe fields are present ...
    assert booking["uuid"] == BOOKING_UUID
    assert booking["location_uuid"] == DURLACH_UUID
    assert booking["timezone"] == "Europe/Berlin"
    assert booking["status_type"] == "CONFIRMED"
    assert booking["is_canceled"] is False
    assert booking["is_completed"] is False
    assert booking["services"]["count"] == 1
    assert booking["services"]["names"] == ["Wimpernverlängerung"]

    # ... and nothing else.
    allowed = {
        "uuid",
        "location_uuid",
        "start_time",
        "end_time",
        "start_time_local",
        "end_time_local",
        "timezone",
        "is_canceled",
        "is_completed",
        "status_type",
        "services",
    }
    assert set(booking) <= allowed
    _assert_no_sentinels(captured.out + captured.err)


def test_probe_output_has_no_customer_subtree(monkeypatch, capsys) -> None:
    _install_client(monkeypatch, _ok_handler)
    probe.main(["--booking-uuid", BOOKING_UUID])
    captured = capsys.readouterr()

    assert "customer" not in captured.out
    assert "notes" not in captured.out
    assert "comment" not in captured.out
    assert "order" not in captured.out
    _assert_no_sentinels(captured.out + captured.err)


def test_unknown_nested_fields_do_not_leak(monkeypatch, capsys) -> None:
    """A field upstream adds tomorrow must not appear by default."""
    _install_client(monkeypatch, _ok_handler)
    probe.main(["--booking-uuid", BOOKING_UUID])
    captured = capsys.readouterr()

    assert FUTURE_FIELD not in captured.out
    assert "brand_new_upstream_field" not in captured.out


def test_default_output_is_redacted_without_the_flag(monkeypatch, capsys) -> None:
    """Redaction is the default: omitting --redact-pii changes nothing."""
    _install_client(monkeypatch, _ok_handler)

    assert probe.main(["--booking-uuid", BOOKING_UUID]) == probe.EXIT_OK
    without_flag = capsys.readouterr()

    assert probe.main(["--booking-uuid", BOOKING_UUID, "--redact-pii"]) == probe.EXIT_OK
    with_flag = capsys.readouterr()

    assert without_flag.out == with_flag.out
    _assert_no_sentinels(without_flag.out + without_flag.err)


def test_probe_exposes_no_unsafe_mode() -> None:
    """PR-2 intentionally ships no way to print raw responses."""
    parser = probe._build_parser()
    flags = {action.dest for action in parser._actions}
    assert flags == {"help", "redact_pii", "booking_uuid"}
    assert parser.get_default("redact_pii") is True


# ===========================================================================
# Error paths
# ===========================================================================


def test_configuration_error_exits_non_zero(monkeypatch, capsys) -> None:
    """No API key configured → non-zero exit, no secret in the output.

    Uses the REAL client so the typed configuration error is raised for real,
    before any network call could happen.
    """
    from altegio_bot import settings as settings_module

    monkeypatch.setattr(settings_module.settings, "easyweek_api_key", "")
    monkeypatch.setattr(settings_module.settings, "easyweek_workspace_slug", "")

    exit_code = probe.main([])

    assert exit_code == probe.EXIT_CONFIG_ERROR
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["ok"] is False
    assert payload["error"] == "EasyWeekConfigError"
    _assert_no_sentinels(captured.out + captured.err)


def test_api_error_exits_non_zero_without_response_body(monkeypatch, capsys) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            401,
            json={"message": CUSTOMER_NAME, "detail": ORDER_MARKER},
            headers={"X-Leak": KEY},
        )

    _install_client(monkeypatch, handler)

    exit_code = probe.main([])

    assert exit_code == probe.EXIT_API_ERROR
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["ok"] is False
    assert payload["error"] == "EasyWeekAuthError"
    assert payload["status"] == 401
    assert payload["operation"] == "ping"
    _assert_no_sentinels(captured.out + captured.err)


def test_protocol_error_exits_non_zero(monkeypatch, capsys) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/ping"):
            return httpx.Response(200, content=b"<html>" + NOTES.encode() + b"</html>")
        return httpx.Response(200, json=[])  # pragma: no cover

    _install_client(monkeypatch, handler)

    exit_code = probe.main([])

    assert exit_code == probe.EXIT_API_ERROR
    captured = capsys.readouterr()
    assert json.loads(captured.out)["error"] == "EasyWeekProtocolError"
    _assert_no_sentinels(captured.out + captured.err)


def test_retryable_exhaustion_exits_non_zero(monkeypatch, capsys) -> None:
    async def _no_sleep(delay: float) -> None:
        return None

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, json={"detail": ORDER_MARKER})

    _install_client(monkeypatch, handler, sleep=_no_sleep, max_attempts=2)

    exit_code = probe.main([])

    assert exit_code == probe.EXIT_API_ERROR
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["error"] == "EasyWeekRetryableError"
    assert payload["retryable"] is True
    _assert_no_sentinels(captured.out + captured.err)


def test_invalid_booking_uuid_exits_non_zero(monkeypatch, capsys) -> None:
    _install_client(monkeypatch, _ok_handler)

    exit_code = probe.main(["--booking-uuid", "not-a-uuid"])

    assert exit_code == probe.EXIT_API_ERROR
    captured = capsys.readouterr()
    assert json.loads(captured.out)["error"] == "EasyWeekPermanentError"


# ===========================================================================
# Pure projection helpers
# ===========================================================================


def test_safe_location_summary_drops_everything_unlisted() -> None:
    summary = probe.safe_location_summary(_LOCATIONS[0])
    assert set(summary) <= {"uuid", "name", "timezone", "address"}
    _assert_no_sentinels(json.dumps(summary))


def test_safe_booking_summary_drops_everything_unlisted() -> None:
    summary = probe.safe_booking_summary(_BOOKING)
    assert "customer" not in summary
    _assert_no_sentinels(json.dumps(summary, ensure_ascii=False))


def test_structured_value_under_allowlisted_key_becomes_marker() -> None:
    """If upstream turns a scalar field into an object, print a type marker."""
    summary = probe.safe_booking_summary({"uuid": {"nested": CUSTOMER_NAME}})
    assert summary["uuid"] == "<dict omitted>"
    _assert_no_sentinels(json.dumps(summary))
