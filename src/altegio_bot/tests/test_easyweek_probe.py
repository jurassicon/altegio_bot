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
import pytest

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

# Only the canonical EasyWeek origin is accepted now; MockTransport still
# intercepts every request, so no real network call happens.
BASE = "https://my.easyweek.io/api/public/v2"
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


def test_empty_locations_list_fails_the_probe(monkeypatch, capsys) -> None:
    """An empty list means no Durlach UUID can be recorded — that is a FAILED probe.

    Printing ok=true here would tell the operator the key works while leaving the
    PR-2 DoD (identify the location UUID) silently unmet.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/ping"):
            return httpx.Response(200, json={"ping": "pong", "version": "v12.32.3"})
        return httpx.Response(200, json=[])

    _install_client(monkeypatch, handler)

    exit_code = probe.main([])

    assert exit_code == probe.EXIT_API_ERROR
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["ok"] is False
    assert payload["error"] == "EasyWeekProtocolError"
    assert payload["operation"] == "list_locations"
    assert '"ok": true' not in captured.out


def test_empty_data_envelope_also_fails_the_probe(monkeypatch, capsys) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/ping"):
            return httpx.Response(200, json={"ping": "pong"})
        return httpx.Response(200, json={"data": []})

    _install_client(monkeypatch, handler)

    assert probe.main([]) == probe.EXIT_API_ERROR
    assert json.loads(capsys.readouterr().out)["ok"] is False


def test_ping_without_success_marker_fails_the_probe(monkeypatch, capsys) -> None:
    """A 200 from the wrong endpoint must not be reported as a working API."""

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/ping"):
            return httpx.Response(200, json={"status": "ok"})  # no ping marker
        return httpx.Response(200, json=_LOCATIONS)  # pragma: no cover

    _install_client(monkeypatch, handler)

    exit_code = probe.main([])

    assert exit_code == probe.EXIT_API_ERROR
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["error"] == "EasyWeekProtocolError"
    assert payload["operation"] == "ping"
    assert '"ok": true' not in captured.out


def test_malformed_booking_envelope_fails_the_probe(monkeypatch, capsys) -> None:
    """`data` present but not an object must not fall back to the envelope."""

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.endswith("/ping"):
            return httpx.Response(200, json={"ping": "pong"})
        if path.endswith("/locations"):
            return httpx.Response(200, json=_LOCATIONS)
        return httpx.Response(200, json={"data": None, "leak": ORDER_MARKER})

    _install_client(monkeypatch, handler)

    exit_code = probe.main(["--booking-uuid", BOOKING_UUID])

    assert exit_code == probe.EXIT_API_ERROR
    captured = capsys.readouterr()
    assert json.loads(captured.out)["error"] == "EasyWeekProtocolError"
    assert '"ok": true' not in captured.out
    _assert_no_sentinels(captured.out + captured.err)


def test_location_without_identity_fields_fails_the_probe(monkeypatch, capsys) -> None:
    """`[{}]` used to print uuid/name/timezone as null with ok=true.

    An operator cannot record a UUID from that, so it must be a failed probe.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/ping"):
            return httpx.Response(200, json={"ping": "pong", "version": "v12.32.3"})
        return httpx.Response(200, json=[{}])

    _install_client(monkeypatch, handler)

    exit_code = probe.main(["--redact-pii"])

    assert exit_code == probe.EXIT_API_ERROR
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["ok"] is False
    assert payload["error"] == "EasyWeekProtocolError"
    assert payload["operation"] == "list_locations"
    # The old broken output must be gone entirely.
    assert '"ok": true' not in captured.out
    assert '"uuid": null' not in captured.out
    assert '"name": null' not in captured.out
    assert '"timezone": null' not in captured.out


def test_partially_identified_location_fails_the_probe(monkeypatch, capsys) -> None:
    """One good location plus one malformed entry must not yield a partial list."""

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/ping"):
            return httpx.Response(200, json={"ping": "pong"})
        return httpx.Response(200, json=[_LOCATIONS[0], {"name": "Broken"}])

    _install_client(monkeypatch, handler)

    assert probe.main([]) == probe.EXIT_API_ERROR
    captured = capsys.readouterr()
    assert json.loads(captured.out)["error"] == "EasyWeekProtocolError"
    assert '"ok": true' not in captured.out
    # No partial list may be shown to the operator.
    assert DURLACH_UUID not in captured.out
    _assert_no_sentinels(captured.out + captured.err)


def test_probe_prints_the_required_identity_fields(monkeypatch, capsys) -> None:
    """Positive counterpart: valid locations expose exactly what the operator needs."""
    _install_client(monkeypatch, _ok_handler)

    assert probe.main(["--redact-pii"]) == probe.EXIT_OK
    captured = capsys.readouterr()
    items = json.loads(captured.out)["locations"]["items"]

    for item in items:
        assert isinstance(item["uuid"], str) and item["uuid"].strip()
        assert isinstance(item["name"], str) and item["name"].strip()
        assert isinstance(item["timezone"], str) and item["timezone"].strip()
    _assert_no_sentinels(captured.out + captured.err)


# ===========================================================================
# The live response shape: object timezone + address_1/postal_code
# ===========================================================================

# Sanitized copy of what the read-only production probe actually returned. No
# real UUID, no real address, no credentials.
_LIVE_LOCATION: dict[str, Any] = {
    "uuid": DURLACH_UUID,
    "name": "Durlach",
    "timezone": {"name": "Europe/Berlin", "offset": "+02:00", "short": "CEST"},
    "address": {
        "address_1": "Beispielstr. 1",
        "apt": None,
        "city": "Karlsruhe",
        "postal_code": "76227",
        "position": {"lat": 0, "lng": 0},
        "unexpected": "must not leak",
    },
    "description": None,
    "images": None,
    "is_address_hidden": False,
    "opening_hours": {},
}


def _live_handler(request: httpx.Request) -> httpx.Response:
    path = request.url.path
    if path.endswith("/ping"):
        return httpx.Response(200, json={"ping": "pong"})
    if path.endswith("/locations"):
        return httpx.Response(200, json={"data": [_LIVE_LOCATION], "links": {}, "meta": {}})
    return httpx.Response(404)  # pragma: no cover


def test_object_timezone_projects_to_the_iana_name() -> None:
    """The live object must print as a plain string, not as a type marker."""
    summary = probe.safe_location_summary(_LIVE_LOCATION)
    assert summary["timezone"] == "Europe/Berlin"


def test_object_timezone_never_prints_a_dict_marker_or_its_details() -> None:
    """offset/short add nothing for picking a branch and only widen the output."""
    blob = json.dumps(probe.safe_location_summary(_LIVE_LOCATION))
    assert "<dict omitted>" not in blob
    assert "offset" not in blob
    assert "short" not in blob
    assert "+02:00" not in blob
    assert "CEST" not in blob


def test_string_timezone_projection_is_unchanged() -> None:
    """The documented/legacy shape must keep working exactly as before."""
    assert probe.safe_location_summary(_LOCATIONS[0])["timezone"] == "Europe/Berlin"


@pytest.mark.parametrize(
    "timezone",
    [None, {}, {"name": None}, {"name": 123}, {"offset": "+02:00"}, [], 123],
    ids=["null", "empty-object", "name-null", "name-number", "no-name", "list", "number"],
)
def test_unusable_timezone_projects_to_null_not_a_partial_object(timezone: Any) -> None:
    """The client rejects these, but the projection must not leak either.

    A type marker or a partial object would put unvalidated upstream data on the
    operator's screen; ``None`` says "nothing usable" without disclosing anything.
    """
    summary = probe.safe_location_summary({**_LOCATIONS[0], "timezone": timezone})
    assert summary["timezone"] is None


def test_live_address_shape_projects_only_allowlisted_fields() -> None:
    """address_1/apt/city/postal_code are printed; position and friends are not."""
    address = probe.safe_location_summary(_LIVE_LOCATION)["address"]
    assert set(address) == {"address_1", "apt", "city", "postal_code"}
    assert address["address_1"] == "Beispielstr. 1"
    assert address["postal_code"] == "76227"
    assert address["apt"] is None

    blob = json.dumps(probe.safe_location_summary(_LIVE_LOCATION))
    for forbidden in ("position", "lat", "lng", "unexpected", "must not leak"):
        assert forbidden not in blob, f"{forbidden!r} leaked into the address projection"


def test_live_address_fields_keep_their_original_names() -> None:
    """No domain renaming here: address_1 stays address_1 (PR-4 territory)."""
    address = probe.safe_location_summary(_LIVE_LOCATION)["address"]
    assert "street" not in address
    assert "zip_code" not in address


def test_legacy_address_fields_are_still_supported() -> None:
    """The documented country/city/street/house/zip_code shape must not regress."""
    legacy = {
        **_LOCATIONS[0],
        "address": {
            "country": "DE",
            "city": "Karlsruhe",
            "street": "Beispielstr.",
            "house": "9",
            "zip_code": "76227",
        },
    }
    address = probe.safe_location_summary(legacy)["address"]
    assert set(address) == {"country", "city", "street", "house", "zip_code"}


def test_live_location_noise_fields_never_reach_the_output() -> None:
    """description/images/opening_hours/is_address_hidden are not allowlisted."""
    blob = json.dumps(probe.safe_location_summary(_LIVE_LOCATION))
    for forbidden in ("description", "images", "opening_hours", "is_address_hidden"):
        assert forbidden not in blob


def test_probe_accepts_the_live_response_end_to_end(monkeypatch, capsys) -> None:
    """The exact shape that made the production probe fail must now succeed."""
    _install_client(monkeypatch, _live_handler)

    assert probe.main(["--redact-pii"]) == probe.EXIT_OK
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert payload["ok"] is True
    assert payload["locations"]["count"] == 1
    item = payload["locations"]["items"][0]
    assert item["timezone"] == "Europe/Berlin"
    assert set(item) == {"uuid", "name", "timezone", "address"}
    assert set(item["address"]) == {"address_1", "apt", "city", "postal_code"}

    for forbidden in (
        "links",
        "meta",
        "position",
        "lat",
        "lng",
        "opening_hours",
        "is_address_hidden",
        "<dict omitted>",
    ):
        assert forbidden not in captured.out, f"{forbidden!r} leaked into the probe output"
    _assert_no_sentinels(captured.out + captured.err)
