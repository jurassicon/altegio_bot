"""Tests for run_test_newsletter_smart helpers."""

from __future__ import annotations

import httpx
import pytest
import respx

from altegio_bot.meta_templates import NEWSLETTER_FOLLOWUP_TEMPLATE, NEWSLETTER_MONTHLY_TEMPLATE
from altegio_bot.scripts.run_test_newsletter_smart import (
    _build_smart_template_params,
    _evaluate_outcome,
    _gen_card_number,
    _send_template_direct,
    check_meta_template,
)

GRAPH_URL = "https://graph.facebook.com"
API_VERSION = "v20.0"
WABA_ID = "waba123"
TOKEN = "tok"


# ---------------------------------------------------------------------------
# _gen_card_number
# ---------------------------------------------------------------------------


def test_gen_card_number_length() -> None:
    num = _gen_card_number()
    assert len(num) == 16, f"Expected 16 digits, got {len(num)}: {num!r}"


def test_gen_card_number_all_digits() -> None:
    num = _gen_card_number()
    assert num.isdigit(), f"Expected all digits: {num!r}"


def test_gen_card_number_starts_with_99() -> None:
    num = _gen_card_number()
    assert num.startswith("99"), f"Expected 99 prefix: {num!r}"


# ---------------------------------------------------------------------------
# _evaluate_outcome
# ---------------------------------------------------------------------------


def test_evaluate_outcome_pass_delivered_when_delivered() -> None:
    assert _evaluate_outcome(["sent", "delivered"], expect_status="delivered") == "pass"


def test_evaluate_outcome_pass_delivered_when_read() -> None:
    assert _evaluate_outcome(["read"], expect_status="delivered") == "pass"


def test_evaluate_outcome_pending_when_only_sent_expect_delivered() -> None:
    assert _evaluate_outcome(["sent"], expect_status="delivered") is None


def test_evaluate_outcome_pass_sent_when_sent() -> None:
    assert _evaluate_outcome(["sent"], expect_status="sent") == "pass"


def test_evaluate_outcome_fail_on_failed() -> None:
    assert _evaluate_outcome(["sent", "failed"], expect_status="delivered") == "fail"


def test_evaluate_outcome_fail_takes_priority() -> None:
    # 'failed' overrides any pass-worthy statuses
    assert _evaluate_outcome(["delivered", "failed"], expect_status="delivered") == "fail"


def test_evaluate_outcome_none_when_empty() -> None:
    assert _evaluate_outcome([], expect_status="delivered") is None


# ---------------------------------------------------------------------------
# check_meta_template (mocked HTTP)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_check_meta_template_approved() -> None:
    url = f"{GRAPH_URL}/{API_VERSION}/{WABA_ID}/message_templates"
    with respx.mock:
        respx.get(url).mock(
            return_value=httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "id": "1331641148769327",
                            "name": "kitilash_ka_newsletter_new_clients_monthly_v1",
                            "status": "APPROVED",
                            "language": "de",
                            "category": "MARKETING",
                        }
                    ]
                },
            )
        )
        ok, err = await check_meta_template(
            "kitilash_ka_newsletter_new_clients_monthly_v1",
            access_token=TOKEN,
            graph_url=GRAPH_URL,
            api_version=API_VERSION,
            waba_id=WABA_ID,
        )

    assert ok is True
    assert err is None


@pytest.mark.asyncio
async def test_check_meta_template_not_found() -> None:
    url = f"{GRAPH_URL}/{API_VERSION}/{WABA_ID}/message_templates"
    with respx.mock:
        respx.get(url).mock(return_value=httpx.Response(200, json={"data": []}))
        ok, err = await check_meta_template(
            "nonexistent_template",
            access_token=TOKEN,
            graph_url=GRAPH_URL,
            api_version=API_VERSION,
            waba_id=WABA_ID,
        )

    assert ok is False
    assert err is not None
    assert "not found" in err


@pytest.mark.asyncio
async def test_check_meta_template_not_approved() -> None:
    url = f"{GRAPH_URL}/{API_VERSION}/{WABA_ID}/message_templates"
    with respx.mock:
        respx.get(url).mock(
            return_value=httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "id": "999",
                            "name": "some_template",
                            "status": "PENDING",
                            "language": "de",
                            "category": "MARKETING",
                        }
                    ]
                },
            )
        )
        ok, err = await check_meta_template(
            "some_template",
            access_token=TOKEN,
            graph_url=GRAPH_URL,
            api_version=API_VERSION,
            waba_id=WABA_ID,
        )

    assert ok is False
    assert err is not None
    assert "PENDING" in err


@pytest.mark.asyncio
async def test_check_meta_template_http_error() -> None:
    url = f"{GRAPH_URL}/{API_VERSION}/{WABA_ID}/message_templates"
    with respx.mock:
        respx.get(url).mock(return_value=httpx.Response(401, json={"error": "Unauthorized"}))
        ok, err = await check_meta_template(
            "some_template",
            access_token="bad_token",
            graph_url=GRAPH_URL,
            api_version=API_VERSION,
            waba_id=WABA_ID,
        )

    assert ok is False
    assert err is not None
    assert "401" in err


# ---------------------------------------------------------------------------
# _send_template_direct — header component in payload
# ---------------------------------------------------------------------------

_PHONE_NUMBER_ID = "12345678901"
_ACCESS_TOKEN = "test-tok"
_GRAPH_URL = "https://graph.facebook.com"
_API_VERSION = "v20.0"
_SEND_URL = f"{_GRAPH_URL}/{_API_VERSION}/{_PHONE_NUMBER_ID}/messages"


@pytest.mark.asyncio
async def test_send_template_direct_with_header_builds_header_and_body() -> None:
    """When header_image_url is given, payload must have HEADER component before BODY."""
    header_url = "https://cdn.example.com/newsletter_header.jpg"
    captured: list[dict] = []

    with respx.mock:
        respx.post(_SEND_URL).mock(return_value=httpx.Response(200, json={"messages": [{"id": "wamid.testHEADER"}]}))

        # Capture the actual request body
        async def _handler(request: httpx.Request) -> httpx.Response:
            import json as _json

            captured.append(_json.loads(request.content))
            return httpx.Response(200, json={"messages": [{"id": "wamid.testHEADER"}]})

        respx.post(_SEND_URL).mock(side_effect=_handler)

        msg_id = await _send_template_direct(
            phone_e164="+491234567890",
            template_name=NEWSLETTER_MONTHLY_TEMPLATE,
            language="de",
            params=["Anna", "https://booking.link/", "Kundenkarte #001"],
            access_token=_ACCESS_TOKEN,
            graph_url=_GRAPH_URL,
            api_version=_API_VERSION,
            phone_number_id=_PHONE_NUMBER_ID,
            header_image_url=header_url,
        )

    assert msg_id == "wamid.testHEADER"
    assert len(captured) == 1
    components = captured[0]["template"]["components"]
    assert len(components) == 2, f"Expected HEADER + BODY, got {len(components)} components"
    assert components[0]["type"] == "header"
    assert components[0]["parameters"][0]["image"]["link"] == header_url
    assert components[1]["type"] == "body"


@pytest.mark.asyncio
async def test_send_template_direct_without_header_is_body_only() -> None:
    """When header_image_url is None, payload must have only BODY component."""
    captured: list[dict] = []

    with respx.mock:

        async def _handler(request: httpx.Request) -> httpx.Response:
            import json as _json

            captured.append(_json.loads(request.content))
            return httpx.Response(200, json={"messages": [{"id": "wamid.testBODY"}]})

        respx.post(_SEND_URL).mock(side_effect=_handler)

        await _send_template_direct(
            phone_e164="+491234567890",
            template_name="kitilash_ka_record_created_v1",
            language="de",
            params=["Anna", "Tanja", "10.02.2026", "14:00", "Service", "60.00", "https://link"],
            access_token=_ACCESS_TOKEN,
            graph_url=_GRAPH_URL,
            api_version=_API_VERSION,
            phone_number_id=_PHONE_NUMBER_ID,
            header_image_url=None,
        )

    assert len(captured) == 1
    components = captured[0]["template"]["components"]
    assert len(components) == 1, f"Expected BODY-only, got {len(components)} components"
    assert components[0]["type"] == "body"


# ---------------------------------------------------------------------------
# run_smart_test — fail-fast when newsletter requires header and URL is missing
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_smart_test_fails_fast_when_monthly_header_url_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    """run_smart_test must return exit_code=2 immediately when monthly header URL is not set."""
    import altegio_bot.scripts.run_test_newsletter_smart as sm

    monkeypatch.setattr(sm.settings, "whatsapp_access_token", "tok")
    monkeypatch.setattr(sm.settings, "meta_waba_id", "waba123")
    monkeypatch.setattr(sm.settings, "meta_wa_phone_number_id", "phone123")
    monkeypatch.setattr(sm.settings, "meta_newsletter_monthly_header_image_url", "")

    exit_code = await sm.run_smart_test(
        phone="381638400431",
        company_id=758285,
        location_id=758285,
        booking_link="https://n813709.alteg.io/",
        template_name=NEWSLETTER_MONTHLY_TEMPLATE,
        expect_status="delivered",
        timeout_sec=10,
        cleanup=False,
        cleanup_on_fail=False,
        force=True,
        card_type_id="999",
        client_name="Test",
    )

    assert exit_code == 2, f"Expected exit_code=2 (fail-fast), got {exit_code}"


@pytest.mark.asyncio
async def test_run_smart_test_fails_fast_when_followup_header_url_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    """run_smart_test must return exit_code=2 immediately when followup header URL is not set."""
    import altegio_bot.scripts.run_test_newsletter_smart as sm

    monkeypatch.setattr(sm.settings, "whatsapp_access_token", "tok")
    monkeypatch.setattr(sm.settings, "meta_waba_id", "waba123")
    monkeypatch.setattr(sm.settings, "meta_wa_phone_number_id", "phone123")
    monkeypatch.setattr(sm.settings, "meta_newsletter_followup_header_image_url", "")

    exit_code = await sm.run_smart_test(
        phone="381638400431",
        company_id=758285,
        location_id=758285,
        booking_link="https://n813709.alteg.io/",
        template_name=NEWSLETTER_FOLLOWUP_TEMPLATE,
        expect_status="delivered",
        timeout_sec=10,
        cleanup=False,
        cleanup_on_fail=False,
        force=True,
        card_type_id="999",
        client_name="Test",
    )

    assert exit_code == 2, f"Expected exit_code=2 (fail-fast), got {exit_code}"


@pytest.mark.asyncio
async def test_run_smart_test_unknown_template_params_fails_before_card_issue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Approved-but-unsupported templates must fail before issuing loyalty cards."""
    import altegio_bot.scripts.run_test_newsletter_smart as sm

    issue_card_called = False

    async def _fake_check_meta_template(*args: object, **kwargs: object) -> tuple[bool, None]:
        return True, None

    class _FailingLoyalty:
        async def issue_card(self, *args: object, **kwargs: object) -> dict[str, object]:
            nonlocal issue_card_called
            issue_card_called = True
            raise AssertionError("issue_card must not be called for unsupported template params")

        async def aclose(self) -> None:
            pass

    monkeypatch.setattr(sm.settings, "whatsapp_access_token", "tok")
    monkeypatch.setattr(sm.settings, "meta_waba_id", "waba123")
    monkeypatch.setattr(sm.settings, "meta_wa_phone_number_id", "phone123")
    monkeypatch.setattr(sm, "check_meta_template", _fake_check_meta_template)
    monkeypatch.setattr(sm, "AltegioLoyaltyClient", _FailingLoyalty)

    exit_code = await sm.run_smart_test(
        phone="381638400431",
        company_id=758285,
        location_id=758285,
        booking_link="https://n813709.alteg.io/",
        template_name="approved_but_unsupported_template",
        expect_status="delivered",
        timeout_sec=10,
        cleanup=False,
        cleanup_on_fail=False,
        force=True,
        card_type_id="999",
        client_name="Test",
    )

    assert exit_code == 2, f"Expected exit_code=2 for unsupported template params, got {exit_code}"
    assert issue_card_called is False


# ---------------------------------------------------------------------------
# _build_smart_template_params — param count by template type
# ---------------------------------------------------------------------------

_PARAMS_KW = {
    "client_name": "Anna",
    "booking_link": "https://booking.link/",
    "loyalty_card_text": "Kundenkarte #001",
}


def test_build_smart_template_params_monthly_has_three_params() -> None:
    """Monthly template requires 3 params: client_name, booking_link, loyalty_card_text."""
    params = _build_smart_template_params(NEWSLETTER_MONTHLY_TEMPLATE, **_PARAMS_KW)
    assert len(params) == 3, f"Expected 3 params for monthly, got {len(params)}: {params}"
    assert params[0] == "Anna"
    assert params[1] == "https://booking.link/"
    assert params[2] == "Kundenkarte #001"


def test_build_smart_template_params_followup_has_two_params() -> None:
    """Followup template requires only 2 params: client_name, booking_link (no card text)."""
    params = _build_smart_template_params(NEWSLETTER_FOLLOWUP_TEMPLATE, **_PARAMS_KW)
    assert len(params) == 2, f"Expected 2 params for followup, got {len(params)}: {params}"
    assert params[0] == "Anna"
    assert params[1] == "https://booking.link/"
    assert "Kundenkarte #001" not in params, "loyalty_card_text must NOT appear in followup params"


def test_build_smart_template_params_unknown_template_returns_empty() -> None:
    """Unknown template name must return [] — caller must treat this as a config error."""
    params = _build_smart_template_params("unknown_template_xyz", **_PARAMS_KW)
    assert params == [], f"Expected [] for unknown template, got {params}"


# ---------------------------------------------------------------------------
# _send_template_direct — followup with header uses exactly 2 body params
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_send_template_direct_followup_with_header_uses_two_body_params() -> None:
    """Followup template with header: payload must have HEADER + BODY with 2 params."""
    header_url = "https://cdn.example.com/followup_header.jpg"
    captured: list[dict] = []

    with respx.mock:

        async def _handler(request: httpx.Request) -> httpx.Response:
            import json as _json

            captured.append(_json.loads(request.content))
            return httpx.Response(200, json={"messages": [{"id": "wamid.followup"}]})

        respx.post(_SEND_URL).mock(side_effect=_handler)

        await _send_template_direct(
            phone_e164="+491234567890",
            template_name=NEWSLETTER_FOLLOWUP_TEMPLATE,
            language="de",
            params=["Anna", "https://booking.link/"],
            access_token=_ACCESS_TOKEN,
            graph_url=_GRAPH_URL,
            api_version=_API_VERSION,
            phone_number_id=_PHONE_NUMBER_ID,
            header_image_url=header_url,
        )

    assert len(captured) == 1
    components = captured[0]["template"]["components"]
    assert len(components) == 2, f"Expected HEADER + BODY, got {len(components)}"
    assert components[0]["type"] == "header"
    assert components[0]["parameters"][0]["image"]["link"] == header_url
    assert components[1]["type"] == "body"
    body_params = components[1]["parameters"]
    assert len(body_params) == 2, f"Followup BODY must have 2 params, got {len(body_params)}: {body_params}"
    assert body_params[0]["text"] == "Anna"
    assert body_params[1]["text"] == "https://booking.link/"
