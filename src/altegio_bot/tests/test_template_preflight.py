"""Tests: preflight validation of Meta template params before send.

Context
-------
Production bug: CRM-only newsletter jobs were sent to Meta with
client_name="" (empty required param) because the fallback from
payload.contact_name was missing.  Meta rejected with
"Required parameter is missing" *after* the HTTP round-trip.

These tests verify that:
1. Empty required params are caught *locally* before Meta is called.
2. Wrong param count is caught locally.
3. Valid params pass through to safe_send_template unchanged.
4. The text (non-template) send path is completely unaffected.
5. Error messages clearly say "Local template validation failed".

Tests
-----
1. monthly template with empty client_name → local failure, no Meta call
2. monthly template with wrong param count → local failure, no Meta call
3. followup template with wrong param count → local failure, no Meta call
4. valid monthly template → safe_send_template called, job succeeds
5. text send path (send_mode=text) → no validation, no Meta template call
6. error text starts with 'Local template validation failed'
7. unit tests for validate_template_params directly
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

import altegio_bot.workers.outbox_worker as ow
from altegio_bot.template_validation import validate_template_params

KA = 758285
MONTHLY = "kitilash_ka_newsletter_new_clients_monthly_v1"
FOLLOWUP = "kitilash_ka_newsletter_new_clients_followup_v1"
MONTHLY_JOB = "newsletter_new_clients_monthly"
FOLLOWUP_JOB = "newsletter_new_clients_followup"


# ---------------------------------------------------------------------------
# Unit tests: validate_template_params
# ---------------------------------------------------------------------------


def test_validate_monthly_empty_client_name_returns_error() -> None:
    """Empty client_name in monthly template → error mentioning param #1."""
    err = validate_template_params(
        MONTHLY,
        ["", "https://n813709.alteg.io/", "Kundenkarte #123"],
    )
    assert err is not None
    assert "Local template validation failed" in err
    assert "client_name" in err or "#1" in err


def test_validate_monthly_wrong_param_count_returns_error() -> None:
    """Monthly template with 2 params instead of 3 → error with counts."""
    err = validate_template_params(
        MONTHLY,
        ["Anna", "https://n813709.alteg.io/"],
    )
    assert err is not None
    assert "Local template validation failed" in err
    assert "3" in err
    assert "2" in err


def test_validate_followup_wrong_param_count_returns_error() -> None:
    """Followup template with 3 params instead of 2 → error."""
    err = validate_template_params(
        FOLLOWUP,
        ["Anna", "https://n813709.alteg.io/", "extra"],
    )
    assert err is not None
    assert "Local template validation failed" in err
    assert "2" in err
    assert "3" in err


def test_validate_monthly_valid_params_returns_none() -> None:
    """Monthly template with correct 3 non-empty params → None (pass)."""
    err = validate_template_params(
        MONTHLY,
        ["Anna Müller", "https://n813709.alteg.io/", "Kundenkarte #0074"],
    )
    assert err is None


def test_validate_followup_valid_params_returns_none() -> None:
    """Followup template with correct 2 non-empty params → None (pass)."""
    err = validate_template_params(
        FOLLOWUP,
        ["Hans", "https://n813709.alteg.io/"],
    )
    assert err is None


def test_validate_empty_param_list_returns_error() -> None:
    """Empty param list (unknown template → build_template_params returned []) → error."""
    err = validate_template_params("kitilash_ka_unknown_template_v99", [])
    assert err is not None
    assert "Local template validation failed" in err


def test_validate_error_prefix_is_local() -> None:
    """All errors start with the canonical local-validation prefix."""
    err = validate_template_params(MONTHLY, ["", "link", "card"])
    assert err is not None
    assert err.startswith("Local template validation failed"), f"Error must start with canonical prefix, got: {err!r}"


# ---------------------------------------------------------------------------
# Helpers shared by integration tests
# ---------------------------------------------------------------------------


@dataclass
class _FakeJob:
    id: int
    company_id: int
    job_type: str
    status: str = "processing"
    record_id: int | None = None
    client_id: int | None = None
    last_error: str | None = None
    attempts: int = 0
    max_attempts: int = 5
    run_at: Any = None
    locked_at: Any = None
    payload: dict = field(default_factory=dict)


class _FakeSession:
    def __init__(self) -> None:
        self.added: list[Any] = []

    def add(self, obj: Any) -> None:
        self.added.append(obj)

    async def get(self, model: Any, pk: Any) -> None:
        return None


def _base_render_ctx(client_name: str = "Anna") -> dict:
    return {
        "client_name": client_name,
        "booking_link": "https://n813709.alteg.io/",
        "loyalty_card_text": "Kundenkarte #0074454347287392",
        "sender_id": 1,
        "sender_code": "default",
        "staff_name": "",
        "date": "",
        "time": "",
        "services": "",
        "primary_service": "",
        "total_cost": "0.00",
        "short_link": "",
        "unsubscribe_link": "",
        "pre_appointment_notes": "",
    }


def _patch_common(monkeypatch: Any) -> None:
    monkeypatch.setattr(ow, "_find_success_outbox", AsyncMock(return_value=None))
    monkeypatch.setattr(ow, "_find_existing_outbox", AsyncMock(return_value=None))
    monkeypatch.setattr(ow, "_load_record", AsyncMock(return_value=None))
    monkeypatch.setattr(ow, "_load_client", AsyncMock(return_value=None))
    monkeypatch.setattr(ow, "_apply_rate_limit", AsyncMock(return_value=None))
    monkeypatch.setattr(ow, "_count_131026_failures", AsyncMock(return_value=0))


# ---------------------------------------------------------------------------
# Integration test 1: monthly template with empty client_name → local fail
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_monthly_empty_client_name_fails_locally(monkeypatch: Any) -> None:
    """Monthly template: empty client_name causes local failure, no Meta call.

    The key invariant: validate_template_params is called between
    build_template_params and safe_send_template, so Meta is never reached.
    """
    send_template_calls: list[Any] = []

    job = _FakeJob(
        id=5001,
        company_id=KA,
        job_type=MONTHLY_JOB,
        payload={
            "kind": MONTHLY_JOB,
            "loyalty_card_text": "Kundenkarte #0074454347287392",
            "phone_e164": "+491234567890",
            # no contact_name → client_name stays ""
        },
    )
    session = _FakeSession()
    _patch_common(monkeypatch)

    async def _fake_render(session, *, company_id, template_code, record, client):
        return "Hallo {{1}}, ...", 1, "de", _base_render_ctx(client_name="")

    monkeypatch.setattr(ow, "_render_message", _fake_render)

    async def _fake_send(provider, sender_id, phone, template_name, language, params, **kw):
        send_template_calls.append(params)
        return "wamid.fake", None

    monkeypatch.setattr(ow, "safe_send_template", _fake_send)
    monkeypatch.setattr(ow.settings, "whatsapp_send_mode", "template")
    monkeypatch.setattr(ow.settings, "meta_newsletter_monthly_header_image_url", "https://cdn.example.com/h.jpg")

    await ow._run_job_logic(session, job, provider=MagicMock())  # type: ignore[arg-type]

    assert len(send_template_calls) == 0, (
        f"safe_send_template must NOT be called on preflight failure; got calls: {send_template_calls}"
    )
    assert job.status == "failed", f"Expected failed, got {job.status!r}"
    assert job.last_error is not None
    assert "Local template validation failed" in job.last_error, (
        f"last_error must indicate local failure, got: {job.last_error!r}"
    )


# ---------------------------------------------------------------------------
# Integration test 2: monthly template wrong param count → local fail
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_monthly_wrong_param_count_fails_locally(monkeypatch: Any) -> None:
    """Monthly template: if build_template_params returns wrong count → local fail.

    We simulate this by patching build_template_params to return 2 params
    instead of 3.
    """
    send_template_calls: list[Any] = []

    job = _FakeJob(
        id=5002,
        company_id=KA,
        job_type=MONTHLY_JOB,
        payload={
            "kind": MONTHLY_JOB,
            "loyalty_card_text": "Kundenkarte #0074454347287392",
            "phone_e164": "+491234567890",
        },
    )
    session = _FakeSession()
    _patch_common(monkeypatch)

    async def _fake_render(session, *, company_id, template_code, record, client):
        return "Hallo {{1}}, ...", 1, "de", _base_render_ctx()

    monkeypatch.setattr(ow, "_render_message", _fake_render)

    # Force build_template_params to return only 2 params (wrong count)
    monkeypatch.setattr(
        ow,
        "build_template_params",
        lambda tpl, ctx: ["Anna", "https://n813709.alteg.io/"],
    )

    async def _fake_send(provider, sender_id, phone, template_name, language, params, **kw):
        send_template_calls.append(params)
        return "wamid.fake", None

    monkeypatch.setattr(ow, "safe_send_template", _fake_send)
    monkeypatch.setattr(ow.settings, "whatsapp_send_mode", "template")
    monkeypatch.setattr(ow.settings, "meta_newsletter_monthly_header_image_url", "https://cdn.example.com/h.jpg")

    await ow._run_job_logic(session, job, provider=MagicMock())  # type: ignore[arg-type]

    assert len(send_template_calls) == 0, "safe_send_template must NOT be called on wrong param count"
    assert job.status == "failed"
    assert job.last_error is not None
    assert "Local template validation failed" in job.last_error


# ---------------------------------------------------------------------------
# Integration test 3: followup template wrong param count → local fail
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_followup_wrong_param_count_fails_locally(monkeypatch: Any) -> None:
    """Followup template: wrong param count → local fail before Meta."""
    send_template_calls: list[Any] = []

    job = _FakeJob(
        id=5003,
        company_id=KA,
        job_type=FOLLOWUP_JOB,
        payload={
            "kind": FOLLOWUP_JOB,
            "contact_name": "Hana Novak",
            "phone_e164": "+491777000111",
            "campaign_recipient_id": 99999,
        },
    )
    session = _FakeSession()
    _patch_common(monkeypatch)

    async def _fake_render(session, *, company_id, template_code, record, client):
        return "Hallo {{1}}, ...", 1, "de", _base_render_ctx()

    monkeypatch.setattr(ow, "_render_message", _fake_render)

    # Force wrong param count: 3 params for a 2-param template
    monkeypatch.setattr(
        ow,
        "build_template_params",
        lambda tpl, ctx: ["Hana", "https://n813709.alteg.io/", "extra"],
    )

    async def _fake_send(provider, sender_id, phone, template_name, language, params, **kw):
        send_template_calls.append(params)
        return "wamid.fake", None

    monkeypatch.setattr(ow, "safe_send_template", _fake_send)
    monkeypatch.setattr(ow.settings, "whatsapp_send_mode", "template")
    monkeypatch.setattr(ow.settings, "meta_newsletter_followup_header_image_url", "https://cdn.example.com/h.jpg")

    await ow._run_job_logic(session, job, provider=MagicMock())  # type: ignore[arg-type]

    assert len(send_template_calls) == 0
    assert job.status == "failed"
    assert job.last_error is not None
    assert "Local template validation failed" in job.last_error


# ---------------------------------------------------------------------------
# Integration test 4: valid monthly template → safe_send_template called
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_valid_monthly_template_calls_send(monkeypatch: Any) -> None:
    """Valid monthly template with all 3 non-empty params → send proceeds."""
    send_template_calls: list[Any] = []

    card_text = "Kundenkarte #0074454347287392"
    job = _FakeJob(
        id=5004,
        company_id=KA,
        job_type=MONTHLY_JOB,
        payload={
            "kind": MONTHLY_JOB,
            "loyalty_card_text": card_text,
            "contact_name": "Anna Müller",
            "phone_e164": "+491234567890",
        },
    )
    session = _FakeSession()
    _patch_common(monkeypatch)

    async def _fake_render(session, *, company_id, template_code, record, client):
        return "Hallo {{1}}, ...", 1, "de", _base_render_ctx(client_name="Anna Müller")

    monkeypatch.setattr(ow, "_render_message", _fake_render)

    async def _fake_send(provider, sender_id, phone, template_name, language, params, **kw):
        send_template_calls.append(list(params))
        return "wamid.fake5004", None

    monkeypatch.setattr(ow, "safe_send_template", _fake_send)
    monkeypatch.setattr(ow.settings, "whatsapp_send_mode", "template")
    monkeypatch.setattr(ow.settings, "meta_newsletter_monthly_header_image_url", "https://cdn.example.com/h.jpg")

    await ow._run_job_logic(session, job, provider=MagicMock())  # type: ignore[arg-type]

    assert len(send_template_calls) == 1, f"safe_send_template must be called once, got {len(send_template_calls)}"
    params = send_template_calls[0]
    assert len(params) == 3
    assert params[0] == "Anna Müller"
    assert params[2] == card_text
    assert job.status == "done", f"Expected done, got {job.status!r}"


# ---------------------------------------------------------------------------
# Integration test 5: text send path unaffected
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_text_send_path_unaffected_by_preflight(monkeypatch: Any) -> None:
    """send_mode=text: no template validation, safe_send called normally."""
    send_calls: list[Any] = []
    send_template_calls: list[Any] = []

    job = _FakeJob(
        id=5005,
        company_id=KA,
        job_type=MONTHLY_JOB,
        payload={
            "kind": MONTHLY_JOB,
            "phone_e164": "+491234567890",
        },
    )
    session = _FakeSession()
    _patch_common(monkeypatch)

    async def _fake_render(session, *, company_id, template_code, record, client):
        return "Hallo Anna, ...", 1, "de", _base_render_ctx(client_name="")

    monkeypatch.setattr(ow, "_render_message", _fake_render)

    async def _fake_send_text(provider, sender_id, phone, text, **kw):
        send_calls.append(text)
        return "wamid.text5005", None

    async def _fake_send_tpl(provider, sender_id, phone, template_name, language, params, **kw):
        send_template_calls.append(params)
        return "wamid.tpl5005", None

    monkeypatch.setattr(ow, "safe_send", _fake_send_text)
    monkeypatch.setattr(ow, "safe_send_template", _fake_send_tpl)
    monkeypatch.setattr(ow.settings, "whatsapp_send_mode", "text")

    await ow._run_job_logic(session, job, provider=MagicMock())  # type: ignore[arg-type]

    assert len(send_template_calls) == 0, "Template send must not be called in text mode"
    assert len(send_calls) == 1, f"safe_send must be called once in text mode, got {len(send_calls)}"
    assert job.status == "done", f"Expected done, got {job.status!r}"
