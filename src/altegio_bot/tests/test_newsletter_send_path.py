"""Tests: newsletter_new_clients_monthly send path uses v1 template consistently.

Context (run 8 bug):
  Campaign ran with OLD code that had v2 in the template map.
  resolve_meta_template returned "kitilash_ka_newsletter_new_clients_monthly_v2".
  build_template_params did not recognise v2 → returned [].
  safe_send_template was called with params=[] → Meta API rejected (expected 3 params).
  Newsletter job failed permanently after max_attempts.

  commit da0b6ba fixed:
    - meta_templates.py MAP: v2 → v1 for both KA (758285) and RA (1271200)
    - dead code "Legacy v1 – 2-params" block removed
    - all test expectations updated to v1

  These tests lock down the v1-consistent send path so the regression cannot
  be silently reintroduced.

Coverage:
  1. resolve_meta_template returns v1 for KA and RA
  2. KA and RA resolve to the SAME canonical template
  3. build_template_params returns exactly 3 params for v1
  4. loyalty_card_text (3rd param) comes from context
  5. Without loyalty_card_text in ctx → 3rd param is empty string (not missing)
  6. v2 is NOT recognised by build_template_params (returns [])
  7. Preview endpoint (normalize_meta_template_name) and outbox_worker DB lookup
     both land on the same template_code — no preview/send desync possible
  8. outbox_worker injects payload loyalty_card_text into msg_ctx
  9. outbox_worker calls safe_send_template with v1 template name and 3 params
     for a newsletter job with loyalty_card_text in payload
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

import altegio_bot.workers.outbox_worker as ow
from altegio_bot.meta_templates import build_template_params, resolve_meta_template
from altegio_bot.ops.campaigns_api import normalize_meta_template_name

KA = 758285
RA = 1271200
V1 = "kitilash_ka_newsletter_new_clients_monthly_v1"
V2 = "kitilash_ka_newsletter_new_clients_monthly_v2"
JOB_TYPE = "newsletter_new_clients_monthly"


# ---------------------------------------------------------------------------
# 1. Template resolution — KA and RA both resolve to v1
# ---------------------------------------------------------------------------


def test_resolve_newsletter_ka_returns_v1() -> None:
    """KA (Karlsruhe) monthly newsletter resolves to v1."""
    result = resolve_meta_template(KA, JOB_TYPE)
    assert result == V1, f"Expected {V1!r}, got {result!r}"


def test_resolve_newsletter_ra_returns_v1() -> None:
    """RA (Rastatt) monthly newsletter resolves to v1 (uses canonical ka_ template)."""
    result = resolve_meta_template(RA, JOB_TYPE)
    assert result == V1, f"Expected {V1!r}, got {result!r}"


def test_resolve_newsletter_ka_and_ra_same_template() -> None:
    """Both companies resolve to the identical canonical template."""
    ka = resolve_meta_template(KA, JOB_TYPE)
    ra = resolve_meta_template(RA, JOB_TYPE)
    assert ka == ra, f"KA={ka!r} ≠ RA={ra!r} — must be identical"
    assert ka is not None, "Template must not be None"


def test_resolve_newsletter_returns_non_none() -> None:
    """resolve_meta_template must not return None for newsletter jobs."""
    for company_id in (KA, RA):
        result = resolve_meta_template(company_id, JOB_TYPE)
        assert result is not None, f"company_id={company_id} resolved to None"


# ---------------------------------------------------------------------------
# 2. build_template_params — exactly 3 params for v1
# ---------------------------------------------------------------------------


_V1_CTX = {
    "client_name": "Maria Müller",
    "booking_link": "https://n813709.alteg.io/",
    "loyalty_card_text": "Kundenkarte #0074454347287392",
}


def test_newsletter_v1_returns_3_params() -> None:
    """v1 template always returns exactly 3 params."""
    params = build_template_params(V1, _V1_CTX)
    assert len(params) == 3, f"Expected 3 params, got {len(params)}: {params}"


def test_newsletter_v1_first_param_is_client_name() -> None:
    """First param is client_name."""
    params = build_template_params(V1, _V1_CTX)
    assert params[0] == "Maria Müller"


def test_newsletter_v1_second_param_is_booking_link() -> None:
    """Second param is booking_link."""
    params = build_template_params(V1, _V1_CTX)
    assert params[1] == "https://n813709.alteg.io/"


def test_newsletter_v1_third_param_is_loyalty_card_text() -> None:
    """Third param is loyalty_card_text from context."""
    params = build_template_params(V1, _V1_CTX)
    assert params[2] == "Kundenkarte #0074454347287392"


def test_newsletter_v1_loyalty_card_text_empty_when_missing() -> None:
    """When loyalty_card_text is absent from ctx, third param is empty string."""
    ctx = {"client_name": "Anna", "booking_link": "https://n813709.alteg.io/"}
    params = build_template_params(V1, ctx)
    assert len(params) == 3
    assert params[2] == "", f"Expected '', got {params[2]!r}"


def test_newsletter_v1_loyalty_card_text_passed_through() -> None:
    """Custom card text is faithfully forwarded to the third param."""
    card_text = "Kundenkarte #9900220845678901"
    ctx = {"client_name": "X", "booking_link": "https://n813709.alteg.io/", "loyalty_card_text": card_text}
    params = build_template_params(V1, ctx)
    assert params[2] == card_text


# ---------------------------------------------------------------------------
# 3. v2 is NOT recognised — guard against silent regression
# ---------------------------------------------------------------------------


def test_newsletter_v2_not_recognised_by_build_template_params() -> None:
    """v2 was never in Meta. build_template_params must return [] for it."""
    params = build_template_params(V2, _V1_CTX)
    assert params == [], f"Expected [] for unrecognised v2, got {params}"


# ---------------------------------------------------------------------------
# 4. Preview / send parity
#    The UI preview endpoint calls normalize_meta_template_name(meta_template_name)
#    to build the DB code for the template-text lookup.
#    The outbox_worker calls _load_template(template_code=job.job_type) for the
#    same lookup (job_type IS the template code).
#    Both must resolve to the identical code so there is no desync between
#    what the operator sees in the preview and what actually gets sent.
# ---------------------------------------------------------------------------


def test_ra_preview_and_send_resolve_to_same_db_code() -> None:
    """Rastatt monthly: normalize_meta_template_name(resolve_meta_template(RA)) == JOB_TYPE.

    The preview endpoint strips brand prefix + version suffix from the Meta template
    name to find the right row in message_templates.
    The outbox_worker passes job.job_type directly as template_code to _load_template.
    Both must land on the same DB record — i.e. the normalised name equals the job_type.

    If this fails it means the Meta template name in the MAP no longer round-trips
    through normalize_meta_template_name back to the canonical job_type, which would
    cause a desync between UI preview and the actual send.
    """
    send_template = resolve_meta_template(RA, JOB_TYPE)
    assert send_template is not None, "resolve_meta_template must not return None for RA monthly"

    preview_code = normalize_meta_template_name(send_template)
    assert preview_code == JOB_TYPE, (
        f"Preview normalises {send_template!r} → {preview_code!r}, "
        f"but outbox_worker looks up by code {JOB_TYPE!r}. "
        "These must be equal — otherwise UI preview and actual send use different DB rows."
    )


def test_ka_preview_and_send_resolve_to_same_db_code() -> None:
    """Karlsruhe monthly: same round-trip check as RA."""
    send_template = resolve_meta_template(KA, JOB_TYPE)
    assert send_template is not None

    preview_code = normalize_meta_template_name(send_template)
    assert preview_code == JOB_TYPE, (
        f"KA preview normalises {send_template!r} → {preview_code!r}, but outbox_worker looks up by code {JOB_TYPE!r}."
    )


# ---------------------------------------------------------------------------
# 5. outbox_worker: loyalty_card_text from payload reaches build_template_params
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


@pytest.mark.asyncio
async def test_outbox_worker_injects_loyalty_card_text_for_newsletter(monkeypatch: Any) -> None:
    """outbox_worker must read loyalty_card_text from payload and pass it to send_template."""
    card_text = "Kundenkarte #0074454347287392"
    captured_params: list[list[str]] = []
    captured_template: list[str] = []

    job = _FakeJob(
        id=100,
        company_id=KA,
        job_type=JOB_TYPE,
        payload={
            "kind": JOB_TYPE,
            "loyalty_card_text": card_text,
            "campaign_run_id": 8,
        },
    )
    session = _FakeSession()

    # Mock all dependencies that are not under test
    monkeypatch.setattr(ow, "_find_success_outbox", AsyncMock(return_value=None))
    monkeypatch.setattr(ow, "_find_existing_outbox", AsyncMock(return_value=None))
    monkeypatch.setattr(ow, "_load_record", AsyncMock(return_value=None))
    monkeypatch.setattr(ow, "_load_client", AsyncMock(return_value=None))
    monkeypatch.setattr(ow, "_apply_rate_limit", AsyncMock(return_value=None))

    # Fake _render_message returning a booking_link context
    async def _fake_render_message(session, *, company_id, template_code, record, client):
        ctx = {
            "client_name": "",  # CRM-only: no local client
            "booking_link": "https://n813709.alteg.io/",
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
        return "Sehr geehrte Frau {{1}}, Ihr Termin...", 1, "de", ctx

    monkeypatch.setattr(ow, "_render_message", _fake_render_message)

    # Intercept safe_send_template to capture what was sent
    async def _fake_safe_send_template(provider, sender_id, phone, template_name, language, params, **kw):
        captured_params.append(list(params))
        captured_template.append(template_name)
        return "wamid.fakeXYZ", None

    monkeypatch.setattr(ow, "safe_send_template", _fake_safe_send_template)

    # use_template = True (template mode)
    monkeypatch.setattr(ow.settings, "whatsapp_send_mode", "template")

    # Provide phone via payload (CRM-only client path)
    job.payload["phone_e164"] = "+491234567890"

    provider = MagicMock()

    await ow._run_job_logic(session, job, provider=provider)  # type: ignore[arg-type]

    assert job.status == "done", f"Expected job.status=done, got {job.status!r} last_error={job.last_error!r}"
    assert len(captured_template) == 1
    assert captured_template[0] == V1, f"Expected v1 template, got {captured_template[0]!r}"
    assert len(captured_params) == 1
    params = captured_params[0]
    assert len(params) == 3, f"Expected 3 params, got {len(params)}: {params}"
    assert params[2] == card_text, f"Third param must be loyalty_card_text, got {params[2]!r}"


@pytest.mark.asyncio
async def test_outbox_worker_newsletter_ra_uses_v1_template(monkeypatch: Any) -> None:
    """Rastatt (RA) newsletter job must resolve and send with v1 template."""
    captured_template: list[str] = []

    job = _FakeJob(
        id=101,
        company_id=RA,  # Rastatt
        job_type=JOB_TYPE,
        payload={
            "kind": JOB_TYPE,
            "loyalty_card_text": "Kundenkarte #0074454347287392",
            "phone_e164": "+49721987654",
        },
    )
    session = _FakeSession()

    monkeypatch.setattr(ow, "_find_success_outbox", AsyncMock(return_value=None))
    monkeypatch.setattr(ow, "_find_existing_outbox", AsyncMock(return_value=None))
    monkeypatch.setattr(ow, "_load_record", AsyncMock(return_value=None))
    monkeypatch.setattr(ow, "_load_client", AsyncMock(return_value=None))
    monkeypatch.setattr(ow, "_apply_rate_limit", AsyncMock(return_value=None))

    async def _fake_render_message(session, *, company_id, template_code, record, client):
        ctx = {
            "client_name": "",
            "booking_link": "https://n813709.alteg.io/",
            "sender_id": 2,
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
        return "Sehr geehrte...", 2, "de", ctx

    monkeypatch.setattr(ow, "_render_message", _fake_render_message)

    async def _fake_safe_send_template(provider, sender_id, phone, template_name, language, params, **kw):
        captured_template.append(template_name)
        return "wamid.fakeRA", None

    monkeypatch.setattr(ow, "safe_send_template", _fake_safe_send_template)
    monkeypatch.setattr(ow.settings, "whatsapp_send_mode", "template")

    provider = MagicMock()
    await ow._run_job_logic(session, job, provider=provider)  # type: ignore[arg-type]

    assert len(captured_template) == 1
    assert captured_template[0] == V1, (
        f"Rastatt must use v1, got {captured_template[0]!r}. If this is v2, the template MAP was not updated."
    )
