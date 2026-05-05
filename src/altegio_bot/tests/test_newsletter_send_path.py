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
from altegio_bot.meta_templates import (
    META_TEMPLATE_MAP,
    NEWSLETTER_FOLLOWUP_TEMPLATE,
    NEWSLETTER_IMAGE_HEADER_TEMPLATES,
    NEWSLETTER_MONTHLY_TEMPLATE,
    build_template_params,
    requires_image_header,
    resolve_meta_template,
)
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

    async def get(self, model: Any, pk: Any) -> None:
        return None

    async def flush(self) -> None:
        pass


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
            "contact_name": "Maria Müller",
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
    monkeypatch.setattr(ow, "_count_131026_failures", AsyncMock(return_value=0))

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
    monkeypatch.setattr(ow.settings, "meta_newsletter_monthly_header_image_url", "https://cdn.example.com/h.jpg")

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
            "contact_name": "Maria Müller",
            "phone_e164": "+49721987654",
        },
    )
    session = _FakeSession()

    monkeypatch.setattr(ow, "_find_success_outbox", AsyncMock(return_value=None))
    monkeypatch.setattr(ow, "_find_existing_outbox", AsyncMock(return_value=None))
    monkeypatch.setattr(ow, "_load_record", AsyncMock(return_value=None))
    monkeypatch.setattr(ow, "_load_client", AsyncMock(return_value=None))
    monkeypatch.setattr(ow, "_apply_rate_limit", AsyncMock(return_value=None))
    monkeypatch.setattr(ow, "_count_131026_failures", AsyncMock(return_value=0))

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
    monkeypatch.setattr(ow.settings, "meta_newsletter_monthly_header_image_url", "https://cdn.example.com/h.jpg")

    provider = MagicMock()
    await ow._run_job_logic(session, job, provider=provider)  # type: ignore[arg-type]

    assert len(captured_template) == 1
    assert captured_template[0] == V1, (
        f"Rastatt must use v1, got {captured_template[0]!r}. If this is v2, the template MAP was not updated."
    )


# ---------------------------------------------------------------------------
# 6. CRM-only client_name fallback from payload.contact_name
#    Regression for job 2031: card issued OK, but Meta rejected with
#    "Required parameter is missing" because client_name="" for CRM-only jobs.
# ---------------------------------------------------------------------------

FOLLOWUP_JOB_TYPE = "newsletter_new_clients_followup"
V1_FOLLOWUP = "kitilash_ka_newsletter_new_clients_followup_v1"


@dataclass
class _FakeClient:
    id: int
    display_name: str = "Anna Müller"
    phone_e164: str = "+4912345678"
    wa_opted_out: bool = False
    altegio_client_id: int | None = None


def _make_render_ctx(client_name: str = "") -> dict:
    return {
        "client_name": client_name,
        "booking_link": "https://n813709.alteg.io/",
        "loyalty_card_text": "",
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


@pytest.mark.asyncio
async def test_crm_only_monthly_newsletter_uses_contact_name_as_param1(monkeypatch: Any) -> None:
    """CRM-only monthly newsletter: template param #1 comes from payload.contact_name.

    Reproduces job 2031 bug: client is None so _render_message returns client_name="".
    Without the fix, build_template_params sends ["", booking_link, card_text] to Meta,
    which rejects with "Required parameter is missing".
    After the fix, outbox_worker patches msg_ctx["client_name"] from payload.contact_name
    before build_template_params is called.
    """
    captured_params: list[list[str]] = []
    card_text = "Kundenkarte #0000491787793093"

    job = _FakeJob(
        id=2031,
        company_id=KA,
        job_type=JOB_TYPE,
        payload={
            "kind": JOB_TYPE,
            "contact_name": "Dilek Celik",
            "loyalty_card_text": card_text,
            "phone_e164": "+491787793093",
        },
    )
    session = _FakeSession()

    monkeypatch.setattr(ow, "_find_success_outbox", AsyncMock(return_value=None))
    monkeypatch.setattr(ow, "_find_existing_outbox", AsyncMock(return_value=None))
    monkeypatch.setattr(ow, "_load_record", AsyncMock(return_value=None))
    monkeypatch.setattr(ow, "_load_client", AsyncMock(return_value=None))
    monkeypatch.setattr(ow, "_apply_rate_limit", AsyncMock(return_value=None))
    monkeypatch.setattr(ow, "_count_131026_failures", AsyncMock(return_value=0))

    async def _fake_render(session, *, company_id, template_code, record, client):
        # Simulates CRM-only: no local client → client_name=""
        return "Hallo {{1}}, ...", 1, "de", _make_render_ctx(client_name="")

    monkeypatch.setattr(ow, "_render_message", _fake_render)

    async def _fake_send_template(provider, sender_id, phone, template_name, language, params, **kw):
        captured_params.append(list(params))
        return "wamid.fake2031", None

    monkeypatch.setattr(ow, "safe_send_template", _fake_send_template)
    monkeypatch.setattr(ow.settings, "whatsapp_send_mode", "template")
    monkeypatch.setattr(ow.settings, "meta_newsletter_monthly_header_image_url", "https://cdn.example.com/h.jpg")

    provider = MagicMock()
    await ow._run_job_logic(session, job, provider=provider)  # type: ignore[arg-type]

    assert job.status == "done", f"Expected job.status='done', got {job.status!r} last_error={job.last_error!r}"
    assert len(captured_params) == 1, "safe_send_template must be called exactly once"
    params = captured_params[0]
    assert len(params) == 3, f"Monthly v1 template expects 3 params, got {len(params)}: {params}"
    assert params[0] == "Dilek Celik", (
        f"Template param #1 must be payload.contact_name='Dilek Celik', got {params[0]!r}. "
        "This was the bug in job 2031: empty client_name → Meta rejected with 'Required parameter is missing'."
    )
    assert params[2] == card_text, f"Template param #3 must be loyalty_card_text, got {params[2]!r}"


@pytest.mark.asyncio
async def test_crm_only_followup_newsletter_uses_contact_name_as_param1(monkeypatch: Any) -> None:
    """CRM-only followup newsletter: template param #1 also comes from payload.contact_name.

    The followup template has 2 params [client_name, booking_link].
    The same fallback logic must apply so followup does not send an empty first param.
    """
    captured_params: list[list[str]] = []

    job = _FakeJob(
        id=2099,
        company_id=KA,
        job_type=FOLLOWUP_JOB_TYPE,
        payload={
            "kind": FOLLOWUP_JOB_TYPE,
            "contact_name": "Hana Novak",
            "phone_e164": "+491777000111",
            "campaign_recipient_id": 99999,
        },
    )
    session = _FakeSession()

    monkeypatch.setattr(ow, "_find_success_outbox", AsyncMock(return_value=None))
    monkeypatch.setattr(ow, "_find_existing_outbox", AsyncMock(return_value=None))
    monkeypatch.setattr(ow, "_load_record", AsyncMock(return_value=None))
    monkeypatch.setattr(ow, "_load_client", AsyncMock(return_value=None))
    monkeypatch.setattr(ow, "_apply_rate_limit", AsyncMock(return_value=None))
    monkeypatch.setattr(ow, "_count_131026_failures", AsyncMock(return_value=0))

    async def _fake_render(session, *, company_id, template_code, record, client):
        return "Hallo {{1}}, ...", 1, "de", _make_render_ctx(client_name="")

    monkeypatch.setattr(ow, "_render_message", _fake_render)

    async def _fake_send_template(provider, sender_id, phone, template_name, language, params, **kw):
        captured_params.append(list(params))
        return "wamid.fakefollowup", None

    monkeypatch.setattr(ow, "safe_send_template", _fake_send_template)
    monkeypatch.setattr(ow.settings, "whatsapp_send_mode", "template")
    monkeypatch.setattr(ow.settings, "meta_newsletter_followup_header_image_url", "https://cdn.example.com/h.jpg")

    provider = MagicMock()
    await ow._run_job_logic(session, job, provider=provider)  # type: ignore[arg-type]

    assert job.status == "done", f"Expected job.status='done', got {job.status!r} last_error={job.last_error!r}"
    assert len(captured_params) == 1
    params = captured_params[0]
    assert len(params) == 2, f"Followup v1 template expects 2 params, got {len(params)}: {params}"
    assert params[0] == "Hana Novak", f"Template param #1 must be payload.contact_name='Hana Novak', got {params[0]!r}"


@pytest.mark.asyncio
async def test_local_client_monthly_newsletter_uses_display_name_not_payload(monkeypatch: Any) -> None:
    """Local-client newsletter: client.display_name takes priority over payload.contact_name.

    When a local Client row exists, _render_message already sets client_name=display_name.
    The payload fallback must NOT overwrite it even if contact_name is present in payload.
    """
    captured_params: list[list[str]] = []
    card_text = "Kundenkarte #0099887766554433"

    job = _FakeJob(
        id=3001,
        company_id=KA,
        job_type=JOB_TYPE,
        payload={
            "kind": JOB_TYPE,
            "contact_name": "WRONG NAME FROM PAYLOAD",
            "loyalty_card_text": card_text,
            "phone_e164": "+491234567890",
        },
    )
    session = _FakeSession()

    local_client = _FakeClient(id=42, display_name="Maria Müller", phone_e164="+491234567890")

    monkeypatch.setattr(ow, "_find_success_outbox", AsyncMock(return_value=None))
    monkeypatch.setattr(ow, "_find_existing_outbox", AsyncMock(return_value=None))
    monkeypatch.setattr(ow, "_load_record", AsyncMock(return_value=None))
    monkeypatch.setattr(ow, "_load_client", AsyncMock(return_value=local_client))
    monkeypatch.setattr(ow, "_apply_rate_limit", AsyncMock(return_value=None))
    monkeypatch.setattr(ow, "_count_131026_failures", AsyncMock(return_value=0))

    async def _fake_render(session, *, company_id, template_code, record, client):
        # Local client present: _render_message sets client_name=display_name
        name = client.display_name if client else ""
        return "Hallo {{1}}, ...", 1, "de", _make_render_ctx(client_name=name)

    monkeypatch.setattr(ow, "_render_message", _fake_render)

    async def _fake_send_template(provider, sender_id, phone, template_name, language, params, **kw):
        captured_params.append(list(params))
        return "wamid.fake3001", None

    monkeypatch.setattr(ow, "safe_send_template", _fake_send_template)
    monkeypatch.setattr(ow.settings, "whatsapp_send_mode", "template")
    monkeypatch.setattr(ow.settings, "meta_newsletter_monthly_header_image_url", "https://cdn.example.com/h.jpg")

    provider = MagicMock()
    await ow._run_job_logic(session, job, provider=provider)  # type: ignore[arg-type]

    assert job.status == "done", f"Expected job.status='done', got {job.status!r} last_error={job.last_error!r}"
    assert len(captured_params) == 1
    params = captured_params[0]
    assert params[0] == "Maria Müller", (
        f"Local-client param #1 must be client.display_name='Maria Müller', got {params[0]!r}. "
        "payload.contact_name must not overwrite a local client's display_name."
    )


@pytest.mark.asyncio
async def test_crm_only_no_contact_name_fails_preflight(monkeypatch: Any) -> None:
    """CRM-only job with no contact_name: preflight catches empty param #1 locally.

    Previously the worker would attempt the send with params[0]="" and Meta
    would reject with "Required parameter is missing".  Now the preflight
    validation catches this before any Meta request is made.

    Expected behaviour after the fix:
    - safe_send_template is NOT called
    - job.status == "failed"
    - job.last_error starts with "Local template validation failed"
    """
    send_template_calls: list[Any] = []
    card_text = "Kundenkarte #0000000000000000"

    job = _FakeJob(
        id=3002,
        company_id=KA,
        job_type=JOB_TYPE,
        payload={
            "kind": JOB_TYPE,
            "loyalty_card_text": card_text,
            "phone_e164": "+491111111111",
            # no contact_name — client_name stays ""
        },
    )
    session = _FakeSession()

    monkeypatch.setattr(ow, "_find_success_outbox", AsyncMock(return_value=None))
    monkeypatch.setattr(ow, "_find_existing_outbox", AsyncMock(return_value=None))
    monkeypatch.setattr(ow, "_load_record", AsyncMock(return_value=None))
    monkeypatch.setattr(ow, "_load_client", AsyncMock(return_value=None))
    monkeypatch.setattr(ow, "_apply_rate_limit", AsyncMock(return_value=None))
    monkeypatch.setattr(ow, "_count_131026_failures", AsyncMock(return_value=0))

    async def _fake_render(session, *, company_id, template_code, record, client):
        return "Hallo {{1}}, ...", 1, "de", _make_render_ctx(client_name="")

    monkeypatch.setattr(ow, "_render_message", _fake_render)

    async def _fake_send_template(provider, sender_id, phone, template_name, language, params, **kw):
        send_template_calls.append(list(params))
        return "wamid.fake3002", None

    monkeypatch.setattr(ow, "safe_send_template", _fake_send_template)
    monkeypatch.setattr(ow.settings, "whatsapp_send_mode", "template")
    # Header URL is set so the header check passes; preflight catches empty client_name.
    monkeypatch.setattr(ow.settings, "meta_newsletter_monthly_header_image_url", "https://cdn.example.com/h.jpg")

    provider = MagicMock()
    await ow._run_job_logic(session, job, provider=provider)  # type: ignore[arg-type]

    assert len(send_template_calls) == 0, (
        f"safe_send_template must NOT be called when preflight validation fails; got calls: {send_template_calls}"
    )
    assert job.status == "failed", f"Expected job.status='failed', got {job.status!r}"
    assert job.last_error is not None
    assert "Local template validation failed" in job.last_error, (
        f"last_error must indicate local validation failure, got: {job.last_error!r}"
    )


# ---------------------------------------------------------------------------
# 7. META_TEMPLATE_MAP uses constants (single source of truth)
# ---------------------------------------------------------------------------


def test_meta_template_map_ka_monthly_uses_constant() -> None:
    assert META_TEMPLATE_MAP[(758285, "newsletter_new_clients_monthly")] == NEWSLETTER_MONTHLY_TEMPLATE


def test_meta_template_map_ra_monthly_uses_constant() -> None:
    assert META_TEMPLATE_MAP[(1271200, "newsletter_new_clients_monthly")] == NEWSLETTER_MONTHLY_TEMPLATE


def test_meta_template_map_ka_followup_uses_constant() -> None:
    assert META_TEMPLATE_MAP[(758285, "newsletter_new_clients_followup")] == NEWSLETTER_FOLLOWUP_TEMPLATE


def test_meta_template_map_ra_followup_uses_constant() -> None:
    assert META_TEMPLATE_MAP[(1271200, "newsletter_new_clients_followup")] == NEWSLETTER_FOLLOWUP_TEMPLATE


# ---------------------------------------------------------------------------
# 8. Image header support — requires_image_header() and NEWSLETTER_IMAGE_HEADER_TEMPLATES
# ---------------------------------------------------------------------------


def test_requires_image_header_true_for_monthly() -> None:
    assert requires_image_header(NEWSLETTER_MONTHLY_TEMPLATE) is True


def test_requires_image_header_true_for_followup() -> None:
    assert requires_image_header(NEWSLETTER_FOLLOWUP_TEMPLATE) is True


def test_requires_image_header_false_for_other_templates() -> None:
    for name in (
        "kitilash_ka_record_created_v1",
        "kitilash_ka_reminder_24h_v1",
        "kitilash_ka_review_3d_v1",
        "kitilash_ka_repeat_10d_v1",
        "kitilash_ka_comeback_3d_v1",
    ):
        assert requires_image_header(name) is False, f"requires_image_header({name!r}) must be False"


def test_newsletter_image_header_templates_contains_both() -> None:
    assert NEWSLETTER_MONTHLY_TEMPLATE in NEWSLETTER_IMAGE_HEADER_TEMPLATES
    assert NEWSLETTER_FOLLOWUP_TEMPLATE in NEWSLETTER_IMAGE_HEADER_TEMPLATES
    assert len(NEWSLETTER_IMAGE_HEADER_TEMPLATES) == 2


# ---------------------------------------------------------------------------
# 8. outbox_worker fails fast when header URL not configured
# ---------------------------------------------------------------------------

_HEADER_URL = "https://cdn.example.com/newsletter_header.jpg"


def _base_patches(monkeypatch: Any) -> None:
    monkeypatch.setattr(ow, "_find_success_outbox", AsyncMock(return_value=None))
    monkeypatch.setattr(ow, "_find_existing_outbox", AsyncMock(return_value=None))
    monkeypatch.setattr(ow, "_load_record", AsyncMock(return_value=None))
    monkeypatch.setattr(ow, "_load_client", AsyncMock(return_value=None))
    monkeypatch.setattr(ow, "_apply_rate_limit", AsyncMock(return_value=None))
    monkeypatch.setattr(ow, "_count_131026_failures", AsyncMock(return_value=0))
    monkeypatch.setattr(ow.settings, "whatsapp_send_mode", "template")


async def _fake_render(session: Any, *, company_id: int, template_code: str, record: Any, client: Any) -> tuple:
    ctx = {
        "client_name": "Test User",
        "booking_link": "https://n813709.alteg.io/",
        "loyalty_card_text": "Kundenkarte #0000000000000001",
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
    return "Hallo {{1}}, ...", 1, "de", ctx


@pytest.mark.asyncio
async def test_outbox_worker_fails_fast_when_monthly_header_url_missing(monkeypatch: Any) -> None:
    """Worker must fail the job immediately when NEWSLETTER_MONTHLY header URL is not set."""
    send_calls: list[Any] = []

    job = _FakeJob(
        id=9001,
        company_id=KA,
        job_type=JOB_TYPE,
        payload={
            "kind": JOB_TYPE,
            "contact_name": "Test User",
            "loyalty_card_text": "Kundenkarte #0000000000000001",
            "phone_e164": "+491111222333",
        },
    )
    session = _FakeSession()
    _base_patches(monkeypatch)
    monkeypatch.setattr(ow, "_render_message", _fake_render)
    monkeypatch.setattr(ow.settings, "meta_newsletter_monthly_header_image_url", "")

    async def _fake_send_template(  # noqa: E501
        provider: Any, sender_id: int, phone: str, template_name: str, language: str, params: list, **kw: Any
    ) -> tuple:
        send_calls.append(True)
        return "wamid.fake", None

    monkeypatch.setattr(ow, "safe_send_template", _fake_send_template)

    provider = MagicMock()
    await ow._run_job_logic(session, job, provider=provider)  # type: ignore[arg-type]

    assert len(send_calls) == 0, "safe_send_template must NOT be called when header URL is missing"
    assert job.status == "failed", f"Expected job.status='failed', got {job.status!r}"
    assert job.last_error is not None
    assert "header" in job.last_error.lower(), f"last_error must mention 'header', got: {job.last_error!r}"


@pytest.mark.asyncio
async def test_outbox_worker_fails_fast_when_followup_header_url_missing(monkeypatch: Any) -> None:
    """Worker must fail followup newsletter job when followup header URL is not set."""
    send_calls: list[Any] = []

    job = _FakeJob(
        id=9002,
        company_id=KA,
        job_type=FOLLOWUP_JOB_TYPE,
        payload={
            "kind": FOLLOWUP_JOB_TYPE,
            "contact_name": "Test User",
            "phone_e164": "+491111222444",
            "campaign_recipient_id": 99999,
        },
    )
    session = _FakeSession()
    _base_patches(monkeypatch)
    monkeypatch.setattr(ow, "_render_message", _fake_render)
    monkeypatch.setattr(ow.settings, "meta_newsletter_followup_header_image_url", "")

    async def _fake_send_template(  # noqa: E501
        provider: Any, sender_id: int, phone: str, template_name: str, language: str, params: list, **kw: Any
    ) -> tuple:
        send_calls.append(True)
        return "wamid.fake", None

    monkeypatch.setattr(ow, "safe_send_template", _fake_send_template)

    provider = MagicMock()
    await ow._run_job_logic(session, job, provider=provider)  # type: ignore[arg-type]

    assert len(send_calls) == 0, "safe_send_template must NOT be called when header URL is missing"
    assert job.status == "failed"
    assert job.last_error is not None
    assert "header" in job.last_error.lower()


# ---------------------------------------------------------------------------
# 9. outbox_worker passes header_image_url to safe_send_template when configured
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_outbox_worker_passes_header_url_for_monthly(monkeypatch: Any) -> None:
    """Worker must pass header_image_url kwarg to safe_send_template for monthly newsletter."""
    captured_kw: list[dict] = []

    job = _FakeJob(
        id=9003,
        company_id=KA,
        job_type=JOB_TYPE,
        payload={
            "kind": JOB_TYPE,
            "contact_name": "Test User",
            "loyalty_card_text": "Kundenkarte #0000000000000002",
            "phone_e164": "+491111222555",
        },
    )
    session = _FakeSession()
    _base_patches(monkeypatch)
    monkeypatch.setattr(ow, "_render_message", _fake_render)
    monkeypatch.setattr(ow.settings, "meta_newsletter_monthly_header_image_url", _HEADER_URL)

    async def _fake_send_template(  # noqa: E501
        provider: Any, sender_id: int, phone: str, template_name: str, language: str, params: list, **kw: Any
    ) -> tuple:
        captured_kw.append(dict(kw))
        return "wamid.fake9003", None

    monkeypatch.setattr(ow, "safe_send_template", _fake_send_template)

    provider = MagicMock()
    await ow._run_job_logic(session, job, provider=provider)  # type: ignore[arg-type]

    assert job.status == "done", f"Expected done, got {job.status!r} error={job.last_error!r}"
    assert len(captured_kw) == 1
    assert captured_kw[0].get("header_image_url") == _HEADER_URL, (
        f"header_image_url must be {_HEADER_URL!r}, got {captured_kw[0].get('header_image_url')!r}"
    )


@pytest.mark.asyncio
async def test_outbox_worker_passes_header_url_for_followup(monkeypatch: Any) -> None:
    """Worker must pass header_image_url kwarg to safe_send_template for followup newsletter."""
    captured_kw: list[dict] = []
    followup_header = "https://cdn.example.com/followup_header.jpg"

    job = _FakeJob(
        id=9004,
        company_id=KA,
        job_type=FOLLOWUP_JOB_TYPE,
        payload={
            "kind": FOLLOWUP_JOB_TYPE,
            "contact_name": "Test User",
            "phone_e164": "+491111222666",
            "campaign_recipient_id": 99999,
        },
    )
    session = _FakeSession()
    _base_patches(monkeypatch)
    monkeypatch.setattr(ow, "_render_message", _fake_render)
    monkeypatch.setattr(ow.settings, "meta_newsletter_followup_header_image_url", followup_header)

    async def _fake_send_template(  # noqa: E501
        provider: Any, sender_id: int, phone: str, template_name: str, language: str, params: list, **kw: Any
    ) -> tuple:
        captured_kw.append(dict(kw))
        return "wamid.fake9004", None

    monkeypatch.setattr(ow, "safe_send_template", _fake_send_template)

    provider = MagicMock()
    await ow._run_job_logic(session, job, provider=provider)  # type: ignore[arg-type]

    assert job.status == "done", f"Expected done, got {job.status!r} error={job.last_error!r}"
    assert len(captured_kw) == 1
    assert captured_kw[0].get("header_image_url") == followup_header


# ---------------------------------------------------------------------------
# 10. MetaCloudProvider.send_template() builds HEADER component
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_meta_cloud_send_template_includes_header_component_when_url_set() -> None:
    """When header_image_url is provided, payload components must start with HEADER."""
    from altegio_bot.providers.meta_cloud import MetaCloudProvider

    captured_payload: list[dict] = []

    provider = MetaCloudProvider.__new__(MetaCloudProvider)
    provider._access_token = "test-token"
    provider._api_version = "v20.0"
    provider._graph_url = "https://graph.facebook.com"
    provider._allow_real_send = True
    provider._sender_cache = {1: "12345678901"}

    class FakeResp:
        status_code = 200

        def json(self) -> dict:
            return {"messages": [{"id": "wamid.testHEADER"}]}

    class FakeClient:
        async def post(self, url: str, headers: dict, json: dict) -> FakeResp:
            captured_payload.append(json)
            return FakeResp()

    provider._client = FakeClient()  # type: ignore[assignment]

    await provider.send_template(
        sender_id=1,
        phone_e164="+491234567890",
        template_name=NEWSLETTER_MONTHLY_TEMPLATE,
        language="de",
        params=["Anna", "https://booking.link/", "Kundenkarte #001"],
        header_image_url=_HEADER_URL,
    )

    assert len(captured_payload) == 1
    components = captured_payload[0]["template"]["components"]
    assert len(components) == 2, f"Expected 2 components (HEADER + BODY), got {len(components)}"
    assert components[0]["type"] == "header"
    assert components[0]["parameters"][0]["type"] == "image"
    assert components[0]["parameters"][0]["image"]["link"] == _HEADER_URL
    assert components[1]["type"] == "body"


@pytest.mark.asyncio
async def test_meta_cloud_send_template_omits_header_component_when_url_none() -> None:
    """When header_image_url is None, payload must have only BODY component."""
    from altegio_bot.providers.meta_cloud import MetaCloudProvider

    captured_payload: list[dict] = []

    provider = MetaCloudProvider.__new__(MetaCloudProvider)
    provider._access_token = "test-token"
    provider._api_version = "v20.0"
    provider._graph_url = "https://graph.facebook.com"
    provider._allow_real_send = True
    provider._sender_cache = {1: "12345678901"}

    class FakeResp:
        status_code = 200

        def json(self) -> dict:
            return {"messages": [{"id": "wamid.testNOHEADER"}]}

    class FakeClient:
        async def post(self, url: str, headers: dict, json: dict) -> FakeResp:
            captured_payload.append(json)
            return FakeResp()

    provider._client = FakeClient()  # type: ignore[assignment]

    await provider.send_template(
        sender_id=1,
        phone_e164="+491234567890",
        template_name="kitilash_ka_record_created_v1",
        language="de",
        params=["Anna", "Tanja", "10.02.2026", "14:00", "Service", "60.00", "https://link"],
        header_image_url=None,
    )

    assert len(captured_payload) == 1
    components = captured_payload[0]["template"]["components"]
    assert len(components) == 1, f"Expected 1 component (BODY only), got {len(components)}"
    assert components[0]["type"] == "body"
