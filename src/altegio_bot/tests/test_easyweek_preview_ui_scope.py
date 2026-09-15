"""The preview editor's scope and generation rules, executed (§37.1).

A preview on screen describes ONE saved run. Which provider's schema to draw it
with, whether Run may be offered, and whether a late answer may paint at all are
decisions about THAT run — not about whatever the dropdown says at the moment
the answer lands.

Those decisions are small pure functions in the page script, and this file runs
the shipped source of them under `node` rather than re-implementing them here: a
test that reasons over its own copy proves the copy.

`node` is used because it is already on the machine and needs no browser. Where
it is absent the execution tests skip, and the rest of the file — which asserts
on the rendered HTML the server produces — still runs.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

import altegio_bot.campaigns.runner as runner_module
import altegio_bot.ops.campaigns_api as campaigns_api_module
import altegio_bot.ops.router as ops_router_module
from altegio_bot.main import app
from altegio_bot.models.models import (
    PROVIDER_ALTEGIO,
    PROVIDER_EASYWEEK,
    MessageTemplate,
)
from altegio_bot.ops.auth import require_ops_auth
from altegio_bot.tests.easyweek_voucher_delivery_fixtures import (  # noqa: F401 - fixtures
    COMPANY_ID,
    seed_recipient,
)

NODE = shutil.which("node")
needs_node = pytest.mark.skipif(NODE is None, reason="node is not installed; JS execution tests need it")

# Synthetic branches. Only Karlsruhe has an owner-approved voucher contract.
KARLSRUHE = 322579
DURLACH = 308697
RASTATT = 315607


@pytest_asyncio.fixture
async def http_client(session_maker, monkeypatch) -> AsyncClient:
    monkeypatch.setattr(ops_router_module, "SessionLocal", session_maker)
    monkeypatch.setattr(campaigns_api_module, "SessionLocal", session_maker)
    monkeypatch.setattr(runner_module, "SessionLocal", session_maker)
    monkeypatch.setitem(app.dependency_overrides, require_ops_auth, lambda: None)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        yield client


@pytest.fixture
def three_branches(monkeypatch) -> None:
    """A registry with all three branches, so branch-specific rules are testable."""
    from altegio_bot.settings import settings

    monkeypatch.setattr(
        settings,
        "easyweek_location_map",
        json.dumps(
            {
                "durlach": {
                    "location_id": DURLACH,
                    "location_uuid": "11111111-1111-4111-8111-111111111111",
                    "meta_template_prefix": "du",
                    "booking_page_url": "https://durlach.example.invalid/",
                },
                "rastatt": {
                    "location_id": RASTATT,
                    "location_uuid": "22222222-2222-4222-8222-222222222222",
                    "meta_template_prefix": "ra",
                    "booking_page_url": "https://rastatt.example.invalid/",
                },
                "karlsruhe": {
                    "location_id": KARLSRUHE,
                    "location_uuid": "8395fab6-7ee8-4702-88d9-fd78f92539c1",
                    "meta_template_prefix": "ka",
                    "booking_page_url": "https://karlsruhe.example.invalid/",
                },
            }
        ),
        raising=False,
    )


# ---------------------------------------------------------------------------
# Extracting and running the shipped decisions
# ---------------------------------------------------------------------------


def _page_script(page: str) -> str:
    """The page's own script text, as served."""
    blocks = re.findall(r"<script>(.*?)</script>", page, re.DOTALL)
    assert blocks, "the page serves no script"
    return max(blocks, key=len)


def _function_source(script: str, name: str) -> str:
    """One function declaration, sliced by matching braces.

    Deliberately crude and deliberately exact: it returns the shipped bytes of
    that function, so what runs below is what runs in a browser.
    """
    marker = f"function {name}("
    start = script.find(marker)
    assert start != -1, f"{name} is not in the page script"
    # `async` is part of the declaration; slicing from `function` would hand the
    # test a synchronous copy of an asynchronous function.
    prefix = "async "
    if script[start - len(prefix) : start] == prefix:
        start -= len(prefix)
    open_brace = script.index("{", start)
    depth = 0
    for index in range(open_brace, len(script)):
        if script[index] == "{":
            depth += 1
        elif script[index] == "}":
            depth -= 1
            if depth == 0:
                return script[start : index + 1]
    raise AssertionError(f"unbalanced braces in {name}")


def _run_node(source: str, driver: str) -> Any:
    """Execute shipped functions plus a driver, and read back its JSON."""
    assert NODE is not None
    with tempfile.TemporaryDirectory() as tmp:
        script = Path(tmp) / "case.mjs"
        script.write_text(source + "\n" + driver, encoding="utf-8")
        result = subprocess.run(  # noqa: S603 - fixed interpreter, generated file
            [NODE, str(script)],
            capture_output=True,
            text=True,
            timeout=60,
        )
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


async def _decisions(http_client: AsyncClient) -> str:
    """The four pure decision functions, as the page ships them."""
    script = _page_script((await http_client.get("/ops/campaigns/new-clients")).text)
    return "\n".join(
        _function_source(script, name) for name in ("scopeMatches", "mayRender", "mayRunFromPreview", "schemaProvider")
    )


@needs_node
@pytest.mark.asyncio
async def test_duplicate_manual_add_uses_a_warning_not_a_success_alert(
    http_client, session_maker, configuration
) -> None:
    run_id, _ = await seed_recipient(session_maker)
    page = (await http_client.get(f"/ops/campaigns/{run_id}")).text
    source = _function_source(_page_script(page), "submitAddRecipient")
    driver = """
let IS_EASYWEEK = true;
let ADD_RECIPIENT_MODE = "manual";
const MANUAL_ADD_REASONS = {};
const elements = {
  "add-phone": {value: "+4915100000042"},
  "add-recipient-alert": {innerHTML: ""},
};
globalThis.document = {getElementById: id => elements[id] || null};
let reply = {action: "unchanged", recipient_id: 5002, candidates_count: 1};
const urls = [];
globalThis.fetch = async url => {
  urls.push(url);
  return {ok: true, json: async () => reply};
};
await submitAddRecipient(38);
const duplicate = elements["add-recipient-alert"].innerHTML;
reply = {action: "created", recipient_id: 5003, candidates_count: 2};
await submitAddRecipient(38);
const created = elements["add-recipient-alert"].innerHTML;
ADD_RECIPIENT_MODE = "test";
reply = {action: "unchanged", recipient_id: 5002, candidates_count: 1};
await submitAddRecipient(38);
const testRecipient = elements["add-recipient-alert"].innerHTML;
IS_EASYWEEK = false;
ADD_RECIPIENT_MODE = "manual";
await submitAddRecipient(38);
const altegio = elements["add-recipient-alert"].innerHTML;
console.log(JSON.stringify({duplicate, created, testRecipient, altegio, urls}));
"""

    alerts = _run_node(source, driver)
    assert "alert-warning" in alerts["duplicate"]
    assert "Повторное добавление не изменило список" in alerts["duplicate"]
    assert "alert-success" not in alerts["duplicate"]
    assert "alert-success" in alerts["created"]
    assert "Получатель добавлен" in alerts["created"]
    assert "alert-warning" in alerts["testRecipient"]
    assert "Повторное добавление не изменило список" in alerts["testRecipient"]
    assert "alert-success" in alerts["altegio"]
    assert alerts["urls"] == [
        "/ops/campaigns/runs/38/recipients/add-manual",
        "/ops/campaigns/runs/38/recipients/add-manual",
        "/ops/campaigns/runs/38/recipients/add",
        "/ops/campaigns/runs/38/recipients/add",
    ]


# ---------------------------------------------------------------------------
# The whole page script parses
# ---------------------------------------------------------------------------


@needs_node
@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["/ops/campaigns/new-clients"])
async def test_the_embedded_javascript_parses(http_client, path: str) -> None:
    """An f-string built page is one stray brace away from broken JS."""
    assert NODE is not None
    script = _page_script((await http_client.get(path)).text)
    with tempfile.TemporaryDirectory() as tmp:
        target = Path(tmp) / "page.js"
        target.write_text(script, encoding="utf-8")
        result = subprocess.run(  # noqa: S603 - fixed interpreter, generated file
            [NODE, "--check", str(target)],
            capture_output=True,
            text=True,
            timeout=60,
        )
    assert result.returncode == 0, result.stderr


# ---------------------------------------------------------------------------
# Scope and generation, executed
# ---------------------------------------------------------------------------


@needs_node
@pytest.mark.asyncio
async def test_a_late_preview_answer_is_dropped_after_a_provider_switch(http_client) -> None:
    """POST in flight, operator switches provider, answer lands. It must not paint."""
    decisions = await _decisions(http_client)
    driver = """
const asked = {provider: "easyweek", companyId: "322579"};
const token = 1;
// The operator switched to Altegio while the POST was in flight. The token is
// still current — only the scope moved.
const now = {provider: "altegio", companyId: "758285"};
console.log(JSON.stringify({rendered: mayRender(token, token, asked, now)}));
"""
    assert _run_node(decisions, driver)["rendered"] is False


@needs_node
@pytest.mark.asyncio
async def test_a_late_preview_answer_is_dropped_after_another_preview_starts(http_client) -> None:
    decisions = await _decisions(http_client)
    driver = """
const asked = {provider: "easyweek", companyId: "322579"};
// A second preview was started, so the generation moved on.
console.log(JSON.stringify({rendered: mayRender(1, 2, asked, asked)}));
"""
    assert _run_node(decisions, driver)["rendered"] is False


@needs_node
@pytest.mark.asyncio
async def test_a_switch_between_easyweek_branches_drops_the_old_snapshot(http_client) -> None:
    decisions = await _decisions(http_client)
    driver = """
const asked = {provider: "easyweek", companyId: "308697"};
const now = {provider: "easyweek", companyId: "322579"};
console.log(JSON.stringify({
  rendered: mayRender(3, 3, asked, now),
  sameBranch: mayRender(3, 3, asked, asked),
}));
"""
    answer = _run_node(decisions, driver)
    assert answer["rendered"] is False
    assert answer["sameBranch"] is True


@needs_node
@pytest.mark.asyncio
async def test_two_recipient_loads_finishing_backwards_leave_only_the_newest(http_client) -> None:
    """A finishes after B. Only B may paint — token equality is the whole rule."""
    decisions = await _decisions(http_client)
    driver = """
const scope = {provider: "easyweek", companyId: "322579"};
// A took token 1, B took token 2; the counter now reads 2.
const b = mayRender(2, 2, scope, scope);
const aLate = mayRender(1, 2, scope, scope);
console.log(JSON.stringify({b: b, aLate: aLate}));
"""
    answer = _run_node(decisions, driver)
    assert answer["b"] is True
    assert answer["aLate"] is False


@needs_node
@pytest.mark.asyncio
async def test_run_is_offered_only_for_a_matching_runnable_altegio_preview(http_client) -> None:
    decisions = await _decisions(http_client)
    driver = """
const altegio = {provider: "altegio", companyId: "758285"};
const easyweek = {provider: "easyweek", companyId: "322579"};
const cases = {
  runnable_altegio: mayRunFromPreview({runId: 1, provider: "altegio", companyId: "758285", runnable: true}, altegio),
  not_runnable: mayRunFromPreview({runId: 1, provider: "altegio", companyId: "758285", runnable: false}, altegio),
  other_branch: mayRunFromPreview({runId: 1, provider: "altegio", companyId: "1271200", runnable: true}, altegio),
  easyweek_run: mayRunFromPreview({runId: 1, provider: "easyweek", companyId: "322579", runnable: true}, easyweek),
  no_context: mayRunFromPreview(null, altegio),
};
console.log(JSON.stringify(cases));
"""
    answer = _run_node(decisions, driver)
    assert answer["runnable_altegio"] is True
    assert answer["not_runnable"] is False
    assert answer["other_branch"] is False
    # EasyWeek never qualifies, whatever the server said.
    assert answer["easyweek_run"] is False
    assert answer["no_context"] is False


@needs_node
@pytest.mark.asyncio
async def test_the_table_schema_follows_the_run_not_the_dropdown(http_client) -> None:
    decisions = await _decisions(http_client)
    driver = """
console.log(JSON.stringify({
  easyweek: schemaProvider({runId: 1, provider: "easyweek", companyId: "322579"}),
  altegio: schemaProvider({runId: 2, provider: "altegio", companyId: "758285"}),
  unknown: schemaProvider(null),
}));
"""
    answer = _run_node(decisions, driver)
    assert answer["easyweek"] == "easyweek"
    assert answer["altegio"] == "altegio"
    assert answer["unknown"] == "altegio"


@needs_node
@pytest.mark.asyncio
async def test_a_late_template_answer_for_another_branch_is_ignored(http_client) -> None:
    """The template probe has its own token, checked against its own scope."""
    decisions = await _decisions(http_client)
    driver = """
const asked = {provider: "easyweek", companyId: "308697"};
const now = {provider: "easyweek", companyId: "322579"};
console.log(JSON.stringify({rendered: mayRender(5, 6, asked, now)}));
"""
    assert _run_node(decisions, driver)["rendered"] is False


# ---------------------------------------------------------------------------
# What the page wires those decisions to
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_page_invalidates_the_preview_on_every_switch(http_client) -> None:
    page = (await http_client.get("/ops/campaigns/new-clients")).text
    script = _page_script(page)

    invalidate = _function_source(script, "invalidatePreviewContext")
    # It takes the surface down through the single function that owns that, and
    # that function is what moves the page clock — revoking everything still in
    # flight. The card select's fate is the caller's decision, not its own.
    assert "takeDownPreview();" in invalidate
    takedown = _function_source(script, "takeDownPreview")
    assert "PAGE_EPOCH += 1;" in takedown
    assert "resetPreviewSurface();" in takedown
    surface = _function_source(script, "resetPreviewSurface")
    assert "PREVIEW_CONTEXT = null;" in surface
    assert "previewRunId = null;" in surface
    assert "applyRunAvailability();" in surface
    # And it does not delete anything: no fetch, no discard, no delete.
    for destructive in ("fetch(", "discard", "delete"):
        assert destructive not in invalidate.lower()

    # Called from the provider switch and from both branch selectors.
    assert script.count("invalidatePreviewContext();") >= 3


@pytest.mark.asyncio
async def test_returning_to_altegio_reloads_that_branch_cards(http_client) -> None:
    script = _page_script((await http_client.get("/ops/campaigns/new-clients")).text)
    switch = _function_source(script, "onProviderChange")

    # Leaving EasyWeek reloads the Altegio list for the branch selected now,
    # and the card types with it.
    assert "loadOutstandingCards(altegioCompany.value)" in switch
    assert "loadCardTypes();" in switch
    # And the button is disabled until that load succeeds — through the one
    # function that owns taking the panel down.
    assert "resetOutstandingPanel();" in switch
    assert "deleteBtn.disabled = true;" in _function_source(script, "resetOutstandingPanel")


@pytest.mark.asyncio
async def test_delete_is_disabled_until_a_scope_is_loaded(http_client) -> None:
    page = (await http_client.get("/ops/campaigns/new-clients")).text
    script = _page_script(page)

    # The button ships disabled.
    assert 'id="btn-delete-outstanding" class="btn btn-sm btn-danger ms-auto" disabled' in page
    load = _function_source(script, "loadOutstandingCards")
    # Every path through the loader starts by clearing the scope and disabling,
    # and only a painted table re-enables.
    assert "OUTSTANDING_SCOPE = null;" in load
    assert "deleteBtnEl.disabled = true;" in load
    assert "okDeleteBtn.disabled = false;" in load
    assert load.index("OUTSTANDING_SCOPE = scope;") < load.index("okDeleteBtn.disabled = false;")


# ---------------------------------------------------------------------------
# The branch-specific template status
# ---------------------------------------------------------------------------


async def _template_status(http_client: AsyncClient, company_id: int) -> dict[str, Any]:
    resp = await http_client.get(
        "/ops/campaigns/new-clients/easyweek-template-status",
        params={"company_id": company_id},
    )
    assert resp.status_code == 200, resp.text
    return resp.json()


def _row(**changes: Any) -> dict[str, Any]:
    from altegio_bot.campaigns.easyweek_voucher_delivery import template_contract

    fields: dict[str, Any] = {
        "provider": PROVIDER_EASYWEEK,
        "company_id": KARLSRUHE,
        "code": template_contract.VOUCHER_TEMPLATE_CODE,
        "language": template_contract.VOUCHER_TEMPLATE_LANGUAGE,
        "body": template_contract.VOUCHER_TEMPLATE_BODY,
        "meta_template_name": template_contract.VOUCHER_META_TEMPLATE_NAME,
        "is_active": True,
    }
    fields.update(changes)
    return fields


async def _seed_template(session_maker, **changes: Any) -> None:
    async with session_maker() as session:
        async with session.begin():
            session.add(MessageTemplate(**_row(**changes)))


@pytest.mark.asyncio
async def test_a_branch_without_its_own_contract_is_never_green(session_maker, three_branches, http_client) -> None:
    """Karlsruhe's approval is Karlsruhe's. It is not a fallback for anyone."""
    from altegio_bot.campaigns.easyweek_voucher_delivery import template_contract

    await _seed_template(session_maker)

    for branch in (DURLACH, RASTATT):
        status = await _template_status(http_client, branch)
        assert status["configured"] is False
        assert status["reason"] == "branch_contract_not_approved"
        # And the approved name is not handed to them.
        assert template_contract.VOUCHER_META_TEMPLATE_NAME not in json.dumps(status)


@pytest.mark.asyncio
async def test_the_approved_branch_is_green_only_with_an_exact_row(session_maker, three_branches, http_client) -> None:
    from altegio_bot.campaigns.easyweek_voucher_delivery import template_contract

    missing = await _template_status(http_client, KARLSRUHE)
    assert missing["configured"] is False
    assert missing["reason"] == "template_row_missing"

    await _seed_template(session_maker)
    proven = await _template_status(http_client, KARLSRUHE)

    assert proven["configured"] is True
    assert proven["reason"] is None
    assert proven["meta_template_name"] == template_contract.VOUCHER_META_TEMPLATE_NAME
    assert proven["language"] == template_contract.VOUCHER_TEMPLATE_LANGUAGE
    assert proven["provider"] == PROVIDER_EASYWEEK


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes, expected",
    [
        pytest.param({"language": "en"}, "template_row_missing", id="wrong_language"),
        pytest.param({"meta_template_name": "something_else"}, None, id="wrong_name"),
        pytest.param({"body": "a body nobody approved"}, None, id="wrong_body"),
        pytest.param({"is_active": False}, "template_row_missing", id="inactive"),
    ],
)
async def test_a_row_that_does_not_match_the_contract_is_never_green(
    session_maker, three_branches, http_client, changes: dict[str, Any], expected: str | None
) -> None:
    await _seed_template(session_maker, **changes)

    status = await _template_status(http_client, KARLSRUHE)

    assert status["configured"] is False
    if expected is not None:
        assert status["reason"] == expected


@pytest.mark.asyncio
async def test_two_active_rows_are_an_ambiguity_not_a_first_match(session_maker, three_branches, http_client) -> None:
    """A row taken with LIMIT 1 would be a row, not a proof."""
    await _seed_template(session_maker)
    await _seed_template(session_maker)

    status = await _template_status(http_client, KARLSRUHE)

    assert status["configured"] is False
    assert status["reason"] == "template_rows_ambiguous"


@pytest.mark.asyncio
async def test_an_altegio_row_with_the_same_code_proves_nothing(session_maker, three_branches, http_client) -> None:
    await _seed_template(session_maker, provider=PROVIDER_ALTEGIO)

    status = await _template_status(http_client, KARLSRUHE)

    assert status["configured"] is False
    assert status["reason"] == "template_row_missing"


@pytest.mark.asyncio
async def test_the_static_block_asserts_no_template_name(http_client, three_branches) -> None:
    """The name appears only after a branch's row has been proven."""
    page = (await http_client.get("/ops/campaigns/new-clients")).text

    block = page[page.find("Шаблон EasyWeek") : page.find("БЛОК ШАБЛОНА: ALTEGIO")]
    assert 'id="ew-template-name"' in block
    # Not hardcoded into the markup for every branch.
    assert "kitilash_ka_new_client_voucher_v1" not in block


# ---------------------------------------------------------------------------
# What the EasyWeek pages no longer show
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_easyweek_detail_page_drops_the_altegio_only_fields(
    session_maker, configuration, http_client
) -> None:
    run_id, _ = await seed_recipient(session_maker)

    page = (await http_client.get(f"/ops/campaigns/{run_id}")).text

    assert "Card Type ID" not in page
    assert ">Queued<" not in page
    assert "Follow-up eligibility" not in page
    # The snapshot metrics that DO mean something stay.
    assert ">Seen<" in page
    assert ">Candidates<" in page
    assert "Excluded (EasyWeek)" in page


@pytest.mark.asyncio
async def test_the_altegio_detail_page_keeps_them(session_maker, configuration, http_client) -> None:
    run_id, _ = await seed_recipient(session_maker, provider=PROVIDER_ALTEGIO)

    page = (await http_client.get(f"/ops/campaigns/{run_id}")).text

    assert "Card Type ID" in page
    assert ">Queued<" in page
    assert ">Seen<" in page


@pytest.mark.asyncio
async def test_the_easyweek_recipients_page_drops_the_send_path_columns(
    session_maker, configuration, http_client
) -> None:
    run_id, _ = await seed_recipient(session_maker)

    page = (await http_client.get(f"/ops/campaigns/{run_id}/recipients")).text

    for gone in ("Card #", "Msg Job ID", "Sent", "Replied", "Booked", "Followup"):
        assert f"<th>{gone}</th>" not in page
    for kept in ("Basis", "Excluded Reason", "EasyWeek customer"):
        assert f"<th>{kept}</th>" in page
    # The filter offers only the statuses an EasyWeek row can have.
    assert "card_issued" not in page
    assert "cleanup_failed" not in page


@pytest.mark.asyncio
async def test_the_altegio_recipients_page_is_unchanged(session_maker, configuration, http_client) -> None:
    run_id, _ = await seed_recipient(session_maker, provider=PROVIDER_ALTEGIO)

    page = (await http_client.get(f"/ops/campaigns/{run_id}/recipients")).text

    for kept in ("Card #", "Msg Job ID", "Sent", "Read", "Replied", "Booked", "Followup"):
        assert f"<th>{kept}</th>" in page
    assert "card_issued" in page


@pytest.mark.asyncio
async def test_no_easyweek_surface_leaks_a_customer_uuid(session_maker, configuration, http_client) -> None:
    from altegio_bot.tests.test_easyweek_manual_recipient import CUSTOMER_UUID

    run_id, _ = await seed_recipient(session_maker)

    for path in (f"/ops/campaigns/{run_id}", f"/ops/campaigns/{run_id}/recipients"):
        page = (await http_client.get(path)).text
        assert CUSTOMER_UUID not in page
        assert "easyweek_customer_uuid" not in page


@pytest.mark.asyncio
async def test_the_easyweek_send_path_is_still_refused(session_maker, configuration, http_client) -> None:
    """Nothing in this fix opened anything."""
    from sqlalchemy import func, select

    from altegio_bot.models.models import CampaignRun, MessageJob, OutboxMessage

    run_id, _ = await seed_recipient(session_maker)
    async with session_maker() as session:
        before = {
            model.__name__: await session.scalar(select(func.count()).select_from(model))
            for model in (CampaignRun, MessageJob, OutboxMessage)
        }

    resp = await http_client.post(
        "/ops/campaigns/new-clients/run",
        json={
            "provider": "easyweek",
            "company_id": COMPANY_ID,
            "location_id": COMPANY_ID,
            "period_start": "2026-08-01T00:00:00Z",
            "period_end": "2026-08-31T23:59:59Z",
            "source_preview_run_id": run_id,
        },
    )

    assert resp.status_code in (400, 409, 422)
    async with session_maker() as session:
        after = {
            model.__name__: await session.scalar(select(func.count()).select_from(model))
            for model in (CampaignRun, MessageJob, OutboxMessage)
        }
    assert after == before


# ---------------------------------------------------------------------------
# The from_preview lifecycle, executed
# ---------------------------------------------------------------------------


def _dom_harness() -> str:
    """A minimal DOM the page script's decisions can read.

    Only what the shipped functions actually touch: `getElementById` returning
    elements with `value`, `checked` and `classList`. Enough to run the real
    code, far short of a browser.
    """
    return """
const ELEMENTS = {};
function el(id, props) {
  ELEMENTS[id] = Object.assign({
    value: "", checked: false, disabled: false, innerHTML: "",
    options: [], appendChild() {}, addEventListener() {},
    classList: {
      _s: new Set(),
      add(c) { this._s.add(c); },
      remove(c) { this._s.delete(c); },
      toggle(c, on) { if (on) this._s.add(c); else this._s.delete(c); },
      contains(c) { return this._s.has(c); },
    },
  }, props || {});
  return ELEMENTS[id];
}
globalThis.document = {
  getElementById: (id) => ELEMENTS[id] || null,
  querySelectorAll: () => [],
  createElement: () => ({ value: "", text: "" }),
};
el("f-provider", {value: "altegio"});
el("f-company", {value: "758285"});
el("f-ew-company", {value: "322579"});
el("f-period-start", {value: "2026-08-01"});
el("f-period-end", {value: "2026-08-31"});
el("f-attribution", {value: "30"});
el("f-card-type", {value: "card-1"});
el("f-followup-enabled", {checked: false});
el("f-followup-delay", {value: ""});
el("f-followup-policy", {value: ""});
el("f-followup-template", {value: ""});
el("btn-run");
el("preview-results");
el("recipients-table");
el("preview-links");
el("excluded-breakdown");
el("preview-alert");
function setAlert() {}
"""


async def _lifecycle(http_client: AsyncClient, *extra: str) -> str:
    """The shipped signature/scope/availability functions, plus a fake DOM."""
    script = _page_script((await http_client.get("/ops/campaigns/new-clients")).text)
    names = [
        "snapshotSignature",
        "signatureMatches",
        "scopeMatches",
        "mayRender",
        "mayRunFromPreview",
        "schemaProvider",
        "currentScope",
        "isEasyWeek",
        "applyRunAvailability",
        "invalidatePreviewContext",
        "takeDownPreview",
        "recoverLiveCardTypes",
        "resetPreviewSurface",
        "unlockSnapshotFields",
        "setPreviewBusy",
        "setRecipientsLoading",
        "cardTypesStatus",
        *extra,
    ]
    bodies = "\n".join(_function_source(script, name) for name in names)
    # The module-level state the functions read, declared the way the page does.
    state = "let PREVIEW_CONTEXT = null;\nlet PAGE_EPOCH = 0;\n"
    state += "let PREVIEW_IN_FLIGHT = null;\n"
    state += "let RECIPIENTS_GENERATION = 0;\nlet previewRunId = null;\n"
    state += "let CARD_TYPES_GENERATION = 0;\n"
    state += 'let CARD_TYPES_STATE = {state: "idle", scope: null};\n'
    state += "let CARD_TYPES_RELOADS = 0;\n"
    state += "function loadCardTypes() { CARD_TYPES_RELOADS += 1; }\n"
    # `SIGNATURE_FIELDS` is a const the page ships; take it verbatim.
    fields = re.search(r"const SIGNATURE_FIELDS = \[.*?\];", script, re.DOTALL)
    assert fields, "SIGNATURE_FIELDS is not in the page script"
    return _dom_harness() + "\n" + state + fields.group(0) + "\n" + bodies


@needs_node
@pytest.mark.asyncio
async def test_the_signature_covers_every_snapshot_defining_field(http_client) -> None:
    """Changing any one of them makes the form describe a different snapshot."""
    source = await _lifecycle(http_client)
    driver = """
const base = snapshotSignature();
const changes = {
  periodStart: () => { ELEMENTS["f-period-start"].value = "2026-07-01"; },
  periodEnd: () => { ELEMENTS["f-period-end"].value = "2026-09-30"; },
  attribution: () => { ELEMENTS["f-attribution"].value = "60"; },
  cardType: () => { ELEMENTS["f-card-type"].value = "card-2"; },
  company: () => { ELEMENTS["f-company"].value = "1271200"; },
  provider: () => { ELEMENTS["f-provider"].value = "easyweek"; },
  followupEnabled: () => { ELEMENTS["f-followup-enabled"].checked = true; },
};
const out = {};
for (const [name, apply] of Object.entries(changes)) {
  const before = JSON.parse(JSON.stringify(ELEMENTS["f-provider"]));
  const snapshot = {
    provider: ELEMENTS["f-provider"].value,
    company: ELEMENTS["f-company"].value,
    start: ELEMENTS["f-period-start"].value,
    end: ELEMENTS["f-period-end"].value,
    attribution: ELEMENTS["f-attribution"].value,
    card: ELEMENTS["f-card-type"].value,
    fu: ELEMENTS["f-followup-enabled"].checked,
  };
  apply();
  out[name] = signatureMatches(base, snapshotSignature());
  // restore
  ELEMENTS["f-provider"].value = snapshot.provider;
  ELEMENTS["f-company"].value = snapshot.company;
  ELEMENTS["f-period-start"].value = snapshot.start;
  ELEMENTS["f-period-end"].value = snapshot.end;
  ELEMENTS["f-attribution"].value = snapshot.attribution;
  ELEMENTS["f-card-type"].value = snapshot.card;
  ELEMENTS["f-followup-enabled"].checked = snapshot.fu;
  void before;
}
out.unchanged = signatureMatches(base, snapshotSignature());
console.log(JSON.stringify(out));
"""
    answer = _run_node(source, driver)
    assert answer["unchanged"] is True
    for field in ("periodStart", "periodEnd", "attribution", "cardType", "company", "provider", "followupEnabled"):
        assert answer[field] is False, f"{field} does not take part in the signature"


@needs_node
@pytest.mark.asyncio
async def test_changing_a_snapshot_field_revokes_run(http_client) -> None:
    """The button goes away the moment the form stops describing the preview."""
    source = await _lifecycle(http_client)
    driver = """
PREVIEW_CONTEXT = {
  runId: 37, provider: "altegio", companyId: "758285", runnable: true,
  signature: snapshotSignature(),
};
previewRunId = 37;
applyRunAvailability();
const before = ELEMENTS["btn-run"].disabled;

ELEMENTS["f-period-end"].value = "2026-09-30";
applyRunAvailability();
const after = ELEMENTS["btn-run"].disabled;

console.log(JSON.stringify({before: before, after: after}));
"""
    answer = _run_node(source, driver)
    assert answer["before"] is False
    assert answer["after"] is True


@needs_node
@pytest.mark.asyncio
async def test_an_easyweek_preview_never_gets_run_even_from_a_saved_run(http_client) -> None:
    source = await _lifecycle(http_client)
    driver = """
ELEMENTS["f-provider"].value = "easyweek";
PREVIEW_CONTEXT = {
  runId: 41, provider: "easyweek", companyId: "322579",
  // Even if a server said yes, which it does not.
  runnable: true, signature: snapshotSignature(),
};
previewRunId = 41;
applyRunAvailability();
console.log(JSON.stringify({
  disabled: ELEMENTS["btn-run"].disabled,
  hidden: ELEMENTS["btn-run"].classList.contains("d-none"),
}));
"""
    answer = _run_node(source, driver)
    assert answer["disabled"] is True
    assert answer["hidden"] is True


@needs_node
@pytest.mark.asyncio
async def test_invalidating_hides_the_preview_and_disables_run(http_client) -> None:
    source = await _lifecycle(http_client)
    driver = """
PREVIEW_CONTEXT = {
  runId: 37, provider: "altegio", companyId: "758285", runnable: true,
  signature: snapshotSignature(),
};
previewRunId = 37;
ELEMENTS["preview-results"].classList.remove("d-none");
ELEMENTS["recipients-table"].innerHTML = "<tr>...</tr>";
ELEMENTS["preview-links"].innerHTML = "<a>...</a>";

invalidatePreviewContext();

console.log(JSON.stringify({
  context: PREVIEW_CONTEXT,
  runId: previewRunId,
  hidden: ELEMENTS["preview-results"].classList.contains("d-none"),
  table: ELEMENTS["recipients-table"].innerHTML,
  links: ELEMENTS["preview-links"].innerHTML,
  runDisabled: ELEMENTS["btn-run"].disabled,
}));
"""
    answer = _run_node(source, driver)
    assert answer["context"] is None
    assert answer["runId"] is None
    assert answer["hidden"] is True
    assert answer["table"] == ""
    assert answer["links"] == ""
    assert answer["runDisabled"] is True


@needs_node
@pytest.mark.asyncio
async def test_a_run_attempt_against_a_changed_form_is_refused(http_client) -> None:
    """No confirm, no POST: the guard fires before either."""
    source = await _lifecycle(http_client, "runCampaign", "buildPayload")
    driver = """
let confirmed = false;
let posted = false;
globalThis.confirm = () => { confirmed = true; return true; };
globalThis.fetch = async () => { posted = true; return {ok: true, json: async () => ({}) }; };
globalThis.alert = () => {};

PREVIEW_CONTEXT = {
  runId: 37, provider: "altegio", companyId: "758285", runnable: true,
  signature: snapshotSignature(),
};
previewRunId = 37;
// The operator moved the period after the preview was built.
ELEMENTS["f-period-end"].value = "2026-09-30";

await runCampaign();

console.log(JSON.stringify({
  confirmed: confirmed,
  posted: posted,
  context: PREVIEW_CONTEXT,
  runDisabled: ELEMENTS["btn-run"].disabled,
}));
"""
    answer = _run_node(source, driver)
    assert answer["confirmed"] is False, "confirm was shown for a stale preview"
    assert answer["posted"] is False, "a send-real POST went out for a stale preview"
    assert answer["context"] is None
    assert answer["runDisabled"] is True


@needs_node
@pytest.mark.asyncio
async def test_a_run_attempt_without_a_context_is_refused(http_client) -> None:
    source = await _lifecycle(http_client, "runCampaign", "buildPayload")
    driver = """
let confirmed = false;
let posted = false;
globalThis.confirm = () => { confirmed = true; return true; };
globalThis.fetch = async () => { posted = true; return {ok: true, json: async () => ({}) }; };
globalThis.alert = () => {};

previewRunId = 37;   // stale id, no context behind it
PREVIEW_CONTEXT = null;

await runCampaign();

console.log(JSON.stringify({confirmed: confirmed, posted: posted}));
"""
    answer = _run_node(source, driver)
    assert answer["confirmed"] is False
    assert answer["posted"] is False


# ---------------------------------------------------------------------------
# The typed audit, end to end
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_easyweek_recipients_page_shows_all_three_audit_facts(
    session_maker, configuration, http_client
) -> None:
    """Basis, current state and the overridden reason are three columns."""
    from altegio_bot.tests.test_easyweek_manual_recipient import (
        CUSTOMER_UUID,
        PHONE,
        _add,
        _auto_excluded,
        _preview,
        _Reader,
    )

    run_id = await _preview(session_maker)
    await _auto_excluded(session_maker, run_id, reason="has_records_before_period")
    await _add(session_maker, run_id, _Reader())

    page = (await http_client.get(f"/ops/campaigns/{run_id}/recipients")).text

    assert "<th>Было исключено автоматически</th>" in page
    # All three facts, separately.
    assert "operator_manual_selection" in page or ">manual<" in page
    assert "has_records_before_period" in page
    # Current exclusion is empty — the row is active again.
    body = page[page.find("<tbody>") : page.find("</tbody>")]
    assert body.count("has_records_before_period") == 1
    # And the identifier is still never rendered.
    assert CUSTOMER_UUID not in page
    assert PHONE in page  # the operator does need to see whom they added


@pytest.mark.asyncio
async def test_the_altegio_recipients_page_has_no_easyweek_audit_column(
    session_maker, configuration, http_client
) -> None:
    run_id, _ = await seed_recipient(session_maker, provider=PROVIDER_ALTEGIO)

    page = (await http_client.get(f"/ops/campaigns/{run_id}/recipients")).text

    assert "<th>Было исключено автоматически</th>" not in page
    assert "<th>Card #</th>" in page


# ---------------------------------------------------------------------------
# The whole page, booted
# ---------------------------------------------------------------------------
#
# Everything above runs individual shipped functions. The lifecycle defects
# this file was extended for live in the ORDER things happen on load — the
# initial provider setup running before the `from_preview` load — so the tests
# below boot the entire served script under `node` against a small DOM and let
# it do what it does in a browser.


BROWSER = """
// A DOM that is small to read and complete enough to run the page. Elements are
// created on demand rather than enumerated, so a new field in the page does not
// silently become `null` here and quietly skip the code under test.
//
// Selects are modelled properly rather than as a string of HTML, because the
// races this file exists to catch are exactly the ones a string cannot show:
// replacing a select's markup replaces its options and moves the selection to
// the first of them, and assigning a value no option carries selects nothing at
// all. A harness that just remembers `innerHTML` reports every one of those as
// a success.
const EVENTS = {};
const ELEMENTS = {};
const ALERTS = [];
const FETCHES = [];
const REJECTIONS = [];

function parseOptions(html) {
  const found = [];
  const pattern = /<option([^>]*)>([\\s\\S]*?)<\\/option>/g;
  let match;
  while ((match = pattern.exec(html)) !== null) {
    const value = match[1].match(/value="([^"]*)"/);
    found.push({value: value ? value[1] : match[2].trim(), text: match[2]});
  }
  return found;
}

function makeElement(id) {
  const element = {
    id: id,
    _value: "",
    _html: "",
    checked: false,
    disabled: false,
    textContent: "",
    className: "",
    options: [],
    classList: {
      _classes: new Set(),
      add(name) { this._classes.add(name); },
      remove(name) { this._classes.delete(name); },
      contains(name) { return this._classes.has(name); },
      toggle(name, on) {
        const next = on === undefined ? !this._classes.has(name) : !!on;
        if (next) this._classes.add(name); else this._classes.delete(name);
        return next;
      },
    },
    appendChild(child) { this.options.push(child); },
    addEventListener(type, handler) {
      EVENTS[id] = EVENTS[id] || {};
      EVENTS[id][type] = (EVENTS[id][type] || []).concat([handler]);
    },
  };
  Object.defineProperty(element, "innerHTML", {
    get() { return this._html; },
    set(html) {
      this._html = String(html);
      const parsed = parseOptions(this._html);
      if (parsed.length || this.options.length) {
        // Rewriting a select's markup rewrites its options, and the browser
        // then selects the first one. This is how a late card-types answer
        // silently replaces a card type a loaded snapshot had fixed.
        this.options = parsed;
        this._value = parsed.length ? parsed[0].value : "";
      }
    },
  });
  Object.defineProperty(element, "value", {
    get() { return this._value; },
    set(next) {
      const wanted = String(next);
      if (this.options.length === 0) { this._value = wanted; return; }
      // A select cannot hold a value none of its options carries.
      this._value = this.options.some((o) => String(o.value) === wanted) ? wanted : "";
    },
  });
  ELEMENTS[id] = element;
  return element;
}

globalThis.el = function (id) { return ELEMENTS[id] || makeElement(id); };
globalThis.hidden = function (id) { return el(id).classList.contains("d-none"); };
globalThis.fireEvent = async function (id, type) {
  for (const handler of ((EVENTS[id] || {})[type] || [])) await handler();
  await settle();
};
// The page as the server rendered it: real options, real defaults.
globalThis.seedSelect = function (id, options, disabled) {
  const element = el(id);
  element.options = options.map((pair) => ({value: pair[0], text: pair[1]}));
  element._value = element.options.length ? element.options[0].value : "";
  element.disabled = !!disabled;
};
globalThis.seedInput = function (id, value, checked, disabled) {
  const element = el(id);
  element._value = String(value);
  element.checked = !!checked;
  element.disabled = !!disabled;
};

// The outstanding-cards table is markup the page generates and then reads back
// through `.oc-check` selectors, so the harness parses its own rows rather than
// pretending the table is empty — otherwise a delete would always look scoped.
function outstandingCheckboxes(selector) {
  const html = el("outstanding-cards-table").innerHTML;
  const rows = [];
  const pattern = /<input([^>]*class="oc-check"[^>]*)>/g;
  let match;
  while ((match = pattern.exec(html)) !== null) {
    const attributes = match[1];
    const recipient = attributes.match(/data-recipient="([^"]*)"/);
    rows.push({
      dataset: {recipient: recipient ? recipient[1] : ""},
      checked: / checked/.test(attributes),
    });
  }
  if (selector.indexOf(":not(:checked)") !== -1) return rows.filter((r) => !r.checked);
  if (selector.indexOf(":checked") !== -1) return rows.filter((r) => r.checked);
  return rows;
}

const DOM_READY = [];
globalThis.document = {
  getElementById: (id) => el(id),
  querySelectorAll: (selector) =>
    String(selector).indexOf(".oc-check") === 0 ? outstandingCheckboxes(String(selector)) : [],
  createElement: () => ({value: "", text: ""}),
  addEventListener: (type, handler) => {
    if (type === "DOMContentLoaded") DOM_READY.push(handler);
  },
};

// No polling inside a test process, and no browser dialogs.
globalThis.setInterval = () => 0;
globalThis.clearInterval = () => {};
globalThis.alert = (message) => { ALERTS.push(String(message)); };
globalThis.CONFIRMED = [];
globalThis.CONFIRM_ANSWER = true;
globalThis.confirm = (message) => { CONFIRMED.push(String(message)); return CONFIRM_ANSWER; };

// Every request the page makes is recorded; the driver decides what comes back
// and, crucially, WHEN — which is how out-of-order answers are staged.
globalThis.FETCHES = FETCHES;
globalThis.ALERTS = ALERTS;
globalThis.REJECTIONS = REJECTIONS;
globalThis.ROUTES = async () => ({ok: true, body: {}});
globalThis.fetch = async function (url, options) {
  FETCHES.push({url: String(url), body: (options && options.body) ? JSON.parse(options.body) : null});
  const answer = await ROUTES(String(url), options || {});
  if (answer && answer.throw) throw new Error(answer.throw);
  const body = (answer && answer.body !== undefined) ? answer.body : {};
  return {
    ok: !answer || answer.ok !== false,
    status: (answer && answer.status) || ((answer && answer.ok === false) ? 400 : 200),
    statusText: "test",
    // `{malformed: true}` answers with a body that cannot be parsed, which is a
    // different failure from a network error and takes a different branch.
    json: async () => {
      if (answer && answer.malformed) throw new SyntaxError("Unexpected token < in JSON");
      return body;
    },
    text: async () => JSON.stringify(body),
  };
};

process.on("unhandledRejection", (reason) => { REJECTIONS.push(String(reason)); });

globalThis.settle = async function (rounds) {
  for (let index = 0; index < (rounds || 60); index += 1) {
    await new Promise((resolve) => setImmediate(resolve));
  }
};
globalThis.boot = async function () {
  for (const handler of DOM_READY) await handler();
  await settle();
};
globalThis.emit = function (payload) { console.log(JSON.stringify(payload)); };

// A deferred answer: the driver decides when — and whether — it lands.
globalThis.deferred = function () {
  let settleIt;
  const promise = new Promise((resolve) => { settleIt = resolve; });
  return {promise: promise, resolve: (value) => settleIt(value)};
};

// Where an Altegio preview run starts out. The driver bends single fields.
globalThis.RUN = {
  id: 37,
  mode: "preview",
  status: "completed",
  provider: "altegio",
  company_ids: [758285],
  period_start: "2026-08-01T00:00:00Z",
  period_end: "2026-08-31T23:59:59Z",
  attribution_window_days: 30,
  card_type_id: "1001",
  followup_enabled: false,
  is_runnable_from_preview: true,
};
globalThis.EMPTY = {items: [], total: 0, rows: []};
// What /card-types answers with: a plain array, as the endpoint returns.
globalThis.CARD_TYPES = [{id: "1001", title: "Bronze"}, {id: "2002", title: "Silver"}];

// The default network: the run itself, the branch's card types, empty rest.
// Outstanding loyalty cards, per branch, so a table can be told apart by which
// branch's rows it holds.
globalThis.OUTSTANDING = {
  "758285": [{recipient_id: 11, display_name: "Karlsruhe One", phone_e164: "+4910000001",
              loyalty_card_number: "KA-1", period_start: "2026-07-01"}],
  "1271200": [{recipient_id: 22, display_name: "Rastatt One", phone_e164: "+4910000002",
               loyalty_card_number: "RA-1", period_start: "2026-07-01"}],
};

globalThis.defaultRoutes = async function (url) {
  if (url.indexOf("/outstanding-cards") !== -1) {
    const company = (url.match(/company_id=(\d+)/) || [])[1] || "";
    return {ok: true, body: {cards: OUTSTANDING[company] || []}};
  }
  if (url.indexOf("/bulk-delete-cards") !== -1) {
    return {ok: true, body: {deleted_count: 1, failed_count: 0, skipped_count: 0, failed: []}};
  }
  if (url.indexOf("/card-types") !== -1) return {ok: true, body: CARD_TYPES};
  if (url.indexOf("/recipients") !== -1) return {ok: true, body: EMPTY};
  const match = url.match(/^\\/ops\\/campaigns\\/runs\\/(\\d+)$/);
  if (match) {
    if (String(RUN.id) !== match[1]) return {ok: false, status: 404, body: {detail: "no such run"}};
    return {ok: true, body: RUN};
  }
  return {ok: true, body: EMPTY};
};
globalThis.ROUTES = defaultRoutes;

globalThis.preset = function (values) {
  for (const [id, value] of Object.entries(values)) {
    if (typeof value === "boolean") el(id).checked = value; else el(id).value = value;
  }
};
globalThis.state = function () {
  return {
    context: PREVIEW_CONTEXT,
    previewRunId: previewRunId,
    inFlight: PREVIEW_IN_FLIGHT === null ? null : PREVIEW_IN_FLIGHT.ticket,
    runDisabled: el("btn-run").disabled,
    runHidden: hidden("btn-run"),
    resultsHidden: hidden("preview-results"),
    spinnerHidden: hidden("preview-spinner"),
    previewDisabled: el("btn-preview").disabled,
    table: el("recipients-table").innerHTML,
    company: el("f-company").value,
    card: el("f-card-type").value,
    cardOptions: el("f-card-type").options.map((o) => o.value),
    cardDisabled: el("f-card-type").disabled,
    cardStatus: cardTypesStatus(),
    cardState: CARD_TYPES_STATE.state,
    outstanding: el("outstanding-cards-table").innerHTML,
    outstandingScope: OUTSTANDING_SCOPE,
    deleteDisabled: el("btn-delete-outstanding").disabled,
    recipientsLoadingHidden: hidden("recipients-loading"),
    recipientsTable: el("recipients-table").innerHTML,
    rejections: REJECTIONS,
    fetched: FETCHES.map((f) => f.url),
  };
};
"""


_SELECT_RE = re.compile(r'<select id="([\w-]+)"([^>]*)>(.*?)</select>', re.DOTALL)
_OPTION_RE = re.compile(r'<option value="([^"]*)"[^>]*>(.*?)</option>', re.DOTALL)
_INPUT_RE = re.compile(r"<input([^>]*)>")
_ID_RE = re.compile(r'id="([\w-]+)"')
_VALUE_RE = re.compile(r'value="([^"]*)"')


def _dom_seed(page: str) -> str:
    """Seed the harness with the form the SERVER actually rendered.

    Which branches exist, which one is selected by default, whether the card
    select starts shut — all of that is the server's answer, not the test's
    opinion. The Rastatt case in particular is only meaningful if the option
    the page must select is one the server really renders.
    """
    lines = []
    for element_id, attributes, inner in _SELECT_RE.findall(page):
        options = [[value, re.sub(r"\s+", " ", text).strip()] for value, text in _OPTION_RE.findall(inner)]
        shut = json.dumps("disabled" in attributes)
        lines.append(f"seedSelect({json.dumps(element_id)}, {json.dumps(options)}, {shut});")
    for attributes in _INPUT_RE.findall(page):
        found = _ID_RE.search(attributes)
        if not found:
            continue
        value = _VALUE_RE.search(attributes)
        lines.append(
            f"seedInput({json.dumps(found.group(1))}, {json.dumps(value.group(1) if value else '')}, "
            f"{json.dumps('checked' in attributes)}, {json.dumps('disabled' in attributes)});"
        )
    return "\n".join(lines)


async def _page(http_client: AsyncClient, query: str = "") -> str:
    return (await http_client.get("/ops/campaigns/new-clients" + query)).text


async def _browser(http_client: AsyncClient, query: str = "") -> str:
    """The harness, the server's own form, and the page script as served."""
    page = await _page(http_client, query)
    return BROWSER + "\n" + _dom_seed(page) + "\n" + _page_script(page)


# The defaults now come from the rendered page, so a driver only names what it
# deliberately changes.
ALTEGIO_FORM: dict[str, str] = {}


@needs_node
@pytest.mark.asyncio
async def test_opening_from_preview_loads_the_run_and_offers_run(http_client) -> None:
    """The blocker: setting up the form erased the id before it was ever used.

    Booted end to end — provider setup, branch loaders, then the `from_preview`
    load — because the defect was invisible in any single function.
    """
    source = await _browser(http_client, "?from_preview=37")
    driver = """
preset(%s);
await boot();
emit(Object.assign(state(), {loaded: FETCHES.some((f) => f.url === "/ops/campaigns/runs/37")}));
""" % json.dumps(ALTEGIO_FORM)
    answer = _run_node(source, driver)

    assert answer["loaded"] is True, "the page never asked the server for the preview"
    assert answer["context"]["runId"] == 37
    assert answer["context"]["provider"] == "altegio"
    assert answer["context"]["runnable"] is True
    assert answer["previewRunId"] == 37
    assert answer["resultsHidden"] is False
    assert answer["runDisabled"] is False
    assert answer["runHidden"] is False
    assert answer["rejections"] == []


@needs_node
@pytest.mark.asyncio
async def test_the_loaded_preview_locks_the_form_to_the_snapshot(http_client) -> None:
    """Every signature field comes back from the run and is then frozen."""
    source = await _browser(http_client, "?from_preview=37")
    driver = """
preset(%s);
RUN.attribution_window_days = 45;
RUN.card_type_id = "2002";
await boot();
emit({
  company: el("f-company").value,
  start: el("f-period-start").value,
  end: el("f-period-end").value,
  attribution: el("f-attribution").value,
  card: el("f-card-type").value,
  locked: {
    company: el("f-company").disabled,
    start: el("f-period-start").disabled,
    end: el("f-period-end").disabled,
    attribution: el("f-attribution").disabled,
    card: el("f-card-type").disabled,
    followup: el("f-followup-enabled").disabled,
  },
  signature: PREVIEW_CONTEXT.signature,
});
""" % json.dumps(ALTEGIO_FORM)
    answer = _run_node(source, driver)

    assert answer["company"] == "758285"
    assert answer["start"] == "2026-08-01"
    assert answer["end"] == "2026-08-31"
    assert answer["attribution"] == "45"
    assert answer["card"] == "2002"
    assert all(answer["locked"].values()), answer["locked"]
    # And the frozen signature describes that same snapshot, not the defaults.
    assert answer["signature"]["attributionWindowDays"] == "45"
    assert answer["signature"]["cardTypeId"] == "2002"


@needs_node
@pytest.mark.asyncio
async def test_an_easyweek_run_opened_from_preview_never_gets_run(http_client) -> None:
    source = await _browser(http_client, "?from_preview=37")
    driver = """
preset(%s);
RUN.provider = "easyweek";
RUN.is_runnable_from_preview = true;   // even if the server said yes
await boot();
emit(Object.assign(state(), {alerts: ALERTS}));
""" % json.dumps(ALTEGIO_FORM)
    answer = _run_node(source, driver)

    assert answer["context"] is None
    assert answer["previewRunId"] is None
    assert answer["runDisabled"] is True


@needs_node
@pytest.mark.asyncio
async def test_a_run_that_is_not_a_completed_preview_is_refused(http_client) -> None:
    for field, value in (("mode", "send-real"), ("status", "running")):
        source = await _browser(http_client, "?from_preview=37")
        driver = """
preset(%s);
RUN[%s] = %s;
await boot();
emit(state());
""" % (json.dumps(ALTEGIO_FORM), json.dumps(field), json.dumps(value))
        answer = _run_node(source, driver)

        assert answer["context"] is None, f"{field}={value} was accepted"
        assert answer["previewRunId"] is None
        assert answer["runDisabled"] is True


@needs_node
@pytest.mark.asyncio
async def test_a_prefill_that_lands_after_a_provider_switch_is_ignored(http_client) -> None:
    """The operator moved to EasyWeek while the run was in flight."""
    source = await _browser(http_client, "?from_preview=37")
    driver = """
preset(%s);
// A period the form would never be showing by itself, so filling it in is
// visible as filling it in.
RUN.period_start = "2026-05-03T00:00:00Z";
RUN.period_end = "2026-05-31T23:59:59Z";
const untouched = el("f-period-start").value;
const late = deferred();
ROUTES = async (url) => {
  if (url === "/ops/campaigns/runs/37") return late.promise;
  return defaultRoutes(url);
};
await boot();
const beforeAnswer = state();

// A real switch, through the page's own handler.
el("f-provider").value = "easyweek";
onProviderChange();
await settle();

late.resolve({ok: true, body: RUN});
await settle();

emit({
  before: beforeAnswer,
  after: state(),
  period: el("f-period-start").value,
  untouched: untouched,
  companyLocked: el("f-company").disabled,
});
""" % json.dumps(ALTEGIO_FORM)
    answer = _run_node(source, driver)

    assert answer["before"]["context"] is None, "the answer had not arrived yet"
    # The late answer paints nothing and fills nothing in behind the operator.
    assert answer["after"]["context"] is None
    assert answer["after"]["previewRunId"] is None
    assert answer["period"] == answer["untouched"], "the run was filled in behind the operator"
    assert answer["period"] != "2026-05-03"
    assert answer["companyLocked"] is False
    assert answer["after"]["runDisabled"] is True
    assert answer["after"]["runHidden"] is True


@needs_node
@pytest.mark.asyncio
async def test_starting_a_preview_revokes_the_previous_one_before_the_request(http_client) -> None:
    """A is gone by the time B's request leaves, not when B's answer lands."""
    source = await _browser(http_client)
    driver = """
preset(%s);
preset({"f-period-start": "2026-08-01", "f-period-end": "2026-08-31", "f-card-type": "1001"});
await boot();

let whenBLeft = null;
ROUTES = async (url) => {
  if (url.indexOf("/preview") !== -1) {
    const id = FETCHES.filter((f) => f.url.indexOf("/preview") !== -1).length;
    if (id === 2) whenBLeft = {context: PREVIEW_CONTEXT, runDisabled: el("btn-run").disabled};
    return {ok: true, body: {id: 100 + id, provider: "altegio", company_ids: [758285],
                             is_runnable_from_preview: true, total_clients_seen: 5, candidates_count: 5}};
  }
  return defaultRoutes(url);
};

await createPreview();
await settle();
const afterA = state();

await createPreview();
await settle();
const afterB = state();

emit({afterA: afterA, afterB: afterB, whenBLeft: whenBLeft});
""" % json.dumps(ALTEGIO_FORM)
    answer = _run_node(source, driver)

    assert answer["afterA"]["context"]["runId"] == 101
    assert answer["afterA"]["runDisabled"] is False
    # The moment B's request went out, A had already been revoked.
    assert answer["whenBLeft"] is not None, "B never reached the network"
    assert answer["whenBLeft"]["context"] is None
    assert answer["whenBLeft"]["runDisabled"] is True
    assert answer["afterB"]["context"]["runId"] == 102


@needs_node
@pytest.mark.asyncio
async def test_a_failed_preview_does_not_restore_the_previous_one(http_client) -> None:
    source = await _browser(http_client)
    driver = """
preset(%s);
preset({"f-period-start": "2026-08-01", "f-period-end": "2026-08-31", "f-card-type": "1001"});
await boot();

let attempts = 0;
ROUTES = async (url) => {
  if (url.indexOf("/preview") !== -1) {
    attempts += 1;
    if (attempts === 1) {
      return {ok: true, body: {id: 101, provider: "altegio", company_ids: [758285],
                               is_runnable_from_preview: true}};
    }
    return {ok: false, status: 422, body: {detail: "period is empty"}};
  }
  return defaultRoutes(url);
};

await createPreview();
await settle();
const afterA = state();

await createPreview();
await settle();
const afterFailure = state();

// And a network error is no different.
ROUTES = async (url) => (url.indexOf("/preview") !== -1 ? {throw: "offline"} : defaultRoutes(url));
await createPreview();
await settle();

emit({afterA: afterA, afterFailure: afterFailure, afterThrow: state()});
""" % json.dumps(ALTEGIO_FORM)
    answer = _run_node(source, driver)

    assert answer["afterA"]["context"]["runId"] == 101
    for stage in ("afterFailure", "afterThrow"):
        assert answer[stage]["context"] is None, stage
        assert answer[stage]["previewRunId"] is None, stage
        assert answer[stage]["runDisabled"] is True, stage
        assert answer[stage]["resultsHidden"] is True, stage
        # The failed attempt does own its own cleanup: nothing is left spinning.
        assert answer[stage]["spinnerHidden"] is True, stage
        assert answer[stage]["previewDisabled"] is False, stage


@needs_node
@pytest.mark.asyncio
async def test_a_stale_preview_answer_neither_paints_nor_clears_the_new_one(http_client) -> None:
    """A finishes last. Its success, its cleanup and its errors are all void."""
    source = await _browser(http_client)
    driver = """
preset(%s);
preset({"f-period-start": "2026-08-01", "f-period-end": "2026-08-31", "f-card-type": "1001"});
await boot();

const slowA = deferred();
const slowB = deferred();
let started = 0;
ROUTES = async (url) => {
  if (url.indexOf("/preview") !== -1) {
    started += 1;
    return started === 1 ? slowA.promise : slowB.promise;
  }
  return defaultRoutes(url);
};

const runningA = createPreview();
const runningB = createPreview();
await settle();

// B answers first and owns the screen.
slowB.resolve({ok: true, body: {id: 102, provider: "altegio", company_ids: [758285],
                                is_runnable_from_preview: true}});
await runningB;
await settle();
const owned = state();

// Now A's answer finally arrives.
slowA.resolve({ok: true, body: {id: 101, provider: "altegio", company_ids: [758285],
                                is_runnable_from_preview: true}});
await runningA;
await settle();

emit({owned: owned, after: state(), alerts: ALERTS});
""" % json.dumps(ALTEGIO_FORM)
    answer = _run_node(source, driver)

    assert answer["owned"]["context"]["runId"] == 102
    # A's late success repaints nothing.
    assert answer["after"]["context"]["runId"] == 102
    assert answer["after"]["previewRunId"] == 102
    assert answer["after"]["runDisabled"] is False
    assert not any("101" in text for text in answer["alerts"]), "A announced itself after B won"
    assert answer["after"]["rejections"] == []


@needs_node
@pytest.mark.asyncio
async def test_a_stale_answer_does_not_hide_the_running_spinner(http_client) -> None:
    """A's `finally` belongs to A, and A is no longer on screen."""
    source = await _browser(http_client)
    driver = """
preset(%s);
preset({"f-period-start": "2026-08-01", "f-period-end": "2026-08-31", "f-card-type": "1001"});
await boot();

const slowA = deferred();
const slowB = deferred();
let started = 0;
ROUTES = async (url) => {
  if (url.indexOf("/preview") !== -1) {
    started += 1;
    return started === 1 ? slowA.promise : slowB.promise;
  }
  return defaultRoutes(url);
};

const runningA = createPreview();
const runningB = createPreview();
await settle();

// A fails late, while B is still in flight.
slowA.resolve({ok: false, status: 500, body: {detail: "gateway"}});
await runningA;
await settle();
const whileBRuns = {
  spinnerHidden: hidden("preview-spinner"),
  previewDisabled: el("btn-preview").disabled,
  alerts: ALERTS.slice(),
};

slowB.resolve({ok: true, body: {id: 102, provider: "altegio", company_ids: [758285],
                                is_runnable_from_preview: true}});
await runningB;
await settle();

emit({whileBRuns: whileBRuns, after: state()});
""" % json.dumps(ALTEGIO_FORM)
    answer = _run_node(source, driver)

    # B is still loading: its spinner is up and its button stays held.
    assert answer["whileBRuns"]["spinnerHidden"] is False
    assert answer["whileBRuns"]["previewDisabled"] is True
    assert not any("gateway" in text for text in answer["whileBRuns"]["alerts"])
    # And B lands normally afterwards.
    assert answer["after"]["context"]["runId"] == 102
    assert answer["after"]["spinnerHidden"] is True
    assert answer["after"]["previewDisabled"] is False


@needs_node
@pytest.mark.asyncio
async def test_editing_a_snapshot_field_revokes_the_loaded_preview(http_client) -> None:
    """Through the page's own change handlers, field by field."""
    fields = {
        "f-period-start": "2026-07-01",
        "f-period-end": "2026-09-30",
        "f-attribution": "60",
        "f-card-type": "2002",
    }
    for field, value in fields.items():
        source = await _browser(http_client, "?from_preview=37")
        driver = """
preset(%s);
await boot();
const before = state();
el(%s).value = %s;
await fireEvent(%s, "change");
emit({before: before, after: state()});
""" % (json.dumps(ALTEGIO_FORM), json.dumps(field), json.dumps(value), json.dumps(field))
        answer = _run_node(source, driver)

        assert answer["before"]["runDisabled"] is False, f"{field}: nothing to revoke"
        assert answer["after"]["context"] is None, f"{field} did not revoke the preview"
        assert answer["after"]["previewRunId"] is None, field
        assert answer["after"]["runDisabled"] is True, field
        assert answer["after"]["resultsHidden"] is True, field
        assert answer["after"]["table"] == "", field


@needs_node
@pytest.mark.asyncio
async def test_run_posts_only_while_the_form_still_describes_the_preview(http_client) -> None:
    """A positive control and the refusal, from the same booted page."""
    source = await _browser(http_client, "?from_preview=37")
    driver = """
preset(%s);
await boot();

let posts = [];
const network = ROUTES;
ROUTES = async (url, options) => {
  if (url.indexOf("/new-clients/run") !== -1) {
    posts.push(JSON.parse(options.body));
    return {ok: true, body: {id: 900}};
  }
  return network(url, options);
};

// 1. Untouched: the run goes out, carrying the preview it came from.
await runCampaign();
await settle();
const allowed = {confirms: CONFIRMED.length, posts: posts.slice()};

// 2. The operator widens the period, then presses Run again.
posts = [];
CONFIRMED.length = 0;
el("f-period-end").disabled = false;
el("f-period-end").value = "2026-09-30";
await runCampaign();
await settle();

emit({allowed: allowed, refused: {confirms: CONFIRMED.length, posts: posts}, after: state()});
""" % json.dumps(ALTEGIO_FORM)
    answer = _run_node(source, driver)

    # The control: without it, a refusal proves nothing.
    assert answer["allowed"]["confirms"] == 1
    assert len(answer["allowed"]["posts"]) == 1
    assert answer["allowed"]["posts"][0]["source_preview_run_id"] == 37
    assert answer["allowed"]["posts"][0]["provider"] == "altegio"

    # The refusal: no confirm, no POST, and the stale screen is taken down.
    assert answer["refused"]["confirms"] == 0, "the operator was asked about a stale preview"
    assert answer["refused"]["posts"] == [], "a send-real POST went out for a stale preview"
    assert answer["after"]["context"] is None
    assert answer["after"]["runDisabled"] is True


@needs_node
@pytest.mark.asyncio
async def test_a_provider_switch_takes_the_run_button_away(http_client) -> None:
    source = await _browser(http_client, "?from_preview=37")
    driver = """
preset(%s);
await boot();
const loaded = state();

el("f-provider").value = "easyweek";
onProviderChange();
await settle();
const easyweek = state();

// Coming back does not resurrect it: the preview is gone, not hidden.
el("f-provider").value = "altegio";
onProviderChange();
await settle();

emit({loaded: loaded, easyweek: easyweek, back: state()});
""" % json.dumps(ALTEGIO_FORM)
    answer = _run_node(source, driver)

    assert answer["loaded"]["context"]["runId"] == 37
    assert answer["easyweek"]["context"] is None
    assert answer["easyweek"]["runHidden"] is True
    assert answer["easyweek"]["runDisabled"] is True
    assert answer["back"]["context"] is None
    assert answer["back"]["runDisabled"] is True


# ---------------------------------------------------------------------------
# Any branch, not just the default one
# ---------------------------------------------------------------------------


@needs_node
@pytest.mark.asyncio
async def test_from_preview_loads_a_non_default_branch(http_client) -> None:
    """Rastatt is not the branch the form opens on, and that is the point.

    The page used to remember the branch it opened with, then set the branch
    the run belongs to, then compare the two and refuse — so every preview of
    any branch but the default was unopenable.
    """
    source = await _browser(http_client, "?from_preview=41")
    driver = """
RUN.id = 41;
RUN.company_ids = [1271200];
RUN.period_start = "2026-05-03T00:00:00Z";
RUN.period_end = "2026-05-31T23:59:59Z";
RUN.attribution_window_days = 45;
RUN.card_type_id = "7007";
const defaultCompany = el("f-company").value;
await boot();
emit(Object.assign(state(), {
  defaultCompany: defaultCompany,
  start: el("f-period-start").value,
  end: el("f-period-end").value,
  attribution: el("f-attribution").value,
  recipientsAsked: FETCHES.some((f) => f.url.indexOf("/runs/41/recipients") !== -1),
}));
"""
    answer = _run_node(source, driver)

    assert answer["defaultCompany"] == "758285", "the page no longer opens on Karlsruhe"
    assert answer["company"] == "1271200", "the branch of the saved run was not selected"
    assert answer["context"]["runId"] == 41
    assert answer["context"]["companyId"] == "1271200"
    assert answer["previewRunId"] == 41
    assert answer["recipientsAsked"] is True, "the snapshot's recipients were never loaded"
    # The signature describes the run, not the defaults the page opened with.
    signature = answer["context"]["signature"]
    assert signature["companyId"] == "1271200"
    assert signature["periodStart"] == "2026-05-03"
    assert signature["periodEnd"] == "2026-05-31"
    assert signature["attributionWindowDays"] == "45"
    assert signature["cardTypeId"] == "7007"
    assert answer["runDisabled"] is False
    assert answer["rejections"] == []


@needs_node
@pytest.mark.asyncio
async def test_run_stays_shut_when_the_server_does_not_allow_it(http_client) -> None:
    """Loading a branch's preview is not the same as being allowed to send it."""
    source = await _browser(http_client, "?from_preview=41")
    driver = """
RUN.id = 41;
RUN.company_ids = [1271200];
RUN.is_runnable_from_preview = false;
await boot();
emit(state());
"""
    answer = _run_node(source, driver)

    assert answer["context"]["runId"] == 41, "the preview still loads"
    assert answer["context"]["runnable"] is False
    assert answer["company"] == "1271200"
    assert answer["runDisabled"] is True


@needs_node
@pytest.mark.asyncio
async def test_a_branch_this_ui_does_not_offer_is_refused_before_anything_is_filled(
    http_client,
) -> None:
    """And the form is left usable, not half-filled and frozen."""
    source = await _browser(http_client, "?from_preview=41")
    driver = """
RUN.id = 41;
RUN.company_ids = [999999];
const before = {company: el("f-company").value, start: el("f-period-start").value};
await boot();
emit(Object.assign(state(), {
  before: before,
  start: el("f-period-start").value,
  companyDisabled: el("f-company").disabled,
  startDisabled: el("f-period-start").disabled,
  attributionDisabled: el("f-attribution").disabled,
  alerts: ALERTS,
}));
"""
    answer = _run_node(source, driver)

    assert answer["context"] is None
    assert answer["company"] == answer["before"]["company"], "a refused run still moved the branch"
    assert answer["start"] == answer["before"]["start"]
    assert answer["companyDisabled"] is False
    assert answer["startDisabled"] is False
    assert answer["attributionDisabled"] is False
    assert answer["spinnerHidden"] is True
    assert answer["previewDisabled"] is False


@needs_node
@pytest.mark.asyncio
async def test_a_response_for_another_run_is_refused(http_client) -> None:
    source = await _browser(http_client, "?from_preview=41")
    driver = """
ROUTES = async (url) => {
  if (url.indexOf("/ops/campaigns/runs/41") !== -1 && url.indexOf("recipients") === -1) {
    // The same shape, a different run.
    return {ok: true, body: Object.assign({}, RUN, {id: 99})};
  }
  return defaultRoutes(url);
};
await boot();
emit(Object.assign(state(), {alerts: ALERTS}));
"""
    answer = _run_node(source, driver)

    assert answer["context"] is None
    assert answer["previewRunId"] is None
    assert answer["runDisabled"] is True


@needs_node
@pytest.mark.asyncio
async def test_a_user_branch_change_cancels_a_prefill_but_the_runs_own_does_not(
    http_client,
) -> None:
    """The distinction the old code could not draw.

    Both are `f-company.value = ...`. One is the operator saying "show me
    something else" and must cancel; the other is the page describing the run
    it was told to load and must not.
    """
    source = await _browser(http_client, "?from_preview=41")
    driver = """
RUN.id = 41;
RUN.company_ids = [1271200];

// (a) the operator changes the branch while the run is in flight
const late = deferred();
ROUTES = async (url) => {
  if (url === "/ops/campaigns/runs/41") return late.promise;
  return defaultRoutes(url);
};
await boot();
el("f-company").value = "1271200";
await fireEvent("f-company", "change");
late.resolve({ok: true, body: RUN});
await settle();
const afterUserChange = state();

emit({afterUserChange: afterUserChange});
"""
    cancelled = _run_node(source, driver)
    # Even though the operator happened to pick the very branch the run uses,
    # their change revoked the request that was in flight.
    assert cancelled["afterUserChange"]["context"] is None
    assert cancelled["afterUserChange"]["previewRunId"] is None
    assert cancelled["afterUserChange"]["runDisabled"] is True
    assert cancelled["afterUserChange"]["spinnerHidden"] is True
    assert cancelled["afterUserChange"]["previewDisabled"] is False

    # (b) nobody touches anything: the run's own branch change is not a cancel
    source = await _browser(http_client, "?from_preview=41")
    driver = """
RUN.id = 41;
RUN.company_ids = [1271200];
await boot();
emit(state());
"""
    loaded = _run_node(source, driver)
    assert loaded["context"]["runId"] == 41
    assert loaded["company"] == "1271200"
    assert loaded["runDisabled"] is False


# ---------------------------------------------------------------------------
# Card types cannot race a loaded snapshot
# ---------------------------------------------------------------------------


@needs_node
@pytest.mark.asyncio
async def test_a_late_card_types_answer_cannot_touch_a_loaded_snapshot(http_client) -> None:
    """The snapshot's card type survives a list that arrives after it.

    The list is deliberately one that does NOT contain the snapshot's card, and
    the harness models a select properly — replacing its markup replaces its
    options and moves the selection — so a leaked answer shows up as a changed
    card, not as an ignored string.
    """
    source = await _browser(http_client, "?from_preview=37")
    driver = """
RUN.card_type_id = "7007";   // not in the branch's live list
const late = deferred();
ROUTES = async (url) => {
  if (url.indexOf("/card-types") !== -1) return late.promise;
  return defaultRoutes(url);
};

await boot();
const loaded = state();

// Whatever asked for card types — a stray earlier load — answers now.
loadCardTypes();
await settle();
late.resolve({ok: true, body: CARD_TYPES});
await settle();

emit({loaded: loaded, after: state(), status: el("card-load-status").textContent});
"""
    answer = _run_node(source, driver)

    assert answer["loaded"]["context"]["runId"] == 37
    assert answer["loaded"]["card"] == "7007"
    assert answer["loaded"]["cardDisabled"] is True

    after = answer["after"]
    assert after["card"] == "7007", "a late card-types answer replaced the snapshot's card"
    assert "7007" in after["cardOptions"], "the snapshot's option was dropped from the list"
    assert after["cardDisabled"] is True, "the frozen select was handed back to the operator"
    assert after["context"]["runId"] == 37
    assert after["context"]["signature"]["cardTypeId"] == "7007"
    assert after["runDisabled"] is False


@needs_node
@pytest.mark.asyncio
async def test_a_from_preview_page_does_not_ask_for_live_card_types(http_client) -> None:
    """The simplest form of the guarantee: the race is never started."""
    source = await _browser(http_client, "?from_preview=37")
    driver = """
await boot();
emit({asked: FETCHES.map((f) => f.url).filter((u) => u.indexOf("/card-types") !== -1)});
"""
    assert _run_node(source, driver)["asked"] == []


@needs_node
@pytest.mark.asyncio
async def test_an_ordinary_page_still_loads_card_types(http_client) -> None:
    """Nothing above may cost the normal flow its card types."""
    source = await _browser(http_client)
    driver = """
await boot();
const booted = {
  asked: FETCHES.map((f) => f.url).filter((u) => u.indexOf("/card-types") !== -1),
  options: el("f-card-type").options.map((o) => o.value),
  disabled: el("f-card-type").disabled,
};
// And choosing a branch by hand asks again, for that branch.
el("f-company").value = "1271200";
await fireEvent("f-company", "change");
emit({booted: booted, asked: FETCHES.map((f) => f.url).filter((u) => u.indexOf("/card-types") !== -1)});
"""
    answer = _run_node(source, driver)

    assert len(answer["booted"]["asked"]) == 1
    assert "location_id=758285" in answer["booted"]["asked"][0]
    assert answer["booted"]["options"] == ["1001", "2002"]
    assert answer["booted"]["disabled"] is False
    assert len(answer["asked"]) == 2
    assert "location_id=1271200" in answer["asked"][1]


# ---------------------------------------------------------------------------
# Cancelling leaves the page idle, not stuck
# ---------------------------------------------------------------------------


PREVIEW_DRIVER_PRELUDE = """
const late = deferred();
ROUTES = async (url) => {
  if (url.indexOf("/new-clients/preview") !== -1) return late.promise;
  return defaultRoutes(url);
};
await boot();
const running = createPreview();
await settle();
const busy = state();
"""

PREVIEW_DRIVER_CODA = """
const cancelled = state();
late.resolve({ok: true, body: {id: 101, provider: "altegio", company_ids: [758285],
                               is_runnable_from_preview: true, total_clients_seen: 9,
                               candidates_count: 9}});
await running;
await settle();
emit({busy: busy, cancelled: cancelled, after: state(), alerts: ALERTS});
"""


def _assert_cancelled_to_idle(answer: dict) -> None:
    """A cancelled preview leaves an idle page and a void answer."""
    assert answer["busy"]["spinnerHidden"] is False, "the request never showed as running"
    assert answer["busy"]["previewDisabled"] is True

    for stage in ("cancelled", "after"):
        assert answer[stage]["spinnerHidden"] is True, f"{stage}: the spinner was left running"
        assert answer[stage]["previewDisabled"] is False, f"{stage}: Create Preview stayed held"
        assert answer[stage]["runDisabled"] is True, stage
        assert answer[stage]["context"] is None, stage
        assert answer[stage]["previewRunId"] is None, stage
        assert answer[stage]["resultsHidden"] is True, stage
        assert answer[stage]["inFlight"] is None, stage
    assert not any("101" in text for text in answer["alerts"]), "the cancelled answer announced itself"


@needs_node
@pytest.mark.asyncio
async def test_a_provider_switch_during_a_preview_returns_the_page_to_idle(http_client) -> None:
    source = await _browser(http_client)
    driver = (
        PREVIEW_DRIVER_PRELUDE
        + """
el("f-provider").value = "easyweek";
onProviderChange();
await settle();
"""
        + PREVIEW_DRIVER_CODA
    )
    _assert_cancelled_to_idle(_run_node(source, driver))


@needs_node
@pytest.mark.asyncio
async def test_a_branch_change_during_a_preview_returns_the_page_to_idle(http_client) -> None:
    source = await _browser(http_client)
    driver = (
        PREVIEW_DRIVER_PRELUDE
        + """
el("f-company").value = "1271200";
await fireEvent("f-company", "change");
"""
        + PREVIEW_DRIVER_CODA
    )
    _assert_cancelled_to_idle(_run_node(source, driver))


@needs_node
@pytest.mark.asyncio
async def test_editing_any_snapshot_field_during_a_preview_revokes_it(http_client) -> None:
    """With no context on screen yet — which is where the old check gave up."""
    edits = {
        "f-period-start": '"2026-07-01"',
        "f-period-end": '"2026-09-30"',
        "f-attribution": '"60"',
        "f-card-type": '"2002"',
        "f-followup-enabled": "true",
        "f-followup-delay": '"7"',  # the server renders 3, so 3 is not a change
        "f-followup-policy": '"skip_if_booked"',
        "f-followup-template": '"some_template"',
    }
    for field, value in edits.items():
        source = await _browser(http_client)
        if field.startswith("f-followup-") and field != "f-followup-enabled":
            # Those three only mean anything while follow-up is on.
            setup = 'el("f-followup-enabled").checked = true;\n'
        else:
            setup = ""
        driver = (
            setup
            + PREVIEW_DRIVER_PRELUDE
            + """
const target = %s;
if (typeof %s === "boolean") el(target).checked = %s; else el(target).value = %s;
await fireEvent(target, "change");
"""
            % (json.dumps(field), value, value, value)
            + PREVIEW_DRIVER_CODA
        )
        answer = _run_node(source, driver)
        try:
            _assert_cancelled_to_idle(answer)
        except AssertionError as failure:  # pragma: no cover - only on a regression
            raise AssertionError(f"{field}: {failure}") from failure


@needs_node
@pytest.mark.asyncio
async def test_a_second_preview_keeps_its_own_spinner_when_the_first_answers(http_client) -> None:
    """A must not clean up after B, and must not paint over it."""
    source = await _browser(http_client)
    driver = """
const slowA = deferred();
const slowB = deferred();
let started = 0;
ROUTES = async (url) => {
  if (url.indexOf("/new-clients/preview") !== -1) {
    started += 1;
    return started === 1 ? slowA.promise : slowB.promise;
  }
  return defaultRoutes(url);
};
await boot();

const runningA = createPreview();
await settle();
const runningB = createPreview();
await settle();

// A answers — late, and with an error, which is the worst case for cleanup.
slowA.resolve({ok: false, status: 500, body: {detail: "gateway"}});
await runningA;
await settle();
const whileBRuns = state();
const alertsWhileBRuns = ALERTS.slice();

slowB.resolve({ok: true, body: {id: 102, provider: "altegio", company_ids: [758285],
                                is_runnable_from_preview: true}});
await runningB;
await settle();
emit({whileBRuns: whileBRuns, alertsWhileBRuns: alertsWhileBRuns, after: state()});
"""
    answer = _run_node(source, driver)

    assert answer["whileBRuns"]["spinnerHidden"] is False, "A's cleanup hid B's spinner"
    assert answer["whileBRuns"]["previewDisabled"] is True, "A's cleanup released B's button"
    assert not any("gateway" in text for text in answer["alertsWhileBRuns"])
    assert answer["after"]["context"]["runId"] == 102
    assert answer["after"]["spinnerHidden"] is True
    assert answer["after"]["previewDisabled"] is False


@needs_node
@pytest.mark.asyncio
async def test_a_late_prefill_cannot_overwrite_a_preview_created_since(http_client) -> None:
    """One clock: the prefill is revoked by the preview that replaced it."""
    source = await _browser(http_client, "?from_preview=41")
    driver = """
RUN.id = 41;
RUN.company_ids = [1271200];
const latePrefill = deferred();
ROUTES = async (url) => {
  if (url === "/ops/campaigns/runs/41") return latePrefill.promise;
  if (url.indexOf("/new-clients/preview") !== -1) {
    return {ok: true, body: {id: 102, provider: "altegio", company_ids: [758285],
                             is_runnable_from_preview: true}};
  }
  return defaultRoutes(url);
};

await boot();          // the prefill is now in flight
// The operator builds their own preview instead. The first press is refused:
// a from-preview page has no live card list, and a preview without a card type
// is a snapshot that can never be run. The refusal asks for the list.
await createPreview();
await settle();
const refused = state();
await createPreview();
await settle();
const own = state();

latePrefill.resolve({ok: true, body: RUN});
await settle();

emit({refused: refused, own: own, after: state(), companyLocked: el("f-company").disabled});
"""
    answer = _run_node(source, driver)

    assert answer["refused"]["context"] is None, "a preview was built without a card type"
    assert answer["own"]["context"]["runId"] == 102
    assert answer["after"]["context"]["runId"] == 102, "the late prefill took the screen back"
    assert answer["after"]["previewRunId"] == 102
    assert answer["after"]["company"] == "758285", "the late prefill moved the branch"
    assert answer["companyLocked"] is False, "the late prefill froze the form"
    assert answer["after"]["runDisabled"] is False


@needs_node
@pytest.mark.asyncio
async def test_returning_to_the_original_scope_does_not_revive_a_revoked_prefill(
    http_client,
) -> None:
    """A → B → A. The clock only moves forward, so A's answer stays revoked."""
    cases = {
        "provider": """
el("f-provider").value = "easyweek";
onProviderChange();
await settle();
el("f-provider").value = "altegio";
onProviderChange();
await settle();
""",
        "company": """
el("f-company").value = "1271200";
await fireEvent("f-company", "change");
el("f-company").value = "758285";
await fireEvent("f-company", "change");
""",
    }
    for name, aba in cases.items():
        source = await _browser(http_client, "?from_preview=37")
        driver = (
            """
RUN.period_start = "2026-05-03T00:00:00Z";
const late = deferred();
ROUTES = async (url) => {
  if (url === "/ops/campaigns/runs/37") return late.promise;
  return defaultRoutes(url);
};
await boot();
const untouched = el("f-period-start").value;
"""
            + aba
            + """
late.resolve({ok: true, body: RUN});
await settle();
emit(Object.assign(state(), {
  untouched: untouched,
  period: el("f-period-start").value,
  companyLocked: el("f-company").disabled,
}));
"""
        )
        answer = _run_node(source, driver)

        assert answer["context"] is None, f"{name}: the revoked prefill was accepted on return"
        assert answer["previewRunId"] is None, name
        assert answer["period"] == answer["untouched"], name
        assert answer["companyLocked"] is False, name
        assert answer["runDisabled"] is True, name
        assert answer["spinnerHidden"] is True, name
        assert answer["previewDisabled"] is False, name


@needs_node
@pytest.mark.asyncio
async def test_an_easyweek_preview_keeps_the_message_that_refuses_it(http_client) -> None:
    """The refusal used to erase itself: invalidation cleared its own alert."""
    source = await _browser(http_client, "?from_preview=37")
    driver = """
RUN.provider = "easyweek";
await boot();
emit(Object.assign(state(), {alertText: el("preview-alert").innerHTML}));
"""
    answer = _run_node(source, driver)

    assert "EasyWeek" in answer["alertText"]
    assert "§37.1" in answer["alertText"]
    assert answer["context"] is None
    assert answer["runDisabled"] is True
    assert answer["spinnerHidden"] is True
    assert answer["previewDisabled"] is False


# ---------------------------------------------------------------------------
# `?from_preview=` is data, not code
# ---------------------------------------------------------------------------


# Everything a link, a typo or an attacker can put in the query string. Each one
# reaches a <script> block, so each one has to leave it a parseable script that
# does nothing the value asked for.
MALFORMED_FROM_PREVIEW = [
    "abc",
    "-1",
    "0",
    "1.5",
    "1e3",
    "99999999999999",
    "37abc",
    "  ",
    '1";alert("pwned");//',
    "1'};alert('pwned');{'",
    "1</script><script>window.PWNED=1;</script>",
    "1\nwindow.PWNED = 1;",
    "1 + window.PWNED",
    "(function(){window.PWNED=1;})()",
    "٣٧",
]


@pytest.mark.asyncio
async def test_a_malformed_from_preview_never_reaches_the_script(http_client) -> None:
    for raw in MALFORMED_FROM_PREVIEW:
        response = await http_client.get("/ops/campaigns/new-clients", params={"from_preview": raw})
        # A controlled page, not a 500 and not a broken one.
        assert response.status_code == 200, raw
        script = _page_script(response.text)

        declaration = [line.strip() for line in script.splitlines() if "FROM_PREVIEW_ID =" in line]
        assert declaration == ["const FROM_PREVIEW_ID = null;"], (raw, declaration)
        # Not merely escaped somewhere else on the page: absent from the script.
        # The page has `alert(` of its own, so the marker is what to look for.
        assert "pwned" not in script.lower(), raw
        # Short numeric junk like "-1" occurs incidentally in ordinary markup
        # ("me-1"), so only the distinctive payloads are checked verbatim.
        if any(character in raw for character in "(<;\n'\""):
            assert raw.strip() not in script, raw


@needs_node
@pytest.mark.asyncio
async def test_a_malformed_from_preview_leaves_a_working_page(http_client) -> None:
    """Parsed and executed, not just inspected: the injected code never runs."""
    for raw in MALFORMED_FROM_PREVIEW:
        page = (await http_client.get("/ops/campaigns/new-clients", params={"from_preview": raw})).text
        source = BROWSER + "\n" + _dom_seed(page) + "\n" + _page_script(page)
        driver = """
globalThis.PWNED = undefined;
await boot();
emit({
  pwned: globalThis.PWNED === undefined ? null : String(globalThis.PWNED),
  fromPreview: FROM_PREVIEW_ID,
  prefilled: FETCHES.some((f) => /\\/ops\\/campaigns\\/runs\\/\\d+$/.test(f.url)),
  cardTypes: FETCHES.some((f) => f.url.indexOf("/card-types") !== -1),
  context: PREVIEW_CONTEXT,
});
"""
        answer = _run_node(source, driver)

        assert answer["pwned"] is None, raw
        assert answer["fromPreview"] is None, raw
        assert answer["prefilled"] is False, raw
        # An ordinary page: it loads card types and offers nothing to run.
        assert answer["cardTypes"] is True, raw
        assert answer["context"] is None, raw


@needs_node
@pytest.mark.asyncio
async def test_a_well_formed_from_preview_still_works(http_client) -> None:
    """Including with the whitespace a hand-edited URL picks up."""
    for raw, expected in (("41", 41), ("  41  ", 41), ("00041", 41)):
        page = (await http_client.get("/ops/campaigns/new-clients", params={"from_preview": raw})).text
        source = BROWSER + "\n" + _dom_seed(page) + "\n" + _page_script(page)
        driver = """
RUN.id = 41;
RUN.company_ids = [1271200];
await boot();
emit({fromPreview: FROM_PREVIEW_ID, context: PREVIEW_CONTEXT, company: el("f-company").value});
"""
        answer = _run_node(source, driver)

        assert answer["fromPreview"] == expected, raw
        assert answer["context"]["runId"] == 41, raw
        assert answer["company"] == "1271200", raw


# ---------------------------------------------------------------------------
# A preview is never built before its card types are
# ---------------------------------------------------------------------------


@needs_node
@pytest.mark.asyncio
async def test_a_preview_is_refused_while_card_types_are_still_loading(http_client) -> None:
    """And accepted, with the real card id, once the list has landed."""
    source = await _browser(http_client)
    driver = """
const slowCards = deferred();
ROUTES = async (url) => {
  if (url.indexOf("/card-types") !== -1) return slowCards.promise;
  if (url.indexOf("/new-clients/preview") !== -1) {
    return {ok: true, body: {id: 101, provider: "altegio", company_ids: [758285],
                             is_runnable_from_preview: true}};
  }
  return defaultRoutes(url);
};

await boot();
const pending = state();
await createPreview();
await settle();
const attempted = {
  posts: FETCHES.filter((f) => f.url.indexOf("/new-clients/preview") !== -1),
  alert: el("preview-alert").innerHTML,
  context: PREVIEW_CONTEXT,
};

slowCards.resolve({ok: true, body: CARD_TYPES});
await settle();
const ready = state();

await createPreview();
await settle();
emit({
  pending: pending,
  attempted: attempted,
  ready: ready,
  posts: FETCHES.filter((f) => f.url.indexOf("/new-clients/preview") !== -1).map((f) => f.body),
  after: state(),
});
"""
    answer = _run_node(source, driver)

    assert answer["pending"]["cardStatus"] == "loading"
    # Nothing went out, and the operator was told why.
    assert answer["attempted"]["posts"] == [], "a preview was requested before its card types"
    assert answer["attempted"]["context"] is None
    assert "Типы карт" in answer["attempted"]["alert"]

    assert answer["ready"]["cardStatus"] == "ready"
    assert answer["ready"]["card"] == "1001"
    assert answer["ready"]["cardDisabled"] is False
    # Exactly one POST, carrying the real card id rather than null.
    assert len(answer["posts"]) == 1
    assert answer["posts"][0]["card_type_id"] == "1001"
    assert answer["posts"][0]["provider"] == "altegio"
    assert answer["after"]["context"]["runId"] == 101


@needs_node
@pytest.mark.asyncio
async def test_a_failed_card_types_load_is_shown_and_can_be_retried(http_client) -> None:
    source = await _browser(http_client)
    driver = """
let attempts = 0;
ROUTES = async (url) => {
  if (url.indexOf("/card-types") !== -1) {
    attempts += 1;
    if (attempts === 1) return {ok: false, status: 502, body: {detail: "upstream is down"}};
    return {ok: true, body: CARD_TYPES};
  }
  if (url.indexOf("/new-clients/preview") !== -1) {
    return {ok: true, body: {id: 101, provider: "altegio", company_ids: [758285],
                             is_runnable_from_preview: true}};
  }
  return defaultRoutes(url);
};

await boot();
const failed = {status: cardTypesStatus(), text: el("card-load-status").textContent,
                disabled: el("f-card-type").disabled};
await createPreview();
await settle();
const refused = {
  posts: FETCHES.filter((f) => f.url.indexOf("/new-clients/preview") !== -1).length,
  alert: el("preview-alert").innerHTML,
};

// The retry the operator has: pick the branch again.
await fireEvent("f-company", "change");
const recovered = state();
await createPreview();
await settle();

emit({failed: failed, refused: refused, recovered: recovered,
      posts: FETCHES.filter((f) => f.url.indexOf("/new-clients/preview") !== -1).map((f) => f.body),
      after: state()});
"""
    answer = _run_node(source, driver)

    assert answer["failed"]["status"] == "failed"
    assert "upstream is down" in answer["failed"]["text"]
    assert "повторить" in answer["failed"]["text"]
    assert answer["failed"]["disabled"] is True
    assert answer["refused"]["posts"] == 0
    assert "недоступны" in answer["refused"]["alert"]

    assert answer["recovered"]["cardStatus"] == "ready"
    assert answer["recovered"]["card"] == "1001"
    assert len(answer["posts"]) == 1
    assert answer["posts"][0]["card_type_id"] == "1001"
    assert answer["after"]["context"]["runId"] == 101


@needs_node
@pytest.mark.asyncio
async def test_a_provider_round_trip_reloads_the_card_list(http_client) -> None:
    """Altegio → EasyWeek → Altegio, with the first list still in flight."""
    source = await _browser(http_client)
    driver = """
const slowFirst = deferred();
let asked = 0;
ROUTES = async (url) => {
  if (url.indexOf("/card-types") !== -1) {
    asked += 1;
    if (asked === 1) return slowFirst.promise;
    return {ok: true, body: [{id: "3003", title: "Gold"}]};
  }
  return defaultRoutes(url);
};

await boot();                      // the first list is in flight
el("f-provider").value = "easyweek";
onProviderChange();
await settle();
const inEasyWeek = state();

el("f-provider").value = "altegio";
onProviderChange();
await settle();
const back = state();

// Only now does the first, abandoned request answer.
slowFirst.resolve({ok: true, body: CARD_TYPES});
await settle();

emit({
  inEasyWeek: inEasyWeek,
  back: back,
  after: state(),
  asked: FETCHES.map((f) => f.url).filter((u) => u.indexOf("/card-types") !== -1),
});
"""
    answer = _run_node(source, driver)

    assert answer["inEasyWeek"]["cardState"] == "idle", "the Altegio loader was left owning the page"
    # Coming back asked again, for the branch on screen.
    assert len(answer["asked"]) == 2
    assert "location_id=758285" in answer["asked"][1]
    assert answer["back"]["cardStatus"] == "ready"
    assert answer["back"]["cardOptions"] == ["3003"]
    # And the abandoned answer changed nothing.
    assert answer["after"]["cardOptions"] == ["3003"], "the abandoned list painted over the new one"
    assert answer["after"]["card"] == "3003"


@needs_node
@pytest.mark.asyncio
async def test_a_refused_prefill_hands_back_an_ordinary_working_page(http_client) -> None:
    """The warning stays, the form is usable, and the page reads its own branch."""
    source = await _browser(http_client, "?from_preview=41")
    driver = """
RUN.id = 41;
RUN.provider = "easyweek";   // refused: the editor opens it, the runner does not
ROUTES = async (url) => {
  if (url.indexOf("/new-clients/preview") !== -1) {
    return {ok: true, body: {id: 105, provider: "altegio", company_ids: [758285],
                             is_runnable_from_preview: true}};
  }
  return defaultRoutes(url);
};

await boot();
const refused = Object.assign(state(), {alert: el("preview-alert").innerHTML});

await createPreview();
await settle();
emit({
  refused: refused,
  after: state(),
  alertAfter: el("preview-alert").innerHTML,
  posts: FETCHES.filter((f) => f.url.indexOf("/new-clients/preview") !== -1).map((f) => f.body),
  cardsAsked: FETCHES.map((f) => f.url).filter((u) => u.indexOf("/card-types") !== -1),
  outstandingAsked: FETCHES.map((f) => f.url).filter((u) => u.indexOf("/outstanding-cards") !== -1),
});
"""
    answer = _run_node(source, driver)

    # The explanation survives the takedown that follows it.
    assert "EasyWeek" in answer["refused"]["alert"]
    assert answer["refused"]["context"] is None
    # The form is not left frozen.
    assert answer["refused"]["runDisabled"] is True
    assert answer["refused"]["previewDisabled"] is False
    # And the page went back to reading what an ordinary page reads.
    assert len(answer["cardsAsked"]) == 1
    assert "location_id=758285" in answer["cardsAsked"][0]
    assert len(answer["outstandingAsked"]) == 1
    assert "company_id=758285" in answer["outstandingAsked"][0]
    # An ordinary preview can be built again, with a real card.
    assert len(answer["posts"]) == 1
    assert answer["posts"][0]["card_type_id"] == "1001"
    assert answer["after"]["context"]["runId"] == 105


@needs_node
@pytest.mark.asyncio
async def test_a_snapshot_without_a_card_type_cannot_be_run(http_client) -> None:
    """Fail closed, with a reason — not a silently substituted card."""
    source = await _browser(http_client, "?from_preview=41")
    driver = """
RUN.id = 41;
RUN.card_type_id = null;
await boot();
emit(Object.assign(state(), {alert: el("preview-alert").innerHTML}));
"""
    answer = _run_node(source, driver)

    assert answer["context"]["runId"] == 41, "the preview still opens for inspection"
    assert answer["card"] == "", "a card type was substituted"
    assert answer["runDisabled"] is True
    assert "тип карты" in answer["alert"]


# ---------------------------------------------------------------------------
# Outstanding cards follow the run's branch, in either finishing order
# ---------------------------------------------------------------------------


@needs_node
@pytest.mark.asyncio
async def test_outstanding_cards_follow_a_non_default_preview(http_client) -> None:
    """Karlsruhe is the default; the run is Rastatt's.

    Selecting a branch from JavaScript fires no `change`, so nothing reloaded
    the panel and the page showed one branch's loyalty cards underneath another
    branch's preview — with Delete enabled over them. Both finishing orders are
    driven, because which request wins the race is not ours to choose.
    """
    for order in ("outstanding-first", "run-first"):
        source = await _browser(http_client, "?from_preview=41")
        driver = """
RUN.id = 41;
RUN.company_ids = [1271200];

const slowRun = deferred();
const slowOutstanding = {};
ROUTES = async (url) => {
  if (url === "/ops/campaigns/runs/41") return slowRun.promise;
  if (url.indexOf("/outstanding-cards") !== -1) {
    const company = (url.match(/company_id=(\\d+)/) || [])[1] || "";
    if (company === "758285") {
      slowOutstanding.karlsruhe = slowOutstanding.karlsruhe || deferred();
      return slowOutstanding.karlsruhe.promise;
    }
  }
  return defaultRoutes(url);
};

await boot();
// A Karlsruhe read may exist from an earlier page state; drive it explicitly so
// the race is real rather than assumed.
const strayKarlsruhe = loadOutstandingCards("758285");
await settle();

const answerKarlsruhe = () => {
  if (slowOutstanding.karlsruhe) {
    slowOutstanding.karlsruhe.resolve({ok: true, body: {cards: OUTSTANDING["758285"]}});
  }
};
const answerRun = () => slowRun.resolve({ok: true, body: RUN});

if (%s) { answerKarlsruhe(); await settle(); answerRun(); }
else { answerRun(); await settle(); answerKarlsruhe(); }
await settle();
await strayKarlsruhe;
await settle();

// And the delete the operator can now press.
await deleteOutstandingCards();
await settle();

emit(Object.assign(state(), {
  outstandingAsked: FETCHES.map((f) => f.url).filter((u) => u.indexOf("/outstanding-cards") !== -1),
  deletePayload: (FETCHES.filter((f) => f.url.indexOf("/bulk-delete-cards") !== -1)[0] || {}).body,
  deleteResult: el("outstanding-delete-result").innerHTML,
}));
""" % ("true" if order == "outstanding-first" else "false")
        answer = _run_node(source, driver)

        assert answer["context"]["runId"] == 41, order
        assert answer["company"] == "1271200", order
        # The table holds the run's branch and nothing else.
        assert "Rastatt One" in answer["outstanding"], order
        assert "Karlsruhe One" not in answer["outstanding"], f"{order}: the default branch's rows survived"
        assert any("company_id=1271200" in url for url in answer["outstandingAsked"]), order
        # The scope, and therefore Delete, belongs to Rastatt.
        assert answer["outstandingScope"] == {"provider": "altegio", "companyId": "1271200"}, order
        assert answer["deleteDisabled"] is False, order
        assert answer["deletePayload"]["company_id"] == 1271200, order
        assert answer["deletePayload"]["provider"] == "altegio", order


@needs_node
@pytest.mark.asyncio
async def test_a_from_preview_page_asks_for_no_default_branch_cards(http_client) -> None:
    """The race is not merely lost by the stale answer; it is never started."""
    source = await _browser(http_client, "?from_preview=41")
    driver = """
RUN.id = 41;
RUN.company_ids = [1271200];
await boot();
emit({asked: FETCHES.map((f) => f.url).filter((u) => u.indexOf("/outstanding-cards") !== -1)});
"""
    asked = _run_node(source, driver)["asked"]

    assert len(asked) == 1, asked
    assert "company_id=1271200" in asked[0]


# ---------------------------------------------------------------------------
# Recipients: one owner, every outcome
# ---------------------------------------------------------------------------


RECIPIENTS_CASES = {
    "success": '{ok: true, body: {items: [{id: 1, phone_e164: "+49STALE"}], total: 1, provider: "altegio"}}',
    "http_error": '{ok: false, status: 500, body: {detail: "stale failure"}}',
    "malformed": "{ok: true, malformed: true}",
    "exception": '{throw: "stale network"}',
}


@needs_node
@pytest.mark.asyncio
async def test_only_the_newest_recipients_read_may_write_the_screen(http_client) -> None:
    """Two reads finishing backwards — for every way the old one can end."""
    for name, stale_answer in RECIPIENTS_CASES.items():
        source = await _browser(http_client, "?from_preview=37")
        driver = (
            """
const slowFirst = deferred();
let reads = 0;
ROUTES = async (url) => {
  if (url.indexOf("/recipients") !== -1) {
    reads += 1;
    if (reads === 1) return slowFirst.promise;
    return {ok: true, body: {items: [{id: 2, phone_e164: "+49FRESH"}], total: 1, provider: "altegio"}};
  }
  return defaultRoutes(url);
};

await boot();                     // the prefill starts read #1
const second = loadRecipients(false);
await settle();
await second;
await settle();
const fresh = state();

// Now the abandoned first read ends, in the way this case is about.
slowFirst.resolve(%s);
await settle();
emit({fresh: fresh, after: state()});
"""
            % stale_answer
        )
        answer = _run_node(source, driver)

        fresh, after = answer["fresh"], answer["after"]
        assert "+49FRESH" in fresh["recipientsTable"], name
        assert fresh["recipientsLoadingHidden"] is True, name
        # Nothing the abandoned read does reaches the screen.
        assert after["recipientsTable"] == fresh["recipientsTable"], f"{name}: the stale read repainted"
        assert "+49STALE" not in after["recipientsTable"], name
        assert "stale failure" not in after["recipientsTable"], name
        assert "stale network" not in after["recipientsTable"], name
        assert "разобрать" not in after["recipientsTable"], name
        assert after["recipientsLoadingHidden"] is True, name


@needs_node
@pytest.mark.asyncio
async def test_a_stale_finally_does_not_put_out_the_new_spinner(http_client) -> None:
    source = await _browser(http_client, "?from_preview=37")
    driver = """
const slowFirst = deferred();
const slowSecond = deferred();
let reads = 0;
ROUTES = async (url) => {
  if (url.indexOf("/recipients") !== -1) {
    reads += 1;
    return reads === 1 ? slowFirst.promise : slowSecond.promise;
  }
  return defaultRoutes(url);
};

await boot();                       // read #1 in flight
const second = loadRecipients(false);
await settle();

// #1 ends while #2 is still running.
slowFirst.resolve({ok: true, body: {items: [], total: 0, provider: "altegio"}});
await settle();
const whileSecondRuns = state();

slowSecond.resolve({ok: true, body: {items: [{id: 2, phone_e164: "+49FRESH"}], total: 1,
                                     provider: "altegio"}});
await second;
await settle();
emit({whileSecondRuns: whileSecondRuns, after: state()});
"""
    answer = _run_node(source, driver)

    assert answer["whileSecondRuns"]["recipientsLoadingHidden"] is False, "the stale finally hid the spinner"
    assert answer["after"]["recipientsLoadingHidden"] is True
    assert "+49FRESH" in answer["after"]["recipientsTable"]


@needs_node
@pytest.mark.asyncio
async def test_invalidation_puts_the_recipients_spinner_out(http_client) -> None:
    """Refusing the old request the right to hide it is only half the rule."""
    source = await _browser(http_client, "?from_preview=37")
    driver = """
const never = deferred();
ROUTES = async (url) => {
  if (url.indexOf("/recipients") !== -1) return never.promise;
  return defaultRoutes(url);
};
await boot();
const loading = state();

invalidatePreviewContext();
await settle();
const cleared = state();

// The abandoned read finally answers, into a page that has moved on.
never.resolve({ok: true, body: {items: [{id: 9, phone_e164: "+49STALE"}], total: 1,
                                provider: "altegio"}});
await settle();
emit({loading: loading, cleared: cleared, after: state()});
"""
    answer = _run_node(source, driver)

    assert answer["loading"]["recipientsLoadingHidden"] is False
    assert answer["cleared"]["recipientsLoadingHidden"] is True, "invalidation left the spinner running"
    assert answer["cleared"]["recipientsTable"] == ""
    assert answer["after"]["recipientsTable"] == ""
    assert "+49STALE" not in answer["after"]["recipientsTable"]


@needs_node
@pytest.mark.asyncio
async def test_discarding_a_snapshot_gives_the_live_card_list_back(http_client) -> None:
    """A frozen select must not outlive the snapshot that froze it.

    The synthetic «из preview» option looks like a loaded list. Once the preview
    it belongs to is gone it is a leftover: the page has to fetch a real list
    for the branch on screen before another preview can be built.
    """
    source = await _browser(http_client, "?from_preview=41")
    driver = """
RUN.id = 41;
RUN.company_ids = [1271200];
RUN.card_type_id = "7007";          // not in any live list

const slowCards = deferred();
ROUTES = async (url) => {
  if (url.indexOf("/card-types") !== -1) return slowCards.promise;
  if (url.indexOf("/new-clients/preview") !== -1) {
    return {ok: true, body: {id: 110, provider: "altegio", company_ids: [1271200],
                             is_runnable_from_preview: true}};
  }
  return defaultRoutes(url);
};

await boot();
const frozen = state();

// The operator edits a snapshot parameter, which discards the preview.
el("f-period-end").value = "2026-09-30";
await fireEvent("f-period-end", "change");
const discarded = state();
// Asked for by the invalidation itself, before anything else happens.
const askedAfterDiscard = FETCHES.map((f) => f.url).filter((u) => u.indexOf("/card-types") !== -1);

// While the real list is still on its way, the leftover option proves nothing.
await createPreview();
await settle();
const tooEarly = {
  posts: FETCHES.filter((f) => f.url.indexOf("/new-clients/preview") !== -1).length,
  status: cardTypesStatus(),
};

slowCards.resolve({ok: true, body: CARD_TYPES});
await settle();
const recovered = state();

await createPreview();
await settle();
emit({
  frozen: frozen, discarded: discarded, tooEarly: tooEarly, recovered: recovered,
  askedAfterDiscard: askedAfterDiscard,
  asked: FETCHES.map((f) => f.url).filter((u) => u.indexOf("/card-types") !== -1),
  posts: FETCHES.filter((f) => f.url.indexOf("/new-clients/preview") !== -1).map((f) => f.body),
  after: state(),
});
"""
    answer = _run_node(source, driver)

    assert answer["frozen"]["cardState"] == "snapshot"
    assert answer["frozen"]["card"] == "7007"
    assert answer["frozen"]["cardDisabled"] is True

    # Discarding asks for a real list, for the branch that is on screen — and
    # the invalidation itself does it, not the next thing the operator tries.
    assert answer["discarded"]["context"] is None
    assert len(answer["askedAfterDiscard"]) == 1, answer["askedAfterDiscard"]
    assert "location_id=1271200" in answer["askedAfterDiscard"][0]
    assert answer["discarded"]["cardState"] == "loading"
    assert len(answer["asked"]) == 1, answer["asked"]
    assert "location_id=1271200" in answer["asked"][0]
    assert answer["tooEarly"]["status"] == "loading"
    assert answer["tooEarly"]["posts"] == 0, "a preview was built on a leftover option"

    assert answer["recovered"]["cardStatus"] == "ready"
    assert answer["recovered"]["cardDisabled"] is False
    assert "7007" not in answer["recovered"]["cardOptions"], "the leftover option survived"
    assert len(answer["posts"]) == 1
    assert answer["posts"][0]["card_type_id"] == "1001"
    assert answer["after"]["context"]["runId"] == 110


@needs_node
@pytest.mark.asyncio
async def test_a_snapshots_card_list_counts_only_while_its_snapshot_does(http_client) -> None:
    """The shipped rule, executed on its own.

    `cardTypesStatus` is the one place that answers "may a preview be built?".
    Its answer for a snapshot must depend on that snapshot still being loaded,
    independently of whatever else the page does afterwards to recover.
    """
    source = await _lifecycle(http_client)
    driver = """
const out = {};
CARD_TYPES_STATE = {state: "snapshot", scope: {provider: "altegio", companyId: "758285"}};

PREVIEW_CONTEXT = {runId: 7, provider: "altegio", companyId: "758285", runnable: true,
                   signature: snapshotSignature()};
out.withSnapshot = cardTypesStatus();

// The snapshot is gone; the frozen option is a leftover.
PREVIEW_CONTEXT = null;
out.withoutSnapshot = cardTypesStatus();

// And a snapshot for another branch is not this branch's list either.
PREVIEW_CONTEXT = {runId: 7, provider: "altegio", companyId: "758285", runnable: true,
                   signature: snapshotSignature()};
CARD_TYPES_STATE = {state: "snapshot", scope: {provider: "altegio", companyId: "1271200"}};
out.otherBranch = cardTypesStatus();

// A live list, by contrast, is only good for the branch it was loaded for.
PREVIEW_CONTEXT = null;
CARD_TYPES_STATE = {state: "loaded", scope: {provider: "altegio", companyId: "758285"}};
out.liveHere = cardTypesStatus();
CARD_TYPES_STATE = {state: "loaded", scope: {provider: "altegio", companyId: "1271200"}};
out.liveElsewhere = cardTypesStatus();

console.log(JSON.stringify(out));
"""
    answer = _run_node(source, driver)

    assert answer["withSnapshot"] == "ready"
    assert answer["withoutSnapshot"] == "stale", "a leftover option passed as a loaded list"
    assert answer["otherBranch"] == "stale"
    assert answer["liveHere"] == "ready"
    assert answer["liveElsewhere"] == "stale"


# ---------------------------------------------------------------------------
# Building a new preview from an open `?from_preview=`
# ---------------------------------------------------------------------------
#
# The operator opens a saved preview, changes nothing they need to change, and
# presses Create Preview to get a fresh snapshot of the same parameters. The
# card type is the snapshot's frozen one, and it has to survive the moment the
# old preview is taken down — the payload and the signature are read on either
# side of that moment.


@needs_node
@pytest.mark.asyncio
async def test_pressing_create_preview_on_a_loaded_snapshot_builds_a_new_run(
    http_client,
) -> None:
    """Driven through the button's own click handler, not by calling the function.

    Taking the previous preview down used to fetch a live card list, which
    blanked the select synchronously: the POST carried card 7007 while the
    signature it was checked against carried none.
    """
    source = await _browser(http_client, "?from_preview=41")
    driver = """
RUN.id = 41;
RUN.company_ids = [1271200];
RUN.card_type_id = "7007";          // frozen, and in no live list

const slowPost = deferred();
ROUTES = async (url) => {
  if (url.indexOf("/new-clients/preview") !== -1) return slowPost.promise;
  return defaultRoutes(url);
};

await boot();
const loaded = Object.assign(state(), {previewButtonDisabled: el("btn-preview").disabled});

// The operator presses the button that is on the page.
const pressed = fireEvent("btn-preview", "click");
await settle();
const inFlight = Object.assign(state(), {
  cardsAsked: FETCHES.map((f) => f.url).filter((u) => u.indexOf("/card-types") !== -1),
  posts: FETCHES.filter((f) => f.url.indexOf("/new-clients/preview") !== -1).map((f) => f.body),
});

slowPost.resolve({ok: true, body: {id: 210, provider: "altegio", company_ids: [1271200],
                                   is_runnable_from_preview: true, total_clients_seen: 12,
                                   candidates_count: 12}});
await pressed;
await settle();

emit({
  loaded: loaded,
  inFlight: inFlight,
  after: state(),
  alert: el("preview-alert").innerHTML,
  cardsAsked: FETCHES.map((f) => f.url).filter((u) => u.indexOf("/card-types") !== -1),
  recipientsAsked: FETCHES.map((f) => f.url).filter((u) => u.indexOf("/recipients") !== -1),
});
"""
    answer = _run_node(source, driver)

    # The saved preview loaded, frozen on its own card, and the button is one
    # the operator can actually press.
    assert answer["loaded"]["context"]["runId"] == 41
    assert answer["loaded"]["card"] == "7007"
    assert answer["loaded"]["runDisabled"] is False
    assert answer["loaded"]["previewButtonDisabled"] is False

    # Exactly one POST, carrying the frozen card — and no card-types GET racing it.
    assert len(answer["inFlight"]["posts"]) == 1, answer["inFlight"]["posts"]
    assert answer["inFlight"]["posts"][0]["card_type_id"] == "7007"
    assert answer["inFlight"]["posts"][0]["company_id"] == 1271200
    assert answer["inFlight"]["cardsAsked"] == [], "a competing card-types read blanked the select"
    assert answer["inFlight"]["card"] == "7007", "the frozen card was cleared mid-request"
    assert answer["inFlight"]["spinnerHidden"] is False

    # The new run is the one on screen, and it is internally consistent.
    after = answer["after"]
    assert after["context"]["runId"] == 210
    assert after["previewRunId"] == 210
    assert after["context"]["signature"]["cardTypeId"] == "7007"
    assert after["context"]["companyId"] == "1271200"
    assert after["card"] == "7007"
    assert after["cardState"] == "snapshot"
    assert after["cardStatus"] == "ready", "the card select was left loading"
    assert after["spinnerHidden"] is True
    assert after["previewDisabled"] is False
    assert after["resultsHidden"] is False
    # Run is offered: the server allows it and the form still describes the run.
    assert after["runDisabled"] is False
    assert "Run ID: 210" in answer["alert"]
    # And the new run's recipients were read, not the old one's.
    assert any("/runs/210/recipients" in url for url in answer["recipientsAsked"])
    assert answer["cardsAsked"] == []
    assert after["rejections"] == []


@needs_node
@pytest.mark.asyncio
async def test_a_repeat_preview_is_not_offered_for_run_when_the_server_refuses(
    http_client,
) -> None:
    """The same path, with the server's verdict being no."""
    source = await _browser(http_client, "?from_preview=41")
    driver = """
RUN.id = 41;
RUN.company_ids = [1271200];
RUN.card_type_id = "7007";
ROUTES = async (url) => {
  if (url.indexOf("/new-clients/preview") !== -1) {
    return {ok: true, body: {id: 211, provider: "altegio", company_ids: [1271200],
                             is_runnable_from_preview: false}};
  }
  return defaultRoutes(url);
};
await boot();
await fireEvent("btn-preview", "click");
emit(state());
"""
    answer = _run_node(source, driver)

    assert answer["context"]["runId"] == 211
    assert answer["context"]["runnable"] is False
    assert answer["context"]["signature"]["cardTypeId"] == "7007"
    assert answer["runDisabled"] is True
    assert answer["cardStatus"] == "ready"
    assert answer["spinnerHidden"] is True


@needs_node
@pytest.mark.asyncio
async def test_a_failed_repeat_preview_leaves_an_ordinary_usable_page(http_client) -> None:
    """Both ways it can fail, and the page is editable again afterwards.

    There is nothing to go back to — the loaded snapshot was revoked before the
    request left — so what has to be true is that the page is ordinary again:
    no spinner, no held button, and a real card list instead of the frozen card
    whose snapshot is gone.
    """
    for failure in ('{ok: false, status: 500, body: {detail: "preview exploded"}}', '{throw: "connection reset"}'):
        source = await _browser(http_client, "?from_preview=41")
        driver = (
            """
RUN.id = 41;
RUN.company_ids = [1271200];
RUN.card_type_id = "7007";

const slowCards = deferred();
ROUTES = async (url) => {
  if (url.indexOf("/new-clients/preview") !== -1 && FETCHES.filter(
        (f) => f.url.indexOf("/new-clients/preview") !== -1).length === 1) {
    return %s;
  }
  if (url.indexOf("/new-clients/preview") !== -1) {
    return {ok: true, body: {id: 212, provider: "altegio", company_ids: [1271200],
                             is_runnable_from_preview: true}};
  }
  if (url.indexOf("/card-types") !== -1) return slowCards.promise;
  return defaultRoutes(url);
};

await boot();
await fireEvent("btn-preview", "click");
const failed = Object.assign(state(), {
  alert: el("preview-alert").innerHTML,
  cardsAsked: FETCHES.map((f) => f.url).filter((u) => u.indexOf("/card-types") !== -1),
});

// Pressing again while the list is still coming is refused, not queued.
await fireEvent("btn-preview", "click");
const whileLoading = {
  posts: FETCHES.filter((f) => f.url.indexOf("/new-clients/preview") !== -1).length,
  cardsAsked: FETCHES.map((f) => f.url).filter((u) => u.indexOf("/card-types") !== -1).length,
};

slowCards.resolve({ok: true, body: CARD_TYPES});
await settle();
const recovered = state();

await fireEvent("btn-preview", "click");
emit({
  failed: failed, whileLoading: whileLoading, recovered: recovered, after: state(),
  posts: FETCHES.filter((f) => f.url.indexOf("/new-clients/preview") !== -1).map((f) => f.body),
  cardsAsked: FETCHES.map((f) => f.url).filter((u) => u.indexOf("/card-types") !== -1),
});
"""
            % failure
        )
        answer = _run_node(source, driver)
        label = failure[:24]

        # The old preview is not restored, and nothing is left running.
        assert answer["failed"]["context"] is None, label
        assert answer["failed"]["previewRunId"] is None, label
        assert answer["failed"]["spinnerHidden"] is True, label
        assert answer["failed"]["previewDisabled"] is False, label
        assert answer["failed"]["runDisabled"] is True, label
        assert answer["failed"]["recipientsLoadingHidden"] is True, label
        # A live list was asked for exactly once, for the branch on screen.
        assert len(answer["failed"]["cardsAsked"]) == 1, answer["failed"]["cardsAsked"]
        assert "location_id=1271200" in answer["failed"]["cardsAsked"][0], label
        assert answer["whileLoading"]["posts"] == 1, f"{label}: a preview was sent on a leftover card"
        assert answer["whileLoading"]["cardsAsked"] == 1, f"{label}: the list was fetched twice"

        # Once it lands the page is an ordinary one that can build previews.
        assert answer["recovered"]["cardStatus"] == "ready", label
        assert answer["recovered"]["cardDisabled"] is False, label
        assert "7007" not in answer["recovered"]["cardOptions"], label
        assert len(answer["posts"]) == 2, label
        assert answer["posts"][1]["card_type_id"] == "1001", label
        assert answer["after"]["context"]["runId"] == 212, label
        assert answer["after"]["spinnerHidden"] is True, label


@needs_node
@pytest.mark.asyncio
async def test_a_repeat_preview_answer_is_dropped_when_the_branch_moved(http_client) -> None:
    """And it does not take the new branch's reference reads with it."""
    source = await _browser(http_client, "?from_preview=41")
    driver = """
RUN.id = 41;
RUN.company_ids = [1271200];
RUN.card_type_id = "7007";

const slowPost = deferred();
ROUTES = async (url) => {
  if (url.indexOf("/new-clients/preview") !== -1) return slowPost.promise;
  return defaultRoutes(url);
};

await boot();
const pressed = fireEvent("btn-preview", "click");
await settle();

// The operator moves to the other branch while the POST is in flight.
el("f-company").value = "758285";
await fireEvent("f-company", "change");
const moved = state();

slowPost.resolve({ok: true, body: {id: 213, provider: "altegio", company_ids: [1271200],
                                   is_runnable_from_preview: true}});
await pressed;
await settle();

emit({
  moved: moved,
  after: state(),
  alert: el("preview-alert").innerHTML,
  cardsAsked: FETCHES.map((f) => f.url).filter((u) => u.indexOf("/card-types") !== -1),
});
"""
    answer = _run_node(source, driver)

    # The branch change asked for that branch's list; the late answer left it be.
    assert len(answer["cardsAsked"]) == 1
    assert "location_id=758285" in answer["cardsAsked"][0]
    assert answer["after"]["cardStatus"] == "ready"
    assert answer["after"]["cardOptions"] == ["1001", "2002"]
    # The answer for the branch that was left is not on screen.
    assert answer["after"]["context"] is None
    assert answer["after"]["previewRunId"] is None
    assert "213" not in answer["alert"]
    # And the page is idle, not stuck mid-request.
    assert answer["after"]["spinnerHidden"] is True
    assert answer["after"]["previewDisabled"] is False
    assert answer["after"]["runDisabled"] is True


@pytest.mark.asyncio
async def test_the_request_is_captured_before_anything_is_taken_down(http_client) -> None:
    """A source-order check, and deliberately so.

    With the takedown no longer touching the card select, reading the payload
    or the signature afterwards happens to give the same answer — so no
    executed test can tell the two orders apart today. That is exactly why the
    ordering is worth pinning: the defect this fixes was a DOM change sneaking
    in between the payload and the signature, and the next one would be too.
    """
    script = _page_script((await http_client.get("/ops/campaigns/new-clients")).text)
    create = _function_source(script, "createPreview")

    capture = create.index("const captured = {")
    takedown = create.index("revokePreviewFor(captured);")
    request = create.index('await fetch("/ops/campaigns/new-clients/preview"')
    assert capture < takedown < request

    # Everything the request is judged by comes out of that one capture.
    assert "payload: buildPayload()," in create
    assert "scope: currentScope()," in create
    assert "signature: snapshotSignature()," in create
    assert "const asked = captured.scope;" in create
    assert "const askedSignature = captured.signature;" in create
    assert "JSON.stringify(captured.payload)" in create
    # And nothing re-reads the form between the takedown and the request.
    between = create[takedown:request]
    for reread in ("buildPayload()", "snapshotSignature()", "currentScope()"):
        assert reread not in between, f"{reread} is read after the takedown"
