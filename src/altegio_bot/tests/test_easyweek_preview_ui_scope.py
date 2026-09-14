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
    # It forgets the page state and offers nothing to run.
    assert "PREVIEW_CONTEXT = null;" in invalidate
    assert "previewRunId = null;" in invalidate
    assert "applyRunAvailability();" in invalidate
    # And it does not delete anything: no fetch, no discard, no delete.
    for destructive in ("fetch(", "discard", "delete"):
        assert destructive not in invalidate.lower()

    # Called from the provider switch and from both branch selectors.
    assert script.count("invalidatePreviewContext();") >= 3


@pytest.mark.asyncio
async def test_returning_to_altegio_reloads_that_branch_cards(http_client) -> None:
    script = _page_script((await http_client.get("/ops/campaigns/new-clients")).text)
    switch = _function_source(script, "onProviderChange")

    # Leaving EasyWeek reloads the Altegio list for the branch selected now.
    assert "loadOutstandingCards(altegioCompany.value)" in switch
    # And the button is disabled until that load succeeds.
    assert "deleteBtn.disabled = true;" in switch


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
        *extra,
    ]
    bodies = "\n".join(_function_source(script, name) for name in names)
    # The module-level state the functions read, declared the way the page does.
    state = "let PREVIEW_CONTEXT = null;\nlet PREVIEW_GENERATION = 0;\n"
    state += "let RECIPIENTS_GENERATION = 0;\nlet previewRunId = null;\n"
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
const EVENTS = {};
const ELEMENTS = {};
const ALERTS = [];
const FETCHES = [];
const REJECTIONS = [];

function makeElement(id) {
  const element = {
    id: id,
    value: "",
    checked: false,
    disabled: false,
    innerHTML: "",
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
  ELEMENTS[id] = element;
  return element;
}

globalThis.el = function (id) { return ELEMENTS[id] || makeElement(id); };
globalThis.hidden = function (id) { return el(id).classList.contains("d-none"); };
globalThis.fireEvent = async function (id, type) {
  for (const handler of ((EVENTS[id] || {})[type] || [])) await handler();
  await settle();
};

const DOM_READY = [];
globalThis.document = {
  getElementById: (id) => el(id),
  querySelectorAll: () => [],
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
    json: async () => body,
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
globalThis.EMPTY = {items: [], total: 0, card_types: [], cards: [], rows: [], items_total: 0};

// The default network: the run itself, and empty everything else.
globalThis.defaultRoutes = async function (url) {
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
    runDisabled: el("btn-run").disabled,
    runHidden: hidden("btn-run"),
    resultsHidden: hidden("preview-results"),
    spinnerHidden: hidden("preview-spinner"),
    previewDisabled: el("btn-preview").disabled,
    table: el("recipients-table").innerHTML,
    rejections: REJECTIONS,
    fetched: FETCHES.map((f) => f.url),
  };
};
"""


async def _page(http_client: AsyncClient, query: str = "") -> str:
    return (await http_client.get("/ops/campaigns/new-clients" + query)).text


async def _browser(http_client: AsyncClient, query: str = "") -> str:
    """The harness plus the page's own script, exactly as served."""
    return BROWSER + "\n" + _page_script(await _page(http_client, query))


# The form as the server renders it, so a boot starts from the real defaults.
ALTEGIO_FORM = {"f-provider": "altegio", "f-company": "758285", "f-ew-company": str(KARLSRUHE)}


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
  companyLocked: el("f-company").disabled,
});
""" % json.dumps(ALTEGIO_FORM)
    answer = _run_node(source, driver)

    assert answer["before"]["context"] is None, "the answer had not arrived yet"
    # The late answer paints nothing and fills nothing in behind the operator.
    assert answer["after"]["context"] is None
    assert answer["after"]["previewRunId"] is None
    assert answer["period"] == ""
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
