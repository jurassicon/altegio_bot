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
