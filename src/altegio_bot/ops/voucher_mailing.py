"""The operator's whole voucher mailing, in a browser (§43).

Four screens and a narrow API. What an operator does here is the entire working
process: curate the audience in the existing preview editor, check the period, the
list, the count and the money, freeze that exact snapshot, then create, pay and
deliver as three separately confirmed stages, watching progress and recovering
when something goes wrong.

What an operator never does here
-------------------------------
Run a command. Copy a digest or a timestamp. Type a ``batch_id``, a UUID or a
URL. Edit ``.env``. Restart a container. Choose a staffer. Every one of those was
part of §42's terminal process and none of them is part of this one.

Where the authorisation is
--------------------------
Not on these pages. A page offers; the server decides. Every POST below resolves
the operator from the session cookie, requires this session's CSRF token in a
header, requires a same-origin request, and then hands the work to
:mod:`...dispatch`, which stores an immutable plan and lets the executor rebuild
and re-verify it before anything leaves the process. A GET renders and reads; no
GET here creates, pays, sends or refunds.

Why the stages are four buttons and not one
-------------------------------------------
There is deliberately no control that walks create → pay → deliver. The stop
between stages IS the control, and the larger the list the more that matters: a
human looking at what the last stage actually did, and deciding to go on, is the
thing that catches the bad day. Finishing a stage therefore does not authorise the
next one; it offers a plan for it.

What is never on these pages
----------------------------
A voucher code — the message preview shows an explicit placeholder. A phone
number, a customer name, a customer UUID, an order UUID, a raw Meta id, a staffer
UUID, a key, or the value of any secret. Readiness is reported as reasons, never
as configuration.
"""

from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

from altegio_bot.campaigns.easyweek_voucher_delivery import template_contract
from altegio_bot.campaigns.easyweek_voucher_production import dispatch as dispatch_module
from altegio_bot.campaigns.easyweek_voucher_production import ledger as ledger_module
from altegio_bot.campaigns.easyweek_voucher_production import operations as operations_module
from altegio_bot.campaigns.easyweek_voucher_production import runner as production_runner
from altegio_bot.campaigns.easyweek_voucher_production.identity import (
    API_UNAVAILABLE,
    OPERATION_UNKNOWN,
    OPS_CSRF_INVALID,
    OPS_ORIGIN_REJECTED,
    OPS_SESSION_REQUIRED,
    PRODUCTION_STAGES,
    STAGE_CREATE,
    UNIT_PRICE_MINOR,
)
from altegio_bot.campaigns.easyweek_voucher_production.issuer import (
    APPROVED_ISSUER_DISPLAY_NAME,
    APPROVED_ISSUER_DISPLAY_NAME_GENITIVE,
)
from altegio_bot.db import SessionLocal
from altegio_bot.easyweek_locations import configured_easyweek_locations
from altegio_bot.models.models import (
    PROVIDER_EASYWEEK,
    RECIPIENT_BASIS_MANUAL,
    CampaignRecipient,
    CampaignRun,
)
from altegio_bot.ops.auth import (
    CSRF_HEADER,
    OpsSessionError,
    csrf_token_for,
    require_csrf,
    require_ops_auth,
    require_same_origin,
    resolve_ops_session,
)
from altegio_bot.ops.router import _esc, _page
from altegio_bot.settings import settings

router = APIRouter(prefix="/ops/voucher-mailings", dependencies=[Depends(require_ops_auth)], tags=["voucher-mailing"])

# What the preview shows where a real code would go. Deliberately not a plausible
# code: a reader must not be able to mistake it for one, and it must be obvious in
# a screenshot that no code was revealed.
VOUCHER_CODE_PLACEHOLDER = "XXXX-XXXX-XXXX"

# What a customer sees where their own name goes.
CLIENT_NAME_PLACEHOLDER = "<Name der Kundin>"

# ---------------------------------------------------------------------------
# Request bodies
# ---------------------------------------------------------------------------
# Deliberately narrow. There is no field here for a staffer, a slot set, a digest,
# a timestamp, a principal or a batch the plan is not about. `staffer_uuid` exists
# ONLY so that a request which tries to name one is refused by name rather than
# silently ignored — silence would leave the next reader unsure.


class PlanRequest(BaseModel):
    stage: str
    preview_run_id: int
    batch_id: int | None = None
    slot: int | None = None
    expected_recipient_count: int | None = None
    approved_exposure_minor: int | None = None
    staffer_uuid: str | None = None


class ConfirmRequest(BaseModel):
    approval_id: int
    # What the operator agreed to for THIS stage. Compared against the stored
    # approval and then discarded: they prove a human said this count and this
    # amount, and they cannot widen anything.
    confirmed_count: int = Field(ge=0)
    confirmed_amount_minor: int = Field(ge=0)


class CompositionRequest(BaseModel):
    preview_run_id: int


class StopRequest(BaseModel):
    batch_id: int


class ReconcileRequest(BaseModel):
    preview_run_id: int
    batch_id: int


class RefundPlanRequest(BaseModel):
    preview_run_id: int
    batch_id: int
    slot: int


# ---------------------------------------------------------------------------
# The stricter door in front of every write
# ---------------------------------------------------------------------------


def _session_error(exc: OpsSessionError) -> JSONResponse:
    """A refusal in this phase's own vocabulary, with nothing leaked."""
    mapping = {
        "ops_csrf_missing": OPS_CSRF_INVALID,
        "ops_csrf_invalid": OPS_CSRF_INVALID,
        "ops_origin_rejected": OPS_ORIGIN_REJECTED,
        "ops_origin_missing": OPS_ORIGIN_REJECTED,
        "ops_origin_unknown": OPS_ORIGIN_REJECTED,
    }
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "accepted": False,
            "reasons": [mapping.get(exc.reason, OPS_SESSION_REQUIRED)],
            # Enough for an operator to know what to do, with no detail about the
            # configuration behind it.
            "detail": exc.reason,
        },
    )


def _principal_or_error(request: Request) -> tuple[operations_module.OpsPrincipal | None, JSONResponse | None]:
    """The authenticated operator, or the response that refuses this request.

    All three checks, in the order that leaks least: the session first, so an
    unauthenticated caller learns nothing about origins or tokens.
    """
    try:
        require_same_origin(request)
        account, fingerprint = resolve_ops_session(request)
        require_csrf(request)
    except OpsSessionError as exc:
        return None, _session_error(exc)
    return (
        operations_module.OpsPrincipal(account=account, session_fingerprint=fingerprint),
        None,
    )


def _executor_available() -> bool:
    """Is the dedicated executor expected to be running in this deployment?

    Answered from configuration rather than by probing a process: the executor is
    a container in the deployment stack, and a deployment that did not enable it
    cannot drain a queue. A false answer here refuses a confirmation with a reason
    an operator can read, instead of parking it forever.

    It is deliberately a weak claim, and the UI says so: the setting means "this
    deployment runs one", not "it is alive this second". What catches an executor
    that died is the operation sitting in ``queued`` on the mailing page.
    """
    return bool(settings.easyweek_voucher_production_executor_enabled)


# ---------------------------------------------------------------------------
# Readiness, as reasons
# ---------------------------------------------------------------------------


async def _readiness() -> dict[str, Any]:
    """Why a mailing could or could not act, without naming a single secret."""
    from altegio_bot.campaigns.easyweek_voucher_production.readiness import prove_prerequisites

    try:
        async with SessionLocal() as session:
            prerequisites = await prove_prerequisites(
                session,
                stage=STAGE_CREATE,
                company_id=production_runner.KARLSRUHE_COMPANY_ID,
                sender_code=dispatch_module.SENDER_CODE,
                # A page render must not make a live EasyWeek call, so the issuer's
                # current MEMBERSHIP of the branch is not asked here — the pin is,
                # because that is a local comparison. The membership is proven when
                # a step is prepared, and the panel says so rather than reporting a
                # blocker nobody checked.
                require_membership=False,
            )
        facts = prerequisites.as_safe_dict()
    except SQLAlchemyError:
        facts = {"reasons": ["voucher_production_database_unavailable"]}
    facts["executor_enabled"] = _executor_available()
    facts["issuer_display_name"] = APPROVED_ISSUER_DISPLAY_NAME
    facts["issuer_membership_checked_here"] = False
    return facts


def _message_preview() -> dict[str, str]:
    """The approved message and the voucher's terms, with a PLACEHOLDER code.

    Rendered from the repository's own named body — the contract the Meta template
    is compared against — rather than from anything a provider returned. The code
    is a placeholder by construction: there is no path from this page to a real
    one, because a real one only exists between a paid order read and one POST.
    """
    registry = configured_easyweek_locations()
    location = registry.locations.get(production_runner.KARLSRUHE_COMPANY_ID) if registry.ready else None
    booking_link = (location.booking_page_url or "").strip() if location is not None else ""
    body = template_contract.VOUCHER_TEMPLATE_BODY.format(
        client_name=CLIENT_NAME_PLACEHOLDER,
        voucher_code=VOUCHER_CODE_PLACEHOLDER,
        booking_link=booking_link or "<Buchungslink nicht konfiguriert>",
    )
    return {
        "meta_template_name": template_contract.VOUCHER_META_TEMPLATE_NAME,
        "language": template_contract.VOUCHER_TEMPLATE_LANGUAGE,
        "body": body,
        "voucher_code_placeholder": VOUCHER_CODE_PLACEHOLDER,
    }


# ---------------------------------------------------------------------------
# API — every one of these is a POST, and every one is authorised server side
# ---------------------------------------------------------------------------


@router.post("/api/plan")
async def api_plan(request: Request, payload: PlanRequest) -> JSONResponse:
    """Offer a plan for one stage. Reads, plus one approval row. Sends nothing."""
    principal, refusal = _principal_or_error(request)
    if refusal is not None or principal is None:
        return refusal or _session_error(OpsSessionError("ops_session_invalid"))
    if payload.stage not in PRODUCTION_STAGES:
        return JSONResponse(status_code=400, content={"ready": False, "reasons": ["voucher_production_unknown_stage"]})

    offer = await dispatch_module.offer_stage(
        SessionLocal,
        stage=payload.stage,
        principal=principal,
        preview_run_id=payload.preview_run_id,
        batch_id=payload.batch_id,
        slot=payload.slot,
        expected_recipient_count=payload.expected_recipient_count,
        approved_exposure_minor=payload.approved_exposure_minor,
        staffer_uuid_from_client=payload.staffer_uuid,
    )
    return JSONResponse(status_code=200 if offer.ready else 409, content=offer.as_safe_dict())


@router.post("/api/confirm")
async def api_confirm(request: Request, payload: ConfirmRequest) -> JSONResponse:
    """Spend one approval and queue one durable operation. Idempotent.

    Returns 200 both when this call created the operation and when it found the
    one an earlier identical click created. A double-click, a retried POST, a
    refresh and a second tab must all end with the operator being told the same
    thing about the same operation — never with two.
    """
    principal, refusal = _principal_or_error(request)
    if refusal is not None or principal is None:
        return refusal or _session_error(OpsSessionError("ops_session_invalid"))

    outcome = await dispatch_module.confirm_stage(
        SessionLocal,
        approval_id=payload.approval_id,
        principal=principal,
        confirmed_count=payload.confirmed_count,
        confirmed_amount_minor=payload.confirmed_amount_minor,
        executor_available=_executor_available,
    )
    body: dict[str, Any] = {
        "accepted": outcome.accepted,
        "created": outcome.created,
        "reasons": list(outcome.reasons),
        "operation": outcome.operation.as_safe_dict() if outcome.operation is not None else None,
    }
    return JSONResponse(status_code=200 if outcome.accepted else 409, content=body)


@router.post("/api/stop")
async def api_stop(request: Request, payload: StopRequest) -> JSONResponse:
    """Stop after the current request, for this batch. Durable, repeatable."""
    principal, refusal = _principal_or_error(request)
    if refusal is not None or principal is None:
        return refusal or _session_error(OpsSessionError("ops_session_invalid"))
    state = await dispatch_module.request_stop(SessionLocal, batch_id=payload.batch_id, principal=principal)
    return JSONResponse(status_code=200, content={"accepted": state.active, **state.as_safe_dict()})


@router.post("/api/reconcile")
async def api_reconcile(request: Request, payload: ReconcileRequest) -> JSONResponse:
    """Read the outside world back. Buys nothing, sends nothing, needs no plan."""
    principal, refusal = _principal_or_error(request)
    if refusal is not None or principal is None:
        return refusal or _session_error(OpsSessionError("ops_session_invalid"))
    outcome = await dispatch_module.reconcile_batch(
        SessionLocal,
        preview_run_id=payload.preview_run_id,
        batch_id=payload.batch_id,
        principal=principal,
    )
    if outcome.report is None:
        # Named, so an operator can tell "a stage is running, look again shortly"
        # from "EasyWeek is unreachable" (review R2).
        return JSONResponse(
            status_code=409,
            content={"accepted": False, "reasons": list(outcome.reasons) or [API_UNAVAILABLE]},
        )
    return JSONResponse(status_code=200, content={"accepted": True, "report": outcome.report.as_safe_dict()})


@router.get("/api/status")
async def api_status(batch_id: int | None = None, preview_run_id: int | None = None) -> JSONResponse:
    """Progress, for polling. A read: it starts nothing and confirms nothing.

    Available with the fence closed and after a stop, deliberately: the moment an
    operator most needs to know what a halted mailing left behind is exactly when
    everything else is blocked.

    Always scoped (review R5). Before a freeze there is no batch to scope by, and the
    earlier version fell back to listing operations with ``batch_id=None`` — which
    means "every operation of every mailing". A page watching one preview would then
    have shown another preview's work as its own. When there is no batch yet the
    scope is the PREVIEW, and never nothing.
    """
    if batch_id is None and preview_run_id is None:
        return JSONResponse(
            status_code=400,
            content={"reasons": ["voucher_production_unscoped_status"]},
        )
    try:
        report = await production_runner.run_status(SessionLocal, batch_id=batch_id, preview_run_id=preview_run_id)
        state = report.as_safe_dict()
    except SQLAlchemyError:
        return JSONResponse(status_code=200, content={"batch": {}, "batches": [], "operations": []})
    resolved = batch_id if batch_id is not None else (state.get("batch") or {}).get("batch_id")
    # One of the two is always set, so the listing is never global.
    scope: dict[str, Any] = (
        {"batch_id": int(resolved)} if resolved is not None else {"campaign_run_id": int(preview_run_id or 0)}
    )
    operations = await operations_module.list_operations(SessionLocal, limit=20, **scope)
    active = await operations_module.active_operation(SessionLocal, **scope)
    stop = (
        (await ledger_module.stop_state(SessionLocal, batch_id=int(resolved))).as_safe_dict()
        if resolved is not None
        else {"stop_active": False}
    )
    state["operations"] = [entry.as_safe_dict() for entry in operations]
    state["active_operation"] = active.as_safe_dict() if active is not None else None
    state.update(stop)
    state["readiness"] = await _readiness()
    # Which per-item stages each slot is in a state to be planned for, derived from
    # the ledger's own contract rather than re-decided in JavaScript (review R6).
    if resolved is not None:
        snapshot = await ledger_module.load(SessionLocal, batch_id=int(resolved))
        actions = {entry.slot: list(production_runner.available_item_actions(entry)) for entry in snapshot.items}
        for item in state.get("batch", {}).get("items", []):
            item["available_actions"] = actions.get(item.get("slot"), [])
    return JSONResponse(status_code=200, content=state)


@router.get("/api/operation")
async def api_operation(operation_id: int, preview_run_id: int) -> JSONResponse:
    """One operation, for watching a stage that has no batch yet (review R5).

    A FREEZE is confirmed before its batch exists, so the confirmation answers with
    ``batch_id: null`` and the page has nothing to poll by. This is what it polls
    instead: the operation it was given, until the freeze produces a batch.

    Scoped by preview and checked against it, so an id guessed or carried over from
    another mailing answers 404 rather than another preview's progress.
    """
    operation = await operations_module.load_operation(SessionLocal, operation_id=operation_id)
    if operation is None or operation.campaign_run_id != preview_run_id:
        return JSONResponse(status_code=404, content={"reasons": [OPERATION_UNKNOWN]})
    return JSONResponse(status_code=200, content={"operation": operation.as_safe_dict()})


@router.post("/api/composition")
async def api_composition(request: Request, payload: CompositionRequest) -> JSONResponse:
    """The audience of one preview. A READ — it authorises nothing (review R3).

    Separate from the freeze plan on purpose. "Check the list" used to ask for a
    freeze plan, which cannot be ready without the count and the exposure — the very
    numbers the operator was about to read off the list — so the screen dead-ended
    before showing them. This proves the composition and reports it; the operator
    then states the numbers, and the freeze plan still demands them exactly.

    A POST rather than a GET because it reaches EasyWeek to re-prove every member,
    and because it goes through the same authenticated, CSRF-protected door as every
    other action here. It stores no approval, writes no batch and sends nothing.
    """
    principal, refusal = _principal_or_error(request)
    if refusal is not None or principal is None:
        return refusal or _session_error(OpsSessionError("ops_session_invalid"))
    view = await dispatch_module.inspect_composition(SessionLocal, preview_run_id=payload.preview_run_id)
    return JSONResponse(status_code=200, content=view.as_ui_dict())


@router.get("/api/recipients")
async def api_recipients(batch_id: int) -> JSONResponse:
    """Slot → recipient for one mailing, for the authorised operator (review R7).

    Its own endpoint, deliberately. A slot number is the right thing for a ledger and
    useless to somebody deciding whether to return a particular person's €15 — but a
    name is exactly what must never enter an operation payload, an audit row or a
    diagnostic report. So the name is served here, to a logged-in operator, and
    nowhere else.
    """
    lines = await dispatch_module.recipient_lines(SessionLocal, batch_id=batch_id)
    return JSONResponse(
        status_code=200,
        content={"batch_id": batch_id, "recipients": [line.as_ui_dict() for line in lines]},
    )


# ---------------------------------------------------------------------------
# Pages
# ---------------------------------------------------------------------------


def _csrf_for(request: Request) -> str:
    token = request.cookies.get("ops_session", "")
    return csrf_token_for(token) if token else ""


def _money(minor: Any) -> str:
    return f"{int(minor or 0) / 100:.2f} €"


def _reason_rows(reasons: list[str]) -> str:
    if not reasons:
        return "<li class='text-success'>Все проверки пройдены.</li>"
    return "".join(f"<li><code>{_esc(reason)}</code></li>" for reason in reasons)


def _issuer_banner() -> str:
    """The one sentence §43.9 asks the interface to say."""
    return (
        "<div class='alert alert-info py-2 mb-3'>"
        f"Ваучеры оформляются от <b>{_esc(APPROVED_ISSUER_DISPLAY_NAME_GENITIVE)}</b>. "
        "Мастер задан на сервере и не выбирается для каждой рассылки."
        "</div>"
    )


def _readiness_block(facts: dict[str, Any]) -> str:
    reasons = [str(value) for value in (facts.get("reasons") or [])]
    css = "alert-success" if not reasons else "alert-warning"
    executor = "включён" if facts.get("executor_enabled") else "не включён"
    return f"""
<div class="alert {css}">
  <b>Готовность:</b>
  <ul class="mb-1 mt-1">{_reason_rows(reasons)}</ul>
  <div class="small text-muted">
    Исполнитель стадий: {_esc(executor)}. Значения секретов и UUID здесь не показываются.
    Принадлежность мастера филиалу проверяется живым чтением при подготовке шага.
  </div>
</div>
"""


@router.get("", response_class=HTMLResponse)
async def page_index(request: Request) -> str:
    """Every mailing, plus the previews that could become one.

    This is the entry point §43.3 asks for: an operator arrives from the campaign
    list or the preview and finds a named mailing, not a technical id they have to
    carry somewhere else.
    """
    try:
        report = await production_runner.run_status(SessionLocal, batch_id=None)
        state = report.as_safe_dict()
    except SQLAlchemyError:
        state = {"batches": []}
    batches = state.get("batches") or []

    candidates: list[dict[str, Any]] = []
    try:
        async with SessionLocal() as session:
            runs = list(
                (
                    await session.execute(
                        select(CampaignRun)
                        .where(
                            CampaignRun.provider == PROVIDER_EASYWEEK,
                            CampaignRun.mode == "preview",
                            CampaignRun.status == "completed",
                        )
                        .order_by(CampaignRun.id.desc())
                        .limit(25)
                    )
                )
                .scalars()
                .all()
            )
            for run in runs:
                locked = await ledger_module.preview_is_locked_by_voucher_production(
                    session, campaign_run_id=int(run.id)
                )
                if locked:
                    continue
                active = list(
                    (
                        await session.execute(
                            select(CampaignRecipient.id, CampaignRecipient.recipient_basis).where(
                                CampaignRecipient.campaign_run_id == run.id,
                                CampaignRecipient.provider == PROVIDER_EASYWEEK,
                                CampaignRecipient.status == "candidate",
                            )
                        )
                    ).all()
                )
                if not active:
                    continue
                manual_only = all(row.recipient_basis == RECIPIENT_BASIS_MANUAL for row in active)
                candidates.append(
                    {
                        "run_id": int(run.id),
                        "count": len(active),
                        "manual_only": manual_only,
                        "period": (
                            f"{run.period_start.date().isoformat()}..{run.period_end.date().isoformat()}"
                            if run.period_start and run.period_end
                            else "—"
                        ),
                    }
                )
    except SQLAlchemyError:
        candidates = []

    batch_rows = "".join(
        "<tr>"
        f"<td><a href='/ops/voucher-mailings/{int(entry.get('batch_id') or 0)}'>"
        f"Рассылка #{_esc(str(entry.get('batch_id')))}</a></td>"
        f"<td>{_esc(str(entry.get('campaign_period') or '—'))}</td>"
        f"<td>{_esc(str(entry.get('recipient_count') or 0))}</td>"
        f"<td>{_esc(_money(entry.get('total_exposure_minor')))}</td>"
        f"<td><code>{_esc(str(entry.get('status') or '—'))}</code></td>"
        f"<td>{_esc('да' if entry.get('reconciliation_required') else 'нет')}</td>"
        f"<td><a class='btn btn-sm btn-outline-primary' "
        f"href='/ops/voucher-mailings/{int(entry.get('batch_id') or 0)}'>Открыть</a></td>"
        "</tr>"
        for entry in batches
    )
    batch_table = (
        "<table class='table table-sm align-middle'>"
        "<thead><tr><th>Рассылка</th><th>Период кампании</th><th>Получателей</th><th>Сумма</th>"
        "<th>Состояние</th><th>Нужна сверка</th><th></th></tr></thead>"
        f"<tbody>{batch_rows}</tbody></table>"
        if batch_rows
        else "<p class='text-muted'>Рассылок пока нет.</p>"
    )

    candidate_rows = "".join(
        "<tr>"
        f"<td><a href='/ops/campaigns/{entry['run_id']}'>Preview #{entry['run_id']}</a></td>"
        f"<td>{_esc(entry['period'])}</td>"
        f"<td>{entry['count']}</td>"
        + (
            "<td><a class='btn btn-sm btn-primary' "
            f"href='/ops/voucher-mailings/prepare?preview_run_id={entry['run_id']}'>"
            "Подготовить рассылку</a></td>"
            if entry["manual_only"]
            else "<td><span class='text-muted small'>в списке есть не-ручные получатели</span></td>"
        )
        + "</tr>"
        for entry in candidates
    )
    candidate_table = (
        "<table class='table table-sm align-middle'>"
        "<thead><tr><th>Preview</th><th>Период</th><th>Активных получателей</th><th></th></tr></thead>"
        f"<tbody>{candidate_rows}</tbody></table>"
        if candidate_rows
        else "<p class='text-muted'>Нет готовых preview с ручным составом.</p>"
    )

    body = f"""
<h1 class="h4 mb-3">Ваучерные рассылки</h1>
{_issuer_banner()}
{_readiness_block(await _readiness())}
<h2 class="h5 mt-4">Рассылки</h2>
{batch_table}
<h2 class="h5 mt-4">Можно подготовить</h2>
<p class="text-muted small">
  Состав готовится в редакторе preview. Здесь рассылка только фиксируется и выполняется —
  по одному подтверждённому шагу.
</p>
{candidate_table}
<div class="alert alert-secondary small mt-4">
  Один ваучер {_esc(_money(UNIT_PRICE_MINOR))} на получателя. Потолка получателей нет:
  количество и сумму оператор подтверждает явно перед фиксацией списка.
  Массовая отправка не разрешена: <code>campaign_send_authorized=false</code>.
</div>
"""
    del request
    return _page("Ваучерные рассылки", body)


@router.get("/prepare", response_class=HTMLResponse)
async def page_prepare(request: Request, preview_run_id: int) -> str:
    """Check the list, the period, the count and the money — then freeze."""
    csrf = _csrf_for(request)
    message = _message_preview()
    body = f"""
<h1 class="h4 mb-3">Подготовка рассылки — preview #{preview_run_id}</h1>
{_issuer_banner()}
{_readiness_block(await _readiness())}
<div class="alert alert-secondary">
  Состав редактируется в <a href="/ops/campaigns/{preview_run_id}">редакторе preview</a>:
  там добавляют и исключают получателей. После фиксации состав меняться не может.
</div>
<div id="composition-panel" class="card mb-3">
  <div class="card-header">Состав и сумма</div>
  <div class="card-body">
    <button id="btn-load" class="btn btn-outline-primary btn-sm" onclick="loadComposition()">
      Проверить состав
    </button>
    <div id="composition-summary" class="mt-3"></div>
    <div id="composition-slots" class="mt-3"></div>
  </div>
</div>
<div id="freeze-panel" class="card mb-3 d-none">
  <div class="card-header">Подтверждение количества и суммы</div>
  <div class="card-body">
    <p class="mb-2">
      Введите количество получателей и общую сумму так, как они показаны выше.
      Сервер сравнит их с фактическим составом и откажет при расхождении.
    </p>
    <div class="row g-2 align-items-end">
      <div class="col-auto">
        <label class="form-label small mb-1" for="f-count">Получателей</label>
        <input id="f-count" type="number" min="1" class="form-control form-control-sm">
      </div>
      <div class="col-auto">
        <div class="small text-muted" id="approval-hint"></div>
      </div>
      <div class="col-auto">
        <label class="form-label small mb-1" for="f-euro">Общая сумма, €</label>
        <input id="f-euro" type="text" class="form-control form-control-sm" placeholder="например 45.00">
      </div>
      <div class="col-auto">
        <button id="btn-plan-freeze" class="btn btn-primary btn-sm" onclick="planFreeze()">
          Проверить и зафиксировать
        </button>
      </div>
    </div>
  </div>
</div>
<div id="confirm-panel" class="card mb-3 d-none border-warning">
  <div class="card-header bg-warning-subtle">Подтвердите действие</div>
  <div class="card-body">
    <div id="confirm-summary"></div>
    <button id="btn-confirm" class="btn btn-danger btn-sm mt-2" onclick="confirmStage()">Подтвердить</button>
    <button class="btn btn-outline-secondary btn-sm mt-2" onclick="cancelConfirm()">Отмена</button>
  </div>
</div>
<div class="card mb-3">
  <div class="card-header">Сообщение и условия ваучера</div>
  <div class="card-body">
    <p class="small text-muted mb-1">
      Шаблон <code>{_esc(message["meta_template_name"])}</code>, язык
      <code>{_esc(message["language"])}</code>. Вместо реального кода — placeholder
      <code>{_esc(message["voucher_code_placeholder"])}</code>: настоящий код не показывается нигде.
    </p>
    <pre class="border rounded p-2 bg-white">{_esc(message["body"])}</pre>
  </div>
</div>
<div id="operation-panel"></div>
<div id="alert-area"></div>
<script>
const CSRF = {json.dumps(csrf)};
const PREVIEW_RUN_ID = {int(preview_run_id)};
const BATCH_ID = null;
const UNIT_PRICE_MINOR = {int(UNIT_PRICE_MINOR)};
let OFFER = null;
let COMPOSITION = null;
let RECIPIENTS = {{}};
let TRACKED_OPERATION = null;

{_PAGE_SCRIPT}

/* The composition panel says what it knows before anything is pressed: "not checked
   yet", which is not the same statement as an empty mailing (review F1). */
renderComposition();

/* Whatever this preview already has, read from the SERVER (review F2): a queued or
   finished operation, or the mailing a freeze already produced. A fresh login, a new
   tab and a browser with nothing stored all land on the same state, because none of
   them is the thing being asked. */
resumeFromServer();

function loadComposition() {{
  /* A READ of the real audience (review R3). It used to ask for a freeze plan,
     which can never be ready without the count and the exposure — the numbers the
     operator is about to read off this very list — so the screen dead-ended before
     revealing the fields. */
  inspectComposition();
}}

function planFreeze() {{
  if (COMPOSITION === null || COMPOSITION_STALE) {{
    /* Review F1: the numbers are only meaningful against a list that was actually
       proven. Nothing on screen may stand in for that check. */
    setAlert("warning", "Сначала проверьте состав — показанные данные не подтверждены.");
    return;
  }}
  const count = parseInt(document.getElementById("f-count").value, 10);
  const euro = document.getElementById("f-euro").value;
  const minor = euroToMinor(euro);
  if (!Number.isInteger(count) || count < 1 || minor === null) {{
    setAlert("danger", "Введите количество получателей и общую сумму.");
    return;
  }}
  planStage("freeze", {{count: count, minor: minor}});
}}
</script>
"""
    return _page(f"Подготовка рассылки #{preview_run_id}", body)


@router.get("/{batch_id}", response_class=HTMLResponse)
async def page_mailing(request: Request, batch_id: int) -> str:
    """One mailing: stages, progress, per-recipient results, recovery."""
    csrf = _csrf_for(request)
    try:
        report = await production_runner.run_status(SessionLocal, batch_id=batch_id)
        state = report.as_safe_dict()
    except SQLAlchemyError:
        state = {"batch": {}}
    batch = state.get("batch") or {}
    if not batch.get("exists"):
        return _page(
            "Рассылка не найдена",
            f"<div class='alert alert-danger'>Рассылка #{_esc(str(batch_id))} не найдена.</div>"
            "<a href='/ops/voucher-mailings'>К списку рассылок</a>",
        )

    preview_run_id = int(batch.get("campaign_run_id") or 0)
    message = _message_preview()
    header_rows = [
        ("Период кампании", batch.get("campaign_period") or "—"),
        ("Получателей (зафиксировано)", str(batch.get("recipient_count") or 0)),
        ("Общая стоимость рассылки", _money(batch.get("total_exposure_minor"))),
        (
            "Подтверждено оператором",
            f"{batch.get('approved_recipient_count') or '—'} / {_money(batch.get('approved_exposure_minor'))}",
        ),
        ("Состояние", batch.get("status") or "—"),
        ("Нужна сверка", "да" if batch.get("reconciliation_required") else "нет"),
        ("Нужно закрыть draft в EasyWeek", "да" if state.get("manual_cleanup_required") else "нет"),
    ]
    header_table = "".join(
        f"<tr><th class='text-nowrap'>{_esc(name)}</th><td>{_esc(str(value))}</td></tr>" for name, value in header_rows
    )

    body = f"""
<h1 class="h4 mb-1">Рассылка #{batch_id}</h1>
<p class="text-muted small mb-3">
  Preview <a href="/ops/campaigns/{preview_run_id}">#{preview_run_id}</a>.
  Период кампании — это волна, за которую положен ваучер, а не месяц отправки.
</p>
{_issuer_banner()}
{_readiness_block(await _readiness())}
<table class="table table-sm w-auto">{header_table}</table>

<div id="stop-banner"></div>

<h2 class="h5 mt-4">Шаги</h2>
<p class="text-muted small">
  Каждый шаг подтверждается отдельно. Завершение одного шага не разрешает следующий,
  и кнопки, которая прошла бы их подряд, здесь нет.
</p>
<div class="d-flex gap-2 flex-wrap mb-3">
  <button id="btn-stage-create" class="btn btn-outline-primary btn-sm" onclick="planStage('create', {{}})">
    Создать ваучеры…
  </button>
  <button id="btn-stage-pay" class="btn btn-outline-danger btn-sm" onclick="planStage('pay', {{}})">
    Оплатить ваучеры…
  </button>
  <button id="btn-stage-deliver" class="btn btn-outline-success btn-sm" onclick="planStage('deliver', {{}})">
    Отправить сообщения…
  </button>
  <button id="btn-stop" class="btn btn-warning btn-sm" onclick="stopBatch()">
    Остановить после текущего запроса
  </button>
  <button id="btn-reconcile" class="btn btn-outline-secondary btn-sm" onclick="reconcile()">
    Сверить с EasyWeek
  </button>
</div>

<div id="confirm-panel" class="card mb-3 d-none border-warning">
  <div class="card-header bg-warning-subtle">Подтвердите действие</div>
  <div class="card-body">
    <div id="confirm-summary"></div>
    <button id="btn-confirm" class="btn btn-danger btn-sm mt-2" onclick="confirmStage()">Подтвердить</button>
    <button class="btn btn-outline-secondary btn-sm mt-2" onclick="cancelConfirm()">Отмена</button>
  </div>
</div>

<h2 class="h5 mt-4">Прогресс</h2>
<div id="operation-panel"></div>
<div id="delivery-panel"></div>
<h2 class="h5 mt-4">Получатели</h2>
<div id="slots-panel"></div>

<div class="card mt-4">
  <div class="card-header">Сообщение и условия ваучера</div>
  <div class="card-body">
    <p class="small text-muted mb-1">
      Шаблон <code>{_esc(message["meta_template_name"])}</code>, язык
      <code>{_esc(message["language"])}</code>. Вместо кода — placeholder
      <code>{_esc(message["voucher_code_placeholder"])}</code>.
    </p>
    <pre class="border rounded p-2 bg-white">{_esc(message["body"])}</pre>
  </div>
</div>

<div id="alert-area"></div>
<script>
const CSRF = {json.dumps(csrf)};
const PREVIEW_RUN_ID = {preview_run_id};
const BATCH_ID = {batch_id};
const UNIT_PRICE_MINOR = {int(UNIT_PRICE_MINOR)};
let OFFER = null;
let COMPOSITION = null;
let RECIPIENTS = {{}};
let TRACKED_OPERATION = null;

{_PAGE_SCRIPT}

/* Names first, so the very first render of the slots table can already say whose
   money each row is about (review R7). */
loadRecipients().then(refreshStatus);
setInterval(refreshStatus, 5000);
</script>
"""
    return _page(f"Рассылка #{batch_id}", body)


# ---------------------------------------------------------------------------
# The page script
# ---------------------------------------------------------------------------
# Shared verbatim by both acting pages, so the decisions an operator depends on
# exist once. The small pure functions below are what the browser-level tests
# execute directly out of the served page, rather than re-implementing.

_PAGE_SCRIPT = r"""
function setAlert(kind, text) {
  const area = document.getElementById("alert-area");
  if (!area) return;
  area.innerHTML = '<div class="alert alert-' + kind + ' mt-3">' + escapeHtml(text) + "</div>";
}

function escapeHtml(value) {
  return String(value === null || value === undefined ? "" : value)
    .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}

function moneyLabel(minor) {
  const value = Number(minor || 0) / 100;
  return value.toFixed(2) + " €";
}

function euroToMinor(text) {
  const cleaned = String(text === null || text === undefined ? "" : text).trim().replace(",", ".");
  if (!/^\d+(\.\d{1,2})?$/.test(cleaned)) return null;
  return Math.round(parseFloat(cleaned) * 100);
}

function stageLabel(stage) {
  if (stage === "freeze") return "Зафиксировать список";
  if (stage === "create") return "Создать ваучеры";
  if (stage === "pay") return "Оплатить ваучеры";
  if (stage === "deliver") return "Отправить сообщения";
  if (stage === "refund") return "Вернуть оплату";
  return stage;
}

/* What this click will actually do, stage by stage. Never a batch total unless
   the stage IS the whole batch: an operator confirming a payment has to read how
   many vouchers this press pays for and how much it moves, which after a partial
   run is the remainder. */
function confirmSummary(offer) {
  if (!offer || !offer.targets) return "";
  const t = offer.targets;
  const stage = offer.stage;
  const lines = [];
  if (stage === "freeze") {
    lines.push("Зафиксировать " + t.stage_target_count +
               " получателей на " + moneyLabel(t.stage_amount_minor) + ".");
    lines.push("Деньги не списываются и сообщения не отправляются.");
  } else if (stage === "create") {
    lines.push("Будет создано ваучеров: " + t.stage_target_count + ".");
    lines.push("На сумму " + moneyLabel(t.stage_amount_minor) + " (оплаты на этом шаге нет).");
  } else if (stage === "pay") {
    lines.push("Будет оплачено ваучеров: " + t.stage_target_count + ".");
    lines.push("Списание " + moneyLabel(t.stage_amount_minor) + " — необратимо после отправки.");
  } else if (stage === "deliver") {
    lines.push("Будет отправлено сообщений: " + t.stage_target_count + ".");
    lines.push("Одна попытка на получателя, повторной не будет.");
  } else if (stage === "refund") {
    const slot = (t.target_slots && t.target_slots.length === 1) ? t.target_slots[0] : null;
    lines.push("Возврат " + moneyLabel(t.stage_amount_minor) + " — " +
      (slot === null ? "по одному получателю." : refundSubject(slot) + "."));
  }
  if (stage !== "freeze") {
    lines.push("Вся рассылка: " + t.batch_recipient_count +
               " получателей, " + moneyLabel(t.batch_exposure_minor) + ".");
  }
  return lines.join("\n");
}

/* Which stage an operator may take next, from the durable state alone. Returns
   one stage, or null. Deliberately conservative: anything in flight, any
   outstanding reconciliation and any halt means the answer is not a stage. */
function nextAction(state) {
  if (!state || !state.batch || !state.batch.exists) return null;
  if (state.active_operation) return null;
  if (state.batch.reconciliation_required) return null;
  if (state.batch.halted) return null;
  const counts = slotCounts(state);
  if (counts.planned > 0) return "create";
  if (counts.created > 0) return "pay";
  if (counts.paid > 0) return "deliver";
  return null;
}

function slotCounts(state) {
  const items = (state && state.batch && state.batch.items) || [];
  const counts = {planned: 0, created: 0, paid: 0, sent: 0, unresolved: 0};
  for (const item of items) {
    if (item.status === "planned" || item.status === "create_rejected") counts.planned += 1;
    else if (item.status === "created" || item.status === "pay_rejected") counts.created += 1;
    else if (item.status === "paid") counts.paid += 1;
    else if (item.status === "provider_accepted" || item.status === "delivered" || item.status === "read") {
      counts.sent += 1;
    }
    if (item.reconciliation_required) counts.unresolved += 1;
  }
  return counts;
}

/* A stop is not a halt and an unknown is not a stop. The banner has to say which
   one happened, because the next step differs: a stop resumes with a fresh
   confirmation, an unknown needs a readback first. */
function stateBanner(state) {
  if (!state || !state.batch) return null;
  if (state.stop_active) {
    return {kind: "warning", text: "Остановлено оператором."
      + " Новые запросы не выдаются; продолжение — только по новому подтверждению."};
  }
  if (state.batch.reconciliation_required) {
    return {kind: "danger", text: "Есть неподтверждённый исход. Нужна сверка с EasyWeek — повтор запрещён."};
  }
  if (state.batch.halted) {
    return {kind: "danger", text: "Рассылка остановлена после неудачного шага."};
  }
  return null;
}

/* The four delivery facts, never merged. "Meta accepted" is not "delivered",
   and neither of them is "read". */
function deliveryFacts(state) {
  const batch = (state && state.batch) || {};
  const total = Number(batch.recipient_count || 0);
  return [
    ["Исполнение шагов завершено", batch.execution_completed ? "да" : "нет"],
    ["Meta приняла", (batch.provider_accepted_count || 0) + " из " + total],
    ["Подтверждена доставка", (batch.webhook_delivered_count || 0) + " из " + total],
    ["Прочитано", (batch.webhook_read_count || 0) + " из " + total]
  ];
}

/* May a refund be PREPARED for this slot? Read off the server's own list (review
   R6), never re-derived here.

   The old version kept its own status list and it had drifted both ways: it hid
   `pay_unknown` and `refund_rejected`, where a refund is genuinely allowed, and it
   offered `send_rejected`, where a refund is forbidden because the one attempt was
   already spent. A second copy of a rule is how that happens, so there is no longer
   a second copy — `available_actions` comes from `available_item_actions` in the
   runner, which is derived from the same table the claim enforces.

   Still only about OFFERING the step. The plan, the claim and a CHECK constraint
   remain the things that decide. */
function mayRefund(item) {
  if (!item) return false;
  const actions = item.available_actions || [];
  return actions.indexOf("refund") !== -1;
}

/* Every POST this page makes. A transport failure is reported as a RESULT rather
   than thrown, because the callers have to tell three states apart (review F1/F2):
   the server refused, the server answered, and the answer never arrived. The third
   one is the dangerous one — it says nothing about whether the request was carried
   out — and the reviewed code could not express it at all. */
async function postJson(path, payload) {
  let response = null;
  try {
    response = await fetch(path, {
      method: "POST",
      headers: {"Content-Type": "application/json", "X-Ops-CSRF": CSRF},
      credentials: "same-origin",
      body: JSON.stringify(payload)
    });
  } catch (err) {
    return {status: 0, transport: true, data: {}};
  }
  let data = {};
  try { data = await response.json(); } catch (err) { data = {}; }
  return {status: response.status, transport: false, data: data};
}

/* ===========================================================================
   The composition READ and the stage OFFER are two different payloads
   ===========================================================================

   Review F1. ``/api/composition`` answers with a composition: ``campaign_period``,
   ``recipient_count``, ``total_exposure_minor``, ``composition_digest``,
   ``recipients``. ``/api/plan`` answers with an offer for one stage: ``ready``,
   ``reasons``, ``approval``, ``targets`` and a PII-free ``plan``. The two shapes
   have not one field in common.

   The reviewed page handed the OFFER to the composition renderer. Every lookup
   missed, so pressing "Проверить и зафиксировать" repainted the list the operator
   had just read as 0 recipients, 0,00 € and "Состав пуст" — while the confirmation
   dialog beside it still said two recipients and 30 €. One of the two was false and
   the screen offered no way to tell which, immediately before the irreversible part
   of the workflow.

   So the composition panel is painted from ONE source: the stored composition read.
   ``renderComposition`` takes no argument, which is what makes the defect
   unrepeatable rather than merely fixed — there is no parameter left to pass the
   wrong payload into. A plan answer can still make the shown list STALE, and then
   the panel says exactly that instead of inventing an empty one. */

/* Why there is no table, when there is none: "not checked yet" and "checked, and
   the audience is empty or unusable" are different facts, and a zeroed table cannot
   say which. */
let COMPOSITION_NOTE = null;

/* The shown composition may no longer hold — a plan refused, or its answer never
   arrived. Said on the panel, and the freeze waits for a fresh check. */
let COMPOSITION_STALE = false;

function compositionDigest() {
  return (COMPOSITION && COMPOSITION.composition_digest) || null;
}

/* Review R3, and now F1: this proves and SHOWS the audience, authorises nothing,
   and is the only thing that may fill the composition panel. */
async function inspectComposition() {
  const result = await postJson("/ops/voucher-mailings/api/composition",
    {preview_run_id: PREVIEW_RUN_ID});
  /* Checking the list must never arm the confirmation. */
  OFFER = null;
  hideConfirm();
  const panel = document.getElementById("freeze-panel");
  if (result.transport) {
    /* No answer is not an empty audience. What was read earlier stays on screen,
       marked: overwriting it with zeros would turn a lost connection into a mailing
       that looks like it has nobody in it. */
    if (panel) panel.classList.add("d-none");
    COMPOSITION_STALE = COMPOSITION !== null;
    renderComposition();
    setAlert("danger", "Ответ сервера не получен: состав не прочитан. Повторите проверку.");
    return;
  }
  const data = result.data || {};
  if (!data.composition_proven) {
    if (panel) panel.classList.add("d-none");
    const reasons = (data.reasons || []).join(", ");
    const note = reasons
      ? "Состав нельзя зафиксировать: " + reasons
      : "Состав пуст.";
    /* A refusal is not a composition, so it does not become one. */
    COMPOSITION_STALE = COMPOSITION !== null;
    COMPOSITION_NOTE = COMPOSITION === null ? note : null;
    renderComposition();
    setAlert("warning", note);
    return;
  }
  COMPOSITION = data;
  COMPOSITION_NOTE = null;
  COMPOSITION_STALE = false;
  renderComposition();
  if (panel) panel.classList.remove("d-none");
  prefillApprovalFields(data);
  setAlert("info", "Состав проверен. Подтвердите количество и сумму.");
}

/* The plan's own view of the audience: PII-free, and authoritative about its SIZE
   and IDENTITY. Used to decide whether the screen is stale — never to paint it,
   precisely because it carries no names. */
function plannedComposition(offer) {
  const plan = (offer && offer.plan) || null;
  const snapshot = (plan && plan.snapshot) || null;
  return (snapshot && snapshot.composition) || null;
}

/* The composition on screen is no longer known to hold. Says so where the numbers
   are, not only in the alert area, because the numbers are what gets believed. */
function markCompositionStale(text) {
  COMPOSITION_STALE = true;
  renderComposition();
  const hint = document.getElementById("approval-hint");
  if (hint) hint.textContent = "";
  setAlert("warning", text);
}

/* The numbers are SHOWN, never submitted for the operator: the fields stay empty
   and they type what they read. Auto-filling them would make the confirmation a
   formality instead of a statement. */
function prefillApprovalFields(data) {
  const hint = document.getElementById("approval-hint");
  if (hint) {
    hint.textContent = "Ожидается: "
      + Number(data.recipient_count || 0) + " / " + moneyLabel(data.total_exposure_minor);
  }
}

async function planStage(stage, options) {
  const payload = {stage: stage, preview_run_id: PREVIEW_RUN_ID};
  if (BATCH_ID !== null) payload.batch_id = BATCH_ID;
  if (options && options.slot !== undefined) payload.slot = options.slot;
  if (options && options.count !== undefined) {
    payload.expected_recipient_count = options.count;
    payload.approved_exposure_minor = options.minor;
  }
  const result = await postJson("/ops/voucher-mailings/api/plan", payload);
  renderOffer(stage, result, options || {});
}

function renderOffer(stage, result, options) {
  const data = result.data || {};
  if (result.transport) {
    /* The plan answer never arrived, so nothing is armed — and for a freeze the
       list on screen is no longer known to be current. */
    OFFER = null;
    hideConfirm();
    if (stage === "freeze") {
      markCompositionStale("Ответ сервера не получен. Проверьте состав заново.");
    } else {
      setAlert("danger", "Ответ сервера не получен. Подготовьте шаг заново.");
    }
    return;
  }
  if (!data.ready) {
    OFFER = null;
    hideConfirm();
    const reasons = (data.reasons || []).join(", ");
    const text = reasons
      ? "Действие недоступно: " + reasons
      : "Действие недоступно.";
    if (stage === "freeze") {
      /* A refused freeze plan says the audience was not re-proved as the operator
         stated it — a miscount, an edited preview, an opt-out or an unreachable
         EasyWeek. Which of those it was cannot be read off a reason code, so the
         shown list stops counting as current and one press re-proves it. The
         reviewed page instead ZEROED the list here, which looked like an answer. */
      markCompositionStale(text + " Проверьте состав заново.");
    } else {
      setAlert("warning", text);
    }
    return;
  }
  if (stage === "freeze") {
    /* The plan re-proved the audience while building itself. If what it proved is
       not what this screen is showing, the screen is stale and nothing is armed
       from a list the operator has not seen: one member exchanged for another
       leaves both the count and the money identical, so the digest is what decides
       and the numbers cannot. */
    const planned = plannedComposition(data);
    const shown = compositionDigest();
    if (planned && shown && planned.frozen_digest && planned.frozen_digest !== shown) {
      OFFER = null;
      hideConfirm();
      markCompositionStale("Состав изменился после проверки. Проверьте список заново.");
      return;
    }
  }
  OFFER = data;
  const summary = document.getElementById("confirm-summary");
  if (summary) {
    summary.innerHTML = "<b>" + escapeHtml(stageLabel(stage)) + "</b><pre class=\"mb-0\">" +
      escapeHtml(confirmSummary(data)) + "</pre>";
  }
  const panel = document.getElementById("confirm-panel");
  if (panel) panel.classList.remove("d-none");
}

/* Takes no argument ON PURPOSE (review F1): the only composition it can show is
   the one that was read, so no caller can hand it a plan answer and silently zero
   the audience. */
function renderComposition() {
  const data = COMPOSITION;
  const summary = document.getElementById("composition-summary");
  const slotsArea = document.getElementById("composition-slots");
  if (!data) {
    /* Nothing proven in this page load. Said in words, because an empty table reads
       as "this mailing has nobody in it", which is a different statement. */
    if (summary) {
      summary.innerHTML = '<p class="text-muted" id="c-unchecked">' +
        escapeHtml(COMPOSITION_NOTE || "Состав ещё не проверен.") + "</p>";
    }
    if (slotsArea) slotsArea.innerHTML = "";
    return;
  }
  const stale = COMPOSITION_STALE
    ? '<div class="alert alert-warning py-2" id="composition-stale">' +
      "Эти данные могли измениться после проверки. Нажмите «Проверить состав» заново." +
      "</div>"
    : "";
  if (summary) {
    summary.innerHTML = stale +
      "<table class=\"table table-sm w-auto\">" +
      "<tr><th>Период кампании</th><td id=\"c-period\">" +
      escapeHtml(data.campaign_period || "—") + "</td></tr>" +
      "<tr><th>Получателей</th><td id=\"c-count\">" +
      Number(data.recipient_count || 0) + "</td></tr>" +
      "<tr><th>Общая сумма</th><td id=\"c-total\">" +
      escapeHtml(moneyLabel(data.total_exposure_minor)) + "</td></tr>" +
      "<tr><th>На одного</th><td>" +
      escapeHtml(moneyLabel(data.unit_price_minor || UNIT_PRICE_MINOR)) + "</td></tr>" +
      "</table>";
  }
  if (slotsArea) {
    const people = data.recipients || [];
    let rows = "";
    for (const person of people) {
      /* The ordinal and the preview row are different things and both are shown:
         confusing them would point an action at the wrong person (review R7). */
      rows += "<tr><td>" + escapeHtml(person.slot) + "</td><td>" +
        escapeHtml(person.display_name) + "</td><td>" +
        "<a href=\"/ops/campaigns/" + encodeURIComponent(person.preview_run_id) +
        "/recipients\" target=\"_blank\">#" + escapeHtml(person.campaign_recipient_id) + "</a></td><td>" +
        escapeHtml(moneyLabel(data.unit_price_minor || UNIT_PRICE_MINOR)) + "</td></tr>";
    }
    slotsArea.innerHTML = rows
      ? "<table class=\"table table-sm w-auto\" id=\"composition-table\"><thead><tr><th>№</th>" +
        "<th>Клиент</th><th>Строка preview</th>" +
        "<th>Сумма</th></tr></thead><tbody>" + rows + "</tbody></table>"
      : "<p class=\"text-muted\">Состав пуст.</p>";
  }
}

function hideConfirm() {
  const panel = document.getElementById("confirm-panel");
  if (panel) panel.classList.add("d-none");
}

function cancelConfirm() {
  OFFER = null;
  hideConfirm();
}

async function confirmStage() {
  if (!OFFER || !OFFER.approval || !OFFER.targets) {
    setAlert("warning", "Нет актуального плана. Подготовьте шаг заново.");
    return;
  }
  const button = document.getElementById("btn-confirm");
  if (button) button.disabled = true;
  const result = await postJson("/ops/voucher-mailings/api/confirm", {
    approval_id: OFFER.approval.approval_id,
    confirmed_count: OFFER.targets.stage_target_count,
    confirmed_amount_minor: OFFER.targets.stage_amount_minor
  });
  /* The approval is spent either way, so the offer is dropped before anything
     else: a second press must not be able to send the same id again. The server
     would answer with the same operation anyway — this just stops asking. */
  OFFER = null;
  hideConfirm();
  if (button) button.disabled = false;
  if (result.transport) {
    /* The answer never came back, and the operation may well have been committed
       before the connection broke (review F2). The one thing that must not happen
       now is a second confirmation, so none is sent: the page asks the server what
       exists. A duplicate would be refused by the spent approval anyway — this is
       about not asking, and about not telling the operator that nothing happened. */
    setAlert("warning", "Ответ не получен. Повторное подтверждение не отправляется —"
      + " состояние уточняется на сервере.");
    await resumeFromServer();
    return;
  }
  const data = result.data || {};
  if (data.accepted && data.operation) {
    const where = data.operation.batch_id;
    if (where) {
      if (BATCH_ID === null) {
        window.location.href = "/ops/voucher-mailings/" + where;
        return;
      }
      setAlert("info", data.created
        ? "Шаг принят в работу."
        : "Этот шаг уже был принят раньше — повторно ничего не выполняется.");
      refreshStatus();
      return;
    }
    /* A FREEZE is confirmed before its batch exists, so the answer carries no
       batch id (review R5). The page watches the OPERATION until one appears
       instead of giving up — and remembers it, so a refresh resumes watching the
       same operation rather than offering a second freeze. */
    TRACKED_OPERATION = data.operation.operation_id;
    rememberTrackedOperation(TRACKED_OPERATION);
    setAlert("info", data.created
      ? "Шаг принят в работу. Можно закрыть страницу — работа продолжится на сервере."
      : "Этот шаг уже был принят раньше — повторно ничего не выполняется.");
    trackOperation();
    return;
  }
  setAlert("warning", "Отказ: " + ((data.reasons || []).join(", ") || "неизвестно"));
  refreshStatus();
}

async function stopBatch() {
  if (BATCH_ID === null) return;
  const result = await postJson("/ops/voucher-mailings/api/stop", {batch_id: BATCH_ID});
  const data = result.data || {};
  setAlert(data.stop_active ? "warning" : "secondary",
    data.stop_active
      ? "Запрос на остановку сохранён."
      : "Остановка не принята.");
  refreshStatus();
}

async function reconcile() {
  if (BATCH_ID === null) return;
  const result = await postJson("/ops/voucher-mailings/api/reconcile",
    {batch_id: BATCH_ID, preview_run_id: PREVIEW_RUN_ID});
  const data = result.data || {};
  setAlert(data.accepted ? "info" : "warning", data.accepted
    ? "Сверка выполнена."
    : "Сверка недоступна.");
  refreshStatus();
}

async function loadRecipients() {
  if (BATCH_ID === null) return;
  const response = await fetch("/ops/voucher-mailings/api/recipients?batch_id=" +
    encodeURIComponent(BATCH_ID), {credentials: "same-origin"});
  let data = {};
  try { data = await response.json(); } catch (err) { return; }
  RECIPIENTS = {};
  for (const person of (data.recipients || [])) {
    RECIPIENTS[person.slot] = person;
  }
}

function refundSlot(slot) {
  /* The slot the button carries, and nothing derived from its position in the
     table: re-sorting or re-rendering must not change whose money comes back. */
  planStage("refund", {slot: slot});
}

/* Review R7: the individual refund confirmation names the same person. */
function refundSubject(slot) {
  const who = RECIPIENTS[slot] || null;
  if (!who) return "получатель №" + slot;
  return who.display_name + " (№" + slot + ", строка preview #" + who.campaign_recipient_id + ")";
}

/* ===========================================================================
   A CACHE, not the source of truth (review F2)
   ===========================================================================

   The reviewed page resumed only from this store. Everything else therefore looked
   like a mailing nobody had started: a second tab, another browser, a private
   window, a fresh login, a cleared store — and the tab whose confirm response was
   lost on the way back, which had no id to remember in the first place even though
   its operation existed on the server.

   The durable state is in PostgreSQL and the status endpoint is already scoped to
   one preview, so that is what the page asks on load. This store is kept for what
   it is good for: showing something immediately, and having a last known id when
   the server cannot be reached at all. It is never the reason a state is believed.
   Wrapped in try/catch because a private window can throw on access. */
function rememberTrackedOperation(id) {
  try {
    window.sessionStorage.setItem("ew-voucher-op-" + PREVIEW_RUN_ID, String(id));
  } catch (err) { /* storage unavailable: tracking still works for this page load */ }
}

function forgetTrackedOperation() {
  try {
    window.sessionStorage.removeItem("ew-voucher-op-" + PREVIEW_RUN_ID);
  } catch (err) { /* nothing to clean up */ }
}

function recallTrackedOperation() {
  try {
    const raw = window.sessionStorage.getItem("ew-voucher-op-" + PREVIEW_RUN_ID);
    return raw ? parseInt(raw, 10) : null;
  } catch (err) { return null; }
}

function operationLabel(status) {
  if (status === "queued") return "в очереди";
  if (status === "running") return "выполняется";
  if (status === "completed") return "исполнение завершено";
  if (status === "refused") return "отказано";
  if (status === "expired") return "план истёк";
  if (status === "interrupted") return "прервано — нужна сверка";
  return status;
}

/* Does this operation still need watching, or has it settled? */
function operationSettled(operation) {
  if (!operation) return false;
  return ["completed", "refused", "expired", "interrupted"].indexOf(operation.status) !== -1;
}

/* How long between polls, and how many failed reads in a row are tried before the
   page stops and asks for a refresh. Bounded: an unreachable server must not be
   polled forever by a tab somebody left open. */
const TRACK_INTERVAL_MS = 1500;
const TRACK_MAX_FAILURES = 5;
let TRACK_FAILURES = 0;

async function trackOperation() {
  if (TRACKED_OPERATION === null) return;
  let response = null;
  try {
    response = await fetch("/ops/voucher-mailings/api/operation?operation_id=" +
      encodeURIComponent(TRACKED_OPERATION) + "&preview_run_id=" + encodeURIComponent(PREVIEW_RUN_ID),
      {credentials: "same-origin"});
  } catch (err) {
    trackAgainAfterFailure();
    return;
  }
  if (response.status === 404) {
    /* Authoritative, and not a failure in disguise: this endpoint answers 404 only
       for an operation that does not exist or belongs to another preview, while a
       database that cannot answer raises a 500. Another preview's work is never
       shown here, so the id goes. */
    TRACKED_OPERATION = null;
    forgetTrackedOperation();
    return;
  }
  if (!response.ok) {
    trackAgainAfterFailure();
    return;
  }
  let data = null;
  try { data = await response.json(); } catch (err) { trackAgainAfterFailure(); return; }
  const operation = data && data.operation;
  if (!operation) { trackAgainAfterFailure(); return; }
  /* A read landed, so the connection notice — if there was one — is over. */
  TRACK_FAILURES = 0;
  renderTrackedOperation(operation);
  if (operation.batch_id) {
    /* The freeze produced a batch. Go to it. */
    TRACKED_OPERATION = null;
    forgetTrackedOperation();
    window.location.href = "/ops/voucher-mailings/" + operation.batch_id;
    return;
  }
  if (operationSettled(operation)) {
    TRACKED_OPERATION = null;
    forgetTrackedOperation();
    return;
  }
  setTimeout(trackOperation, TRACK_INTERVAL_MS);
}

/* A read that did not land is a LOST CONNECTION and is never "there is no
   operation" (review F2). The reviewed version returned silently on a network error
   or an unparseable body, which ended the polling for good and left the screen
   looking like a mailing that was never started — while a confirmed stage was in
   fact running on the server.

   So the page says what happened and tries again on a widening delay. The id stays
   in memory and in the cache, so even once the attempts are spent, a reload resumes
   watching the same operation; nothing here re-sends a confirmation or starts
   anything. */
function trackAgainAfterFailure() {
  TRACK_FAILURES += 1;
  if (TRACK_FAILURES >= TRACK_MAX_FAILURES) {
    renderConnectionLost("Связь с сервером потеряна. Шаг продолжает выполняться на сервере"
      + " — обновите страницу, чтобы снова увидеть его состояние.");
    return;
  }
  renderConnectionLost("Связь с сервером потеряна. Повторная попытка…");
  setTimeout(trackOperation, TRACK_INTERVAL_MS * TRACK_FAILURES);
}

/* Not knowing is its own state, and it is not "nothing is happening". */
function renderConnectionLost(text) {
  const area = document.getElementById("operation-panel") || document.getElementById("alert-area");
  if (!area) return;
  area.innerHTML = '<div class="alert alert-warning" id="operation-unreachable">' +
    escapeHtml(text) + "</div>";
}

/* ===========================================================================
   What this page shows comes from the SERVER (review F2)
   ===========================================================================

   Called on load, and after any confirmation whose answer was lost. It reads the
   state of THIS preview from the scoped status endpoint — the one the mailing page
   already polls, rather than a second, narrower cousin that could disagree with it —
   and shows whichever of the three things is true: the mailing already exists, an
   operation is queued or running or has settled, or this preview genuinely has
   nothing. */
async function resumeFromServer() {
  if (BATCH_ID !== null) {
    /* A mailing page already has its scope and its own poll; that IS its resume. */
    await refreshStatus();
    return;
  }
  const read = await readPreviewState();
  if (!read.ok) {
    const cached = recallTrackedOperation();
    if (cached !== null) {
      /* The server is unreachable and this browser has a last known id: watch that,
         while saying plainly that the connection — not the mailing — is the
         problem. */
      TRACKED_OPERATION = cached;
      renderConnectionLost("Связь с сервером потеряна. Повторная попытка…");
      await trackOperation();
      return;
    }
    renderConnectionLost("Состояние на сервере прочитать не удалось."
      + " Это не значит, что шаг не выполняется — обновите страницу.");
    return;
  }
  const state = read.state || {};
  const batch = state.batch || {};
  if (batch.batch_id) {
    /* The freeze finished while nobody was watching, so this is the wrong screen to
       be on: the mailing has its own. */
    forgetTrackedOperation();
    window.location.href = "/ops/voucher-mailings/" + batch.batch_id;
    return;
  }
  const history = state.operations || [];
  const operation = state.active_operation || (history.length ? history[0] : null);
  if (!operation) {
    /* The server says this preview has no operation at all. THAT is when a cached id
       is wrong, and the only time the cache is cleared on a successful read. */
    TRACKED_OPERATION = null;
    forgetTrackedOperation();
    return;
  }
  TRACKED_OPERATION = operation.operation_id;
  rememberTrackedOperation(TRACKED_OPERATION);
  renderTrackedOperation(operation);
  if (!operationSettled(operation)) await trackOperation();
}

/* This preview's durable state, or an honest failure. Scoped by preview, so one
   preview can never be shown another's work. */
async function readPreviewState() {
  let response = null;
  try {
    response = await fetch("/ops/voucher-mailings/api/status?preview_run_id=" +
      encodeURIComponent(PREVIEW_RUN_ID), {credentials: "same-origin"});
  } catch (err) {
    return {ok: false};
  }
  if (!response.ok) return {ok: false};
  try {
    return {ok: true, state: await response.json()};
  } catch (err) {
    return {ok: false};
  }
}

function renderTrackedOperation(operation) {
  const area = document.getElementById("operation-panel") || document.getElementById("alert-area");
  if (!area) return;
  const settled = operationSettled(operation);
  const kind = operation.status === "completed" ? "info"
    : (settled ? "warning" : "primary");
  let text = stageLabel(operation.stage) + ": " + operationLabel(operation.status);
  if (operation.outcome_code) text += " (" + operation.outcome_code + ")";
  if (!settled) text += ". Можно закрыть страницу — работа продолжится на сервере.";
  area.innerHTML = '<div class="alert alert-' + kind + '" id="tracked-operation">' +
    escapeHtml(text) + "</div>";
}

async function refreshStatus() {
  if (BATCH_ID === null) return;
  const response = await fetch("/ops/voucher-mailings/api/status?batch_id=" + BATCH_ID,
    {credentials: "same-origin"});
  let state = {};
  try { state = await response.json(); } catch (err) { return; }
  renderStatus(state);
}

function renderStatus(state) {
  const banner = stateBanner(state);
  const bannerArea = document.getElementById("stop-banner");
  if (bannerArea) {
    bannerArea.innerHTML = banner
      ? '<div class="alert alert-' + banner.kind + '">' + escapeHtml(banner.text) + "</div>"
      : "";
  }

  const operation = state.active_operation;
  const opArea = document.getElementById("operation-panel");
  if (opArea) {
    if (operation) {
      opArea.innerHTML = '<div class="alert alert-primary">' +
        escapeHtml(stageLabel(operation.stage)) + ": " + escapeHtml(operation.status) +
        ". Можно закрыть страницу — работа продолжится на сервере.</div>";
    } else {
      const history = state.operations || [];
      const last = history.length ? history[0] : null;
      opArea.innerHTML = last
        ? '<p class="small text-muted">Последний шаг: ' +
          escapeHtml(stageLabel(last.stage)) + " — " + escapeHtml(last.status) +
          (last.outcome_code ? " (" + escapeHtml(last.outcome_code) + ")" : "") + "</p>"
        : '<p class="small text-muted">Шагов ещё не было.</p>';
    }
  }

  const deliveryArea = document.getElementById("delivery-panel");
  if (deliveryArea) {
    let rows = "";
    for (const pair of deliveryFacts(state)) {
      rows += "<tr><th class=\"text-nowrap\">" + escapeHtml(pair[0]) + "</th><td>" +
        escapeHtml(pair[1]) + "</td></tr>";
    }
    deliveryArea.innerHTML = "<table class=\"table table-sm w-auto\">" + rows + "</table>";
  }

  const slotsArea = document.getElementById("slots-panel");
  if (slotsArea) {
    const items = (state.batch && state.batch.items) || [];
    let rows = "";
    for (const item of items) {
      /* Review R7: a slot number does not tell an operator whose €15 this is. The
         name comes from the authorised recipients endpoint and is escaped like
         every other value here. */
      const who = RECIPIENTS[item.slot] || null;
      const action = mayRefund(item)
        ? '<button class="btn btn-sm btn-outline-danger" data-slot="' + item.slot +
          '" onclick="refundSlot(' + item.slot + ')">Вернуть оплату</button>'
        : "";
      rows += '<tr data-slot="' + escapeHtml(item.slot) + '"><td>' + escapeHtml(item.slot) + "</td><td>" +
        (who ? escapeHtml(who.display_name) : "<span class=\"text-muted\">…</span>") + "</td><td>" +
        (who
          ? '<a href="/ops/campaigns/' + encodeURIComponent(who.preview_run_id) +
            '/recipients" target="_blank">#' + escapeHtml(who.campaign_recipient_id) + "</a>"
          : "—") +
        "</td><td><code>" + escapeHtml(item.status) +
        "</code></td><td><code>" + escapeHtml(item.reason_code || "—") + "</code></td><td>" +
        escapeHtml(item.send_attempt_count || 0) + "</td><td>" +
        escapeHtml(item.provider_accepted ? "да" : "нет") + "</td><td>" +
        escapeHtml(item.webhook_delivered ? "да" : "нет") + "</td><td>" +
        escapeHtml(item.webhook_read ? "да" : "нет") + "</td><td>" +
        escapeHtml(item.reconciliation_required ? "да" : "нет") + "</td><td>" +
        action + "</td></tr>";
    }
    slotsArea.innerHTML = rows
      ? "<table class=\"table table-sm align-middle\" id=\"slots-table\"><thead><tr><th>№</th>" +
        "<th>Клиент</th><th>Строка preview</th><th>Статус</th>" +
        "<th>Причина</th><th>Попыток</th><th>Meta приняла</th>" +
        "<th>Доставлено</th><th>Прочитано</th><th>Сверка</th><th></th>" +
        "</tr></thead><tbody>" + rows + "</tbody></table>"
      : "<p class=\"text-muted\">Получателей нет.</p>";
  }

  applyStageAvailability(state);
}

/* The buttons follow the durable state. This is a convenience, never a control:
   the server re-checks every constraint independently, so an operator who finds
   an enabled button they should not have gets a refusal, not an effect. */
function applyStageAvailability(state) {
  const next = nextAction(state);
  for (const stage of ["create", "pay", "deliver"]) {
    const button = document.getElementById("btn-stage-" + stage);
    if (button) button.disabled = next !== stage;
  }
  const stop = document.getElementById("btn-stop");
  if (stop) stop.disabled = Boolean(state && state.stop_active);
}
"""


__all__ = ["CSRF_HEADER", "VOUCHER_CODE_PLACEHOLDER", "router"]
