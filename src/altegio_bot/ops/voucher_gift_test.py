"""One server-selected test gift, isolated from campaign previews and delivery."""

from __future__ import annotations

import json
from typing import Literal

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.exc import SQLAlchemyError

from altegio_bot.campaigns import easyweek_voucher_owner_test as owner_test
from altegio_bot.db import SessionLocal
from altegio_bot.ops.auth import OpsSessionError, require_ops_auth, resolve_ops_session
from altegio_bot.ops.router import _page
from altegio_bot.ops.voucher_mailing import _csrf_for, _principal_or_error, _session_error

router = APIRouter(
    prefix="/ops/voucher-gift-test", dependencies=[Depends(require_ops_auth)], tags=["voucher-gift-test"]
)
PRIVATE_HEADERS = {"Cache-Control": "no-store, no-cache, must-revalidate, private", "Pragma": "no-cache"}


class EmptyRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")


class PlanRequest(EmptyRequest):
    stage: Literal["create", "pay"]


class ConfirmRequest(EmptyRequest):
    approval_id: str = Field(min_length=1, max_length=128)
    confirmed_count: int = Field(strict=True, ge=1, le=1)
    confirmed_nominal_minor: int = Field(strict=True, ge=1000, le=1000)
    confirmed_issue_minor: int = Field(strict=True, ge=0, le=0)


def _reply(body: dict, status: int = 200) -> JSONResponse:
    return JSONResponse(content=body, status_code=status, headers=PRIVATE_HEADERS)


def _read_guard(request: Request) -> JSONResponse | None:
    try:
        resolve_ops_session(request)
    except OpsSessionError as exc:
        return _session_error(exc)
    if request.query_params:
        return _reply({"accepted": False, "reasons": ["owner_test_unexpected_parameters"]}, 400)
    return None


async def _call(action, *args, **kwargs) -> JSONResponse:
    try:
        return _reply(await action(*args, **kwargs))
    except owner_test.OwnerTestError as exc:
        return _reply({"accepted": False, "ready": False, "reasons": [exc.reason]}, 409)
    except SQLAlchemyError:
        return _reply({"status_known": False, "reasons": ["owner_test_database_unavailable"]}, 503)


@router.get("/api/status")
async def api_status(request: Request) -> JSONResponse:
    refusal = _read_guard(request)
    if refusal is not None:
        return refusal
    return await _call(owner_test.get_status, SessionLocal)


@router.post("/api/plan")
async def api_plan(request: Request, payload: PlanRequest) -> JSONResponse:
    principal, refusal = _principal_or_error(request)
    if refusal is not None or principal is None:
        return refusal or _session_error(OpsSessionError("ops_session_invalid"))
    return await _call(owner_test.offer, SessionLocal, principal=principal, stage=payload.stage)


@router.post("/api/confirm")
async def api_confirm(request: Request, payload: ConfirmRequest) -> JSONResponse:
    principal, refusal = _principal_or_error(request)
    if refusal is not None or principal is None:
        return refusal or _session_error(OpsSessionError("ops_session_invalid"))
    return await _call(owner_test.confirm, SessionLocal, principal=principal, **payload.model_dump())


@router.post("/api/reconcile")
async def api_reconcile(request: Request, payload: EmptyRequest) -> JSONResponse:
    principal, refusal = _principal_or_error(request)
    if refusal is not None or principal is None:
        return refusal or _session_error(OpsSessionError("ops_session_invalid"))
    return await _call(owner_test.reconcile, SessionLocal, principal=principal)


@router.post("/api/stop")
async def api_stop(request: Request, payload: EmptyRequest) -> JSONResponse:
    principal, refusal = _principal_or_error(request)
    if refusal is not None or principal is None:
        return refusal or _session_error(OpsSessionError("ops_session_invalid"))
    return await _call(owner_test.stop, SessionLocal, principal=principal)


@router.get("", response_class=HTMLResponse, response_model=None)
async def page_owner_test(request: Request) -> HTMLResponse | JSONResponse:
    refusal = _read_guard(request)
    if refusal is not None:
        return refusal
    # Only an HMAC-derived CSRF token enters the script; no customer or product
    # identifiers, voucher codes or runtime configuration are embedded in HTML.
    csrf = json.dumps(_csrf_for(request))
    body = """
<h1 class="h4">Тест бесплатного сертификата</h1>
<p><a href="/ops/voucher-mailings">Вернуться к ваучерным рассылкам</a></p>
<div class="alert alert-info">
  Один тестовый клиент, заданный на сервере. Это отдельный тест, не рассылка:
  previews и их получатели не изменяются. WhatsApp и Meta здесь не вызываются.
</div>
<dl class="row" id="gift-contract">
  <dt class="col-sm-5">Количество</dt><dd class="col-sm-7">1 сертификат</dd>
  <dt class="col-sm-5">Номинал</dt><dd class="col-sm-7">10,00 €</dd>
  <dt class="col-sm-5">Цена выпуска</dt><dd class="col-sm-7">0,00 €</dd>
  <dt class="col-sm-5">Касса</dt><dd class="col-sm-7">Aktionsgutscheine</dd>
  <dt class="col-sm-5">Издатель</dt><dd class="col-sm-7">Юлия Мюллер</dd>
</dl>
<p class="small text-muted">
  Нулевая цена не означает, что заказ оплачен. Сначала создаётся один заказ, затем
  отдельным подтверждением выполняется его оплата. Успех теста не открывает рассылку.
  Срок и погашение контролирует EasyWeek; влияние на отчётность этим тестом не подтверждается.
</p>
<div id="gift-message" role="status" aria-live="polite" class="alert alert-secondary">Загрузка состояния…</div>
<div id="gift-loading" class="d-none mb-3" role="status" aria-live="polite">
  <span class="spinner-border spinner-border-sm" aria-hidden="true"></span>
  Проверка выполняется. Дождитесь результата…
</div>
<div class="card mb-3"><div class="card-body">
  <p>Состояние: <strong id="gift-status">неизвестно</strong></p>
  <p id="gift-facts"></p><ul id="gift-reasons"></ul>
  <button type="button" id="gift-refresh" class="btn btn-outline-secondary">Обновить состояние</button>
  <button type="button" data-action="create" id="gift-create" class="btn btn-primary" disabled>
    Подготовить CREATE</button>
  <button type="button" data-action="pay" id="gift-pay" class="btn btn-primary" disabled>Подготовить PAY</button>
  <button type="button" data-action="reconcile" id="gift-reconcile" class="btn btn-outline-primary" disabled>
    Сверить с EasyWeek</button>
  <button type="button" data-action="stop" id="gift-stop" class="btn btn-outline-danger" disabled>
    Остановить тест навсегда</button>
</div></div>
<div id="gift-confirm-panel" class="card border-warning d-none"><div class="card-body">
  <h2 class="h5">Отдельное подтверждение шага</h2>
  <p id="gift-confirm-summary"></p>
  <p>Один сертификат: номинал 10,00 €, цена выпуска 0,00 €, касса Aktionsgutscheine.</p>
  <button type="button" id="gift-confirm" class="btn btn-warning">Подтвердить только этот шаг</button>
  <button type="button" id="gift-cancel" class="btn btn-outline-secondary">Отмена</button>
</div></div>
<p class="small text-muted mt-3">
  STOP окончателен: новый план и перезапуск не разрешат повторный выпуск. Уже начатый
  запрос может завершиться после STOP — его результат нужно сверить. Автоматических
  повторов, возвратов или заменяющих сертификатов нет. Код сертификата здесь не показывается.
</p>
"""
    script = _SCRIPT.replace("__CSRF__", csrf).replace("__SCOPE__", json.dumps(owner_test.SCOPE))
    return HTMLResponse(_page("Тест бесплатного сертификата", body + script), headers=PRIVATE_HEADERS)


_SCRIPT = """
<script>
(() => {
  'use strict';
  const csrf = __CSRF__;
  const scope = __SCOPE__;
  const root = '/ops/voucher-gift-test/api/';
  const byId = id => document.getElementById(id);
  let state = null, approval = null, busy = false, polling = false, generation = 0;
  function message(text, failed = false) {
    byId('gift-message').textContent = text;
    byId('gift-message').className = 'alert ' + (failed ? 'alert-warning' : 'alert-info');
  }
  function controls() {
    const actions = state && state.status_known === true ? (state.available_actions || []) : [];
    document.querySelectorAll('[data-action]').forEach(button => {
      button.disabled = busy || !actions.includes(button.dataset.action);
    });
    byId('gift-refresh').disabled = busy;
    byId('gift-confirm').disabled = busy || !approval;
    byId('gift-cancel').disabled = busy;
    byId('gift-loading').classList.toggle('d-none', !busy);
  }
  function closeApproval() {
    approval = null;
    byId('gift-confirm-panel').classList.add('d-none');
  }
  async function request(path, payload) {
    const options = {credentials: 'same-origin', cache: 'no-store'};
    if (payload !== undefined) {
      options.method = 'POST';
      options.headers = {'Content-Type': 'application/json', 'X-Ops-CSRF': csrf};
      options.body = JSON.stringify(payload);
    }
    const response = await fetch(root + path, options);
    const data = await response.json();
    if (!response.ok) {
      const reasons = Array.isArray(data.reasons) ? data.reasons.join(', ') : '';
      throw new Error(reasons || 'Ответ сервера не подтверждает выполнение. Обновите состояние.');
    }
    return data;
  }
  async function refresh() {
    const requestedGeneration = generation;
    const incoming = await request('status');
    // A response from before STOP/confirm must not restore the old controls.
    if (requestedGeneration !== generation) return;
    state = incoming;
    if (state.status_known !== true || state.scope !== scope) {
      state = null;
      throw new Error('Состояние теста не подтверждено. Действия заблокированы.');
    }
    byId('gift-status').textContent = state.state || 'не начат';
    byId('gift-facts').textContent =
      'Остановлен: ' + (state.stopped ? 'да' : 'нет') +
      '; заказ прочитан: ' + (state.order_observed ? 'да' : 'нет') +
      '; наличие кода подтверждено: ' + (state.code_present ? 'да' : 'нет') +
      '; операция: ' + (state.operation_status || 'нет');
    byId('gift-reasons').replaceChildren();
    const reasons = [...(state.reasons || [])];
    if (state.reason && !reasons.includes(state.reason)) reasons.push(state.reason);
    for (const reason of reasons) {
      const li = document.createElement('li'); li.textContent = reason;
      byId('gift-reasons').appendChild(li);
    }
    controls();
  }
  async function action(work) {
    if (busy) return;
    generation += 1;
    busy = true; controls();
    message('Запрос выполняется. Не повторяйте действие.');
    try { await work(); }
    catch (error) {
      state = null;
      closeApproval();
      message('Не подтверждено: ' + error.message +
        ' Не запускайте повторный выпуск; обновите состояние или выполните сверку.', true);
    }
    finally { busy = false; controls(); }
  }
  async function offer(stage) {
    closeApproval();
    const result = await request('plan', {stage});
    if (result.ready !== true || result.stage !== stage || !result.approval_id) {
      throw new Error((result.reasons || []).join(', ') || 'План не готов.');
    }
    approval = result;
    byId('gift-confirm-summary').textContent = 'Шаг: ' + stage.toUpperCase() +
      '. Разрешение действительно до: ' + (result.expires_at || 'не указано');
    byId('gift-confirm-panel').classList.remove('d-none');
    message('Проверки завершены. Прочитайте и отдельно подтвердите только этот шаг.');
  }
  byId('gift-create').addEventListener('click', () => action(() => offer('create')));
  byId('gift-pay').addEventListener('click', () => action(() => offer('pay')));
  byId('gift-confirm').addEventListener('click', () => action(async () => {
    if (!approval) throw new Error('Нет подтверждённого плана.');
    const result = await request('confirm', {
      approval_id: approval.approval_id,
      confirmed_count: 1, confirmed_nominal_minor: 1000, confirmed_issue_minor: 0
    });
    if (result.accepted !== true) throw new Error((result.reasons || []).join(', ') || 'Шаг не принят.');
    closeApproval(); await refresh();
    message('Шаг поставлен в очередь. Дождитесь результата исполнителя; следующий шаг не запускается автоматически.');
  }));
  byId('gift-reconcile').addEventListener('click', () => action(async () => {
    closeApproval(); await request('reconcile', {}); await refresh();
    message('Сверка завершена. Проверьте состояние и причины выше.');
  }));
  byId('gift-stop').addEventListener('click', () => {
    const warning = 'Окончательно остановить этот единственный тест? Возобновления и повторного выпуска не будет.';
    if (!window.confirm(warning)) return;
    action(async () => {
      closeApproval(); await request('stop', {}); await refresh();
      message('STOP сохранён. Уже начатый запрос может завершиться; при необходимости выполните сверку.');
    });
  });
  byId('gift-cancel').addEventListener('click', () => { closeApproval(); controls(); });
  byId('gift-refresh').addEventListener('click', () => action(async () => {
    closeApproval(); await refresh(); message('Состояние обновлено.');
  }));
  action(async () => { await refresh(); message('Состояние прочитано. Выберите один доступный шаг.'); });
  window.setInterval(async () => {
    if (busy || polling || !state || !['queued', 'running'].includes(state.operation_status)) return;
    polling = true;
    const requestedGeneration = generation;
    try { await refresh(); }
    catch (_) {
      if (requestedGeneration === generation) {
        state = null; controls();
        message('Не удалось прочитать результат. Обновите состояние; не повторяйте выпуск.', true);
      }
    }
    finally { polling = false; }
  }, 2500);
})();
</script>
"""
