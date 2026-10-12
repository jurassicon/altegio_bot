"""One compact screen: what happened to the messages, and who booked after them.

Where it sits
-------------
Inside the existing Ops voucher-mailing section, as its own page, reachable
from the mailing list and from the mailing page. It deliberately does not
replace the mailing page and removes nothing from it: the stage buttons, the
stop, the reconcile and the per-recipient refund stay exactly where they are,
with exactly the restrictions they already had, and this page links to them.

What it is
----------
A READ. Every route here is a GET that reads the local database and nothing
else. No EasyWeek request, no Meta request, no write of any kind — not to the
recipients, the eligibility, the stages, the dispatch queue, the execution
counters or the send permissions. Opening it, reloading it and pressing
"Обновить" twenty times does the same nothing, which is why the refresh button
is safe to offer at all.

What is on it, and what is deliberately not
-------------------------------------------
Six numbers, one compact row per recipient, and everything technical folded
into a per-row details panel: slot, preview row, basis, item status, reason
code, attempt count, the send anchor and its evidence, the attribution window
and where it came from, how the identity was bridged, reconciliation flags, and
every matching booking with its branch, creation date and current state.

Dates on this page are CREATION dates, and are labelled as such. The report has
no proven appointment start, so it never presents a creation as "the next
visit" — the compact column is "Созданные записи" and each date reads
"Создана: …".

There are no wide columns for UUIDs, attempts, reason codes, basis,
reconciliation or technical timestamps, and the Altegio campaign/follow-up
table is not brought here. A voucher code, an HMAC, a raw payload, an order
UUID, a customer UUID and a booking UUID are not on this page at all.

Times
-----
The salon's configured zone (``ops_local_tz``, Europe/Berlin for the German
branches), named once at the top, and never duplicated as UTC. Storage and
comparison stay timezone-aware UTC in the analytics module; only the rendering
is local, so a DST change moves the printed clock and not a boundary.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Final
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, JSONResponse
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

from altegio_bot.campaigns.easyweek_voucher_production import booking_attribution as attribution
from altegio_bot.campaigns.easyweek_voucher_production.identity import DATABASE_UNAVAILABLE
from altegio_bot.db import SessionLocal
from altegio_bot.models.models import (
    CampaignRecipient,
    EasyWeekVoucherProductionBatchItem,
)
from altegio_bot.ops.auth import require_ops_auth
from altegio_bot.ops.router import _esc, _local_tz, _page

router = APIRouter(
    prefix="/ops/voucher-mailings",
    dependencies=[Depends(require_ops_auth)],
    tags=["voucher-mailing-report"],
)

# This page names real people. It is for one authenticated operator in one
# moment and must not sit in a shared cache or a browser's history store.
_PRIVATE_HEADERS: Final = {"Cache-Control": "no-store, no-cache, must-revalidate, private", "Pragma": "no-cache"}

_UNSCOPED: Final = "voucher_production_unscoped_report"


def _local(value: str | None, tz: ZoneInfo) -> str:
    """One stored instant, in the salon's zone. Never a second copy in UTC.

    The zone itself comes from the dashboard's own resolver (``ops_local_tz``,
    falling back to Europe/Berlin), so this page cannot drift into a second idea
    of what "local" means. A broken setting must not take the report down, and it
    must not silently become UTC either: every German branch here is on Berlin
    time.
    """
    if not value:
        return ""
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return ""
    return parsed.astimezone(tz).strftime("%d.%m.%Y %H:%M")


def _ui_report(report: attribution.BookingReport) -> dict[str, Any]:
    """The safe report, plus the local strings the page prints.

    Formatting happens on the SERVER on purpose. The browser's own zone is the
    operator's laptop, which may be anywhere; the zone that means something here
    is the salon's.
    """
    tz = _local_tz()
    payload = report.as_safe_dict()
    payload["local_timezone"] = tz.key
    payload["generated_at_local"] = _local(payload.get("generated_at"), tz)
    coverage = payload["coverage"]
    coverage["scan_from_local"] = _local(coverage.get("scan_from"), tz)
    coverage["scan_to_local"] = _local(coverage.get("scan_to"), tz)
    for row in payload["rows"]:
        row["send_attempted_at_local"] = _local(row.get("send_attempted_at"), tz)
        row["window_start_local"] = _local(row.get("window_start"), tz)
        row["window_end_local"] = _local(row.get("window_end"), tz)
        for booking in row["bookings"]:
            booking["booking_created_at_local"] = _local(booking.get("booking_created_at"), tz)
    return payload


@router.get("/api/booking-report")
async def api_booking_report(batch_id: int | None = None, preview_run_id: int | None = None) -> JSONResponse:
    """The whole report for one mailing. A read, and always scoped.

    Scoped for the same reason the status endpoint is: before a freeze there is
    no batch to scope by, and falling back to "everything" would show one
    mailing's work on another mailing's page.
    """
    if batch_id is None and preview_run_id is None:
        return JSONResponse(status_code=400, content={"report_known": False, "reasons": [_UNSCOPED]})
    try:
        report = await attribution.build_report(SessionLocal, batch_id=batch_id, preview_run_id=preview_run_id)
    except SQLAlchemyError:
        # A database that cannot answer is NOT a mailing without bookings. The
        # two must never be confused, so this says the state is unknown in its
        # status code and in one reason code, with no SQL and no identity.
        return JSONResponse(
            status_code=503,
            content={"report_known": False, "reasons": [DATABASE_UNAVAILABLE]},
            headers=_PRIVATE_HEADERS,
        )
    return JSONResponse(status_code=200, content=_ui_report(report), headers=_PRIVATE_HEADERS)


@router.get("/api/booking-report/clients")
async def api_booking_report_clients(batch_id: int) -> JSONResponse:
    """Slot → the person it is about, for the authorised operator only.

    Its own endpoint, and deliberately not part of the report body: the report
    is PII-free so that it can be logged, quoted in a review and compared
    across runs. A name and a phone can be none of those things.

    Separate from ``dispatch.recipient_lines`` because that type is shaped for
    the refund confirmation — slot, name, proof state — and this page needs the
    phone as a second line when two recipients share a display name. Extending
    the refund line with a phone would widen a surface that exists to name one
    person at a time.
    """
    async with SessionLocal() as session:
        found = (
            await session.execute(
                select(
                    EasyWeekVoucherProductionBatchItem.slot,
                    EasyWeekVoucherProductionBatchItem.campaign_recipient_id,
                    CampaignRecipient.display_name,
                    CampaignRecipient.phone_e164,
                )
                .join(
                    CampaignRecipient,
                    CampaignRecipient.id == EasyWeekVoucherProductionBatchItem.campaign_recipient_id,
                )
                .where(EasyWeekVoucherProductionBatchItem.batch_id == batch_id)
                .order_by(EasyWeekVoucherProductionBatchItem.slot.asc())
            )
        ).all()
    return JSONResponse(
        status_code=200,
        content={
            "batch_id": batch_id,
            "clients": [
                {
                    "slot": int(slot),
                    "campaign_recipient_id": int(recipient_id),
                    # A blank name is answered as a readable placeholder: an
                    # empty cell is indistinguishable from a bug.
                    "display_name": (display_name or "").strip() or f"без имени (строка preview {int(recipient_id)})",
                    "phone_e164": phone or "",
                }
                for slot, recipient_id, display_name, phone in found
            ],
        },
        headers=_PRIVATE_HEADERS,
    )


@router.get("/report", response_class=HTMLResponse)
async def page_report(request: Request, batch_id: int | None = None, preview_run_id: int | None = None) -> str:
    """The compact report, for one mailing or for a preview that has none yet."""
    del request
    if batch_id is None and preview_run_id is None:
        return _page(
            "Отчёт по рассылке",
            "<div class='alert alert-danger'>Нужно указать рассылку или preview.</div>"
            "<a href='/ops/voucher-mailings'>К списку рассылок</a>",
        )
    tz = _local_tz()
    scope = json.dumps({"batch_id": batch_id, "preview_run_id": preview_run_id})
    title = f"Отчёт по рассылке #{batch_id}" if batch_id is not None else f"Отчёт по preview #{preview_run_id}"
    back = (
        f"<a href='/ops/voucher-mailings/{int(batch_id)}'>страница рассылки</a>"
        if batch_id is not None
        else "<a href='/ops/voucher-mailings'>список рассылок</a>"
    )
    body = f"""
<style>
  /* The one responsive rule this page DEPENDS on, defined locally.
   *
   * Bootstrap arrives from a CDN, and the Ops page shell already repeats
   * `.d-none` for exactly that reason: on a restricted network the pinned
   * bundle is blocked and every `d-*-table-cell` becomes a no-op, which would
   * hide the widest column at every width instead of only on a phone. So the
   * fold is this page's own class and its own media query. */
  .report-wide {{ display: table-cell; }}
  @media (max-width: 767.98px) {{ .report-wide {{ display: none; }} }}
</style>
<h1 class="h4 mb-1">{_esc(title)}</h1>
<p class="text-muted small mb-2">
  Время — {_esc(tz.key)}. Политика <code>{_esc(attribution.ATTRIBUTION_POLICY_VERSION)}</code>.
  <span id="report-window">Окно учёта записей берётся из самой кампании.</span>
  «Записался после рассылки» — наблюдаемый факт новой записи, а не доказательство влияния рекламы,
  визита, оплаты или использования ваучера.
</p>
<p class="small mb-3">
  Шаги, сверка, остановка и возврат оплаты — на {back}. Этот отчёт только читает.
</p>

<div class="d-flex align-items-center gap-2 flex-wrap mb-2">
  <button id="btn-refresh" class="btn btn-sm btn-outline-primary" onclick="loadReport()">Обновить</button>
  <span id="report-spinner" class="text-muted small d-none">Обновляется…</span>
  <span id="report-updated" class="text-muted small"></span>
</div>
<div id="report-error"></div>
<div id="report-stage"></div>
<div id="report-summary" class="mb-3"></div>
<div id="report-coverage" class="mb-3"></div>

<div class="row g-2 align-items-end mb-2">
  <div class="col-12 col-sm-auto">
    <label class="form-label small mb-0" for="filter-message">Состояние сообщения</label>
    <select id="filter-message" class="form-select form-select-sm" onchange="applyFilters()">
      <option value="">любое</option>
      <option value="read_confirmed">прочитано</option>
      <option value="delivered_confirmed">доставка подтверждена</option>
      <option value="provider_accepted">принято Meta</option>
      <option value="attempt_unconfirmed">нет подтверждения</option>
      <option value="not_sent">отправки не было</option>
      <option value="refunded">возврат</option>
      <option value="__error">с ошибкой</option>
    </select>
  </div>
  <div class="col-12 col-sm-auto">
    <label class="form-label small mb-0" for="filter-booking">Запись после отправки</label>
    <select id="filter-booking" class="form-select form-select-sm" onchange="applyFilters()">
      <option value="">любая</option>
      <option value="booking_found">записались</option>
      <option value="booking_not_observed">записей не обнаружено</option>
      <option value="insufficient_data">недостаточно данных</option>
      <option value="send_not_attempted">отправки ещё не было</option>
    </select>
  </div>
  <div class="col-12 col-sm-auto">
    <span id="filter-count" class="text-muted small"></span>
  </div>
</div>

<div class="table-responsive">
  <table class="table table-sm align-middle" id="report-table">
    <thead>
      <tr>
        <th>Клиент</th>
        <th>Сообщение</th>
        <th>Запись после отправки</th>
        <th class="report-wide">Созданные записи</th>
        <th></th>
      </tr>
    </thead>
    <tbody id="report-rows"></tbody>
  </table>
</div>
<p id="report-empty" class="text-muted small d-none">Нет строк по выбранным фильтрам.</p>

<script>
const SCOPE = {scope};
{_REPORT_SCRIPT}
loadReport();
</script>
"""
    return _page(title, body)


# ---------------------------------------------------------------------------
# The page script
# ---------------------------------------------------------------------------
# Small and self-contained. Every value that came from the database is written
# with `textContent` or through `esc()`; nothing builds markup out of a name, a
# phone, a reason code or a branch name.

_REPORT_SCRIPT: Final = r"""
let REPORT = null;
let CLIENTS = {};

function esc(value) {
  return String(value === null || value === undefined ? "" : value)
    .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}

function el(id) { return document.getElementById(id); }

function query() {
  const parts = [];
  if (SCOPE.batch_id !== null && SCOPE.batch_id !== undefined) {
    parts.push("batch_id=" + encodeURIComponent(SCOPE.batch_id));
  }
  if (SCOPE.preview_run_id !== null && SCOPE.preview_run_id !== undefined) {
    parts.push("preview_run_id=" + encodeURIComponent(SCOPE.preview_run_id));
  }
  return parts.join("&");
}

const MESSAGE_LABELS = {
  "not_sent": "отправки не было",
  "attempt_unconfirmed": "попытка без подтверждения",
  "provider_accepted": "принято Meta",
  "delivered_confirmed": "доставка подтверждена",
  "read_confirmed": "прочитано",
  "refunded": "оплата возвращена"
};

const BOOKING_LABELS = {
  "booking_found": "записался",
  "booking_not_observed": "записей не обнаружено",
  "insufficient_data": "недостаточно данных",
  "send_not_attempted": "отправки ещё не было"
};

const BOOKING_STATE_LABELS = {
  "active": "активна",
  "cancelled": "отменена",
  "completed": "завершена",
  "unknown": "состояние неизвестно"
};

const REASON_LABELS = {
  "send_attempt_unconfirmed": "нет подтверждения принятия той же отправки",
  "identity_numeric_missing": "нет доказанного numeric ID получателя",
  "identity_conflict": "конфликт identity получателя",
  "easyweek_location_registry_unavailable": "конфигурация филиалов EasyWeek недоступна",
  "attribution_window_unusable": "у кампании нет пригодного окна учёта",
  "lifecycle_order_unproven": "порядок событий не доказан",
  "voucher_production_batch_unknown": "рассылка не найдена",
  "truncated_payload": "обрезанный payload",
  "invalid_payload": "непригодный payload",
  "foreign_location": "локация вне настроенного workspace",
  "location_identity_mismatch": "несовпадение идентификатора локации",
  "booking_identity_unusable": "нет пригодного идентификатора записи",
  "customer_identity_unusable": "нет пригодного идентификатора клиента",
  "booking_created_at_unusable": "нет корректного времени создания записи",
  "conflicting_creation_events": "противоречивые события создания записи"
};

function label(map, code) {
  return map[code] || code || "—";
}

function setSpinner(on) {
  const spinner = el("report-spinner");
  if (spinner) spinner.classList.toggle("d-none", !on);
  const button = el("btn-refresh");
  if (button) button.disabled = on;
}

function setError(text) {
  const area = el("report-error");
  if (!area) return;
  area.innerHTML = text ? '<div class="alert alert-danger py-2 small">' + esc(text) + "</div>" : "";
}

/* The report is fetched, then the names — in that order, so a failing name read
   never hides the numbers. A row with no name yet still shows its preview row. */
async function loadReport() {
  setSpinner(true);
  setError("");
  try {
    const answer = await fetch("/ops/voucher-mailings/api/booking-report?" + query(), {
      headers: {"Accept": "application/json"}
    });
    const data = await answer.json();
    if (!answer.ok || !data.report_known) {
      const reasons = (data.reasons || []).map(function (code) { return label(REASON_LABELS, code); });
      setError("Отчёт не прочитан. " + (reasons.length ? reasons.join("; ") : "Повторите обновление."));
      setSpinner(false);
      return;
    }
    REPORT = data;
    if (data.batch_id !== null && data.batch_id !== undefined) {
      try {
        const names = await fetch("/ops/voucher-mailings/api/booking-report/clients?batch_id=" +
                                  encodeURIComponent(data.batch_id), {headers: {"Accept": "application/json"}});
        if (names.ok) {
          const body = await names.json();
          CLIENTS = {};
          (body.clients || []).forEach(function (entry) { CLIENTS[entry.slot] = entry; });
        }
      } catch (err) {
        /* The report itself was read. Say that the names were not, and render
           the rest: a row without a name still shows its preview row. */
        setError("Имена получателей не прочитаны, остальное актуально.");
      }
    }
    render();
    const updated = el("report-updated");
    if (updated) {
      updated.textContent = "Обновлено: " + (data.generated_at_local || "") +
                            " (" + (data.local_timezone || "") + ")";
    }
  } catch (err) {
    setError("Отчёт не прочитан: соединение потеряно. Нажмите «Обновить».");
  }
  setSpinner(false);
}

function card(title, value, note) {
  return '<div class="col-6 col-md-4 col-xl-2"><div class="border rounded p-2 h-100">' +
    '<div class="small text-muted">' + esc(title) + "</div>" +
    '<div class="fs-5">' + esc(value) + "</div>" +
    (note ? '<div class="small text-muted">' + esc(note) + "</div>" : "") +
    "</div></div>";
}

/* The window belongs to the campaign, so the page states the number it was
   actually judged against — including when there is none. */
function renderWindow() {
  const area = el("report-window");
  if (!area || !REPORT) return;
  if (REPORT.attribution_window_days) {
    area.textContent = "Окно учёта записей: " + REPORT.attribution_window_days +
      " дн. от подтверждённой отправки (из параметров кампании).";
  } else {
    area.textContent = "Окно учёта записей не определено: у кампании нет пригодного значения, " +
      "записи не учитываются.";
  }
}

function renderStage() {
  const stage = el("report-stage");
  if (!stage || !REPORT) return;
  if ((REPORT.reasons || []).indexOf("voucher_production_batch_unknown") >= 0) {
    /* An id that names nothing is not an empty mailing, and must not be drawn
       as one. */
    stage.innerHTML = '<div class="alert alert-danger py-2 small">' +
      esc("Рассылка с таким номером не найдена.") + "</div>";
    return;
  }
  if (!REPORT.batch_exists) {
    stage.innerHTML = '<div class="alert alert-secondary py-2 small">' +
      "Рассылка ещё не зафиксирована: партии нет и отправки не было. " +
      "Активных получателей в preview: " + esc(REPORT.preview_active_count) + "." + "</div>";
    return;
  }
  if (!REPORT.any_send_confirmed) {
    stage.innerHTML = '<div class="alert alert-secondary py-2 small">' +
      "Список зафиксирован, подтверждённой отправки пока нет. " +
      "Записи не учитываются, пока отправка не подтверждена." + "</div>";
    return;
  }
  stage.innerHTML = "";
}

function renderSummary() {
  const area = el("report-summary");
  if (!area || !REPORT) return;
  const s = REPORT.summary || {};
  area.innerHTML = '<div class="row g-2">' +
    card("Получателей", s.recipients,
         REPORT.batch_exists ? "зафиксировано" : "активных в preview, список не зафиксирован") +
    card("Принято Meta", s.provider_accepted, null) +
    card("Доставлено", s.delivered_confirmed, null) +
    card("Прочитано", s.read_confirmed, null) +
    card("Записались после отправки", s.booked_after_send, null) +
    card("Ошибки и нехватка данных", s.attention, null) +
    "</div>" +
    '<p class="small text-muted mt-2 mb-0">' +
    esc("Принятие, доставка и прочтение пересекаются: прочитанное сообщение также доставлено и принято. " +
        "Это не взаимоисключающие части общего количества. Отсутствие callback о прочтении не доказывает, " +
        "что клиент не прочитал сообщение.") + "</p>";
}

function renderCoverage() {
  const area = el("report-coverage");
  if (!area || !REPORT) return;
  const c = REPORT.coverage || {};
  const branches = (c.branches || []).map(function (branch) {
    return esc(branch.name) + ": " + esc(branch.bookings_observed);
  }).join(" · ");
  const rejected = Object.keys(c.rejected || {}).map(function (code) {
    return "<li>" + esc(label(REASON_LABELS, code)) + " — " + esc(c.rejected[code]) + "</li>";
  }).join("");
  let warning = "";
  if (!c.registry_ready) {
    warning = '<div class="alert alert-warning py-2 small mb-2">' +
      esc("Конфигурация филиалов EasyWeek недоступна: охват не доказан, записи не учитываются.") + "</div>";
  } else if (c.scan_truncated) {
    warning = '<div class="alert alert-warning py-2 small mb-2">' +
      esc("Просмотр доставок ограничен: данные могут быть неполными.") + "</div>";
  }
  area.innerHTML = warning +
    '<div class="border rounded p-2 small">' +
    "<div><b>Охват филиалов</b> (наблюдаемые новые записи за просмотренный период): " +
    (branches || esc("филиалы не настроены")) + "</div>" +
    "<div class='text-muted'>Просмотрено доставок: " + esc(c.scanned_events) +
    (c.scan_from_local ? ", с " + esc(c.scan_from_local) + " по " + esc(c.scan_to_local) : "") + ".</div>" +
    "<div class='text-muted'>Ноль по филиалу означает «за этот период ничего не наблюдалось», " +
    "а не доказанное отсутствие записей.</div>" +
    (rejected ? "<details class='mt-1'><summary>Почему доставки не учтены</summary><ul class='mb-0'>" +
                rejected + "</ul></details>" : "") +
    "</div>";
}

/* The earliest CREATED booking of the window, labelled as a creation.
   Deliberately not "the nearest appointment": the report has no proven
   appointment start, so promising one would be a date the data does not
   support. The branch, the current state and how many other bookings there
   are stay on the line. */
function bookingSummary(row) {
  if (!row.bookings || !row.bookings.length) return "—";
  const first = row.bookings[0];
  let text = "Создана: " + (first.booking_created_at_local || "") + " · " + first.branch_name +
             " · " + label(BOOKING_STATE_LABELS, first.state);
  if (row.bookings.length > 1) text += " · ещё записей: " + (row.bookings.length - 1);
  return text;
}

function detailLines(row) {
  const lines = [];
  lines.push(["Слот", row.slot]);
  lines.push(["Строка preview", row.campaign_recipient_id]);
  lines.push(["Основание", row.recipient_basis]);
  lines.push(["Состояние позиции", row.item_status + (row.reason_code ? " / " + row.reason_code : "")]);
  lines.push(["Попыток отправки", row.send_attempt_count]);
  lines.push(["Начало окна (попытка отправки)", row.send_attempted_at_local || "—"]);
  lines.push(["Подтверждения отправки", (row.send_evidence || []).join(", ") || "—"]);
  lines.push(["Окно учёта", row.window_start_local ? row.window_start_local + " — " + row.window_end_local : "—"]);
  lines.push(["Identity", label(REASON_LABELS, row.identity_state) +
                         ((row.identity_sources || []).length ? " (" + row.identity_sources.join(", ") + ")" : "")]);
  lines.push(["Карточки в филиалах", (row.identity_card_branches || []).join(", ") || "—"]);
  lines.push(["Нужна сверка", row.reconciliation_required ? "да" : "нет"]);
  lines.push(["Нужно закрыть draft", row.manual_cleanup_required ? "да" : "нет"]);
  if ((row.reasons || []).length) {
    lines.push(["Причины", row.reasons.map(function (code) { return label(REASON_LABELS, code); }).join("; ")]);
  }
  let html = '<dl class="row small mb-0">';
  lines.forEach(function (pair) {
    html += '<dt class="col-sm-4">' + esc(pair[0]) + "</dt>" +
            '<dd class="col-sm-8">' + esc(pair[1]) + "</dd>";
  });
  html += "</dl>";
  if ((row.bookings || []).length) {
    html += '<div class="small mt-1"><b>Созданные записи в окне</b><ul class="mb-0">' +
      row.bookings.map(function (booking) {
        return "<li>" + esc("создана ") + esc(booking.booking_created_at_local) +
               ", " + esc(booking.branch_name) +
               " — " + esc(label(BOOKING_STATE_LABELS, booking.state)) +
               (booking.state_reason ? " " + esc("(" + label(REASON_LABELS, booking.state_reason) + ")") : "") +
               (booking.customer_reassigned
                 ? " " + esc("(другая доставка этой записи называет другого клиента)") : "") + "</li>";
      }).join("") + "</ul></div>";
  }
  html += '<p class="small text-muted mb-0 mt-1">' +
    esc("Показана дата СОЗДАНИЯ записи, а не дата визита: подтверждённого времени приёма отчёт не имеет. " +
        "Время доставки и прочтения — момент регистрации подтверждения, а не точное время действия клиента. " +
        "Один факт «создал запись» не означает, что клиент посетил салон.") + "</p>";
  return html;
}

function render() {
  renderWindow();
  renderStage();
  renderSummary();
  renderCoverage();
  const body = el("report-rows");
  if (!body || !REPORT) return;
  body.innerHTML = "";
  const names = {};
  (REPORT.rows || []).forEach(function (row) {
    const entry = CLIENTS[row.slot];
    const name = entry ? entry.display_name : "строка preview " + row.campaign_recipient_id;
    names[name] = (names[name] || 0) + 1;
  });

  (REPORT.rows || []).forEach(function (row) {
    const entry = CLIENTS[row.slot];
    const name = entry ? entry.display_name : "строка preview " + row.campaign_recipient_id;
    const main = document.createElement("tr");
    main.setAttribute("data-slot", String(row.slot));
    main.setAttribute("data-message", row.execution_error ? "__error" : row.message_state);
    main.setAttribute("data-booking", row.attribution_state);

    const client = document.createElement("td");
    const nameLine = document.createElement("div");
    nameLine.textContent = name;
    client.appendChild(nameLine);
    /* The phone is a second line only when it is needed to tell two rows apart. */
    if (entry && entry.phone_e164 && names[name] > 1) {
      const phoneLine = document.createElement("div");
      phoneLine.className = "small text-muted";
      phoneLine.textContent = entry.phone_e164;
      client.appendChild(phoneLine);
    }
    main.appendChild(client);

    const message = document.createElement("td");
    message.textContent = label(MESSAGE_LABELS, row.message_state);
    if (row.execution_error) {
      const badge = document.createElement("span");
      badge.className = "badge bg-danger ms-1";
      badge.textContent = "ошибка";
      message.appendChild(badge);
    }
    main.appendChild(message);

    const booking = document.createElement("td");
    booking.textContent = label(BOOKING_LABELS, row.attribution_state);
    if (row.booking_count > 1) {
      const count = document.createElement("span");
      count.className = "small text-muted ms-1";
      count.textContent = "(" + row.booking_count + ")";
      booking.appendChild(count);
    }
    main.appendChild(booking);

    const nearest = document.createElement("td");
    nearest.className = "report-wide";
    nearest.textContent = bookingSummary(row);
    main.appendChild(nearest);

    const toggle = document.createElement("td");
    const button = document.createElement("button");
    button.className = "btn btn-sm btn-outline-secondary";
    button.textContent = "Подробности";
    button.setAttribute("data-details-toggle", String(row.slot));
    button.onclick = function () { toggleDetails(row.slot); };
    toggle.appendChild(button);
    main.appendChild(toggle);

    body.appendChild(main);

    const details = document.createElement("tr");
    details.className = "d-none";
    details.setAttribute("data-details-for", String(row.slot));
    const cell = document.createElement("td");
    cell.colSpan = 5;
    cell.innerHTML = detailLines(row);
    details.appendChild(cell);
    body.appendChild(details);
  });
  applyFilters();
}

function toggleDetails(slot) {
  const row = document.querySelector('[data-details-for="' + slot + '"]');
  if (!row) return;
  const hidden = row.classList.contains("d-none");
  /* A hidden details row must stay hidden when its main row is filtered out. */
  row.classList.toggle("d-none", !hidden);
  row.setAttribute("data-open", hidden ? "1" : "0");
}

function applyFilters() {
  const message = el("filter-message") ? el("filter-message").value : "";
  const booking = el("filter-booking") ? el("filter-booking").value : "";
  let shown = 0;
  document.querySelectorAll("#report-rows tr[data-slot]").forEach(function (row) {
    const keep = (!message || row.getAttribute("data-message") === message) &&
                 (!booking || row.getAttribute("data-booking") === booking);
    row.classList.toggle("d-none", !keep);
    const details = document.querySelector('[data-details-for="' + row.getAttribute("data-slot") + '"]');
    if (details) details.classList.toggle("d-none", !keep || details.getAttribute("data-open") !== "1");
    if (keep) shown += 1;
  });
  const counter = el("filter-count");
  if (counter) counter.textContent = "Показано строк: " + shown;
  const empty = el("report-empty");
  if (empty) empty.classList.toggle("d-none", shown > 0 || !REPORT || !(REPORT.rows || []).length);
}
"""


__all__ = ["router"]
