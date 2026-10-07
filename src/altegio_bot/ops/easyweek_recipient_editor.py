"""The explicit, read-then-confirm manual audience editor (§44)."""

from __future__ import annotations

import json


def manual_batch_editor(*, run_id: int, csrf: str) -> str:
    return _EDITOR.replace("__RUN_ID__", str(run_id)).replace("__CSRF__", json.dumps(csrf))


_EDITOR = r"""
<div id="manual-batch-editor" class="card mb-3">
  <div class="card-header">Добавить список: предыдущий визит в Altegio</div>
  <div class="card-body">
    <p>Ручное дополнение к указанному периоду кампании. Решение оператора не доказывает первый визит.
      API проверяет существующую карточку и <b>ровно ноль записей во всей истории EasyWeek</b>:
      будущая, отменённая или завершённая запись исключает контакт из этого режима.</p>
    <label for="bulk-phones" class="form-label">Телефоны, по одному на строку</label>
    <textarea id="bulk-phones" class="form-control" rows="6" maxlength="16384"
      placeholder="+49…" autocomplete="off" spellcheck="false"></textarea>
    <div class="form-text">До 100 разных телефонов и 200 строк. Повторения проверяются один раз.</div>
    <div class="form-check mt-2">
      <input id="bulk-altegio" type="checkbox" class="form-check-input">
      <label for="bulk-altegio" class="form-check-label">Подтверждаю предыдущий визит этих клиентов в Altegio.
        Это моё заявление, а не факт, проверенный API EasyWeek.</label>
    </div>
    <div class="form-check mt-2">
      <input id="bulk-karlsruhe" type="checkbox" class="form-check-input">
      <label for="bulk-karlsruhe" class="form-check-label">Назначаю этих клиентов в Karlsruhe для этой рассылки.
        Карточка workspace сама по себе не доказывает филиал;
        конфликты другого филиала будут отклонены.</label>
    </div>
    <p class="small text-muted mt-2">Отсутствие истории EasyWeek
      не доказывает отсутствие прежних подарков в Altegio.</p>
    <button id="bulk-check" class="btn btn-outline-primary" type="button">Проверить список</button>
    <div id="bulk-status" class="mt-2" role="status"></div>
    <div id="bulk-results" class="table-responsive mt-2"></div>
    <div id="bulk-confirm-panel" class="alert alert-warning mt-2 d-none">
      <p id="bulk-confirm-summary"></p>
      <label><input id="bulk-subset-confirmed" type="checkbox"> Подтверждаю добавление именно строк «Можно добавить»
        из показанного списка. Отклонённые и уже присутствующие строки добавлены не будут.</label>
      <button id="bulk-confirm" class="btn btn-primary d-block mt-2" type="button" disabled>
        Добавить проверенный состав
      </button>
    </div>
  </div>
</div>
<script>
(() => {
  const runId = __RUN_ID__;
  const csrf = __CSRF__;
  const el = id => document.getElementById(id);
  let plan = null;
  let generation = 0;
  let busy = false;
  const labels = {
    addable: "Можно добавить", already_present: "Уже присутствует",
    rejected: "Не будет добавлен", duplicate: "Повтор строки",
    manual_recipient_phone_unusable: "Телефон не распознан: укажите международный формат",
    manual_recipient_customer_absent: "Отсутствует во внешнем EasyWeek",
    manual_recipient_customer_ambiguous: "Неоднозначный телефон / customer",
    manual_recipient_customer_unproven: "Identity не удалось доказать через EasyWeek",
    manual_recipient_customer_name_missing: "Не хватает имени для локальной карточки и шаблона",
    manual_recipient_local_client_ambiguous: "Неоднозначная локальная identity",
    manual_recipient_branch_assignment_required: "Для локальной identity требуется явное назначение Karlsruhe",
    manual_batch_timeout: "Лимит времени проверки истёк: список не подтверждён",
    manual_batch_duplicate: "Дубликат после нормализации телефона",
    manual_recipient_identity_conflict: "Конфликт identity / телефона",
    manual_recipient_branch_conflict: "Конфликт филиала / identity",
    manual_recipient_opted_out: "Отказ от рассылок",
    manual_recipient_preview_frozen: "Состав зафиксирован: редактирование закрыто",
    manual_recipient_run_not_editable: "Preview больше нельзя редактировать",
    manual_recipient_history_nonempty: "История EasyWeek не пустая",
    manual_recipient_history_unproven: "Историю EasyWeek не удалось доказать",
    manual_batch_input_limit: "Превышен лимит списка: 100 телефонов, 200 строк, 16 КиБ",
    manual_batch_rate_limit: "Предыдущая проверка ещё действует. Дождитесь разрешённого интервала",
    manual_batch_input_invalid: "Список слишком большой или содержит некорректные поля",
    manual_batch_provider_unavailable: "EasyWeek не ответил: проверка не подтверждена",
    manual_batch_database_unavailable: "База недоступна: результат не подтверждён",
    manual_batch_plan_expired: "Проверка истекла: проверьте список заново",
    manual_batch_plan_changed: "Предпосылки изменились: проверьте список заново",
    manual_batch_plan_invalid: "Подтверждение не соответствует проверке: проверьте список заново",
    manual_batch_attestation_required: "Подтвердите предыдущий визит в Altegio и назначение Karlsruhe",
  };
  function reason(code) { return labels[code] || code || "Проверка не подтверждена"; }
  function status(text) { el("bulk-status").textContent = text; }
  function invalidate() {
    generation += 1;
    plan = null;
    el("bulk-confirm-panel").classList.add("d-none");
    el("bulk-subset-confirmed").checked = false;
    el("bulk-confirm").disabled = true;
    el("bulk-results").replaceChildren();
    status("Список или подтверждение изменены. Нужна новая проверка.");
  }
  for (const id of ["bulk-phones", "bulk-altegio", "bulk-karlsruhe"]) el(id).addEventListener("input", invalidate);
  el("bulk-subset-confirmed").addEventListener("change", () => {
    el("bulk-confirm").disabled = busy || !plan || !el("bulk-subset-confirmed").checked;
  });
  function lock(value) {
    busy = value;
    for (const id of ["bulk-phones", "bulk-altegio", "bulk-karlsruhe", "bulk-check", "bulk-subset-confirmed"])
      el(id).disabled = value;
    el("bulk-confirm").disabled = value || !plan || !el("bulk-subset-confirmed").checked;
  }
  async function post(action, body) {
    const response = await fetch("/ops/voucher-mailings/api/recipients/" + action, {
      method: "POST", credentials: "same-origin",
      headers: {"Content-Type": "application/json", "X-Ops-CSRF": csrf}, body: JSON.stringify(body)
    });
    const data = await response.json();
    if (!response.ok || !data.ok) throw new Error(reason(data.reason || (data.reasons || [])[0]));
    return data;
  }
  function renderRows(rows, lines) {
    const table = document.createElement("table");
    table.id = "bulk-result-table";
    table.className = "table table-sm";
    const header = table.createTHead().insertRow();
    for (const title of ["Строка", "Введённый телефон", "Результат", "Причина"]) {
      const th = document.createElement("th"); th.textContent = title; header.append(th);
    }
    const body = table.createTBody();
    for (const result of rows) {
      const row = body.insertRow();
      row.dataset.status = result.status;
      // The source line disambiguates masked numbers; it never leaves this browser again.
      for (const value of [result.line, lines[Number(result.line) - 1] || result.phone,
                          reason(result.status), result.reason ? reason(result.reason) : "—"])
        row.insertCell().textContent = String(value || "—");
    }
    el("bulk-results").replaceChildren(table);
  }
  el("bulk-check").addEventListener("click", async () => {
    if (busy) return;
    invalidate();
    const current = generation;
    const phones = el("bulk-phones").value;
    if (!el("bulk-altegio").checked || !el("bulk-karlsruhe").checked) {
      status(reason("manual_batch_attestation_required")); return;
    }
    lock(true);
    status("Проверяем EasyWeek. Состав preview пока не меняется…");
    try {
      const data = await post("check", {preview_run_id: runId, phones,
        prior_altegio_visit_confirmed: true, assign_karlsruhe_confirmed: true});
      if (current !== generation) return;
      renderRows(data.rows || [], phones.split(/\r?\n/));
      status("Проверка завершена. Состав preview и локальные карточки не изменены.");
      if (data.plan_id && Number(data.eligible_count) > 0) {
        plan = {id: data.plan_id, count: Number(data.eligible_count)};
        el("bulk-confirm-summary").textContent = "Будут добавлены: " + plan.count +
          ". Уже присутствуют: " + Number(data.already_present_count || 0) +
          ". Отклонены: " + Number(data.rejected_count || 0) +
          ". Повторы: " + Number(data.duplicate_count || 0) +
          ". Проверка действует до " + String(data.expires_at || "—") +
          ". Перед добавлением сервер повторит проверку. Ваучеры и сообщения не создаются.";
        el("bulk-confirm-panel").classList.remove("d-none");
      }
    } catch (error) { status(String(error.message || "Проверка не подтверждена")); }
    finally { lock(false); }
  });
  el("bulk-confirm").addEventListener("click", async () => {
    if (busy || !plan || !el("bulk-subset-confirmed").checked) return;
    const selected = plan;
    lock(true);
    status("Повторная проверка и добавление подтверждённого состава…");
    try {
      const data = await post("confirm",
        {preview_run_id: runId, plan_id: selected.id, confirmed_count: selected.count});
      plan = null;
      el("bulk-confirm-panel").classList.add("d-none");
      status("Состав добавлен: " + Number(data.added_count || 0) +
        ". Без изменений: " + Number(data.unchanged_count || 0) + ". Обновите страницу для новых счётчиков.");
      const link = document.createElement("a"); link.href = "/ops/campaigns/" + runId;
      link.textContent = " Обновить preview"; el("bulk-status").append(link);
    } catch (error) {
      plan = null;
      el("bulk-confirm-panel").classList.add("d-none");
      status(String(error.message || "Результат не подтверждён") +
        ". Обновите preview и проверьте список заново; обновление страницы само ничего не добавляет.");
    } finally { lock(false); }
  });
})();
</script>
"""
