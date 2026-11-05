"""One screen: the follow-up PREVIEW for one original voucher mailing (§46.6).

What an operator does here
--------------------------
Open an original mailing's follow-up page, tick which of the three groups the
repeat message would be for, type how long must have passed since the first
attempt, read the exact text that would be sent, and press one button to check
the composition. They get back how many people are eligible, who is excluded and
who could not be established, each with a reason in words.

What is deliberately NOT here
-----------------------------
There is no send. No confirmation of a send, no endpoint that could become one,
no queue, no job, no stage. This page offers a proposal and the page says so in
its own words, in a banner the operator cannot miss. The executor, its one-shot
claim and its tombstones are PR-22.3, and nothing on this screen reaches the
existing dispatch, the Outbox or the old follow-up worker: the only server-side
action available is "compute and store a preview", which sends nothing.

Where the authorisation is
--------------------------
Exactly where the rest of the section's is, reusing it rather than repeating it:
the router carries ``require_ops_auth``, and the one POST resolves the operator
from the session cookie, requires this session's CSRF token in a header and
requires a same-origin request. There is no second authorisation system, no API
key and no shared secret. The GET renders and reads; it creates no operation and
changes nothing.

What is never on this page
--------------------------
A voucher code. A phone number, whole or partial. A customer name, a customer
uuid, an order uuid, a raw Meta identifier, a digest the operator would have to
copy, a batch id they would have to type, a key or any value of a secret. The
message is shown with square-bracket placeholders, and every reason is a label.

Two sentences the page is required to say
-----------------------------------------
That the absence of a read receipt does not prove nobody read the first message
— a device can be configured never to send one — so the second group is
"delivered, read not confirmed" and never "did not read". And that an empty
audience is a correct result: §46.6 forbids treating zero as a reason to loosen
anything.
"""

from __future__ import annotations

from typing import Any, Final

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.exc import SQLAlchemyError

from altegio_bot.campaigns.easyweek_voucher_followup import followup_template_contract as message
from altegio_bot.campaigns.easyweek_voucher_followup import preview as preview_module
from altegio_bot.campaigns.easyweek_voucher_followup.failure_policy import CURRENT_FAILURE_POLICY
from altegio_bot.campaigns.easyweek_voucher_followup.identity import (
    FOLLOWUP_GROUP_DELIVERED,
    FOLLOWUP_GROUP_FAILED,
    FOLLOWUP_GROUP_READ,
    FOLLOWUP_ROW_EXCLUDED,
    FOLLOWUP_ROW_INCLUDED,
    FOLLOWUP_ROW_UNKNOWN,
    canonical_groups,
)
from altegio_bot.campaigns.easyweek_voucher_production.identity import (
    DATABASE_UNAVAILABLE,
    OPS_CSRF_INVALID,
    OPS_ORIGIN_REJECTED,
    OPS_SESSION_REQUIRED,
)
from altegio_bot.db import SessionLocal
from altegio_bot.easyweek_client import EasyWeekClient, EasyWeekConfigError
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

router = APIRouter(
    prefix="/ops/voucher-mailings",
    dependencies=[Depends(require_ops_auth)],
    tags=["voucher-followup-preview"],
)

# This page names counts and reasons about real people. One authenticated
# operator, one moment; never a shared cache or a back/forward store.
_PRIVATE_HEADERS: Final = {"Cache-Control": "no-store, no-cache, must-revalidate, private", "Pragma": "no-cache"}

# The longest interval the form accepts, in days. A ceiling rather than a
# default: there is no default, and §46.3 forbids inventing one. The bound only
# keeps a typo from becoming a year.
MAX_INTERVAL_DAYS: Final = 365

_GROUP_LABELS: Final[dict[str, str]] = {
    FOLLOWUP_GROUP_READ: "Прочитано (подтверждено)",
    FOLLOWUP_GROUP_DELIVERED: "Доставлено, прочтение не подтверждено",
    FOLLOWUP_GROUP_FAILED: "Подтверждённая ошибка отправки или доставки",
}

_GROUP_HINTS: Final[dict[str, str]] = {
    FOLLOWUP_GROUP_READ: "Пришла отметка о прочтении именно этого сообщения.",
    FOLLOWUP_GROUP_DELIVERED: (
        "Пришла отметка о доставке, отметки о прочтении нет. Это НЕ значит, что человек не прочитал: "
        "устройство может не отправлять отметку о прочтении вообще."
    ),
    FOLLOWUP_GROUP_FAILED: (
        "Есть подтверждённый отказ Meta или подтверждённый сбой доставки по этому же сообщению. "
        "Повторная отправка такой группе разрешается только доказанной политикой допустимости причин."
    ),
}

# What every refusal code says to a person. A code with no sentence here is
# printed as itself rather than guessed at, which is why this mapping may be
# incomplete without becoming wrong.
_REASON_LABELS: Final[dict[str, str]] = {
    "followup_batch_not_found": "Такой рассылки нет.",
    "followup_batch_schema_unsupported": (
        "Это не рассылка фиксированного продукта 10 €. Исторические рассылки 15 € сюда не переносятся."
    ),
    "followup_batch_scope_unsupported": (
        "Эта партия собрана не по заработанным визитам: ручная подборка, канарейка, снимок или кампания "
        "другого провайдера. Повторное сообщение по ней не готовится."
    ),
    "followup_batch_not_frozen": "У партии нет зафиксированного состава.",
    "followup_batch_terminal_stop": (
        "Рассылка остановлена оператором. Для этого контракта остановка окончательная, "
        "и предпросмотр повторного сообщения по ней не создаётся."
    ),
    "followup_groups_required": "Не выбрана ни одна группа. Пустой выбор — это не «все группы».",
    "followup_interval_required": "Не указан минимальный интервал. Значения по умолчанию здесь нет.",
    "followup_template_unproven": (
        "Текст повторного сообщения не подтверждён: нужен отдельный шаблон Meta со статусом APPROVED "
        "и локальная запись о нём. Приложение не создаёт и не меняет шаблоны Meta и не отправляет "
        "свободный текст вместо шаблона."
    ),
    "followup_sender_unproven": "Отправитель исходной рассылки не подтверждён.",
    "followup_source_attempt_unproven": (
        "По этому адресату нет доказанной исходной попытки с зафиксированным маршрутом."
    ),
    "followup_interval_not_elapsed": "С момента исходной попытки прошло меньше указанного интервала.",
    "followup_evidence_unproven": (
        "Нет доказательств ни прочтения, ни доставки, ни подтверждённой ошибки: принято провайдером и тишина, "
        "таймаут, неизвестный результат, локальный отказ или попытки не было. Это не «не прочитано»."
    ),
    "followup_failure_policy_unproven": (
        "Для подтверждённых ошибок нет доказанной политики допустимости причин и срока ожидания, "
        "поэтому эта группа сейчас не даёт права на повторное сообщение."
    ),
    "followup_consent_refused": "Человек отказался от сообщений (STOP/СТОП).",
    "followup_consent_pending_inbound": (
        "Есть принятое, но ещё не обработанное входящее сообщение от этого человека. "
        "Пока оно не разобрано, отправка не разрешается."
    ),
    "followup_consent_unknown": "Согласие установить не удалось. Неизвестность не означает разрешение.",
    "followup_customer_replied": "Человек уже ответил после исходной отправки.",
    "followup_new_booking_after_attempt": "После исходной отправки появилась новая запись (в том числе отменённая).",
    "followup_future_booking_active": "У человека есть активная запись в будущем.",
    "followup_history_unproven": (
        "Историю записей подтвердить не удалось: таймаут, неполная постраничная выдача или несовпадение личности. "
        "Это не «записей нет»."
    ),
    "followup_identity_conflict": (
        "Личность адресата не совпадает с исходной: сменился номер, карточка или связь с визитом."
    ),
    "followup_entitlement_invalid": "Исходный ваучер возвращён, закрыт вручную или его состояние не урегулировано.",
    "followup_entitlement_unproven": "Привязку исходного ваучера подтвердить не удалось.",
    "followup_duplicate_source": "Этот же человек или этот же ваучер уже включены в состав по другой позиции.",
    "followup_historical_stop_unaccounted": (
        "В сохранённых событиях есть отписка этого человека, которая ещё не учтена. "
        "До завершения исторической сверки такой адресат не считается доступным."
    ),
    "followup_historical_consent_unproven": (
        "Проверку сохранённых событий на неучтённые отписки завершить не удалось, "
        "поэтому историческое состояние согласия не установлено ни для кого."
    ),
    "followup_status_evidence_unproven": (
        "Статус доставки или прочтения не подтверждён достаточным сохранённым доказательством: "
        "нет проверенной подлинности обратного вызова либо не совпадает маршрут исходной попытки. "
        "Запись о рассылке при этом не меняется."
    ),
    "followup_booking_creation_unproven": (
        "Есть запись, дата визита которой позже исходной отправки, а дату её создания подтвердить не удалось. "
        "Без даты создания правило «новая запись после рассылки» применить нельзя."
    ),
    "followup_booking_capture_unproven": (
        "Даты создания записей для этого человека не установлены: не удалось связать его "
        "с числовым идентификатором EasyWeek, не настроены филиалы, либо просмотр событий не завершён."
    ),
    "followup_entitlement_unreadable": (
        "Исходный заказ прочитать не удалось: таймаут, нечитаемый ответ или другой заказ. "
        "Это не означает, что ваучер действителен."
    ),
    "followup_entitlement_settlement_unproven": (
        "Исходный заказ прочитан, но его оплата не доказана: он не в оплаченном состоянии либо его суммы "
        "противоречат финансовому контракту своей схемы. Наличие кода и совпавший отпечаток этого не заменяют."
    ),
    "followup_group_moved_out_of_selection": (
        "Пока выполнялись внешние проверки, этот адресат перешёл в группу, которая не была выбрана. "
        "В состав он не включается, и его место никем не занимается."
    ),
}


class PreviewRequest(BaseModel):
    """Everything a browser may choose. Four fields, and nothing else.

    ``extra="forbid"`` is the point of the model: a request that tries to name a
    recipient list, a digest, a composition, a template, an operator or a batch
    other than the one in the path is refused by name rather than ignored.
    """

    model_config = ConfigDict(extra="forbid")

    groups: list[str] = Field(min_length=1, max_length=3)
    min_interval_days: int = Field(gt=0, le=MAX_INTERVAL_DAYS)


def _session_error(exc: OpsSessionError) -> JSONResponse:
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
            "detail": exc.reason,
        },
    )


def _principal_or_error(request: Request) -> tuple[str | None, JSONResponse | None]:
    """The authenticated operator, or the response that refuses this request.

    The same three checks the rest of the section makes, in the same order and
    from the same module: same origin, then the session, then the CSRF token.
    An unauthenticated caller therefore learns nothing about origins or tokens.
    """
    try:
        require_same_origin(request)
        account, fingerprint = resolve_ops_session(request)
        require_csrf(request)
    except OpsSessionError as exc:
        return None, _session_error(exc)
    return f"{account}:{fingerprint}"[:128], None


def _csrf_for(request: Request) -> str:
    token = request.cookies.get("ops_session", "")
    return csrf_token_for(token) if token else ""


def _reason_sentence(code: str | None) -> str:
    if not code:
        return ""
    return _REASON_LABELS.get(code, "")


# ---------------------------------------------------------------------------
# The one protected action
# ---------------------------------------------------------------------------


@router.post("/{batch_id}/followup/preview")
async def api_create_preview(request: Request, batch_id: int, payload: PreviewRequest) -> JSONResponse:
    """Compute and store ONE new immutable preview version.

    An explicit protected action, and still not a send: it reads the original
    mailing, performs read-only booking proofs and writes one snapshot. Nothing
    in this path spends a send attempt, queues a job, touches a §45 counter or
    reaches Meta's message API.

    Every bound comes from the server. The path names the batch; the body may
    name groups and an interval and nothing else; the composition, the bindings,
    the counts and the digest are computed here from the ledger. A caller
    replaying an old request therefore gets a NEW version computed from today's
    facts, never an old composition under a new digest.
    """
    principal, refusal = _principal_or_error(request)
    if refusal is not None:
        return refusal

    groups = canonical_groups(payload.groups)
    spec = preview_module.PreviewSpec(
        batch_id=int(batch_id),
        groups=groups,
        # Days on the wire, seconds in the contract. The operator types a unit
        # they can reason about and the stored snapshot keeps an unambiguous
        # one; neither end has a default.
        min_interval_seconds=int(payload.min_interval_days) * 24 * 60 * 60,
        operator_principal=str(principal or ""),
    )

    try:
        # A deployment with no EasyWeek credentials cannot prove anybody's
        # bookings, and §46.5 is explicit that this must become "unknown" rather
        # than an empty booking list — so the preview is computed with no reader
        # and every candidate is reported as unestablished. An exception here
        # would instead be a 500 with no reason an operator could read.
        try:
            async with EasyWeekClient() as reader:
                outcome = await preview_module.create_preview(
                    SessionLocal,
                    spec=spec,
                    booking_reader=reader,
                )
        except EasyWeekConfigError:
            outcome = await preview_module.create_preview(
                SessionLocal,
                spec=spec,
                booking_reader=None,
            )
    except SQLAlchemyError:
        return JSONResponse(
            status_code=503,
            content={"accepted": False, "reasons": [DATABASE_UNAVAILABLE]},
            headers=_PRIVATE_HEADERS,
        )

    if not outcome.created:
        return JSONResponse(
            status_code=409,
            content={
                "accepted": False,
                "reasons": [outcome.reason or "followup_preview_unavailable"],
                "detail": _reason_sentence(outcome.reason),
            },
            headers=_PRIVATE_HEADERS,
        )
    return JSONResponse(
        content={
            "accepted": True,
            "version": outcome.version,
            "included": outcome.included,
            "excluded": outcome.excluded,
            "unknown": outcome.unknown,
            "composition_digest": outcome.composition_digest,
            "checked_at": outcome.checked_at.isoformat() if outcome.checked_at else None,
            "valid_until": outcome.valid_until.isoformat() if outcome.valid_until else None,
            "booking_reads": outcome.booking_reads,
            "retryable_uncertainty": outcome.retryable_uncertainty,
            # No send endpoint exists in this phase, and the answer says so
            # rather than leaving a client to assume one.
            "sending_available": False,
        },
        headers=_PRIVATE_HEADERS,
    )


# ---------------------------------------------------------------------------
# The page
# ---------------------------------------------------------------------------


def _groups_block(selected: tuple[str, ...]) -> str:
    rows = []
    for group in (FOLLOWUP_GROUP_READ, FOLLOWUP_GROUP_DELIVERED, FOLLOWUP_GROUP_FAILED):
        checked = " checked" if group in selected else ""
        rows.append(
            "<div class='form-check mb-2'>"
            f"<input class='form-check-input followup-group' type='checkbox' value='{_esc(group)}'"
            f" id='group-{_esc(group)}'{checked}>"
            f"<label class='form-check-label' for='group-{_esc(group)}'>"
            f"<b>{_esc(_GROUP_LABELS[group])}</b>"
            f"<div class='small text-muted'>{_esc(_GROUP_HINTS[group])}</div>"
            "</label></div>"
        )
    return "".join(rows)


def _rows_table(rows: tuple[preview_module.PreviewRowView, ...]) -> str:
    if not rows:
        return "<p class='text-muted'>В сохранённом составе нет позиций.</p>"
    badges = {
        FOLLOWUP_ROW_INCLUDED: "bg-success",
        FOLLOWUP_ROW_EXCLUDED: "bg-secondary",
        FOLLOWUP_ROW_UNKNOWN: "bg-warning text-dark",
    }
    labels = {
        FOLLOWUP_ROW_INCLUDED: "включён",
        FOLLOWUP_ROW_EXCLUDED: "исключён",
        FOLLOWUP_ROW_UNKNOWN: "не установлено",
    }
    body = []
    for row in rows:
        sentence = _reason_sentence(row.reason)
        body.append(
            "<tr>"
            f"<td class='text-nowrap'>{int(row.slot)}</td>"
            f"<td class='small'>{_esc(_GROUP_LABELS.get(row.group or '', row.group or ''))}</td>"
            f"<td><span class='badge {badges.get(row.decision, 'bg-light text-dark')}'>"
            f"{_esc(labels.get(row.decision, row.decision))}</span></td>"
            f"<td class='small'>{_esc(sentence)}"
            + (f" <code>{_esc(row.reason)}</code>" if row.reason else "")
            + "</td></tr>"
        )
    return (
        "<div class='table-responsive'><table class='table table-sm align-middle'>"
        "<thead><tr><th>Позиция</th><th>Группа</th><th>Решение</th><th>Причина</th></tr></thead>"
        f"<tbody>{''.join(body)}</tbody></table></div>"
    )


_DRIFT_LABELS: Final[dict[str, str]] = {
    "template_body": "текст повторного сообщения изменился",
    "template_code": "изменился код шаблона",
    "template_name": "изменилось имя шаблона Meta",
    "template_language": "изменился язык шаблона",
    "template_category": "изменилась категория шаблона",
    "template_parameters": "изменился порядок подстановок",
    "contract_version": "изменилась версия контракта предпросмотра",
    "failure_policy": "изменилась версия политики причин ошибок",
    "source_composition": "изменился зафиксированный состав исходной рассылки",
    "source_stop_generation": "исходная рассылка была остановлена после этой проверки",
}


def _stored_block(
    stored: preview_module.StoredPreview | None,
    *,
    currency: preview_module.SnapshotCurrency | None,
) -> str:
    """The saved proposal, with its standing stated rather than implied.

    §46.6 asks the page to keep three things apart, and this block says all
    three in words: what the check found WHEN it ran, how long that snapshot is
    treated as current, and whether anything holds today. A green "проверки
    актуальны" is shown only when the snapshot is young AND the message
    contract, the policy and the source batch are still the ones it was computed
    under AND nothing blocks a follow-up right now — so a failed refresh, a
    paused template or a stop since cannot leave a stale result looking fresh.

    And the green state says exactly what that conjunction establishes, which is
    LESS than it used to claim. A GET re-proves the batch, the snapshot's own
    validity window and the message contract; it does not re-read anybody's
    consent, replies, artifact or bookings, because that is an external
    recalculation and §46.6 makes it an explicit operator action rather than a
    side effect of opening a page. So nothing here says the recipients or their
    number are confirmed now: every stored figure is printed with the moment it
    was computed, and the sentence beside them names the three things that were
    re-checked and the ones that were not.
    """
    if stored is None:
        return (
            "<div class='alert alert-light border'>Предпросмотр ещё не сохранялся. "
            "Выберите группы и интервал, затем проверьте состав.</div>"
        )
    assert currency is not None
    checked = stored.checked_at.strftime("%d.%m.%Y %H:%M UTC")
    if currency.current:
        css, standing = (
            "alert-success",
            "Ничто не запрещает повторное сообщение по этой рассылке: сейчас заново проверены "
            "сама партия, срок действия снимка и контракт текста. Состав при этом не пересчитывался — "
            f"числа и решения ниже относятся к {checked}.",
        )
    elif not currency.fresh:
        css, standing = (
            "alert-warning",
            "Срок действия этих проверок истёк. Ниже — исторический результат; состав нужно пересчитать.",
        )
    elif currency.blocked_now is not None:
        css, standing = (
            "alert-danger",
            "Сейчас повторное сообщение по этой рассылке запрещено. Ниже — исторический результат проверки.",
        )
    elif not currency.template_proven_now:
        css, standing = (
            "alert-warning",
            "Текст повторного сообщения сейчас не подтверждён. Ниже — исторический результат проверки.",
        )
    else:
        changes = ", ".join(_DRIFT_LABELS.get(item, item) for item in currency.drift)
        css, standing = (
            "alert-warning",
            "Условия изменились после этой проверки "
            f"({changes}). Ниже — исторический результат; состав нужно пересчитать.",
        )

    groups = ", ".join(_GROUP_LABELS.get(group, group) for group in stored.selected_groups)
    days = stored.min_interval_seconds / 86400
    text_block = (
        "<div class='small mt-2'>Текст, с которым считался этот состав, совпадает с текущим.</div>"
        if currency.text_restorable
        else (
            "<div class='small mt-2'><b>Текст повторного сообщения с тех пор изменился.</b> "
            "Сохранён только его отпечаток, а не сам текст, поэтому прежний текст показать нельзя; "
            "текущий текст относится не к этому составу.</div>"
        )
    )
    standing_state = "current" if currency.current else "historical"
    return f"""
<div class="alert {css}" id="followup-stored" data-followup-standing="{standing_state}">
  <div><b>Результат проверки, версия {int(stored.version)}.</b>
    <span id="followup-standing">{_esc(standing)}</span></div>
  <div class="small mt-1">
    Группы: {_esc(groups)}.<br>
    Минимальный интервал после исходной попытки: {days:.0f} дн.<br>
    Проверено: {_esc(checked)};
    снимок считается актуальным до {_esc(stored.valid_until.strftime("%d.%m.%Y %H:%M UTC"))}.<br>
    Версия контракта: <code>{_esc(stored.contract_version)}</code>,
    политика причин ошибок: <code>{_esc(stored.failure_policy_version)}</code>.
  </div>
  {text_block}
</div>
<div class="row g-2 mb-3">
  <div class="col"><div class="card"><div class="card-body py-2">
    <div class="h4 mb-0">{int(stored.included_count)}</div>
    <div class="small text-muted">подходили для повторного сообщения<br>на {_esc(checked)}</div>
  </div></div></div>
  <div class="col"><div class="card"><div class="card-body py-2">
    <div class="h4 mb-0">{int(stored.excluded_count)}</div>
    <div class="small text-muted">исключены<br>на {_esc(checked)}</div>
  </div></div></div>
  <div class="col"><div class="card"><div class="card-body py-2">
    <div class="h4 mb-0">{int(stored.unknown_count)}</div>
    <div class="small text-muted">не удалось установить<br>на {_esc(checked)}</div>
  </div></div></div>
</div>
<div class="small text-muted mb-2">
  Это числа на {_esc(checked)}. <span id="followup-confirmation">{
        "Сейчас перепроверены только сама партия, срок снимка и контракт текста. "
        "Согласие, ответы и записи каждого получателя заново не проверялись, "
        "поэтому текущее количество получателей этим не подтверждается — "
        "чтобы узнать его, пересчитайте состав."
        if currency.current
        else "Они НЕ подтверждены заново: это историческая запись проверки, а не текущая готовность."
    }</span>
</div>
{_rows_table(stored.rows)}
"""


def _template_block(proof: message.FollowupTemplateProof) -> str:
    css = "alert-success" if proof.proven else "alert-warning"
    state = "подтверждён" if proof.proven else "не подтверждён"
    blocker = "" if proof.proven else f"<div class='small mt-1'>{_esc(_reason_sentence(proof.reason))}</div>"
    return f"""
<div class="alert {css}">
  <div><b>Текст повторного сообщения: {_esc(state)}.</b></div>
  <div class="small mt-1">
    Это отдельный шаблон, а не текст первого сообщения. Первое сообщение не изменяется.
    Повторное сообщение не содержит кода ваучера: приложение хранит только криптографическую привязку
    кода и не может его восстановить, а новый ваучер, замена кода и продление срока здесь не создаются.
  </div>
  <pre class="bg-light border rounded p-2 mt-2 mb-1 small text-wrap">{_esc(message.safe_example())}</pre>
  <div class="small text-muted">
    Это текущий утверждённый текст. В квадратных скобках — подстановки;
    реальные имена, ссылки, номера и коды на этой странице не показываются.
    Текст одинаково верен для всех трёх групп: он не утверждает, что человек получил или прочитал первое сообщение.
  </div>
  {blocker}
</div>
"""


@router.get("/{batch_id}/followup", response_class=HTMLResponse)
async def page_followup(request: Request, batch_id: int) -> HTMLResponse:
    """The follow-up preview screen. A read; it creates and changes nothing.

    Opening it runs no operation, writes no row and sends no request to Meta or
    EasyWeek. It shows which original mailing this is about, what the server
    would check, the exact text, the prerequisites that are still missing, and
    the last stored proposal if there is one.
    """
    async with SessionLocal() as session:
        gate = await preview_module.batch_gate(session, batch_id=int(batch_id))
        stored = await preview_module.latest_preview(session, batch_id=int(batch_id))
        local_template = bool(gate.company_id) and await preview_module.local_template_row_proven(
            session, company_id=int(gate.company_id or 0)
        )
    # The live Meta read happens with the session closed. This page holds no
    # lock, but the rule is the rule: nothing external runs while this phase has
    # a database handle open.
    template = (
        await preview_module.prove_meta_followup_template()
        if local_template
        else message.FollowupTemplateProof(proven=False, reason="followup_template_unproven")
    )
    # Whether the saved result may be presented as current, decided on the
    # server from the stored contract rather than from its age alone.
    currency = (
        preview_module.assess_snapshot(stored, gate=gate, template_proven_now=template.proven)
        if stored is not None
        else None
    )

    csrf = _csrf_for(request)
    selected = stored.selected_groups if stored is not None else ()
    interval_value = int(stored.min_interval_seconds // 86400) if stored is not None else ""

    gate_block = ""
    if not gate.allowed:
        gate_block = (
            "<div class='alert alert-danger'>"
            f"<b>Предпросмотр недоступен.</b> {_esc(_reason_sentence(gate.reason))} "
            f"<code>{_esc(gate.reason)}</code></div>"
        )

    policy_block = ""
    if not CURRENT_FAILURE_POLICY.admits_anything:
        policy_block = (
            "<div class='alert alert-warning'>"
            "<b>Группа подтверждённых ошибок сейчас не даёт права на отправку.</b> "
            "Доказанной политики допустимости причин и срока ожидания нет, поэтому каждая такая позиция "
            "исключается с причиной. Группу можно выбрать — результатом будет честный ноль."
            "</div>"
        )

    body = f"""
{_page_banner()}
{gate_block}
<h1 class="h4 mb-3">Повторное сообщение: предпросмотр</h1>
<div class="card mb-3"><div class="card-body py-2 small">
  <b>Исходная рассылка №{int(batch_id)}.</b>
  Состояние: {_esc(gate.status or "—")}.
  Состав: {int(gate.recipient_count)} позиций.
  Схема продукта: {_esc(gate.schema_version or "—")}.
  Основание состава: {_esc(gate.recipient_basis or "—")}.
  <div class="text-muted mt-1">
    Аудитория повторного сообщения берётся только из этой партии и остаётся в её филиале с её подтверждённым
    отправителем. Поиск дополнительных получателей не выполняется, и адресат не заменяется текущим владельцем
    совпадающего номера. Проверка записей при этом смотрит все настроенные филиалы того же рабочего пространства.
  </div>
</div></div>
{_template_block(template)}
{policy_block}
<div class="card mb-3"><div class="card-body">
  <h2 class="h6">1. Группы получателей исходной рассылки</h2>
  <p class="small text-muted">
    По умолчанию не выбрано ничего. Пустой выбор — это не «все группы». Один человек попадает ровно в одну группу:
    прочтение важнее доставки, доставка важнее поздней ошибки.
  </p>
  {_groups_block(selected)}
  <h2 class="h6 mt-3">2. Минимальный интервал после исходной попытки</h2>
  <p class="small text-muted">
    Указывается явно, в днях. Значения по умолчанию нет. Интервал считается от доказанной исходной попытки
    отправки, а не от времени ответа провайдера, не от времени изменения строки и не от момента открытия страницы.
  </p>
  <div class="input-group mb-2" style="max-width:20rem">
    <input type="number" min="1" max="{MAX_INTERVAL_DAYS}" step="1" class="form-control"
           id="followup-interval" value="{_esc(interval_value)}" placeholder="например, 14">
    <span class="input-group-text">дн.</span>
  </div>
  <h2 class="h6 mt-3">3. Проверка состава</h2>
  <p class="small text-muted">
    Это сохраняет новую версию предпросмотра. Отправки, подтверждения отправки и списания попытки здесь нет,
    и повторная проверка не блокирует получателя навсегда.
  </p>
  <button class="btn btn-primary" id="followup-check" {"disabled" if not gate.allowed else ""}>
    Проверить состав
  </button>
  <div class="small text-muted mt-2" id="followup-result"></div>
</div></div>
{_stored_block(stored, currency=currency)}
<div class="card"><div class="card-body small text-muted">
  <b>Что означают результаты.</b>
  Отсутствие отметки о прочтении не доказывает, что человек не прочитал сообщение.
  Пустая аудитория — это корректный результат, а не повод ослаблять фильтры.
  «Не установлено» — это не разрешение: так помечены позиции, по которым проверку не удалось завершить.
  Отказы от сообщений, ответы клиента и записи показаны как отдельные причины.
  Отсутствие строк в журнале отписок не является доказательством согласия.
</div></div>
<script>
const CSRF = {_json_literal(csrf)};
const HEADER = {_json_literal(CSRF_HEADER)};
// A refresh that did not produce a new result must not leave the previous one
// standing as current. The page was rendered when the saved snapshot still held;
// the click just proved that nothing can be asserted about NOW — a 409, a
// network failure, or an answer that could not be read. So the displayed
// standing drops to historical immediately, without a reload, and without
// touching the immutable snapshot: the counts, the dates and the rows stay
// readable as the history they are.
function followupDemoteStoredResult() {{
  const block = document.getElementById('followup-stored');
  if (block === null) {{ return; }}
  block.classList.remove('alert-success', 'alert-danger');
  block.classList.add('alert-warning');
  block.dataset.followupStanding = 'historical';
  const standing = document.getElementById('followup-standing');
  if (standing !== null) {{
    standing.textContent = 'Проверка состава не выполнена, поэтому актуальность этих проверок '
      + 'сейчас не подтверждена. Ниже — исторический результат; состав нужно пересчитать.';
  }}
  const confirmation = document.getElementById('followup-confirmation');
  if (confirmation !== null) {{
    confirmation.textContent = 'Они НЕ подтверждены заново: это историческая запись проверки, '
      + 'а не текущая готовность.';
  }}
}}
document.getElementById('followup-check')?.addEventListener('click', async (event) => {{
  const button = event.currentTarget;
  const out = document.getElementById('followup-result');
  const groups = Array.from(document.querySelectorAll('.followup-group'))
    .filter((box) => box.checked).map((box) => box.value);
  const days = parseInt(document.getElementById('followup-interval').value, 10);
  if (groups.length === 0) {{
    out.textContent = 'Выберите хотя бы одну группу. Пустой выбор — это не «все группы».';
    return;
  }}
  if (!Number.isInteger(days) || days < 1) {{
    out.textContent = 'Укажите минимальный интервал в днях. Значения по умолчанию нет.';
    return;
  }}
  button.disabled = true;
  out.textContent = 'Проверяем состав…';
  try {{
    const response = await fetch(window.location.pathname + '/preview', {{
      method: 'POST',
      credentials: 'same-origin',
      headers: {{'Content-Type': 'application/json', [HEADER]: CSRF}},
      body: JSON.stringify({{groups: groups, min_interval_days: days}}),
    }});
    let data = null;
    try {{
      data = await response.json();
    }} catch (parseError) {{
      // An answer we cannot read is not an answer. Same consequence as a
      // refusal: nothing is asserted about now.
      data = null;
    }}
    if (data !== null && data.accepted) {{
      out.textContent = 'Готово. Обновляем страницу…';
      window.location.reload();
      return;
    }}
    followupDemoteStoredResult();
    if (data === null) {{
      out.textContent = 'Проверка не выполнена: ответ сервера не удалось прочитать. '
        + 'Предыдущий результат не изменился, но он больше не считается актуальным.';
    }} else {{
      out.textContent = 'Проверка не выполнена, предыдущий результат не изменился и больше '
        + 'не считается актуальным. ' + (data.detail || '') + ' ' + (data.reasons || []).join(', ');
    }}
  }} catch (error) {{
    followupDemoteStoredResult();
    out.textContent = 'Проверка не выполнена, предыдущий результат не изменился и больше '
      + 'не считается актуальным.';
  }} finally {{
    button.disabled = false;
  }}
}});
</script>
"""
    return HTMLResponse(content=_page("Повторное сообщение", body), headers=_PRIVATE_HEADERS)


def _page_banner() -> str:
    """The sentence §46.6 requires, where it cannot be missed."""
    return (
        "<div class='alert alert-info'>"
        "<b>Доступен только предпросмотр.</b> Отправки повторного сообщения в этой версии нет: "
        "нет кнопки отправки, нет подтверждения отправки и нет серверного пути, который мог бы её выполнить. "
        "Предпросмотр ничего не отправляет, не расходует попытку отправки и не является разрешением на отправку."
        "</div>"
    )


def _json_literal(value: Any) -> str:
    """One JSON literal for the page's script, escaped for an HTML context."""
    import json

    return json.dumps(str(value)).replace("<", "\\u003c").replace(">", "\\u003e").replace("&", "\\u0026")
