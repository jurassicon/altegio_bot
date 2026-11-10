"""One screen: the follow-up for one original voucher mailing (§46.6, §46.8).

What an operator does here
--------------------------
Open an original mailing's follow-up page, tick which of the three groups the
repeat message would be for, type how long must have passed since the first
attempt, read the exact text that would be sent, and press one button to check
the composition. They get back how many people are eligible, who is excluded and
who could not be established, each with a reason in words.

Then, and only as a SEPARATE step, they prepare a send — which states the exact
number of messages pressing the next button can produce — confirm that number,
watch the progress, and stop the run for good if they want to.

The four steps are four different things on purpose
---------------------------------------------------
Looking at this page computes nothing. Checking the composition stores a
proposal and spends nothing. Preparing a send stores an offer and spends
nothing. Only the confirmation creates a durable run, and only the dedicated
executor sends — so closing the tab, pressing twice, opening a second tab or
reloading cannot produce a second message. Every GET here is a read.

Where the authorisation is
--------------------------
Exactly where the rest of the section's is, reusing it rather than repeating it:
the router carries ``require_ops_auth``, and every POST resolves the operator
from the session cookie, requires this session's CSRF token in a header and
requires a same-origin request. There is no second authorisation system, no API
key and no shared secret. The send itself needs more than a session: a stored
offer with its own TTL, its own keyed signature, the operator it was issued to
and the exact count they confirmed.

The fence
---------
The follow-up has its own, closed by default, and the primary mailing's
permission does not open it. With it closed this page shows the whole scenario
and every send action refuses by name.

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
from altegio_bot.campaigns.easyweek_voucher_followup import send_dispatch as send_module
from altegio_bot.campaigns.easyweek_voucher_followup import send_identity as send_ids
from altegio_bot.campaigns.easyweek_voucher_followup import send_ledger as send_ledger
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
from altegio_bot.campaigns.easyweek_voucher_production.operations import OpsPrincipal
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
        "с числовым идентификатором EasyWeek, не настроены филиалы, просмотр событий не завершён, "
        "либо сохранённые события об одной записи противоречат друг другу — расходятся в том, кто или в каком "
        "филиале создал запись, либо относят её появление ко времени после собственного же более раннего события о ней."
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
    # --- §46.8: the send's own refusals ----------------------------------
    "followup_send_disabled": (
        "Отправка повторных сообщений закрыта отдельным переключателем и по умолчанию выключена. "
        "Разрешение первичной рассылки её не открывает."
    ),
    "followup_executor_unavailable": (
        "В этой установке не запущен выделенный исполнитель повторной отправки, "
        "поэтому подтверждать нечего: подтверждение не должно оставаться в очереди, которую никто не разбирает."
    ),
    "followup_preview_unknown": "Для этой рассылки ещё не сохранён предпросмотр состава.",
    "followup_preview_not_current": (
        "Сохранённый предпросмотр больше не актуален: истёк срок, изменился контракт текста или политика, "
        "исходная рассылка была остановлена либо изменился её состав. Нужно пересчитать состав заново."
    ),
    "followup_approval_unknown": "Такого подтверждения нет.",
    "followup_approval_expired": (
        "Срок действия подготовленной отправки истёк. Подготовьте её заново: план, прочитанный давно, "
        "описывает мир, у которого было время измениться."
    ),
    "followup_approval_already_consumed": (
        "Эта подготовленная отправка уже подтверждена. Повторное нажатие, вторая вкладка и перезагрузка "
        "дают ту же самую отправку, а не вторую."
    ),
    "followup_approval_principal_mismatch": "Эта подготовленная отправка принадлежит другой сессии оператора.",
    "followup_approval_signature_invalid": "Подпись подготовленной отправки не совпадает с выданной сервером.",
    "followup_approval_count_unconfirmed": (
        "Подтверждённое количество не совпадает с количеством в подготовленной отправке."
    ),
    "followup_approval_contract_moved": (
        "После подготовки отправки изменилось что-то из зафиксированного: шаблон, политика, отправитель, "
        "состав исходной рассылки, её остановка или сам предложенный состав. Требуется новое подтверждение."
    ),
    "followup_approval_offer_unproven": (
        "В подготовленной отправке не зафиксирован точный состав получателей, поэтому подтвердить её нельзя. "
        "Подготовьте отправку заново."
    ),
    "followup_not_offered": (
        "Этот получатель не входил в подготовленную отправку: на момент подготовки отправить ему было нельзя. "
        "Подтверждение только сужает предложенный состав, поэтому он не добавляется — "
        "чтобы включить его, подготовьте отправку заново."
    ),
    "followup_run_already_active": (
        "По этой исходной рассылке уже есть отправка в очереди или в работе. Одновременно выполняется одна."
    ),
    "followup_run_stopped": "Отправка остановлена оператором. Остановка окончательная.",
    "followup_run_interrupted": (
        "Исполнитель прервался. Начатые попытки сохранены как «результат неизвестен»; повторная отправка запрещена."
    ),
    "followup_database_unavailable": "База данных недоступна.",
    "followup_source_terminal_stop": (
        "Исходная рассылка остановлена окончательно, поэтому её повторная отправка запрещена."
    ),
    "followup_attempt_already_used": (
        "Дополнительная попытка по этому получателю исходной рассылки уже учтена: она была израсходована "
        "или по нему сохранена окончательная остановка. Второго повторного сообщения не будет."
    ),
    "followup_terminally_stopped": (
        "По этому получателю сохранена окончательная остановка. Новая партия, новый предпросмотр, "
        "другая группа и другой шаблон её не обходят."
    ),
    "followup_consent_moved": (
        "На финальной границе разрешения согласие изменилось: отказ, непросмотренное входящее сообщение "
        "либо изменившийся набор адресов. Отправка по этой позиции не выполнена."
    ),
    "followup_claim_lease_lost": "Исполнитель потерял право на эту отправку, поэтому ничего не отправил.",
    "followup_meta_accepted": "Провайдер принял сообщение. Это не доказательство доставки.",
    "followup_meta_unknown": (
        "Результат внешнего запроса неизвестен: таймаут, сбой связи, нечитаемый ответ или ответ без "
        "идентификатора сообщения. Попытка израсходована, повторная отправка запрещена, нужна сверка фактов."
    ),
    "followup_meta_rejected": "Провайдер отказал в самом запросе. Попытка израсходована и не возобновляется.",
    "followup_meta_callback_failed": "Пришёл подтверждённый отказ доставки именно по этому повторному сообщению.",
    "followup_booking_link_unproven": "Для этого филиала не настроена ссылка на запись, поэтому текст не собрать.",
    "followup_template_parameters_unproven": (
        "Одну из двух подстановок утверждённого текста собрать не удалось, поэтому сообщение не отправлено."
    ),
}

# What each follow-up row state says to a person, and how it is coloured.
_SEND_STATUS_LABELS: Final[dict[str, str]] = {
    send_ids.PLANNED: "запланировано",
    send_ids.SKIPPED: "исключено",
    send_ids.CLAIMED: "попытка начата",
    send_ids.ACCEPTED: "принято провайдером",
    send_ids.DELIVERED: "доставлено",
    send_ids.READ: "прочитано",
    send_ids.REJECTED: "отказ провайдера",
    send_ids.FAILED: "ошибка доставки",
    send_ids.UNKNOWN: "результат неизвестен",
    send_ids.STOPPED: "остановлено",
}

_SEND_STATUS_BADGES: Final[dict[str, str]] = {
    send_ids.PLANNED: "bg-light text-dark",
    send_ids.SKIPPED: "bg-secondary",
    send_ids.CLAIMED: "bg-info text-dark",
    send_ids.ACCEPTED: "bg-primary",
    send_ids.DELIVERED: "bg-success",
    send_ids.READ: "bg-success",
    send_ids.REJECTED: "bg-danger",
    send_ids.FAILED: "bg-danger",
    send_ids.UNKNOWN: "bg-warning text-dark",
    send_ids.STOPPED: "bg-dark",
}

# The order the counters are shown in, declared once so the server-rendered
# tiles and the ones the script repaints cannot disagree.
_COUNTER_ORDER: Final[tuple[str, ...]] = (
    send_ids.PLANNED,
    send_ids.ACCEPTED,
    send_ids.DELIVERED,
    send_ids.READ,
    send_ids.FAILED,
    send_ids.UNKNOWN,
    send_ids.SKIPPED,
    send_ids.STOPPED,
)

_RUN_STATUS_LABELS: Final[dict[str, str]] = {
    "queued": "в очереди",
    "running": "выполняется",
    "completed": "завершена",
    "stopped": "остановлена оператором",
    "interrupted": "прервана",
    "refused": "отклонена",
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


def _ops_principal_or_error(request: Request) -> tuple[OpsPrincipal | None, JSONResponse | None]:
    """The authenticated operator as the SERVER resolved them, or a refusal.

    The same three checks in the same order and from the same module as the
    preview's action: same origin, then the session, then the CSRF token. The
    principal is built here and never from a request body — §46.8 is explicit
    that a request may not name its own actor.
    """
    try:
        require_same_origin(request)
        account, fingerprint = resolve_ops_session(request)
        require_csrf(request)
    except OpsSessionError as exc:
        return None, _session_error(exc)
    return OpsPrincipal(account=account, session_fingerprint=fingerprint), None


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
# The send: prepare, confirm, stop, status
# ---------------------------------------------------------------------------


class PlanRequest(BaseModel):
    """What a browser may choose when preparing a send: a version, or nothing.

    ``extra="forbid"`` is the point of the model. A request that tries to name a
    recipient list, a count, a digest, a template, an operator or a batch other
    than the one in the path is refused by name rather than ignored — the
    composition and the count are read from the stored preview by the server.
    """

    model_config = ConfigDict(extra="forbid")

    preview_version: int | None = Field(default=None, ge=1)


class ConfirmRequest(BaseModel):
    """The confirmation: which offer, its signature, and the exact count.

    All three are COMPARED against the stored approval and none of them is
    used as a value, so a browser cannot widen anything by sending them. What
    the count proves is that the human agreed to this many messages and not to
    whatever the page last rendered.
    """

    model_config = ConfigDict(extra="forbid")

    approval_id: int = Field(ge=1)
    plan_signature: str = Field(min_length=64, max_length=64)
    confirmed_count: int = Field(ge=0)


class StopRequest(BaseModel):
    """Which run to stop. Terminal, and it takes nothing else."""

    model_config = ConfigDict(extra="forbid")

    run_id: int = Field(ge=1)


def _refusal(reasons: tuple[str, ...], *, status_code: int = 409) -> JSONResponse:
    """One refusal shape for every send action. Codes plus one readable sentence."""
    primary = reasons[0] if reasons else "followup_send_unavailable"
    return JSONResponse(
        status_code=status_code,
        content={"accepted": False, "reasons": list(reasons), "detail": _reason_sentence(primary)},
        headers=_PRIVATE_HEADERS,
    )


@router.post("/{batch_id}/followup/plan")
async def api_plan_send(request: Request, batch_id: int, payload: PlanRequest) -> JSONResponse:
    """Prepare ONE send and store it as an immutable, single-use offer.

    Still not a send. It reads the stored preview, re-proves the batch, the
    message contract, the live Meta approval and the branch sender, counts how
    many messages pressing the next button could produce, and writes one
    approval row with its own expiry and its own signature. Nothing here claims
    an attempt, queues a run or reaches Meta's message API.

    The count it returns is the exact number the operator then confirms. It is
    narrower than the preview's included count on purpose: a recipient whose one
    extra attempt is already accounted for — spent in an earlier run, or stopped
    for good — is left out here rather than discovered later.
    """
    principal, refusal = _ops_principal_or_error(request)
    if refusal is not None:
        return refusal
    assert principal is not None

    try:
        offer = await send_module.offer_send(
            SessionLocal,
            principal=principal,
            batch_id=int(batch_id),
            preview_version=payload.preview_version,
        )
    except SQLAlchemyError:
        return _refusal((DATABASE_UNAVAILABLE,), status_code=503)

    if not offer.ready or offer.approval is None:
        return _refusal(offer.reasons or (send_ids.PREVIEW_NOT_CURRENT,))
    return JSONResponse(content={"accepted": True, **offer.as_safe_dict()}, headers=_PRIVATE_HEADERS)


@router.post("/{batch_id}/followup/confirm")
async def api_confirm_send(request: Request, batch_id: int, payload: ConfirmRequest) -> JSONResponse:
    """Spend ONE offer and queue ONE durable run.

    Returns as soon as the run is committed. Nothing has reached Meta yet and
    nothing in this request will: the dedicated executor picks it up, which is
    why closing the tab or losing this response changes nothing.

    A second POST of the same click — a double-click, a retry after a lost
    response, a refresh, the other tab — gets the SAME run back with
    ``created: false``. What must never happen is two runs, and that is the
    approval's single-use status plus a unique constraint, not a disabled
    button.
    """
    principal, refusal = _ops_principal_or_error(request)
    if refusal is not None:
        return refusal
    assert principal is not None

    stored = await send_ledger.load_approval(SessionLocal, approval_id=int(payload.approval_id))
    if stored is None or int(stored.source_batch_id) != int(batch_id):
        # An approval of another mailing is not this page's to spend, and is
        # refused by the same code as one that does not exist: a caller learns
        # nothing about offers that are not theirs.
        return _refusal((send_ids.APPROVAL_UNKNOWN,))

    try:
        outcome = await send_module.confirm_send(
            SessionLocal,
            approval_id=int(payload.approval_id),
            presented_signature=payload.plan_signature,
            principal=principal,
            confirmed_count=int(payload.confirmed_count),
        )
    except SQLAlchemyError:
        return _refusal((DATABASE_UNAVAILABLE,), status_code=503)

    if not outcome.accepted or outcome.run is None:
        return _refusal(outcome.reasons or (send_ids.APPROVAL_UNKNOWN,))
    return JSONResponse(
        content={
            "accepted": True,
            "created": outcome.created,
            "planned": outcome.planned,
            "skipped": outcome.skipped,
            **outcome.run.as_safe_dict(),
        },
        headers=_PRIVATE_HEADERS,
    )


@router.post("/{batch_id}/followup/stop")
async def api_stop_send(request: Request, batch_id: int, payload: StopRequest) -> JSONResponse:
    """Stop this run for good and tombstone every recipient it has not started.

    Terminal by contract, and deliberately available while the run is queued,
    running or already finished, and with the fence closed — those are exactly
    the moments an operator wants it. Pressing it twice is reported as such.

    It does not promise to cancel a request that has already left the process:
    nothing can. A row whose attempt was already claimed keeps its own state and
    its own honest outcome.
    """
    principal, refusal = _ops_principal_or_error(request)
    if refusal is not None:
        return refusal
    assert principal is not None

    run = await send_ledger.load_run(SessionLocal, run_id=int(payload.run_id))
    if run is None or int(run.source_batch_id) != int(batch_id):
        return _refusal((send_ids.APPROVAL_UNKNOWN,))
    try:
        outcome = await send_ledger.request_terminal_stop(
            SessionLocal,
            run_id=int(payload.run_id),
            requested_by=principal.account,
        )
    except SQLAlchemyError:
        return _refusal((DATABASE_UNAVAILABLE,), status_code=503)
    if not outcome.applied:
        return _refusal((outcome.reason or send_ids.APPROVAL_UNKNOWN,))
    return JSONResponse(content={"accepted": True, **outcome.as_safe_dict()}, headers=_PRIVATE_HEADERS)


@router.get("/{batch_id}/followup/status")
async def api_send_status(batch_id: int) -> JSONResponse:
    """The newest run of this mailing and its rows. A READ; it starts nothing.

    What the page polls while a run is in flight. It claims nothing, confirms
    nothing and sends nothing — §46.8 requires a GET to be incapable of
    starting execution, so this one reads two tables and returns labels.
    """
    run = await send_ledger.latest_run_for_batch(SessionLocal, source_batch_id=int(batch_id))
    if run is None:
        return JSONResponse(content={"run": None, "rows": [], "counters": {}}, headers=_PRIVATE_HEADERS)
    rows = await send_ledger.send_rows(SessionLocal, run_id=run.id)
    counters = await send_ledger.run_counters(SessionLocal, run_id=run.id)
    return JSONResponse(
        content={
            "run": run.as_safe_dict(),
            "rows": [row.as_safe_dict() for row in rows],
            "counters": counters,
            "send_enabled": send_module.send_allowed(),
            "executor_enabled": send_module.executor_available(),
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
        "<div class='table-responsive' style='overflow-x:auto'><table class='table table-sm align-middle'>"
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


def _send_rows_table(rows: tuple[send_ledger.SendRowView, ...]) -> str:
    """The follow-up's own result, slot by slot, beside the ORIGINAL status.

    Two separate columns on purpose (§46.8). The left one is what the FIRST
    message did — the group this recipient was chosen from — and the right one is
    what the repeat message did. Nothing on this screen mixes the two, and no
    follow-up outcome is ever printed as a statistic of the original mailing.
    """
    if not rows:
        return "<p class='text-muted'>В этой отправке нет позиций.</p>"
    body = []
    for row in rows:
        sentence = _reason_sentence(row.reason)
        body.append(
            "<tr>"
            f"<td class='text-nowrap'>{int(row.slot)}</td>"
            f"<td class='small'>{_esc(_GROUP_LABELS.get(row.group, row.group))}</td>"
            f"<td><span class='badge {_SEND_STATUS_BADGES.get(row.status, 'bg-light text-dark')}'>"
            f"{_esc(_SEND_STATUS_LABELS.get(row.status, row.status))}</span></td>"
            f"<td class='small'>{'да' if row.attempt_count else 'нет'}</td>"
            f"<td class='small text-break'>{_esc(sentence)}"
            + (f" <code class='text-break'>{_esc(row.reason)}</code>" if row.reason else "")
            + (
                f" <span class='badge bg-warning text-dark'>противоречий: {int(row.contradiction_count)}</span>"
                if row.contradiction_count
                else ""
            )
            + "</td></tr>"
        )
    return (
        "<div class='table-responsive' style='overflow-x:auto'>"
        "<table class='table table-sm align-middle' id='followup-send-table'>"
        "<thead><tr><th>Позиция</th><th>Первое сообщение</th><th>Повторное сообщение</th>"
        "<th>Попытка израсходована</th><th>Причина</th></tr></thead>"
        f"<tbody>{''.join(body)}</tbody></table></div>"
    )


def _send_block(
    *,
    stored: preview_module.StoredPreview | None,
    currency: preview_module.SnapshotCurrency | None,
    run: send_ledger.StoredRun | None,
    rows: tuple[send_ledger.SendRowView, ...],
    counters: dict[str, int],
    send_enabled: bool,
    executor_enabled: bool,
) -> str:
    """Steps 4 and 5: the separate confirmation, then the run and its stop.

    Rendered server-side AND owned by the page's script afterwards. Both halves
    matter, and the second one is the fix for a real defect: the first
    confirmation on a mailing that had never been sent produced a run with no
    badge, no counters and no stop button, because the page had rendered none of
    those and the poll only updated elements that already existed. The operator
    was told the send was confirmed while the section below still said there had
    never been one — with no way to stop it.

    So ``#followup-run`` is a container the script fills from the status
    endpoint, every time, whether or not the server rendered a run into it. The
    server-rendered copy is what a reload shows and what a browser with no
    script would see; the script replaces it with the same shape built from
    durable state.

    The confirmation panel stays hidden until an offer has been prepared: §46.8
    forbids a hidden selection, and an operator may not confirm a number they
    have not been shown.
    """
    fence = ""
    if not send_enabled:
        fence = (
            "<div class='alert alert-warning'><b>Отправка закрыта отдельным переключателем.</b> "
            "Весь сценарий ниже доступен для просмотра, но подготовка и подтверждение отправки "
            "сейчас отказывают по имени. Разрешение первичной рассылки этот переключатель не открывает.</div>"
        )
    elif not executor_enabled:
        fence = (
            "<div class='alert alert-warning'><b>Выделенный исполнитель не запущен.</b> "
            "Подтверждение сейчас отказывает: оно не должно оставаться в очереди, которую никто не разбирает.</div>"
        )

    ready = stored is not None and currency is not None and currency.current
    prepare_note = (
        "Подготовка отправки ничего не отправляет и не расходует попытку. "
        "Она пересчитывает разрешения и называет точное число сообщений, которое может дать следующая кнопка."
        if ready
        else "Сначала пересчитайте состав: подтверждать можно только актуальный предпросмотр."
    )

    run_block = "<p class='text-muted mb-0'>Отправок по этой рассылке ещё не было.</p>"
    if run is not None:
        status_label = _RUN_STATUS_LABELS.get(run.status, run.status)
        stop_note = (
            "<div class='small text-danger mt-1'>Остановлена оператором. Остановка окончательная: "
            "новая партия, новый предпросмотр, другая группа и другой шаблон её не обходят.</div>"
            if run.stopped
            else ""
        )
        stop_button = (
            f"<button class='btn btn-outline-danger btn-sm' id='followup-stop' data-run-id='{int(run.id)}'>"
            "Остановить окончательно</button>"
            if not run.stopped
            else "<button class='btn btn-outline-secondary btn-sm' disabled>Уже остановлена</button>"
        )
        tiles = "".join(
            "<div class='col'><div class='card'><div class='card-body py-2'>"
            f"<div class='h5 mb-0'>{int(counters.get(state, 0))}</div>"
            f"<div class='small text-muted'>{_esc(_SEND_STATUS_LABELS[state])}</div>"
            "</div></div></div>"
            for state in _COUNTER_ORDER
        )
        # ``flex-wrap`` is load-bearing, not decoration: at phone width the
        # badge, the sentence and the stop button do not fit on one line, and a
        # flex row that cannot wrap pushes the whole page sideways. §46.8 asks
        # for this screen to be readable on mobile, and a page that scrolls
        # horizontally is how a stop button ends up off-screen.
        run_block = f"""
<div class="d-flex flex-wrap align-items-center gap-2 mb-2">
  <span class="badge bg-secondary" id="followup-run-status"
        data-followup-finished="{"true" if run.finished else "false"}">{_esc(status_label)}</span>
  <span class="small text-muted">отправка №{int(run.id)}, подтверждено позиций: {int(run.planned_count)}</span>
  <span class="ms-sm-auto">{stop_button}</span>
</div>
{stop_note}
<div class="row row-cols-2 row-cols-md-4 g-2 mb-3" id="followup-send-counters">{tiles}</div>
{_send_rows_table(rows)}
"""

    return f"""
{fence}
<div class="card mb-3"><div class="card-body">
  <h2 class="h6">4. Подтверждение отправки</h2>
  <p class="small text-muted">{_esc(prepare_note)}</p>
  <button class="btn btn-outline-primary" id="followup-prepare" {"" if ready else "disabled"}>
    Подготовить отправку
  </button>
  <div class="alert alert-light border mt-3 d-none" id="followup-confirm-panel">
    <div><b>Будет отправлено сообщений: <span id="followup-confirm-count">—</span>.</b></div>
    <div class="small text-muted mt-1" id="followup-confirm-detail"></div>
    <div class="small text-muted mt-1">
      Это отдельное подтверждение именно этого состава и этого текста. Оно не создаёт новый ваучер,
      не продлевает срок и не меняет финансовую историю. Каждому получателю исходной рассылки
      разрешена не более одной дополнительной попытки.
    </div>
    <button class="btn btn-danger mt-2" id="followup-confirm">Подтвердить и отправить</button>
    <button class="btn btn-link mt-2" id="followup-cancel">Отмена</button>
  </div>
  <div class="small text-muted mt-2" id="followup-send-result"></div>
</div></div>
<div class="card mb-3"><div class="card-body">
  <h2 class="h6">5. Исполнение, статус и остановка</h2>
  <p class="small text-muted">
    Исполнение продолжается независимо от этой вкладки: его ведёт выделенный исполнитель,
    а не этот запрос. Повторное нажатие, вторая вкладка и перезагрузка не создают вторую отправку.
    «Принято провайдером» — это не доставка. «Результат неизвестен» означает израсходованную попытку
    без доказанного результата: повторная отправка запрещена, допускается только сверка фактов.
  </p>
  <div id="followup-run">{run_block}</div>
</div></div>
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
    # The newest run of this mailing, read with its own session. A read: opening
    # this page claims nothing, confirms nothing and sends nothing.
    run = await send_ledger.latest_run_for_batch(SessionLocal, source_batch_id=int(batch_id))
    send_rows = await send_ledger.send_rows(SessionLocal, run_id=run.id) if run is not None else ()
    send_counters = await send_ledger.run_counters(SessionLocal, run_id=run.id) if run is not None else {}
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
{
        _send_block(
            stored=stored,
            currency=currency,
            run=run,
            rows=send_rows,
            counters=send_counters,
            send_enabled=send_module.send_allowed(),
            executor_enabled=send_module.executor_available(),
        )
    }
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

// --- steps 4 and 5: prepare, confirm, poll, stop -------------------------
// The prepared offer lives in this page only. It is the approval id and the
// signature the SERVER issued; the page cannot compute either, and the count it
// shows is the one the server counted. Nothing here assembles permission out of
// fields — every value is compared server-side and discarded.
let followupOffer = null;

function followupSay(text) {{
  const out = document.getElementById('followup-send-result');
  if (out !== null) {{ out.textContent = text; }}
}}

function followupHideConfirm() {{
  followupOffer = null;
  document.getElementById('followup-confirm-panel')?.classList.add('d-none');
}}

document.getElementById('followup-prepare')?.addEventListener('click', async (event) => {{
  const button = event.currentTarget;
  button.disabled = true;
  followupHideConfirm();
  followupSay('Подготавливаем отправку…');
  try {{
    const response = await fetch(window.location.pathname + '/plan', {{
      method: 'POST',
      credentials: 'same-origin',
      headers: {{'Content-Type': 'application/json', [HEADER]: CSRF}},
      body: JSON.stringify({{}}),
    }});
    let data = null;
    try {{ data = await response.json(); }} catch (parseError) {{ data = null; }}
    if (data === null || !data.accepted) {{
      followupSay('Подготовка не выполнена. ' + ((data && data.detail) || '')
        + ' ' + ((data && data.reasons) || []).join(', '));
      return;
    }}
    followupOffer = {{id: data.approval.approval_id, signature: data.plan_signature,
                      count: data.approval.approved_recipient_count}};
    document.getElementById('followup-confirm-count').textContent = String(followupOffer.count);
    document.getElementById('followup-confirm-detail').textContent =
      'Группы: ' + (data.approval.selected_groups || []).join(', ')
      + '. Интервал: ' + Math.round((data.approval.min_interval_seconds || 0) / 86400) + ' дн.'
      + ' Текст: ' + data.approval.meta_template_name + ' (' + data.approval.template_language + ').'
      + ' Подтверждение действует до ' + data.approval.expires_at + '.';
    document.getElementById('followup-confirm-panel').classList.remove('d-none');
    followupSay('');
  }} catch (error) {{
    followupSay('Подготовка не выполнена: запрос не удался.');
  }} finally {{
    button.disabled = false;
  }}
}});

document.getElementById('followup-cancel')?.addEventListener('click', () => {{
  followupHideConfirm();
  followupSay('Подтверждение отменено. Ничего не отправлено и попытки не израсходованы.');
}});

document.getElementById('followup-confirm')?.addEventListener('click', async (event) => {{
  const button = event.currentTarget;
  if (followupOffer === null) {{
    followupSay('Сначала подготовьте отправку.');
    return;
  }}
  // Disabled for the whole request, so a double click cannot produce a second
  // POST. The server would answer the same run anyway — the approval is
  // single-use — and this only keeps the screen honest while it waits.
  button.disabled = true;
  followupSay('Подтверждаем…');
  try {{
    const response = await fetch(window.location.pathname + '/confirm', {{
      method: 'POST',
      credentials: 'same-origin',
      headers: {{'Content-Type': 'application/json', [HEADER]: CSRF}},
      body: JSON.stringify({{approval_id: followupOffer.id, plan_signature: followupOffer.signature,
                             confirmed_count: followupOffer.count}}),
    }});
    // A PROVEN refusal and an UNKNOWN answer are different facts, and
    // collapsing them was a defect: an unreadable body — a proxy's 502 HTML, a
    // truncated response — was reported as «Отправка не подтверждена» and the
    // page returned without reconciling, while the confirmation had actually
    // committed and a run was queued with no stop button on screen.
    //
    // So the only thing that counts as a refusal is a readable answer that says
    // so. Everything else is unknown, and unknown means: go and read the
    // durable state. Never a second confirm — the approval is single-use and
    // this page does not retry it.
    let data = null;
    try {{ data = await response.json(); }} catch (parseError) {{ data = null; }}
    if (data !== null && data.accepted === false) {{
      followupSay('Отправка не подтверждена. ' + (data.detail || '')
        + ' ' + ((data.reasons) || []).join(', '));
      return;
    }}
    if (data === null || data.accepted !== true) {{
      followupHideConfirm();
      followupSay('Ответ сервера не удалось прочитать, поэтому результат подтверждения неизвестен. '
        + 'Повторное подтверждение не отправляется. Ниже — сохранённое состояние отправки.');
      await followupPoll();
      return;
    }}
    followupHideConfirm();
    followupSay(data.created
      ? ('Отправка подтверждена: позиций ' + data.planned + ', исключено ' + data.skipped
         + '. Исполняет выделенный исполнитель.')
      : 'Эта отправка уже была подтверждена ранее. Второй отправки не создано.');
    await followupPoll();
  }} catch (error) {{
    // The request itself failed — aborted, offline, DNS. It may still have
    // reached the server and committed, so this is unknown too and is settled
    // the same way: by reading the durable state, never by confirming again.
    followupHideConfirm();
    followupSay('Ответ сервера не получен, результат подтверждения неизвестен. '
      + 'Повторное подтверждение не отправляется. Ниже — сохранённое состояние отправки.');
    await followupPoll();
  }} finally {{
    button.disabled = false;
  }}
}});

document.addEventListener('click', async (event) => {{
  const button = event.target.closest('#followup-stop');
  if (button === null) {{ return; }}
  button.disabled = true;
  followupSay('Останавливаем…');
  try {{
    const response = await fetch(window.location.pathname + '/stop', {{
      method: 'POST',
      credentials: 'same-origin',
      headers: {{'Content-Type': 'application/json', [HEADER]: CSRF}},
      body: JSON.stringify({{run_id: Number(button.dataset.runId)}}),
    }});
    let data = null;
    try {{ data = await response.json(); }} catch (parseError) {{ data = null; }}
    if (data === null || !data.accepted) {{
      followupSay('Остановить не удалось. ' + ((data && data.detail) || ''));
      button.disabled = false;
      return;
    }}
    followupSay(data.already_stopped
      ? 'Отправка уже была остановлена. Повторное нажатие ничего не изменило.'
      : ('Отправка остановлена окончательно. Отложено навсегда позиций: ' + data.tombstoned + '.'));
    followupPoll();
  }} catch (error) {{
    followupSay('Остановить не удалось: запрос не выполнен.');
    button.disabled = false;
  }}
}});

const FOLLOWUP_STATUS_LABELS = {_json_object(_SEND_STATUS_LABELS)};
const FOLLOWUP_RUN_LABELS = {_json_object(_RUN_STATUS_LABELS)};
const FOLLOWUP_GROUP_LABELS = {_json_object(_GROUP_LABELS)};
const FOLLOWUP_REASON_LABELS = {_json_object(_REASON_LABELS)};
const FOLLOWUP_STATUS_BADGES = {_json_object(_SEND_STATUS_BADGES)};
const FOLLOWUP_COUNTER_ORDER = {_json_literal_list(_COUNTER_ORDER)};

function followupEscape(value) {{
  const box = document.createElement('div');
  box.textContent = value === null || value === undefined ? '' : String(value);
  return box.innerHTML;
}}

// The whole run block, built from DURABLE state rather than patched into
// whatever the server happened to render. That is what makes the first
// confirmation on a mailing with no previous send show its status, its counters
// and its stop button without a reload — and it is the same shape the server
// renders, so a reload and a repaint agree.
function followupRenderRun(data) {{
  const host = document.getElementById('followup-run');
  if (host === null) {{ return; }}
  if (data.run === null) {{
    host.innerHTML = '<p class="text-muted mb-0">Отправок по этой рассылке ещё не было.</p>';
    return;
  }}
  const run = data.run;
  const label = FOLLOWUP_RUN_LABELS[run.status] || run.status;
  const stopped = run.stop_requested === true;
  const stopControl = stopped
    ? '<button class="btn btn-outline-secondary btn-sm" disabled>Уже остановлена</button>'
    : '<button class="btn btn-outline-danger btn-sm" id="followup-stop" data-run-id="'
      + String(run.run_id) + '">Остановить окончательно</button>';
  const stopNote = stopped
    ? '<div class="small text-danger mt-1">Остановлена оператором. Остановка окончательная: '
      + 'новая партия, новый предпросмотр, другая группа и другой шаблон её не обходят.</div>'
    : '';
  const tiles = FOLLOWUP_COUNTER_ORDER.map((state) =>
    '<div class="col"><div class="card"><div class="card-body py-2">'
    + '<div class="h5 mb-0">' + ((data.counters || {{}})[state] || 0) + '</div>'
    + '<div class="small text-muted">' + followupEscape(FOLLOWUP_STATUS_LABELS[state] || state)
    + '</div></div></div></div>').join('');
  const rows = (data.rows || []).map((row) => {{
    const reason = row.reason
      ? followupEscape(FOLLOWUP_REASON_LABELS[row.reason] || '')
        + ' <code class="text-break">' + followupEscape(row.reason) + '</code>'
      : '';
    const contradictions = row.contradictions
      ? ' <span class="badge bg-warning text-dark">противоречий: ' + String(row.contradictions) + '</span>'
      : '';
    return '<tr><td class="text-nowrap">' + String(row.slot) + '</td>'
      + '<td class="small">' + followupEscape(FOLLOWUP_GROUP_LABELS[row.group] || row.group) + '</td>'
      + '<td><span class="badge ' + (FOLLOWUP_STATUS_BADGES[row.status] || 'bg-light text-dark') + '">'
      + followupEscape(FOLLOWUP_STATUS_LABELS[row.status] || row.status) + '</span></td>'
      + '<td class="small">' + (row.attempt_spent ? 'да' : 'нет') + '</td>'
      + '<td class="small text-break">' + reason + contradictions + '</td></tr>';
  }}).join('');
  const table = rows === ''
    ? '<p class="text-muted">В этой отправке нет позиций.</p>'
    : '<div class="table-responsive" style="overflow-x:auto">'
      + '<table class="table table-sm align-middle" id="followup-send-table">'
      + '<thead><tr><th>Позиция</th><th>Первое сообщение</th><th>Повторное сообщение</th>'
      + '<th>Попытка израсходована</th><th>Причина</th></tr></thead>'
      + '<tbody>' + rows + '</tbody></table></div>';
  host.innerHTML =
    '<div class="d-flex flex-wrap align-items-center gap-2 mb-2">'
    + '<span class="badge bg-secondary" id="followup-run-status" data-followup-finished="'
    + (run.finished ? 'true' : 'false') + '">' + followupEscape(label) + '</span>'
    + '<span class="small text-muted">отправка №' + String(run.run_id)
    + ', подтверждено позиций: ' + String(run.planned_count) + '</span>'
    + '<span class="ms-sm-auto">' + stopControl + '</span>'
    + '</div>' + stopNote
    + '<div class="row row-cols-2 row-cols-md-4 g-2 mb-3" id="followup-send-counters">' + tiles + '</div>'
    + table;
}}

let followupPollTimer = null;

async function followupPoll(options) {{
  const keepPolling = !(options && options.once === true);
  try {{
    const response = await fetch(window.location.pathname + '/status', {{
      method: 'GET', credentials: 'same-origin', cache: 'no-store',
    }});
    const data = await response.json();
    followupRenderRun(data);
    // Keep asking only while the run can still change. A finished run is
    // final, and a page that kept asking would suggest otherwise. No reload is
    // involved either way: the block above is built from the same durable state
    // a reload would read.
    if (keepPolling && data.run !== null && !data.run.finished) {{
      if (followupPollTimer !== null) {{ window.clearTimeout(followupPollTimer); }}
      followupPollTimer = window.setTimeout(followupPoll, 2000);
    }}
  }} catch (error) {{
    // A failed poll says nothing about the run, so the page must not say
    // «Отправок по этой рассылке ещё не было» — that is a claim about durable
    // state this request could not read. It says it does not know instead, and
    // keeps whatever it was already showing.
    const host = document.getElementById('followup-run');
    if (host !== null && document.getElementById('followup-run-status') === null) {{
      host.innerHTML = '<p class="text-muted mb-0" id="followup-run-unknown">'
        + 'Состояние отправки сейчас прочитать не удалось. Это не значит, что отправки нет — '
        + 'обновите страницу.</p>';
    }}
  }}
}}

// Always, on every load. The status endpoint is a read and answers `null` when
// this mailing has never been sent, so this costs one request and removes the
// class of defect where the page could only update what it had already drawn.
followupPoll();
</script>
"""
    return HTMLResponse(content=_page("Повторное сообщение", body), headers=_PRIVATE_HEADERS)


def _page_banner() -> str:
    """The sentences §46.6 and §46.8 require, where they cannot be missed.

    Four separate steps, named as four, because the operator's whole protection
    is that none of them does the next one's work: a preview is not permission,
    a prepared send is not a send, and only the confirmation creates a run.
    """
    return (
        "<div class='alert alert-info'>"
        "<b>Четыре отдельных шага.</b> Предпросмотр состава ничего не отправляет и не расходует попытку. "
        "Подготовка отправки тоже ничего не отправляет — она называет точное число сообщений. "
        "Только отдельное подтверждение создаёт отправку, и выполняет её выделенный исполнитель, "
        "а не эта страница. Каждому получателю исходной рассылки разрешена не более одной "
        "дополнительной попытки; новые ваучеры и финансовые операции здесь не создаются."
        "</div>"
    )


def _json_literal(value: Any) -> str:
    """One JSON literal for the page's script, escaped for an HTML context."""
    import json

    return json.dumps(str(value)).replace("<", "\\u003c").replace(">", "\\u003e").replace("&", "\\u0026")


def _json_literal_list(value: tuple[str, ...]) -> str:
    """One JSON array literal for the page's script, escaped the same way."""
    import json

    return (
        json.dumps(list(value), ensure_ascii=False)
        .replace("<", "\\u003c")
        .replace(">", "\\u003e")
        .replace("&", "\\u0026")
    )


def _json_object(value: dict[str, str]) -> str:
    """One JSON object literal for the page's script, escaped the same way.

    Used for the two label maps, so the words the polling script prints are the
    same words the server-rendered table prints. A second copy in JavaScript is
    exactly how the two end up disagreeing.
    """
    import json

    return json.dumps(value, ensure_ascii=False).replace("<", "\\u003c").replace(">", "\\u003e").replace("&", "\\u0026")
