# Runbook — operator-only recovery упавшей отмены с `service_id: null`

Контракт — §40 `docs/easyweek/INTEGRATION_PLAN.md`. Этот runbook описывает
только порядок выполнения на production-сервере.

Все команды выполняются на сервере из `/opt/altegio_bot`. SSH-команды с Mac в
runbook не входят. Агент production-команды не выполняет.

---

## 1. Что это и чем не является

**Это** одноразовая операторская процедура из трёх шагов для **явно
перечисленных** `easyweek_events.id`, которые:

- терминально упали как `status=failed` / `error_code=invalid_payload`;
- пришли триггером `booking-canceled`;
- имеют `body_truncated=false`;
- несут в payload literal JSON `null` в поле `service_id`.

**Это не**: replay failed events, фоновый reconciliation, второй планировщик,
backfill lifecycle/review/retention/campaign, отправка сообщений и не изменение
обычного Altegio path.

Жёсткие границы:

- `--event-id` обязателен и повторяем; `--all` не существует;
- `plan` и `verify` read-only;
- `apply` меняет ровно три вещи: `Record.is_deleted=true`, `queued` EasyWeek
  `reminder_24h`/`reminder_2h` этой записи → `canceled`, и статус самого
  события → `processed`;
- ни один `MessageJob` не создаётся, `OutboxMessage` не пишется, Meta и
  Chatwoot не вызываются, EasyWeek mutation API не вызывается;
- Altegio `Record` и Altegio jobs не затрагиваются — это работа reminder
  handover §30;
- ручные `UPDATE`/`DELETE` в PostgreSQL запрещены.

Разрешённый scope текущей волны — **только event 360**.
Events 558, 559, 613 и 614 имеют numeric `service_id` и в эту процедуру
не входят. Кандидат без ledger-строки в неё тоже не входит.

---

## 2. Предусловия

1. Образ с исправленным нормализатором задеплоен, `altegio-easyweek-inbox-worker`
   пересоздан и здоров.
2. Выбранные `--event-id` подтверждены read-only выборкой (ниже).
3. Inbox и capture **не** отключаются. Outbox для этой процедуры останавливать
   не требуется: apply отказывается сам, если относящийся EasyWeek reminder
   находится в `status=processing`.

Общий префикс для всех команд ниже:

```bash
cd /opt/altegio_bot
```

```bash
dc() { docker compose -p altegio_bot -f docker-compose.yml -f docker-compose.chatwoot-internal.yml "$@"; }
```

Read-only сверка кандидатов перед выбором `--event-id`:

```bash
dc exec -T postgres psql -X -v ON_ERROR_STOP=1 -U altegio -d altegio_bot -c "BEGIN TRANSACTION READ ONLY; SELECT id, event_hint, status, error_code, body_truncated, booking_uuid, jsonb_typeof(payload -> 'service_id') AS service_id_type FROM easyweek_events WHERE status = 'failed' AND error_code = 'invalid_payload' AND event_hint = 'booking-canceled' ORDER BY id; COMMIT;"
```

В scope этой волны берётся только строка с `service_id_type = 'null'`.

---

## 3. Порядок после deploy

### Шаг 1 — recovery plan только для event 360

```bash
dc --profile ops run --rm --build --no-deps -T easyweek-failed-cancellation-recovery plan --event-id 360 --plan /recovery/easyweek-failed-cancellation-plan.json
```

Команда читает событие, сверяет его с локальной `Record` и доказывает текущее
состояние бронирования реальным `GET /bookings/{uuid}`. Она **не меняет
ничего**: ни CRM, ни PostgreSQL, ни `MessageJob`, ни `Record`, ни
`easyweek_events`. Meta и Chatwoot не вызываются.

Exit code `0` и `summary.apply_ready = true` означают, что план применим. Любой
другой результат — это `blockers` со стабильным кодом; apply запускать нельзя,
причину нужно устранить и выполнить plan заново.

### Шаг 2 — ручная проверка PII-free отчёта

Прочитайте напечатанный отчёт и убедитесь, что:

- `requested = 1`, `recoverable = 1`, `blocked = 0`, `blockers = []`;
- `events[0].disposition = recover_failed_cancellation`;
- `events[0].booking_uuid` — именно `5b5e9222-a0c4-4b44-a3d6-61132fcfea10`;
- `events[0].record_is_deleted = false`;
- `events[0].live` показывает `is_canceled = true`, `is_completed = false`,
  `status_type = canceled` и тот же start instant;
- `events[0].processing_reminder_job_ids = []`;
- `events[0].later_event_ids = []`;
- в отчёте нет имён, телефонов, email и webhook payload.

Файл плана лежит под смонтированным `/recovery`, имеет права `0600`, каталог —
`0700`, и в Git не попадает. Он содержит booking UUID и технические id и не
должен пересылаться.

Запишите `plan_digest` из отчёта.

### Шаг 3 — recovery apply с digest и confirmation

```bash
dc --profile ops run --rm --build --no-deps -T -e EASYWEEK_FAILED_CANCELLATION_RECOVERY_ALLOW_APPLY=true easyweek-failed-cancellation-recovery apply --event-id 360 --plan /recovery/easyweek-failed-cancellation-plan.json --apply-report /recovery/easyweek-failed-cancellation-apply.json --apply --plan-digest <PLAN_DIGEST> --confirm 'recover easyweek failed cancellation <PLAN_DIGEST>'
```

Все четыре разрешения обязательны одновременно: environment authorization,
`--apply`, точный `--plan-digest` и confirmation phrase, содержащая тот же
digest. Одного `--apply` недостаточно.

Перед транзакцией apply заново запрашивает живое состояние бронирования. Внутри
одной транзакции он блокирует точное событие, `Record` и её jobs, повторно
доказывает fingerprints и отсутствие более поздних событий, и только затем
пишет. Любой drift, любой `processing` reminder и любая ошибка откатывают всю
операцию целиком.

Отчёт apply записывается с правами `0600` и требуется шагу 4.

Если контейнер умер после commit, но до записи отчёта — повторите **ту же**
команду: повторный apply того же плана идемпотентен, даёт
`outcome = already_applied` и нулевые мутации.

### Шаг 4 — recovery verify

```bash
dc --profile ops run --rm --build --no-deps -T easyweek-failed-cancellation-recovery verify --event-id 360 --plan /recovery/easyweek-failed-cancellation-plan.json --apply-report /recovery/easyweek-failed-cancellation-apply.json
```

`passed = true` доказывает: событие `processed` с очищенными `error_code` и
`next_retry_at`; точная `Record` имеет `is_deleted=true`; открытых EasyWeek
reminders у неё нет; ни одного нового `MessageJob` или `OutboxMessage`; другие
события, записи и задания не изменены.

### Шаг 5 — HANNA reminder handover plan

Восстановленная строка принадлежит волне **HANNA**, а не CORE. Это разные
manifest, разные ledger `run_id` и разные snapshot/report. Смешивать их в одном
snapshot запрещено: `migration_wave_changed` — STOP, а применённый не к той
волне snapshot затрагивает чужие записи.

Точный и единственный scope этого раздела:

| Параметр | Значение |
|---|---|
| manifest | `/migration/input/manifest.handover.hanna.json` |
| company-id | `758285` |
| run-id | `55be3c0a62164e06` |
| run-id | `f61323dc49384c62` |

Ровно два run ID. Пять CORE run ID
(`27d8b9b5c59a446c`, `887cfbbe881149ad`, `90b183e121294f49`, `9f895ed02dc64073`,
`f6897b60b99b4860`), CORE manifest `manifest.karlsruhe.api-contract.20260831.json`
и общий snapshot `reminder_handover.v5.json` в этом разделе **не используются**.
Run ID Ирины и Алёны (`b4ca41ac1ad54591`, `c52bb4f62fb64a35`,
`02d514703aec466f`, `de193299b9974859`) тоже не входят.

Один и тот же фрагмент от `--manifest` до последнего `--run-id` используется без
изменений во **всех трёх** режимах plan/apply/verify ниже.

```bash
cd /opt/altegio_bot
dc() { docker compose -p altegio_bot -f docker-compose.yml -f docker-compose.chatwoot-internal.yml "$@"; }
dc --profile ops run --rm --no-deps -T easyweek-migration-prepare-handover plan --manifest /migration/input/manifest.handover.hanna.json --company-id 758285 --run-id 55be3c0a62164e06 --run-id f61323dc49384c62 --snapshot /migration/state/reminder_handover.hanna.after-failed-cancellation.v5.json
```

Строка, которую восстановил шаг 3, обязана теперь получить disposition
`handover_terminal_canceled` вместо `local_target_mismatch`.

Полный контракт отчёта, значения `guard_ready` / `coverage_ready` /
`cutover_ready` и все предупреждения — §6b.2 `migration_preparation_runbook.md`.

### Шаг 6 — проверка отчёта перед apply

Продолжать **только если** отчёт шага 5 показывает одновременно:

- `cutover_ready = true`;
- ни одного `unproven`;
- ни одного `local_target_mismatch`;
- `rows_with_processing_source_jobs` пуст.

Иначе — STOP, разбор причины, новый plan. Запишите `plan_digest` отчёта: он
нужен шагу 7 дважды — в `--plan-digest` и внутри `--confirm`.

### Шаг 7 — HANNA handover apply с короткой остановкой outbox

Outbox останавливается только на время одной транзакции, и `trap` возвращает
воркер при любом выходе, включая ошибку и Ctrl-C. Inbox и capture не
останавливаются, notification-флаги не трогаются.

```bash
(
set -euo pipefail
cd /opt/altegio_bot

dc() { docker compose -p altegio_bot -f docker-compose.yml -f docker-compose.chatwoot-internal.yml "$@"; }

restore_outbox() {
  original_rc=$?
  trap - EXIT INT TERM HUP
  set +e
  dc up -d altegio-outbox-worker
  restart_rc=$?
  dc ps altegio-outbox-worker
  running="$(dc ps --status running -q altegio-outbox-worker | wc -l | tr -d ' ')"
  if [ "$running" -ne 1 ]; then
    restart_rc=1
    echo 'STOP: altegio-outbox-worker не вернулся в running' >&2
  fi
  if [ "$original_rc" -ne 0 ]; then
    exit "$original_rc"
  fi
  exit "$restart_rc"
}

trap restore_outbox EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'exit 129' HUP

dc stop altegio-outbox-worker
dc --profile ops run --rm --no-deps -T -e EASYWEEK_REMINDER_HANDOVER_ALLOW_APPLY=true easyweek-migration-prepare-handover apply --manifest /migration/input/manifest.handover.hanna.json --company-id 758285 --run-id 55be3c0a62164e06 --run-id f61323dc49384c62 --snapshot /migration/state/reminder_handover.hanna.after-failed-cancellation.v5.json --apply-report /migration/state/reminder_handover.hanna.after-failed-cancellation.apply-report.v3.json --apply --plan-digest PLAN_DIGEST_ИЗ_ШАГА_5 --confirm 'apply reminder handover PLAN_DIGEST_ИЗ_ШАГА_5'
)
```

### Шаг 8 — проверка, что outbox вернулся

Trap проверяет это сам; после выхода команды это доказывается независимо.

```bash
cd /opt/altegio_bot
dc() { docker compose -p altegio_bot -f docker-compose.yml -f docker-compose.chatwoot-internal.yml "$@"; }
dc ps altegio-outbox-worker
test "$(dc ps --status running -q altegio-outbox-worker | wc -l | tr -d ' ')" -eq 1
```

### Шаг 9 — HANNA handover verify

```bash
cd /opt/altegio_bot
dc() { docker compose -p altegio_bot -f docker-compose.yml -f docker-compose.chatwoot-internal.yml "$@"; }
dc --profile ops run --rm --no-deps -T easyweek-migration-prepare-handover verify --manifest /migration/input/manifest.handover.hanna.json --company-id 758285 --run-id 55be3c0a62164e06 --run-id f61323dc49384c62 --snapshot /migration/state/reminder_handover.hanna.after-failed-cancellation.v5.json --apply-report /migration/state/reminder_handover.hanna.after-failed-cancellation.apply-report.v3.json
```

Требования к PASS — §6b.4 `migration_preparation_runbook.md`:
`wave_closures_expected == wave_closures_verified`,
`wave_closures_missing == []`, `wave_closures_with_foreign_digest == []`.

Проверка идемпотентности — повторный apply того же snapshot в **отдельный**
report, чтобы не уничтожить evidence шага 7. Результат обязан показать
`mutations: 0`.

```bash
(
set -euo pipefail
cd /opt/altegio_bot
dc() { docker compose -p altegio_bot -f docker-compose.yml -f docker-compose.chatwoot-internal.yml "$@"; }
restore_outbox() {
  original_rc=$?
  trap - EXIT INT TERM HUP
  set +e
  dc up -d altegio-outbox-worker
  restart_rc=$?
  dc ps altegio-outbox-worker
  running="$(dc ps --status running -q altegio-outbox-worker | wc -l | tr -d ' ')"
  if [ "$running" -ne 1 ]; then restart_rc=1; fi
  if [ "$original_rc" -ne 0 ]; then exit "$original_rc"; fi
  exit "$restart_rc"
}
trap restore_outbox EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'exit 129' HUP
dc stop altegio-outbox-worker
dc --profile ops run --rm --no-deps -T -e EASYWEEK_REMINDER_HANDOVER_ALLOW_APPLY=true easyweek-migration-prepare-handover apply --manifest /migration/input/manifest.handover.hanna.json --company-id 758285 --run-id 55be3c0a62164e06 --run-id f61323dc49384c62 --snapshot /migration/state/reminder_handover.hanna.after-failed-cancellation.v5.json --apply-report /migration/state/reminder_handover.hanna.after-failed-cancellation.repeat-apply-report.v3.json --apply --plan-digest PLAN_DIGEST_ИЗ_ШАГА_5 --confirm 'apply reminder handover PLAN_DIGEST_ИЗ_ШАГА_5'
)
```

### Шаг 10 — reminder preflight и read-only сверка

Существующий API preflight читает актуальные EasyWeek booking и прогоняет
production reminder guard для всей открытой очереди. Он не подменяет verify.

```bash
cd /opt/altegio_bot
dc() { docker compose -p altegio_bot -f docker-compose.yml -f docker-compose.chatwoot-internal.yml "$@"; }
dc run --rm --no-deps -T --entrypoint /app/.venv/bin/python altegio-outbox-worker -m altegio_bot.scripts.easyweek_reminder_preflight --limit 200 --pause-sec 1.05
```

```bash
cd /opt/altegio_bot
dc() { docker compose -p altegio_bot -f docker-compose.yml -f docker-compose.chatwoot-internal.yml "$@"; }
dc exec -T postgres psql -X -v ON_ERROR_STOP=1 -U altegio -d altegio_bot -c "BEGIN TRANSACTION READ ONLY; SELECT provider, job_type, status, count(*) FROM message_jobs WHERE job_type IN ('reminder_24h','reminder_2h') GROUP BY 1,2,3 ORDER BY 1,2,3; COMMIT;"
```

Полная marker-сверка обоих видов отметок — §6b.4
`migration_preparation_runbook.md`; она этой ревизией не изменена.

---

## 4. Стоп-условия

Остановитесь и не переходите к следующему шагу, если:

- plan вернул любой `blocker`;
- `apply_ready = false`;
- apply вернул `refused` по любой причине — в этом случае **ничего не
  изменилось**, нужен новый plan;
- verify вернул `passed = false`;
- handover plan после recovery всё ещё показывает `local_target_mismatch` или
  любой `unproven`;
- handover-команда содержит CORE manifest, любой из пяти CORE run ID или общий
  snapshot `reminder_handover.v5.json` — это чужая волна, выполнять нельзя;
- `altegio-outbox-worker` не вернулся в `running` после шага 7.

Во всех этих случаях никаких ручных правок в PostgreSQL не делается. Причина
разбирается, при необходимости запрашивается отдельное решение владельца.

---

## 5. Артефакты

| Файл | Права | Содержит | В Git |
|---|---|---|---|
| `/recovery/easyweek-failed-cancellation-plan.json` | `0600` | booking UUID, технические id, digests | нет |
| `/recovery/easyweek-failed-cancellation-apply.json` | `0600` | id изменённых строк, счётчики, digests | нет |

Каталог монтируется из
`${EASYWEEK_FAILED_CANCELLATION_RECOVERY_STATE_DIR:-./outputs/easyweek_failed_cancellation_recovery}`
и имеет права `0700`. Ни один артефакт не содержит имён, телефонов, email и
webhook payload.

Артефакты HANNA handover — отдельные от CORE и друг от друга, чтобы evidence
одного запуска не затирало другое:

| Файл | Шаг |
|---|---|
| `/migration/state/reminder_handover.hanna.after-failed-cancellation.v5.json` | snapshot шага 5, читается шагами 7 и 9 |
| `/migration/state/reminder_handover.hanna.after-failed-cancellation.apply-report.v3.json` | apply report шага 7, читается шагом 9 |
| `/migration/state/reminder_handover.hanna.after-failed-cancellation.repeat-apply-report.v3.json` | отдельный report проверки идемпотентности |

CORE-файл `/migration/state/reminder_handover.v5.json` в этой процедуре не
используется и не перезаписывается.
