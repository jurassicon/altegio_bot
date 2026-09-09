# PR-7.4 — exactly-two-service notifications: rollout, recovery и rollback

Этот runbook относится только к EasyWeek booking с **ровно двумя разными
доказанными бизнес-услугами**. Runtime поддерживает lifecycle и reminders, но
operator recovery ниже восстанавливает исключительно ещё актуальные
`reminder_24h` и `reminder_2h`. Он не создаёт `record_created` задним числом,
не трогает review/retention/campaign, запрещённые категории и Altegio, не
вызывает Meta/Chatwoot и не создаёт `OutboxMessage`.

Все production-команды выполняет оператор на сервере из `/opt/altegio_bot` с
отдельно подтверждённым доступом. Сам PR такого доступа не предоставляет.

## 1. Deploy: planning открыт, отдельный send fence закрыт

До пересоздания контейнеров проверить в `easyweek.env`:

```dotenv
EASYWEEK_REMINDERS_ENABLED=true
EASYWEEK_REMINDER_API_GUARD_ENABLED=true
EASYWEEK_MULTI_SERVICE_NOTIFICATIONS_ENABLED=true
EASYWEEK_MULTI_SERVICE_SEND_ENABLED=false
```

`EASYWEEK_MULTI_SERVICE_SEND_ENABLED=false` удерживает digest-bound jobs в
`queued`, не расходует attempt и не вызывает Meta/Chatwoot. Общий outbox worker
останавливать не нужно: он продолжает обслуживать остальные уведомления.

```bash
cd /opt/altegio_bot
uv run alembic heads
docker compose -p altegio_bot config --quiet
docker compose -p altegio_bot up -d --build --force-recreate \
  altegio-easyweek-inbox-worker altegio-outbox-worker
```

`docker compose restart` не перечитывает `env_file`, поэтому в этом runbook
всегда используется `up -d --force-recreate`.

## 2. Приватный каталог snapshot/report вне Git

По умолчанию one-off service монтирует git-ignored host-каталог
`outputs/easyweek_multi_service_recovery` в `/recovery`:

```bash
cd /opt/altegio_bot
install -d -m 0700 outputs/easyweek_multi_service_recovery
test "$(stat -c '%a' outputs/easyweek_multi_service_recovery)" = "700"
```

Snapshot содержит реальные booking UUID и technical IDs. CLI пишет snapshot и
apply report атомарно с mode `0600`; их нельзя коммитить, публиковать или
прикладывать к общедоступным тикетам.

## 3. Read-only plan

`plan` заново выбирает весь текущий future scope, делает paced GET каждой
booking и полного каталога, повторяет основной PR-7.4 proof и только затем
сравнивает jobs/outbox. SQL-мутирующих команд, commit и внешней отправки нет.

```bash
docker compose -p altegio_bot --profile ops run --rm --build \
  easyweek-multi-service-reminder-recovery plan \
  --snapshot /recovery/plan.json \
  --apply-report /recovery/apply-report.json \
  --limit 500 \
  --pause-sec 1.10
```

Ожидаемый первый production baseline — ориентир, а не хардкод:

```text
records_seen≈20
structurally_proven≈20
allowed_records≈4
disallowed_records≈16
reminders_to_create=0..8
blockers=0
truncated=false
apply_ready=true
```

Ровно восемь jobs не требуется: прошедший `reminder_24h` получает
`window_passed`, а если прошло и двухчасовое окно, recovery ничего не создаёт.
`Nagelservice` и mixed allowed/disallowed pair получают
`category_not_allowed`. Любой API timeout/429/5xx, неполный каталог,
identity/price/time/digest mismatch или processing/non-terminal outbox делает
план неприменимым.

## 4. Ручная проверка агрегатов и точного scope

Сначала проверить stdout plan и убедиться, что фактические числа объяснимы.
Для PII-free просмотра только технических record/job решений:

```bash
jq '{planned_at, plan_digest, configuration_digest, summary,
     records: [.records[] | {
       record_id, company_id, starts_at,
       multi_service_snapshot_digest, live_business_pair_digest,
       category_proof_digest, eligibility,
       reminders: [.reminders[] | {
         job_type, run_at, disposition, existing_job_ids, existing_outbox_ids
       }]
     }]}' outputs/easyweek_multi_service_recovery/plan.json
```

Не печатать поле `booking_uuid`. При любом неожиданном record/disposition
остановиться, исправить причину и выполнить новый plan. Не редактировать
snapshot вручную: его canonical digest перестанет совпадать.

## 5. Apply с точным digest и confirmation phrase

Snapshot TTL по умолчанию — 600 секунд. Digest и фраза берутся только из
свежего plan:

```bash
PLAN_DIGEST="$(jq -r '.plan_digest' outputs/easyweek_multi_service_recovery/plan.json)"
test "${#PLAN_DIGEST}" = "64"
docker compose -p altegio_bot --profile ops run --rm --build \
  easyweek-multi-service-reminder-recovery apply \
  --snapshot /recovery/plan.json \
  --apply-report /recovery/apply-report.json \
  --plan-digest "$PLAN_DIGEST" \
  --confirm "create easyweek multi-service reminders $PLAN_DIGEST" \
  --pause-sec 1.10 \
  --max-snapshot-age-sec 600
```

Apply повторяет live booking/catalog proof, сверяет полный замороженный scope,
затем в одной PostgreSQL-транзакции блокирует точные Record/Client/MessageJob/
Outbox rows и ещё раз проверяет локальное состояние. Любой drift откатывает
всю транзакцию. Разрешённая мутация одна: insert отсутствующих `queued`
`reminder_24h`/`reminder_2h` с canonical run_at/dedupe/payload. Existing
queued/done не переписываются, processing блокирует волну, canceled/failed не
воскрешаются. Повторный apply того же snapshot идемпотентен (`created=0`).

## 6. Verify snapshot/apply/result

```bash
docker compose -p altegio_bot --profile ops run --rm --build \
  easyweek-multi-service-reminder-recovery verify \
  --snapshot /recovery/plan.json \
  --apply-report /recovery/apply-report.json
```

Требуются `passed=true`, `counts_match=true`, `created_jobs_match=true` и
пустые `unexpected_job_ids`, `unexpected_outbox_ids`, `disallowed_jobs`,
`overdue_jobs`, `digest_mismatches`, `identity_mismatches`. До открытия send
fence ожидается только status `queued`; `subsequent_job_ids` и
`subsequent_outbox_ids` должны быть пустыми.

## 7. Общий reminder preflight

Recovery verify доказывает точность созданных обязательств; общий PR-8
preflight независимо доказывает, что **каждая открытая EasyWeek reminder job**
прямо сейчас проходит тот же live API guard, что и outbox worker:

Post-recovery `easyweek_multi_service_preflight` принимает digest-валидный
snapshot из payload recovery job, даже если
`Record.raw.easyweek.multi_service_snapshot` отсутствует. Это ожидаемый
embedded-only контракт §34.5, а не повод делать запрещённый Record backfill;
при наличии stored и embedded snapshot их digest обязан совпадать с текущим
live proof.

```bash
docker compose -p altegio_bot run --rm --no-deps \
  --entrypoint /app/.venv/bin/python altegio-outbox-worker \
  -m altegio_bot.scripts.easyweek_reminder_preflight \
  --limit 500 --pause-sec 1.00
```

Требуется `ready=true`; `candidate_count == checked_count`, все outcomes —
`proven_current`, очередь не truncated. Это обязательный отдельный gate.

## 8. Controlled canary без открытия production send fence

До bulk-send проверить штатный future canary на заранее разрешённом тестовом
получателе: две разные услуги `Wimpernverlängerung`, standard price/duration,
без discount. Его webhook должен создать digest-bound lifecycle/reminder job,
а `EASYWEEK_MULTI_SERVICE_SEND_ENABLED=false` — удержать её в `queued` с
`attempts=0` и без Outbox/Meta/Chatwoot. Renderer в staging/capture-provider
должен показать обе услуги по одному разу с индивидуальными ценами и одной
итоговой суммой.

Canary не является разрешением отправлять реальному клиенту. Если нужен живой
WhatsApp canary, требуется отдельное операторское решение и заранее
разрешённый recipient; recovery CLI сам его не отправляет.

## 9. Открытие multi-service send fence

Только после зелёных recovery verify, общего reminder preflight и canary:

```dotenv
EASYWEEK_MULTI_SERVICE_NOTIFICATIONS_ENABLED=true
EASYWEEK_MULTI_SERVICE_SEND_ENABLED=true
```

## 10. Пересоздание только читающего send flag сервиса

Planning уже открыт в inbox; после изменения только send fence пересоздаётся
только общий outbox worker:

```bash
docker compose -p altegio_bot up -d --force-recreate altegio-outbox-worker
docker compose -p altegio_bot ps altegio-outbox-worker
```

## 11. Проверка jobs/outbox после открытия

Повторить verify и общий reminder preflight командами из §§6–7. Нормальный
последующий переход expected job (`processing`/`done`/guard-canceled) и связанный
outbox отображаются отдельно как `subsequent_*`, а не маскируются под apply-
мутацию. Для каждого фактического send до Meta выполняется live GET + catalog
proof. Cancel/reschedule/completed/service/price/category/location drift штатно
останавливает job без Meta/Chatwoot.

Не использовать прямые `UPDATE`/`INSERT` SQL для исправления результата.

## 12. Rollback: закрыть только multi-service send fence

При любой аномалии немедленно вернуть:

```dotenv
EASYWEEK_MULTI_SERVICE_SEND_ENABLED=false
```

и пересоздать только outbox:

```bash
docker compose -p altegio_bot up -d --force-recreate altegio-outbox-worker
docker compose -p altegio_bot ps altegio-outbox-worker
```

Уже queued multi-service jobs остаются в `queued`, не расходуют attempts и не
вызывают внешние сервисы. Общий outbox продолжает single-service, Altegio,
review/retention/campaign. Если нужно также остановить новое multi planning,
отдельно вернуть `EASYWEEK_MULTI_SERVICE_NOTIFICATIONS_ENABLED=false` и
пересоздать inbox и outbox. Rollback не удаляет records, jobs, snapshot или
apply report; их судьба — отдельное операторское решение.
