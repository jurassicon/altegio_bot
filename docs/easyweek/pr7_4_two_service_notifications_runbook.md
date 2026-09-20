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
После этого hotfix старые recovery `plan.json`/apply report version 1
несовместимы: не переиспользовать и не редактировать их, а обязательно создать
новый plan и новый digest после deploy.

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
structurally_proven≈14
allowed_records≈4
disallowed_records≈10
contract_excluded_records≈6
reminders_to_create=0..8
blockers=0
truncated=false
apply_ready=true
```

Ровно восемь jobs не требуется: прошедший `reminder_24h` получает
`window_passed`, а если прошло и двухчасовое окно, recovery ничего не создаёт.
`Nagelservice` и mixed allowed/disallowed pair получают
`category_not_allowed`. Exact `multi_service_custom_duration_unsupported`
получает `contract_not_supported`, не получает snapshot/dedupe/reminder и не
блокирует независимые allowed records. Эти числа — текущий наблюдаемый
baseline, а не постоянный контракт. Любой API timeout/429/5xx, неполный каталог,
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
Apply запускать только при `blockers=0` и `apply_ready=true`; contract-excluded
records не входят в mutation set и reminders не получают.

## 6. Verify snapshot/apply/result

```bash
docker compose -p altegio_bot --profile ops run --rm --build \
  easyweek-multi-service-reminder-recovery verify \
  --snapshot /recovery/plan.json \
  --apply-report /recovery/apply-report.json
```

Требуются `passed=true`, `counts_match=true`, `created_jobs_match=true` и
пустые `unexpected_job_ids`, `unexpected_outbox_ids`, `disallowed_jobs`,
`contract_excluded_jobs`, `overdue_jobs`, `digest_mismatches`,
`identity_mismatches`, `state_mismatch_record_ids`. Проверка охватывает tagged
и untagged reminder jobs, новые outbox rows и PII-free digests Record, Client и
RecordService для contract-excluded records. До открытия send fence ожидается
только status `queued`; `subsequent_job_ids` и `subsequent_outbox_ids` должны
быть пустыми.

## 7. Общий reminder preflight

Recovery verify доказывает точность созданных обязательств; общий PR-8
preflight независимо доказывает, что **каждая открытая EasyWeek reminder job**
прямо сейчас проходит тот же live API guard, что и outbox worker:

Post-recovery `easyweek_multi_service_preflight` принимает digest-валидный
snapshot из payload recovery job, даже если
`Record.raw.easyweek.multi_service_snapshot` отсутствует. Это ожидаемый
embedded-only контракт §34.5, а не повод делать запрещённый Record backfill;
при наличии stored и embedded snapshot их digest обязан совпадать с текущим
live proof. Новый contract-excluded subset участвует в readiness как
`structurally_proven + contract_excluded == active_multi_service`; при этом
`allowed + disallowed_by_category == structurally_proven`, а exclusion с
open job/non-terminal outbox остаётся `unexplained` и закрывает gate.

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

---

# PR-7.5 — Karlsruhe resource-shadow proof: rollout и rollback

Эта часть добавляет **третий независимый fence**
`EASYWEEK_RESOURCE_SHADOW_PROOF_ENABLED`. Он не расширяет контракт двух услуг,
не меняет `EASYWEEK_ALLOWED_SERVICE_CATEGORIES`, не включает отправку и не
трогает Durlach, Rastatt и Altegio. Единственная его задача — позволить общему
PR-7.4 proof распознать техническую resource-строку одного owner-approved
статического контракта Karlsruhe, чтобы 16 записей перестали быть
`multi_service_duplicate_ambiguous` и дошли до обычной all-categories
eligibility.

Ожидаемый результат для этих записей — `multi_service_category_not_allowed`.
Это доказанное безопасное подавление, а не разрешение отправлять `Nagelservice`.

## 13. Deploy: новый fence закрыт

До пересоздания контейнеров проверить в `easyweek.env`:

```dotenv
EASYWEEK_MULTI_SERVICE_NOTIFICATIONS_ENABLED=true
EASYWEEK_MULTI_SERVICE_SEND_ENABLED=false
EASYWEEK_RESOURCE_SHADOW_PROOF_ENABLED=false
```

При `false` поведение полностью совпадает с текущим PR-7.4: те же snapshot v1,
те же digests, те же jobs, те же fail-closed причины.

```bash
cd /opt/altegio_bot
docker compose -p altegio_bot config --quiet
docker compose -p altegio_bot up -d --build --force-recreate \
  altegio-easyweek-inbox-worker altegio-outbox-worker
```

## 14. Проверка code/profile/catalog contract до открытия fence

Статический контракт лежит в коде (`easyweek_resource_shadow_contract.py`) и
проверяется в diff владельцем. Он ограничен `provider=easyweek`,
`company_id=322579` и location UUID `8395fab6-7ee8-4702-88d9-fd78f92539c1`;
catalog UUID не хардкодятся и разрешаются заново по точному имени в полном
живом каталоге.

Сверить точную таблицу и revision в diff, затем убедиться, что живой каталог
отдаёт каждое имя ровно один раз и с категорией `Nagelservice`:

```bash
cd /opt/altegio_bot
docker compose -p altegio_bot run --rm --no-deps \
  --entrypoint /app/.venv/bin/python altegio-outbox-worker \
  -m altegio_bot.scripts.easyweek_multi_service_preflight \
  --limit 500 --pause-sec 1.10
```

При закрытом fence этот прогон обязан по-прежнему показывать стабильную
причину `multi_service_duplicate_ambiguous` и `ready=false`. Отсутствие
изменений здесь — и есть доказательство, что deploy ничего не поменял.

## 15. Открыть ТОЛЬКО resource-shadow fence

Send fence остаётся закрытым:

```dotenv
EASYWEEK_MULTI_SERVICE_NOTIFICATIONS_ENABLED=true
EASYWEEK_MULTI_SERVICE_SEND_ENABLED=false
EASYWEEK_RESOURCE_SHADOW_PROOF_ENABLED=true
```

```bash
cd /opt/altegio_bot
docker compose -p altegio_bot up -d --force-recreate \
  altegio-easyweek-inbox-worker altegio-outbox-worker
```

`docker compose restart` не перечитывает `env_file`, поэтому используется
`up -d --force-recreate` обоих сервисов: planning читает флаг в inbox, а общий
outbox читает его же для удержания resource-aware jobs.

## 16. Повторный multi-service preflight

```bash
cd /opt/altegio_bot
docker compose -p altegio_bot run --rm --no-deps \
  --entrypoint /app/.venv/bin/python altegio-outbox-worker \
  -m altegio_bot.scripts.easyweek_multi_service_preflight \
  --limit 500 --pause-sec 1.10
```

Ожидаемый наблюдённый baseline — ориентир для этого rollout, а не хардкод:

```text
active_multi_service=17
checked=17
structurally_proven=17
allowed=0
disallowed_by_category=17
contract_excluded=0
ambiguous=0
stale_snapshot_digest=0
unexplained=0
truncated=false
ready=true
```

Ключевой переход — `ambiguous=16 → 0` при `allowed=0`. Если `allowed` перестал
быть нулём, остановиться: это означало бы изменение category allowlist, которое
данный PR не разрешает.

## 17. Ожидаемое category suppression

Для всех Karlsruhe записей корректный исход — `multi_service_category_not_allowed`.
Проверить, что ни одна из них не получила обязательств:

```bash
cd /opt/altegio_bot
docker compose -p altegio_bot exec -T postgres sh -lc \
  'psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' <<'SQL'
\pset pager off
SELECT j.status, count(*)
FROM message_jobs j
JOIN records r ON r.id = j.record_id
WHERE j.provider = 'easyweek' AND r.company_id = 322579
GROUP BY j.status ORDER BY j.status;
SQL
```

Новых `queued` lifecycle/reminder jobs у этих записей быть не должно, равно как
и новых `OutboxMessage` и Meta/Chatwoot attempts.

## 18. Controlled suppression canary

Это **suppression canary**, а не send canary. Любая настоящая version 2
проекция состоит только из услуг статического Karlsruhe-контракта, а все они
относятся к категории `Nagelservice`, которой нет в production allowlist.
Значит корректно доказанная resource-shadow запись обязана завершиться
`multi_service_category_not_allowed` и не может создать job — ожидать здесь
queued job или клиентский рендер означало бы временно разрешить `Nagelservice`,
что запрещено §38.6.

### 18.1 Зафиксировать exact identity canary booking

Создать одну контролируемую будущую Karlsruhe booking ровно из двух разных
услуг контракта, одна из которых resource-backed (например
`Pediküre mit Gel-Lack`), и **выписать точный booking UUID этой записи** до
любой диагностики.

Дальше весь canary привязан только к этому UUID. В реальном филиале между
созданием booking и запросом появляются другие записи, поэтому запрещено
искать canary по «последним записям», `ORDER BY ... LIMIT`, имени клиента,
телефону, времени без UUID или ручным визуальным сопоставлением нескольких
строк: любой более старый resource-shadow Record с `jobs = 0` и `outbox = 0`
дал бы ложноположительный результат.

### 18.2 Identity-bound проверка Record

Подставить выписанный UUID в первую строку и выполнить блок целиком. UUID
передаётся как значение psql-переменной (`:'canary'`), а не склейкой SQL.

Первый запрос — жёсткая проверка identity: если по полному
`provider` + `company_id` + `easyweek_booking_uuid` найдено не ровно одна
запись, он падает с делением на ноль, и `ON_ERROR_STOP=1` останавливает весь
блок до печати деталей.

```bash
cd /opt/altegio_bot
CANARY_BOOKING_UUID='PASTE-CANARY-BOOKING-UUID-HERE'
docker compose -p altegio_bot exec -T \
  -e CANARY_BOOKING_UUID="$CANARY_BOOKING_UUID" postgres sh -lc \
  'psql -X -v ON_ERROR_STOP=1 -v canary="$CANARY_BOOKING_UUID" -U "$POSTGRES_USER" -d "$POSTGRES_DB"' <<'SQL'
\pset pager off
SELECT 1 / (count(*) = 1)::int AS exactly_one_canary_record
FROM records
WHERE provider = 'easyweek'
  AND company_id = 322579
  AND easyweek_booking_uuid = :'canary'::uuid;

SELECT r.id                                                              AS record_id,
       r.company_id                                                      AS company_id,
       r.easyweek_booking_uuid                                           AS booking_uuid,
       r.raw #>> '{easyweek,services_count}'                             AS services_count,
       r.raw #>> '{easyweek,multi_service_snapshot,version}'             AS snapshot_version,
       r.raw #>> '{easyweek,multi_service_snapshot,digest}'              AS snapshot_digest,
       r.raw #>> '{easyweek,multi_service_snapshot,resource_shadow_proof,proof_kind}'       AS proof_kind,
       r.raw #>> '{easyweek,multi_service_snapshot,resource_shadow_proof,contract_revision}' AS contract_revision,
       r.raw #>> '{easyweek,multi_service_snapshot,resource_shadow_proof,contract_digest}'   AS contract_digest,
       jsonb_array_length(r.raw #> '{easyweek,multi_service_snapshot,lines}')                AS snapshot_lines,
       r.raw #>> '{easyweek,multi_service_snapshot,lines,0,category}'    AS line_1_category,
       r.raw #>> '{easyweek,multi_service_snapshot,lines,1,category}'    AS line_2_category,
       (SELECT count(*) FROM message_jobs j WHERE j.record_id = r.id)    AS jobs,
       (SELECT count(*) FROM outbox_messages o WHERE o.record_id = r.id) AS outbox
FROM records r
WHERE r.provider = 'easyweek'
  AND r.company_id = 322579
  AND r.easyweek_booking_uuid = :'canary'::uuid;
SQL
```

Запрос печатает только технические поля. Имя клиента, телефон, e-mail, notes,
ссылки и любые другие PII в вывод не попадают.

Ожидаемый результат — ровно одна строка, и в ней одновременно:

```text
services_count    = 2
snapshot_version  = 2
proof_kind        = karlsruhe_resource_shadow
contract_revision = <текущая revision из easyweek_resource_shadow_contract.py>
contract_digest   = <непустой 64-символьный hex>
snapshot_lines    = 2
line_1_category   = Nagelservice
line_2_category   = Nagelservice
jobs              = 0
outbox            = 0
```

### 18.3 Suppression reason того же Record

Взять `record_id` из §18.2 — не из журнала, не «на глаз» — и проверить, что
именно для него inbox worker записал точный стабильный reason:

```bash
cd /opt/altegio_bot
CANARY_RECORD_ID='PASTE-CANARY-RECORD-ID-FROM-18.2-HERE'
docker compose -p altegio_bot logs --no-color --since 24h \
  altegio-easyweek-inbox-worker \
  | grep -E "record_id=${CANARY_RECORD_ID}[^0-9].*reason=multi_service_category_not_allowed"
```

Требуется хотя бы одна строка `easyweek lifecycle suppressed` или
`easyweek reminders suppressed`, содержащая одновременно точный
`record_id=<canary record_id>` и точный
`reason=multi_service_category_not_allowed`.

Строка `category_not_allowed` без этого `record_id` доказательством не
является: она может относиться к любой другой из 17 записей. Пустой вывод
grep — это **не** PASS, а STOP.

### 18.4 Structural proof остаётся отдельной проверкой

Общий preflight из §16 read-only и печатает только агрегаты. Он доказывает,
что во всём scope нет `ambiguous` и `unexplained`, но не доказывает, что
конкретная canary booking — та самая запись, которая стала
`structurally_proven`. Identity-bound запрос из §18.2 доказывает обратное:
именно этот Record получил version 2 snapshot и ноль обязательств, но ничего
не говорит об остальном scope.

Это разные утверждения, и нужны оба: агрегатный preflight `ready=true` **и**
зелёный identity-bound canary.

### 18.5 Fail-closed условия остановки rollout

Rollout останавливается, если верно хотя бы одно:

- exact canary Record не найден;
- найдено больше одной записи по полному identity;
- `multi_service_snapshot` отсутствует или `snapshot_version` не равен `2`;
- отсутствуют `proof_kind`, `contract_revision` или `contract_digest`;
- `proof_kind` не равен `karlsruhe_resource_shadow`;
- `contract_revision` не совпадает с текущей revision в коде;
- `snapshot_lines` не равен `2`;
- хотя бы одна из `line_1_category` / `line_2_category` не `Nagelservice`;
- `jobs` или `outbox` больше нуля;
- в логах нет строки с exact `record_id` и
  `reason=multi_service_category_not_allowed`;
- `EASYWEEK_ALLOWED_SERVICE_CATEGORIES` изменился;
- preflight из §16 не `ready=true` либо canary остаётся `ambiguous`.

При любом из этих условий выполнить rollback из §19 и не открывать общий send
fence.

Runtime rendering и фактическая отправка version 2 пары доказываются
автоматизированными integration-тестами
(`test_open_fences_render_a_resource_shadow_pair_once_each_with_one_total`,
`test_a_v2_reminder_makes_exactly_one_live_proof_not_two`), а не production-
процедурой: воспроизводить их на проде потребовало бы временно разрешить
`Nagelservice`, чего этот PR не допускает.

## 19. Rollback нового fence

При любой аномалии закрыть сначала именно новый fence:

```dotenv
EASYWEEK_RESOURCE_SHADOW_PROOF_ENABLED=false
```

```bash
cd /opt/altegio_bot
docker compose -p altegio_bot up -d --force-recreate \
  altegio-easyweek-inbox-worker altegio-outbox-worker
docker compose -p altegio_bot ps altegio-easyweek-inbox-worker altegio-outbox-worker
```

Уже созданные resource-aware jobs (snapshot version 2) остаются `queued`, не
расходуют attempts и не вызывают внешние сервисы. Обычные Durlach/Rastatt пары
версии 1, single-service и Altegio продолжают работать без изменений: новый
fence их не касается.

## 20. Запрет на открытие общего send fence

`EASYWEEK_MULTI_SERVICE_SEND_ENABLED=true` запрещено включать, пока
одновременно не зелёные все три выполнимые проверки:

1. multi-service preflight (`ready=true`) из §16;
2. общий reminder preflight (`ready=true`) из §7;
3. существующий PR-7.4 send canary из §8 — на разрешённой категории и обычной
   version 1 паре.

Пункт 3 намеренно остаётся PR-7.4 canary версии 1: он и есть проверка общего
send fence. Suppression canary из §18 к нему не относится и send-canary не
является. Открытие общего send fence не требует и не разрешает менять
`EASYWEEK_ALLOWED_SERVICE_CATEGORIES`: ни один из этих шагов не добавляет
`Nagelservice` в allowlist.

Отдельно: шесть `deadline_expired` reminder jobs `13934`–`13939` — это
операторское rollout-состояние, а не дефект resource-shadow. Данный PR их не
восстанавливает, не отменяет и не отправляет; их судьба требует отдельного
операторского решения вне этого PR и не является условием его завершения.

---

# PR-7.5 — operator-only recovery старых snapshot version 1

Этот раздел применяется **после** §§13–20, когда resource-shadow fence уже
открыт и повторный multi-service preflight показал
`stale_snapshot_digest > 0` при `ambiguous=0`.

Причина такого состояния штатная: записи, доказанные до resource-shadow proof,
хранят корректный snapshot version 1, а сегодняшний общий resolver доказывает
для них version 2. §38.3 запрещает молча адаптировать старый snapshot — его
меняет только отдельный операторский recovery, описанный ниже.

Единственная мутация recovery — замена ровно одного JSONB-ключа
`Record.raw.easyweek.multi_service_snapshot`. Ни MessageJob, ни OutboxMessage,
ни Client, ни RecordService, ни другие ключи `Record.raw` не меняются, и ни
одного обращения к Meta, Chatwoot или mutation API не выполняется.

## 21. Deploy с закрытыми production-флагами

Recovery выполняется на уже задеплоенном коде этой ветки. Send fence обязан
оставаться закрытым:

```dotenv
EASYWEEK_MULTI_SERVICE_NOTIFICATIONS_ENABLED=true
EASYWEEK_MULTI_SERVICE_SEND_ENABLED=false
EASYWEEK_RESOURCE_SHADOW_PROOF_ENABLED=true
```

```bash
cd /opt/altegio_bot
docker compose -p altegio_bot -f docker-compose.yml -f docker-compose.chatwoot-internal.yml config --quiet
docker compose -p altegio_bot -f docker-compose.yml -f docker-compose.chatwoot-internal.yml up -d --build --force-recreate \
  altegio-easyweek-inbox-worker altegio-outbox-worker
```

## 22. Проверка фактических значений флагов в обоих workers

Проверяются значения внутри уже запущенных контейнеров, а не содержимое файла:

```bash
cd /opt/altegio_bot
docker compose -p altegio_bot -f docker-compose.yml -f docker-compose.chatwoot-internal.yml exec -T altegio-easyweek-inbox-worker sh -lc \
  'printenv EASYWEEK_MULTI_SERVICE_NOTIFICATIONS_ENABLED EASYWEEK_MULTI_SERVICE_SEND_ENABLED EASYWEEK_RESOURCE_SHADOW_PROOF_ENABLED'
docker compose -p altegio_bot -f docker-compose.yml -f docker-compose.chatwoot-internal.yml exec -T altegio-outbox-worker sh -lc \
  'printenv EASYWEEK_MULTI_SERVICE_NOTIFICATIONS_ENABLED EASYWEEK_MULTI_SERVICE_SEND_ENABLED EASYWEEK_RESOURCE_SHADOW_PROOF_ENABLED'
```

Требуется `true`, `false`, `true` в обоих сервисах. Любое другое сочетание —
STOP: apply откажется и при открытом send fence, и при закрытом proof fence.

## 23. Приватный каталог для plan и apply report

```bash
cd /opt/altegio_bot
install -d -m 0700 outputs/easyweek_multi_service_recovery
test "$(stat -c '%a' outputs/easyweek_multi_service_recovery)" = "700"
```

## 24. Read-only plan

`plan` не меняет ни одной строки: он завершает сессию rollback, делает paced
live GET booking и полного каталога, строит target snapshot общим production
resolver и замораживает scope.

```bash
cd /opt/altegio_bot
docker compose -p altegio_bot -f docker-compose.yml -f docker-compose.chatwoot-internal.yml --profile ops run --rm --build \
  easyweek-multi-service-snapshot-recovery plan \
  --plan /recovery/snapshot-plan.json \
  --apply-report /recovery/snapshot-apply.json \
  --limit 500 \
  --pause-sec 1.10
```

Ожидаемый первый production baseline — ориентир, а не хардкод:

```text
candidates=5
source_version_1=5
target_version_2=5
blocked=0
truncated=false
apply_ready=true
```

stdout содержит только агрегаты, стабильные reason codes и технические Record
ID. Booking UUID есть только в приватном файле: его нельзя коммитить,
публиковать или прикладывать к общедоступным тикетам.

## 25. Проверка ожидаемых technical Record IDs

Frozen plan хранит агрегаты в `.summary`, а dispositions — в `.records`.
Команда ниже читает именно эту схему, вычисляет `migrate_record_ids` из
`.records` и **падает с ненулевым кодом**, если хоть одно ожидание не
выполнено. Список ожидаемых Record ID задаётся оператором из независимого
read-only SQL — первый подтверждённый rollout дал `8205 8206 8207 8208 8238`.

```bash
cd /opt/altegio_bot
EXPECTED_MIGRATE_IDS='[8205,8206,8207,8208,8238]'
jq -e --argjson expected "$EXPECTED_MIGRATE_IDS" '
  (.summary | objects) as $s
  | (.records | arrays) as $r
  | {
      candidates: $s.candidates,
      source_version_1: $s.source_version_1,
      target_version_2: $s.target_version_2,
      blocked: $s.blocked,
      truncated: $s.truncated,
      apply_ready: $s.apply_ready,
      migrate_record_ids: ([$r[] | select(.disposition == "migrate") | .record_id] | sort),
      blocked_record_ids: ([$r[] | select(.disposition == "blocked") | .record_id] | sort),
      reasons: ([$r[] | select(.refusal_reason != null) | .refusal_reason] | unique)
    }
  | select(
      .candidates == 5
      and .source_version_1 == 5
      and .target_version_2 == 5
      and .blocked == 0
      and .truncated == false
      and .apply_ready == true
      and .migrate_record_ids == $expected
    )
' outputs/easyweek_multi_service_recovery/snapshot-plan.json
```

Вывод печатает только технические Record ID и стабильные reason codes: booking
UUID, имена услуг, имена клиентов и телефоны в него не попадают.

`jq -e` завершается ненулевым кодом, если `select` не пропустил объект или если
`.summary`/`.records` отсутствуют либо имеют неверный тип. Пустой вывод — это
STOP, а не PASS: остановиться, разобраться и не применять план.

## 26. Получение plan digest

```bash
cd /opt/altegio_bot
PLAN_DIGEST="$(jq -r '.plan_digest' outputs/easyweek_multi_service_recovery/snapshot-plan.json)"
test "${#PLAN_DIGEST}" = "64"
echo "${PLAN_DIGEST}"
```

## 27. Apply с exact digest и confirmation phrase

Apply перечитывает frozen plan, проверяет его digest, заново выполняет полный
live booking/catalog proof, сравнивает весь текущий scope с замороженным,
проверяет configuration и contract digest и возраст плана, и только потом, в
одной транзакции, блокирует точные Record строки через `FOR UPDATE`, ещё раз
проверяет state digests и полное отсутствие jobs/outbox и заменяет один ключ.

```bash
cd /opt/altegio_bot
docker compose -p altegio_bot -f docker-compose.yml -f docker-compose.chatwoot-internal.yml --profile ops run --rm --build \
  easyweek-multi-service-snapshot-recovery apply \
  --plan /recovery/snapshot-plan.json \
  --apply-report /recovery/snapshot-apply.json \
  --plan-digest "$PLAN_DIGEST" \
  --confirm "migrate easyweek multi-service snapshots $PLAN_DIGEST" \
  --pause-sec 1.10 \
  --max-snapshot-age-sec 600
```

Любой drift откатывает **всю** волну: частично обновлённых Records не бывает.

## 27a. Неопределённый результат apply: повтор той же команды

Контейнер apply одноразовый. Он может успеть закоммитить транзакцию и всё
равно исчезнуть до того, как вывод дойдёт до оператора или apply report
запишется. В этом случае **безопасно повторить ровно ту же команду из §27** с
тем же frozen plan, тем же digest и той же фразой.

Повтор различает три состояния и сам выбирает исход:

1. Все целевые Records всё ещё exact source version 1, остальной scope не
   изменился — обычный атомарный apply. В отчёте
   `outcome = applied`, а `migrated_this_run_record_ids` содержит все ID.

2. Все целевые Records уже имеют exact frozen target version 2 — повторное
   доказательство identity, текущего contract, живого target digest,
   неизменности всех нецелевых полей Record и отсутствия jobs/outbox, **без
   единой мутации БД**. В отчёте `outcome = already_applied`,
   `migrated_this_run_record_ids` пуст, `already_applied_record_ids` содержит
   все ID, а `mutation_counts.records_snapshot_migrated = 0`. Этот прогон
   заново записывает валидный apply report, пригодный для §28.

3. Что-либо между: часть Records мигрирована, а часть нет; чужой version 2;
   пропавший Record; появившийся job или outbox; изменённый соседний ключ
   `Record.raw`; live booking/catalog/contract drift; новый v1 кандидат.
   Это **не** идемпотентность: команда завершается ненулевым кодом со
   стабильной причиной (`partial_apply_detected`, `record_state_changed`,
   `job_state_changed`, `outbox_state_changed`, `scope_drift`) и ничего не
   меняет.

Различать исходы по отчёту. Проверка самодостаточна: она читает **текущий**
frozen plan и привязывает отчёт к нему по `plan_digest` и по точному составу
migrate IDs. Это существенно: при отказе apply файл отчёта не
перезаписывается, поэтому на постоянном пути может лежать отчёт от прежнего
плана, и проверка без такой привязки дала бы ложный успех.

```bash
cd /opt/altegio_bot
PLAN=outputs/easyweek_multi_service_recovery/snapshot-plan.json
REPORT=outputs/easyweek_multi_service_recovery/snapshot-apply.json
EXPECTED_PLAN_DIGEST="$(jq -er '.plan_digest' "$PLAN")"
EXPECTED_MIGRATE_IDS="$(jq -ec '[.records[] | select(.disposition == "migrate") | .record_id] | sort' "$PLAN")"
jq -e \
  --arg expected_plan_digest "$EXPECTED_PLAN_DIGEST" \
  --argjson expected_migrate_ids "$EXPECTED_MIGRATE_IDS" '
  def id_array:
    type == "array"
    and all(.[]; type == "number" and . == floor)
    and (length == (unique | length));
  select(
    (.version == 2)
    and (.mode == "apply-report")
    and (.plan_digest == $expected_plan_digest)
    and (.halted == false)
    and (.migrated_record_ids | id_array)
    and (.migrated_this_run_record_ids | id_array)
    and (.already_applied_record_ids | id_array)
    and ((.migrated_record_ids | sort) == $expected_migrate_ids)
    and (((.migrated_this_run_record_ids + .already_applied_record_ids) | sort) == (.migrated_record_ids | sort))
    and (((.migrated_this_run_record_ids + .already_applied_record_ids) | unique | length) == (.migrated_record_ids | length))
    and (
      (
        .outcome == "applied"
        and ((.migrated_this_run_record_ids | sort) == $expected_migrate_ids)
        and (.already_applied_record_ids == [])
        and (.mutation_counts.records_snapshot_migrated == ($expected_migrate_ids | length))
      )
      or
      (
        .outcome == "already_applied"
        and (.migrated_this_run_record_ids == [])
        and ((.already_applied_record_ids | sort) == $expected_migrate_ids)
        and (.mutation_counts.records_snapshot_migrated == 0)
      )
    )
    and (.mutation_counts.message_jobs_created == 0)
    and (.mutation_counts.message_jobs_changed == 0)
    and (.mutation_counts.outbox_messages_created == 0)
    and (.mutation_counts.outbox_messages_changed == 0)
    and (.mutation_counts.clients_changed == 0)
    and (.mutation_counts.record_services_changed == 0)
  )
  | {outcome, migrated_record_ids, migrated_this_run_record_ids,
     already_applied_record_ids, mutation_counts}
' "$REPORT"
```

Вывод содержит только технические Record ID и счётчики: booking UUID, имена
услуг и клиентские данные в него не попадают.

Ненулевой код возвращается при отсутствующем или нечитаемом plan либо report,
при отчёте от другого плана, при неверном `version`/`mode`/`halted`, при
нецелых или повторяющихся ID, при несовпадении состава ID с текущим планом,
при пересечении или неполноте двух списков, при несогласованном `outcome` и
при любом ненулевом mutation counter. Пустой вывод — это STOP, а не PASS.

Состояние 3 никогда не даёт отчёта: при нём файл не перезаписывается, а
предыдущий отчёт (если он есть) остаётся прежним — и именно поэтому проверка
выше сверяет `plan_digest`. Не «чинить» такое состояние повторными запусками —
нужен новый `plan` и разбор причины.

## 28. Verify

```bash
cd /opt/altegio_bot
docker compose -p altegio_bot -f docker-compose.yml -f docker-compose.chatwoot-internal.yml --profile ops run --rm --build \
  easyweek-multi-service-snapshot-recovery verify \
  --plan /recovery/snapshot-plan.json \
  --apply-report /recovery/snapshot-apply.json \
  --pause-sec 1.10
```

Требуется `passed=true` и пустые `missing_record_ids`,
`snapshot_mismatch_record_ids`, `live_proof_mismatch_record_ids`,
`non_target_raw_changed_record_ids`, `unexpected_job_ids`,
`unexpected_outbox_ids`, `still_version_1_record_ids`, при `counts_match=true`.

## 29. Повторный multi-service preflight

```bash
cd /opt/altegio_bot
docker compose -p altegio_bot -f docker-compose.yml -f docker-compose.chatwoot-internal.yml run --rm --no-deps \
  --entrypoint /app/.venv/bin/python altegio-outbox-worker \
  -m altegio_bot.scripts.easyweek_multi_service_preflight \
  --limit 500 --pause-sec 1.10
```

## 30. Ожидаемый переход preflight

Ключевой инвариант, а не точные динамические числа:

```text
stale_snapshot_digest: 5 -> 0
unexplained:           5 -> 0
ambiguous:                  0
truncated:              false
ready:          false -> true
```

`active_multi_service` и `structurally_proven` могут отличаться от прежних 18 и
15 из-за живой очереди — сравнивать нужно инварианты, а не зафиксированные
числа. Если `allowed` перестал быть нулём, остановиться: это означало бы
изменение category allowlist, которое не разрешено.

## 31. Rollback и fail-closed условия

У recovery нет «отмены»: он заменяет корректный доказанный v1 на корректный
доказанный v2, и обратная замена так же потребовала бы отдельного доказанного
плана. Поэтому вся защита стоит **до** мутации. Остановиться и не применять
план, если верно хотя бы одно:

- `apply_ready` не `true` или `blocked` больше нуля;
- `truncated` не `false`;
- `migrate_record_ids` не совпал с ожидаемым списком;
- флаги в контейнерах не `true` / `false` / `true`;
- plan старше `--max-snapshot-age-sec`;
- plan digest или confirmation phrase не совпали;
- apply отказал с любым `scope_drift`, `record_state_changed`,
  `job_state_changed`, `outbox_state_changed` или `configuration_digest_changed`;
- verify вернул `passed=false`.

При отказе apply никакие Records не изменены: достаточно устранить причину и
выполнить новый `plan`.

## 32. Запрет ручного SQL по Record.raw

Ручные `UPDATE` или `DELETE` по `records.raw` запрещены. Такой снимок не
проходит live proof, не имеет plan digest и не отражается в apply report, а
send-time guard и preflight его немедленно отвергнут.

## 33. Запрет повторного использования старого plan

Plan одноразовый **для новой волны**. После любого deploy, изменения
конфигурации, изменения contract revision или любого drift старый
`snapshot-plan.json` использовать нельзя: нужен новый `plan` и новый digest.
Bounded max snapshot age существует именно для этого.

Единственное исключение — немедленный same-plan retry из §27a после
неопределённого результата: та же команда, тот же digest, та же фраза, в
пределах `--max-snapshot-age-sec`. Это не повторное применение плана: apply
сам доказывает, что либо ещё ничего не записано, либо всё уже записано ровно
этим планом, и во втором случае не выполняет ни одной мутации. Если план
истёк — повтор запрещён, нужен новый `plan`.

## 34. Запрет открытия общего send fence

`EASYWEEK_MULTI_SERVICE_SEND_ENABLED=true` остаётся запрещённым и после
успешного snapshot recovery. Он требует всех независимых rollout gates из §20,
а также отдельного безопасного решения по шести `deadline_expired` reminder
jobs `13934`–`13939`. Эти шесть jobs не входят в snapshot recovery: данный
change их не отменяет, не восстанавливает и не отправляет.

---

# §38.8 — rollout после фикса count semantics

Фикс убирает единственное несогласованное правило: multi-service proof больше
не требует top-level `quantity == 2`. Authoritative whole-set count — это
`services_count`, а top-level `quantity` допускается как точный `1` или `2` и
ничего не разрешает сам по себе. Line-level `ordered_services[].quantity`
остаётся точным `1`. Полный live proof обязателен по-прежнему.

## 35. Deploy при закрытом send fence

```dotenv
EASYWEEK_MULTI_SERVICE_NOTIFICATIONS_ENABLED=true
EASYWEEK_RESOURCE_SHADOW_PROOF_ENABLED=true
EASYWEEK_MULTI_SERVICE_SEND_ENABLED=false
```

```bash
cd /opt/altegio_bot
docker compose -p altegio_bot -f docker-compose.yml -f docker-compose.chatwoot-internal.yml config --quiet
docker compose -p altegio_bot -f docker-compose.yml -f docker-compose.chatwoot-internal.yml up -d --build --force-recreate \
  altegio-easyweek-inbox-worker altegio-outbox-worker
```

## 36. Проверка фактических значений флагов в обоих контейнерах

```bash
cd /opt/altegio_bot
docker compose -p altegio_bot -f docker-compose.yml -f docker-compose.chatwoot-internal.yml exec -T altegio-easyweek-inbox-worker sh -lc \
  'printenv EASYWEEK_MULTI_SERVICE_NOTIFICATIONS_ENABLED EASYWEEK_RESOURCE_SHADOW_PROOF_ENABLED EASYWEEK_MULTI_SERVICE_SEND_ENABLED'
docker compose -p altegio_bot -f docker-compose.yml -f docker-compose.chatwoot-internal.yml exec -T altegio-outbox-worker sh -lc \
  'printenv EASYWEEK_MULTI_SERVICE_NOTIFICATIONS_ENABLED EASYWEEK_RESOURCE_SHADOW_PROOF_ENABLED EASYWEEK_MULTI_SERVICE_SEND_ENABLED'
```

Требуется `true`, `true`, `false` в обоих сервисах.

## 37. Свежий multi-service preflight

```bash
cd /opt/altegio_bot
docker compose -p altegio_bot -f docker-compose.yml -f docker-compose.chatwoot-internal.yml run --rm --no-deps \
  --entrypoint /app/.venv/bin/python altegio-outbox-worker \
  -m altegio_bot.scripts.easyweek_multi_service_preflight \
  --limit 500 --pause-sec 1.10
```

Ожидаемый переход — rollout evidence, а не контракт:

```text
active_multi_service=20
checked=20
structurally_proven: 16 -> 17
allowed=1
disallowed_by_category: 15 -> 16
contract_excluded=3
ambiguous: 1 -> 0
unexplained: 1 -> 0
stale_snapshot_digest=0
open_jobs=3
jobs_held_by_send_fence=3
ready: false -> true
```

Продолжать можно только при `ready=true`, `ambiguous=0`, `unexplained=0`,
`stale_snapshot_digest=0`, `truncated=false` и
`open_jobs == jobs_held_by_send_fence`.

## 38. Отдельный общий reminder preflight

```bash
cd /opt/altegio_bot
docker compose -p altegio_bot -f docker-compose.yml -f docker-compose.chatwoot-internal.yml run --rm --no-deps \
  --entrypoint /app/.venv/bin/python altegio-outbox-worker \
  -m altegio_bot.scripts.easyweek_reminder_preflight \
  --limit 500 --pause-sec 1.00
```

Он тоже обязан вернуть `ready=true`. Текущее состояние с `canceled=3` и jobs
`14143`/`14168`/`14304` при `ready=false` **не является** причиной менять count
proof и не разрешает открывать send fence. Эти jobs должны либо штатно стать
terminal после своего `run_at`, либо быть обработаны отдельной заранее
проверенной операторской процедурой очистки.

## 39. Owner canary

Проверить на активной записи владельца: две snapshot lines, digest совпадает,
`record_created` и reminder jobs в `queued` и удержаны send fence,
`attempts=0`, `Outbox=0`, dry render показывает обе услуги и правильный общий
total.

## 40. Открытие send fence

Только когда **оба** preflight зелёные:

```dotenv
EASYWEEK_MULTI_SERVICE_SEND_ENABLED=true
```

```bash
cd /opt/altegio_bot
docker compose -p altegio_bot -f docker-compose.yml -f docker-compose.chatwoot-internal.yml up -d --force-recreate altegio-outbox-worker
docker compose -p altegio_bot -f docker-compose.yml -f docker-compose.chatwoot-internal.yml ps altegio-outbox-worker
```

## 41. Проверка после открытия

Ожидается один `record_created` outcome, ровно один Outbox/provider attempt,
фактическое WhatsApp-сообщение владельцу с обеими услугами и корректным общим
total.

## 42. Rollback

```dotenv
EASYWEEK_MULTI_SERVICE_SEND_ENABLED=false
```

```bash
cd /opt/altegio_bot
docker compose -p altegio_bot -f docker-compose.yml -f docker-compose.chatwoot-internal.yml up -d --force-recreate altegio-outbox-worker
```

Jobs не удалять и `attempts` вручную не менять: удержанные jobs остаются
`queued` и повторно доказываются при следующем открытии fence.
