# Runbook: подготовка волны миграции Altegio → EasyWeek

План: §29 (ревизия 23). Этап **предшествует** мигратору из
`pr11_1_cutover_runbook.md` и ничего в нём не заменяет.

Перенос одного мастера занимал часы, и почти всё это время уходило не на
перенос: поиск услуг, сопоставление по глазам, регенерация экспорта клиентов,
ручное создание карточек, перенос идентификаторов между командами. Этот runbook
— один процесс на всю подготовку.

**Read-only часть бота не останавливает.** Остановка уведомлений и всё, что
делает мигратор, начинается позже и описано в существующем runbook.

---

## 0. Что понадобится

- `manifest.json` — тот же файл, что у мигратора. Может быть неполным: услуги
  этой волны как раз и предлагает подготовка. `selected_altegio_staff_ids`,
  `staff` и `easyweek_location_*` должны быть заполнены — это описание волны,
  и его выбирает человек.
- Altegio company id филиала (Karlsruhe `758285`, Rastatt `1271200`).
- Cutover — ровно тот, с которым потом пойдёт apply.

**Каталог состояния содержит персональные данные.** Локально это
`outputs/easyweek_migration_prepare/`, в контейнере — `/migration/state`. Он не
коммитится, файлы создаются с правами `0600`, каталог — `0700`. Не пересылайте
эти файлы и не вставляйте их в тикеты.

---

## 1. Собрать данные и получить предложения

Локально:

```bash
cd /path/to/altegio_bot && uv run python -m altegio_bot.scripts.easyweek_migration_prepare prepare --manifest outputs/easyweek_migration/input/manifest.json --company-id 758285 --cutover-at 2026-09-01T00:00:00+02:00
```

На проде через ops-сервис:

```bash
cd /opt/altegio_bot && docker compose --profile ops run --rm --build easyweek-migration-prepare prepare --manifest /migration/input/manifest.json --company-id 758285 --cutover-at 2026-09-01T00:00:00+02:00
```

Команда читает Altegio, каталог EasyWeek и клиентов EasyWeek. Она **ничего не
создаёт**: у этого режима нет пути к мутации.

На stdout — машинный отчёт без персональных данных. Читать в нём блок `ready`,
в котором пять отдельных ответов, а не одно слово:

| Поле | Что означает |
|---|---|
| `customers_ready` | клиенты, чей UUID реально увиден в EasyWeek |
| `mapping_ready` | все услуги волны сопоставлены и согласованы |
| `records_ready` | записи, готовые к переносу прямо сейчас |
| `records_needing_manual_work` | записи с индивидуальной ценой, длительностью или несколькими услугами — подготовка их не трогает |
| `blocked_by_technical_error` | поиски, которые не дали ответа, и незавершённые создания |

`blocked_by_technical_error > 0` — это **не** «клиентов нет». Это «мы не смогли
посмотреть». Повторите шаг 1 после того, как причина устранена.

---

## 2. Прочитать решения глазами

Файл с персональными данными:

```bash
cat outputs/easyweek_migration_prepare/operator_review.json
```

На проде:

```bash
sudo cat /opt/altegio_bot/outputs/easyweek_migration_prepare/operator_review.json
```

В нём три блока.

`customers` — имя и телефон из источника, email (только если он действительно
есть), количество связанных записей, `review_digest` и рядом с каждым клиентом
напоминание, что **создание карточки не переносит историю визитов**. Клиент,
созданный сейчас, выглядит в EasyWeek как пришедший впервые.

`service_mapping` — исходный service ID и точное имя, целевой service UUID и
точное имя, цена с валютой, длительность, количество затронутых записей,
`staff_availability` с доказательством, `drift_fields` и `review_digest`.

**`review_digest` — это то, что вы копируете в команду подтверждения.** Он
строится из того же канонического review payload, который показан оператору.
Для клиента он покрывает контакты, source identity и record IDs, смысл свежего
lookup, EasyWeek customer UUID, blocker/review evidence, intended action и
состояние correction. Для услуги — исходные имя, цены, service-line и booking
duration, фактических мастеров, target identity/baseline и доказательство
доступности. Если решение или хотя бы одно из этих доказательств изменилось,
дайджест другой, и старое подтверждение больше не действует. Старые pending или
confirmed решения формата v1 без полного evidence автоматически write не
разрешают: нужен свежий review.

`records` — каждая запись волны: `altegio_record_id`, мастер, начало и конец в
Europe/Berlin (плюс тот же момент в UTC), длительность, услуга, цена и
`price_to_pay`, телефон клиента и причина блокировки, если она есть. Это тот
список, который сверяют с экраном Altegio. Время, попавшее в переход на зимнее
или летнее время и не имеющее единственного значения, показано как `null` — не
приблизительно.

Причины блокировки, которые встречаются чаще всего:

| `blocked_reason` | Что делать |
|---|---|
| `source_name_not_split` | в источнике только полное имя. Разделите его сами (шаг 3, `--correct-customer`) — автоматически оно не делится. Исправление сохраняется и переживает следующий `prepare` |
| `correction_source_identity_changed` | исходные данные клиента изменились после исправления | посмотреть заново: прежнее исправление больше не применяется автоматически |
| `source_customers_share_phone` | на одном номере два разных клиента Altegio. Слить их автоматически нельзя; разберитесь в Altegio |
| `customer_ambiguous` | в EasyWeek две карточки на один номер. Разберитесь в EasyWeek |
| `customer_already_exists` | клиент уже есть; создавать нечего, он попал в directory |
| `lookup_undetermined` | ответа не было. Не «нет клиента» — повторите шаг 1 |

---

## 3. Подтвердить услуги и клиентов

Ни одна команда этапа не читает stdin: Docker без TTY, закрытый pipe и EOF
согласием не являются. Согласие — это явные аргументы.

**Порядок всегда один: сначала review из шага 2, потом команда.** Одиночное
подтверждение принимает не идентификатор, а пару `ИДЕНТИФИКАТОР=DIGEST`, где
digest — это `review_digest`, напечатанный рядом с этим самым элементом. Голый
идентификатор командой отклоняется: он означал бы «подтверждаю то, что сегодня
лежит под этим номером», а человек, читающий список, имеет в виду не это.

`confirm` не верит сохранённому файлу. Он заново проверяет branch identity,
заново классифицирует записи, заново читает каталог и клиентов — и сверяет три
вещи: ваш digest, digest только что перестроенного предложения и внутреннюю
целостность сохранённого решения. Расхождение в любой из трёх — STOP, при
котором **ничего** не меняется: ни manifest, ни решения по клиентам.

Услуги, по одной (id и digest из `service_mapping`):

```bash
cd /path/to/altegio_bot && uv run python -m altegio_bot.scripts.easyweek_migration_prepare confirm --manifest outputs/easyweek_migration/input/manifest.json --company-id 758285 --cutover-at 2026-09-01T00:00:00+02:00 --confirm-service '6001=REVIEW_DIGEST_УСЛУГИ_6001' --confirm-service '6002=REVIEW_DIGEST_УСЛУГИ_6002'
```

Все однозначные сразу — только против дайджеста напечатанного списка
(`mapping.pending_digest` из шага 1):

```bash
cd /path/to/altegio_bot && uv run python -m altegio_bot.scripts.easyweek_migration_prepare confirm --manifest outputs/easyweek_migration/input/manifest.json --company-id 758285 --cutover-at 2026-09-01T00:00:00+02:00 --confirm-all-services --mapping-digest ДАЙДЖЕСТ_ИЗ_ШАГА_1
```

Если список изменился, команда откажет. Это не придирка: «да» было сказано
конкретному списку.

Исправить данные клиента (например, разделить полное имя):

```bash
cd /path/to/altegio_bot && uv run python -m altegio_bot.scripts.easyweek_migration_prepare confirm --manifest outputs/easyweek_migration/input/manifest.json --company-id 758285 --cutover-at 2026-09-01T00:00:00+02:00 --correct-customer +4915112345678 --first-name "Anna Maria" --last-name "Schmidt"
```

Подтвердить клиента, пропустить клиента, подтвердить весь список:

```bash
cd /path/to/altegio_bot && uv run python -m altegio_bot.scripts.easyweek_migration_prepare confirm --manifest outputs/easyweek_migration/input/manifest.json --company-id 758285 --cutover-at 2026-09-01T00:00:00+02:00 --confirm-customer '+4915112345678=REVIEW_DIGEST_КЛИЕНТА'
```

```bash
cd /path/to/altegio_bot && uv run python -m altegio_bot.scripts.easyweek_migration_prepare confirm --manifest outputs/easyweek_migration/input/manifest.json --company-id 758285 --cutover-at 2026-09-01T00:00:00+02:00 --skip-customer +4915112345678
```

```bash
cd /path/to/altegio_bot && uv run python -m altegio_bot.scripts.easyweek_migration_prepare confirm --manifest outputs/easyweek_migration/input/manifest.json --company-id 758285 --cutover-at 2026-09-01T00:00:00+02:00 --confirm-all-pending-customers --pending-digest ДАЙДЖЕСТ_ИЗ_ШАГА_1
```

Подтверждение привязано к показанным данным. Если данные изменились,
подтверждение снимается само, и клиент возвращается в `pending`.

Повторный `prepare` **не переспрашивает** про то, что не изменилось: при
неизменившихся входных данных `prepare` и `confirm` строят одинаковые
предложения и одинаковые дайджесты.

Исправление данных (`--correct-customer`) digest не требует — это не одобрение,
а замена. Но оно меняет digest и возвращает клиента в `pending`, так что
подтверждать придётся заново, уже глядя на исправленные значения. Исправить и
подтвердить одного клиента одной командой нельзя: команда откажет.

Исправление сохраняется отдельно и **переживает пересборку**: следующий
`prepare` или `confirm` накладывает его поверх свежих исходных данных и заново
считает digest, так что подтвердить исправленное предложение можно. Исправление
привязано к доказанной исходной identity клиента — телефону, его Altegio-карточкам
и составу связанных записей, — а не к имени. Если что-то из этого изменилось,
исправление становится stale, клиент блокируется с
`correction_source_identity_changed`, и его нужно посмотреть заново.
Уже созданного (`created`) или находящегося в процессе создания (`in_flight`)
клиента исправить нельзя.

---

## 4. Создать подтверждённых клиентов

Это единственная команда этапа, которая пишет в EasyWeek, и у неё **отдельное
разрешение**: типизируемый флаг и переменная окружения. Ни один из них по
отдельности разрешением не является. Это разрешение **не позволяет перенести ни
одной записи**.

Локально:

```bash
cd /path/to/altegio_bot && EASYWEEK_MIGRATION_ALLOW_CUSTOMER_CREATE=true uv run python -m altegio_bot.scripts.easyweek_migration_prepare create-customers --manifest outputs/easyweek_migration/input/manifest.json --company-id 758285 --cutover-at 2026-09-01T00:00:00+02:00 --i-authorise-creating-customers
```

На проде:

```bash
cd /opt/altegio_bot && docker compose --profile ops run --rm --build -e EASYWEEK_MIGRATION_ALLOW_CUSTOMER_CREATE=true easyweek-migration-prepare create-customers --manifest /migration/input/manifest.json --company-id 758285 --cutover-at 2026-09-01T00:00:00+02:00 --i-authorise-creating-customers
```

Что делает команда для каждого подтверждённого клиента: проверяет отсутствие
заново, ставит маркер на диск **до** запроса, отправляет **один** `POST`,
проверяет результат чтением карточки по UUID и точному номеру, и только тогда
записывает клиента в directory.

Если исход неизвестен — таймаут, разрыв, 5xx, `2xx` без читаемого UUID — запуск
**останавливается**. Это правильно. Просто запустите ту же команду ещё раз:
она начнёт с чтения и сама разберётся, состоялось создание или нет. Повторный
`POST` вслепую — это дубликат карточки поверх чужой истории визитов.

Если команда сообщает, что каталог состояния заблокирован, значит идёт другой
запуск. Дождитесь его. Файл блокировки удаляйте руками только если уверены, что
процесс мёртв.

---

## 5. Обновить план и получить verified dry-run id

Подготовка уже собрала merged manifest и customer directory. Дальше — обычный
dry-run существующего мигратора, запущенный так, что его digest берётся с
объекта отчёта, а не из «последнего файла в каталоге».

```bash
cd /path/to/altegio_bot && uv run python -m altegio_bot.scripts.easyweek_migration_prepare verify-dry-run --manifest outputs/easyweek_migration_prepare/manifest.proposed.json --company-id 758285 --cutover-at 2026-09-01T00:00:00+02:00
```

На проде:

```bash
cd /opt/altegio_bot && docker compose --profile ops run --rm --build easyweek-migration-prepare verify-dry-run --manifest /migration/state/manifest.proposed.json --company-id 758285 --cutover-at 2026-09-01T00:00:00+02:00
```

В ответе: `verified_dry_run_id`, пути к manifest и customer-directory и готовая
команда `next_command_after_a_clean_canary`. Идентификаторы больше не нужно
переносить руками.

`manifest.proposed.json` — аддитивное слияние: уже согласованные услуги
предыдущих волн не переписываются. Перед первым apply перенесите этот файл в
`/migration/input/manifest.json` (или укажите его путь мигратору напрямую) и
сохраните как artefact волны.

---

## 6. Дальше — существующий процесс без изменений

С этого места действует `pr11_1_cutover_runbook.md` целиком и без послаблений:
отключение нативных уведомлений EasyWeek, canary на одной именованной записи,
проверка отсутствия уведомления клиенту, apply с `--verified-dry-run-id` из
шага 5, `reconcile`, `reconcile --final`.

Подготовка не ослабляет ни одну из этих проверок и не даёт права пропустить
canary.

---

## 6b. Передача напоминаний после переноса

План: §30. Отдельный одноразовый процесс. Выполняется **после** того, как волна
перенесена и ledger подтвердил `status=created`.

Зачем он нужен. Мигратор намеренно не создаёт `MessageJob`. Сразу после переноса
у клиента есть запись в EasyWeek, её будущие напоминания всё ещё стоят в очереди
со стороны Altegio и указывают на запись, с которой уже никто не работает, а со
стороны EasyWeek напоминаний нет вообще.

Наблюдавшиеся при первом запуске числа (56, 84, 223) — это evidence того
конкретного дня, а не контракт. Каждый запуск считает всё заново.

**Что такое ownership marker и зачем он.** Altegio inbox и capture во время
handover намеренно не останавливаются: их остановка была бы куда большим
простоем, чем нужно этой процедуре. Значит доставка Altegio, уже летевшая в этот
момент или пришедшая минутой позже, всё равно доходит до обычного планировщика.
А он создаёт напоминания через `add_job()`, который при совпадении ключа
переводит отменённое задание обратно в очередь. Без отдельной отметки поздний
`create` переоткрывал ровно то напоминание, которое handover только что отменил,
а поздний `reschedule` добавлял новое под другим ключом — и у одной записи
оказывались открытые напоминания сразу с обеих сторон.

Поэтому apply записывает в ledger durable отметку: момент передачи владения и
digest плана, под которым это произошло. Отметка ставится в той же транзакции и
последней — волна, откатившаяся по любой причине, отметки не оставляет, а
существующая отметка всегда означает, что отмена закоммичена.

Дальше отметку читают два места. Обычный планировщик Altegio — перед созданием
напоминания, внутри своей транзакции, так что доставка, ждавшая блокировки во
время apply, увидит отметку сразу после commit. И сам outbox — непосредственно
перед Meta, как вторая линия защиты для заданий, которые уже успели попасть в
очередь. Ни одно из этих мест не подавляет ничего, кроме `reminder_24h` и
`reminder_2h` этой конкретной source-записи.

### 6b.1 Dry-run со снимком

Выберите точный manifest миграции и `run_id` из сохранённого отчёта её canary/apply.
Это исходный `run_id` ledger, а не `last_resolution_run_id` последующего reconcile.
В командах ниже замените `MIGRATION_RUN_ID` на это значение; если волна включает
несколько запусков, повторите `--run-id` для каждого во **всех** трёх режимах.
Список проверяется вместе с canonical digest manifest; филиал без выбранных
мастеров и записи других запусков в handover не входят.

При отсутствии отчёта получите доступные ID на сервере read-only командой и
сверьте их с журналом конкретной миграции, прежде чем выбирать волну:

```bash
cd /opt/altegio_bot
dc() { docker compose -p altegio_bot -f docker-compose.yml -f docker-compose.chatwoot-internal.yml "$@"; }
dc exec -T postgres psql -X -v ON_ERROR_STOP=1 -U altegio -d altegio_bot -c "BEGIN TRANSACTION READ ONLY; SELECT source_company_id, run_id, status, count(*) FROM easyweek_migration_ledger WHERE source_provider = 'altegio' AND target_provider = 'easyweek' AND source_company_id = 758285 GROUP BY 1,2,3 ORDER BY 1,2,3; COMMIT;"
```

Snapshot v4 включает выбранные run IDs, digest manifest и конфигурации, client
identity и fingerprints локальных данных. Старые v3 снимки не применяются.
Plan использует `SET TRANSACTION READ ONLY`, повторно читает данные после API
обхода и при `candidate_set_changed=true` запрещает apply. Один booking GET
выполняется без автоматического retry, пауза между запросами — минимум 1 секунда.
Любая попытка нового plan уничтожает предыдущее разрешение по тому же пути.
Файл не переименовывается: его содержимое **перезаписывается** PII-free
tombstone (`{"version", "mode": "invalidated", "invalidated_at", "reason"}`)
атомарной заменой, режим файла остаётся `0600`, каталога — `0700`. Авторизующих
байт после этого не существует нигде, поэтому переименовать файл обратно и
применить его нельзя: `read_snapshot` отвечает `snapshot_invalidated`, а apply и
verify отказываются ещё и по имени пути (`.invalidated`, `.tombstone`, `.bak`,
`.old`) — до открытия write-сессии.

Инвалидация выполняется **до разбора аргументов**, поэтому команда plan с
ошибкой аргументов (пропущенный `--run-id`, нечисловой `--company-id`,
неизвестный флаг) тоже не оставляет старое разрешение применимым. Режим и путь
snapshot определяются отдельным минимальным argparse-препарсером, который знает
арность опций: значение опции больше не принимается за mode, а `--run-id plan`
в команде apply/verify не инвалидирует снимок. `--help` попыткой plan не
считается.

Plan сообщает о невозможности cutover заранее: `pending` или `uncertain` в
выбранном scope дают `cutover_ready=false`, `wave_blockers` со стабильным кодом
`migration_wave_unresolved` и exit code 1, и CLI не печатает команду apply.
Сначала выполняется reconcile, затем plan заново.
Неуспешные apply и verify снимок не трогают — инвалидация относится только к
попытке нового plan. Если tombstone записать не удалось, CLI завершается с
`snapshot_invalidation_failed` и ничего не делает.

Практическое следствие: после неуспешного нового plan повторять старый snapshot
нельзя. Plan останавливается в том числе потому, что живое состояние EasyWeek
разошлось с планом, а apply запросов к API не делает — он применил бы устаревшую
картину CRM. Единственный путь дальше — устранить причину и выполнить plan
заново.

Snapshot действителен не дольше 3600 секунд; после ожидания блокировок возраст
и границы времени проверяются заново. Изменение client, ownership, jobs или
конфигурации требует нового plan. Notification flags для handover не меняются:
snapshot фиксирует их текущие значения; API guard/preflight проверяет актуальность
отдельно от разрешения отправки. Plan никогда не расширяет category allowlist.

```bash
cd /opt/altegio_bot
dc() { docker compose -p altegio_bot -f docker-compose.yml -f docker-compose.chatwoot-internal.yml "$@"; }
dc --profile ops run --rm --build --no-deps -T easyweek-migration-prepare-handover plan --manifest /migration/input/manifest.json --company-id 758285 --run-id MIGRATION_RUN_ID --snapshot /migration/state/reminder_handover.v4.json
```

Команда читает ledger, доказывает каждую перенесённую запись живым
`GET /bookings/{uuid}` и пишет приватный снимок. Она **не меняет ничего**: ни
CRM, ни PostgreSQL, ни `MessageJob`, ни `Record`, ни ledger, ни `OutboxMessage`.
Meta и Chatwoot не вызываются, сообщения не отправляются.

Именно здесь выполняется весь обход API — пока outbox работает. Поэтому на шаге
apply воркер останавливается лишь на время одной транзакции.

### 6b.2 Прочитать отчёт

В отчёте три разных ответа, и путать их нельзя:

| Поле | Вопрос |
|---|---|
| `guard_ready` | существующие EasyWeek-напоминания корректны |
| `coverage_ready` | все необходимые напоминания существуют |
| `cutover_ready` | владение можно переключить атомарно прямо сейчас |

Пустая очередь EasyWeek даёт `guard_ready=true` тривиально. Это ровно то
состояние, которое здесь и исправляется, поэтому `guard_ready` сам по себе
разрешением не является.

Смотреть также `rows_with_blockers` (канонический ключ занят
canceled/failed-заданием — решает человек, автоматически не переоткрывается) и
`rows_with_processing_source_jobs`. Snapshot **v4** содержит и защищает digest-ом
весь eligible `status=created` scope выбранных run IDs, отказы доказательства, readiness, identity
строк, каждое obligation, полный список старых job ID и ожидаемое состояние
ownership marker каждой ledger-строки. Любой snapshot более ранней версии — v1
v2 или v3, — повреждённый JSON, неизвестное поле или изменение `created_at` write
не разрешают. Нулевая или частично доказанная волна никогда не бывает
`cutover_ready`.

### 6b.3 Apply

Требуются одновременно: режим `apply`, флаг `--apply`, точный `--plan-digest`,
точная фраза `--confirm` и переменная окружения. Ни один из них по отдельности
разрешением не является. Снимок старше часа не принимается: обязательства
двигаются вместе с часами.

Остановка outbox — только на время транзакции, и `trap` возвращает воркер при
любом выходе, включая ошибку и Ctrl-C:

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
dc --profile ops run --rm --no-deps -T -e EASYWEEK_REMINDER_HANDOVER_ALLOW_APPLY=true easyweek-migration-prepare-handover apply --manifest /migration/input/manifest.json --company-id 758285 --run-id MIGRATION_RUN_ID --snapshot /migration/state/reminder_handover.v4.json --apply-report /migration/state/reminder_handover.apply-report.v2.json --apply --plan-digest PLAN_DIGEST_ИЗ_ШАГА_6B1 --confirm 'apply reminder handover PLAN_DIGEST_ИЗ_ШАГА_6B1'
)
```

Inbox и capture при этом **не** останавливаются, и notification-флаги не
трогаются. `EASYWEEK_NOTIFICATIONS_ENABLED` общим Altegio send fence не является
и старые Altegio-напоминания не останавливает — не полагайтесь на него.

Trap сам проверяет, что воркер снова `running`. После выхода команды это можно
доказать ещё раз независимо:

```bash
cd /opt/altegio_bot
dc() { docker compose -p altegio_bot -f docker-compose.yml -f docker-compose.chatwoot-internal.yml "$@"; }
dc ps altegio-outbox-worker
test "$(dc ps --status running -q altegio-outbox-worker | wc -l | tr -d ' ')" -eq 1
```

Транзакция одна: сначала создаются все недостающие EasyWeek-напоминания, и
только после этого отменяются старые `queued` Altegio-напоминания тех же
записей. Порядок — это и есть гарантия: если создание не прошло, откат
оставляет клиенту то напоминание, которое у него уже было.

Если хотя бы одно относящееся к scope старое задание оказалось в
`status=processing`, apply останавливается целиком и не меняет ничего.

Успешный apply после commit атомарно пишет приватный PII-free apply report **v2**.
В нём находятся snapshot version/digest, company scope, created/canceled job
IDs и counts, already-present count, ID и счётчики проставленных и уже
существовавших ownership markers, и scoped Outbox before/after evidence.
Verify не принимает отчёт от другого snapshot или отредактированный отчёт.

### 6b.4 Verify

```bash
cd /opt/altegio_bot
dc() { docker compose -p altegio_bot -f docker-compose.yml -f docker-compose.chatwoot-internal.yml "$@"; }
dc --profile ops run --rm --no-deps -T easyweek-migration-prepare-handover verify --manifest /migration/input/manifest.json --company-id 758285 --run-id MIGRATION_RUN_ID --snapshot /migration/state/reminder_handover.v4.json --apply-report /migration/state/reminder_handover.apply-report.v2.json
```

Повторный apply того же snapshot — отдельная проверка идемпотентности. Используем
другой файл отчёта, чтобы не уничтожить evidence первого apply; результат обязан
показать `mutations: 0`:

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
dc --profile ops run --rm --no-deps -T -e EASYWEEK_REMINDER_HANDOVER_ALLOW_APPLY=true easyweek-migration-prepare-handover apply --manifest /migration/input/manifest.json --company-id 758285 --run-id MIGRATION_RUN_ID --snapshot /migration/state/reminder_handover.v4.json --apply-report /migration/state/reminder_handover.repeat-apply-report.v2.json --apply --plan-digest PLAN_DIGEST_ИЗ_ШАГА_6B1 --confirm 'apply reminder handover PLAN_DIGEST_ИЗ_ШАГА_6B1'
)
```

Затем новый read-only plan обязан показать `easyweek_reminders_to_create: 0` и
`coverage_ready: true`:

```bash
cd /opt/altegio_bot
dc() { docker compose -p altegio_bot -f docker-compose.yml -f docker-compose.chatwoot-internal.yml "$@"; }
dc --profile ops run --rm --no-deps -T easyweek-migration-prepare-handover plan --manifest /migration/input/manifest.json --company-id 758285 --run-id MIGRATION_RUN_ID --snapshot /migration/state/reminder_handover.after.v4.json
```

Отдельно запускается существующий API preflight. Он повторно читает актуальные
EasyWeek booking и прогоняет production reminder guard для всей открытой очереди.
CLI verify уже делает GET-проверку frozen scope; отдельный preflight проверяет
остальные открытые напоминания и не подменяет проверку полноты handover:

```bash
cd /opt/altegio_bot
dc() { docker compose -p altegio_bot -f docker-compose.yml -f docker-compose.chatwoot-internal.yml "$@"; }
dc run --rm --no-deps -T --entrypoint /app/.venv/bin/python altegio-outbox-worker -m altegio_bot.scripts.easyweek_reminder_preflight --limit 200 --pause-sec 1.05
```

Read-only сверка в базе:

```bash
cd /opt/altegio_bot
dc() { docker compose -p altegio_bot -f docker-compose.yml -f docker-compose.chatwoot-internal.yml "$@"; }
dc exec -T postgres psql -X -v ON_ERROR_STOP=1 -U altegio -d altegio_bot -c "BEGIN TRANSACTION READ ONLY; SELECT provider, job_type, status, count(*) FROM message_jobs WHERE job_type IN ('reminder_24h','reminder_2h') GROUP BY 1,2,3 ORDER BY 1,2,3; COMMIT;"
```

Отметки владения и отсутствие открытых Altegio-напоминаний у переданных записей
— одним read-only запросом:

```bash
cd /opt/altegio_bot
dc() { docker compose -p altegio_bot -f docker-compose.yml -f docker-compose.chatwoot-internal.yml "$@"; }
dc exec -T postgres psql -X -v ON_ERROR_STOP=1 -U altegio -d altegio_bot -c "BEGIN TRANSACTION READ ONLY; SELECT l.source_company_id, count(*) AS handed_over, count(DISTINCT l.reminder_handover_plan_digest) AS plans, count(j.id) AS still_open_altegio_reminders FROM easyweek_migration_ledger l LEFT JOIN records r ON r.provider = l.source_provider AND r.company_id = l.source_company_id AND r.altegio_record_id = l.source_record_id LEFT JOIN message_jobs j ON j.provider = 'altegio' AND j.record_id = r.id AND j.job_type IN ('reminder_24h','reminder_2h') AND j.status IN ('queued','processing') WHERE l.reminders_handed_over_at IS NOT NULL GROUP BY 1 ORDER BY 1; COMMIT;"
```

`still_open_altegio_reminders` обязан быть `0`. `plans` больше единицы означает,
что записи филиала передавались разными планами — само по себе не ошибка, но
повод свериться с историей apply-отчётов.

### 6b.5 Если что-то пошло не так

`ownership_unproven`, `target_client_unproven`, `staff_scope_unproven`,
`ledger_duplicate_target`, `stale_target_reminder`, `configuration_changed`,
`migration_wave_changed`, `candidate_set_changed`, `snapshot_expired` и
`reminder_boundary_passed` — STOP. Любой `*_changed` в apply также означает
откат всей волны. При `database_lock_timeout`/`database_statement_timeout`
откат обязателен; повторите свежий plan после устранения конкурирующей операции.
Outbox восстанавливается через trap при EXIT, INT, TERM и HUP. При потере SSH
проверьте его состояние после переподключения; SIGKILL/выключение хоста trap
обработать не может.

Повтор того же свежего snapshot с исходным digest является проверкой с нулевыми
мутациями. Новый snapshot после уже выполненного handover показывает покрытие,
но не разрешает переписать исходный ownership marker чужим digest.
Если commit прошёл, а файл apply-report записать не удалось, сохраните исходный
snapshot и повторите его с тем же digest, пока он свежий, в отдельный report.
Если он уже истёк, остановитесь для разбора; не снимайте marker и не переоткрывайте
jobs вручную. Восстановление выполняется вперёд, по доказанному текущему состоянию.

| Симптом | Что это значит | Что делать |
|---|---|---|
| `halted: source_reminder_processing` | воркер держит старое задание | ничего не изменено; подождать, повторить `plan` и `apply` |
| `halted: snapshot_incomplete_scope` | snapshot пуст, содержит eligible refusal или неполный created scope | ничего не изменено; новый `plan`, устранить причину refusal |
| `halted: snapshot_obligation_blocked` | snapshot содержит canceled/failed/unknown target obligation | ничего не изменено; решение оператора, автоматического reopen нет |
| `halted: eligible_scope_changed` | полный company/status ledger scope изменился после plan | ничего не изменено; новый `plan` |
| `halted: obligation_identity_mismatch` | dedupe key занят строкой с неверным status или identity | вся транзакция откатилась; проверить job вручную |
| `halted: source_reminder_changed` | старый source job исчез или изменил identity/status | вся транзакция откатилась; новый `plan` |
| `halted: source_reminder_scope_changed` | между `plan` и `apply` появилось новое открытое Altegio-напоминание | ничего не изменено; новый `plan` — он увидит и его |
| `halted: reminder_marker_conflict` | у ledger-строки уже есть отметка от другого плана | ничего не изменено; чужое решение не переписывается, сверить историю apply-отчётов |
| `marker_incomplete` в `rows_refused` | у ledger-строки заполнена половина отметки | строка вне волны; такое состояние БД запрещает CHECK, значит её писали в обход инструмента |
| `verify: ledger_rows_missing_marker` | у переданной записи нет отметки | handover не доказан; не запускать повторно вслепую, разобраться |
| `verify: ledger_rows_with_foreign_marker` | отметка принадлежит другому плану или другой паре | то же; сверить identity ledger-строки |
| `halted: scoped_outbox_side_effect` | scoped Outbox before/after внутри apply не совпал | вся транзакция откатилась; расследовать до повтора |
| `halted: local_target_mismatch` | запись сдвинулась или отменена после `plan` | новый `plan` |
| `halted: ledger_not_created` | ledger-строка изменила статус | новый `plan`; разобраться, что её двигало |
| `halted: reminder_boundary_passed` | напоминание уже прошло свой момент | новый `plan` |
| `the snapshot is ...s old` | снимок устарел | новый `plan` |
| `rows_with_blockers` не пуст | ключ занят canceled/failed заданием | решение оператора; инструмент такие не переоткрывает |

Отката как отдельной команды нет и он не нужен: apply либо проходит целиком,
либо не меняет ничего. Если apply прошёл, а результат не устраивает, отменять
созданные EasyWeek-напоминания и возвращать Altegio-задания — отдельное ручное
решение, и делается оно после `verify`, с полным пониманием, какие записи
затронуты.

После переключения дальнейшее перепланирование — reschedule, update, cancel —
целиком за обычными EasyWeek-вебхуками. Никакого фонового синхронизатора этот
шаг не оставляет.

Altegio inbox и capture остаются включёнными и после handover. Поздние
Altegio-события по переданным записям больше не создают и не переоткрывают
напоминаний — их отсекает отметка владения, — но всё остальное на этом пути
планируется как раньше: `record_*`, review, retention и campaign-задания
затронуты не будут, как и записи, которые не переносились.

---

## 7. Что этот этап не делает

- Не переносит записи и не содержит второго мигратора.
- Не сопоставляет услуги «похоже»: только точное совпадение канонического
  имени, всё остальное — на человека.
- Не считает mapping из прошлой волны разрешением для мастера новой волны: если
  каталог не отдаёт услугу тому, кто её реально ведёт, это
  `existing_mapping_staff_unavailable`, а не готовность.
- Не считает `staff_availability=UNSTATED` автоматической готовностью. Existing
  mapping с неизменным target остаётся outstanding до явного подтверждения
  точного current digest; подтверждение не перезаписывает UUID в manifest.
- Не утверждает, что EasyWeek API принимает индивидуальную длительность или
  цену. Запись, растянутая руками, попадает в `manual_adjustment_candidate` —
  это работа человека, а не автоматический путь.
- Не считает совпадение UUID достаточным: если у уже сопоставленной услуги в
  каталоге изменились имя, валюта, цена или длительность, это `drift`, а не
  готовность.
- Не считает услугу доступной мастеру A на том основании, что каталог отдаёт её
  мастеру B из той же волны. Покрыты должны быть все мастера, которые эту услугу
  действительно ведут.
- Не сливает клиентов, не выдумывает имя, email, телефон или историю визитов.
- Не считает отсутствие клиента в EasyWeek доказательством первого визита и не
  обнуляет счётчики.
- Не обходит конфликт уникальности телефона или email изменением контактов.
- Не пишет имена, телефоны, email и сырые тела ответов в обычные логи. Строка
  запроса `GET /customers?phone=…` содержит номер, поэтому HTTP-логгер поднят
  до `WARNING` и на нём стоит фильтр.

---

## 8. Если что-то пошло не так

| Симптом | Что это значит | Что делать |
|---|---|---|
| `lookups_undetermined > 0` | воркспейс не прочитан | повторить шаг 1; ничего не создавать |
| `halted: create_outcome_unknown` | `POST` мог пройти | повторить шаг 4; она сверится чтением |
| `halted: create_not_verified` | карточка не подтверждена чтением | то же; при повторе несовпадений — смотреть в EasyWeek руками |
| `halted: create_access_denied` | ключ не имеет права писать; с данными клиента всё в порядке | проверить `EASYWEEK_API_KEY` и `EASYWEEK_WORKSPACE_SLUG`; подтверждение сохранено |
| `blocked_reason: create_rejected_by_workspace` | телефон или email уже занят другим клиентом | найти владельца в EasyWeek. Контакты **не** менять ради обхода |
| `DecisionStoreLocked` | идёт другой запуск | дождаться; файл блокировки снимать только вручную |
| `the pending customer list has changed` | список изменился после печати | шаг 1, прочитать заново, подтвердить с новым дайджестом |
| `needs IDENT=DIGEST` | передан голый идентификатор без дайджеста | скопировать `review_digest` из шага 2 |
| `the supplied review digest does not match` | дайджест не от этого элемента или устарел | шаг 1 и 2 заново, взять свежий `review_digest` |
| `the live data no longer matches the reviewed decision` | данные изменились после review | шаг 1 и 2 заново; ничего не изменено |
| `existing_mapping_drift` | UUID тот же, но услуга в каталоге изменилась | сверить `drift_fields` и `existing_manifest_baseline` в review; manifest сам не переписывается |
| `existing_mapping_baseline_incomplete` | в manifest нет замороженной identity услуги | дописать `catalog_service_name` и `catalog_currency` в manifest |
| `existing_mapping_staff_unavailable` | услуга сопоставлена, но каталог не отдаёт её мастеру этой волны | открыть услугу мастеру в EasyWeek, затем повторить шаг 1 |
| `correction_source_identity_changed` | исходные данные клиента изменились после исправления | шаг 1 и 2 заново; исправление молча не применяется |
| `branch identity unproven` | manifest указывает не на тот филиал | проверить `EASYWEEK_LOCATION_MAP` и `easyweek_location_*` в manifest |
