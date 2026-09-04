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
покрывает все показанные поля целиком: имя, телефон, email и количество записей
у клиента; исходную цену и длительность, целевой UUID, имя, валюту, цену,
длительность и доказательство доступности у услуги. Если изменилось что-то одно,
дайджест другой, и старое подтверждение больше не действует.

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

### 6b.1 Dry-run со снимком

```bash
cd /opt/altegio_bot && docker compose --profile ops run --rm --build easyweek-migration-prepare-handover plan --manifest /migration/input/manifest.json --company-id 758285 --snapshot /migration/state/reminder_handover.json
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
`rows_with_processing_source_jobs`.

### 6b.3 Apply

Требуются одновременно: режим `apply`, флаг `--apply`, точный `--plan-digest`,
точная фраза `--confirm` и переменная окружения. Ни один из них по отдельности
разрешением не является. Снимок старше часа не принимается: обязательства
двигаются вместе с часами.

Остановка outbox — только на время транзакции, и `trap` возвращает воркер при
любом выходе, включая ошибку и Ctrl-C:

```bash
cd /opt/altegio_bot && trap 'docker compose up -d altegio-outbox-worker' EXIT INT TERM && docker compose stop altegio-outbox-worker && docker compose --profile ops run --rm -e EASYWEEK_REMINDER_HANDOVER_ALLOW_APPLY=true easyweek-migration-prepare-handover apply --manifest /migration/input/manifest.json --company-id 758285 --snapshot /migration/state/reminder_handover.json --apply --plan-digest PLAN_DIGEST_ИЗ_ШАГА_6B1 --confirm 'apply reminder handover PLAN_DIGEST_ИЗ_ШАГА_6B1'
```

Inbox и capture при этом **не** останавливаются, и notification-флаги не
трогаются. `EASYWEEK_NOTIFICATIONS_ENABLED` общим Altegio send fence не является
и старые Altegio-напоминания не останавливает — не полагайтесь на него.

Убедиться, что воркер поднялся:

```bash
cd /opt/altegio_bot && docker compose ps altegio-outbox-worker
```

Транзакция одна: сначала создаются все недостающие EasyWeek-напоминания, и
только после этого отменяются старые `queued` Altegio-напоминания тех же
записей. Порядок — это и есть гарантия: если создание не прошло, откат
оставляет клиенту то напоминание, которое у него уже было.

Если хотя бы одно относящееся к scope старое задание оказалось в
`status=processing`, apply останавливается целиком и не меняет ничего.

### 6b.4 Verify

```bash
cd /opt/altegio_bot && docker compose --profile ops run --rm easyweek-migration-prepare-handover verify --manifest /migration/input/manifest.json --company-id 758285 --snapshot /migration/state/reminder_handover.json
```

Затем существующий preflight по открытым EasyWeek-напоминаниям:

```bash
cd /opt/altegio_bot && docker compose --profile ops run --rm easyweek-migration-prepare-handover plan --manifest /migration/input/manifest.json --company-id 758285 --snapshot /migration/state/reminder_handover_after.json
```

```bash
cd /opt/altegio_bot && docker compose run --rm --no-deps altegio-outbox-worker python -m altegio_bot.scripts.easyweek_reminder_preflight
```

Повторный `plan` должен показать `easyweek_reminders_to_create: 0` и
`coverage_ready: true`. Повторный `apply` идемпотентен и ничего не меняет.

Read-only сверка в базе:

```bash
cd /opt/altegio_bot && docker compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT provider, job_type, status, count(*) FROM message_jobs WHERE job_type IN ('reminder_24h','reminder_2h') GROUP BY 1,2,3 ORDER BY 1,2,3;"
```

### 6b.5 Если что-то пошло не так

| Симптом | Что это значит | Что делать |
|---|---|---|
| `halted: source_reminder_processing` | воркер держит старое задание | ничего не изменено; подождать, повторить `plan` и `apply` |
| `halted: local_target_mismatch` | запись сдвинулась или отменена после `plan` | новый `plan` |
| `halted: ledger_not_created` | ledger-строка изменила статус | новый `plan`; разобраться, что её двигало |
| `halted: reminder_boundary_passed` | напоминание уже прошло свой момент | новый `plan` |
| `halted: obligation_not_created` | канонический ключ занят чужой записью | ничего не изменено; разобраться в `message_jobs` руками |
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

---

## 7. Что этот этап не делает

- Не переносит записи и не содержит второго мигратора.
- Не сопоставляет услуги «похоже»: только точное совпадение канонического
  имени, всё остальное — на человека.
- Не считает mapping из прошлой волны разрешением для мастера новой волны: если
  каталог не отдаёт услугу тому, кто её реально ведёт, это
  `existing_mapping_staff_unavailable`, а не готовность.
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
