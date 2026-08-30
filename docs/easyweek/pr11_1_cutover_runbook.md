# PR-11.1 — production runbook: перенос будущих активных записей Altegio → EasyWeek

Область: **только будущие активные бронирования** филиалов Karlsruhe
(Altegio `758285`) и Rastatt (Altegio `1271200`), и **только у явно выбранных
мастеров этой волны**. Прошлые, завершённые и отменённые записи в EasyWeek не
создаются никогда. Durlach в Altegio отсутствует и в миграции не участвует ни на
одном шаге.

**Волны.** Первая волна переносит мастеров, перечисленных в
`selected_altegio_staff_ids`. Ногтевые мастера отложены и указаны в
`deferred_altegio_staff_ids` — их записи **переносятся отдельной будущей
волной**, для которой этот же runbook выполняется заново. Мастер, не попавший ни
в один список, блокирует cutover: «отложили» и «забыли» не должны выглядеть
одинаково.

**Manifest волны 2 и далее — не новый файл, а предыдущий плюс новые mappings.**
Selector (`selected` / `deferred`) описывает состав текущей волны; mappings
накапливаются и не удаляются, пока живы записи, которые по ним переносили.
Полный порядок сборки — шаг 5a; пропустить его нельзя: manifest, потерявший
mapping предыдущей волны, останавливает canary и apply до первой mutation.

Исторические счётчики визитов переносятся **нативным импортом клиентов
EasyWeek**, а не этим инструментом. Canary подтвердил: EasyWeek сохраняет
импортированный baseline, после завершённой записи `booking-succeeded` приносит
числовой `visits_total`, и PR-11 сохраняет его в локальный `Client`. Поэтому
PR-11.1 **не пишет счётчики через API**.

## Неизменяемые аргументы волны

Волна имеет **durable identity**, и её нельзя менять после apply. Шесть значений
образуют эту identity; её дайджест печатается в отчёте как
`scope.wave_identity`:

| значение | что скрыла бы подмена |
|---|---|
| `manifest` (его canonical содержимое) | другой mapping — другие targets |
| `selected_altegio_staff_ids` / `deferred_altegio_staff_ids` | мастер переведён в другую волну, его записи и targets выпали из проверки |
| `--cutover-at` | более поздняя граница выкидывает ранние записи из окна |
| `--horizon-days` | более узкий горизонт отбрасывает дальний край волны |
| branch identity (`EASYWEEK_LOCATION_MAP`) | доказан другой филиал |
| request schema версия | форма запроса не проходила canary |

Отсюда правила, обязательные к соблюдению:

- **`--cutover-at` нельзя опускать.** Для `reconcile`, `reconcile --final` и
  `resolve-created` он обязателен и должен быть **точно тем же значением**, с
  которым выполнялся apply. Раньше без него подставлялось текущее время, и
  запись на 10:00, сверяемая в 12:00, выпадала из окна вместе со своим EasyWeek
  target — удалённый target не мог провалить проверку, которая на него не
  смотрела.
- **`--horizon-days` должен совпадать** с тем, что использовал apply. Значение
  по умолчанию (180) считается таким же явным выбором, как и указанное.
- **Manifest и staff selector после apply менять нельзя**, пока эта волна не
  доказана финальной reconciliation. Перевод уже перенесённого мастера в
  `deferred_altegio_staff_ids` — это подмена волны, а не уточнение.
- **Следующая волна — это новая identity.** Волна ногтевых мастеров получит свой
  manifest со своим selector, свой canary и свой `wave_identity`; доказать её
  targets аргументами первой волны невозможно, и наоборот.

Проверяет совпадение сам инструмент: любая команда, продолжающая волну, до
чтения источника и любого target ищет verified canary proof с точно совпадающим
scope. Подтверждение — поле `scope` в отчёте (`scope_proven: true` и
`wave_identity`). Несовпадение даёт ненулевой код и одну из причин:
`migration_scope_missing`, `migration_scope_ambiguous`,
`migration_scope_manifest_mismatch`, `migration_scope_staff_scope_mismatch`,
`migration_scope_cutover_mismatch`, `migration_scope_horizon_mismatch`,
`migration_scope_branch_mismatch`, `migration_scope_schema_mismatch`.

---

Все команды выполняются из корня проекта:

```bash
cd /opt/altegio_bot
```

---

## 0. Как запускается инструмент

Через отдельный one-off compose service `easyweek-booking-migration` под профилем
`ops`. Он не запускается обычным `docker compose up -d`.

Почему не внутри воркера: у контейнера воркера нет монтирований, поэтому manifest
и экспорт клиентов туда не попадают, а отчёт умирает вместе с контейнером. У
one-off сервиса входы смонтированы **read-only** (экспорт клиентов — это PII), а
каталог отчётов — на запись, поэтому отчёт остаётся на хосте.

Пути на хосте задаются переменными (по умолчанию — внутри git-ignored `outputs/`):

```bash
export EASYWEEK_MIGRATION_INPUT_DIR=/opt/altegio_bot/outputs/easyweek_migration/input
```

```bash
export EASYWEEK_MIGRATION_REPORT_DIR=/opt/altegio_bot/outputs/easyweek_migration
```

```bash
mkdir -p "$EASYWEEK_MIGRATION_INPUT_DIR" "$EASYWEEK_MIGRATION_REPORT_DIR"
```

Внутри контейнера они видны как `/migration/input` (ro) и `/migration/reports`.
`--build` обязателен всегда: инструмент должен быть кодом разворачиваемого
коммита.

| Режим | Пишет в EasyWeek | Пишет в БД | Назначение |
|---|---|---|---|
| `inventory` | нет | нет | какие Altegio staff/service ID нужны и каких нет в manifest. Работает на **незаполненном** manifest |
| `dry-run` | нет | нет | проверяемый план; его `plan_digest` открывает запись |
| `canary` | одна запись | ledger + proof | создаёт одну **названную** запись, перечитывает её и сохраняет durable proof |
| `apply` | да | ledger | bulk; требует подходящий canary proof |
| `reconcile` | только GET | ledger | состояние; с `--final` доказывает полноту |
| `resolve-created` | только GET | ledger | разрешает неизвестный исход по UUID, который инструмент проверяет |
| `resolve-absent` | нет | ledger | фиксирует, что оператор убедился: записи нет |
| `rollback-dry-run` | только GET | нет | что откат **бы** отменил |

`dry-run` — режим по умолчанию. Без `--apply` mutation-путь недостижим по
конструкции.

---

## 1. Экспорт клиентов EasyWeek **до** импорта счётчиков

**Что делает:** сохраняет снимок клиентской базы EasyWeek до того, как импорт её
изменит.
**Зачем:** это единственная точка возврата. Если импорт счётчиков склеит или
задвоит клиентов, восстановить «как было» можно только из этого файла.

Настройки → Клиенты → Экспорт. Сохранить в `$EASYWEEK_MIGRATION_INPUT_DIR`,
например `easyweek-customers-before-import.csv`. Это PII: `outputs/` в
`.gitignore`, коммитить нельзя.

## 2. Импорт исторических счётчиков визитов

**Что делает:** загружает в EasyWeek количество завершённых визитов по каждому
клиенту нативным импортом.
**Зачем:** это единственный доказанный способ перенести историю. PR-12
(`repeat_10d` / `comeback_3d`) принимает решение «писать клиенту или нет» именно
по этому числу: постоянный клиент, приехавший в EasyWeek с нулём визитов,
получит сообщение для новичка.

## 3. Сохранить отчёт импорта

**Что делает:** сохраняет выданный EasyWeek отчёт импорта.
**Зачем:** единственное доказательство, сколько строк принято и сколько
отклонено. Без него расхождение счётчиков через месяц не расследуется.

## 4. Повторный экспорт клиентов EasyWeek

**Что делает:** выгружает клиентскую базу **после** импорта в
`$EASYWEEK_MIGRATION_INPUT_DIR/easyweek-customers-after-import.csv`.
**Зачем:** два применения одного файла: сверка с шагом 1 (импорт не создал
дублей) и `--customer-directory` для миграции. Клиент ищется по точному
нормализованному международному номеру: ноль совпадений и больше одного —
`blocked`, ровно одно — его UUID. Клиенты автоматически не создаются.

Экспорт — **полный и свежий**, а не выборка под мастеров текущей волны. Каждая
волна берёт новый экспорт, и он обязан резолвить не только клиентов этой волны,
но и клиентов живых записей всех предыдущих: их проверяет и cumulative-guard
(шаг 5a), и финальная reconciliation. Файл фильтровать нельзя.

## 5. Подготовка mapping location / staff / service

**Что делает:** заполняется manifest — явное соответствие Altegio-идентификаторов
и EasyWeek UUID, плюс каталожные длительность и цена каждой услуги.
**Зачем:** ни один идентификатор не выводится автоматически, а каталожные
значения — это то, **с чем** сравнивается запись: без них растянутый слот и
скидка до нуля невидимы. Шаблон и правила:
`docs/easyweek/migration_manifest.example.json` и `migration_manifest.README.md`.

**Что делает команда:** выгружает локации EasyWeek с UUID и именами, read-only.

```bash
docker compose --profile ops run --rm --build easyweek-booking-migration --help
```

```bash
docker compose exec altegio-easyweek-inbox-worker python -m altegio_bot.scripts.easyweek_probe --redact-pii
```

**Что делает команда:** показывает, какие Altegio staff/service ID реально
встречаются в будущих записях и каких ещё нет в manifest. Работает на
незаполненном manifest, экспорт клиентов не нужен, ничего не пишет.

```bash
docker compose --profile ops run --rm --build easyweek-booking-migration inventory --manifest /migration/input/manifest.json --cutover-at 2026-09-01T00:00:00+02:00
```

Повторять, пока `source_identifiers[*].staff.missing` и `.services.missing` не
станут пустыми. Пустыми они обязаны быть **в двух смыслах**, и путать их нельзя:

- **новое в этой волне** — каждый мастер из `selected_altegio_staff_ids` и каждая
  услуга, встречающаяся в его будущих записях, должны быть отображены. Без этого
  волна не поедет;
- **унаследованное от предыдущих волн** — mappings и каталожные baselines всех
  мастеров и услуг, по которым уже есть живые `created`-строки ledger, должны
  остаться в файле, даже если эти мастера теперь в `deferred`. Без этого волна
  **поедет и не закроется** (шаг 5a).

Для первой волны второй список пуст: у отложенных мастеров ещё нет
`created`-строк, и заранее отображать их не требуется.

**Что делает оператор:** распределяет каждый Altegio staff ID из отчёта ровно в
один список — `selected_altegio_staff_ids` или `deferred_altegio_staff_ids`.
**Зачем:** мастер вне обоих списков блокирует cutover (`staff_not_in_wave_scope`),
а незаполненный mapping намеренно **не** работает как способ исключить мастера.
Иначе забытый мастер выглядел бы как отложенный, полнота волны была бы объявлена
доказанной, и его клиенты приехали бы в салон, где о них не знают.

Имена мастеров помогают принять решение, но в файл идут только ID.

**Что делает команда:** печатает, сколько активных записей приходится на каждый
Altegio staff ID и как они распределены по selected / deferred / unknown.
**Зачем:** это число оператор сверяет с интерфейсом Altegio и позже с EasyWeek —
«ожидал 34 записи у выбранных мастеров, вижу 34».

```bash
docker compose --profile ops run --rm --build easyweek-booking-migration inventory --manifest /migration/input/manifest.json --cutover-at 2026-09-01T00:00:00+02:00
```

В отчёте раздел `wave`: `active_bookings_selected`, `active_bookings_deferred`,
`active_bookings_unknown_staff` и разбивка `by_altegio_staff_id`. Перед
переходом дальше `active_bookings_unknown_staff` обязан быть нулём.

**Проверка филиалов.** Manifest дополнительно сверяется с рантайм-реестром
`EASYWEEK_LOCATION_MAP`: location ID, location UUID и slug филиала
(`758285 → karlsruhe`, `1271200 → rastatt`) должны совпасть. Перепутанные
Karlsruhe и Rastatt отвергаются до первого запроса.

---

## 5a. Волна 2 и далее — кумулятивный manifest

**Пропустить этот шаг нельзя, если волна не первая.**

**Что делает оператор:** берёт manifest **предыдущей** волны, меняет в нём только
selector и **добавляет** mappings новых мастеров и услуг.
**Зачем:** финальная reconciliation каждой волны перечитывает живые записи всех
предыдущих волн полным классификатором. Ей нужны их staff/service mappings и
каталожные baselines. Manifest, собранный с нуля «под текущую волну», их теряет —
и волна проходит canary и apply, а затем навсегда упирается в
`migrated_source_lifecycle_unprovable` о записи, которую никто не трогал. К этому
моменту бронирования уже созданы, и правка manifest их не отменяет.

Порядок:

1. Скопировать manifest предыдущей волны — он основа, а не образец.
2. Изменить **только** `selected_altegio_staff_ids` и
   `deferred_altegio_staff_ids`: мастера этой волны — в `selected`, все
   остальные, включая уже перенесённых, — в `deferred`.
3. **Добавить** mappings новых мастеров и их услуг, вместе с
   `catalog_duration_minutes` и `catalog_price`.
4. **Ничего не удалять.** Mappings и baselines предыдущих волн остаются как есть.
   Менять их значения тоже нельзя: изменённая каталожная цена читается так же,
   как пропавшая.
5. Взять **свежий полный** экспорт клиентов (шаг 4). Прошлый файл устарел,
   отфильтрованный — не подходит.

**Наличие mapping не делает мастера selected.** Состав волны задаёт только
selector: мастер ресниц с сохранённым mapping, но в `deferred`, повторно не
переносится — его записи остаются `already_migrated`, а новые пропускаются с
`staff_deferred_to_later_wave`.

### Пример: волна A — ресницы, волна B — ногти

Волна A: мастер ресниц в `selected`, мастер ногтей в `deferred`, отображена
только услуга LASH. Mapping для NAIL не нужен — у ногтевого мастера ещё нет ни
одной `created`-строки.

Волна B, тот же файл, три правки:

| | волна A | волна B |
|---|---|---|
| `selected_altegio_staff_ids` | мастер ресниц | **мастер ногтей** |
| `deferred_altegio_staff_ids` | мастер ногтей | **мастер ресниц** |
| mapping мастера ресниц | есть | **есть — остаётся** |
| mapping услуги LASH + baseline | есть | **есть — остаётся** |
| mapping мастера ногтей | нет | **добавлен** |
| mapping услуги NAIL + baseline | нет | **добавлен** |

Мастер ресниц переехал в `deferred` — но его mapping и baseline услуги LASH
остаются в файле, потому что её записи живы. Переносится в волне B только мастер
ногтей.

**Что делает команда:** строит план и печатает раздел `previous_wave_context` —
результат read-only проверки того, что текущие manifest и экспорт клиентов всё
ещё доказывают живые `created`-строки предыдущих волн. Ни одного EasyWeek-запроса.

```bash
docker compose --profile ops run --rm --build easyweek-booking-migration dry-run --manifest /migration/input/manifest.json --customer-directory /migration/input/easyweek-customers-after-import.csv --cutover-at 2026-09-01T00:00:00+02:00
```

**Зачем именно здесь:** та же проверка встроена в gate перед первой mutation
`canary` и `apply`, и там она уже отказ. Здесь она — предупреждение, которое
чинится правкой файла.

Требуется `previous_wave_context.proven: true`. При `false` каждая строка
`previous_wave_context.rows` называет source identity, причину и Altegio ID,
которого не хватает:

| причина | что вернуть в manifest |
|---|---|
| `previous_wave_staff_mapping_missing` | staff mapping для `altegio_staff_id` |
| `previous_wave_service_mapping_missing` | service mapping для `altegio_service_id` |
| `previous_wave_catalogue_baseline_missing` | `catalog_duration_minutes` / `catalog_price` услуги — пропали или изменены |
| `previous_wave_customer_unresolved` | взять полный свежий экспорт клиентов, не отфильтрованный |
| `previous_wave_source_fingerprint_mismatch` | запись предыдущей волны изменилась в Altegio: её target — ghost, разобрать вручную до начала волны |

Общий код отказа gate — `previous_wave_context_unprovable`. Отказ происходит до
первой mutation: ни одного созданного бронирования, ledger и proof предыдущих
волн не тронуты.

Строки **текущей** волны эта проверка не считает: их разбирает обычный
`blocked_rows` на шаге 12. `previous_wave_context.checked` для первой волны равен
нулю — это нормально.

---

# Notification maintenance window

Шаги 6–19 выполняются **внутри одного окна с выключенными уведомлениями**.
Окно закрывается только на шаге 20, отдельным ручным решением.

## 6. Отключить **все** нативные уведомления EasyWeek

**Что делает:** оператор вручную выключает в интерфейсе EasyWeek каждый
клиентский канал: email, SMS, push, WhatsApp и любые другие подключённые, а
также автоматические подтверждения, напоминания и сообщения об изменении записи.
**Зачем:** это обязательное условие миграции, а не рекомендация. Инструмент
создаёт сотни будущих записей за минуты; с включёнными нативными уведомлениями
столько же живых людей одновременно получат сообщение о «новой записи», которую
они сделали недели назад. Код это не видит и выключить не может.

## 7. Выключить planning-флаги уведомлений и отзывов бота

**Что делает:** в `easyweek.env`:

```bash
EASYWEEK_NOTIFICATIONS_ENABLED=false
```

```bash
EASYWEEK_REVIEWS_ENABLED=false
```

**Зачем:** наша половина той же защиты. Миграция породит вебхуки
`booking-created`; с включёнными флагами бот отправит lifecycle-сообщения и
запланирует отзывы. Оба флага проверяются gate'ом.

## 8. Закрыть существующие send fences

**Что делает:** в `easyweek.env`:

```bash
EASYWEEK_REVIEW_SEND_ENABLED=false
```

```bash
EASYWEEK_REMINDER_API_GUARD_ENABLED=false
```

**Зачем:** planning-флаги останавливают создание **новых** job. Уже стоящие в
очереди review и reminder они не трогают, а мигрированные записи — это будущие
визиты, по которым очередь может ожить. Fence закрывает отправку.

## 9. Оставить capture и processing включёнными

**Что делает:** в `easyweek.env` остаются

```bash
EASYWEEK_ENABLED=true
```

```bash
EASYWEEK_PROCESSING_ENABLED=true
```

**Зачем:** это не противоречие «выключить всё». EasyWeek **не переигрывает
историю доставок** (§1.3): выключенный capture потеряет события самой миграции
навсегда — вместе с будущими `booking-succeeded`, из которых PR-11 берёт
`visits_total`. Gate требует оба флага включёнными. Visit counter
(`EASYWEEK_VISIT_COUNTER_ENABLED=true`) можно оставить: он ничего не отправляет.

## 10. Пересоздать контейнеры и проверить effective settings

**Что делает:** пересоздаёт контейнеры, чтобы они перечитали `env_file`.
`restart` этого не делает — `env_file` читается только при создании контейнера.

```bash
docker compose up -d --force-recreate altegio-easyweek-inbox-worker altegio-outbox-worker
```

**Что делает:** печатает флаги, с которыми процесс реально работает.

```bash
docker compose --profile ops run --rm --build easyweek-booking-migration inventory --manifest /migration/input/manifest.json --cutover-at 2026-09-01T00:00:00+02:00
```

В отчёте раздел `gate.effective_settings` появляется при записи; для проверки
сейчас достаточно, что команда не отказала. Ожидается: `EASYWEEK_ENABLED=true`,
`EASYWEEK_PROCESSING_ENABLED=true`, `EASYWEEK_NOTIFICATIONS_ENABLED=false`,
`EASYWEEK_REVIEWS_ENABLED=false`.

**Опция: остановка общего outbox worker.** Если нужна максимальная гарантия, что
за время окна не уйдёт ни одного клиентского сообщения, общий outbox worker
можно остановить: `docker compose stop altegio-outbox-worker`. Последствия —
на время окна замирает **вся** исходящая очередь, включая Altegio-филиалы;
job'ы не теряются, они остаются `queued` и разбираются после `start`. Это
операторский компромисс, а не требование: архитектура общего Altegio outbox
ради миграции не меняется.

## 11. Read-only preflight по клиентским job

**Что делает:** показывает, есть ли в очереди EasyWeek клиентские job, которые
могли бы уйти во время окна.
**Зачем:** флаги закрывают будущее; эта проверка смотрит на настоящее.

```bash
docker compose exec postgres psql -U "${POSTGRES_USER:-altegio}" -d "${POSTGRES_DB:-altegio_bot}" -c "select job_type, status, count(*) from message_jobs where provider = 'easyweek' and status in ('queued','processing') group by job_type, status order by job_type"
```

Ожидается пусто. Ненулевые строки разбираются до canary.

## 12. Dry-run

**Что делает:** строит полный план — ready / already_migrated / blocked /
skipped — и печатает `plan_digest`. Ни одного EasyWeek-запроса, ни одной строки
в ledger.
**Зачем:** артефакт, который проверяет человек, и его `plan_digest` —
единственный ключ, открывающий запись.

```bash
docker compose --profile ops run --rm --build easyweek-booking-migration dry-run --manifest /migration/input/manifest.json --customer-directory /migration/input/easyweek-customers-after-import.csv --cutover-at 2026-09-01T00:00:00+02:00
```

Разобрать **каждую** строку `blocked_rows` вручную. Типовые причины:
`staff_mapping_missing`, `service_mapping_missing`, `customer_not_found`,
`customer_ambiguous`, `multi_service_unsupported`, `custom_price_unsupported`,
`custom_duration_unsupported`, `price_baseline_missing`,
`start_time_ambiguous_dst`, `staff_not_in_wave_scope`. Ни одна не чинится
инструментом — правится manifest или экспорт, либо запись переносится руками.

Записи отложенных мастеров в `blocked_rows` **не попадают**: они пропускаются с
`staff_deferred_to_later_wave` и видны отдельным счётчиком в разделе `wave`.
Ещё раз сверить `wave.active_bookings_selected` с ожиданием по Altegio.

Для волны 2 и далее здесь же проверить `previous_wave_context.proven: true` —
записи предыдущих волн живут в отдельном разделе отчёта, а не в `blocked_rows`
(шаг 5a).

Записать `plan_digest` из отчёта.

## 13. Canary — одна **названная** запись

**Что делает:** создаёт одно бронирование, выбранное по точной source identity,
перечитывает его через GET и строго сверяет booking UUID, marker, location,
staff, service, customer, время начала, длительность и активный статус. Результат
сохраняется durable proof'ом.
**Зачем:** тело `POST /bookings` подтверждено планом как эндпоинт, но не как
схема. Canary доказывает форму запроса на одной живой записи прежде, чем ей
подвергнутся сотни. `--limit` больше не существует: он брал первую попавшуюся
строку ответа Altegio API — на каждом прогоне другого живого клиента.

Выбрать конкретную запись из `dry-run` (её `source_company_id` и
`source_record_id`) и подставить ниже.

Для волны 2 и далее gate дополнительно требует
`previous_wave_context_unprovable` **не** в списке отказов: manifest, потерявший
mapping или baseline живой записи предыдущей волны, останавливает canary **до
первого POST** (шаг 5a). Тот же gate стоит и перед bulk apply на шаге 16.

```bash
docker compose --profile ops run --rm --build easyweek-booking-migration canary --manifest /migration/input/manifest.json --customer-directory /migration/input/easyweek-customers-after-import.csv --cutover-at 2026-09-01T00:00:00+02:00 --verified-dry-run-id ПЛАН_DIGEST_ИЗ_ШАГА_12 --canary-company-id 758285 --canary-record-id ID_ЗАПИСИ --confirm-easyweek-native-notifications-disabled --apply
```

Green — когда `errors` пуст и `totals.created = 1`. Любое несовпадение поля
означает, что canary не доказан, и bulk запрещён.

**Если canary завершился неизвестным исходом** (`canary_post_uncertain` —
timeout, обрыв, 5xx или успешный ответ без читаемого UUID): бронирование, скорее
всего, создано, но подтвердить это нечем. **Повторять canary POST для той же
записи запрещено** — это единственный способ выдать живому клиенту два
бронирования. Bulk остаётся закрытым.

Порядок действий: найти бронирование в интерфейсе EasyWeek по marker'у
`altegio-migration:<company_id>:<record_id>` и запустить `resolve-created`
(шаг 17) для этой же source identity. Успешное полное доказательство
одновременно переводит ledger-строку в `created` **и** повышает сам canary proof
до `verified` — одной транзакцией. Только после этого bulk разрешён.

Если бронирования в EasyWeek действительно нет — `resolve-absent` из шага 17, и
затем canary повторяется как новая попытка.

## 14. Проверить, что клиент ничего не получил

**Что делает:** оператор проверяет почту/SMS/WhatsApp/push тестового клиента и
Chatwoot.
**Зачем:** одно неожиданное уведомление означает, что нативный канал остался
включённым, и bulk превратил бы ошибку в сотни сообщений живым людям.

**Если хотя бы одно уведомление обнаружено — дальнейший apply останавливается.**
Вернуться к шагу 6, а последующие запуски помечать
`--canary-notification-observed`, что gate трактует как безусловный отказ.

**Что делает:** подтверждает, что миграция не создала клиентских задач.

```bash
docker compose exec postgres psql -U "${POSTGRES_USER:-altegio}" -d "${POSTGRES_DB:-altegio_bot}" -c "select count(*) as easyweek_jobs_last_hour from message_jobs where provider = 'easyweek' and created_at > now() - interval '1 hour'"
```

Ожидается `0`.

## 15. Reconciliation canary

**Что делает:** печатает состояние ledger и разрешает то, что можно разрешить.
**Зачем:** перед bulk не должно оставаться ни одной записи с неизвестным исходом.

Строка с известным target UUID повышается до `created` только после полного
доказательства — успешного GET недостаточно. Поэтому команде передаются
`--customer-directory` (без него такие строки останутся `uncertain`) и тот же
`--cutover-at`, с которым выполнялся apply: без него команда откажется
запускаться.

```bash
docker compose --profile ops run --rm --build easyweek-booking-migration reconcile --manifest /migration/input/manifest.json --customer-directory /migration/input/easyweek-customers-after-import.csv --cutover-at 2026-09-01T00:00:00+02:00
```

Требуется `uncertain = 0` и `pending = 0`.

## 16. Bulk apply

**Что делает:** создаёт все оставшиеся `ready` бронирования, с паузами под лимит
EasyWeek 60 запросов/мин. Перед каждым POST выполняется последний read-only
re-proof исходной записи.
**Зачем:** основная работа. Уже созданное повторно не создаётся, а запись,
отменённая или перенесённая во время прогона, не создаётся вовсе.

Digest после canary изменился, поэтому dry-run повторяется.

```bash
docker compose --profile ops run --rm --build easyweek-booking-migration dry-run --manifest /migration/input/manifest.json --customer-directory /migration/input/easyweek-customers-after-import.csv --cutover-at 2026-09-01T00:00:00+02:00
```

```bash
docker compose --profile ops run --rm --build easyweek-booking-migration apply --manifest /migration/input/manifest.json --customer-directory /migration/input/easyweek-customers-after-import.csv --cutover-at 2026-09-01T00:00:00+02:00 --verified-dry-run-id НОВЫЙ_ПЛАН_DIGEST --confirm-easyweek-native-notifications-disabled --apply
```

Остановка на `uncertain` — штатное поведение, не сбой. Перейти к шагу 17.

## 17. Разрешение неизвестных исходов

**Что делает:** timeout, обрыв и 5xx оставляют запись в `uncertain` **без**
target UUID — перечитать нечего. Оператор находит бронирование в интерфейсе
EasyWeek по marker'у `altegio-migration:<company_id>:<record_id>` и называет его.
Инструмент не верит на слово: он делает GET и доказывает marker, филиал и
критичные поля.

Инструмент перечитывает исходную запись, восстанавливает бронирование, которое
миграция собиралась создать, и требует точного совпадения marker, филиала,
мастера, услуги, клиента, времени начала, длительности и активного статуса.
Поэтому команде нужны тот же `--customer-directory` (без него невозможно
восстановить ожидаемого клиента) и тот же неизменный `--cutover-at` — **ровно то
значение, с которым выполнялся apply**. Инструмент дополнительно сверяет весь
scope волны и откажет с `migration_scope_*`, если manifest, selector, cutover,
горизонт, филиал или версия схемы отличаются от доказанных.

```bash
docker compose --profile ops run --rm --build easyweek-booking-migration resolve-created --manifest /migration/input/manifest.json --customer-directory /migration/input/easyweek-customers-after-import.csv --cutover-at 2026-09-01T00:00:00+02:00 --resolve-company-id 758285 --resolve-record-id ID_ЗАПИСИ --target-uuid UUID_НАЙДЕННОГО_БРОНИРОВАНИЯ
```

Любое несовпадение оставляет строку `uncertain` — это не сбой команды, а отказ
записать недоказанное.

**Та же команда восстанавливает и неизвестный canary.** Когда неопределённым
оказался именно canary, его proof лежит `verified=false`, и обычный scope gate
такой волны не знает. Для этой — и только для этой — строки допускается
восстановление по её собственному attempt'у: инструмент требует полного
совпадения binding волны, source identity, origin run и ровно одной попытки, а
затем выполняет то же самое полное доказательство. При успехе ledger и canary
proof подтверждаются одной транзакцией; при отказе не меняется ничего и не
выполняется ни одного обращения к Altegio или EasyWeek. Unverified proof
по-прежнему не открывает ни bulk, ни финальную reconciliation.

**Если бронирования в EasyWeek действительно нет.** Это опасное направление:
после подтверждения следующий apply создаст запись, и ошибка оператора означает
два бронирования у живого человека. Поэтому нужны два отдельных флага.

```bash
docker compose --profile ops run --rm --build easyweek-booking-migration resolve-absent --manifest /migration/input/manifest.json --cutover-at 2026-09-01T00:00:00+02:00 --resolve-company-id 758285 --resolve-record-id ID_ЗАПИСИ --i-verified-the-booking-does-not-exist-in-easyweek --i-understand-the-next-apply-will-create-it
```

После разрешения повторить шаг 16.

## 18. Delta apply

**Что делает:** переносит записи, созданные в Altegio уже после bulk.
**Зачем:** между dry-run и cutover клиенты продолжают записываться. `cutover_at`
остаётся прежним, иначе граница поедет.

```bash
docker compose --profile ops run --rm --build easyweek-booking-migration dry-run --manifest /migration/input/manifest.json --customer-directory /migration/input/easyweek-customers-after-import.csv --cutover-at 2026-09-01T00:00:00+02:00
```

```bash
docker compose --profile ops run --rm --build easyweek-booking-migration apply --manifest /migration/input/manifest.json --customer-directory /migration/input/easyweek-customers-after-import.csv --cutover-at 2026-09-01T00:00:00+02:00 --verified-dry-run-id PLAN_DIGEST_ЭТОГО_DRY_RUN --confirm-easyweek-native-notifications-disabled --apply
```

## 19. Финальная reconciliation

**Что делает:** перечитывает **живой** Altegio, а затем **перечитывает каждый
target в EasyWeek**: GET, строгая проверка marker, location, мастера, услуги,
клиента, времени начала, длительности и активного статуса, и сверка live
fingerprint с сохранённым при создании. Печатает source active bookings,
created, already_migrated, blocked, failed, uncertain, source_changed, число
отложенных, разрез по филиалам и reason codes.
**Зачем:** перечисление ledger доказывает только то, что мы и так знаем, и
остаётся истинным после того, как booking удалили, отменили или перенесли.
Полноту cutover доказывает лишь сверка и с источником, и с целью.

```bash
docker compose --profile ops run --rm --build easyweek-booking-migration reconcile --final --manifest /migration/input/manifest.json --customer-directory /migration/input/easyweek-customers-after-import.csv --cutover-at 2026-09-01T00:00:00+02:00
```

Перед чтением источника и любого target команда доказывает scope волны. В
отчёте это раздел `scope`: `scope_proven: true` и `wave_identity`. Если manifest,
selector, `--cutover-at`, `--horizon-days`, филиал или версия схемы отличаются от
доказанных canary, команда завершается ненулевым кодом с `migration_scope_*` и
**не** делает вывода о полноте — именно так закрывается обход, при котором
смещённое окно или переведённый в deferred мастер убирали записи из проверки.

PASS только при `completeness.passed = true`: `uncertain = 0`, `pending = 0`,
`failed = 0`, `live_targets_proven = accounted_for`, и каждая активная запись
**выбранной волны** имеет доказанный живой target. Записи отложенных мастеров
учитываются в `deferred_bookings` и пробелом не считаются; неизвестный мастер —
считается и валит проверку. Иначе команда завершается ненулевым кодом, а
`unaccounted_reason_codes` называет причину по каждой строке.

**Проверка двусторонняя.** Кроме «каждая активная запись источника имеет
доказанный живой target», выполняется обратная: **каждая строка ledger со
статусом `created`** обязана быть либо только что доказана активной, либо иметь
target, доказанно отсутствующий (404) или доказанно завершённый
(`is_canceled`/`is_completed` при совпавшем marker).

Зачем: клиент может отменить запись в Altegio уже после переноса. Тогда источник
классифицируется как `skipped`, и раньше его EasyWeek-бронирование выпадало из
проверки — лишняя активная запись оставалась в расписании при зелёном отчёте.
Теперь это `source_inactive_target_still_active`, и PASS не выдаётся.

Нечитаемый или malformed target при неактивном источнике — тоже отказ
(`source_inactive_target_unreadable` / `source_inactive_target_malformed`): «не
смогли прочитать» не значит «его нет».

Команда **ничего не отменяет**: она только сообщает и блокирует PASS. Что делать
с ghost-бронированием — решает человек; отмена выполняется вручную в интерфейсе
EasyWeek либо через `rollback` (шаг 19a), если запись входит в run, который
целиком откатывается.

Поля отчёта для этой проверки: `migration_targets_checked`,
`inactive_source_targets_checked`, `inactive_source_targets_terminal`,
`ghost_targets_active` и `manual_action_required` — список source identity,
требующих ручного действия.

Здесь же оператор сверяет `wave.active_bookings_selected` с числом записей,
фактически появившихся в EasyWeek у выбранных мастеров.

## 19a. Rollback — **внутри того же окна**

Универсального автоматического отката нет. Безопасный откат: остановить apply,
сохранить ledger и отчёты, найти только записи конкретного run и не трогать
изменённые вручную. Rollback выполняется **до** шага 20: отмена тоже порождает
события EasyWeek, и с включёнными уведомлениями она сообщила бы об отмене
каждому из мигрированных клиентов. Инструмент проверяет тот же notification
gate и откажет, если уведомления вернули.

**Что делает:** показывает, что откат отменил бы. Только GET.

```bash
docker compose --profile ops run --rm --build easyweek-booking-migration rollback-dry-run --manifest /migration/input/manifest.json --cutover-at 2026-09-01T00:00:00+02:00 --rollback-run-id RUN_ID_ИЗ_ОТЧЁТА_APPLY
```

Записи, у которых изменились время, мастер, услуга, клиент, филиал,
длительность или marker, помечаются
`rollback_target_modified_after_migration` и не отменяются.

**Что делает:** реально отменяет отобранные записи. Требует двух явных флагов.

```bash
docker compose --profile ops run --rm --build easyweek-booking-migration rollback-dry-run --manifest /migration/input/manifest.json --cutover-at 2026-09-01T00:00:00+02:00 --rollback-run-id RUN_ID --confirm-easyweek-native-notifications-disabled --apply --confirm-rollback
```

---

## 20. Отдельное решение о включении уведомлений

**Что делает:** ничего автоматического. Обратное включение каналов — отдельное
ручное решение владельца проекта **после** PASS на шаге 19 и после того, как
rollback (если он был нужен) выполнен.
**Зачем:** миграция не знает, когда бизнес готов снова писать клиентам, а
автоматическое включение сразу после массового создания записей — ровно тот
случай, когда рассылка уходит по всей мигрированной базе.

Порядок: сначала нативные каналы EasyWeek в UI, затем — при необходимости —
`EASYWEEK_NOTIFICATIONS_ENABLED` / `EASYWEEK_REVIEWS_ENABLED` /
`EASYWEEK_REVIEW_SEND_ENABLED` / `EASYWEEK_REMINDER_API_GUARD_ENABLED` с
`--force-recreate` затронутых контейнеров. Если outbox worker останавливали —
`docker compose start altegio-outbox-worker`.

После этого удалить экспорт клиентов из `$EASYWEEK_MIGRATION_INPUT_DIR` — он
больше не нужен.

---

## Что никогда не попадает в Git

Токены и API-ключи; клиентские XLS/XLSX/CSV; телефоны и имена; сырые
webhook/API payload'ы; manifest'ы и отчёты с PII; экспорты EasyWeek; содержимое
`outputs/`. Правила — в `.gitignore`. В репозитории живёт только
`docs/easyweek/migration_manifest.example.json` — шаблон с placeholder-значениями.
Реальные production ID, имена, телефоны и UUID клиентов в него не переносятся ни
при каких обстоятельствах, в том числе «для наглядности» после успешной волны.
