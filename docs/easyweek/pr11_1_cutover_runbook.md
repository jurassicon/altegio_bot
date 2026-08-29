# PR-11.1 — production runbook: перенос будущих активных записей Altegio → EasyWeek

Область: **только будущие активные бронирования** филиалов Karlsruhe
(Altegio `758285`) и Rastatt (Altegio `1271200`). Прошлые, завершённые и
отменённые записи в EasyWeek не создаются никогда. Durlach в Altegio отсутствует
и в миграции не участвует ни на одном шаге.

Исторические счётчики визитов переносятся **нативным импортом клиентов
EasyWeek**, а не этим инструментом. Canary подтвердил: EasyWeek сохраняет
импортированный baseline, после завершённой записи `booking-succeeded` приносит
числовой `visits_total`, и PR-11 сохраняет его в локальный `Client`. Поэтому
PR-11.1 **не пишет счётчики через API**.

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
станут пустыми.

**Проверка филиалов.** Manifest дополнительно сверяется с рантайм-реестром
`EASYWEEK_LOCATION_MAP`: location ID, location UUID и slug филиала
(`758285 → karlsruhe`, `1271200 → rastatt`) должны совпасть. Перепутанные
Karlsruhe и Rastatt отвергаются до первого запроса.

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
`start_time_ambiguous_dst`. Ни одна не чинится инструментом — правится manifest
или экспорт, либо запись переносится руками.

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

```bash
docker compose --profile ops run --rm --build easyweek-booking-migration canary --manifest /migration/input/manifest.json --customer-directory /migration/input/easyweek-customers-after-import.csv --cutover-at 2026-09-01T00:00:00+02:00 --verified-dry-run-id ПЛАН_DIGEST_ИЗ_ШАГА_12 --canary-company-id 758285 --canary-record-id ID_ЗАПИСИ --confirm-easyweek-native-notifications-disabled --apply
```

Green — когда `errors` пуст и `totals.created = 1`. Любое несовпадение поля
означает, что canary не доказан, и bulk запрещён.

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

```bash
docker compose --profile ops run --rm --build easyweek-booking-migration reconcile --manifest /migration/input/manifest.json --cutover-at 2026-09-01T00:00:00+02:00
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

```bash
docker compose --profile ops run --rm --build easyweek-booking-migration resolve-created --manifest /migration/input/manifest.json --cutover-at 2026-09-01T00:00:00+02:00 --resolve-company-id 758285 --resolve-record-id ID_ЗАПИСИ --target-uuid UUID_НАЙДЕННОГО_БРОНИРОВАНИЯ
```

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

**Что делает:** перечитывает **живой** Altegio и сверяет каждую активную запись
с доказанным target. Печатает source active bookings, created, already_migrated,
blocked, failed, uncertain, source_changed, разрез по филиалам и reason codes.
**Зачем:** перечисление ledger доказывает только то, что мы и так знаем. Полноту
cutover доказывает лишь сверка с источником.

```bash
docker compose --profile ops run --rm --build easyweek-booking-migration reconcile --final --manifest /migration/input/manifest.json --customer-directory /migration/input/easyweek-customers-after-import.csv --cutover-at 2026-09-01T00:00:00+02:00
```

PASS только при `completeness.passed = true`: `uncertain = 0`, `pending = 0`,
`failed = 0`, каждая активная запись источника имеет доказанный target либо
принятый операторский blocked-исход. Иначе команда завершается ненулевым кодом.

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
