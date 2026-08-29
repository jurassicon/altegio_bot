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

Инструмент запускается **внутри контейнера EasyWeek-воркера**. Это не стилевое
предпочтение: gate проверяет effective settings текущего процесса, и только
запуск с `env_file` самого воркера делает эту проверку утверждением о воркере, а
не о чьём-то ноутбуке.

---

## 0. Что означают режимы

| Режим | Пишет в EasyWeek | Пишет в БД | Назначение |
|---|---|---|---|
| `inventory` | нет | нет | что есть в Altegio и что покрыто mapping'ом |
| `dry-run` | нет | нет | проверяемый план; его `plan_digest` открывает apply |
| `apply` | да, только через полный gate | ledger | единственный пишущий режим |
| `reconcile` | только GET | ledger | разрешает `uncertain` строки по факту |
| `rollback-dry-run` | только GET | нет | что откат **бы** отменил |

`dry-run` — режим по умолчанию. Без `--apply` ни один mutation-запрос не
существует как достижимый путь кода.

---

## 1. Экспорт клиентов EasyWeek **до** импорта счётчиков

**Что делает:** сохраняет снимок клиентской базы EasyWeek до того, как импорт её
изменит.
**Зачем:** это единственная точка возврата. Если импорт счётчиков склеит или
задвоит клиентов, восстановить «как было» можно только из этого файла.

Настройки → Клиенты → Экспорт. Сохранить **вне репозитория**, например
`/opt/altegio_bot/outputs/easyweek-customers-before-import.csv`.
`outputs/` в `.gitignore`; коммитить экспорт нельзя — это PII.

## 2. Импорт исторических счётчиков визитов

**Что делает:** загружает в EasyWeek количество завершённых визитов по каждому
клиенту нативным импортом.
**Зачем:** это единственный доказанный способ перенести историю. PR-11.1 не
пишет счётчики через API, а PR-12 (`repeat_10d` / `comeback_3d`) принимает
решение «писать клиенту или нет» именно по этому числу: постоянный клиент,
приехавший в EasyWeek с нулём визитов, получит сообщение для новичка.

## 3. Сохранить отчёт импорта

**Что делает:** сохраняет выданный EasyWeek отчёт импорта.
**Зачем:** отчёт — единственное доказательство, сколько строк принято, сколько
отклонено и почему. Без него расхождение счётчиков через месяц не расследуется.

## 4. Повторный экспорт клиентов EasyWeek

**Что делает:** выгружает клиентскую базу **после** импорта.
**Зачем:** два разных применения одного файла:

1. сверка с шагом 1 — импорт не создал дублей;
2. это и есть `--customer-directory` для миграции. Клиент ищется по точному
   нормализованному международному номеру: ноль совпадений и больше одного —
   `blocked`, ровно одно — берётся его UUID. Клиенты автоматически не создаются.

Файл — PII. Держать в `outputs/`, не коммитить, после миграции удалить.

## 5. Подготовка mapping location / staff / service

**Что делает:** заполняется manifest — явное соответствие Altegio-идентификаторов
и EasyWeek UUID.
**Зачем:** ни один из трёх идентификаторов не выводится автоматически. Fuzzy-match
по именам верен в 95 % случаев, а оставшиеся 5 % — это запись к другому мастеру,
и узнаёт об этом клиент.

Шаблон и правила: `docs/easyweek/migration_manifest.example.json` и
`docs/easyweek/migration_manifest.README.md`.

**Что делает команда:** выгружает список локаций EasyWeek с их UUID и
человекочитаемыми именами, read-only.

```bash
docker compose exec altegio-easyweek-inbox-worker python -m altegio_bot.scripts.easyweek_probe --redact-pii
```

**Что делает команда:** показывает, какие Altegio staff/service id реально
встречаются в будущих записях, чтобы заполнить manifest ровно ими. Ничего не
пишет и не требует экспорта клиентов.

```bash
docker compose exec altegio-easyweek-inbox-worker python -m altegio_bot.scripts.easyweek_migration inventory --manifest /opt/altegio_bot/outputs/migration_manifest.json
```

Повторять шаги 5 и `inventory`, пока `mapping_missing` не станет нулём.

## 6. Отключить **все** нативные уведомления EasyWeek

**Что делает:** оператор вручную выключает в интерфейсе EasyWeek каждый
клиентский канал: email, SMS, push, WhatsApp и любые другие подключённые, а
также автоматические подтверждения, напоминания и сообщения об изменении записи.
**Зачем:** это обязательное условие миграции, а не рекомендация. Инструмент
создаёт сотни будущих записей за минуты; с включёнными нативными уведомлениями
столько же живых людей одновременно получат сообщение о «новой записи», которую
они сделали недели назад. Код это не видит и выключить не может.

## 7. Выключить уведомления и отзывы бота

**Что делает:** в `easyweek.env` выставляется

```bash
EASYWEEK_NOTIFICATIONS_ENABLED=false
```

```bash
EASYWEEK_REVIEWS_ENABLED=false
```

**Зачем:** это наша половина той же защиты. Миграция породит вебхуки
`booking-created`; с включёнными флагами бот отправит по ним lifecycle-сообщения
и запланирует отзывы. Оба флага проверяются gate'ом и без них apply не начнётся.

## 8. Оставить capture и processing включёнными

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
`visits_total`, и вместе с доказательствами для reconciliation. Gate требует
оба флага включёнными.

Visit counter (`EASYWEEK_VISIT_COUNTER_ENABLED=true`) можно оставить: он ничего
не отправляет, а лишь фиксирует уже подтверждённый EasyWeek факт.

## 9. Пересоздать воркер и проверить effective settings

**Что делает:** пересоздаёт контейнер, чтобы он перечитал `env_file`. Обычный
`restart` этого не делает — env_file читается только при создании контейнера.

```bash
docker compose up -d --force-recreate altegio-easyweek-inbox-worker
```

**Что делает:** печатает флаги, с которыми процесс реально работает.
**Зачем:** «я поправил .env» и «воркер работает с новыми значениями» — разные
утверждения, и между ними стоит именно `--force-recreate`.

```bash
docker compose exec altegio-easyweek-inbox-worker python -c "from altegio_bot.easyweek_migration.gates import read_effective_settings; import json; print(json.dumps(read_effective_settings().as_safe_dict(), indent=2))"
```

Ожидается: `EASYWEEK_ENABLED=true`, `EASYWEEK_PROCESSING_ENABLED=true`,
`EASYWEEK_NOTIFICATIONS_ENABLED=false`, `EASYWEEK_REVIEWS_ENABLED=false`.

## 10. Inventory

**Что делает:** читает Altegio API по обоим филиалам и печатает, что там есть и
что покрыто manifest'ом. Не пишет ничего и не требует экспорта клиентов.
**Зачем:** последняя проверка mapping'а до того, как в игру вступает PII-файл.

```bash
docker compose exec altegio-easyweek-inbox-worker python -m altegio_bot.scripts.easyweek_migration inventory --manifest /opt/altegio_bot/outputs/migration_manifest.json --cutover-at 2026-09-01T00:00:00+02:00
```

## 11. Dry-run

**Что делает:** строит полный план — ready / already_migrated / blocked /
skipped — и печатает `plan_digest`. Ни одного EasyWeek-запроса, ни одной строки
в ledger.
**Зачем:** это артефакт, который проверяет человек, и его `plan_digest` —
единственный ключ, открывающий apply.

```bash
docker compose exec altegio-easyweek-inbox-worker python -m altegio_bot.scripts.easyweek_migration dry-run --manifest /opt/altegio_bot/outputs/migration_manifest.json --customer-directory /opt/altegio_bot/outputs/easyweek-customers-after-import.csv --cutover-at 2026-09-01T00:00:00+02:00
```

Разобрать **каждую** строку `blocked_rows` вручную. Типовые причины:
`staff_mapping_missing`, `service_mapping_missing`, `customer_not_found`,
`customer_ambiguous`, `multi_service_unsupported`, `custom_price_unsupported`,
`custom_duration_unsupported`, `start_time_ambiguous_dst`. Ни одна из них не
чинится инструментом — либо правится manifest/экспорт, либо запись переносится
руками.

Записать `plan_digest` из отчёта.

## 12. Canary — ровно одна запись

**Что делает:** создаёт **одно** бронирование в EasyWeek.
**Зачем:** тело `POST /bookings` подтверждено планом как эндпоинт, но не как
схема. Canary — то, что доказывает форму запроса на одной живой записи прежде,
чем ей подвергнутся сотни. Он же проверяет, что нативные уведомления действительно
молчат.

```bash
docker compose exec altegio-easyweek-inbox-worker python -m altegio_bot.scripts.easyweek_migration apply --manifest /opt/altegio_bot/outputs/migration_manifest.json --customer-directory /opt/altegio_bot/outputs/easyweek-customers-after-import.csv --cutover-at 2026-09-01T00:00:00+02:00 --verified-dry-run-id <plan_digest из шага 11> --confirm-easyweek-native-notifications-disabled --apply --limit 1
```

## 13. Проверить, что клиент ничего не получил

**Что делает:** оператор проверяет почту/SMS/WhatsApp/push тестового клиента и
Chatwoot.
**Зачем:** одно неожиданное уведомление означает, что нативный канал остался
включённым, и bulk превратил бы ошибку в сотни сообщений живым людям.

**Если хотя бы одно уведомление обнаружено — дальнейший apply останавливается.**
Вернуться к шагу 6, а последующие запуски помечать
`--canary-notification-observed`, что gate трактует как безусловный отказ.

**Что делает:** подтверждает, что миграция не создала ни одной клиентской
задачи на отправку.

```bash
docker compose exec postgres psql -U "${POSTGRES_USER:-altegio}" -d "${POSTGRES_DB:-altegio_bot}" -c "select count(*) as easyweek_jobs_last_hour from message_jobs where provider = 'easyweek' and created_at > now() - interval '1 hour'"
```

```bash
docker compose exec postgres psql -U "${POSTGRES_USER:-altegio}" -d "${POSTGRES_DB:-altegio_bot}" -c "select status, count(*) from easyweek_migration_ledger group by status order by status"
```

Ожидается `easyweek_jobs_last_hour = 0`. Любое ненулевое значение означает, что
флаг уведомлений остался включённым — вернуться к шагам 7 и 9.

## 14. Reconciliation canary

**Что делает:** печатает состояние ledger и разрешает `uncertain` строки чтением
EasyWeek.
**Зачем:** перед bulk не должно оставаться ни одной записи, про которую неизвестно,
создалась она или нет.

```bash
docker compose exec altegio-easyweek-inbox-worker python -m altegio_bot.scripts.easyweek_migration reconcile --manifest /opt/altegio_bot/outputs/migration_manifest.json --cutover-at 2026-09-01T00:00:00+02:00
```

Требуется `uncertain = 0`.

## 15. Bulk apply

**Что делает:** создаёт все оставшиеся `ready` бронирования, с паузами под
лимит EasyWeek 60 запросов/мин.
**Зачем:** основная работа миграции. Уже созданное повторно не создаётся —
ledger не даёт claim'ить строку со статусом `created`.

Digest после canary изменился, поэтому dry-run повторяется, и его новый
`plan_digest` подставляется в команду.

```bash
docker compose exec altegio-easyweek-inbox-worker python -m altegio_bot.scripts.easyweek_migration dry-run --manifest /opt/altegio_bot/outputs/migration_manifest.json --customer-directory /opt/altegio_bot/outputs/easyweek-customers-after-import.csv --cutover-at 2026-09-01T00:00:00+02:00
```

```bash
docker compose exec altegio-easyweek-inbox-worker python -m altegio_bot.scripts.easyweek_migration apply --manifest /opt/altegio_bot/outputs/migration_manifest.json --customer-directory /opt/altegio_bot/outputs/easyweek-customers-after-import.csv --cutover-at 2026-09-01T00:00:00+02:00 --verified-dry-run-id <новый plan_digest> --confirm-easyweek-native-notifications-disabled --apply
```

Если прогон остановился на `uncertain` — это штатное поведение, а не сбой.
Выполнить шаг 14 и только потом повторить apply.

## 16. Повторный dry-run и delta apply

**Что делает:** ловит записи, созданные в Altegio уже после bulk, и переносит
только их.
**Зачем:** между dry-run и cutover клиенты продолжают записываться. Delta — это
тот же цикл, а не особый режим; `cutover_at` остаётся прежним, иначе граница
поедет.

```bash
docker compose exec altegio-easyweek-inbox-worker python -m altegio_bot.scripts.easyweek_migration dry-run --manifest /opt/altegio_bot/outputs/migration_manifest.json --customer-directory /opt/altegio_bot/outputs/easyweek-customers-after-import.csv --cutover-at 2026-09-01T00:00:00+02:00
```

```bash
docker compose exec altegio-easyweek-inbox-worker python -m altegio_bot.scripts.easyweek_migration apply --manifest /opt/altegio_bot/outputs/migration_manifest.json --customer-directory /opt/altegio_bot/outputs/easyweek-customers-after-import.csv --cutover-at 2026-09-01T00:00:00+02:00 --verified-dry-run-id <plan_digest этого dry-run> --confirm-easyweek-native-notifications-disabled --apply
```

## 17. Финальная reconciliation

**Что делает:** итоговый PII-free отчёт: source active bookings, created,
already_migrated, blocked, uncertain, failed, разрез по филиалам и распределение
reason codes.
**Зачем:** это документ, по которому принимается решение «миграция закончена».

```bash
docker compose exec altegio-easyweek-inbox-worker python -m altegio_bot.scripts.easyweek_migration reconcile --manifest /opt/altegio_bot/outputs/migration_manifest.json --cutover-at 2026-09-01T00:00:00+02:00
```

Приёмка: `uncertain = 0`, `failed = 0`, каждая `blocked` строка либо перенесена
руками, либо сознательно оставлена, `created + already_migrated` сходится с
`source active bookings`.

После этого удалить экспорт клиентов из `outputs/` — он больше не нужен.

## 18. Отдельное решение о включении уведомлений

**Что делает:** ничего автоматического. Обратное включение каналов — отдельное
ручное решение владельца проекта **после** шага 17.
**Зачем:** миграция не знает, когда бизнес готов снова писать клиентам, а
автоматическое включение сразу после массового создания записей — ровно тот
случай, когда рассылка уходит по всей мигрированной базе.

Порядок: сначала нативные каналы EasyWeek в UI, затем — при необходимости —
`EASYWEEK_NOTIFICATIONS_ENABLED` / `EASYWEEK_REVIEWS_ENABLED` с
`--force-recreate` воркера.

## 19. Rollback

Универсального автоматического отката **нет**, и инструмент его не обещает.
Безопасный откат: остановить apply, сохранить ledger и отчёты, найти только
записи конкретного run и не трогать изменённые руками.

**Что делает:** показывает, что откат отменил бы. Только GET, ничего не меняет.
**Зачем:** режим по умолчанию. Записи, у которых пропал migration marker или
которые уже отменены/завершены, помечаются
`rollback_target_modified_after_migration` и не отменяются — кто-то работал с
ними после миграции.

```bash
docker compose exec altegio-easyweek-inbox-worker python -m altegio_bot.scripts.easyweek_migration rollback-dry-run --manifest /opt/altegio_bot/outputs/migration_manifest.json --cutover-at 2026-09-01T00:00:00+02:00 --rollback-run-id <run_id из отчёта apply>
```

**Что делает:** реально отменяет отобранные записи. Требует **двух** явных
флагов.
**Зачем:** отмена бронирования — это потеря записи у живого клиента, поэтому
одного «пишущего» флага недостаточно.

```bash
docker compose exec altegio-easyweek-inbox-worker python -m altegio_bot.scripts.easyweek_migration rollback-dry-run --manifest /opt/altegio_bot/outputs/migration_manifest.json --cutover-at 2026-09-01T00:00:00+02:00 --rollback-run-id <run_id> --apply --confirm-rollback
```

Ledger при откате сохраняет `target_booking_uuid`: через полгода вопрос «что
именно отменил откат» должен иметь ответ.

---

## Что никогда не попадает в Git

Токены и API-ключи; клиентские XLS/XLSX/CSV; телефоны и имена; сырые
webhook/API payload'ы; manifest'ы и отчёты с PII; экспорты EasyWeek; содержимое
`outputs/`. Соответствующие правила добавлены в `.gitignore`. В репозитории
живёт только `docs/easyweek/migration_manifest.example.json` — шаблон с
placeholder-значениями.
