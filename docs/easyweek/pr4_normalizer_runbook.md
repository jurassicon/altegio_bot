# PR-4 — normalizer и easyweek_inbox_worker (операторский runbook)

Что добавляет PR-4: отдельный воркер, который берёт строки `easyweek_events`
со статусом `captured`, нормализует payload и обновляет provider-scoped
`clients` / `records`. Отправки сообщений в PR-4 нет.

**Этот документ не выполняет деплой.** Он описывает безопасный порядок
включения, который выполняет оператор вручную.

---

## 0. Жёсткие запреты

- Не включать `EASYWEEK_NOTIFICATIONS_ENABLED=true` на production в рамках PR-4.
- Не выполнять `downgrade` миграции PR-3 (`c1a7d3f905b2`). Откат PR-4 снимает
  ТОЛЬКО две свои колонки и останавливается на PR-3.
- Никаких реальных Meta/WABA/WhatsApp/Chatwoot отправок и никаких mutation-
  вызовов EasyWeek API.
- Не править `alembic_version` руками и не выполнять `alembic stamp`.

---

## 1. Три независимых флага

| Флаг | Чем управляет | Production после PR-4 |
|---|---|---|
| `EASYWEEK_ENABLED` | ТОЛЬКО публичный capture endpoint | `true` (уже) |
| `EASYWEEK_PROCESSING_ENABLED` | claim + нормализация captured строк | `false` |
| `EASYWEEK_NOTIFICATIONS_ENABLED` | создание EasyWeek `MessageJob` | `false` |

Почему флаг отдельный: в production `EASYWEEK_ENABLED` уже `true` ради capture.
Если бы воркер гейтился тем же флагом, он начал бы разбирать **весь накопленный
backlog** сразу после деплоя PR-4. Выключение обработки при этом никогда не
выключает capture — доставки продолжают сохраняться и просто ждут.

Дополнительно:

```text
EASYWEEK_LOCATION_MAP={}      # пусто/невалидно → воркер не claim'ит
EASYWEEK_INBOX_WORKER_POLL_SEC=1.0
```

При `EASYWEEK_PROCESSING_ENABLED=true` и пустом/невалидном реестре воркер
fail-closed: он не берёт события, потому что не смог бы отличить свою локацию
от чужой.

> `docker compose restart` **не** перечитывает `env_file`. После правки любого
> флага нужен `up -d --force-recreate <сервис>`.

`easyweek.env` читают ТРИ сервиса, и все — с `required: false`, чтобы
Altegio-only хост без этого файла продолжал разворачиваться:

* `altegio-api` — приём вебхуков (PR-1/PR-2);
* `altegio-easyweek-inbox-worker` — нормализатор (PR-4);
* `altegio-outbox-worker` — общий outbox, он рендерит EasyWeek lifecycle jobs
  (PR-5).

Compose передаёт каждому сервису файл целиком; selective interpolation не
используется. Поэтому «кто читает переменную» — это вопрос кода, а не Compose,
и таблица ниже отвечает на практический вопрос: какой сервис пересоздавать.

| Группа переменных | Кто реально читает | Что пересоздавать |
| --- | --- | --- |
| `EASYWEEK_ENABLED`, `EASYWEEK_WEBHOOK_SECRET` | приём и запись вебхуков | `altegio-api` |
| `EASYWEEK_PROCESSING_ENABLED`, `EASYWEEK_LOCATION_MAP`, `EASYWEEK_INBOX_WORKER_POLL_SEC`, `EASYWEEK_NOTIFICATIONS_ENABLED` | нормализатор и планирование job | `altegio-easyweek-inbox-worker` |
| `EASYWEEK_LOCATION_MAP`, `EASYWEEK_DEFAULT_LANGUAGE` | рендер lifecycle-сообщений | `altegio-outbox-worker` |
| `EASYWEEK_API_*` (probe) | только ручной запуск probe-команды | тот сервис/команда, где probe реально запускается |

```bash
docker compose -p altegio_bot up -d --force-recreate altegio-outbox-worker
```

Обычный `restart altegio-outbox-worker` оставит контейнер со старыми
значениями, и это молчаливый режим отказа: невалидный или пустой
`booking_page_url` филиала в реестре роняет lifecycle-job локально, а устаревший язык
уйдёт в Meta.

`booking_page_url` из реестра валидируется на send-time, а не на старте
процесса — умышленно. Глобальный Settings-валидатор уронил бы общий
outbox-воркер и вместе с ним весь Altegio-трафик из-за EasyWeek-опечатки.
Требования: абсолютный URL, только `https`, обязательный hostname, без
credentials, без fragment, без control-символов. Невалидное значение приводит
к локальному `failed` только EasyWeek lifecycle-job.

---

## 2. Безопасный порядок включения

### Шаг 1 — оставить production как есть

```text
EASYWEEK_ENABLED=true
EASYWEEK_PROCESSING_ENABLED=false
EASYWEEK_NOTIFICATIONS_ENABLED=false
```

### Шаг 2 — задеплоить PR-4

Обычный деплой. Появится новый сервис `altegio-easyweek-inbox-worker`.

### Шаг 3 — проверить, что capture продолжает отвечать 200

Сделать тестовую доставку из кабинета EasyWeek (Resend любой существующей
записи) и убедиться, что вебхук получил 200 и в таблице появилась новая строка.

### Шаг 3b — ОБЯЗАТЕЛЬНО: пересоздать `altegio-api` на новом image

```bash
docker compose -p altegio_bot up -d --force-recreate altegio-api
```

Почему это отдельный обязательный шаг, а не «оно само при деплое».

Миграция `d4e8a1c39f57` backfill'ит `booking_uuid` только для строк, которые
существовали в момент её выполнения. Деплой при этом **намеренно не
останавливает** `altegio-api` — capture должен продолжать принимать вебхуки.
Значит есть окно: доставка, пришедшая ПОСЛЕ backfill, но ДО пересоздания API,
записывается **старым** image, который про колонку `booking_uuid` не знает.

Такая строка получает валидный `uid` и `booking_uuid IS NULL`. Для причинного
порядка она невидима: не блокирует более позднюю доставку той же booking, сама
не блокируется более ранней, не участвует в canonical replay lookup — и после
transient retry способна лечь поверх более нового состояния и откатить время,
service snapshot, стоимость или связь с клиентом.

Проверить, что API уже на новом image:

```bash
docker compose -p altegio_bot exec -T altegio-api /app/.venv/bin/python -c "from altegio_bot.models.models import EasyWeekEvent; print('booking_uuid' in EasyWeekEvent.__table__.c)"
```

Ожидается `True`. Пока здесь `False`, включать обработку нельзя.

#### Почему завершение этой команды является write fence

Границы поддерживаемого rollout, зафиксированы явно:

1. Production-топология — **один контейнер `altegio-api`**, а не rolling API
   replicas. Одновременно работающих старых и новых API-процессов нет.
2. `EASYWEEK_PROCESSING_ENABLED=false` сохраняется до полного завершения
   пересоздания API.
3. Команда `docker compose -p altegio_bot up -d --force-recreate altegio-api`
   должна **полностью завершиться** до включения обработки.

Завершение этой команды и есть write fence. Для любой доставки, которую принимал
старый контейнер, возможны ровно два исхода:

- её транзакция закоммитилась до остановки контейнера — тогда строка уже в
  таблице, с `booking_uuid IS NULL`, и её найдёт reconciliation;
- транзакция не успела закоммититься — соединение закрыто вместе с процессом, и
  PostgreSQL откатывает её; строки не существует вовсе.

Третьего варианта — «строка появится позже, уже после reconciliation» — быть не
может, потому что писать её больше некому. После fence новый API записывает
canonical `booking_uuid` прямо на capture.

Только после этого разрешается включить processing и пересоздать EasyWeek-воркер
(шаги 6–7).

#### Rollback API на старый image

Порядок строго обратный, иначе старый producer снова начнёт писать строки без
ключа, пока воркер уже claim'ит:

1. поставить `EASYWEEK_PROCESSING_ENABLED=false`;
2. пересоздать EasyWeek-воркер и убедиться, что он перестал claim'ить события;
3. только затем откатывать `altegio-api`;
4. перед повторным включением — вернуть новый API image, снова пройти write
   fence и reconciliation gate.

**Ограничение, заявленное честно:** система НЕ поддерживает одновременно
работающий старый producer после включения обработки. Если в будущем появятся
rolling API replicas или другой процесс, пишущий в `easyweek_events`,
понадобится отдельный runtime barrier. В рамках PR-4 он намеренно не
реализуется.

### Шаг 3c — проверить, что окно закрыто

```bash
docker compose -p altegio_bot exec -T postgres sh -lc 'psql -tAX -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT count(*) FROM easyweek_events WHERE booking_uuid IS NULL AND jsonb_typeof(payload -> '"'"'uid'"'"') = '"'"'string'"'"'"'
```

Это число — **количество string-кандидатов**, а не количество валидных и не
количество гарантированно восстановимых UUID.

Запрос считает любую строку, у которой `uid` — JSON-строка. Сюда попадают и
заведомо malformed значения: `not-a-uuid`, синтетические smoke-доставки вида
`public-deploy-smoke-<uuid>` и прочее. Reconciliation увеличивает `repaired`
только для значений, которые `canonical_booking_uuid()` реально разобрал в UUID,
поэтому:

```text
repaired <= число string-кандидатов
```

Неравенство — норма, а не сбой. Malformed, отсутствующие и нестроковые `uid`
**ожидаемо** остаются с `booking_uuid IS NULL` и позже получают детерминированный
отказ (`invalid_booking_uuid` / `missing_booking_uuid`).

Число полезно записать как верхнюю границу для сверки, но **равенство
`repaired` и этого счётчика НЕ является gate деплоя.**

Не пытаться уточнить счётчик небезопасным приведением `(payload->>'uid')::uuid`:
на первом же malformed значении PostgreSQL прервёт запрос. Не добавлять и
отдельную regex-проверку — она неизбежно разойдётся с Python `uuid.UUID()`,
который является единственным источником истины (`canonical_booking_uuid()`).

### Шаг 4 — убедиться, что воркер работает, но backlog не тронут

```bash
docker compose -p altegio_bot ps altegio-easyweek-inbox-worker
```

После деплоя этот сервис входит в обязательные post-deploy проверки workflow
(`CRITICAL_SERVICES`): он обязан быть `running`, не в restart loop и не
`unhealthy` — иначе деплой падает.

Ожидается `running`, без restart-цикла. В логе — одна строка о том, что
обработка выключена, а не поток сообщений.

```bash
docker compose -p altegio_bot logs --tail=50 altegio-easyweek-inbox-worker
```

### Шаг 5 — зафиксировать безопасные счётчики (без payload)

```bash
docker compose -p altegio_bot exec -T postgres sh -lc 'psql -tAX -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT status, count(*) FROM easyweek_events GROUP BY status ORDER BY status"'
```

Записать числа. Ни payload, ни PII в отчёт не переносить.

### Шаг 6 — задать реестр локаций

В production `easyweek.env`:

```text
EASYWEEK_LOCATION_MAP=<JSON-реестр филиалов; формат см. easyweek.env.example>
```

### Шаг 7 — в контролируемом окне включить обработку

```text
EASYWEEK_PROCESSING_ENABLED=true
```

```bash
docker compose -p altegio_bot up -d --force-recreate altegio-easyweek-inbox-worker
```

Пересоздаётся **только** EasyWeek-воркер. Altegio-сервисы не трогаются.

**Перед первым claim воркер выполняет reconciliation.** Он находит строки с
`booking_uuid IS NULL`, вычисляет canonical UUID тем же парсером, что capture и
нормализатор, и записывает **только эту колонку**: payload, body, хэши и статус
не трогаются. Missing/нестроковые/malformed `uid` остаются `NULL` и доходят до
своего детерминированного отказа.

Это fail-closed: **пока reconciliation не завершилась успешно, ни одно событие
не claim'ится**. При ошибке воркер не берёт события, применяет ограниченный
backoff и пишет в лог только имя класса исключения. Параллельные реплики
сериализуются PostgreSQL advisory-lock; та, что не получила lock, ждёт, а не
начинает claim. Обход — keyset-пагинация по `id`, вся таблица в память не
загружается.

В логе после включения обработки:

```text
easyweek reconcile complete repaired=<N>
```

`N` — сколько строк реально получили canonical-ключ. Оно может быть **меньше**
числа string-кандидатов из шага 3c: malformed значения не восстанавливаются и
это ожидаемо.

Gate прохождения — не равенство чисел, а следующие три условия одновременно:

1. в логе появилась строка `easyweek reconcile complete`;
2. `easyweek reconcile_error` не повторяется;
3. воркер начал claim'ить события только ПОСЛЕ завершения reconciliation.

Если вместо этого повторяется `easyweek reconcile_error`, обработка НЕ идёт — это
ожидаемое fail-closed поведение, надо чинить причину, а не обходить проверку.

### Шаг 8 — проверки после включения

```bash
docker compose -p altegio_bot exec -T postgres sh -lc 'psql -tAX -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT status, error_code, count(*) FROM easyweek_events GROUP BY status, error_code ORDER BY status, error_code"'
```

Ожидается:

- реальные lifecycle-события (`booking-created` / `-updated` / `-rescheduled` /
  `-canceled`) → `processed`;
- старые synthetic smoke-строки → `failed` с безопасным кодом
  (`invalid_event_hint`, `foreign_location`, …). Это ожидаемое fail-closed
  поведение, а не поломка;
- ни одной строки в `processing` (это статус только внутри открытой транзакции).

Один EasyWeek Client и один EasyWeek Record на реальную запись:

```bash
docker compose -p altegio_bot exec -T postgres sh -lc 'psql -tAX -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT count(*) FROM clients WHERE provider = '"'"'easyweek'"'"'"'
```

```bash
docker compose -p altegio_bot exec -T postgres sh -lc 'psql -tAX -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT provider, company_id, easyweek_booking_uuid IS NOT NULL AS has_uuid, easyweek_booking_hash_id IS NOT NULL AS has_hash, short_link IS NOT NULL AS has_link, is_deleted FROM records WHERE provider = '"'"'easyweek'"'"'"'
```

Resend не должен был создать вторую доменную строку. **Проверять итоговое
состояние, а не только количество строк** — совпадения счётчиков недостаточно:
устаревшая повторная доставка может не создать вторую строку и при этом
откатить состояние существующей.

```bash
docker compose -p altegio_bot exec -T postgres sh -lc 'psql -tAX -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT r.easyweek_booking_uuid, r.is_deleted, r.starts_at, r.total_cost FROM records r WHERE r.provider = '"'"'easyweek'"'"'"'
```

Для записи, прошедшей полный цикл до отмены, ожидается `is_deleted = t` и
`starts_at`, равное ПОСЛЕДНЕМУ перенесённому времени. Если после Resend
`is_deleted` стало `f` или время откатилось к исходному создению — это регресс.

Клиентская связь не должна теряться на частичных доставках: `update`/`cancel`
без `customer_id` сохраняют и `client_id`, и `altegio_client_id`.

```bash
docker compose -p altegio_bot exec -T postgres sh -lc 'psql -tAX -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT r.easyweek_booking_uuid, r.client_id IS NOT NULL AS linked, r.altegio_client_id FROM records r WHERE r.provider = '"'"'easyweek'"'"'"'
```

Identity проверяется ДО cancel-guard: доставка с чужим numeric id получает
`failed/identity_conflict`, даже если её UUID уже отменён — иначе противоречивое
событие тихо помечалось бы `processed`.

Отмена терминальна: после `booking-canceled` ни `booking-updated`, ни
`booking-rescheduled`, ни повторный `booking-created` не снимают `is_deleted`,
не меняют время/услугу/стоимость/клиента и не создают новых lifecycle job.
Подтверждённого сигнала «отмена снята» в payload EasyWeek нет, а локализованный
`booking_status` парсить нельзя — поэтому fail-closed.

Услуга и стоимость должны быть сохранены (их использует PR-5):

```bash
docker compose -p altegio_bot exec -T postgres sh -lc 'psql -tAX -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT rs.service_id, rs.title, rs.cost_to_pay FROM record_services rs JOIN records r ON r.id = rs.record_id WHERE r.provider = '"'"'easyweek'"'"'"'
```

Ожидается ровно одна строка на booking с непустыми `title` и `cost_to_pay`.

Семантика присутствия для service/price полей та же, что и для остальных:

| Поле в доставке | Что происходит |
|---|---|
| отсутствует | известное значение сохраняется |
| присутствует со значением | snapshot обновляется |
| присутствует пустым (`""` / `null`) | значение **очищается** |

При явной очистке `services_description` fallback на `service_name` НЕ
срабатывает — иначе осталась бы устаревшая подпись.

`title` берётся из `services_description` (описание ВСЕГО набора услуг), и лишь
при его отсутствии/пустоте — из singular `service_name`: для booking с двумя услугами
одиночное имя вводило бы клиента в заблуждение. `amount` — из `services_count`
(иначе `quantity`).

`Record.total_cost` и `RecordService.cost_to_pay` обязаны совпадать — цена
синхронизируется даже когда доставка не содержит `service_id`:

```bash
docker compose -p altegio_bot exec -T postgres sh -lc 'psql -tAX -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT count(*) FROM records r JOIN record_services rs ON rs.record_id = r.id WHERE r.provider = '"'"'easyweek'"'"' AND r.total_cost IS DISTINCT FROM rs.cost_to_pay"'
```

Ожидается `0`.

**Очередь должна остаться пустой:**

```bash
docker compose -p altegio_bot exec -T postgres sh -lc 'psql -tAX -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT count(*) FROM message_jobs WHERE provider = '"'"'easyweek'"'"'"'
```

Ожидается `0` — `EASYWEEK_NOTIFICATIONS_ENABLED=false`.

Outbox / Meta send count по EasyWeek тоже `0`.

Проверить, что Altegio-путь не задет: новая реальная Altegio-доставка
обработалась штатно, её jobs создались как обычно.

Логи не должны содержать PII и секретов:

```bash
docker compose -p altegio_bot logs --since=1h altegio-easyweek-inbox-worker | grep -Eci 'customer_phone|customer_email|Authorization: Bearer|token='
```

Ожидается `0`.

### Шаг 9 — если что-то пошло не так

```text
EASYWEEK_PROCESSING_ENABLED=false
```

```bash
docker compose -p altegio_bot up -d --force-recreate altegio-easyweek-inbox-worker
```

Capture продолжает работать, накопление событий не прерывается. Уже
обработанные строки остаются `processed` — это нормально и повторно не
разбирается.

---

## 3. Что воркер делает и чего не делает

Делает:

- claim ровно одной `captured` строки (`FOR UPDATE SKIP LOCKED`);
- `captured → processing → доменные записи → processed` в **одной** транзакции;
- provider-scoped upsert `Client` (`provider='easyweek'`,
  `company_id = payload.location_id`, `altegio_client_id = customer_id`), после
  проверки пары `location_id + location_uuid` по реестру;
- UUID-first upsert `Record` по `uid`, с `altegio_record_id = numeric id`;
- отмена → `is_deleted = true`;
- manage-link строго из доказанной пары `booking_page` + `booking_hash_id`.

Не делает:

- не вызывает EasyWeek API (GET тоже нет — он не в критическом пути);
- не запускает promo / visit / review / reminders / campaigns / followups;
- не применяет Altegio-парсер дат, Europe/Belgrade и фильтр услуг;
- не трогает Altegio-строки, jobs и dedupe-ключи.

### 3a. Snapshot услуги и стоимости

`Record.total_cost` и `RecordService.cost_to_pay` — это **один и тот же**
booking-level снимок: payload присылает одну сумму на всю запись. Они обязаны
совпадать всегда, потому что PR-5 рендерит шаблон из service-строки, и
расхождение означало бы «0.00» в сообщении по записи с известной стоимостью.

**Откуда берётся сумма (исправлено в PR-7.3).** Ранее здесь и в normalizer
утверждалось, что authoritative значение — это `booking_price_int` в центах.
Production capture это опроверг: для реальной цены `120.00 €` EasyWeek
присылает `booking_price_int=120` (МАЖОРНЫЕ единицы), `booking_price="12000"`
(storage format, точные минорные единицы), `booking_price_float="120.00"` и
`booking_price_formatted="€120.00"`. Деление `booking_price_int` на 100 давало
`1.20` — цену в сто раз меньше реальной.

Действующий контракт:

- **authoritative** — `booking_price`, строка из одних цифр, точные минорные
  единицы; парсится целочисленной арифметикой, без `float`;
- **cross-check** — `booking_price_float`; если поле пришло, оно обязано
  описывать ту же сумму, иначе доставка отклоняется с `price_fields_conflict`;
- **никогда не парсятся** — `booking_price_formatted` (локализованный текст) и
  `booking_price_int`;
- присутствие цены определяется **только** ключом `booking_price`.

Отсюда правила при смене `service_id`:

- пришёл новый `service_id` **без** `booking_price` → старая `RecordService`
  удаляется, новая создаётся и **наследует уже доказанный `Record.total_cost`**;
- пришёл новый `service_id` **с** `booking_price` → обе величины получают
  новое значение;
- пришёл явный сброс цены (`booking_price: null`, и остальные price-поля этому
  не противоречат) → обе величины очищаются одновременно;
- если известной стоимости не было — новая строка остаётся с `NULL`;
- настоящая цена `0.00` — это цена, а не отсутствие цены.

`title` и `amount` при смене услуги **не переносятся** автоматически: другая
service identity означает другую услугу, и если доставка их не подтвердила, они
следуют обычной presence-семантике ниже.

Проверка инварианта (ожидается **0 строк**):

```bash
docker compose -p altegio_bot exec -T postgres sh -lc 'psql -tAX -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT r.id FROM records r JOIN record_services rs ON rs.record_id = r.id WHERE r.provider = '"'"'easyweek'"'"' AND r.total_cost IS DISTINCT FROM rs.cost_to_pay"'
```

### 3b. Authoritative clear: приоритет решается ПРИСУТСТВИЕМ поля

Для каждой группы полей выбор ветки определяется тем, **какой ключ пришёл**, а
не тем, «истинно» ли его значение:

| Группа | Приоритет | Правило |
|---|---|---|
| title | `services_description`, затем `service_name` | `services_description` — поле про ВЕСЬ набор услуг; если оно пришло, оно авторитетно **даже пустым** и fallback на singular `service_name` НЕ срабатывает |
| amount | `services_count`, затем `quantity` | то же: пришедший `services_count` авторитетен, включая `null` и `0` |

Следствия:

- `{"services_description": "", "service_name": "Manicure"}` → `title = NULL`
  (набор описан как пустой; подставлять имя одной услуги нельзя — для
  multi-service booking это было бы прямой ложью);
- `{"services_count": null, "quantity": 1}` → `amount = NULL`;
- `{"services_count": 0, "quantity": 1}` → `amount = 0`;
- singular-поле используется **только** когда whole-set поле отсутствует;
- оба поля отсутствуют → прежнее значение сохраняется.

**`service_id` в эту таблицу НЕ входит.** Это identity-поле: оно выбирает, к
какой строке `record_services` относится snapshot, а не является патчируемым
атрибутом. Explicit-clear семантики у него нет:

| `service_id` в доставке | Поведение |
|---|---|
| ключ отсутствует | известная service identity сохраняется |
| валидный положительный integer | обычная смена услуги |
| `null`, boolean, строка, `0`, отрицательное, дробное | **детерминированный отказ** |

Отказ — это `failed` + безопасный код (`invalid_payload` для «это вообще не
число», `invalid_numeric_range` для «число, но вне диапазона» — та же пара кодов,
что у `id`, `customer_id` и `location_id`) + `next_retry_at = NULL`. Ни `Client`,
ни `Record`, ни `RecordService`, ни `MessageJob` при этом не меняются.

Почему fail-closed, а не догадка: ни одна захваченная доставка не присылала
`service_id: null`, поэтому смысл значения не доказан. Молча сохранить старую
identity (прежнее поведение) означало бы привязать НОВЫЕ title/amount/цену к
СТАРОМУ `service_id`; удалить snapshot — уничтожить доказанные данные. Отказ
оставляет строки нетронутыми и делает payload видимым оператору.

---

## 4. Статусы и безопасные коды ошибок

| Статус | Смысл |
|---|---|
| `captured` | ещё не обработано (или обработка выключена) |
| `processed` | терминальный успех |
| `failed` | детерминированный отказ + `error_code` |

`error_code` — фиксированный безопасный идентификатор, никогда не
`str(exception)`: текст исключения драйвера содержит SQL-параметры, а с ними
телефон, e-mail и имя клиента.

```text
invalid_event_hint     invalid_payload        truncated_payload
missing_booking_uuid   invalid_booking_uuid   missing_booking_id
invalid_location_id    foreign_location       invalid_datetime
invalid_manage_link    identity_conflict      invalid_numeric_range
```

`invalid_numeric_range` — число синтаксически валидно, но не помещается в
целевую колонку (booking/customer/location/service id вне BIGINT/INTEGER,
отрицательный id, стоимость больше `Numeric(12,2)`). Отвергается один раз как
payload-ошибка, а не превращается в бесконечный retry на DataError.

`identity_conflict` — numeric booking id уже принадлежит Record с ДРУГИМ
`easyweek_booking_uuid` (или строке, чья принадлежность не доказана —
`easyweek_booking_uuid IS NULL`). Ни одна строка не меняется.

Неклассифицированные транзиентные ошибки терминального кода НЕ получают —
см. §4a.

`identity_conflict` — numeric booking id уже принадлежит Record с ДРУГИМ
`easyweek_booking_uuid`. Существующая строка не меняется; событие падает
fail-closed. Это не ошибка деплоя: это защита от захвата чужой записи.

### 4a. Транзиентные ошибки и per-event retry

**Транзиентная ошибка** (недоступность БД, сетевой сбой) НЕ помечает событие
`failed`: транзакция откатывается целиком, строка остаётся `captured`.

Важно: глобальный backoff воркера сам по себе НЕ решает head-of-line blocking —
claim всегда берёт старейшую готовую строку, поэтому «отравленное» событие
выбиралось бы снова и снова, лишь медленнее. Поэтому расписание **по событию**:

- `processing_attempts` увеличивается на 1;
- `next_retry_at` = now() + экспоненциальный backoff (5 с → 300 с максимум);
- claim берёт только строки, у которых `next_retry_at IS NULL OR next_retry_at <= now()`;
- остальной backlog **других booking** продолжает обрабатываться, пока строка ждёт;
- текст исключения нигде не сохраняется.

### 4b. Порядок событий внутри одной booking

Per-event retry сам по себе ломал бы причинный порядок: более ранний
`reschedule`, упавший транзиентно, уходил бы в ожидание, более новый
`reschedule` той же записи применялся бы, а затем первый ложился бы сверху и
откатывал время, услугу и связь с клиентом. `already_applied()` этого не ловит —
у доставок разные `payload_hash`.

Поэтому claim берёт старейшую готовую строку, **для которой не существует более
ранней нетерминальной строки с тем же `easyweek_events.booking_uuid`**.
Блокирующими считаются `captured` (включая ожидание `next_retry_at`) и
`processing`; `processed` и `failed` не блокируют.

**Ключ порядка — колонка `booking_uuid`, а НЕ сырой `payload->>'uid'`.** Одна и
та же UUID приходит текстом в разных формах: lowercase, uppercase, в фигурных
скобках, без дефисов, с пробелами по краям, с префиксом `urn:uuid:`.
Нормализатор сводит их все к одному `uuid.UUID`, а вот сырой текст остаётся
разным. Если ключом был бы текст, поздняя доставка не увидела бы раннюю (ещё
ретраящуюся) как предшественника, обогнала бы её, и восстановленная ранняя
легла бы сверху — откатив время, service snapshot, стоимость или связь с
клиентом. Возможна была бы и параллельная обработка двумя воркерами.

Колонка заполняется на capture тем же парсером, что использует нормализатор, а
для уже захваченного backlog — backfill'ом в миграции `d4e8a1c39f57`.

Строки с отсутствующим или синтаксически невалидным `uid` имеют
`booking_uuid IS NULL`: они **не блокируют никого и сами не блокируются**, и
доходят до своего детерминированного отказа (`missing_booking_uuid` /
`invalid_booking_uuid`). Сериализация действует **только внутри одной
booking** — другие UUID идут параллельно.

Найти booking, который держит очередь:

```bash
docker compose -p altegio_bot exec -T postgres sh -lc 'psql -tAX -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT booking_uuid, min(received_at) AS oldest, count(*) AS pending, max(processing_attempts) AS attempts, min(next_retry_at) AS next_try FROM easyweek_events WHERE status IN ('"'"'captured'"'"','"'"'processing'"'"') GROUP BY 1 ORDER BY oldest LIMIT 20"'
```

Строки без пригодного идентификатора (они не участвуют в сериализации):

```bash
docker compose -p altegio_bot exec -T postgres sh -lc 'psql -tAX -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT status, error_code, count(*) FROM easyweek_events WHERE booking_uuid IS NULL GROUP BY 1,2 ORDER BY 1,2"'
```

Много `pending` у одной booking при растущем `attempts` означает, что её первое
событие ждёт восстановления зависимости. Другие booking при этом обязаны
продолжать переходить в `processed`.

**Событие НЕ становится `failed` по числу попыток.** Неизвестное исключение —
это чаще всего кратковременная недоступность PostgreSQL, сетевой сбой или
несовпадение версий во время деплоя. Доказанного классификатора «это навсегда»
в проекте нет, поэтому событие остаётся `captured` и автоматически
обрабатывается, как только зависимость починена. Ограничена не живучесть
события, а нагрузка: задержка растёт до 300 с и дальше остаётся на этом уровне,
счётчик попыток насыщается.

Реальное расписание задержек (секунды): 5, 10, 20, 40, 80, 160, 300, 300, …

Оператор видит «застрявшее» событие по `processing_attempts` и `next_retry_at`.

**Терминальная строка никогда не ждёт.** Любой переход в `processed` или
`failed` — обычная обработка, `booking-succeeded`, точный replay, no-op по уже
отменённой booking, детерминированный отказ — проходит через общие хелперы
`mark_processed()` / `mark_failed()`, которые всегда снимают `next_retry_at`.
Без этого восстановившееся событие оставалось бы с будущим таймстампом ретрая и
выглядело бы для мониторинга «ожидающим», хотя работа уже завершена.

`processing_attempts` при этом **сохраняется** — это история восстановлений, а
не признак ожидания. Признак ожидания — только пара `status='captured'` +
`next_retry_at IS NOT NULL`.

Инварианты:

| Состояние | `processed_at` | `error_code` | `next_retry_at` |
|---|---|---|---|
| `processed` | NOT NULL | NULL | **NULL** |
| `failed` (детерминированный) | NOT NULL | безопасный код | **NULL** |
| `captured` после транзиентной ошибки | NULL | NULL | NOT NULL |
| `processing` | — | — | не виден после commit |

Проверка, что недопустимых «терминальных, но ждущих» строк нет:

```bash
docker compose -p altegio_bot exec -T postgres sh -lc 'psql -tAX -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT id, status, error_code, next_retry_at FROM easyweek_events WHERE status IN ('"'"'processed'"'"','"'"'failed'"'"') AND next_retry_at IS NOT NULL"'
```

Ожидается **0 строк**.

В логе — только `processing_error` и имя класса исключения; ни текста ошибки,
ни SQL-параметров, ни traceback.

Проверить, что backlog не заблокирован:

```bash
docker compose -p altegio_bot exec -T postgres sh -lc 'psql -tAX -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT status, error_code, processing_attempts, next_retry_at IS NOT NULL AS waiting, count(*) FROM easyweek_events GROUP BY 1,2,3,4 ORDER BY 1,2"'
```

Тревожный признак — строки `captured` с растущим `processing_attempts` при том,
что события **других** booking НЕ переходят в `processed`. Задержка событий той
же booking — это норма (см. §4b).

Восстановление после починки зависимости не требует ручных действий: событие
берётся автоматически по истечении `next_retry_at`. Если нужно ускорить, можно
обнулить задержку — это единственная безопасная ручная операция, она не трогает
payload и не меняет статус:

```bash
docker compose -p altegio_bot exec -T postgres sh -lc 'psql -tAX -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "UPDATE easyweek_events SET next_retry_at = NULL WHERE status = '"'"'captured'"'"' AND next_retry_at IS NOT NULL"'
```

Инварианты терминальных статусов:

| Статус | domain writes | `processed_at` | `error_code` |
|---|---|---|---|
| `processed` | применены | заполнен | NULL |
| `failed` | отсутствуют | заполнен | безопасный код |
| `captured` | отсутствуют | NULL | NULL |

`processing` после коммита не виден никогда — он существует только внутри
открытой транзакции.

---

## 5. Multi-bot: выбран вариант A (фан-аут)

Решение §3.4 канонического плана зафиксировано: **вариант A — фан-аут на нашей
стороне**. EasyWeek → 4 URL → altegio_bot как единственный приёмник, который
пишет сырое событие себе.

Варианты B и C не реализуются. EasyWeek GET API в критический путь воркера не
ставится.

**Открытый contract (follow-up до включения второго бота):** транспорт от
приёмника ко второму боту (push или pull) **не определён**. Конкретно не
определены: механизм доставки, формат сообщения, порядок и повторы, гарантия
доставки, разграничение доступа. Пока это не согласовано, второй бот не
включается. Изменения в Irida_Whisper в рамках PR-4 не вносятся.

---

## 6. Связанные документы

- `docs/easyweek/INTEGRATION_PLAN.md` — канонический план (§3.4, §4, PR-4).
- `docs/easyweek/capture_runbook.md` — PR-1, сырой capture.
- `docs/easyweek/pr3_production_dump_rehearsal.md` — репетиция схемы PR-3.
- `docs/ops/pr3_deploy.md` — модель production-деплоя и чтение Alembic revision.
