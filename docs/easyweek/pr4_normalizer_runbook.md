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
EASYWEEK_LOCATION_ID=0        # 0 = не сконфигурировано → воркер не claim'ит
EASYWEEK_INBOX_WORKER_POLL_SEC=1.0
```

При `EASYWEEK_PROCESSING_ENABLED=true` и `EASYWEEK_LOCATION_ID=0` воркер
fail-closed: он не берёт события, потому что не смог бы отличить свою локацию
от чужой.

> `docker compose restart` **не** перечитывает `env_file`. После правки любого
> флага нужен `up -d --force-recreate <сервис>`.

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

### Шаг 6 — задать numeric location id

В production `easyweek.env`:

```text
EASYWEEK_LOCATION_ID=305156
```

### Шаг 7 — в контролируемом окне включить обработку

```text
EASYWEEK_PROCESSING_ENABLED=true
```

```bash
docker compose -p altegio_bot up -d --force-recreate altegio-easyweek-inbox-worker
```

Пересоздаётся **только** EasyWeek-воркер. Altegio-сервисы не трогаются.

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

Услуга и стоимость должны быть сохранены (их использует PR-5):

```bash
docker compose -p altegio_bot exec -T postgres sh -lc 'psql -tAX -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT rs.service_id, rs.title, rs.cost_to_pay FROM record_services rs JOIN records r ON r.id = rs.record_id WHERE r.provider = '"'"'easyweek'"'"'"'
```

Ожидается ровно одна строка на booking с непустыми `title` и `cost_to_pay`.

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
  `company_id = EASYWEEK_LOCATION_ID`, `altegio_client_id = customer_id`);
- UUID-first upsert `Record` по `uid`, с `altegio_record_id = numeric id`;
- отмена → `is_deleted = true`;
- manage-link строго из доказанной пары `booking_page` + `booking_hash_id`.

Не делает:

- не вызывает EasyWeek API (GET тоже нет — он не в критическом пути);
- не запускает promo / visit / review / reminders / campaigns / followups;
- не применяет Altegio-парсер дат, Europe/Belgrade и фильтр услуг;
- не трогает Altegio-строки, jobs и dedupe-ключи.

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
invalid_manage_link    identity_conflict
```

`identity_conflict` — numeric booking id уже принадлежит Record с ДРУГИМ
`easyweek_booking_uuid`. Существующая строка не меняется; событие падает
fail-closed. Это не ошибка деплоя: это защита от захвата чужой записи.

**Транзиентная ошибка** (недоступность БД, сетевой сбой) НЕ помечает событие
`failed`: транзакция откатывается целиком, строка остаётся `captured` и будет
повторена. В логе появляется только `processing_error` и имя класса исключения —
ни текста ошибки, ни SQL-параметров, ни traceback (там были бы телефон, e-mail и
имя клиента). Повторы идут с экспоненциальным backoff до 30 с, поэтому одна
постоянно падающая строка не крутит цикл и не блокирует backlog навсегда.

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
