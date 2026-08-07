# PR-6 — активация локации Дурлах (EasyWeek)

Порядок включения уведомлений для локации Дурлах. Все шаги обратимы; откат —
в §7.

**Фаза 1 — только три немаркетинговых lifecycle-события:** `record_created`,
`record_updated`, `record_canceled`. Reminders (`reminder_24h` / `reminder_2h`)
в фазу 1 не входят: их job'ы не планируются, шаблоны для них не сидятся. Meta
одобрила 6 шаблонов Дурлаха — 2 напоминания и `record_created_new_client`
останутся неиспользованными до следующей фазы.

---

## 1. Предварительная проверка senders

Один `phone_number_id` обслуживает все три филиала (общий номер бота). Схема это
поддерживает: `pick_sender_id` ищет по `(provider, company_id, sender_code,
is_active)` и на номер не смотрит — каждый филиал владеет своей строкой, все
строки указывают на один номер.

Проверьте фактическое состояние прода **до** сида:

```sql
SELECT provider, company_id, sender_code, phone_number_id, is_active
FROM whatsapp_senders ORDER BY company_id;
```

Что ожидать:

* строки Карлсруэ (758285) и Растатта (1271200) с `provider='altegio'` и
  **одинаковым** `phone_number_id` — это штатная, проверенная боем конфигурация;
* строки Дурлаха ещё нет — её создаст сид.

**Если у KA и RA `phone_number_id` РАЗНЫЕ** — это новая для проекта ситуация.
Общий номер для трёх филиалов тогда не подтверждён практикой, и до активации
нужно отдельно проверить, как маршрутизируются входящие. В этом случае
остановитесь и не продолжайте: маршрутизация в PR-6 не менялась.

---

## 2. Chatwoot: обязательный шаг, не опциональный

`CHATWOOT_INBOX_COMPANY_MAP` сопоставляет Chatwoot inbox_id → company_id и
**обязателен, когда несколько company_id делят один `phone_number_id`** — это
ровно наш случай. Поведение при непустой карте: если inbox_id в ней не найден,
релей **fail-closed**.

Значит: пока Дурлах не добавлен в карту, операторский релей для его диалогов не
работает. Добавьте inbox Дурлаха ДО включения уведомлений:

```text
CHATWOOT_INBOX_COMPANY_MAP={"8": 758285, "7": 1271200, "<inbox_id Дурлаха>": <EASYWEEK_LOCATION_ID>}
```

Числа `8` и `7` — примеры из документации настройки; подставьте фактические
значения прода. `<inbox_id Дурлаха>` берётся из Chatwoot после создания inbox.

Чужие inbox, агентов и правила автоматизации не трогаем.

### `whatsapp_allowed_phone_number_ids` — правка НЕ нужна

Этот allowlist фильтрует **входящие вебхуки по `phone_number_id`** и о
company_id ничего не знает (`webhooks/whatsapp.py`,
`_parse_allowed_phone_number_ids`). Номер общий и уже разрешён, поэтому Дурлах
проходит без изменений. Если список пуст, он неявно сводится к
`META_WA_PHONE_NUMBER_ID` — тоже тот же номер.

---

## 3. Конфигурация в `easyweek.env`

Перед сидом должны быть заполнены:

```text
EASYWEEK_LOCATION_ID=<numeric :location_id Дурлаха>
EASYWEEK_BOOKING_PAGE_URL=<https-URL страницы записи Дурлаха>
EASYWEEK_DEFAULT_LANGUAGE=de
```

`EASYWEEK_BOOKING_PAGE_URL` валидируется на send-time: абсолютный URL, только
`https`, обязательный hostname, без credentials/fragment/control-символов.
Невалидное значение роняет lifecycle-job локально, до вызова Meta.

`EASYWEEK_NOTIFICATIONS_ENABLED` на этом шаге остаётся `false`.

---

## 4. Применение сидов

Сид идемпотентен: повторный прогон не создаёт дублей и ничего не удаляет.
Шаблоны и отправитель сидятся одним скриптом и одной транзакцией — это один
атом активации: без шаблона job падает с `Template not found`, без отправителя —
с `No active sender`.

```bash
docker compose -p altegio_bot run --rm altegio-outbox-worker \
  /app/.venv/bin/python -m altegio_bot.scripts.seed_easyweek_templates
```

Сервис выбран не случайно: `altegio-outbox-worker` — один из трёх, кто читает
`easyweek.env`, поэтому `EASYWEEK_LOCATION_ID` и язык будут видны скрипту.

Скрипт fail-closed: при `EASYWEEK_LOCATION_ID=0`, пустом языке или пустом
`META_WA_PHONE_NUMBER_ID` он откажется работать и ничего не запишет.

---

## 5. Проверка строк в БД

```sql
SELECT company_id, code, language, meta_template_name, is_active
FROM message_templates
WHERE provider = 'easyweek'
ORDER BY code;
```

Ожидается ровно три строки, все `is_active = true`, язык `de`,
`meta_template_name` — `kitilash_du_record_created_v1`,
`kitilash_du_record_updated_v1`, `kitilash_du_record_canceled_v1`.

```sql
SELECT provider, company_id, sender_code, phone_number_id, is_active
FROM whatsapp_senders
WHERE provider = 'easyweek';
```

Ожидается одна строка: `sender_code='default'`, `phone_number_id` — общий номер
бота, `is_active = true`.

То же самое глазами: **Ops → EasyWeek** (`/ops/easyweek`) — там же счётчики
`easyweek_events`, `message_jobs` и `outbox_messages` по статусам.

---

## 6. Включение и smoke

**Порядок строгий.** Сначала сиды и проверка строк, только потом флаг.

1. В `easyweek.env`: `EASYWEEK_NOTIFICATIONS_ENABLED=true`.
2. Пересоздать сервисы, которые читают этот флаг и шаблоны:

```bash
docker compose -p altegio_bot up -d --force-recreate altegio-easyweek-inbox-worker altegio-outbox-worker
```

Обычный `docker compose restart` **не перечитывает `env_file`** — контейнер
останется со старым значением, и это молчаливый режим отказа.

### Smoke

Создайте тестовую запись в EasyWeek на свой номер, затем измените её и отмените.
Для каждого события ожидается:

| Где | Что ожидать |
| --- | --- |
| `easyweek_events` | новая строка, `status` доходит до `processed` |
| `message_jobs` (`provider='easyweek'`) | job нужного `job_type`, `status` → `done` |
| `outbox_messages` | строка со `status='sent'`, `template_code` = job_type |
| WhatsApp | сообщение с адресом Дурлаха в футере |

Проверка ссылок — главное, что отличает Дурлах:

* `record_created` / `record_updated` → ссылка вида `https://eyw.me/r/<hash>`
  (управление записью), но только если пара `short_link` + `booking_hash_id`
  подтвердилась на send-time; иначе — статическая страница записи;
* `record_canceled` → **всегда** статическая страница записи, никогда не ссылка
  на управление отменённой записью.

Быстрый разбор одной отправки:

```sql
SELECT o.id, o.template_code, o.status, o.language, o.meta ->> 'send_type' AS send_type
FROM outbox_messages o
         JOIN message_jobs j ON j.id = o.job_id
WHERE j.provider = 'easyweek'
ORDER BY o.id DESC
LIMIT 20;
```

Если job'ы встают в `failed`, смотрите `message_jobs.last_error` — сообщения
инвариантные и без PII (`Template not found: provider=easyweek …`,
`No active sender for provider=easyweek …`, `EasyWeek service snapshot …`).

---

## 7. Откат

```text
EASYWEEK_NOTIFICATIONS_ENABLED=false
```

```bash
docker compose -p altegio_bot up -d --force-recreate altegio-easyweek-inbox-worker altegio-outbox-worker
```

**Что при этом НЕ ломается:**

* захват вебхуков продолжается (`EASYWEEK_ENABLED` не трогаем) — события
  копятся в `easyweek_events` и не теряются;
* нормализация продолжается (`EASYWEEK_PROCESSING_ENABLED` не трогаем) —
  `Client` и `Record` обновляются как раньше;
* перестаёт происходить только одно: планировщик больше не создаёт
  `MessageJob`, то есть уведомления не отправляются.

Уже созданные до отката job'ы останутся в очереди и будут отправлены. Если это
нежелательно, отмените их вручную:

```sql
UPDATE message_jobs
SET status = 'canceled', locked_at = NULL, last_error = 'Canceled: activation rolled back'
WHERE provider = 'easyweek' AND status = 'queued';
```

Сиды откатывать не нужно: строки шаблонов и отправителя без флага никем не
читаются и никому не мешают.

---

## 8. Что НЕ входит в PR-6

* reminders (`reminder_24h`, `reminder_2h`) — фаза 2 / PR-7;
* `record_created_new_client` — ветка `is_new_client` в `_render_message` стоит
  под `if not is_easyweek`, EasyWeek её не выбирает;
* маркетинг, кампании, promo для EasyWeek;
* изменения маршрутизации отправителей и Altegio-пути.
