# PR-6 — активация локации Durlach (EasyWeek)

Порядок включения уведомлений для локации Durlach. Все шаги обратимы; откат —
в §8.

**Фаза 1 — только три немаркетинговых lifecycle-события:** `record_created`,
`record_updated`, `record_canceled`. Reminders (`reminder_24h` / `reminder_2h`)
в фазу 1 не входят: их job'ы не планируются, шаблоны для них не сидятся.

Первичная запись нового клиента получает отдельный шаблон
(`kitilash_du_record_created_new_client_v1`). Это **не отдельный тип джоба** —
job остаётся `record_created`, отличается только строка шаблона в БД и, значит,
`meta_template_name`.

---

## 0. Что должно быть заполнено до начала

Активация **не начинается**, пока не подтверждены два значения. Оба намеренно
fail-closed: без них система останавливается, а не работает «как получится».

| Что | Где | Пока не заполнено |
| --- | --- | --- |
| numeric `:location_id` Durlach | `DURLACH_LOCATION_ID` в `scripts/seed_easyweek_templates.py` | сид отказывается работать: `SeedConfigError` |
| approved host страницы записи | `EASYWEEK_BOOKING_PAGE_ALLOWED_HOSTS` в `easyweek.env` | любой booking URL отвергается, lifecycle-job падает локально |

`DURLACH_LOCATION_ID` — константа в скрипте, а не настройка, намеренно: скрипт
пишет контент именно Durlach (Meta-имена, адрес, карту). Если значение из
`EASYWEEK_LOCATION_ID` не совпадёт с константой, сид откажется — иначе контент
Durlach молча привязался бы к другой локации, и ничто ниже по потоку этого бы не
заметило.

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
* строки Durlach ещё нет — её создаст сид.

Сверьте, что `META_WA_PHONE_NUMBER_ID` в окружении равен этому общему
`phone_number_id`: сид запишет в строку Durlach именно его. Проверка не
автоматизирована в скрипте намеренно — сид выполняется одной транзакцией, а
читать чужие строки, чтобы решить, что писать в свою, значит завязать сид на
состояние, которое он не контролирует.

**Если у KA и RA `phone_number_id` РАЗНЫЕ** — это новая для проекта ситуация.
Общий номер для трёх филиалов тогда не подтверждён практикой, и до активации
нужно отдельно проверить, как маршрутизируются входящие. В этом случае
остановитесь: маршрутизация в PR-6 не менялась.

---

## 2. Meta-preflight: шаблоны должны быть APPROVED

Одобрение шаблонов — предпосылка активации, а не предположение. Проверьте
статусы read-only прогоном клонировщика (без `--apply` он ничего не отправляет):

```bash
docker compose -p altegio_bot run --rm altegio-api \
  /app/.venv/bin/python -m altegio_bot.scripts.clone_meta_templates_for_location \
  --target-location du --language de
```

В выводе должны быть `SKIP APPROVED target already exists` для **всех четырёх
реально используемых** шаблонов:

```text
kitilash_du_record_created_v1
kitilash_du_record_created_new_client_v1
kitilash_du_record_updated_v1
kitilash_du_record_canceled_v1
```

Любой другой статус — `PENDING`, `REJECTED`, `PAUSED`, `DISABLED`, `MISSING` —
**останавливает активацию**. `PENDING` тоже: Meta отвергнет отправку по
неодобренному шаблону, и job уйдёт в `failed`.

`kitilash_du_reminder_24h_v1` и `kitilash_du_reminder_2h_v1` в фазе 1 не
используются — их статус на активацию не влияет.

---

## 3. Chatwoot: обязательный шаг, не опциональный

`CHATWOOT_INBOX_COMPANY_MAP` сопоставляет Chatwoot inbox_id → company_id и
**обязателен, когда несколько company_id делят один `phone_number_id`** — это
ровно наш случай. Поведение при непустой карте: если inbox_id в ней не найден,
релей **fail-closed**.

```text
CHATWOOT_INBOX_COMPANY_MAP={"8": 758285, "7": 1271200, "<inbox_id Durlach>": <EASYWEEK_LOCATION_ID>}
```

Числа `8` и `7` — примеры из документации настройки; подставьте фактические
значения прода.

### Карту читает `altegio-whatsapp-inbox-worker`

Это отдельный сервис, и именно в нём живёт operator relay. После правки карты
пересоздайте **его**:

```bash
docker compose -p altegio_bot up -d --force-recreate altegio-whatsapp-inbox-worker
```

**Если пропустить этот шаг**, карта не вступит в силу: воркер продолжит работать
со старым значением, релей для бесед Durlach останется fail-closed, а всё
остальное будет выглядеть здоровым — события захватываются, job'ы создаются,
уведомления уходят. Симптом проявится только тогда, когда оператор попробует
ответить клиенту Durlach из Chatwoot.

### Smoke релея — ДО включения notifications

1. Напишите с тестового номера в inbox Durlach.
2. Ответьте оператором из Chatwoot.
3. Убедитесь, что ответ дошёл в WhatsApp.

Если ответ не доходит — не включайте notifications, сначала разберитесь с картой.

### `whatsapp_allowed_phone_number_ids` — правка НЕ нужна

Этот allowlist фильтрует **входящие вебхуки по `phone_number_id`** и о
company_id ничего не знает (`webhooks/whatsapp.py`,
`_parse_allowed_phone_number_ids`). Номер общий и уже разрешён. Если список
пуст, он неявно сводится к `META_WA_PHONE_NUMBER_ID` — тоже тот же номер.

---

## 4. Конфигурация в `easyweek.env`

```text
EASYWEEK_LOCATION_ID=<numeric :location_id Durlach>
EASYWEEK_BOOKING_PAGE_URL=<https-URL страницы записи Durlach>
EASYWEEK_BOOKING_PAGE_ALLOWED_HOSTS=<host из URL выше>
EASYWEEK_DEFAULT_LANGUAGE=de
```

`EASYWEEK_BOOKING_PAGE_URL` валидируется на send-time: абсолютный URL, только
`https`, обязательный hostname, без credentials/fragment/control-символов **и
host из allowlist**. Пустой allowlist отвергает всё — это защита от опечатки в
URL, которая иначе уехала бы клиенту как ссылка после отмены.

`EASYWEEK_NOTIFICATIONS_ENABLED` на этом шаге остаётся `false`.

---

## 5. Применение сида

Сид идемпотентен: повторный прогон не создаёт дублей и ничего не удаляет.
Шаблоны и отправитель сидятся одним скриптом и одной транзакцией — это один
атом активации: без шаблона job падает с `Template not found`, без отправителя —
с `No active sender`.

```bash
docker compose -p altegio_bot run --rm altegio-outbox-worker \
  /app/.venv/bin/python -m altegio_bot.scripts.seed_easyweek_templates
```

Сервис выбран не случайно: `altegio-outbox-worker` — один из трёх, кто читает
`easyweek.env`.

Скрипт fail-closed и ничего не запишет, если: `EASYWEEK_LOCATION_ID` не равен
`DURLACH_LOCATION_ID`, язык не `de`, или `META_WA_PHONE_NUMBER_ID` пуст.

---

## 6. Проверка строк в БД

```sql
SELECT company_id, code, language, meta_template_name, is_active
FROM message_templates
WHERE provider = 'easyweek'
ORDER BY code;
```

Ожидается ровно **четыре** строки, все `is_active = true`, язык `de`:

| code | meta_template_name |
| --- | --- |
| `record_canceled` | `kitilash_du_record_canceled_v1` |
| `record_created` | `kitilash_du_record_created_v1` |
| `record_created_new_client` | `kitilash_du_record_created_new_client_v1` |
| `record_updated` | `kitilash_du_record_updated_v1` |

```sql
SELECT provider, company_id, sender_code, phone_number_id, is_active
FROM whatsapp_senders
WHERE provider = 'easyweek';
```

Ожидается одна строка: `sender_code='default'`, `phone_number_id` — общий номер
бота, `is_active = true`.

То же самое глазами: **Ops → EasyWeek** (`/ops/easyweek`).

> Флаги в карточках наверху этой страницы — окружение **контейнера
> `altegio-api`**, а не воркеров. `altegio-api` при активации не пересоздаётся
> (это живой эндпоинт вебхуков, и рестарт ради строчки статуса — плохой размен),
> поэтому сразу после включения там ожидаемо будет `off`, пока воркеры уже
> отправляют. Достоверны счётчики ниже — они из БД.

---

## 7. Включение и smoke

**Порядок строгий.** Сначала preflight, карта, сид и проверка строк — только
потом флаг.

1. В `easyweek.env`: `EASYWEEK_NOTIFICATIONS_ENABLED=true`.
2. Пересоздать воркеры:

```bash
docker compose -p altegio_bot up -d --force-recreate altegio-easyweek-inbox-worker altegio-outbox-worker
```

Обычный `docker compose restart` **не перечитывает `env_file`**.

### Smoke

Создайте тестовую запись в EasyWeek на свой номер, затем измените её и отмените.

| Где | Что ожидать |
| --- | --- |
| `easyweek_events` | новая строка, `status` доходит до `processed` |
| `message_jobs` (`provider='easyweek'`) | job нужного `job_type`, `status` → `done` |
| `outbox_messages` | строка со `status='sent'`, `template_code` = job_type |
| WhatsApp | сообщение с адресом Durlach в футере |

Отдельно проверьте первичную запись: у клиента без прошлых записей в Durlach
сообщение должно содержать блок «Wichtige Hinweise», а `outbox_messages.meta`
— имя `kitilash_du_record_created_new_client_v1`. У повторного клиента — обычный
`kitilash_du_record_created_v1` без блока.

Ссылки:

* `record_created` / `record_updated` → `https://eyw.me/r/<hash>`, но только если
  пара `short_link` + `booking_hash_id` подтвердилась на send-time; иначе —
  статическая страница записи;
* `record_canceled` → **всегда** статическая страница записи.

```sql
SELECT o.id, o.template_code, o.status, o.language, o.meta ->> 'send_type' AS send_type
FROM outbox_messages o
         JOIN message_jobs j ON j.id = o.job_id
WHERE j.provider = 'easyweek'
ORDER BY o.id DESC
LIMIT 20;
```

Если job'ы встают в `failed`, смотрите `message_jobs.last_error` — сообщения
инвариантные и без PII.

---

## 8. Откат: два режима

Важно понимать, что именно гейтит флаг. `EASYWEEK_NOTIFICATIONS_ENABLED`
проверяется **только в планировщике** (`easyweek_inbox_worker`): он перестаёт
создавать новые `MessageJob`. `outbox_worker` этим флагом **не гейтится** — уже
созданные job'ы он доработает.

Поэтому режимов два, и выбор зависит от того, что не так.

### 8.1 Мягкий откат — «слишком много сообщений»

Подходит, когда содержание сообщений корректно, а проблема в объёме или в самом
факте рассылки.

```text
EASYWEEK_NOTIFICATIONS_ENABLED=false
```

```bash
docker compose -p altegio_bot up -d --force-recreate altegio-easyweek-inbox-worker
```

Новые job'ы не планируются; уже созданные будут отправлены. Это осознанный
выбор: незавершённая очередь дорабатывается, клиент не остаётся без
подтверждения записи, которую он только что сделал.

### 8.2 Жёсткая остановка — «эти сообщения нельзя отправлять»

Нужна, когда содержание неверно: не тот текст, не та ссылка, не тот филиал.
Здесь важен порядок — иначе гонка: пока вы отменяете job'ы, воркер их забирает.

**Цена, о которой надо знать заранее: `altegio-outbox-worker` общий. Его
остановка приостанавливает и Altegio-отправки — Карлсруэ и Растатт тоже.**
Это не побочный эффект, это условие корректности: без остановки воркера
нейтрализовать очередь без гонки нельзя.

1. Остановить outbox-воркер:

```bash
docker compose -p altegio_bot stop altegio-outbox-worker
```

2. Выключить планировщик, чтобы не появлялись новые job'ы:

```text
EASYWEEK_NOTIFICATIONS_ENABLED=false
```

```bash
docker compose -p altegio_bot up -d --force-recreate altegio-easyweek-inbox-worker
```

3. Нейтрализовать EasyWeek-джобы — обязательно покрывая и `queued`, и
   `processing` (воркер остановлен, поэтому `processing` больше никем не
   держится):

```sql
UPDATE message_jobs
SET status     = 'canceled',
    locked_at  = NULL,
    updated_at = now(),
    last_error = 'Canceled: activation rolled back'
WHERE provider = 'easyweek'
  AND status IN ('queued', 'processing');
```

4. Убедиться, что не осталось отправляемых:

```sql
SELECT status, COUNT(*) FROM message_jobs
WHERE provider = 'easyweek' GROUP BY status;
```

5. Поднять outbox обратно — Altegio возобновляется:

```bash
docker compose -p altegio_bot up -d altegio-outbox-worker
```

### Что НЕ ломается в обоих режимах

* захват вебхуков продолжается (`EASYWEEK_ENABLED` не трогаем) — события
  копятся в `easyweek_events` и не теряются;
* нормализация продолжается (`EASYWEEK_PROCESSING_ENABLED` не трогаем) —
  `Client` и `Record` обновляются как раньше;
* Altegio-путь не затронут (кроме паузы в §8.2 на время остановки outbox).

Сиды откатывать не нужно: строки шаблонов и отправителя без флага никем не
читаются.

---

## 9. Что НЕ входит в PR-6

* reminders (`reminder_24h`, `reminder_2h`) — фаза 2 / PR-7;
* маркетинг, кампании, promo для EasyWeek;
* гейт `EASYWEEK_NOTIFICATIONS_ENABLED` в `outbox_worker` — отдельное решение
  вне PR-6 (см. §8: именно поэтому жёсткая остановка требует ручных шагов);
* изменения маршрутизации отправителей и Altegio-пути.
