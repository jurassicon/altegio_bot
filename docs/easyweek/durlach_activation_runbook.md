# PR-6/PR-7 — активация локаций EasyWeek

Порядок включения уведомлений для **всех** филиалов реестра. Сейчас их два —
Durlach и Rastatt — и оба активируются одним проходом: `EASYWEEK_NOTIFICATIONS_ENABLED`
глобален, отдельного флага на филиал нет и не планируется (§7.0). Все шаги
обратимы; откат — в §8.

**Фаза 1 — только три немаркетинговых lifecycle-события:** `record_created`,
`record_updated`, `record_canceled`. Reminders (`reminder_24h` / `reminder_2h`)
в фазу 1 не входят: их job'ы не планируются, шаблоны для них не сидятся.

Первичная запись нового клиента получает отдельный шаблон
(`kitilash_<xx>_record_created_new_client_v1`, где `<xx>` — Meta-префикс
филиала). Это **не отдельный тип джоба** — job остаётся `record_created`,
отличается только строка шаблона в БД и, значит, `meta_template_name`.

## Профили филиалов

Идентичность филиала неделима и живёт в исходниках
(`src/altegio_bot/easyweek_branches.py`). Профиль связывает четыре вещи, и
проверяются они вместе, а не по отдельности:

| Профиль (slug) | API-имя в `GET /locations` | Meta-префикс | Контент футера |
| --- | --- | --- | --- |
| `durlach` | `KitiLash Durlach` | `du` | адрес и карта Durlach |
| `rastatt` | `KitiLash Rastatt` | `ra` | адрес и карта Rastatt |

Numeric `location_id` и `location_uuid` в исходниках **нет** — §10 канонического
плана показал, что они не стабильны, поэтому они живут только в
`EASYWEEK_LOCATION_MAP`. Slug — верхнеуровневый ключ реестра — выбирает профиль;
всё остальное, что вводит оператор, проверяется *против* профиля.

Именно это делает путаницу §10 невозможной: раньше контент выбирался по
`meta_template_prefix`, который оператор вписывает руками, а API-имя лишь
печаталось на экран. Теперь префикс обязан совпасть с профилем slug'а, а
API-имя по UUID — с ожидаемым именем профиля, иначе сид падает до первой записи.

Филиал без профиля в исходниках **не сидится и не отправляет**. Добавление
третьего филиала — это его одобренные branch metadata плюс тесты, без правки
архитектуры.

---

## 0. Что должно быть заполнено до начала

Активация **не начинается**, пока не подтверждены реестр и host-allowlist. Оба
намеренно fail-closed: без них система останавливается.

| Что | Где | Пока не заполнено |
| --- | --- | --- |
| пары numeric `location_id` + `location_uuid` **каждого** филиала | `EASYWEEK_LOCATION_MAP` в `easyweek.env`; сид независимо сверяет UUID через live `GET /locations` | worker не claim'ит; сид отказывается: `SeedConfigError` |
| slug каждой записи совпадает с одобренным профилем | верхнеуровневый ключ `EASYWEEK_LOCATION_MAP` | `SeedConfigError`: `no source-controlled profile`; отправка запрещена |
| `meta_template_prefix` совпадает с профилем slug'а | `EASYWEEK_LOCATION_MAP` | `SeedConfigError` про `meta_template_prefix` |
| approved host страницы записи | `EASYWEEK_BOOKING_PAGE_ALLOWED_HOSTS` в `easyweek.env` | любой booking URL отвергается, lifecycle-job падает локально |

### Проверка пар до записи чего-либо

Для КАЖДОГО филиала подтвердите пару из двух независимых источников:

1. **Webhook capture** — `location_id` и `location_uuid` приходят в одном
   payload, поэтому их можно сверить между собой:

```bash
docker compose -p altegio_bot exec -T postgres sh -lc 'psql -tAX -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT DISTINCT payload->>'"'"'location_id'"'"' AS location_id, payload->>'"'"'location_uuid'"'"' AS location_uuid FROM easyweek_events WHERE payload ? '"'"'location_uuid'"'"' ORDER BY 1"'
```

2. **Read-only `GET /locations`** — независимый источник, где UUID стоит рядом
   с человекочитаемым именем филиала:

```bash
docker compose -p altegio_bot exec -T altegio-api /app/.venv/bin/python -m altegio_bot.scripts.easyweek_probe --redact-pii
```

Сверьте по таблице профилей выше: slug → API-имя → Meta-префикс → страница
записи → ожидаемый адрес в футере. Расхождение любой из четырёх величин
означает, что реестр собран неверно — правьте `easyweek.env`, не сид.

Location id в репозитории **не хранится** — он живёт только в production
`easyweek.env`, и это закреплено тестом
`test_the_production_location_id_is_not_hardcoded_in_python`.

Перед записью сид вызывает live `GET /locations`: каждый UUID реестра обязан
найтись, а API-имя филиала печатается оператору. Это независимый источник
identity; недоступный API, отсутствующий UUID или неизвестный seed-префикс
останавливает сид до первой записи.

Сообщения об отказе называют нарушенный инвариант и **не печатают сам id** — они
попадают в логи.

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
* EasyWeek-строк Durlach и Rastatt может ещё не быть — сид создаст или
  идемпотентно исправит обе provider-scoped строки.

Сверьте, что `META_WA_PHONE_NUMBER_ID` в окружении равен этому общему
`phone_number_id`: сид запишет его в EasyWeek-строки обоих филиалов. Проверка не
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
статусы read-only прогонами клонировщика (без `--apply` он ничего не отправляет)
для **обоих** production-префиксов:

```bash
docker compose -p altegio_bot run --rm altegio-api \
  /app/.venv/bin/python -m altegio_bot.scripts.clone_meta_templates_for_location \
  --target-location du --language de

docker compose -p altegio_bot run --rm altegio-api \
  /app/.venv/bin/python -m altegio_bot.scripts.clone_meta_templates_for_location \
  --target-location ra --language de \
  --address '76437 Rastatt, Rathausstraße 5' \
  --maps-url 'https://maps.app.goo.gl/xvYYbJbPaWcnp9Xv5'
```

В выводе должны быть `SKIP APPROVED target already exists` для **всех четырёх
реально используемых** шаблонов:

```text
kitilash_du_record_created_v1
kitilash_du_record_created_new_client_v1
kitilash_du_record_updated_v1
kitilash_du_record_canceled_v1
```

И такой же набор с префиксом `kitilash_ra_` для Rastatt. Любой branch-specific
шаблон, отсутствующий хотя бы у одного из двух филиалов, блокирует общий rollout:
флаг notifications глобален и частично включить только готовый филиал нельзя.

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
CHATWOOT_INBOX_COMPANY_MAP={"8": 758285, "7": 1271200, "<inbox_id EasyWeek>": <location_id из EASYWEEK_LOCATION_MAP>}
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
EASYWEEK_ENABLED=true
EASYWEEK_PROCESSING_ENABLED=false
EASYWEEK_NOTIFICATIONS_ENABLED=false
EASYWEEK_LOCATION_MAP=<JSON-реестр с id, uuid, Meta-префиксом и booking_page_url каждого филиала>
EASYWEEK_BOOKING_PAGE_ALLOWED_HOSTS=<host из URL выше>
EASYWEEK_DEFAULT_LANGUAGE=de
```

Каждый `booking_page_url` из реестра валидируется на send-time: абсолютный URL, только
`https`, обязательный hostname, без credentials/fragment/control-символов **и
host из allowlist**. Пустой allowlist отвергает всё — это защита от опечатки в
URL, которая иначе уехала бы клиенту как ссылка после отмены.

Capture уже включён и остаётся включённым: `EASYWEEK_ENABLED=true` не меняйте и
`altegio-api` не пересоздавайте. На время смены tenant boundary оба downstream
флага обязаны быть `false`: worker не должен ни разбирать backlog по
недопроверенной карте, ни создавать клиентские job'ы.

После записи нового реестра пересоздайте **оба** его потребителя. Обычный
`restart` и `up -d` без `--force-recreate` не перечитывают `env_file`:

```bash
docker compose -p altegio_bot up -d --force-recreate \
  altegio-easyweek-inbox-worker altegio-outbox-worker
```

Проверьте эффективную конфигурацию внутри обоих контейнеров, не печатая raw
JSON, UUID, URLs или секреты:

```bash
for EW_SERVICE in altegio-easyweek-inbox-worker altegio-outbox-worker; do
  echo "SERVICE=$EW_SERVICE"
  docker compose -p altegio_bot exec -T "$EW_SERVICE" /app/.venv/bin/python - <<'PY'
from altegio_bot.easyweek_locations import configured_easyweek_locations
from altegio_bot.settings import settings

registry = configured_easyweek_locations()
print(
    {
        "processing_enabled": settings.easyweek_processing_enabled,
        "notifications_enabled": settings.easyweek_notifications_enabled,
        "registry_configured": registry.configured,
        "registry_valid": registry.valid,
        "branches": sorted(
            (location.name, location.company_id, location.meta_template_prefix)
            for location in registry.locations.values()
        ),
        "booking_hosts_configured": bool(settings.easyweek_booking_page_allowed_hosts),
    }
)
PY
done
```

Gate: оба контейнера показывают один и тот же полный список `durlach`/`du` и
`rastatt`/`ra`, registry configured+valid, processing=false и
notifications=false. Любое расхождение останавливает rollout до сида.

---

## 5. Применение сида

Сид идемпотентен: повторный прогон не создаёт дублей и ничего не удаляет.
Шаблоны и отправитель сидятся одним скриптом и одной транзакцией — это один
атом активации: без шаблона job падает с `Template not found`, без отправителя —
с `No active sender`.

### Как подтверждается identity

Берите numeric `:location_id` из источника, не зависящего от конфигурации
контейнера, в порядке предпочтения:

1. **Захваченный вебхук.** Это число прислала сама EasyWeek, и в нашу БД оно
   попало помимо `easyweek.env`:

```sql
SELECT DISTINCT payload ->> 'location_id' AS location_id, COUNT(*) AS events
FROM easyweek_events
GROUP BY 1
ORDER BY events DESC;
```

   Ожидаются все филиалы реестра. Для каждой строки свяжите pair с Durlach или
   Rastatt через независимый `GET /locations` и таблицу профилей; неизвестная
   или отсутствующая пара останавливает rollout.

2. **Кабинет EasyWeek** — id локации в интерфейсе.

3. **Операционное подтверждение владельца локации.**

Read-only проба помогает собрать реестр, а сам сид повторяет `GET /locations`
непосредственно перед записью и печатает найденные имена. CLI-аргумента с id
больше нет:

```bash
docker compose -p altegio_bot run --rm altegio-outbox-worker \
  /app/.venv/bin/python -m altegio_bot.scripts.seed_easyweek_templates
```

Сервис выбран не случайно: `altegio-outbox-worker` — один из трёх, кто читает
`easyweek.env`; сид прочитает весь реестр и проверит его через API.

**Расхождение — это стоп, а не повод «подправить».** Оно означает одно из двух:
контейнер сконфигурирован не на ту локацию, либо оператор подтвердил не ту. Сид
не может отличить один случай от другого и обязан отказать — иначе контент
одного филиала привяжется к чужой локации. Выясните, какая сторона неверна, исправьте
её и запустите сид заново.

Скрипт fail-closed и ничего не запишет, если реестр пуст/невалиден, API
недоступен, UUID не найден, язык не `de`, booking page не проходит allowlist
или `META_WA_PHONE_NUMBER_ID` пуст. Все проверки выполняются до первой записи.

---

## 6. Проверка строк в БД

```sql
SELECT company_id, code, language, meta_template_name, is_active
FROM message_templates
WHERE provider = 'easyweek'
ORDER BY code;
```

Ожидается ровно **четыре** строки **на каждый** сконфигурированный
`company_id`, все `is_active = true`, язык `de`. Для двух филиалов — восемь
строк:

| code | Durlach | Rastatt |
| --- | --- | --- |
| `record_canceled` | `kitilash_du_record_canceled_v1` | `kitilash_ra_record_canceled_v1` |
| `record_created` | `kitilash_du_record_created_v1` | `kitilash_ra_record_created_v1` |
| `record_created_new_client` | `kitilash_du_record_created_new_client_v1` | `kitilash_ra_record_created_new_client_v1` |
| `record_updated` | `kitilash_du_record_updated_v1` | `kitilash_ra_record_updated_v1` |

Машинная проверка «четыре на филиал и никаких чужих префиксов»:

```sql
SELECT company_id,
       count(*)                                               AS templates,
       count(*) FILTER (WHERE is_active)                      AS active,
       count(DISTINCT split_part(meta_template_name, '_', 2)) AS distinct_prefixes,
       min(split_part(meta_template_name, '_', 2))            AS prefix
FROM message_templates
WHERE provider = 'easyweek'
GROUP BY company_id
ORDER BY company_id;
```

Ожидается по строке на филиал: `templates = 4`, `active = 4`,
`distinct_prefixes = 1`, а `prefix` совпадает с профилем этого `company_id`.

**STOP:** любое другое значение (`templates != 4`, `active != 4`,
`distinct_prefixes != 1` или чужой `prefix`) блокирует rollout. Повторный запуск
сида не считается исправлением: при дубликатах он обновляет только строку с
минимальным `id`, а лишние строки намеренно не удаляет. Ничего не отправляйте и
не включайте notifications, пока оператор вручную не установит причину и не
согласует восстановление данных.

Read-only проверка дубликатов по фактическому ключу lookup:

```sql
SELECT candidate.id,
       candidate.company_id,
       candidate.code,
       candidate.language,
       candidate.meta_template_name AS name,
       candidate.is_active          AS active
FROM message_templates AS candidate
WHERE provider = 'easyweek'
  AND EXISTS (
      SELECT 1
      FROM message_templates AS duplicate
      WHERE duplicate.provider = candidate.provider
        AND duplicate.company_id = candidate.company_id
        AND duplicate.code = candidate.code
        AND duplicate.language = candidate.language
        AND duplicate.id <> candidate.id
  )
ORDER BY company_id, code, language, id;
```

Ожидается **0 строк**. Любая строка — **STOP** и ручной разбор. Не выполняйте
`DELETE`, не пытайтесь «починить» данные повторным сидом и не выбирайте строку
для удаления на глаз: в PR-7 нет ни unique constraint, ни автоматической
destructive cleanup. Исправление дубликатов — отдельная контролируемая
операторская процедура после проверки содержимого и истории строк.

Футер не должен пересекаться между филиалами:

```sql
SELECT company_id,
       count(*) FILTER (WHERE body LIKE '%Durlach%') AS mentions_durlach,
       count(*) FILTER (WHERE body LIKE '%Rastatt%') AS mentions_rastatt
FROM message_templates
WHERE provider = 'easyweek'
GROUP BY company_id
ORDER BY company_id;
```

У Durlach ожидается `mentions_rastatt = 0`, у Rastatt — `mentions_durlach = 0`.
Любое cross-branch упоминание — **STOP**; повторный сид не доказывает, что
дубликат, который worker выбирает по минимальному `id`, устранён.

```sql
SELECT provider, company_id, sender_code, phone_number_id, is_active
FROM whatsapp_senders
WHERE provider = 'easyweek';
```

Ожидается **ровно одна активная** строка на филиал (для двух филиалов — две):
`sender_code='default'`, `phone_number_id` — общий номер бота, `is_active = true`.

```sql
SELECT company_id, count(*) FILTER (WHERE is_active) AS active_senders
FROM whatsapp_senders
WHERE provider = 'easyweek'
GROUP BY company_id
ORDER BY company_id;
```

`active_senders` должен быть `1` для каждого `company_id` реестра.

Страница записи каждого филиала берётся из его записи реестра — сверьте, что
`booking_page_url` Durlach ведёт на страницу Durlach, а Rastatt — на свою.

То же самое глазами: **Ops → EasyWeek** (`/ops/easyweek`).

> Флаги в карточках наверху этой страницы — окружение **контейнера
> `altegio-api`**, а не воркеров. `altegio-api` при активации не пересоздаётся
> (это живой эндпоинт вебхуков, и рестарт ради строчки статуса — плохой размен),
> поэтому сразу после включения там ожидаемо будет `off`, пока воркеры уже
> отправляют. Достоверны счётчики ниже — они из БД.

---

## 7. Включение и smoke

### 7.0 Обязательный gate: Altegio-путь Раштата должен быть выключен

`EASYWEEK_NOTIFICATIONS_ENABLED` **глобален**: он включает создание job для
ВСЕХ филиалов реестра сразу. Отдельного флага на филиал нет — канонический план
требует операторского cutover, а не расширения архитектуры.

Раштат (`1271200`) мигрирует с Altegio на EasyWeek. Пока Altegio-путь этого
филиала жив, включение EasyWeek-уведомлений даст клиенту **два сообщения об
одной записи** — по одному из каждой системы.

Поэтому перед установкой `true`:

1. Получите независимое подтверждение, что Altegio notification path для
   company `1271200` выключен на дату миграции.
2. Зафиксируйте это подтверждение письменно (кто, когда, что именно выключено).
3. Убедитесь, что в Altegio для `1271200` больше не создаются новые
   lifecycle-job:

```sql
SELECT count(*) AS queued_altegio_rastatt
FROM message_jobs
WHERE provider = 'altegio'
  AND company_id = 1271200
  AND job_type IN ('record_created', 'record_updated', 'record_canceled')
  AND status IN ('queued', 'processing')
  AND created_at > now() - interval '1 hour';
```

**Если cutover ещё не подтверждён — `EASYWEEK_NOTIFICATIONS_ENABLED` остаётся
`false`.** Durlach в этом случае тоже ждёт: одного флага на всех достаточно,
чтобы частичное включение было невозможно. Это осознанный размен — лучше
задержать Durlach, чем удвоить уведомления клиентам Раштата.

### 7.1 Включение

**Порядок строгий.** Сначала preflight, карта, сид, проверка строк и gate 7.0 —
только потом флаг.

1. Сначала разрешите нормализацию, оставив отправки выключенными:

```text
EASYWEEK_PROCESSING_ENABLED=true
EASYWEEK_NOTIFICATIONS_ENABLED=false
```

2. Пересоздайте inbox-worker и убедитесь, что captured backlog обрабатывается
по новой карте, а EasyWeek `MessageJob` ещё не создаются:

```bash
docker compose -p altegio_bot up -d --force-recreate altegio-easyweek-inbox-worker
```

3. Только после успешной нормализации и обязательного gate §7.0 установите:

```text
EASYWEEK_NOTIFICATIONS_ENABLED=true
```

4. Снова пересоздайте inbox-worker. Outbox уже получил новую карту на §4; его
лишний рестарт здесь только приостановил бы общий Altegio-трафик:

```bash
docker compose -p altegio_bot up -d --force-recreate altegio-easyweek-inbox-worker
```

Обычный `docker compose restart` **не перечитывает `env_file`**.

### Smoke — матрица PR-7

Smoke гоняется **одновременно по обоим филиалам**. Прогон только Durlach не
закрывает DoD PR-7: именно cross-branch путаница была исходным дефектом.

Для КАЖДОГО филиала (Durlach и Rastatt) создайте тестовую запись на свой номер,
затем измените её, перенесите и отмените:

| Сценарий | Durlach | Rastatt |
| --- | --- | --- |
| create | `du_*`, футер Durlach | `ra_*`, футер Rastatt |
| update | `du_*` | `ra_*` |
| reschedule | `du_*` (job `record_updated`) | `ra_*` |
| cancel | `du_*`, статическая страница Durlach | `ra_*`, статическая страница Rastatt |
| новый клиент | `kitilash_du_record_created_new_client_v1` + блок «Wichtige Hinweise» | `kitilash_ra_record_created_new_client_v1` + блок |
| повторный клиент | `kitilash_du_record_created_v1`, без блока | `kitilash_ra_record_created_v1`, без блока |

Общие ожидания по каждому событию:

| Где | Что ожидать |
| --- | --- |
| `easyweek_events` | новая строка, `status` доходит до `processed` |
| `message_jobs` (`provider='easyweek'`) | job нужного `job_type` и `company_id` своего филиала, `status` → `done` |
| `outbox_messages` | строка со `status='sent'`, `template_code` = job_type |
| WhatsApp | сообщение с адресом СВОЕГО филиала в футере |

**Cross-branch проверки — обязательны:**

* Durlach не отправил ни одного `ra_*`, Rastatt — ни одного `du_*`;
* в сообщении Durlach нет адреса/карты Rastatt и наоборот;
* ссылка ведёт на страницу своего филиала;
* `record_canceled` → статическая страница СВОЕГО филиала, не другого.

```sql
SELECT j.company_id,
       COALESCE(o.meta ->> 'template', o.meta ->> 'original_template') AS meta_template,
       count(*)
FROM outbox_messages o
         JOIN message_jobs j ON j.id = o.job_id
WHERE j.provider = 'easyweek'
GROUP BY 1, 2
ORDER BY 1, 2;
```

Каждая строка обязана нести префикс своего `company_id`. Любая пара
«company Durlach + `ra_*`» или «company Rastatt + `du_*`» — это стоп.

Логи обоих воркеров не должны содержать PII и секретов:

```bash
docker compose -p altegio_bot logs --since=1h altegio-outbox-worker altegio-easyweek-inbox-worker | grep -Eci 'customer_phone|customer_email|Authorization: Bearer|token='
```

Ожидается `0`.

Ссылки:

* `record_created` / `record_updated` → `https://eyw.me/r/<hash>`, но только если
  пара `short_link` + `booking_hash_id` подтвердилась на send-time; иначе —
  статическая страница записи;
* `record_canceled` → **всегда** статическая страница записи.

```sql
SELECT o.id,
       o.template_code,
       o.status,
       o.language,
       o.meta ->> 'send_type'                                            AS send_type,
       COALESCE(o.meta ->> 'template', o.meta ->> 'original_template')   AS meta_template
FROM outbox_messages o
         JOIN message_jobs j ON j.id = o.job_id
WHERE j.provider = 'easyweek'
ORDER BY o.id DESC
LIMIT 20;
```

`meta_template` берётся из двух ключей не случайно: при шаблонной отправке имя
лежит в `meta->>'template'`, а при успешной текстовой внутри 24-часового окна —
в `meta->>'original_template'`. Без `COALESCE` первичная запись, ушедшая текстом,
показала бы пустое имя, и проверку new-client шаблона сделать было бы нельзя.

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

**Очередь имеет ДВА источника, и флаг закрывает только один.**
`EASYWEEK_NOTIFICATIONS_ENABLED=false` останавливает планировщик
(`easyweek_inbox_worker`) — новые lifecycle-джобы не создаются. Но
delivery-retry рождается не там: его создаёт `_handle_failed_delivery_status` в
`whatsapp_inbox_worker`, то есть обработчик Meta status-callbacks. Этот воркер
`EASYWEEK_NOTIFICATIONS_ENABLED` **не читает вообще** (единственные читатели
флага — `easyweek_inbox_worker` и ops-роутер).

Отсюда сценарий, который шаги 1–4 не закрывают: сообщение ушло до отката →
вы остановили outbox, выключили флаг и почистили очередь → приходит запоздавший
`failed`-callback по уже отправленному сообщению → создаётся новый джоб с
`provider='easyweek'` (он наследуется из доказанной identity) и
`status='queued'` → вы поднимаете общий outbox → повторная отправка уходит
клиенту. `DELIVERY_RETRY_JOB_TYPES` содержит `record_created`,
`record_updated`, `record_canceled` — ровно наши типы фазы 1.

Повторный `UPDATE` прямо перед стартом outbox **не помогает**: callback может
прийти уже после запуска. Поэтому шаг 5 закрывает производителя, а не только
потребителя.

**Это best-effort остановка для ещё НЕ НАЧАТЫХ отправок, а не гарантия.**
`run_outbox_worker.py` не реализует SIGTERM/drain, а отправка провайдеру
происходит внутри транзакции, которая коммитится уже ПОСЛЕ ответа Meta. Значит
существует узкое окно: Meta приняла сообщение → процесс убит до коммита → джоб
остался `processing` → шаг 3 пометил его `canceled`. Клиент сообщение получил,
а в БД оно выглядит отменённым. Одна уже начатая отправка может иметь
неопределённый исход; всё, что ещё не начиналось, остановлено надёжно.

Как найти такой случай после остановки — шаг 4a ниже.

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
   держится). **Двумя отдельными запросами, с разными маркерами**: происхождение
   отмены потом нельзя восстановить, а разбирать эти две группы надо
   по-разному.

   Воркер на шаге 1 уже остановлен, поэтому переходов `queued` → `processing`
   быть не должно и порядок запросов не влияет. Но выполняйте их именно в
   порядке ниже — так набор деградирует безопасно, если остановка почему-то не
   вступила в силу: джоб, захваченный между запросами, будет пойман вторым
   запросом и получит консервативный маркер «исход неизвестен», а не потеряется
   между двумя условиями.

```sql
UPDATE message_jobs
SET status     = 'canceled',
    locked_at  = NULL,
    updated_at = now(),
    last_error = 'Canceled: activation rolled back before send'
WHERE provider = 'easyweek'
  AND status = 'queued';
```

```sql
UPDATE message_jobs
SET status     = 'canceled',
    locked_at  = NULL,
    updated_at = now(),
    last_error = 'Canceled: activation rolled back from processing; outcome unknown'
WHERE provider = 'easyweek'
  AND status = 'processing';
```

   Маркер «before send» получает только строка, которая на момент `UPDATE` всё
   ещё была `queued`, то есть заведомо не бралась воркером и не отправлялась.

4. Убедиться, что не осталось отправляемых:

```sql
SELECT status, COUNT(*) FROM message_jobs
WHERE provider = 'easyweek' GROUP BY status;
```

4a. Разобрать бывшие `processing` — **только их**: именно у этой группы исход
    отправки неопределён. Их немного (в пределе — по одному на убитый воркер),
    и каждый надо разобрать вручную:

```sql
SELECT j.id            AS job_id,
       j.job_type,
       o.id            AS outbox_id,
       o.status        AS outbox_status,
       o.provider_message_id,
       o.sent_at
FROM message_jobs j
         LEFT JOIN outbox_messages o ON o.job_id = j.id
WHERE j.provider = 'easyweek'
  AND j.last_error = 'Canceled: activation rolled back from processing; outcome unknown'
ORDER BY j.id DESC;
```

Как читать результат — правило зависит от того, из какого состояния джоб был
отменён:

| Маркер | `outbox_messages` | Что это значит |
| --- | --- | --- |
| `…before send` (бывший `queued`) | строки нет | Отправка не начиналась, отмена корректна. Подавляющее большинство; разбирать не нужно. |
| `…from processing` | строка есть, `provider_message_id` не пуст | Meta приняла сообщение, клиент его, скорее всего, получил. Джоб помечен `canceled` **ошибочно**. |
| `…from processing` | **строки нет** | **Исход неизвестен.** Вставка `OutboxMessage` откатывается вместе с транзакцией, поэтому отсутствие строки — ровно то, как выглядит уже отправленное сообщение, у которого не успел пройти коммит. Проверять вручную. |
| `…from processing` | строка есть, `provider_message_id` пуст | Ответ Meta не сохранился. Проверять вручную. |

Внимание на третью строку таблицы: для бывшего `queued` отсутствие
`outbox_messages` — доказательство, что отправки не было, а для бывшего
`processing` — **не доказательство ничего**. Ровно поэтому маркеры и разделены:
без этого единственный опасный случай выглядел бы как самый безобидный.

Для случаев «проверять вручную» есть один внешний признак: если сообщение
всё-таки ушло, входящий status-callback (`delivered` / `read`) придёт на
`wamid`, которого нет ни в одном живом джобе. Плюс прямая проверка переписки с
клиентом в WhatsApp.

5. **Закрыть производителя.** Выберите вариант по ситуации — у каждого своя
   цена, и угадывать не нужно.

#### Общий запрос: что появилось после остановки

Обоим вариантам нужен один и тот же запрос. Подставьте момент остановки outbox
(шаг 1) вместо `<момент остановки>`:

```sql
SELECT id, job_type, status, created_at, dedupe_key
FROM message_jobs
WHERE provider = 'easyweek'
  AND created_at > TIMESTAMPTZ '<момент остановки>'
ORDER BY created_at DESC;
```

Джобы с `dedupe_key` вида `delivery_retry:<id>:<n>` — это delivery-retry, а не
планировщик.

**Трактовка результата у вариантов РАЗНАЯ, и это не оплошность:**

| Вариант | Непустой результат | Почему |
| --- | --- | --- |
| A | **норма** | Производитель жив намеренно; отправлять его джобы некому, пока outbox остановлен. Запрос — инвентаризация. |
| B | **неисправность** | Производитель должен быть закрыт шагом 2. Запрос — gate. |

#### Вариант A — оставить общий outbox остановленным

Самый надёжный вариант: производитель может создавать retry-джобы сколько
угодно, отправлять их некому. Останавливать его не требуется — но перед
подъёмом outbox накопленное придётся просмотреть (шаг 5A.2).

**Цена:** пока outbox стоит, не отправляется **ничего** — включая Altegio,
Карлсруэ и Растатт. Подходит, когда причина чинится быстро (минуты, не часы).

**Накопление retry-джобов здесь — норма, а не поломка.** Производитель жив, и
пока outbox остановлен, каждый запоздавший `failed`-callback добавляет в очередь
новый `delivery_retry:*`. Их число ограничено: не более
`DELIVERY_RETRY_MAX_ATTEMPTS = 4` попыток на цепочку.

5A.1 **Посмотрите, что накопилось** — запросом из «Общего запроса» выше. Здесь
     он не gate, а инвентаризация: **непустой результат ожидаем**.

5A.2 **Решите, что с накопленным делать. Это обязательный шаг перед подъёмом
     outbox** — поднять его, не посмотрев очередь, значит выпустить всё
     накопленное разом:

  * **причина отката была в СОДЕРЖИМОМ** (не тот текст, не та ссылка, не тот
    филиал) — накопленные retry несут ровно то же неверное содержимое.
    **Отмените их** теми же двумя `UPDATE`, что и в общем шаге 3 §8.2;
  * **причина была внешней** (Meta была недоступна, сетевой сбой, ложная
    тревога) — содержимое корректно, retry можно дать уйти. Учтите, что клиент
    получит уведомление с задержкой на всё время простоя.

     Часть накопленного при подъёме отменится сама: presend-гард отменяет
     retry, у которого истёк дедлайн (для `record_created` / `record_updated`
     это за 30 минут до начала записи). Полагаться на это как на фильтр нельзя —
     решение всё равно принимает оператор.

5A.3 Только теперь поднимите outbox:

```bash
docker compose -p altegio_bot up -d altegio-outbox-worker
```

#### Вариант B — выключить создание delivery-retry и поднять outbox

Нужен, когда чинить придётся долго, а останавливать Altegio на это время
нельзя. **Порядок обязателен.**

1. В `.env` (не в `easyweek.env` — `altegio-whatsapp-inbox-worker` читает
   именно `.env`):

```text
OUTBOX_DELIVERY_RETRY_ENABLED=false
```

2. Пересоздать сервис, который обрабатывает status-callbacks:

```bash
docker compose -p altegio_bot up -d --force-recreate altegio-whatsapp-inbox-worker
```

   **Этот шаг обязателен, и пропустить его — значит не сделать ничего.**
   `docker compose restart` и `up -d` без `--force-recreate` не перечитывают
   `env_file`: воркер продолжит работать со старым `true` и продолжит создавать
   retry-джобы. Ровно та же ловушка, что с `CHATWOOT_INBOX_COMPANY_MAP` в §3,
   и здесь она тише — новых джобов вы не увидите, пока не поднимете outbox.

3. Ещё раз почистить EasyWeek-очередь: между шагами 1 и 2 воркер работал со
   старым значением и мог успеть создать новые джобы. Повторите оба `UPDATE`,
   что и в общем шаге 3 §8.2.

4. **Проверка: производитель закрыт.** Прогоните запрос из «Общего запроса»
   выше. Здесь пустой результат — **обязательный gate**, и проверять его надо
   ДО подъёма outbox: иначе вы отпускаете потребителя, не убедившись, что
   очередь больше не пополняется.

   Непустой результат означает, что производитель всё ещё жив: скорее всего,
   пропущен или не подействовал шаг 2. Вернитесь к нему, повторите шаг 3 и
   проверьте снова — outbox пока не поднимайте.

5. Теперь поднять общий outbox — Altegio возобновляется:

```bash
docker compose -p altegio_bot up -d altegio-outbox-worker
```

6. Повторите проверку из шага 4 несколько раз в течение получаса: запоздавшие
   callback'и приходят не мгновенно, и появление новых `delivery_retry:*` уже
   после подъёма outbox означает, что флаг не подействовал.

**Цена варианта B:** delivery-retry выключены **для всех провайдеров**. Пока
флаг снят, неудачная доставка Altegio не будет повторяться автоматически —
такие сообщения просто не дойдут, и повторять их придётся вручную.

**После устранения причины верните флаг и снова пересоздайте тот же сервис:**

```text
OUTBOX_DELIVERY_RETRY_ENABLED=true
```

```bash
docker compose -p altegio_bot up -d --force-recreate altegio-whatsapp-inbox-worker
```

### Что НЕ ломается в обоих режимах

* захват вебхуков продолжается (`EASYWEEK_ENABLED` не трогаем) — события
  копятся в `easyweek_events` и не теряются;
* нормализация продолжается (`EASYWEEK_PROCESSING_ENABLED` не трогаем) —
  `Client` и `Record` обновляются как раньше;
* Altegio-путь не затронут в §8.1. В §8.2 он затронут дважды, и оба раза
  временно: пауза на время остановки общего outbox, а в варианте B — ещё и
  выключенные delivery-retry для всех провайдеров, пока флаг снят.

Сиды откатывать не нужно: строки шаблонов и отправителя без флага никем не
читаются.

---

## 9. Что НЕ входит в PR-7

* reminders (`reminder_24h`, `reminder_2h`) — следующая фаза / PR-8;
* маркетинг, кампании, promo для EasyWeek;
* гейт `EASYWEEK_NOTIFICATIONS_ENABLED` в `outbox_worker` — отдельное решение
  вне PR-7 (см. §8: именно поэтому жёсткая остановка требует ручных шагов);
* изменения маршрутизации отправителей и Altegio-пути.
