# PR-6/PR-7/PR-7.1 — активация локаций EasyWeek

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

## Production Compose contract

На production все команды этого runbook выполняются из `/opt/altegio_bot` с
одним и тем же file set. Optional override сохраняет подключение реальных
потребителей Chatwoot к `chatwoot_internal`; команда только с
`docker-compose.yml` при recreate отсоединит контейнер от этой сети:

```bash
cd /opt/altegio_bot
COMPOSE="docker compose -p altegio_bot -f docker-compose.yml -f docker-compose.chatwoot-internal.yml"
$COMPOSE config >/tmp/altegio_bot-pr7-compose-config.txt
```

Далее `$COMPOSE` означает именно эту переменную. Пока
`CHATWOOT_BASE_URL` указывает на внутренний host, тот же file set обязателен и
при rollback. Local/dev без внешней сети Chatwoot продолжает использовать
только base-файл: `docker compose -f docker-compose.yml ...`; optional override
в base-конфигурацию не добавляется.

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

Нужны **четыре разных inbox**:

* `Karlsruhe` — branch inbox;
* `Durlach` — branch inbox;
* `Rastatt` — branch inbox;
* `General / Unassigned` — глобальный `CHATWOOT_INBOX_ID`, только для нового
  входящего WhatsApp без authoritative company identity.

`CHATWOOT_INBOX_COMPANY_MAP` — единственный источник обоих направлений:

* outbound mirror: `(provider, company_id) → inbox_id`;
* operator relay: `inbox_id → (provider, company_id) → sender`.

Каноническое направление JSON остаётся `inbox_id → tenant`, но tenant всегда
записывается полной provider-scoped парой:

```text
CHATWOOT_INBOX_COMPANY_MAP={"<KA inbox>":{"provider":"altegio","company_id":<KA Altegio company_id>},"<DU inbox>":{"provider":"easyweek","company_id":<DU EasyWeek location_id>},"<RA inbox>":{"provider":"easyweek","company_id":<RA EasyWeek location_id>}}
```

После cutover для Rastatt указывайте текущий EasyWeek `location_id`, который
стал `company_id` EasyWeek-строк. Старый Altegio company ID Rastatt в этой карте
после cutover недопустим.

Configured map обязана быть однозначной. Parser отвергает duplicate JSON inbox
keys, неизвестный provider, неканонические/неположительные ID и одну tenant-пару,
назначенную двум inbox. Одинаковый numeric `company_id` у Altegio и EasyWeek —
две разные identity и может вести в два разных inbox. Старый integer-only JSON
считается provider-unscoped и при configured map fail-closed: он не может
однозначно разрешить provider collision. При configured+invalid карте оба
направления fail-closed. При валидной карте неизвестный inbox блокирует operator
relay, а неизвестная tenant-пара
пропускает только secondary Chatwoot mirror; успешная WhatsApp-отправка клиенту
не отменяется. Global inbox не используется как скрытый outbound fallback.

Пустая карта (`""`/`{}`) — только legacy single-inbox mode: outbound mirror
остаётся в глобальном `CHATWOOT_INBOX_ID`, как до PR-7.

### Проверка карты до deploy

```bash
$COMPOSE run --rm altegio-outbox-worker \
  /app/.venv/bin/python - <<'PY'
from altegio_bot.easyweek_locations import configured_easyweek_locations
from altegio_bot.settings import settings
from altegio_bot.webhooks.common import (
    ChatwootTenantIdentity,
    parse_chatwoot_inbox_company_map,
    positive_int,
    resolve_chatwoot_general_inbox,
)

branches = configured_easyweek_locations()
inboxes = parse_chatwoot_inbox_company_map(settings.chatwoot_inbox_company_map)
easyweek_tenants = {
    ChatwootTenantIdentity(provider="easyweek", company_id=location.company_id)
    for location in branches.locations.values()
}
mapped_tenants = set(inboxes.inverse_mapping)
configured_general_id = positive_int(settings.chatwoot_inbox_id)
general_inbox_id, general_inbox_reason = resolve_chatwoot_general_inbox(
    inboxes,
    settings.chatwoot_inbox_id,
)
print(
    {
        "map_configured": inboxes.configured,
        "map_valid": inboxes.valid,
        "provider_scoped": inboxes.provider_scoped,
        "map_entries": len(inboxes.mapping),
        "unique_inboxes": len(inboxes.mapping) == len(set(inboxes.mapping)),
        "unique_tenants": len(inboxes.inverse_mapping) == len(inboxes.mapping),
        "easyweek_tenants_all_mapped": easyweek_tenants <= mapped_tenants,
        "global_general_inbox_configured": configured_general_id is not None,
        "global_general_inbox_distinct_from_branches": general_inbox_id is not None,
        "global_general_inbox_validation_reason": general_inbox_reason,
    }
)
PY
```

Gate: configured=true, valid=true, provider_scoped=true, `map_entries=3`, оба
unique=true, EasyWeek tenant-пары полностью покрыты, global General/Unassigned
настроен отдельно: `global_general_inbox_configured=true` и
`global_general_inbox_distinct_from_branches=true`. Если General ID совпал хотя
бы с одним ключом branch map (`general_inbox_overlaps_branch`) — **STOP**: новый
или неразрешённый inbound иначе попадёт в филиал. Вручную
сверьте: DU/RA IDs равны одноимённым записям `EASYWEEK_LOCATION_MAP`, а третья
company — действующая Karlsruhe Altegio company. Любое расхождение — STOP.

### Карту читают два worker

После правки карты пересоздайте оба потребителя; обычный `restart` и `up -d`
без `--force-recreate` не перечитывают env. До recreate зафиксируйте сети обоих
текущих контейнеров:

```bash
for CHATWOOT_SERVICE in altegio-outbox-worker altegio-whatsapp-inbox-worker; do
  CONTAINER_ID="$($COMPOSE ps -q "$CHATWOOT_SERVICE")"
  test -n "$CONTAINER_ID"
  echo "SERVICE=$CHATWOOT_SERVICE"
  docker inspect "$CONTAINER_ID" \
    --format '{{range $name, $_ := .NetworkSettings.Networks}}{{println $name}}{{end}}' | sort
done
```

Gate до deploy: у каждого worker видны `altegio_bot_default` и
`${CHATWOOT_INTERNAL_NETWORK:-chatwoot_default}`. Затем recreate выполняется
тем же production file set:

```bash
$COMPOSE up -d --force-recreate \
  altegio-outbox-worker altegio-whatsapp-inbox-worker
```

Сразу после recreate повторите inspect — отсутствие любой из двух сетей
останавливает rollout:

```bash
for CHATWOOT_SERVICE in altegio-outbox-worker altegio-whatsapp-inbox-worker; do
  CONTAINER_ID="$($COMPOSE ps -q "$CHATWOOT_SERVICE")"
  test -n "$CONTAINER_ID"
  echo "SERVICE=$CHATWOOT_SERVICE"
  docker inspect "$CONTAINER_ID" \
    --format '{{range $name, $_ := .NetworkSettings.Networks}}{{println $name}}{{end}}' | sort
done
```

Проверьте внутренний DNS и authenticated API из **обоих** реальных
потребителей. Проба выводит только boolean/status и тип технической ошибки: не
печатает token, телефон, URL или response body.

```bash
for CHATWOOT_SERVICE in altegio-outbox-worker altegio-whatsapp-inbox-worker; do
  echo "SERVICE=$CHATWOOT_SERVICE"
  $COMPOSE exec -T "$CHATWOOT_SERVICE" /app/.venv/bin/python - <<'PY'
import asyncio
import socket
from urllib.parse import urlsplit

import httpx

from altegio_bot.settings import settings

base_url = settings.chatwoot_base_url.rstrip("/")
hostname = urlsplit(base_url).hostname
if not hostname:
    print({"dns_ok": False, "dns_error_type": "MissingHostname"})
    raise SystemExit(1)
try:
    socket.getaddrinfo(hostname, None)
except Exception as exc:
    print({"dns_ok": False, "dns_error_type": type(exc).__name__})
    raise SystemExit(1) from None

headers = {"api_access_token": settings.chatwoot_api_token}
if settings.chatwoot_api_forwarded_proto:
    headers["X-Forwarded-Proto"] = settings.chatwoot_api_forwarded_proto
url = f"{base_url}/api/v1/accounts/{settings.chatwoot_account_id}/inboxes"

async def probe() -> None:
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(url, headers=headers)
    except Exception as exc:
        print({"dns_ok": True, "api_ok": False, "api_error_type": type(exc).__name__})
        raise SystemExit(1) from None
    ok = response.status_code == 200
    print({"dns_ok": True, "api_ok": ok, "api_status": response.status_code})
    if not ok:
        raise SystemExit(1)

asyncio.run(probe())
PY
done
```

`altegio-outbox-worker` читает tenant→inbox для outbound mirror;
`altegio-whatsapp-inbox-worker` читает inbox→tenant для operator relay и
tenant route для входящих reply/reaction.
`altegio-api` ради этой карты пересоздавать не нужно.

### General / Unassigned — единственный разрешённый global fallback

Совершенно новое входящее WhatsApp без authoritative company identity нельзя
безопасно отнести к филиалу: общий `phone_number_id`, телефон клиента и последний
booking не являются tenant proof. Такое входящее остаётся в глобальном
`CHATWOOT_INBOX_ID` (`General / Unassigned`), а оператор определяет филиал
вручную. По телефону филиал не угадывается. Для outbound lifecycle mirror и
operator relay этот fallback запрещён при configured map.

Ответы на новые identity-less команды используют тот же маршрут только по
явному outbound intent: STOP ACK, START ACK, существующий synchronous promo info
reply и существующий synchronous promo funnel reply зеркалируются в
`General / Unassigned`. Это не EasyWeek promo и не филиальная маршрутизация:
результат `_pick_sender()` по общему `phone_number_id` не считается tenant proof.
Любой обычный lifecycle send со случайно отсутствующей или невалидной парой
`(provider, company_id)` по-прежнему блокирует Chatwoot mirror fail-closed и не
получает неявный fallback в General.

Explicit-General intent сохраняется вместе с каждой такой отправкой в
`OutboxMessage.meta.chatwoot_route`. Поэтому reply или reaction клиента на
STOP/START ACK либо synchronous promo reply снова проходит проверку отдельного
General inbox и возвращается в `General / Unassigned`, а не пытается вывести
филиал из `sender_id`, `company_id`, общего номера или телефона клиента.

Исторические STOP/START и synchronous promo Outbox-строки без marker
поддерживаются только по строгой старой provenance: internal `message_source=bot`,
`job_id IS NULL`, непустой Meta message ID и точное согласование
`meta.source` + `meta.command` + `template_code`. Почти похожая либо
противоречивая строка, неизвестный marker и любой другой bot Outbox без
`MessageJob` остаются fail-closed с технической route-причиной. Marker на
lifecycle Outbox с `MessageJob` не заменяет provider-scoped identity, а marker
на operator Outbox не заменяет identity из `WhatsAppSender`: оба случая
считаются audit-конфликтом и блокируются fail-closed.

General ID по-прежнему валидируется против всей branch map до создания
Chatwoot client. Совпадение General хотя бы с одним branch inbox остаётся hard
**STOP** (`general_inbox_overlaps_branch`) для нового inbound и для всей цепочки
reply/reaction на explicit-General Outbox.

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
EASYWEEK_ALLOWED_SERVICE_CATEGORIES=[]
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

`EASYWEEK_ALLOWED_SERVICE_CATEGORIES` — строгий JSON-массив точных root-level
`service_category`. Пустая строка/массив, malformed JSON, duplicate после
нормализации либо любой нестроковый/blank/control/слишком длинный элемент
ничего не разрешает. Production-значение PR-7.1:

```text
EASYWEEK_ALLOWED_SERVICE_CATEGORIES=["Wimpernverlängerung"]
```

Категория не выводится из `service_name`, `services_description` или
`service_id`. Настройку читают `altegio-easyweek-inbox-worker` и общий
`altegio-outbox-worker`, поэтому после её изменения оба контейнера требуется
пересоздать через `--force-recreate`.

До отдельного реального multi-service capture действует временное строгое
правило: только root-level `services_count=1` доказывает применимость singular
`service_category`. Значение больше 1 подавляется как
`category_ambiguous_multi_service`; отсутствующий/null/invalid/0 count и старый
snapshot без count — как `service_count_unproven`. `quantity`, текст услуги и
`RecordService.amount` доказательством не являются.

После записи нового реестра пересоздайте **оба** его потребителя. Обычный
`restart` и `up -d` без `--force-recreate` не перечитывают `env_file`:

```bash
$COMPOSE up -d --force-recreate \
  altegio-easyweek-inbox-worker altegio-outbox-worker
```

Проверьте эффективную конфигурацию внутри обоих контейнеров, не печатая raw
JSON, UUID, URLs или секреты:

```bash
for EW_SERVICE in altegio-easyweek-inbox-worker altegio-outbox-worker; do
  echo "SERVICE=$EW_SERVICE"
  $COMPOSE exec -T "$EW_SERVICE" /app/.venv/bin/python - <<'PY'
from altegio_bot.easyweek_locations import configured_easyweek_locations
from altegio_bot.easyweek_service_category import parse_allowed_service_categories
from altegio_bot.settings import settings

registry = configured_easyweek_locations()
categories = parse_allowed_service_categories(settings.easyweek_allowed_service_categories)
print(
    {
        "processing_enabled": settings.easyweek_processing_enabled,
        "notifications_enabled": settings.easyweek_notifications_enabled,
        "registry_configured": registry.configured,
        "registry_valid": registry.valid,
        "service_categories_configured": categories.configured,
        "service_categories_valid": categories.valid,
        "service_categories_count": len(categories.keys),
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
notifications=false. Для PR-7.1 также обязательны
`service_categories_configured=true`, `service_categories_valid=true` и
`service_categories_count=1`. Raw allowlist и название категории эта проверка
намеренно не печатает. Любое расхождение останавливает rollout до сида.

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
$COMPOSE run --rm altegio-outbox-worker \
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

## 6A. Обязательный rollout PR-7.1: service-category filter

PR-8 и reminders не начинаются, пока этот gate не пройден. На production не
запускаются pytest, миграционные тесты или произвольные replay: только
read-only SQL/log checks ниже и две контролируемые реальные smoke-записи.

`EASYWEEK_NOTIFICATIONS_ENABLED` остаётся `true` на всём протяжении rollout.
Write fence — это `EASYWEEK_PROCESSING_ENABLED`.

> **Запрещённая пара.** Работающий inbox worker не должен ни на одном шаге
> rollout видеть `EASYWEEK_PROCESSING_ENABLED=true` одновременно с
> `EASYWEEK_NOTIFICATIONS_ENABLED=false`. В этом режиме
> `processing_is_configured()` возвращает `true`, то есть **разрешает claim**:
> worker забирает новые captured events, обновляет domain snapshot, выходит из
> `plan_lifecycle_job()` до INSERT и переводит event в терминальный `processed`
> без lifecycle job. Автоматического replay после обратного включения
> notifications нет, поэтому даже короткое deploy-окно необратимо теряет
> клиентские уведомления. Единственная процедура, где эта пара допустима, —
> явно необратимый domain-only drain (§6A, раздел восстановления); он не
> является ни rollout, ни recovery, и настоящий порядок на него не ссылается.

Порядок строгий:

1. В `easyweek.env` поставить write fence, не трогая notifications:

```text
EASYWEEK_PROCESSING_ENABLED=false
```

   `EASYWEEK_NOTIFICATIONS_ENABLED` остаётся `true` и на этом шаге не
   редактируется.

2. Пересоздать **только** inbox worker, чтобы он перестал claim'ить новые
   события:

```bash
$COMPOSE up -d --force-recreate altegio-easyweek-inbox-worker
```

3. Убедиться, что claim действительно остановлен. Выполнить запрос дважды с
   интервалом в минуту: `captured` не должен убывать, новые `processed` не
   должны появляться.

```sql
SELECT status, count(*)
FROM easyweek_events
GROUP BY status
ORDER BY status;
```

   Живые вебхуки продолжают приниматься и копиться в `captured` — это и есть
   цель fence: события сохраняются до включения нового фильтра.

4. Остановить **второй источник** EasyWeek jobs. `EASYWEEK_PROCESSING_ENABLED`
   закрывает только planner в `easyweek_inbox_worker`. Delivery-retry рождается
   не там: его создаёт `_handle_failed_delivery_status` в
   `altegio-whatsapp-inbox-worker` при запоздавшем Meta `failed`-callback. Этот
   путь гейтится только `OUTBOX_DELIVERY_RETRY_ENABLED` и **не читает** ни
   `EASYWEEK_PROCESSING_ENABLED`, ни `EASYWEEK_NOTIFICATIONS_ENABLED`, а
   создаёт `provider='easyweek'` job ровно тех трёх типов, ради которых
   делается rollout: `record_created`, `record_updated`, `record_canceled`
   (`DELIVERY_RETRY_JOB_TYPES`, ключ `delivery_retry:<outbox_id>:<attempt>`).

   Поэтому запрос очереди при работающем producer — это моментальный снимок, а
   не fence. Пока старый `altegio-outbox-worker` ещё не пересоздан, он не
   содержит send-time category guard PR-7.1, и один поздний retryable callback
   по disallowed booking отправит запрещённое уведомление. Первая retry delay
   (10 минут) защитой не является: rollout не должен зависеть от того, успеет
   ли оператор закончить deploy быстрее неё.

   Каким способом останавливать — зависит от того, какой image сейчас в
   production. Проверьте это **до** остановки, у запущенного контейнера, а не
   по checked-out исходникам: в рабочем дереве новый runner есть всегда, в том
   числе на том самом deploy, который его ещё только устанавливает.

```bash
WA_CID="$(docker ps -a \
  --filter 'label=com.docker.compose.project=altegio_bot' \
  --filter 'label=com.docker.compose.service=altegio-whatsapp-inbox-worker' \
  --format '{{.ID}}')"
printf 'containers: %s\n' "$(printf '%s\n' "$WA_CID" | grep -c '[0-9a-f]')"
printf 'oneoff: %s\n' "$(docker inspect -f '{{index .Config.Labels "com.docker.compose.oneoff"}}' $WA_CID)"
docker exec $WA_CID /app/.venv/bin/python -c 'import inspect; from altegio_bot.workers import whatsapp_inbox_worker as w; print("graceful" if "stop_event" in inspect.signature(w.run_loop).parameters else "legacy")'
```

   Контейнеров должно быть ровно `1`, `oneoff` — `false`. Несколько service
   containers, неизвестная one-off replica или пустой вывод — **STOP** и ручной
   разбор: дренировать один контейнер ничего не говорит о claimed batch другого.

##### Вариант «graceful» — обычный порядок для всех последующих deploy

```bash
$COMPOSE stop -t 300 altegio-whatsapp-inbox-worker
```

   `-t 300` соответствует `stop_grace_period: 5m` в `docker-compose.yml`.
   Worker обрабатывает SIGTERM: новый batch он больше не claim'ит, но уже
   закоммиченный `received -> processing` batch дорабатывает до конца.

   Успешный код возврата `docker stop` доказательством **не является**: демон
   возвращает `0` и тогда, когда истёк timeout и процесс пришлось убить
   SIGKILL. Проверяйте финальное состояние контейнера, **не удаляя** его:

```bash
docker inspect -f 'status={{.State.Status}} exit={{.State.ExitCode}} oom={{.State.OOMKilled}}' $WA_CID
```

   Требуется `status=exited`, `exit=0`, `oom=false`. `exit=137` — это SIGKILL
   после истечения timeout, то есть batch оборвали. Любое отклонение —
   **STOP**: контейнер не удалять, он единственный носитель этих данных.

##### Вариант «legacy» — РАЗОВЫЙ переход со старого image (a82d449 и раньше)

Старый worker **не обрабатывает SIGTERM**: у него нет ни `stop_event`, ни
signal handlers. `docker stop` с любым timeout убивает процесс там, где он
находится, и уже закоммиченный `processing` batch остаётся навсегда —
`-t 300` здесь бесполезен.

**`docker pause` сам по себе эту гонку не закрывает.** Он замораживает
клиентский процесс в контейнере, но не его backend в PostgreSQL. Уже
отправленный COMMIT доводится до конца независимо от замороженного клиента:

```text
worker отправил COMMIT
  → контейнер заморожен до получения ответа
  → backend PostgreSQL завершает COMMIT
  → проверочный SELECT в другой сессии ещё видит 0
  → контейнер удалён
  → строки становятся видимыми: 'processing' без владельца
```

Ни второй SELECT, ни более долгая пауза, ни аудит после `rm` эту гонку не
закрывают — они лишь фиксируют уже нанесённый ущерб. Барьер должен быть на
стороне БД.

Поэтому переход выполняется одним helper'ом, который держит DB-side барьер на
всё время проверки и retirement.

Helper'у одновременно нужны три вещи: код именно этого commit, доступ к
production PostgreSQL и управление host Docker (pause/rm). Обычный
`altegio-api` не подходит ни по одному пункту: его image собран из предыдущего
commit, в нём нет Docker CLI и у него нет Docker socket. Поэтому helper
запускается в отдельном одноразовом ops-сервисе `easyweek-legacy-retire`
(profile `ops`, `Dockerfile.ops`) — **единственном**, который получает bind
mount host Docker socket. Обычный `docker compose up -d` его не запускает.

1. Собрать ops image из текущего checkout и выполнить **read-only** probe:

```bash
$COMPOSE --profile ops run --rm --build easyweek-legacy-retire \
  -m altegio_bot.scripts.retire_legacy_whatsapp_worker --probe
```

   Ожидаемый вывод — все четыре значения истинны, например:

```text
{'helper_module': 'altegio_bot.scripts.retire_legacy_whatsapp_worker',
 'docker_cli': True, 'docker_daemon': True,
 'whatsapp_worker_containers': 1, 'database': True}
```

   `--build` обязателен: без него запустился бы старый image без helper'а.
   Probe ничего не pause'ит, не удаляет и не переписывает. Любое `False`,
   `whatsapp_worker_containers` не равное `1` или ненулевой код возврата —
   **STOP**, retirement не запускать.

2. Выполнить сам retirement:

```bash
$COMPOSE --profile ops run --rm --build easyweek-legacy-retire \
  -m altegio_bot.scripts.retire_legacy_whatsapp_worker
```

   Helper сам находит свой target по Compose-меткам и доказывает его identity.
   Передавать container ID вручную не нужно и не следует: опция
   `--expect-container` существует только как перекрёстная проверка и цель
   **не** выбирает — при несовпадении helper отказывается работать.

Что он делает по шагам:

1. Сам резолвит цель и доказывает её identity: ровно один контейнер с метками
   `com.docker.compose.project=altegio_bot` и
   `com.docker.compose.service=altegio-whatsapp-inbox-worker`, метка
   `oneoff` — отрицательная (регистр не важен: Compose пишет и `False`, и
   `false`), состояние `running`, а живой image **не** поддерживает graceful
   shutdown. Любое расхождение — **STOP**, ничего не трогается.
2. `docker pause` — чтобы клиент не начинал новых действий. Сразу после
   заморозки топология проверяется повторно: если появилась вторая replica,
   helper размораживает worker и завершается **STOP**.
3. Открывает отдельную транзакцию с `lock_timeout` и `statement_timeout`.
4. Берёт `LOCK TABLE whatsapp_events IN SHARE MODE`. Это и есть барьер: claim
   старого worker'а выполняет `UPDATE whatsapp_events` под `ROW EXCLUSIVE`, а
   `SHARE` — минимальный режим, который с ним конфликтует. Либо lock ждёт
   завершения in-flight claim-транзакции и последующий счёт видит её строки,
   либо lock взят первым, и тогда `UPDATE` старого worker'а уже не пройдёт.
5. Под этим lock считает `whatsapp_events` в `processing`.
6. При `0` — удаляет **тот самый** проверенный контейнер, **не отпуская lock**,
   и повторяет проверку под тем же lock.
7. Только после этого фиксирует транзакцию.

Ожидаемый вывод при успехе — `{'retired': True, 'processing_under_lock': 0}` и
нулевой код возврата.

**STOP-условия** (helper завершается ненулевым кодом и печатает `STOP: ...`):

| Причина | Что означает |
| --- | --- |
| `could not acquire the whatsapp_events barrier` | claim-транзакция ещё открыта; контейнер не удалён, worker разморожен |
| `N row(s) are in 'processing' under the barrier` | worker закоммитил batch, который не доработал |
| `expected exactly one … container` / one-off | топология не доказана |
| `container belongs to project/service …` | цель не тот сервис — ничего не трогается |
| `the one-off label … is not recognisable` | метка нераспознаваема; fail-closed |
| `is '<state>', not 'running'` | не живой worker: helper только для running legacy |
| `already honours the graceful shutdown contract` | это не legacy image — используйте обычный deploy path |
| `cannot read the shutdown capability` | capability не читается |
| `the container set changed after the freeze` | появилась вторая replica |
| `Docker discovery failed` / `cannot inspect` | состояние не читается |

Ни одна из этих веток не удаляет контейнер, не переписывает строки и не
продолжает rollout. Если `docker rm` всё же не удался и состояние контейнера
прочитать нельзя, helper явно предупредит, что worker **может остаться
paused** — молча считать Docker-ошибку отсутствием контейнера он не будет. Массовый `UPDATE processing -> received` запрещён: для
обычных webhook events безопасность повторного side effect не доказана.

Если helper вернул STOP из-за открытой claim-транзакции, дайте старому worker'у
доработать batch (он размораживается автоматически) и запустите helper снова.
Если `processing` не сходится к нулю — ручной разбор по event id, без replay.

**Цена барьера.** Пока lock удерживается (секунды: только финальная проверка и
удаление контейнера), запись новых WhatsApp webhook events в `whatsapp_events`
может ждать освобождения lock. Приём вебхуков при этом не теряется — запросы
просто дожидаются lock. EasyWeek capture идёт в свою таблицу и не затрагивается.

После успешного helper'а продолжайте rollout обычным порядком: сам deploy
legacy worker уже не увидит, а его gate падает non-zero до build и migrations,
если такой контейнер всё же обнаружится.

##### Общее для обоих вариантов

   Что именно останавливается: `altegio-api` продолжает принимать вебхуки Meta
   и сохранять их в `whatsapp_events` со статусом `received`, ничего не
   теряется. EasyWeek capture тоже продолжает работать. Приостановлена только
   обработка inbound-сообщений и status-callbacks — до шага 12. Это общий для
   всех филиалов worker, поэтому пауза затрагивает и Altegio-трафик: входящие
   сообщения и статусы Карлсруэ и Раштата будут разобраны после возобновления,
   с задержкой на длительность rollout. Это цена корректности, а не побочный
   эффект.

   Обязательный gate перед продолжением — обычных событий в `processing` быть
   не должно:

```sql
SELECT count(*) AS stranded_processing
FROM whatsapp_events
WHERE status = 'processing';
```

   Ожидается `0`. Ненулевое значение — **STOP**: batch не дренирован.
   Разбирать такие строки нужно поштучно. Массовый
   `UPDATE processing -> received` делать нельзя: для обычных webhook events
   безопасность повторного side effect не доказана, и replay может отправить

   сообщение повторно.

5. Только теперь — финальный queue gate. Producer закрыт, поэтому пустая
   очередь останется пустой:

```sql
SELECT status, count(*)
FROM message_jobs
WHERE provider = 'easyweek'
  AND job_type IN ('record_created', 'record_updated', 'record_canceled')
  AND status IN ('queued', 'processing')
GROUP BY status
ORDER BY status;
```

   Ожидается 0 строк. Если очередь непуста — **STOP** и индивидуальный разбор
   каждой job до deploy. Нельзя ни автоматически отменять, ни переписывать, ни
   безусловно выпускать их старым outbox.
6. Записать точный production allowlist:

```text
EASYWEEK_ALLOWED_SERVICE_CATEGORIES=["Wimpernverlängerung"]
```

7. Развернуть новый код при `EASYWEEK_PROCESSING_ENABLED=false`. Notifications
   по-прежнему не трогаются.
8. Проверить effective-конфигурацию одноразовым контейнером, который не
   запускает loop (probe из §4). Gate проверяет только booleans и количество:
   `configured=true`, `valid=true`, `count=1`, а также
   `notifications_enabled=true`. Raw env и название категории не печатаются.
9. Пересоздать `altegio-outbox-worker` с новым кодом и валидной конфигурацией:

```bash
$COMPOSE up -d --force-recreate altegio-outbox-worker
```

10. Убедиться, что новый outbox действительно поднялся и обслуживает очередь,
    **до** возврата retry producer. Порядок здесь и есть защита: сохранённые
    поздние status-callbacks могут создать retries, и обрабатывать их должен
    только новый образ с send-time eligibility guard PR-7.1.
11. Только теперь вернуть `altegio-whatsapp-inbox-worker`:

```bash
$COMPOSE up -d --force-recreate altegio-whatsapp-inbox-worker
```

    Накопленные `whatsapp_events` разбираются штатно. Retry-jobs, которые из
    них родятся, попадут уже в guarded outbox: для disallowed категории job
    завершится локально, без Meta attempt.

12. Ещё раз подтвердить, что `EASYWEEK_NOTIFICATIONS_ENABLED=true` — до снятия
   fence, а не после.
13. Снять write fence:

```text
EASYWEEK_PROCESSING_ENABLED=true
```

14. Пересоздать inbox worker:

```bash
$COMPOSE up -d --force-recreate altegio-easyweek-inbox-worker
```

15. Сохранённые за время fence `captured` события проходят новый
    category-фильтр: разрешённые создают lifecycle job, остальные штатно
    suppress'атся по стабильной технической причине. Ни одно из них не было
    списано терминально без решения фильтра.

### Контролируемый smoke rollout

Положительным доказательством является **новая** job, созданная после начала
smoke. Resend старой доставки таким доказательством быть не может: он
воспроизводит тот же `event_hint`, `booking_uuid` и `payload_hash`, а значит и
тот же expected dedupe key, поэтому находит job, созданную задолго до rollout.
Поэтому smoke делается только на новых bookings с новыми `booking_uuid`.

1. Зафиксировать baseline до создания smoke-записей:

```sql
SELECT COALESCE(MAX(id), 0) AS smoke_event_id_baseline
FROM easyweek_events;
```

   Записать результат как `<baseline_event_id>` и текущий UTC-момент как
   `<smoke_start>`.

2. Создать **новую** allowed single-service booking категории
   `Wimpernverlängerung` — новая запись, новый `booking_uuid`. Записать её
   `easyweek_events.id` как `<allowed_event_id>`.
3. Создать **новую** disallowed single-service booking другой категории —
   отдельная новая booking с собственным `booking_uuid`. Записать её id как
   `<disallowed_event_id>`. Не подбирать текст услуги под ресничную категорию:
   фильтр намеренно смотрит только root-level category.
4. Проверить обе доставки одной read-only командой:

```bash
$COMPOSE run --rm --no-deps \
  --entrypoint /app/.venv/bin/python \
  altegio-easyweek-inbox-worker \
  -m altegio_bot.scripts.easyweek_recovery_audit \
  --baseline-event-id <baseline_event_id> \
  --smoke-start '<smoke_start>' \
  --smoke-event-id <allowed_event_id> \
  --smoke-event-id <disallowed_event_id>
```

   Команда read-only, не поднимает worker loop, использует production
   `easyweek_job_dedupe_key()` и печатает только booleans, counts и технические
   ID. `booking_uuid`, dedupe key, payload, категория и клиентские данные не
   выводятся. Отсутствующий event, нераспознанный hint или `booking_uuid=NULL`
   приводят к ошибке, а не к зелёному отчёту.

   Общий gate для обеих записей: `distinct_bookings=2`,
   `newer_than_baseline=true`, `booking_first_seen_here=true`. Последнее и
   отличает новую booking от Resend: у Resend более ранний event с тем же
   `booking_uuid` уже существует.

   Gate для `<allowed_event_id>`:

   | Поле | Ожидание |
   | --- | --- |
   | `event_status` | `processed` |
   | `expected_job_type` | соответствует hint доставки |
   | `exact_jobs` | ровно `1` |
   | `job_created_after_smoke_start` | `true` — это и есть положительное доказательство |
   | `job_type_matches_event` | `true` |
   | `job_company_matches_record` | `true` |
   | `job_record_matches_booking` | `true` |
   | `job_statuses` | одно объяснимое значение: `queued`, `processing`, `done` либо terminal по штатному deadline |
   | `outbox_rows` | ровно `1` |
   | `outbox_delivery_proven` | `true` |
   | `outbox_status_counts` | единственный статус из `delivered` или `read` |

   **Наличие Outbox-строки и успешная отправка — разные доказательства.**
   `outbox_rows=1` доказывает только то, что planner и фильтр PR-7.1
   отработали и job дошла до outbox: строка существует и в `queued`, и в
   `sending`, и в `failed`, и в `unknown`. Успешную provider-попытку
   доказывает только статус:

   | `outbox_outcome` | Статусы | Что делать |
   | --- | --- | --- |
   | `proven` | `delivered`, `read` | smoke зелёный |
   | `pending` | `queued`, `sending`, `sent` | ещё не завершено — подождать и повторить audit |
   | `not_green` | `failed`, `unknown`, либо более одной строки | **STOP** |
   | `none` | строк нет | ожидаемо для disallowed |

   **`sent` не является доказательством доставки.** Он означает только, что
   Meta приняла сообщение; после него ещё может прийти `failed`, строка
   перейдёт в `failed` и будет создан delivery retry. Поэтому `sent` — это
   `pending` (в отчёте виден отдельно как `outbox_provider_accepted=true`), и
   зелёными считаются только `delivered` и `read`, ровно как в runtime
   (`_SUCCESSFUL_DELIVERY_STATUSES`).

   `sent_at` или `provider_message_id` сами по себе `failed`/`unknown` в успех
   не превращают: оба поля остаются на строке отклонённой попытки. Если
   allowed smoke получил `failed`/`unknown` — разобраться с внешней причиной
   (Meta, шаблон, номер), затем сделать **новую** контролируемую booking с
   новым UUID и повторить smoke. Не засчитывать неудачную попытку через
   старую строку и не чинить это replay'ем.

   Gate для `<disallowed_event_id>`:

   | Поле | Ожидание |
   | --- | --- |
   | `event_status` | `processed` |
   | `record_id` | не `null` — domain snapshot сохранён |
   | `exact_jobs` | `0` |
   | `outbox_rows` | `0` |
   | `outbox_outcome` | `none` — это ожидаемый результат, а не ошибка |
   | Meta / Chatwoot | сообщений нет |

   Отсутствие job у disallowed объясняется контролируемым disallowed payload
   contract этой конкретной smoke-записи, а не текущим `Record.raw`.

   Gate привязан к `<allowed_event_id>` и `<disallowed_event_id>`. Агрегаты за
   окно (ниже) — только наблюдаемость: при живом трафике в окно попадают чужие
   события, поэтому изменение общего счётчика ничего не доказывает.

Безопасная агрегированная наблюдаемость за выбранное окно:

```sql
SELECT status, count(*)
FROM easyweek_events
WHERE received_at >= TIMESTAMPTZ '<smoke_start>'
GROUP BY status
ORDER BY status;
```

```sql
SELECT status, count(*)
FROM message_jobs
WHERE provider = 'easyweek'
  AND job_type IN ('record_created', 'record_updated', 'record_canceled')
  AND created_at >= TIMESTAMPTZ '<smoke_start>'
GROUP BY status
ORDER BY status;
```

Количество planner/send-time suppression по стабильной технической причине:

```bash
$COMPOSE logs --since='<smoke_start>' altegio-easyweek-inbox-worker altegio-outbox-worker \
  | grep -Eo 'reason=(category_missing|category_not_allowed|category_ambiguous_multi_service|service_count_unproven|allowed_categories_unconfigured|allowed_categories_invalid)' \
  | sort | uniq -c
```

`category_missing`, `category_not_allowed`,
`category_ambiguous_multi_service` и `service_count_unproven` — terminal
business suppression. `allowed_categories_unconfigured` и
`allowed_categories_invalid` означают recoverable configuration outage:
inbox не claim'ит новые events при включённых notifications, а outbox не
отменяет job, а откладывает её с bounded backoff без расхода Meta attempts.

### Восстановление после invalid/unconfigured allowlist

#### Почему notifications НЕ выключают

Ключевая асимметрия `processing_is_configured()`, и её надо понимать до первой
команды:

| `processing` | `notifications` | allowlist невалиден | Что делает inbox worker |
| --- | --- | --- | --- |
| `true` | **`true`** | да | **не claim'ит** — captured backlog цел |
| `true` | **`false`** | да | **claim'ит**: обновляет domain snapshot, `plan_lifecycle_job` выходит на первой строке, событие уходит в terminal `processed` **без job** |

Вторая строка необратима. Автоматического replay для `processed` события нет:
после исправления allowlist уведомление уже не восстановится никогда. Поэтому
**невалидный allowlist при включённых notifications — это и есть штатный
fail-closed fence**, и трогать его не нужно: он уже остановил конвейер ровно
там, где надо, и сохранил backlog.

Выключение notifications в этой ситуации — не «безопасный шаг перед починкой»,
а способ молча уничтожить backlog. О том, когда это всё же допустимо, — ниже,
в «Domain-only drain».

#### Штатное восстановление

`EASYWEEK_NOTIFICATIONS_ENABLED` остаётся `true` на всём протяжении. Работающий
inbox worker не пересоздаётся, пока конфигурация невалидна.

1. Зафиксировать объём затронутого backlog — только aggregate counts:

```sql
SELECT status, count(*)
FROM easyweek_events
GROUP BY status
ORDER BY status;
```

```sql
SELECT status, count(*)
FROM message_jobs
WHERE provider = 'easyweek'
  AND job_type IN ('record_created', 'record_updated', 'record_canceled')
  AND status IN ('queued', 'processing')
GROUP BY status
ORDER BY status;
```

   Ни удалять эти строки, ни переводить руками в `processed`, ни делать любой
   другой `UPDATE` production-данных ради восстановления нельзя.

   Что именно обязано сохраниться: до пересоздания сервисов ни один
   отложенный конфигурацией job не должен исчезнуть из БД или израсходовать
   попытку Meta из-за невалидного allowlist. **После** восстановления статусы
   меняться могут и это штатно: outbox доводит job до `done`, retry
   продолжается, а истёкший штатный delivery deadline законно переводит job в
   `canceled`/`failed` по существующей политике. Поэтому требование
   «`queued`/`processing` должны сохраниться до конца процедуры» неверно —
   проверять надо существование job и объяснимый terminal/retry исход, а не
   застывший статус.

2. Исправить allowlist в `easyweek.env`, сохранив production-инвариант:

```text
EASYWEEK_ALLOWED_SERVICE_CATEGORIES=["Wimpernverlängerung"]
```

3. Проверить новую effective-конфигурацию **до** пересоздания потребителей —
   одноразовым контейнером, который не запускает inbox loop:

```bash
$COMPOSE run --rm --no-deps \
  --entrypoint /app/.venv/bin/python \
  altegio-easyweek-inbox-worker \
  -c 'from altegio_bot.easyweek_locations import configured_easyweek_locations; from altegio_bot.easyweek_service_category import parse_allowed_service_categories; from altegio_bot.settings import settings; r = configured_easyweek_locations(); c = parse_allowed_service_categories(settings.easyweek_allowed_service_categories); print({"processing_enabled": settings.easyweek_processing_enabled, "notifications_enabled": settings.easyweek_notifications_enabled, "registry_ready": r.ready, "service_categories_configured": c.configured, "service_categories_valid": c.valid, "service_categories_count": len(c.keys)})'
```

   `--entrypoint` и `--no-deps` здесь обязательны: они гарантируют, что
   контейнер выполнит только эту проверку и не стартует inbox loop. Вывод —
   только booleans и число: raw allowlist, название категории, API key, webhook
   secret, UUID, URL и payload не печатаются.

   Gate: `service_categories_configured=true`, `service_categories_valid=true`,
   `service_categories_count=1`, `registry_ready=true`,
   `notifications_enabled=true`. Любое расхождение — вернуться к шагу 2, не
   пересоздавая работающие сервисы.

4. Только после успешной валидации пересоздать обоих потребителей настройки:

```bash
$COMPOSE up -d --force-recreate \
  altegio-easyweek-inbox-worker altegio-outbox-worker
```

   Inbox worker стартует с `notifications=true` и валидным allowlist, поэтому
   `processing_is_configured()` снова истинно: сохранённые `captured` события
   штатно берутся в работу и **создают** lifecycle jobs.

5. Подтвердить результат восстановления. Проверка двухуровневая: уровень A
   разбирает исторический backlog, уровень B доказывает, что pipeline снова
   создаёт jobs. Уровень B обязателен — только он является положительным
   доказательством.

##### Уровень A — исторический backlog, по конкретной доставке

Вопрос уровня A звучит не «есть ли у этой записи хоть какая-нибудь job», а
«создала ли **эта доставка** свою собственную job». Разница принципиальна:
у записи почти всегда уже есть более старая job от `booking-created`, и связь
через `record_id` покажет её вместо потерянного `booking-updated`.

Идентичность доставки — это `easyweek_job_dedupe_key()`: SHA-256 от
`event_hint | booking_uuid | payload_hash`. Аудит **импортирует эту
production-функцию**, а не повторяет формулу в SQL: иначе при будущей смене
формата ключа аудит продолжил бы показывать зелёный по устаревшей формуле.

```bash
$COMPOSE run --rm --no-deps \
  --entrypoint /app/.venv/bin/python \
  altegio-easyweek-inbox-worker \
  -m altegio_bot.scripts.easyweek_recovery_audit --since '<outage_start>'
```

Команда read-only: только `SELECT`, без `UPDATE`, `DELETE` и replay, без
запуска inbox/outbox loop. Печатает только event id, технические статусы и
агрегаты — ни payload, ни названия категории, ни имени, ни телефона, ни email,
ни секретов, ни самого dedupe key.

Как читать вывод:

| Поле | Что означает |
| --- | --- |
| `lifecycle_delivery_groups` | сколько различных доставок ожидали job. Byte-identical Resend'ы схлопываются в одну группу — у них общий expected key |
| `resend_groups` | сколько из них были повторными доставками. Одна job на такую группу — **успешная дедупликация**, а не потеря |
| `groups_with_exact_job` | сколько групп имеют job именно со своим ключом. Это и есть доказательство создания job |
| `job_status_counts` | статусы найденных jobs |
| `no_event_specific_job_unclassified` | event id без собственной job — **список для разбора, а не вердикт** |
| `non_lifecycle_event_ids` | `booking-succeeded`: терминальный, job не ожидается |
| `unmappable_event_ids` | нераспознанный hint или отсутствующий `booking_uuid` |

**`no_event_specific_job_unclassified` не означает «потеряно».** Отсутствие
своей job законно для:

* terminal business suppression — `category_not_allowed`, `category_missing`,
  `category_ambiguous_multi_service`, `service_count_unproven`;
* post-cancel no-op;
* `already_applied` replay.

И означает реальную потерю, если доставка была обработана с выключенными
notifications. Различить эти случаи по текущему `Record.raw` **нельзя**: более
поздняя доставка могла перезаписать snapshot, против которого решение
принималось в тот момент. Допустимые доказательства suppression — сохранённая
стабильная runtime-причина именно для этого event id (см. подсчёт `reason=` в
логах выше) либо контролируемый сценарий с заранее известным payload contract.

Поэтому список требует ручного разбора по event id. Ни ноль, ни ненулевое
значение сами по себе не закрывают восстановление.

##### Уровень B — контролируемый smoke, обязательное положительное доказательство

Уровень A показывает, что не потерялось. Что pipeline **снова работает**,
доказывает только новая доставка, создавшая новую job после восстановления.

> **Resend запрещён как положительный smoke.** Byte-identical Resend сохраняет
> `event_hint`, `booking_uuid` и `payload_hash`, а значит и тот же expected
> `easyweek_job_dedupe_key()`. Если исходная доставка создала job ещё до
> outage, аудит найдёт именно её: `exact_jobs=1` при том, что inbox worker мог
> вообще не обработать новый event. Для исторического уровня A такая
> группировка корректна — несколько byte-identical доставок образуют одну
> delivery group, и одна job на группу означает успешную дедупликацию. Для
> уровня B она бесполезна: доказывать надо создание **новой** job сейчас.
> Поэтому smoke делается только на новой business identity. Update или
> reschedule существующей booking тоже не подходит как основной вариант —
> новая booking с новым `booking_uuid` даёт более простое доказательство того,
> что старой exact job существовать не могло.

1. Зафиксировать baseline **до** создания smoke-записей:

```sql
SELECT COALESCE(MAX(id), 0) AS smoke_event_id_baseline
FROM easyweek_events;
```

   Записать результат как `<baseline_event_id>` и текущий UTC-момент как
   `<smoke_start>`. Payload, категория, UUID, телефон и email не выводятся.

2. Создать **новую** allowed single-service booking категории
   `Wimpernverlängerung`: новая запись, новый `booking_uuid`, новый
   `EasyWeekEvent`. Записать её id как `<allowed_event_id>`.
3. Создать **новую** disallowed single-service booking другой категории —
   отдельная booking с собственным новым `booking_uuid`. Записать её id как
   `<disallowed_event_id>`.
4. Проверить обе доставки одной read-only командой:

```bash
$COMPOSE run --rm --no-deps \
  --entrypoint /app/.venv/bin/python \
  altegio-easyweek-inbox-worker \
  -m altegio_bot.scripts.easyweek_recovery_audit \
  --baseline-event-id <baseline_event_id> \
  --smoke-start '<smoke_start>' \
  --smoke-event-id <allowed_event_id> \
  --smoke-event-id <disallowed_event_id>
```

   Доказательства свежести, общие для обеих записей: `distinct_bookings=2`
   (allowed и disallowed используют разные новые booking), у каждой
   `newer_than_baseline=true` и `booking_first_seen_here=true`. Последнее прямо
   исключает Resend: у повторной доставки существует более ранний event с тем
   же `booking_uuid`.

   `<allowed_event_id>` обязан дать `event_status=processed`, `exact_jobs=1`,
   `job_created_after_smoke_start=true`, `job_type_matches_event=true`,
   `job_company_matches_record=true`, `job_record_matches_booking=true`,
   объяснимый `job_statuses`, `outbox_rows=1` и
   `outbox_delivery_proven=true`. Новая exact job, созданная после
   `<smoke_start>`, доказывает, что planner и фильтр снова работают; успешную
   отправку доказывает отдельно статус Outbox — единственный из `delivered` или
   `read`. `outbox_rows` сам по себе доказательством отправки не
   является: строка существует и в `queued`, и в `sent`, и в `failed`. `sent`
   означает лишь приём сообщения на стороне Meta — после него ещё может прийти
   `failed`, поэтому он тоже `pending`. `queued`/`sending`/`sent`
   (`outbox_outcome=pending`) — повторить audit позже; `failed`/`unknown` или
   более одной строки (`not_green`) — **STOP**, затем новая контролируемая
   booking с новым UUID.

   `<disallowed_event_id>` обязан дать `event_status=processed`, непустой
   `record_id`, `exact_jobs=0`, `outbox_rows=0` и `outbox_outcome=none`
   (ожидаемый результат, а не ошибка); Meta и Chatwoot не вызваны.

   Gate привязан к этим двум конкретным event ID. Общий
   `groups_with_exact_job` из уровня A главным gate быть не может: при живом
   трафике в окно попадают чужие события.

#### Staged fence, если inbox worker нужно остановить

Иногда конвейер надо остановить жёстче, чем это делает невалидный allowlist, —
например, когда правится не только allowlist. Тогда fence — это
`EASYWEEK_PROCESSING_ENABLED=false`, а **не** notifications.

1. `EASYWEEK_PROCESSING_ENABLED=false` в `easyweek.env`.
2. Force-recreate **только** inbox worker, чтобы он гарантированно перестал
   claim'ить:

```bash
$COMPOSE up -d --force-recreate altegio-easyweek-inbox-worker
```

3. `EASYWEEK_NOTIFICATIONS_ENABLED` **остаётся `true`** и на этом шаге не
   трогается.
4. Исправить allowlist и проверить его одноразовым контейнером из шага 3
   штатного восстановления.
5. Пересоздать потребителей с валидной конфигурацией:

```bash
$COMPOSE up -d --force-recreate \
  altegio-easyweek-inbox-worker altegio-outbox-worker
```

6. Вернуть `EASYWEEK_PROCESSING_ENABLED=true`.
7. Ещё раз force-recreate inbox worker:

```bash
$COMPOSE up -d --force-recreate altegio-easyweek-inbox-worker
```

**Инвариант обоих вариантов:** при наличии captured backlog не должно
существовать ни одного окна, в котором inbox worker работает с
`EASYWEEK_PROCESSING_ENABLED=true` и `EASYWEEK_NOTIFICATIONS_ENABLED=false`.
Именно эта пара молча превращает captured события в `processed` без jobs.

#### Domain-only drain — НЕОБРАТИМАЯ операция, не восстановление

`EASYWEEK_PROCESSING_ENABLED=true` вместе с
`EASYWEEK_NOTIFICATIONS_ENABLED=false` действительно прогоняет captured backlog
и обновляет `Client`/`Record`. Но lifecycle jobs при этом не создаются, а
события становятся терминальными. **Это не безопасное восстановление и не
подготовительный шаг к нему.**

Операция допустима, только если оператор осознанно решил **отказаться** от
клиентских уведомлений за этот период — например, backlog устарел настолько,
что подтверждение записи, которая уже прошла, только запутает клиента.

Прежде чем её выполнять, обязательно:

1. Подсчитать затрагиваемый backlog — сколько уведомлений будет потеряно:

```sql
SELECT count(*) AS captured_backlog
FROM easyweek_events
WHERE status = 'captured';
```

2. Явно подтвердить отказ от этих уведомлений — с тем, кто отвечает за
   клиентскую коммуникацию, а не в одиночку.
3. Понимать, что **автоматического replay после `processed` не существует**:
   ни исправление allowlist, ни повторное включение notifications, ни
   пересоздание воркеров эти уведомления не вернут. Единственный способ —
   ручная работа с клиентами.

Если хотя бы один из трёх пунктов не выполнен — используйте штатное
восстановление выше.

Отдельный production follow-up до любого ослабления count gate: создать одну
контролируемую смешанную multi-service booking, сохранить безопасный реальный
webhook capture для анализа и подтвердить отсутствие job/Outbox/send. Не
выводить payload/category/PII в консоль и не включать поддержку multi-service в
этом rollout; её семантика принимается только отдельным будущим решением.

Последняя проверка log hygiene — ожидается `0`; названия категорий, услуг,
payload и PII в логах не допускаются:

```bash
$COMPOSE logs --since='<smoke_start>' altegio-easyweek-inbox-worker altegio-outbox-worker \
  | grep -Eci 'service_category|service_name|customer_phone|customer_email|booking_page|Authorization: Bearer|token=|Traceback'
```

Любой traceback, утечка, job/Outbox для disallowed записи или отсутствие
job/Outbox для allowed записи — **STOP** до PR-8.

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

> **Это domain-only drain — необратимая операция, а не подготовительный шаг.**
> Пара `processing=true` + `notifications=false` прогоняет captured backlog,
> обновляя `Client`/`Record`, но делает события терминальными **без** lifecycle
> jobs, и автоматического replay после этого не существует. Шаг допустим
> только здесь, при первичной активации, когда backlog накоплен до запуска
> интеграции и клиентских уведомлений за него никто не ждёт. Прежде чем его
> выполнять, обязательно выполните все три требования из «Domain-only drain —
> НЕОБРАТИМАЯ операция» (§6A): подсчитать `captured_backlog`, явно подтвердить
> отказ от этих уведомлений с ответственным за клиентскую коммуникацию и
> понимать, что вернуть их будет нельзя. Если backlog содержит записи, по
> которым клиент ждёт подтверждения, — этот шаг пропускается: держите
> `EASYWEEK_PROCESSING_ENABLED=false` при `EASYWEEK_NOTIFICATIONS_ENABLED=true`
> до §7.1.3, как это делает штатный rollout §6A.

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

### Chatwoot smoke-матрица

**Outbound mirror:** отправьте безопасные тестовые lifecycle notifications для
Durlach, Rastatt и контрольную для Karlsruhe. Проверьте, что private note и
conversation появились только в DU, RA и KA inbox соответственно. Для каждого
проверьте `conversation.inbox_id`, branch footer и отсутствие копии в двух
остальных branch inbox. Один и тот же test phone для DU и RA обязан создать или
использовать две разные conversations — по одной на inbox.

**Inbound reply и reaction:** ответьте в WhatsApp через reply context сначала
на DU lifecycle notification, затем тем же test phone на RA notification. Reply
и reaction на DU обязаны остаться в DU conversation/inbox, на RA — в RA;
cross-inbox native `in_reply_to` запрещён. Повторите контроль для KA. Новое
сообщение или reaction без найденного target остаётся в General / Unassigned,
но найденный authoritative target с missing/invalid tenant route обязан
fail-closed без fallback в General.

**Operator relay:** из каждого branch inbox ответьте на тестовый номер.
Durlach должен выбрать Durlach EasyWeek company/sender, Rastatt — текущий
Rastatt EasyWeek company/sender, Karlsruhe — Karlsruhe Altegio company/sender.
Общий `phone_number_id` не разрешает выбирать sender без inbox mapping;
cross-provider/company selection блокирует rollout.

**General inbound:** отправьте совершенно новое клиентское сообщение без reply
context. Оно обязано появиться только в `General / Unassigned`, не в DU/RA/KA.
Филиал не должен угадываться по телефону.

Любое cross-inbox попадание, повторное использование conversation другого
inbox или cross-company sender selection — **STOP** до продолжения rollout.

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

Логи потребителей не должны содержать PII, raw map и секреты:

```bash
$COMPOSE logs --since=1h altegio-outbox-worker altegio-whatsapp-inbox-worker altegio-easyweek-inbox-worker \
  | grep -Eci 'customer_phone|customer_email|Authorization: Bearer|token=|CHATWOOT_INBOX_COMPANY_MAP=|Traceback|gaierror'
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
$COMPOSE stop altegio-outbox-worker
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
$COMPOSE up -d altegio-outbox-worker
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
$COMPOSE up -d --force-recreate altegio-whatsapp-inbox-worker
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
$COMPOSE up -d altegio-outbox-worker
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
$COMPOSE up -d --force-recreate altegio-whatsapp-inbox-worker
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

---

## 10. PR-7.2 — Chatwoot affinity routing и обратимый one-inbox UX

Три технических inbox (Karlsruhe, Durlach, Rastatt) и отдельный validated
General остаются раздельными. Ничего не объединяется физически.

### 10.1 Режимы

`CHATWOOT_INBOUND_ROUTING_MODE` — ровно одно из трёх значений. Невалидное
значение роняет запуск settings; тихого fallback нет.

| Режим | Клиентский inbound | Ответ оператора из General |
| --- | --- | --- |
| `context` (default) | reply/reaction context решает, иначе General | заблокирован, как сегодня |
| `affinity` | context, затем доказанная affinity; только настоящий NO_EVIDENCE → General | разрешён при PROVEN tenant |
| `general` | всё показывается в General | разрешён при PROVEN tenant |

`general` — это режим **отображения**. Он не ослабляет доказательство, которое
нужно ответу оператора, и никогда не означает «взять первый sender».

Порядок доказательств: последняя доставленная (`delivered`/`read`) tenant-
коммуникация → ближайшая будущая запись клиента, иначе последняя прошедшая →
единственная provider/company identity клиента. `AMBIGUOUS` и `INVALID`
блокируют, а не уходят в General.

Единственный потребитель настройки — `altegio-whatsapp-inbox-worker`.

### 10.2 Переключение и rollback

```bash
$COMPOSE up -d --force-recreate altegio-whatsapp-inbox-worker
```

Обычный `restart` или `up -d` без `--force-recreate` **не** перечитывает `.env`.
API, outbox и EasyWeek inbox worker пересоздавать не нужно: настройку читает
только WhatsApp inbox worker.

Проверка эффективного значения без вывода секретов:

```bash
$COMPOSE exec -T altegio-whatsapp-inbox-worker /app/.venv/bin/python -c \
  'from altegio_bot.settings import settings; print({"mode": settings.chatwoot_inbound_routing_mode})'
```

Активация: `CHATWOOT_INBOUND_ROUTING_MODE=affinity` → recreate.
Откат в один inbox: `CHATWOOT_INBOUND_ROUTING_MODE=general` → recreate.
Возврат к текущему поведению: `CHATWOOT_INBOUND_ROUTING_MODE=context` → recreate.

Откат не требует ни отката commit, ни downgrade миграции, не меняет EasyWeek
processing/notifications и не трогает outbound branch notifications.

### 10.3 Оператор на iPhone

Оператор добавлен во все четыре inbox (Karlsruhe, Durlach, Rastatt, General) и
работает из общего списка **All** или **Mine**. Переключать филиальные inbox
вручную не нужно: название inbox в списке показывает филиал, фильтр по inbox
нужен только для поиска, push открывает нужную conversation.

### 10.4 Smoke после deploy

Выводить только IDs, provider/company, inbox, статусы, reason codes и booleans.
Не выводить `phone_e164`, тело сообщения, payload, token и secret.

**A. `affinity`.** Для контакта с недавним Durlach-уведомлением новое сообщение
без Reply должно появиться в Durlach inbox; аналогично Rastatt и Karlsruhe.
Ответ оператора из branch inbox уходит своим sender; ответ из General для
контакта с proven affinity — тоже своим. Outbox доходит до `delivered`/`read`.

**B. Неизвестный контакт.** Новый номер без Client/Record/коммуникации →
General. Ответ из General заблокирован, Meta не вызывается, оператор видит
private note.

**C. Ambiguity.** Подготовленный конфликтующий tenant evidence → blocked, Meta
не вызвана, произвольный sender не выбран.

**D. `general` rollback rehearsal.** Выставить `general`, пересоздать только
WhatsApp inbox worker: новое inbound появляется в General, branch conversation
не переиспользуется, ответ из General с proven affinity уходит правильным
branch sender. Затем вернуть `affinity` и снова пересоздать worker.

Для каждого шага проверить `conversation.inbox_id`, `WhatsAppEvent.status`,
`forwarded_chatwoot_conversation_id`, отсутствие cross-inbox дубля и traceback.

```sql
SELECT id, status, error, chatwoot_conversation_id
FROM whatsapp_events
ORDER BY id DESC
LIMIT 10;
```

**STOP-условия:** sender другого филиала или другого provider; сообщение,
ушедшее клиенту из чужого салона; появление conversation в двух inbox;
`general_affinity_ambiguous` / `general_affinity_invalid`, трактованные как
норма.

**Событие 20794 (conversation 230, message 9343) терминально и не
переигрывается.** Проверка выполняется новым сообщением с новым Chatwoot
message ID.


---

## 11. PR-7.3 — корректный контракт цены EasyWeek

### 11.1 Что было сломано

Для реальной цены `120.00 €` EasyWeek присылает четвёрку полей:

```text
booking_price_int:       120        # МАЖОРНЫЕ единицы, не центы
booking_price:           "12000"    # storage format: точные минорные единицы
booking_price_float:     "120.00"   # мажорная проекция
booking_price_formatted: "€120.00"  # локализованный display text
```

Старый normalizer делил `booking_price_int` на 100 и сохранял `1.20`. То есть
любая EasyWeek-запись, созданная до PR-7.3, держит цену в сто раз меньше
реальной — и в `records.total_cost`, и в `record_services.cost_to_pay`.
Клиентский шаблон рендерится именно из service-строки, поэтому дефект дошёл бы
до клиента.

### 11.2 Действующий контракт парсера

- **authoritative** — `booking_price`. Только строка из одних цифр; парсится
  целочисленной арифметикой. Запятая, точка, валюта, экспонента, пробелы,
  JSON-число и `bool` отклоняются как `invalid_payload`; отрицательное значение
  и выход за `Numeric(12, 2)` — как `invalid_numeric_range`.
- **cross-check** — `booking_price_float`. Если поле пришло, оно обязано
  описывать ту же сумму; расхождение — `price_fields_conflict`.
- **никогда не источник** — `booking_price_formatted` и `booking_price_int`.
- Присутствие цены определяется ключом `booking_price`: поля нет → цена
  не менялась; `booking_price: null` при непротиворечивых остальных полях →
  цена очищается; `"0"` → настоящий ноль.
- Ни один код ошибки не содержит саму сумму.

### 11.3 Rollout: порядок обязателен

Флаг остаётся выключенным до конца шага 8.

**1. Подтвердить effective config ДО deploy.** Значение читается из процесса,
не из файла:

```bash
docker compose -p altegio_bot exec -T altegio-api /app/.venv/bin/python -c 'from altegio_bot.settings import settings; print("notifications_enabled=", settings.easyweek_notifications_enabled)'
```

Требуется `notifications_enabled= False`. Если `True` — выставить в
`easyweek.env` `EASYWEEK_NOTIFICATIONS_ENABLED=false`, пересоздать сервисы и
повторить проверку. С известной ошибкой цены включённые уведомления
запрещены.

**2. Deploy исправления** обычным порядком по разделу 4. Сам deploy ничего не
чинит в данных: он только ставит правильный парсер.

**3. Read-only audit.** Ничего не меняет:

```bash
docker compose -p altegio_bot exec -T altegio-api /app/.venv/bin/python -m altegio_bot.scripts.easyweek_price_repair
```

Вывод — только счётчики и технические ID:

```text
{'mode': 'dry-run', 'scanned': N, 'repairable': K, 'repaired': 0,
 'skipped': {...}, 'repairable_record_ids': [...],
 'evidence_event_ids': [...]}
```

`repairable` — строки, у которых доказана сигнатура именно этого бага. Всё
остальное попадает в `skipped` с причиной и **не будет** тронуто:

| Причина | Что означает |
| --- | --- |
| `no_booking_uuid` | нет канонической identity |
| `no_usable_evidence` | нет доставки, которая вообще могла записать цену (см. 11.3a) |
| `not_exactly_one_service` | ноль или несколько service-строк: сумму не к чему привязать |
| `inconsistent_service_snapshot` | `total_cost` и `cost_to_pay` уже расходятся — отдельная проблема |
| `already_correct` | цена уже правильная |
| `legacy_signature_mismatch` | сохранённое значение не равно результату старой формулы: значение писал кто-то ещё |
| `ambiguous_evidence` | несколько допустимых доставок одинаково объясняют сохранённое значение, но называют разные цены |

#### 11.3a Какие события допускаются как evidence

Кандидатом считается доставка этой же booking UUID, которая:

- имеет `status='processed'` — `captured`/`received`/`processing` ещё не
  доработали, а `failed` не записывал вообще ничего;
- имеет lifecycle-хинт (`booking-created`, `booking-updated`,
  `booking-rescheduled`, `booking-canceled`). `booking-succeeded` исключён:
  normalizer возвращает по нему `None`, и worker помечает событие `processed`,
  не тронув Client, Record и MessageJob;
- не truncated;
- разбирается исправленным парсером и реально несёт цену.

**`processed` не означает «записал».** Worker помечает доставку `processed` и
выходит без domain write ещё в трёх случаях: `booking-succeeded`, точный replay
(`already_applied` — Resend байт-идентичен) и update, пришедший после отмены
записи (`is_cancel_terminal`). Схема не хранит, какая именно доставка выполнила
запись, и repair этого не выдумывает.

Поэтому решение принимается не по одной доставке:

1. собираются **все** кандидаты;
2. остаются те, у которых старая формула воспроизводит текущее сохранённое
   `Record.total_cost`;
3. если таких нет — `legacy_signature_mismatch`;
4. если все они называют одну и ту же исправленную цену — repair разрешён;
5. если называют разные — `ambiguous_evidence`, строка не меняется.

Это не теория: `booking_price_int` считает целые евро, поэтому `120.00` и
`120.50` дают одну и ту же старую сигнатуру `1.20`. Выбор «самого нового
события» записал бы `120.50` там, где верно `120.00`. `event.id` используется
только как стабильная метка в отчёте и **никогда** не выбирает сумму.

Если `ambiguous_evidence` встречается, разбирать такие booking поимённо по
`repairable_record_ids`/`evidence_event_ids` и `easyweek_events`; массового
автоматического решения для них нет и не будет.

**4. Backup / фиксация восстановимых значений.** До `--apply` сохранить текущее
состояние кандидатов, чтобы откат был арифметически проверяем:

```bash
docker compose -p altegio_bot exec -T postgres sh -lc 'psql -tAX -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "COPY (SELECT r.id, r.total_cost, rs.id, rs.cost_to_pay FROM records r JOIN record_services rs ON rs.record_id = r.id WHERE r.provider = '"'"'easyweek'"'"') TO STDOUT WITH CSV" ' > easyweek_prices_before.csv
```

Этот CSV содержит суммы: хранить как обычный production dump, не пересылать в
чаты и не прикладывать к тикетам.

**5. Explicit repair.** Только после успешного audit:

```bash
docker compose -p altegio_bot exec -T altegio-api /app/.venv/bin/python -m altegio_bot.scripts.easyweek_price_repair --apply
```

Ожидается `repaired == repairable` из шага 3, либо меньше — если между audit и
apply worker успел записать новые корректные цены (см. 11.3b). Команда не
создаёт и не переоткрывает job, ничего не отправляет и не меняет статус
`easyweek_events`.

**6. Проверка идемпотентности.** Повторный `--apply` обязан дать
`repaired: 0` и `already_correct` на тех же строках.

#### 11.3b Concurrency: останавливать ли обработку

**Останавливать EasyWeek inbox worker не требуется.** `EASYWEEK_PROCESSING_ENABLED`
может оставаться `true`: корректность обеспечена на уровне строк, а не режимом
обслуживания.

- audit (`dry-run`) **не берёт write-блокировок** вообще — его безопасно
  запускать на нагруженной БД, он не может заблокировать worker;
- `--apply` берёт `SELECT ... FOR UPDATE` на `records`, затем на связанные
  `record_services` — тот же порядок блокировок, что и у `upsert_record`, —
  и **заново** собирает evidence и переклассифицирует строку уже под
  блокировкой;
- значение, вычисленное до блокировки, не записывается никогда;
- обход — read-only keyset-страница по `records.id`, дальше по одной короткой
  транзакции на запись, поэтому долгих удержаний блокировок нет.

Практический смысл: если worker успел сохранить новую корректную цену, пока
repair ждал блокировку, сохранённое значение перестаёт соответствовать старой
сигнатуре, и строка уходит в `legacy_signature_mismatch` вместо отката к
историческому значению. Более новый business update не теряется ни при одном
порядке событий.

Что при этом обязано остаться выключенным до конца шага 8 — это
**`EASYWEEK_NOTIFICATIONS_ENABLED`**. Обработка и capture безопасны; отправка
клиентам при известной ошибке цены — нет.

**7. Проверка инварианта.** Ожидается **0 строк**:

```bash
docker compose -p altegio_bot exec -T postgres sh -lc 'psql -tAX -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT r.id FROM records r JOIN record_services rs ON rs.record_id = r.id WHERE r.provider = '"'"'easyweek'"'"' AND r.total_cost IS DISTINCT FROM rs.cost_to_pay"'
```

Дополнительно — что не осталось «подозрительно мелких» цен:

```bash
docker compose -p altegio_bot exec -T postgres sh -lc 'psql -tAX -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT count(*) FROM records WHERE provider = '"'"'easyweek'"'"' AND total_cost > 0 AND total_cost < 1"'
```

Ненулевой результат — не автоматический откат, а повод разобрать эти строки
поимённо: настоящая цена в 0.50 € возможна.

**7a. Повторный audit после repair.** Тот же dry-run, что и в шаге 3:

```bash
docker compose -p altegio_bot exec -T altegio-api /app/.venv/bin/python -m altegio_bot.scripts.easyweek_price_repair
```

Ожидается `repairable: 0`, а исправленные строки — в `already_correct`.
Ненулевой `repairable` означает, что появились новые кандидаты (например, worker
обработал старое событие), и шаги 4–7 нужно повторить.

**7b. Подтвердить, что jobs/outbox/events не изменились.** Repair их не трогает,
и это проверяется прямо: счётчики до и после должны совпадать, а `updated_at`
джобов не должен сдвинуться в окно repair.

```bash
docker compose -p altegio_bot exec -T postgres sh -lc 'psql -tAX -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT (SELECT count(*) FROM message_jobs WHERE provider = '"'"'easyweek'"'"'), (SELECT count(*) FROM outbox_messages), (SELECT count(*) FROM easyweek_events), (SELECT count(*) FROM easyweek_events WHERE status = '"'"'failed'"'"')"'
```

**8. Controlled smoke.** Только теперь и только на собственном тестовом
клиенте:

- выставить `EASYWEEK_NOTIFICATIONS_ENABLED=true`, пересоздать consumers;
- создать **новую** тестовую booking с известной ценой (или реально изменить
  цену существующей), чтобы EasyWeek прислал новый payload;
- проверить цепочку `easyweek_events` → конкретный `message_jobs` →
  `outbox_messages` → `delivered`/`read`;
- убедиться, что в доставленном шаблоне стоит правильная сумма.

**Почему старое событие не является smoke.** Уже обработанное событие
терминально: worker его не переигрывает, нового job не создаст, и «зелёный»
результат будет означать лишь, что строка осталась в прежнем статусе.

**Почему Resend не является доказательством.** Байт-идентичная повторная
доставка даёт тот же `payload_hash` и тот же dedupe key, поэтому она сознательно
дедуплицируется. Это проверяет дедупликацию, а не новый pipeline. Нужен новый
бизнес-факт: новая booking либо реальное изменение цены.

### 11.4 Откат

Если на любом шаге результат расходится с ожидаемым, первым действием выключить
уведомления: `EASYWEEK_NOTIFICATIONS_ENABLED=false` в `easyweek.env`, затем
пересоздать consumers и повторить проверку effective config из шага 1. Отправки
прекращаются; capture и запись `easyweek_events` продолжаются, ничего не
теряется.

Откат самих данных выполняется из CSV шага 4 поимённо, по `records.id` и
`record_services.id`. Массовый `UPDATE` по эвристике «умножить на 100»
запрещён: после repair в таблице сосуществуют исправленные и изначально
корректные строки, и такой запрос испортил бы вторые.


---

## 12. PR-8 — reminders, обязательный API guard и preflight

### 12.1 Что добавлено

Два клиентских уведомления: `reminder_24h` и `reminder_2h`. Отличие от
lifecycle принципиальное — reminder планируется за часы или сутки и срабатывает
по времени, поэтому всё, что его обосновывало, к моменту отправки может стать
неправдой без вебхука, который мы видели.

Поэтому перед **каждым** Meta attempt выполняется read-only
`GET /bookings/{uuid}`, и отправка разрешена только если одновременно доказано:
booking существует, тот же UUID, тот же филиал, то же время, `is_canceled` и
`is_completed` — настоящие `false`.

### 12.2 Два флага и почему их два

| Флаг | Что делает | Читает сервис |
| --- | --- | --- |
| `EASYWEEK_REMINDERS_ENABLED` | только СОЗДАНИЕ reminder jobs | `altegio-easyweek-inbox-worker` |
| `EASYWEEK_REMINDER_API_GUARD_ENABLED` | send fence | `altegio-outbox-worker` |

Планирование дополнительно требует `EASYWEEK_NOTIFICATIONS_ENABLED=true`:
reminder — клиентское уведомление, мастер-флаг вторым флагом не обходится.

При закрытом fence reminder jobs **вообще не claim'ятся**: остаются `queued`,
не тратят attempts, сохраняют `run_at`. Это и есть состояние, которое читает
preflight. Altegio jobs и EasyWeek lifecycle jobs не затронуты.

`true` не отключает guard — он разрешает обработку, в которой guard обязателен
всегда. Режима «отправить reminder без проверки API» не существует.

`docker compose restart` не перечитывает `env_file`. Нужен
`up -d --force-recreate <сервис>`.

### 12.3 Rollout — порядок обязателен

**1. Deploy с обоими флагами `false`.**

**2. Проверить effective settings внутри обоих контейнеров:**

```bash
docker compose -p altegio_bot exec -T altegio-easyweek-inbox-worker /app/.venv/bin/python -c 'from altegio_bot.settings import settings; print("reminders=", settings.easyweek_reminders_enabled, "notifications=", settings.easyweek_notifications_enabled)'
```

```bash
docker compose -p altegio_bot exec -T altegio-outbox-worker /app/.venv/bin/python -c 'from altegio_bot.settings import settings; print("guard=", settings.easyweek_reminder_api_guard_enabled)'
```

**3. Проверить API key/workspace/registry read-only probe:**

```bash
docker compose -p altegio_bot exec -T altegio-outbox-worker /app/.venv/bin/python -m altegio_bot.scripts.easyweek_probe
```

**4. Проверить, что reminder Meta templates существуют и APPROVED** для каждого
филиала: `kitilash_du_reminder_24h_v1`, `kitilash_du_reminder_2h_v1`,
`kitilash_ra_reminder_24h_v1`, `kitilash_ra_reminder_2h_v1`. Отсутствующий или
неодобренный шаблон блокирует rollout — включать флаги нельзя.

**5. Идемпотентный seed DB-строк шаблонов:**

```bash
docker compose -p altegio_bot exec -T altegio-outbox-worker /app/.venv/bin/python -m altegio_bot.scripts.seed_easyweek_templates
```

Ожидается 6 строк на филиал (4 lifecycle + 2 reminder). Повтор — 0 created.

**6. API guard остаётся `false`.**

**7. Включить планирование** — только после шагов 4–5:
`EASYWEEK_REMINDERS_ENABLED=true` в `easyweek.env`, затем

```bash
docker compose -p altegio_bot up -d --force-recreate altegio-easyweek-inbox-worker
```

**8. Создать контролируемую тестовую booking** собственного тестового клиента:
разрешённая категория, одна услуга, начало достаточно далеко в будущем (больше
суток), чтобы появились обе reminder job.

**9. Проверить очередь в PostgreSQL:**

```bash
docker compose -p altegio_bot exec -T postgres sh -lc 'psql -tAX -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT id, company_id, job_type, status, run_at, payload->>'"'"'record_starts_at'"'"' FROM message_jobs WHERE provider = '"'"'easyweek'"'"' AND job_type IN ('"'"'reminder_24h'"'"','"'"'reminder_2h'"'"') ORDER BY run_at"'
```

Требуется: `provider=easyweek`, правильный `company_id`, оба job_type,
`status=queued`, `run_at` = старт минус 24ч и минус 2ч, `record_starts_at`
совпадает с реальным началом, дублей нет.

**10. Read-only preflight:**

```bash
docker compose -p altegio_bot exec -T altegio-outbox-worker /app/.venv/bin/python -m altegio_bot.scripts.easyweek_reminder_preflight
```

**11. Требуется:** `candidate_count > 0`, `truncated=false`, `ready=true`,
`outcomes = {'proven_current': N}` и exit code 0. Любой другой outcome —
**STOP**, fence не открывать.

**12. Только теперь открыть fence:** `EASYWEEK_REMINDER_API_GUARD_ENABLED=true`,
затем

```bash
docker compose -p altegio_bot up -d --force-recreate altegio-outbox-worker
```

**13. Controlled smoke.** Дождаться реального reminder job:

- guard прошёл;
- Meta HTTP 200;
- Outbox `sent` → `delivered`/`read`;
- правильный branch template и sender;
- клиент получил сообщение.

**14. Negative smoke без порчи production data.** До срабатывания второго
reminder перенести либо отменить **свою тестовую** booking. Ожидается: guard
локально отменяет reminder (`status=canceled`, `last_error` начинается с
`easyweek_reminder_guard:`), Meta не вызывается, Outbox-строка не создаётся.

### 12.4 Как читать отказ guard

`last_error` содержит короткий стабильный код без данных клиента:

| Outcome | Что означает | Поведение job |
| --- | --- | --- |
| `proven_current` | всё доказано | отправка |
| `retryable_unavailable` | 429/5xx/timeout/сеть | queued, backoff, attempts не тратятся |
| `configuration_unavailable` | нет API key/slug, 401/403 | queued до reminder deadline; preflight красный |
| `not_found` | 404 | локальная отмена, Meta не вызывается |
| `identity_mismatch` | UUID/provider/company/record не совпали | локальная отмена |
| `location_mismatch` | филиал не тот | локальная отмена |
| `start_time_mismatch` | время изменилось | локальная отмена |
| `canceled` / `completed` | booking отменена или завершена | локальная отмена |
| `malformed_response` | поле отсутствует, не bool, naive timestamp, противоречивый `status.type` | локальная отмена |
| `permanent_error` | прочий permanent 4xx | локальная отмена |

### 12.4a Deadline: current booking ≠ отправляемый reminder

Reminder подчиняется тому же delivery deadline, что и остальные job, **включая
первую отправку**:

- `reminder_24h` — `min(starts_at - 3ч, run_at + 6ч)`;
- `reminder_2h` — `starts_at - 15м`.

Практический смысл при закрытом fence: job может пролежать `queued` дольше
своего окна. Booking при этом остаётся полностью корректным — тот же UUID,
филиал, время, не отменён — но момент, когда напоминание имело смысл, прошёл.

Поэтому:

- **preflight красный** для такой job: отдельный outcome `deadline_expired`,
  `ready=false`, ненулевой exit code. EasyWeek API на неё не тратится —
  просрочка доказывается локально;
- **runtime отменяет** job локально: `status=canceled`, короткий стабильный
  `last_error`, Meta не вызывается, строка Outbox не создаётся.

Если preflight показал `deadline_expired`, fence открывать **нельзя** до
разбора: это backlog, который нельзя досылать. Отдельного режима «доставить
просроченное» нет и не планируется.

Altegio jobs и EasyWeek lifecycle jobs это изменение не затрагивает — их
first-attempt семантика прежняя.

### 12.4b Delivery retry напоминания

Retryable failed callback от Meta создаёт retry обычным механизмом delivery
retry (тот же chain, тот же `delivery_retry:<root>:<attempt>`, тот же бюджет
попыток). Для reminder дополнительно:

- retry наследует из **корневой** job два значения: canonical `booking_uuid`
  и `record_starts_at` — исходный запланированный старт;
- `record_starts_at` **не** перечитывается из текущего `Record`. Если между
  отправкой и callback запись перенесли, retry сохранит старое время, и это
  расхождение будет поймано локально — Meta не вызовется;
- каждый retry заново проходит read-only API guard. Положительный результат
  предыдущей попытки не кэшируется и не наследуется;
- если у корневой job нет доказуемых `booking_uuid`/`record_starts_at`, retry
  **не создаётся вообще**; в `outbox_messages.meta` пишется
  `delivery_retry_skip_reason=easyweek_reminder_retry_identity_unproven`.

### 12.5 Rollback

Первым действием — закрыть fence:

`EASYWEEK_REMINDER_API_GUARD_ENABLED=false`, затем

```bash
docker compose -p altegio_bot up -d --force-recreate altegio-outbox-worker
```

Reminder jobs остаются `queued` и не отправляются. При необходимости следом
`EASYWEEK_REMINDERS_ENABLED=false` и force-recreate
`altegio-easyweek-inbox-worker` — новые reminders перестанут планироваться.

Capture, lifecycle notifications, Chatwoot и Altegio при обоих шагах
продолжают работать без изменений.

**`booking-succeeded` в PR-8 не включается** — он нужен последующей
review/visit-counter фазе.
