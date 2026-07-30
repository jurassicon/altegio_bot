# EasyWeek webhook capture — операторский runbook (PR-1)

Research-grade capture доставок вебхуков EasyWeek. Эндпоинт сохраняет **каждую
аутентифицированную доставку** (включая ретраи, Resend и не-JSON тела) и не
делает **никакой** обработки. Цель — по живым данным разобрать структуру payload
и семантику доставок до того, как строить нормализацию и воркер.

Эндпоинт: `POST /webhooks/easyweek` — выключен по умолчанию, аутентификация по
токену в query.

---

## 1. Включение capture

Секреты EasyWeek живут в отдельном файле `easyweek.env` рядом с `.env`. Его
читает только сервис `altegio-api`, файл gitignored и объявлен как
`required: false` — без него сервис поднимается, а capture остаётся закрытым.

```bash
cp easyweek.env.example easyweek.env
```

Сгенерировать секрет:

```bash
python3 -c "import secrets; print(secrets.token_urlsafe(32))"
```

Схему создают ДВЕ ревизии: `8923be993170` (базовая таблица) и `8705ec49cc73`
(колонки `body_raw`/`body_size_bytes`). Вторая аддитивна и идемпотентна, поэтому
`upgrade head` чинит и среду, где успели применить только базовую. Совместимость
покрыта автотестом на одноразовой БД:

```bash
uv run pytest src/altegio_bot/tests/test_easyweek_migration_integration.py
```

Тест требует роль с `CREATEDB`. Если у локальной роли её нет, он пропустится с
подсказкой — тогда прогоните его против одноразового сервера:

```bash
docker run -d --name ew-mig -e POSTGRES_PASSWORD=postgres -p 55433:5432 postgres:16-alpine
```

```bash
ALTEGIO_MIGTEST_DATABASE_URL=postgresql+asyncpg://postgres:postgres@localhost:55433/postgres uv run pytest src/altegio_bot/tests/test_easyweek_migration_integration.py
```

Вписать в `easyweek.env` `EASYWEEK_WEBHOOK_SECRET=<секрет>` и
`EASYWEEK_ENABLED=true`, применить миграцию и пересоздать контейнер:

```bash
docker compose -p altegio_bot --profile ops run --rm migrate
```

```bash
docker compose -p altegio_bot up -d --force-recreate altegio-api
```

Обычный `restart` НЕ перечитывает `env_file` — нужен именно
`--force-recreate`.

Поведение флагов:
- `EASYWEEK_ENABLED` не задан / false → **404** (поверхность скрыта до go-live);
- пустой `EASYWEEK_WEBHOOK_SECRET` → **403** даже при `EASYWEEK_ENABLED=true`
  (fail-closed).

---

## 2. Секрет и логи

Токен передаётся в query string:

```text
https://<host>/webhooks/easyweek?event=booking-created&token=<secret>
```

Поэтому **любой лог, который пишет полный request target, пишет и секрет**.
Ответственность здесь разделена, и это разделение важно не путать.

### 2.1. Логи приложения — гарантировано кодом и тестами

Всё, что контролирует репозиторий, закрыто и покрыто тестами:

| Канал | Что сделано |
|---|---|
| uvicorn access log | запускается с `--no-access-log` (Dockerfile и `command:` в compose) |
| замена ему | `AccessLogMiddleware` в `main.py`: метод, экранированный **путь**, статус, длительность — без query; путь пропущен через `safe_log_path` (защита от log injection) |
| application log | ни payload, ни тело, ни заголовки, ни значения query, ни `event_hint` |
| строки в БД | `mask_query` заменяет значения `token`/`secret`/`key`/… на `***`; `authorization`, `cookie`, `x-altegio-token` не сохраняются |
| диагностика тестов | INFO-лог httpx (пишет URL целиком) приглушён в `conftest.py` |

**Для этих каналов** секрет восстановить из логов нельзя — и это норма, а не
поломка. Если секрет утерян, его не ищут в логах, а ротируют: новый секрет в
`easyweek.env` → `--force-recreate altegio-api` → обновить URL всех вебхуков в
кабинете EasyWeek.

> ВАЖНО: утверждение «секрета нет в логах» относится **только** к логам
> приложения, перечисленным выше. Оно НЕ распространяется на внешний reverse
> proxy — его конфигурация вне репозитория, и её обязан проверить оператор
> (2.2). Пока proxy-логи не проверены, считать секрет защищённым нельзя.

### 2.2. Логи reverse proxy — ПОДТВЕРЖДЁННАЯ утечка, обязательный gate до go-live

> **Статус: утечка подтверждена на production.** Активная проверка на
> `api.kitilash.com` показала, что полный request target вебхука (вместе с
> `token=`) попал в `/var/log/nginx/access.log`. В логах приложения и в
> `docker logs` маркер отсутствовал — значит проблема именно в host-level Nginx
> logging, а не в коде. Безопасный `access_log` закрывает только один канал:
> при upstream connection failure, timeout и некоторых proxy errors Nginx может
> записать полную request line вместе с query string в `error_log`. Пока оба
> канала не закрыты и все проверки ниже не выполнены, go-live блокируется, а
> текущие query-секреты считаются **потенциально раскрытыми**.

Nginx-конфиг живёт вне репозитория, поэтому код за его фактическое применение не
отвечает. Стандартный `log_format combined` содержит `$request` (метод +
**полный URI вместе с query string**) — для этих эндпоинтов он недопустим.
Формат `error_log` не настраивается, поэтому заменить в нём `$request_uri` на
`$uri` невозможно.

Чувствительны **все** webhook-маршруты, а не только EasyWeek:

| Маршрут | Секрет в query |
|---|---|
| `/webhooks/easyweek` | `?event=<trigger>&token=<secret>` |
| `/webhooks/altegio` | `?secret=<secret>` (в части вариантов ещё `userGuid`) |
| `/webhook/whatsapp` | Meta verification: `?hub.mode=…&hub.verify_token=<token>&hub.challenge=…` |

#### Шаг 1. Сделать backup и описать фактически активный routing

Сначала сделать backup активного production-конфига. Production-конфиг, IP,
сертификаты и секреты не копировать в репозиторий.

```bash
sudo nginx -T
```

В выводе найти `server_name api.kitilash.com` и фактические блоки, которые
обслуживают `/webhooks/easyweek`, `/webhooks/altegio` и `/webhook/whatsapp`.
До любых изменений зафиксировать:

- modifier и path каждого `location`, включая regex locations и rewrite rules;
- полную цепочку `external URI → initial location → destination(s)` для
  `rewrite`, `try_files`, `index`, `error_page`, named locations, internal
  redirects и повторного выбора regex locations;
- фактический `proxy_pass` и proxy headers;
- `client_max_body_size` и `client_body_timeout`;
- proxy connect/read/send timeouts;
- rate/connection limiting и allow/deny;
- buffering, retry policy и upstream;
- все эффективные `access_log` и `error_log`, включая унаследованные, в
  начальном location и в каждом достижимом destination.

Нельзя угадывать эту конфигурацию по содержимому репозитория.

#### Шаг 2. Выбрать routing-neutral access logging

Versioned reference теперь разделён на две части:

- `deploy/nginx/kitilash_webhook_log_formats.conf.example` — определения для
  существующего `http` context: безопасный `log_format` и опциональные `map` для
  conditional logging;
- `deploy/nginx/kitilash_webhook_logging.inc.example` — только `access_log` и
  безопасная политика `error_log`; include добавляется исключительно внутрь уже
  существующего production `location` или `server`, который фактически
  обслуживает webhook.

Reference **не создаёт маршруты**. Не копировать и не добавлять из него generic
`location ^~ /webhook`, exact locations или предполагаемый `location /`.
Не менять modifier/path существующего location, `proxy_pass`, headers, limits,
timeouts, access restrictions, buffering, retries или upstream.

Selector и safe log format используют разные URI-переменные с разным
security-контрактом:

- `$uri` изменяется после `rewrite`, `try_files`, `error_page` и других internal
  redirects. Это текущий нормализованный path без query; safe format продолжает
  логировать текущий безопасный `$uri` как `uri=$uri`;
- `$request_uri` сохраняет исходный request URI вместе с query. Он используется
  только как source для boolean `map`, который выдаёт `0` или `1`;
- `$request_uri` никогда не добавляется в `log_format`, `access_log` path,
  headers или diagnostic output. В `log_format kitilash_webhook_safe` по-прежнему
  запрещены `$request`, `$request_uri`, `$args`, `$query_string`, `$is_args`,
  `$http_referer` и произвольные request-заголовки.

**Raw request target ≠ URI, по которому выбирается location.** `$request_uri` —
это то, что прислал клиент, а location Nginx выбирает по **нормализованному** URI
(percent-decoding, свёртка dot segments). Эти две формы расходятся, например:

```text
/%77ebhooks/easyweek?token=<secret>
/webhooks%2Feasyweek?token=<secret>
/foo/../webhooks/easyweek?token=<secret>
```

Такой запрос может обслуживаться webhook-routing, при том что канонический regex
по сырому `$request_uri` его webhook'ом не считает → `$kitilash_is_webhook=0` →
обычный `combined` → `$request` с секретом на диске.

**Не реализовывать нормализацию URI своим regex** — это заведомо неполное
решение. Вместо этого selector содержит два entry:

1. канонический path (`~^/webhooks?(?:/|\?|$)`);
2. fail-safe по известным secret-bearing query keys, независимо от формы path:
   `token`, `secret`, `userGuid`, `hub.verify_token` (case-insensitive, с
   границами `?`/`&` и обязательным `=`).

Список ключей соответствует тому, чем реально аутентифицируются вебхуки этого
проекта (`token` — EasyWeek, `secret` и `userGuid` — Altegio, `hub.verify_token` —
Meta verification). Расширять его без подтверждения в коде/настройках не следует.
`hub.verify_token` перечислен отдельно: граница `?`/`&` не совпала бы с
`…verify_token=`, потому что перед `token` стоит точка.

**Намеренные false positives допустимы.** `/health?token=…` или `/other?secret=…`
тоже уйдут в безопасный webhook-лог. Это верный компромисс: не-webhook запрос,
записанный с путём и статусом без query, безвреден, а один утёкший секрет в
`combined` невосстановим. Границы при этом узкие — `?notsecret=`, `?mytoken=`,
`?tokenized=`, `?hub.verify_token_extra=` и `?foo=secret` не срабатывают.

Статически это зафиксировано в
`src/altegio_bot/tests/test_nginx_webhook_logging_reference.py`, а фактическое
поведение Nginx — в
`src/altegio_bot/tests/test_nginx_webhook_logging_integration.py` (одноразовый
контейнер, обязательный режим `ALTEGIO_REQUIRE_NGINX_LOGTEST=1`).

**Вариант A — отдельные webhook locations уже существуют.** Добавить
logging-only include непосредственно в каждый из этих существующих блоков.
Production diff должен состоять только из logging directives или одной строки
`include`; routing/security directives остаются без изменений.

**Вариант B — все запросы обслуживает общий `location /`.** Не создавать новый
generic или exact location. Использовать `map` из http-level reference и две
условные `access_log` на уровне существующего `server`: обычный site log только
для non-webhook routes и safe log только для webhook routes. Перед применением
проверить вывод `nginx -T`: `access_log` внутри более низкоуровневого location
отменяет наследование server-level logging. Такой unsafe location исправляется
отдельно, но его routing directives не меняются.

Первый selector обязан использовать неизменяемый исходный request target:

```nginx
map $request_uri $kitilash_is_webhook {
    default                    0;
    ~^/webhooks?(?:/|\?|$)     1;
}
```

Граница `(?:/|\?|$)` классифицирует `/webhook?x=1` и `/webhooks?x=1`, но
исключает `/webhookevil`, `/webhooks-old`, `/webhook_backup` и вложенный
`/api/webhooks/easyweek`. После любого internal redirect
`$kitilash_is_webhook` остаётся boolean `1`; inverse selector оставляет
`$kitilash_is_not_webhook=0`. Поэтому webhook должен попасть только в safe
webhook log, а не одновременно в него и в обычный combined log.

Проверить всю цепочку destinations: конечный или named location с собственным
`access_log ... combined;` переопределит server-level conditional logging и
снова запишет исходную request line. Если такой override остаётся активным,
security DoD не выполнен.

Не вставлять logging-only include вслепую в общий `location /`: это изменит
логирование всего API и подавит request-level error logging для всех маршрутов.

#### Шаг 3. Выбрать безопасный scope для error log

Обычный error log, например
`error_log /var/log/nginx/webhooks_error.log error;`, небезопасен: при
proxy-level failure он может сохранить полный request target. Для контекста,
который обслуживает query-secret webhooks, обязательна политика:

```nginx
error_log /dev/null emerg;
```

- Если отдельные webhook locations уже существуют, logging-only include с этой
  директивой добавляется в них без изменения routing.
- Если существует только общий `location /`, локально и условно переопределить
  формат `error_log` по `$uri` нельзя. Оператор должен выбрать одно из двух:
  подавить request-level error logging на уровне существующего
  `server_name api.kitilash.com`, если это допустимо для всего API-server; либо
  сделать production-specific locations только после изучения `nginx -T`.

Во втором случае нужно скопировать всю эффективную routing/security
конфигурацию прежнего location: body limits, timeouts, rate limiting,
proxy headers, upstream, buffering/retry policy и access restrictions; отдельно
проверить regex precedence и доказать route parity. Такой production-specific
location нельзя добавлять в репозиторий как универсальный reference.

Если server-level suppression неприемлем, а parity отдельного location не
доказана, безопасный scope для `error_log` не выбран и production security DoD
не выполнен. Отсутствие маркера после обычного application-level `403` это не
опровергает.

Политику нужно проверить во всех достижимых destinations. Конечный location с
собственным persistent `error_log /var/log/nginx/...;` переопределяет безопасный
scope и может сохранить исходную request line. В таком случае security DoD не
выполнен, даже если начальный location использует `/dev/null emerg`.

#### Шаг 4. Проверить конфиг, reload и effective diff

После правки:

```bash
sudo nginx -t
```

Только при успешном результате (reload, не restart):

```bash
sudo systemctl reload nginx
```

Затем снова получить effective config:

```bash
sudo nginx -T
```

Сравнить его с зафиксированным на шаге 1: неожиданно не должны измениться
location matching, upstream, headers, body limits, timeouts, rate limiting,
allow/deny, buffering, retries и rewrite rules. `restart` вместо `reload` не
использовать, если reload достаточен.

Отдельно подтвердить в effective config:

- source первого map — ровно `$request_uri`, не `$uri`;
- selector boundary — `(?:/|\?|$)`;
- safe format по-прежнему содержит `uri=$uri`, но не `$request_uri`;
- все rewrite/internal-redirect destinations сохранили routing parity;
- ни один destination не вводит unsafe `access_log` или persistent `error_log`.

#### Шаг 5. Production route-parity до и после изменения

До и после Nginx-изменения выполнить одинаковые проверки:

```text
GET  /health
POST /webhooks/easyweek с неправильным token
POST /webhooks/altegio с неправильным secret
GET  /webhook/whatsapp с неправильным hub.verify_token
```

Для каждого запроса сравнить HTTP status, response body contract, headers,
timeout, body-size behaviour и доступность upstream. Auth-контракт не должен
измениться:

```text
EasyWeek wrong token            → 403
Altegio wrong secret            → 403
WhatsApp wrong verification token → 403
```

Также проверить, что `/health` и остальные API routes, проходившие через прежний
`location /`, продолжают работать. Любое расхождение сначала рассматривается
как routing regression.

Для каждой реально достижимой ветки `rewrite`, `try_files`, `index`,
`error_page`, named location и повторного regex matching сравнить конечный
handler, status, body, headers, timeout, body-size behaviour и upstream.

#### Шаг 6. Normal-path marker tests (обязательны)

Использовать три разных искусственных маркера, никогда реальные секреты:

```bash
EW_MARKER="EW_LOG_LEAK_TEST_$(openssl rand -hex 8)"
ALT_MARKER="ALT_LOG_LEAK_TEST_$(openssl rand -hex 8)"
WA_MARKER="WA_LOG_LEAK_TEST_$(openssl rand -hex 8)"
```

```bash
curl -sS -o /dev/null -w '%{http_code}\n' -X POST \
  "https://api.kitilash.com/webhooks/easyweek?event=booking-created&token=${EW_MARKER}" \
  -H "Content-Type: application/json" \
  -d '{"probe":"nginx-log-leak-test"}'
```

```bash
curl -sS -o /dev/null -w '%{http_code}\n' -X POST \
  "https://api.kitilash.com/webhooks/altegio?secret=${ALT_MARKER}" \
  -H "Content-Type: application/json" \
  -d '{}'
```

```bash
curl -sS -o /dev/null -w '%{http_code}\n' \
  "https://api.kitilash.com/webhook/whatsapp?hub.mode=subscribe&hub.verify_token=${WA_MARKER}&hub.challenge=123"
```

Каждый запрос должен вернуть `403`. Для каждого маркера выполнить все три
поиска:

```bash
for MARKER in "$EW_MARKER" "$ALT_MARKER" "$WA_MARKER"; do
  grep -R "$MARKER" /var/log/nginx/ 2>/dev/null
  journalctl -u nginx --since "15 minutes ago" --no-pager | grep "$MARKER"
  docker compose -p altegio_bot logs --since 15m 2>&1 | grep "$MARKER"
done
```

Ожидаемый результат для каждого поиска — `not found`. Отсутствие вывода означает
успех; значения маркеров в отчёт не копировать.

Отдельно проверить новый безопасный лог:

```bash
tail -n 20 /var/log/nginx/webhooks_access.log
```

Там должны быть метод, путь, статус, размер ответа и длительность — и не должно
быть `token=`, `secret=`, `userGuid=`, `hub.verify_token`, символа `?` и query
string целиком.

#### Шаг 6b. URI-normalization marker tests (обязательны)

Канонических маршрутов недостаточно: нужно проверить формы, где сырой
`$request_uri` не выглядит webhook'ом, а маршрутизация идёт по нормализованному
URI. `curl` по умолчанию сам сворачивает путь, поэтому **обязателен**
`--path-as-is` (или другой клиент, не нормализующий путь до отправки):

```bash
for RAW in \
  '/%77ebhooks/easyweek?token=' \
  '/webhooks%2Feasyweek?token=' \
  '/foo/../webhooks/easyweek?token=' \
  '/health?token=' ; do
  M="NORM_LEAK_TEST_$(openssl rand -hex 8)"
  curl -sS --path-as-is -o /dev/null -w "%{http_code} ${RAW}\n" \
    -X POST "https://api.kitilash.com${RAW}${M}" \
    -H 'Content-Type: application/json' -d '{"probe":"uri-normalization"}'
  grep -R "$M" /var/log/nginx/ 2>/dev/null && echo "LEAK" || echo "not found"
done
```

Зафиксировать **фактический** status и обработчик каждого случая: конкретный build
Nginx может отклонить какую-то encoded-форму ещё до routing (например `400`). Это
допустимо — но тогда нельзя утверждать, что случай дошёл до webhook handler; в
отчёте указывается фактическое поведение. Критерий успеха один и тот же:
**маркера нет ни в одном логе**, а не конкретный HTTP-код.

Проверять для каждого маркера: `combined` access log, безопасный webhook access
log, Nginx error logs и весь каталог логов целиком.

Normal-path `403` доказывает только отсутствие утечки на запросе, дошедшем до
работающего приложения. Он не проверяет Nginx `error_log`.

#### Шаг 7. Failure-path marker test для Nginx error log (обязателен)

Нельзя останавливать production API, подменять его upstream или ломать реальные
webhook deliveries. Тест выполнить в изоляции: временный localhost-only Nginx
server либо disposable Nginx container.

Изолированный конфиг должен использовать тот же
`kitilash_webhook_safe` log format и тот же logging-only include, направлять
`/var/log/nginx` в отдельный временный каталог и проксировать только тестовый
маршрут на заведомо недоступный upstream, например `127.0.0.1:1`. Маркер должен
быть искусственным:

```bash
FAILURE_MARKER="FAILURE_LOG_LEAK_TEST_$(openssl rand -hex 8)"
```

Ожидаемый сценарий:

```text
request with ?token=${FAILURE_MARKER}
→ isolated Nginx returns 502
→ safe access log contains method/path/status, but not the marker
→ error log contains no marker because error_log is /dev/null emerg
```

После запроса проверить весь отдельный временный каталог:

```bash
grep -R "$FAILURE_MARKER" <temporary-nginx-log-directory>
```

Ожидаемый результат — `not found`. Затем удалить disposable container,
временный config и временные logs. Обычный запрос к работающему FastAPI, даже с
ответом `403`, не засчитывается как failure-path test.

#### Шаг 8. Internal-redirect marker test (обязателен)

Static regex test недостаточен. В disposable Nginx container или временном
localhost-only instance использовать тот же `$request_uri` map, тот же safe
format и те же conditional access logs. Тестовый routing существует только в
одноразовом конфиге и не копируется в production reference.

Сначала отправить non-webhook control request на отдельный тестовый path и
убедиться, что он появился в combined log. Это доказывает, что отсутствие
webhook-запроса в combined не вызвано отключённым или неработающим обычным
логом.

Отправить искусственный marker:

```bash
INTERNAL_REDIRECT_MARKER="INTERNAL_REDIRECT_LOG_TEST_$(openssl rand -hex 8)"
```

Исходный запрос должен прийти на:

```text
/webhooks/easyweek?token=${INTERNAL_REDIRECT_MARKER}
```

и пройти `rewrite` или другой internal redirect на non-webhook destination:

```text
/internal-handler
```

Проверить одновременно:

- текущий `$uri` действительно стал `/internal-handler`;
- original `$request_uri` сохранил webhook classification;
- combined log содержит non-webhook control request;
- combined log вообще не содержит этот webhook request;
- safe webhook log содержит одну запись с допустимым
  `uri=/internal-handler`;
- safe webhook log не содержит marker, `token=`, `?`, `$request_uri` или query;
- error log не содержит marker;
- поиск marker по всему временному каталогу возвращает `not found`.

После теста удалить disposable container, config и logs. После применения
production config повторить marker-проверку для каждой реально существующей
internal-redirect ветки, не меняя production routing или upstream.

#### Шаг 9. Ротация потенциально раскрытых секретов

Порядок важен: сначала должны пройти `nginx -t`, reload, повторный `nginx -T`,
route-parity, все normal-path marker tests и изолированный failure-path marker
test, а также internal-redirect marker test. Только после этого ротировать
секреты — иначе новые значения снова могут попасть в логи.

Ротировать минимум:

```text
EASYWEEK_WEBHOOK_SECRET
ALTEGIO_WEBHOOK_SECRET
```

Дополнительно проверить исторические Nginx-логи на наличие Meta verification URL;
если там встречался `hub.verify_token` — ротировать и
`WHATSAPP_WEBHOOK_VERIFY_TOKEN`.

Правила: не печатать реальные секреты в stdout, не вставлять их в PR/issue/commit
и в shell history, не коммитить `easyweek.env`/`.env`/production-конфиг Nginx,
генерировать значения криптографически безопасно
(`python3 -c "import secrets; print(secrets.token_urlsafe(32))"`). Сначала
обновить production env, затем URL/конфигурацию у провайдера, затем:

```bash
docker compose -p altegio_bot up -d --force-recreate altegio-api
```

После ротации проверить: старый EasyWeek token → `403`; старый Altegio secret →
`403`; новая реальная доставка EasyWeek → `200`; новая реальная доставка Altegio
→ `200`; новых значений нет в Nginx, journald и Docker logs.

Удаление или очистка старых логов **не заменяет** ротацию: сначала сделать старые
секреты недействительными.

В отчётах указывать только `found / not found`, `rotated / not rotated`,
`old token rejected / new delivery accepted` — без самих значений.

### 2.3. Публикация порта — ОБЯЗАТЕЛЬНЫЙ операторский gate

`altegio-api` публикует порт как `${API_BIND_HOST:-127.0.0.1}:8000:8000`: по
умолчанию слушает только loopback, чтобы наружу трафик шёл строго через reverse
proxy с TLS и лимитами. Кодовая ответственность на этом исчерпана — публичный
обход proxy закрыт, но топологию сервера код угадывать не должен.

До деплоя оператор обязан определить, где физически находится proxy:

```bash
ss -ltnp | grep :8000
```

```bash
docker ps --format '{{.Names}}\t{{.Ports}}' | grep -Ei 'nginx|proxy|caddy|traefik'
```

Дальше по результату:

- **proxy на этом же хосте** → оставить loopback-дефолт, ничего не менять;
- **proxy — контейнер** → либо подключить его к сети compose и ходить по
  service-DNS `altegio-api:8000`, либо задать `API_BIND_HOST=0.0.0.0` в `.env`
  и **обязательно** ограничить внешний доступ firewall/security group;
- **proxy на отдельном сервере** → `API_BIND_HOST=0.0.0.0` + firewall,
  разрешающий только IP proxy.

Проверить после деплоя, что порт не открыт в мир (`ss -ltnp` показывает
`127.0.0.1:8000`, а не `0.0.0.0:8000`, если только это не сделано осознанно).

---

## 3. Коды ответа и что они значат

| Код | Когда | Реакция EasyWeek |
|---|---|---|
| `200` | доставка **надёжно записана** в `easyweek_events` | успех |
| `403` | неверный/отсутствующий токен либо пустой секрет в настройках | доставка отклонена, строки нет |
| `404` | `EASYWEEK_ENABLED=false` | поверхность скрыта, строки нет |
| `503` | инфраструктурный сбой записи: БД недоступна, нет таблицы, исчерпан пул | **временная ошибка — EasyWeek повторит доставку** |

Важное уточнение к формулировке «capture всегда отвечает 200»: она относится к
проблемам **содержимого**. Не-JSON, NUL, невалидный UTF-8, `NaN` и слишком
большое тело не являются ошибкой — такая доставка приводится к безопасному виду,
сохраняется и подтверждается `200`. Но если строка **не записана**, ответ `200`
означал бы безвозвратную потерю события (durable spool в PR-1 нет), поэтому
такие случаи отвечают `503`.

Серия `503` в Request history EasyWeek — это сигнал смотреть на БД, а не на
формат payload:

```bash
docker compose -p altegio_bot logs --tail=200 altegio-api | grep "persistence failed"
```

В логе будет только класс ошибки (`error_type=...`) и безопасные метаданные:
текст исключения драйвера может содержать SQL-параметры, то есть PII.

### 3.1. После серии 503 — обязательная проверка вебхука в EasyWeek

Устранить причину `503` (поднять БД, применить миграцию, расширить пул) — это
только половина. EasyWeek после серии неуспешных доставок может **сам отключить**
вебхук, и тогда новые события просто не придут. Поэтому после восстановления:

1. открыть EasyWeek → **Settings → Developer → Webhooks**;
2. проверить, не отключён ли вебхук автоматически, и при необходимости включить;
3. нажать **Resend** на последней неуспешной доставке;
4. убедиться, что появилась новая строка:
   ```sql
   SELECT id, received_at, event_hint FROM easyweek_events ORDER BY id DESC LIMIT 5;
   ```
5. проверить, что маркер-секрет (2.2) по-прежнему отсутствует в логах proxy.

### 3.2. Внешние лимиты размера и времени запроса — ОБЯЗАТЕЛЬНЫЙ gate

`read_bounded_body` ограничивает **память** приложения (не более 128 КиБ + текущий
chunk), но намеренно дочитывает поток до конца и не защищает от медленного
клиента, лавины подключений и гигантских тел на уровне сокета. Это ответственность
reverse proxy, и без этих лимитов go-live считается **заблокированным**:

```nginx
client_max_body_size 1m;
client_body_timeout 10s;
```

Плюс rate/connection limiting по политике площадки (`limit_req` / `limit_conn`).
Конкретные значения можно адаптировать, но **отсутствие** лимитов — блокер, а не
рекомендация. `1m` с большим запасом покрывает нормальную доставку EasyWeek
(приложение всё равно хранит максимум 128 КиБ).

---

## 4. Что лежит в таблице

| Колонка | Смысл |
|---|---|
| `body_raw` | **источник истины**: исходные байты доставки, максимум **128 КиБ** |
| `body_size_bytes` | полный фактический размер доставки, включая не влезший хвост |
| `body_truncated` | `true`, если доставка была больше лимита |
| `payload` | JSONB — лишь **разбор** байтов; теряет порядок ключей, пробелы, формат чисел и дубли ключей |
| `payload_hash` | sha256 канонизированного JSON; `NULL`, если тело не удалось сохранить как JSONB |
| `body_text` | текстовая проекция для нечитаемых как JSON тел (NUL и битые байты заменены на `U+FFFD`) |
| `query` / `headers` | сохранённые метаданные запроса, секреты замаскированы |

Дедупликации нет **сознательно**: ретрай и Resend дают отдельные строки с
одинаковым `payload_hash` — по ним изучается ретрай-семантика EasyWeek.

Полезные запросы:

```sql
SELECT received_at, event_hint, auth_via, body_size_bytes, body_truncated, payload_hash
FROM easyweek_events ORDER BY id DESC LIMIT 20;
```

```sql
SELECT payload_hash, count(*) FROM easyweek_events
WHERE payload_hash IS NOT NULL GROUP BY 1 HAVING count(*) > 1;
```

---

## 4.1. Chatwoot message tracing (расследование конкретной доставки)

Телефон, текст сообщения, имя клиента и имя агента **сознательно удалены** из
логов Chatwoot-вебхука (`chatwoot_webhook`) и из путей `whatsapp_inbox_worker`,
которые обрабатывают эти события (пересылка входящих в Chatwoot и operator
relay) — это PII, а логи читает и хранит больше людей и систем, чем БД. Поэтому
расследование доставки теперь двухшаговое.

> ОБЛАСТЬ ДЕЙСТВИЯ. Гарантия покрывает именно эти пути и закреплена
> runtime-тестами каждой ветки (`test_worker_log_hygiene.py`). Там же есть
> AST-проверка logger-вызовов в `whatsapp_inbox_worker` — это defense-in-depth
> от прямого использования известных PII-символов, а НЕ полноценный taint-анализ:
> она сознательно не отслеживает значение через промежуточные функции и
> переменные, поэтому первичная гарантия — именно runtime-тесты веток, а не
> статический проход. Остальные модули — `outbox_worker`, `promo_lead_handler`,
> `chatwoot_client`, провайдеры, campaign-код — **всё ещё пишут телефон** в свои
> логи. Это известный незакрытый участок, а не гарантия; при разборе инцидентов и
> при выдаче доступа к логам исходите из того, что телефон в них встречается.
> Приведение остальных модулей к той же политике — отдельная задача.

**Шаг 1 — в логах ищем только технические идентификаторы:**

| Что искать | Пример |
|---|---|
| `conv_id` | `conv_id="501"` |
| `msg_id` | `msg_id="5001"` |
| `dedupe_key` | `dedupe_key="chatwoot:501:5001"` |

```bash
docker compose -p altegio_bot logs --tail=500 altegio-api | grep 'conv_id="501"'
```

**Искать по телефону или тексту сообщения в логах бессмысленно** — их там нет, и
это не поломка. Отправной точкой служит `conversation_id`/`message_id` из
интерфейса Chatwoot.

**Шаг 2 — детали берём из БД.** Безопасный запрос без payload:

```sql
SELECT
    id,
    received_at,
    status,
    dedupe_key,
    chatwoot_conversation_id,
    chatwoot_message_id,
    error
FROM whatsapp_events
WHERE chatwoot_conversation_id = :conv_id
   OR chatwoot_message_id = :msg_id
ORDER BY received_at DESC
LIMIT 20;
```

Сам payload открывать **отдельным запросом и только при необходимости**,
пользователем с разрешённым доступом:

```sql
SELECT payload FROM whatsapp_events WHERE id = :id;
```

Правила обращения с результатом:
- `whatsapp_events` содержит PII — доступ ограничивать как к `clients`;
- не копировать полный payload в тикеты, чаты и скриншоты — переносить только
  те поля, которые действительно нужны;
- если нужно поделиться контекстом, ссылаться на `id` строки, а не на её
  содержимое.

Если scalar-колонка пуста (`chatwoot_conversation_id IS NULL`), значит id в
доставке был нечисловым или вне диапазона BIGINT — исходное значение при этом
сохранено в `payload`, искать нужно там.

---

## 5. Доступ и хранение

Таблица содержит **PII** (телефоны и имена клиентов в сыром payload и в
`body_raw`). Доступ к ней ограничивать так же, как к `clients` и
`whatsapp_events`; не выгружать её целиком в тикеты, чаты и скриншоты.

Автоматического TTL в PR-1 **нет** — ни cron, ни job, ни триггера. Рекомендуемая
**ручная** политика хранения — 30 дней:

```sql
DELETE FROM easyweek_events WHERE received_at < now() - interval '30 days';
```

Запускать только по отдельному согласованию и после бэкапа: пока идёт
исследовательская фаза, каждая удалённая строка — потерянные данные, которые
EasyWeek не переотправит.
