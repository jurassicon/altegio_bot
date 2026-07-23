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

## 2. Секрет и логи — почему его нельзя искать в access-логах

Токен передаётся в query string:

```text
https://<host>/webhooks/easyweek?event=booking-created&token=<secret>
```

Поэтому **любой лог, который пишет полный request target, пишет и секрет**. В
этом репозитории закрыты все каналы, которые он контролирует:

| Канал | Что сделано |
|---|---|
| uvicorn access log | запускается с `--no-access-log` (Dockerfile и `command:` в compose) |
| замена ему | `AccessLogMiddleware` в `main.py`: метод, **путь**, статус, длительность — без query |
| application log | ни payload, ни тело, ни заголовки, ни значения query, ни `event_hint` |
| строки в БД | `mask_query` заменяет значения `token`/`secret`/`key`/… на `***`; `authorization`, `cookie`, `x-altegio-token` не сохраняются |
| диагностика тестов | INFO-лог httpx (пишет URL целиком) приглушён в `conftest.py` |

**Следствие для оператора: секрет невозможно восстановить из логов — и это
норма, а не поломка.** Если он утерян, его не ищут в логах, а ротируют: новый
секрет в `easyweek.env` → `--force-recreate altegio-api` → обновить URL всех
вебхуков в кабинете EasyWeek.

### Reverse proxy

Nginx-конфигурация живёт вне репозитория, поэтому её нужно проверить руками.
Стандартный `log_format combined` содержит `$request` (метод + **полный URI**) —
он для этого эндпоинта не подходит. Достаточно любого из вариантов:

```nginx
log_format safe '$remote_addr - $status $body_bytes_sent "$uri" $request_time';

server {
    location /webhooks/easyweek {
        access_log /var/log/nginx/webhooks.log safe;
        client_max_body_size 1m;
        proxy_pass http://127.0.0.1:8000;
    }
}
```

Проверить, что в конфиге нет `$request`, `$request_uri` и `$args` для этого
location, и провалидировать:

```bash
nginx -t
```

`client_max_body_size 1m` — внешний лимит запроса: он отсекает гигантские тела
до приложения, но с большим запасом покрывает нормальную доставку EasyWeek
(приложение само хранит максимум 128 КиБ).

### Публикация порта

`altegio-api` публикует порт как `${API_BIND_HOST:-127.0.0.1}:8000:8000`, то
есть по умолчанию слушает только loopback: наружу трафик обязан идти через
reverse proxy с TLS и лимитами. Если proxy живёт **не на этом хосте** и не в
сети compose, он получит connection refused — тогда задайте `API_BIND_HOST` в
`.env` явно и ограничьте доступ файрволом.

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
