# Altegio Bot

WhatsApp-бот для интеграции с CRM Altegio. Автоматизирует отправку уведомлений клиентам о записях, напоминаниях и рассылках.

## Основные возможности

- 📅 Автоматические уведомления о создании/изменении/отмене записей
- ⏰ Напоминания за 24 часа и 2 часа до визита
- 💳 Выпуск карт лояльности через Altegio API
- 📧 Ежемесячные рассылки для новых клиентов
- 📊 Ops-кабинет для мониторинга сообщений

## Архитектура

- **API**: FastAPI (webhooks от Altegio и WhatsApp)
- **БД**: PostgreSQL + SQLAlchemy
- **Очередь**: Асинхронный воркер для отправки сообщений
- **WhatsApp**: Meta Business API (официальные шаблоны)

## Установка и запуск

### Переменные окружения

Создайте файл `.env`:

```bash
# Database
DATABASE_URL=postgresql+asyncpg://user:password@localhost/altegio_bot

# Altegio API
ALTEGIO_PARTNER_TOKEN=your_partner_token
ALTEGIO_USER_TOKEN=your_user_token
ALTEGIO_WEBHOOK_SECRET=your_webhook_secret
ALTEGIO_API_BASE_URL=https://api.alteg.io/api/v1

# WhatsApp Meta Business
WHATSAPP_ACCESS_TOKEN=your_meta_access_token
META_WA_PHONE_NUMBER_ID=your_phone_number_id
META_WABA_ID=your_waba_id
META_APP_SECRET=your_app_secret
WHATSAPP_WEBHOOK_VERIFY_TOKEN=your_verify_token

# Loyalty cards
LOYALTY_CARD_TYPE_ID=46454

# Ops-cabinet
OPS_USER=admin
OPS_PASS=your_secure_password
OPS_SECRET=your_jwt_secret

# Settings
ALLOW_REAL_SEND=true
WHATSAPP_SEND_MODE=template
WA_OPTOUT_POLICY=marketing_only
```

### Docker Compose

```bash
docker compose up -d
```

### Миграции

```bash
docker exec -i altegio-api alembic upgrade head
```

## Тестирование

### 🧪 Тестирование создания карты лояльности

Проект включает специальный скрипт для комплексного тестирования функционала выпуска карт лояльности и отправки сообщений.

#### Базовый тест (с автоматическим удалением карты)

```bash
docker exec -i altegio-api sh -lc '
set -a
. /app/.env
set +a

/app/.venv/bin/python -m altegio_bot.scripts.run_test_newsletter_smart \
  --phone 381638400431 \
  --company-id 758285 \
  --booking-link https://n813709.alteg.io/ \
  --template kitilash_ka_newsletter_new_clients_monthly_v2 \
  --expect-status delivered \
  --timeout 180 \
  --cleanup \
  --card-type-id 46454
'```

**Флаг `--cleanup`**: Автоматически удаляет тестовую карту после завершения теста. Используйте для "чистых" тестов, которые не оставляют следов в CRM.

#### Тест с сохранением карты в CRM

Если нужно оставить карту в системе Altegio (например, для ручной проверки в CRM):

```bash
docker exec -i altegio-api sh -lc '
set -a
. /app/.env
set +a

/app/.venv/bin/python -m altegio_bot.scripts.run_test_newsletter_smart \
  --phone 381638400431 \
  --company-id 758285 \
  --booking-link https://n813709.alteg.io/ \
  --template kitilash_ka_newsletter_new_clients_monthly_v2 \
  --expect-status delivered \
  --timeout 180 \
  --card-type-id 46454
'```

⚠️ **Без флага `--cleanup` карта останется в CRM Altegio!**

#### Что делает тест?

1. ✅ **Проверяет Meta-шаблон**: Убеждается, что шаблон существует и имеет статус `APPROVED` в WhatsApp Business API
2. ✅ **Проверяет идемпотентность**: Пропускает тест, если он уже выполнялся успешно в последние 24 часа (можно обойти флагом `--force`)
3. ✅ **Создаёт карту лояльности**: Выпускает тестовую карту через Altegio API с префиксом `99` (тестовые карты)
4. ✅ **Отправляет WhatsApp сообщение**: Использует официальный Meta-шаблон с номером карты
5. ✅ **Отслеживает доставку**: Ждёт webhook-событий от WhatsApp (`sent` → `delivered` → `read`)
6. ✅ **Опционально удаляет карту**: Если указан `--cleanup`, удаляет тестовую карту из Altegio

#### Параметры команды

| Параметр | Описание | Обязательный | Пример |
|----------|----------|--------------|--------|
| `--phone` | Номер телефона получателя (с кодом страны) | ✅ | `381638400431` или `+381638400431` |
| `--company-id` | ID компании в Altegio | ✅ | `758285` |
| `--card-type-id` | ID типа карты лояльности | ✅ | `46454` |
| `--booking-link` | Ссылка на онлайн-запись | | `https://n813709.alteg.io/` |
| `--template` | Имя Meta-шаблона | | `kitilash_ka_newsletter_new_clients_monthly_v2` |
| `--expect-status` | Ожидаемый статус доставки | | `delivered` (по умолчанию) или `sent` |
| `--timeout` | Таймаут ожидания webhook (секунды) | | `180` (по умолчанию) |
| `--cleanup` | Удалить карту после теста | | флаг без значения |
| `--force` | Игнорировать кэш (запустить повторно) | | флаг без значения |
| `--location-id` | ID локации (если отличается от company-id) | | `758285` |

#### Форматы номера карты

- **Тестовые карты**: Префикс `99` + `YYMMDD` + 8 случайных цифр = 16 символов
  - Пример: `9926022832320706` (тест от 28.02.2026)
  
- **Продакшн карты**: Префикс `00` + `YYMMDD` + 8 случайных цифр = 16 символов
  - Пример: `0026022845678901` (продакшн от 28.02.2026)
  - Используются в реальных рассылках (`run_monthly_newsletter_smart.py --mode send-real`)

#### Логи успешного теста

```
INFO smart_test: Smart test START phone=+381638400431 company=758285
INFO smart_test: Template 'kitilash_ka_newsletter_new_clients_monthly_v2' APPROVED
INFO smart_test: Issuing loyalty card number=9926022832320706 type_id=46454 phone=381638400431
INFO smart_test: Card issued: id=47719627 number=9926022832320706
INFO smart_test: Message sent: provider_message_id=wamid.HBg...
INFO smart_test: Poll complete: outcome=pass statuses_seen=['sent', 'delivered']
INFO smart_test: Card deleted: card_id=47719627
INFO smart_test: === RESULT: PASS ===
```

#### Exit коды

- `0` = PASS (тест успешен)
- `2` = FAIL (тест провален или ошибка)

### 🧪 Unit-тесты

Запуск всех тестов:

```bash
docker exec -i altegio-api pytest
```

Тесты с coverage:

```bash
docker exec -i altegio-api pytest --cov=altegio_bot --cov-report=html
```

Тесты клиента карт лояльности:

```bash
docker exec -i altegio-api pytest src/altegio_bot/tests/test_altegio_loyalty.py -v
```

### 📋 Ручное создание карты через Python

Если нужно создать карту программно без отправки сообщения:

```python
import asyncio
from altegio_bot.altegio_loyalty import AltegioLoyaltyClient

async def create_card():
    loyalty = AltegioLoyaltyClient()
    try:
        card = await loyalty.issue_card(
            location_id=758285,
            loyalty_card_number='9926022899999999',  # Уникальный 16-значный номер
            loyalty_card_type_id='46454',
            phone=381638400431,  # Номер БЕЗ знака +
        )
        print(f"Карта создана: {card}")
        print(f"ID карты: {card.get('id')}")
        print(f"Номер: {card.get('loyalty_card_number')}")
    finally:
        await loyalty.aclose()

asyncio.run(create_card())
```

### 📧 Тестирование месячной рассылки

#### Режим `list` (посмотреть кандидатов)

```bash
docker exec -i altegio-api /app/.venv/bin/python \
  -m altegio_bot.scripts.run_monthly_newsletter_smart \
  --mode list \
  --company-id 758285
```

#### Режим `dry-run` (имитация без отправки)

```bash
docker exec -i altegio-api /app/.venv/bin/python \
  -m altegio_bot.scripts.run_monthly_newsletter_smart \
  --mode dry-run \
  --company-id 758285 \
  --booking-link https://n813709.alteg.io/
```

#### Режим `send-test` (отправка одному тестовому получателю)

```bash
docker exec -i altegio-api /app/.venv/bin/python \
  -m altegio_bot.scripts.run_monthly_newsletter_smart \
  --mode send-test \
  --company-id 758285 \
  --test-phone 381638400431 \
  --booking-link https://n813709.alteg.io/ \
  --cleanup
```

#### Режим `send-real` (боевая рассылка)

⚠️ **ВНИМАНИЕ**: Этот режим создаёт реальные карты и отправляет реальные сообщения!

```bash
docker exec -i altegio-api /app/.venv/bin/python \
  -m altegio_bot.scripts.run_monthly_newsletter_smart \
  --mode send-real \
  --company-id 758285 \
  --booking-link https://n813709.alteg.io/ \
  --limit 10  # Ограничить первыми 10 получателями
```

## Структура проекта

```
altegio_bot/
├── src/altegio_bot/
│   ├── altegio_loyalty.py       # Клиент Altegio Loyalty API
│   ├── meta_templates.py        # Маппинг шаблонов WhatsApp
│   ├── message_planner.py       # Планировщик отправки сообщений
│   ├── models/
│   │   └── models.py            # SQLAlchemy модели
│   ├── ops/
│   │   ├── router.py            # Ops-кабинет (мониторинг)
│   │   └── auth.py              # Аутентификация
│   ├── scripts/
│   │   ├── run_test_newsletter_smart.py      # 🧪 Тестовая рассылка
│   │   ├── run_monthly_newsletter_smart.py   # 📧 Продакшн рассылка
│   │   └── seed_templates.py                 # Seed шаблонов сообщений
│   ├── workers/
│   │   └── outbox_worker.py     # Воркер отправки сообщений
│   ├── webhooks/
│   │   └── whatsapp.py          # WhatsApp webhook handler
│   └── tests/                   # Unit-тесты
├── alembic/                     # Миграции БД
├── docker-compose.yml
├── Dockerfile
└── README.md
```

## Мониторинг и отладка

### Ops-кабинет

Откройте в браузере: `http://localhost:8000/ops/login`

- **Monitoring**: Статистика сообщений за 24ч, проваленные задачи
- **Outbox**: Очередь исходящих сообщений
- **Events**: Webhook-события от WhatsApp
- **Opt-outs**: Клиенты, отписавшиеся от рассылок
- **Campaign Runs**: История рассылок

### Логи

```bash
# API логи
docker logs -f altegio-api

# Воркер логи
docker logs -f altegio-worker
```

### Проверка health

```bash
curl http://localhost:8000/health
```

## API Endpoints

| Метод | Путь | Описание |
|-------|------|----------|
| `POST` | `/webhooks/altegio` | Webhook от Altegio (записи) |
| `POST` | `/webhooks/whatsapp` | Webhook от WhatsApp (статусы доставки) |
| `GET` | `/health` | Health check |
| `GET` | `/ops/monitoring` | Дашборд мониторинга (требует auth) |
| `GET` | `/ops/outbox` | Очередь сообщений (требует auth) |

## Troubleshooting

### Карта создана, но не видна в CRM

✅ **Это нормально**, если вы использовали флаг `--cleanup`. Карта создаётся и сразу удаляется после теста.

🔧 **Решение**: Запустите тест **без** `--cleanup`, чтобы карта осталась в системе.

### Сообщение не доставлено

1. Проверьте, что телефон зарегистрирован в WhatsApp
2. Убедитесь, что Meta-шаблон имеет статус `APPROVED`
3. Проверьте лимиты отправки в WhatsApp Business API

### Ошибка "No loyalty card types found"

Убедитесь, что:
1. В Altegio настроены типы карт лояльности
2. Правильный `location_id` (обычно равен `company_id`)
3. API токены имеют права на работу с картами

## Лицензия

Проприетарный проект.
