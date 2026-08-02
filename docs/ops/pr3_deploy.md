# PR-3 production deploy — источник истины по Alembic revision

Операторский справочник по `scripts/deploy_pr3.sh`: как деплой определяет
текущую ревизию, почему он сверяет два независимых источника и в каком состоянии
остаётся база после каждого типа отказа.

Связанные документы:

- `docs/easyweek/pr3_production_dump_rehearsal.md` — репетиция upgrade/downgrade
  на копии production dump (обязательный DoD PR-3).
- `.github/workflows/ci_deploy.yml` — workflow, который фетчит точный commit и
  передаёт управление в `scripts/deploy_pr3.sh`.
- `src/altegio_bot/tests/test_pr3_revision_source.py` — исполняемые гарантии
  всего, что описано ниже.

---

## 1. Что известно точно, что воспроизведено, а что — нет

Раздел разделён намеренно: смешивать факты с гипотезами в runbook'е опасно.

### Подтверждённые факты

- Production `alembic_version` содержит **`9a1f4c7b2e3d`**. Прочитано напрямую:
  `SELECT version_num FROM alembic_version;`
- `9a1f4c7b2e3d` — это ровно `PRE_PR3_REVISION`.
- Следовательно production **не отстаёт** ни на одну миграцию, и текущий деплой
  должен сразу выполнить переход `9a1f4c7b2e3d → c1a7d3f905b2`.
- Ревизии `57cd7c3a7a27` **не существует** в этом репозитории: её нет в графе
  Alembic и нет ни в одном blob'е за всю историю (821 коммит).
- Предыдущая попытка деплоя тем не менее классифицировала базу как стоящую на
  `57cd7c3a7a27`. То есть **два источника разошлись**, и деплой поверил не тому.

### Воспроизведённое поведение

- Прежний ридер брал первый 12-hex токен из **человекочитаемого** вывода
  `alembic current`. Этот вывод содержит и прозу (`(head)`), и предупреждения, и
  текст ошибки `FAILED: Can't locate revision identified by '<id>'` — любой hex
  в нём неотличим от настоящей ревизии. Он же схлопывал multi-head базу до
  первой строки.
- Имя базы для `postgres` (`POSTGRES_DB`) и для Alembic (`DATABASE_URL`) — два
  **независимых** значения в `.env`; в Compose ничто их не связывает. На стенде
  воспроизведено: один сервер, один пользователь, один порт, но две разные
  базы — `SELECT` видит `9a1f4c7b2e3d`, Alembic видит `8705ec49cc73`.

### Не доказано

- **Каким именно образом на production появилось значение `57cd7c3a7a27`, не
  установлено.** Это не воспроизведено. Оба дефекта выше способны привести к
  ложной диагностике, но какой из них (или что-то третье) сработал на боевом
  хосте — открытый вопрос.
- Поэтому исправление не полагается на конкретную гипотезу: деплой теперь
  сверяет источники и падает fail-closed при любом расхождении.

---

## 2. Единый источник истины

`alembic_revision_facts()` выполняется **внутри контейнера `migrate`**, тем же
движком и той же конфигурацией, которыми будет выполняться сама миграция, и
отдаёт машиночитаемый протокол:

```text
REVISION_STATUS=ok|none|multiple|unknown|error
REVISION=<12 hex>
DB_HEAD_COUNT=<n>
DRIVER / HOST / PORT / URL_DATABASE
DB_NAME / DB_SYSTEM_ID / DB_OID
```

Свойства:

- ревизия берётся через `MigrationContext.get_current_heads()`, а не парсингом
  текста;
- принимается **ровно один** DB head; `none`, `multiple`, `unknown` и `error`
  завершают деплой;
- значение проверяется целиком — ровно 12 строчных hex-символов; префикс
  длинной строки, `9a1f4c7b2e3d unexpected text` и два id подряд отклоняются;
- в лог идут только безопасные компоненты подключения: driver, host, port и имя
  базы. Пароль, DSN и содержимое окружения не печатаются никогда.

---

## 3. Cross-check идентичности базы

До остановки worker'а, до deploy boundary и до любой миграции деплой доказывает,
что оба читателя работают с одной физической базой. Сравнивается тройка:

```text
current_database() | pg_control_system().system_identifier | pg_database.oid
```

- одинаковый `system_identifier` → один и тот же кластер;
- одинаковые имя и OID → одна и та же физическая база.

Затем та же таблица читается вторым путём — прямым `SELECT` из контейнера
`postgres` — и сравнивается с ревизией из `migrate`. Любое расхождение:

```text
❌ The migration runner and the postgres container are NOT on the same database.
   migrate  sees: <db>|<cluster>|<oid>
   postgres sees: <db>|<cluster>|<oid>
```

```text
❌ The two revision sources disagree.
   migrate  sees: X
   postgres sees: Y
```

— и деплой останавливается, ничего не изменив.

---

## 4. State machine

```text
   verify DEPLOY_SHA
        ↓
   build images  →  postgres healthy  →  SELECT 1  →  pg_dump (обязательный backup)
        ↓
   structured revision read (внутри migrate, через MigrationContext)
        ├─ none / multiple / unknown / error ──→ FAIL CLOSED
        ├─ значение не 12-hex ─────────────────→ FAIL CLOSED
        ↓
   cross-check идентичности базы (migrate ↔ postgres)
        ├─ тройки не совпали ──────────────────→ FAIL CLOSED
        ├─ ревизии не совпали ─────────────────→ FAIL CLOSED
        ↓
   классификация по графу ScriptDirectory
        ├─ PR-3 уже в lineage БД ──────────────→ обычный идемпотентный deploy
        ├─ PR-3 не в графе ────────────────────→ обычный deploy
        ├─ PR3_PARENT != PRE_PR3 ──────────────→ FAIL CLOSED
        ├─ revision == 9a1f4c7b2e3d ───────────→ Phase B  ← текущий production
        └─ иначе (отставание) ─────────────────→ FAIL CLOSED
        ↓
   ┌── Phase B (окно с остановленным worker'ом) ──────────┐
   │  deploy boundary из PostgreSQL                       │
   │  stop legacy inbox worker  →  drain verification     │
   │  bounded recovery: processing → received             │
   │  alembic upgrade → c1a7d3f905b2                      │
   │  canary на новом образе  →  verification             │
   │  recreate regular worker →  verification             │
   │  retire canary                                       │
   └──────────────────────────────────────────────────────┘
        ↓
   compose up -d --remove-orphans, followup/Chatwoot checks
```

`altegio-api` не останавливается: вебхуки продолжают приниматься и копятся как
`received`.

---

## 5. Phase A (audited catch-up) — сознательно НЕ в этом hotfix

Production стоит ровно на `PRE_PR3_REVISION`, поэтому catch-up ему не нужен.
Механизм автоматического доката отставшей базы **удалён** из этого исправления,
чтобы hotfix остался минимальным: достоверное чтение ревизии и безопасный точный
переход PR-3.

Если база всё же отстаёт, деплой отказывается и просит привести её к
`9a1f4c7b2e3d` отдельно:

```text
❌ This deploy would apply PR-3 (c1a7d3f905b2) as part of a multi-revision upgrade
   from '<revision>' to 'c1a7d3f905b2'.
❗ Bring the database to 9a1f4c7b2e3d first. No schema change was made.
```

Автоматический двухфазный catch-up — отдельная задача с собственным аудитом
промежуточных миграций.

---

## 6. Состояние базы после каждого отказа

| Сценарий | Ревизия | Worker | Очередь |
| --- | --- | --- | --- |
| Ревизия не читается / unknown / multiple / пусто | без изменений | работает | не тронута |
| Идентичность баз не совпала | без изменений | работает | не тронута |
| Источники ревизии разошлись | без изменений | работает | не тронута |
| База отстаёт от `9a1f4c7b2e3d` | без изменений | работает | не тронута |
| `PR3_PARENT != PRE_PR3` | без изменений | работает | не тронута |
| Phase B упала до canary | откат до `9a1f4c7b2e3d` | возвращается прежний worker | bounded recovery |
| Phase B упала после verified canary | `c1a7d3f905b2` | canary оставлен работать | bounded recovery |
| Phase B упала после verified regular worker | `c1a7d3f905b2`, **downgrade запрещён** | regular worker | bounded recovery |

Автоматический откат ограничен ровно одним шагом:

```text
c1a7d3f905b2  →  9a1f4c7b2e3d
```

---

## 7. Диагностика при расхождении источников

Только чтение, ничего не меняем:

```bash
docker compose -p altegio_bot -f docker-compose.yml -f docker-compose.chatwoot-internal.yml exec -T postgres sh -lc 'psql -tAX -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT current_database(), version_num FROM alembic_version"'
```

```bash
docker compose -p altegio_bot -f docker-compose.yml -f docker-compose.chatwoot-internal.yml --profile ops run --rm --no-deps -T migrate /app/.venv/bin/python -c 'from altegio_bot.db import engine; u=engine.url; print(u.drivername, u.host, u.port, u.database)'
```

Сравнить имя базы из двух выводов. Если они различаются — расходятся
`POSTGRES_DB` и `DATABASE_URL` в `.env`; это конфигурационная проблема хоста, а
не схемы.

**Никогда** не «чинить» классификацию через `alembic stamp` или ручной `UPDATE
alembic_version`: это записало бы в базу значение, не соответствующее фактической
схеме. Деплой сознательно падает вместо этого.
