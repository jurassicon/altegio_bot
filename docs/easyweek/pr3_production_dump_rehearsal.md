# PR-3 — репетиция миграции на копии production dump (операторский runbook)

Обязательный DoD PR-3 из `docs/easyweek/INTEGRATION_PLAN.md` §7:
**upgrade/downgrade на восстановленной копии прод-дампа.**

Автоматические тесты (`test_easyweek_migration_integration.py`) гоняются на
**пустой** одноразовой базе. Они доказывают логику миграции, но не форму
production-данных: реальные объёмы, унаследованные индексы, ручные правки схемы
и, главное, возможные дубликаты `(company_id, external_id)`. Поэтому репетиция
на копии дампа не заменяется тестами.

Статус в отчёте PR-3, пока оператор её фактически не выполнил:

```text
production dump rehearsal: NOT RUN — required before PR-3 deployment
```

---

## 0. Жёсткие запреты

- **Никогда не выполнять `downgrade` на production.**
- Не подключать Alembic к production или к обычной development-базе.
- Не коммитить дамп, его фрагменты, реальные данные клиентов или креды.
- Не выкладывать вывод с PII в тикет/PR; в отчёт идут только счётчики и имена
  объектов схемы.

Репетиция выполняется **только** на отдельном одноразовом PostgreSQL.

---

## 1. Поднять одноразовый PostgreSQL

Отдельный инстанс, не тот, где живёт production:

```bash
docker run -d --name pr3-rehearsal -e POSTGRES_PASSWORD=rehearsal -p 55440:5432 postgres:16-alpine
```

```bash
until docker exec pr3-rehearsal pg_isready -U postgres; do sleep 1; done
```

```bash
docker exec pr3-rehearsal psql -U postgres -c 'CREATE DATABASE altegio_pr3_rehearsal'
```

## 2. Восстановить актуальный pre-deploy дамп

Деплой уже кладёт дампы в `/opt/altegio_bot/backups/altegio_before_deploy_*.dump`.
Взять самый свежий и восстановить его в одноразовую базу:

```bash
docker exec -i pr3-rehearsal pg_restore -U postgres -d altegio_pr3_rehearsal --no-owner --no-privileges < <путь-к-дампу>
```

## 3. Подтвердить, что цель — НЕ production

Обязательный шаг перед любым Alembic-вызовом:

```bash
export REHEARSAL_URL='postgresql+asyncpg://postgres:rehearsal@localhost:55440/altegio_pr3_rehearsal'
```

```bash
echo "$REHEARSAL_URL"
```

Убедиться глазами, что host/port/имя базы принадлежат одноразовому инстансу и
отличаются от production `DATABASE_URL`. Если совпадает хоть что-то — остановиться.

## 4. Зафиксировать исходное состояние

```bash
DATABASE_URL="$REHEARSAL_URL" uv run alembic current
```

Ожидается `9a1f4c7b2e3d` (состояние до PR-3).

```bash
docker exec pr3-rehearsal psql -U postgres -d altegio_pr3_rehearsal -c "SELECT 'clients' AS t, count(*) FROM clients UNION ALL SELECT 'records', count(*) FROM records UNION ALL SELECT 'message_templates', count(*) FROM message_templates UNION ALL SELECT 'message_jobs', count(*) FROM message_jobs UNION ALL SELECT 'whatsapp_senders', count(*) FROM whatsapp_senders"
```

Записать пять чисел — они должны совпасть на шагах 6, 8 и 9.

## 5. Upgrade до `c1a7d3f905b2`

```bash
DATABASE_URL="$REHEARSAL_URL" uv run alembic upgrade c1a7d3f905b2
```

```bash
DATABASE_URL="$REHEARSAL_URL" uv run alembic current
```

## 6. Проверки после upgrade

Колонки `provider`: тип, `NOT NULL`, server default, backfill:

```bash
docker exec pr3-rehearsal psql -U postgres -d altegio_pr3_rehearsal -c "SELECT table_name, data_type, character_maximum_length, is_nullable, column_default FROM information_schema.columns WHERE column_name = 'provider' ORDER BY table_name"
```

Ожидается ровно пять строк: `character varying(32)`, `is_nullable = NO`,
`column_default` содержит `altegio`.

```bash
docker exec pr3-rehearsal psql -U postgres -d altegio_pr3_rehearsal -c "SELECT 'clients' AS t, provider, count(*) FROM clients GROUP BY provider UNION ALL SELECT 'records', provider, count(*) FROM records GROUP BY provider UNION ALL SELECT 'message_templates', provider, count(*) FROM message_templates GROUP BY provider UNION ALL SELECT 'message_jobs', provider, count(*) FROM message_jobs GROUP BY provider UNION ALL SELECT 'whatsapp_senders', provider, count(*) FROM whatsapp_senders GROUP BY provider"
```

Ожидается: единственное значение `altegio`, а суммы равны числам из шага 4.

Новые уники есть, старых нет:

```bash
docker exec pr3-rehearsal psql -U postgres -d altegio_pr3_rehearsal -c "SELECT conname FROM pg_constraint WHERE conname LIKE 'uq_clients%' OR conname LIKE 'uq_records%' OR conname LIKE 'uq_whatsapp_senders%' ORDER BY conname"
```

Должны присутствовать:

```text
uq_clients_provider_company_altegio_id
uq_records_provider_company_altegio_id
uq_whatsapp_senders_provider_company_code
```

Должны отсутствовать:

```text
uq_clients_company_altegio_id
uq_records_company_altegio_id
uq_whatsapp_senders_company_code
```

Индексы, включая partial unique по EasyWeek UUID:

```bash
docker exec pr3-rehearsal psql -U postgres -d altegio_pr3_rehearsal -c "SELECT indexname, indexdef FROM pg_indexes WHERE indexname IN ('ix_clients_provider_company_phone','ix_message_jobs_provider_company_type_status','ix_message_templates_provider_company_code_lang','uq_records_easyweek_booking_uuid','ix_clients_company_phone','ix_message_jobs_company_type_status') ORDER BY indexname"
```

Ожидается: три новых индекса присутствуют, `uq_records_easyweek_booking_uuid`
уникален и содержит предикат `provider = 'easyweek' AND easyweek_booking_uuid IS NOT NULL`,
а `ix_clients_company_phone` и `ix_message_jobs_company_type_status` отсутствуют.

Row counts не изменились — повторить запрос из шага 4 и сверить.

## 7. Downgrade строго до `9a1f4c7b2e3d`

```bash
DATABASE_URL="$REHEARSAL_URL" uv run alembic downgrade 9a1f4c7b2e3d
```

Не использовать `downgrade -1`.

**Если downgrade отказался** с сообщением `Cannot downgrade: ... group(s) that
exist for more than one provider` — это не сбой репетиции, а срабатывание
fail-closed защиты. На копии production-дампа такого быть не должно (EasyWeek-строк
там ещё нет); если это произошло, значит в данных уже есть дубликаты
`(company_id, external_id)` — зафиксировать это как блокер и разбираться до
деплоя, ничего не удаляя и не объединяя.

## 8. Проверки после downgrade

```bash
docker exec pr3-rehearsal psql -U postgres -d altegio_pr3_rehearsal -c "SELECT conname FROM pg_constraint WHERE conname IN ('uq_clients_company_altegio_id','uq_records_company_altegio_id','uq_whatsapp_senders_company_code') ORDER BY conname"
```

Ожидается: все три старых уника восстановлены.

```bash
docker exec pr3-rehearsal psql -U postgres -d altegio_pr3_rehearsal -c "SELECT count(*) FROM information_schema.columns WHERE column_name IN ('provider','easyweek_booking_uuid','easyweek_booking_hash_id','meta_template_name')"
```

Ожидается `0`.

```bash
docker exec pr3-rehearsal psql -U postgres -d altegio_pr3_rehearsal -c "SELECT indexname FROM pg_indexes WHERE indexname IN ('ix_clients_company_phone','ix_message_jobs_company_type_status') ORDER BY indexname"
```

Ожидается: оба старых индекса на месте.

Row counts — снова сверить с шагом 4.

## 9. Повторный upgrade

```bash
DATABASE_URL="$REHEARSAL_URL" uv run alembic upgrade head
```

```bash
DATABASE_URL="$REHEARSAL_URL" uv run alembic heads
```

Ожидается ровно одна head — `c1a7d3f905b2`. Повторить проверки шага 6 целиком:
схема и row counts должны совпасть с первым прогоном.

## 10. Удалить одноразовую базу

```bash
docker rm -f pr3-rehearsal
```

Дамп, восстановленный локально, тоже удалить.

---

## Что записать в отчёт

Только безопасные факты:

- ревизия до и после;
- пять row counts до/после upgrade, после downgrade и после повторного upgrade;
- список найденных/отсутствующих constraint и index по именам;
- отказал ли fail-closed downgrade и почему;
- время выполнения upgrade на реальном объёме (нужно для выбора окна деплоя).

Без payload, без данных клиентов, без строк подключения с паролями.

После успешной репетиции статус в отчёте меняется на:

```text
production dump rehearsal: RUN
```

---

## Связанные документы

- `docs/easyweek/INTEGRATION_PLAN.md` — канонический план (§3.1, §7 PR-3).
- `src/altegio_bot/tests/test_easyweek_migration_integration.py` — те же проверки
  на пустой одноразовой базе, включая fail-closed поведение при неожиданных
  объектах схемы.
- `.github/workflows/ci_deploy.yml` — порядок production-деплоя: drain старого
  inbox-worker → проверка `processing` → миграция → canary нового worker.
- `docs/ops/pr3_deploy.md` — двухфазная модель деплоя: аудированный catch-up
  Phase A (`… → 9a1f4c7b2e3d`) при работающем старом runtime и окно Phase B
  (`9a1f4c7b2e3d → c1a7d3f905b2`) с остановленным worker'ом, плюс ожидаемое
  состояние базы после каждого типа отказа.
