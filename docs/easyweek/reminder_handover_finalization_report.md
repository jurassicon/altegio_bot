# PR-11.2: проверка и завершение reminder handover

База: `a7de631016c00a2ccc080af2a3328842d2cdbf55`, ветка
`feature/easyweek-migration-preparation`. Исходное рабочее дерево было чистым.
Область работы: §30 канонического `INTEGRATION_PLAN.md`, только передача
`reminder_24h` / `reminder_2h` с ledger-подтверждённых Altegio-записей на EasyWeek.
Существующая реализация сохранена и исправлена; новый мигратор не создавался.
Канонический план не изменён. Новых Alembic-миграций нет.

## Найденные пробелы и исправления

| Пробел исходного кода | Изменение и доказательство |
|---|---|
| Ledger выбирался по всей компании, manifest в apply фактически не перечитывался | Обязательный повторяемый `--run-id`, digest manifest в snapshot, SQL scope по company/run; тест блокирует другую волну отдельно и доказывает, что apply её не ждёт |
| Plan не включал PostgreSQL read-only и не перечитывал данные после API walk | Первое SQL-выражение — `SET TRANSACTION READ ONLY`; попытка UPDATE отвергается PostgreSQL; изменение клиента во время GET делает plan непригодным |
| Категория и provider/company клиента target не доказывались | Переиспользован `evaluate_service_category`; неподходящая категория, клиент или мастер отклоняются до API-вызова |
| Snapshot не фиксировал Client, source/target state, ledger content и конфигурацию | Snapshot v4 содержит техническую identity и digests этих данных; изменение каждого блокирует apply |
| Target coverage проверяло не всю identity, stale jobs выявлялись слишком поздно | Проверяются client_id, canonical key/run_at/payload и открытые stale jobs; неправильный dedupe occupant обнаруживается уже в plan |
| Время apply фиксировалось до ожидания locks | Возраст snapshot и reminder boundary проверяются после locks и перед cancellation; реальные lock-wait тесты доказывают rollback |
| На весь ledger брался table lock | Применяются locks только для выбранных ledger/Record/Client/jobs; другая волна не блокируется этим table lock |
| Repeat apply report не проходил собственный reader | Нулевой повтор исходного snapshot читается и подтверждается verify; противоречивый report всё ещё отвергается |
| Verify не доказывал актуальные локальные данные и CRM-состояние | DB-проверки scope/client/evidence + один GET на пару через общий runtime proof + повторная DB-проверка после API walk |
| Ошибка до build_plan оставляла старый применимый файл | Новый plan инвалидирует прежний snapshot до проверок; повреждённый manifest не оставляет старое разрешение по тому же пути |
| Handover не имел отдельного обязательного CI-step | Все три handover test files явно перечислены с `REQUIRE_PG_CONCURRENCY=1`; contract test защищает шаг от пропусков и ослабления |

## Identity и архитектура

CLI отвечает за read-only plan, чтение приватного snapshot, явные apply gates,
одну внешнюю транзакцию и запись apply report. `reminder_handover.py` содержит
канонический snapshot/digest и reminder obligations. `handover_evidence.py`
повторно использует production category policy, собирает ограниченные локальные
доказательства и сохраняет только их hashes. DB-операции остаются в
`reminder_handover_db.py`.
Apply не выполняет сетевых вызовов. PII/secrets не выводятся в отчёты и не
сохраняются в Git; реальные production snapshots не создавались.

Связь пары: `(source_provider=altegio, source_company_id, source_record_id)`
в выбранных ledger run IDs → `status=created` → canonical target booking UUID
→ EasyWeek Record нужного location → Client с тем же provider/company.
Manifest доказывает филиал и выбранного мастера. Имена, телефоны, email,
услуга или совпадение времени не используются как доказательство пары.

Reminder obligations рассчитываются существующими `plan_reminders`,
`easyweek_reminder_dedupe_key`, `reminder_job_payload`. Старый payload не
копируется. В snapshot нет customer PII; UUID bookings доступны только в
приватном файле. Локальные строки с PII читаются для fingerprints, но их
содержимое в отчёты не попадает.

## Транзакция и конкуренция

Lock order: **wave lock** (по одному на каждую пару company/run снимка, в
отсортированном порядке) → ledger IDs → source/target Record IDs → связанные
Client IDs → reminder MessageJob IDs, в каждом наборе по возрастанию ID.

Wave lock — это transaction-scoped advisory lock, ключ которого выведен из
`provider:company:run`. Он нужен потому, что row lock не может заблокировать
строку, которой ещё нет: параллельный migration apply мог вставить новую
`status=created` строку в ту же волну после последней проверки полноты и до
commit, и handover сообщил бы успех для волны с бронированием, которое он не
проверял, не покрывал и не помечал.

Тот же lock берут все пути, способные добавить eligible-строку или перевести
строку волны в `created`: ledger claim обычного apply, `resolve-created` и
запись `created`. Возможны ровно два исхода:

- писатель успел первым — handover видит лишнюю строку и целиком
  останавливается с `eligible_scope_changed`, откатывая созданные target jobs,
  отмены source jobs и markers;
- handover успел первым — писатель ждёт освобождения и затем получает отказ
  `migration_wave_closed`, потому что волна с перенесёнными напоминаниями не
  может получить новое бронирование. Отказ происходит **до** запроса к EasyWeek,
  поэтому запись вообще не создаётся.

Волна с нерешёнными строками (`pending`, `uncertain`) к handover не допускается:
такая строка может стать `created` уже после закрытия волны, а отказать ей
позже нельзя — бронирование к тому моменту реально существует. Причина остановки
— `migration_wave_unresolved`; сначала выполняется reconcile.

Действия оператора при scope drift: выполнить reconcile до нуля нерешённых
строк, затем новый plan; переносить бронирования, появившиеся после закрытия
волны, отдельным новым `--run-id`. Другой run, другая компания и другой provider
lock не берут и не ждут его.
Apply задаёт локальные `lock_timeout=5s` и `statement_timeout=15s`.
После locks повторно проверяются scope, полный source reminder set,
fingerprints, configuration, identity, expiry и boundaries.

Порядок записи: создать недостающие EasyWeek jobs → доказать всё target coverage
→ проверить оставшееся время → отменить только queued source jobs → записать
durable marker → commit. Любой halted результат откатывает savepoint; CLI
откатывает всю внешнюю транзакцию. Исключение также откатывает изменения.

Существующий marker `reminders_handed_over_at` +
`reminder_handover_plan_digest` защищён DB CHECK и partial index. Planner и
send-time fence сохраняют узкую защиту от поздних Altegio-событий. Общая
семантика `add_job`, lifecycle, review, retention и campaigns не изменена.

Старое разрешение уничтожается, а не архивируется: попытка нового plan
перезаписывает файл snapshot PII-free tombstone (`mode="invalidated"`,
`invalidated_at`, `reason`), атомарно и с сохранением прав `0600`/`0700`.
Авторизующих байт не остаётся, `read_snapshot` отвечает `snapshot_invalidated`,
переименование tombstone ничего не восстанавливает, а apply/verify отказываются
и по имени архивного пути — до открытия write-сессии. Инвалидация выполняется
до разбора аргументов, поэтому plan с ошибочными аргументами тоже не оставляет
применимого разрешения; неуспешные apply и verify snapshot не трогают.

Повтор исходного snapshot с тем же digest допускает только нулевые мутации
при сохранившемся coverage. Новое разрешение не переписывает marker чужого
digest. Исчезнувшие или failed/canceled target jobs автоматически не чинятся.

## STOP-коды

Identity/scope: `migration_run_scope_invalid`, `manifest_scope_invalid`,
`migration_wave_changed`, `branch_identity_unproven`, `configuration_unproven`,
`configuration_changed`, `staff_scope_unproven`, `ownership_unproven`,
`ledger_duplicate_target`, `target_uuid_invalid`, `source_record_missing`,
`target_record_missing`, `provider_mismatch`, `company_mismatch`,
`target_client_unproven`, `source_client_mismatch`, `local_target_mismatch`.

Snapshots/state: `snapshot_invalidated`, `snapshot_invalidation_failed`,
`migration_wave_unresolved`, `migration_wave_closed`, `candidate_set_changed`,
`snapshot_incomplete_scope`,
`snapshot_not_cutover_ready`, `snapshot_obligation_blocked`,
`snapshot_obligations_incomplete`, `duplicate_job_identity`,
`ledger_pair_ambiguous`, `eligible_scope_changed`, `ledger_changed`,
`source_changed`, `target_changed`, `clients_changed`, `source_jobs_changed`,
`source_reminder_scope_changed`, `source_reminder_changed`,
`source_reminder_processing`, `stale_target_reminder`,
`obligation_identity_mismatch`, `reminder_identity_mismatch`,
`reminder_marker_conflict`, `marker_incomplete`.

Time/API/DB: `snapshot_time_invalid`, `snapshot_expired`,
`snapshot_age_limit_invalid`, `reminder_boundary_passed`, `api_pacing_invalid`,
`api_not_found`, `api_unauthorized`, `api_malformed_response`,
`api_rate_limited`, `target_unproven`, `database_lock_timeout`,
`database_statement_timeout`, `database_error`, `scoped_outbox_side_effect`,
`private_artifact_io_error`, `handover_unexpected_error`.

Reader также возвращает безопасное текстовое объяснение неверной schema,
digest, confirmation phrase или expiry. Полные HTTP/SQL exceptions не печатаются.
Blocked outcomes `occupied_by_canceled`, `occupied_by_failed`,
`occupied_by_unknown_status` требуют решения оператора.

## Изменённые файлы и состояние Git

`git status --short`: 9 изменённых и 3 новых файла, staging пуст.

```text
 M .github/workflows/ci_deploy.yml
 M docs/easyweek/migration_preparation_runbook.md
 M src/altegio_bot/easyweek_migration/reminder_handover.py
 M src/altegio_bot/easyweek_migration/reminder_handover_db.py
 M src/altegio_bot/reminder_ownership.py
 M src/altegio_bot/scripts/easyweek_reminder_handover.py
 M src/altegio_bot/tests/test_easyweek_migration_preparation_runbook.py
 M src/altegio_bot/tests/test_easyweek_reminder_handover.py
 M src/altegio_bot/tests/test_easyweek_reminder_handover_db.py
?? docs/easyweek/reminder_handover_finalization_report.md
?? src/altegio_bot/easyweek_migration/handover_evidence.py
?? src/altegio_bot/tests/test_easyweek_reminder_handover_safety.py
```

Reviewable changed lines считаются как additions + deletions относительно
базового commit, включая полное содержимое новых файлов, но без ignored
graphify outputs. Итог: **1351 changed lines = 1296 additions + 55 deletions**.
Числовой review budget в действующих локальных инструкциях не найден;
разрешения или исключения из других репозиториев не использовались.

Краткий diff: 628 строк production-кода, 465 строк tests, 12 строк CI;
остальное — операторская инструкция и этот отчёт. Runtime planner/send fence
не переписывались; в ownership helper только убрано небезопасное логирование
полного exception.

## Проверки

До исправлений: 190 существующих handover tests passed.
Ruff check и format check: PASS. `git diff --check`: PASS.
Alembic в отдельной пустой PostgreSQL 16 БД: единственная head
`c4b7e2f1a983`, `upgrade head` PASS, `current` совпадает с head.
Финальный targeted suite: **425 passed** (83.28 s), включая handover,
сквозной CLI, PostgreSQL concurrency, runbook и runtime reminder regression tests.
Полный suite: **7428 passed**, без skip (1621.80 s, 27:01).
После последних правок повторно выполнен targeted suite, указанный выше.
Для полного suite включены обязательные PostgreSQL, migration-compatibility
и Nginx CI-проверки (`REQUIRE_PG_CONCURRENCY=1`, `ALTEGIO_REQUIRE_MIGTEST=1`,
`ALTEGIO_REQUIRE_NGINX_LOGTEST=1`). Отправка отключена:
`WHATSAPP_PROVIDER=dummy`, `ALLOW_REAL_SEND=false`.
`graphify update .`: PASS, AST без LLM/API; обновлено 391 файлов.

Тестовые базы локальные и одноразовые; их контейнер после проверок остановлен.
Production, реальные CRM bookings, Meta и Chatwoot не использовались.
Проверка настоящей миграционной волны и production smoke — операторские шаги.
GitHub CI не запускался: изменения не закоммичены и не отправлены.

## Операторские команды и результат

Полные проверяемые команды выбора run IDs, plan, apply с trap, verify,
preflight и SQL-сверки находятся в §6b `migration_preparation_runbook.md`.
Каждая команда выполняется владельцем на сервере из `/opt/altegio_bot`.
Значения `MIGRATION_RUN_ID` и `PLAN_DIGEST_ИЗ_ШАГА_6B1` — placeholders;
реального production digest в этом отчёте нет.

PASS после apply: `verify.passed=true`, `api_guard_ready=true`,
`scope_drift=null`, `uncovered_obligations=0`, `open_altegio_reminders=[]`,
`stray_easyweek_reminders=[]`, неизменный scoped Outbox. Новый plan показывает
0 jobs для создания и отмены и `coverage_ready=true`.
Если outbox уже отправил сообщение или обычный webhook изменил запись после
apply, verify намеренно не заявляет неизменность frozen state; нужен разбор
сохранённого apply evidence, а не слепой повтор или reopen.

Commit не создаётся. Проверенные изменения остаются в working tree для
ручного review и commit владельцем.
