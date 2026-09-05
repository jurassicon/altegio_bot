# PR-12.1 — post-migration handover record-bound marketing jobs (§31)

Отдельная фаза **после** завершённого reminder handover §30. Забирает у Altegio
`review_3d`, `repeat_10d` и `comeback_3d` перенесённых бронирований и **ничего
не создаёт взамен**: мигрированная будущая запись не доказывает состоявшийся
визит, поэтому EasyWeek `review_3d`/`repeat_10d` может создать только доказанный
`booking-succeeded`, а `comeback_3d` — только доказанная EasyWeek cancellation.

Инструмент не вызывает EasyWeek, Altegio, Meta и Chatwoot, не создаёт
`OutboxMessage` и не отправляет сообщений ни в одном режиме.

## 0. Предусловия

- §30 выполнен и независимо проверен по тем же manifest и origin run IDs; у
  каждой eligible ledger-строки стоит `reminders_handed_over_at`, а волна имеет
  durable closure. Ad-hoc `UPDATE message_jobs` предусловием не является.
- В выбранной волне нет `pending` / `uncertain` строк.
- Развёрнут код PR-12.1: обе runtime-линии защиты безопасны до первого apply —
  без marker они не находят ни одной строки и ничего не подавляют.
- Применена миграция `d5a8c31e7f04` (`alembic upgrade head`).

## 1. Plan (read-only)

```bash
cd /opt/altegio_bot
dc() { docker compose -p altegio_bot -f docker-compose.yml -f docker-compose.chatwoot-internal.yml "$@"; }
dc --profile ops run --rm --build --no-deps -T easyweek-migration-post-booking-handover plan --manifest /migration/input/manifest.json --company-id 758285 --run-id MIGRATION_RUN_ID --snapshot /migration/state/post_booking.v1.json
```

Plan открывает PostgreSQL-транзакцию `SET TRANSACTION READ ONLY`, ничего не
пишет и не делает ни одного API-вызова. В отчёте раздельно считаются:

| Поле | Что означает |
|---|---|
| `rows_in_scope` | ledger-строки, которым будет поставлен marker |
| `source_jobs_queued` | открытые Altegio jobs трёх типов — их и отменит apply |
| `source_jobs_processing` | занятые worker'ом; **любая** блокирует всю волну |
| `source_jobs_terminal` | история (`done`/`failed`/`canceled`), не переписывается |
| `source_jobs_with_non_terminal_outbox` | `queued`/`sending`/`unknown` в Outbox; блокирует волну |
| `target_easyweek_jobs_present` | существующие EasyWeek jobs — остаются нетронутыми |
| `rows_without_source_job` | строки без job'ов; marker им нужен так же |
| `rows_already_marked` | уже обработанные предыдущим apply |
| `rows_with_source_target_overlap` | одна запись держит открытыми обе стороны |
| `blockers` | стабильные STOP-коды; при непустом списке apply невозможен |

`apply_ready=false` → exit code 1, и CLI **не печатает** команду apply. Snapshot
всё равно записывается как приватный диагностический артефакт (0600, каталог
0700) и разрешением не является.

Любая попытка нового plan уничтожает предыдущее разрешение по тому же пути:
файл перезаписывается PII-free tombstone, `read_snapshot` отвечает
`snapshot_invalidated`, а apply/verify отказываются и по имени архивного пути.

## 2. Apply (одна транзакция, короткая остановка outbox)

Apply не ходит в сеть, поэтому окно измеряется секундами. Останавливается
**общий** outbox-worker, и обязателен trap, который вернёт его при любом выходе,
включая Ctrl-C и kill.

```bash
cd /opt/altegio_bot
dc() { docker compose -p altegio_bot -f docker-compose.yml -f docker-compose.chatwoot-internal.yml "$@"; }
trap 'dc start altegio-outbox-worker' EXIT INT TERM HUP
dc stop altegio-outbox-worker
EASYWEEK_POST_BOOKING_HANDOVER_ALLOW_APPLY=true dc --profile ops run --rm --build --no-deps -T -e EASYWEEK_POST_BOOKING_HANDOVER_ALLOW_APPLY=true easyweek-migration-post-booking-handover apply --manifest /migration/input/manifest.json --company-id 758285 --run-id MIGRATION_RUN_ID --snapshot /migration/state/post_booking.v1.json --apply-report /migration/state/post_booking-apply.json --apply --plan-digest PLAN_DIGEST_ИЗ_ОТЧЁТА --confirm 'ФРАЗА_ИЗ_ОТЧЁТА'
```

Проверить состояние worker'а независимо от trap:

```bash
cd /opt/altegio_bot
docker compose -p altegio_bot -f docker-compose.yml -f docker-compose.chatwoot-internal.yml ps altegio-outbox-worker
```

Разрешение состоит из пяти независимых частей, и ни одна не заменяет другую:
режим `apply`, флаг `--apply`, точный `--plan-digest`, точная `--confirm` фраза
и переменная `EASYWEEK_POST_BOOKING_HANDOVER_ALLOW_APPLY=true`. Фраза и
переменная — **свои**, от §30 не подходят.

Внутри одной транзакции: origin-wave advisory locks → row locks
(ledger → records → jobs) → повторное доказательство manifest/run/ledger/closure
и §30-marker → полная перепроверка набора source jobs → отмена только
`status=queued` трёх типов со стабильной причиной → доказательство неизменности
target EasyWeek jobs → durable marker всем строкам scope → commit.

## 3. Немедленно вернуть outbox и выполнить verify

```bash
cd /opt/altegio_bot
docker compose -p altegio_bot -f docker-compose.yml -f docker-compose.chatwoot-internal.yml start altegio-outbox-worker
```

```bash
cd /opt/altegio_bot
dc() { docker compose -p altegio_bot -f docker-compose.yml -f docker-compose.chatwoot-internal.yml "$@"; }
dc --profile ops run --rm --build --no-deps -T easyweek-migration-post-booking-handover verify --manifest /migration/input/manifest.json --company-id 758285 --run-id MIGRATION_RUN_ID --snapshot /migration/state/post_booking.v1.json --apply-report /migration/state/post_booking-apply.json
```

`passed=true` требует: marker у всех строк scope, живой §30-marker и closure,
ноль открытых source jobs трёх типов, неизменные target jobs и scoped Outbox,
совпадение counts с apply report.

## 4. Повтор и новый plan

Повторный apply того же snapshot обязан дать ноль мутаций
(`canceled_job_ids: []`, `marked_ledger_ids: []`). Новый plan обязан показать
`source_jobs_queued: 0` и `rows_already_marked` равным `rows_in_scope`.

Read-only SQL-сверка (выполняет владелец):

```bash
cd /opt/altegio_bot
dc() { docker compose -p altegio_bot -f docker-compose.yml -f docker-compose.chatwoot-internal.yml "$@"; }
dc exec -T postgres psql -X -v ON_ERROR_STOP=1 -U altegio -d altegio_bot -c "BEGIN TRANSACTION READ ONLY; SELECT j.job_type, j.status, count(*) FROM message_jobs j JOIN records r ON r.id = j.record_id JOIN easyweek_migration_ledger l ON l.source_company_id = r.company_id AND l.source_record_id = r.altegio_record_id WHERE j.provider = 'altegio' AND j.job_type IN ('review_3d','repeat_10d','comeback_3d') AND l.post_booking_jobs_handed_over_at IS NOT NULL GROUP BY 1,2 ORDER BY 1,2; COMMIT;"
```

## 5. STOP-коды

| Код | Что делать |
|---|---|
| `reminder_handover_incomplete` | сначала завершить §30 для этой волны |
| `migration_wave_not_closed` | §30 closure отсутствует; закрыть волну |
| `migration_wave_unresolved` | reconcile до нуля `pending`/`uncertain` |
| `ledger_scope_changed` | scope изменился между plan и apply; новый plan |
| `source_job_processing` | worker держит job; дождаться и повторить plan |
| `source_job_outbox_non_terminal` | сообщение может быть в полёте; дождаться терминального статуса |
| `source_job_set_changed` | появился job, которого plan не видел; новый plan |
| `target_job_set_changed` | EasyWeek jobs изменились; новый plan |
| `post_booking_marker_conflict` | строку уже закрыл другой plan; разобрать вручную |
| `snapshot_invalidated` | разрешение уничтожено новым plan; выполнить plan заново |
| `database_lock_timeout` / `database_statement_timeout` | повторить в спокойный момент |

## 6. Incident path

Отмена source job необратима штатным путём: инструмент **не** переоткрывает
`canceled`/`failed` jobs. Если после apply выяснилось, что волна выбрана
ошибочно, восстановление — отдельное операторское решение с владельцем; marker
снимается только вручную и только после разбора, автоматического возврата
ownership в Altegio нет.

Исторические доставки (`sent`/`delivered`/`read`) не отзываются и не
переписываются: сообщение уже получено человеком. Они видны в отчёте как
история и не мешают защитить будущее.

## 7. Вне scope

Campaigns, `newsletter_new_clients_monthly`, newsletter follow-up, promo и
promo-card остаются Altegio-owned. Они **не** record-bound, поэтому ledger
marker к ним неприменим: их eligibility, segment/live guards, sender, template
и `booking_page_url` тоже Altegio-specific, и одна замена ссылки проблему не
решает. Отдельная provider-scoped фаза должна доказать всё это вместе.
