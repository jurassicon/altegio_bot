# Runbook: подготовка волны миграции Altegio → EasyWeek

План: §29 (ревизия 23). Этап **предшествует** мигратору из
`pr11_1_cutover_runbook.md` и ничего в нём не заменяет.

Перенос одного мастера занимал часы, и почти всё это время уходило не на
перенос: поиск услуг, сопоставление по глазам, регенерация экспорта клиентов,
ручное создание карточек, перенос идентификаторов между командами. Этот runbook
— один процесс на всю подготовку.

**Read-only часть бота не останавливает.** Остановка уведомлений и всё, что
делает мигратор, начинается позже и описано в существующем runbook.

---

## 0. Что понадобится

- `manifest.json` — тот же файл, что у мигратора. Может быть неполным: услуги
  этой волны как раз и предлагает подготовка. `selected_altegio_staff_ids`,
  `staff` и `easyweek_location_*` должны быть заполнены — это описание волны,
  и его выбирает человек.
- Altegio company id филиала (Karlsruhe `758285`, Rastatt `1271200`).
- Cutover — ровно тот, с которым потом пойдёт apply.

**Каталог состояния содержит персональные данные.** Локально это
`outputs/easyweek_migration_prepare/`, в контейнере — `/migration/state`. Он не
коммитится, файлы создаются с правами `0600`, каталог — `0700`. Не пересылайте
эти файлы и не вставляйте их в тикеты.

---

## 1. Собрать данные и получить предложения

Локально:

```bash
cd /path/to/altegio_bot && uv run python -m altegio_bot.scripts.easyweek_migration_prepare prepare --manifest outputs/easyweek_migration/input/manifest.json --company-id 758285 --cutover-at 2026-09-01T00:00:00+02:00
```

На проде через ops-сервис:

```bash
cd /opt/altegio_bot && docker compose --profile ops run --rm --build easyweek-migration-prepare prepare --manifest /migration/input/manifest.json --company-id 758285 --cutover-at 2026-09-01T00:00:00+02:00
```

Команда читает Altegio, каталог EasyWeek и клиентов EasyWeek. Она **ничего не
создаёт**: у этого режима нет пути к мутации.

На stdout — машинный отчёт без персональных данных. Читать в нём блок `ready`,
в котором пять отдельных ответов, а не одно слово:

| Поле | Что означает |
|---|---|
| `customers_ready` | клиенты, чей UUID реально увиден в EasyWeek |
| `mapping_ready` | все услуги волны сопоставлены и согласованы |
| `records_ready` | записи, готовые к переносу прямо сейчас |
| `records_needing_manual_work` | записи с индивидуальной ценой, длительностью или несколькими услугами — подготовка их не трогает |
| `blocked_by_technical_error` | поиски, которые не дали ответа, и незавершённые создания |

`blocked_by_technical_error > 0` — это **не** «клиентов нет». Это «мы не смогли
посмотреть». Повторите шаг 1 после того, как причина устранена.

---

## 2. Прочитать решения глазами

Файл с персональными данными:

```bash
cat outputs/easyweek_migration_prepare/operator_review.json
```

На проде:

```bash
sudo cat /opt/altegio_bot/outputs/easyweek_migration_prepare/operator_review.json
```

В нём три блока.

`customers` — имя и телефон из источника, email (только если он действительно
есть), количество связанных записей и рядом с каждым клиентом напоминание, что
**создание карточки не переносит историю визитов**. Клиент, созданный сейчас,
выглядит в EasyWeek как пришедший впервые.

`service_mapping` — исходный service ID и точное имя, целевой service UUID и
точное имя, цена с валютой, длительность и количество затронутых записей.

`records` — каждая запись волны: `altegio_record_id`, мастер, начало и конец в
Europe/Berlin (плюс тот же момент в UTC), длительность, услуга, цена и
`price_to_pay`, телефон клиента и причина блокировки, если она есть. Это тот
список, который сверяют с экраном Altegio. Время, попавшее в переход на зимнее
или летнее время и не имеющее единственного значения, показано как `null` — не
приблизительно.

Причины блокировки, которые встречаются чаще всего:

| `blocked_reason` | Что делать |
|---|---|
| `source_name_not_split` | в источнике только полное имя. Разделите его сами (шаг 3, `--correct-customer`) — автоматически оно не делится |
| `source_customers_share_phone` | на одном номере два разных клиента Altegio. Слить их автоматически нельзя; разберитесь в Altegio |
| `customer_ambiguous` | в EasyWeek две карточки на один номер. Разберитесь в EasyWeek |
| `customer_already_exists` | клиент уже есть; создавать нечего, он попал в directory |
| `lookup_undetermined` | ответа не было. Не «нет клиента» — повторите шаг 1 |

---

## 3. Подтвердить услуги и клиентов

Ни одна команда этапа не читает stdin: Docker без TTY, закрытый pipe и EOF
согласием не являются. Согласие — это явные аргументы.

Услуги, по одной:

```bash
cd /path/to/altegio_bot && uv run python -m altegio_bot.scripts.easyweek_migration_prepare confirm --manifest outputs/easyweek_migration/input/manifest.json --company-id 758285 --cutover-at 2026-09-01T00:00:00+02:00 --confirm-service 6001 --confirm-service 6002
```

Все однозначные сразу — только против дайджеста напечатанного списка
(`mapping.pending_digest` из шага 1):

```bash
cd /path/to/altegio_bot && uv run python -m altegio_bot.scripts.easyweek_migration_prepare confirm --manifest outputs/easyweek_migration/input/manifest.json --company-id 758285 --cutover-at 2026-09-01T00:00:00+02:00 --confirm-all-services --mapping-digest ДАЙДЖЕСТ_ИЗ_ШАГА_1
```

Если список изменился, команда откажет. Это не придирка: «да» было сказано
конкретному списку.

Исправить данные клиента (например, разделить полное имя):

```bash
cd /path/to/altegio_bot && uv run python -m altegio_bot.scripts.easyweek_migration_prepare confirm --manifest outputs/easyweek_migration/input/manifest.json --company-id 758285 --cutover-at 2026-09-01T00:00:00+02:00 --correct-customer +4915112345678 --first-name "Anna Maria" --last-name "Schmidt"
```

Подтвердить клиента, пропустить клиента, подтвердить весь список:

```bash
cd /path/to/altegio_bot && uv run python -m altegio_bot.scripts.easyweek_migration_prepare confirm --manifest outputs/easyweek_migration/input/manifest.json --company-id 758285 --cutover-at 2026-09-01T00:00:00+02:00 --confirm-customer +4915112345678
```

```bash
cd /path/to/altegio_bot && uv run python -m altegio_bot.scripts.easyweek_migration_prepare confirm --manifest outputs/easyweek_migration/input/manifest.json --company-id 758285 --cutover-at 2026-09-01T00:00:00+02:00 --skip-customer +4915112345678
```

```bash
cd /path/to/altegio_bot && uv run python -m altegio_bot.scripts.easyweek_migration_prepare confirm --manifest outputs/easyweek_migration/input/manifest.json --company-id 758285 --cutover-at 2026-09-01T00:00:00+02:00 --confirm-all-pending-customers --pending-digest ДАЙДЖЕСТ_ИЗ_ШАГА_1
```

Подтверждение привязано к показанным данным. Если данные изменились,
подтверждение снимается само, и клиент возвращается в `pending`.

Повторный `prepare` **не переспрашивает** про то, что не изменилось.

---

## 4. Создать подтверждённых клиентов

Это единственная команда этапа, которая пишет в EasyWeek, и у неё **отдельное
разрешение**: типизируемый флаг и переменная окружения. Ни один из них по
отдельности разрешением не является. Это разрешение **не позволяет перенести ни
одной записи**.

Локально:

```bash
cd /path/to/altegio_bot && EASYWEEK_MIGRATION_ALLOW_CUSTOMER_CREATE=true uv run python -m altegio_bot.scripts.easyweek_migration_prepare create-customers --manifest outputs/easyweek_migration/input/manifest.json --company-id 758285 --cutover-at 2026-09-01T00:00:00+02:00 --i-authorise-creating-customers
```

На проде:

```bash
cd /opt/altegio_bot && docker compose --profile ops run --rm --build -e EASYWEEK_MIGRATION_ALLOW_CUSTOMER_CREATE=true easyweek-migration-prepare create-customers --manifest /migration/input/manifest.json --company-id 758285 --cutover-at 2026-09-01T00:00:00+02:00 --i-authorise-creating-customers
```

Что делает команда для каждого подтверждённого клиента: проверяет отсутствие
заново, ставит маркер на диск **до** запроса, отправляет **один** `POST`,
проверяет результат чтением карточки по UUID и точному номеру, и только тогда
записывает клиента в directory.

Если исход неизвестен — таймаут, разрыв, 5xx, `2xx` без читаемого UUID — запуск
**останавливается**. Это правильно. Просто запустите ту же команду ещё раз:
она начнёт с чтения и сама разберётся, состоялось создание или нет. Повторный
`POST` вслепую — это дубликат карточки поверх чужой истории визитов.

Если команда сообщает, что каталог состояния заблокирован, значит идёт другой
запуск. Дождитесь его. Файл блокировки удаляйте руками только если уверены, что
процесс мёртв.

---

## 5. Обновить план и получить verified dry-run id

Подготовка уже собрала merged manifest и customer directory. Дальше — обычный
dry-run существующего мигратора, запущенный так, что его digest берётся с
объекта отчёта, а не из «последнего файла в каталоге».

```bash
cd /path/to/altegio_bot && uv run python -m altegio_bot.scripts.easyweek_migration_prepare verify-dry-run --manifest outputs/easyweek_migration_prepare/manifest.proposed.json --company-id 758285 --cutover-at 2026-09-01T00:00:00+02:00
```

На проде:

```bash
cd /opt/altegio_bot && docker compose --profile ops run --rm --build easyweek-migration-prepare verify-dry-run --manifest /migration/state/manifest.proposed.json --company-id 758285 --cutover-at 2026-09-01T00:00:00+02:00
```

В ответе: `verified_dry_run_id`, пути к manifest и customer-directory и готовая
команда `next_command_after_a_clean_canary`. Идентификаторы больше не нужно
переносить руками.

`manifest.proposed.json` — аддитивное слияние: уже согласованные услуги
предыдущих волн не переписываются. Перед первым apply перенесите этот файл в
`/migration/input/manifest.json` (или укажите его путь мигратору напрямую) и
сохраните как artefact волны.

---

## 6. Дальше — существующий процесс без изменений

С этого места действует `pr11_1_cutover_runbook.md` целиком и без послаблений:
отключение нативных уведомлений EasyWeek, canary на одной именованной записи,
проверка отсутствия уведомления клиенту, apply с `--verified-dry-run-id` из
шага 5, `reconcile`, `reconcile --final`.

Подготовка не ослабляет ни одну из этих проверок и не даёт права пропустить
canary.

---

## 7. Что этот этап не делает

- Не переносит записи и не содержит второго мигратора.
- Не сопоставляет услуги «похоже»: только точное совпадение канонического
  имени, всё остальное — на человека.
- Не сливает клиентов, не выдумывает имя, email, телефон или историю визитов.
- Не считает отсутствие клиента в EasyWeek доказательством первого визита и не
  обнуляет счётчики.
- Не обходит конфликт уникальности телефона или email изменением контактов.
- Не пишет имена, телефоны, email и сырые тела ответов в обычные логи. Строка
  запроса `GET /customers?phone=…` содержит номер, поэтому HTTP-логгер поднят
  до `WARNING` и на нём стоит фильтр.

---

## 8. Если что-то пошло не так

| Симптом | Что это значит | Что делать |
|---|---|---|
| `lookups_undetermined > 0` | воркспейс не прочитан | повторить шаг 1; ничего не создавать |
| `halted: create_outcome_unknown` | `POST` мог пройти | повторить шаг 4; она сверится чтением |
| `halted: create_not_verified` | карточка не подтверждена чтением | то же; при повторе несовпадений — смотреть в EasyWeek руками |
| `halted: create_access_denied` | ключ не имеет права писать; с данными клиента всё в порядке | проверить `EASYWEEK_API_KEY` и `EASYWEEK_WORKSPACE_SLUG`; подтверждение сохранено |
| `blocked_reason: create_rejected_by_workspace` | телефон или email уже занят другим клиентом | найти владельца в EasyWeek. Контакты **не** менять ради обхода |
| `DecisionStoreLocked` | идёт другой запуск | дождаться; файл блокировки снимать только вручную |
| `the pending customer list has changed` | список изменился после печати | шаг 1, прочитать заново, подтвердить с новым дайджестом |
| `branch identity unproven` | manifest указывает не на тот филиал | проверить `EASYWEEK_LOCATION_MAP` и `easyweek_location_*` в manifest |
