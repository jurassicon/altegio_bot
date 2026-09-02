# Точечное копирование нейтральных Meta-шаблонов

## Scope

Отдельный небольшой operational PR после PR-12: разрешить скрипту
`clone_meta_templates_for_location` создавать отдельные branch-prefixed копии
шаблонов без адреса и карты. Это не изменение отправки сообщений, seed,
содержимого EasyWeek-контракта, feature-флагов или Altegio runtime.

Без новых опций прежнее поведение сохраняется: скрипт обрабатывает полный
набор branch-specific шаблонов, а нейтральные пропускает.

Новые опции:

- `--template-name SOURCE_NAME` — точное имя исходника, опцию можно повторять.
  Проверяется полнота выбранного набора, а не всех шаблонов филиала.
- `--include-neutral` — разрешить копирование нейтральных шаблонов; обязательно
  указать точный список через `--template-name`. Это не разрешение копировать
  все маркетинговые шаблоны аккаунта.

Исходник должен быть APPROVED и иметь выбранный язык и source-префикс. Для
нейтрального шаблона меняется только имя: BODY, STOP, параметры, примеры,
кнопки и категория сохраняются (служебные поля GET не отправляются в POST).
Отсутствие футера не доказывает универсальность текста: упоминания Юлии,
условий коррекции и фиксированные ссылки оператор проверяет в preview.

## Dry-run для трёх шаблонов Durlach

Команды выполняет оператор на сервере после деплоя. Агент самостоятельно
не подключается к production или Meta. Эта команда делает только GET:

```bash
(
  set -euo pipefail
  cd /opt/altegio_bot

  docker compose -p altegio_bot \
    -f docker-compose.yml \
    -f docker-compose.chatwoot-internal.yml \
    exec -T altegio-outbox-worker \
    /app/.venv/bin/python -m altegio_bot.scripts.clone_meta_templates_for_location \
    --source-location ka \
    --target-location du \
    --language de \
    --address 'Pfinztalstraße 4, 76227 Karlsruhe-Durlach' \
    --maps-url 'https://maps.app.goo.gl/HnVPnHaJHf2DW3Nn8' \
    --include-neutral \
    --template-name kitilash_ka_review_3d_v1 \
    --template-name kitilash_ka_comeback_3d_v1 \
    --template-name kitilash_ka_repeat_10d_v1
)
```

Если все три копии отсутствуют, ожидаются `ready=3`, `neutral-included=3` и
три target-имени `kitilash_du_*`. В payload должны остаться исходные тексты,
`de`, `MARKETING`, `POSITIONAL` и порядок параметров 2 / 2 / 3 для
review / comeback / repeat. Другие шаблоны не должны попасть в план.

Preview печатает также примеры из исходников Meta. Если они содержат реальные
данные клиентов, не публикуйте вывод и не коммитьте его в репозиторий.

## Создание после проверки preview

Повторите ту же команду, **убрав `-T` у `exec` для интерактивного ввода** и
добавив к аргументам скрипта `--apply`. Не добавляйте `--yes`: скрипт заново
прочитает Meta, покажет актуальный payload и попросит подтверждение.
Если создаются ровно три копии Durlach, подтверждение — `CREATE:DU:3`.

`--address` и `--maps-url` сохраняют прежний обязательный контракт apply.
У нейтрального шаблона они ничего не добавляют в текст и не создают футер.

Повторный запуск не пересоздаёт найденные APPROVED/PENDING копии. PENDING
означает только ожидание проверки, не готовность к отправке клиенту.
REJECTED/PAUSED/DISABLED или неизвестный статус блокируют весь план до POST.
Пропущенный, не APPROVED или не того языка исходник также блокирует весь план.

Новые опции не обходят проверки частично распознанного адреса/карты,
остаточных source-маркеров и изменённых параметров. Шаблон, для которого
старый контракт требует адресный футер, не становится нейтральным только
потому, что его футер исчез: такой исходник по-прежнему блокируется.

При ошибке или неизвестном результате POST проверьте список в Meta перед
повторным apply. Скрипт не делает автоматический retry POST.

Создание копий не отправляет сообщения клиентам, не меняет БД и не включает
retention. Включение рассылки — отдельный rollout после APPROVED, проверки
имён/параметров и согласования локального текста с реальным Meta-шаблоном.
Этот PR **не устраняет** обнаруженное расхождение Meta BODY и локального
EasyWeek BODY, не редактирует одобренные исходники и не меняет их содержание.

## Локальная проверка

```bash
PYTHONPATH=src .venv/bin/python -m pytest src/altegio_bot/tests/test_clone_meta_templates.py
.venv/bin/ruff check src/altegio_bot/scripts/clone_meta_templates_for_location.py src/altegio_bot/tests/test_clone_meta_templates.py
.venv/bin/ruff format --check src/altegio_bot/scripts/clone_meta_templates_for_location.py src/altegio_bot/tests/test_clone_meta_templates.py
```

Тесты используют поддельный Meta-клиент / HTTP mocks, без production API.
