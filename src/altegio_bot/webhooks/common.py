"""Общие хелперы для вебхук-роутеров: auth, санитайзинг заголовков/query, хэш.

Вынесено из отдельных обработчиков, чтобы altegio и easyweek использовали одну
реализацию. Каждый эндпоинт добавляет собственный deny-set через ``extra_deny``
(например свой auth-заголовок). Поведение санитайзинга и хэширования совпадает с
прежними локальными копиями; сравнение токенов сознательно поднято до
constant-time (``hmac.compare_digest``).
"""

from __future__ import annotations

import hashlib
import hmac
import json
import math
import re
from collections.abc import Iterable
from dataclasses import dataclass, field

from fastapi import Request

# Ключ inbox-company map: канонический положительный decimal без ведущих нулей
# ("8", "42"), но не "0"/"-1"/"+8"/"8.0"/""/"abc". БЕЗ якорей — проверяем через
# ``fullmatch`` (``$`` совпал бы перед завершающим ``\n``, пропустив "8\n").
_CANONICAL_POSITIVE_INT_RE = re.compile(r"[1-9][0-9]*")

# Границы целочисленных типов PostgreSQL. Значение вне диапазона не «обрежется»,
# а уронит INSERT, поэтому проверяем до записи.
PG_INT_MIN, PG_INT_MAX = -(2**31), 2**31 - 1
PG_BIGINT_MIN, PG_BIGINT_MAX = -(2**63), 2**63 - 1
# Длина десятичной записи PG_INT_MAX. Ключ длиннее заведомо вне диапазона —
# отсекаем ДО ``int()`` (Python бросает ValueError на очень длинной строке).
_PG_INT_MAX_DIGITS = len(str(PG_INT_MAX))

# E.164 допускает максимум 15 цифр; больше не влезает и в downstream-колонки
# (OutboxMessage.phone_e164 = VARCHAR(32)) — режем ДО persistence.
_E164_MAX_DIGITS = 15
# ЗАКРЫТАЯ грамматика телефона: единственные допустимые символы. Всё остальное
# (буквы, emoji, ☎, control chars, CR/LF, zero-width, не-ASCII цифры, Unicode-
# пунктуация) делает строку целиком невалидной — НИКАКОГО удаления «мусора»,
# иначе "+49 151 O23 4567" превратилось бы в другого получателя. Пробел — только
# обычный ASCII space; tab/CR/LF сознательно НЕ разрешены (fail closed).
_ALLOWED_PHONE_CHARS = frozenset("0123456789 +-()./")

# Заголовки, которые никогда не попадают в сохранённые строки событий.
# Эндпоинт может расширить набор через ``extra_deny``.
_BASE_HEADER_DENY = frozenset({"authorization", "cookie"})

# Подстроки в имени query-параметра, делающие его значение чувствительным.
_SENSITIVE_QUERY_SUBSTRINGS = ("secret", "token", "key", "password", "signature")

# U+FFFD REPLACEMENT CHARACTER — чем подменяются символы, которые Postgres не
# принимает в текстовых типах.
_REPLACEMENT = "�"


def token_matches(candidate: str, secret: str) -> bool:
    """Constant-time сравнение токенов, не падающее на не-ASCII входе.

    Оба аргумента — ``str``; кодирование в байты сохраняет constant-time
    ``hmac.compare_digest`` И убирает ``TypeError``, который он бросает на
    не-ASCII ``str``. Токен контролируется атакующим, поэтому исключение
    превратилось бы в 500 и позволило бы отличить «неверный токен» от «ошибки
    сервера».
    """
    return hmac.compare_digest(candidate.encode("utf-8"), secret.encode("utf-8"))


def safe_headers(request: Request, *, extra_deny: Iterable[str] = frozenset()) -> dict[str, str]:
    """Заголовки запроса минус чувствительные/авторизационные.

    Запрещает ``authorization``/``cookie`` плюс всё из ``extra_deny`` (значения
    должны быть в нижнем регистре). Ключи возвращаются ровно так, как их отдаёт
    ``request.headers.items()`` (Starlette приводит имена к нижнему регистру);
    сама проверка deny в любом случае case-insensitive. Значения приводятся к
    Postgres-безопасному виду — заголовки контролируются отправителем, и NUL в
    них уронил бы commit.
    """
    deny = _BASE_HEADER_DENY | frozenset(extra_deny)
    return {postgres_safe_text(k): postgres_safe_text(v) for k, v in request.headers.items() if k.lower() not in deny}


def is_sensitive_query_key(key: str) -> bool:
    """True, если имя query-параметра выглядит как носитель секрета."""
    low = key.lower()
    return any(marker in low for marker in _SENSITIVE_QUERY_SUBSTRINGS)


def mask_query(query: dict[str, str]) -> dict[str, str]:
    """Маскирует значения чувствительных query-параметров (``secret``/``token``…).

    Ключи и оставшиеся значения приводятся к Postgres-безопасному виду: query
    полностью контролируется вызывающей стороной, а NUL в нём уронил бы commit.
    """
    return {
        postgres_safe_text(k): "***" if is_sensitive_query_key(k) else postgres_safe_text(v) for k, v in query.items()
    }


def postgres_safe_text(value: str) -> str:
    """Делает строку пригодной для записи в Postgres TEXT/JSONB.

    Postgres отвергает NUL в текстовых типах, а asyncpg не может закодировать
    непарные суррогаты в UTF-8 — и то и другое приходит из содержимого,
    контролируемого отправителем, поэтому дефект контента не должен превращаться
    в 500 на commit. Оба класса символов заменяются на U+FFFD; корректные
    значения возвращаются без изменений (быстрый путь). Исходные байты при этом
    не теряются: они лежат в ``EasyWeekEvent.body_raw``.
    """
    safe = value.replace("\x00", _REPLACEMENT) if "\x00" in value else value
    try:
        safe.encode("utf-8")
    except UnicodeEncodeError:
        # Непарные суррогаты (json.loads принимает "\ud800", UTF-8 — нет).
        safe = safe.encode("utf-8", errors="replace").decode("utf-8")
    return safe


def mapping_or_empty(value: object) -> dict:
    """Возвращает ``value``, если это словарь, иначе пустой словарь.

    Вложенные поля тела вебхука типизированы только по договорённости: клиент
    вправе прислать ``{"conversation": []}`` или ``{"sender": 123}``. Идиома
    ``payload.get("conversation") or {}`` от этого НЕ защищает — непустой список
    truthy, и следующий же ``.get()`` падает с AttributeError, то есть 500.
    """
    return value if isinstance(value, dict) else {}


def list_or_empty(value: object) -> list:
    """Возвращает ``value``, если это список, иначе пустой список.

    ``for x in value or []`` не защищает от truthy не-списка: по ``int`` цикл
    падает с ``TypeError``, а по строке молча итерирует символы. Строка
    сознательно НЕ считается последовательностью для этого контракта.
    """
    return value if isinstance(value, list) else []


def normalize_phone_candidate(value: object) -> str | None:
    """Единый type-safe нормализатор телефона: ``+<цифры>`` или ``None``.

    Общий контракт для Chatwoot ingress, worker и window-логики — чтобы правила
    не расходились между модулями. Принимает ТОЛЬКО ``str``; всё остальное
    (``list``/``dict``/``int``/``bool``/``float``/``None``) → ``None`` без
    исключения. ``bool`` — не строка и не число телефона.

    ЗАКРЫТАЯ грамматика: допустимы только ASCII-цифры и разделители из
    :data:`_ALLOWED_PHONE_CHARS` (space ``+ - ( ) . /``). ЛЮБОЙ другой символ
    (буква, ``O`` вместо ``0``, ``ext``, emoji, ``☎``, control char, CR/LF,
    zero-width, не-ASCII цифра, Unicode-пунктуация) отклоняет строку ЦЕЛИКОМ.
    Мусор НЕ вычищается: тихая очистка ``"+49 151 O23 4567"`` → ``"+49151234567"``
    или ``"49١٢٣15"`` → ``"+4915"`` отправила бы сообщение другому получателю.

    ``+`` допускается не более одного раза и только ДО первой цифры (``"49+15"``
    и ``"++49"`` невалидны). Нужна минимум одна ASCII-цифра; максимум 15 (E.164 и
    downstream-колонки) — иначе ``None``, без обрезки. Принимается только ``str``.
    Исходное значение (PII) здесь не логируется — это забота вызывающего кода.
    """
    if not isinstance(value, str):
        return None

    digits: list[str] = []
    plus_seen = False
    for ch in value:
        if ch not in _ALLOWED_PHONE_CHARS:
            return None  # закрытая грамматика: любой посторонний символ фатален
        if ch == "+":
            if plus_seen or digits:
                return None  # второй '+' или '+' после цифры
            plus_seen = True
        elif ch in "0123456789":
            digits.append(ch)
        # разделители (space - ( ) . /) допустимы и игнорируются

    if not digits or len(digits) > _E164_MAX_DIGITS:
        return None
    return "+" + "".join(digits)


def classify_message_type(value: object) -> str | None:
    """Классифицирует Chatwoot ``message_type`` через ТОЧНЫЕ типы.

    Возвращает ``"incoming"`` | ``"outgoing"`` | ``None`` (неподдерживаемо).

    Сознательно НЕ membership (``value in (1, "outgoing")``): в Python
    ``True == 1`` и ``1.0 == 1``, поэтому ``True``/``1.0`` протащило бы событие в
    relay-путь. ``type(value) is int`` исключает ``bool`` (``type(True) is bool``).
    Не принимаются ``"1"``/``"0"``/``2``/``-1``/контейнеры/``None``/float.
    """
    if (type(value) is int and value == 0) or value == "incoming":
        return "incoming"
    if (type(value) is int and value == 1) or value == "outgoing":
        return "outgoing"
    return None


def positive_int(value: object, *, max_value: int = PG_INT_MAX) -> int | None:
    """Точный положительный int (``0 < v <= max_value``), иначе ``None``.

    ``bool`` НЕ считается int (``type(True) is bool``); строки/float/контейнеры не
    коэрсятся — сырой sender-controlled JSON нельзя прогонять через ``int()``.
    Для inbox/company id в tenant-роутинге, где ошибка = чужой company.
    """
    if type(value) is int and 0 < value <= max_value:
        return value
    return None


@dataclass(frozen=True)
class InboxCompanyMap:
    """Результат разбора ``CHATWOOT_INBOX_COMPANY_MAP``.

    * ``configured=False`` — карта не настроена (``""``/``"{}"``): разрешён
      backward-compatible fallback по ``phone_number_id``.
    * ``configured=True, valid=False`` — синтаксически/семантически невалидная
      конфигурация: relay блокируется стабильным ``invalid_inbox_company_map``.
    * ``configured=True, valid=True`` — ``mapping`` содержит только валидные
      пары ``int inbox_id -> int company_id``, а ``inverse_mapping`` — их
      однозначную инверсию ``company_id -> inbox_id`` для outbound mirror.
    """

    configured: bool
    valid: bool
    mapping: dict[int, int] = field(default_factory=dict)
    inverse_mapping: dict[int, int] = field(default_factory=dict)


def _canonical_inbox_key(key: object) -> int | None:
    """Строгий inbox-ключ → положительный int в диапазоне PG Integer, иначе None.

    ``fullmatch`` (не ``match``!) по всей строке: ``match`` c ``$`` совпал бы
    перед завершающим ``\\n``, и ``"8\\n"`` прошёл бы как canonical, а затем
    ``int("8\\n") == 8`` схлопнул бы его с ``"8"`` — прямой wrong-tenant риск.
    Никакого ``.strip()`` (создал бы новые коллизии). Тотальность: ключ длиннее
    десятичной записи PG_INT_MAX заведомо вне диапазона — отсекаем ДО ``int()``,
    иначе Python бросит ``ValueError`` на строке из тысяч цифр.
    """
    if not isinstance(key, str) or not _CANONICAL_POSITIVE_INT_RE.fullmatch(key):
        return None
    if len(key) > _PG_INT_MAX_DIGITS:
        return None
    value = int(key)  # безопасно: канонический decimal ограниченной длины
    return value if 0 < value <= PG_INT_MAX else None


class _DuplicateJSONKey(Exception):
    """Raised by the object_pairs_hook when a JSON object repeats a key."""


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict:
    keys = [k for k, _ in pairs]
    if len(keys) != len(set(keys)):
        raise _DuplicateJSONKey
    return dict(pairs)


def parse_chatwoot_inbox_company_map(raw: object) -> InboxCompanyMap:
    """Тотальный ambiguity-safe парсер inbox→company конфигурации.

    Для ЛЮБОГО ``raw`` возвращает :class:`InboxCompanyMap` и НИКОГДА не бросает
    исключение, не возвращает и не логирует сырой конфиг/значение. Правила:
      * не-``str`` (в т.ч. уже разобранный объект) → невалидна;
      * пустая/whitespace строка ИЛИ любой распарсенный пустой JSON-объект
        (``"{}"``/``"{ }"``/``"{\\n}"``) → НЕ настроена (fallback разрешён);
      * ошибка JSON / duplicate JSON key / не-``dict`` top-level → невалидна;
      * ключ — строгий canonical positive int в диапазоне PG Integer
        (``fullmatch``, без ``.strip()``, длина отсекается до ``int()``);
      * значение — только ``type(v) is int`` и ``0 < v <= PG_INT_MAX`` (никакого
        ``int(value)`` для произвольного JSON);
      * коллизия нормализованного ключа (defense-in-depth) → невалидна;
      * один ``company_id`` в нескольких inbox → невалидна: обратный outbound
        routing не имеет права выбирать inbox по порядку JSON.
    """
    if not isinstance(raw, str):
        return InboxCompanyMap(configured=True, valid=False)

    stripped = raw.strip()
    if not stripped:
        return InboxCompanyMap(configured=False, valid=True)

    try:
        parsed = json.loads(stripped, object_pairs_hook=_reject_duplicate_keys)
    except _DuplicateJSONKey:
        return InboxCompanyMap(configured=True, valid=False)
    except Exception:
        return InboxCompanyMap(configured=True, valid=False)

    if not isinstance(parsed, dict):
        return InboxCompanyMap(configured=True, valid=False)

    # Любой распарсенный пустой объект — независимо от форматирования — означает
    # «map не настроена» и не должен отключать documented fallback.
    if not parsed:
        return InboxCompanyMap(configured=False, valid=True)

    mapping: dict[int, int] = {}
    inverse_mapping: dict[int, int] = {}
    for key, company in parsed.items():
        inbox_id = _canonical_inbox_key(key)
        if inbox_id is None:
            return InboxCompanyMap(configured=True, valid=False)
        if type(company) is not int or not (0 < company <= PG_INT_MAX):
            return InboxCompanyMap(configured=True, valid=False)
        if inbox_id in mapping:  # defense-in-depth против коллизий ключей
            return InboxCompanyMap(configured=True, valid=False)
        if company in inverse_mapping:
            return InboxCompanyMap(configured=True, valid=False)
        mapping[inbox_id] = company
        inverse_mapping[company] = inbox_id

    return InboxCompanyMap(
        configured=True,
        valid=True,
        mapping=mapping,
        inverse_mapping=inverse_mapping,
    )


def nonempty_str(value: object) -> str | None:
    """Возвращает непустую строку как есть, иначе ``None``.

    Для sender-controlled идентификаторов, уходящих в SQL против String-колонки
    (например ``metadata.phone_number_id``): ``dict``/``list``/``bool``/число
    нельзя биндить как строковый параметр — драйвер упал бы. Значение НЕ
    обрезается, чтобы точное сравнение с сохранённым id не менялось.
    """
    if isinstance(value, str) and value.strip():
        return value
    return None


def postgres_safe_json_value(value: object) -> object:
    """Рекурсивно приводит разобранный JSON к виду, который примет Postgres JSONB.

    Контракт: результат ГАРАНТИРОВАННО сериализуется через
    ``json.dumps(..., allow_nan=False)`` и переживает INSERT в JSONB. Это нужно
    для сохраняемой проекции payload: тело вебхука контролируется отправителем,
    ``json.loads`` спокойно принимает и NUL (``\\u0000``), и непарные суррогаты, и
    нестандартные литералы ``NaN``/``Infinity``/``-Infinity``, а INSERT в JSONB
    на них падает — то есть один дефектный вебхук иначе превращается в
    бесконечную серию 500 на каждом ретрае.

    Правила преобразования:
      * ``str`` (в том числе строковые ключи) → :func:`postgres_safe_text`;
      * не-finite ``float`` (NaN / ±Infinity) → ``None``. Политика единая для
        всех эндпоинтов, использующих этот helper: значение недоступно для JSONB,
        а ``None`` — единственное представление, которое не выдаёт себя за
        осмысленное число и не ломает читателей колонки;
      * ``dict``/``list``/``tuple`` — рекурсивно (tuple → list);
      * ``int``, ``bool``, ``None`` и finite ``float`` — без изменений.

    Исходный объект НЕ мутируется: оригинал ещё нужен для дедуп-ключей и уже
    посчитанных подписей.

    Родственный :func:`postgres_safe_json_hash` решает ту же задачу иначе — он
    ОТКЛОНЯЕТ такое значение вместо преобразования. Это осознанное различие:
    EasyWeek-capture хранит исходные байты в ``body_raw`` и может позволить себе
    откатиться в текст, а у Altegio/Chatwoot/WhatsApp сырой колонки нет, поэтому
    единственный способ не потерять доставку — сохранить безопасную проекцию.
    """
    if isinstance(value, str):
        return postgres_safe_text(value)
    if isinstance(value, float) and not math.isfinite(value):
        # NaN / Infinity / -Infinity: Postgres JSONB их не принимает.
        return None
    if isinstance(value, dict):
        return {
            (postgres_safe_text(k) if isinstance(k, str) else k): postgres_safe_json_value(v) for k, v in value.items()
        }
    if isinstance(value, (list, tuple)):
        # tuple → list: JSON-совместимое представление.
        return [postgres_safe_json_value(item) for item in value]
    return value


def optional_int(value: object, *, bigint: bool = True, min_value: int | None = None) -> int | None:
    """Приводит sender-controlled значение к int для числовой колонки, иначе None.

    ``bool`` НЕ считается целым числом (в Python ``True`` — это ``int``, но в
    колонке ``company_id`` он был бы бессмыслицей). Строка принимается, только
    если это целое число целиком. Значение вне диапазона колонки отбрасывается:
    Postgres на нём падает, а доставку терять нельзя — payload всё равно
    сохраняется целиком в JSONB, поэтому NULL в scalar-проекции ничего не теряет.

    ``min_value`` поднимает нижнюю границу (см. :func:`optional_chatwoot_id`).

    Преобразование строки обёрнуто в ``try``, а не защищено ``isdigit()``:
    ``"9" * 5000`` проходит ``isdigit()``, но с Python 3.11 ``int()`` бросает
    ``ValueError`` из-за лимита на длину десятичной записи — то есть корректно
    подписанный вебхук снова стал бы необработанным 500.
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        candidate = value
    elif isinstance(value, str):
        try:
            candidate = int(value.strip())
        except (TypeError, ValueError):
            return None
    else:
        return None

    low, high = (PG_BIGINT_MIN, PG_BIGINT_MAX) if bigint else (PG_INT_MIN, PG_INT_MAX)
    if min_value is not None:
        low = max(low, min_value)
    return candidate if low <= candidate <= high else None


def optional_chatwoot_id(value: object) -> int | None:
    """Chatwoot conversation/message id для BIGINT-колонки.

    Chatwoot IDs are intentionally restricted to non-negative BIGINT values.
    Negative numeric strings are rejected rather than coerced.

    Отдельная обёртка над :func:`optional_int`, потому что общий контракт
    допускает отрицательные значения (например Altegio ``company_id``), а
    отрицательный id беседы в Chatwoot смысла не имеет и почти наверняка означает
    подделанное тело.
    """
    return optional_int(value, bigint=True, min_value=0)


def bounded_text(value: object, *, limit: int) -> str | None:
    """Postgres-безопасная строковая проекция, обрезанная под длину колонки.

    ``None`` остаётся ``None``; не-строки приводятся к ``str`` (числовой
    ``resource_id``-подобный ввод не должен ронять запись). Обрезка идёт ПОСЛЕ
    санитизации, чтобы длина считалась по тому, что реально уедет в колонку.
    """
    if value is None:
        return None
    text = value if isinstance(value, str) else str(value)
    return postgres_safe_text(text)[:limit]


def bounded_dedupe_key(prefix: str, *parts: object, limit: int = 128) -> str:
    """Собирает dedupe-ключ, гарантированно помещающийся в ``VARCHAR(128)``.

    Части приводятся к Postgres-безопасному тексту. Если ключ не влезает в
    колонку (sender-controlled id может быть сколь угодно длинным), хвост
    заменяется на sha256 — ключ остаётся детерминированным и уникальным, но
    ограниченным. Для корректных коротких id результат ПОБАЙТОВО совпадает с
    прежней f-string, поэтому исторические ключи не меняются.
    """
    key = ":".join([prefix, *(postgres_safe_text(str(p)) for p in parts)])
    if len(key) <= limit:
        return key
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
    return f"{prefix}:sha256:{digest}"[:limit]


# Маркер, которым помечается обрезанное значение в логах.
_LOG_TRUNCATION_MARKER = "...<truncated>"


def _escape_for_log(value: str, *, limit: int) -> str:
    """Однострочное экранированное представление, ограниченное ПО ИТОГОВОЙ длине.

    Лимит применяется к экранированному результату, а не к исходной строке:
    один astral-символ разворачивается в две ``\\uXXXX``-последовательности (12
    символов), поэтому обрезка входа не давала бы никакой гарантии на размер
    лога. Сборка идёт посимвольно, чтобы не разрезать escape-последовательность
    пополам, и результат всегда остаётся синтаксически однозначным.
    """
    escaped = json.dumps(value, ensure_ascii=True)
    if len(escaped) <= limit:
        return escaped

    # Две кавычки + маркер обрезки должны поместиться в лимит.
    budget = limit - 2 - len(_LOG_TRUNCATION_MARKER)
    if budget <= 0:
        return '"' + _LOG_TRUNCATION_MARKER[: max(limit - 2, 0)] + '"'

    out: list[str] = []
    used = 0
    for ch in value:
        piece = json.dumps(ch, ensure_ascii=True)[1:-1]
        if used + len(piece) > budget:
            break
        out.append(piece)
        used += len(piece)

    return '"' + "".join(out) + _LOG_TRUNCATION_MARKER + '"'


def safe_log_path(path: str, *, limit: int = 2048) -> str:
    """Экранированный и ограниченный по длине путь, пригодный для записи в лог.

    Путь полностью контролируется клиентом и не требует валидного токена, то
    есть это классический вектор log injection: перевод строки подделал бы
    отдельную log-строку, ANSI-escape перекрасил бы вывод, а U+2028/U+2029
    разорвали бы строку в части просмотрщиков. Экранирование закрывает всё
    разом; длина ограничена по итоговому результату.
    """
    return _escape_for_log(path, limit=limit)


def safe_log_value(value: object, *, limit: int = 256) -> str:
    """То же для произвольного значения, попадающего в application-лог.

    Значения вебхуков (event, content_type, sender_type, id) приходят от
    отправителя и не валидируются до логирования — их нельзя писать дословно.
    Helper ничего не раскрывает сверх того, что ему передали: минимизация
    данных — обязанность вызывающего кода.
    """
    return _escape_for_log(value if isinstance(value, str) else str(value), limit=limit)


def contains_nul(value: object) -> bool:
    """Рекурсивно ищет NUL в разобранном JSON-значении (ключи и значения).

    Проверять результат ``json.dumps`` бесполезно: он экранирует NUL в
    ``\\u0000``, и подстрока ``"\\x00"`` там никогда не встретится, а вот
    Postgres JSONB такое значение отвергнет уже на commit.
    """
    if isinstance(value, str):
        return "\x00" in value
    if isinstance(value, dict):
        return any(contains_nul(key) or contains_nul(item) for key, item in value.items())
    if isinstance(value, (list, tuple)):
        return any(contains_nul(item) for item in value)
    return False


def _canonical_json(payload: object, *, allow_nan: bool) -> str:
    """Канонизированный JSON: sort_keys + компактные разделители, без ASCII-escape.

    Единственная точка сериализации для обоих хэшей ниже, чтобы канон-форма (а
    значит и хэш корректных payload) не могла разойтись между ними.
    """
    return json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=allow_nan,
    )


def canonical_json_hash(payload: object) -> str:
    """sha256 канонизированного JSON. Чистое хэширование, без JSONB-гарантий.

    Не проверяет пригодность значения к записи в Postgres: NaN/Infinity
    сериализуются как есть (``allow_nan=True``). Для payload, который будет
    сохранён в JSONB, используйте :func:`postgres_safe_json_hash` — он отвергает
    то, что БД не примет.
    """
    return hashlib.sha256(_canonical_json(payload, allow_nan=True).encode("utf-8")).hexdigest()


def postgres_safe_json_hash(payload: object) -> str:
    """sha256 канонизированного JSON с контрактом «переживёт запись в Postgres JSONB».

    Бросает ``ValueError`` (или его подкласс) на всём, что JSONB не принимает,
    ЕДИНООБРАЗНО — вне зависимости от вида дефекта:
      * NaN / Infinity / переполнение экспоненты — через ``allow_nan=False``;
      * NUL в строках и ключах — через ``contains_nul`` (``json.dumps`` экранирует
        NUL, поэтому проверять сериализованную строку бесполезно);
      * непарные суррогаты — через ``UnicodeEncodeError`` на ``encode("utf-8")``
        (это подкласс ``ValueError``).
    Вызывающие, которые сохраняют канонизированный payload в JSONB, обязаны
    использовать именно эту функцию: иначе запись упала бы на commit, а доставка
    терялась бы на каждом ретрае.
    """
    if contains_nul(payload):
        raise ValueError("payload contains NUL, which PostgreSQL JSONB rejects")

    return hashlib.sha256(_canonical_json(payload, allow_nan=False).encode("utf-8")).hexdigest()


async def read_bounded_body(request: Request, *, limit: int) -> tuple[bytes, int, bool]:
    """Читает тело потоком, удерживая в памяти не более ``limit`` байт.

    Возвращает ``(prefix, total_size, truncated)``:
      * ``prefix`` — первые ``limit`` байт тела (или всё тело, если оно меньше);
      * ``total_size`` — полный размер фактически полученной доставки;
      * ``truncated`` — превышен ли лимит.

    В отличие от ``await request.body()`` пиковая память ограничена ``limit``
    плюс текущий chunk, а не полным размером запроса: иначе несколько
    параллельных гигантских доставок выжрали бы память процесса до применения
    лимита. Поток дочитывается до конца даже после превышения лимита — оборвать
    его на середине означало бы отдать ответ по недочитанному запросу.
    ``Content-Length`` сознательно не используется: он может отсутствовать
    (chunked) или врать.
    """
    prefix = bytearray()
    total_size = 0
    async for chunk in request.stream():
        total_size += len(chunk)
        if len(prefix) < limit:
            prefix.extend(chunk[: limit - len(prefix)])
    return bytes(prefix), total_size, total_size > limit
