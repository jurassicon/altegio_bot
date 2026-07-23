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
from collections.abc import Iterable

from fastapi import Request

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


def canonical_json_hash(payload: object, *, strict: bool = False) -> str:
    """sha256 канонизированного JSON (sort_keys, компактные разделители).

    ``strict=True`` — контракт «это значение переживёт запись в Postgres JSONB».
    Бросает ``ValueError`` (или его подкласс) на всём, что JSONB не принимает:
      * NaN / Infinity / переполнение экспоненты — через ``allow_nan=False``;
      * NUL в строках и ключах — через ``contains_nul``;
      * непарные суррогаты — через ``UnicodeEncodeError`` на ``encode("utf-8")``
        (это подкласс ``ValueError``, ловится тем же except).
    Без strict остаётся чистым хэшированием. Вызывающие, которые сохраняют
    канонизированный payload в JSONB (а не только хэшируют его), обязаны
    передавать ``strict=True``: иначе запись упала бы на commit, а доставка
    терялась бы на каждом ретрае.
    """
    if strict and contains_nul(payload):
        raise ValueError("payload contains NUL, which PostgreSQL JSONB rejects")

    canon = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=not strict,
    )
    return hashlib.sha256(canon.encode("utf-8")).hexdigest()


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
