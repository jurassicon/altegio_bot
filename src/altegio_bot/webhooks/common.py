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
    сама проверка deny в любом случае case-insensitive.
    """
    deny = _BASE_HEADER_DENY | frozenset(extra_deny)
    return {k: v for k, v in request.headers.items() if k.lower() not in deny}


def is_sensitive_query_key(key: str) -> bool:
    """True, если имя query-параметра выглядит как носитель секрета."""
    low = key.lower()
    return any(marker in low for marker in _SENSITIVE_QUERY_SUBSTRINGS)


def mask_query(query: dict[str, str]) -> dict[str, str]:
    """Маскирует значения чувствительных query-параметров (``secret``/``token``…)."""
    return {k: "***" if is_sensitive_query_key(k) else v for k, v in query.items()}


def canonical_json_hash(payload: object, *, strict: bool = False) -> str:
    """sha256 канонизированного JSON (sort_keys, компактные разделители).

    ``strict=True`` передаёт ``allow_nan=False``: бросает ``ValueError`` на
    NaN/Infinity/переполнении вместо того, чтобы протащить токены, которые
    Postgres JSONB не принимает. Вызывающие, которые сохраняют канонизированный
    payload в JSONB (а не только хэшируют его), обязаны передавать ``strict=True``.
    """
    canon = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=not strict,
    )
    return hashlib.sha256(canon.encode("utf-8")).hexdigest()
