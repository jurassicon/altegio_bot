"""Unit tests for the shared webhook helpers.

Focus: the two now-separate JSON hash functions have honest, distinct contracts
and never disagree on a payload both accept.
"""

from __future__ import annotations

import hashlib
import json

import pytest

from altegio_bot.webhooks.common import (
    canonical_json_hash,
    contains_nul,
    postgres_safe_json_hash,
    postgres_safe_text,
)


def _sha(payload: object) -> str:
    canon = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canon.encode("utf-8")).hexdigest()


def test_canonical_json_hash_is_stable_and_key_order_independent() -> None:
    a = canonical_json_hash({"a": 1, "b": 2})
    b = canonical_json_hash({"b": 2, "a": 1})
    assert a == b == _sha({"a": 1, "b": 2})


def test_canonical_json_hash_allows_nan() -> None:
    """The plain hash makes no JSONB promise and must not raise on NaN."""
    assert canonical_json_hash({"x": float("nan")})  # does not raise


def test_both_hashes_agree_on_a_clean_payload() -> None:
    payload = {"id": 7, "nested": {"z": [1, 2, 3]}, "s": "ok"}
    assert canonical_json_hash(payload) == postgres_safe_json_hash(payload)


@pytest.mark.parametrize(
    "payload",
    [
        {"x": float("nan")},
        {"x": float("inf")},
        {"comment": "a\x00b"},  # NUL in a value
        {"a\x00b": 1},  # NUL in a key
        {"x": "\ud800"},  # lone surrogate — UnicodeEncodeError (a ValueError)
    ],
)
def test_postgres_safe_json_hash_rejects_jsonb_hostile_content(payload: object) -> None:
    with pytest.raises(ValueError):
        postgres_safe_json_hash(payload)


def test_contains_nul_walks_keys_and_nested_containers() -> None:
    assert contains_nul({"ok": ["fine", {"deep": "a\x00b"}]}) is True
    assert contains_nul({"ok": ["fine", {"deep": "clean"}]}) is False
    assert contains_nul({"a\x00": 1}) is True


def test_postgres_safe_text_replaces_nul_and_surrogates() -> None:
    assert "\x00" not in postgres_safe_text("a\x00b")
    # A lone surrogate must be coerced to something UTF-8-encodable.
    postgres_safe_text("\ud800").encode("utf-8")  # does not raise
    assert postgres_safe_text("clean") == "clean"
