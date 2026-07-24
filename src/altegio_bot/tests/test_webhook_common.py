"""Unit tests for the shared webhook helpers.

Focus: the two now-separate JSON hash functions have honest, distinct contracts
and never disagree on a payload both accept.
"""

from __future__ import annotations

import hashlib
import json

import pytest

from altegio_bot.webhooks.common import (
    PG_BIGINT_MAX,
    PG_INT_MAX,
    bounded_dedupe_key,
    bounded_text,
    canonical_json_hash,
    contains_nul,
    optional_int,
    postgres_safe_json_hash,
    postgres_safe_json_value,
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


# ---------------------------------------------------------------------------
# postgres_safe_json_value: the result must always be JSONB-writable
# ---------------------------------------------------------------------------

NUL = chr(0)
LONE_SURROGATE = chr(0xD800)


@pytest.mark.parametrize(
    "hostile",
    [
        {"x": float("nan")},
        {"x": float("inf")},
        {"x": float("-inf")},
        {"x": [float("nan"), 1.5]},
        {"x": {"deep": float("inf")}},
        {"x": f"a{NUL}b"},
        {"x": LONE_SURROGATE},
        {f"key{NUL}": 1},
    ],
)
def test_safe_json_value_output_is_always_jsonb_writable(hostile: dict) -> None:
    safe = postgres_safe_json_value(hostile)
    # allow_nan=False is exactly what PostgreSQL JSONB accepts.
    encoded = json.dumps(safe, allow_nan=False)
    encoded.encode("utf-8")
    assert NUL not in encoded


def test_non_finite_floats_become_none() -> None:
    assert postgres_safe_json_value({"a": float("nan")}) == {"a": None}
    assert postgres_safe_json_value({"a": float("inf")}) == {"a": None}
    assert postgres_safe_json_value({"a": float("-inf")}) == {"a": None}


def test_finite_numbers_bools_and_none_are_untouched() -> None:
    payload = {"i": 42, "f": 1.5, "t": True, "f2": False, "n": None, "zero": 0}
    assert postgres_safe_json_value(payload) == payload


def test_clean_nested_payload_is_unchanged() -> None:
    payload = {"a": {"b": ["x", 1, {"c": "ok"}]}, "d": "gr\u00fc\u00df"}
    assert postgres_safe_json_value(payload) == payload


def test_tuples_become_lists() -> None:
    assert postgres_safe_json_value({"a": (1, "x")}) == {"a": [1, "x"]}


def test_original_object_is_not_mutated() -> None:
    original = {"s": f"a{NUL}b", "f": float("nan"), "nested": {"list": [f"c{NUL}"]}}
    snapshot_keys = set(original)
    safe = postgres_safe_json_value(original)

    assert original["s"] == f"a{NUL}b"  # untouched
    assert original["nested"]["list"] == [f"c{NUL}"]
    assert set(original) == snapshot_keys
    assert safe is not original
    assert safe["nested"] is not original["nested"]


# ---------------------------------------------------------------------------
# Scalar projections
# ---------------------------------------------------------------------------


def test_optional_int_accepts_ints_and_digit_strings() -> None:
    assert optional_int(5) == 5
    assert optional_int("42") == 42
    assert optional_int("  7 ") == 7
    assert optional_int(-3) == -3
    assert optional_int("-3") == -3


def test_optional_int_rejects_bool_and_garbage() -> None:
    assert optional_int(True) is None
    assert optional_int(False) is None
    assert optional_int("not-an-integer") is None
    assert optional_int(None) is None
    assert optional_int(1.5) is None
    assert optional_int({"a": 1}) is None


def test_optional_int_enforces_column_range() -> None:
    assert optional_int(PG_INT_MAX, bigint=False) == PG_INT_MAX
    assert optional_int(PG_INT_MAX + 1, bigint=False) is None
    assert optional_int(PG_BIGINT_MAX) == PG_BIGINT_MAX
    assert optional_int(PG_BIGINT_MAX + 1) is None


def test_bounded_text_sanitises_then_truncates() -> None:
    assert bounded_text("record", limit=32) == "record"
    assert bounded_text(None, limit=32) is None
    assert len(bounded_text("x" * 100, limit=32)) == 32
    assert NUL not in bounded_text(f"a{NUL}b", limit=32)
    bounded_text(LONE_SURROGATE, limit=32).encode("utf-8")
    # Non-strings are coerced rather than crashing the insert.
    assert bounded_text(123, limit=32) == "123"


def test_bounded_dedupe_key_is_stable_for_valid_ids() -> None:
    """Short, valid ids must keep byte-identical historical keys."""
    assert bounded_dedupe_key("chatwoot", 99, 1) == "chatwoot:99:1"
    assert bounded_dedupe_key("chatwoot_out", 701, 7001) == "chatwoot_out:701:7001"


def test_bounded_dedupe_key_bounds_hostile_ids() -> None:
    key = bounded_dedupe_key("chatwoot", "x" * 500, 1)
    assert len(key) <= 128
    assert key.startswith("chatwoot:sha256:")
    # Deterministic and collision-resistant.
    assert key == bounded_dedupe_key("chatwoot", "x" * 500, 1)
    assert key != bounded_dedupe_key("chatwoot", "y" * 500, 1)


def test_bounded_dedupe_key_strips_nul() -> None:
    assert NUL not in bounded_dedupe_key("chatwoot", f"a{NUL}b", 1)
