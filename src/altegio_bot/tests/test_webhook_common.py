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
    mapping_or_empty,
    optional_chatwoot_id,
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


# ---------------------------------------------------------------------------
# mapping_or_empty
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("bad", [[], [1, 2], "text", 123, 1.5, None, True])
def test_mapping_or_empty_replaces_non_mappings(bad: object) -> None:
    """`x or {}` is not enough: a non-empty list is truthy and then .get() blows up."""
    assert mapping_or_empty(bad) == {}


def test_mapping_or_empty_passes_through_dicts() -> None:
    d = {"id": 1}
    assert mapping_or_empty(d) is d
    assert mapping_or_empty({}) == {}


# ---------------------------------------------------------------------------
# optional_chatwoot_id: non-negative BIGINT only
# ---------------------------------------------------------------------------


def test_optional_chatwoot_id_accepts_non_negative() -> None:
    assert optional_chatwoot_id(42) == 42
    assert optional_chatwoot_id("42") == 42
    assert optional_chatwoot_id(0) == 0
    assert optional_chatwoot_id(PG_BIGINT_MAX) == PG_BIGINT_MAX


def test_optional_chatwoot_id_rejects_negative() -> None:
    """Deliberate: a negative Chatwoot id is meaningless, so it is not coerced."""
    assert optional_chatwoot_id(-42) is None
    assert optional_chatwoot_id("-42") is None


def test_optional_chatwoot_id_rejects_bool_and_garbage() -> None:
    assert optional_chatwoot_id(True) is None
    assert optional_chatwoot_id(False) is None
    assert optional_chatwoot_id("abc") is None
    assert optional_chatwoot_id(None) is None
    assert optional_chatwoot_id(1.5) is None
    assert optional_chatwoot_id({"a": 1}) is None
    assert optional_chatwoot_id([1]) is None


def test_optional_chatwoot_id_survives_a_5000_digit_string() -> None:
    """isdigit() passes but int() raises ValueError past the decimal limit."""
    assert optional_chatwoot_id("9" * 5000) is None


def test_optional_chatwoot_id_rejects_out_of_range() -> None:
    assert optional_chatwoot_id(2**70) is None
    assert optional_chatwoot_id(PG_BIGINT_MAX + 1) is None


def test_optional_int_survives_a_5000_digit_string() -> None:
    assert optional_int("9" * 5000) is None


def test_optional_int_min_value_bound() -> None:
    assert optional_int(-1, min_value=0) is None
    assert optional_int(0, min_value=0) == 0


# ---------------------------------------------------------------------------
# normalize_phone_candidate / nonempty_str
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("+4915112345678", "+4915112345678"),
        ("+49 151 123-45-67", "+491511234567"),
        ("0049 151 1234567", "+00491511234567"),
        ("(030) 12345", "+03012345"),
        ("15112345678", "+15112345678"),
    ],
)
def test_normalize_phone_candidate_valid(raw: str, expected: str) -> None:
    from altegio_bot.webhooks.common import normalize_phone_candidate

    assert normalize_phone_candidate(raw) == expected


@pytest.mark.parametrize("raw", ["abc", "+", "---", "()", "no phone", "", "   "])
def test_normalize_phone_candidate_digitless_is_none(raw: str) -> None:
    from altegio_bot.webhooks.common import normalize_phone_candidate

    assert normalize_phone_candidate(raw) is None


@pytest.mark.parametrize("raw", [[], {}, 123, True, False, 1.5, None, ["+49"], {"x": 1}])
def test_normalize_phone_candidate_non_string_is_none(raw: object) -> None:
    from altegio_bot.webhooks.common import normalize_phone_candidate

    # Must never raise on a non-string leaf.
    assert normalize_phone_candidate(raw) is None


@pytest.mark.parametrize("raw", ["1" * 16, "1" * 40, "+" + "9" * 16])
def test_normalize_phone_candidate_overlong_is_none(raw: str) -> None:
    """More than 15 ASCII digits cannot be E.164 and would overflow downstream."""
    from altegio_bot.webhooks.common import normalize_phone_candidate

    assert normalize_phone_candidate(raw) is None


@pytest.mark.parametrize("raw", ["１２３４５", "١٢٣٤٥"])
def test_normalize_phone_candidate_unicode_digits_are_none(raw: str) -> None:
    """Only ASCII [0-9] count; a string with no ASCII digits → None."""
    from altegio_bot.webhooks.common import normalize_phone_candidate

    assert normalize_phone_candidate(raw) is None


def test_normalize_phone_candidate_length_boundary() -> None:
    from altegio_bot.webhooks.common import normalize_phone_candidate

    assert normalize_phone_candidate("1" * 15) == "+" + "1" * 15  # 15 accepted
    assert normalize_phone_candidate("1" * 16) is None  # 16 rejected


@pytest.mark.parametrize("raw", ["49١٢٣15", "+49١٥١١٢٣４５６７", "+49 151 １２34567"])
def test_normalize_phone_candidate_mixed_unicode_digits_are_rejected(raw: str) -> None:
    """A string with ANY non-ASCII decimal digit is rejected whole — silent
    deletion of "49١٢٣15" → "+4915" would send to a different recipient."""
    from altegio_bot.webhooks.common import normalize_phone_candidate

    assert normalize_phone_candidate(raw) is None


@pytest.mark.parametrize("raw", ["PNID_1", "  x  "])
def test_nonempty_str_keeps_value(raw: str) -> None:
    from altegio_bot.webhooks.common import nonempty_str

    assert nonempty_str(raw) == raw  # not stripped — exact SQL match preserved


@pytest.mark.parametrize("raw", ["", "   ", [], {}, 123, True, False, 1.5, None, ["x"]])
def test_nonempty_str_rejects_non_string_and_blank(raw: object) -> None:
    from altegio_bot.webhooks.common import nonempty_str

    assert nonempty_str(raw) is None


# ---------------------------------------------------------------------------
# classify_message_type / positive_int / parse_chatwoot_inbox_company_map
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value,expected",
    [
        (0, "incoming"),
        (1, "outgoing"),
        ("incoming", "incoming"),
        ("outgoing", "outgoing"),
        (True, None),
        (False, None),
        (1.0, None),
        (0.0, None),
        ([], None),
        ({}, None),
        (None, None),
        ("1", None),
        ("0", None),
        (2, None),
        (-1, None),
    ],
)
def test_classify_message_type_exact(value: object, expected: str | None) -> None:
    from altegio_bot.webhooks.common import classify_message_type

    assert classify_message_type(value) == expected


@pytest.mark.parametrize("value", [1, 5, 42, 2**31 - 1])
def test_positive_int_accepts(value: int) -> None:
    from altegio_bot.webhooks.common import positive_int

    assert positive_int(value) == value


@pytest.mark.parametrize("value", [True, False, 1.9, "1", None, [], {}, 0, -7, 2**31])
def test_positive_int_rejects(value: object) -> None:
    from altegio_bot.webhooks.common import positive_int

    assert positive_int(value) is None


@pytest.mark.parametrize("raw", ["", "   ", "{}"])
def test_inbox_map_not_configured(raw: str) -> None:
    from altegio_bot.webhooks.common import parse_chatwoot_inbox_company_map

    m = parse_chatwoot_inbox_company_map(raw)
    assert m.configured is False
    assert m.valid is True
    assert m.mapping == {}


@pytest.mark.parametrize(
    "raw",
    [
        "null",
        "42",
        "1.9",
        "true",
        "[]",
        '"string"',
        "{not json",
        '{"42": null}',
        '{"42": true}',
        '{"42": false}',
        '{"42": 1.9}',
        '{"42": "1"}',
        '{"42": "token=SECRETVAL"}',
        '{"42": []}',
        '{"42": {}}',
        '{"42": 0}',
        '{"42": -7}',
        '{"42": ' + str(2**31) + "}",  # PG_INT_MAX + 1
        '{"": 7}',
        '{"0": 7}',
        '{"-1": 7}',
        '{"+8": 7}',
        '{"8.0": 7}',
        '{"abc": 7}',
    ],
)
def test_inbox_map_invalid(raw: str) -> None:
    from altegio_bot.webhooks.common import parse_chatwoot_inbox_company_map

    m = parse_chatwoot_inbox_company_map(raw)
    assert m.configured is True
    assert m.valid is False
    assert m.mapping == {}


def test_inbox_map_valid() -> None:
    from altegio_bot.webhooks.common import parse_chatwoot_inbox_company_map

    m = parse_chatwoot_inbox_company_map('{"8": 758285, "42": 1271200}')
    assert m.configured is True
    assert m.valid is True
    assert m.provider_scoped is False
    assert m.mapping == {}
    assert m.inverse_mapping == {}
    assert m.legacy_mapping == {8: 758285, 42: 1271200}


def test_inbox_map_three_branches_has_one_bidirectional_source() -> None:
    from altegio_bot.webhooks.common import ChatwootTenantIdentity, parse_chatwoot_inbox_company_map

    m = parse_chatwoot_inbox_company_map(
        '{"101":{"provider":"easyweek","company_id":900001},'
        '"102":{"provider":"easyweek","company_id":900002},'
        '"103":{"provider":"altegio","company_id":900003}}'
    )
    assert m.configured is True
    assert m.valid is True
    assert m.provider_scoped is True
    assert m.mapping == {
        101: ChatwootTenantIdentity("easyweek", 900001),
        102: ChatwootTenantIdentity("easyweek", 900002),
        103: ChatwootTenantIdentity("altegio", 900003),
    }
    assert m.inverse_mapping == {identity: inbox for inbox, identity in m.mapping.items()}


def test_general_inbox_must_be_separate_from_all_branch_inboxes() -> None:
    from altegio_bot.webhooks.common import (
        parse_chatwoot_inbox_company_map,
        resolve_chatwoot_general_inbox,
    )

    parsed = parse_chatwoot_inbox_company_map(
        '{"101":{"provider":"easyweek","company_id":900001},'
        '"102":{"provider":"easyweek","company_id":900002},'
        '"103":{"provider":"altegio","company_id":900003}}'
    )
    assert resolve_chatwoot_general_inbox(parsed, 999) == (999, None)
    assert resolve_chatwoot_general_inbox(parsed, 101) == (None, "general_inbox_overlaps_branch")


@pytest.mark.parametrize(
    ("raw_map", "general_id", "expected_reason"),
    [
        ("{not json", 999, "invalid_inbox_company_map"),
        ('{"101":900001}', 999, "provider_scope_missing"),
        ('{"101":{"provider":"easyweek","company_id":900001}}', 0, "invalid_general_inbox_id"),
        ('{"101":{"provider":"easyweek","company_id":900001}}', "999", "invalid_general_inbox_id"),
    ],
)
def test_general_inbox_validation_has_stable_fail_closed_reasons(
    raw_map: str,
    general_id: object,
    expected_reason: str,
) -> None:
    from altegio_bot.webhooks.common import (
        parse_chatwoot_inbox_company_map,
        resolve_chatwoot_general_inbox,
    )

    parsed = parse_chatwoot_inbox_company_map(raw_map)
    assert resolve_chatwoot_general_inbox(parsed, general_id) == (None, expected_reason)


def test_general_inbox_empty_map_preserves_legacy_single_inbox_mode() -> None:
    from altegio_bot.webhooks.common import (
        parse_chatwoot_inbox_company_map,
        resolve_chatwoot_general_inbox,
    )

    parsed = parse_chatwoot_inbox_company_map("{}")
    assert resolve_chatwoot_general_inbox(parsed, 0) == (None, None)


def test_inbox_map_duplicate_company_is_invalid_for_both_directions() -> None:
    from altegio_bot.webhooks.common import parse_chatwoot_inbox_company_map

    m = parse_chatwoot_inbox_company_map(
        '{"101":{"provider":"easyweek","company_id":900001},"102":{"provider":"easyweek","company_id":900001}}'
    )
    assert m.configured is True
    assert m.valid is False
    assert m.mapping == {}
    assert m.inverse_mapping == {}


def test_same_numeric_company_in_different_providers_is_two_tenants() -> None:
    from altegio_bot.webhooks.common import ChatwootTenantIdentity, parse_chatwoot_inbox_company_map

    m = parse_chatwoot_inbox_company_map(
        '{"101":{"provider":"easyweek","company_id":900001},"102":{"provider":"altegio","company_id":900001}}'
    )
    assert m.valid is True
    assert m.inverse_mapping == {
        ChatwootTenantIdentity("easyweek", 900001): 101,
        ChatwootTenantIdentity("altegio", 900001): 102,
    }


@pytest.mark.parametrize(
    "raw",
    [
        '{"8":{"provider":"unknown","company_id":1}}',
        '{"8":{"provider":"easyweek","company_id":"1"}}',
        '{"8":{"provider":"easyweek","company_id":1,"extra":true}}',
        '{"8":{"company_id":1}}',
        '{"8":1,"9":{"provider":"altegio","company_id":2}}',
    ],
)
def test_provider_scoped_map_rejects_invalid_or_mixed_values(raw: str) -> None:
    from altegio_bot.webhooks.common import parse_chatwoot_inbox_company_map

    parsed = parse_chatwoot_inbox_company_map(raw)
    assert parsed.configured is True
    assert parsed.valid is False


def test_inbox_map_parser_never_logs_raw_config(caplog: pytest.LogCaptureFixture) -> None:
    from altegio_bot.webhooks.common import parse_chatwoot_inbox_company_map

    secret_marker = "raw-map-secret-marker"
    parse_chatwoot_inbox_company_map('{"101": "' + secret_marker + '"}')
    assert secret_marker not in caplog.text


def test_inbox_map_non_string_input_is_invalid() -> None:
    from altegio_bot.webhooks.common import parse_chatwoot_inbox_company_map

    # A non-str (e.g. already-parsed object) is a configuration bug → invalid,
    # never "not configured".
    m = parse_chatwoot_inbox_company_map({"8": 1})
    assert m.configured is True
    assert m.valid is False


# ---------------------------------------------------------------------------
# Closed phone grammar: no character outside the supported set is ever cleaned
# ---------------------------------------------------------------------------

_ZWSP = chr(0x200B)


@pytest.mark.parametrize(
    "raw",
    [
        "+49 151 O23 4567",  # letter O instead of zero
        "+49 151 ext 23",  # extension text
        "4915abc123",  # letters
        "+49☎1511234567",  # ☎ telephone symbol
        "+49\U0001f6421511234567",  # emoji
        "49" + _ZWSP + "1511234567",  # zero-width space
        "49\n1511234567",  # LF
        "49\r1511234567",  # CR
        "49\t1511234567",  # tab (not an allowed separator)
        "49+1511234567",  # '+' after a digit
        "++491511234567",  # two '+'
        "+49*1511234567",  # '*' not in grammar
        "+49,1511234567",  # ',' not in grammar
        "49١٢٣15",  # mixed ASCII + Arabic-Indic digits
    ],
)
def test_normalize_phone_candidate_closed_grammar_rejects(raw: str) -> None:
    from altegio_bot.webhooks.common import normalize_phone_candidate

    assert normalize_phone_candidate(raw) is None


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("+49/151/1234567", "+491511234567"),
        ("+49.151.1234567", "+491511234567"),
        ("+49 (151) 123-45-67", "+491511234567"),
    ],
)
def test_normalize_phone_candidate_allowed_separators(raw: str, expected: str) -> None:
    from altegio_bot.webhooks.common import normalize_phone_candidate

    assert normalize_phone_candidate(raw) == expected


# ---------------------------------------------------------------------------
# Map parser: duplicate keys, normalized-key collisions, empty-object, totality
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw",
    [
        '{"8": 1, "8": 2}',  # duplicate raw key, different value
        '{"42": 1, "42": 1}',  # duplicate raw key, same value — still ambiguous
        '{"8": 1, "8\n": 2}',  # newline-colliding key ("8\n" -> 8 without fullmatch)
        '{"8": 1, "08": 2}',  # leading-zero key ("08" is invalid AND would collide)
    ],
)
def test_inbox_map_duplicate_and_collision_invalid(raw: str) -> None:
    from altegio_bot.webhooks.common import parse_chatwoot_inbox_company_map

    m = parse_chatwoot_inbox_company_map(raw)
    assert m.configured is True
    assert m.valid is False
    assert m.mapping == {}


@pytest.mark.parametrize("raw", ["{}", "{ }", "{\n}", "{\r\n    }", "{\t}"])
def test_inbox_map_empty_object_any_formatting_is_unconfigured(raw: str) -> None:
    from altegio_bot.webhooks.common import parse_chatwoot_inbox_company_map

    m = parse_chatwoot_inbox_company_map(raw)
    assert m.configured is False
    assert m.valid is True
    assert m.mapping == {}
    assert m.inverse_mapping == {}


def test_inbox_map_key_range_and_totality() -> None:
    from altegio_bot.webhooks.common import PG_INT_MAX, parse_chatwoot_inbox_company_map

    # str(PG_INT_MAX) accepted; +1 rejected; 5000-digit key rejected WITHOUT raising.
    ok = parse_chatwoot_inbox_company_map('{"' + str(PG_INT_MAX) + '": 5}')
    assert ok.valid is True and ok.legacy_mapping == {PG_INT_MAX: 5}

    over = parse_chatwoot_inbox_company_map('{"' + str(PG_INT_MAX + 1) + '": 5}')
    assert over.configured is True and over.valid is False

    huge = parse_chatwoot_inbox_company_map('{"' + "9" * 5000 + '": 5}')
    assert huge.configured is True and huge.valid is False


def test_inbox_map_parser_is_total_on_any_input() -> None:
    from altegio_bot.webhooks.common import parse_chatwoot_inbox_company_map

    # Must never raise, whatever the input type/shape.
    for raw in [None, True, False, 42, 1.9, [], {}, {"8": 1}, "42", "1.9", "[1,2]", "not json", '{"8": 1']:
        m = parse_chatwoot_inbox_company_map(raw)
        assert isinstance(m.configured, bool) and isinstance(m.valid, bool)


def test_inbox_map_trailing_newline_key_rejected_via_fullmatch() -> None:
    """A single valid-JSON key that is "8" + newline must be rejected by fullmatch.

    (`.match()` with a `$` anchor would accept it and int("8\n") == 8, silently
    remapping inbox 8.) This isolates the fullmatch requirement from the
    duplicate-key and collision defenses.
    """
    from altegio_bot.webhooks.common import parse_chatwoot_inbox_company_map

    m = parse_chatwoot_inbox_company_map('{"8\\n": 5}')  # JSON source: {"8<LF>": 5}
    assert m.configured is True
    assert m.valid is False
    assert m.mapping == {}
