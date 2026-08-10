from __future__ import annotations

import json

import pytest

from altegio_bot.easyweek_locations import parse_easyweek_location_map


def _raw(**overrides: object) -> str:
    entry: dict[str, object] = {
        "location_id": 999001,
        "location_uuid": "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
        "meta_template_prefix": "du",
        "booking_page_url": "https://booking.example.invalid/durlach",
    }
    entry.update(overrides)
    return json.dumps({"durlach": entry})


def test_valid_registry_is_keyed_by_numeric_location_id() -> None:
    parsed = parse_easyweek_location_map(_raw())
    assert parsed.ready is True
    assert parsed.locations[999001].name == "durlach"
    assert parsed.locations[999001].company_id == 999001


@pytest.mark.parametrize("raw", ["", "   ", "{}", "{ }", "{\n}"])
def test_empty_registry_is_unconfigured(raw: str) -> None:
    parsed = parse_easyweek_location_map(raw)
    assert parsed.configured is False
    assert parsed.valid is True
    assert parsed.ready is False


@pytest.mark.parametrize(
    "raw",
    [
        "{not json",
        "[]",
        "null",
        '{"durlach":{},"durlach":{}}',
        '{"durlach":{"location_id":999001,"location_id":999002}}',
    ],
)
def test_invalid_json_or_shape_never_degrades_to_empty_registry(raw: str) -> None:
    parsed = parse_easyweek_location_map(raw)
    assert parsed.configured is True
    assert parsed.valid is False
    assert parsed.ready is False


@pytest.mark.parametrize(
    ("field", "bad"),
    [
        ("location_id", True),
        ("location_id", "999001"),
        ("location_id", 0),
        ("location_uuid", "not-a-uuid"),
        ("location_uuid", " AAAAAAAA-BBBB-4CCC-8DDD-EEEEEEEEEEEE "),
        ("meta_template_prefix", "DU"),
        ("meta_template_prefix", "d"),
        ("booking_page_url", ""),
        ("booking_page_url", " https://booking.example.invalid/durlach"),
    ],
)
def test_invalid_entry_rejects_the_whole_registry(field: str, bad: object) -> None:
    parsed = parse_easyweek_location_map(_raw(**{field: bad}))
    assert parsed.configured is True
    assert parsed.valid is False
    assert parsed.locations == {}


def test_duplicate_identity_or_prefix_rejects_the_whole_registry() -> None:
    first = json.loads(_raw())["durlach"]
    second = dict(first, location_id=999002)
    for changed in ({}, {"location_uuid": "bbbbbbbb-cccc-4ddd-8eee-ffffffffffff"}):
        parsed = parse_easyweek_location_map(json.dumps({"durlach": first, "rastatt": dict(second, **changed)}))
        assert parsed.valid is False
        assert parsed.locations == {}
