"""PR-11.1: what the cutover planner will and will not migrate.

Pure unit tests — no database, no network. Everything here is about one question:
given a source booking, does the tool make the right call, and does it refuse
rather than approximate when it cannot?
"""

from __future__ import annotations

import json

import pytest

from altegio_bot.easyweek_migration.classify import (
    ALREADY_MIGRATED,
    BLOCK_CUSTOM_DURATION,
    BLOCK_CUSTOM_PRICE,
    BLOCK_LEDGER_UNCERTAIN,
    BLOCK_MULTI_SERVICE,
    BLOCK_SERVICE_MAPPING_MISSING,
    BLOCK_SOURCE_CHANGED,
    BLOCK_STAFF_NOT_IN_WAVE,
    BLOCK_STATUS_UNRECOGNISED,
    BLOCKED,
    READY,
    SKIP_CANCELED,
    SKIP_COMPLETED,
    SKIP_DELETED,
    SKIP_FOREIGN_COMPANY,
    SKIP_PAST,
    SKIPPED,
    LedgerView,
    classify_record,
)
from altegio_bot.easyweek_migration.customers import (
    CUSTOMER_AMBIGUOUS,
    CUSTOMER_NOT_FOUND,
    CUSTOMER_PHONE_UNUSABLE,
    CustomerCard,
    CustomerDirectory,
    load_customer_directory,
)
from altegio_bot.easyweek_migration.cutover import (
    TIME_AMBIGUOUS_DST,
    TIME_NONEXISTENT_DST,
    TIME_UNPARSEABLE,
    CutoverError,
    LocalTimeError,
    parse_altegio_local_to_utc,
    parse_cutover,
)
from altegio_bot.easyweek_migration.manifest import (
    KARLSRUHE_COMPANY_ID,
    RASTATT_COMPANY_ID,
    parse_manifest,
)

KA_LOCATION_UUID = "11111111-1111-4111-8111-111111111111"
RA_LOCATION_UUID = "22222222-2222-4222-8222-222222222222"
KA_STAFF_UUID = "33333333-3333-4333-8333-333333333333"
KA_SERVICE_UUID = "44444444-4444-4444-8444-444444444444"
RA_STAFF_UUID = "55555555-5555-4555-8555-555555555555"
RA_SERVICE_UUID = "66666666-6666-4666-8666-666666666666"
CUSTOMER_UUID = "77777777-7777-4777-8777-777777777777"
OTHER_CUSTOMER_UUID = "88888888-8888-4888-8888-888888888888"

KA_STAFF_ID = 5001
KA_SERVICE_ID = 6001
RA_STAFF_ID = 5002
RA_SERVICE_ID = 6002
# A master deliberately held back for a later wave (the nail services).
KA_DEFERRED_STAFF_ID = 5003

CUSTOMER_PHONE = "+4915112345678"


def manifest_text(**overrides) -> str:
    payload = {
        "manifest_id": "cutover-test",
        "branches": {
            str(KARLSRUHE_COMPANY_ID): {
                "altegio_company_id": KARLSRUHE_COMPANY_ID,
                "easyweek_location_id": 308697,
                "easyweek_location_uuid": KA_LOCATION_UUID,
                "selected_altegio_staff_ids": [KA_STAFF_ID],
                "deferred_altegio_staff_ids": [KA_DEFERRED_STAFF_ID],
                "staff": {str(KA_STAFF_ID): KA_STAFF_UUID},
                "services": {
                    str(KA_SERVICE_ID): {
                        "easyweek_service_uuid": KA_SERVICE_UUID,
                        "catalog_duration_minutes": 60,
                        "catalog_price": "90.00",
                        # The reviewed identity: what the operator saw in
                        # EasyWeek, matching the fake catalogue exactly.
                        "catalog_service_name": "Mascara Effekt",
                        "catalog_currency": "EUR",
                    }
                },
            },
            str(RASTATT_COMPANY_ID): {
                "altegio_company_id": RASTATT_COMPANY_ID,
                "easyweek_location_id": 315607,
                "easyweek_location_uuid": RA_LOCATION_UUID,
                "selected_altegio_staff_ids": [RA_STAFF_ID],
                "deferred_altegio_staff_ids": [],
                "staff": {str(RA_STAFF_ID): RA_STAFF_UUID},
                "services": {
                    str(RA_SERVICE_ID): {
                        "easyweek_service_uuid": RA_SERVICE_UUID,
                        "catalog_duration_minutes": 60,
                        "catalog_price": "90.00",
                        "catalog_service_name": "Eyeliner Effekt",
                        "catalog_currency": "EUR",
                    }
                },
            },
        },
    }
    payload.update(overrides)
    return json.dumps(payload)


@pytest.fixture
def manifest():
    parsed = parse_manifest(manifest_text())
    assert parsed.valid, parsed.reason
    return parsed


def directory_with(**cards: str) -> CustomerDirectory:
    """An export that can actually address its customers — phone AND given name."""
    entries = {CUSTOMER_UUID: "Testkundin", **cards}
    return CustomerDirectory(
        valid=True,
        by_phone={CUSTOMER_PHONE: list(entries)},
        cards={uuid: CustomerCard(uuid=uuid, phone=CUSTOMER_PHONE, first_name=name) for uuid, name in entries.items()},
    )


@pytest.fixture
def directory() -> CustomerDirectory:
    return directory_with()


@pytest.fixture
def cutover():
    return parse_cutover("2026-09-01T00:00:00Z")


def record(**overrides):
    """A minimal Karlsruhe booking that classifies as ready."""
    base = {
        "id": 900001,
        "date": "2026-09-10 14:00:00",
        "staff_id": KA_STAFF_ID,
        "seance_length": 3600,
        "client": {"phone": CUSTOMER_PHONE},
        # `amount` is what a real Altegio service line carries, and the
        # classifier requires an exact 1 — the only quantity either request
        # shape can express (plan §30.12).
        "services": [{"id": KA_SERVICE_ID, "cost": 90.0, "cost_to_pay": 90.0, "amount": 1}],
    }
    base.update(overrides)
    return base


def classify(rec, *, manifest, directory, cutover, company_id=KARLSRUHE_COMPANY_ID, ledger=None):
    return classify_record(
        rec,
        company_id=company_id,
        manifest=manifest,
        directory=directory,
        cutover=cutover,
        ledger=ledger,
    )


# ---------------------------------------------------------------------------
# Manifest
# ---------------------------------------------------------------------------


def test_a_valid_manifest_maps_both_migrating_branches(manifest):
    assert manifest.company_ids == (KARLSRUHE_COMPANY_ID, RASTATT_COMPANY_ID)
    assert manifest.branch(KARLSRUHE_COMPANY_ID).staff_uuid(KA_STAFF_ID) == KA_STAFF_UUID
    assert manifest.branch(RASTATT_COMPANY_ID).service_uuid(RA_SERVICE_ID) == RA_SERVICE_UUID


def test_durlach_cannot_be_written_into_the_manifest():
    """Durlach exists only in EasyWeek. Naming any non-Altegio company rejects the file."""
    text = json.dumps(
        {
            "manifest_id": "cutover-test",
            "branches": {
                "308697": {
                    "altegio_company_id": 308697,
                    "easyweek_location_id": 308697,
                    "easyweek_location_uuid": KA_LOCATION_UUID,
                    "selected_altegio_staff_ids": [1],
                    "deferred_altegio_staff_ids": [],
                    "staff": {"1": KA_STAFF_UUID},
                    "services": {
                        "2": {
                            "easyweek_service_uuid": KA_SERVICE_UUID,
                            "catalog_duration_minutes": 60,
                            "catalog_price": "90.00",
                        }
                    },
                }
            },
        }
    )
    parsed = parse_manifest(text)
    assert not parsed.valid
    assert parsed.reason == "manifest_unknown_company"


def test_the_two_company_ids_must_agree():
    """The key and the field are the same value twice — plan §10's lesson."""
    text = manifest_text()
    swapped = json.loads(text)
    swapped["branches"][str(KARLSRUHE_COMPANY_ID)]["altegio_company_id"] = RASTATT_COMPANY_ID
    assert not parse_manifest(json.dumps(swapped)).valid


@pytest.mark.parametrize(
    "value",
    [
        "ABCDEF01-1111-4111-8111-111111111111",  # uppercase
        "{abcdef01-1111-4111-8111-111111111111}",  # braced
        " abcdef01-1111-4111-8111-111111111111",  # padded
        "abcdef0111114111811111111111",  # unhyphenated
    ],
)
def test_a_non_canonical_uuid_is_rejected_at_parse_time(value):
    """Whatever EasyWeek returned, it was not this. Catch it here, not mid-apply."""
    payload = json.loads(manifest_text())
    payload["branches"][str(KARLSRUHE_COMPANY_ID)]["easyweek_location_uuid"] = value
    assert not parse_manifest(json.dumps(payload)).valid


def test_an_empty_mapping_is_an_unfinished_manifest_not_an_empty_one():
    payload = json.loads(manifest_text())
    payload["branches"][str(KARLSRUHE_COMPANY_ID)]["staff"] = {}
    parsed = parse_manifest(json.dumps(payload))
    assert not parsed.valid
    assert parsed.reason == "manifest_empty"


def test_the_digest_ignores_formatting_but_not_identifiers():
    compact = parse_manifest(manifest_text())
    spaced = parse_manifest(json.dumps(json.loads(manifest_text()), indent=4))
    assert compact.digest == spaced.digest

    changed = json.loads(manifest_text())
    changed["branches"][str(KARLSRUHE_COMPANY_ID)]["staff"][str(KA_STAFF_ID)] = RA_STAFF_UUID
    assert parse_manifest(json.dumps(changed)).digest != compact.digest


def test_a_manifest_never_leaks_its_content_into_its_report(manifest):
    safe = manifest.as_safe_dict()
    blob = json.dumps(safe)
    assert KA_STAFF_UUID not in blob
    assert safe["branches"][0]["staff_mappings"] == 1


# ---------------------------------------------------------------------------
# Cutover, timezone and DST
# ---------------------------------------------------------------------------


def test_a_cutover_without_an_offset_is_refused():
    """A wall clock is not an instant; reading it as UTC would move the boundary."""
    with pytest.raises(CutoverError):
        parse_cutover("2026-09-01T00:00:00")


def test_a_cutover_with_an_offset_is_normalised_to_utc():
    assert parse_cutover("2026-09-01T02:00:00+02:00").iso == "2026-09-01T00:00:00Z"


def test_summer_and_winter_local_times_use_the_right_offset():
    # CEST (+02:00) in September, CET (+01:00) in December.
    assert parse_altegio_local_to_utc("2026-09-10 14:00:00").isoformat() == "2026-09-10T12:00:00+00:00"
    assert parse_altegio_local_to_utc("2026-12-10 14:00:00").isoformat() == "2026-12-10T13:00:00+00:00"


def test_an_ambiguous_autumn_fold_time_is_refused_not_guessed():
    with pytest.raises(LocalTimeError) as exc:
        parse_altegio_local_to_utc("2026-10-25 02:30:00")
    assert exc.value.reason == TIME_AMBIGUOUS_DST


def test_a_nonexistent_spring_gap_time_is_refused_not_shifted():
    with pytest.raises(LocalTimeError) as exc:
        parse_altegio_local_to_utc("2026-03-29 02:30:00")
    assert exc.value.reason == TIME_NONEXISTENT_DST


def test_an_unparseable_time_is_refused():
    with pytest.raises(LocalTimeError) as exc:
        parse_altegio_local_to_utc("not a date")
    assert exc.value.reason == TIME_UNPARSEABLE


def test_a_dst_ambiguous_booking_blocks_rather_than_migrating_an_hour_off(manifest, directory, cutover):
    decision = classify(record(date="2026-10-25 02:30:00"), manifest=manifest, directory=directory, cutover=cutover)
    assert decision.outcome == BLOCKED
    assert decision.reason == TIME_AMBIGUOUS_DST


# ---------------------------------------------------------------------------
# Scope: which bookings are ours at all
# ---------------------------------------------------------------------------


def test_a_ready_karlsruhe_booking_resolves_every_target(manifest, directory, cutover):
    decision = classify(record(), manifest=manifest, directory=directory, cutover=cutover)
    assert decision.outcome == READY
    assert decision.easyweek_location_uuid == KA_LOCATION_UUID
    assert decision.easyweek_staff_uuid == KA_STAFF_UUID
    assert decision.easyweek_service_uuid == KA_SERVICE_UUID
    assert decision.easyweek_customer_uuid == CUSTOMER_UUID
    assert decision.duration_minutes == 60


def test_karlsruhe_and_rastatt_are_provider_and_branch_scoped(manifest, directory, cutover):
    """A Rastatt booking must never resolve through Karlsruhe's mapping."""
    ka = classify(record(), manifest=manifest, directory=directory, cutover=cutover)
    ra = classify(
        record(staff_id=RA_STAFF_ID, services=[{"id": RA_SERVICE_ID, "cost": 90.0, "cost_to_pay": 90.0, "amount": 1}]),
        manifest=manifest,
        directory=directory,
        cutover=cutover,
        company_id=RASTATT_COMPANY_ID,
    )
    assert ka.easyweek_location_uuid == KA_LOCATION_UUID
    assert ra.easyweek_location_uuid == RA_LOCATION_UUID

    # Karlsruhe's staff id in a Rastatt booking is in neither of Rastatt's wave
    # lists, so it is an unknown master there — not a missing mapping.
    crossed = classify(
        record(services=[{"id": RA_SERVICE_ID, "cost": 90.0, "cost_to_pay": 90.0, "amount": 1}]),
        manifest=manifest,
        directory=directory,
        cutover=cutover,
        company_id=RASTATT_COMPANY_ID,
    )
    assert crossed.outcome == BLOCKED
    assert crossed.reason == BLOCK_STAFF_NOT_IN_WAVE


def test_a_company_outside_the_manifest_is_skipped_not_blocked(manifest, directory, cutover):
    decision = classify(record(), manifest=manifest, directory=directory, cutover=cutover, company_id=308697)
    assert decision.outcome == SKIPPED
    assert decision.reason == SKIP_FOREIGN_COMPANY


def test_a_booking_before_the_cutover_is_skipped(manifest, directory, cutover):
    decision = classify(record(date="2026-08-20 14:00:00"), manifest=manifest, directory=directory, cutover=cutover)
    assert decision.outcome == SKIPPED
    assert decision.reason == SKIP_PAST


@pytest.mark.parametrize(
    "overrides,expected",
    [
        ({"deleted": True}, SKIP_DELETED),
        ({"confirmed": 0}, SKIP_CANCELED),
        ({"attendance": 1}, SKIP_COMPLETED),
        ({"attendance": -1}, SKIP_COMPLETED),
        ({"visit_attendance": 1}, SKIP_COMPLETED),
    ],
)
def test_past_cancelled_and_completed_bookings_are_never_migrated(manifest, directory, cutover, overrides, expected):
    decision = classify(record(**overrides), manifest=manifest, directory=directory, cutover=cutover)
    assert decision.outcome == SKIPPED
    assert decision.reason == expected


def test_an_active_attendance_value_still_migrates(manifest, directory, cutover):
    for value in (0, 2):
        decision = classify(record(attendance=value), manifest=manifest, directory=directory, cutover=cutover)
        assert decision.outcome == READY


def test_an_unrecognised_status_blocks_rather_than_being_assumed_live(manifest, directory, cutover):
    decision = classify(record(attendance="maybe"), manifest=manifest, directory=directory, cutover=cutover)
    assert decision.outcome == BLOCKED
    assert decision.reason == BLOCK_STATUS_UNRECOGNISED


# ---------------------------------------------------------------------------
# Mapping
# ---------------------------------------------------------------------------


def test_an_unlisted_master_blocks_that_row_only(manifest, directory, cutover):
    blocked = classify(record(staff_id=999999), manifest=manifest, directory=directory, cutover=cutover)
    healthy = classify(record(id=900002), manifest=manifest, directory=directory, cutover=cutover)
    assert blocked.outcome == BLOCKED
    assert blocked.reason == BLOCK_STAFF_NOT_IN_WAVE
    # The independent booking keeps going — one gap does not stop the cutover.
    assert healthy.outcome == READY


def test_a_missing_service_mapping_blocks(manifest, directory, cutover):
    decision = classify(
        record(services=[{"id": 424242, "cost": 90.0, "cost_to_pay": 90.0, "amount": 1}]),
        manifest=manifest,
        directory=directory,
        cutover=cutover,
    )
    assert decision.outcome == BLOCKED
    assert decision.reason == BLOCK_SERVICE_MAPPING_MISSING


def test_a_string_staff_id_never_matches_an_integer_selector(manifest, directory, cutover):
    """A master we could not classify is unknown, never "deliberately deferred"."""
    decision = classify(record(staff_id=str(KA_STAFF_ID)), manifest=manifest, directory=directory, cutover=cutover)
    assert decision.outcome == BLOCKED
    assert decision.reason == BLOCK_STAFF_NOT_IN_WAVE


# ---------------------------------------------------------------------------
# Multi-service, custom duration, custom price — fail closed
# ---------------------------------------------------------------------------


def test_a_three_service_booking_is_blocked_not_flattened(manifest, directory, cutover):
    """Two is the widest shape a real canary proved; three has no evidence."""
    decision = classify(
        record(
            services=[
                {"id": KA_SERVICE_ID, "cost": 90.0, "cost_to_pay": 90.0, "amount": 1},
                {"id": KA_SERVICE_ID, "cost": 90.0, "cost_to_pay": 90.0, "amount": 1},
                {"id": KA_SERVICE_ID, "cost": 90.0, "cost_to_pay": 90.0, "amount": 1},
            ],
            seance_length=10800,
        ),
        manifest=manifest,
        directory=directory,
        cutover=cutover,
    )
    assert decision.outcome == BLOCKED
    assert decision.reason == BLOCK_MULTI_SERVICE


def test_a_two_service_booking_with_a_custom_price_is_still_blocked(manifest, directory, cutover):
    """The cart contract needs standard prices; a discount is not one."""
    decision = classify(
        record(
            services=[
                {"id": KA_SERVICE_ID, "cost": 90.0, "cost_to_pay": 90.0, "amount": 1},
                {"id": KA_SERVICE_ID, "cost": 30.0, "cost_to_pay": 30.0, "amount": 1},
            ],
            seance_length=7200,
        ),
        manifest=manifest,
        directory=directory,
        cutover=cutover,
    )
    assert decision.outcome == BLOCKED
    assert decision.reason == "custom_price_unsupported"


def test_a_custom_price_is_blocked_not_replaced_by_the_catalogue_price(manifest, directory, cutover):
    decision = classify(
        record(services=[{"id": KA_SERVICE_ID, "cost": 90.0, "cost_to_pay": 70.0, "amount": 1}]),
        manifest=manifest,
        directory=directory,
        cutover=cutover,
    )
    assert decision.outcome == BLOCKED
    assert decision.reason == BLOCK_CUSTOM_PRICE


def test_a_discount_counts_as_a_custom_price(manifest, directory, cutover):
    decision = classify(
        record(services=[{"id": KA_SERVICE_ID, "cost": 90.0, "cost_to_pay": 90.0, "discount": 10, "amount": 1}]),
        manifest=manifest,
        directory=directory,
        cutover=cutover,
    )
    assert decision.outcome == BLOCKED
    assert decision.reason == BLOCK_CUSTOM_PRICE


def test_a_hand_stretched_slot_is_blocked_not_rounded(manifest, directory, cutover):
    decision = classify(
        record(
            seance_length=5400,
            services=[{"id": KA_SERVICE_ID, "cost": 90.0, "cost_to_pay": 90.0, "seance_length": 3600, "amount": 1}],
        ),
        manifest=manifest,
        directory=directory,
        cutover=cutover,
    )
    assert decision.outcome == BLOCKED
    assert decision.reason == BLOCK_CUSTOM_DURATION


def test_an_explicit_staff_policy_normalizes_duration_to_the_catalogue(directory, cutover):
    payload = json.loads(manifest_text())
    payload["branches"][str(KARLSRUHE_COMPANY_ID)]["normalize_duration_to_catalog_for_staff_ids"] = [KA_STAFF_ID]
    manifest = parse_manifest(json.dumps(payload))
    assert manifest.valid

    decision = classify(
        record(
            seance_length=5400,
            services=[
                {
                    "id": KA_SERVICE_ID,
                    "cost": 90.0,
                    "cost_to_pay": 90.0,
                    "seance_length": 5400,
                    "amount": 1,
                }
            ],
        ),
        manifest=manifest,
        directory=directory,
        cutover=cutover,
    )

    assert decision.outcome == READY
    assert decision.source_booked_duration_minutes == 90
    assert decision.duration_minutes == 60
    assert decision.duration_normalized_to_catalog is True
    assert decision.as_safe_dict()["target_duration_minutes"] == 60


def test_duration_normalization_does_not_normalize_custom_price(directory, cutover):
    payload = json.loads(manifest_text())
    payload["branches"][str(KARLSRUHE_COMPANY_ID)]["normalize_duration_to_catalog_for_staff_ids"] = [KA_STAFF_ID]
    manifest = parse_manifest(json.dumps(payload))
    assert manifest.valid

    decision = classify(
        record(
            seance_length=5400,
            services=[{"id": KA_SERVICE_ID, "cost": 90.0, "cost_to_pay": 70.0, "amount": 1}],
        ),
        manifest=manifest,
        directory=directory,
        cutover=cutover,
    )
    assert decision.outcome == BLOCKED
    assert decision.reason == BLOCK_CUSTOM_PRICE


def test_a_non_whole_minute_duration_is_blocked(manifest, directory, cutover):
    decision = classify(record(seance_length=3630), manifest=manifest, directory=directory, cutover=cutover)
    assert decision.outcome == BLOCKED
    assert decision.reason == BLOCK_CUSTOM_DURATION


# ---------------------------------------------------------------------------
# Customer resolution
# ---------------------------------------------------------------------------


def test_exactly_one_phone_match_resolves(directory):
    match = directory.resolve(CUSTOMER_PHONE)
    assert match.resolved and match.uuid == CUSTOMER_UUID


def test_zero_matches_block(manifest, directory, cutover):
    decision = classify(
        record(client={"phone": "+4915199999999"}), manifest=manifest, directory=directory, cutover=cutover
    )
    assert decision.outcome == BLOCKED
    assert decision.reason == CUSTOMER_NOT_FOUND


def test_more_than_one_match_blocks(manifest, cutover):
    ambiguous = CustomerDirectory(valid=True, by_phone={CUSTOMER_PHONE: [CUSTOMER_UUID, OTHER_CUSTOMER_UUID]})
    decision = classify(record(), manifest=manifest, directory=ambiguous, cutover=cutover)
    assert decision.outcome == BLOCKED
    assert decision.reason == CUSTOMER_AMBIGUOUS


def test_an_unusable_phone_blocks(manifest, directory, cutover):
    decision = classify(record(client={"phone": "1234"}), manifest=manifest, directory=directory, cutover=cutover)
    assert decision.outcome == BLOCKED
    assert decision.reason == CUSTOMER_PHONE_UNUSABLE


def test_matching_is_by_normalised_number_not_by_formatting(directory):
    assert directory.resolve("+49 (151) 123-456-78").uuid == CUSTOMER_UUID
    # A digit swapped is a different person, not a near miss.
    assert directory.resolve("+4915112345679").reason == CUSTOMER_NOT_FOUND


def test_names_are_never_used_to_resolve_a_customer(manifest, cutover):
    """Even a perfect name match resolves nothing without a phone."""
    directory = directory_with()
    decision = classify(
        record(client={"name": "A. Muster", "phone": None}),
        manifest=manifest,
        directory=directory,
        cutover=cutover,
    )
    assert decision.outcome == BLOCKED
    assert decision.reason == CUSTOMER_PHONE_UNUSABLE


def test_a_directory_summary_carries_no_phone_numbers(directory):
    blob = json.dumps(directory.as_safe_dict())
    assert CUSTOMER_PHONE not in blob
    assert "distinct_phones" in blob


def test_a_csv_export_indexes_by_phone(tmp_path):
    path = tmp_path / "customers.csv"
    path.write_text(
        f"uuid,phone,first_name\n{CUSTOMER_UUID},{CUSTOMER_PHONE},Someone\n{OTHER_CUSTOMER_UUID},+4915100000000,Else\n",
        encoding="utf-8",
    )
    loaded = load_customer_directory(path)
    assert loaded.valid
    assert loaded.resolve(CUSTOMER_PHONE).uuid == CUSTOMER_UUID
    card = loaded.transport_fields(CUSTOMER_UUID)
    assert card is not None
    assert (card.phone, card.first_name) == (CUSTOMER_PHONE, "Someone")


def test_a_full_name_column_is_not_a_first_name(tmp_path):
    """`POST /bookings` wants a given name, and a full name is not one.

    Splitting "Anna Maria" on the space to produce "Anna" is a guess about a real
    person's name, made to get a request accepted. The export is refused per row
    instead, so an operator adds the column rather than the tool inventing it.
    """
    path = tmp_path / "customers.csv"
    path.write_text(f"uuid,phone,name\n{CUSTOMER_UUID},{CUSTOMER_PHONE},Anna Maria Müller\n", encoding="utf-8")
    loaded = load_customer_directory(path)
    assert loaded.valid
    match = loaded.resolve(CUSTOMER_PHONE)
    assert match.uuid is None
    assert match.reason == "customer_first_name_missing"
    assert loaded.transport_fields(CUSTOMER_UUID) is None


def test_a_directory_summary_counts_unaddressable_rows_without_naming_them(tmp_path):
    path = tmp_path / "customers.csv"
    path.write_text(
        f"uuid,phone,first_name\n{CUSTOMER_UUID},{CUSTOMER_PHONE},Someone\n{OTHER_CUSTOMER_UUID},+4915100000000,\n",
        encoding="utf-8",
    )
    summary = load_customer_directory(path).as_safe_dict()
    assert summary["rows_without_first_name"] == 1
    blob = json.dumps(summary)
    assert CUSTOMER_PHONE not in blob and "Someone" not in blob


def test_a_csv_with_unrecognised_columns_fails_loudly(tmp_path):
    """Better one clear refusal than every customer silently 'not found'."""
    path = tmp_path / "customers.csv"
    path.write_text("id_of_person,contact\nx,y\n", encoding="utf-8")
    loaded = load_customer_directory(path)
    assert not loaded.valid
    assert loaded.reason == "customer_directory_shape_invalid"


def test_two_export_rows_for_one_phone_are_ambiguous_not_first_wins(tmp_path):
    path = tmp_path / "customers.csv"
    path.write_text(
        f"uuid,phone\n{CUSTOMER_UUID},{CUSTOMER_PHONE}\n{OTHER_CUSTOMER_UUID},{CUSTOMER_PHONE}\n",
        encoding="utf-8",
    )
    loaded = load_customer_directory(path)
    assert loaded.resolve(CUSTOMER_PHONE).reason == CUSTOMER_AMBIGUOUS


# ---------------------------------------------------------------------------
# Ledger interaction
# ---------------------------------------------------------------------------


def test_an_already_created_row_is_reported_as_already_migrated(manifest, directory, cutover):
    first = classify(record(), manifest=manifest, directory=directory, cutover=cutover)
    view = LedgerView(
        status="created",
        target_booking_uuid="99999999-9999-4999-8999-999999999999",
        source_fingerprint=first.source_fingerprint,
    )
    again = classify(record(), manifest=manifest, directory=directory, cutover=cutover, ledger=view)
    assert again.outcome == ALREADY_MIGRATED
    assert again.target_booking_uuid == view.target_booking_uuid


def test_a_source_that_changed_after_migration_blocks_instead_of_double_booking(manifest, directory, cutover):
    view = LedgerView(
        status="created",
        target_booking_uuid="99999999-9999-4999-8999-999999999999",
        source_fingerprint="a-fingerprint-from-a-different-schedule",
    )
    decision = classify(record(), manifest=manifest, directory=directory, cutover=cutover, ledger=view)
    assert decision.outcome == BLOCKED
    assert decision.reason == BLOCK_SOURCE_CHANGED


def test_an_uncertain_row_blocks_and_asks_for_reconciliation(manifest, directory, cutover):
    view = LedgerView(status="uncertain", target_booking_uuid=None, source_fingerprint="x")
    decision = classify(record(), manifest=manifest, directory=directory, cutover=cutover, ledger=view)
    assert decision.outcome == BLOCKED
    assert decision.reason == BLOCK_LEDGER_UNCERTAIN


def test_a_previously_blocked_row_is_re_evaluated_from_scratch(manifest, directory, cutover):
    view = LedgerView(status="blocked", target_booking_uuid=None, source_fingerprint="x")
    decision = classify(record(), manifest=manifest, directory=directory, cutover=cutover, ledger=view)
    assert decision.outcome == READY


def test_a_decision_never_carries_pii_into_a_report(manifest, directory, cutover):
    decision = classify(record(), manifest=manifest, directory=directory, cutover=cutover)
    blob = json.dumps(decision.as_safe_dict())
    assert CUSTOMER_PHONE not in blob
    assert "Muster" not in blob


def test_a_pending_row_is_as_unresolved_as_an_uncertain_one(manifest, directory, cutover):
    """A claim whose outcome was never recorded may still have sent its POST."""
    view = LedgerView(status="pending", target_booking_uuid=None, source_fingerprint="x")
    decision = classify(record(), manifest=manifest, directory=directory, cutover=cutover, ledger=view)
    assert decision.outcome == BLOCKED
    assert decision.reason == BLOCK_LEDGER_UNCERTAIN


def test_the_two_unresolved_status_lists_stay_in_step():
    """`classify` spells the statuses as literals to avoid a circular import."""
    from altegio_bot.easyweek_migration.classify import LEDGER_UNRESOLVED_STATUSES
    from altegio_bot.easyweek_migration.ledger import UNRESOLVED_STATUSES

    assert LEDGER_UNRESOLVED_STATUSES == frozenset(UNRESOLVED_STATUSES)
