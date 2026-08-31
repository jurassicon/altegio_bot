"""PR-11.1 revision 17: the cutover migrates a named wave of masters, not everyone.

The business decision that made this necessary: the nail-service masters move in
a later wave, so the first one carries an explicitly chosen set.

The trap it has to avoid is subtle and cheap to fall into. Leaving a master out
of the ``staff`` mapping would exclude her too — and if that were allowed to
work, "we deliberately deferred her" and "we forgot her" would become the same
state. The day somebody forgot a master, the tool would agree, the completeness
check would call the wave finished, and her customers would arrive at a salon
with no record of them.

So the selector is explicit, unknown masters block, and deferred masters are
counted out loud.
"""

from __future__ import annotations

import json

import pytest

from altegio_bot.easyweek_migration.classify import (
    BLOCK_STAFF_MAPPING_MISSING,
    BLOCK_STAFF_NOT_IN_WAVE,
    BLOCKED,
    READY,
    SKIP_STAFF_DEFERRED,
    SKIPPED,
    classify_record,
)
from altegio_bot.easyweek_migration.cutover import parse_cutover
from altegio_bot.easyweek_migration.manifest import (
    INVALID_SELECTED_STAFF_UNMAPPED,
    INVALID_STAFF_SCOPE_EMPTY,
    INVALID_STAFF_SCOPE_OVERLAP,
    KARLSRUHE_COMPANY_ID,
    STAFF_DEFERRED,
    STAFF_SELECTED,
    STAFF_UNKNOWN,
    inventory_manifest,
    parse_manifest,
)
from altegio_bot.tests.test_easyweek_migration_planning import (
    KA_DEFERRED_STAFF_ID,
    KA_LOCATION_UUID,
    KA_SERVICE_ID,
    KA_SERVICE_UUID,
    KA_STAFF_ID,
    KA_STAFF_UUID,
    directory_with,
    manifest_text,
    record,
)

DEFERRED_STAFF_UUID = "aaaaaaaa-0000-4000-8000-00000000dddd"
UNLISTED_STAFF_ID = 5999


def wave_manifest(*, selected, deferred, staff=None) -> str:
    """One Karlsruhe branch with an explicitly stated wave selector."""
    staff_map = staff if staff is not None else {str(KA_STAFF_ID): KA_STAFF_UUID}
    return json.dumps(
        {
            "manifest_id": "wave-test",
            "branches": {
                str(KARLSRUHE_COMPANY_ID): {
                    "altegio_company_id": KARLSRUHE_COMPANY_ID,
                    "easyweek_location_id": 308001,
                    "easyweek_location_uuid": KA_LOCATION_UUID,
                    "selected_altegio_staff_ids": selected,
                    "deferred_altegio_staff_ids": deferred,
                    "staff": staff_map,
                    "services": {
                        str(KA_SERVICE_ID): {
                            "easyweek_service_uuid": KA_SERVICE_UUID,
                            "catalog_duration_minutes": 60,
                            "catalog_price": "90.00",
                        }
                    },
                }
            },
        }
    )


@pytest.fixture
def directory():

    return directory_with()


def classify(rec, manifest_json, directory):
    return classify_record(
        rec,
        company_id=KARLSRUHE_COMPANY_ID,
        manifest=parse_manifest(manifest_json),
        directory=directory,
        cutover=parse_cutover("2026-09-01T00:00:00Z"),
        ledger=None,
    )


# ---------------------------------------------------------------------------
# Manifest invariants
# ---------------------------------------------------------------------------


def test_a_valid_selector_names_both_sets():
    manifest = parse_manifest(wave_manifest(selected=[KA_STAFF_ID], deferred=[KA_DEFERRED_STAFF_ID]))
    assert manifest.valid
    branch = manifest.branch(KARLSRUHE_COMPANY_ID)
    assert branch.selected_staff_ids == frozenset({KA_STAFF_ID})
    assert branch.deferred_staff_ids == frozenset({KA_DEFERRED_STAFF_ID})


def test_a_master_cannot_be_selected_and_deferred_at_once():
    parsed = parse_manifest(wave_manifest(selected=[KA_STAFF_ID], deferred=[KA_STAFF_ID]))
    assert not parsed.valid
    assert parsed.reason == INVALID_STAFF_SCOPE_OVERLAP


def test_a_selected_master_must_have_a_real_easyweek_uuid():
    """This is what stops "no mapping" from being a back-door exclusion."""
    parsed = parse_manifest(wave_manifest(selected=[KA_STAFF_ID, UNLISTED_STAFF_ID], deferred=[]))
    assert not parsed.valid
    assert parsed.reason == INVALID_SELECTED_STAFF_UNMAPPED


def test_a_wave_that_migrates_nobody_is_an_unfinished_manifest():
    parsed = parse_manifest(wave_manifest(selected=[], deferred=[KA_DEFERRED_STAFF_ID]))
    assert not parsed.valid
    assert parsed.reason == INVALID_STAFF_SCOPE_EMPTY


def test_inventory_still_runs_before_the_selector_exists():
    """Choosing the wave needs the id list inventory is there to produce."""
    assert inventory_manifest(wave_manifest(selected=[], deferred=[])).valid


@pytest.mark.parametrize("bad", [[KA_STAFF_ID, KA_STAFF_ID], ["5001"], [True], [0], [-1], {}, None])
def test_a_malformed_selector_rejects_the_manifest(bad):
    assert not parse_manifest(wave_manifest(selected=bad, deferred=[])).valid


def test_a_deferred_master_may_already_carry_a_mapping():
    """Preparing wave two early is fine; it is not the same as selecting her."""
    manifest = parse_manifest(
        wave_manifest(
            selected=[KA_STAFF_ID],
            deferred=[KA_DEFERRED_STAFF_ID],
            staff={str(KA_STAFF_ID): KA_STAFF_UUID, str(KA_DEFERRED_STAFF_ID): DEFERRED_STAFF_UUID},
        )
    )
    assert manifest.valid
    assert manifest.branch(KARLSRUHE_COMPANY_ID).staff_scope(KA_DEFERRED_STAFF_ID) == STAFF_DEFERRED


def test_the_selector_is_part_of_the_manifest_digest():
    """Moving a master between waves changes which customers get booked."""
    base = parse_manifest(wave_manifest(selected=[KA_STAFF_ID], deferred=[KA_DEFERRED_STAFF_ID]))
    moved = parse_manifest(wave_manifest(selected=[KA_STAFF_ID], deferred=[]))
    assert base.valid and moved.valid
    assert base.digest != moved.digest


def test_the_manifest_summary_states_both_sets_without_names():
    manifest = parse_manifest(wave_manifest(selected=[KA_STAFF_ID], deferred=[KA_DEFERRED_STAFF_ID]))
    entry = manifest.as_safe_dict()["branches"][0]
    assert entry["selected_staff_ids"] == [KA_STAFF_ID]
    assert entry["deferred_staff_ids"] == [KA_DEFERRED_STAFF_ID]


@pytest.mark.parametrize(
    "staff_id,expected",
    [
        (KA_STAFF_ID, STAFF_SELECTED),
        (KA_DEFERRED_STAFF_ID, STAFF_DEFERRED),
        (UNLISTED_STAFF_ID, STAFF_UNKNOWN),
        ("5001", STAFF_UNKNOWN),
        (None, STAFF_UNKNOWN),
    ],
)
def test_scope_lookup_is_exact(staff_id, expected):
    branch = parse_manifest(wave_manifest(selected=[KA_STAFF_ID], deferred=[KA_DEFERRED_STAFF_ID])).branch(
        KARLSRUHE_COMPANY_ID
    )
    assert branch.staff_scope(staff_id) == expected


# ---------------------------------------------------------------------------
# What the classifier does with each scope
# ---------------------------------------------------------------------------


def test_a_selected_master_migrates(directory):
    decision = classify(record(), wave_manifest(selected=[KA_STAFF_ID], deferred=[KA_DEFERRED_STAFF_ID]), directory)
    assert decision.outcome == READY


def test_a_deferred_master_is_skipped_with_its_own_reason(directory):
    """Not an error, not a gap — somebody else's wave."""
    decision = classify(
        record(staff_id=KA_DEFERRED_STAFF_ID),
        wave_manifest(selected=[KA_STAFF_ID], deferred=[KA_DEFERRED_STAFF_ID]),
        directory,
    )
    assert decision.outcome == SKIPPED
    assert decision.reason == SKIP_STAFF_DEFERRED


def test_an_unknown_master_blocks_the_cutover(directory):
    decision = classify(
        record(staff_id=UNLISTED_STAFF_ID),
        wave_manifest(selected=[KA_STAFF_ID], deferred=[KA_DEFERRED_STAFF_ID]),
        directory,
    )
    assert decision.outcome == BLOCKED
    assert decision.reason == BLOCK_STAFF_NOT_IN_WAVE


def test_a_deferred_master_is_never_reported_as_a_missing_mapping(directory):
    """The distinction the whole selector exists to preserve."""
    decision = classify(
        record(staff_id=KA_DEFERRED_STAFF_ID),
        wave_manifest(selected=[KA_STAFF_ID], deferred=[KA_DEFERRED_STAFF_ID]),
        directory,
    )
    assert decision.reason != BLOCK_STAFF_MAPPING_MISSING


def test_a_deferred_master_is_skipped_even_when_her_booking_is_otherwise_blocked(directory):
    """Her wave has not started, so her data is not this wave's problem yet."""
    decision = classify(
        record(
            staff_id=KA_DEFERRED_STAFF_ID,
            services=[
                {"id": KA_SERVICE_ID, "cost": 90.0, "cost_to_pay": 90.0},
                {"id": KA_SERVICE_ID, "cost": 30.0, "cost_to_pay": 30.0},
            ],
        ),
        wave_manifest(selected=[KA_STAFF_ID], deferred=[KA_DEFERRED_STAFF_ID]),
        directory,
    )
    assert decision.outcome == SKIPPED
    assert decision.reason == SKIP_STAFF_DEFERRED


def test_a_past_booking_of_a_deferred_master_is_still_just_past(directory):
    """Scope is asked after the window, so the reason stays the accurate one."""
    decision = classify(
        record(staff_id=KA_DEFERRED_STAFF_ID, date="2026-08-01 10:00:00"),
        wave_manifest(selected=[KA_STAFF_ID], deferred=[KA_DEFERRED_STAFF_ID]),
        directory,
    )
    assert decision.outcome == SKIPPED
    assert decision.reason == "starts_before_cutover"


def test_the_plan_digest_moves_when_the_wave_changes(directory):
    """A verified dry-run of one wave must not open an apply of another."""
    from altegio_bot.easyweek_migration.report import plan_digest

    wide = parse_manifest(manifest_text())
    narrow_text = json.loads(manifest_text())
    narrow_text["branches"][str(KARLSRUHE_COMPANY_ID)]["selected_altegio_staff_ids"] = []
    narrow_text["branches"][str(KARLSRUHE_COMPANY_ID)]["deferred_altegio_staff_ids"] = [
        KA_STAFF_ID,
        KA_DEFERRED_STAFF_ID,
    ]
    narrow = parse_manifest(json.dumps(narrow_text))
    # One branch may select nobody — Rastatt still does, so this is a real wave.
    # The protection is not refusal, it is that the file is a DIFFERENT file: its
    # digest moves, and the verified dry-run of the wide wave stops matching.
    assert narrow.valid, narrow.reason
    assert narrow.digest != wide.digest
    assert narrow.staff_scope_digest != wide.staff_scope_digest

    decisions = [
        classify_record(
            record(),
            company_id=KARLSRUHE_COMPANY_ID,
            manifest=wide,
            directory=directory,
            cutover=parse_cutover("2026-09-01T00:00:00Z"),
            ledger=None,
        )
    ]
    deferred_manifest = parse_manifest(wave_manifest(selected=[KA_STAFF_ID], deferred=[KA_DEFERRED_STAFF_ID]))
    a = plan_digest(decisions, cutover_iso="2026-09-01T00:00:00Z", manifest_digest=wide.digest)
    b = plan_digest(decisions, cutover_iso="2026-09-01T00:00:00Z", manifest_digest=deferred_manifest.digest)
    assert a != b
