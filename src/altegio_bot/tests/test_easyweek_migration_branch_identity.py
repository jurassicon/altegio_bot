"""PR-11.1 revision 16: proving a manifest target is the branch it claims to be.

A manifest with Karlsruhe and Rastatt swapped is internally consistent — canonical
UUIDs, positive ids, no duplicates — and would migrate every Karlsruhe customer
into Rastatt. Plan §10 records the production incident where exactly that class
of mismatch went unnoticed until real messages reached the wrong salon's
customers.

The third independent value is the runtime registry the bot already runs on.
"""

from __future__ import annotations

import json

import pytest

from altegio_bot.easyweek_locations import parse_easyweek_location_map
from altegio_bot.easyweek_migration.branch_identity import (
    BRANCH_LOCATION_ID_UNKNOWN,
    BRANCH_LOCATION_UUID_MISMATCH,
    BRANCH_MANIFEST_UNUSABLE,
    BRANCH_REGISTRY_INVALID,
    BRANCH_REGISTRY_UNCONFIGURED,
    BRANCH_SLUG_MISMATCH,
    verify_branch_identity,
)
from altegio_bot.easyweek_migration.manifest import (
    KARLSRUHE_COMPANY_ID,
    RASTATT_COMPANY_ID,
    parse_manifest,
)
from altegio_bot.tests.test_easyweek_migration_planning import (
    KA_LOCATION_UUID,
    KA_SERVICE_ID,
    KA_SERVICE_UUID,
    KA_STAFF_ID,
    KA_STAFF_UUID,
    RA_LOCATION_UUID,
    RA_SERVICE_ID,
    RA_SERVICE_UUID,
    RA_STAFF_ID,
    RA_STAFF_UUID,
)

KA_LOCATION_ID = 308001
RA_LOCATION_ID = 315001


def registry(**overrides) -> str:
    entries = {
        "karlsruhe": {
            "location_id": KA_LOCATION_ID,
            "location_uuid": KA_LOCATION_UUID,
            "meta_template_prefix": "ka",
            "booking_page_url": "https://booking.example.invalid/ka",
        },
        "rastatt": {
            "location_id": RA_LOCATION_ID,
            "location_uuid": RA_LOCATION_UUID,
            "meta_template_prefix": "ra",
            "booking_page_url": "https://booking.example.invalid/ra",
        },
    }
    entries.update(overrides)
    return json.dumps(entries)


def manifest(
    *, ka_location_id=KA_LOCATION_ID, ka_uuid=KA_LOCATION_UUID, ra_location_id=RA_LOCATION_ID, ra_uuid=RA_LOCATION_UUID
):
    return parse_manifest(
        json.dumps(
            {
                "manifest_id": "identity-test",
                "branches": {
                    str(KARLSRUHE_COMPANY_ID): {
                        "altegio_company_id": KARLSRUHE_COMPANY_ID,
                        "easyweek_location_id": ka_location_id,
                        "easyweek_location_uuid": ka_uuid,
                        "selected_altegio_staff_ids": [KA_STAFF_ID],
                        "deferred_altegio_staff_ids": [],
                        "staff": {str(KA_STAFF_ID): KA_STAFF_UUID},
                        "services": {
                            str(KA_SERVICE_ID): {
                                "easyweek_service_uuid": KA_SERVICE_UUID,
                                "catalog_duration_minutes": 60,
                                "catalog_price": "90.00",
                            }
                        },
                    },
                    str(RASTATT_COMPANY_ID): {
                        "altegio_company_id": RASTATT_COMPANY_ID,
                        "easyweek_location_id": ra_location_id,
                        "easyweek_location_uuid": ra_uuid,
                        "selected_altegio_staff_ids": [RA_STAFF_ID],
                        "deferred_altegio_staff_ids": [],
                        "staff": {str(RA_STAFF_ID): RA_STAFF_UUID},
                        "services": {
                            str(RA_SERVICE_ID): {
                                "easyweek_service_uuid": RA_SERVICE_UUID,
                                "catalog_duration_minutes": 60,
                                "catalog_price": "90.00",
                            }
                        },
                    },
                },
            }
        )
    )


def test_correct_karlsruhe_and_rastatt_are_proven():
    result = verify_branch_identity(manifest(), registry=parse_easyweek_location_map(registry()))
    assert result.proven
    assert result.proven_branches == {KARLSRUHE_COMPANY_ID: "karlsruhe", RASTATT_COMPANY_ID: "rastatt"}


def test_swapped_target_locations_block_apply():
    """The whole reason this module exists: both halves look individually right."""
    swapped = manifest(
        ka_location_id=RA_LOCATION_ID,
        ka_uuid=RA_LOCATION_UUID,
        ra_location_id=KA_LOCATION_ID,
        ra_uuid=KA_LOCATION_UUID,
    )
    result = verify_branch_identity(swapped, registry=parse_easyweek_location_map(registry()))
    assert not result.proven
    assert result.failures.count(BRANCH_SLUG_MISMATCH) == 2


# A third, unrelated UUID: pointing Karlsruhe at Rastatt's would be caught one
# step earlier, by the manifest's own duplicate-target rule.
FOREIGN_LOCATION_UUID = "99999999-9999-4999-8999-999999999999"


def test_a_correct_id_with_a_foreign_uuid_blocks():
    """The registry's id and the manifest's uuid disagree — a §10-shaped mismatch."""
    result = verify_branch_identity(
        manifest(ka_uuid=FOREIGN_LOCATION_UUID),
        registry=parse_easyweek_location_map(registry()),
    )
    assert not result.proven
    assert BRANCH_LOCATION_UUID_MISMATCH in result.failures


def test_an_unusable_manifest_is_never_vacuously_proven():
    """An empty loop collects no failures; that must not read as a verified map."""
    result = verify_branch_identity(parse_manifest("{not json"), registry=parse_easyweek_location_map(registry()))
    assert not result.proven
    assert result.failures == [BRANCH_MANIFEST_UNUSABLE]


def test_a_correct_uuid_under_the_wrong_slug_blocks():
    """id and uuid agree with each other and the registry — and are still wrong."""
    relabelled = json.loads(registry())
    relabelled["durlach"] = relabelled.pop("karlsruhe")
    result = verify_branch_identity(manifest(), registry=parse_easyweek_location_map(json.dumps(relabelled)))
    assert not result.proven
    assert BRANCH_SLUG_MISMATCH in result.failures


def test_a_location_id_absent_from_the_registry_blocks():
    result = verify_branch_identity(
        manifest(ka_location_id=999999),
        registry=parse_easyweek_location_map(registry()),
    )
    assert not result.proven
    assert BRANCH_LOCATION_ID_UNKNOWN in result.failures


@pytest.mark.parametrize(
    "raw,expected",
    [("", BRANCH_REGISTRY_UNCONFIGURED), ("{not json", BRANCH_REGISTRY_INVALID)],
)
def test_an_unusable_registry_blocks_rather_than_skipping_the_check(raw, expected):
    """ "We could not verify" and "it is correct" must never be the same answer."""
    result = verify_branch_identity(manifest(), registry=parse_easyweek_location_map(raw))
    assert not result.proven
    assert result.failures == [expected]


def test_every_failure_is_reported_not_just_the_first():
    swapped = manifest(
        ka_location_id=RA_LOCATION_ID,
        ka_uuid=RA_LOCATION_UUID,
        ra_location_id=KA_LOCATION_ID,
        ra_uuid=KA_LOCATION_UUID,
    )
    result = verify_branch_identity(swapped, registry=parse_easyweek_location_map(registry()))
    assert len(result.failures) == 2


def test_the_result_is_report_safe():
    result = verify_branch_identity(manifest(), registry=parse_easyweek_location_map(registry()))
    blob = json.dumps(result.as_safe_dict())
    assert "karlsruhe" in blob
    assert KA_STAFF_UUID not in blob
