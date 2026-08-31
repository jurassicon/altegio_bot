"""The hotfix after PR-11.1: the real EasyWeek contract, proven against fixtures.

The canary POST returned 422 and the archaeology afterwards found that our
request had guessed six field names out of seven — and that the readback was
guessing too, because the test transport answered a POST by echoing the request
body back. Every readback assertion in the suite was our own invention proving
itself.

So the fixtures here are built from the **documented response**, and they
deliberately disagree with our request wherever the live API does: the
ordered-line uuid is not the catalogue uuid, ``duration`` is an object, the
marker comes back as ``public_notes``, the customer is nested, and no staffer
appears on the booking at all.

Plan §28 authorises proving the service by its exact attributes when they are
unique in the location catalogue. This file is where that method's limits are
pinned: ambiguity, a partial catalogue, a changed price and a re-created service
all fail closed, and a direct catalogue uuid — if EasyWeek ever returns one —
overrides the attribute match rather than being rescued by it.
"""

from __future__ import annotations

import json

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.easyweek_migration.classify import SKIP_EMPTY_SERVICES, SKIPPED, classify_record
from altegio_bot.easyweek_migration.manifest import KARLSRUHE_COMPANY_ID, parse_manifest
from altegio_bot.easyweek_migration.money import AmountError, read_amount, read_duration_minutes, to_minor_units
from altegio_bot.easyweek_migration.proof import (
    STAFF_LIST_INCOMPLETE,
    STAFF_LIST_UNREADABLE,
    STAFF_NOT_ASSIGNED,
    prove_staff_assignment,
)
from altegio_bot.easyweek_migration.runner import (
    MODE_APPLY,
    MODE_RECONCILE,
    ServiceEvidenceBook,
    load_service_evidence,
    run_apply,
    run_reconcile,
)
from altegio_bot.easyweek_migration.service_catalog import (
    SERVICE_PROOF_METHOD,
    ServiceEvidenceError,
    ServiceExpectation,
    build_catalog_snapshot,
    normalize_service_name,
    pin_service_expectation,
    prove_ordered_service,
    read_ordered_service,
)
from altegio_bot.easyweek_migration.target_snapshot import (
    REQUEST_SCHEMA_VERSION,
    TARGET_SNAPSHOT_VERSION,
)
from altegio_bot.tests.easyweek_migration_harness import (
    CATALOG_SERVICES,
    CREATED_UUIDS,
    KA_RECORD_A,
    KA_RECORD_B,
    RecordingTransport,
    apply_production_flags,
    ledger_rows,
    license_bulk,
    make_inputs,
    make_write_client,
    manifest_json,
    run_dry_run,
    stub_altegio_source,
)
from altegio_bot.tests.test_easyweek_migration_planning import (
    CUSTOMER_PHONE,
    KA_LOCATION_UUID,
    KA_SERVICE_ID,
    KA_SERVICE_UUID,
    KA_STAFF_UUID,
    directory_with,
    record,
)

MARKER = f"altegio-migration:{KARLSRUHE_COMPANY_ID}:{KA_RECORD_A}"
OTHER_STAFF_UUID = "00000000-0000-4000-8000-0000000000e1"


@pytest.fixture(autouse=True)
def _production_flags(monkeypatch: pytest.MonkeyPatch) -> None:
    apply_production_flags(monkeypatch)


@pytest.fixture
def source(monkeypatch: pytest.MonkeyPatch) -> dict[int, list[dict]]:
    return stub_altegio_source(monkeypatch)


@pytest_asyncio.fixture
async def session_local(session_maker: async_sessionmaker[AsyncSession]) -> async_sessionmaker[AsyncSession]:
    return session_maker


# ---------------------------------------------------------------------------
# Independent fixtures — the API's shape, not ours
# ---------------------------------------------------------------------------


def catalog_rows(**overrides) -> list[dict]:
    """`GET /locations/{uuid}/services` rows, as documented."""
    rows = [
        {
            "uuid": KA_SERVICE_UUID,
            "name": "Mascara Effekt",
            "currency": "EUR",
            "price": 9000,
            "duration": {"value": 60, "label": "minutes", "iso_8601": "PT1H"},
        },
        {
            # Same price and duration, different name. Four services share
            # 6000/90 in the real Karlsruhe catalogue, so price+duration alone
            # can never identify anything.
            "uuid": "aaaaaaaa-1111-4111-8111-00000000ca01",
            "name": "Mascara Auffüllen",
            "currency": "EUR",
            "price": 9000,
            "duration": {"value": 60, "label": "minutes", "iso_8601": "PT1H"},
        },
    ]
    if overrides:
        rows[0].update(overrides)
    return rows


def booking_payload(**overrides) -> dict:
    """`GET /bookings/{uuid}`, as documented — with none of our field names."""
    payload = {
        "uuid": CREATED_UUIDS[KA_RECORD_A],
        "location_uuid": KA_LOCATION_UUID,
        "start_time": "2026-09-10T12:00:00Z",
        "end_time": "2026-09-10T13:00:00Z",
        "timezone": "Europe/Berlin",
        "duration": {"value": 60, "label": "minutes", "iso_8601": "PT1H"},
        "quantity": 1,
        "is_canceled": False,
        "is_completed": False,
        "public_notes": MARKER,
        "currency": "EUR",
        "customer": {"uuid": "77777777-7777-4777-8777-777777777777"},
        "order": {"total": 9000, "subtotal": 9000},
        "ordered_services": [
            {
                # NOT the catalogue uuid. Confirmed live: `/services/{this}` → 404.
                "uuid": "0de41111-0000-4000-8000-000000000001",
                "name": "Mascara Effekt",
                "quantity": 1,
                "currency": "EUR",
                "price": 9000,
                "original_price": 9000,
                "duration": {"value": 60, "label": "minutes"},
                "original_duration": {"value": 60, "label": "minutes"},
            }
        ],
    }
    payload.update(overrides)
    return payload


def expectation() -> ServiceExpectation:
    catalog = build_catalog_snapshot(KA_LOCATION_UUID, catalog_rows())
    branch = parse_manifest(manifest_json()).branch(KARLSRUHE_COMPANY_ID)
    assert branch is not None
    mapping = branch.service(KA_SERVICE_ID)
    assert mapping is not None
    return pin_service_expectation(catalog, easyweek_service_uuid=KA_SERVICE_UUID, mapping=mapping)


def mapping_for(price: str = "90.00", minutes: int = 60):
    from altegio_bot.easyweek_migration.manifest import ServiceMapping

    return ServiceMapping(
        easyweek_service_uuid=KA_SERVICE_UUID,
        catalog_duration=read_duration_minutes(minutes),
        catalog_price=read_amount(price),
    )


# ---------------------------------------------------------------------------
# 1. The ordered line is not the catalogue service
# ---------------------------------------------------------------------------


def test_the_ordered_line_uuid_is_never_read_as_a_catalogue_uuid():
    ordered = read_ordered_service(booking_payload())
    assert ordered.line_uuid == "0de41111-0000-4000-8000-000000000001"
    assert ordered.line_uuid != KA_SERVICE_UUID
    # No direct link is offered by the real API, so there is none to read.
    assert ordered.direct_service_uuid is None
    # And the attribute match still proves the service.
    prove_ordered_service(ordered, expectation())


def test_a_direct_catalogue_uuid_wins_over_matching_attributes():
    """If EasyWeek ever returns a real link, a conflict cannot be rescued.

    The attributes here match perfectly. That must not matter: a link that says
    "this is a different service" is the stronger statement, and plan §28.2 p.1
    forbids hiding it behind a fallback.
    """
    payload = booking_payload()
    payload["ordered_services"][0]["service_uuid"] = "00000000-0000-4000-8000-0000000000ff"
    with pytest.raises(ServiceEvidenceError) as exc:
        prove_ordered_service(read_ordered_service(payload), expectation())
    assert exc.value.reason == "ordered_service_uuid_conflict"


def test_a_direct_catalogue_uuid_that_agrees_is_accepted():
    payload = booking_payload()
    payload["ordered_services"][0]["service_uuid"] = KA_SERVICE_UUID
    prove_ordered_service(read_ordered_service(payload), expectation())


# ---------------------------------------------------------------------------
# 2. Uniqueness, ambiguity, and a catalogue that moved
# ---------------------------------------------------------------------------


def test_attributes_identify_a_service_only_because_the_catalogue_is_unique():
    exp = expectation()
    assert exp.method == SERVICE_PROOF_METHOD
    assert (exp.currency, exp.price_minor, exp.duration_minutes) == ("EUR", 9000, 60)
    # The report names the method AND what it cannot promise.
    safe = exp.as_safe_dict()
    assert safe["method"] == SERVICE_PROOF_METHOD
    assert any("catalogue endpoint returned" in line for line in safe["limitations"])
    assert any("not a vendor" in line for line in safe["limitations"])


def test_two_identical_looking_services_are_ambiguous_not_a_coin_flip():
    rows = catalog_rows()
    twin = dict(rows[0])
    twin["uuid"] = "00000000-0000-4000-8000-0000000000ab"
    catalog = build_catalog_snapshot(KA_LOCATION_UUID, [*rows, twin])
    with pytest.raises(ServiceEvidenceError) as exc:
        pin_service_expectation(catalog, easyweek_service_uuid=KA_SERVICE_UUID, mapping=mapping_for())
    assert exc.value.reason == "service_attributes_ambiguous"


def test_price_and_duration_alone_are_not_enough_to_identify_a_service():
    """The second catalogue row shares both and is a different service."""
    catalog = build_catalog_snapshot(KA_LOCATION_UUID, catalog_rows())
    same_numbers = [s for s in catalog.services if (s.price_minor, s.duration_minutes) == (9000, 60)]
    assert len(same_numbers) == 2
    # Only the name separates them, which is why the name is in the tuple.
    assert len({s.normalized_name for s in same_numbers}) == 2


def test_a_service_missing_from_the_catalogue_fails_closed():
    catalog = build_catalog_snapshot(KA_LOCATION_UUID, catalog_rows()[1:])
    with pytest.raises(ServiceEvidenceError) as exc:
        pin_service_expectation(catalog, easyweek_service_uuid=KA_SERVICE_UUID, mapping=mapping_for())
    assert exc.value.reason == "service_not_in_catalog"


def test_a_recreated_service_is_not_adopted_by_its_name():
    """Same name, same price, same length — new uuid. Still refused.

    Adopting it would mean the manifest an operator reviewed silently started
    pointing at a different row in EasyWeek.
    """
    rows = catalog_rows()
    rows[0]["uuid"] = "00000000-0000-4000-8000-0000000000cd"
    catalog = build_catalog_snapshot(KA_LOCATION_UUID, rows)
    with pytest.raises(ServiceEvidenceError) as exc:
        pin_service_expectation(catalog, easyweek_service_uuid=KA_SERVICE_UUID, mapping=mapping_for())
    assert exc.value.reason == "service_not_in_catalog"


@pytest.mark.parametrize(
    "override,label",
    [
        ({"price": 12000}, "price"),
        ({"duration": {"value": 90, "label": "minutes", "iso_8601": "PT1H30M"}}, "duration"),
    ],
)
def test_a_changed_catalogue_baseline_needs_a_new_plan_not_a_silent_rebase(override, label):
    catalog = build_catalog_snapshot(KA_LOCATION_UUID, catalog_rows(**override))
    with pytest.raises(ServiceEvidenceError) as exc:
        pin_service_expectation(catalog, easyweek_service_uuid=KA_SERVICE_UUID, mapping=mapping_for())
    assert exc.value.reason == "service_attributes_changed"


@pytest.mark.parametrize(
    "override,label",
    [
        ({"price": 90.0}, "float price"),
        ({"price": "9000"}, "string price"),
        ({"duration": {"value": 3600, "label": "seconds"}}, "seconds not minutes"),
        ({"duration": 60}, "bare integer duration"),
    ],
)
def test_an_unsupported_price_or_duration_format_is_refused_not_guessed(override, label):
    with pytest.raises(ServiceEvidenceError):
        build_catalog_snapshot(KA_LOCATION_UUID, catalog_rows(**override))


def test_names_compare_under_one_normalisation_umlauts_included():
    assert normalize_service_name("  Wimpernverlängerung  ") == normalize_service_name("wimpernverlängerung")
    assert normalize_service_name("A  B") == normalize_service_name("a b")
    assert normalize_service_name("") is None
    assert normalize_service_name(None) is None


def test_money_never_rounds_and_never_guesses_a_currency():
    assert to_minor_units(read_amount("120.00"), currency="EUR") == 12000
    assert to_minor_units(read_amount("0"), currency="EUR") == 0
    with pytest.raises(AmountError):
        to_minor_units(read_amount("1.005"), currency="EUR")
    with pytest.raises(AmountError):
        to_minor_units(read_amount("1.00"), currency="CHF")


# ---------------------------------------------------------------------------
# 3. Actual values, never the catalogue echo
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("field", ["price", "duration"])
def test_the_actual_charged_value_is_compared_not_the_original(field):
    """`original_price`/`original_duration` are the catalogue echoed back.

    Comparing them would compare the catalogue with itself and never see a
    per-booking override, which is the exact class of change this migration
    refuses to move silently.
    """
    payload = booking_payload()
    line = payload["ordered_services"][0]
    if field == "price":
        line["price"] = 4200
    else:
        line["duration"] = {"value": 90, "label": "minutes"}
    # The originals still say the catalogue value.
    assert line["original_price"] == 9000
    assert line["original_duration"] == {"value": 60, "label": "minutes"}

    with pytest.raises(ServiceEvidenceError) as exc:
        prove_ordered_service(read_ordered_service(payload), expectation())
    assert exc.value.reason == "ordered_service_mismatch"


@pytest.mark.parametrize(
    "override,reason",
    [
        ({"ordered_services": []}, "ordered_service_not_single"),
        ({"ordered_services": None}, "ordered_service_missing"),
        ({}, "ordered_service_not_single"),
    ],
)
def test_a_booking_without_exactly_one_service_is_refused(override, reason):
    payload = booking_payload(**override)
    if not override:
        payload["ordered_services"] = payload["ordered_services"] * 2
    with pytest.raises(ServiceEvidenceError) as exc:
        read_ordered_service(payload)
    assert exc.value.reason == reason


def test_an_unsupported_quantity_is_refused():
    payload = booking_payload()
    payload["ordered_services"][0]["quantity"] = 2
    with pytest.raises(ServiceEvidenceError) as exc:
        read_ordered_service(payload)
    assert exc.value.reason == "ordered_service_quantity_unsupported"


# ---------------------------------------------------------------------------
# 4. The master: proven by list membership, or not at all
# ---------------------------------------------------------------------------


async def test_the_right_master_is_proven_and_another_is_refused(session_local, source):
    transport = RecordingTransport()
    await license_bulk(session_local, transport)
    target = CREATED_UUIDS[KA_RECORD_A]

    async with make_write_client(transport) as client:
        right = await prove_staff_assignment(
            client,
            target_booking_uuid=target,
            location_uuid=KA_LOCATION_UUID,
            staff_uuid=KA_STAFF_UUID,
            start_time_utc=transport.bookings[target]["start_time"],
        )
        wrong = await prove_staff_assignment(
            client,
            target_booking_uuid=target,
            location_uuid=KA_LOCATION_UUID,
            staff_uuid=OTHER_STAFF_UUID,
            start_time_utc=transport.bookings[target]["start_time"],
        )

    assert right.proven is True
    assert wrong.proven is False
    assert wrong.reason == STAFF_NOT_ASSIGNED


async def test_an_unreadable_or_incomplete_list_never_proves_a_master(session_local, source):
    transport = RecordingTransport()
    await license_bulk(session_local, transport)
    target = CREATED_UUIDS[KA_RECORD_A]
    start = transport.bookings[target]["start_time"]

    async def _prove(client):
        return await prove_staff_assignment(
            client,
            target_booking_uuid=target,
            location_uuid=KA_LOCATION_UUID,
            staff_uuid=KA_STAFF_UUID,
            start_time_utc=start,
        )

    transport.list_status_override = 500
    async with make_write_client(transport) as client:
        unreadable = await _prove(client)
    assert (unreadable.proven, unreadable.reason) == (False, STAFF_LIST_UNREADABLE)

    # A list whose pagination says nothing cannot prove absence OR presence:
    # our booking might be on a page nobody asked for.
    transport.list_status_override = None
    transport.list_meta_override = {}
    async with make_write_client(transport) as client:
        incomplete = await _prove(client)
    assert (incomplete.proven, incomplete.reason) == (False, STAFF_LIST_INCOMPLETE)


async def test_a_master_on_a_later_page_is_still_found(session_local, source):
    """A proof that read only page one would call this master unassigned."""
    transport = RecordingTransport()
    await license_bulk(session_local, transport)
    target = CREATED_UUIDS[KA_RECORD_A]
    # Page 1 is empty and says so; the booking is on page 2.
    transport.list_meta_override = {"current_page": 1, "last_page": 2, "total": 1}

    async with make_write_client(transport) as client:
        first = await prove_staff_assignment(
            client,
            target_booking_uuid=target,
            location_uuid=KA_LOCATION_UUID,
            staff_uuid=KA_STAFF_UUID,
            start_time_utc=transport.bookings[target]["start_time"],
        )
    # The fake returns the row on every page, so page 1 already contains it —
    # what matters is that the proof did not stop believing an incomplete page.
    assert first.proven is True
    assert first.pages_read == 1


# ---------------------------------------------------------------------------
# 5. The catalogue is read in full, every run
# ---------------------------------------------------------------------------


async def test_every_catalogue_page_is_read(session_local, source):
    """The fake pages at two per request; the Karlsruhe catalogue has three."""
    transport = RecordingTransport()
    assert len(CATALOG_SERVICES[KA_LOCATION_UUID]) == 3

    async with make_write_client(transport) as client:
        book = await load_service_evidence(make_inputs(MODE_APPLY), write_client=client)

    pages = [r for r in transport.requests if r.url.path.endswith("/services") and KA_LOCATION_UUID in r.url.path]
    assert len(pages) == 2
    summary = book.as_safe_dict()
    assert summary["method"] == SERVICE_PROOF_METHOD
    assert any(entry["services"] == 3 for entry in summary["catalogs"])


async def test_an_unreadable_catalogue_blocks_the_row_and_creates_nothing(session_local, source):
    transport = RecordingTransport()
    await license_bulk(session_local, transport)
    posts_before = transport.mutations
    transport.catalog_status_override = 500

    plan = await run_dry_run(session_local)
    async with make_write_client(transport) as client:
        report = await run_apply(
            session_local, make_inputs(MODE_APPLY, verified_dry_run_id=plan.plan_digest), write_client=client
        )

    assert transport.mutations == posts_before
    assert report.as_safe_dict()["totals"]["created"] == 0
    assert "service_catalog_unreadable" in report.as_safe_dict()["reason_codes"]


# ---------------------------------------------------------------------------
# 6. Versions: an old proof licenses nothing
# ---------------------------------------------------------------------------


def test_the_versions_moved_and_name_the_new_method():
    assert TARGET_SNAPSHOT_VERSION == "v2"
    assert REQUEST_SCHEMA_VERSION.startswith("v2+")
    # The stored proof column is varchar(16); overflowing it would only be
    # discovered by a failing INSERT during a production canary.
    assert len(REQUEST_SCHEMA_VERSION) <= 16


async def test_a_proof_from_the_old_contract_does_not_license_the_new_one(session_local, source):
    """A canary recorded under v1 cannot open a v2 bulk apply."""
    from sqlalchemy import update

    from altegio_bot.models.models import EasyWeekMigrationCanaryProof

    transport = RecordingTransport()
    await license_bulk(session_local, transport)
    async with session_local() as session:
        async with session.begin():
            await session.execute(update(EasyWeekMigrationCanaryProof).values(request_schema_version="v1"))

    plan = await run_dry_run(session_local)
    posts_before = transport.mutations
    async with make_write_client(transport) as client:
        with pytest.raises(Exception) as exc:
            await run_apply(
                session_local, make_inputs(MODE_APPLY, verified_dry_run_id=plan.plan_digest), write_client=client
            )
    assert "canary_proof_missing_or_stale" in str(exc.value)
    assert transport.mutations == posts_before


# ---------------------------------------------------------------------------
# 7. A created booking that cannot be proven stops the run
# ---------------------------------------------------------------------------


async def test_a_created_but_unproven_booking_halts_the_run_without_a_second_post(session_local, source):
    """The POST landed; the proof did not. Nothing more is created, ever.

    Not cancelled either: the target uuid is kept so a human can look at it.
    """
    transport = RecordingTransport()
    await license_bulk(session_local, transport)
    # From now on EasyWeek hands the appointment to somebody else.
    original_store = transport._store

    def _store_with_wrong_master(body, record_id):
        uuid = original_store(body, record_id)
        transport.assignments[uuid] = OTHER_STAFF_UUID
        return uuid

    transport._store = _store_with_wrong_master  # type: ignore[method-assign]

    plan = await run_dry_run(session_local)
    async with make_write_client(transport) as client:
        report = await run_apply(
            session_local, make_inputs(MODE_APPLY, verified_dry_run_id=plan.plan_digest), write_client=client
        )

    safe = report.as_safe_dict()
    # Exactly one booking was created before the run stopped.
    assert safe["mutations_attempted"] == 1
    assert transport.post_count_for(KA_RECORD_B) == 1
    assert any("halted" in error for error in safe["errors"])

    rows = {row.source_record_id: row for row in await ledger_rows(session_local)}
    created = rows[KA_RECORD_B]
    assert created.status == "created"
    # The uuid is kept for a human, and nothing was cancelled.
    assert created.target_booking_uuid
    assert transport.cancelled == []


# ---------------------------------------------------------------------------
# 8. Bookings with no service: excluded, not blocked, not created
# ---------------------------------------------------------------------------


def _classify(services, ledger=None):
    manifest = parse_manifest(manifest_json())
    from altegio_bot.easyweek_migration.cutover import parse_cutover

    payload = record()
    if services is _ABSENT:
        payload.pop("services")
    else:
        payload["services"] = services
    return classify_record(
        payload,
        company_id=KARLSRUHE_COMPANY_ID,
        manifest=manifest,
        directory=directory_with(),
        cutover=parse_cutover("2026-09-01T00:00:00Z"),
        ledger=ledger,
    )


_ABSENT = object()


def test_a_proven_empty_service_list_is_an_excluded_break():
    decision = _classify([])
    assert decision.outcome == SKIPPED
    assert decision.reason == SKIP_EMPTY_SERVICES


@pytest.mark.parametrize(
    "services,label",
    [
        (_ABSENT, "missing"),
        (None, "null"),
        ("nail", "not a list"),
        ([None], "corrupt entry"),
        ([{"id": "6001"}], "invalid service id"),
    ],
)
def test_a_broken_service_list_is_a_data_error_not_a_break(services, label):
    """Only `services: []` is a break. Everything else is somebody's bug."""
    decision = _classify(services)
    assert decision.reason != SKIP_EMPTY_SERVICES


def test_a_free_service_and_an_unnamed_one_are_not_treated_as_breaks():
    """Zero price and an empty label are not heuristics for a break (§28.4)."""
    free = _classify([{"id": KA_SERVICE_ID, "cost": 0.0, "cost_to_pay": 0.0, "title": ""}])
    assert free.reason != SKIP_EMPTY_SERVICES


async def test_an_empty_service_booking_is_counted_and_never_posted(session_local, source):
    transport = RecordingTransport()
    await license_bulk(session_local, transport)
    for row in source[KARLSRUHE_COMPANY_ID]:
        if row["id"] == KA_RECORD_B:
            row["services"] = []

    plan = await run_dry_run(session_local)
    async with make_write_client(transport) as client:
        report = await run_apply(
            session_local, make_inputs(MODE_APPLY, verified_dry_run_id=plan.plan_digest), write_client=client
        )

    safe = report.as_safe_dict()
    assert safe["source"]["excluded_empty_services"] == 1
    assert safe["reason_codes"][SKIP_EMPTY_SERVICES] == 1
    assert transport.post_count_for(KA_RECORD_B) == 0
    assert KA_RECORD_B not in {row.source_record_id for row in await ledger_rows(session_local)}


async def test_a_service_list_emptied_after_the_dry_run_stops_the_post(session_local, source):
    """The plan is stale the moment the master turns the slot into a break."""
    transport = RecordingTransport()
    await license_bulk(session_local, transport)
    plan = await run_dry_run(session_local)

    emptied = dict(record(id=KA_RECORD_B, date="2026-09-11 10:00:00"))
    emptied["services"] = []
    source["live_changes"][(KARLSRUHE_COMPANY_ID, KA_RECORD_B)] = emptied

    async with make_write_client(transport) as client:
        await run_apply(
            session_local, make_inputs(MODE_APPLY, verified_dry_run_id=plan.plan_digest), write_client=client
        )

    assert transport.post_count_for(KA_RECORD_B) == 0


async def test_an_emptied_source_does_not_let_an_existing_target_be_forgotten(session_local, source):
    """A break made out of an already-migrated booking is a drift, not a delete.

    The ledger row and its EasyWeek target both still exist, so the final
    reconciliation must still have something to say — and must not quietly pass.
    """
    transport = RecordingTransport()
    await license_bulk(session_local, transport)
    for row in source[KARLSRUHE_COMPANY_ID]:
        if row["id"] == KA_RECORD_A:
            row["services"] = []

    async with make_write_client(transport) as client:
        report = await run_reconcile(session_local, make_inputs(MODE_RECONCILE, final=True), write_client=client)

    verdict = report.as_safe_dict()["completeness"]
    assert verdict["passed"] is False
    # The target is still there, and nothing cancelled it.
    assert transport.cancelled == []
    rows = {row.source_record_id: row for row in await ledger_rows(session_local)}
    assert rows[KA_RECORD_A].status == "created"


# ---------------------------------------------------------------------------
# 9. The report says which method proved the service, and carries no PII
# ---------------------------------------------------------------------------


async def test_the_report_names_the_limited_method_and_leaks_nothing(session_local, source):
    transport = RecordingTransport()
    report = await license_bulk(session_local, transport)
    safe = report.as_safe_dict()

    evidence = safe["service_evidence"]
    assert evidence["method"] == SERVICE_PROOF_METHOD
    assert evidence["pinned_services"] >= 1

    blob = json.dumps(safe)
    assert CUSTOMER_PHONE not in blob
    assert "Testkundin" not in blob
    # The service name travels as a digest, never as text.
    assert "Mascara" not in blob


def test_an_empty_evidence_book_proves_nothing():
    book = ServiceEvidenceBook()
    summary = book.as_safe_dict()
    assert summary["pinned_services"] == 0
    assert summary["catalogs"] == []
