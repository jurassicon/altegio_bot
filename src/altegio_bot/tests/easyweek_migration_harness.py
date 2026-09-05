"""Shared harness for the EasyWeek cutover test suites (PR-11.1).

Two suites drive the same machinery — the apply/rollback tests and the live-proof
tests — and they must drive it identically: the same fake Altegio, the same
MockTransport EasyWeek, the same production flag state, the same manifest. A
second copy of any of that would let the two suites disagree about what the tool
does, which is the one thing a test harness must never allow.

Plain callables and one class. The pytest fixtures stay in the suites, so each
one keeps control of what is autouse in it.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta

import httpx
import pytest
from sqlalchemy import func, select

from altegio_bot.easyweek_migration import reproof as reproof_module
from altegio_bot.easyweek_migration import runner as runner_module
from altegio_bot.easyweek_migration.customers import CustomerCard, CustomerDirectory
from altegio_bot.easyweek_migration.cutover import parse_cutover
from altegio_bot.easyweek_migration.manifest import KARLSRUHE_COMPANY_ID, RASTATT_COMPANY_ID, parse_manifest
from altegio_bot.easyweek_migration.runner import (
    MODE_APPLY,
    MODE_CANARY,
    MODE_DRY_RUN,
    RunInputs,
    new_run_id,
    run_canary,
    run_inventory_or_dry_run,
)
from altegio_bot.easyweek_migration.write_client import (
    EASYWEEK_BOOKING_TIMEZONE,
    EasyWeekMigrationWriteClient,
    RateLimiter,
)
from altegio_bot.models.models import EasyWeekMigrationLedger, MessageJob, OutboxMessage
from altegio_bot.settings import settings
from altegio_bot.tests.test_easyweek_migration_planning import (
    CUSTOMER_PHONE,
    CUSTOMER_UUID,
    KA_LOCATION_UUID,
    KA_SERVICE_ID,
    KA_SERVICE_UUID,
    KA_STAFF_ID,
    RA_LOCATION_UUID,
    RA_SERVICE_ID,
    RA_SERVICE_UUID,
    RA_STAFF_ID,
    manifest_text,
    record,
)

CUTOVER = "2026-09-01T00:00:00Z"
KA_RECORD_A = 900001
KA_RECORD_B = 900002
RA_RECORD_A = 910001

CREATED_UUIDS = {
    KA_RECORD_A: "aaaaaaaa-0000-4000-8000-000000000001",
    KA_RECORD_B: "aaaaaaaa-0000-4000-8000-000000000002",
    RA_RECORD_A: "bbbbbbbb-0000-4000-8000-000000000001",
}


KA_LOCATION_ID = 308001
RA_LOCATION_ID = 315001


def _registry_json() -> str:
    """The runtime registry the branch-identity check proves the manifest against."""
    return json.dumps(
        {
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
    )


def apply_production_flags(monkeypatch: pytest.MonkeyPatch) -> None:
    """The exact flag state the runbook demands before an apply."""
    monkeypatch.setattr(settings, "easyweek_location_map", _registry_json(), raising=False)
    monkeypatch.setattr(settings, "easyweek_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_processing_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_notifications_enabled", False, raising=False)
    monkeypatch.setattr(settings, "easyweek_reviews_enabled", False, raising=False)
    monkeypatch.setattr(settings, "easyweek_review_send_enabled", False, raising=False)
    monkeypatch.setattr(settings, "easyweek_reminders_enabled", False, raising=False)
    monkeypatch.setattr(settings, "easyweek_visit_counter_enabled", True, raising=False)


def stub_altegio_source(monkeypatch: pytest.MonkeyPatch) -> dict[int, list[dict]]:
    """Stub the Altegio API. Karlsruhe and Rastatt each return their own rows.

    A Durlach entry is deliberately impossible to add: the fetch is keyed by the
    company ids the manifest names, and Durlach has none.
    """
    # Keyed by (company_id, record_id): what the per-row re-proof sees INSTEAD of
    # the planned row. `None` means the booking is gone.
    live_changes: dict[tuple[int, int], dict | None] = {}

    rows: dict[int, list[dict]] = {
        KARLSRUHE_COMPANY_ID: [record(id=KA_RECORD_A), record(id=KA_RECORD_B, date="2026-09-11 10:00:00")],
        RASTATT_COMPANY_ID: [
            record(
                id=RA_RECORD_A,
                staff_id=RA_STAFF_ID,
                services=[{"id": RA_SERVICE_ID, "cost": 90.0, "cost_to_pay": 90.0, "amount": 1}],
            )
        ],
    }

    async def _fetch(*, company_id, window, timeout_sec=30.0, client=None):
        return list(rows.get(company_id, []))

    async def _fetch_one(*, company_id, record_id, timeout_sec=30.0, client=None):
        """What the LAST look at the source sees, which may differ from the plan.

        `live_changes` is the whole point: a bulk run walks a plan for many
        minutes, and a booking can be cancelled or moved *while it walks*. The
        plan-wide fetch never sees that — only this per-row read does, moments
        before the POST. Tests put the mid-run change here.
        """
        if (company_id, record_id) in live_changes:
            return live_changes[(company_id, record_id)]
        for row in rows.get(company_id, []):
            if row.get("id") == record_id:
                return row
        return None

    monkeypatch.setattr(runner_module, "fetch_company_records", _fetch)
    monkeypatch.setattr(reproof_module, "fetch_single_record", _fetch_one)
    rows["live_changes"] = live_changes  # type: ignore[assignment]
    return rows


def manifest_json() -> str:
    """The planning fixture's manifest, re-pointed at this file's registry ids."""
    payload = json.loads(manifest_text())
    payload["branches"][str(KARLSRUHE_COMPANY_ID)]["easyweek_location_id"] = KA_LOCATION_ID
    payload["branches"][str(RASTATT_COMPANY_ID)]["easyweek_location_id"] = RA_LOCATION_ID
    return json.dumps(payload)


def test_directory(**cards: str) -> CustomerDirectory:
    """A customer export carrying what `POST /bookings` actually needs.

    The API rejects `customer_uuid` and requires a phone and a given name, so a
    directory that resolves a UUID and nothing else can no longer book anybody.
    That is deliberate: a test whose directory cannot address its customer should
    fail the same way production would.
    """
    entries = {CUSTOMER_UUID: "Testkundin", **cards}
    return CustomerDirectory(
        valid=True,
        by_phone={CUSTOMER_PHONE: list(entries)},
        cards={uuid: CustomerCard(uuid=uuid, phone=CUSTOMER_PHONE, first_name=name) for uuid, name in entries.items()},
    )


def make_inputs(mode: str, **overrides) -> RunInputs:
    manifest = parse_manifest(manifest_json())
    assert manifest.valid
    kwargs = {
        "mode": mode,
        "run_id": new_run_id(),
        "cutover": parse_cutover(CUTOVER),
        "manifest": manifest,
        "directory": test_directory(),
        "apply_requested": mode in (MODE_APPLY, MODE_CANARY),
        # A rollback is a customer-visible write and carries the same
        # attestation as an apply; the negative case has its own test.
        "native_notifications_confirmed": mode in (MODE_APPLY, MODE_CANARY) or mode.startswith("rollback"),
        "cutover_supplied": True,
    }
    kwargs.update(overrides)
    return RunInputs(**kwargs)


def _planned_start_time(company_id: int, record_id: int) -> str:
    """The UTC start the migration would have written for this source record.

    Derived from the same local wall-clock parsing the classifier uses, so a
    planted booking matches the expected target instead of a hardcoded instant.
    """
    from altegio_bot.easyweek_migration.cutover import parse_altegio_local_to_utc

    known = {
        KA_RECORD_A: record(id=KA_RECORD_A),
        KA_RECORD_B: record(id=KA_RECORD_B, date="2026-09-11 10:00:00"),
        RA_RECORD_A: record(id=RA_RECORD_A),
    }
    source_record = known.get(record_id)
    assert source_record is not None, f"no planned start time known for record {record_id}"
    return parse_altegio_local_to_utc(source_record["date"]).isoformat().replace("+00:00", "Z")


# ---------------------------------------------------------------------------
# The fake EasyWeek, shaped like the real one
# ---------------------------------------------------------------------------
# The first version of this transport answered a POST by echoing the request body
# back as the booking. That made every readback test pass against a contract we
# had invented: our own field names, proving themselves. The live API answered
# 422 to that request shape and returns a completely different response shape.
#
# So this fake is built from the DOCUMENTED response instead, and it deliberately
# disagrees with our request wherever the real API does:
#
#   * `ordered_services[0].uuid` is an order-line id that is NOT the catalogue
#     service uuid, and is not derivable from it;
#   * `duration` is an object, not an integer;
#   * the marker comes back as `public_notes`, not `comment`;
#   * the customer is nested under `customer.uuid`;
#   * NO staffer appears anywhere on the booking — the master is answerable only
#     through the filtered `GET /bookings` list.

# The location catalogue, as `GET /locations/{uuid}/services` returns it. Names
# are distinct and the prices/durations deliberately repeat across entries, which
# is what the real Karlsruhe catalogue looks like: four services share 6000/90,
# so a proof that leaned on price and duration alone would be ambiguous.
CATALOG_SERVICES: dict[str, list[dict]] = {
    KA_LOCATION_UUID: [
        {"uuid": KA_SERVICE_UUID, "name": "Mascara Effekt", "price": 9000, "minutes": 60},
        {"uuid": "aaaaaaaa-1111-4111-8111-00000000ca01", "name": "Mascara Auffüllen", "price": 9000, "minutes": 60},
        {"uuid": "aaaaaaaa-1111-4111-8111-00000000ca02", "name": "Wimpernlifting", "price": 5000, "minutes": 60},
    ],
    RA_LOCATION_UUID: [
        {"uuid": RA_SERVICE_UUID, "name": "Eyeliner Effekt", "price": 9000, "minutes": 60},
        {"uuid": "bbbbbbbb-1111-4111-8111-00000000ca01", "name": "Augenbrauenlifting", "price": 5000, "minutes": 60},
    ],
}

# One order-line uuid per created booking. Deliberately unrelated to any
# catalogue uuid: that is the whole defect this hotfix exists for.
ORDER_LINE_UUIDS = {
    KA_RECORD_A: "0de41111-0000-4000-8000-000000000001",
    KA_RECORD_B: "0de41111-0000-4000-8000-000000000002",
    RA_RECORD_A: "0de41111-0000-4000-8000-000000000003",
}
_FALLBACK_ORDER_LINE = "0de41111-0000-4000-8000-0000000000ff"
# The booking uuid a cart POST returns for a record the fixture has no dedicated
# uuid for.
_FALLBACK_CART_UUID = "0ca47111-0000-4000-8000-0000000000ff"

CATALOG_PER_PAGE = 2


def catalog_entry(location_uuid: str, service_uuid: str, *, catalog: dict[str, list[dict]] | None = None) -> dict:
    for entry in (catalog or CATALOG_SERVICES).get(location_uuid, []):
        if entry["uuid"] == service_uuid:
            return entry
    raise AssertionError(f"no catalogue entry for {service_uuid} at {location_uuid}")


def _duration_object(minutes: int) -> dict:
    return {"value": minutes, "label": "minutes", "iso_8601": f"PT{minutes}M"}


def _service_row(entry: dict) -> dict:
    return {
        "uuid": entry["uuid"],
        "name": entry["name"],
        "currency": "EUR",
        "price": entry["price"],
        "price_formatted": f"€{entry['price'] / 100:.2f}",
        "duration": _duration_object(entry["minutes"]),
        "starting_at": False,
    }


class RecordingTransport:
    """Counts every request that actually left, and answers in the API's shape."""

    def __init__(
        self,
        *,
        fail_with: dict[int, object] | None = None,
        readback_override: dict[str, object] | None = None,
        catalog: dict[str, list[dict]] | None = None,
    ) -> None:
        # Per-instance so a suite with its own services cannot mutate the shared
        # catalogue out from under every other suite in the session.
        self.catalog = {location: list(rows) for location, rows in (catalog or CATALOG_SERVICES).items()}
        self.requests: list[httpx.Request] = []
        self.fail_with = fail_with or {}
        self.cancelled: list[str] = []
        # Bookings the fixture has cancelled. Kept apart from `cancelled` so a
        # test can pre-cancel one WITHOUT recording a mutation, which is how the
        # idempotent "already cancelled" path is exercised.
        self.canceled_uuids: set[str] = set()
        self.bookings: dict[str, dict] = {}
        # booking uuid -> the staffer the POST named. Held OUTSIDE the booking,
        # exactly as EasyWeek holds it: the only way to read it back is the
        # filtered list, and a test that wants to model "EasyWeek gave it to
        # somebody else" edits this rather than the booking.
        self.assignments: dict[str, str] = {}
        # Makes a failing POST still create the booking — the exact shape of a
        # 5xx returned after the write has landed.
        self.create_side_effect_on_failure = False
        self.side_effects = 0
        # Lets a test return a booking that disagrees with what was sent, which
        # is what the canary read-back has to catch.
        self.readback_override = readback_override or {}
        # booking uuid -> HTTP status a GET should answer with instead. Models a
        # target that exists but cannot be read right now.
        self.get_status_override: dict[str, int] = {}
        # Status a catalogue or list GET should answer with instead.
        self.catalog_status_override: int | None = None
        self.list_status_override: int | None = None
        # Drops the filtered list's pagination metadata, so a proof that reads
        # only page one is caught.
        self.list_meta_override: dict | None = None

    # -- routing ------------------------------------------------------------

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        path = request.url.path

        if request.method == "GET" and path.endswith("/services"):
            return self._catalog_page(request)
        if request.method == "GET" and path.rstrip("/").endswith("/bookings"):
            return self._booking_list(request)
        if request.method == "GET":
            return self._one_booking(path)
        if request.method == "POST" and path.rstrip("/").endswith("/bookings/cart"):
            return self._create_cart(request)
        if request.method == "PUT" and path.endswith("/status/cancel"):
            # The PROVEN cancel: `PUT /bookings/{uuid}/status/cancel`. The old
            # `POST .../set-booking-cancel` is deliberately NOT handled here —
            # the real API answers 404 for it, and a fixture that accepted it
            # would let a rollback that cannot cancel anything look green.
            uuid = path.split("/")[-3]
            body = json.loads(request.content.decode() or "{}")
            assert set(body) == {"cancel_reason", "internal_notes"}, f"unexpected cancel body: {sorted(body)}"
            self.cancelled.append(uuid)
            # The booking now reads back as cancelled, which is what the client
            # checks before it reports a rollback as done.
            self.canceled_uuids.add(uuid)
            return httpx.Response(200, json={})
        return self._create(request)

    # -- GET /locations/{uuid}/services -------------------------------------

    def _catalog_page(self, request: httpx.Request) -> httpx.Response:
        if self.catalog_status_override is not None:
            return httpx.Response(self.catalog_status_override, json={"error": "unavailable"})
        location_uuid = request.url.path.split("/")[-2]
        entries = self.catalog.get(location_uuid, [])
        page = int(request.url.params.get("page", 1))
        last_page = max(1, -(-len(entries) // CATALOG_PER_PAGE))
        window = entries[(page - 1) * CATALOG_PER_PAGE : page * CATALOG_PER_PAGE]
        return httpx.Response(
            200,
            json={
                "data": [_service_row(entry) for entry in window],
                "meta": {"current_page": page, "last_page": last_page, "total": len(entries)},
            },
        )

    # -- GET /bookings?staffer_uuid=... -------------------------------------

    def _booking_list(self, request: httpx.Request) -> httpx.Response:
        if self.list_status_override is not None:
            return httpx.Response(self.list_status_override, json={"error": "unavailable"})
        params = request.url.params
        staffer = params.get("staffer_uuid")
        location = params.get("location_uuid")
        start_from = params.get("reserved_on_from")
        start_to = params.get("reserved_on_to")

        rows = []
        for uuid, booking in self.bookings.items():
            if staffer is not None and self.assignments.get(uuid) != staffer:
                continue
            if location is not None and booking.get("location_uuid") != location:
                continue
            if start_from is not None and booking.get("start_time") < start_from:
                continue
            if start_to is not None and booking.get("start_time") > start_to:
                continue
            # The documented list row: no staffer, no service.
            rows.append(
                {
                    "uuid": uuid,
                    "location_uuid": booking["location_uuid"],
                    "start_time": booking["start_time"],
                    "end_time": booking["end_time"],
                    "is_canceled": booking["is_canceled"],
                    "is_completed": booking["is_completed"],
                }
            )
        meta = self.list_meta_override
        if meta is None:
            meta = {"current_page": 1, "last_page": 1, "total": len(rows)}
        return httpx.Response(200, json={"data": rows, "meta": meta})

    # -- GET /bookings/{uuid} -----------------------------------------------

    def _one_booking(self, path: str) -> httpx.Response:
        uuid = path.rsplit("/", 1)[-1]
        status = self.get_status_override.get(uuid)
        if status is not None:
            return httpx.Response(status, json={"error": "unavailable"})
        booking = self.bookings.get(uuid)
        if booking is None:
            return httpx.Response(404)
        if uuid in self.canceled_uuids:
            # The read the cancel path proves itself with. A booking the fixture
            # cancelled has to READ as cancelled, or the client is right to
            # report the outcome as unproven.
            booking = {**booking, "is_canceled": True, "status": {"type": "CANCELED"}}
        return httpx.Response(200, json=booking)

    # -- POST /bookings ------------------------------------------------------

    def _create(self, request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content.decode())
        record_id = int(str(body["booking_comment"]).rsplit(":", 1)[-1])
        failure = self.fail_with.get(record_id)
        if isinstance(failure, Exception):
            raise failure
        if isinstance(failure, int):
            if self.create_side_effect_on_failure:
                self.side_effects += 1
                self._store(body, record_id)
            return httpx.Response(failure, json={"error": "no"})

        uuid = self._store(body, record_id)
        return httpx.Response(201, json={"uuid": uuid})

    def _create_cart(self, request: httpx.Request) -> httpx.Response:
        """`POST /bookings/cart`, answering the way the live canary answered it.

        One cart item with two services came back as ONE booking carrying TWO
        order lines. The fixture reproduces that rather than the convenient
        single-line shape: a two-line booking is exactly what the readback
        cannot yet project, and a fixture that returned one line would hide the
        very gap the closed cart gate exists for.
        """
        body = json.loads(request.content.decode())
        record_id = int(str(body["booking_comment"]).rsplit(":", 1)[-1])
        failure = self.fail_with.get(record_id)
        if isinstance(failure, Exception):
            raise failure
        if isinstance(failure, int):
            return httpx.Response(failure, json={"error": "no"})

        item = body["items"][0]
        uuid = CREATED_UUIDS.get(record_id, _FALLBACK_CART_UUID)
        entries = [
            catalog_entry(body["location_uuid"], service["service_uuid"], catalog=self.catalog)
            for service in item["services"]
        ]
        minutes = sum(entry["minutes"] for entry in entries)
        start = datetime.fromisoformat(item["datetime_start"].replace("Z", "+00:00"))
        total = sum(entry["price"] for entry in entries)

        self.bookings[uuid] = {
            "uuid": uuid,
            "location_uuid": body["location_uuid"],
            "start_time": item["datetime_start"],
            "end_time": (start + timedelta(minutes=minutes)).isoformat().replace("+00:00", "Z"),
            "timezone": body["timezone"],
            "duration": _duration_object(minutes),
            "quantity": 1,
            "is_canceled": False,
            "is_completed": False,
            "public_notes": body["booking_comment"],
            "currency": "EUR",
            "customer": {"uuid": CUSTOMER_UUID},
            "order": {"total": total, "subtotal": total},
            "ordered_services": [
                {
                    "uuid": f"{_FALLBACK_ORDER_LINE[:-2]}{index:02d}",
                    "name": entry["name"],
                    "quantity": 1,
                    "currency": "EUR",
                    "price": entry["price"],
                    "original_price": entry["price"],
                    "duration": _duration_object(entry["minutes"]),
                    "original_duration": _duration_object(entry["minutes"]),
                }
                for index, entry in enumerate(entries)
            ],
        }
        self.assignments[uuid] = item["services"][0]["staffer_uuid"]
        return httpx.Response(201, json={"uuid": uuid})

    def _store(self, body: dict, record_id: int) -> str:
        """Record a created booking IN THE API'S SHAPE, not in ours.

        Everything here is derived the way EasyWeek would derive it: the length
        and the price come from the catalogue rather than from the request (the
        request carries neither), the marker lands in `public_notes`, and the
        staffer is filed away where only the filtered list can see it.
        """
        uuid = CREATED_UUIDS[record_id]
        entry = catalog_entry(body["location_uuid"], body["service_uuid"], catalog=self.catalog)
        start = datetime.fromisoformat(body["reserved_on"].replace("Z", "+00:00"))
        end = start + timedelta(minutes=entry["minutes"])

        booking = {
            "uuid": uuid,
            "location_uuid": body["location_uuid"],
            "start_time": body["reserved_on"],
            "end_time": end.isoformat().replace("+00:00", "Z"),
            "timezone": body["timezone"],
            "duration": _duration_object(entry["minutes"]),
            "quantity": 1,
            "is_canceled": False,
            "is_completed": False,
            "public_notes": body["booking_comment"],
            "currency": "EUR",
            "customer": {"uuid": CUSTOMER_UUID},
            "order": {"total": entry["price"], "subtotal": entry["price"]},
            "ordered_services": [
                {
                    "uuid": ORDER_LINE_UUIDS.get(record_id, _FALLBACK_ORDER_LINE),
                    "name": entry["name"],
                    "quantity": 1,
                    "currency": "EUR",
                    "price": entry["price"],
                    "original_price": entry["price"],
                    "duration": _duration_object(entry["minutes"]),
                    "original_duration": _duration_object(entry["minutes"]),
                }
            ],
        }
        booking.update(self.readback_override)
        self.bookings[uuid] = booking
        self.assignments[uuid] = body["staffer_uuid"]
        return uuid

    def plant_booking(self, uuid: str, *, record_id: int, start_time: str | None = None) -> None:
        """Put a booking in EasyWeek that our ledger does not know about.

        Stands in for the real situation behind a timeout: the write landed, the
        response did not, and an operator later finds the booking by its marker.
        Built through `_store` so a planted booking is byte-identical to one the
        migration would have created.
        """
        company_id = RASTATT_COMPANY_ID if record_id == RA_RECORD_A else KARLSRUHE_COMPANY_ID
        branch = parse_manifest(manifest_json()).branch(company_id)
        assert branch is not None
        altegio_service = RA_SERVICE_ID if record_id == RA_RECORD_A else KA_SERVICE_ID
        altegio_staff = RA_STAFF_ID if record_id == RA_RECORD_A else KA_STAFF_ID
        service = branch.service(altegio_service)
        assert service is not None
        self._store(
            {
                "location_uuid": branch.easyweek_location_uuid,
                "service_uuid": service.easyweek_service_uuid,
                "reserved_on": start_time or _planned_start_time(company_id, record_id),
                "booking_comment": f"altegio-migration:{company_id}:{record_id}",
                "timezone": EASYWEEK_BOOKING_TIMEZONE,
                "staffer_uuid": branch.staff[altegio_staff],
            },
            record_id,
        )
        assert self.bookings[uuid] is not None

    def post_count_for(self, record_id: int) -> int:
        """How many CREATE posts were issued for one source booking."""
        marker_tail = f":{record_id}"
        count = 0
        for request in self.requests:
            if request.method != "POST":
                continue
            body = json.loads(request.content.decode())
            if str(body.get("booking_comment", "")).endswith(marker_tail):
                count += 1
        return count

    @property
    def mutations(self) -> int:
        return sum(1 for r in self.requests if r.method == "POST")


def make_write_client(transport: RecordingTransport) -> EasyWeekMigrationWriteClient:
    async def _sleep(_delay: float) -> None:
        return None

    return EasyWeekMigrationWriteClient(
        api_key="test-key",
        workspace_slug="test-slug",
        transport=httpx.MockTransport(transport),
        sleep=_sleep,
        rate_limiter=RateLimiter(requests_per_minute=6000, sleep=_sleep),
    )


async def run_dry_run(session_local, **overrides):
    async with session_local() as session:
        return await run_inventory_or_dry_run(session, make_inputs(MODE_DRY_RUN, **overrides))


async def license_bulk(session_local, transport, *, company_id=KARLSRUHE_COMPANY_ID, record_id=None):
    """Run the real canary so a bulk apply has the proof it now requires.

    Not a shortcut: it goes through `run_canary`, which POSTs one named booking
    and reads it back. Every bulk test therefore also exercises the canary path.
    """
    plan = await run_dry_run(session_local)
    async with make_write_client(transport) as client:
        report = await run_canary(
            session_local,
            make_inputs(
                MODE_CANARY,
                verified_dry_run_id=plan.plan_digest,
                canary_company_id=company_id,
                canary_record_id=record_id if record_id is not None else KA_RECORD_A,
            ),
            write_client=client,
        )
    assert report.as_safe_dict()["totals"]["created"] == 1, report.errors
    return report


async def ledger_rows(session_local) -> list[EasyWeekMigrationLedger]:
    async with session_local() as session:
        return list(
            (
                await session.execute(
                    select(EasyWeekMigrationLedger).order_by(
                        EasyWeekMigrationLedger.source_company_id, EasyWeekMigrationLedger.source_record_id
                    )
                )
            )
            .scalars()
            .all()
        )


async def message_side_effects(session_local) -> tuple[int, int]:
    async with session_local() as session:
        jobs = (await session.execute(select(func.count()).select_from(MessageJob))).scalar_one()
        outbox = (await session.execute(select(func.count()).select_from(OutboxMessage))).scalar_one()
    return jobs, outbox


# ---------------------------------------------------------------------------
# "Somebody changed the booking" — expressed the way the real API expresses it
# ---------------------------------------------------------------------------
# Shared by every suite that asserts a changed target is refused (rollback,
# canary recovery, final reconciliation). One definition, because three copies
# would let three suites disagree about what a change even looks like — and two
# of these changes are not visible on the booking payload at all.


def mutate_field(field: str, value: object):
    def apply(transport: RecordingTransport, uuid: str) -> None:
        transport.bookings[uuid][field] = value

    return apply


def reassign_master(transport: RecordingTransport, uuid: str) -> None:
    """EasyWeek moved the appointment to a different master.

    The booking payload names no staffer, so this is what a reassignment really
    looks like: the booking is byte-identical and it simply stops appearing in
    the expected master's filtered list. Only the independent query sees it.
    """
    transport.assignments[uuid] = "00000000-0000-4000-8000-0000000000d2"


def swap_service(transport: RecordingTransport, uuid: str) -> None:
    """A different catalogue service on the order line.

    Not expressible as a changed uuid: the ordered-line uuid is not the
    catalogue's, so the only visible difference is the service's attributes.
    """
    transport.bookings[uuid]["ordered_services"][0]["name"] = "Some Other Service"


def reprice(transport: RecordingTransport, uuid: str) -> None:
    """Charged something other than the catalogue price.

    The ACTUAL price on the line, never `original_price`: the original is the
    catalogue value echoed back, so comparing it would compare the catalogue
    with itself and miss the override entirely.
    """
    transport.bookings[uuid]["ordered_services"][0]["price"] = 4200


TARGET_MUTATIONS = [
    ("location", mutate_field("location_uuid", "00000000-0000-4000-8000-0000000000d1")),
    ("master", reassign_master),
    ("service", swap_service),
    ("customer", mutate_field("customer", {"uuid": "00000000-0000-4000-8000-0000000000d4"})),
    ("start_time", mutate_field("start_time", "2026-09-14T07:00:00Z")),
    ("duration", mutate_field("duration", {"value": 120, "label": "minutes", "iso_8601": "PT120M"})),
    ("marker", mutate_field("public_notes", "rewritten by hand")),
    ("price", reprice),
]
MUTATION_IDS = [name for name, _ in TARGET_MUTATIONS]
