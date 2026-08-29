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

import httpx
import pytest
from sqlalchemy import func, select

from altegio_bot.easyweek_migration import reproof as reproof_module
from altegio_bot.easyweek_migration import runner as runner_module
from altegio_bot.easyweek_migration.customers import CustomerDirectory
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
from altegio_bot.easyweek_migration.write_client import EasyWeekMigrationWriteClient, RateLimiter
from altegio_bot.models.models import EasyWeekMigrationLedger, MessageJob, OutboxMessage
from altegio_bot.settings import settings
from altegio_bot.tests.test_easyweek_migration_planning import (
    CUSTOMER_PHONE,
    CUSTOMER_UUID,
    KA_LOCATION_UUID,
    KA_SERVICE_ID,
    KA_STAFF_ID,
    RA_LOCATION_UUID,
    RA_SERVICE_ID,
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
                services=[{"id": RA_SERVICE_ID, "cost": 90.0, "cost_to_pay": 90.0}],
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


def make_inputs(mode: str, **overrides) -> RunInputs:
    manifest = parse_manifest(manifest_json())
    assert manifest.valid
    kwargs = {
        "mode": mode,
        "run_id": new_run_id(),
        "cutover": parse_cutover(CUTOVER),
        "manifest": manifest,
        "directory": CustomerDirectory(valid=True, by_phone={CUSTOMER_PHONE: [CUSTOMER_UUID]}),
        "apply_requested": mode in (MODE_APPLY, MODE_CANARY),
        # A rollback is a customer-visible write and carries the same
        # attestation as an apply; the negative case has its own test.
        "native_notifications_confirmed": mode in (MODE_APPLY, MODE_CANARY) or mode.startswith("rollback"),
        "cutover_supplied": True,
    }
    kwargs.update(overrides)
    return RunInputs(**kwargs)


class RecordingTransport:
    """Counts every request that actually left, and answers per source record."""

    def __init__(
        self,
        *,
        fail_with: dict[int, object] | None = None,
        readback_override: dict[str, object] | None = None,
    ) -> None:
        self.requests: list[httpx.Request] = []
        self.fail_with = fail_with or {}
        self.cancelled: list[str] = []
        self.bookings: dict[str, dict] = {}
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

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)

        if request.method == "GET":
            uuid = request.url.path.rsplit("/", 1)[-1]
            status = self.get_status_override.get(uuid)
            if status is not None:
                return httpx.Response(status, json={"error": "unavailable"})
            booking = self.bookings.get(uuid)
            if booking is None:
                return httpx.Response(404)
            return httpx.Response(200, json=booking)

        if request.url.path.endswith("set-booking-cancel"):
            uuid = request.url.path.split("/")[-2]
            self.cancelled.append(uuid)
            return httpx.Response(200, json={})

        body = json.loads(request.content.decode())
        record_id = int(body["comment"].rsplit(":", 1)[-1])
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

    def _store(self, body: dict, record_id: int) -> str:
        """Record a created booking in full.

        A read-back that cannot see every write-critical field is a read-back
        that proves nothing, so the fake stores everything the projection needs.
        """
        uuid = CREATED_UUIDS[record_id]
        booking = {
            "uuid": uuid,
            "comment": body["comment"],
            "start_time": body["start_time"],
            "duration": body["duration"],
            "location_uuid": body["location_uuid"],
            "staff_uuid": body["staff_uuid"],
            "customer_uuid": body["customer_uuid"],
            "service_uuid": body["services"][0]["service_uuid"],
            "is_canceled": False,
            "is_completed": False,
        }
        booking.update(self.readback_override)
        self.bookings[uuid] = booking
        return uuid

    def plant_booking(self, uuid: str, *, record_id: int) -> None:
        """Put a booking in EasyWeek that our ledger does not know about.

        Stands in for the real situation behind a timeout: the write landed, the
        response did not, and an operator later finds the booking by its marker.
        """
        company_id = RASTATT_COMPANY_ID if record_id == RA_RECORD_A else KARLSRUHE_COMPANY_ID
        branch = parse_manifest(manifest_json()).branch(company_id)
        assert branch is not None
        service = branch.service(RA_SERVICE_ID if record_id == RA_RECORD_A else KA_SERVICE_ID)
        assert service is not None
        self.bookings[uuid] = {
            "uuid": uuid,
            "comment": f"altegio-migration:{company_id}:{record_id}",
            "start_time": "2026-09-11T08:00:00Z",
            "duration": 60,
            "location_uuid": branch.easyweek_location_uuid,
            "staff_uuid": branch.staff[RA_STAFF_ID if record_id == RA_RECORD_A else KA_STAFF_ID],
            "customer_uuid": CUSTOMER_UUID,
            "service_uuid": service.easyweek_service_uuid,
            "is_canceled": False,
            "is_completed": False,
        }

    def post_count_for(self, record_id: int) -> int:
        """How many CREATE posts were issued for one source booking."""
        marker_tail = f":{record_id}"
        count = 0
        for request in self.requests:
            if request.method != "POST" or request.url.path.endswith("set-booking-cancel"):
                continue
            body = json.loads(request.content.decode())
            if str(body.get("comment", "")).endswith(marker_tail):
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
