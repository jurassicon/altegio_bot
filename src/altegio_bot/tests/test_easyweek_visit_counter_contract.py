"""Deployment and preflight contract for the PR-11 EasyWeek visit counter.

Two things are pinned here that no unit test can see:

* the read-only preflight really is read-only, and its report carries technical
  ids and reason codes rather than anything a customer would recognise;
* the flag is declared exactly where it is read, and the runbook says to
  recreate exactly the service that reads it. A flag documented on a service
  that never reads it teaches an operator to recreate the wrong container and
  then wonder why nothing changed.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
import pytest_asyncio
import yaml
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

import altegio_bot.db as app_db
from altegio_bot.models.models import Client, EasyWeekEvent, Record
from altegio_bot.scripts import easyweek_visit_counter_preflight as preflight
from altegio_bot.settings import settings
from altegio_bot.tests.easyweek_fixtures import (
    TEST_CUSTOMER_ID,
    TEST_LOCATION_ID,
    TEST_LOCATION_UUID,
    booking_created,
)
from altegio_bot.workers import easyweek_inbox_worker as worker

REPO_ROOT = Path(__file__).resolve().parents[3]
COMPOSE = REPO_ROOT / "docker-compose.yml"
EASYWEEK_ENV_EXAMPLE = REPO_ROOT / "easyweek.env.example"
RUNBOOK = REPO_ROOT / "docs/easyweek/durlach_activation_runbook.md"
PLAN = REPO_ROOT / "docs/easyweek/INTEGRATION_PLAN.md"

FLAG = "EASYWEEK_VISIT_COUNTER_ENABLED"
COUNTER_SERVICE = "altegio-easyweek-inbox-worker"

# Values a report must never contain, taken from the fixture that seeds it.
PII_MARKERS = ("customer_phone", "customer_email", "customer_name", "booking_comment", "@", "+49")


# ===========================================================================
# The flag is declared where it is read, and nowhere else
# ===========================================================================


def test_the_flag_is_declared_false_in_the_env_example() -> None:
    lines = EASYWEEK_ENV_EXAMPLE.read_text().splitlines()
    assignments = [line for line in lines if line.startswith(f"{FLAG}=")]

    assert assignments == [f"{FLAG}=false"], "declared exactly once, and fail-closed"


def test_the_env_example_says_the_counter_is_not_a_notification_fence() -> None:
    text = EASYWEEK_ENV_EXAMPLE.read_text()
    block = text[text.index("# --- PR-11") : text.index(f"{FLAG}=false")]

    assert "EASYWEEK_NOTIFICATIONS_ENABLED" in block
    assert COUNTER_SERVICE in block
    assert "altegio-outbox-worker" in block, "the reader boundary is stated, not implied"
    assert "force-recreate" in block, "restart does not re-read env_file"


def test_only_the_easyweek_inbox_worker_is_documented_as_the_reader() -> None:
    """A second reader would be a second place to recreate — and there is none."""
    compose = COMPOSE.read_text()
    mentions = [line for line in compose.splitlines() if FLAG in line]

    assert mentions, "the compose contract must name the flag"
    marker = compose.index(FLAG)
    following = compose[marker:]
    next_service = following.index(f"  {COUNTER_SERVICE}:")
    # The comment introducing the flag sits directly above the one service that
    # reads it; no other service block may claim it.
    assert next_service < 600, "the flag comment must belong to the inbox worker block"
    assert "read HERE and nowhere else" in compose


def test_the_compose_services_are_unchanged_apart_from_that_comment() -> None:
    config = yaml.safe_load(COMPOSE.read_text())

    assert COUNTER_SERVICE in config["services"]
    # The counter sends nothing, so it has no fence on the shared outbox worker.
    outbox_env = config["services"]["altegio-outbox-worker"].get("environment") or []
    assert not any(FLAG in str(entry) for entry in outbox_env)


# ===========================================================================
# Runbook: rollout, rollback, and what each command does
# ===========================================================================


def _visit_counter_section() -> str:
    text = RUNBOOK.read_text()
    start = text.index("## 15. PR-11")
    following = re.search(r"^## \d+\. ", text[start + 1 :], flags=re.M)
    end = start + 1 + following.start() if following else len(text)
    return text[start:end]


def test_the_runbook_documents_the_full_rollout_and_rollback() -> None:
    section = _visit_counter_section()

    for required in (
        f"{FLAG}=false",
        f"{FLAG}=true",
        "alembic upgrade head",
        "easyweek_visit_counter_preflight",
        f"up -d --force-recreate {COUNTER_SERVICE}",
    ):
        assert required in section, required


def _command_blocks() -> list[tuple[str, str]]:
    """(preamble, commands) for every fenced block in the PR-11 section.

    The preamble is everything since the previous fence — exactly the text an
    operator reads before pasting the block below it.
    """
    section = _visit_counter_section()
    parts = re.split(r"```(?:bash|sql)?\n", section)
    blocks: list[tuple[str, str]] = []
    for index in range(1, len(parts), 2):
        blocks.append((parts[index - 1], parts[index].split("```")[0]))
    return blocks


def test_the_runbook_recreates_only_the_service_that_reads_the_flag() -> None:
    """The prose may NAME the outbox worker; no command may recreate it."""
    for _preamble, commands in _command_blocks():
        assert "altegio-outbox-worker" not in commands, commands

    section = _visit_counter_section()
    assert f"force-recreate {COUNTER_SERVICE}" in section
    assert "altegio-outbox-worker" in section, "and it must say WHY that service is not recreated"


def test_every_runbook_command_is_introduced_by_an_explanation() -> None:
    """A production command with no stated effect is a command run on faith."""
    blocks = _command_blocks()
    assert len(blocks) >= 8, "rollout, smoke and rollback all carry commands"

    for preamble, commands in blocks:
        lowered = preamble.lower()
        assert any(marker in lowered for marker in ("**шаг", "**откат", "**проверка")), commands
        # ...and each states whether it touches production.
        assert any(phrase in lowered for phrase in ("не меняет", "меняет production", "только читает", "read-only")), (
            commands
        )


def test_the_runbook_keeps_proven_counters_on_rollback() -> None:
    section = _visit_counter_section()
    rollback = section[section.index("Откат") :]

    assert "DELETE" not in rollback.upper().replace("НЕ DELETE", "")
    assert "не удаляются" in rollback or "сохраняются" in rollback


def test_the_runbook_never_prints_pii_in_its_commands() -> None:
    """Prose may discuss a `payload hash`; a SELECT may not return a payload."""
    for _preamble, commands in _command_blocks():
        for forbidden in ("phone_e164", "display_name", "email", "payload", "body_raw", "customer_"):
            assert forbidden not in commands, (forbidden, commands)


# ===========================================================================
# The plan records the owner's decision
# ===========================================================================


# The canonical plan is deliberately NOT tracked by git (`.gitignore` line 62),
# so it exists on a maintainer's machine and not in CI. These two assertions are
# still worth keeping — they catch a plan left stale next to the code it governs
# — but they must skip rather than crash where the file was never checked out.
_PLAN_PRESENT = pytest.mark.skipif(not PLAN.exists(), reason="INTEGRATION_PLAN.md is untracked (.gitignore)")


@_PLAN_PRESENT
def test_the_plan_marks_pr11_active_and_pr9_pr10_closed() -> None:
    text = PLAN.read_text()

    assert "### PR-11" in text
    assert "PR-9 review_3d" not in text.split("## 0.")[0], "the stale header line is gone"
    assert "### PR-9 — EasyWeek review_3d после booking-succeeded (M) — ✅ DONE" in text
    assert "### PR-10" in text and "✅ DONE" in text.split("### PR-10")[1][:400]
    assert "PR-12" in text, "the next PR is named"


@_PLAN_PRESENT
def test_the_plan_records_visits_total_as_a_root_level_number() -> None:
    text = PLAN.read_text()

    assert "visits_total" in text
    assert "root" in text.lower() or "корнев" in text.lower()


# ===========================================================================
# The preflight reads, and only reads
# ===========================================================================


@pytest.fixture(autouse=True)
def _preflight_config(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "easyweek_processing_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_notifications_enabled", False, raising=False)
    monkeypatch.setattr(settings, "easyweek_visit_counter_enabled", False, raising=False)
    monkeypatch.setattr(
        settings,
        "easyweek_allowed_service_categories",
        json.dumps(["Fixture Category"]),
        raising=False,
    )
    monkeypatch.setattr(
        settings,
        "easyweek_location_map",
        json.dumps(
            {
                "test-branch": {
                    "location_id": TEST_LOCATION_ID,
                    "location_uuid": TEST_LOCATION_UUID,
                    "meta_template_prefix": "tb",
                    "booking_page_url": "https://booking.example.invalid/test",
                }
            }
        ),
        raising=False,
    )


@pytest_asyncio.fixture
async def bound_session_local(
    session_maker: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> async_sessionmaker[AsyncSession]:
    monkeypatch.setattr(app_db, "SessionLocal", session_maker, raising=False)
    monkeypatch.setattr(worker, "SessionLocal", session_maker, raising=False)
    return session_maker


def _succeeded(*, visits_total=1, **overrides):
    payload = booking_created()
    payload["booking_status"] = "Succeeded appointment"
    payload["visits_total"] = visits_total
    payload.update(overrides)
    return payload


async def _seed_proven_visit(session_maker, *, visits_total=3, status="processed"):
    from altegio_bot.easyweek_normalizer import canonical_booking_uuid

    async with session_maker() as session:
        async with session.begin():
            session.add(
                EasyWeekEvent(
                    status="captured",
                    event_hint="booking-created",
                    auth_via="query",
                    payload_hash="created-1",
                    payload=booking_created(),
                    body_truncated=False,
                    booking_uuid=canonical_booking_uuid(booking_created()),
                )
            )
    for _ in range(5):
        if not await worker.process_one():
            break

    payload = _succeeded(visits_total=visits_total)
    async with session_maker() as session:
        async with session.begin():
            session.add(
                EasyWeekEvent(
                    status=status,
                    event_hint="booking-succeeded",
                    auth_via="query",
                    payload_hash="succeeded-1",
                    payload=payload,
                    body_truncated=False,
                    booking_uuid=canonical_booking_uuid(payload),
                )
            )


async def test_a_production_shaped_sample_is_green(bound_session_local) -> None:
    await _seed_proven_visit(bound_session_local)

    async with bound_session_local() as session:
        report = await preflight.run_visit_counter_preflight(session)

    assert report.ready is True
    assert report.candidate_count == 1
    assert report.green_count == 1
    assert report.truncated is False


async def test_an_empty_sample_is_never_green(bound_session_local) -> None:
    """A flag opened on "no problems found" in an empty queue is opened blind."""
    async with bound_session_local() as session:
        report = await preflight.run_visit_counter_preflight(session)

    assert report.candidate_count == 0
    assert report.ready is False


async def test_a_truncated_sample_is_never_green(bound_session_local) -> None:
    await _seed_proven_visit(bound_session_local)
    async with bound_session_local() as session:
        async with session.begin():
            payload = _succeeded(visits_total=4)
            from altegio_bot.easyweek_normalizer import canonical_booking_uuid

            session.add(
                EasyWeekEvent(
                    status="processed",
                    event_hint="booking-succeeded",
                    auth_via="query",
                    payload_hash="succeeded-2",
                    payload=payload,
                    body_truncated=False,
                    booking_uuid=canonical_booking_uuid(payload),
                )
            )

    async with bound_session_local() as session:
        report = await preflight.run_visit_counter_preflight(session, limit=1)

    assert report.truncated is True
    assert report.ready is False


async def test_an_unusable_visits_total_is_reported_and_never_green(bound_session_local) -> None:
    await _seed_proven_visit(bound_session_local, visits_total="3")

    async with bound_session_local() as session:
        report = await preflight.run_visit_counter_preflight(session)

    assert report.ready is False
    assert report.reasons.get("invalid_visits_total") == 1
    assert report.blocked_event_ids


async def test_the_preflight_writes_nothing(bound_session_local) -> None:
    await _seed_proven_visit(bound_session_local)

    async def _snapshot():
        async with bound_session_local() as session:
            client = (await session.execute(select(Client).where(Client.provider == "easyweek"))).scalars().one()
            statuses = list(
                (await session.execute(select(EasyWeekEvent.status).order_by(EasyWeekEvent.id))).scalars().all()
            )
            records = (
                await session.execute(select(func.count()).select_from(Record).where(Record.provider == "easyweek"))
            ).scalar_one()
            return (
                client.easyweek_visits_total,
                client.easyweek_visits_total_updated_at,
                statuses,
                records,
            )

    before = await _snapshot()
    async with bound_session_local() as session:
        await preflight.run_visit_counter_preflight(session)
    after = await _snapshot()

    assert before == after, "a preflight that changes anything is not a preflight"
    assert before[0] is None, "and it certainly does not backfill the counter"


async def test_the_report_carries_no_pii(bound_session_local) -> None:
    await _seed_proven_visit(bound_session_local)

    async with bound_session_local() as session:
        report = await preflight.run_visit_counter_preflight(session)
    printed = str(report.as_safe_dict())

    for marker in PII_MARKERS:
        assert marker not in printed, marker
    assert str(TEST_CUSTOMER_ID) not in printed, "an external customer id is not a technical id of ours"


@pytest.mark.parametrize(
    ("flag", "value", "expected"),
    [
        pytest.param("easyweek_visit_counter_enabled", True, "visit_counter_already_enabled", id="already_on"),
        pytest.param("easyweek_processing_enabled", False, "processing_disabled", id="processing_off"),
    ],
)
def test_a_wrong_rollout_state_short_circuits_the_audit(monkeypatch, flag, value, expected) -> None:
    monkeypatch.setattr(settings, flag, value, raising=False)

    assert preflight.rollout_state_error() == expected


def test_an_unready_location_registry_short_circuits_the_audit(monkeypatch) -> None:
    monkeypatch.setattr(settings, "easyweek_location_map", "{not json", raising=False)

    assert preflight.rollout_state_error() == "location_registry_unready"


async def test_a_config_error_does_not_read_the_queue(bound_session_local, monkeypatch) -> None:
    await _seed_proven_visit(bound_session_local)
    monkeypatch.setattr(settings, "easyweek_visit_counter_enabled", True, raising=False)

    async with bound_session_local() as session:
        report = await preflight.run_visit_counter_preflight(session)

    assert report.config_error == "visit_counter_already_enabled"
    assert report.candidate_count == 0, "auditing a world that no longer exists proves nothing"
    assert report.ready is False
