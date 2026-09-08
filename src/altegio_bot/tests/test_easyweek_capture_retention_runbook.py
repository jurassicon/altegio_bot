"""PR-14 contract for manual EasyWeek event retention documentation."""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
CAPTURE_RUNBOOK = REPO_ROOT / "docs/easyweek/capture_runbook.md"
CAMPAIGN_RUNBOOK = REPO_ROOT / "docs/easyweek/campaign_readiness_runbook.md"

START_MARKER = "<!-- easyweek-event-retention:start -->"
END_MARKER = "<!-- easyweek-event-retention:end -->"


def _retention_section() -> str:
    text = CAPTURE_RUNBOOK.read_text()
    start = text.index(START_MARKER)
    end = text.index(END_MARKER, start)
    return text[start:end]


def _sql_blocks(section: str) -> list[str]:
    return re.findall(r"```sql\n(.*?)```", section, flags=re.S)


def test_retention_delete_excludes_durable_campaign_source_events() -> None:
    blocks = _sql_blocks(_retention_section())
    assert len(blocks) == 1
    sql = blocks[0]
    delete = sql[sql.index("DELETE FROM easyweek_events") : sql.index("RETURNING 1")]

    assert "NOT EXISTS" in delete
    assert "FROM campaign_recipients AS cr" in delete
    assert "cr.provider = 'easyweek'" in delete
    assert "cr.source_easyweek_event_id = e.id" in delete
    assert "CASCADE" not in sql.upper()


def test_retention_command_is_transactional_and_verifies_both_outcomes() -> None:
    sql = _sql_blocks(_retention_section())[0]
    preview = sql[: sql.index("DELETE FROM easyweek_events")]

    assert "\\set ON_ERROR_STOP on" in sql
    assert sql.index("BEGIN;") < sql.index("DELETE FROM easyweek_events") < sql.index("COMMIT;")
    assert sql.index("old_events_total") < sql.index("DELETE FROM easyweek_events")
    assert sql.index("deletable_old_events") < sql.index("DELETE FROM easyweek_events")
    assert "EXISTS" in preview and "NOT EXISTS" in preview
    assert "cr.provider = 'easyweek'" in preview
    assert "cr.source_easyweek_event_id = old_events.id" in preview
    assert "remaining_deletable_old_events" in sql
    assert "retained_campaign_source_events" in sql
    assert "RAISE EXCEPTION 'easyweek event retention verification failed'" in sql


def test_runbooks_warn_that_campaign_source_events_remain_retained() -> None:
    retention = _retention_section().lower()
    campaign = CAMPAIGN_RUNBOOK.read_text().lower()

    assert "durable source proof" in retention
    assert "skips every old event referenced" in campaign
    assert "does not physically delete" in campaign
    assert "outside pr-14" in campaign
    assert "separate" in campaign and "authorised" in campaign


def test_retention_sql_outputs_counts_only_and_no_payload_or_pii() -> None:
    sql = _sql_blocks(_retention_section())[0].lower()

    for forbidden in (
        "payload",
        "body_raw",
        "phone",
        "customer",
        "booking_uuid",
        "display_name",
        "email",
    ):
        assert forbidden not in sql
    assert "returning e.id" not in sql
    assert "returning 1" in sql
