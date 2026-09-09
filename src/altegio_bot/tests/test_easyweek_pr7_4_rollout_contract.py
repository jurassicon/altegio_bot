"""Source-controlled rollout fences and docs for PR-7.4."""

from __future__ import annotations

import inspect
from pathlib import Path

import pytest
import yaml

from altegio_bot.easyweek_multi_service_recovery import build_recovery_plan
from altegio_bot.scripts import easyweek_multi_service_preflight as preflight
from altegio_bot.scripts import easyweek_multi_service_reminder_recovery as recovery_cli
from altegio_bot.settings import Settings

ROOT = Path(__file__).resolve().parents[3]
PLAN = ROOT / "docs/easyweek/INTEGRATION_PLAN.md"
RUNBOOK = ROOT / "docs/easyweek/pr7_4_two_service_notifications_runbook.md"
ENV_EXAMPLE = ROOT / "easyweek.env.example"
COMPOSE = ROOT / "docker-compose.yml"
_PLAN_PRESENT = pytest.mark.skipif(not PLAN.exists(), reason="INTEGRATION_PLAN.md is untracked (.gitignore)")


def test_both_multi_service_fences_default_false() -> None:
    fields = Settings.model_fields
    assert fields["easyweek_multi_service_notifications_enabled"].default is False
    assert fields["easyweek_multi_service_send_enabled"].default is False


@_PLAN_PRESENT
def test_canonical_plan_records_owner_authorization_and_narrow_scope() -> None:
    text = PLAN.read_text(encoding="utf-8")
    section = text.split("## 34. Ревизия 31 — exactly-two-service notifications (PR-7.4)", 1)[1]
    for required in (
        "09.09.2026",
        "20 из 20",
        "ровно двух разных",
        "all-categories policy",
        "EASYWEEK_MULTI_SERVICE_NOTIFICATIONS_ENABLED=false",
        "EASYWEEK_MULTI_SERVICE_SEND_ENABLED=false",
        "Single-service",
        "replay/resend запрещены",
        "review_3d",
        "repeat_10d",
        "comeback_3d",
        "fail-closed",
        "Controlled recovery",
        "historical backfill",
        "automatic resend",
        "outbox worker",
        "contract_not_supported",
        "multi_service_custom_duration_unsupported",
        "structurally_proven + contract_excluded_records == records_seen",
    ):
        assert required in section


def test_runbook_pins_safe_rollout_preflight_canary_and_rollback() -> None:
    text = RUNBOOK.read_text(encoding="utf-8")
    for required in (
        "EASYWEEK_MULTI_SERVICE_NOTIFICATIONS_ENABLED=true",
        "EASYWEEK_MULTI_SERVICE_SEND_ENABLED=false",
        "easyweek-multi-service-reminder-recovery plan",
        "easyweek-multi-service-reminder-recovery apply",
        "easyweek-multi-service-reminder-recovery verify",
        "records_seen≈20",
        "structurally_proven≈14",
        "allowed_records≈4",
        "disallowed_records≈10",
        "contract_excluded_records≈6",
        "reminders_to_create=0..8",
        "easyweek_reminder_preflight",
        "force-recreate",
        "Controlled canary",
        "не создаёт `record_created` задним числом",
        "не создаёт `OutboxMessage`",
        "Rollback",
    ):
        assert required in text


def test_env_example_exposes_both_closed_fences() -> None:
    text = ENV_EXAMPLE.read_text(encoding="utf-8")
    assert text.count("EASYWEEK_MULTI_SERVICE_NOTIFICATIONS_ENABLED=false") == 1
    assert text.count("EASYWEEK_MULTI_SERVICE_SEND_ENABLED=false") == 1


def test_preflight_source_has_no_write_or_send_capability() -> None:
    source = inspect.getsource(preflight)
    assert "select(" in source
    for forbidden in (
        ".commit(",
        ".add(",
        ".delete(",
        "insert(",
        "update(",
        "safe_send",
        "ChatwootClient(",
        "post_booking",
        "cancel_booking",
    ):
        assert forbidden not in source


def test_recovery_defaults_to_plan_and_plan_function_has_no_write_primitive() -> None:
    args = recovery_cli.build_parser().parse_args([])
    assert args.mode == "plan"
    source = inspect.getsource(build_recovery_plan)
    for forbidden in (".commit(", ".add(", ".delete(", "pg_insert(", "OutboxMessage("):
        assert forbidden not in source


def test_apply_requires_an_explicit_snapshot_argument() -> None:
    args = recovery_cli.build_parser().parse_args(["apply"])
    assert args.snapshot is None
    assert args.plan_digest is None
    assert args.confirm is None


def test_recovery_runner_is_an_ops_only_one_off_with_private_state_mount() -> None:
    service = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))["services"][
        "easyweek-multi-service-reminder-recovery"
    ]
    assert service["profiles"] == ["ops"]
    assert service["restart"] == "no"
    assert any("easyweek.env" in str(item) for item in service["env_file"])
    assert any(":/recovery" in item for item in service["volumes"])
    assert "easyweek_multi_service_reminder_recovery" in " ".join(service["entrypoint"])
