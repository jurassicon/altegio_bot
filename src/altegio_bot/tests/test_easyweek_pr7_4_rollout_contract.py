"""Source-controlled rollout fences and docs for PR-7.4."""

from __future__ import annotations

import inspect
from pathlib import Path

import pytest
import yaml

from altegio_bot import easyweek_resource_shadow_contract as resource_shadow_contract
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


# ===========================================================================
# PR-7.5: the Karlsruhe resource-shadow fence
# ===========================================================================


def test_the_resource_shadow_fence_defaults_false_and_is_independent() -> None:
    fields = Settings.model_fields
    assert fields["easyweek_resource_shadow_proof_enabled"].default is False
    # Three separate switches, not one widened flag.
    assert fields["easyweek_multi_service_notifications_enabled"].default is False
    assert fields["easyweek_multi_service_send_enabled"].default is False


@_PLAN_PRESENT
def test_canonical_plan_records_the_normative_resource_shadow_scope() -> None:
    text = PLAN.read_text(encoding="utf-8")
    section = text.split("### 38.6 Ревизия 38 — production evidence корректирует исходную гипотезу", 1)[1]
    for required in (
        "company_id=322579",
        "8395fab6-7ee8-4702-88d9-fd78f92539c1",
        "1030228",
        "1030246",
        "Hygienische Pediküre für Damen",
        "Pediküre mit Gel-Lack",
        "Pediküre Mit French",
        "multi_service_duplicate_ambiguous",
        "multi_service_category_not_allowed",
        "catalog UUID не хардкодится",
        "Durlach и Rastatt поведение не меняется",
        "default-false kill switch",
        "13934",
    ):
        assert required in section


def test_the_static_contract_lives_in_one_module_with_provenance() -> None:
    source = inspect.getsource(resource_shadow_contract)
    assert "8395fab6-7ee8-4702-88d9-fd78f92539c1" in source
    assert "322579" in source
    # Catalogue service UUIDs are resolved live, never pinned.
    assert source.count("uuid.UUID") == 1
    for marker in ("Provenance", "revision", "digest"):
        assert marker in source
    # A contract module cannot reach the database, the API or a sender.
    for forbidden in ("select(", "session", "httpx", "requests", "EasyWeekClient"):
        assert forbidden not in source


def test_runbook_pins_the_resource_shadow_rollout_and_rollback() -> None:
    text = RUNBOOK.read_text(encoding="utf-8")
    for required in (
        "EASYWEEK_RESOURCE_SHADOW_PROOF_ENABLED=false",
        "EASYWEEK_RESOURCE_SHADOW_PROOF_ENABLED=true",
        "easyweek_multi_service_preflight",
        "structurally_proven=17",
        "disallowed_by_category=17",
        "ambiguous=0",
        "allowed=0",
        "ready=true",
        "multi_service_category_not_allowed",
        "Controlled suppression canary",
        "Rollback нового fence",
        "13934",
        "13939",
    ):
        assert required in text
    # The rollback section closes the NEW fence first.
    rollback = text.split("## 19. Rollback нового fence", 1)[1]
    assert "EASYWEEK_RESOURCE_SHADOW_PROOF_ENABLED=false" in rollback
    # Opening the shared send fence stays gated on all three green checks.
    gate = text.split("## 20. Запрет на открытие общего send fence", 1)[1]
    for required in ("multi-service preflight", "reminder preflight", "canary"):
        assert required in gate


def test_the_resource_shadow_canary_is_a_suppression_canary_not_a_send_canary() -> None:
    """The observed shapes are Nagelservice, so a v2 canary cannot send.

    Expecting a queued version 2 job or a customer-facing render in production
    would require temporarily allowing `Nagelservice`, which §38.6 forbids. The
    runbook therefore has to ask for proven suppression, and to say that the
    runtime render is proved by tests rather than on production.
    """
    text = RUNBOOK.read_text(encoding="utf-8")
    canary = text.split("## 18. Controlled suppression canary", 1)[1].split("## 19.", 1)[0]
    for required in (
        "suppression canary",
        "multi_service_category_not_allowed",
        "`version: 2`",
        "structurally_proven",
        "не изменялся",
        "integration-тестами",
    ):
        assert required in canary
    # It must demand the ABSENCE of the queue the old text asked for.
    for required in ("`jobs = 0`", "`outbox = 0`", "отсутствуют"):
        assert required in canary

    gate = text.split("## 20. Запрет на открытие общего send fence", 1)[1]
    # The shared send fence keeps its own PR-7.4 version 1 canary, and neither
    # step may be described as changing the production allowlist.
    assert "PR-7.4 send canary" in gate
    assert "version 1" in gate
    for forbidden in (
        "EASYWEEK_ALLOWED_SERVICE_CATEGORIES=[",
        'EASYWEEK_ALLOWED_SERVICE_CATEGORIES=["Nagelservice"]',
    ):
        assert forbidden not in text


def test_the_runbook_never_asks_to_allow_the_suppressed_category() -> None:
    text = RUNBOOK.read_text(encoding="utf-8")
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("EASYWEEK_ALLOWED_SERVICE_CATEGORIES"):
            raise AssertionError(f"runbook assigns the category allowlist: {stripped}")


def test_env_example_exposes_the_third_closed_fence() -> None:
    text = ENV_EXAMPLE.read_text(encoding="utf-8")
    assert text.count("EASYWEEK_RESOURCE_SHADOW_PROOF_ENABLED=false") == 1


def test_both_services_that_read_the_new_fence_are_documented_in_compose() -> None:
    text = COMPOSE.read_text(encoding="utf-8")
    assert text.count("EASYWEEK_RESOURCE_SHADOW_PROOF_ENABLED") == 2
