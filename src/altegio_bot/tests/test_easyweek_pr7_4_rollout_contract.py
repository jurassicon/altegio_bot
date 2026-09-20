"""Source-controlled rollout fences and docs for PR-7.4."""

from __future__ import annotations

import inspect
from pathlib import Path

import pytest
import yaml

from altegio_bot import easyweek_multi_service
from altegio_bot import easyweek_multi_service_recovery as recovery_module
from altegio_bot import easyweek_resource_shadow_contract as resource_shadow_contract
from altegio_bot import easyweek_snapshot_recovery as snapshot_recovery
from altegio_bot.easyweek_multi_service_recovery import build_recovery_plan
from altegio_bot.easyweek_snapshot_recovery import build_snapshot_recovery_plan
from altegio_bot.scripts import easyweek_multi_service_preflight as preflight
from altegio_bot.scripts import easyweek_multi_service_reminder_recovery as recovery_cli
from altegio_bot.scripts import easyweek_multi_service_snapshot_recovery as snapshot_recovery_cli
from altegio_bot.settings import Settings
from altegio_bot.workers import easyweek_inbox_worker as inbox_worker

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


def _runbook_section(title: str) -> str:
    """The text of one runbook section, up to the next top-level heading."""
    text = RUNBOOK.read_text(encoding="utf-8")
    assert title in text, f"missing runbook section: {title}"
    after = text.split(title, 1)[1]
    remainder = [part for part in after.split("\n## ") if part]
    return remainder[0] if len(after.split("\n## ")) > 1 else after


def _fenced_blocks(section: str) -> str:
    """Only the copy-paste command blocks, so prose cannot satisfy a check."""
    parts = section.split("```")
    return "\n".join(parts[index] for index in range(1, len(parts), 2))


CANARY_TITLE = "## 18. Controlled suppression canary"
SEND_FENCE_TITLE = "## 20. Запрет на открытие общего send fence"


def test_the_resource_shadow_canary_is_a_suppression_canary_not_a_send_canary() -> None:
    """The observed shapes are Nagelservice, so a v2 canary cannot send.

    Expecting a queued version 2 job or a customer-facing render in production
    would require temporarily allowing `Nagelservice`, which §38.6 forbids. The
    runbook therefore has to ask for proven suppression, and to say that the
    runtime render is proved by tests rather than on production.
    """
    canary = _runbook_section(CANARY_TITLE)
    for required in (
        "suppression canary",
        "multi_service_category_not_allowed",
        "structurally_proven",
        "integration-тестами",
    ):
        assert required in canary


def test_the_canary_is_bound_to_the_exact_canary_booking_uuid() -> None:
    """A rollout check that matches "some record" proves nothing.

    Between creating the controlled booking and running the query, a live
    branch produces other records — including older resource-shadow ones with
    no jobs and no outbox rows. Every canary statement must therefore select on
    the full provider/company/booking identity.
    """
    canary = _runbook_section(CANARY_TITLE)
    blocks = _fenced_blocks(canary)

    # The operator has to write the exact UUID down before any diagnosis.
    assert "booking UUID" in canary
    assert "CANARY_BOOKING_UUID=" in blocks

    # Every SELECT over `records` carries the whole identity triple.
    record_selects = [
        statement
        for statement in blocks.split(";")
        if "FROM records" in statement.replace("\n", " ") or "FROM records r" in statement.replace("\n", " ")
    ]
    assert len(record_selects) >= 2, "expected an identity assertion and a detail query"
    for statement in record_selects:
        flattened = " ".join(statement.split())
        assert "provider = 'easyweek'" in flattened
        assert "company_id = 322579" in flattened
        assert "easyweek_booking_uuid = :'canary'::uuid" in flattened

    # The UUID travels as a psql value, never as concatenated SQL text.
    assert ":'canary'" in blocks
    assert "||" not in blocks
    assert "$CANARY_BOOKING_UUID" in blocks


def test_the_canary_never_guesses_which_record_it_found() -> None:
    """The defect this replaced: ORDER BY r.id DESC LIMIT 5 over the company."""
    canary = _runbook_section(CANARY_TITLE)
    blocks = _fenced_blocks(canary)
    flattened = " ".join(blocks.split())

    for forbidden in ("ORDER BY", "LIMIT", "ORDER BY r.id DESC", "LIMIT 5"):
        assert forbidden not in flattened, f"canary must not rank or truncate: {forbidden}"

    # Exactly one match is asserted by the SQL itself, not by eyeballing rows.
    assert "exactly_one_canary_record" in blocks
    assert "1 / (count(*) = 1)::int" in flattened
    assert "ON_ERROR_STOP=1" in blocks


def test_the_canary_prints_only_safe_technical_fields() -> None:
    canary = _runbook_section(CANARY_TITLE)
    blocks = _fenced_blocks(canary)

    for required in (
        "AS record_id",
        "AS company_id",
        "AS booking_uuid",
        "AS services_count",
        "AS snapshot_version",
        "AS snapshot_digest",
        "AS proof_kind",
        "AS contract_revision",
        "AS contract_digest",
        "AS snapshot_lines",
        "AS line_1_category",
        "AS line_2_category",
        "AS jobs",
        "AS outbox",
    ):
        assert required in blocks, f"canary does not report {required}"

    # No customer-facing column may be selected.
    lowered = blocks.lower()
    for forbidden in (
        "customer",
        "phone",
        "email",
        "display_name",
        "notes",
        "comment",
        "short_link",
        "booking_page",
        "manage_link",
    ):
        assert forbidden not in lowered, f"canary query would print PII: {forbidden}"


def test_the_canary_states_one_unambiguous_expected_result() -> None:
    canary = _runbook_section(CANARY_TITLE)
    flattened = " ".join(canary.split())
    for required in (
        "services_count = 2",
        "snapshot_version = 2",
        "proof_kind = karlsruhe_resource_shadow",
        "contract_revision =",
        "contract_digest =",
        "snapshot_lines = 2",
        "line_1_category = Nagelservice",
        "line_2_category = Nagelservice",
        "jobs = 0",
        "outbox = 0",
    ):
        assert required in flattened, f"canary does not pin the expected {required}"
    assert "ровно одна строка" in flattened


def test_the_suppression_reason_is_bound_to_the_exact_record_id() -> None:
    """A lone `category_not_allowed` line belongs to any of the 17 records."""
    canary = _runbook_section(CANARY_TITLE)
    blocks = _fenced_blocks(canary)

    assert "CANARY_RECORD_ID=" in blocks
    assert "record_id=${CANARY_RECORD_ID}" in blocks
    assert "reason=multi_service_category_not_allowed" in blocks
    # The record id comes from the identity-bound query, not from the log.
    assert "18.2" in canary
    flattened = " ".join(canary.split())
    assert "доказательством не является" in flattened
    assert "не** PASS" in flattened or "не PASS" in flattened


def test_the_canary_and_the_aggregate_preflight_are_both_required() -> None:
    canary = _runbook_section(CANARY_TITLE)
    flattened = " ".join(canary.split())
    assert "read-only" in flattened
    assert "агрегат" in flattened
    assert "нужны оба" in flattened


def test_the_canary_lists_its_fail_closed_stop_conditions() -> None:
    canary = _runbook_section(CANARY_TITLE)
    flattened = " ".join(canary.split())
    for required in (
        "не найден",
        "больше одной записи",
        "snapshot_lines",
        "Nagelservice",
        "EASYWEEK_ALLOWED_SERVICE_CATEGORIES",
        "ready=true",
    ):
        assert required in flattened, f"missing stop condition: {required}"
    assert "rollback" in flattened.lower()


def test_the_shared_send_fence_keeps_its_own_version_one_canary() -> None:
    gate = _runbook_section(SEND_FENCE_TITLE)
    # The shared send fence keeps its own PR-7.4 version 1 canary, and neither
    # step may be described as changing the production allowlist.
    assert "PR-7.4 send canary" in gate
    assert "version 1" in gate
    assert "send-canary не" in " ".join(gate.split())
    for forbidden in (
        "EASYWEEK_ALLOWED_SERVICE_CATEGORIES=[",
        'EASYWEEK_ALLOWED_SERVICE_CATEGORIES=["Nagelservice"]',
    ):
        assert forbidden not in RUNBOOK.read_text(encoding="utf-8")


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


# ===========================================================================
# PR-7.5 operator-only snapshot recovery (v1 -> v2)
# ===========================================================================

SNAPSHOT_RECOVERY_SERVICE = "easyweek-multi-service-snapshot-recovery"
RECOVERY_TITLE = "# PR-7.5 — operator-only recovery старых snapshot version 1"


def _recovery_part() -> str:
    text = RUNBOOK.read_text(encoding="utf-8")
    assert RECOVERY_TITLE in text, "the runbook has no snapshot-recovery part"
    # Stop at the next top-level part, so a later section cannot be read as
    # part of the recovery contract.
    return text.split(RECOVERY_TITLE, 1)[1].split("\n# ", 1)[0]


@_PLAN_PRESENT
def test_the_canonical_plan_authorises_the_snapshot_recovery_narrowly() -> None:
    text = PLAN.read_text(encoding="utf-8")
    section = text.split("### 38.7", 1)[1]
    for required in (
        "operator-only",
        "plan",
        "apply",
        "verify",
        "Record.raw.easyweek.multi_service_snapshot",
        "karlsruhe_resource_shadow",
        "multi_service_category_not_allowed",
        "EASYWEEK_MULTI_SERVICE_SEND_ENABLED=false",
        "13934",
    ):
        assert required in section
    flattened = " ".join(section.split())
    # The narrow boundaries the owner authorised, restated as refusals.
    for required in (
        "автоматический background backfill",
        "не разрешает отправку",
        "не меняет business scope",
        "не попадают ни в код, ни в тесты как селектор",
    ):
        assert required in flattened


def test_the_runbook_documents_the_full_operator_order() -> None:
    part = _recovery_part()
    for heading in (
        "## 21. Deploy",
        "## 22. Проверка фактических значений флагов",
        "## 23. Приватный каталог",
        "## 24. Read-only plan",
        "## 25. Проверка ожидаемых technical Record IDs",
        "## 26. Получение plan digest",
        "## 27. Apply",
        "## 28. Verify",
        "## 29. Повторный multi-service preflight",
        "## 30. Ожидаемый переход preflight",
        "## 31. Rollback и fail-closed условия",
        "## 32. Запрет ручного SQL",
        "## 33. Запрет повторного использования старого plan",
        "## 34. Запрет открытия общего send fence",
    ):
        assert heading in part, f"missing runbook step: {heading}"


def test_every_recovery_command_names_both_production_compose_files() -> None:
    """A command with only one Compose file would target a different graph."""
    blocks = _fenced_blocks(_recovery_part())
    invocations = [line for line in blocks.splitlines() if "docker compose" in line]
    assert invocations, "the recovery part documents no Compose command"
    for line in invocations:
        assert "-f docker-compose.yml" in line, line
        assert "-f docker-compose.chatwoot-internal.yml" in line, line


def test_the_runbook_and_the_cli_agree_on_the_apply_command() -> None:
    """The CLI must not print a command the runbook contradicts."""
    printed = snapshot_recovery_cli.apply_command(
        plan_path="/recovery/snapshot-plan.json",
        apply_report="/recovery/snapshot-apply.json",
        plan_digest="0" * 64,
        max_snapshot_age_sec=600,
    )
    flattened = " ".join(printed.replace("\\\n", " ").split())
    assert "docker compose -p altegio_bot" in flattened
    assert "-f docker-compose.yml -f docker-compose.chatwoot-internal.yml" in flattened
    assert "--profile ops run --rm --build" in flattened
    assert "easyweek-multi-service-snapshot-recovery apply" in flattened
    # The host trap the review found: a module path and a host /recovery.
    assert "python -m altegio_bot" not in printed

    blocks = _fenced_blocks(_recovery_part())
    runbook_apply = " ".join(
        blocks.split("easyweek-multi-service-snapshot-recovery apply", 1)[1].split("\n\n", 1)[0].split()
    )
    for flag in ("--plan", "--apply-report", "--plan-digest", "--confirm", "--max-snapshot-age-sec"):
        assert flag in runbook_apply, flag
        assert flag in flattened, flag


def test_the_runbook_documents_the_same_plan_retry() -> None:
    part = _recovery_part()
    assert "## 27a." in part
    flattened = " ".join(part.split())
    for required in (
        "already_applied",
        "migrated_this_run_record_ids",
        "already_applied_record_ids",
        "partial_apply_detected",
        "единой мутации БД",
    ):
        assert required in flattened, f"missing retry contract: {required}"
    # Section 33 must not contradict the documented retry.
    assert "Единственное исключение" in flattened


def test_the_recovery_commands_use_the_real_compose_files_and_ops_service() -> None:
    part = _recovery_part()
    blocks = _fenced_blocks(part)
    assert "docker-compose.yml" in blocks
    assert "docker-compose.chatwoot-internal.yml" in blocks
    assert f"{SNAPSHOT_RECOVERY_SERVICE} plan" in blocks
    assert f"{SNAPSHOT_RECOVERY_SERVICE} apply" in blocks
    assert f"{SNAPSHOT_RECOVERY_SERVICE} verify" in blocks
    # The apply command carries the exact digest and confirmation phrase.
    assert '--plan-digest "$PLAN_DIGEST"' in blocks
    assert '--confirm "migrate easyweek multi-service snapshots $PLAN_DIGEST"' in blocks
    assert "--max-snapshot-age-sec" in blocks


def test_the_recovery_runbook_states_the_expected_transition_and_stop_rules() -> None:
    part = _recovery_part()
    flattened = " ".join(part.split())
    for required in (
        "candidates=5",
        "source_version_1=5",
        "target_version_2=5",
        "blocked=0",
        "apply_ready=true",
        "stale_snapshot_digest: 5 -> 0",
        "unexplained: 5 -> 0",
        "passed=true",
        "не хардкод",
    ):
        assert required in flattened, f"missing expected outcome: {required}"
    # The six deadline-expired reminders stay out of this change.
    for required in ("13934", "13939", "не отменяет, не восстанавливает и не отправляет"):
        assert required in flattened


def test_the_recovery_runbook_forbids_manual_sql_and_plan_reuse() -> None:
    flattened = " ".join(_recovery_part().split())
    assert "Ручные `UPDATE` или `DELETE` по `records.raw` запрещены" in flattened
    assert "Plan одноразовый" in flattened
    assert "нужен новый `plan` и новый digest" in flattened


def test_the_recovery_never_asks_to_open_the_send_fence_or_the_allowlist() -> None:
    part = _recovery_part()
    assert "EASYWEEK_MULTI_SERVICE_SEND_ENABLED=true" not in _fenced_blocks(part)
    for line in part.splitlines():
        stripped = line.strip()
        if stripped.startswith("EASYWEEK_ALLOWED_SERVICE_CATEGORIES"):
            raise AssertionError(f"recovery runbook assigns the allowlist: {stripped}")
        if stripped == "EASYWEEK_MULTI_SERVICE_SEND_ENABLED=true":
            raise AssertionError("recovery runbook opens the shared send fence")


def test_the_snapshot_recovery_is_an_ops_only_one_off_with_a_private_mount() -> None:
    service = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))["services"][SNAPSHOT_RECOVERY_SERVICE]
    assert service["profiles"] == ["ops"]
    assert service["restart"] == "no"
    assert service["command"] == ["--help"]
    assert any("easyweek.env" in str(item) for item in service["env_file"])
    assert any(":/recovery" in item for item in service["volumes"])
    assert "easyweek_multi_service_snapshot_recovery" in " ".join(service["entrypoint"])


def test_the_snapshot_recovery_defaults_to_plan_and_cannot_write_without_authorisation() -> None:
    args = snapshot_recovery_cli.build_parser().parse_args([])
    assert args.mode == "plan"
    apply_args = snapshot_recovery_cli.build_parser().parse_args(["apply"])
    assert apply_args.plan is None
    assert apply_args.plan_digest is None
    assert apply_args.confirm is None


def test_the_snapshot_recovery_plan_function_has_no_write_primitive() -> None:
    source = inspect.getsource(build_snapshot_recovery_plan)
    for forbidden in (".commit(", "session.add(", ".delete(", "pg_insert(", "MessageJob(", "OutboxMessage("):
        assert forbidden not in source
    assert "await session.rollback()" in inspect.getsource(snapshot_recovery_cli)


# ===========================================================================
# §38.8: authoritative services_count
# ===========================================================================

COUNT_SEMANTICS_TITLE = "# §38.8 — rollout после фикса count semantics"


@_PLAN_PRESENT
def test_the_canonical_plan_records_the_authoritative_count_revision() -> None:
    text = PLAN.read_text(encoding="utf-8")
    section = text.split("### 38.8", 1)[1]
    flattened = " ".join(section.split())
    for required in (
        "authoritative whole-set count",
        "ordered_services[].quantity",
        "точный integer `1` или `2`",
        "остаётся точным integer `1`",
        "полный live proof обязателен",
        "`2→1` по-прежнему отзывает snapshot",
        "запускает полный re-proof",
        "не добавляется",
        "rollout evidence",
    ):
        assert required in flattened, f"missing normative statement: {required}"


def test_the_three_counts_are_named_apart_in_one_shared_primitive() -> None:
    """The relaxed envelope rule must be unreachable from an API line."""
    source = inspect.getsource(easyweek_multi_service)
    assert source.count("def authoritative_services_count(") == 1
    assert source.count("def envelope_quantity(") == 1
    assert source.count("def _order_line_quantity(") == 1
    assert easyweek_multi_service.ALLOWED_ENVELOPE_QUANTITIES == (1, 2)
    assert easyweek_multi_service.REQUIRED_ORDER_LINE_QUANTITY == 1
    assert easyweek_multi_service.EXACTLY_TWO_SERVICES == 2

    # Exactly one caller each, so no consumer can pick the wrong rule.
    assert source.count("authoritative_services_count(pair.services_count)") == 1
    assert source.count("envelope_quantity(pair.quantity)") == 1
    assert source.count('_order_line_quantity(value.get("quantity"))') == 1

    # And the rules really are different.
    assert easyweek_multi_service.envelope_quantity(1) == 1
    assert easyweek_multi_service.envelope_quantity(2) == 2
    for rejected in (None, True, False, 0, 3, -1, "1", 1.0):
        assert easyweek_multi_service.envelope_quantity(rejected) is None
        assert easyweek_multi_service.authoritative_services_count(rejected) is None
    assert easyweek_multi_service.authoritative_services_count(2) == 2
    assert easyweek_multi_service.authoritative_services_count(1) is None


def test_no_consumer_reimplements_the_envelope_quantity_rule() -> None:
    """One primitive decides the envelope rule; no consumer repeats it.

    The authoritative-count revoke rule (`services_count != 2` clears the
    snapshot) deliberately stays in the inbox worker — §38.8 keeps it — so the
    check below targets the ENVELOPE quantity specifically.
    """
    for module in (preflight, inbox_worker, snapshot_recovery, recovery_module):
        source = inspect.getsource(module)
        for forbidden in ("quantity != 2", "quantity == 2", "quantity != 1", "quantity in (1, 2)"):
            assert forbidden not in source, f"{module.__name__} re-implements {forbidden}"
        # They all go through the one shared proof instead.
        assert "prove_exactly_two_service_snapshot" in source or "fetch_and_prove" in source

    # The authoritative count still revokes, and that rule is still there.
    assert "booking.services_count != 2" in inspect.getsource(inbox_worker)


def test_the_runbook_pins_the_count_semantics_rollout_order() -> None:
    text = RUNBOOK.read_text(encoding="utf-8")
    assert COUNT_SEMANTICS_TITLE in text
    part = text.split(COUNT_SEMANTICS_TITLE, 1)[1]
    for heading in (
        "## 35. Deploy при закрытом send fence",
        "## 36. Проверка фактических значений флагов",
        "## 37. Свежий multi-service preflight",
        "## 38. Отдельный общий reminder preflight",
        "## 39. Owner canary",
        "## 40. Открытие send fence",
        "## 41. Проверка после открытия",
        "## 42. Rollback",
    ):
        assert heading in part, f"missing rollout step: {heading}"

    flattened = " ".join(part.split())
    for required in (
        "EASYWEEK_MULTI_SERVICE_SEND_ENABLED=false",
        "ready: false -> true",
        "ambiguous: 1 -> 0",
        "unexplained: 1 -> 0",
        "open_jobs == jobs_held_by_send_fence",
        "Он тоже обязан вернуть `ready=true`",
        "не является** причиной менять count proof",
        "14143",
        "rollout evidence, а не контракт",
    ):
        assert required in flattened, f"missing rollout assertion: {required}"

    # Every production command names both Compose files.
    blocks = _fenced_blocks(part)
    for line in blocks.splitlines():
        if "docker compose" in line:
            assert "-f docker-compose.yml" in line, line
            assert "-f docker-compose.chatwoot-internal.yml" in line, line

    # The send fence is opened only in its own step, after both preflights.
    opening = part.split("## 40. Открытие send fence", 1)[1]
    assert "EASYWEEK_MULTI_SERVICE_SEND_ENABLED=true" in opening
    assert "EASYWEEK_MULTI_SERVICE_SEND_ENABLED=true" not in part.split("## 40.", 1)[0]
