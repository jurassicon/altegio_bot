"""§38.9: the pair-queue canary parser, and the configuration a report is about.

No database and no network. What is proven here is the total behaviour of the
one parser both the claim predicate and the race guard call, and the
fail-closed configuration gate both read-only preflights call.

The single property every case below serves: a value the operator did not
mean can hold the whole queue, but it can never release it.
"""

from __future__ import annotations

import pytest

from altegio_bot.easyweek_multi_service import (
    MULTI_SERVICE_DISABLED,
    MULTI_SERVICE_RESOURCE_SHADOW_DISABLED,
    MULTI_SERVICE_SEND_DISABLED,
)
from altegio_bot.easyweek_multi_service_rollout import (
    EASYWEEK_NOTIFICATIONS_DISABLED,
    EASYWEEK_REMINDER_API_GUARD_DISABLED,
    EASYWEEK_REMINDERS_DISABLED,
    MULTI_SERVICE_CANARY_CONFIGURED,
    MULTI_SERVICE_CANARY_INVALID,
    MULTI_SERVICE_CANARY_NOT_CONFIGURED,
    MULTI_SERVICE_SEND_FENCE_OPEN,
    RolloutPhase,
    multi_service_canary,
    multi_service_configuration_error,
    parse_multi_service_canary_job_id,
)
from altegio_bot.settings import Settings, settings

# Every flag §38.9 requires, in the pre-open state the runbook deploys.
_PRE_OPEN = {
    "easyweek_notifications_enabled": True,
    "easyweek_reminders_enabled": True,
    "easyweek_multi_service_notifications_enabled": True,
    "easyweek_resource_shadow_proof_enabled": True,
    "easyweek_reminder_api_guard_enabled": True,
    "easyweek_multi_service_send_enabled": False,
    "easyweek_multi_service_canary_job_id": "",
}


@pytest.fixture
def pre_open(monkeypatch: pytest.MonkeyPatch) -> None:
    for name, value in _PRE_OPEN.items():
        monkeypatch.setattr(settings, name, value, raising=False)


# ===========================================================================
# The parser
# ===========================================================================


@pytest.mark.parametrize("raw", ["", "   ", "\t\n", None])
def test_an_empty_value_is_no_restriction_at_all(raw: object) -> None:
    canary = parse_multi_service_canary_job_id(raw)
    assert canary.configured is False
    assert canary.valid is True
    assert canary.restricted is False
    assert canary.job_id is None
    assert canary.unavailable_reason is None


@pytest.mark.parametrize(("raw", "expected"), [("7", 7), (" 7 ", 7), ("14211", 14211), (3, 3)])
def test_one_positive_decimal_id_restricts_the_queue_to_that_job(raw: object, expected: int) -> None:
    canary = parse_multi_service_canary_job_id(raw)
    assert canary.restricted is True
    assert canary.job_id == expected
    assert canary.unavailable_reason is None


@pytest.mark.parametrize(
    "raw",
    [
        True,
        False,
        "true",
        "false",
        "yes",
        "0",
        0,
        "-1",
        -1,
        "+1",
        "1.0",
        1.0,
        "1e3",
        "1 2",
        "1,2",
        "1;2",
        "1 ,2",
        "١٤",  # Arabic-Indic digits: int() would accept them, the operator did not type them
        "７",  # fullwidth digit, same trap
        "٧",
        "7a",
        "a7",
        ["7"],
        {"job_id": 7},
        object(),
    ],
)
def test_anything_else_is_invalid_and_never_degrades_to_unrestricted(raw: object) -> None:
    canary = parse_multi_service_canary_job_id(raw)
    assert canary.configured is True
    assert canary.valid is False
    assert canary.restricted is False, "a malformed value must never release the queue"
    assert canary.job_id is None
    assert canary.unavailable_reason == MULTI_SERVICE_CANARY_INVALID


def test_a_bool_is_not_read_as_job_one() -> None:
    """`True` is an `int` in Python; job 1 is a real row in production."""
    assert parse_multi_service_canary_job_id(True).job_id is None


def test_the_setting_is_a_string_so_a_typo_cannot_break_worker_startup() -> None:
    field = Settings.model_fields["easyweek_multi_service_canary_job_id"]
    assert field.annotation is str
    assert field.default == ""


def test_the_resolver_reads_the_one_setting(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "easyweek_multi_service_canary_job_id", "4242", raising=False)
    assert multi_service_canary().job_id == 4242
    monkeypatch.setattr(settings, "easyweek_multi_service_canary_job_id", "oops", raising=False)
    assert multi_service_canary().unavailable_reason == MULTI_SERVICE_CANARY_INVALID


# ===========================================================================
# The mandatory configuration gate
# ===========================================================================


def test_the_pre_open_configuration_is_the_one_the_runbook_deploys(pre_open: None) -> None:
    assert multi_service_configuration_error(RolloutPhase.PRE_OPEN) is None


@pytest.mark.parametrize(
    ("flag", "expected"),
    [
        ("easyweek_notifications_enabled", EASYWEEK_NOTIFICATIONS_DISABLED),
        ("easyweek_reminders_enabled", EASYWEEK_REMINDERS_DISABLED),
        ("easyweek_multi_service_notifications_enabled", MULTI_SERVICE_DISABLED),
        ("easyweek_resource_shadow_proof_enabled", MULTI_SERVICE_RESOURCE_SHADOW_DISABLED),
        ("easyweek_reminder_api_guard_enabled", EASYWEEK_REMINDER_API_GUARD_DISABLED),
    ],
)
def test_each_mandatory_flag_off_on_its_own_names_its_own_reason(
    pre_open: None,
    monkeypatch: pytest.MonkeyPatch,
    flag: str,
    expected: str,
) -> None:
    monkeypatch.setattr(settings, flag, False, raising=False)
    for phase in RolloutPhase:
        assert multi_service_configuration_error(phase) == expected


def test_the_reminder_api_guard_blocks_every_phase(pre_open: None, monkeypatch: pytest.MonkeyPatch) -> None:
    """With the guard off the outbox never claims a reminder at all."""
    monkeypatch.setattr(settings, "easyweek_reminder_api_guard_enabled", False, raising=False)
    monkeypatch.setattr(settings, "easyweek_multi_service_send_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_multi_service_canary_job_id", "11", raising=False)
    assert multi_service_configuration_error(RolloutPhase.CANARY) == EASYWEEK_REMINDER_API_GUARD_DISABLED


def test_a_malformed_canary_fails_every_phase_closed(pre_open: None, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "easyweek_multi_service_canary_job_id", "12, 13", raising=False)
    for phase in RolloutPhase:
        assert multi_service_configuration_error(phase) == MULTI_SERVICE_CANARY_INVALID


def test_pre_open_refuses_an_already_open_fence(pre_open: None, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "easyweek_multi_service_send_enabled", True, raising=False)
    assert multi_service_configuration_error(RolloutPhase.PRE_OPEN) == MULTI_SERVICE_SEND_FENCE_OPEN


def test_pre_open_refuses_a_canary_that_is_already_named(pre_open: None, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "easyweek_multi_service_canary_job_id", "99", raising=False)
    assert multi_service_configuration_error(RolloutPhase.PRE_OPEN) == MULTI_SERVICE_CANARY_CONFIGURED


def test_the_canary_phase_needs_both_an_open_fence_and_a_named_job(
    pre_open: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert multi_service_configuration_error(RolloutPhase.CANARY) == MULTI_SERVICE_SEND_DISABLED
    monkeypatch.setattr(settings, "easyweek_multi_service_send_enabled", True, raising=False)
    assert multi_service_configuration_error(RolloutPhase.CANARY) == MULTI_SERVICE_CANARY_NOT_CONFIGURED
    monkeypatch.setattr(settings, "easyweek_multi_service_canary_job_id", "99", raising=False)
    assert multi_service_configuration_error(RolloutPhase.CANARY) is None


def test_the_bulk_phase_needs_an_open_fence_and_no_restriction(
    pre_open: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert multi_service_configuration_error(RolloutPhase.BULK) == MULTI_SERVICE_SEND_DISABLED
    monkeypatch.setattr(settings, "easyweek_multi_service_send_enabled", True, raising=False)
    assert multi_service_configuration_error(RolloutPhase.BULK) is None
    monkeypatch.setattr(settings, "easyweek_multi_service_canary_job_id", "99", raising=False)
    assert multi_service_configuration_error(RolloutPhase.BULK) == MULTI_SERVICE_CANARY_CONFIGURED


def test_reason_codes_carry_no_customer_data_and_no_job_id(pre_open: None, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "easyweek_multi_service_canary_job_id", "14211", raising=False)
    reason = multi_service_configuration_error(RolloutPhase.PRE_OPEN)
    assert reason is not None
    assert "14211" not in reason
    assert reason.replace("_", "").isalnum()
