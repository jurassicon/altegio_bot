"""§38.9: the rollout controls that decide WHEN a proven pair job may be sent.

The proof lives in :mod:`altegio_bot.easyweek_multi_service` and is not touched
here.  What lives here is the narrow rollout question the proof never answers:
given a queue of already-proven pair jobs, which of them is the operator
allowed to release *right now*.

Two things answer it.

``EASYWEEK_MULTI_SERVICE_CANARY_JOB_ID``
    One internal ``message_jobs.id``, and while it is set, the ONLY EasyWeek
    pair job the worker may claim or send.  ``EASYWEEK_MULTI_SERVICE_SEND_ENABLED``
    is a global switch: opening it releases every due pair job carrying the
    canonical digest — lifecycle and reminder, of active, past and deleted
    records alike — which is strictly more than either preflight audits.  The
    canary makes the first opening a single, named, reversible message.

The mandatory flag gate
    A preflight is a statement about a configuration, not only about data.
    "Every held pair job would be delivered correctly" is true in exactly one
    configuration; in any other one the report describes a world the operator
    is not in.  The gate is evaluated from the SAME ``settings`` object the
    workers read, so a report and a worker cannot disagree.

Imports nothing from the workers, so the outbox worker, both read-only
preflights and the release audit share one definition without a cycle.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Final

from altegio_bot.easyweek_multi_service import (
    MULTI_SERVICE_DISABLED,
    MULTI_SERVICE_RESOURCE_SHADOW_DISABLED,
    MULTI_SERVICE_SEND_DISABLED,
)
from altegio_bot.settings import settings

# --- stable reason codes ----------------------------------------------------
#
# They reach ``job.last_error``, the worker log and both preflight reports, so
# none of them may ever carry a booking uuid, a name, a price or a payload
# value.  An internal ``message_jobs.id`` is not customer data and is reported
# separately as a number, never interpolated into a reason.

# The canary is set and names a DIFFERENT job.  A hold, not a refusal: the row
# keeps `queued`, its `run_at` and its zero attempts.
MULTI_SERVICE_CANARY_RESTRICTED: Final = "multi_service_canary_restricted"
# The configured value is not one positive decimal integer.  Fail-closed: the
# WHOLE pair queue is held rather than silently degrading to "no restriction",
# which is the one interpretation that could turn a typo into a bulk send.
MULTI_SERVICE_CANARY_INVALID: Final = "multi_service_canary_job_id_invalid"

# Mandatory prerequisites, checked as effective configuration.
EASYWEEK_NOTIFICATIONS_DISABLED: Final = "easyweek_notifications_disabled"
EASYWEEK_REMINDERS_DISABLED: Final = "easyweek_reminders_disabled"
EASYWEEK_REMINDER_API_GUARD_DISABLED: Final = "easyweek_reminder_api_guard_disabled"

# Phase mismatches: the right flags, in the wrong rollout state.
MULTI_SERVICE_SEND_FENCE_OPEN: Final = "multi_service_send_fence_open"
MULTI_SERVICE_CANARY_CONFIGURED: Final = "multi_service_canary_configured"
MULTI_SERVICE_CANARY_NOT_CONFIGURED: Final = "multi_service_canary_not_configured"
# The canary is set, and names a job that is not the one being audited.
MULTI_SERVICE_CANARY_JOB_MISMATCH: Final = "multi_service_canary_job_mismatch"
# The canary names a job that is not in the audited release set at all.
MULTI_SERVICE_CANARY_JOB_NOT_FOUND: Final = "multi_service_canary_job_not_found"


@dataclass(frozen=True)
class MultiServiceCanary:
    """Total parse result; a malformed value never degrades to "no restriction"."""

    configured: bool
    valid: bool
    job_id: int | None = None

    @property
    def restricted(self) -> bool:
        """One named job may move, and nothing else in the pair queue may."""
        return self.configured and self.valid and self.job_id is not None

    @property
    def unavailable_reason(self) -> str | None:
        """The whole pair queue is held, because the restriction is unreadable."""
        if self.configured and not self.valid:
            return MULTI_SERVICE_CANARY_INVALID
        return None


def parse_multi_service_canary_job_id(raw: object) -> MultiServiceCanary:
    """Parse the canary setting into a total, fail-closed result.

    Deliberately the same shape and the same strictness as the proven PR-12
    retention canary: empty means no restriction, an exact positive decimal
    integer means one job, and EVERYTHING else — ``bool``, a float, a sign, a
    comma-separated list, a non-ASCII digit, whitespace inside, zero, a
    negative number — is invalid rather than ignored.

    Invalid is not "unset". The caller turns it into a hold of the whole pair
    queue, so the worst outcome of a typo is that nothing is sent.
    """
    if raw is None:
        return MultiServiceCanary(configured=False, valid=True)
    # `bool` is an `int` subclass, and `True` would otherwise parse as job 1.
    if isinstance(raw, bool):
        return MultiServiceCanary(configured=True, valid=False)
    if isinstance(raw, int):
        return MultiServiceCanary(configured=True, valid=raw > 0, job_id=raw if raw > 0 else None)
    if not isinstance(raw, str):
        return MultiServiceCanary(configured=True, valid=False)
    text = raw.strip()
    if not text:
        return MultiServiceCanary(configured=False, valid=True)
    # ASCII digits only, and no sign: `str.isdigit()` accepts superscripts and
    # `int()` accepts other scripts' decimal digits, neither of which an
    # operator ever means to type into a rollout variable.
    if not text.isascii() or not text.isdecimal():
        return MultiServiceCanary(configured=True, valid=False)
    try:
        value = int(text)
    except ValueError:  # pragma: no cover - guarded by isdecimal above
        return MultiServiceCanary(configured=True, valid=False)
    if value <= 0:
        return MultiServiceCanary(configured=True, valid=False)
    return MultiServiceCanary(configured=True, valid=True, job_id=value)


def multi_service_canary() -> MultiServiceCanary:
    """The one place the canary environment variable is read.

    Claim predicate, race guard, both preflights and the release audit call
    this. Parsing the variable a second time somewhere else is how a claim and
    a send come to disagree about which job the operator chose.
    """
    return parse_multi_service_canary_job_id(getattr(settings, "easyweek_multi_service_canary_job_id", ""))


class RolloutPhase(Enum):
    """The three configurations §38.9 recognises, and nothing in between."""

    # Deployed, audited, nothing released: send fence shut, no canary.
    PRE_OPEN = "pre_open"
    # Exactly one named job may reach the provider.
    CANARY = "canary"
    # The restriction is gone and the whole proven release set may move.
    BULK = "bulk"


def _prerequisite_error() -> str | None:
    """The flags every §38.9 phase requires, whatever the fence is doing.

    Read through ``getattr`` with a false default for the same reason the rest
    of the EasyWeek code does: a hand-built ``Settings`` stand-in in a test, or
    an older row of configuration, must fail closed rather than raise.
    """
    if not bool(getattr(settings, "easyweek_notifications_enabled", False)):
        return EASYWEEK_NOTIFICATIONS_DISABLED
    if not bool(getattr(settings, "easyweek_reminders_enabled", False)):
        return EASYWEEK_REMINDERS_DISABLED
    if not bool(getattr(settings, "easyweek_multi_service_notifications_enabled", False)):
        return MULTI_SERVICE_DISABLED
    if not bool(getattr(settings, "easyweek_resource_shadow_proof_enabled", False)):
        return MULTI_SERVICE_RESOURCE_SHADOW_DISABLED
    if not bool(getattr(settings, "easyweek_reminder_api_guard_enabled", False)):
        # The reminder API guard is not a nicety: with it off the outbox does
        # not claim reminders at all, so a "the pair queue is ready" report
        # would be a statement about a queue half of which cannot move.
        return EASYWEEK_REMINDER_API_GUARD_DISABLED
    return None


def multi_service_configuration_error(phase: RolloutPhase) -> str | None:
    """The reason this configuration is not the one *phase* describes, or ``None``.

    Prerequisites first, because a missing prerequisite explains the fence
    state rather than the other way round; then the phase's own two facts —
    where the global send fence is, and whether a canary restriction is in
    force.
    """
    prerequisite = _prerequisite_error()
    if prerequisite is not None:
        return prerequisite

    canary = multi_service_canary()
    if canary.unavailable_reason is not None:
        return canary.unavailable_reason

    send_open = bool(getattr(settings, "easyweek_multi_service_send_enabled", False))
    if phase is RolloutPhase.PRE_OPEN:
        if send_open:
            # Auditing a backlog while the fence is already open describes a
            # world that no longer exists: those jobs are being released now.
            return MULTI_SERVICE_SEND_FENCE_OPEN
        if canary.configured:
            return MULTI_SERVICE_CANARY_CONFIGURED
        return None

    if not send_open:
        return MULTI_SERVICE_SEND_DISABLED
    if phase is RolloutPhase.CANARY:
        return None if canary.restricted else MULTI_SERVICE_CANARY_NOT_CONFIGURED
    return MULTI_SERVICE_CANARY_CONFIGURED if canary.configured else None


__all__ = [
    "EASYWEEK_NOTIFICATIONS_DISABLED",
    "EASYWEEK_REMINDERS_DISABLED",
    "EASYWEEK_REMINDER_API_GUARD_DISABLED",
    "MULTI_SERVICE_CANARY_CONFIGURED",
    "MULTI_SERVICE_CANARY_INVALID",
    "MULTI_SERVICE_CANARY_JOB_MISMATCH",
    "MULTI_SERVICE_CANARY_JOB_NOT_FOUND",
    "MULTI_SERVICE_CANARY_NOT_CONFIGURED",
    "MULTI_SERVICE_CANARY_RESTRICTED",
    "MULTI_SERVICE_SEND_FENCE_OPEN",
    "MultiServiceCanary",
    "RolloutPhase",
    "multi_service_canary",
    "multi_service_configuration_error",
    "parse_multi_service_canary_job_id",
]
