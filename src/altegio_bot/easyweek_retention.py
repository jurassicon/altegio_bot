"""PR-12: when an EasyWeek booking earns a retention message, and how it is identified.

Two messages live here, and nothing else does:

``repeat_10d``
    Ten days after an appointment that FINISHED. The only evidence that an
    appointment finished is a proven ``booking-succeeded`` delivery — never
    ``booking-created``, never ``booking-updated``, and never the local
    observation that a start time is now in the past. EasyWeek has no
    ``attendance`` field, and "the clock passed the hour" is not attendance: a
    no-show, a walk-out and a completed visit all look identical from here.

``comeback_3d``
    Three days after a booking was CANCELLED. The evidence is the cancellation
    delivery itself, and the moment it carries is frozen into the job — a
    comeback whose "three days" silently re-anchored on a later delivery would
    drift away from the event the customer actually experienced.

**The counter is the whole point of the split from PR-11.** Both messages ask
"has this customer come back yet?", and the answer is
``Client.easyweek_visits_total`` — the snapshot EasyWeek itself states. Each job
freezes the value proven at the moment the obligation arose, and the send path
compares the current value against that baseline. Higher means the customer
already returned and the message must not go out; equal means the question is
still open; anything else — absent, lower, from another tenant — is a fail-closed
refusal, never a reason to guess.

**Never Altegio.** Altegio's own ``repeat_10d`` / ``comeback_3d`` prove the same
things by asking the Altegio API with an ``altegio_client_id`` and by rendering
a ``BOOKING_LINKS`` entry keyed by an Altegio company id. EasyWeek has neither,
and its ``company_id`` is the numeric EasyWeek ``:location_id``, which shares an
integer space with Altegio's — so a provider-blind lookup answers for a
different salon rather than failing. Every rule here is therefore stated on its
own instead of borrowed.

Identity is the business fact — provider, booking, job type — hashed into a
bounded key. A Resend, a second delivery with a different payload hash, a
concurrent worker and a restart all describe the SAME earned message.

Imports nothing from the workers, so the inbox planner, the outbox guard and the
read-only preflight can share one definition without a cycle.
"""

from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Final

from altegio_bot.easyweek_policy import COMEBACK_3D, REPEAT_10D

# How long after the proven visit we ask again, and how long after a
# cancellation we invite the customer back. Both are the product decisions the
# Altegio path already uses; PR-12 keeps them rather than inventing a second
# pair of numbers for the same two messages.
REPEAT_DELAY: Final = timedelta(days=10)
COMEBACK_DELAY: Final = timedelta(days=3)

# How long a comeback stays suppressed by an earlier comeback that was actually
# delivered. Deliberately the same 30 days the Altegio path already enforces:
# this is one product rule about how often a person may be invited back, not a
# per-CRM setting.
COMEBACK_REPEAT_WINDOW: Final = timedelta(days=30)

# Stamped into every retention payload. It is not decoration: the send-time
# guard refuses a payload whose proof it does not recognise, so a job planned by
# an older, weaker contract can never be released by a newer, stricter one that
# assumes fields it does not carry.
#
# Version 2 adds the frozen service identity to `repeat_10d`. A version-1 job
# proved which booking it belonged to but not which SERVICE, so the guard could
# not tell an unchanged appointment from one whose service was swapped after the
# visit. Those jobs are refused rather than re-interpreted: assuming the missing
# field is what makes a stricter contract weaker than the one it replaced.
RETENTION_PROOF_VERSION: Final = 2

_KEY_PREFIX: Final = "easyweek_retention"

# --- payload keys -----------------------------------------------------------
#
# Named constants rather than inline strings: the planner writes them, the send
# guard reads them and the preflight reads them again, and a typo in one of the
# three would read as "the field is missing" — which is a fail-closed refusal
# nobody could explain.
PAYLOAD_JOB_TYPE: Final = "job_type"
PAYLOAD_BOOKING_UUID: Final = "booking_uuid"
PAYLOAD_COMPANY_ID: Final = "company_id"
PAYLOAD_RECORD_STARTS_AT: Final = "record_starts_at"
PAYLOAD_SOURCE_CANCELLED_AT: Final = "source_cancelled_at"
PAYLOAD_VISITS_BASELINE: Final = "visits_baseline"
PAYLOAD_PROOF_VERSION: Final = "proof_version"
# The external service id of the ONE service the source booking had when the
# visit was proven. A technical identifier and nothing else: no title, no price,
# no description — the customer-facing text is read from the current row at send
# time, and only after this identity has matched.
PAYLOAD_SOURCE_SERVICE_ID: Final = "source_service_id"

# --- refusal reasons --------------------------------------------------------
#
# Stable, PII-free codes. They reach `job.last_error`, the worker log and the
# preflight report, so none of them may ever carry a name, a phone, a link or a
# payload value. Kept as one vocabulary because the runbook documents an
# operator action for each.
RETENTION_JOB_INCOMPLETE: Final = "retention_job_incomplete"
RETENTION_PROOF_VERSION_UNKNOWN: Final = "retention_proof_version_unknown"
RETENTION_BOOKING_UUID_UNPROVEN: Final = "retention_booking_uuid_unproven"
RETENTION_BOOKING_UUID_MISMATCH: Final = "retention_booking_uuid_mismatch"
RETENTION_BASELINE_UNPROVEN: Final = "retention_baseline_unproven"
RETENTION_COUNTER_MISSING: Final = "retention_counter_missing"
RETENTION_COUNTER_REGRESSED: Final = "retention_counter_regressed"
RETENTION_COUNTER_UNSTAMPED: Final = "retention_counter_unstamped"
RETENTION_CLIENT_RETURNED: Final = "retention_client_returned"
RETENTION_FUTURE_BOOKING: Final = "retention_future_booking"
RETENTION_SERVICE_UNPROVEN: Final = "retention_service_unproven"
RETENTION_SERVICE_CHANGED: Final = "retention_service_changed"
RETENTION_SOURCE_NOT_FINISHED: Final = "retention_source_not_finished"
RETENTION_SOURCE_NOT_CANCELED: Final = "retention_source_canceled_state_lost"
RETENTION_SOURCE_START_MISMATCH: Final = "retention_source_start_mismatch"
RETENTION_CANCELLED_AT_UNPROVEN: Final = "retention_cancelled_at_unproven"
RETENTION_COMEBACK_ALREADY_SENT: Final = "retention_comeback_already_sent"
RETENTION_CLIENT_UNSUBSCRIBED: Final = "retention_client_unsubscribed"
RETENTION_BOOKING_PAGE_UNPROVEN: Final = "retention_booking_page_unproven"

# --- hold reasons -----------------------------------------------------------
#
# Distinct from the refusals above, and the distinction is the point: a REFUSAL
# cancels the job because it can never legitimately be sent, while a HOLD leaves
# it exactly as planned — `queued`, original `run_at`, zero attempts — because
# the configuration currently forbids sending it. Folding the two together would
# either cancel a queue an operator meant to pause, or send one they meant to
# hold.
RETENTION_NOTIFICATIONS_DISABLED: Final = "retention_notifications_disabled"
RETENTION_SEND_FENCE_CLOSED: Final = "retention_send_fence_closed"
RETENTION_CANARY_RESTRICTED: Final = "retention_canary_restricted"
# The canary value itself is unreadable. Defined here with the other hold codes
# rather than beside the parser below, so the vocabulary set can name it instead
# of repeating the string.
CANARY_INVALID: Final = "retention_canary_job_id_invalid"

# The complete send-time vocabulary, so the runbook table and the code cannot
# drift apart. Asserted by the rollout contract test.
RETENTION_SEND_REFUSAL_REASONS: Final = frozenset(
    {
        RETENTION_JOB_INCOMPLETE,
        RETENTION_PROOF_VERSION_UNKNOWN,
        RETENTION_BOOKING_UUID_UNPROVEN,
        RETENTION_BOOKING_UUID_MISMATCH,
        RETENTION_BASELINE_UNPROVEN,
        RETENTION_COUNTER_MISSING,
        RETENTION_COUNTER_REGRESSED,
        RETENTION_COUNTER_UNSTAMPED,
        RETENTION_CLIENT_RETURNED,
        RETENTION_FUTURE_BOOKING,
        RETENTION_SERVICE_UNPROVEN,
        RETENTION_SERVICE_CHANGED,
        RETENTION_SOURCE_NOT_FINISHED,
        RETENTION_SOURCE_NOT_CANCELED,
        RETENTION_SOURCE_START_MISMATCH,
        RETENTION_CANCELLED_AT_UNPROVEN,
        RETENTION_COMEBACK_ALREADY_SENT,
        RETENTION_CLIENT_UNSUBSCRIBED,
        RETENTION_BOOKING_PAGE_UNPROVEN,
    }
)

# Every reason a retention job is HELD rather than cancelled.
RETENTION_HOLD_REASONS: Final = frozenset(
    {
        RETENTION_NOTIFICATIONS_DISABLED,
        RETENTION_SEND_FENCE_CLOSED,
        RETENTION_CANARY_RESTRICTED,
        CANARY_INVALID,
    }
)


def _as_utc(value: datetime) -> datetime:
    """Naive input is a bug upstream, but it must not become a silent offset."""
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def retention_delay(job_type: str) -> timedelta | None:
    """How long after its source moment this job type fires, or ``None``."""
    if job_type == REPEAT_10D:
        return REPEAT_DELAY
    if job_type == COMEBACK_3D:
        return COMEBACK_DELAY
    return None


def parse_visits_total(value: object) -> int | None:
    """A usable visit count, or ``None``. Never raises, never coerces.

    The same strictness the PR-11 normalizer applies to the payload, applied
    here to a stored value: ``True`` is not 1, ``"3"`` is not 3, and ``3.0``
    would mean the column's type changed under us. A baseline this function
    cannot read is a fail-closed refusal, because the alternative — treating an
    unreadable count as zero — would call a returning customer a new one and
    send the very message this compares against.
    """
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value if value >= 1 else None


def parse_service_id(value: object) -> int | None:
    """A usable external service id, or ``None``. Never raises, never coerces.

    The same strictness the counter gets, and for the same reason: ``True`` is
    not 1 and ``"11"`` is not 11. An identity this function cannot read is a
    fail-closed refusal — the alternative is comparing a frozen identity against
    something that merely looks like it.
    """
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value if value > 0 else None


def service_identity_reason(*, frozen: object, current: object) -> str | None:
    """Does the booking still have the SERVICE its repeat was earned for?

    THE single definition, shared by the send guard and the read-only preflight,
    so the two cannot disagree about a queue.

    ``repeat_10d`` names a service in the message a customer reads. The booking
    proved exactly one at the moment the visit finished, and that id is frozen
    into the job. Ten days is long enough for a ``booking-updated`` to swap it —
    and a message inviting someone back for a treatment they did not have is
    exactly the failure the frozen identity exists to prevent.

    Only the ID is compared. The title, the price and the description are
    display fields: the salon may legitimately rewrite any of them, and refusing
    on a re-worded title would cancel perfectly good messages. The title the
    customer finally reads is taken from the current row, and only after this
    function has returned ``None``.
    """
    proven = parse_service_id(frozen)
    if proven is None:
        # A payload with no readable identity cannot be checked at all. That is
        # the version-1 shape, and it is refused rather than waved through.
        return RETENTION_SERVICE_UNPROVEN
    live = parse_service_id(current)
    if live is None:
        return RETENTION_SERVICE_UNPROVEN
    if live != proven:
        return RETENTION_SERVICE_CHANGED
    return None


# ---------------------------------------------------------------------------
# The canary restriction
# ---------------------------------------------------------------------------
#
# A controlled canary is a promise that exactly ONE message goes out. Until now
# that promise was procedural — "pick one job and hope the queue holds only
# it" — and a queue that quietly grew a second due job would break it silently,
# at the worst possible moment in the rollout.
#
# `EASYWEEK_RETENTION_CANARY_JOB_ID` makes the promise mechanical: the claim and
# the pre-send check both restrict EasyWeek retention to that one internal
# MessageJob id. Every other retention job stays exactly as planned — `queued`,
# original `run_at`, zero attempts — and Altegio and every other EasyWeek job
# type are not touched at all.


@dataclass(frozen=True)
class RetentionCanary:
    """Total parse result; a malformed value never degrades to "no restriction"."""

    configured: bool
    valid: bool
    job_id: int | None = None

    @property
    def restricted(self) -> bool:
        """True when the worker may send exactly one named job and nothing else."""
        return self.configured and self.valid and self.job_id is not None

    @property
    def unavailable_reason(self) -> str | None:
        if self.configured and not self.valid:
            return CANARY_INVALID
        return None


def parse_retention_canary_job_id(raw: object) -> RetentionCanary:
    """Parse ``EASYWEEK_RETENTION_CANARY_JOB_ID``. Never raises.

    Three outcomes, and the third is the one worth stating:

    * absent or empty — no restriction, ordinary bulk behaviour;
    * a positive integer — that one job, and no other retention job;
    * anything else — CONFIGURED BUT INVALID, which fails CLOSED: no EasyWeek
      retention job is claimed or sent at all.

    Failing closed on a typo is the whole point. An operator setting this
    variable has decided that at most one message may go out; reading a
    malformed value as "no restriction" would turn that decision into a bulk
    send of the entire queue, which is the exact accident the flag exists to
    make impossible.

    Parsed at call time rather than at import, so an operator can correct a typo
    and recreate the service without a code change.
    """
    if raw is None:
        return RetentionCanary(configured=False, valid=True)
    if isinstance(raw, bool):
        return RetentionCanary(configured=True, valid=False)
    if isinstance(raw, int):
        return RetentionCanary(configured=True, valid=raw > 0, job_id=raw if raw > 0 else None)
    if not isinstance(raw, str):
        return RetentionCanary(configured=True, valid=False)

    text = raw.strip()
    if not text:
        return RetentionCanary(configured=False, valid=True)
    # ASCII digits only, and no sign: `str.isdigit()` accepts Arabic-Indic and
    # other Unicode digits, and a leading `+` would make a second spelling of the
    # same id. A job id is written one way.
    if not text.isascii() or not text.isdecimal():
        return RetentionCanary(configured=True, valid=False)
    try:
        value = int(text)
    except ValueError:  # pragma: no cover - isdecimal already proves this parses
        return RetentionCanary(configured=True, valid=False)
    if value <= 0:
        return RetentionCanary(configured=True, valid=False)
    return RetentionCanary(configured=True, valid=True, job_id=value)


@dataclass(frozen=True)
class PlannedRetention:
    """One earned retention message: when it fires, and how it is identified."""

    job_type: str
    run_at: datetime
    dedupe_key: str
    visits_baseline: int
    # Only `repeat_10d` carries one: it is the single service the source booking
    # had when the visit was proven. `comeback_3d` deliberately has none — its
    # template names no service, so freezing one would be a field nobody reads
    # and one more way for a good message to be refused.
    service_id: int | None = None


def easyweek_retention_dedupe_key(*, booking_uuid: uuid.UUID, job_type: str) -> str:
    """Stable identity for "this booking, this retention message, exactly once".

    Keyed on the booking and the job type — and deliberately NOT on the start
    instant, the cancellation instant, the payload hash or the delivery. Every
    one of those varies across deliveries that describe the SAME earned message:

    * a Resend repeats the body with a new row;
    * a second ``booking-succeeded`` may carry a different hash;
    * two cancellation deliveries arrive at two different moments;
    * a reschedule after the visit finished would move the start instant.

    Keying on any of them would let the same business fact earn a second message
    to a real person. The booking is the fact, so the booking is the key. The
    price of that choice is stated plainly: a booking cannot earn a second
    repeat, ever — which is the correct answer, because a booking happens once.
    """
    material = "|".join((_KEY_PREFIX, str(booking_uuid), job_type))
    digest = hashlib.sha256(material.encode("utf-8")).hexdigest()[:40]
    return f"{_KEY_PREFIX}:{job_type}:{digest}"


def repeat_run_at(starts_at: datetime) -> datetime:
    """When the repeat invitation is due, in UTC."""
    return _as_utc(starts_at) + REPEAT_DELAY


def comeback_run_at(cancelled_at: datetime) -> datetime:
    """When the comeback invitation is due, in UTC."""
    return _as_utc(cancelled_at) + COMEBACK_DELAY


def retention_moment_passed(run_at: datetime, now: datetime) -> bool:
    """Is this moment already gone — and therefore not worth planning at all?

    THE single definition, shared by both planners and by the preflight, so
    "too late" cannot come to mean two different things.

    Past this instant no configuration change produces a job. A repeat for a
    visit ten days gone and a comeback for a cancellation three days gone are
    not late reminders; they are unsolicited marketing about something the
    customer has stopped thinking about. Planning one would also be a backfill
    in disguise — the flag flips on, and a backlog of historic deliveries
    suddenly owes messages to people who never expected them.
    """
    return _as_utc(run_at) <= _as_utc(now)


def plan_repeat(
    *,
    booking_uuid: uuid.UUID,
    starts_at: datetime | None,
    now: datetime,
    visits_baseline: int,
    service_id: object,
    is_deleted: bool = False,
) -> PlannedRetention | None:
    """The repeat this FINISHED booking owes, or ``None``.

    Total and side-effect free, so the planner, the tests and anyone reasoning
    about production get the same answer. Everything that needs a database — the
    identity proof, the counter, the category, the opt-out — is the caller's job;
    what is decided here is the moment, the identity and the baseline.

    ``visits_baseline`` is the counter value already PROVEN for this client after
    this visit. It is required rather than optional: a repeat with no baseline
    could never answer "has the customer come back?", and a repeat that silently
    treated a missing baseline as zero would send to everyone.

    ``service_id`` is the external id of the ONE service the booking had when the
    visit finished. Equally required: the message names a service, and a repeat
    that could not say WHICH one has no way to notice the booking's service being
    swapped underneath it in the ten days before it fires.
    """
    if is_deleted or starts_at is None:
        return None
    baseline = parse_visits_total(visits_baseline)
    if baseline is None:
        return None
    proven_service = parse_service_id(service_id)
    if proven_service is None:
        return None

    run_at = repeat_run_at(starts_at)
    if retention_moment_passed(run_at, now):
        return None

    return PlannedRetention(
        job_type=REPEAT_10D,
        run_at=run_at,
        dedupe_key=easyweek_retention_dedupe_key(booking_uuid=booking_uuid, job_type=REPEAT_10D),
        visits_baseline=baseline,
        service_id=proven_service,
    )


def plan_comeback(
    *,
    booking_uuid: uuid.UUID,
    cancelled_at: datetime | None,
    now: datetime,
    visits_baseline: int,
) -> PlannedRetention | None:
    """The comeback this CANCELLED booking owes, or ``None``.

    ``cancelled_at`` is the moment the cancellation was proven, and it is frozen
    into the job: it is what the three days are counted from, and what the send
    path compares the payload against. A comeback that re-derived it from
    "whenever we happen to look" would move every time a later delivery touched
    the row.
    """
    if cancelled_at is None:
        return None
    baseline = parse_visits_total(visits_baseline)
    if baseline is None:
        return None

    run_at = comeback_run_at(cancelled_at)
    if retention_moment_passed(run_at, now):
        return None

    return PlannedRetention(
        job_type=COMEBACK_3D,
        run_at=run_at,
        dedupe_key=easyweek_retention_dedupe_key(booking_uuid=booking_uuid, job_type=COMEBACK_3D),
        visits_baseline=baseline,
    )


def repeat_job_payload(
    *,
    booking_uuid: uuid.UUID,
    company_id: int,
    starts_at: datetime,
    visits_baseline: int,
    service_id: int,
    source_event_id: int | None = None,
    source_payload_hash: str | None = None,
) -> dict[str, Any]:
    """The minimal technical payload a repeat job carries.

    No name, no phone, no e-mail, no service text, no price, no webhook body:
    everything customer-facing is re-read and re-proven at send time, and a
    payload is where data goes to rot.

    ``record_starts_at`` is the immutable instant this repeat was earned for. At
    send time it is compared against the current Record, so a booking that moved
    after it finished cannot deliver an invitation about a visit that did not
    happen when we thought it did. The two source markers are audit only — they
    are never used to decide anything.

    ``source_service_id`` is the frozen service IDENTITY, and deliberately not
    the service NAME. A numeric id is not customer data, survives a re-worded
    title, and is the only thing that can prove ten days later that the booking
    still holds the service this invitation is about.
    """
    payload: dict[str, Any] = {
        "provider": "easyweek",
        PAYLOAD_COMPANY_ID: company_id,
        PAYLOAD_BOOKING_UUID: str(booking_uuid),
        PAYLOAD_JOB_TYPE: REPEAT_10D,
        PAYLOAD_RECORD_STARTS_AT: _as_utc(starts_at).isoformat(),
        PAYLOAD_VISITS_BASELINE: visits_baseline,
        PAYLOAD_SOURCE_SERVICE_ID: service_id,
        PAYLOAD_PROOF_VERSION: RETENTION_PROOF_VERSION,
    }
    if source_event_id is not None:
        payload["source_event_id"] = source_event_id
    if source_payload_hash is not None:
        payload["source_payload_hash"] = source_payload_hash
    return payload


def comeback_job_payload(
    *,
    booking_uuid: uuid.UUID,
    company_id: int,
    cancelled_at: datetime,
    visits_baseline: int,
    source_event_id: int | None = None,
    source_payload_hash: str | None = None,
) -> dict[str, Any]:
    """The minimal technical payload a comeback job carries.

    ``source_cancelled_at`` is the cancellation instant this job was planned
    from — the anchor of its three days, and the value the send-time guard
    re-reads rather than recomputing. It is written under the same key the
    Altegio path already uses so one operator query answers for both CRMs; the
    two paths still never share a lookup, a template or a link.
    """
    payload: dict[str, Any] = {
        "provider": "easyweek",
        PAYLOAD_COMPANY_ID: company_id,
        PAYLOAD_BOOKING_UUID: str(booking_uuid),
        PAYLOAD_JOB_TYPE: COMEBACK_3D,
        PAYLOAD_SOURCE_CANCELLED_AT: _as_utc(cancelled_at).isoformat(),
        PAYLOAD_VISITS_BASELINE: visits_baseline,
        PAYLOAD_PROOF_VERSION: RETENTION_PROOF_VERSION,
    }
    if source_event_id is not None:
        payload["source_event_id"] = source_event_id
    if source_payload_hash is not None:
        payload["source_payload_hash"] = source_payload_hash
    return payload


def counter_refusal_reason(*, baseline: object, current: object) -> str | None:
    """Compare a job's frozen baseline with the client's current counter.

    THE single definition of "has the customer already come back?", shared by
    the outbox guard and the read-only preflight so the two can never disagree
    about a queue.

    Four outcomes, and only one of them sends:

    * ``current > baseline`` — the customer completed another visit since the
      obligation arose. Nothing to invite them back to; suppress.
    * ``current == baseline`` — still open. Continue with the other checks.
    * ``current`` absent, unreadable, or LOWER than the baseline — refuse. A
      counter that went backwards contradicts PR-11's monotonic snapshot, which
      means the row is not the one this job was planned against; guessing which
      of the two numbers to believe is exactly how a message reaches the wrong
      person.
    * an unreadable baseline — refuse, because there is nothing to compare.

    ``None`` means "keep going", never "looks fine".
    """
    proven_baseline = parse_visits_total(baseline)
    if proven_baseline is None:
        return RETENTION_BASELINE_UNPROVEN
    proven_current = parse_visits_total(current)
    if proven_current is None:
        return RETENTION_COUNTER_MISSING
    if proven_current > proven_baseline:
        return RETENTION_CLIENT_RETURNED
    if proven_current < proven_baseline:
        return RETENTION_COUNTER_REGRESSED
    return None
