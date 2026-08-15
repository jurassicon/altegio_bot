"""One definition of what a delivery-retry job is allowed to be.

A delivery retry is the only way a message reaches Meta without ever passing
through job planning: the status callback resurrects a send from an
``OutboxMessage``, and ``OutboxMessage`` has no ``provider`` column. So the
provider — and with it the template, the sender and the tenant — can only come
from the ``MessageJob`` that produced the anchor outbox row, and it has to be
proven rather than assumed.

Three places need that answer and must not disagree:

* the status callback, deciding whether to create a retry;
* the comparison against a row that already occupies the dedupe key;
* the outbox worker's presend guard, re-proving the chain before it sends.

Three separate implementations would drift, and a drift here is a cross-tenant
message. Hence one resolver returning one canonical identity.

Imports only ``models`` and ``easyweek_policy``, so both workers can import it
at module level without a cycle.
"""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.easyweek_policy import (
    EASYWEEK_CUSTOMER_JOB_TYPES,
    EASYWEEK_REMINDER_JOB_TYPES,
    EASYWEEK_REVIEW_JOB_TYPES,
    normalize_provider,
)
from altegio_bot.models.models import (
    PROVIDER_EASYWEEK,
    Client,
    MessageJob,
    OutboxMessage,
    Record,
)

DELIVERY_RETRY_DEDUPE_PREFIX = "delivery_retry:"
_DELIVERY_RETRY_DEDUPE_RE = re.compile(r"^delivery_retry:([1-9][0-9]*):([1-9][0-9]*)$")
POSTGRES_BIGINT_MAX = 9_223_372_036_854_775_807
DELIVERY_RETRY_MAX_ATTEMPTS = 4
DELIVERY_RETRY_JOB_TYPES = (
    "record_created",
    "record_updated",
    "record_canceled",
    "reminder_24h",
    "reminder_2h",
)

# Everything a retry job must carry over from the chain it belongs to. Compared
# as a set: a row that disagrees on ANY of them is not "the same retry", it is a
# different job wearing the same dedupe key.
RETRY_IDENTITY_FIELDS = ("provider", "company_id", "record_id", "client_id", "job_type")


@dataclass(frozen=True)
class ReminderRetryIdentity:
    """The two values a reminder retry must inherit from its ROOT job.

    A reminder is verified against the live EasyWeek API before every Meta
    attempt, and that guard needs a canonical booking uuid and the start instant
    the reminder was PLANNED for. A retry skips planning entirely, so both have
    to come across from the original job — and from nowhere else.

    In particular ``record_starts_at`` is deliberately NOT read from the current
    ``Record``. If the appointment moved between the first send and the failed
    callback, taking today's value would manufacture a retry that agrees with
    the new time and sends a reminder the customer was never owed. Inheriting
    the original instant makes the guard notice the move and refuse.

    Both fields are stored as ISO-8601 text, the same shape
    ``reminder_job_payload`` writes, so the guard reads one format everywhere.
    """

    booking_uuid: str
    record_starts_at: str


def easyweek_reminder_retry_identity(original_job: MessageJob | None) -> ReminderRetryIdentity | None:
    """Validate the reminder identity carried by a root job, or ``None``.

    ``None`` means the root cannot support a retry — a missing payload, a
    booking uuid that is not canonical, a start instant that is absent or has no
    timezone. Every one of those is a refusal rather than a value to patch up:
    the retry would otherwise be built on an identity nobody proved.
    """
    payload = getattr(original_job, "payload", None)
    if not isinstance(payload, dict):
        return None

    raw_uuid = payload.get("booking_uuid")
    if not isinstance(raw_uuid, str):
        return None
    try:
        booking_uuid = uuid.UUID(raw_uuid.strip())
    except (ValueError, AttributeError, TypeError):
        return None

    raw_start = payload.get("record_starts_at")
    if not isinstance(raw_start, str):
        return None
    text = raw_start.strip()
    if text.endswith(("Z", "z")):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        # A naive instant would compare as a different moment against the API.
        return None

    return ReminderRetryIdentity(
        booking_uuid=str(booking_uuid),
        record_starts_at=parsed.astimezone(timezone.utc).isoformat(),
    )


@dataclass(frozen=True)
class EasyWeekReviewRetryIdentity:
    """What an EasyWeek review retry must inherit from its ROOT job (PR-9).

    A separate object from :class:`ReminderRetryIdentity` on purpose. A reminder
    carries two values; a review carries three, and one of them is a customer
    facing URL. Widening the reminder object would hand reminders a field they
    have no use for and no rule about.

    All three come from the root job's payload and from nowhere else. Not from
    the current Record — if the appointment moved between the send and the
    callback, today's values would manufacture a retry that agrees with the new
    state and asks a customer to review a visit that did not happen as recorded.
    Not from the Outbox body or meta, which hold rendered text rather than proven
    identity. Not from the callback payload, which is Meta's, not ours.
    """

    booking_uuid: str
    record_starts_at: str
    review_url: str


def easyweek_review_retry_identity(original_job: MessageJob | None) -> EasyWeekReviewRetryIdentity | None:
    """Validate the review identity carried by a root job, or ``None``.

    ``None`` means no retry may be built: a missing payload, a booking uuid that
    is not canonical, a start instant without a timezone, or a review URL that is
    not a bounded string. The URL is re-proven against the Record at send time
    anyway; what this refuses is carrying something unusable forward at all.
    """
    payload = getattr(original_job, "payload", None)
    if not isinstance(payload, dict):
        return None

    raw_uuid = payload.get("booking_uuid")
    if not isinstance(raw_uuid, str):
        return None
    try:
        booking_uuid = uuid.UUID(raw_uuid.strip())
    except (ValueError, AttributeError, TypeError):
        return None

    raw_start = payload.get("record_starts_at")
    if not isinstance(raw_start, str):
        return None
    text = raw_start.strip()
    if text.endswith(("Z", "z")):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None

    review_url = payload.get("review_url")
    if not isinstance(review_url, str) or not review_url.strip():
        return None

    return EasyWeekReviewRetryIdentity(
        booking_uuid=str(booking_uuid),
        record_starts_at=parsed.astimezone(timezone.utc).isoformat(),
        review_url=review_url.strip(),
    )


@dataclass(frozen=True)
class RetryIdentity:
    """The proven identity of one delivery-retry chain."""

    provider: str
    company_id: int
    record_id: int | None
    client_id: int | None
    job_type: str

    def as_job_fields(self) -> dict[str, Any]:
        """Columns to stamp on a new retry job — never left to a column default."""
        return {
            "provider": self.provider,
            "company_id": self.company_id,
            "record_id": self.record_id,
            "client_id": self.client_id,
            "job_type": self.job_type,
        }

    def mismatch_field(self, job: MessageJob) -> str | None:
        """Name the first identity field *job* disagrees on, or ``None``.

        ``provider`` is normalized on both sides so a row whose column is an
        empty string does not read as "matches nothing" by accident.
        """
        if normalize_provider(getattr(job, "provider", None), default="") != self.provider:
            return "provider"
        for field in ("company_id", "record_id", "client_id", "job_type"):
            if getattr(job, field, None) != getattr(self, field):
                return field
        return None

    def outbox_mismatch_field(self, outbox: OutboxMessage) -> str | None:
        """Name the first field *outbox* disagrees on, or ``None``.

        ``OutboxMessage`` has no ``provider`` column, so belonging to a proven
        member job is what places a row in a chain; these fields then check that
        the row itself is consistent with the tenant the chain belongs to.

        ``client_id`` is compared against the EFFECTIVE client, which is what
        this identity already carries. PR-4 partial deliveries legitimately
        produce a job with ``client_id = NULL`` while the send resolves the
        client through the record, so the outbox row names a real client that
        the job does not — comparing against the job's column would reject an
        ordinary booking.

        ``template_code`` mirrors ``job_type``: the outbox column is named for
        the template, the job column for the lifecycle event, and they are the
        same value.
        """
        if outbox.company_id != self.company_id:
            return "company_id"
        if outbox.record_id != self.record_id:
            return "record_id"
        if outbox.client_id != self.client_id:
            return "client_id"
        if outbox.template_code != self.job_type:
            return "template_code"
        return None


@dataclass(frozen=True)
class RetryIdentityResolution:
    """Either a proven identity or a PII-free reason it could not be proven."""

    identity: RetryIdentity | None = None
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.identity is not None


@dataclass(frozen=True)
class RetryReference:
    """The chain pointer carried by one syntactically valid retry job."""

    original_outbox_id: int
    attempt_number: int


@dataclass(frozen=True)
class RetryReferenceResolution:
    """Either a proven retry payload/namespace link or a PII-free error."""

    reference: RetryReference | None = None
    error: str | None = None


@dataclass(frozen=True)
class StatusRetryChain:
    """Authoritative chain identity for one delivery-status callback.

    Original outbox rows form a one-row chain whose id is their own id. Retry
    rows are accepted only after their current ``MessageJob`` proves the
    reference and identity; ``OutboxMessage.meta`` is compared as audit data and
    is never used as the pointer.
    """

    original_outbox_id: int
    attempt_number: int | None
    anchor_outbox: OutboxMessage
    original_job: MessageJob | None
    identity: RetryIdentity | None
    is_retry: bool


@dataclass(frozen=True)
class StatusRetryChainResolution:
    chain: StatusRetryChain | None = None
    error: str | None = None


def _refuse(reason: str) -> RetryIdentityResolution:
    return RetryIdentityResolution(error=reason)


def is_delivery_retry_dedupe_key(dedupe_key: str | None) -> bool:
    """True for any job living in the reserved retry namespace."""
    return isinstance(dedupe_key, str) and dedupe_key.startswith(DELIVERY_RETRY_DEDUPE_PREFIX)


def claims_delivery_retry(job: object) -> bool:
    """Return whether a row claims the reserved retry security boundary."""
    payload = getattr(job, "payload", None)
    return is_delivery_retry_dedupe_key(getattr(job, "dedupe_key", None)) or (
        isinstance(payload, dict) and payload.get("kind") == "delivery_failed_retry"
    )


def parse_bounded_positive_int(value: object, *, maximum: int) -> int | None:
    """Parse a canonical positive integer without unsafe unbounded ``int``.

    String length and lexicographic bounds are checked before conversion, so a
    malicious value cannot reach Python's integer-conversion digit limit or a
    PostgreSQL ``int8`` bind with an unsupported value.
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if 0 < value <= maximum else None
    if not isinstance(value, str) or re.fullmatch(r"[1-9][0-9]*", value) is None:
        return None

    maximum_text = str(maximum)
    if len(value) > len(maximum_text):
        return None
    if len(value) == len(maximum_text) and value > maximum_text:
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if 0 < parsed <= maximum:
        return parsed
    return None


def parse_retry_outbox_id(value: object) -> int | None:
    return parse_bounded_positive_int(value, maximum=POSTGRES_BIGINT_MAX)


def parse_retry_attempt(value: object) -> int | None:
    return parse_bounded_positive_int(value, maximum=DELIVERY_RETRY_MAX_ATTEMPTS)


def _outbox_meta_claims_retry(outbox: OutboxMessage) -> bool:
    meta = outbox.meta
    if not isinstance(meta, dict):
        return False
    return (
        meta.get("delivery_retry") is True
        or meta.get("delivery_retry_of_outbox_id") is not None
        or meta.get("delivery_retry_attempt") is not None
    )


def retry_outbox_audit_mismatch(
    outbox: OutboxMessage,
    *,
    original_outbox_id: int,
    attempt_number: int,
) -> str | None:
    """Does this outbox row's own audit corroborate THIS root and THIS attempt?

    Returns ``None`` when it does, else the name of the broken invariant.

    Every retry send writes this audit unconditionally (``outbox_worker`` sets
    ``delivery_retry`` / ``delivery_retry_of_outbox_id`` /
    ``delivery_retry_attempt`` on ``send_meta`` before the row is built, for both
    the sent and the failed row), so a retry row without it is malformed, not
    legacy.

    One predicate, two callers, on purpose. The callback resolver used to demand
    this while the success scanner did not, so the very same row could be
    rejected as unproven when a status arrived for it and simultaneously
    accepted as proof that the chain had landed — enough to cancel a correct
    queued retry and lose a notification silently.

    Deliberately NOT implemented by running each row back through
    :func:`resolve_status_retry_chain`. That function answers a different, wider
    question — "which chain does this outbox belong to?" — by re-deriving the
    anchor, the anchor job and the whole identity from the row itself. A caller
    that has ALREADY proven the root identity and the member job would be paying
    for that per row and, worse, re-deriving facts it holds: if the re-derivation
    disagreed, there would once again be two answers. This narrow predicate is
    exactly the part both callers need and nothing else.
    """
    meta = outbox.meta if isinstance(outbox.meta, dict) else {}
    if meta.get("delivery_retry") is not True:
        return "retry_outbox_audit_marker_missing"
    meta_outbox_id = parse_retry_outbox_id(meta.get("delivery_retry_of_outbox_id"))
    if meta_outbox_id is None:
        return "retry_outbox_audit_reference_invalid"
    if meta_outbox_id != original_outbox_id:
        return "retry_outbox_audit_reference_mismatch"
    meta_attempt = parse_retry_attempt(meta.get("delivery_retry_attempt"))
    if meta_attempt is None:
        return "retry_outbox_audit_attempt_invalid"
    if meta_attempt != attempt_number:
        return "retry_outbox_audit_attempt_mismatch"
    return None


def delivery_retry_audit(job: MessageJob) -> dict[str, Any]:
    """The audit fields EVERY outbox row produced by a retry job must carry.

    Returns ``{}`` for a job that does not claim the retry boundary, so callers
    can splat it into any ``meta`` unconditionally — which is the point. The
    fields used to be written at a single spot near the end of ``_run_job_logic``
    that three early-returning paths never reached (the 24h text send, the text
    failure, the template preflight failure), and a retry that went out through
    one of them produced a row the reader then rejected as unproven.

    Writer and reader are one contract: the values are normalized through the
    very parsers :func:`retry_outbox_audit_mismatch` will use, so anything this
    function writes is by construction something that predicate can accept. A
    job whose payload does not parse still gets the marker with ``None`` ids —
    it is a retry row with an unusable pointer, and saying so is more honest
    than omitting the marker and having it read as an ordinary send.
    """
    if not claims_delivery_retry(job):
        return {}
    payload = getattr(job, "payload", None)
    payload = payload if isinstance(payload, dict) else {}
    return {
        "delivery_retry": True,
        "delivery_retry_of_outbox_id": parse_retry_outbox_id(payload.get("delivery_retry_of_outbox_id")),
        "delivery_retry_attempt": parse_retry_attempt(payload.get("delivery_retry_attempt")),
    }


def resolve_retry_reference(job: MessageJob) -> RetryReferenceResolution:
    """Prove that retry payload and reserved dedupe namespace describe one row.

    The namespace is a security boundary, not merely a naming convention. A
    namespaced row must carry the retry kind, a positive original outbox id and
    a positive attempt number, and both numbers must exactly match its key.
    Otherwise an old or hand-written row could point the presend guard at one
    chain while occupying another chain's globally unique idempotency key.

    Both halves are mandatory. A retry-kind payload outside the namespace and a
    namespaced row without the retry payload are malformed claims, not ordinary
    jobs.
    """
    payload = getattr(job, "payload", None)
    if not isinstance(payload, dict):
        return RetryReferenceResolution(error="delivery_retry_payload_invalid")

    original_outbox_id = parse_retry_outbox_id(payload.get("delivery_retry_of_outbox_id"))
    if original_outbox_id is None:
        return RetryReferenceResolution(error="invalid delivery_retry_of_outbox_id")
    if payload.get("kind") != "delivery_failed_retry":
        return RetryReferenceResolution(error="delivery_retry_kind_missing")

    dedupe_key = getattr(job, "dedupe_key", None)
    if not is_delivery_retry_dedupe_key(dedupe_key):
        return RetryReferenceResolution(error="delivery_retry_dedupe_namespace_missing")
    match = _DELIVERY_RETRY_DEDUPE_RE.fullmatch(str(dedupe_key))
    if match is None:
        return RetryReferenceResolution(error="delivery_retry_dedupe_key_invalid")

    key_outbox_id = parse_retry_outbox_id(match.group(1))
    if key_outbox_id is None:
        return RetryReferenceResolution(error="delivery_retry_dedupe_outbox_id_invalid")
    key_attempt = parse_retry_attempt(match.group(2))
    if key_attempt is None:
        return RetryReferenceResolution(error="delivery_retry_dedupe_attempt_invalid")
    payload_attempt = parse_retry_attempt(payload.get("delivery_retry_attempt"))
    if payload_attempt is None:
        return RetryReferenceResolution(error="delivery_retry_attempt_invalid")
    if key_outbox_id != original_outbox_id:
        return RetryReferenceResolution(error="delivery_retry_outbox_reference_mismatch")
    if key_attempt != payload_attempt:
        return RetryReferenceResolution(error="delivery_retry_attempt_mismatch")

    repeated_original = payload.get("delivery_retry_original_outbox_id")
    if repeated_original is not None and parse_retry_outbox_id(repeated_original) != original_outbox_id:
        return RetryReferenceResolution(error="delivery_retry_original_reference_mismatch")

    job_type = getattr(job, "job_type", None)
    if job_type not in DELIVERY_RETRY_JOB_TYPES and not (
        # PR-9: review is retry-enabled for EasyWeek only. Keeping it out of the
        # shared tuple is what leaves Altegio's own review without a retry and
        # without a delivery deadline, exactly as before.
        job_type in EASYWEEK_REVIEW_JOB_TYPES
        and normalize_provider(getattr(job, "provider", None), default="") == PROVIDER_EASYWEEK
    ):
        return RetryReferenceResolution(error="delivery_retry_job_type_not_enabled")

    return RetryReferenceResolution(
        reference=RetryReference(
            original_outbox_id=original_outbox_id,
            attempt_number=payload_attempt,
        )
    )


async def resolve_retry_identity(
    session: AsyncSession,
    *,
    anchor_outbox: OutboxMessage,
    original_job: MessageJob | None,
    job_type: str,
) -> RetryIdentityResolution:
    """Prove who a retry for *anchor_outbox* would belong to.

    Provider is read from the original job and from nowhere else. It is never
    inferred from ``company_id`` (EasyWeek's is the numeric EasyWeek
    ``:location_id`` and shares an integer space with Altegio company ids), nor
    from the Record, the Client, the sender or the Meta template name — every
    one of which can look Altegio-shaped for an EasyWeek booking.

    *anchor_outbox* must be the CANONICAL ROOT of the chain — the original send,
    not one of its retries. Accepting a retry row as an anchor would nest one
    chain inside another: the four-attempt budget would restart for every branch,
    and a later delivered/read would cancel by the nested root's dedupe prefix
    while the real root's queued retries sailed on. There is exactly one root per
    chain and its id is the one every attempt key is built from.

    Every refusal reason names an invariant and never a value: a mismatching row
    may belong to another tenant, and these strings reach ``job.last_error``,
    the outbox audit metadata and the log.
    """
    if anchor_outbox.job_id is None:
        return _refuse("anchor_outbox_job_id_missing")
    if original_job is None:
        return _refuse("anchor_outbox_job_missing")
    if original_job.id != anchor_outbox.job_id:
        # The caller handed us a job that did not produce this outbox row. The
        # provider would then be read off an unrelated row.
        return _refuse("anchor_outbox_job_mismatch")
    if claims_delivery_retry(original_job):
        return _refuse("anchor_job_is_retry")
    if _outbox_meta_claims_retry(anchor_outbox):
        # The job does not claim the boundary but the audit trail does. One of
        # the two is wrong, and neither can be trusted to say which.
        return _refuse("anchor_outbox_meta_claims_retry")

    provider = normalize_provider(getattr(original_job, "provider", None), default="")
    if not provider:
        return _refuse("original_job_provider_unknown")

    if original_job.job_type != job_type:
        return _refuse("job_type_mismatch")
    if anchor_outbox.template_code != job_type:
        return _refuse("anchor_job_type_mismatch")
    if original_job.company_id != anchor_outbox.company_id:
        return _refuse("company_mismatch")
    if original_job.record_id != anchor_outbox.record_id:
        return _refuse("record_mismatch")

    if provider != PROVIDER_EASYWEEK:
        # Altegio keeps exactly the comparison it always had.
        if original_job.client_id != anchor_outbox.client_id:
            return _refuse("client_mismatch")
        return RetryIdentityResolution(
            identity=RetryIdentity(
                provider=provider,
                company_id=original_job.company_id,
                record_id=original_job.record_id,
                client_id=original_job.client_id,
                job_type=job_type,
            )
        )

    # ------------------------------------------------------------------
    # EasyWeek: re-prove the domain scope the outbox worker performs on a
    # freshly planned job. A retry skips planning entirely.
    # ------------------------------------------------------------------
    if job_type not in EASYWEEK_CUSTOMER_JOB_TYPES:
        return _refuse("easyweek_job_type_not_enabled")
    if job_type in EASYWEEK_REVIEW_JOB_TYPES and easyweek_review_retry_identity(original_job) is None:
        return _refuse("easyweek_review_retry_identity_unproven")
    if job_type in EASYWEEK_REMINDER_JOB_TYPES and easyweek_reminder_retry_identity(original_job) is None:
        # A reminder retry has to re-prove itself against the live API, and the
        # guard needs the ROOT job's booking uuid and planned start to do it.
        # Without both, there is nothing to build a provable retry on.
        return _refuse("easyweek_reminder_retry_identity_unproven")
    if original_job.record_id is None:
        return _refuse("easyweek_retry_missing_record")

    record = await session.get(Record, original_job.record_id)
    if record is None:
        return _refuse("easyweek_retry_record_missing")
    if normalize_provider(getattr(record, "provider", None), default="") != PROVIDER_EASYWEEK:
        return _refuse("easyweek_retry_record_provider_mismatch")
    if record.company_id != original_job.company_id:
        return _refuse("easyweek_retry_record_company_mismatch")

    # PR-4 supports PARTIAL deliveries on purpose: a booking-updated or
    # booking-canceled payload that carries no `customer_id` leaves the already
    # known `Record.client_id` in place, and the planner then creates a job with
    # `client_id = NULL` because THAT delivery carried no Client. The outbox
    # worker resolves it through the record (see `_load_client`), so the send
    # succeeds and the outbox row records the real client.
    #
    # Comparing `original_job.client_id` to `anchor_outbox.client_id` directly
    # therefore rejects a perfectly normal booking. The effective client is what
    # the send actually used, and that is what the chain identity is.
    effective_client_id = original_job.client_id if original_job.client_id is not None else record.client_id
    if effective_client_id is None:
        return _refuse("easyweek_retry_missing_client")

    # A job that DOES name a client and names a different one than the record is
    # still a hard refusal — that is a contradiction, not a partial delivery.
    if original_job.client_id is not None and original_job.client_id != record.client_id:
        return _refuse("easyweek_retry_job_client_conflicts_with_record")
    if record.client_id != effective_client_id:
        return _refuse("easyweek_retry_record_client_unlinked")
    if anchor_outbox.client_id != effective_client_id:
        return _refuse("client_mismatch")

    client = await session.get(Client, effective_client_id)
    if client is None:
        return _refuse("easyweek_retry_client_missing")
    if normalize_provider(getattr(client, "provider", None), default="") != PROVIDER_EASYWEEK:
        return _refuse("easyweek_retry_client_provider_mismatch")
    if client.company_id != original_job.company_id:
        return _refuse("easyweek_retry_client_company_mismatch")

    return RetryIdentityResolution(
        identity=RetryIdentity(
            provider=provider,
            company_id=original_job.company_id,
            record_id=record.id,
            # Materialized, so the retry is self-contained: it no longer depends
            # on the record still pointing at the same client when it runs. The
            # ORIGINAL job keeps its NULL — PR-4 semantics are not rewritten.
            client_id=effective_client_id,
            job_type=job_type,
        )
    )


def _status_chain_refuse(reason: str) -> StatusRetryChainResolution:
    return StatusRetryChainResolution(error=reason)


async def resolve_status_retry_chain(
    session: AsyncSession,
    outbox: OutboxMessage,
) -> StatusRetryChainResolution:
    """Resolve callback chain data from jobs, treating outbox meta as audit-only.

    No id read from ``meta`` is ever used in SQL. A meta value is bounded and
    compared only after the current job has independently proven the canonical
    retry reference.
    """
    meta_claim = _outbox_meta_claims_retry(outbox)

    current_job: MessageJob | None = None
    if outbox.job_id is not None:
        current_job = await session.get(MessageJob, outbox.job_id)

    job_claim = current_job is not None and claims_delivery_retry(current_job)
    if not job_claim:
        if meta_claim:
            if outbox.job_id is None:
                return _status_chain_refuse("retry_outbox_job_id_missing")
            if current_job is None:
                return _status_chain_refuse("retry_outbox_job_missing")
            return _status_chain_refuse("retry_outbox_job_claim_missing")
        return StatusRetryChainResolution(
            chain=StatusRetryChain(
                original_outbox_id=int(outbox.id),
                attempt_number=None,
                anchor_outbox=outbox,
                original_job=current_job,
                identity=None,
                is_retry=False,
            )
        )

    assert current_job is not None
    reference_resolution = resolve_retry_reference(current_job)
    if reference_resolution.reference is None:
        return _status_chain_refuse(reference_resolution.error or "delivery_retry_reference_unproven")
    reference = reference_resolution.reference

    audit_mismatch = retry_outbox_audit_mismatch(
        outbox,
        original_outbox_id=reference.original_outbox_id,
        attempt_number=reference.attempt_number,
    )
    if audit_mismatch is not None:
        return _status_chain_refuse(audit_mismatch)

    anchor_outbox = await session.get(OutboxMessage, reference.original_outbox_id)
    if anchor_outbox is None:
        return _status_chain_refuse("retry_anchor_outbox_missing")

    original_job: MessageJob | None = None
    if anchor_outbox.job_id is not None:
        original_job = await session.get(MessageJob, anchor_outbox.job_id)
    identity_resolution = await resolve_retry_identity(
        session,
        anchor_outbox=anchor_outbox,
        original_job=original_job,
        job_type=current_job.job_type,
    )
    if identity_resolution.identity is None:
        return _status_chain_refuse(identity_resolution.error or "retry_chain_identity_unproven")
    identity = identity_resolution.identity

    mismatch = identity.mismatch_field(current_job)
    if mismatch is not None:
        return _status_chain_refuse(f"retry_current_job_{mismatch}_mismatch")
    if outbox.company_id != identity.company_id:
        return _status_chain_refuse("retry_current_outbox_company_mismatch")
    if outbox.record_id != identity.record_id:
        return _status_chain_refuse("retry_current_outbox_record_mismatch")
    if outbox.client_id != identity.client_id:
        return _status_chain_refuse("retry_current_outbox_client_mismatch")
    if outbox.template_code != identity.job_type:
        return _status_chain_refuse("retry_current_outbox_job_type_mismatch")

    return StatusRetryChainResolution(
        chain=StatusRetryChain(
            original_outbox_id=reference.original_outbox_id,
            attempt_number=reference.attempt_number,
            anchor_outbox=anchor_outbox,
            original_job=original_job,
            identity=identity,
            is_retry=True,
        )
    )


@dataclass(frozen=True)
class RetryChainMember:
    """One job proven to belong to a chain, with the pointer that proved it."""

    job: MessageJob
    reference: RetryReference


@dataclass(frozen=True)
class RetryChainMembers:
    """The proven membership of one chain: its root identity and its jobs."""

    original_outbox_id: int
    identity: RetryIdentity | None = None
    members: tuple[RetryChainMember, ...] = ()
    candidate_count: int = 0
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.identity is not None

    @property
    def job_ids(self) -> list[int]:
        return [int(member.job.id) for member in self.members]

    @property
    def attempt_numbers(self) -> set[int]:
        return {member.reference.attempt_number for member in self.members}

    @property
    def has_unproven_candidates(self) -> bool:
        """True when rows carry the chain's key prefix without proving membership."""
        return self.candidate_count > len(self.members)


async def resolve_retry_chain_members(
    session: AsyncSession,
    original_outbox_id: int,
    *,
    statuses: tuple[str, ...] | None = None,
    for_update: bool = False,
) -> RetryChainMembers:
    """Who actually belongs to the chain rooted at *original_outbox_id*.

    The one definition of chain membership. Sharing the dedupe key prefix is not
    it: the prefix says a row was NAMED after this root, nothing about who wrote
    it or which chain its payload points at. A row counts only when its own
    payload and namespace prove a reference to this root AND its identity
    matches the identity proven from the root itself.

    ``statuses`` narrows the candidate set (the cancel path wants only
    ``queued``); ``for_update`` takes a row lock on the candidates, which the
    cancel path needs so the outbox worker cannot claim a row between the proof
    and the cancellation. Both are read shapes — the membership rule below is
    the same either way, which is the entire point of this living in one place.
    """
    anchor_outbox = await session.get(OutboxMessage, original_outbox_id)
    if anchor_outbox is None:
        return RetryChainMembers(original_outbox_id=original_outbox_id, error="chain_root_outbox_missing")

    anchor_job: MessageJob | None = None
    if anchor_outbox.job_id is not None:
        anchor_job = await session.get(MessageJob, anchor_outbox.job_id)

    resolution = await resolve_retry_identity(
        session,
        anchor_outbox=anchor_outbox,
        original_job=anchor_job,
        job_type=anchor_outbox.template_code,
    )
    if resolution.identity is None:
        # An unprovable root has no provable members. Fail closed rather than
        # letting an unproven pointer decide anything about other rows.
        return RetryChainMembers(
            original_outbox_id=original_outbox_id,
            error=resolution.error or "chain_root_identity_unproven",
        )
    identity = resolution.identity

    prefix = f"{DELIVERY_RETRY_DEDUPE_PREFIX}{original_outbox_id}:"
    stmt = select(MessageJob).where(MessageJob.dedupe_key.like(prefix + "%"))
    if statuses is not None:
        stmt = stmt.where(MessageJob.status.in_(statuses))
    if for_update:
        stmt = stmt.with_for_update()
    candidates = list((await session.execute(stmt)).scalars().all())

    members: list[RetryChainMember] = []
    for candidate in candidates:
        reference = resolve_retry_reference(candidate)
        if reference.reference is None:
            continue
        if reference.reference.original_outbox_id != original_outbox_id:
            continue
        if identity.mismatch_field(candidate) is not None:
            continue
        members.append(RetryChainMember(job=candidate, reference=reference.reference))

    return RetryChainMembers(
        original_outbox_id=original_outbox_id,
        identity=identity,
        members=tuple(members),
        candidate_count=len(candidates),
    )
