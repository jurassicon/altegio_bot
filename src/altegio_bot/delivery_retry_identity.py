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
from dataclasses import dataclass
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.easyweek_policy import (
    EASYWEEK_LIFECYCLE_JOB_TYPES,
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

    if getattr(job, "job_type", None) not in DELIVERY_RETRY_JOB_TYPES:
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

    Every refusal reason names an invariant and never a value: a mismatching row
    may belong to another tenant, and these strings reach ``job.last_error``,
    the outbox audit metadata and the log.
    """
    if original_job is None:
        return _refuse("original_job_missing")

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
    if job_type not in EASYWEEK_LIFECYCLE_JOB_TYPES:
        return _refuse("easyweek_job_type_not_enabled")
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


def _outbox_meta_claims_retry(outbox: OutboxMessage) -> bool:
    meta = outbox.meta
    if not isinstance(meta, dict):
        return False
    return (
        meta.get("delivery_retry") is True
        or meta.get("delivery_retry_of_outbox_id") is not None
        or meta.get("delivery_retry_attempt") is not None
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

    meta = outbox.meta if isinstance(outbox.meta, dict) else {}
    if meta.get("delivery_retry") is not True:
        return _status_chain_refuse("retry_outbox_audit_marker_missing")
    meta_outbox_id = parse_retry_outbox_id(meta.get("delivery_retry_of_outbox_id"))
    if meta_outbox_id is None:
        return _status_chain_refuse("retry_outbox_audit_reference_invalid")
    if meta_outbox_id != reference.original_outbox_id:
        return _status_chain_refuse("retry_outbox_audit_reference_mismatch")
    meta_attempt = parse_retry_attempt(meta.get("delivery_retry_attempt"))
    if meta_attempt is None:
        return _status_chain_refuse("retry_outbox_audit_attempt_invalid")
    if meta_attempt != reference.attempt_number:
        return _status_chain_refuse("retry_outbox_audit_attempt_mismatch")

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
