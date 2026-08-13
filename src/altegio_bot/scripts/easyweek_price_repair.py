"""PR-7.3: audit and repair EasyWeek price snapshots stored by the old parser.

Until PR-7.3 the normalizer read ``booking_price_int`` as a cent count and
divided it by 100. Production capture then proved the field is expressed in
MAJOR units: a real 120.00 € booking arrived as ``booking_price_int=120`` and
``booking_price="12000"``, and was persisted as ``1.20``. Every EasyWeek record
written before the fix therefore holds a price a hundred times too small.

This command exists to correct exactly those rows, and nothing else.

What makes a row repairable is not "the number looks small". It is a proof,
assembled per booking from evidence we already stored:

1. the record is ``provider='easyweek'`` and has a canonical booking UUID;
2. the booking has exactly one service snapshot, and its ``cost_to_pay`` already
   equals ``Record.total_cost`` — the consistent pre-repair state;
3. among the deliveries that could have written a price — ``processed``, a
   lifecycle hint, untruncated, and carrying a price the NEW contract accepts —
   at least one reproduces the stored value through the OLD formula;
4. and every such candidate agrees on the same corrected value.

**Why the last point is not pedantry.** ``status='processed'`` does not mean
"this delivery wrote the price". The worker marks a delivery processed and
returns without touching the domain when it is a ``booking-succeeded`` event, an
exact replay of one already applied, or an update arriving after the booking was
cancelled. The schema does not record which processed delivery performed a
write, and this command does not invent that record. It therefore refuses to
pick a winner: if two candidates are equally consistent with the stored value
and name different prices, the row is reported as ``ambiguous_evidence`` and
left alone. Event ids order the report; they never decide the amount.

Anything short of that proof is skipped and counted, never guessed at. A row
edited by hand, a booking whose service set is ambiguous, a truncated capture,
an Altegio record, a row that is already correct: all skips.

Blast radius, deliberately small:

* only ``Record.total_cost`` and the single ``RecordService.cost_to_pay`` are
  written — both halves of one snapshot, so the invariant holds at every commit;
* no ``MessageJob`` is created, reopened or retried, and nothing is sent;
* ``easyweek_events`` rows are read only — status, payload and error stay as
  captured;
* the default mode changes nothing at all. Writing requires ``--apply``.

Idempotent by construction: after a successful repair the stored value no longer
matches the old formula, so a second run classifies the row as already correct
and writes nothing.

**Concurrency.** ``EASYWEEK_PROCESSING_ENABLED`` may legitimately stay on while
this runs, so the worker can write a newer, already-correct price at any moment.
Under ``--apply`` each record is therefore locked with ``SELECT ... FOR UPDATE``
— the same lock ``upsert_record`` takes, in the same order, record before
service row — and the ENTIRE decision is re-derived underneath that lock. A
value computed before the lock is never written. If the worker committed a new
price while we waited, the stored value no longer carries the bug's signature
and the row is skipped instead of being dragged back to a historical amount.

Traversal is a read-only keyset page over ``records.id`` followed by one short
transaction per record, so a long production table never holds one lock for
minutes and an audit takes no write locks at all.

Output discipline matches the rest of the EasyWeek ops tooling: counts and
technical ids (record, service, event, company) only. No payload, no price, no
name, phone, e-mail or service title ever reaches stdout or the logs — a price
is customer data, and this command runs in a terminal someone may paste from.
"""

from __future__ import annotations

import argparse
import asyncio
from collections import Counter
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Final

from sqlalchemy import Select, select
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.db import SessionLocal
from altegio_bot.easyweek_normalizer import (
    _EVENT_HINT_MAP,
    IGNORE,
    MAX_MONEY_CENTS,
    NormalizationError,
    _as_exact_int,
    _price_to_decimal,
)
from altegio_bot.models.models import (
    PROVIDER_EASYWEEK,
    EasyWeekEvent,
    Record,
    RecordService,
)
from altegio_bot.workers.easyweek_inbox_worker import STATUS_PROCESSED

DEFAULT_BATCH_SIZE: Final = 200
# Reported id lists are for an operator to spot-check, not a data export.
MAX_REPORTED_IDS: Final = 50

# The hints whose deliveries can reach a domain write at all. Derived from the
# normalizer's own map rather than restated, so a new trigger cannot silently
# become evidence here without going through the parser first.
# `booking-succeeded` is excluded because it normalises to None: the worker
# marks it processed and returns before touching Client, Record or MessageJob.
PRICE_WRITING_HINTS: Final = frozenset(hint for hint, action in _EVENT_HINT_MAP.items() if action != IGNORE)

# Why a record was left alone. Every one of these is a refusal to guess.
SKIP_NO_BOOKING_UUID: Final = "no_booking_uuid"
SKIP_NO_USABLE_EVIDENCE: Final = "no_usable_evidence"
SKIP_NOT_ONE_SERVICE: Final = "not_exactly_one_service"
SKIP_INCONSISTENT_SERVICE_SNAPSHOT: Final = "inconsistent_service_snapshot"
SKIP_ALREADY_CORRECT: Final = "already_correct"
SKIP_LEGACY_SIGNATURE_MISMATCH: Final = "legacy_signature_mismatch"
SKIP_AMBIGUOUS_EVIDENCE: Final = "ambiguous_evidence"


@dataclass(frozen=True)
class PriceEvidence:
    """What one captured event proves about a booking's price."""

    event_id: int
    # What the OLD parser produced from this event, and what the fixed parser
    # produces from the same bytes. The repair is only ever the step between.
    old_value: Decimal
    new_value: Decimal


@dataclass(frozen=True)
class Decision:
    """Either a skip reason, or the single value every candidate agreed on."""

    reason: str | None
    value: Decimal | None = None
    service: RecordService | None = None
    evidence_event_id: int | None = None


def _maybe_lock(stmt: Select[Any], lock: bool) -> Select[Any]:
    """``FOR UPDATE`` when we intend to write, a plain read when we do not.

    A dry-run must stay genuinely read-only: taking write locks during an audit
    would let a report block the live worker.
    """
    return stmt.with_for_update() if lock else stmt


@dataclass
class RepairReport:
    """Counts and technical ids only — never an amount, never a payload."""

    applied: bool
    scanned: int = 0
    repairable: int = 0
    repaired: int = 0
    skipped: Counter = field(default_factory=Counter)
    repairable_record_ids: list[int] = field(default_factory=list)
    repaired_record_ids: list[int] = field(default_factory=list)
    evidence_event_ids: list[int] = field(default_factory=list)

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "mode": "apply" if self.applied else "dry-run",
            "scanned": self.scanned,
            "repairable": self.repairable,
            "repaired": self.repaired,
            "skipped": dict(sorted(self.skipped.items())),
            "repairable_record_ids": self.repairable_record_ids[:MAX_REPORTED_IDS],
            "repaired_record_ids": self.repaired_record_ids[:MAX_REPORTED_IDS],
            "evidence_event_ids": self.evidence_event_ids[:MAX_REPORTED_IDS],
        }


def legacy_price(payload: dict[str, Any]) -> Decimal | None:
    """Replay the OLD parser exactly: ``booking_price_int`` divided by 100.

    Kept as a faithful reproduction, guards included, because it is the only way
    to prove a stored value came from the bug rather than from something else we
    have not accounted for. It is never used to decide a new price.
    """
    raw = payload.get("booking_price_int")
    if raw is None:
        return None
    cents = _as_exact_int(raw)
    if cents is None or cents < 0 or cents > MAX_MONEY_CENTS:
        return None
    return (Decimal(cents) / Decimal(100)).quantize(Decimal("0.01"))


def price_evidence(event: EasyWeekEvent) -> PriceEvidence | None:
    """What this one captured delivery proves, or ``None`` if it proves nothing.

    A truncated body is refused outright: the missing bytes are exactly where a
    contradicting price field would have been.
    """
    if event.body_truncated:
        return None
    payload = event.payload
    if not isinstance(payload, dict):
        return None

    try:
        new_value = _price_to_decimal(payload)
    except NormalizationError:
        # The delivery would be rejected by the running worker too. It cannot be
        # the source of a corrected price.
        return None
    if new_value is None:
        return None

    old_value = legacy_price(payload)
    if old_value is None:
        return None

    return PriceEvidence(event_id=event.id, old_value=old_value, new_value=new_value)


async def collect_price_evidence(session: AsyncSession, record: Record) -> list[PriceEvidence]:
    """Every delivery of this booking that could have written the stored price.

    The filter is the set of preconditions the worker itself imposes before a
    price reaches ``Record``:

    * ``status='processed'`` — a ``captured``/``processing`` row has not run to
      completion, and a ``failed`` row never wrote anything at all;
    * a lifecycle hint — ``booking-succeeded`` normalises to ``None`` and returns
      before any domain write, so it can never have set a price;
    * a body the fixed parser accepts, that actually carries a price.

    ``processed`` is necessary but NOT sufficient, and the schema does not record
    which processed delivery performed a write: a replay and a post-cancel
    delivery both reach ``processed`` having changed nothing. That gap is not
    papered over here. Every surviving candidate is returned, the caller requires
    them to AGREE, and disagreement is a refusal rather than a tie-break.

    Ordered by id purely so the report is stable.
    """
    if record.easyweek_booking_uuid is None:
        return []

    events = (
        (
            await session.execute(
                select(EasyWeekEvent)
                .where(EasyWeekEvent.booking_uuid == record.easyweek_booking_uuid)
                .where(EasyWeekEvent.status == STATUS_PROCESSED)
                .where(EasyWeekEvent.event_hint.in_(PRICE_WRITING_HINTS))
                .order_by(EasyWeekEvent.id.asc())
            )
        )
        .scalars()
        .all()
    )
    return [evidence for event in events if (evidence := price_evidence(event)) is not None]


async def classify(session: AsyncSession, record: Record, *, lock: bool = False) -> Decision:
    """Decide what to do with one record, from ALL of its usable evidence.

    ``lock`` takes the same row-level locks the worker takes, in the same order
    (record first, then its service snapshot), so a decision made with it can
    still be true when it is written.
    """
    if record.easyweek_booking_uuid is None:
        return Decision(SKIP_NO_BOOKING_UUID)

    services = list(
        (await session.execute(_maybe_lock(select(RecordService).where(RecordService.record_id == record.id), lock)))
        .scalars()
        .all()
    )
    if len(services) != 1:
        # Zero rows leave nothing to keep in step with the record; several rows
        # mean the booking-level total cannot be attributed to one of them.
        return Decision(SKIP_NOT_ONE_SERVICE)
    service = services[0]

    usable = await collect_price_evidence(session, record)
    if not usable:
        return Decision(SKIP_NO_USABLE_EVIDENCE)

    if any(record.total_cost == item.new_value for item in usable):
        # Either already repaired, or written after the fix. Both are correct,
        # and this is what makes a second --apply a no-op.
        return Decision(SKIP_ALREADY_CORRECT)

    compatible = [item for item in usable if record.total_cost is not None and item.old_value == record.total_cost]
    if not compatible:
        # Nothing we can prove wrote this value. Something else did, and we do
        # not know what it meant.
        return Decision(SKIP_LEGACY_SIGNATURE_MISMATCH)

    if service.cost_to_pay != record.total_cost:
        # The pre-repair state is supposed to be consistent. A divergence is a
        # separate problem, and overwriting it would hide it.
        return Decision(SKIP_INCONSISTENT_SERVICE_SNAPSHOT)

    canonical_values = {item.new_value for item in compatible}
    if len(canonical_values) != 1:
        # Several deliveries are equally consistent with the stored value and
        # they disagree about the real price. Picking the newest would be a
        # guess dressed up as a rule.
        return Decision(SKIP_AMBIGUOUS_EVIDENCE)

    value = canonical_values.pop()
    return Decision(
        None,
        value=value,
        service=service,
        # Lowest id: a stable label for the report, chosen AFTER the value was
        # agreed. It never selects the value.
        evidence_event_id=min(item.event_id for item in compatible),
    )


async def _record_id_page(session: AsyncSession, after_id: int, limit: int) -> list[int]:
    """One bounded keyset page of EasyWeek record ids, read-only.

    Altegio records are excluded by the query itself, so no Altegio row is ever
    read, let alone written.
    """
    return list(
        (
            await session.execute(
                select(Record.id)
                .where(Record.provider == PROVIDER_EASYWEEK)
                .where(Record.id > after_id)
                .order_by(Record.id.asc())
                .limit(limit)
            )
        )
        .scalars()
        .all()
    )


async def repair_prices(
    session_factory: Any,
    *,
    apply: bool = False,
    batch_size: int = DEFAULT_BATCH_SIZE,
    max_records: int | None = None,
) -> RepairReport:
    """Audit — and, with ``apply``, correct — EasyWeek price snapshots.

    Traversal is a read-only keyset page of ids, followed by one short
    transaction per record. Under ``apply`` that transaction locks the record
    and its service row FIRST and re-derives the whole decision underneath the
    lock, so a price the worker wrote while we were paging cannot be replaced by
    a value computed before it existed.
    """
    report = RepairReport(applied=apply)
    last_id = 0

    while True:
        remaining = None if max_records is None else max_records - report.scanned
        if remaining is not None and remaining <= 0:
            break
        limit = batch_size if remaining is None else min(batch_size, remaining)

        async with session_factory() as session:
            record_ids = await _record_id_page(session, last_id, limit)
        if not record_ids:
            break
        last_id = record_ids[-1]

        for record_id in record_ids:
            report.scanned += 1
            async with session_factory() as session:
                async with session.begin():
                    # Under --apply this SELECT waits for any worker transaction
                    # holding the row, and returns what that worker committed —
                    # not what we saw while paging.
                    record = (
                        (await session.execute(_maybe_lock(select(Record).where(Record.id == record_id), apply)))
                        .scalars()
                        .first()
                    )
                    if record is None or record.provider != PROVIDER_EASYWEEK:
                        # Deleted, or re-provisioned, between the page and the
                        # lock. Not ours to touch.
                        report.scanned -= 1
                        continue

                    decision = await classify(session, record, lock=apply)
                    if decision.reason is not None:
                        report.skipped[decision.reason] += 1
                        continue

                    assert decision.value is not None and decision.service is not None
                    report.repairable += 1
                    report.repairable_record_ids.append(record.id)
                    if decision.evidence_event_id is not None:
                        report.evidence_event_ids.append(decision.evidence_event_id)

                    if not apply:
                        continue

                    # Both halves of one snapshot, under the same lock and in
                    # one transaction: the invariant PR-5 renders from is never
                    # observable as broken, and the value written was derived
                    # from the state we are holding.
                    record.total_cost = decision.value
                    decision.service.cost_to_pay = decision.value
                    report.repaired += 1
                    report.repaired_record_ids.append(record.id)

    return report


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audit (default) or repair EasyWeek price snapshots written by the pre-PR-7.3 parser."
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write the corrected prices. Without this flag the command changes nothing.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=(
            f"Record ids read per keyset page (default {DEFAULT_BATCH_SIZE}). "
            "Each record is then handled in its own short transaction."
        ),
    )
    parser.add_argument(
        "--max-records",
        type=int,
        default=None,
        help="Stop after examining this many records. Useful for a first bounded look.",
    )
    args = parser.parse_args(argv)
    if args.batch_size < 1:
        parser.error("--batch-size must be at least 1")
    if args.max_records is not None and args.max_records < 1:
        parser.error("--max-records must be at least 1")
    return args


async def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    report = await repair_prices(
        SessionLocal,
        apply=args.apply,
        batch_size=args.batch_size,
        max_records=args.max_records,
    )
    print(report.as_safe_dict())


if __name__ == "__main__":
    asyncio.run(main())
