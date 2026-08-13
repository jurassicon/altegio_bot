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
2. a captured, untruncated event for that UUID carries a price the NEW contract
   accepts — the same ``_price_to_decimal`` the worker runs, never a second
   implementation that could drift from it;
3. that same event also carries an exact ``booking_price_int``, so the OLD
   formula can be replayed;
4. the stored ``Record.total_cost`` equals what the old formula produced, and
   the new canonical value differs from it — i.e. the row carries the bug's
   signature rather than merely an unexpected amount;
5. the booking has exactly one service snapshot, and its ``cost_to_pay`` already
   equals ``Record.total_cost`` — the consistent pre-repair state.

Anything short of that is skipped and counted, never guessed at. A row edited by
hand, a booking whose service set is ambiguous, a truncated capture, an Altegio
record, a row that is already correct: all skips.

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

Traversal is keyset-paged over ``records.id`` in bounded batches, each in its own
short transaction, so a long production table never holds one lock for minutes.

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

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.db import SessionLocal
from altegio_bot.easyweek_normalizer import (
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

DEFAULT_BATCH_SIZE: Final = 200
# Reported id lists are for an operator to spot-check, not a data export.
MAX_REPORTED_IDS: Final = 50

# Why a record was left alone. Every one of these is a refusal to guess.
SKIP_NO_BOOKING_UUID: Final = "no_booking_uuid"
SKIP_NO_USABLE_EVENT: Final = "no_usable_event"
SKIP_NOT_ONE_SERVICE: Final = "not_exactly_one_service"
SKIP_SNAPSHOT_INCONSISTENT: Final = "snapshot_inconsistent"
SKIP_ALREADY_CORRECT: Final = "already_correct"
SKIP_SIGNATURE_MISMATCH: Final = "signature_mismatch"


@dataclass(frozen=True)
class PriceEvidence:
    """What one captured event proves about a booking's price."""

    event_id: int
    # What the OLD parser produced from this event, and what the fixed parser
    # produces from the same bytes. The repair is only ever the step between.
    old_value: Decimal
    new_value: Decimal


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


async def find_price_evidence(session: AsyncSession, record: Record) -> PriceEvidence | None:
    """The newest captured delivery for this booking that proves a price.

    Deterministic by ``easyweek_events.id`` descending: capture is append-only
    and monotonic, so "newest" is a total order, not a timestamp tie-break.
    """
    if record.easyweek_booking_uuid is None:
        return None

    events = (
        (
            await session.execute(
                select(EasyWeekEvent)
                .where(EasyWeekEvent.booking_uuid == record.easyweek_booking_uuid)
                .order_by(EasyWeekEvent.id.desc())
            )
        )
        .scalars()
        .all()
    )
    for event in events:
        evidence = price_evidence(event)
        if evidence is not None:
            return evidence
    return None


async def classify(
    session: AsyncSession, record: Record
) -> tuple[str | None, PriceEvidence | None, RecordService | None]:
    """Decide what to do with one record.

    Returns ``(skip_reason, evidence, service)``. A ``None`` skip reason means
    the record carries the bug's full signature and may be repaired.
    """
    if record.easyweek_booking_uuid is None:
        return SKIP_NO_BOOKING_UUID, None, None

    evidence = await find_price_evidence(session, record)
    if evidence is None:
        return SKIP_NO_USABLE_EVENT, None, None

    services = list(
        (await session.execute(select(RecordService).where(RecordService.record_id == record.id))).scalars().all()
    )
    if len(services) != 1:
        # Zero rows leave nothing to keep in step with the record; several rows
        # mean the booking-level total cannot be attributed to one of them.
        return SKIP_NOT_ONE_SERVICE, evidence, None
    service = services[0]

    if record.total_cost == evidence.new_value:
        # Either already repaired, or written after the fix. Both are correct.
        return SKIP_ALREADY_CORRECT, evidence, service

    if record.total_cost is None or record.total_cost != evidence.old_value:
        # Not what the old formula would have produced: something else wrote
        # this value, and we do not know what it meant.
        return SKIP_SIGNATURE_MISMATCH, evidence, service

    if service.cost_to_pay != record.total_cost:
        # The pre-repair state is supposed to be consistent. A divergence is a
        # separate problem, and overwriting it would hide it.
        return SKIP_SNAPSHOT_INCONSISTENT, evidence, service

    return None, evidence, service


async def repair_prices(
    session_factory: Any,
    *,
    apply: bool = False,
    batch_size: int = DEFAULT_BATCH_SIZE,
    max_records: int | None = None,
) -> RepairReport:
    """Audit — and, with ``apply``, correct — EasyWeek price snapshots.

    One short transaction per batch. Altegio records are excluded by the query
    itself, so no Altegio row is ever read, let alone written.
    """
    report = RepairReport(applied=apply)
    last_id = 0

    while True:
        remaining = None if max_records is None else max_records - report.scanned
        if remaining is not None and remaining <= 0:
            break
        limit = batch_size if remaining is None else min(batch_size, remaining)

        async with session_factory() as session:
            async with session.begin():
                records = list(
                    (
                        await session.execute(
                            select(Record)
                            .where(Record.provider == PROVIDER_EASYWEEK)
                            .where(Record.id > last_id)
                            .order_by(Record.id.asc())
                            .limit(limit)
                        )
                    )
                    .scalars()
                    .all()
                )
                if not records:
                    break

                for record in records:
                    last_id = record.id
                    report.scanned += 1

                    reason, evidence, service = await classify(session, record)
                    if reason is not None:
                        report.skipped[reason] += 1
                        continue

                    assert evidence is not None and service is not None
                    report.repairable += 1
                    report.repairable_record_ids.append(record.id)
                    report.evidence_event_ids.append(evidence.event_id)

                    if not apply:
                        continue

                    # Both halves of one snapshot, in one transaction: the
                    # invariant PR-5 renders from is never observable as broken.
                    record.total_cost = evidence.new_value
                    service.cost_to_pay = evidence.new_value
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
        help=f"Records per transaction (default {DEFAULT_BATCH_SIZE}).",
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
