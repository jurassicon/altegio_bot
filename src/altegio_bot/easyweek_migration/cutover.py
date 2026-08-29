"""The cutover instant, and every timezone decision the migration makes (PR-11.1).

Two rules, both non-negotiable:

1. **One immutable instant per run.** ``cutover_at`` is computed once, at the top
   of the run, and then only read. If each booking asked "is this in the future?"
   against its own ``now``, a bulk apply spanning an hour would migrate a
   different set than the dry-run that was verified, and the boundary would
   depend on how long the run happened to take.

2. **No naive datetimes, anywhere.** Altegio hands us *local wall-clock strings*
   with no offset. A wall-clock string is not a moment in time until a zone is
   attached, and Central European time has two days a year where attaching one is
   genuinely ambiguous or impossible. Those two days are not hypothetical: the
   autumn fold falls in late October, and this cutover migrates bookings weeks
   ahead. So the fold and the gap are detected and the booking is BLOCKED — a
   customer whose appointment we would place an hour off is worse than a customer
   whose appointment an operator moves by hand.

The zone itself is the same ``Europe/Belgrade`` the production Altegio path
already uses (``altegio_records._ALTEGIO_LOCAL_TZ``). That is not a cosmetic
choice: reading the same strings under a different zone would make the migration
disagree with the running bot about when a booking starts. Belgrade and Berlin
share CET/CEST offsets and DST rules to the second, so the interpretation is the
salons' local time either way — but the *code path* stays the one production has
been right about for a year, rather than a second opinion introduced here.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Final
from zoneinfo import ZoneInfo

# Deliberately the same zone the Altegio production path uses. See module docstring.
ALTEGIO_LOCAL_TZ: Final = ZoneInfo("Europe/Belgrade")

# Stable, PII-free reasons a source timestamp cannot become an instant.
TIME_UNPARSEABLE: Final = "start_time_unparseable"
TIME_AMBIGUOUS_DST: Final = "start_time_ambiguous_dst"
TIME_NONEXISTENT_DST: Final = "start_time_nonexistent_dst"


class CutoverError(ValueError):
    """The operator-supplied cutover timestamp is unusable. Raised before any work."""


@dataclass(frozen=True)
class Cutover:
    """One immutable UTC instant, plus how it was obtained."""

    at: datetime
    # "operator" for an explicit --cutover-at, "run_start" for a dry-run default.
    source: str

    def __post_init__(self) -> None:
        if self.at.tzinfo is None or self.at.utcoffset() is None:
            raise CutoverError("cutover_at must be timezone-aware")
        if self.at.utcoffset() != timedelta(0):
            raise CutoverError("cutover_at must be normalised to UTC")

    @property
    def iso(self) -> str:
        return self.at.isoformat().replace("+00:00", "Z")

    def as_safe_dict(self) -> dict[str, Any]:
        return {"cutover_at": self.iso, "cutover_source": self.source}


def parse_cutover(raw: object) -> Cutover:
    """Parse an operator-supplied cutover timestamp into UTC.

    An **explicit offset is required**. ``2026-09-01T00:00:00`` names a wall
    clock, not an instant, and silently reading it as UTC would shift the
    boundary by two hours in summer — quietly migrating (or quietly skipping) two
    hours' worth of real appointments. ``Z`` and ``+02:00`` are both accepted;
    nothing else is.
    """
    if not isinstance(raw, str) or not raw.strip():
        raise CutoverError("cutover_at is required and must be an ISO-8601 string")

    text = raw.strip()
    # ``fromisoformat`` accepts "Z" only from Python 3.11 onwards; the project
    # targets 3.12, so this is a normalisation for symmetry, not a workaround.
    candidate = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        raise CutoverError("cutover_at is not a valid ISO-8601 timestamp") from None

    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise CutoverError("cutover_at must carry an explicit UTC offset (e.g. 'Z' or '+02:00')")

    return Cutover(at=parsed.astimezone(timezone.utc), source="operator")


def run_start_cutover(now: datetime) -> Cutover:
    """The default cutover for a read-only mode: the moment the run began.

    Only inventory and dry-run may use this. ``apply`` demands an explicit
    timestamp, because "whatever the clock said when the operator pressed enter"
    is not something a second operator can verify afterwards.
    """
    if now.tzinfo is None or now.utcoffset() is None:
        raise CutoverError("run start must be timezone-aware")
    return Cutover(at=now.astimezone(timezone.utc), source="run_start")


class LocalTimeError(ValueError):
    """A local wall-clock string could not be resolved to exactly one instant."""

    def __init__(self, reason: str) -> None:
        # The reason is a stable code; the offending value is never carried.
        self.reason = reason
        super().__init__(reason)


def parse_altegio_local_to_utc(raw: object) -> datetime:
    """Resolve one Altegio local wall-clock string to a single UTC instant.

    Raises :class:`LocalTimeError` — never returns an approximation — when:

    * the value is not a parseable ``YYYY-MM-DD HH:MM:SS`` local timestamp;
    * the wall clock falls in the **autumn fold**, where it happens twice
      (``fold=0`` and ``fold=1`` give different instants and nothing in the
      source says which one was meant);
    * the wall clock falls in the **spring gap**, where it never happens at all
      and Python would silently hand back a time an hour away from the one
      written down.

    Both DST branches are the reason this function exists instead of a
    ``replace(tzinfo=...)`` one-liner. ``replace`` on a folded time picks the
    first occurrence with no complaint, which is a coin flip performed silently
    on a real customer's appointment.

    An explicit offset in the value is honoured as-is: if Altegio ever starts
    sending one, it is authoritative and no local resolution is needed.
    """
    if not isinstance(raw, str) or not raw.strip():
        raise LocalTimeError(TIME_UNPARSEABLE)

    text = raw.strip().replace(" ", "T")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        raise LocalTimeError(TIME_UNPARSEABLE) from None

    if parsed.tzinfo is not None and parsed.utcoffset() is not None:
        return parsed.astimezone(timezone.utc)

    first = parsed.replace(tzinfo=ALTEGIO_LOCAL_TZ, fold=0)
    second = parsed.replace(tzinfo=ALTEGIO_LOCAL_TZ, fold=1)

    if first.utcoffset() != second.utcoffset():
        # The two folds disagree. Which of the two it is depends on the DIRECTION
        # of the transition, and the standard test for that is whether the
        # instant round-trips back to the same wall clock.
        round_tripped = first.astimezone(timezone.utc).astimezone(ALTEGIO_LOCAL_TZ)
        if round_tripped.replace(tzinfo=None, fold=0) != parsed.replace(fold=0):
            # The wall clock does not exist: the spring-forward gap.
            raise LocalTimeError(TIME_NONEXISTENT_DST)
        # The wall clock exists twice: the autumn fold.
        raise LocalTimeError(TIME_AMBIGUOUS_DST)

    return first.astimezone(timezone.utc)
