"""Resolving an Altegio customer to an EXISTING EasyWeek customer (PR-11.1).

Customers were imported into EasyWeek before this migration ran — that import is
also what carries the historical visit counters, and the canary proved EasyWeek
keeps that baseline. So this package never creates a customer. It only *finds*
one, and only when it can find exactly one.

Where the directory comes from
------------------------------
The confirmed EasyWeek Public API v2 surface (plan §1.1) is ``/workspace``,
``/locations`` and ``/bookings/{uuid}``. There is no confirmed customer-search
endpoint, and guessing one is precisely the failure PR-9 already paid for: a
field that was documented, assumed present, and simply never sent. So the
directory is the operator's own EasyWeek customer export — the artefact the
runbook produces twice anyway, once before the counter import and once after it.

That export is PII. It is passed by path at run time, it is never committed, and
nothing from it beyond a customer UUID ever reaches a report or a log.

The matching rule
-----------------
Exactly one match, on the normalised international number, or the booking is
blocked:

* **0 matches** → ``customer_not_found``. The customer was not in the import.
  Creating them here would put a person into EasyWeek with no visit history, and
  PR-12's ``repeat_10d`` would then treat a ten-year regular as a first-timer.
* **more than 1 match** → ``customer_ambiguous``. Two EasyWeek records share a
  number (a couple, a family, a duplicated import row). Picking either one books
  a stranger's appointment onto someone else's profile.
* **exactly 1** → that UUID.

Names are never matched on, not even as a tie-breaker. Two people called
"A. Müller" in one salon is not a corner case, it is Tuesday.
"""

from __future__ import annotations

import csv
import json
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Final

from altegio_bot.easyweek_migration.manifest import canonical_uuid
from altegio_bot.webhooks.common import normalize_phone_candidate

# Below this, a "phone number" is not an international number — it is an internal
# extension, a truncated cell or an import artefact, and any match it produced
# would be a coincidence. E.164 country code + national number never gets close.
MIN_E164_DIGITS: Final = 8

CUSTOMER_NOT_FOUND: Final = "customer_not_found"
CUSTOMER_AMBIGUOUS: Final = "customer_ambiguous"
CUSTOMER_PHONE_UNUSABLE: Final = "customer_phone_unusable"
# `POST /bookings` requires a first name and rejects `customer_uuid`, so a card
# we matched but cannot address is a card we cannot book against. Blocking is the
# only honest answer: inventing a name, or borrowing Altegio's spelling of it,
# would write our guess over what EasyWeek already holds.
CUSTOMER_FIRST_NAME_MISSING: Final = "customer_first_name_missing"

DIRECTORY_NOT_READABLE: Final = "customer_directory_not_readable"
DIRECTORY_SHAPE_INVALID: Final = "customer_directory_shape_invalid"
DIRECTORY_EMPTY: Final = "customer_directory_empty"

# Column names accepted in a CSV export, lower-cased. A closed list on purpose:
# an export whose columns we do not recognise must fail loudly at load time, not
# resolve every customer to "not found" three hours into a bulk apply.
_UUID_COLUMNS: Final = ("uuid", "customer_uuid", "id")
_PHONE_COLUMNS: Final = ("phone", "phone_number", "telephone", "mobile")
# Only columns that mean *given name*. A column called `name` is deliberately not
# here: a full name is not a first name, and splitting one on a space is how
# "Anna Maria" becomes "Anna" and a German double surname becomes nonsense. An
# export without a first-name column blocks loudly instead of guessing.
_FIRST_NAME_COLUMNS: Final = ("first_name", "firstname", "given_name", "vorname")


def normalized_international_phone(raw: object) -> str | None:
    """Normalise to ``+<digits>`` using the project-wide contract, or ``None``.

    Delegates to :func:`normalize_phone_candidate` so the migration cannot drift
    from how the bot itself reads a phone number: closed grammar, no silent
    cleaning of stray characters, no truncation. The extra length floor here is
    the migration's own — see :data:`MIN_E164_DIGITS`.
    """
    normalized = normalize_phone_candidate(raw)
    if normalized is None:
        return None
    if len(normalized) - 1 < MIN_E164_DIGITS:
        return None
    return normalized


@dataclass(frozen=True)
class CustomerCard:
    """The EasyWeek customer as the export states them.

    Holds PII — a phone number and a given name — and is therefore never
    serialised into a report, a log or the ledger. It exists for exactly one
    reason: ``POST /bookings`` identifies the customer by phone and first name,
    with no ``customer_uuid`` accepted, so those two values have to travel from
    the matched card to the request body without being invented on the way.
    """

    uuid: str
    phone: str
    first_name: str | None

    @property
    def addressable(self) -> bool:
        return bool(self.first_name and self.first_name.strip())


@dataclass(frozen=True)
class CustomerMatch:
    """A resolution outcome. Either a UUID, or a stable reason there is none."""

    uuid: str | None
    reason: str | None
    match_count: int

    @property
    def resolved(self) -> bool:
        return self.uuid is not None


@dataclass
class CustomerDirectory:
    """Phone → EasyWeek customer UUIDs, built from an operator export.

    ``valid`` is the only field a caller may branch on; an unreadable or
    unrecognised export yields an invalid directory that no mode will act on.
    """

    valid: bool
    reason: str | None = None
    by_phone: dict[str, list[str]] = field(default_factory=dict)
    # customer uuid -> the card's own transport fields. Populated by the loader;
    # a directory built without it can resolve a UUID but cannot address anybody,
    # and `transport_fields` says so rather than filling in a plausible name.
    cards: dict[str, CustomerCard] = field(default_factory=dict)
    # How many export rows were skipped for having no usable phone. Reported as a
    # count so an operator can see "the export is half unusable" without any row
    # being printed.
    unusable_rows: int = 0

    @property
    def size(self) -> int:
        return sum(len(uuids) for uuids in self.by_phone.values())

    def resolve(self, raw_phone: object) -> CustomerMatch:
        """Resolve one source phone to exactly one EasyWeek customer, or refuse."""
        phone = normalized_international_phone(raw_phone)
        if phone is None:
            return CustomerMatch(uuid=None, reason=CUSTOMER_PHONE_UNUSABLE, match_count=0)

        matches = self.by_phone.get(phone, [])
        if not matches:
            return CustomerMatch(uuid=None, reason=CUSTOMER_NOT_FOUND, match_count=0)
        if len(matches) > 1:
            return CustomerMatch(uuid=None, reason=CUSTOMER_AMBIGUOUS, match_count=len(matches))

        # Matched exactly one card — but the booking request needs to address it.
        card = self.cards.get(matches[0])
        if card is None or not card.addressable:
            return CustomerMatch(uuid=None, reason=CUSTOMER_FIRST_NAME_MISSING, match_count=1)
        return CustomerMatch(uuid=matches[0], reason=None, match_count=1)

    def transport_fields(self, customer_uuid: str) -> CustomerCard | None:
        """The matched card's phone and first name, for the request body only.

        Returns the card as the EasyWeek export states it — never Altegio's
        spelling of the same person. The two systems disagree about names more
        often than not, and the one that must not be overwritten is the one
        holding the imported visit history.
        """
        card = self.cards.get(customer_uuid)
        return card if card is not None and card.addressable else None

    def as_safe_dict(self) -> dict[str, Any]:
        """Counts only. A phone number never leaves this object."""
        ambiguous = sum(1 for uuids in self.by_phone.values() if len(uuids) > 1)
        return {
            "valid": self.valid,
            "reason": self.reason,
            "distinct_phones": len(self.by_phone),
            "customer_rows": self.size,
            "ambiguous_phones": ambiguous,
            "rows_without_usable_phone": self.unusable_rows,
            # A count, so an operator can see "half the export cannot be booked
            # against" without a single name being printed.
            "rows_without_first_name": sum(1 for card in self.cards.values() if not card.addressable),
        }


def _invalid(reason: str) -> CustomerDirectory:
    return CustomerDirectory(valid=False, reason=reason)


def _first_name(raw: object) -> str | None:
    """The given name as written, trimmed. Never derived, never split."""
    if not isinstance(raw, str):
        return None
    text = " ".join(raw.split())
    return text or None


def _index(rows: list[tuple[object, object, object]]) -> CustomerDirectory:
    """Build the index from ``(uuid, phone, first_name)`` triples.

    Duplicate ``(phone, uuid)`` pairs collapse — the same customer listed twice in
    an export is one customer. Two *different* uuids on one phone are both kept,
    so ``resolve`` can see the ambiguity instead of picking whichever came first.

    A row with no usable first name is still indexed. It has to be: dropping it
    would turn "this customer cannot be addressed" into "this customer is not in
    EasyWeek", and those call for opposite operator actions.
    """
    by_phone: dict[str, list[str]] = defaultdict(list)
    cards: dict[str, CustomerCard] = {}
    unusable = 0

    for raw_uuid, raw_phone, raw_first_name in rows:
        customer_uuid = canonical_uuid(raw_uuid)
        phone = normalized_international_phone(raw_phone)
        if customer_uuid is None or phone is None:
            unusable += 1
            continue
        if customer_uuid not in by_phone[phone]:
            by_phone[phone].append(customer_uuid)
        cards.setdefault(
            customer_uuid,
            CustomerCard(uuid=customer_uuid, phone=phone, first_name=_first_name(raw_first_name)),
        )

    if not by_phone:
        return CustomerDirectory(valid=False, reason=DIRECTORY_EMPTY, unusable_rows=unusable)
    return CustomerDirectory(valid=True, by_phone=dict(by_phone), cards=cards, unusable_rows=unusable)


def _pick_column(fieldnames: list[str], candidates: tuple[str, ...]) -> str | None:
    lowered = {name.strip().lower(): name for name in fieldnames if isinstance(name, str)}
    for candidate in candidates:
        if candidate in lowered:
            return lowered[candidate]
    return None


def load_customer_directory(path: str | Path) -> CustomerDirectory:
    """Load an EasyWeek customer export from ``.csv`` or ``.json``.

    XLSX is not read here on purpose: it would add a dependency, and an operator
    exporting a spreadsheet can save it as CSV in one step. The runbook says so.
    """
    file_path = Path(path)
    try:
        raw = file_path.read_text(encoding="utf-8-sig")
    except OSError:
        return _invalid(DIRECTORY_NOT_READABLE)
    except UnicodeDecodeError:
        return _invalid(DIRECTORY_SHAPE_INVALID)

    if file_path.suffix.lower() == ".json":
        return _load_json(raw)
    return _load_csv(raw)


def _load_json(raw: str) -> CustomerDirectory:
    try:
        parsed = json.loads(raw)
    except Exception:
        return _invalid(DIRECTORY_SHAPE_INVALID)

    if isinstance(parsed, dict):
        parsed = parsed.get("data")
    if not isinstance(parsed, list):
        return _invalid(DIRECTORY_SHAPE_INVALID)

    rows: list[tuple[object, object, object]] = []
    for entry in parsed:
        if not isinstance(entry, dict):
            return _invalid(DIRECTORY_SHAPE_INVALID)

        def _pick(keys: tuple[str, ...], row: dict[str, Any] = entry) -> object:
            for key in keys:
                if key in row:
                    return row[key]
            return None

        rows.append((_pick(_UUID_COLUMNS), _pick(_PHONE_COLUMNS), _pick(_FIRST_NAME_COLUMNS)))
    return _index(rows)


def _load_csv(raw: str) -> CustomerDirectory:
    reader = csv.DictReader(raw.splitlines())
    fieldnames = list(reader.fieldnames or [])
    if not fieldnames:
        return _invalid(DIRECTORY_SHAPE_INVALID)

    uuid_column = _pick_column(fieldnames, _UUID_COLUMNS)
    phone_column = _pick_column(fieldnames, _PHONE_COLUMNS)
    if uuid_column is None or phone_column is None:
        return _invalid(DIRECTORY_SHAPE_INVALID)
    # Optional at load time so an `inventory` run on a half-built export still
    # works; a missing first name blocks per row, at resolve time, where the
    # operator can see which customers it affects.
    name_column = _pick_column(fieldnames, _FIRST_NAME_COLUMNS)

    rows: list[tuple[object, object, object]] = [
        (row.get(uuid_column), row.get(phone_column), row.get(name_column) if name_column else None) for row in reader
    ]
    return _index(rows)
