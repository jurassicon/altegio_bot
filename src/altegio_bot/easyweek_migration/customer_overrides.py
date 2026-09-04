"""Operator corrections that survive the next rebuild (plan §30.9).

The defect this exists for
--------------------------
An Altegio card that carries only a full name blocks with
``source_name_not_split`` — the stage refuses to guess where "Anna Maria
Schmidt" divides. The operator supplies the split with ``--correct-customer``,
the record goes back to pending, and everything looks right.

Then the next command runs. Every command rebuilds its proposals from live data,
and :meth:`DecisionSet.upsert_proposal` replaces a pending record with the fresh
one — which is derived from the same source that still has no first name. The
correction is gone, the record is blocked again, and it can never be confirmed.
The operator can retype it forever and it will be discarded forever.

A correction is not a proposal, so it must not be stored as one. It is a
separate, durable fact: *a person looked at this customer and told us what the
source could not*. It lives in its own file, it is applied ON TOP of each fresh
rebuild, and the rebuilt proposal is then digested from the corrected values —
which is what makes the corrected proposal reviewable and confirmable.

What a correction is bound to
-----------------------------
Two things, and neither of them is a name:

* the **proven source identity** — the normalised phone, the Altegio customer
  ids behind it, and the bookings it links. A correction is evidence about one
  person as the source describes them, and if that description moves, the
  evidence no longer obviously applies.
* the **proposal it was entered against**, so an operator can be shown what they
  were correcting.

Matching on a name is deliberately impossible here. Two people called
"A. Müller" in one salon is not a corner case, and writing one person's
corrected surname onto the other's card is a mistake nobody would catch.

When the identity moves, the correction is not silently applied and not silently
dropped: it becomes STALE, the customer blocks with a reason that says so, and a
person looks again. Fail-closed, like everything else in this stage.
"""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Final

from altegio_bot.easyweek_migration.customer_api import phone_fingerprint

STORE_VERSION: Final = 1
FILE_MODE: Final = 0o600
DIR_MODE: Final = 0o700
_FILE_NAME: Final = "customer_overrides.json"

# Why a correction could not be applied. Stable codes, safe to print.
OVERRIDE_STALE_IDENTITY: Final = "correction_source_identity_changed"


class CustomerOverrideError(Exception):
    """The override store cannot be used. Never a reason to proceed without it."""


def source_identity_digest(
    *,
    phone: str,
    source_client_ids: object,
    record_ids: object,
) -> str:
    """Digest of the evidence a correction was made about.

    The phone is the key; the Altegio customer ids and the linked booking ids are
    what makes it *this* person's card rather than whoever holds the number now.
    A number reassigned to somebody else, a second Altegio customer appearing on
    it, or a completely different set of bookings all change this digest — and a
    correction bound to the old one stops applying.

    Sorted, so the digest follows the data and not the order the API returned it.
    """
    material = json.dumps(
        {
            "phone": phone,
            "source_client_ids": sorted(str(item) for item in (source_client_ids or ())),
            "record_ids": sorted(int(item) for item in (record_ids or ())),
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class CustomerOverride:
    """One correction a person entered, and what it was entered against.

    Holds PII — a corrected name, possibly an e-mail — so this object never
    reaches a machine report or an ordinary log line. :meth:`as_safe_dict` is
    what those get.
    """

    phone: str
    first_name: str | None = None
    last_name: str | None = None
    email: str | None = None
    # The source evidence at the moment of correction.
    identity_digest: str = ""
    # The proposal digest the operator was looking at when they corrected it.
    # Kept so the review can say what the correction was a correction OF.
    base_review_digest: str = ""

    def applies_to(self, identity: str) -> bool:
        """Only to the exact source identity it was entered against."""
        return bool(self.identity_digest) and self.identity_digest == identity

    def as_safe_dict(self) -> dict[str, Any]:
        """A fingerprint and which fields were set. Never a name or a number."""
        return {
            "phone": phone_fingerprint(self.phone) if self.phone else None,
            "sets_first_name": bool(self.first_name),
            "sets_last_name": bool(self.last_name),
            "sets_email": bool(self.email),
            "identity_digest": self.identity_digest[:12],
            "base_review_digest": self.base_review_digest[:12],
        }

    def to_json(self) -> dict[str, Any]:
        return {
            "phone": self.phone,
            "first_name": self.first_name,
            "last_name": self.last_name,
            "email": self.email,
            "identity_digest": self.identity_digest,
            "base_review_digest": self.base_review_digest,
        }

    @classmethod
    def from_json(cls, payload: object) -> CustomerOverride:
        if not isinstance(payload, dict):
            raise CustomerOverrideError("override record is not an object")
        phone = payload.get("phone")
        if not isinstance(phone, str) or not phone:
            raise CustomerOverrideError("override record has no phone key")
        identity = payload.get("identity_digest")
        if not isinstance(identity, str) or not identity:
            # An override with nothing to bind it to could be applied to any
            # future version of this customer. Refuse rather than guess.
            raise CustomerOverrideError("override record has no source identity digest")

        def _text(key: str) -> str | None:
            value = payload.get(key)
            return value if isinstance(value, str) and value else None

        return cls(
            phone=phone,
            first_name=_text("first_name"),
            last_name=_text("last_name"),
            email=_text("email"),
            identity_digest=identity,
            base_review_digest=_text("base_review_digest") or "",
        )


@dataclass
class OverrideSet:
    """Every correction for one preparation run, keyed by normalised phone."""

    records: dict[str, CustomerOverride]

    def __init__(self, records: dict[str, CustomerOverride] | None = None) -> None:
        self.records = dict(records or {})

    def __len__(self) -> int:
        return len(self.records)

    def get(self, phone: str) -> CustomerOverride | None:
        return self.records.get(phone)

    def put(self, override: CustomerOverride) -> None:
        self.records[override.phone] = override

    def merged(
        self, phone: str, *, first_name: str | None, last_name: str | None, email: str | None
    ) -> CustomerOverride:
        """Fold a new correction into whatever was already corrected.

        A second correction that only fixes the surname must not erase the first
        name the operator supplied an hour earlier.
        """
        existing = self.records.get(phone)
        if existing is None:
            return CustomerOverride(phone=phone, first_name=first_name, last_name=last_name, email=email)
        return replace(
            existing,
            first_name=first_name or existing.first_name,
            last_name=last_name or existing.last_name,
            email=email or existing.email,
        )

    def as_safe_list(self) -> list[dict[str, Any]]:
        return [record.as_safe_dict() for _phone, record in sorted(self.records.items())]


class CustomerOverrideStore:
    """The corrections on disk: 0600, atomic, fsynced, and versioned.

    A separate file from the decision store on purpose. The decision store holds
    terminal and in-flight states that a schema change must never endanger, so a
    new kind of durable fact gets a new file rather than a new version of that
    one. Nothing here can lose a ``created`` or ``in_flight`` decision, because
    nothing here writes to that file at all.
    """

    def __init__(self, directory: str | Path) -> None:
        self._dir = Path(directory)
        self._path = self._dir / _FILE_NAME

    @property
    def path(self) -> Path:
        return self._path

    def load(self) -> OverrideSet:
        try:
            raw = self._path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return OverrideSet()
        except OSError as error:
            raise CustomerOverrideError(f"cannot read the override store: {error.strerror}") from None

        try:
            payload = json.loads(raw)
        except Exception:
            # A truncated file is not an empty one. Reading it as empty would
            # silently discard corrections a person typed.
            raise CustomerOverrideError("the override store is not valid JSON") from None
        if not isinstance(payload, dict):
            raise CustomerOverrideError("the override store is not an object")
        version = payload.get("version")
        if version != STORE_VERSION:
            # Forward-compatible refusal: a newer file is not silently ignored,
            # because ignoring it would drop corrections and re-block customers.
            raise CustomerOverrideError(f"the override store has an unsupported version ({version!r})")
        rows = payload.get("overrides")
        if not isinstance(rows, list):
            raise CustomerOverrideError("the override store has no overrides array")

        records: dict[str, CustomerOverride] = {}
        for row in rows:
            record = CustomerOverride.from_json(row)
            records[record.phone] = record
        return OverrideSet(records)

    def save(self, overrides: OverrideSet) -> None:
        self._dir.mkdir(parents=True, exist_ok=True)
        os.chmod(self._dir, DIR_MODE)
        payload = {
            "version": STORE_VERSION,
            "overrides": [record.to_json() for _phone, record in sorted(overrides.records.items())],
        }
        tmp = self._path.with_suffix(".json.tmp")
        fd = os.open(tmp, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, FILE_MODE)
        try:
            os.write(fd, json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"))
            os.fsync(fd)
        finally:
            os.close(fd)
        os.replace(tmp, self._path)
        os.chmod(self._path, FILE_MODE)
        dir_fd = os.open(self._dir, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
