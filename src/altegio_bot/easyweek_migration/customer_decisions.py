"""The durable record of what an operator agreed to, and what happened next.

The preparation stage is two runs, not one. The read-only run works out which
customers are missing and writes down what it would ask; a person looks at that
list; a second, separately-permissioned run creates exactly the customers that
were confirmed. Between those runs the process exits — in Docker it may not even
have a terminal — so the agreement has to survive on disk or it does not exist.

Three properties this file is responsible for
---------------------------------------------
**A confirmation binds to what was shown.** Every record carries a digest of the
exact fields the operator saw. If the source data moved between the review and
the creation, the digest stops matching and the record refuses to be acted on.
"Yes" was said about a name and a number, not about a slot in a list.

**A restart never creates a customer twice.** ``POST /customers`` has no
idempotency key, so the marker goes down *before* the request goes out and is
fsynced. A process that dies mid-request leaves an ``in_flight`` record, which on
the next run is INDETERMINATE — reconciled by reading the workspace, never by
posting again.

**Two runs cannot race.** A lock file guards the state directory. It is not
advisory politeness: two concurrent creation runs holding the same decision list
is the one way left to produce a duplicate card for a confirmed customer.

The file holds names, phone numbers and e-mail addresses. It is written 0600, it
lives outside the repository, and it is never committed.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Final

from altegio_bot.easyweek_migration.customer_api import phone_fingerprint

STORE_VERSION: Final = 1
STORE_MODE: Final = 0o600
DIR_MODE: Final = 0o700

# What a record can be. Only CONFIRMED is a licence to post, and only once.
STATE_PENDING: Final = "pending"
STATE_CONFIRMED: Final = "confirmed"
STATE_SKIPPED: Final = "skipped"
STATE_IN_FLIGHT: Final = "in_flight"
STATE_CREATED: Final = "created"
STATE_BLOCKED: Final = "blocked"

_TERMINAL: Final = frozenset({STATE_CREATED, STATE_SKIPPED})

# Refusal codes, safe to print.
DECISION_STALE: Final = "decision_stale"
DECISION_NOT_CONFIRMED: Final = "decision_not_confirmed"
DECISION_INDETERMINATE: Final = "decision_indeterminate"

_LOCK_NAME: Final = "prepare.lock"
_STATE_NAME: Final = "customer_decisions.json"


class DecisionStoreError(Exception):
    """The store cannot be used. Never a reason to proceed without it."""


class DecisionStoreLocked(DecisionStoreError):
    """Another run holds the state directory."""


def _digest(payload: dict[str, Any]) -> str:
    """A digest of exactly the fields an operator was shown.

    Sorted keys and a compact separator so the same decision digests the same on
    any machine; the digest is what makes "yes" specific to this data.
    """
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class CustomerDecision:
    """One customer the stage may be asked to create.

    ``phone`` is already normalised — the record is keyed by it, and a record
    whose key could drift is a record that can be created twice.
    """

    phone: str
    first_name: str | None
    last_name: str | None
    email: str | None
    linked_record_count: int
    source_label: str
    state: str = STATE_PENDING
    customer_uuid: str | None = None
    shown_digest: str = ""
    blocked_reason: str | None = None
    attempt_id: str | None = None
    updated_at: float = 0.0

    @property
    def key(self) -> str:
        return self.phone

    @property
    def creatable(self) -> bool:
        """``POST`` is permitted only from a confirmed record with a real name."""
        return self.state == STATE_CONFIRMED and bool(self.phone) and bool(self.first_name)

    def presentation(self) -> dict[str, Any]:
        """The fields a confirmation is *about*.

        Deliberately excludes state and bookkeeping: re-running the read-only
        pass must not invalidate a confirmation, but a changed name, number,
        e-mail or record count must.
        """
        return {
            "phone": self.phone,
            "first_name": self.first_name,
            "last_name": self.last_name,
            "email": self.email,
            "linked_record_count": self.linked_record_count,
        }

    def with_digest(self) -> CustomerDecision:
        return replace(self, shown_digest=_digest(self.presentation()))

    def matches_shown(self) -> bool:
        return bool(self.shown_digest) and self.shown_digest == _digest(self.presentation())

    def as_safe_dict(self) -> dict[str, Any]:
        """Log/report shape: a fingerprint and a state, never a person."""
        return {
            "phone": phone_fingerprint(self.phone) if self.phone else None,
            "state": self.state,
            "customer_uuid": self.customer_uuid,
            "linked_record_count": self.linked_record_count,
            "blocked_reason": self.blocked_reason,
            "has_first_name": bool(self.first_name),
            "has_email": bool(self.email),
        }

    def to_json(self) -> dict[str, Any]:
        return {
            "phone": self.phone,
            "first_name": self.first_name,
            "last_name": self.last_name,
            "email": self.email,
            "linked_record_count": self.linked_record_count,
            "source_label": self.source_label,
            "state": self.state,
            "customer_uuid": self.customer_uuid,
            "shown_digest": self.shown_digest,
            "blocked_reason": self.blocked_reason,
            "attempt_id": self.attempt_id,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_json(cls, payload: object) -> CustomerDecision:
        if not isinstance(payload, dict):
            raise DecisionStoreError("decision record is not an object")
        phone = payload.get("phone")
        if not isinstance(phone, str) or not phone:
            raise DecisionStoreError("decision record has no phone key")
        state = payload.get("state")
        if state not in {
            STATE_PENDING,
            STATE_CONFIRMED,
            STATE_SKIPPED,
            STATE_IN_FLIGHT,
            STATE_CREATED,
            STATE_BLOCKED,
        }:
            raise DecisionStoreError("decision record has an unknown state")
        count = payload.get("linked_record_count")
        if isinstance(count, bool) or not isinstance(count, int) or count < 0:
            raise DecisionStoreError("decision record has an invalid linked_record_count")

        def _text(key: str) -> str | None:
            value = payload.get(key)
            return value if isinstance(value, str) and value else None

        return cls(
            phone=phone,
            first_name=_text("first_name"),
            last_name=_text("last_name"),
            email=_text("email"),
            linked_record_count=count,
            source_label=_text("source_label") or "",
            state=state,
            customer_uuid=_text("customer_uuid"),
            shown_digest=_text("shown_digest") or "",
            blocked_reason=_text("blocked_reason"),
            attempt_id=_text("attempt_id"),
            updated_at=float(payload.get("updated_at") or 0.0),
        )


@dataclass
class DecisionSet:
    """Every decision for one preparation run, keyed by normalised phone."""

    records: dict[str, CustomerDecision] = field(default_factory=dict)

    def __len__(self) -> int:
        return len(self.records)

    def get(self, phone: str) -> CustomerDecision | None:
        return self.records.get(phone)

    def in_state(self, *states: str) -> list[CustomerDecision]:
        wanted = set(states)
        return [record for record in self.records.values() if record.state in wanted]

    def upsert_proposal(self, proposal: CustomerDecision) -> CustomerDecision:
        """Add a proposal, or refresh one that has not been acted on yet.

        A record that already reached a terminal state is left exactly as it is —
        re-running the read-only pass must not re-open a creation, and must not
        silently re-ask about a customer who was already skipped on purpose.
        A confirmed record whose underlying data CHANGED loses its confirmation:
        the digest no longer matches what the person agreed to.
        """
        existing = self.records.get(proposal.key)
        candidate = proposal.with_digest()

        if existing is None:
            self.records[candidate.key] = candidate
            return candidate
        if existing.state in _TERMINAL or existing.state == STATE_IN_FLIGHT:
            return existing
        if existing.state == STATE_CONFIRMED:
            if existing.shown_digest == candidate.shown_digest:
                # Unchanged data must not re-ask. This is the property that
                # stopped the operator confirming the same customer per run.
                return existing
            candidate = replace(candidate, state=STATE_PENDING)
        self.records[candidate.key] = candidate
        return candidate

    def set_state(self, phone: str, state: str, **fields: Any) -> CustomerDecision:
        existing = self.records.get(phone)
        if existing is None:
            raise DecisionStoreError("no decision for that customer")
        updated = replace(existing, state=state, updated_at=time.time(), **fields)
        self.records[phone] = updated
        return updated

    def summary(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for record in self.records.values():
            counts[record.state] = counts.get(record.state, 0) + 1
        return counts


class CustomerDecisionStore:
    """The decision set plus the durability that makes it worth something."""

    def __init__(self, directory: str | Path) -> None:
        self._dir = Path(directory)
        self._path = self._dir / _STATE_NAME
        self._lock_path = self._dir / _LOCK_NAME
        self._lock_fd: int | None = None

    @property
    def path(self) -> Path:
        return self._path

    # -- locking ----------------------------------------------------------

    def acquire(self) -> None:
        """Take the directory lock, or refuse to run.

        ``O_EXCL`` rather than a "does it exist" check: the check-then-create gap
        is exactly wide enough for the second run to slip through. A stale lock
        is left for a person to remove, because the alternative — deciding a lock
        is stale after N seconds — is a duplicate-customer generator on a slow
        machine.
        """
        self._dir.mkdir(parents=True, exist_ok=True)
        os.chmod(self._dir, DIR_MODE)
        try:
            fd = os.open(self._lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, STORE_MODE)
        except FileExistsError:
            raise DecisionStoreLocked(
                f"another preparation run holds {self._lock_path}; "
                "wait for it to finish, or remove the file if you are certain it is dead"
            ) from None
        except OSError as error:
            raise DecisionStoreError(f"cannot create the lock file: {error.strerror}") from None
        os.write(fd, f"{os.getpid()}\n".encode())
        os.fsync(fd)
        self._lock_fd = fd

    def release(self) -> None:
        if self._lock_fd is not None:
            os.close(self._lock_fd)
            self._lock_fd = None
        self._lock_path.unlink(missing_ok=True)

    def __enter__(self) -> CustomerDecisionStore:
        self.acquire()
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.release()

    # -- persistence ------------------------------------------------------

    def load(self) -> DecisionSet:
        try:
            raw = self._path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return DecisionSet()
        except OSError as error:
            raise DecisionStoreError(f"cannot read the decision store: {error.strerror}") from None

        try:
            payload = json.loads(raw)
        except Exception:
            # A truncated store is not an empty store. Starting fresh here would
            # forget which customers already exist and create them again.
            raise DecisionStoreError("the decision store is not valid JSON") from None
        if not isinstance(payload, dict) or payload.get("version") != STORE_VERSION:
            raise DecisionStoreError("the decision store has an unexpected version")
        rows = payload.get("decisions")
        if not isinstance(rows, list):
            raise DecisionStoreError("the decision store has no decisions array")

        records: dict[str, CustomerDecision] = {}
        for row in rows:
            record = CustomerDecision.from_json(row)
            records[record.key] = record
        return DecisionSet(records=records)

    def save(self, decisions: DecisionSet) -> None:
        """Write atomically and durably, 0600.

        ``fsync`` on the file and then on the directory: a marker that is only in
        the page cache when the machine loses power is a marker that did not
        exist, and the record it was protecting gets created twice.
        """
        self._dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": STORE_VERSION,
            "decisions": [record.to_json() for record in sorted(decisions.records.values(), key=lambda r: r.key)],
        }
        tmp = self._path.with_suffix(".json.tmp")
        fd = os.open(tmp, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, STORE_MODE)
        try:
            os.write(fd, json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"))
            os.fsync(fd)
        finally:
            os.close(fd)
        os.replace(tmp, self._path)
        os.chmod(self._path, STORE_MODE)
        dir_fd = os.open(self._dir, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
