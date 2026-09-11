"""The read-only plan every voucher-canary mutation must be authorised by (§35).

A plan is a bounded, PII-free snapshot of everything that must be true before a
real order is created, paid for or refunded, plus a digest of that snapshot. The
digest is the authorisation token: an operator reads the plan, an owner approves
the exact digest, and a mutation command refuses to run unless the plan it
recomputes *live, seconds before the claim* still hashes to the same value.

That is why the plan is never written to disk or cached. Re-deriving it is the
drift check: a template whose price moved, a staffer who left the branch, an
account that disappeared or a marker order that already exists all change the
snapshot, so they all change the digest, so they all block the mutation before
anything is claimed.

Nothing here stores or prints a runtime customer, staffer or account UUID. Those
arrive through named environment variables, are validated, are used in memory to
build a request, and survive into the report and the ledger only as salted
SHA-256 fingerprints.
"""

from __future__ import annotations

import hashlib
import json
import uuid as uuid_module
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Final, Protocol

from altegio_bot.easyweek_client import EasyWeekError
from altegio_bot.easyweek_voucher_identity import (
    EASYWEEK_VOUCHER_TEMPLATE_UUID,
    EASYWEEK_WORKSPACE_CURRENCY,
    EASYWEEK_WORKSPACE_SLUG,
    EASYWEEK_WORKSPACE_UUID,
    FROZEN_TEMPLATE_FACTS,
    KARLSRUHE_LOCATION_UUID,
    SUPPORTED_VOUCHER_PRICE_MINOR,
    VOUCHER_CANARY_REQUEST_SCHEMA_VERSION,
    VOUCHER_CANARY_SCOPE,
)
from altegio_bot.utils import utcnow

# An approved plan goes stale quickly on purpose. The digest already catches
# drift, but an operator who approved a plan an hour ago has had an hour in
# which the workspace could have been edited and edited back.
PLAN_MAX_AGE: Final = timedelta(minutes=30)

# How far back the create reconciliation is allowed to look for a marker order.
# Bounded so an unresolved create cannot quietly widen into a workspace scan.
CREATE_WINDOW_BEFORE: Final = timedelta(minutes=10)
CREATE_WINDOW_AFTER: Final = timedelta(hours=6)

# Stable, PII-free reason codes. No UUID, no name, no provider prose.
CANARY_DISABLED_BY_ENV: Final = "canary_disabled_by_env"
CANARY_RUNTIME_IDENTITY_MISSING: Final = "canary_runtime_identity_missing"
CANARY_RUNTIME_IDENTITY_INVALID: Final = "canary_runtime_identity_invalid"
CANARY_WORKSPACE_MISMATCH: Final = "canary_workspace_mismatch"
CANARY_LOCATION_UNPROVEN: Final = "canary_location_unproven"
CANARY_TEMPLATE_UNPROVEN: Final = "canary_template_unproven"
CANARY_TEMPLATE_UNFROZEN: Final = "canary_template_unfrozen"
CANARY_TEMPLATE_COUNTERS_UNUSABLE: Final = "canary_template_counters_unusable"
CANARY_CUSTOMER_UNPROVEN: Final = "canary_customer_unproven"
CANARY_STAFFER_UNPROVEN: Final = "canary_staffer_unproven"
CANARY_ACCOUNT_UNPROVEN: Final = "canary_account_unproven"
CANARY_EXISTING_MARKER_ORDER: Final = "canary_existing_marker_order"
CANARY_API_UNAVAILABLE: Final = "canary_api_unavailable"
CANARY_API_UNCERTAIN: Final = "canary_api_uncertain"
CANARY_PLAN_EXPIRED: Final = "canary_plan_expired"
CANARY_PLAN_DIGEST_MISMATCH: Final = "canary_plan_digest_mismatch"
CANARY_CONFIRMATION_MISMATCH: Final = "canary_confirmation_mismatch"

STAGE_CREATE: Final = "create"
STAGE_PAY: Final = "pay"
STAGE_REFUND: Final = "refund"
MUTATION_STAGES: Final = (STAGE_CREATE, STAGE_PAY, STAGE_REFUND)

_COUNTER_FIELDS: Final = ("vouchers_count", "activated_vouchers_count")


class CanaryReader(Protocol):
    """The reviewed GET surface a plan is allowed to use."""

    async def get_workspace(self) -> dict[str, Any]: ...

    async def list_locations(self) -> list[dict[str, Any]]: ...

    async def list_voucher_templates(self) -> list[dict[str, Any]]: ...

    async def get_voucher_template(self, voucher_template_uuid: str) -> dict[str, Any]: ...

    async def get_customer(self, customer_uuid: str) -> dict[str, Any]: ...

    async def list_location_staffers(self, location_uuid: str, *, page: int) -> dict[str, Any]: ...

    async def list_location_accounts(self, location_uuid: str, *, page: int) -> dict[str, Any]: ...

    async def list_location_orders(
        self, *, location_uuid: str, customer_uuid: str, page: int, per_page: int = ...
    ) -> dict[str, Any]: ...


# ---------------------------------------------------------------------------
# Runtime identity — supplied by env, never committed, never printed
# ---------------------------------------------------------------------------


def identity_fingerprint(role: str, value: str) -> str:
    """A salted, one-way digest of one runtime UUID.

    Salted with the canary scope and the role so the same UUID used as both a
    customer and a recipient does not produce the same fingerprint, and so a
    fingerprint from this canary cannot be matched against one from anything
    else. The UUID itself is never stored.
    """
    material = f"{VOUCHER_CANARY_SCOPE}:{role}:{value}".encode("utf-8")
    return hashlib.sha256(material).hexdigest()


def canary_marker() -> str:
    """The deterministic, non-personal comment marker for this canary scope.

    Deterministic on purpose: after a crash, the reconciler has to be able to
    recompute the exact marker it would have sent, and an operator has to be able
    to paste it into the EasyWeek dashboard search box to find an open draft.

    It carries nothing about a person and nothing free-form — a short slug
    derived from the scope constant.
    """
    return "ewvc1-" + hashlib.sha256(VOUCHER_CANARY_SCOPE.encode("utf-8")).hexdigest()[:12]


@dataclass(frozen=True)
class RuntimeIdentity:
    """The three operator-supplied UUIDs, in memory only."""

    customer_uuid: str
    staffer_uuid: str
    account_uuid: str

    @property
    def fingerprints(self) -> dict[str, str]:
        return {
            "customer": identity_fingerprint("customer", self.customer_uuid),
            "staffer": identity_fingerprint("staffer", self.staffer_uuid),
            "account": identity_fingerprint("account", self.account_uuid),
        }


def _canonical_uuid(value: object) -> str | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        canonical = str(uuid_module.UUID(value))
    except (ValueError, AttributeError, TypeError):
        return None
    return canonical if canonical == value else None


def resolve_runtime_identity(
    *,
    customer_uuid: object,
    staffer_uuid: object,
    account_uuid: object,
) -> tuple[RuntimeIdentity | None, tuple[str, ...]]:
    """Validate the three runtime UUIDs without ever echoing one back.

    Returns ``(None, reasons)`` when any of them is absent or not a canonical
    lowercase UUID. The reason names the ROLE that was unusable and nothing else:
    an error message carrying the value would put a production identity into a
    log line, which is exactly what the fingerprints exist to avoid.
    """
    reasons: list[str] = []
    resolved: dict[str, str] = {}
    for role, raw in (("customer", customer_uuid), ("staffer", staffer_uuid), ("account", account_uuid)):
        if not isinstance(raw, str) or not raw.strip():
            reasons.append(CANARY_RUNTIME_IDENTITY_MISSING)
            continue
        canonical = _canonical_uuid(raw.strip())
        if canonical is None:
            reasons.append(CANARY_RUNTIME_IDENTITY_INVALID)
            continue
        resolved[role] = canonical

    unique = tuple(dict.fromkeys(reasons))
    if unique or len(resolved) != 3:
        return None, unique or (CANARY_RUNTIME_IDENTITY_MISSING,)
    return RuntimeIdentity(
        customer_uuid=resolved["customer"],
        staffer_uuid=resolved["staffer"],
        account_uuid=resolved["account"],
    ), ()


# ---------------------------------------------------------------------------
# Template freeze
# ---------------------------------------------------------------------------


def _object(payload: object) -> dict[str, Any]:
    if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
        return payload["data"]
    return payload if isinstance(payload, dict) else {}


def _exact_int(value: object) -> int | None:
    return value if type(value) is int else None


def _rows(payload: object) -> list[Any]:
    rows = payload.get("data") if isinstance(payload, dict) else payload
    return rows if isinstance(rows, list) else []


def template_counters(template_payload: object) -> dict[str, int] | None:
    """Both voucher counters, or ``None`` when they are not usable evidence.

    ``None`` is never "zero": a counter we cannot read is a counter we cannot
    compare after a mutation. The pair must also be internally possible — more
    activated vouchers than issued ones means we are not looking at what we think
    we are looking at.
    """
    template = _object(template_payload)
    counters: dict[str, int] = {}
    for name in _COUNTER_FIELDS:
        exact = _exact_int(template.get(name))
        if exact is None or exact < 0:
            return None
        counters[name] = exact
    if counters["activated_vouchers_count"] > counters["vouchers_count"]:
        return None
    return counters


def frozen_template_mismatches(template_payload: object) -> tuple[str, ...]:
    """Field names whose value is not the frozen fact the owner approved.

    Only NAMES are returned. A mismatch could be an operator editing the product
    mid-canary, and the observed value belongs in the EasyWeek UI, not in a
    report that gets pasted into a ticket.
    """
    template = _object(template_payload)
    mismatched: list[str] = []
    if template.get("uuid") != EASYWEEK_VOUCHER_TEMPLATE_UUID:
        mismatched.append("uuid")
    for name, expected in FROZEN_TEMPLATE_FACTS.items():
        observed = template.get(name)
        if expected is None:
            if observed is not None:
                mismatched.append(name)
            continue
        if isinstance(expected, bool):
            if observed is not expected:
                mismatched.append(name)
            continue
        if type(observed) is not int or observed != expected:
            mismatched.append(name)
    return tuple(mismatched)


def template_snapshot_digest(template_payload: object) -> str:
    """A digest over the frozen facts plus the counters, and nothing else.

    Deliberately narrow: a digest over the whole payload would change whenever
    EasyWeek added a cosmetic field and would block a canary for no reason, while
    a digest over the identity alone would miss a price edit.
    """
    template = _object(template_payload)
    material: dict[str, Any] = {"uuid": template.get("uuid")}
    for name in sorted(FROZEN_TEMPLATE_FACTS):
        material[name] = template.get(name)
    for name in _COUNTER_FIELDS:
        material[name] = template.get(name)
    return hashlib.sha256(json.dumps(material, sort_keys=True, default=str).encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# The plan
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CanaryPlan:
    """A PII-free snapshot plus the digest that authorises acting on it."""

    ready: bool
    reasons: tuple[str, ...]
    digest: str
    issued_at: datetime
    snapshot: dict[str, Any]
    template_snapshot_digest: str
    counters: dict[str, int] | None
    marker: str

    @property
    def expires_at(self) -> datetime:
        return self.issued_at + PLAN_MAX_AGE

    def confirmation_phrase(self, stage: str) -> str:
        """The exact phrase an operator must type for one stage of this plan.

        Bound to the digest, so a phrase cannot be prepared before the plan
        exists, reused after the workspace drifted, or copied from one stage to
        another.
        """
        if stage not in MUTATION_STAGES:
            raise ValueError("unknown canary stage")
        return f"{stage}-voucher-canary-{self.digest[:12]}"

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "mode": "voucher_canary_plan",
            "canary_scope": VOUCHER_CANARY_SCOPE,
            "request_schema_version": VOUCHER_CANARY_REQUEST_SCHEMA_VERSION,
            "ready": self.ready,
            "reasons": list(self.reasons),
            "plan_digest": self.digest,
            "plan_issued_at": self.issued_at.isoformat(),
            "plan_expires_at": self.expires_at.isoformat(),
            "template_snapshot_digest": self.template_snapshot_digest,
            "template_counters": dict(self.counters) if self.counters is not None else None,
            "reconciliation_marker": self.marker,
            "snapshot": dict(self.snapshot),
            "confirmation_phrases": {stage: self.confirmation_phrase(stage) for stage in MUTATION_STAGES},
            # Repeated on every plan, ready or not.
            "campaign_send_authorized": False,
            "customer_message_sent": False,
            "ready_for_send": False,
        }


def _digest_over(snapshot: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(snapshot, sort_keys=True, default=str).encode("utf-8")).hexdigest()


async def _one_page_uuids(
    fetch: Any,
    *,
    location_uuid: str,
    max_pages: int = 20,
) -> tuple[set[str], bool]:
    """Walk a bounded, strictly paginated location listing into a UUID set.

    Returns ``(uuids, complete)``. ``complete`` is false when the walk hit its
    page ceiling, and a caller must treat an incomplete listing as unproven
    rather than as "not present".
    """
    seen: set[str] = set()
    for page in range(1, max_pages + 1):
        payload = await fetch(location_uuid, page=page)
        rows = _rows(payload)
        for row in rows:
            if isinstance(row, dict) and isinstance(row.get("uuid"), str):
                seen.add(row["uuid"])
        if not rows:
            return seen, True
    return seen, False


async def build_plan(
    reader: CanaryReader,
    *,
    identity: RuntimeIdentity,
    enabled: bool,
    now: datetime | None = None,
) -> CanaryPlan:
    """Re-prove everything a mutation depends on, using GETs only.

    Creates no ledger row, sends no mutation and writes nothing. Every failure
    is a stable reason code; a plan that is not ``ready`` still prints, because
    an operator needs to see WHICH fact is missing.
    """
    issued_at = now or utcnow()
    marker = canary_marker()
    reasons: list[str] = []
    snapshot: dict[str, Any] = {
        "canary_scope": VOUCHER_CANARY_SCOPE,
        "request_schema_version": VOUCHER_CANARY_REQUEST_SCHEMA_VERSION,
        "workspace_uuid": EASYWEEK_WORKSPACE_UUID,
        "location_uuid": KARLSRUHE_LOCATION_UUID,
        "voucher_template_uuid": EASYWEEK_VOUCHER_TEMPLATE_UUID,
        "price_minor": SUPPORTED_VOUCHER_PRICE_MINOR,
        "quantity": 1,
        "reconciliation_marker": marker,
        "identity_fingerprints": identity.fingerprints,
    }

    if not enabled:
        reasons.append(CANARY_DISABLED_BY_ENV)

    counters: dict[str, int] | None = None
    template_digest = ""
    try:
        workspace = _object(await reader.get_workspace())
        workspace_ok = (
            workspace.get("uuid") == EASYWEEK_WORKSPACE_UUID
            and workspace.get("slug") == EASYWEEK_WORKSPACE_SLUG
            and workspace.get("currency") == EASYWEEK_WORKSPACE_CURRENCY
        )
        if not workspace_ok:
            reasons.append(CANARY_WORKSPACE_MISMATCH)
        snapshot["workspace_proven"] = workspace_ok

        locations = await reader.list_locations()
        location_ok = (
            sum(1 for row in _rows(locations) if isinstance(row, dict) and row.get("uuid") == KARLSRUHE_LOCATION_UUID)
            == 1
        )
        if not location_ok:
            reasons.append(CANARY_LOCATION_UNPROVEN)
        snapshot["location_proven"] = location_ok

        templates = await reader.list_voucher_templates()
        listed_once = (
            sum(
                1
                for row in _rows(templates)
                if isinstance(row, dict) and row.get("uuid") == EASYWEEK_VOUCHER_TEMPLATE_UUID
            )
            == 1
        )
        template = _object(await reader.get_voucher_template(EASYWEEK_VOUCHER_TEMPLATE_UUID)) if listed_once else {}
        if not listed_once:
            reasons.append(CANARY_TEMPLATE_UNPROVEN)
        snapshot["template_listed_once"] = listed_once

        mismatches = frozen_template_mismatches(template) if listed_once else ("uuid",)
        if mismatches:
            reasons.append(CANARY_TEMPLATE_UNFROZEN)
        snapshot["template_frozen"] = not mismatches
        snapshot["template_unfrozen_fields"] = list(mismatches)

        counters = template_counters(template) if listed_once else None
        if counters is None:
            reasons.append(CANARY_TEMPLATE_COUNTERS_UNUSABLE)
        template_digest = template_snapshot_digest(template)

        customer = _object(await reader.get_customer(identity.customer_uuid))
        customer_ok = customer.get("uuid") == identity.customer_uuid
        # Only the PRESENCE of contactable fields is recorded. The canary needs
        # to know the test customer is a complete card; nothing here keeps a
        # name, a phone number or an address.
        contact_present = {
            name: bool(isinstance(customer.get(name), str) and customer.get(name, "").strip())
            for name in ("first_name", "last_name", "email", "phone")
        }
        if not customer_ok or not all(contact_present.values()):
            reasons.append(CANARY_CUSTOMER_UNPROVEN)
        snapshot["customer_proven"] = customer_ok
        snapshot["customer_contact_fields_present"] = contact_present

        staffers, staffers_complete = await _one_page_uuids(
            reader.list_location_staffers, location_uuid=KARLSRUHE_LOCATION_UUID
        )
        staffer_ok = staffers_complete and identity.staffer_uuid in staffers
        if not staffer_ok:
            reasons.append(CANARY_STAFFER_UNPROVEN)
        snapshot["staffer_proven"] = staffer_ok

        accounts, accounts_complete = await _one_page_uuids(
            reader.list_location_accounts, location_uuid=KARLSRUHE_LOCATION_UUID
        )
        account_ok = accounts_complete and identity.account_uuid in accounts
        if not account_ok:
            reasons.append(CANARY_ACCOUNT_UNPROVEN)
        snapshot["account_proven"] = account_ok

        existing = await _marker_order_count(reader, identity=identity, marker=marker)
        if existing != 0:
            reasons.append(CANARY_EXISTING_MARKER_ORDER)
        snapshot["existing_marker_orders"] = existing
    except EasyWeekError as exc:
        reasons.append(CANARY_API_UNCERTAIN if exc.retryable else CANARY_API_UNAVAILABLE)

    snapshot["template_snapshot_digest"] = template_digest
    snapshot["template_counters"] = dict(counters) if counters is not None else None

    unique = tuple(dict.fromkeys(reasons))
    return CanaryPlan(
        ready=not unique,
        reasons=unique,
        digest=_digest_over(snapshot),
        issued_at=issued_at,
        snapshot=snapshot,
        template_snapshot_digest=template_digest,
        counters=counters,
        marker=marker,
    )


async def _marker_order_count(
    reader: CanaryReader,
    *,
    identity: RuntimeIdentity,
    marker: str,
    max_pages: int = 50,
) -> int:
    """How many of this customer's orders in this branch already carry the marker.

    A strict, complete walk. An incomplete walk returns a sentinel that is not
    zero, because "we did not finish looking" must never read as "there is
    nothing there".
    """
    found = 0
    for page in range(1, max_pages + 1):
        payload = await reader.list_location_orders(
            location_uuid=KARLSRUHE_LOCATION_UUID,
            customer_uuid=identity.customer_uuid,
            page=page,
        )
        rows = _rows(payload)
        for row in rows:
            if isinstance(row, dict) and row.get("comment") == marker:
                found += 1
        if not rows:
            return found
    # Ceiling hit: report at least one so the plan refuses rather than proceeds.
    return found or 1


def verify_plan_authorisation(
    plan: CanaryPlan,
    *,
    stage: str,
    supplied_digest: str,
    supplied_issued_at: datetime | None,
    supplied_phrase: str,
    now: datetime | None = None,
) -> tuple[str, ...]:
    """Reasons this freshly recomputed plan does NOT authorise *stage*.

    An empty tuple means the operator's digest, the operator's phrase and the
    live workspace all still agree, and the approval is not stale. Anything else
    stops the command before a claim exists.
    """
    moment = now or utcnow()
    reasons: list[str] = list(plan.reasons)
    if supplied_digest != plan.digest:
        reasons.append(CANARY_PLAN_DIGEST_MISMATCH)
    if supplied_phrase != plan.confirmation_phrase(stage):
        reasons.append(CANARY_CONFIRMATION_MISMATCH)
    if supplied_issued_at is None or moment - supplied_issued_at > PLAN_MAX_AGE or supplied_issued_at > moment:
        reasons.append(CANARY_PLAN_EXPIRED)
    return tuple(dict.fromkeys(reasons))
