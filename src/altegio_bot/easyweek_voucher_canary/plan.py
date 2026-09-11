"""The read-only STAGE plan every voucher-canary mutation is authorised by (§35).

A plan is a bounded, PII-free snapshot of everything that must be true before
one particular stage may run, plus a digest of that snapshot. The digest is the
authorisation token: an operator reads the plan, an owner approves the exact
digest, and the mutation command refuses to run unless the plan it recomputes
*live, seconds before the claim* still hashes to the same value.

Why the plan is per stage
-------------------------
A single plan covering the whole canary could only ever be valid before the
first mutation. The moment ``create`` succeeds, that plan invalidates itself:
the marker order it required to be absent now exists, and the counters it
recorded may have moved. An operator would have been left holding an approval
that could never be used again, with nothing to approve a payment or a refund
with.

So each stage gets its own plan, its own digest and its own confirmation phrase,
and each asserts what is true at *that* point in the sequence:

``create``   no ledger row, no marker order, frozen configuration and readable
             counters;
``pay``      the ledger is ``created`` (or ``pay_rejected``, for a manual retry
             after a rejection that provably did not act), the target order is
             proven, exactly one marker order exists, it is still open, it is in
             the canary's own customer/template scope, and it is EXACTLY the
             one-voucher order §35 authorises — because a payment settles
             whatever the order contains;
``refund``   the ledger is ``paid`` (or ``refund_rejected``) and the target
             order reads as paid.

Every stage after the first also re-proves that the runtime identity in the
environment is still the one the ledger row was opened with. A different
customer, staffer or account of the same branch passes every other check and
would still be a real mutation on somebody else.

Four different things, kept apart
---------------------------------
``immutable_template_digest``
    the frozen product configuration — price, flags, branch and service counts.
    Changing any of it is a template edit and stops the canary.

``counters_observed``
    ``vouchers_count``/``activated_vouchers_count`` at this stage. Recorded as a
    per-stage baseline and deliberately NOT part of the authorisation digest: a
    counter that moved because a voucher was issued is the product working, not
    somebody editing the template, and treating it as an edit would make the
    refund unreachable exactly when it matters most.

``ledger_state`` / ``order_state``
    where the canary is, and what the remote order currently reads as.

``digest``
    the stage authorisation itself, over the first, third and fourth of those.

Nothing here stores or prints a runtime customer, staffer or account UUID. Those
arrive through named environment variables, are validated, are used in memory,
and survive into reports and the ledger only as salted SHA-256 fingerprints.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Final, Protocol

from altegio_bot.easyweek_client import EasyWeekError
from altegio_bot.easyweek_voucher_canary.orders import (
    CANARY_ORDER_EXTRA_ITEMS,
    CANARY_ORDER_TOTAL_UNPROVEN,
    CANARY_VOUCHER_LINE_UNPROVEN,
    ORDER_MALFORMED,
    ORDER_OPEN,
    ORDER_PAID,
    canonical_uuid,
    classify_order,
    find_marker_orders,
    order_object,
    payable_order_reasons,
    rows,
    walk_pages,
    within_window,
)
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
CANARY_MARKER_ORDER_MISSING: Final = "canary_marker_order_missing"
CANARY_MARKER_ORDER_AMBIGUOUS: Final = "canary_marker_order_ambiguous"
CANARY_ORDER_WALK_INCOMPLETE: Final = "canary_order_walk_incomplete"
CANARY_LEDGER_STATE_UNEXPECTED: Final = "canary_ledger_state_unexpected"
CANARY_TARGET_ORDER_UNPROVEN: Final = "canary_target_order_unproven"
CANARY_TARGET_ORDER_NOT_OPEN: Final = "canary_target_order_not_open"
CANARY_TARGET_ORDER_NOT_PAID: Final = "canary_target_order_not_paid"
# The runtime identity in the environment is not the one this ledger row was
# opened with. Never says WHICH — only that they differ.
CANARY_IDENTITY_BINDING_MISMATCH: Final = "canary_identity_binding_mismatch"
CANARY_API_UNAVAILABLE: Final = "canary_api_unavailable"
CANARY_API_UNCERTAIN: Final = "canary_api_uncertain"
CANARY_PLAN_EXPIRED: Final = "canary_plan_expired"
CANARY_PLAN_DIGEST_MISMATCH: Final = "canary_plan_digest_mismatch"
CANARY_CONFIRMATION_MISMATCH: Final = "canary_confirmation_mismatch"
CANARY_UNKNOWN_STAGE: Final = "canary_unknown_stage"

STAGE_CREATE: Final = "create"
STAGE_PAY: Final = "pay"
STAGE_REFUND: Final = "refund"
MUTATION_STAGES: Final = (STAGE_CREATE, STAGE_PAY, STAGE_REFUND)

# The ledger status each stage normally starts from. `None` means "no row".
STAGE_REQUIRED_LEDGER_STATUS: Final = {
    STAGE_CREATE: None,
    STAGE_PAY: "created",
    STAGE_REFUND: "paid",
}

# Every ledger status a stage may be planned FROM. Besides the normal source
# state, each stage accepts its own ``*_rejected``: a rejection this transport
# proved did not act leaves the canary exactly where it was, and the operator
# must be able to fix the cause and try once more. That retry is manual all the
# way through — a fresh plan, a fresh digest, a fresh issued_at, a fresh phrase
# and another `--apply` — and nothing here ever re-sends anything by itself.
#
# Deliberately NOT here: `*_claimed`, `*_unknown`, `ambiguous`,
# `manually_cleaned` and the completed states. Those are exactly the cases where
# a request may have acted, and a second one could duplicate it.
STAGE_SOURCE_LEDGER_STATUSES: Final = {
    STAGE_CREATE: frozenset({None, "create_rejected"}),
    STAGE_PAY: frozenset({"created", "pay_rejected"}),
    STAGE_REFUND: frozenset({"paid", "refund_rejected"}),
}

# The proofs a payable order must produce, kept in one place so the CLI, the
# tests and the runbook name the same codes.
PAY_ORDER_PROOF_REASONS: Final = (
    CANARY_VOUCHER_LINE_UNPROVEN,
    CANARY_ORDER_TOTAL_UNPROVEN,
    CANARY_ORDER_EXTRA_ITEMS,
)

_IDENTITY_ROLES: Final = ("customer", "staffer", "account")
_COUNTER_FIELDS: Final = ("vouchers_count", "activated_vouchers_count")
_MAX_LISTING_PAGES: Final = 20


class CanaryReader(Protocol):
    """The reviewed GET surface a plan is allowed to use."""

    async def get_workspace(self) -> dict[str, Any]: ...

    async def list_locations(self) -> list[dict[str, Any]]: ...

    async def list_voucher_templates(self) -> list[dict[str, Any]]: ...

    async def get_voucher_template(self, voucher_template_uuid: str) -> dict[str, Any]: ...

    async def get_customer(self, customer_uuid: str) -> dict[str, Any]: ...

    async def list_location_staffers(self, location_uuid: str, *, page: int) -> dict[str, Any]: ...

    async def list_location_accounts(self, location_uuid: str) -> Any: ...

    async def list_location_customer_orders(
        self,
        *,
        location_uuid: str,
        customer_uuid: str,
        page: int,
        per_page: int = ...,
    ) -> dict[str, Any]: ...

    async def get_order(self, order_uuid: str) -> dict[str, Any]: ...


# ---------------------------------------------------------------------------
# Runtime identity — supplied by env, never committed, never printed
# ---------------------------------------------------------------------------


def identity_fingerprint(role: str, value: str) -> str:
    """A salted, one-way digest of one runtime UUID.

    Salted with the canary scope and the role so the same UUID used in two roles
    does not produce the same fingerprint. A UUID has 122 bits of entropy, so
    unlike a voucher code or a phone number it cannot be recovered from its
    digest — which is why fingerprints are used here and refused for artifact
    values (see ``artifact``).
    """
    material = f"{VOUCHER_CANARY_SCOPE}:{role}:{value}".encode("utf-8")
    return hashlib.sha256(material).hexdigest()


def canary_marker() -> str:
    """The deterministic, non-personal comment marker for this canary scope.

    Deterministic on purpose: after a crash the reconciler has to recompute the
    exact marker it would have sent, and an operator has to be able to paste it
    into the EasyWeek dashboard search box to find an open draft.
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


def canonical_order_uuid(value: object) -> str | None:
    """A canonical lowercase order UUID, or ``None``. Never echoes the input.

    One implementation, shared with the listing matcher in ``orders``: two
    spellings of "canonical" could disagree about the same identifier, and every
    comparison downstream — ledger target, listing match, exact readback — is a
    string comparison.
    """
    return canonical_uuid(value)


def resolve_runtime_identity(
    *,
    customer_uuid: object,
    staffer_uuid: object,
    account_uuid: object,
) -> tuple[RuntimeIdentity | None, tuple[str, ...]]:
    """Validate the three runtime UUIDs without ever echoing one back.

    The reason names the ROLE that was unusable and nothing else: an error
    message carrying the value would put a production identity into a log line,
    which is exactly what the fingerprints exist to avoid.
    """
    reasons: list[str] = []
    resolved: dict[str, str] = {}
    for role, raw in (("customer", customer_uuid), ("staffer", staffer_uuid), ("account", account_uuid)):
        if not isinstance(raw, str) or not raw.strip():
            reasons.append(CANARY_RUNTIME_IDENTITY_MISSING)
            continue
        canonical = canonical_uuid(raw.strip())
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
# Template: immutable configuration, and counters that legitimately move
# ---------------------------------------------------------------------------


def _object(payload: object) -> dict[str, Any]:
    if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
        return payload["data"]
    return payload if isinstance(payload, dict) else {}


def _exact_int(value: object) -> int | None:
    return value if type(value) is int else None


def template_counters(template_payload: object) -> dict[str, int] | None:
    """Both voucher counters, or ``None`` when they are not usable evidence.

    ``None`` is never "zero": a counter we cannot read is a counter we cannot
    compare. The pair must also be internally possible — more activated vouchers
    than issued ones means we are not looking at what we think we are.
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


def immutable_template_digest(template_payload: object) -> str:
    """A digest over the frozen CONFIGURATION only — never over the counters.

    That separation is the point. A counter moves when a voucher is issued,
    which is the product working; folding it into this digest would turn every
    successful create into an apparent template edit and would strand the refund
    behind a fake drift alarm.
    """
    template = _object(template_payload)
    material: dict[str, Any] = {"uuid": template.get("uuid")}
    for name in sorted(FROZEN_TEMPLATE_FACTS):
        material[name] = template.get(name)
    return hashlib.sha256(json.dumps(material, sort_keys=True, default=str).encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# The stage plan
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class StagePlan:
    """A PII-free snapshot plus the digest that authorises ONE stage of it."""

    stage: str
    ready: bool
    reasons: tuple[str, ...]
    digest: str
    issued_at: datetime
    immutable_template_digest: str
    counters_observed: dict[str, int] | None
    ledger_state: dict[str, Any]
    order_state: str | None
    snapshot: dict[str, Any]
    marker: str
    observations: tuple[dict[str, Any], ...] = field(default=())

    @property
    def expires_at(self) -> datetime:
        return self.issued_at + PLAN_MAX_AGE

    def digest_for(self, issued_at: datetime) -> str:
        """This plan's digest AS IF it had been issued at ``issued_at``.

        The authorisation check needs this because the plan it verifies against
        is rebuilt seconds before the claim and therefore carries a new
        ``issued_at`` of its own. Recomputing with the operator's timestamp is
        what makes that timestamp part of what was signed: a digest approved for
        one moment does not authorise the same stage at another, and an operator
        cannot keep an old digest alive by pairing it with a fresh timestamp
        they typed themselves.
        """
        return _stage_digest(
            stage=self.stage,
            snapshot=self.snapshot,
            ledger_state=self.ledger_state,
            issued_at=issued_at,
        )

    def phrase_for(self, digest: str) -> str:
        return f"{self.stage}-voucher-canary-{digest[:12]}"

    @property
    def confirmation_phrase(self) -> str:
        """The exact phrase an operator must type for THIS stage of THIS plan.

        Bound to the stage digest — which is itself bound to the stage and to
        the moment the plan was issued — so a phrase cannot be prepared before
        the plan exists, reused after the workspace drifted, carried from one
        stage to the next, or revived once the approval has aged out.
        """
        return self.phrase_for(self.digest)

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "mode": "voucher_canary_stage_plan",
            "canary_scope": VOUCHER_CANARY_SCOPE,
            "request_schema_version": VOUCHER_CANARY_REQUEST_SCHEMA_VERSION,
            "stage": self.stage,
            "ready": self.ready,
            "reasons": list(self.reasons),
            "plan_digest": self.digest,
            "plan_issued_at": self.issued_at.isoformat(),
            "plan_expires_at": self.expires_at.isoformat(),
            # The frozen product configuration, separate from anything that
            # legitimately moves.
            "immutable_template_digest": self.immutable_template_digest,
            # This stage's counter baseline — an observation, not an authority.
            "counters_observed": dict(self.counters_observed) if self.counters_observed is not None else None,
            "ledger_state": dict(self.ledger_state),
            "order_state": self.order_state,
            "reconciliation_marker": self.marker,
            "snapshot": dict(self.snapshot),
            "confirmation_phrase": self.confirmation_phrase,
            "observations": [dict(entry) for entry in self.observations],
            # Repeated on every plan, ready or not.
            "campaign_send_authorized": False,
            "customer_message_sent": False,
            "ready_for_send": False,
        }


def _digest_over(material: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(material, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def _stage_digest(
    *,
    stage: str,
    snapshot: dict[str, Any],
    ledger_state: dict[str, Any],
    issued_at: datetime,
) -> str:
    """The authorisation digest of one stage, at one exact moment.

    ``issued_at`` is canonical material, not a label printed beside the digest.
    An approval is a statement about a workspace AT A MOMENT, and a digest that
    did not cover the moment could be replayed indefinitely by pairing it with
    any fresh timestamp — the age check alone cannot stop that, because the age
    check only ever sees the timestamp the operator typed.

    Microsecond resolution is deliberate: two plans built in the same second are
    two different approvals.

    The counters stay out, as they always have. A counter that moved because a
    voucher was issued is the product working, and folding it in here would make
    the refund unreachable exactly when it matters most.
    """
    return _digest_over(
        {
            "stage": stage,
            "snapshot": snapshot,
            "ledger_state": ledger_state,
            "plan_issued_at": issued_at.isoformat(),
        }
    )


async def _listed_uuids(
    fetch: Any,
    *,
    location_uuid: str,
    paginated: bool,
) -> tuple[set[str], bool]:
    """UUIDs from a location-scoped listing, plus whether the walk was complete.

    Two shapes, because the API has two: the accounts collection is served whole
    under the location, while staffers are paginated and publish a ``last_page``.
    An incomplete walk is unproven, never "not present".
    """
    if not paginated:
        payload = await fetch(location_uuid)
        found = {row["uuid"] for row in rows(payload) if isinstance(row, dict) and isinstance(row.get("uuid"), str)}
        return found, True

    async def page_fetch(page: int) -> Any:
        return await fetch(location_uuid, page=page)

    walk = await walk_pages(page_fetch, max_pages=_MAX_LISTING_PAGES)
    found = {row["uuid"] for row in walk.rows if isinstance(row, dict) and isinstance(row.get("uuid"), str)}
    return found, walk.complete


def identity_binding_matches(
    identity: RuntimeIdentity,
    stored: dict[str, str | None] | None,
) -> bool:
    """Is the environment's identity the one this ledger row was opened with?

    ``None`` — no row yet — matches: there is nothing to contradict. A row that
    exists must agree on all three roles.

    This is what stops a canary from being pointed at a different customer, a
    different staffer or a different account of the SAME branch half-way
    through. Every one of those would still pass the branch, template and
    workspace proofs, and would still be a real mutation on somebody else.
    """
    if stored is None:
        return True
    current = identity.fingerprints
    return all(stored.get(role) == current[role] for role in _IDENTITY_ROLES)


async def build_stage_plan(
    reader: CanaryReader,
    *,
    stage: str,
    identity: RuntimeIdentity,
    enabled: bool,
    ledger_status: str | None = None,
    target_order_uuid: str | None = None,
    ledger_identity: dict[str, str | None] | None = None,
    create_window_start: datetime | None = None,
    create_window_end: datetime | None = None,
    now: datetime | None = None,
) -> StagePlan:
    """Re-prove everything THIS stage depends on, using GETs only.

    Creates no ledger row, sends no mutation and writes nothing. Every failure
    is a stable reason code; a plan that is not ``ready`` still prints, because
    an operator needs to see WHICH fact is missing.
    """
    issued_at = now or utcnow()
    marker = canary_marker()
    reasons: list[str] = []
    observations: list[dict[str, Any]] = []
    snapshot: dict[str, Any] = {
        "canary_scope": VOUCHER_CANARY_SCOPE,
        "request_schema_version": VOUCHER_CANARY_REQUEST_SCHEMA_VERSION,
        "stage": stage,
        "workspace_uuid": EASYWEEK_WORKSPACE_UUID,
        "location_uuid": KARLSRUHE_LOCATION_UUID,
        "voucher_template_uuid": EASYWEEK_VOUCHER_TEMPLATE_UUID,
        "price_minor": SUPPORTED_VOUCHER_PRICE_MINOR,
        "quantity": 1,
        "reconciliation_marker": marker,
        "identity_fingerprints": identity.fingerprints,
    }
    # The runtime identity must be the one the ledger row was opened with. The
    # comparison is over fingerprints and the result is a boolean; neither the
    # stored digest nor the runtime UUID is reported by it.
    identity_bound = identity_binding_matches(identity, ledger_identity)
    ledger_state: dict[str, Any] = {
        "status": ledger_status,
        "target_order_known": target_order_uuid is not None,
        "required_status": STAGE_REQUIRED_LEDGER_STATUS.get(stage),
        "identity_binding_proven": identity_bound,
    }
    snapshot["identity_binding_proven"] = identity_bound

    if stage not in MUTATION_STAGES:
        reasons.append(CANARY_UNKNOWN_STAGE)
    if not enabled:
        reasons.append(CANARY_DISABLED_BY_ENV)
    if not identity_bound:
        reasons.append(CANARY_IDENTITY_BINDING_MISMATCH)

    counters: dict[str, int] | None = None
    config_digest = ""
    order_state: str | None = None

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
            sum(1 for row in rows(locations) if isinstance(row, dict) and row.get("uuid") == KARLSRUHE_LOCATION_UUID)
            == 1
        )
        if not location_ok:
            reasons.append(CANARY_LOCATION_UNPROVEN)
        snapshot["location_proven"] = location_ok

        templates = await reader.list_voucher_templates()
        listed_once = (
            sum(
                1
                for row in rows(templates)
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
        config_digest = immutable_template_digest(template)

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

        staffers, staffers_complete = await _listed_uuids(
            reader.list_location_staffers, location_uuid=KARLSRUHE_LOCATION_UUID, paginated=True
        )
        staffer_ok = staffers_complete and identity.staffer_uuid in staffers
        if not staffer_ok:
            reasons.append(CANARY_STAFFER_UNPROVEN)
        snapshot["staffer_proven"] = staffer_ok

        accounts, accounts_complete = await _listed_uuids(
            reader.list_location_accounts, location_uuid=KARLSRUHE_LOCATION_UUID, paginated=False
        )
        account_ok = accounts_complete and identity.account_uuid in accounts
        if not account_ok:
            reasons.append(CANARY_ACCOUNT_UNPROVEN)
        snapshot["account_proven"] = account_ok

        stage_reasons, order_state, stage_facts, stage_observations = await _stage_preconditions(
            reader,
            stage=stage,
            identity=identity,
            marker=marker,
            ledger_status=ledger_status,
            target_order_uuid=target_order_uuid,
            create_window_start=create_window_start,
            create_window_end=create_window_end,
        )
        reasons.extend(stage_reasons)
        snapshot.update(stage_facts)
        observations.extend(stage_observations)
    except EasyWeekError as exc:
        reasons.append(CANARY_API_UNCERTAIN if exc.retryable else CANARY_API_UNAVAILABLE)

    snapshot["immutable_template_digest"] = config_digest
    ledger_state["order_state"] = order_state

    # The authorisation digest covers the stage, the moment, the frozen
    # configuration, the identities, the proven prerequisites and where the
    # canary is — and deliberately NOT the counters, which legitimately move
    # when the product does its job.
    digest = _stage_digest(
        stage=stage,
        snapshot=snapshot,
        ledger_state=ledger_state,
        issued_at=issued_at,
    )
    unique = tuple(dict.fromkeys(reasons))
    return StagePlan(
        stage=stage,
        ready=not unique,
        reasons=unique,
        digest=digest,
        issued_at=issued_at,
        immutable_template_digest=config_digest,
        counters_observed=counters,
        ledger_state=ledger_state,
        order_state=order_state,
        snapshot=snapshot,
        marker=marker,
        observations=tuple(observations),
    )


async def _stage_preconditions(
    reader: CanaryReader,
    *,
    stage: str,
    identity: RuntimeIdentity,
    marker: str,
    ledger_status: str | None,
    target_order_uuid: str | None,
    create_window_start: datetime | None,
    create_window_end: datetime | None,
) -> tuple[list[str], str | None, dict[str, Any], list[dict[str, Any]]]:
    """What must additionally hold for one particular stage."""
    reasons: list[str] = []
    facts: dict[str, Any] = {}
    observations: list[dict[str, Any]] = []

    # The normal source state, or this stage's own proven-rejected state. A
    # rejection that provably did not act is the one thing an operator may fix
    # and try again — with a whole new plan, digest, issued_at, phrase and
    # `--apply`. Everything else stays unclaimable and goes to reconciliation.
    allowed = STAGE_SOURCE_LEDGER_STATUSES.get(stage, frozenset())
    if ledger_status not in allowed:
        reasons.append(CANARY_LEDGER_STATE_UNEXPECTED)
    normal_source = STAGE_REQUIRED_LEDGER_STATUS.get(stage)
    facts["retry_after_proven_rejection"] = ledger_status in allowed and ledger_status != normal_source

    if stage == STAGE_CREATE:
        # Nothing of ours may exist yet — neither a ledger row nor a marker
        # order somebody's earlier attempt left behind.
        window_start = create_window_start or (utcnow() - CREATE_WINDOW_BEFORE)
        window_end = create_window_end or (utcnow() + CREATE_WINDOW_AFTER)
        match = await find_marker_orders(
            reader,
            location_uuid=KARLSRUHE_LOCATION_UUID,
            customer_uuid=identity.customer_uuid,
            marker=marker,
            window_start=window_start,
            window_end=window_end,
        )
        facts["existing_marker_orders"] = match.count
        facts["order_walk_complete"] = match.complete
        if not match.complete:
            reasons.append(CANARY_ORDER_WALK_INCOMPLETE)
        if match.count != 0:
            reasons.append(CANARY_EXISTING_MARKER_ORDER)
        return reasons, None, facts, observations

    if target_order_uuid is None:
        reasons.append(CANARY_TARGET_ORDER_UNPROVEN)
        return reasons, None, facts, observations

    payload = await reader.get_order(target_order_uuid)
    order = order_object(payload) or {}
    state, payment_proof = classify_order(payload)
    facts["payment_proof"] = payment_proof

    from altegio_bot.easyweek_voucher_canary.artifact import observe_artifact

    observation = observe_artifact(
        payload,
        stage=f"{stage}_plan_readback",
        expected_customer_uuid=identity.customer_uuid,
        expected_template_uuid=EASYWEEK_VOUCHER_TEMPLATE_UUID,
        expected_price_minor=SUPPORTED_VOUCHER_PRICE_MINOR,
    )
    observations.append(observation.as_safe_dict())
    facts["order_customer_binding_proven"] = observation.order_customer_binding_proven
    facts["voucher_line_proven"] = observation.voucher_line_proven
    # The label travels into the stage snapshot, so the digest an owner approves
    # names WHICH proof of "one voucher" this plan was built on.
    facts["voucher_quantity_proof"] = observation.voucher_quantity_proof
    facts["individual_voucher_artifact_observed"] = observation.individual_voucher_artifact_observed

    if order.get("comment") != marker:
        reasons.append(CANARY_TARGET_ORDER_UNPROVEN)
    if state == ORDER_MALFORMED:
        reasons.append(CANARY_TARGET_ORDER_UNPROVEN)

    if stage == STAGE_PAY:
        if not observation.order_customer_binding_proven:
            reasons.append(CANARY_CUSTOMER_UNPROVEN)
        if state != ORDER_OPEN:
            reasons.append(CANARY_TARGET_ORDER_NOT_OPEN)
        # The exact readback is held to the same bounded window as a listing
        # row. The window is the only thing separating the order this canary
        # created from an older order that happens to carry the same marker,
        # and the listing is no longer the only way a target can be reached.
        window_start = create_window_start or (utcnow() - CREATE_WINDOW_BEFORE)
        window_end = create_window_end or (utcnow() + CREATE_WINDOW_AFTER)
        if not within_window(order, start=window_start, end=window_end):
            reasons.append(CANARY_TARGET_ORDER_UNPROVEN)
        # A payment settles whatever the order happens to contain, so the order
        # itself is the amount. Before one euro moves, this has to be EXACTLY
        # the order §35 authorises: one voucher line, the confirmed template,
        # price 1500 as an integer, quantity 1 as an integer, no services, no
        # goods, no second line, and a published total that agrees. An open
        # order with our marker and our customer but an empty `vouchers` list
        # and some other sum is somebody else's money.
        proof_reasons = payable_order_reasons(
            payload,
            expected_template_uuid=EASYWEEK_VOUCHER_TEMPLATE_UUID,
            expected_price_minor=SUPPORTED_VOUCHER_PRICE_MINOR,
        )
        facts["payable_order_proven"] = not proof_reasons
        reasons.extend(proof_reasons)
        # Exactly one marker order must exist, and it must be the target.
        match = await find_marker_orders(
            reader,
            location_uuid=KARLSRUHE_LOCATION_UUID,
            customer_uuid=identity.customer_uuid,
            marker=marker,
            window_start=window_start,
            window_end=window_end,
        )
        facts["existing_marker_orders"] = match.count
        facts["order_walk_complete"] = match.complete
        if not match.complete:
            reasons.append(CANARY_ORDER_WALK_INCOMPLETE)
        elif match.count == 0:
            reasons.append(CANARY_MARKER_ORDER_MISSING)
        elif match.count > 1:
            reasons.append(CANARY_MARKER_ORDER_AMBIGUOUS)
        elif match.order_uuid != target_order_uuid:
            reasons.append(CANARY_TARGET_ORDER_UNPROVEN)
        return reasons, state, facts, observations

    # STAGE_REFUND. An unreadable or surprising artifact is recorded and does
    # NOT block: an un-refunded real payment is worse than an unanswered
    # research question, and the refund only needs a proven paid order.
    if state != ORDER_PAID:
        reasons.append(CANARY_TARGET_ORDER_NOT_PAID)
    return reasons, state, facts, observations


def verify_plan_authorisation(
    plan: StagePlan,
    *,
    supplied_digest: str,
    supplied_issued_at: datetime | None,
    supplied_phrase: str,
    now: datetime | None = None,
) -> tuple[str, ...]:
    """Reasons this freshly recomputed plan does NOT authorise its stage.

    An empty tuple means the operator's digest, the operator's phrase and the
    live workspace all still agree, and the approval is not stale. Anything else
    stops the command before a claim exists.

    The digest is recomputed over the freshly rebuilt plan USING THE OPERATOR'S
    OWN ``issued_at``. That is what binds the moment into the approval: the
    timestamp is not metadata travelling next to a digest that would have been
    valid whenever, it is part of what was signed. Supply the digest from an
    old plan with a newly invented timestamp and the two no longer agree.
    """
    moment = now or utcnow()
    reasons: list[str] = list(plan.reasons)

    if supplied_issued_at is None:
        # With no timestamp there is nothing to recompute against, so the
        # digest and the phrase cannot be checked at all. Everything fails.
        return tuple(dict.fromkeys([*reasons, CANARY_PLAN_EXPIRED, CANARY_PLAN_DIGEST_MISMATCH]))

    expected_digest = plan.digest_for(supplied_issued_at)
    if supplied_digest != expected_digest:
        reasons.append(CANARY_PLAN_DIGEST_MISMATCH)
    if supplied_phrase != plan.phrase_for(expected_digest):
        reasons.append(CANARY_CONFIRMATION_MISMATCH)
    if moment - supplied_issued_at > PLAN_MAX_AGE or supplied_issued_at > moment:
        reasons.append(CANARY_PLAN_EXPIRED)
    return tuple(dict.fromkeys(reasons))
