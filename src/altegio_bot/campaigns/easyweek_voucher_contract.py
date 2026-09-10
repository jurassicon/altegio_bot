"""Fail-closed evidence for the EasyWeek non-persistent voucher calculation.

Three different things are kept apart here, because production evidence proves
only the first one and conflating them is how a preview becomes a send:

``calculation_contract_ready``
    One fully proven, non-persistent ``POST /orders/calculate`` for the exact
    Karlsruhe location, the exact voucher template and the exact price the
    template itself reports — with the whole normative template state, counters
    included, re-read and unchanged around it.

``issue_contract_ready``
    That a voucher may be created, paid for, refunded or reconciled. Nothing
    proves this. It is a constant ``False``.

``delivery_authorized`` / ``ready_for_send``
    That anything may be sent to a customer. Also constant ``False``.

What the live evidence actually showed (10.09.2026)
---------------------------------------------------
EasyWeek requires ``price`` to be present but does NOT check it against the
template: ``price=0`` and ``price=1499`` both returned a happily calculated
invoice, and ``price=1500`` with ``discount_amount=1500`` returned a zero total.
So ``/orders/calculate`` is not a server-side price validator, and the
application's supported contract refuses zero prices, arbitrary prices and
discounts on its own side instead — in the transport, before the wire, and again
here.

``account_paid_amount`` was observed as ``-1500``. It is an opaque bookkeeping
field: it is projected as an optional exact integer and takes no part in
readiness. It is not a payment, and a successful zero-total calculation is not
proof that a free voucher could be created, paid for or handed to anybody.

Why the whole envelope is inspected
-----------------------------------
``order_uuid`` and ``status`` may appear on the outer envelope, inside ``data``
and inside ``invoice``. Every level that carries one is checked and every
occurrence must be strictly ``null``; a ``null`` inside ``invoice`` next to a
non-null value outside it is a persistence signal, not a pass. Anything that
looks like an individual voucher artifact — a code, a customer-facing URL, a
non-empty voucher collection, a customer binding — is a persistence signal too,
and a field with no proven semantics is a malformed response rather than a
tolerated extra.

Nothing in this module is a customer message, a write instruction, or durable
authorization. A green result is a moment of operator evidence and expires with
the process that produced it.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Final, Protocol

from altegio_bot.easyweek_client import EasyWeekError, EasyWeekProtocolError
from altegio_bot.easyweek_voucher_calculation import (
    EasyWeekCalculationUncertain,
    VoucherCalculationResult,
)
from altegio_bot.easyweek_voucher_identity import (
    EASYWEEK_VOUCHER_TEMPLATE_UUID,
    EASYWEEK_WORKSPACE_CURRENCY,
    EASYWEEK_WORKSPACE_SLUG,
    EASYWEEK_WORKSPACE_UUID,
    KARLSRUHE_LOCATION_UUID,
    SUPPORTED_VOUCHER_PRICE_MINOR,
)

# Stable, PII-free reasons. No UUID, no status prose, no server message.
GIFT_CARD_CALCULATION_CONFIGURATION_UNAVAILABLE: Final = "gift_card_calculation_configuration_unavailable"
GIFT_CARD_CALCULATION_TEMPLATE_UNPROVEN: Final = "gift_card_calculation_template_unproven"
GIFT_CARD_CALCULATION_PRICE_UNPROVEN: Final = "gift_card_calculation_price_unproven"
GIFT_CARD_CALCULATION_REJECTED: Final = "gift_card_calculation_rejected"
# Named "uncertain", not "retryable": the outcome is unknown and the command
# must NOT be re-run automatically after it.
GIFT_CARD_CALCULATION_UNCERTAIN: Final = "gift_card_calculation_uncertain"
GIFT_CARD_CALCULATION_RESPONSE_MALFORMED: Final = "gift_card_calculation_response_malformed"
GIFT_CARD_CALCULATION_AMOUNT_MISMATCH: Final = "gift_card_calculation_amount_mismatch"
GIFT_CARD_CALCULATION_PERSISTENCE_SIGNAL: Final = "gift_card_calculation_persistence_signal"
GIFT_CARD_TEMPLATE_COUNTER_DRIFT: Final = "gift_card_template_counter_drift"
GIFT_CARD_TEMPLATE_STATE_DRIFT: Final = "gift_card_template_state_drift"
# The post-POST re-read itself did not answer. The calculation may have been
# perfectly clean; we simply cannot say, and that is not a pass.
GIFT_CARD_TEMPLATE_VERIFICATION_UNCERTAIN: Final = "gift_card_template_verification_uncertain"
GIFT_CARD_CALCULATION_CONTRACT_UNPROVEN: Final = "gift_card_calculation_contract_unproven"

# The two reasons that mean "unknown", as opposed to "proven wrong". A caller
# maps these to its own unknown disposition; neither licenses an automatic
# re-run of a flow that has already sent one POST.
UNCERTAIN_REASONS: Final = frozenset(
    {
        GIFT_CARD_CALCULATION_UNCERTAIN,
        GIFT_CARD_TEMPLATE_VERIFICATION_UNCERTAIN,
    }
)

# Every monetary field the supported invoice must carry as an exact integer.
_REQUIRED_INVOICE_AMOUNTS: Final = (
    "base_amount",
    "base_price",
    "subtotal",
    "total",
    "amount_due",
    "discount_amount",
    "amount_paid",
    "voucher_paid_amount",
    "promocode_discount_amount",
)
# Of those, the ones that must equal the price we sent...
_PRICED_INVOICE_AMOUNTS: Final = ("base_amount", "base_price", "subtotal", "total", "amount_due")
# ...and the ones that must be exactly zero. A non-zero discount, promo discount
# or paid amount is a different contract, not a cheaper one.
_ZERO_INVOICE_AMOUNTS: Final = (
    "discount_amount",
    "amount_paid",
    "voucher_paid_amount",
    "promocode_discount_amount",
)

# Presence AND null are both required, at every level that carries the field: a
# missing key proves nothing, and a non-null value anywhere is a signal that
# something may have been persisted.
_PERSISTENCE_FIELDS: Final = ("order_uuid", "status")

# Fields whose mere non-empty presence means an individual voucher artifact or a
# customer side effect. Their VALUES are never read, never compared and never
# reported — only the fact that one was there.
_ARTIFACT_FIELDS: Final = frozenset(
    {
        "code",
        "customer",
        "customer_url",
        "customer_uuid",
        "public_purchase_url",
        "public_url",
        "url",
        "voucher",
        "voucher_code",
        "voucher_uuid",
        "vouchers",
    }
)

# Anything outside these sets has no proven semantics for this contract, so it
# is a malformed response rather than a tolerated extra.
_ALLOWED_ENVELOPE_FIELDS: Final = _ARTIFACT_FIELDS | {
    "data",
    "invoice",
    "account_paid_amount",
    *_PERSISTENCE_FIELDS,
}
_ALLOWED_DATA_FIELDS: Final = _ALLOWED_ENVELOPE_FIELDS - {"data"}
_ALLOWED_INVOICE_FIELDS: Final = _ARTIFACT_FIELDS | {
    *_REQUIRED_INVOICE_AMOUNTS,
    "account_paid_amount",
    "promocode",
    "taxes",
    *_PERSISTENCE_FIELDS,
}

_COUNTER_FIELDS: Final = ("vouchers_count", "activated_vouchers_count")


class VoucherTemplateReader(Protocol):
    """The reviewed GET surface this evidence is allowed to use."""

    async def get_workspace(self) -> dict[str, Any]: ...

    async def list_locations(self) -> list[dict[str, Any]]: ...

    async def list_voucher_templates(self) -> list[dict[str, Any]]: ...

    async def get_voucher_template(self, voucher_template_uuid: str) -> dict[str, Any]: ...


class VoucherCalculator(Protocol):
    """The single non-persistent POST this evidence is allowed to use."""

    async def calculate_single_voucher(
        self,
        *,
        location_uuid: str,
        voucher_template_uuid: str,
        price_minor: int,
    ) -> VoucherCalculationResult: ...


def _object(payload: object) -> dict[str, Any]:
    if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
        return payload["data"]
    return payload if isinstance(payload, dict) else {}


def _exact_int(value: object) -> int | None:
    """Exact ``int`` only. ``True`` is not 1 here, and 15.0 is not 1500."""
    return value if type(value) is int else None


def _rows(payload: object) -> list[Any]:
    """The list inside a bare list or a ``{"data": [...]}`` envelope."""
    rows = payload.get("data") if isinstance(payload, dict) else payload
    return rows if isinstance(rows, list) else []


def _uuid_occurrences(payload: object, expected: str) -> int:
    """How many rows name *expected*. Two is as wrong as none."""
    return sum(1 for row in _rows(payload) if isinstance(row, dict) and row.get("uuid") == expected)


def template_counters(template_payload: object) -> dict[str, int] | None:
    """Both voucher counters, or ``None`` when they are not usable evidence.

    ``None`` is not "zero" and not "unchanged": a counter we cannot read is a
    counter we cannot compare, and the caller must fail closed on it.

    Beyond types, the pair has to be internally possible. More activated
    vouchers than issued ones is not a state this product can be in, so reading
    it means we are not looking at what we think we are looking at.
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


@dataclass(frozen=True)
class TemplateState:
    """The normative template facts, read once and compared later.

    ``fingerprint`` is every field the supported contract depends on, and
    ``counters`` is the pair that must not move around a POST. Both must be
    readable for the state to be usable, and both must be identical before and
    after the POST for a green result. Neither holds a raw API payload.
    """

    fingerprint: dict[str, Any] | None
    counters: dict[str, int] | None

    @property
    def valid(self) -> bool:
        return self.fingerprint is not None and self.counters is not None


def _template_fingerprint(template_payload: object) -> dict[str, Any] | None:
    """The normative template facts, or ``None`` when any of them is unusable.

    Branch applicability is part of this and not a separate afterthought:
    Karlsruhe merely appearing in ``/locations`` says nothing about whether this
    product reaches that branch. What is proven is an all-branches template
    whose connected count equals the workspace's branch count — which includes
    Karlsruhe. It is NOT proof of a Karlsruhe-only product, and nothing here
    claims that.
    """
    template = _object(template_payload)
    if template.get("uuid") != EASYWEEK_VOUCHER_TEMPLATE_UUID:
        return None
    if template.get("is_enabled") is not True:
        return None
    if template.get("is_single_charge") is not True:
        return None
    if template.get("is_connected_all_branches") is not True:
        return None

    cost = _exact_int(template.get("cost"))
    value = _exact_int(template.get("value"))
    branches_count = _exact_int(template.get("branches_count"))
    all_branches_count = _exact_int(template.get("all_branches_count"))
    if cost is None or cost < 0 or value is None or value < 0:
        return None
    if branches_count is None or branches_count < 0:
        return None
    if all_branches_count is None or all_branches_count < 1:
        return None
    if branches_count != all_branches_count:
        return None

    return {
        "is_enabled": True,
        "is_single_charge": True,
        "is_connected_all_branches": True,
        "cost": cost,
        "value": value,
        "branches_count": branches_count,
        "all_branches_count": all_branches_count,
    }


def template_state(template_payload: object) -> TemplateState:
    """Read the normative template facts and counters from one exact GET."""
    return TemplateState(
        fingerprint=_template_fingerprint(template_payload),
        counters=template_counters(template_payload),
    )


@dataclass(frozen=True)
class VoucherCalculationPrerequisites:
    """Everything that must hold BEFORE a single POST may be sent."""

    workspace_proven: bool
    location_proven: bool
    template_proven: bool
    template_pristine: bool
    price_minor: int | None
    state_before: TemplateState
    reasons: tuple[str, ...]

    @property
    def counters_before(self) -> dict[str, int] | None:
        return self.state_before.counters

    @property
    def proven(self) -> bool:
        return not self.reasons and self.price_minor is not None and self.state_before.valid


def evaluate_calculation_prerequisites(
    *,
    workspace_payload: object,
    locations_payload: object,
    templates_payload: object,
    template_payload: object,
) -> VoucherCalculationPrerequisites:
    """Re-prove workspace, branch applicability and template, or refuse the POST."""
    workspace = _object(workspace_payload)
    reasons: list[str] = []

    workspace_proven = (
        workspace.get("uuid") == EASYWEEK_WORKSPACE_UUID
        and workspace.get("slug") == EASYWEEK_WORKSPACE_SLUG
        and workspace.get("currency") == EASYWEEK_WORKSPACE_CURRENCY
    )
    # Exactly once. A duplicated branch row means the identity is ambiguous, and
    # an ambiguous identity is not a proven one.
    location_proven = _uuid_occurrences(locations_payload, KARLSRUHE_LOCATION_UUID) == 1
    if not (workspace_proven and location_proven):
        reasons.append(GIFT_CARD_CALCULATION_CONFIGURATION_UNAVAILABLE)

    listed_once = _uuid_occurrences(templates_payload, EASYWEEK_VOUCHER_TEMPLATE_UUID) == 1
    state = template_state(template_payload)
    template_proven = listed_once and state.fingerprint is not None
    if not template_proven:
        reasons.append(GIFT_CARD_CALCULATION_TEMPLATE_UNPROVEN)

    if state.counters is None:
        # Counters we cannot read are counters we cannot compare afterwards, so
        # the POST must not happen at all.
        reasons.append(GIFT_CARD_TEMPLATE_COUNTER_DRIFT)

    # The POST price comes from the FRESH template and from nowhere else — not
    # from a CLI argument, not from this module's constant. The constant only
    # decides whether the fresh value is one the application supports.
    price_minor: int | None = None
    if state.fingerprint is not None:
        cost = state.fingerprint["cost"]
        if cost == state.fingerprint["value"] == SUPPORTED_VOUCHER_PRICE_MINOR:
            price_minor = cost
    if price_minor is None:
        reasons.append(GIFT_CARD_CALCULATION_PRICE_UNPROVEN)

    # Reported as evidence, never as a permanent product contract: a workspace
    # that legitimately sells a voucher later stops being pristine without the
    # calculation contract changing.
    template_pristine = state.counters is not None and all(count == 0 for count in state.counters.values())

    return VoucherCalculationPrerequisites(
        workspace_proven=workspace_proven,
        location_proven=location_proven,
        template_proven=template_proven,
        template_pristine=template_pristine,
        price_minor=price_minor,
        state_before=state,
        reasons=tuple(dict.fromkeys(reasons)),
    )


@dataclass(frozen=True)
class VoucherInvoiceProjection:
    """The strict projection of one calculate response. No raw field survives."""

    proven: bool
    account_paid_amount_observed: int | None
    reasons: tuple[str, ...]


def _artifact_present(value: object) -> bool:
    """True when *value* actually carries something, not just a null/empty slot.

    ``vouchers: []`` and ``code: ""`` are empty slots — the API describing a
    shape, not handing over an artifact. Anything with content is an artifact,
    and its content is never looked at beyond emptiness.
    """
    if value is None:
        return False
    if isinstance(value, (str, bytes, list, tuple, set, dict)):
        return len(value) > 0
    return True


def _levels(envelope: dict[str, Any]) -> list[tuple[dict[str, Any], frozenset[str]]]:
    """Every object level of the response, with the fields allowed on it."""
    levels: list[tuple[dict[str, Any], frozenset[str]]] = [(envelope, _ALLOWED_ENVELOPE_FIELDS)]
    inner = envelope.get("data")
    if isinstance(inner, dict):
        levels.append((inner, _ALLOWED_DATA_FIELDS))
    invoice = _invoice_object(envelope)
    if invoice is not None:
        levels.append((invoice, _ALLOWED_INVOICE_FIELDS))
    return levels


def _invoice_object(envelope: dict[str, Any]) -> dict[str, Any] | None:
    """The invoice object from the envelope or from ``data``, if there is one."""
    direct = envelope.get("invoice")
    if isinstance(direct, dict):
        return direct
    inner = envelope.get("data")
    if isinstance(inner, dict) and isinstance(inner.get("invoice"), dict):
        return inner["invoice"]
    return None


def evaluate_calculation_invoice(
    *,
    http_status: int,
    envelope: object,
    expected_price_minor: int,
) -> VoucherInvoiceProjection:
    """Project one complete calculate response into pass/fail plus stable reasons.

    *envelope* is the full body as received. Every level is inspected: an
    ``order_uuid`` on the outer object is exactly as disqualifying as one inside
    ``invoice``, and an unexplained field anywhere is a malformed response
    rather than a harmless extra.
    """
    reasons: list[str] = []

    if http_status != 200 or not isinstance(envelope, dict):
        return VoucherInvoiceProjection(
            proven=False,
            account_paid_amount_observed=None,
            reasons=(GIFT_CARD_CALCULATION_RESPONSE_MALFORMED,),
        )

    invoice = _invoice_object(envelope)
    if invoice is None:
        return VoucherInvoiceProjection(
            proven=False,
            account_paid_amount_observed=None,
            reasons=(GIFT_CARD_CALCULATION_RESPONSE_MALFORMED,),
        )

    levels = _levels(envelope)

    # 1. Persistence identity, at every level that carries it.
    for name in _PERSISTENCE_FIELDS:
        observed = [mapping[name] for mapping, _ in levels if name in mapping]
        if not observed:
            reasons.append(GIFT_CARD_CALCULATION_RESPONSE_MALFORMED)
        elif any(value is not None for value in observed):
            # Never reported as a value — only as the fact that it was there.
            reasons.append(GIFT_CARD_CALCULATION_PERSISTENCE_SIGNAL)

    # 2. Voucher artifacts and unexplained fields, at every level.
    for mapping, allowed in levels:
        for name, value in mapping.items():
            if name in _ARTIFACT_FIELDS:
                if _artifact_present(value):
                    reasons.append(GIFT_CARD_CALCULATION_PERSISTENCE_SIGNAL)
            elif name not in allowed:
                reasons.append(GIFT_CARD_CALCULATION_RESPONSE_MALFORMED)

    # 3. Money, exactly typed and exactly equal.
    amounts: dict[str, int] = {}
    for name in _REQUIRED_INVOICE_AMOUNTS:
        exact = _exact_int(invoice.get(name))
        if exact is None:
            reasons.append(GIFT_CARD_CALCULATION_RESPONSE_MALFORMED)
            continue
        amounts[name] = exact

    if len(amounts) == len(_REQUIRED_INVOICE_AMOUNTS):
        priced_ok = all(amounts[name] == expected_price_minor for name in _PRICED_INVOICE_AMOUNTS)
        zeroed_ok = all(amounts[name] == 0 for name in _ZERO_INVOICE_AMOUNTS)
        if not (priced_ok and zeroed_ok):
            reasons.append(GIFT_CARD_CALCULATION_AMOUNT_MISMATCH)

    # 4. Promocode and taxes: present, and provably not in play.
    if "promocode" not in invoice:
        reasons.append(GIFT_CARD_CALCULATION_RESPONSE_MALFORMED)
    elif invoice["promocode"] is not None:
        # Any promocode at all is a different contract. Its value is not read.
        reasons.append(GIFT_CARD_CALCULATION_AMOUNT_MISMATCH)

    if "taxes" not in invoice:
        reasons.append(GIFT_CARD_CALCULATION_RESPONSE_MALFORMED)
    elif not isinstance(invoice["taxes"], list):
        reasons.append(GIFT_CARD_CALCULATION_RESPONSE_MALFORMED)
    elif invoice["taxes"]:
        reasons.append(GIFT_CARD_CALCULATION_AMOUNT_MISMATCH)

    # Observed only. `-1500` is an opaque bookkeeping figure, not a payment, and
    # it neither proves nor blocks the calculation contract.
    account_paid = _exact_int(invoice.get("account_paid_amount"))
    if account_paid is None:
        account_paid = _exact_int(envelope.get("account_paid_amount"))

    unique = tuple(dict.fromkeys(reasons))
    return VoucherInvoiceProjection(
        proven=not unique,
        account_paid_amount_observed=account_paid,
        reasons=unique,
    )


@dataclass(frozen=True)
class VoucherContractEvidence:
    """The whole PII-free result. Safe to print; never durable authorization."""

    workspace_proven: bool
    location_proven: bool
    template_proven: bool
    template_pristine: bool
    template_counters_before: dict[str, int] | None
    template_counters_after: dict[str, int] | None
    template_counters_unchanged: bool
    template_state_unchanged: bool
    calculation_contract_ready: bool
    account_paid_amount_observed: int | None
    reasons: tuple[str, ...]

    # Constants, not computations. Until a separately authorised mutation PR
    # exists, there is no input that could make any of these true — including a
    # response that appeared to hand us a voucher code or a customer URL.
    @property
    def issue_contract_ready(self) -> bool:
        return False

    @property
    def individual_voucher_artifact_proven(self) -> bool:
        return False

    @property
    def customer_binding_proven(self) -> bool:
        return False

    @property
    def write_idempotency_proven(self) -> bool:
        return False

    @property
    def unknown_result_reconciliation_proven(self) -> bool:
        return False

    @property
    def delivery_authorized(self) -> bool:
        return False

    @property
    def ready_for_send(self) -> bool:
        return False

    @property
    def uncertain(self) -> bool:
        """True when the outcome is unknown rather than proven wrong."""
        return bool(UNCERTAIN_REASONS.intersection(self.reasons))

    def as_safe_dict(self) -> dict[str, Any]:
        """PII-free projection: no UUID, no body, no code, no customer, no URL."""
        safe: dict[str, Any] = {
            "mode": "nonpersistent_calculation_evidence",
            "workspace_proven": self.workspace_proven,
            "location_proven": self.location_proven,
            "template_proven": self.template_proven,
            "template_pristine": self.template_pristine,
            "template_counters_before": dict(self.template_counters_before)
            if self.template_counters_before is not None
            else None,
            "template_counters_after": dict(self.template_counters_after)
            if self.template_counters_after is not None
            else None,
            "template_counters_unchanged": self.template_counters_unchanged,
            "template_state_unchanged": self.template_state_unchanged,
            "calculation_contract_ready": self.calculation_contract_ready,
            "issue_contract_ready": self.issue_contract_ready,
            "individual_voucher_artifact_proven": self.individual_voucher_artifact_proven,
            "customer_binding_proven": self.customer_binding_proven,
            "write_idempotency_proven": self.write_idempotency_proven,
            "unknown_result_reconciliation_proven": self.unknown_result_reconciliation_proven,
            "delivery_authorized": self.delivery_authorized,
            "ready_for_send": self.ready_for_send,
            "reasons": list(self.reasons),
        }
        # Only ever an exact integer, and only when the API actually sent one.
        if self.account_paid_amount_observed is not None:
            safe["account_paid_amount_observed"] = self.account_paid_amount_observed
        return safe


def _failed(
    prerequisites: VoucherCalculationPrerequisites,
    *,
    extra_reasons: tuple[str, ...] = (),
    counters_after: dict[str, int] | None = None,
    counters_unchanged: bool = False,
    state_unchanged: bool = False,
    account_paid_amount_observed: int | None = None,
) -> VoucherContractEvidence:
    reasons = tuple(dict.fromkeys((*prerequisites.reasons, *extra_reasons, GIFT_CARD_CALCULATION_CONTRACT_UNPROVEN)))
    return VoucherContractEvidence(
        workspace_proven=prerequisites.workspace_proven,
        location_proven=prerequisites.location_proven,
        template_proven=prerequisites.template_proven,
        template_pristine=prerequisites.template_pristine,
        template_counters_before=prerequisites.counters_before,
        template_counters_after=counters_after,
        template_counters_unchanged=counters_unchanged,
        template_state_unchanged=state_unchanged,
        calculation_contract_ready=False,
        account_paid_amount_observed=account_paid_amount_observed,
        reasons=reasons,
    )


def _is_uncertain(exc: EasyWeekError) -> bool:
    return isinstance(exc, EasyWeekCalculationUncertain) or exc.retryable


def _reason_for_transport_error(exc: EasyWeekError) -> str:
    """Map one typed POST failure to one stable reason.

    A malformed 2xx is kept apart from a rejection: the server accepted the
    request and answered with something we cannot read, which is a contract
    problem, not a refusal.
    """
    if _is_uncertain(exc):
        return GIFT_CARD_CALCULATION_UNCERTAIN
    if isinstance(exc, EasyWeekProtocolError):
        return GIFT_CARD_CALCULATION_RESPONSE_MALFORMED
    return GIFT_CARD_CALCULATION_REJECTED


@dataclass(frozen=True)
class _TemplateVerification:
    """What the one post-POST re-read actually established."""

    state: TemplateState | None
    reason: str | None

    @property
    def counters(self) -> dict[str, int] | None:
        return self.state.counters if self.state is not None else None


async def _verify_template_after(reader: VoucherTemplateReader) -> _TemplateVerification:
    """Read the exact template once more, keeping the failure's own disposition.

    A timeout or a 5xx here is not template drift and must not be reported as
    one: it means the verification did not happen, which is an unknown. An
    auth failure or a 404 is a configuration problem. Collapsing both into
    "counters changed" told an operator something false and picked the wrong
    exit code.
    """
    try:
        after = await reader.get_voucher_template(EASYWEEK_VOUCHER_TEMPLATE_UUID)
    except EasyWeekError as exc:
        if _is_uncertain(exc):
            return _TemplateVerification(state=None, reason=GIFT_CARD_TEMPLATE_VERIFICATION_UNCERTAIN)
        return _TemplateVerification(state=None, reason=GIFT_CARD_CALCULATION_CONFIGURATION_UNAVAILABLE)
    return _TemplateVerification(state=template_state(after), reason=None)


def _verification_reasons(
    before: TemplateState,
    verification: _TemplateVerification,
) -> tuple[tuple[str, ...], bool, bool]:
    """Reasons, counters_unchanged and state_unchanged for the post-POST re-read."""
    if verification.reason is not None:
        return (verification.reason,), False, False

    after = verification.state
    assert after is not None  # reason is None only when the read succeeded
    reasons: list[str] = []

    counters_unchanged = (
        after.counters is not None and before.counters is not None and after.counters == before.counters
    )
    if not counters_unchanged:
        reasons.append(GIFT_CARD_TEMPLATE_COUNTER_DRIFT)

    state_unchanged = (
        after.fingerprint is not None and before.fingerprint is not None and after.fingerprint == before.fingerprint
    )
    if not state_unchanged:
        reasons.append(GIFT_CARD_TEMPLATE_STATE_DRIFT)

    return tuple(reasons), counters_unchanged, state_unchanged


async def probe_voucher_calculation_contract(
    reader: VoucherTemplateReader,
    calculator: VoucherCalculator,
) -> VoucherContractEvidence:
    """Reviewed GETs, then at most one POST, then the exact template again.

    The POST is issued only when every prerequisite already holds, and it is
    issued at most once whatever happens. The template is re-read after any POST
    attempt — including a failed one — because a request that left the process
    is exactly the case where the product could have moved. That re-read keeps
    its own typed disposition: an unanswered verification is an unknown, not a
    drift.
    """
    try:
        workspace = await reader.get_workspace()
        locations = await reader.list_locations()
        templates = await reader.list_voucher_templates()
        listed_once = _uuid_occurrences(templates, EASYWEEK_VOUCHER_TEMPLATE_UUID) == 1
        # An unlisted or ambiguous UUID is never fetched: asking for it would be
        # probing, not re-proving what the workspace already showed.
        template = await reader.get_voucher_template(EASYWEEK_VOUCHER_TEMPLATE_UUID) if listed_once else {}
    except EasyWeekError as exc:
        empty = VoucherCalculationPrerequisites(
            workspace_proven=False,
            location_proven=False,
            template_proven=False,
            template_pristine=False,
            price_minor=None,
            state_before=TemplateState(fingerprint=None, counters=None),
            reasons=(GIFT_CARD_CALCULATION_UNCERTAIN,)
            if _is_uncertain(exc)
            else (GIFT_CARD_CALCULATION_CONFIGURATION_UNAVAILABLE,),
        )
        return _failed(empty)

    prerequisites = evaluate_calculation_prerequisites(
        workspace_payload=workspace,
        locations_payload=locations,
        templates_payload=templates,
        template_payload=template,
    )
    if not prerequisites.proven:
        return _failed(prerequisites)

    assert prerequisites.price_minor is not None  # guarded by `proven`
    result: VoucherCalculationResult | None
    try:
        result = await calculator.calculate_single_voucher(
            location_uuid=KARLSRUHE_LOCATION_UUID,
            voucher_template_uuid=EASYWEEK_VOUCHER_TEMPLATE_UUID,
            price_minor=prerequisites.price_minor,
        )
    except EasyWeekError as exc:
        post_reason: str | None = _reason_for_transport_error(exc)
        result = None
    else:
        post_reason = None

    verification = await _verify_template_after(reader)
    verify_reasons, counters_unchanged, state_unchanged = _verification_reasons(
        prerequisites.state_before, verification
    )

    if result is None:
        assert post_reason is not None
        return _failed(
            prerequisites,
            extra_reasons=(post_reason, *verify_reasons),
            counters_after=verification.counters,
            counters_unchanged=counters_unchanged,
            state_unchanged=state_unchanged,
        )

    invoice = evaluate_calculation_invoice(
        http_status=result.http_status,
        envelope=result.envelope,
        expected_price_minor=prerequisites.price_minor,
    )
    if not (invoice.proven and counters_unchanged and state_unchanged):
        return _failed(
            prerequisites,
            extra_reasons=(*invoice.reasons, *verify_reasons),
            counters_after=verification.counters,
            counters_unchanged=counters_unchanged,
            state_unchanged=state_unchanged,
            account_paid_amount_observed=invoice.account_paid_amount_observed,
        )

    return VoucherContractEvidence(
        workspace_proven=prerequisites.workspace_proven,
        location_proven=prerequisites.location_proven,
        template_proven=prerequisites.template_proven,
        template_pristine=prerequisites.template_pristine,
        template_counters_before=prerequisites.counters_before,
        template_counters_after=verification.counters,
        template_counters_unchanged=True,
        template_state_unchanged=True,
        calculation_contract_ready=True,
        account_paid_amount_observed=invoice.account_paid_amount_observed,
        reasons=(),
    )
