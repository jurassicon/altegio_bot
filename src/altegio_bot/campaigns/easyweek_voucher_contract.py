"""Fail-closed evidence for the EasyWeek non-persistent voucher calculation.

Three different things are kept apart here, because production evidence proves
only the first one and conflating them is how a preview becomes a send:

``calculation_contract_ready``
    One fully proven, non-persistent ``POST /orders/calculate`` for the exact
    Karlsruhe location, the exact voucher template and the exact price the
    template itself reports — with the voucher counters unchanged around it.

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
discounts on its own side instead.

``account_paid_amount`` was observed as ``-1500``. It is an opaque bookkeeping
field: it is projected as an optional exact integer and takes no part in
readiness. It is not a payment, and a successful zero-total calculation is not
proof that a free voucher could be created, paid for or handed to anybody.

Nothing in this module is a customer message, a write instruction, or durable
authorization. A green result is a moment of operator evidence and expires with
the process that produced it.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Final, Protocol

from altegio_bot.campaigns.gift_card_readiness import (
    EASYWEEK_VOUCHER_TEMPLATE_UUID,
    EASYWEEK_WORKSPACE_SLUG,
    EASYWEEK_WORKSPACE_UUID,
    KARLSRUHE_LOCATION_UUID,
)
from altegio_bot.easyweek_client import EasyWeekError, EasyWeekProtocolError
from altegio_bot.easyweek_voucher_calculation import (
    EasyWeekCalculationUncertain,
    VoucherCalculationResult,
)

# The only currency and the only nominal this contract supports. `cost` and
# `value` were both observed as 1500 minor units in EUR; the POST price is
# still read from the *fresh* template rather than from this constant, which
# only decides whether that fresh value is one we support.
EASYWEEK_WORKSPACE_CURRENCY: Final = "EUR"
SUPPORTED_VOUCHER_PRICE_MINOR: Final = 1500

# Stable, PII-free reasons. No UUID, no status prose, no server message.
GIFT_CARD_CALCULATION_CONFIGURATION_UNAVAILABLE: Final = "gift_card_calculation_configuration_unavailable"
GIFT_CARD_CALCULATION_TEMPLATE_UNPROVEN: Final = "gift_card_calculation_template_unproven"
GIFT_CARD_CALCULATION_PRICE_UNPROVEN: Final = "gift_card_calculation_price_unproven"
GIFT_CARD_CALCULATION_REJECTED: Final = "gift_card_calculation_rejected"
GIFT_CARD_CALCULATION_RETRYABLE_UNCERTAINTY: Final = "gift_card_calculation_retryable_uncertainty"
GIFT_CARD_CALCULATION_RESPONSE_MALFORMED: Final = "gift_card_calculation_response_malformed"
GIFT_CARD_CALCULATION_AMOUNT_MISMATCH: Final = "gift_card_calculation_amount_mismatch"
GIFT_CARD_CALCULATION_PERSISTENCE_SIGNAL: Final = "gift_card_calculation_persistence_signal"
GIFT_CARD_TEMPLATE_COUNTER_DRIFT: Final = "gift_card_template_counter_drift"
GIFT_CARD_CALCULATION_CONTRACT_UNPROVEN: Final = "gift_card_calculation_contract_unproven"

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
)
# Of those, the ones that must equal the price we sent...
_PRICED_INVOICE_AMOUNTS: Final = ("base_amount", "base_price", "subtotal", "total", "amount_due")
# ...and the ones that must be exactly zero. A non-zero discount or paid amount
# is a different contract, not a cheaper one.
_ZERO_INVOICE_AMOUNTS: Final = ("discount_amount", "amount_paid", "voucher_paid_amount")

# Presence AND null are both required: a missing key proves nothing, and a
# non-null value is a signal that something may have been persisted.
_PERSISTENCE_FIELDS: Final = ("order_uuid", "status")

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


def template_counters(template_payload: object) -> dict[str, int] | None:
    """Both voucher counters as exact integers, or ``None`` if unusable.

    ``None`` is not "zero" and not "unchanged": a counter we cannot read is a
    counter we cannot compare, and the caller must fail closed on it.
    """
    template = _object(template_payload)
    counters: dict[str, int] = {}
    for field in _COUNTER_FIELDS:
        exact = _exact_int(template.get(field))
        if exact is None:
            return None
        counters[field] = exact
    return counters


@dataclass(frozen=True)
class VoucherCalculationPrerequisites:
    """Everything that must hold BEFORE a single POST may be sent."""

    workspace_proven: bool
    location_proven: bool
    template_proven: bool
    template_pristine: bool
    price_minor: int | None
    counters_before: dict[str, int] | None
    reasons: tuple[str, ...]

    @property
    def proven(self) -> bool:
        return not self.reasons and self.price_minor is not None and self.counters_before is not None


def evaluate_calculation_prerequisites(
    *,
    workspace_payload: object,
    locations_payload: object,
    templates_payload: object,
    template_payload: object,
) -> VoucherCalculationPrerequisites:
    """Re-prove workspace, branch and template from fresh reads, or refuse."""
    workspace = _object(workspace_payload)
    template = _object(template_payload)
    reasons: list[str] = []

    workspace_proven = (
        workspace.get("uuid") == EASYWEEK_WORKSPACE_UUID
        and workspace.get("slug") == EASYWEEK_WORKSPACE_SLUG
        and workspace.get("currency") == EASYWEEK_WORKSPACE_CURRENCY
    )

    locations = locations_payload
    if isinstance(locations, dict):
        locations = locations.get("data")
    location_rows = locations if isinstance(locations, list) else []
    location_proven = any(isinstance(row, dict) and row.get("uuid") == KARLSRUHE_LOCATION_UUID for row in location_rows)
    if not (workspace_proven and location_proven):
        reasons.append(GIFT_CARD_CALCULATION_CONFIGURATION_UNAVAILABLE)

    templates = templates_payload
    if isinstance(templates, dict):
        templates = templates.get("data")
    template_rows = templates if isinstance(templates, list) else []
    listed = any(isinstance(row, dict) and row.get("uuid") == EASYWEEK_VOUCHER_TEMPLATE_UUID for row in template_rows)

    # `is_single_charge` is part of the supported identity: a multi-charge
    # product would price and behave differently, and none of that is proven.
    template_proven = (
        listed
        and template.get("uuid") == EASYWEEK_VOUCHER_TEMPLATE_UUID
        and template.get("is_enabled") is True
        and template.get("is_single_charge") is True
    )
    if not template_proven:
        reasons.append(GIFT_CARD_CALCULATION_TEMPLATE_UNPROVEN)

    # The POST price comes from the FRESH template and from nowhere else — not
    # from a CLI argument, not from this module's constant. The constant only
    # decides whether the fresh value is one the application supports.
    cost = _exact_int(template.get("cost"))
    value = _exact_int(template.get("value"))
    price_minor: int | None = None
    if cost is not None and cost == value == SUPPORTED_VOUCHER_PRICE_MINOR:
        price_minor = cost
    else:
        reasons.append(GIFT_CARD_CALCULATION_PRICE_UNPROVEN)

    counters_before = template_counters(template)
    if counters_before is None:
        # Counters we cannot read are counters we cannot compare afterwards, so
        # the POST must not happen at all.
        reasons.append(GIFT_CARD_TEMPLATE_COUNTER_DRIFT)

    # Reported as evidence, never as a permanent product contract: a workspace
    # that legitimately sells a voucher later stops being pristine without the
    # calculation contract changing.
    template_pristine = counters_before is not None and all(count == 0 for count in counters_before.values())

    return VoucherCalculationPrerequisites(
        workspace_proven=workspace_proven,
        location_proven=location_proven,
        template_proven=template_proven,
        template_pristine=template_pristine,
        price_minor=price_minor,
        counters_before=counters_before,
        reasons=tuple(dict.fromkeys(reasons)),
    )


@dataclass(frozen=True)
class VoucherInvoiceProjection:
    """The strict projection of one calculate response. No raw field survives."""

    proven: bool
    account_paid_amount_observed: int | None
    reasons: tuple[str, ...]


def _persistence_value(invoice: dict[str, Any], body: dict[str, Any], field: str) -> tuple[bool, Any]:
    """Find *field* on the invoice, else on the envelope. Presence is strict.

    The live shape carries ``order_uuid``/``status`` alongside the amounts, but
    an envelope-level placement is equally plausible for a preview response.
    Both are accepted; absence from both is not.
    """
    if field in invoice:
        return True, invoice[field]
    if field in body:
        return True, body[field]
    return False, None


def evaluate_calculation_invoice(
    *,
    http_status: int,
    payload: object,
    expected_price_minor: int,
) -> VoucherInvoiceProjection:
    """Project one calculate response into pass/fail plus stable reasons.

    Everything the supported contract needs must be present, exactly typed and
    exactly equal. Extra response fields are allowed and are simply not read:
    nothing outside this function's own vocabulary can reach a report.
    """
    reasons: list[str] = []

    if http_status != 200:
        return VoucherInvoiceProjection(
            proven=False,
            account_paid_amount_observed=None,
            reasons=(GIFT_CARD_CALCULATION_RESPONSE_MALFORMED,),
        )

    if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
        body: Any = payload["data"]
    else:
        body = payload
    if not isinstance(body, dict):
        return VoucherInvoiceProjection(
            proven=False,
            account_paid_amount_observed=None,
            reasons=(GIFT_CARD_CALCULATION_RESPONSE_MALFORMED,),
        )

    invoice = body.get("invoice")
    if not isinstance(invoice, dict):
        return VoucherInvoiceProjection(
            proven=False,
            account_paid_amount_observed=None,
            reasons=(GIFT_CARD_CALCULATION_RESPONSE_MALFORMED,),
        )

    for field in _PERSISTENCE_FIELDS:
        present, observed = _persistence_value(invoice, body, field)
        if not present:
            reasons.append(GIFT_CARD_CALCULATION_RESPONSE_MALFORMED)
        elif observed is not None:
            # Something that looks like a created order. Never reported as a
            # value — only as the fact that the signal was there.
            reasons.append(GIFT_CARD_CALCULATION_PERSISTENCE_SIGNAL)

    amounts: dict[str, int] = {}
    for field in _REQUIRED_INVOICE_AMOUNTS:
        exact = _exact_int(invoice.get(field))
        if exact is None:
            reasons.append(GIFT_CARD_CALCULATION_RESPONSE_MALFORMED)
            continue
        amounts[field] = exact

    if len(amounts) == len(_REQUIRED_INVOICE_AMOUNTS):
        priced_ok = all(amounts[field] == expected_price_minor for field in _PRICED_INVOICE_AMOUNTS)
        zeroed_ok = all(amounts[field] == 0 for field in _ZERO_INVOICE_AMOUNTS)
        if not (priced_ok and zeroed_ok):
            reasons.append(GIFT_CARD_CALCULATION_AMOUNT_MISMATCH)

    # Observed only. `-1500` is an opaque bookkeeping figure, not a payment, and
    # it neither proves nor blocks the calculation contract.
    account_paid = _exact_int(invoice.get("account_paid_amount"))
    if account_paid is None:
        account_paid = _exact_int(body.get("account_paid_amount"))

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
    calculation_contract_ready: bool
    account_paid_amount_observed: int | None
    reasons: tuple[str, ...]

    # Constants, not computations. Until a separately authorised mutation PR
    # exists, there is no input that could make any of these true.
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
        calculation_contract_ready=False,
        account_paid_amount_observed=account_paid_amount_observed,
        reasons=reasons,
    )


def _reason_for_transport_error(exc: EasyWeekError) -> str:
    """Map one typed transport failure to one stable reason.

    A malformed 2xx is kept apart from a rejection: the server accepted the
    request and answered with something we cannot read, which is a contract
    problem, not a refusal.
    """
    if isinstance(exc, EasyWeekCalculationUncertain) or exc.retryable:
        return GIFT_CARD_CALCULATION_RETRYABLE_UNCERTAINTY
    if isinstance(exc, EasyWeekProtocolError):
        return GIFT_CARD_CALCULATION_RESPONSE_MALFORMED
    return GIFT_CARD_CALCULATION_REJECTED


async def probe_voucher_calculation_contract(
    reader: VoucherTemplateReader,
    calculator: VoucherCalculator,
) -> VoucherContractEvidence:
    """Reviewed GETs, then at most one POST, then the exact template again.

    The POST is issued only when every prerequisite already holds, and it is
    issued at most once whatever happens. The template is re-read after any POST
    attempt — including a failed one — because a request that left the process
    is exactly the case where a counter could have moved.
    """
    try:
        workspace = await reader.get_workspace()
        locations = await reader.list_locations()
        templates = await reader.list_voucher_templates()
        listed = any(
            isinstance(row, dict) and row.get("uuid") == EASYWEEK_VOUCHER_TEMPLATE_UUID
            for row in (templates if isinstance(templates, list) else [])
        )
        # An unlisted UUID is never fetched: asking for it would be probing, not
        # re-proving what the workspace already showed.
        template = await reader.get_voucher_template(EASYWEEK_VOUCHER_TEMPLATE_UUID) if listed else {}
    except EasyWeekError as exc:
        empty = VoucherCalculationPrerequisites(
            workspace_proven=False,
            location_proven=False,
            template_proven=False,
            template_pristine=False,
            price_minor=None,
            counters_before=None,
            reasons=(GIFT_CARD_CALCULATION_CONFIGURATION_UNAVAILABLE,)
            if not exc.retryable
            else (GIFT_CARD_CALCULATION_RETRYABLE_UNCERTAINTY,),
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
    try:
        result = await calculator.calculate_single_voucher(
            location_uuid=KARLSRUHE_LOCATION_UUID,
            voucher_template_uuid=EASYWEEK_VOUCHER_TEMPLATE_UUID,
            price_minor=prerequisites.price_minor,
        )
    except EasyWeekError as exc:
        post_reason = _reason_for_transport_error(exc)
        result = None
    else:
        post_reason = None

    counters_after = await _reread_counters(reader)
    counters_unchanged = (
        counters_after is not None
        and prerequisites.counters_before is not None
        and counters_after == prerequisites.counters_before
    )
    drift_reasons: tuple[str, ...] = () if counters_unchanged else (GIFT_CARD_TEMPLATE_COUNTER_DRIFT,)

    if result is None:
        assert post_reason is not None
        return _failed(
            prerequisites,
            extra_reasons=(post_reason, *drift_reasons),
            counters_after=counters_after,
            counters_unchanged=counters_unchanged,
        )

    invoice = evaluate_calculation_invoice(
        http_status=result.http_status,
        payload=result.payload,
        expected_price_minor=prerequisites.price_minor,
    )
    if not (invoice.proven and counters_unchanged):
        return _failed(
            prerequisites,
            extra_reasons=(*invoice.reasons, *drift_reasons),
            counters_after=counters_after,
            counters_unchanged=counters_unchanged,
            account_paid_amount_observed=invoice.account_paid_amount_observed,
        )

    return VoucherContractEvidence(
        workspace_proven=prerequisites.workspace_proven,
        location_proven=prerequisites.location_proven,
        template_proven=prerequisites.template_proven,
        template_pristine=prerequisites.template_pristine,
        template_counters_before=prerequisites.counters_before,
        template_counters_after=counters_after,
        template_counters_unchanged=True,
        calculation_contract_ready=True,
        account_paid_amount_observed=invoice.account_paid_amount_observed,
        reasons=(),
    )


async def _reread_counters(reader: VoucherTemplateReader) -> dict[str, int] | None:
    """Read the exact template once more; a failed read proves no drift-free state."""
    try:
        after = await reader.get_voucher_template(EASYWEEK_VOUCHER_TEMPLATE_UUID)
    except EasyWeekError:
        return None
    return template_counters(after)
