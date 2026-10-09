"""Three fixed production products; request versions and product versions are distinct.

This is deliberately dependency-free. Historical callers keep the 15 EUR
contract; an explicit schema 3 selects the owner-approved 10 EUR product, and an
explicit schema 4 selects the free gift certificate. There are no configuration
overrides and no mutable product registries.

Two sums, never one (§45 follow-up)
-----------------------------------
A voucher has a FACE VALUE — what the holder may spend — and an ISSUE PRICE —
what putting it into their hands costs us. For the 15 EUR and 10 EUR products
those two numbers are equal, which is why one field carried both for as long as
it did. The free gift certificate separates them: a face value of 1000 minor
units and an issue price of 0.

They are kept apart because they answer different questions and the wrong one in
the wrong place is a real loss. The issue price is what a CREATE sends, what an
order's totals must equal and what may ever be refunded. The face value is what
the product publishes as ``value``, what the message promises the customer, and
what an operator approves as the size of the giveaway. A screen that showed the
face value as money would ask somebody to approve paying 330 EUR for nothing;
one that showed the issue price as the nominal would say the gift is worth zero.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any, Final

# Non-secret identity pins for the two approved tills, so no account UUID is
# tracked in this repository. See ``account.account_fingerprint`` for the salt.
# The Card account, for the paid 10 EUR product.
CURRENT_PAYMENT_ACCOUNT_FINGERPRINT: Final = "77d6ada907150fa5e2620a0ef3571333c6c07d3c6bdda9a2fa0531ad0b0c3d64"
# Aktionsgutscheine, approved for the free gift certificate and for nothing else.
GIFT_PAYMENT_ACCOUNT_FINGERPRINT: Final = "325a9e81d292139be2bc250acf65685f745c0785665e1cedfa60634949b9f570"

CURRENT_PRODUCTION_META_BODY: Final = (
    "Hallo {{1}}!\n"
    "\n"
    "Als Dankeschön für Ihren ersten Besuch erhalten Sie einen KitiLash-Gutschein im Wert von 10 €.\n"
    "\n"
    "Ihr Gutscheincode: {{2}}\n"
    "\n"
    "Der Gutschein ist ab Aktivierung einen Monat gültig und einmalig einlösbar. Ein Restbetrag verfällt.\n"
    "\n"
    "Termin buchen:\n"
    "{{3}}\n"
    "\n"
    "Bitte zeigen Sie den Gutscheincode bei Ihrem nächsten Besuch vor.\n"
    "\n"
    "Wenn Sie keine weiteren Nachrichten erhalten möchten, antworten Sie mit STOP."
)


@dataclass(frozen=True)
class ProductionVoucherContract:
    version: str
    request_schema_version: str
    template_uuid: str
    # What the holder may spend. The product's ``value``, the sum the message
    # promises, and the size an operator approves.
    face_value_minor: int
    # What issuing one costs us. The price a CREATE sends, the total an order must
    # publish, and the only sum a refund could ever return.
    issue_price_minor: int
    baseline_version: str
    message_code: str
    meta_template_name: str
    validity_months: int | None
    # Which till this product's money moves through. Pinned per contract, so the
    # Card account cannot settle a gift certificate and Aktionsgutscheine cannot
    # settle a paid one.
    payment_account_fingerprint: str
    quantity: int = 1
    title: str | None = None
    # The till's name, for an operator to recognise on screen. Display only: the
    # identity is the fingerprint above, and a name proves nothing.
    payment_account_label: str = ""

    @property
    def free_issue(self) -> bool:
        """Does issuing one of these move no money at all?

        Its own question, asked by the guards that must not accept zero amounts as
        evidence of a payment: at a zero price there is nothing for a settled
        invoice to prove, so only an explicit paid status can.
        """
        return self.issue_price_minor == 0

    def template_facts(self) -> dict[str, Any]:
        return {
            "is_enabled": True,
            "is_online": False,
            "is_single_charge": True,
            "cost": self.issue_price_minor,
            "value": self.face_value_minor,
            "validity": self.validity_months,
            "forces_activation": True,
            "activate_after": 0,
            "activate_at": None,
            "is_connected_all_branches": True,
            "branches_count": 3,
            "all_branches_count": 3,
            "is_connected_all_services": True,
            "services_count": 43,
            "all_services_count": 43,
            "goods_count": 0,
        }

    def digest_material(self) -> dict[str, Any]:
        return {
            "product_contract_version": self.version,
            "voucher_template_uuid": self.template_uuid,
            # Historically one number for both sums, and still fed by the face
            # value: for the 15 EUR and 10 EUR contracts the two are equal, so
            # every digest and signature already written over this key keeps its
            # exact bytes. The split is published alongside it, for the contracts
            # that have one, rather than by redefining this.
            "voucher_unit_price_minor": self.face_value_minor,
            "voucher_quantity": self.quantity,
            "workspace_uuid": "e66be240-362c-4fe4-9388-6ed187b27b93",
            "workspace_slug": "kitilash",
            "currency": "EUR",
            "location_uuid": "8395fab6-7ee8-4702-88d9-fd78f92539c1",
            "baseline_version": self.baseline_version,
            "template_facts": self.template_facts(),
            "message_contract_code": self.message_code,
            "meta_template_name": self.meta_template_name,
            "message_language": "de",
            "message_category": "MARKETING",
            "message_parameter_format": "POSITIONAL",
            "message_parameters": ["client_name", "voucher_code", "booking_link"],
            **(
                {
                    "message_body_sha256": hashlib.sha256(CURRENT_PRODUCTION_META_BODY.encode()).hexdigest(),
                    "payment_account_fingerprint": self.payment_account_fingerprint,
                }
                if is_current_fixed_contract(self.request_schema_version)
                else {}
            ),
            # Only where the two sums differ. Adding these keys unconditionally
            # would change the signed bytes of every historical contract, and the
            # whole point of a digest is that it did not change.
            **(
                {
                    "voucher_face_value_minor": self.face_value_minor,
                    "voucher_issue_price_minor": self.issue_price_minor,
                    "product_title": self.title,
                }
                if self.issue_price_minor != self.face_value_minor
                else {}
            ),
        }


LEGACY_PRODUCTION_CONTRACT: Final = ProductionVoucherContract(
    version="easyweek-production-15eur-v1",
    request_schema_version="2",
    template_uuid="49bc000c-c3a6-47c7-bdfd-b8ccd3ae2677",
    face_value_minor=1500,
    issue_price_minor=1500,
    baseline_version="2026-09-27-43",
    message_code="new_client_voucher",
    meta_template_name="kitilash_ka_new_client_voucher_v1",
    validity_months=None,
    payment_account_fingerprint=CURRENT_PAYMENT_ACCOUNT_FINGERPRINT,
    payment_account_label="Card",
)
CURRENT_PRODUCTION_CONTRACT: Final = ProductionVoucherContract(
    version="easyweek-production-10eur-v1",
    request_schema_version="3",
    template_uuid="0ffb0346-57b8-475e-9c22-152dd23e25ca",
    face_value_minor=1000,
    issue_price_minor=1000,
    baseline_version="2026-10-07-10eur-43",
    message_code="new_client_voucher_10eur_v2",
    meta_template_name="kitilash_ka_new_client_voucher_10eur_v2",
    validity_months=1,
    payment_account_fingerprint=CURRENT_PAYMENT_ACCOUNT_FINGERPRINT,
    payment_account_label="Card",
)
# The owner-approved free gift certificate. Same face value as the paid 10 EUR
# product and the same message to the customer; what changed is that issuing one
# costs nothing and settles through Aktionsgutscheine instead of the Card account.
#
# Its own request schema, not a variant of 3. A product version alone must never be
# able to turn a paid contract into a free one, and schema 3 keeps meaning exactly
# what it meant: 1000 minor units of real money through the Card account.
GIFT_PRODUCTION_CONTRACT: Final = ProductionVoucherContract(
    version="easyweek-production-gift-10eur-v1",
    request_schema_version="4",
    template_uuid="0ffb0346-57b8-475e-9c22-152dd23e25ca",
    face_value_minor=1000,
    issue_price_minor=0,
    baseline_version="2026-10-09-gift-10eur-43",
    message_code="new_client_voucher_10eur_v2",
    meta_template_name="kitilash_ka_new_client_voucher_10eur_v2",
    validity_months=1,
    payment_account_fingerprint=GIFT_PAYMENT_ACCOUNT_FINGERPRINT,
    payment_account_label="Aktionsgutscheine",
    title="Kundenkarte - 10€",
)

# Which contract a NEW mailing is prepared under. The owner replaced the paid
# 10 EUR rule with the free gift certificate, so this is the gift contract; schema
# 3 keeps meaning exactly what it meant for anything already written under it.
#
# One name, so the preparation page, the composition read and the stage plan cannot
# disagree about which product an operator is looking at.
NEW_MAILING_SCHEMA_VERSION: Final = "4"


# The request schemas of the FIXED, owner-approved products: the paid 10 EUR one
# and the free gift certificate. Schemas 1 and 2 are the historical 15 EUR contract
# and keep their own, older rules.
#
# One name instead of a literal repeated at every branch. Every guard that reads
# "this is a current fixed contract" — the live Meta proof, the product baseline,
# the till pin, the signed product material, provider-managed validity, the
# terminal stop — asks this, so a new version inherits all of them at once rather
# than inheriting whichever ones somebody remembered to update.
CURRENT_SCHEMA_VERSIONS: Final = frozenset({"3", "4"})


# Every schema whose voucher MAC is bound to the recipient and the frozen
# snapshot, rather than to the batch and slot alone. Schema 1 is deliberately
# absent: its MACs are already issued over the legacy domain and must stay
# verifiable byte for byte.
BOUND_SCHEMA_VERSIONS: Final = frozenset({"2", "3", NEW_MAILING_SCHEMA_VERSION})


def is_current_fixed_contract(schema_version: str) -> bool:
    """Is this request schema one of the fixed, owner-approved products?"""
    return schema_version in CURRENT_SCHEMA_VERSIONS


_BY_SCHEMA: Final = {
    "1": LEGACY_PRODUCTION_CONTRACT,
    "2": LEGACY_PRODUCTION_CONTRACT,
    "3": CURRENT_PRODUCTION_CONTRACT,
    "4": GIFT_PRODUCTION_CONTRACT,
}


def new_mailing_contract() -> ProductionVoucherContract:
    """The product a mailing prepared today is issued under."""
    return production_contract(NEW_MAILING_SCHEMA_VERSION)


def production_contract(schema_version: str = "3", *, contract_version: str | None = None) -> ProductionVoucherContract:
    """The one contract this request schema means, or a refusal.

    The schema decides, and the product version may only AGREE with it. Reversing
    that — letting a product version select a contract — would make the version a
    way to re-price an existing batch, and a free product a way to re-read a paid
    one.
    """
    contract = _BY_SCHEMA.get(schema_version)
    if contract is None:
        raise ValueError("voucher_production_contract_unproven")
    if contract_version is not None and contract_version != contract.version:
        raise ValueError("voucher_production_contract_unproven")
    return contract
