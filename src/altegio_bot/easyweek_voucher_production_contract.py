"""Two fixed production products; request versions and product versions are distinct.

This is deliberately dependency-free. Historical callers keep the 15 EUR
contract; only an explicit schema 3 selects the owner-approved 10 EUR product.
There are no configuration overrides or mutable product registries.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any, Final

CURRENT_PAYMENT_ACCOUNT_FINGERPRINT: Final = "77d6ada907150fa5e2620a0ef3571333c6c07d3c6bdda9a2fa0531ad0b0c3d64"

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
    unit_price_minor: int
    baseline_version: str
    message_code: str
    meta_template_name: str
    validity_months: int | None
    quantity: int = 1

    def template_facts(self) -> dict[str, Any]:
        return {
            "is_enabled": True,
            "is_online": False,
            "is_single_charge": True,
            "cost": self.unit_price_minor,
            "value": self.unit_price_minor,
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
            "voucher_unit_price_minor": self.unit_price_minor,
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
                    "payment_account_fingerprint": CURRENT_PAYMENT_ACCOUNT_FINGERPRINT,
                }
                if self.request_schema_version == "3"
                else {}
            ),
        }


LEGACY_PRODUCTION_CONTRACT: Final = ProductionVoucherContract(
    version="easyweek-production-15eur-v1",
    request_schema_version="2",
    template_uuid="49bc000c-c3a6-47c7-bdfd-b8ccd3ae2677",
    unit_price_minor=1500,
    baseline_version="2026-09-27-43",
    message_code="new_client_voucher",
    meta_template_name="kitilash_ka_new_client_voucher_v1",
    validity_months=None,
)
CURRENT_PRODUCTION_CONTRACT: Final = ProductionVoucherContract(
    version="easyweek-production-10eur-v1",
    request_schema_version="3",
    template_uuid="0ffb0346-57b8-475e-9c22-152dd23e25ca",
    unit_price_minor=1000,
    baseline_version="2026-10-07-10eur-43",
    message_code="new_client_voucher_10eur_v2",
    meta_template_name="kitilash_ka_new_client_voucher_10eur_v2",
    validity_months=1,
)


def production_contract(schema_version: str = "3", *, contract_version: str | None = None) -> ProductionVoucherContract:
    if schema_version in ("1", "2"):
        contract = LEGACY_PRODUCTION_CONTRACT
    elif schema_version == "3":
        contract = CURRENT_PRODUCTION_CONTRACT
    else:
        raise ValueError("voucher_production_contract_unproven")
    if contract_version is not None and contract_version != contract.version:
        raise ValueError("voucher_production_contract_unproven")
    return contract
