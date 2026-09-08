"""Read-only EasyWeek voucher-template evidence for PR-13."""

from __future__ import annotations

import inspect

import pytest

from altegio_bot.campaigns import gift_card_readiness as readiness_module
from altegio_bot.campaigns.gift_card_readiness import (
    EASYWEEK_VOUCHER_TEMPLATE_UUID,
    GIFT_CARD_BRANCH_SCOPE_MISMATCH,
    GIFT_CARD_ISSUE_CONTRACT_UNPROVEN,
    GIFT_CARD_ONLINE_SALES_DISABLED,
    GIFT_CARD_PUBLIC_URL_UNPROVEN,
    GIFT_CARD_SEMANTICS_UNPROVEN,
    GIFT_CARD_WORKSPACE_MISMATCH,
    KARLSRUHE_LOCATION_UUID,
    evaluate_gift_card_readiness,
    probe_gift_card_readiness,
)

WORKSPACE = {
    "uuid": "e66be240-362c-4fe4-9388-6ed187b27b93",
    "slug": "kitilash",
    "domain": "kitilash.easyweek.de",
    "currency": "EUR",
    "country_iso": "Germany",
}
LOCATIONS = [
    {
        "uuid": KARLSRUHE_LOCATION_UUID,
        "name": "KitiLash Karlsruhe",
        "timezone": "Europe/Berlin",
    }
]
TEMPLATE = {
    "uuid": EASYWEEK_VOUCHER_TEMPLATE_UUID,
    "title": "Kundenkarte - 10%",
    "cost": 1500,
    "value": 1500,
    "is_single_charge": True,
    "validity": None,
    "is_enabled": True,
    "is_online": False,
    "is_shown_on_website": True,
    "is_shown_in_marketplace": True,
    "is_connected_all_branches": True,
    "branches_count": 3,
    "all_branches_count": 3,
    "is_connected_all_services": True,
    "services_count": 43,
    "all_services_count": 43,
    "has_limit_of_sales": False,
    "sales_limit": None,
    "vouchers_count": 0,
    "activated_vouchers_count": 0,
}


def _evaluate(**template_changes):
    return evaluate_gift_card_readiness(
        workspace_payload=WORKSPACE,
        locations_payload=LOCATIONS,
        template_payload={**TEMPLATE, **template_changes},
    )


def test_only_confirmed_uuid_is_kept_as_api_identity() -> None:
    assert EASYWEEK_VOUCHER_TEMPLATE_UUID == "49bc000c-c3a6-47c7-bdfd-b8ccd3ae2677"
    source = inspect.getsource(readiness_module)
    assert "33701" not in source
    assert "/promote/gift-cards/" not in source


def test_confirmed_read_only_payload_remains_fail_closed() -> None:
    result = _evaluate()

    assert result.ready is False
    assert result.monetary_value == "15"
    assert result.branch_scope == "all_branches"
    assert result.validity == "unproven_or_unlimited_per_api"
    assert result.customer_purchase_url is None
    assert set(result.reasons) >= {
        GIFT_CARD_ONLINE_SALES_DISABLED,
        GIFT_CARD_PUBLIC_URL_UNPROVEN,
        GIFT_CARD_SEMANTICS_UNPROVEN,
        GIFT_CARD_ISSUE_CONTRACT_UNPROVEN,
    }


def test_wrong_workspace_blocks_readiness() -> None:
    result = evaluate_gift_card_readiness(
        workspace_payload={**WORKSPACE, "uuid": "00000000-0000-0000-0000-000000000000"},
        locations_payload=LOCATIONS,
        template_payload=TEMPLATE,
    )
    assert GIFT_CARD_WORKSPACE_MISMATCH in result.reasons


def test_all_branches_template_does_not_prove_karlsruhe_only_scope() -> None:
    result = evaluate_gift_card_readiness(
        workspace_payload=WORKSPACE,
        locations_payload=LOCATIONS,
        template_payload=TEMPLATE,
        required_location_uuid=KARLSRUHE_LOCATION_UUID,
    )
    assert GIFT_CARD_BRANCH_SCOPE_MISMATCH in result.reasons


def test_website_visibility_does_not_bypass_online_sales_blocker() -> None:
    result = _evaluate(is_shown_on_website=True, is_shown_in_marketplace=True, is_online=False)
    assert GIFT_CARD_ONLINE_SALES_DISABLED in result.reasons
    assert result.ready is False


def test_missing_public_url_is_not_synthesized_from_known_identifiers() -> None:
    result = _evaluate()
    assert GIFT_CARD_PUBLIC_URL_UNPROVEN in result.reasons
    assert result.customer_purchase_url is None


def test_percentage_title_does_not_replace_machine_readable_semantics() -> None:
    result = _evaluate(title="Kundenkarte - 10%", cost=1500, value=1500)
    assert result.monetary_value == "15"
    assert GIFT_CARD_SEMANTICS_UNPROVEN in result.reasons


@pytest.mark.parametrize(
    "counts",
    [
        {"vouchers_count": 0, "activated_vouchers_count": 0},
        {"vouchers_count": 1, "activated_vouchers_count": 1},
    ],
)
def test_template_counts_never_prove_an_issue_contract(counts) -> None:
    result = _evaluate(**counts)
    assert GIFT_CARD_ISSUE_CONTRACT_UNPROVEN in result.reasons


@pytest.mark.asyncio
async def test_probe_uses_only_reviewed_get_operations_and_exact_uuid() -> None:
    calls: list[tuple[str, str | None]] = []

    class ReadOnlyClient:
        async def get_workspace(self):
            calls.append(("GET workspace", None))
            return WORKSPACE

        async def list_locations(self):
            calls.append(("GET locations", None))
            return LOCATIONS

        async def list_voucher_templates(self):
            calls.append(("GET voucher-templates", None))
            return [TEMPLATE]

        async def get_voucher_template(self, voucher_template_uuid: str):
            calls.append(("GET voucher-template", voucher_template_uuid))
            return TEMPLATE

    result = await probe_gift_card_readiness(ReadOnlyClient())

    assert result.ready is False
    assert calls == [
        ("GET workspace", None),
        ("GET locations", None),
        ("GET voucher-templates", None),
        ("GET voucher-template", EASYWEEK_VOUCHER_TEMPLATE_UUID),
    ]


@pytest.mark.asyncio
async def test_probe_does_not_fetch_unlisted_template() -> None:
    class MissingTemplateClient:
        async def get_workspace(self):
            return WORKSPACE

        async def list_locations(self):
            return LOCATIONS

        async def list_voucher_templates(self):
            return []

        async def get_voucher_template(self, voucher_template_uuid: str):
            raise AssertionError("unlisted UUID must not be fetched")

    result = await probe_gift_card_readiness(MissingTemplateClient())
    assert result.ready is False
    assert "gift_card_template_unproven" in result.reasons
