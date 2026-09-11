"""Fences the voucher-calculation evidence PR must NOT move.

Everything here is a regression guard: the readiness endpoint stays GET-only,
EasyWeek campaign execution stays refused, the Altegio path stays untouched, and
no production identity reaches the repository.
"""

from __future__ import annotations

import inspect
import re
from pathlib import Path

import pytest

from altegio_bot import easyweek_voucher_calculation as calculation_module
from altegio_bot import easyweek_voucher_identity as identity_module
from altegio_bot.campaigns import gift_card_readiness as gift_card_module
from altegio_bot.campaigns.easyweek_voucher_contract import (
    GIFT_CARD_CALCULATION_CONTRACT_UNPROVEN,
    probe_voucher_calculation_contract,
)
from altegio_bot.campaigns.gift_card_readiness import (
    GIFT_CARD_ISSUE_CONTRACT_UNPROVEN,
    GIFT_CARD_ONLINE_SALES_DISABLED,
    GIFT_CARD_PUBLIC_URL_UNPROVEN,
    GIFT_CARD_SEMANTICS_UNPROVEN,
    evaluate_gift_card_readiness,
)
from altegio_bot.campaigns.provider import (
    CAMPAIGN_EXECUTION_NOT_AUTHORIZED,
    CAMPAIGN_JOB_TYPES,
    CampaignProviderRefusal,
    campaign_provider_refusal,
    require_campaign_execution_provider,
)
from altegio_bot.easyweek_client import EasyWeekClient
from altegio_bot.ops import campaigns_api
from altegio_bot.tests.easyweek_voucher_evidence_fixtures import (
    LOCATIONS,
    TEMPLATE,
    WORKSPACE,
    code_without_docstrings,
    imported_modules,
)

REPO_ROOT = Path(__file__).resolve().parents[3]

# Shipped code, fixtures and documentation added or touched by this work.
SHIPPED_ARTIFACTS = (
    REPO_ROOT / "src/altegio_bot/easyweek_voucher_identity.py",
    REPO_ROOT / "src/altegio_bot/easyweek_voucher_calculation.py",
    REPO_ROOT / "src/altegio_bot/campaigns/easyweek_voucher_contract.py",
    REPO_ROOT / "src/altegio_bot/scripts/easyweek_voucher_contract_preflight.py",
    REPO_ROOT / "src/altegio_bot/tests/easyweek_voucher_evidence_fixtures.py",
    REPO_ROOT / "docs/easyweek/voucher_contract_evidence_runbook.md",
    REPO_ROOT / "docs/easyweek/campaign_readiness_runbook.md",
)
NEW_TEST_FILES = (
    REPO_ROOT / "src/altegio_bot/tests/test_easyweek_voucher_calculation.py",
    REPO_ROOT / "src/altegio_bot/tests/campaigns/test_easyweek_voucher_contract.py",
    REPO_ROOT / "src/altegio_bot/tests/test_easyweek_voucher_contract_preflight.py",
)

# Assembled from fragments so this guard file does not itself become the place
# where the forbidden literals live.
FORBIDDEN_DASHBOARD_ID = "337" + "01"
FORBIDDEN_ADMIN_PATH = "/promote/" + "gift-cards/"


# ---------------------------------------------------------------------------
# The readiness endpoint stays GET-only
# ---------------------------------------------------------------------------


def test_the_readiness_endpoint_cannot_reach_the_calculate_client() -> None:
    imported = imported_modules(campaigns_api)
    assert "altegio_bot.easyweek_voucher_calculation" not in imported
    assert "altegio_bot.campaigns.easyweek_voucher_contract" not in imported

    source = inspect.getsource(campaigns_api)
    assert "orders/calculate" not in source
    assert "calculate_single_voucher" not in source


def test_the_readiness_probe_still_uses_the_get_only_client() -> None:
    # Structural, not incidental: the client the endpoint constructs has no POST.
    for name in ("post", "put", "patch", "delete", "request"):
        assert not hasattr(EasyWeekClient, name), name

    imported = imported_modules(gift_card_module)
    assert not any("voucher_calculation" in name for name in imported)


def test_gift_card_readiness_keeps_every_existing_blocker() -> None:
    result = evaluate_gift_card_readiness(
        workspace_payload=WORKSPACE,
        locations_payload=LOCATIONS,
        template_payload=TEMPLATE,
    )

    assert result.ready is False
    assert GIFT_CARD_ONLINE_SALES_DISABLED in result.reasons
    assert GIFT_CARD_PUBLIC_URL_UNPROVEN in result.reasons
    assert GIFT_CARD_SEMANTICS_UNPROVEN in result.reasons
    assert GIFT_CARD_ISSUE_CONTRACT_UNPROVEN in result.reasons


def test_a_proven_calculation_does_not_relax_gift_card_readiness() -> None:
    # The two evaluators share no state and no reason: one cannot unblock the
    # other, whatever the calculation proved.
    readiness_reasons = set(
        evaluate_gift_card_readiness(
            workspace_payload=WORKSPACE,
            locations_payload=LOCATIONS,
            template_payload=TEMPLATE,
        ).reasons
    )
    assert GIFT_CARD_CALCULATION_CONTRACT_UNPROVEN not in readiness_reasons
    assert GIFT_CARD_ISSUE_CONTRACT_UNPROVEN in readiness_reasons

    signature = inspect.signature(evaluate_gift_card_readiness)
    assert "calculation" not in " ".join(signature.parameters)


# ---------------------------------------------------------------------------
# Campaign execution stays refused
# ---------------------------------------------------------------------------


def test_easyweek_campaign_execution_is_still_refused() -> None:
    with pytest.raises(CampaignProviderRefusal):
        require_campaign_execution_provider("easyweek")
    assert campaign_provider_refusal("easyweek") is not None
    assert CAMPAIGN_EXECUTION_NOT_AUTHORIZED == "campaign_execution_not_authorized"


def test_the_altegio_campaign_path_is_untouched() -> None:
    assert require_campaign_execution_provider("altegio") == "altegio"
    assert campaign_provider_refusal("altegio") is None
    assert CAMPAIGN_JOB_TYPES == frozenset(
        {
            "campaign_execute_new_clients_monthly",
            "newsletter_new_clients_monthly",
            "newsletter_new_clients_followup",
        }
    )


def test_the_evidence_probe_creates_no_job_recipient_or_outbox() -> None:
    source = inspect.getsource(probe_voucher_calculation_contract)
    for forbidden in ("CampaignRun", "CampaignRecipient", "MessageJob", "Outbox", "session"):
        assert forbidden not in source, forbidden


# ---------------------------------------------------------------------------
# No production identity in the repository
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("path", SHIPPED_ARTIFACTS + NEW_TEST_FILES, ids=lambda path: path.name)
def test_no_dashboard_identity_or_admin_url_is_committed(path: Path) -> None:
    text = path.read_text()
    # The numeric dashboard id is not an API identity and must not appear in
    # code, in a fixture, in an endpoint or in documentation.
    assert FORBIDDEN_DASHBOARD_ID not in text
    assert FORBIDDEN_ADMIN_PATH not in text
    # An administrative edit page is never customer-facing evidence.
    assert not re.search(r"my\.easyweek\.io/[a-z]+/gift", text)


@pytest.mark.parametrize("path", SHIPPED_ARTIFACTS + NEW_TEST_FILES, ids=lambda path: path.name)
def test_only_confirmed_or_obviously_synthetic_uuids_appear(path: Path) -> None:
    allowed = {
        gift_card_module.EASYWEEK_WORKSPACE_UUID,
        gift_card_module.KARLSRUHE_LOCATION_UUID,
        gift_card_module.EASYWEEK_VOUCHER_TEMPLATE_UUID,
        # Obviously synthetic values used only to prove a refusal.
        "00000000-0000-0000-0000-000000000000",
        "11111111-2222-4333-8444-555555555555",
    }
    pattern = r"[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}"
    found = {match.lower() for match in re.findall(pattern, path.read_text())}
    assert found <= allowed, found - allowed


def test_no_new_alembic_migration_is_added() -> None:
    versions = REPO_ROOT / "alembic" / "versions"
    assert versions.is_dir()
    for path in versions.glob("*.py"):
        text = path.read_text()
        assert "voucher_calculation" not in text
        assert "voucher_contract" not in text


# ---------------------------------------------------------------------------
# The pinned identity module stays a literal, not a configuration layer
# ---------------------------------------------------------------------------


def test_the_identity_module_is_literals_only() -> None:
    """Constants a reviewer sees in a diff, not values a deployment can change."""
    imported = imported_modules(identity_module)
    assert imported == {"__future__", "typing"}, imported

    code = code_without_docstrings(identity_module)
    for forbidden in ("os.environ", "getenv", "settings", "Settings", "def ", "class "):
        assert forbidden not in code, forbidden


def test_the_readiness_module_uses_the_same_literals() -> None:
    # One source of truth: a divergence here would let readiness and the
    # calculation evidence disagree about which product they are talking about.
    assert gift_card_module.EASYWEEK_WORKSPACE_UUID is identity_module.EASYWEEK_WORKSPACE_UUID
    assert gift_card_module.EASYWEEK_WORKSPACE_SLUG is identity_module.EASYWEEK_WORKSPACE_SLUG
    assert gift_card_module.KARLSRUHE_LOCATION_UUID is identity_module.KARLSRUHE_LOCATION_UUID
    assert gift_card_module.EASYWEEK_VOUCHER_TEMPLATE_UUID is identity_module.EASYWEEK_VOUCHER_TEMPLATE_UUID


def test_the_transport_is_pinned_to_those_literals() -> None:
    assert calculation_module.VOUCHER_QUANTITY == 1
    assert identity_module.SUPPORTED_VOUCHER_PRICE_MINOR == 1500
    assert identity_module.SUPPORTED_VOUCHER_QUANTITY == 1
    imported = imported_modules(calculation_module)
    assert "altegio_bot.easyweek_voucher_identity" in imported
    # A transport module must not depend on the campaigns package.
    assert not any(name.startswith("altegio_bot.campaigns") for name in imported), imported


def test_the_calculate_client_owns_its_transport_and_never_redirects() -> None:
    parameters = inspect.signature(calculation_module.EasyWeekVoucherCalculationClient.__init__).parameters
    assert "http_client" not in parameters
    code = code_without_docstrings(calculation_module)
    assert "follow_redirects=False" in code
    assert "follow_redirects=True" not in code


# ---------------------------------------------------------------------------
# The runbook says what the code actually does
# ---------------------------------------------------------------------------


EVIDENCE_RUNBOOK = REPO_ROOT / "docs/easyweek/voucher_contract_evidence_runbook.md"


def test_the_runbook_documents_both_operator_commands() -> None:
    text = EVIDENCE_RUNBOOK.read_text()
    assert "uv run python -m altegio_bot.scripts.easyweek_voucher_contract_preflight" in text
    # The production-native form runs through the existing compose topology.
    assert "docker compose -p altegio_bot run --rm --no-deps" in text
    assert "altegio-outbox-worker" in text
    assert "--confirm-nonpersistent-calculate" in text


def test_the_runbook_states_the_unknown_and_help_exit_semantics() -> None:
    text = EVIDENCE_RUNBOOK.read_text()
    assert "do not auto-retry" in text.casefold()
    assert "`--help` returns" in text or "`--help` exits" in text or "`--help` also exits" in text
    # It must not describe exit 3 as a retryable condition any more.
    assert "Retryable API uncertainty" not in text


def test_the_runbook_describes_the_literal_pinned_transport() -> None:
    text = EVIDENCE_RUNBOOK.read_text()
    assert "literal-pinned" in text.casefold()
    assert "Redirects are never followed" in text
    for invariant in ("promocode", "promocode_discount_amount", "taxes", "template state"):
        assert invariant in text, invariant


def test_the_runbook_keeps_every_send_blocker() -> None:
    text = EVIDENCE_RUNBOOK.read_text()
    for blocker in (
        "individual_voucher_artifact_proven",
        "customer_binding_proven",
        "write_idempotency_proven",
        "unknown_result_reconciliation_proven",
        "delivery_authorized",
        "ready_for_send",
        "issue_contract_ready",
    ):
        assert blocker in text, blocker
    assert "is not send authorization" in text or "not permission to" in text


def test_the_runbook_claims_no_absolute_consistency_guarantee() -> None:
    text = EVIDENCE_RUNBOOK.read_text()
    assert "not an absolute consistency" in text
