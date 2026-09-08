"""Provider boundary shared by campaign orchestration and workers (PR-13)."""

from __future__ import annotations

from typing import Final

from altegio_bot.models.models import PROVIDER_ALTEGIO, PROVIDER_EASYWEEK

EASYWEEK_CAMPAIGN_SEGMENT_NOT_IMPLEMENTED: Final = "easyweek_campaign_segment_not_implemented"
CAMPAIGN_LIVE_GUARD_UNPROVEN: Final = "campaign_live_guard_unproven"
CAMPAIGN_EXECUTION_NOT_AUTHORIZED: Final = "campaign_execution_not_authorized"
CAMPAIGN_PROVIDER_UNKNOWN: Final = "campaign_provider_unknown"
CAMPAIGN_PROVIDER_MISMATCH: Final = "campaign_provider_mismatch"
CAMPAIGN_IDENTITY_MISMATCH: Final = "campaign_identity_mismatch"

CAMPAIGN_JOB_TYPES: frozenset[str] = frozenset(
    {
        "campaign_execute_new_clients_monthly",
        "newsletter_new_clients_monthly",
        "newsletter_new_clients_followup",
    }
)


class CampaignProviderRefusal(ValueError):
    """A stable, PII-free refusal at a campaign provider boundary."""

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)


def validate_campaign_provider(provider: object) -> str:
    """Return the exact supported provider or refuse unknown/implicit input."""
    if provider in (PROVIDER_ALTEGIO, PROVIDER_EASYWEEK):
        return str(provider)
    raise CampaignProviderRefusal(CAMPAIGN_PROVIDER_UNKNOWN)


def require_campaign_execution_provider(provider: object) -> str:
    """Allow the implemented Altegio campaign engine and refuse everything else."""
    exact = validate_campaign_provider(provider)
    if exact == PROVIDER_EASYWEEK:
        raise CampaignProviderRefusal(EASYWEEK_CAMPAIGN_SEGMENT_NOT_IMPLEMENTED)
    return exact


def campaign_provider_refusal(provider: object) -> str | None:
    """Return a stable refusal reason without raising, for worker terminalization."""
    try:
        require_campaign_execution_provider(provider)
    except CampaignProviderRefusal as exc:
        return exc.reason
    return None


def require_same_provider(*providers: object) -> str:
    """Prove that every campaign row in a relationship has one provider."""
    if not providers:
        raise CampaignProviderRefusal(CAMPAIGN_IDENTITY_MISMATCH)
    exact = tuple(validate_campaign_provider(provider) for provider in providers)
    if len(set(exact)) != 1:
        raise CampaignProviderRefusal(CAMPAIGN_PROVIDER_MISMATCH)
    return exact[0]


def campaign_dedupe_key(
    *,
    provider: object,
    job_type: str,
    company_id: int,
    record_id: int | None,
    run_at_iso: str,
) -> str:
    """Provider-scoped identity for newly-created campaign jobs only."""
    exact = validate_campaign_provider(provider)
    rid = int(record_id) if record_id is not None else 0
    return f"{exact}:{job_type}:{company_id}:{rid}:{run_at_iso}"
