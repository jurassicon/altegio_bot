"""Stable Chatwoot route provenance stored in Outbox audit metadata."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from altegio_bot.providers.base import ChatwootRoute

CHATWOOT_ROUTE_META_KEY = "chatwoot_route"

_GENERAL_COMMAND_TEMPLATES = {
    "stop": "wa_cmd_stop",
    "start": "wa_cmd_start",
}

# Exact synchronous producers in ``handle_promo_info_command`` and
# ``handle_promo_command``. Async eligibility/job replies are deliberately not
# included: they carry a MessageJob tenant identity and must stay branch-routed.
_GENERAL_PROMO_TEMPLATES = frozenset(
    {
        "wa_promo_info",
        "wa_promo_lead_expired",
        "wa_promo_lead_repeat_applied",
        "wa_promo_lead_repeat_booked_manual",
        "wa_promo_loyalty_card_issued",
        "wa_promo_loyalty_card_issue_failed",
        "wa_promo_lead_already_issued",
        "wa_promo_lead_rejected_not_new",
        "wa_promo_lead_checking_still_in_progress",
        "wa_promo_lead_manual_check",
        "wa_promo_lead_checking_eligibility",
        "wa_promo_lead_issued",
    }
)


def outbox_meta_with_chatwoot_route(
    meta: Mapping[str, Any],
    route: ChatwootRoute,
) -> dict[str, Any]:
    """Return audit metadata carrying one centralized route marker."""
    return {**meta, CHATWOOT_ROUTE_META_KEY: route.value}


def outbox_has_chatwoot_route_marker(meta: object) -> bool:
    """Detect marker presence without accepting malformed JSON shapes."""
    return isinstance(meta, Mapping) and CHATWOOT_ROUTE_META_KEY in meta


def _matches_general_provenance(template_code: object, meta: Mapping[str, Any]) -> bool:
    source = meta.get("source")
    command = meta.get("command")
    if source == "inbound_command":
        return isinstance(command, str) and _GENERAL_COMMAND_TEMPLATES.get(command) == template_code
    if source == "promo_lead" and command == "promo":
        return template_code in _GENERAL_PROMO_TEMPLATES
    return False


def resolve_jobless_bot_outbox_route(
    *,
    message_source: object,
    job_id: object,
    provider_message_id: object,
    template_code: object,
    meta: object,
) -> tuple[ChatwootRoute | None, str | None]:
    """Prove explicit/legacy General for one identity-less internal bot row.

    Historical rows are accepted only through the exact source-controlled
    producer shape. A present marker never overrides contradictory provenance.
    """
    if message_source != "bot" or job_id is not None:
        return None, "bot_job_identity_missing"
    if not isinstance(provider_message_id, str) or not provider_message_id.strip():
        return None, "bot_job_identity_missing"

    audit_meta: Mapping[str, Any] = meta if isinstance(meta, Mapping) else {}
    marker_present = CHATWOOT_ROUTE_META_KEY in audit_meta
    provenance_matches = _matches_general_provenance(template_code, audit_meta)

    if marker_present:
        if audit_meta.get(CHATWOOT_ROUTE_META_KEY) != ChatwootRoute.GENERAL.value:
            return None, "invalid_outbox_route_marker"
        if not provenance_matches:
            return None, "general_outbox_provenance_mismatch"
        return ChatwootRoute.GENERAL, None

    if provenance_matches:
        return ChatwootRoute.GENERAL, None
    return None, "bot_job_identity_missing"
