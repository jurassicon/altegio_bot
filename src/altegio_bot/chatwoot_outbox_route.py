"""Stable Chatwoot route provenance stored in Outbox audit metadata."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from altegio_bot.providers.base import ChatwootRoute

CHATWOOT_ROUTE_META_KEY = "chatwoot_route"

# PR-7.4. The single-inbox operator relay picks a sender the operator named in
# the environment. That sender is a TRANSPORT line — the only active row on the
# shared Meta number that may carry the reply — and deliberately NOT proof of
# which branch the customer belongs to: during the rollback there is no such
# proof, which is the whole reason the setting exists.
#
# Without provenance the row would lie later. `_get_outbox_context_target` reads
# an operator Outbox's `WhatsAppSender` as authoritative tenant evidence, so once
# the branch map and `affinity` come back, a customer replying to a message sent
# during the rollback would be dragged into the transport sender's branch inbox.
# This marker records "General route, sender was transport only" so the reply and
# the reaction both stay in General.
SINGLE_INBOX_RELAY_META_KEY = "single_inbox_relay"
SINGLE_INBOX_RELAY_SENDER_SCOPE = "transport_only"

# The only template codes the operator relay itself writes. A marker on anything
# else is not a relay row and is not believed.
_SINGLE_INBOX_RELAY_TEMPLATES = frozenset({"operator_relay", "operator_reopen_template"})

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


def outbox_meta_with_single_inbox_general_route(meta: Mapping[str, Any]) -> dict[str, Any]:
    """Stamp one operator-relay row as General-routed with a transport sender.

    Both halves are written together and are meaningless apart: the centralized
    route marker says WHERE the row belongs, the provenance says WHY its sender
    proves nothing. :func:`resolve_operator_outbox_route` accepts only the pair.
    """
    return {
        **meta,
        CHATWOOT_ROUTE_META_KEY: ChatwootRoute.GENERAL.value,
        SINGLE_INBOX_RELAY_META_KEY: {
            "route": ChatwootRoute.GENERAL.value,
            "sender_scope": SINGLE_INBOX_RELAY_SENDER_SCOPE,
        },
    }


def _matches_single_inbox_relay_provenance(value: object) -> bool:
    """Exact shape only — no extra keys, no coercion, no partial credit."""
    if not isinstance(value, Mapping):
        return False
    if set(value) != {"route", "sender_scope"}:
        return False
    return (
        value.get("route") == ChatwootRoute.GENERAL.value
        and value.get("sender_scope") == SINGLE_INBOX_RELAY_SENDER_SCOPE
    )


def resolve_operator_outbox_route(
    *,
    message_source: object,
    job_id: object,
    template_code: object,
    sender_id: object,
    meta: object,
) -> tuple[ChatwootRoute | None, str | None]:
    """Prove the route of ONE operator Outbox row before its sender is read.

    Three outcomes, and nothing in between:

    * ``(TENANT, None)`` — no route provenance at all. Every operator row
      written before this hotfix looks like this, and keeps resolving its tenant
      through ``WhatsAppSender`` exactly as before.
    * ``(GENERAL, None)`` — the exact shape the single-inbox relay writes, on a
      row that is actually an operator-relay row with a sender and no job. The
      caller must NOT read the sender as tenant evidence.
    * ``(None, reason)`` — a partial, contradictory or foreign marker. Half a
      proof is not a proof: it fails closed rather than falling back to the
      sender, because falling back is precisely the wrong-branch bug.

    A marker alone still never overrides sender identity — that stayed an audit
    conflict. What changed is that the relay now writes a second, matching field
    that says why the sender must be ignored.
    """
    audit_meta: Mapping[str, Any] = meta if isinstance(meta, Mapping) else {}
    marker_present = CHATWOOT_ROUTE_META_KEY in audit_meta
    provenance_present = SINGLE_INBOX_RELAY_META_KEY in audit_meta

    if not marker_present and not provenance_present:
        return ChatwootRoute.TENANT, None

    if not (marker_present and provenance_present):
        # Exactly one half present: a hand-edited row, a truncated write, or a
        # foreign marker. Never resolved by preferring whichever half exists.
        return None, "operator_route_marker_conflict"

    if audit_meta.get(CHATWOOT_ROUTE_META_KEY) != ChatwootRoute.GENERAL.value:
        return None, "invalid_outbox_route_marker"

    if not _matches_single_inbox_relay_provenance(audit_meta.get(SINGLE_INBOX_RELAY_META_KEY)):
        return None, "operator_route_marker_conflict"

    # The row must also BE what the provenance claims it is.
    if message_source != "operator" or job_id is not None:
        return None, "operator_route_marker_conflict"
    if template_code not in _SINGLE_INBOX_RELAY_TEMPLATES:
        return None, "operator_route_marker_conflict"
    if sender_id is None:
        return None, "operator_route_marker_conflict"

    return ChatwootRoute.GENERAL, None
