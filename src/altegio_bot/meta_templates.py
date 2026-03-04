"""Meta WhatsApp template resolution and parameter building.

Maps (company_id, job_type) to approved Meta template names and
builds the ordered positional parameter list for each template.

Branch prefixes:
  ka = Karlsruhe  (company_id 758285)
  ra = Rastatt    (company_id 1271200)

Universal templates (no address footer, shared across branches):
  review_3d, repeat_10d, comeback_3d, newsletter_new_clients_monthly
  → always use canonical kitilash_ka_* variant for both companies.

Branch-specific templates (contain address footer):
  record_created, record_updated, record_canceled, reminder_24h, reminder_2h
  → Karlsruhe: kitilash_ka_*
  → Rastatt: kitilash_ra_* where available; others currently fall back
    to kitilash_ka_* and the fallback is logged as a WARNING.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

# Language code sent with every template request.
TEMPLATE_LANGUAGE = "de"

# Job types whose templates have NO address footer and are therefore
# shared across all branches (canonical ka_* is used for everyone).
UNIVERSAL_JOB_TYPES: frozenset[str] = frozenset(
    {
        "review_3d",
        "repeat_10d",
        "comeback_3d",
        "newsletter_new_clients_monthly",
    }
)

# ---------------------------------------------------------------------------
# Template name map: (company_id, job_type) -> Meta template name
# ---------------------------------------------------------------------------
# Karlsruhe mappings
_KA = 758285
# Rastatt mappings
_RA = 1271200

META_TEMPLATE_MAP: dict[tuple[int, str], str] = {
    # --- Karlsruhe ---
    (_KA, "record_created"): "kitilash_ka_record_created_v1",
    (_KA, "record_updated"): "kitilash_ka_record_updated_v1",
    (_KA, "record_canceled"): "kitilash_ka_record_canceled_v1",
    (_KA, "reminder_24h"): "kitilash_ka_reminder_24h_v1",
    (_KA, "reminder_2h"): "kitilash_ka_reminder_2h_v1",
    (_KA, "review_3d"): "kitilash_ka_review_3d_v1",
    (_KA, "repeat_10d"): "kitilash_ka_repeat_10d_v1",
    (_KA, "comeback_3d"): "kitilash_ka_comeback_3d_v1",
    (_KA, "newsletter_new_clients_monthly"): "kitilash_ka_newsletter_new_clients_monthly_v2",
    # --- Rastatt ---
    # ra_record_created_v1 exists; others fall back to ka_* templates
    (_RA, "record_created"): "kitilash_ra_record_created_v1",
    (_RA, "record_updated"): "kitilash_ka_record_updated_v1",
    (_RA, "record_canceled"): "kitilash_ka_record_canceled_v1",
    (_RA, "reminder_24h"): "kitilash_ka_reminder_24h_v1",
    (_RA, "reminder_2h"): "kitilash_ka_reminder_2h_v1",
    (_RA, "review_3d"): "kitilash_ka_review_3d_v1",
    (_RA, "repeat_10d"): "kitilash_ka_repeat_10d_v1",
    (_RA, "comeback_3d"): "kitilash_ka_comeback_3d_v1",
    (_RA, "newsletter_new_clients_monthly"): "kitilash_ka_newsletter_new_clients_monthly_v2",
}

# Karlsruhe new-client variant for record_created
_KA_NEW_CLIENT_TEMPLATE = "kitilash_ka_record_created_new_client_v1"

# Branch-specific job types that have a dedicated ra_* template in Meta WABA.
# All other branch-specific Rastatt types fall back to ka_* (logged as WARNING).
_RA_DEDICATED: frozenset[str] = frozenset({"record_created"})


def resolve_meta_template(
    company_id: int,
    job_type: str,
    *,
    is_new_client: bool = False,
) -> str | None:
    """Return the Meta template name for *company_id* / *job_type*.

    Returns ``None`` when no template is registered (caller should
    fail the job or fall back to free-form text depending on send_mode).

    Resolution rules:
    1. Universal templates (no address footer) always use the canonical
       kitilash_ka_* variant regardless of company_id.
    2. Karlsruhe ``record_created`` with ``is_new_client=True`` uses the
       dedicated new-client variant.
    3. Rastatt branch-specific types without a dedicated ra_* template
       fall back to the matching ka_* template; a WARNING is logged.
    """
    if company_id == _KA and job_type == "record_created" and is_new_client:
        return _KA_NEW_CLIENT_TEMPLATE

    name = META_TEMPLATE_MAP.get((company_id, job_type))

    if name is None:
        logger.warning(
            "No Meta template for company_id=%s job_type=%s",
            company_id,
            job_type,
        )
        return None

    if company_id == _RA and job_type not in UNIVERSAL_JOB_TYPES and job_type not in _RA_DEDICATED:
        logger.warning(
            "Rastatt: no dedicated ra_* template for job_type=%s, using fallback template=%s",
            job_type,
            name,
        )

    return name


def build_template_params(
    template_name: str,
    ctx: dict[str, Any],
) -> list[str]:
    """Build the ordered positional parameter list for *template_name*.

    *ctx* must contain the keys produced by ``_build_msg_ctx`` in
    ``outbox_worker``:
      client_name, staff_name, date, time, services (multiline),
      total_cost, short_link, primary_service, booking_link.

    Returns an empty list for unknown template names (caller logs).
    """
    n = template_name

    # record_created (ka and ra variants share the same param order)
    if n in (
        "kitilash_ka_record_created_v1",
        "kitilash_ka_record_created_new_client_v1",
        "kitilash_ra_record_created_v1",
    ):
        return [
            ctx.get("client_name", ""),
            ctx.get("staff_name", ""),
            ctx.get("date", ""),
            ctx.get("time", ""),
            ctx.get("services", ""),
            ctx.get("total_cost", ""),
            ctx.get("short_link", ""),
        ]

    if n == "kitilash_ka_record_updated_v1":
        return [
            ctx.get("client_name", ""),
            ctx.get("staff_name", ""),
            ctx.get("date", ""),
            ctx.get("time", ""),
            ctx.get("services", ""),
            ctx.get("total_cost", ""),
            ctx.get("short_link", ""),
        ]

    if n == "kitilash_ka_record_canceled_v1":
        return [
            ctx.get("client_name", ""),
            ctx.get("date", ""),
            ctx.get("time", ""),
            ctx.get("services", ""),
            ctx.get("booking_link", ""),
        ]

    if n in ("kitilash_ka_reminder_24h_v1", "kitilash_ka_reminder_2h_v1"):
        return [
            ctx.get("client_name", ""),
            ctx.get("staff_name", ""),
            ctx.get("date", ""),
            ctx.get("time", ""),
            ctx.get("services", ""),
            ctx.get("short_link", ""),
        ]

    if n == "kitilash_ka_review_3d_v1":
        return [
            ctx.get("client_name", ""),
            ctx.get("short_link", ""),
        ]

    if n == "kitilash_ka_repeat_10d_v1":
        return [
            ctx.get("client_name", ""),
            ctx.get("primary_service", ""),
            ctx.get("booking_link", ""),
        ]

    if n == "kitilash_ka_comeback_3d_v1":
        return [
            ctx.get("client_name", ""),
            ctx.get("booking_link", ""),
        ]

    if n == "kitilash_ka_newsletter_new_clients_monthly_v2":
        return [
            ctx.get("client_name", ""),
            ctx.get("booking_link", ""),
            ctx.get("loyalty_card_text", ""),
        ]

    # Legacy v1 – kept for backward compatibility
    if n == "kitilash_ka_newsletter_new_clients_monthly_v1":
        return [
            ctx.get("client_name", ""),
            ctx.get("booking_link", ""),
        ]

    logger.warning("build_template_params: unknown template_name=%s", template_name)
    return []
