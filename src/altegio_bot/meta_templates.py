"""Meta WhatsApp template resolution and parameter building.

Maps (company_id, job_type) to approved Meta template names and
builds the ordered positional parameter list for each template.

Branch prefixes:
  ka = Karlsruhe  (company_id 758285)
  ra = Rastatt    (company_id 1271200)

Rastatt currently only has kitilash_ra_record_created_v1 registered
in Meta WABA.  All other Rastatt job types fall back to the matching
kitilash_ka_* template and this is explicitly logged.
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

# Language code sent with every template request.
TEMPLATE_LANGUAGE = 'de'

# ---------------------------------------------------------------------------
# Template name map: (company_id, job_type) -> Meta template name
# ---------------------------------------------------------------------------
# Karlsruhe mappings
_KA = 758285
# Rastatt mappings
_RA = 1271200

META_TEMPLATE_MAP: dict[tuple[int, str], str] = {
    # --- Karlsruhe ---
    (_KA, 'record_created'):
        'kitilash_ka_record_created_v1',
    (_KA, 'record_updated'):
        'kitilash_ka_record_updated_v1',
    (_KA, 'record_canceled'):
        'kitilash_ka_record_canceled_v1',
    (_KA, 'reminder_24h'):
        'kitilash_ka_reminder_24h_v1',
    (_KA, 'reminder_2h'):
        'kitilash_ka_reminder_2h_v1',
    (_KA, 'review_3d'):
        'kitilash_ka_review_3d_v1',
    (_KA, 'repeat_10d'):
        'kitilash_ka_repeat_10d_v1',
    (_KA, 'comeback_3d'):
        'kitilash_ka_comeback_3d_v1',
    (_KA, 'newsletter_new_clients_monthly'):
        'kitilash_ka_newsletter_new_clients_monthly_v1',
    # --- Rastatt ---
    # ra_record_created_v1 exists; others fall back to ka_* templates
    (_RA, 'record_created'):
        'kitilash_ra_record_created_v1',
    (_RA, 'record_updated'):
        'kitilash_ka_record_updated_v1',
    (_RA, 'record_canceled'):
        'kitilash_ka_record_canceled_v1',
    (_RA, 'reminder_24h'):
        'kitilash_ka_reminder_24h_v1',
    (_RA, 'reminder_2h'):
        'kitilash_ka_reminder_2h_v1',
    (_RA, 'review_3d'):
        'kitilash_ka_review_3d_v1',
    (_RA, 'repeat_10d'):
        'kitilash_ka_repeat_10d_v1',
    (_RA, 'comeback_3d'):
        'kitilash_ka_comeback_3d_v1',
    (_RA, 'newsletter_new_clients_monthly'):
        'kitilash_ka_newsletter_new_clients_monthly_v1',
}

# Karlsruhe new-client variant for record_created
_KA_NEW_CLIENT_TEMPLATE = 'kitilash_ka_record_created_new_client_v1'

# Template names that are actually owned by a different branch (fallbacks).
# Used purely for log warnings.
_RA_FALLBACKS: frozenset[str] = frozenset({
    'record_updated',
    'record_canceled',
    'reminder_24h',
    'reminder_2h',
    'review_3d',
    'repeat_10d',
    'comeback_3d',
    'newsletter_new_clients_monthly',
})


def resolve_meta_template(
    company_id: int,
    job_type: str,
    *,
    is_new_client: bool = False,
) -> str | None:
    """Return the Meta template name for *company_id* / *job_type*.

    Returns ``None`` when no template is registered (caller should fall
    back to free-form text and log a warning).

    For Karlsruhe ``record_created`` with ``is_new_client=True`` the
    dedicated new-client template is returned.

    Rastatt job types without a dedicated ra_* template fall back to
    the matching ka_* template; a WARNING is logged.
    """
    if (
        company_id == _KA
        and job_type == 'record_created'
        and is_new_client
    ):
        return _KA_NEW_CLIENT_TEMPLATE

    name = META_TEMPLATE_MAP.get((company_id, job_type))

    if name is None:
        logger.warning(
            'No Meta template for company_id=%s job_type=%s',
            company_id,
            job_type,
        )
        return None

    if company_id == _RA and job_type in _RA_FALLBACKS:
        logger.warning(
            'Rastatt: no ra_* template for job_type=%s, '
            'using fallback template=%s',
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
        'kitilash_ka_record_created_v1',
        'kitilash_ka_record_created_new_client_v1',
        'kitilash_ra_record_created_v1',
    ):
        return [
            ctx.get('client_name', ''),
            ctx.get('staff_name', ''),
            ctx.get('date', ''),
            ctx.get('time', ''),
            ctx.get('services', ''),
            ctx.get('total_cost', ''),
            ctx.get('short_link', ''),
        ]

    if n == 'kitilash_ka_record_updated_v1':
        return [
            ctx.get('client_name', ''),
            ctx.get('staff_name', ''),
            ctx.get('date', ''),
            ctx.get('time', ''),
            ctx.get('services', ''),
            ctx.get('total_cost', ''),
            ctx.get('short_link', ''),
        ]

    if n == 'kitilash_ka_record_canceled_v1':
        return [
            ctx.get('client_name', ''),
            ctx.get('date', ''),
            ctx.get('time', ''),
            ctx.get('services', ''),
            ctx.get('booking_link', ''),
        ]

    if n in ('kitilash_ka_reminder_24h_v1', 'kitilash_ka_reminder_2h_v1'):
        return [
            ctx.get('client_name', ''),
            ctx.get('staff_name', ''),
            ctx.get('date', ''),
            ctx.get('time', ''),
            ctx.get('services', ''),
            ctx.get('short_link', ''),
        ]

    if n == 'kitilash_ka_review_3d_v1':
        return [
            ctx.get('client_name', ''),
            ctx.get('short_link', ''),
        ]

    if n == 'kitilash_ka_repeat_10d_v1':
        return [
            ctx.get('client_name', ''),
            ctx.get('primary_service', ''),
            ctx.get('booking_link', ''),
        ]

    if n in (
        'kitilash_ka_comeback_3d_v1',
        'kitilash_ka_newsletter_new_clients_monthly_v1',
    ):
        return [
            ctx.get('client_name', ''),
            ctx.get('booking_link', ''),
        ]

    logger.warning(
        'build_template_params: unknown template_name=%s', template_name
    )
    return []
