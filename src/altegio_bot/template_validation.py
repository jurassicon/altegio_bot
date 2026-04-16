"""Preflight validation for Meta WhatsApp template parameters.

Validates assembled params *before* calling safe_send_template so that
data errors are caught locally without burning a Meta API request.

Design notes
------------
- ``validate_template_params`` is the single public entry point.
- It returns ``str | None``: an error string on failure, ``None`` on OK.
  Returning a value (rather than raising) keeps the call site a plain
  ``if`` — no try/except in outbox_worker.
- ``_TEMPLATE_RULES`` maps each known Meta template name to its
  expected param count and (optionally) named param labels for clearer
  error messages.  New templates should be registered here alongside
  their entry in ``meta_templates.META_TEMPLATE_MAP``.
- For templates *not* in the rules dict the function falls back to
  minimal validation: param list must be non-empty and every element
  must be a non-empty string.  This catches the ``build_template_params``
  empty-list fallback for unknown templates while not requiring every
  template to be explicitly registered.
"""

from __future__ import annotations

_PREFIX = "Local template validation failed"

# (expected_count, param_names | None)
# param_names: list of label strings used in error messages (index = 0-based
# position).  Provide None to get generic "#N" labels.
_TemplateRule = tuple[int, list[str] | None]

_TEMPLATE_RULES: dict[str, _TemplateRule] = {
    # --- record_created ---
    "kitilash_ka_record_created_v1": (
        7,
        ["client_name", "staff_name", "date", "time", "services", "total_cost", "short_link"],
    ),
    "kitilash_ka_record_created_new_client_v1": (
        7,
        ["client_name", "staff_name", "date", "time", "services", "total_cost", "short_link"],
    ),
    "kitilash_ra_record_created_v1": (
        7,
        ["client_name", "staff_name", "date", "time", "services", "total_cost", "short_link"],
    ),
    "kitilash_ra_record_created_new_client_v1": (
        7,
        ["client_name", "staff_name", "date", "time", "services", "total_cost", "short_link"],
    ),
    # --- record_updated ---
    "kitilash_ka_record_updated_v1": (
        7,
        ["client_name", "staff_name", "date", "time", "services", "total_cost", "short_link"],
    ),
    "kitilash_ra_record_updated_v1": (
        7,
        ["client_name", "staff_name", "date", "time", "services", "total_cost", "short_link"],
    ),
    # --- record_canceled ---
    "kitilash_ka_record_canceled_v1": (
        5,
        ["client_name", "date", "time", "services", "booking_link"],
    ),
    "kitilash_ra_record_canceled_v1": (
        5,
        ["client_name", "date", "time", "services", "booking_link"],
    ),
    # --- reminders ---
    "kitilash_ka_reminder_24h_v1": (
        6,
        ["client_name", "staff_name", "date", "time", "services", "short_link"],
    ),
    "kitilash_ka_reminder_2h_v1": (
        6,
        ["client_name", "staff_name", "date", "time", "services", "short_link"],
    ),
    "kitilash_ra_reminder_24h_v1": (
        6,
        ["client_name", "staff_name", "date", "time", "services", "short_link"],
    ),
    "kitilash_ra_reminder_2h_v1": (
        6,
        ["client_name", "staff_name", "date", "time", "services", "short_link"],
    ),
    # --- marketing ---
    "kitilash_ka_review_3d_v1": (
        2,
        ["client_name", "short_link"],
    ),
    "kitilash_ra_review_3d_v1": (
        2,
        ["client_name", "short_link"],
    ),
    "kitilash_ka_repeat_10d_v1": (
        3,
        ["client_name", "primary_service", "booking_link"],
    ),
    "kitilash_ka_comeback_3d_v1": (
        2,
        ["client_name", "booking_link"],
    ),
    # --- newsletter ---
    "kitilash_ka_newsletter_new_clients_monthly_v1": (
        3,
        ["client_name", "booking_link", "loyalty_card_text"],
    ),
    "kitilash_ka_newsletter_new_clients_followup_v1": (
        2,
        ["client_name", "booking_link"],
    ),
}


def validate_template_params(
    template_name: str,
    params: list[str],
) -> str | None:
    """Return an error string if *params* fail validation, else ``None``.

    Checks performed:
    1. Param list is non-empty (catches unknown template → build returned []).
    2. Param count matches the registered expected count (if template is
       in ``_TEMPLATE_RULES``).
    3. Every param value is a non-empty string (catches missing/None values).

    Error messages use the format:
      ``Local template validation failed: <reason>``
    so callers can distinguish local failures from Meta API errors.
    """
    if not params:
        return f"{_PREFIX}: no params built for template {template_name!r} — template may be unrecognised"

    rule = _TEMPLATE_RULES.get(template_name)
    if rule is not None:
        expected_count, param_names = rule
        if len(params) != expected_count:
            return f"{_PREFIX}: expected {expected_count} params, got {len(params)}"
        for i, val in enumerate(params):
            if not val:
                label = param_names[i] if param_names and i < len(param_names) else f"#{i + 1}"
                return f"{_PREFIX}: missing required param #{i + 1} {label}"
    else:
        # Generic fallback for templates not explicitly registered.
        for i, val in enumerate(params):
            if not val:
                return f"{_PREFIX}: missing required param #{i + 1}"

    return None
