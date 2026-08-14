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
        0,
        [],
    ),
    # --- promo ---
    "kitilash_ka_promo_card_booking_reminder_v1": (
        3,
        ["discount_amount", "expires_at", "booking_link"],
    ),
}


# Lifecycle contracts keyed by CODE, for templates whose Meta name is only known
# at runtime (EasyWeek reads it from `message_templates.meta_template_name`).
#
# Without this, a perfectly valid EasyWeek template name would miss
# `_TEMPLATE_RULES` and fall into the generic path, which only checks
# "non-empty" — so a 5-param list for a 7-param created template would pass
# preflight and be rejected by Meta instead of locally.
_LIFECYCLE_RULES: dict[str, _TemplateRule] = {
    "record_created": (
        7,
        ["client_name", "staff_name", "date", "time", "services", "total_cost", "booking_link"],
    ),
    "record_updated": (
        7,
        ["client_name", "staff_name", "date", "time", "services", "total_cost", "booking_link"],
    ),
    "record_canceled": (
        5,
        ["client_name", "date", "time", "services", "booking_link"],
    ),
    # PR-8. Same six positional slots as the approved reminder templates above,
    # with the link slot named `booking_link` because EasyWeek fills it with the
    # link re-verified at send time rather than a stored `short_link`.
    "reminder_24h": (
        6,
        ["client_name", "staff_name", "date", "time", "services", "booking_link"],
    ),
    "reminder_2h": (
        6,
        ["client_name", "staff_name", "date", "time", "services", "booking_link"],
    ),
    "review_3d": (
        2,
        ["client_name", "review_url"],
    ),
}


def validate_lifecycle_template_params(code: str, params: list[str]) -> str | None:
    """Validate params against the contract for a lifecycle *code*.

    Same return convention as :func:`validate_template_params`: an error string,
    or ``None`` when the params are acceptable. An unknown code is an error
    rather than a pass — a lifecycle job whose code has no contract must not
    reach Meta unchecked.
    """
    rule = _LIFECYCLE_RULES.get(code)
    if rule is None:
        return f"{_PREFIX}: no lifecycle param contract for code {code!r}"
    return _check_against_rule(rule, params, subject=f"code {code!r}")


def _check_against_rule(
    rule: _TemplateRule,
    params: list[str],
    *,
    subject: str,
) -> str | None:
    """Shared count/emptiness checks so both entry points agree exactly."""
    expected_count, param_names = rule
    if expected_count == 0:
        if params:
            return f"{_PREFIX}: expected {expected_count} params, got {len(params)}"
        return None
    if not params:
        return f"{_PREFIX}: no params built for {subject} — template may be unrecognised"
    if len(params) != expected_count:
        return f"{_PREFIX}: expected {expected_count} params, got {len(params)}"
    for i, val in enumerate(params):
        if not val:
            label = param_names[i] if param_names and i < len(param_names) else f"#{i + 1}"
            return f"{_PREFIX}: missing required param #{i + 1} {label}"
    return None


def validate_template_params(
    template_name: str,
    params: list[str],
) -> str | None:
    """Return an error string if *params* fail validation, else ``None``.

    Checks performed:
    1. If the template is registered with expected_count=0 (static body, no
       variables): params must be empty — any non-empty list is an error.
    2. For templates with expected_count>0: param list must be non-empty
       (catches unknown template → build returned []).
    3. Param count matches the registered expected count.
    4. Every param value is a non-empty string (catches missing/None values).

    Error messages use the format:
      ``Local template validation failed: <reason>``
    so callers can distinguish local failures from Meta API errors.
    """
    rule = _TEMPLATE_RULES.get(template_name)
    if rule is not None:
        expected_count, param_names = rule
        if expected_count == 0:
            if params:
                return f"{_PREFIX}: expected {expected_count} params, got {len(params)}"
            return None
        if not params:
            return f"{_PREFIX}: no params built for template {template_name!r} — template may be unrecognised"
        if len(params) != expected_count:
            return f"{_PREFIX}: expected {expected_count} params, got {len(params)}"
        for i, val in enumerate(params):
            if not val:
                label = param_names[i] if param_names and i < len(param_names) else f"#{i + 1}"
                return f"{_PREFIX}: missing required param #{i + 1} {label}"
    else:
        # Generic fallback for templates not explicitly registered.
        if not params:
            return f"{_PREFIX}: no params built for template {template_name!r} — template may be unrecognised"
        for i, val in enumerate(params):
            if not val:
                return f"{_PREFIX}: missing required param #{i + 1}"

    return None
