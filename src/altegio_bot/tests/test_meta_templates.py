from __future__ import annotations

from altegio_bot.meta_templates import (
    TEMPLATE_LANGUAGE,
    UNIVERSAL_JOB_TYPES,
    build_template_params,
    resolve_meta_template,
)

KA = 758285
RA = 1271200


# ---------------------------------------------------------------------------
# resolve_meta_template
# ---------------------------------------------------------------------------


def test_resolve_ka_record_created_returning_client() -> None:
    name = resolve_meta_template(KA, "record_created", is_new_client=False)
    assert name == "kitilash_ka_record_created_v1"


def test_resolve_ka_record_created_new_client() -> None:
    name = resolve_meta_template(KA, "record_created", is_new_client=True)
    assert name == "kitilash_ka_record_created_new_client_v1"


def test_resolve_ka_record_updated() -> None:
    assert resolve_meta_template(KA, "record_updated") == ("kitilash_ka_record_updated_v1")


def test_resolve_ka_record_canceled() -> None:
    assert resolve_meta_template(KA, "record_canceled") == ("kitilash_ka_record_canceled_v1")


def test_resolve_ka_reminder_24h() -> None:
    assert resolve_meta_template(KA, "reminder_24h") == ("kitilash_ka_reminder_24h_v1")


def test_resolve_ka_reminder_2h() -> None:
    assert resolve_meta_template(KA, "reminder_2h") == ("kitilash_ka_reminder_2h_v1")


def test_resolve_ka_review_3d() -> None:
    assert resolve_meta_template(KA, "review_3d") == ("kitilash_ka_review_3d_v1")


def test_resolve_ka_repeat_10d() -> None:
    assert resolve_meta_template(KA, "repeat_10d") == ("kitilash_ka_repeat_10d_v1")


def test_resolve_ka_comeback_3d() -> None:
    assert resolve_meta_template(KA, "comeback_3d") == ("kitilash_ka_comeback_3d_v1")


def test_resolve_ka_newsletter() -> None:
    assert resolve_meta_template(KA, "newsletter_new_clients_monthly") == (
        "kitilash_ka_newsletter_new_clients_monthly_v1"
    )


def test_resolve_ra_record_created() -> None:
    assert resolve_meta_template(RA, "record_created") == ("kitilash_ra_record_created_v1")


def test_resolve_ra_record_updated() -> None:
    # Rastatt has a dedicated ra_* template for record_updated.
    assert resolve_meta_template(RA, "record_updated") == ("kitilash_ra_record_updated_v1")


def test_resolve_ra_repeat_10d_uses_canonical_ka() -> None:
    # repeat_10d is a UNIVERSAL template (no address footer).
    # Rastatt should use the canonical kitilash_ka_* variant.
    assert resolve_meta_template(RA, "repeat_10d") == ("kitilash_ka_repeat_10d_v1")


def test_resolve_ra_review_3d() -> None:
    assert resolve_meta_template(RA, "review_3d") == ("kitilash_ra_review_3d_v1")


def test_resolve_ra_comeback_3d_uses_canonical_ka() -> None:
    assert resolve_meta_template(RA, "comeback_3d") == ("kitilash_ka_comeback_3d_v1")


def test_resolve_ra_newsletter_uses_canonical_ka() -> None:
    assert resolve_meta_template(RA, "newsletter_new_clients_monthly") == (
        "kitilash_ka_newsletter_new_clients_monthly_v1"
    )


def test_resolve_unknown_job_type_returns_none() -> None:
    assert resolve_meta_template(KA, "unknown_job") is None


def test_resolve_unknown_company_returns_none() -> None:
    assert resolve_meta_template(999999, "record_created") is None


def test_template_language_constant() -> None:
    assert TEMPLATE_LANGUAGE == "de"


def test_universal_job_types_contains_expected() -> None:
    assert "repeat_10d" in UNIVERSAL_JOB_TYPES
    assert "review_3d" in UNIVERSAL_JOB_TYPES
    assert "comeback_3d" in UNIVERSAL_JOB_TYPES
    assert "newsletter_new_clients_monthly" in UNIVERSAL_JOB_TYPES
    # branch-specific types must NOT be in the universal set
    assert "record_created" not in UNIVERSAL_JOB_TYPES
    assert "record_updated" not in UNIVERSAL_JOB_TYPES
    assert "reminder_24h" not in UNIVERSAL_JOB_TYPES


# ---------------------------------------------------------------------------
# build_template_params
# ---------------------------------------------------------------------------

_CTX = {
    "client_name": "Anna",
    "staff_name": "Tanja",
    "date": "10.02.2026",
    "time": "14:00",
    "services": "Mascara — 60.00€",
    "total_cost": "60.00",
    "short_link": "https://short.link/abc",
    "primary_service": "Mascara",
    "booking_link": "https://booking.link/",
}


def test_params_record_created_ka() -> None:
    params = build_template_params("kitilash_ka_record_created_v1", _CTX)
    assert params == [
        "Anna",
        "Tanja",
        "10.02.2026",
        "14:00",
        "Mascara — 60.00€",
        "60.00",
        "https://short.link/abc",
    ]


def test_params_record_created_new_client_ka() -> None:
    params = build_template_params("kitilash_ka_record_created_new_client_v1", _CTX)
    assert params == [
        "Anna",
        "Tanja",
        "10.02.2026",
        "14:00",
        "Mascara — 60.00€",
        "60.00",
        "https://short.link/abc",
    ]


def test_params_record_created_ra() -> None:
    params = build_template_params("kitilash_ra_record_created_v1", _CTX)
    assert params == [
        "Anna",
        "Tanja",
        "10.02.2026",
        "14:00",
        "Mascara — 60.00€",
        "60.00",
        "https://short.link/abc",
    ]


def test_params_record_updated() -> None:
    params = build_template_params("kitilash_ka_record_updated_v1", _CTX)
    assert params == [
        "Anna",
        "Tanja",
        "10.02.2026",
        "14:00",
        "Mascara — 60.00€",
        "60.00",
        "https://short.link/abc",
    ]


def test_params_record_canceled() -> None:
    params = build_template_params("kitilash_ka_record_canceled_v1", _CTX)
    assert params == [
        "Anna",
        "10.02.2026",
        "14:00",
        "Mascara — 60.00€",
        "https://booking.link/",
    ]


def test_params_reminder_24h() -> None:
    params = build_template_params("kitilash_ka_reminder_24h_v1", _CTX)
    assert params == [
        "Anna",
        "Tanja",
        "10.02.2026",
        "14:00",
        "Mascara — 60.00€",
        "https://short.link/abc",
    ]


def test_params_reminder_2h() -> None:
    params = build_template_params("kitilash_ka_reminder_2h_v1", _CTX)
    assert params == [
        "Anna",
        "Tanja",
        "10.02.2026",
        "14:00",
        "Mascara — 60.00€",
        "https://short.link/abc",
    ]


def test_params_review_3d() -> None:
    params = build_template_params("kitilash_ka_review_3d_v1", _CTX)
    assert params == ["Anna", "https://short.link/abc"]


def test_params_review_3d_ka_uses_google_maps_link() -> None:
    ctx = dict(_CTX, short_link="https://g.page/r/CdOqDUWhxCAbEBM/review")
    params = build_template_params("kitilash_ka_review_3d_v1", ctx)
    assert params == ["Anna", "https://g.page/r/CdOqDUWhxCAbEBM/review"]


def test_params_review_3d_ra_uses_google_maps_link() -> None:
    ctx = dict(_CTX, short_link="https://g.page/r/CWd7fy4dua5kEBM/review")
    params = build_template_params("kitilash_ra_review_3d_v1", ctx)
    assert params == ["Anna", "https://g.page/r/CWd7fy4dua5kEBM/review"]


def test_params_repeat_10d() -> None:
    params = build_template_params("kitilash_ka_repeat_10d_v1", _CTX)
    assert params == ["Anna", "Mascara", "https://booking.link/"]


def test_params_comeback_3d() -> None:
    params = build_template_params("kitilash_ka_comeback_3d_v1", _CTX)
    assert params == ["Anna", "https://booking.link/"]


def test_params_newsletter() -> None:
    params = build_template_params("kitilash_ka_newsletter_new_clients_monthly_v1", _CTX)
    assert params == ["Anna", "https://booking.link/", ""]


def test_params_newsletter_with_loyalty_card() -> None:
    ctx = dict(_CTX, loyalty_card_text="Kundenkarte #0074454347287392")
    params = build_template_params("kitilash_ka_newsletter_new_clients_monthly_v1", ctx)
    assert params == [
        "Anna",
        "https://booking.link/",
        "Kundenkarte #0074454347287392",
    ]


def test_params_newsletter_v1_no_card() -> None:
    """v1 без loyalty_card_text в ctx → третий параметр пустая строка."""
    params = build_template_params("kitilash_ka_newsletter_new_clients_monthly_v1", _CTX)
    assert params == ["Anna", "https://booking.link/", ""]


def test_params_unknown_template_returns_empty() -> None:
    params = build_template_params("unknown_template_xyz", _CTX)
    assert params == []


def test_params_missing_ctx_keys_use_empty_string() -> None:
    params = build_template_params("kitilash_ka_record_updated_v1", {})
    assert params == ["", "", "", "", "", "", ""]


# ---------------------------------------------------------------------------
# Newline replacement in services param (multi-service records)
# ---------------------------------------------------------------------------

_CTX_MULTI = dict(
    _CTX,
    services="Maniküre mit Gel-Lack — 45.00€\nHygienische Pediküre — 45.00€",
)
_SERVICES_FLAT = "Maniküre mit Gel-Lack — 45.00€, Hygienische Pediküre — 45.00€"


def test_multiservice_record_created_newline_replaced() -> None:
    params = build_template_params("kitilash_ka_record_created_v1", _CTX_MULTI)
    assert params[4] == _SERVICES_FLAT


def test_multiservice_record_updated_newline_replaced() -> None:
    params = build_template_params("kitilash_ka_record_updated_v1", _CTX_MULTI)
    assert params[4] == _SERVICES_FLAT


def test_multiservice_record_canceled_newline_replaced() -> None:
    params = build_template_params("kitilash_ka_record_canceled_v1", _CTX_MULTI)
    assert params[3] == _SERVICES_FLAT


def test_multiservice_reminder_24h_newline_replaced() -> None:
    params = build_template_params("kitilash_ka_reminder_24h_v1", _CTX_MULTI)
    assert params[4] == _SERVICES_FLAT
