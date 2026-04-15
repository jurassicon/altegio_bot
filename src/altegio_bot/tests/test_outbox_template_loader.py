"""Tests for outbox_worker._load_template.

Covers:
  Phase 1 — company-specific rows (always executed):
  1. Exact language match (step 1)
  2. Fallback to DEFAULT_LANGUAGE within same company (step 2)
  3. Fallback to any language within same company (step 3)
  4. Branch-specific code not found → returns (None, language) after 3 calls.
     Phase 2 is intentionally SKIPPED — no cross-company for branch-specific codes.
  5. When language == DEFAULT_LANGUAGE the duplicate DEFAULT_LANGUAGE step is skipped.

  Phase 2 — cross-company fallback (ONLY for UNIVERSAL_JOB_TYPES):
  6. Universal code with no company row → exact-language cross-company match
  7. Universal code with no cross-company exact-language → DEFAULT_LANGUAGE cross fallback
  8. Universal code, all cross-company lookups fail → any-language cross fallback
  9. Universal code, company-specific row exists → wins over cross-company (Phase 2 never runs)
  10. Universal code, all lookups exhausted → returns (None, language) after 6 calls

  Safety guard — branch-specific codes never cross company boundaries:
  11. record_created does NOT use cross-company fallback
  12. record_updated does NOT use cross-company fallback
  13. reminder_24h does NOT use cross-company fallback

  Regression — Rastatt monthly/followup (run 8 / job 2009 scenario):
  14. Rastatt monthly resolves via cross-company fallback (Karlsruhe row only)
  15. Rastatt followup resolves via cross-company fallback
  16. Preview and send fallback parity for universal templates
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

import pytest

from altegio_bot.workers import outbox_worker as ow

RA = 1271200
KA = 758285


@dataclass
class FakeTemplate:
    id: int
    company_id: int
    code: str
    language: str
    is_active: bool = True
    body: str = "Hi {client_name}"


class FakeResult:
    def __init__(self, value: Any) -> None:
        self._value = value

    def scalar_one_or_none(self) -> Any:
        return self._value


class FakeSession:
    def __init__(self, results: list[Any]) -> None:
        self._results = results
        self.execute_calls = 0

    async def execute(self, stmt: Any) -> FakeResult:
        _ = stmt
        self.execute_calls += 1
        if not self._results:
            return FakeResult(None)
        return FakeResult(self._results.pop(0))


def run(coro: Any) -> Any:
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# Phase 1 — company-specific rows
# ---------------------------------------------------------------------------


def test_load_template_exact_language_match() -> None:
    tmpl = FakeTemplate(id=1, company_id=1, code="record_updated", language="de")
    session = FakeSession(results=[tmpl])

    res_tmpl, used_lang = run(ow._load_template(session, company_id=1, template_code="record_updated", language="de"))

    assert res_tmpl is tmpl
    assert used_lang == "de"
    assert session.execute_calls == 1


def test_load_template_fallback_to_default_language() -> None:
    default_tmpl = FakeTemplate(id=2, company_id=1, code="record_updated", language=ow.DEFAULT_LANGUAGE)
    session = FakeSession(results=[None, default_tmpl])

    res_tmpl, used_lang = run(ow._load_template(session, company_id=1, template_code="record_updated", language="en"))

    assert res_tmpl is default_tmpl
    assert used_lang == ow.DEFAULT_LANGUAGE
    assert session.execute_calls == 2


def test_load_template_fallback_to_any_language() -> None:
    any_tmpl = FakeTemplate(id=3, company_id=1, code="record_updated", language="sr")
    session = FakeSession(results=[None, None, any_tmpl])

    res_tmpl, used_lang = run(ow._load_template(session, company_id=1, template_code="record_updated", language="en"))

    assert res_tmpl is any_tmpl
    assert used_lang == "sr"
    assert session.execute_calls == 3


def test_load_template_branch_specific_not_found_returns_none_after_3_calls() -> None:
    """Branch-specific code with no matching row returns None after exactly 3 DB calls.

    Phase 2 is intentionally SKIPPED for branch-specific codes (record_updated,
    record_created, reminder_*, record_canceled).  Those templates contain
    branch-specific address footers; borrowing another company's row would
    produce incorrect salon address in the message.

    Call sequence for language="en" (≠ DEFAULT_LANGUAGE):
      1. company + code + "en" → None
      2. company + code + "de" → None
      3. company + code (any)  → None
      Phase 2 SKIPPED (record_updated not in UNIVERSAL_JOB_TYPES)
    Total: 3 execute calls.
    """
    session = FakeSession(results=[None, None, None])

    res_tmpl, used_lang = run(ow._load_template(session, company_id=1, template_code="record_updated", language="en"))

    assert res_tmpl is None
    assert used_lang == "en"
    assert session.execute_calls == 3


def test_load_template_when_language_is_default_skips_duplicate_step() -> None:
    """When language==DEFAULT_LANGUAGE the duplicate DEFAULT_LANGUAGE step is skipped.

    company_id=1 has a "sr"-language row.  With language="de" (==DEFAULT_LANGUAGE):
      Call 1: company + code + "de" → None
      (step 2 skipped — language IS DEFAULT_LANGUAGE)
      Call 2: company + code (any)  → any_tmpl (sr)
    Total: 2 execute calls; Phase 2 never reached.
    """
    any_tmpl = FakeTemplate(id=4, company_id=1, code="record_updated", language="sr")
    session = FakeSession(results=[None, any_tmpl])

    res_tmpl, used_lang = run(
        ow._load_template(session, company_id=1, template_code="record_updated", language=ow.DEFAULT_LANGUAGE)
    )

    assert res_tmpl is any_tmpl
    assert used_lang == "sr"
    assert session.execute_calls == 2


# ---------------------------------------------------------------------------
# Phase 2 — cross-company fallback (universal codes only)
# ---------------------------------------------------------------------------


def test_load_template_cross_company_fallback_exact_language() -> None:
    """Universal code, no company row → falls back to cross-company row with matching language.

    Call sequence for company=RA, language="de" (== DEFAULT_LANGUAGE):
      1. RA + code + "de"    → None  (Phase 1, step 1)
      (step 2 skipped — language IS DEFAULT_LANGUAGE)
      2. RA + code (any)     → None  (Phase 1, step 3)
      3. cross + code + "de" → ka_tmpl  (Phase 2, step 4)
    Total: 3 execute calls.
    """
    ka_tmpl = FakeTemplate(id=10, company_id=KA, code="newsletter_new_clients_monthly", language="de")
    session = FakeSession(results=[None, None, ka_tmpl])

    res_tmpl, used_lang = run(
        ow._load_template(session, company_id=RA, template_code="newsletter_new_clients_monthly", language="de")
    )

    assert res_tmpl is ka_tmpl
    assert used_lang == "de"
    assert session.execute_calls == 3


def test_load_template_cross_company_fallback_default_language() -> None:
    """Universal code, no cross-company exact-language → DEFAULT_LANGUAGE cross fallback.

    Call sequence for company=RA, language="en" (≠ DEFAULT_LANGUAGE):
      1. RA + code + "en"  → None  (Phase 1, step 1)
      2. RA + code + "de"  → None  (Phase 1, step 2)
      3. RA + code (any)   → None  (Phase 1, step 3)
      4. cross + "en"      → None  (Phase 2, step 4)
      5. cross + "de"      → ka_tmpl  (Phase 2, step 5)
    Total: 5 execute calls.
    """
    ka_tmpl = FakeTemplate(id=11, company_id=KA, code="newsletter_new_clients_monthly", language="de")
    session = FakeSession(results=[None, None, None, None, ka_tmpl])

    res_tmpl, used_lang = run(
        ow._load_template(session, company_id=RA, template_code="newsletter_new_clients_monthly", language="en")
    )

    assert res_tmpl is ka_tmpl
    assert used_lang == ow.DEFAULT_LANGUAGE
    assert session.execute_calls == 5


def test_load_template_cross_company_fallback_any_language() -> None:
    """Universal code, all language-specific lookups fail → any-language cross fallback.

    Call sequence for company=RA, language="en":
      1. RA + "en"    → None
      2. RA + "de"    → None
      3. RA + any     → None
      4. cross + "en" → None
      5. cross + "de" → None
      6. cross + any  → ka_tmpl
    Total: 6 execute calls.
    """
    ka_tmpl = FakeTemplate(id=12, company_id=KA, code="newsletter_new_clients_monthly", language="fr")
    session = FakeSession(results=[None, None, None, None, None, ka_tmpl])

    res_tmpl, used_lang = run(
        ow._load_template(session, company_id=RA, template_code="newsletter_new_clients_monthly", language="en")
    )

    assert res_tmpl is ka_tmpl
    assert used_lang == "fr"
    assert session.execute_calls == 6


def test_load_template_company_row_wins_over_cross_company() -> None:
    """Company-specific row is found first; Phase 2 is never reached."""
    ra_tmpl = FakeTemplate(id=20, company_id=RA, code="newsletter_new_clients_monthly", language="de")
    session = FakeSession(results=[ra_tmpl])

    res_tmpl, used_lang = run(
        ow._load_template(session, company_id=RA, template_code="newsletter_new_clients_monthly", language="de")
    )

    assert res_tmpl is ra_tmpl
    assert used_lang == "de"
    assert session.execute_calls == 1


def test_load_template_universal_not_found_anywhere_returns_none() -> None:
    """Universal code with no row in any company returns None after 6 calls.

    Phase 1: 3 calls (language="en" triggers the DEFAULT_LANGUAGE step).
    Phase 2: 3 calls.
    Total: 6 execute calls.
    """
    session = FakeSession(results=[None, None, None, None, None, None])

    res_tmpl, used_lang = run(
        ow._load_template(session, company_id=RA, template_code="newsletter_new_clients_monthly", language="en")
    )

    assert res_tmpl is None
    assert used_lang == "en"
    assert session.execute_calls == 6


# ---------------------------------------------------------------------------
# Safety guard — branch-specific codes never cross company boundaries
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "branch_code",
    [
        "record_created",
        "record_updated",
        "record_canceled",
        "reminder_24h",
        "reminder_2h",
    ],
)
def test_branch_specific_code_does_not_use_cross_company_fallback(branch_code: str) -> None:
    """Branch-specific template codes must NEVER fall back to another company's row.

    These templates contain branch-specific address footers.  Borrowing another
    company's row would send the wrong salon address to the client.

    When no company row exists, _load_template must return (None, language) after
    exactly 3 Phase-1 calls, without making any cross-company DB queries.
    """
    # Simulate: another company has a row for this code, but the target company doesn't.
    other_company_tmpl = FakeTemplate(id=999, company_id=KA, code=branch_code, language="de")
    # Results list: Phase-1 exhausted (3 Nones), then a cross-company row that MUST NOT be used.
    session = FakeSession(results=[None, None, None, other_company_tmpl])

    res_tmpl, used_lang = run(ow._load_template(session, company_id=RA, template_code=branch_code, language="en"))

    assert res_tmpl is None, (
        f"Branch-specific code '{branch_code}' must return None when no company row exists, "
        f"not fall back to company={other_company_tmpl.company_id}"
    )
    # Phase 2 was skipped: only 3 calls, not 4+
    assert session.execute_calls == 3, (
        f"Expected 3 DB calls (Phase 1 only), got {session.execute_calls}. "
        "Phase 2 must be skipped for branch-specific codes."
    )


@pytest.mark.parametrize(
    "universal_code",
    [
        "review_3d",
        "repeat_10d",
        "comeback_3d",
        "newsletter_new_clients_monthly",
        "newsletter_new_clients_followup",
    ],
)
def test_all_universal_codes_eligible_for_cross_company_fallback(universal_code: str) -> None:
    """Every code in UNIVERSAL_JOB_TYPES must be eligible for Phase 2 fallback.

    For each universal code: Phase 1 finds nothing (2 calls, language==DEFAULT_LANGUAGE),
    Phase 2 finds the KA row on the 3rd call.
    """
    ka_tmpl = FakeTemplate(id=300, company_id=KA, code=universal_code, language="de")
    session = FakeSession(results=[None, None, ka_tmpl])

    res_tmpl, used_lang = run(ow._load_template(session, company_id=RA, template_code=universal_code, language="de"))

    assert res_tmpl is ka_tmpl, f"Universal code '{universal_code}' must reach Phase 2 and return the cross-company row"
    assert used_lang == "de"
    assert session.execute_calls == 3


# ---------------------------------------------------------------------------
# Regression — Rastatt monthly/followup (run 8 / job 2009 scenario)
# ---------------------------------------------------------------------------


def test_rastatt_monthly_resolves_via_cross_company_fallback() -> None:
    """Rastatt monthly newsletter must not raise ValueError when only KA row exists.

    This is the exact scenario that caused run 8 / job 2009 to fail with
    'Template not found: company=1271200 code=newsletter_new_clients_monthly'.
    After Phase 2 fallback, the KA template body is used for fallback_text in
    safe_send_template; the actual Meta send still uses the v1 name and 3 params
    built by resolve_meta_template + build_template_params.
    """
    ka_tmpl = FakeTemplate(
        id=100,
        company_id=KA,
        code="newsletter_new_clients_monthly",
        language="de",
        body="Sehr geehrte {{1}}, ...",
    )
    # language="de" == DEFAULT_LANGUAGE → step 2 skipped in both phases
    # Call 1: RA + code + "de" → None  (Phase 1, step 1)
    # Call 2: RA + code (any)  → None  (Phase 1, step 3)
    # Call 3: cross + "de"     → ka_tmpl  (Phase 2, step 4)
    session = FakeSession(results=[None, None, ka_tmpl])

    res_tmpl, used_lang = run(
        ow._load_template(
            session,
            company_id=RA,
            template_code="newsletter_new_clients_monthly",
            language="de",
        )
    )

    assert res_tmpl is ka_tmpl, "Must fall back to KA template when no RA row exists"
    assert res_tmpl is not None, "Must not return None (would cause Template not found ValueError)"
    assert used_lang == "de"
    assert session.execute_calls == 3


def test_rastatt_followup_resolves_via_cross_company_fallback() -> None:
    """Rastatt followup newsletter resolves via cross-company fallback.

    DB currently has no RA row for newsletter_new_clients_followup either.
    The fallback ensures _render_message does not raise for followup jobs.
    """
    ka_tmpl = FakeTemplate(
        id=101,
        company_id=KA,
        code="newsletter_new_clients_followup",
        language="de",
        body="Wir freuen uns auf Ihren Besuch {{1}}...",
    )
    session = FakeSession(results=[None, None, ka_tmpl])

    res_tmpl, used_lang = run(
        ow._load_template(
            session,
            company_id=RA,
            template_code="newsletter_new_clients_followup",
            language="de",
        )
    )

    assert res_tmpl is ka_tmpl
    assert used_lang == "de"
    assert session.execute_calls == 3


def test_preview_and_send_fallback_parity() -> None:
    """Preview endpoint get_template_text and _load_template use the same effective
    lookup strategy for universal templates: company-specific first, then any active
    row with the same code.

    This test verifies _load_template returns the KA row when no RA row exists,
    matching what the preview endpoint returns.
    """
    ka_tmpl = FakeTemplate(id=200, company_id=KA, code="newsletter_new_clients_monthly", language="de")
    session = FakeSession(results=[None, None, ka_tmpl])

    res_tmpl, _ = run(
        ow._load_template(
            session,
            company_id=RA,
            template_code="newsletter_new_clients_monthly",
            language="de",
        )
    )

    assert res_tmpl is not None, "send path must find the same template as preview"
    assert res_tmpl.company_id == KA, f"Expected cross-company KA row (company={KA}), got company={res_tmpl.company_id}"
