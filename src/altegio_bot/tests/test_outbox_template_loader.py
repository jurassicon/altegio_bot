"""Tests for outbox_worker._load_template.

Covers:
  1. Exact language match (Phase 1, step 1)
  2. Fallback to DEFAULT_LANGUAGE within same company (Phase 1, step 2)
  3. Fallback to any language within same company (Phase 1, step 3)
  4. Not found → returns (None, language)  — now 6 DB calls total
  5. When language == DEFAULT_LANGUAGE the duplicate step is skipped

  Phase 2 — cross-company fallback (universal templates):
  6. When no company row exists, cross-company exact-language match is used
  7. When no company row and no cross-company exact-language, DEFAULT_LANGUAGE cross fallback
  8. Company-specific row always wins over cross-company fallback
  9. Rastatt monthly resolves via cross-company fallback (Karlsruhe row only)
  10. Rastatt followup resolves via cross-company fallback
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

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
# Phase 1 — company-specific rows (unchanged existing behaviour)
# ---------------------------------------------------------------------------


def test_load_template_exact_language_match() -> None:
    tmpl = FakeTemplate(
        id=1,
        company_id=1,
        code="record_updated",
        language="de",
    )
    session = FakeSession(results=[tmpl])

    res_tmpl, used_lang = run(
        ow._load_template(
            session,
            company_id=1,
            template_code="record_updated",
            language="de",
        )
    )

    assert res_tmpl is tmpl
    assert used_lang == "de"
    assert session.execute_calls == 1


def test_load_template_fallback_to_default_language() -> None:
    default_tmpl = FakeTemplate(
        id=2,
        company_id=1,
        code="record_updated",
        language=ow.DEFAULT_LANGUAGE,
    )
    session = FakeSession(results=[None, default_tmpl])

    res_tmpl, used_lang = run(
        ow._load_template(
            session,
            company_id=1,
            template_code="record_updated",
            language="en",
        )
    )

    assert res_tmpl is default_tmpl
    assert used_lang == ow.DEFAULT_LANGUAGE
    assert session.execute_calls == 2


def test_load_template_fallback_to_any_language() -> None:
    any_tmpl = FakeTemplate(
        id=3,
        company_id=1,
        code="record_updated",
        language="sr",
    )
    session = FakeSession(results=[None, None, any_tmpl])

    res_tmpl, used_lang = run(
        ow._load_template(
            session,
            company_id=1,
            template_code="record_updated",
            language="en",
        )
    )

    assert res_tmpl is any_tmpl
    assert used_lang == "sr"
    assert session.execute_calls == 3


def test_load_template_not_found_returns_none() -> None:
    """When no row exists at all, _load_template returns None after all 6 DB calls.

    Phase 1 exhausts 3 company-specific queries; Phase 2 exhausts 3 cross-company
    queries.  Total: 6 execute calls.  (language="en" != DEFAULT_LANGUAGE triggers
    the intermediate DEFAULT_LANGUAGE step in both phases.)
    """
    session = FakeSession(results=[None, None, None, None, None, None])

    res_tmpl, used_lang = run(
        ow._load_template(
            session,
            company_id=1,
            template_code="record_updated",
            language="en",
        )
    )

    assert res_tmpl is None
    assert used_lang == "en"
    assert session.execute_calls == 6


def test_load_template_when_language_is_default_skips_second_try() -> None:
    """When language==DEFAULT_LANGUAGE the duplicate step is skipped.

    With language="de" (== DEFAULT_LANGUAGE) both Phase 1 and Phase 2 skip
    the intermediate DEFAULT_LANGUAGE query, so only 2 Phase-1 calls are made
    before finding the result (company any-language row wins on call 2).
    """
    any_tmpl = FakeTemplate(
        id=4,
        company_id=1,
        code="record_updated",
        language="sr",
    )
    session = FakeSession(results=[None, any_tmpl])

    res_tmpl, used_lang = run(
        ow._load_template(
            session,
            company_id=1,
            template_code="record_updated",
            language=ow.DEFAULT_LANGUAGE,
        )
    )

    assert res_tmpl is any_tmpl
    assert used_lang == "sr"
    assert session.execute_calls == 2


# ---------------------------------------------------------------------------
# Phase 2 — cross-company fallback
# ---------------------------------------------------------------------------


def test_load_template_cross_company_fallback_exact_language() -> None:
    """No company row → falls back to cross-company row with matching language.

    Call sequence for company=RA, language="de" (== DEFAULT_LANGUAGE):
      1. RA + code + "de"  → None  (Phase 1, step 1)
      (step 2 skipped — language IS DEFAULT_LANGUAGE)
      2. RA + code (any)   → None  (Phase 1, step 3)
      3. cross + code + "de" → ka_tmpl  (Phase 2, step 4)
    Total: 3 execute calls.
    """
    ka_tmpl = FakeTemplate(id=10, company_id=KA, code="newsletter_new_clients_monthly", language="de")
    session = FakeSession(results=[None, None, ka_tmpl])

    res_tmpl, used_lang = run(
        ow._load_template(
            session,
            company_id=RA,
            template_code="newsletter_new_clients_monthly",
            language="de",
        )
    )

    assert res_tmpl is ka_tmpl
    assert used_lang == "de"
    assert session.execute_calls == 3


def test_load_template_cross_company_fallback_default_language() -> None:
    """No company row, no cross-company exact-language → DEFAULT_LANGUAGE cross fallback.

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
        ow._load_template(
            session,
            company_id=RA,
            template_code="newsletter_new_clients_monthly",
            language="en",
        )
    )

    assert res_tmpl is ka_tmpl
    assert used_lang == ow.DEFAULT_LANGUAGE
    assert session.execute_calls == 5


def test_load_template_cross_company_fallback_any_language() -> None:
    """All language-specific lookups fail → any-language cross fallback.

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
        ow._load_template(
            session,
            company_id=RA,
            template_code="newsletter_new_clients_monthly",
            language="en",
        )
    )

    assert res_tmpl is ka_tmpl
    assert used_lang == "fr"
    assert session.execute_calls == 6


def test_load_template_company_row_wins_over_cross_company() -> None:
    """Company-specific row is found first; cross-company fallback is never reached."""
    ra_tmpl = FakeTemplate(id=20, company_id=RA, code="newsletter_new_clients_monthly", language="de")
    # Only one result — Phase 1 step 1 succeeds, Phase 2 never runs.
    session = FakeSession(results=[ra_tmpl])

    res_tmpl, used_lang = run(
        ow._load_template(
            session,
            company_id=RA,
            template_code="newsletter_new_clients_monthly",
            language="de",
        )
    )

    assert res_tmpl is ra_tmpl
    assert used_lang == "de"
    assert session.execute_calls == 1


# ---------------------------------------------------------------------------
# Regression: Rastatt monthly and followup (run 8 scenario)
# ---------------------------------------------------------------------------


def test_rastatt_monthly_resolves_via_cross_company_fallback() -> None:
    """Rastatt monthly newsletter must not raise ValueError when only KA row exists.

    This is the exact scenario that caused run 8 / job 2009 to fail with
    'Template not found: company=1271200 code=newsletter_new_clients_monthly'.
    After Phase 2 fallback, the KA template body is used for the fallback_text
    argument to safe_send_template; the actual Meta send still uses the v1 name
    and 3 params built by resolve_meta_template + build_template_params.
    """
    ka_tmpl = FakeTemplate(
        id=100,
        company_id=KA,
        code="newsletter_new_clients_monthly",
        language="de",
        body="Sehr geehrte {{1}}, ...",
    )
    # language="de" == DEFAULT_LANGUAGE → step 2 skipped in both phases
    # Call 1: RA + code + "de" → None
    # Call 2: RA + code (any)  → None
    # Call 3: cross + code + "de" → ka_tmpl
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
    assert res_tmpl is not None, "Must not raise ValueError (Template not found)"
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
    """Preview endpoint get_template_text and _load_template now use the same
    effective lookup strategy for universal templates.

    Both:
    - first try company-specific row
    - then fall back to any active row with same code (cross-company)

    This test verifies _load_template returns the KA row when no RA row exists,
    which is exactly what the preview endpoint returns.
    """
    ka_tmpl = FakeTemplate(
        id=200,
        company_id=KA,
        code="newsletter_new_clients_monthly",
        language="de",
    )
    # Simulate: no RA row, KA row available cross-company
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
