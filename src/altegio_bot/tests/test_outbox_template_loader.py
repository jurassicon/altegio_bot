from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Optional

from altegio_bot.workers import outbox_worker as ow


@dataclass
class FakeTemplate:
    id: int
    language: str


class FakeResult:
    def __init__(self, value: Optional[FakeTemplate]) -> None:
        self._value = value

    def scalar_one_or_none(self) -> Optional[FakeTemplate]:
        return self._value


class FakeSession:
    def __init__(self, results: list[FakeResult]) -> None:
        self._results = results
        self.execute_calls = 0

    async def execute(self, stmt: Any) -> FakeResult:  # noqa: ARG002
        self.execute_calls += 1
        if not self._results:
            raise AssertionError("No more fake results left for session.execute")
        return self._results.pop(0)


def run(coro: Any) -> Any:
    return asyncio.run(coro)


def test_load_template_exact_language_match() -> None:
    tmpl_de = FakeTemplate(id=1, language="de")
    session = FakeSession(results=[FakeResult(tmpl_de)])

    tmpl, used_lang = run(
        ow._load_template(
            session=session,  # type: ignore[arg-type]
            company_id=758285,
            template_code="record_updated",
            language="de",
        )
    )

    assert tmpl is tmpl_de
    assert used_lang == "de"
    assert session.execute_calls == 1


def test_load_template_fallback_to_default_language() -> None:
    tmpl_de = FakeTemplate(id=2, language="de")
    session = FakeSession(results=[FakeResult(None), FakeResult(tmpl_de)])

    tmpl, used_lang = run(
        ow._load_template(
            session=session,  # type: ignore[arg-type]
            company_id=758285,
            template_code="record_updated",
            language="sr",
        )
    )

    assert tmpl is tmpl_de
    assert used_lang == ow.DEFAULT_LANGUAGE
    assert session.execute_calls == 2


def test_load_template_fallback_to_any_language() -> None:
    tmpl_en = FakeTemplate(id=3, language="en")
    session = FakeSession(
        results=[
            FakeResult(None),      # requested language
            FakeResult(None),      # default language
            FakeResult(tmpl_en),   # any language
        ]
    )

    tmpl, used_lang = run(
        ow._load_template(
            session=session,  # type: ignore[arg-type]
            company_id=758285,
            template_code="record_updated",
            language="sr",
        )
    )

    assert tmpl is tmpl_en
    assert used_lang == "en"
    assert session.execute_calls == 3


def test_load_template_not_found_returns_none() -> None:
    session = FakeSession(
        results=[FakeResult(None), FakeResult(None), FakeResult(None)]
    )

    tmpl, used_lang = run(
        ow._load_template(
            session=session,  # type: ignore[arg-type]
            company_id=758285,
            template_code="record_updated",
            language="sr",
        )
    )

    assert tmpl is None
    assert used_lang == "sr"
    assert session.execute_calls == 3


def test_load_template_when_language_is_default_skips_second_try() -> None:
    tmpl_en = FakeTemplate(id=4, language="en")
    session = FakeSession(results=[FakeResult(None), FakeResult(tmpl_en)])

    tmpl, used_lang = run(
        ow._load_template(
            session=session,  # type: ignore[arg-type]
            company_id=758285,
            template_code="record_updated",
            language=ow.DEFAULT_LANGUAGE,
        )
    )

    assert tmpl is tmpl_en
    assert used_lang == "en"
    assert session.execute_calls == 2
