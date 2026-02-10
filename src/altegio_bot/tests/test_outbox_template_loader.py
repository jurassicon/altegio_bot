from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from altegio_bot.workers import outbox_worker as ow


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
    session = FakeSession(results=[None, None, None])

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
    assert session.execute_calls == 3


def test_load_template_when_language_is_default_skips_second_try() -> None:
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
