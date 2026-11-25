"""What the operator actually reads on the zero-counter row, in a real browser.

Why this file exists
--------------------
The first review round found the sentences wrong, not the contracts: the API
reported two distinct facts and the page collapsed them into one, and a tab that
had been open while another tab included the person still told its operator the
row "осталась исключённой". Neither defect is visible from a JSON assertion —
the text is assembled in the page's own JavaScript — so these tests click the
button a person clicks and read the element a person reads.

Chromium, against the real pages, over real HTTP, with a real session cookie and
this session's CSRF token. EasyWeek is faked at the dispatch seam; the preview,
the row, the binding, the check, the Add and the refusals are all the
application's own.

Required, not skippable: ``ALTEGIO_REQUIRE_BROWSER_TESTS=1`` makes a missing
browser a failure. See ``easyweek_voucher_mailing_browser_fixtures``.
"""

from __future__ import annotations

import contextlib
import uuid
from datetime import datetime
from typing import Any

import pytest

import altegio_bot.campaigns.easyweek_segment as segment_module
import altegio_bot.campaigns.runner as runner_module
import altegio_bot.ops.campaigns_api as campaigns_api_module
import altegio_bot.ops.voucher_mailing as voucher_mailing_module
from altegio_bot.campaigns.easyweek_zero_visit import ZERO_VISITS_REPORTED
from altegio_bot.campaigns.runner import RunParams
from altegio_bot.models.models import PROVIDER_EASYWEEK, RECIPIENT_BASIS_MANUAL, CampaignRecipient
from altegio_bot.tests.easyweek_voucher_mailing_browser_fixtures import (
    WAIT_MS,
    assert_no_page_errors,
    watch_for_errors,
)
from altegio_bot.tests.easyweek_zero_visit_fixtures import (
    COMPANY_ID,
    LOCATION_UUID,
    PERIOD_END,
    PERIOD_START,
    START,
    Person,
    seed_person,
)

CUSTOMER_UUID = uuid.UUID("c0ffee00-1111-4222-8333-444444444444")
STATUS_BOX = "#zero-visit-{recipient_id}"


@pytest.fixture(autouse=True)
def _configuration(easyweek_zero_visit_configuration: None) -> None:
    """The synthetic branch registry, for the served pages and the check alike."""


def _history_row(
    *,
    booking_uuid: uuid.UUID,
    completed: bool = True,
    canceled: bool = False,
    start_time: datetime = START,
) -> dict[str, Any]:
    return {
        "uuid": str(booking_uuid),
        "customer": {"uuid": str(CUSTOMER_UUID)},
        "is_canceled": canceled,
        "is_completed": completed,
        "start_time": start_time.isoformat(),
    }


def _history_page(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "data": rows,
        "meta": {"current_page": 1, "last_page": 1, "per_page": 100, "total": len(rows)},
    }


def _booking_response(*, booking_uuid: uuid.UUID) -> dict[str, Any]:
    return {
        "uuid": str(booking_uuid),
        "location_uuid": LOCATION_UUID,
        "customer": {"uuid": str(CUSTOMER_UUID)},
        "is_canceled": False,
        "is_completed": True,
        "start_time": START.isoformat(),
        "ordered_services": [{}],
    }


class BrowserEasyWeek:
    """Every read the two served paths need: the check and the manual Add."""

    def __init__(self, *, person: Person, rows: list[dict[str, Any]], failure: Exception | None = None) -> None:
        self.person = person
        self.rows = rows
        self.failure = failure

    async def get_booking(self, booking_uuid: str) -> dict[str, Any]:
        if self.failure is not None:
            raise self.failure
        return _booking_response(booking_uuid=self.person.booking_uuid)

    async def get_customer(self, customer_uuid: str) -> dict[str, Any]:
        return {"uuid": str(CUSTOMER_UUID), "phone": self.person.phone, "first_name": "Synthetic"}

    async def list_customer_bookings(self, customer_uuid: str, page: int, per_page: int = 100) -> dict[str, Any]:
        return _history_page(self.rows)

    async def list_customers(self, *, params: dict[str, Any]) -> dict[str, Any]:
        row = {"uuid": str(CUSTOMER_UUID), "phone": self.person.phone, "first_name": "Synthetic"}
        return {"data": [row], "meta": {"current_page": 1, "last_page": 1, "per_page": 100, "total": 1}}


def _install(monkeypatch: pytest.MonkeyPatch, reader: BrowserEasyWeek) -> None:
    """One fake behind both served paths, so the browser drives them for real."""

    @contextlib.asynccontextmanager
    async def _client():
        yield reader

    for module in (voucher_mailing_module, campaigns_api_module):
        monkeypatch.setattr(module, "EasyWeekClient", lambda *a, **k: _client())


async def _preview_with_zero_row(session_maker, monkeypatch: pytest.MonkeyPatch) -> tuple[int, int, Person]:
    person = await seed_person(session_maker, 1, customer_uuid=CUSTOMER_UUID)
    monkeypatch.setattr(segment_module, "SessionLocal", session_maker)
    monkeypatch.setattr(runner_module, "SessionLocal", session_maker)
    run = await runner_module.run_preview(
        RunParams(
            provider=PROVIDER_EASYWEEK,
            company_id=COMPANY_ID,
            location_id=COMPANY_ID,
            period_start=PERIOD_START,
            period_end=PERIOD_END,
            mode="preview",
            campaign_code="new_clients_monthly",
        )
    )
    async with session_maker() as session:
        from sqlalchemy import select

        rows = list(
            (await session.execute(select(CampaignRecipient).where(CampaignRecipient.campaign_run_id == run.id)))
            .scalars()
            .all()
        )
    assert len(rows) == 1
    assert rows[0].excluded_reason == ZERO_VISITS_REPORTED
    return run.id, rows[0].id, person


async def _press_check(page: Any, *, run_id: int, recipient_id: int) -> str:
    """Click the button a person clicks; return the text they then read."""
    await page.goto(f"/ops/campaigns/{run_id}/recipients")
    box = page.locator(STATUS_BOX.format(recipient_id=recipient_id))
    await page.get_by_role("button", name="Проверить историю").click()
    await box.filter(has_text="Проверено").or_(box.filter(has_text="изменила")).wait_for(timeout=WAIT_MS)
    return await box.inner_text()


async def test_the_rendered_result_separates_presence_from_being_counted(
    page, session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Webhook 0, API 1 — and the source booking's own state said in words."""
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    _install(monkeypatch, BrowserEasyWeek(person=person, rows=[_history_row(booking_uuid=person.booking_uuid)]))

    text = await _press_check(page, run_id=run_id, recipient_id=recipient_id)

    assert "Счётчик из события: 0." in text
    assert "Завершённых неотменённых визитов по истории API: 1." in text
    assert "Исходная запись: найдена, завершена и не отменена (учтена в числе визитов)." in text
    assert "Активная будущая запись: нет." in text
    assert "Состояние строки на момент проверки: исключена." in text
    assert "Просмотр не меняет состав preview и не даёт права на выдачу ваучера." in text
    assert str(CUSTOMER_UUID) not in text
    assert person.phone not in text
    assert_no_page_errors(page)


async def test_a_canceled_source_booking_is_not_rendered_as_missing(
    page, session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Present but not counted: the one case the old single flag could not say."""
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    _install(
        monkeypatch,
        BrowserEasyWeek(
            person=person,
            rows=[_history_row(booking_uuid=person.booking_uuid, canceled=True)],
        ),
    )

    text = await _press_check(page, run_id=run_id, recipient_id=recipient_id)

    assert "Исходная запись: найдена, но отменена (в числе визитов не учтена)." in text
    assert "в истории не найдена" not in text
    assert "Завершённых неотменённых визитов по истории API: 0." in text
    assert_no_page_errors(page)


async def test_an_unfinished_source_booking_is_rendered_as_such(
    page, session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    _install(
        monkeypatch,
        BrowserEasyWeek(
            person=person,
            rows=[_history_row(booking_uuid=person.booking_uuid, completed=False)],
        ),
    )

    text = await _press_check(page, run_id=run_id, recipient_id=recipient_id)

    assert "Исходная запись: найдена, но не завершена (в числе визитов не учтена)." in text
    assert_no_page_errors(page)


async def test_a_missing_source_booking_is_rendered_as_absent(
    page, session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    other = uuid.UUID(int=0x8100_0001, version=4)
    _install(monkeypatch, BrowserEasyWeek(person=person, rows=[_history_row(booking_uuid=other)]))

    text = await _press_check(page, run_id=run_id, recipient_id=recipient_id)

    assert "Исходная запись: в истории не найдена." in text
    assert_no_page_errors(page)


async def test_a_temporary_api_failure_is_rendered_as_retryable(
    page, session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An outage reads as an outage, and never as a doubt about the person."""
    from altegio_bot.easyweek_client import EasyWeekRetryableError

    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    _install(
        monkeypatch,
        BrowserEasyWeek(person=person, rows=[], failure=EasyWeekRetryableError("503", status_code=503)),
    )

    text = await _press_check(page, run_id=run_id, recipient_id=recipient_id)

    assert "Временно не удалось проверить историю" in text
    assert "Можно повторить проверку позже." in text
    assert "Эта проверка ничего не изменила." in text
    # No number is invented for an answer that was never read.
    assert "визитов по истории API" not in text
    assert_no_page_errors(page)


async def test_a_stale_tab_does_not_claim_the_person_is_still_excluded(
    page, ops_server: str, session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two tabs. One includes the person; the other must not contradict it.

    The old text appended "Строка осталась исключённой" to every refusal, so the
    tab that had been open while the decision was taken told its operator the
    opposite of what the server had just recorded.
    """
    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    _install(monkeypatch, BrowserEasyWeek(person=person, rows=[_history_row(booking_uuid=person.booking_uuid)]))

    # Tab one: the recipients list, with the check button, loaded BEFORE the
    # decision — exactly the operator who is about to be out of date.
    await page.goto(f"/ops/campaigns/{run_id}/recipients")
    await page.get_by_role("button", name="Проверить историю").wait_for(timeout=WAIT_MS)

    # Tab two: a second real tab doing the ordinary single Add through the form.
    second = watch_for_errors(await page.context.new_page())
    await second.goto(f"{ops_server}/ops/campaigns/{run_id}")
    await second.get_by_role("button", name="Add recipient").first.click()
    await second.locator("#add-phone").fill(person.phone)
    await second.get_by_role("button", name="Добавить", exact=True).click()
    alert = second.locator("#add-recipient-alert")
    await alert.filter(has_text="Получатель добавлен").wait_for(timeout=WAIT_MS)
    assert "included" in await alert.inner_text()

    async with session_maker() as session:
        decided = await session.get(CampaignRecipient, recipient_id)
        assert decided is not None
        assert decided.recipient_basis == RECIPIENT_BASIS_MANUAL
        assert decided.auto_excluded_reason == ZERO_VISITS_REPORTED

    # Back in the stale tab: the button is still drawn, and pressing it must
    # report the decision rather than the state the page remembers.
    box = page.locator(STATUS_BOX.format(recipient_id=recipient_id))
    await page.get_by_role("button", name="Проверить историю").click()
    await box.filter(has_text="изменила").wait_for(timeout=WAIT_MS)
    text = await box.inner_text()

    assert "По этой строке уже принято решение оператора. Обновите страницу." in text
    assert "Эта проверка ничего не изменила." in text
    assert "исключённой" not in text
    assert_no_page_errors(page)
    assert_no_page_errors(second)


async def test_a_permanent_refusal_is_not_rendered_as_retryable(
    page, session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Round 2, item 5: a 403 must never be rendered as "try again later".

    The three families say three different things to do next, and the one thing
    an operator must not be told after a permanent refusal is to come back and
    press the button again.
    """
    from altegio_bot.easyweek_client import EasyWeekAuthError

    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    _install(monkeypatch, BrowserEasyWeek(person=person, rows=[], failure=EasyWeekAuthError("403", status_code=403)))

    text = await _press_check(page, run_id=run_id, recipient_id=recipient_id)

    assert "EasyWeek отказал в запросе" in text
    assert "Можно повторить проверку позже." not in text
    assert "Эта проверка ничего не изменила." in text
    assert "визитов по истории API" not in text
    assert_no_page_errors(page)


async def test_a_malformed_answer_is_rendered_as_a_format_problem(
    page, session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A 200 of the wrong shape is its own answer, not an outage."""
    from altegio_bot.easyweek_client import EasyWeekProtocolError

    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    _install(monkeypatch, BrowserEasyWeek(person=person, rows=[], failure=EasyWeekProtocolError("shape")))

    text = await _press_check(page, run_id=run_id, recipient_id=recipient_id)

    assert "ответ неожидаемого формата" in text
    assert "Можно повторить проверку позже." not in text
    assert "визитов по истории API" not in text
    assert_no_page_errors(page)


async def test_a_deadline_before_the_first_api_call_does_not_blame_easyweek(
    page, session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Round 2, item 5: the local deadline, as an operator actually reads it.

    The check's one deadline covers reading our own rows as well as the network.
    When it expires in the first half, EasyWeek was never asked — and the
    sentence on the screen must not say EasyWeek failed to answer.
    """
    import asyncio

    from altegio_bot.campaigns import easyweek_zero_visit_history as history_module

    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    _install(monkeypatch, BrowserEasyWeek(person=person, rows=[]))
    real_local = history_module._local_facts

    async def slow_local(*args: Any, **kwargs: Any):
        await asyncio.sleep(5)
        return await real_local(*args, **kwargs)

    monkeypatch.setattr(history_module, "_local_facts", slow_local)
    monkeypatch.setattr(history_module, "CHECK_TIMEOUT_SEC", 0.05)

    text = await _press_check(page, run_id=run_id, recipient_id=recipient_id)

    assert "на нашей стороне" in text
    assert "EasyWeek не запрашивался" in text
    assert "EasyWeek не ответил" not in text
    assert "визитов по истории API" not in text
    assert "Эта проверка ничего не изменила." in text
    assert_no_page_errors(page)


async def test_an_unconfigured_integration_does_not_read_as_an_outage(
    page, session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Round 3, item 6: ours, not theirs, in the sentence on the screen.

    No API key means nobody was asked anything. "EasyWeek не ответил" would send
    an operator to look at a branch, and "можно повторить позже" would have them
    pressing a button that cannot start working.
    """
    from altegio_bot.easyweek_client import EasyWeekConfigError

    run_id, recipient_id, _person = await _preview_with_zero_row(session_maker, monkeypatch)

    class Unconfigured:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            raise EasyWeekConfigError("EASYWEEK_API_KEY is not configured")

    monkeypatch.setattr(voucher_mailing_module, "EasyWeekClient", Unconfigured)

    text = await _press_check(page, run_id=run_id, recipient_id=recipient_id)

    assert "интеграция с EasyWeek не настроена" in text
    assert "EasyWeek не ответил" not in text
    assert "Можно повторить проверку позже." not in text
    assert "визитов по истории API" not in text
    assert "Эта проверка ничего не изменила." in text
    assert_no_page_errors(page)


async def test_a_deadline_after_the_api_answered_does_not_blame_easyweek(
    page, session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Round 3, item 6: EasyWeek answered, and then our own phase ran out.

    The operator must read neither "EasyWeek не ответил", which is false, nor a
    visit count, which was never confirmed.
    """
    import asyncio

    from altegio_bot.campaigns import easyweek_zero_visit_history as history_module

    run_id, recipient_id, person = await _preview_with_zero_row(session_maker, monkeypatch)
    _install(monkeypatch, BrowserEasyWeek(person=person, rows=[_history_row(booking_uuid=person.booking_uuid)]))
    real_recheck = history_module._recheck_local_identity

    async def slow_recheck(*args: Any, **kwargs: Any):
        await asyncio.sleep(5)
        return await real_recheck(*args, **kwargs)

    monkeypatch.setattr(history_module, "_recheck_local_identity", slow_recheck)
    monkeypatch.setattr(history_module, "CHECK_TIMEOUT_SEC", 0.3)

    text = await _press_check(page, run_id=run_id, recipient_id=recipient_id)

    assert "на нашей стороне после ответа EasyWeek" in text
    assert "EasyWeek не ответил" not in text
    assert "EasyWeek не запрашивался" not in text
    assert "визитов по истории API" not in text
    assert "Эта проверка ничего не изменила." in text
    assert_no_page_errors(page)


async def test_a_row_without_the_contact_proof_reads_as_its_own_reason(
    page, session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Round 3, item 5: what an old row's operator actually reads.

    Not "личность не доказана", which reads as a statement about the person, and
    not a retry suggestion: the row simply does not carry the proof, and only a
    new preview can give it one.
    """
    from altegio_bot.campaigns.easyweek_zero_visit import ZERO_VISIT_SOURCE_META_KEY

    person = await seed_person(session_maker, 1, customer_uuid=None, capture_phone=True)
    monkeypatch.setattr(segment_module, "SessionLocal", session_maker)
    monkeypatch.setattr(runner_module, "SessionLocal", session_maker)
    run = await runner_module.run_preview(
        RunParams(
            provider=PROVIDER_EASYWEEK,
            company_id=COMPANY_ID,
            location_id=COMPANY_ID,
            period_start=PERIOD_START,
            period_end=PERIOD_END,
            mode="preview",
            campaign_code="new_clients_monthly",
        )
    )
    async with session_maker() as session:
        from sqlalchemy import select

        row = (
            await session.execute(select(CampaignRecipient).where(CampaignRecipient.campaign_run_id == run.id))
        ).scalar_one()
        recipient_id = int(row.id)
    # The exact shape the previous revision stored: no contact proof.
    async with session_maker() as session:
        async with session.begin():
            stored_row = await session.get(CampaignRecipient, recipient_id)
            assert stored_row is not None
            meta = dict(stored_row.meta or {})
            binding = dict(meta[ZERO_VISIT_SOURCE_META_KEY])
            binding.pop("contact_sources", None)
            binding.pop("contact_event_id", None)
            meta[ZERO_VISIT_SOURCE_META_KEY] = binding
            stored_row.meta = meta
    _install(monkeypatch, BrowserEasyWeek(person=person, rows=[_history_row(booking_uuid=person.booking_uuid)]))

    text = await _press_check(page, run_id=run.id, recipient_id=recipient_id)

    assert "нет доказательства единственности контакта" in text
    assert "Можно повторить проверку позже." not in text
    assert "визитов по истории API" not in text
    assert "Эта проверка ничего не изменила." in text
    assert_no_page_errors(page)
