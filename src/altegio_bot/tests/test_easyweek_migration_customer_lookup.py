"""The live customer lookup, and the one distinction the whole stage rests on.

"Not found" authorises creating a customer card. Everything else must not. So
these tests spend most of their effort on the ways a lookup can fail to be an
absence — an auth error, a timeout, a malformed page, a page count that does not
add up, rows that do not carry the number that was asked for — and prove that
none of them is allowed to read as "this person is new".

The rest guard the two things that leak: a phone number in an HTTP log line
(``GET /customers?phone=...`` puts it in the URL), and a phone number in our own
log lines.
"""

from __future__ import annotations

import logging
from typing import Any

import httpx
import pytest

from altegio_bot.easyweek_client import (
    EasyWeekAuthError,
    EasyWeekPermanentError,
    EasyWeekProtocolError,
    EasyWeekRetryableError,
)
from altegio_bot.easyweek_migration.customer_api import (
    DETAIL_ACCESS_DENIED,
    DETAIL_FILTER_UNVERIFIED,
    DETAIL_MALFORMED,
    DETAIL_PAGINATION_INCOMPLETE,
    DETAIL_PAGINATION_UNBOUNDED,
    DETAIL_TRANSPORT,
    LOOKUP_ABSENT,
    LOOKUP_AMBIGUOUS,
    LOOKUP_FIRST_NAME_MISSING,
    LOOKUP_FOUND,
    LOOKUP_PHONE_UNUSABLE,
    LOOKUP_UNDETERMINED,
    CustomerLookupUndetermined,
    PhoneQueryLogFilter,
    lookup_customer_by_phone,
    phone_fingerprint,
    silence_http_request_logs,
    verify_customer,
)
from altegio_bot.easyweek_migration.write_client import (
    EasyWeekMigrationWriteClient,
    EasyWeekUncertainMutation,
    RateLimiter,
)


async def _no_sleep(_seconds: float) -> None:
    """Tests must not spend the rate budget in wall-clock time."""


PHONE = "+4915112345678"
OTHER_PHONE = "+4915199999999"
UUID_A = "77777777-7777-4777-8777-777777777777"
UUID_B = "88888888-8888-4888-8888-888888888888"


def card(uuid: str = UUID_A, *, phone: str = PHONE, first_name: str | None = "Testkundin") -> dict[str, Any]:
    return {"uuid": uuid, "first_name": first_name, "last_name": "M.", "phone": phone}


def page(rows: list[dict[str, Any]], *, current: int = 1, last: int = 1, total: int | None = None) -> dict[str, Any]:
    return {
        "data": rows,
        "meta": {"current_page": current, "last_page": last, "total": len(rows) if total is None else total},
    }


class FakeCustomerReads:
    """A client that answers `list_customers` from a script, and records calls."""

    def __init__(self, pages: list[Any] | None = None, cards: dict[str, Any] | None = None) -> None:
        self.pages = pages or [page([])]
        self.cards = cards or {}
        self.calls: list[dict[str, Any]] = []

    async def list_customers(self, *, params: dict[str, Any]) -> dict[str, Any]:
        self.calls.append(dict(params))
        index = min(int(params.get("page", 1)) - 1, len(self.pages) - 1)
        answer = self.pages[index]
        if isinstance(answer, Exception):
            raise answer
        return answer

    async def get_customer(self, customer_uuid: str) -> dict[str, Any]:
        answer = self.cards.get(customer_uuid)
        if isinstance(answer, Exception):
            raise answer
        if answer is None:
            raise EasyWeekProtocolError("missing", operation="get_customer")
        return answer


# ---------------------------------------------------------------------------
# The happy answers
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_one_exact_match_resolves() -> None:
    client = FakeCustomerReads([page([card()])])
    result = await lookup_customer_by_phone(client, PHONE)

    assert result.outcome == LOOKUP_FOUND
    assert result.uuid == UUID_A
    assert result.card is not None and result.card.first_name == "Testkundin"
    assert result.creatable is False


@pytest.mark.asyncio
async def test_an_empty_workspace_answer_is_a_proven_absence() -> None:
    client = FakeCustomerReads([page([])])
    result = await lookup_customer_by_phone(client, PHONE)

    assert result.outcome == LOOKUP_ABSENT
    assert result.creatable is True


@pytest.mark.asyncio
async def test_the_lookup_is_never_scoped_to_a_branch() -> None:
    """Customers belong to the workspace; a branch filter would answer nothing."""
    client = FakeCustomerReads([page([card()])])
    await lookup_customer_by_phone(client, PHONE)

    sent = client.calls[0]
    for forbidden in ("location", "location_uuid", "location_id", "branch"):
        assert forbidden not in sent, sent
    assert sent["phone"] == PHONE


@pytest.mark.asyncio
async def test_two_cards_on_one_number_are_ambiguous_not_merged() -> None:
    client = FakeCustomerReads([page([card(UUID_A), card(UUID_B)])])
    result = await lookup_customer_by_phone(client, PHONE)

    assert result.outcome == LOOKUP_AMBIGUOUS
    assert result.match_count == 2
    assert result.uuid is None
    assert result.creatable is False


@pytest.mark.asyncio
async def test_the_same_uuid_listed_twice_is_one_customer() -> None:
    client = FakeCustomerReads([page([card(UUID_A), card(UUID_A)])])
    result = await lookup_customer_by_phone(client, PHONE)

    assert result.outcome == LOOKUP_FOUND
    assert result.match_count == 1


@pytest.mark.asyncio
async def test_a_card_without_a_first_name_is_found_but_unaddressable() -> None:
    client = FakeCustomerReads([page([card(first_name=None)])])
    result = await lookup_customer_by_phone(client, PHONE)

    assert result.outcome == LOOKUP_FIRST_NAME_MISSING
    assert result.uuid == UUID_A
    assert result.creatable is False, "an existing card is never re-created"


@pytest.mark.asyncio
async def test_a_card_without_an_email_still_matches() -> None:
    """Excluding cards with no e-mail would invent absences out of thin air."""
    row = card()
    row.pop("email", None)
    client = FakeCustomerReads([page([row])])

    assert (await lookup_customer_by_phone(client, PHONE)).outcome == LOOKUP_FOUND


# ---------------------------------------------------------------------------
# Everything that is NOT an absence
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failure", "detail"),
    [
        (EasyWeekAuthError("denied", operation="list_customers"), DETAIL_ACCESS_DENIED),
        (EasyWeekRetryableError("timeout", operation="list_customers"), DETAIL_TRANSPORT),
        (EasyWeekProtocolError("bad json", operation="list_customers"), DETAIL_TRANSPORT),
    ],
)
async def test_an_api_failure_is_undetermined_never_absent(failure: Exception, detail: str) -> None:
    client = FakeCustomerReads([failure])
    result = await lookup_customer_by_phone(client, PHONE)

    assert result.outcome == LOOKUP_UNDETERMINED
    assert result.detail == detail
    assert result.creatable is False, "an unreachable API must never authorise a POST"


@pytest.mark.asyncio
async def test_a_missing_data_array_is_undetermined() -> None:
    client = FakeCustomerReads([{"meta": {"current_page": 1, "last_page": 1, "total": 0}}])
    result = await lookup_customer_by_phone(client, PHONE)

    assert result.outcome == LOOKUP_UNDETERMINED
    assert result.detail == DETAIL_MALFORMED
    assert result.creatable is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "meta",
    [
        {},
        {"current_page": 1, "last_page": 1},
        {"current_page": "1", "last_page": 1, "total": 0},
        {"current_page": 1, "last_page": True, "total": 0},
        {"current_page": 1, "last_page": 0, "total": 0},
    ],
)
async def test_an_unreadable_envelope_is_never_read_as_one_empty_page(meta: dict[str, Any]) -> None:
    """Assuming a single page is how a second page holding the customer is skipped."""
    client = FakeCustomerReads([{"data": [], "meta": meta}])
    result = await lookup_customer_by_phone(client, PHONE)

    assert result.outcome == LOOKUP_UNDETERMINED
    assert result.creatable is False


@pytest.mark.asyncio
async def test_a_total_larger_than_what_arrived_is_an_unfinished_read() -> None:
    client = FakeCustomerReads([page([], total=3)])
    result = await lookup_customer_by_phone(client, PHONE)

    assert result.outcome == LOOKUP_UNDETERMINED
    assert result.detail == DETAIL_PAGINATION_INCOMPLETE
    assert result.creatable is False


@pytest.mark.asyncio
async def test_a_page_the_server_answered_differently_is_refused() -> None:
    client = FakeCustomerReads([page([card()], current=7, last=7)])
    result = await lookup_customer_by_phone(client, PHONE)

    assert result.outcome == LOOKUP_UNDETERMINED
    assert result.detail == DETAIL_PAGINATION_INCOMPLETE


@pytest.mark.asyncio
async def test_every_page_is_read_before_an_answer_is_given() -> None:
    client = FakeCustomerReads(
        [
            page([card(UUID_A)], current=1, last=2, total=2),
            page([card(UUID_B)], current=2, last=2, total=2),
        ]
    )
    result = await lookup_customer_by_phone(client, PHONE)

    assert [call["page"] for call in client.calls] == [1, 2]
    assert result.outcome == LOOKUP_AMBIGUOUS, "the second page held the second card"


class EndlessCustomerPages:
    """A server that always has one more page. Consistent, and never finished."""

    def __init__(self) -> None:
        self.calls = 0

    async def list_customers(self, *, params: dict[str, Any]) -> dict[str, Any]:
        self.calls += 1
        current = int(params["page"])
        return page([card(f"{current:08d}-0000-4000-8000-000000000000")], current=current, last=9999, total=9999)


@pytest.mark.asyncio
async def test_an_endless_page_set_is_bounded() -> None:
    """A filter that is not filtering must not spend the whole rate budget."""
    client = EndlessCustomerPages()
    result = await lookup_customer_by_phone(client, PHONE)

    assert result.outcome == LOOKUP_UNDETERMINED
    assert result.detail == DETAIL_PAGINATION_UNBOUNDED
    assert client.calls <= 20


@pytest.mark.asyncio
async def test_rows_that_do_not_carry_the_requested_number_prove_nothing() -> None:
    """A 200 is not proof that the filter filtered."""
    client = FakeCustomerReads([page([card(phone=OTHER_PHONE)])])
    result = await lookup_customer_by_phone(client, PHONE)

    assert result.outcome == LOOKUP_UNDETERMINED
    assert result.detail == DETAIL_FILTER_UNVERIFIED
    assert result.creatable is False


@pytest.mark.asyncio
async def test_a_row_without_a_readable_number_is_undetermined() -> None:
    client = FakeCustomerReads([page([{"uuid": UUID_A, "first_name": "A", "phone": "12"}])])
    result = await lookup_customer_by_phone(client, PHONE)

    assert result.outcome == LOOKUP_UNDETERMINED
    assert result.creatable is False


@pytest.mark.asyncio
async def test_an_unusable_source_number_never_reaches_the_api() -> None:
    client = FakeCustomerReads([page([card()])])
    result = await lookup_customer_by_phone(client, "12")

    assert result.outcome == LOOKUP_PHONE_UNUSABLE
    assert client.calls == [], "no request is worth making for a number that is not one"


@pytest.mark.asyncio
async def test_names_are_never_matched_on() -> None:
    """A name match must not rescue a number that is not in the workspace."""
    client = FakeCustomerReads([page([])])
    result = await lookup_customer_by_phone(client, PHONE)

    assert result.outcome == LOOKUP_ABSENT
    assert "name" not in client.calls[0]
    assert "email" not in client.calls[0], "an e-mail filter is a different question"


# ---------------------------------------------------------------------------
# Verifying a card after a creation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_verification_reads_the_workspace_and_checks_the_number() -> None:
    client = FakeCustomerReads(cards={UUID_A: {"data": card()}})
    proven = await verify_customer(client, UUID_A, expected_phone=PHONE)

    assert proven.uuid == UUID_A


@pytest.mark.asyncio
async def test_verification_refuses_a_card_carrying_a_different_number() -> None:
    client = FakeCustomerReads(cards={UUID_A: {"data": card(phone=OTHER_PHONE)}})

    with pytest.raises(CustomerLookupUndetermined):
        await verify_customer(client, UUID_A, expected_phone=PHONE)


@pytest.mark.asyncio
async def test_verification_refuses_a_card_answering_with_another_uuid() -> None:
    client = FakeCustomerReads(cards={UUID_A: {"data": card(UUID_B)}})

    with pytest.raises(CustomerLookupUndetermined):
        await verify_customer(client, UUID_A, expected_phone=PHONE)


@pytest.mark.asyncio
async def test_verification_failure_is_undetermined_not_success() -> None:
    client = FakeCustomerReads(cards={UUID_A: EasyWeekAuthError("denied", operation="get_customer")})

    with pytest.raises(CustomerLookupUndetermined) as raised:
        await verify_customer(client, UUID_A, expected_phone=PHONE)
    assert raised.value.detail == DETAIL_ACCESS_DENIED


# ---------------------------------------------------------------------------
# Keeping the number out of the logs
# ---------------------------------------------------------------------------


def test_the_http_request_logger_is_silenced_and_filtered() -> None:
    httpx_logger = logging.getLogger("httpx")
    httpx_logger.setLevel(logging.INFO)
    silence_http_request_logs()

    assert httpx_logger.level >= logging.WARNING
    assert any(isinstance(existing, PhoneQueryLogFilter) for existing in httpx_logger.filters)


def test_the_filter_scrubs_a_number_out_of_a_url_line() -> None:
    record = logging.LogRecord(
        name="httpx",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg='HTTP Request: GET https://my.easyweek.io/api/public/v2/customers?phone=%2B4915112345678&page=1 "200 OK"',
        args=(),
        exc_info=None,
    )

    assert PhoneQueryLogFilter().filter(record) is True
    assert "4915112345678" not in record.getMessage()
    assert "<redacted>" in record.getMessage()


def test_the_filter_also_scrubs_an_email_parameter() -> None:
    record = logging.LogRecord(
        name="httpx",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="GET /customers?email=kundin%40example.invalid",
        args=(),
        exc_info=None,
    )

    PhoneQueryLogFilter().filter(record)
    assert "kundin" not in record.getMessage()


def test_the_safe_shape_carries_a_fingerprint_not_a_number() -> None:
    client = FakeCustomerReads([page([card()])])
    import asyncio

    result = asyncio.run(lookup_customer_by_phone(client, PHONE))
    safe = result.as_safe_dict()

    assert PHONE not in str(safe)
    assert safe["phone"] == phone_fingerprint(PHONE)
    assert len(safe["phone"]) == 12


def test_the_fingerprint_is_stable_and_not_a_masked_number() -> None:
    assert phone_fingerprint(PHONE) == phone_fingerprint(PHONE)
    assert phone_fingerprint(PHONE) != phone_fingerprint(OTHER_PHONE)
    for fragment in (PHONE, PHONE[-4:], PHONE[:4]):
        assert fragment not in phone_fingerprint(PHONE)


# ---------------------------------------------------------------------------
# Where a customer mutation is allowed to live at all
# ---------------------------------------------------------------------------


def test_the_shared_client_stays_get_only() -> None:
    """The bot's own EasyWeek client must not acquire a way to write a customer."""
    from altegio_bot.easyweek_client import EasyWeekClient

    for forbidden in ("create_customer", "update_customer", "delete_customer", "create_booking"):
        assert not hasattr(EasyWeekClient, forbidden), forbidden


def test_the_migration_client_can_create_but_never_modify_a_customer() -> None:
    """A positive match must not be able to overwrite what EasyWeek already holds."""
    from altegio_bot.easyweek_migration.write_client import EasyWeekMigrationWriteClient

    assert hasattr(EasyWeekMigrationWriteClient, "create_customer")
    for forbidden in ("update_customer", "patch_customer", "delete_customer", "merge_customers"):
        assert not hasattr(EasyWeekMigrationWriteClient, forbidden), forbidden


@pytest.mark.asyncio
async def test_every_customer_request_goes_through_the_pacer() -> None:
    """The 60/min budget covers the new endpoints, not only the old ones."""
    acquired: list[int] = []

    class CountingLimiter(RateLimiter):
        async def acquire(self) -> None:
            acquired.append(1)

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(201, json={"uuid": UUID_A, "phone": PHONE, "first_name": "T"})
        return httpx.Response(200, json=page([card()]))

    client = EasyWeekMigrationWriteClient(
        api_key="k",
        workspace_slug="s",
        transport=httpx.MockTransport(handler),
        rate_limiter=CountingLimiter(sleep=_no_sleep),
    )
    async with client:
        await client.list_customers(params={"phone": PHONE})
        await client.get_customer(UUID_A)
        await client.create_customer({"phone": PHONE, "first_name": "T"})

    assert len(acquired) == 3


@pytest.mark.asyncio
async def test_a_customer_create_never_follows_a_redirect() -> None:
    """A 3xx to another host would post a name and a number somewhere else."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(302, headers={"Location": "https://elsewhere.invalid/customers"})

    client = EasyWeekMigrationWriteClient(
        api_key="k",
        workspace_slug="s",
        transport=httpx.MockTransport(handler),
        sleep=_no_sleep,
        rate_limiter=RateLimiter(requests_per_minute=6000, sleep=_no_sleep),
    )
    async with client:
        with pytest.raises(EasyWeekPermanentError):
            await client.create_customer({"phone": PHONE, "first_name": "T"})


@pytest.mark.asyncio
async def test_a_5xx_after_a_post_is_uncertain_not_failed() -> None:
    posts: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        posts.append(1)
        return httpx.Response(503)

    client = EasyWeekMigrationWriteClient(
        api_key="k",
        workspace_slug="s",
        transport=httpx.MockTransport(handler),
        sleep=_no_sleep,
        rate_limiter=RateLimiter(requests_per_minute=6000, sleep=_no_sleep),
    )
    async with client:
        with pytest.raises(EasyWeekUncertainMutation):
            await client.create_customer({"phone": PHONE, "first_name": "T"})

    assert len(posts) == 1, "an unknown outcome is never retried"


@pytest.mark.asyncio
async def test_a_2xx_with_an_unreadable_body_is_uncertain() -> None:
    client = EasyWeekMigrationWriteClient(
        api_key="k",
        workspace_slug="s",
        transport=httpx.MockTransport(lambda request: httpx.Response(201, content=b"not json")),
        sleep=_no_sleep,
        rate_limiter=RateLimiter(requests_per_minute=6000, sleep=_no_sleep),
    )
    async with client:
        with pytest.raises(EasyWeekUncertainMutation):
            await client.create_customer({"phone": PHONE, "first_name": "T"})


@pytest.mark.asyncio
async def test_the_customer_list_asks_for_the_documented_page_size() -> None:
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(str(request.url))
        return httpx.Response(200, json=page([]))

    client = EasyWeekMigrationWriteClient(
        api_key="k",
        workspace_slug="s",
        transport=httpx.MockTransport(handler),
        sleep=_no_sleep,
        rate_limiter=RateLimiter(requests_per_minute=6000, sleep=_no_sleep),
    )
    async with client:
        await client.list_customers(params={"phone": PHONE})

    assert "per_page=100" in seen[0]
