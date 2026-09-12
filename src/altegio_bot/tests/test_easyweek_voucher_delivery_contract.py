"""The three contracts a voucher delivery rests on (§36).

The keyed binding that lets the code stay unstored, the approved template that
decides what a customer actually reads, and the one Meta request whose answer
decides whether anybody was messaged at all.

No database and no network: every HTTP interaction runs through
``httpx.MockTransport`` and every identity is synthetic.
"""

from __future__ import annotations

import hashlib
import hmac
import json

import httpx
import pytest

from altegio_bot.campaigns.easyweek_voucher_delivery import template_contract
from altegio_bot.campaigns.easyweek_voucher_delivery.binding import (
    MIN_KEY_BYTES,
    VoucherBindingKeyError,
    binding_key_reason,
    voucher_code_mac,
    voucher_code_matches,
)
from altegio_bot.campaigns.easyweek_voucher_delivery.delivery import (
    DELIVERY_ACCEPTED,
    DELIVERY_REJECTED,
    DELIVERY_UNKNOWN,
    VoucherDeliveryClient,
)
from altegio_bot.campaigns.easyweek_voucher_delivery.identity import (
    HMAC_KEY_INVALID,
    HMAC_KEY_MISSING,
    TEMPLATE_MISMATCH,
    TEMPLATE_UNPROVEN,
)
from altegio_bot.tests.easyweek_voucher_delivery_fixtures import (
    PROVIDER_MESSAGE_ID,
    VOUCHER_CODE_SENTINEL,
    meta_template,
)

KEY = "k" * 48
KEY_ID = "test-key-1"
LEDGER = "17"
ORDER = "dddddddd-4444-4444-8444-dddddddddddd"
TEMPLATE = "49bc000c-c3a6-47c7-bdfd-b8ccd3ae2677"


def _mac(**changes) -> tuple[str, str]:
    args = {
        "voucher_code": VOUCHER_CODE_SENTINEL,
        "ledger_uuid": LEDGER,
        "target_order_uuid": ORDER,
        "voucher_template_uuid": TEMPLATE,
        "key": KEY,
        "key_id": KEY_ID,
    }
    args.update(changes)
    return voucher_code_mac(**args)


def _matches(**changes) -> bool:
    key_id, mac = _mac()
    args = {
        "voucher_code": VOUCHER_CODE_SENTINEL,
        "expected_mac": mac,
        "expected_key_id": key_id,
        "ledger_uuid": LEDGER,
        "target_order_uuid": ORDER,
        "voucher_template_uuid": TEMPLATE,
        "key": KEY,
        "key_id": KEY_ID,
    }
    args.update(changes)
    return voucher_code_matches(**args)


# ---------------------------------------------------------------------------
# The keyed binding
# ---------------------------------------------------------------------------


def test_the_binding_is_not_the_code_and_not_a_plain_digest_of_it() -> None:
    """A SHA-256 of a short code is the code wearing a hat.

    Voucher codes are short and drawn from a small alphabet, so an unkeyed
    digest of one can be enumerated. The stored value must be neither the
    plaintext nor anything derivable without the secret.
    """
    _, mac = _mac()

    assert VOUCHER_CODE_SENTINEL not in mac
    assert mac != hashlib.sha256(VOUCHER_CODE_SENTINEL.encode()).hexdigest()
    assert mac != hashlib.sha256(json.dumps(VOUCHER_CODE_SENTINEL).encode()).hexdigest()
    assert len(mac) == 64


def test_the_same_code_in_the_same_place_verifies() -> None:
    assert _matches() is True


def test_a_binding_cannot_be_carried_to_another_ledger_row() -> None:
    """Copying the MAC into a second row must prove nothing there.

    Without this, a second canary row could inherit a first one's proof and
    "verify" a code that was never issued for it.
    """
    assert _matches(ledger_uuid="18") is False


def test_a_binding_cannot_be_carried_to_another_order() -> None:
    assert _matches(target_order_uuid="eeeeeeee-5555-4555-8555-eeeeeeeeeeee") is False


def test_a_different_code_does_not_verify() -> None:
    assert _matches(voucher_code=VOUCHER_CODE_SENTINEL + "x") is False


def test_a_rotated_key_refuses_rather_than_comparing_against_the_wrong_secret() -> None:
    """The stored key id names which secret made the MAC.

    After a rotation the honest answer is "this binding was made with a key I no
    longer have", not a mismatch somebody might investigate as tampering.
    """
    key_id, mac = _mac()

    assert (
        voucher_code_matches(
            voucher_code=VOUCHER_CODE_SENTINEL,
            expected_mac=mac,
            expected_key_id=key_id,
            ledger_uuid=LEDGER,
            target_order_uuid=ORDER,
            voucher_template_uuid=TEMPLATE,
            key="j" * 48,
            key_id="test-key-2",
        )
        is False
    )


def test_a_missing_binding_never_verifies() -> None:
    assert _matches(expected_mac=None) is False
    assert _matches(expected_key_id=None) is False


def test_the_comparison_uses_a_constant_time_primitive() -> None:
    """An ordinary `==` leaks, by timing, how many leading characters matched."""
    import inspect

    from altegio_bot.campaigns.easyweek_voucher_delivery import binding

    source = inspect.getsource(binding.voucher_code_matches)
    assert "compare_digest" in source
    assert hmac.compare_digest is not None


@pytest.mark.parametrize("key", ["", "   ", "k" * (MIN_KEY_BYTES - 1)])
def test_an_unusable_key_stops_the_workflow_before_anything_external(key) -> None:
    """A short or absent secret is a refusal, never a downgrade to no key."""
    expected = HMAC_KEY_MISSING if not key.strip() else HMAC_KEY_INVALID

    with pytest.raises(VoucherBindingKeyError) as excinfo:
        _mac(key=key)

    assert excinfo.value.reason == expected
    assert binding_key_reason(key=key, key_id=KEY_ID) == expected


def test_a_missing_key_id_is_also_a_refusal() -> None:
    assert binding_key_reason(key=KEY, key_id="") == HMAC_KEY_MISSING


def test_no_key_material_appears_in_the_refusal() -> None:
    with pytest.raises(VoucherBindingKeyError) as excinfo:
        _mac(key="short")

    assert "short" not in str(excinfo.value)


def test_the_encoding_cannot_be_confused_between_fields() -> None:
    """Length prefixes: ``("ab","c")`` must not encode like ``("a","bc")``."""
    _, first = _mac(ledger_uuid="ab", target_order_uuid="c")
    _, second = _mac(ledger_uuid="a", target_order_uuid="bc")

    assert first != second


# ---------------------------------------------------------------------------
# The approved template
# ---------------------------------------------------------------------------


def test_the_voucher_template_is_not_the_old_ten_percent_newsletter() -> None:
    """A different offer needs a different template and a different approval."""
    assert template_contract.VOUCHER_META_TEMPLATE_NAME != "kitilash_ka_newsletter_new_clients_monthly_v1"
    body = template_contract.VOUCHER_TEMPLATE_BODY
    assert "10" not in body
    assert "Kundenkarte" not in body
    assert "Gutschein" in body and "15 €" in body


def test_the_body_declares_exactly_three_positional_parameters() -> None:
    positional = template_contract.positional_body()

    assert template_contract.VOUCHER_TEMPLATE_FIELDS == ("client_name", "voucher_code", "booking_link")
    for index in (1, 2, 3):
        assert positional.count("{{" + str(index) + "}}") == 1
    assert "{{4}}" not in positional
    # The voucher code is the second slot, which is what the send path fills.
    assert positional.index("{{2}}") > positional.index("{{1}}")


def test_an_approved_matching_template_proves_out() -> None:
    proof = template_contract.prove_meta_templates([meta_template()])

    assert proof.proven is True
    assert proof.meta_verified is True
    assert proof.reason is None


@pytest.mark.parametrize(
    "changes",
    [
        {"status": "PENDING"},
        {"status": "REJECTED"},
        {"category": "UTILITY"},
        {"language": "en"},
        {"parameter_format": "NAMED"},
        {"components": [{"type": "BODY", "text": "something else"}]},
        {
            "components": [
                {"type": "HEADER", "format": "IMAGE"},
                {"type": "BODY", "text": template_contract.positional_body()},
            ]
        },
        {
            "components": [
                {"type": "BODY", "text": template_contract.positional_body()},
                {"type": "FOOTER", "text": "footer"},
            ]
        },
        {
            "components": [
                {"type": "BODY", "text": template_contract.positional_body()},
                {"type": "BUTTONS", "buttons": []},
            ]
        },
        {"components": []},
    ],
)
def test_anything_but_the_exact_approved_shape_blocks(changes) -> None:
    proof = template_contract.prove_meta_templates([meta_template(**changes)])

    assert proof.proven is False
    assert proof.reason in {TEMPLATE_MISMATCH, TEMPLATE_UNPROVEN}


def test_an_absent_parameter_format_is_unproven_rather_than_assumed() -> None:
    """Slot two carries a bearer secret; "probably positional" is not enough."""
    template = meta_template()
    del template["parameter_format"]

    assert template_contract.meta_template_blocker(template) == TEMPLATE_MISMATCH


def test_a_body_with_a_fourth_placeholder_blocks() -> None:
    template = meta_template(components=[{"type": "BODY", "text": template_contract.positional_body() + " {{4}}"}])

    assert template_contract.meta_template_blocker(template) == TEMPLATE_MISMATCH


def test_a_template_for_another_branch_never_answers_for_this_one() -> None:
    other = meta_template(name="kitilash_du_new_client_voucher_v1")

    assert template_contract.select_meta_templates([other]) == []
    assert template_contract.prove_meta_templates([other]).reason == TEMPLATE_UNPROVEN


def test_two_rows_for_the_same_name_are_an_ambiguity_not_a_choice() -> None:
    proof = template_contract.prove_meta_templates([meta_template(), meta_template()])

    assert proof.proven is False
    assert proof.reason == TEMPLATE_UNPROVEN


def test_a_missing_template_is_unproven() -> None:
    assert template_contract.prove_meta_templates([]).reason == TEMPLATE_UNPROVEN


# ---------------------------------------------------------------------------
# The one Meta request
# ---------------------------------------------------------------------------


def _client(handler) -> VoucherDeliveryClient:
    return VoucherDeliveryClient(
        access_token="SENTINEL_TOKEN",
        graph_url="https://graph.example.invalid",
        api_version="v21.0",
        transport=httpx.MockTransport(handler),
    )


async def _send(client: VoucherDeliveryClient):
    return await client.send_voucher_template(
        phone_number_id="PNID",
        to_e164="+4915112345678",
        template_name=template_contract.VOUCHER_META_TEMPLATE_NAME,
        language="de",
        params=["Name", VOUCHER_CODE_SENTINEL, "https://example.invalid/"],
    )


@pytest.mark.asyncio
async def test_a_2xx_with_one_message_id_is_accepted_and_only_accepted() -> None:
    """Accepted means Meta took the message, not that a phone showed it."""
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={"messages": [{"id": PROVIDER_MESSAGE_ID}]})

    async with _client(handler) as client:
        outcome = await _send(client)

    assert outcome.outcome == DELIVERY_ACCEPTED
    assert outcome.provider_message_id == PROVIDER_MESSAGE_ID
    assert len(seen) == 1
    assert seen[0].method == "POST"
    assert seen[0].url.path.endswith("/PNID/messages")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response",
    [
        {"json": {"messages": []}},
        {"json": {"messages": [{"id": ""}]}},
        {"json": {"messages": [{"id": PROVIDER_MESSAGE_ID}, {"id": "wamid.second"}]}},
        {"json": {"messages": [{}]}},
        {"json": {"ok": True}},
        {"text": "not json", "headers": {"content-type": "text/plain"}},
    ],
)
async def test_a_2xx_without_one_usable_message_id_is_unknown(response) -> None:
    """The message probably went out and we have nothing to match a webhook to."""
    headers = response.pop("headers", None)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, headers=headers, **response)

    async with _client(handler) as client:
        outcome = await _send(client)

    assert outcome.outcome == DELIVERY_UNKNOWN
    assert outcome.provider_message_id is None


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [409, 429, 500, 502, 503, 504])
async def test_a_doubtful_status_is_unknown_and_never_retried(status) -> None:
    calls: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(status)
        return httpx.Response(status, json={"error": {"message": "x"}})

    async with _client(handler) as client:
        outcome = await _send(client)

    assert outcome.outcome == DELIVERY_UNKNOWN
    assert len(calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("exc", [httpx.ReadTimeout("t"), httpx.ConnectError("c"), httpx.RemoteProtocolError("r")])
async def test_a_transport_failure_is_unknown_and_never_retried(exc) -> None:
    calls: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        raise exc

    async with _client(handler) as client:
        outcome = await _send(client)

    assert outcome.outcome == DELIVERY_UNKNOWN
    assert len(calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [301, 302, 307, 308])
async def test_a_redirect_is_never_followed(status) -> None:
    """Following a 307 would re-send the voucher code wherever it pointed."""
    calls: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        return httpx.Response(status, headers={"location": "https://evil.example.invalid/"})

    async with _client(handler) as client:
        outcome = await _send(client)

    assert outcome.outcome == DELIVERY_UNKNOWN
    assert len(calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [400, 401, 403, 404, 422])
async def test_a_proven_meta_refusal_is_rejected(status) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, json={"error": {"message": "refused", "code": 100}})

    async with _client(handler) as client:
        outcome = await _send(client)

    assert outcome.outcome == DELIVERY_REJECTED


@pytest.mark.asyncio
async def test_a_4xx_without_metas_own_envelope_stays_unknown() -> None:
    """An edge or a proxy cannot tell us whether Meta acted."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(403, text="<html>Forbidden</html>", headers={"content-type": "text/html"})

    async with _client(handler) as client:
        outcome = await _send(client)

    assert outcome.outcome == DELIVERY_UNKNOWN


@pytest.mark.asyncio
async def test_the_request_carries_the_code_and_the_log_does_not(caplog) -> None:
    """The one place the plaintext may appear is the request body itself."""
    import logging

    caplog.set_level(logging.DEBUG)
    bodies: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        bodies.append(request.content.decode())
        return httpx.Response(200, json={"messages": [{"id": PROVIDER_MESSAGE_ID}]})

    async with _client(handler) as client:
        outcome = await _send(client)

    assert VOUCHER_CODE_SENTINEL in bodies[0]
    recorded = "\n".join(record.getMessage() for record in caplog.records)
    assert VOUCHER_CODE_SENTINEL not in recorded
    assert VOUCHER_CODE_SENTINEL not in repr(outcome)
    assert VOUCHER_CODE_SENTINEL not in json.dumps(outcome.as_safe_dict())
