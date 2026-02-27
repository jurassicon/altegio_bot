"""Tests for run_test_newsletter_smart helpers."""
from __future__ import annotations

import pytest
import respx
import httpx

from altegio_bot.scripts.run_test_newsletter_smart import (
    _evaluate_outcome,
    _gen_card_number,
    check_meta_template,
)

GRAPH_URL = 'https://graph.facebook.com'
API_VERSION = 'v20.0'
WABA_ID = 'waba123'
TOKEN = 'tok'


# ---------------------------------------------------------------------------
# _gen_card_number
# ---------------------------------------------------------------------------

def test_gen_card_number_length() -> None:
    num = _gen_card_number()
    assert len(num) == 16, f'Expected 16 digits, got {len(num)}: {num!r}'


def test_gen_card_number_all_digits() -> None:
    num = _gen_card_number()
    assert num.isdigit(), f'Expected all digits: {num!r}'


def test_gen_card_number_starts_with_99() -> None:
    num = _gen_card_number()
    assert num.startswith('99'), f'Expected 99 prefix: {num!r}'


# ---------------------------------------------------------------------------
# _evaluate_outcome
# ---------------------------------------------------------------------------

def test_evaluate_outcome_pass_delivered_when_delivered() -> None:
    assert _evaluate_outcome(['sent', 'delivered'], expect_status='delivered') == 'pass'


def test_evaluate_outcome_pass_delivered_when_read() -> None:
    assert _evaluate_outcome(['read'], expect_status='delivered') == 'pass'


def test_evaluate_outcome_pending_when_only_sent_expect_delivered() -> None:
    assert _evaluate_outcome(['sent'], expect_status='delivered') is None


def test_evaluate_outcome_pass_sent_when_sent() -> None:
    assert _evaluate_outcome(['sent'], expect_status='sent') == 'pass'


def test_evaluate_outcome_fail_on_failed() -> None:
    assert _evaluate_outcome(['sent', 'failed'], expect_status='delivered') == 'fail'


def test_evaluate_outcome_fail_takes_priority() -> None:
    # 'failed' overrides any pass-worthy statuses
    assert _evaluate_outcome(['delivered', 'failed'], expect_status='delivered') == 'fail'


def test_evaluate_outcome_none_when_empty() -> None:
    assert _evaluate_outcome([], expect_status='delivered') is None


# ---------------------------------------------------------------------------
# check_meta_template (mocked HTTP)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_check_meta_template_approved() -> None:
    url = (
        f'{GRAPH_URL}/{API_VERSION}/{WABA_ID}/message_templates'
    )
    with respx.mock:
        respx.get(url).mock(
            return_value=httpx.Response(
                200,
                json={
                    'data': [
                        {
                            'id': '1331641148769327',
                            'name': 'kitilash_ka_newsletter_new_clients_monthly_v2',
                            'status': 'APPROVED',
                            'language': 'de',
                            'category': 'MARKETING',
                        }
                    ]
                },
            )
        )
        ok, err = await check_meta_template(
            'kitilash_ka_newsletter_new_clients_monthly_v2',
            access_token=TOKEN,
            graph_url=GRAPH_URL,
            api_version=API_VERSION,
            waba_id=WABA_ID,
        )

    assert ok is True
    assert err is None


@pytest.mark.asyncio
async def test_check_meta_template_not_found() -> None:
    url = f'{GRAPH_URL}/{API_VERSION}/{WABA_ID}/message_templates'
    with respx.mock:
        respx.get(url).mock(
            return_value=httpx.Response(200, json={'data': []})
        )
        ok, err = await check_meta_template(
            'nonexistent_template',
            access_token=TOKEN,
            graph_url=GRAPH_URL,
            api_version=API_VERSION,
            waba_id=WABA_ID,
        )

    assert ok is False
    assert err is not None
    assert 'not found' in err


@pytest.mark.asyncio
async def test_check_meta_template_not_approved() -> None:
    url = f'{GRAPH_URL}/{API_VERSION}/{WABA_ID}/message_templates'
    with respx.mock:
        respx.get(url).mock(
            return_value=httpx.Response(
                200,
                json={
                    'data': [
                        {
                            'id': '999',
                            'name': 'some_template',
                            'status': 'PENDING',
                            'language': 'de',
                            'category': 'MARKETING',
                        }
                    ]
                },
            )
        )
        ok, err = await check_meta_template(
            'some_template',
            access_token=TOKEN,
            graph_url=GRAPH_URL,
            api_version=API_VERSION,
            waba_id=WABA_ID,
        )

    assert ok is False
    assert err is not None
    assert 'PENDING' in err


@pytest.mark.asyncio
async def test_check_meta_template_http_error() -> None:
    url = f'{GRAPH_URL}/{API_VERSION}/{WABA_ID}/message_templates'
    with respx.mock:
        respx.get(url).mock(
            return_value=httpx.Response(401, json={'error': 'Unauthorized'})
        )
        ok, err = await check_meta_template(
            'some_template',
            access_token='bad_token',
            graph_url=GRAPH_URL,
            api_version=API_VERSION,
            waba_id=WABA_ID,
        )

    assert ok is False
    assert err is not None
    assert '401' in err
