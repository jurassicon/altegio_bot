"""Operator preflight for the non-persistent EasyWeek voucher calculation.

What it does, in this order and nothing else:

1. reviewed GETs — ``/workspace``, ``/locations``, ``/voucher-templates`` and
   the exact ``/voucher-templates/{uuid}``;
2. exactly one ``POST /orders/calculate`` for one voucher line;
3. the exact ``/voucher-templates/{uuid}`` again, to compare the counters.

Usage::

    python -m altegio_bot.scripts.easyweek_voucher_contract_preflight \\
        --confirm-nonpersistent-calculate

Without that flag the command performs **no HTTP call at all** — not even the
reads — and exits with the argument code.

Deliberately not here
---------------------
This is not a web endpoint, not a worker and not a job. It opens no ORM
session, writes no row, creates no ``CampaignRun``/``CampaignRecipient``/
``MessageJob``/outbox entry, and stores no raw response on disk. Location,
template and price cannot be passed in: the location and template are the
confirmed constants, and the price is read from the *fresh* template.

``exit 0`` means one calculation was proven non-persistent at that moment. It is
NOT permission to issue a voucher, to charge anybody, or to send a message. The
report says so on every run, and the exit code cannot be stored as a standing
authorization.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from typing import Any, Final

from altegio_bot.campaigns.easyweek_voucher_contract import (
    GIFT_CARD_CALCULATION_CONFIGURATION_UNAVAILABLE,
    GIFT_CARD_CALCULATION_RETRYABLE_UNCERTAINTY,
    VoucherCalculator,
    VoucherContractEvidence,
    VoucherTemplateReader,
    probe_voucher_calculation_contract,
)
from altegio_bot.easyweek_client import EasyWeekClient, EasyWeekConfigError
from altegio_bot.easyweek_voucher_calculation import EasyWeekVoucherCalculationClient

# Distinct, stable exit codes so a wrapper can tell the three failures apart
# without parsing text. EXIT_OK is evidence, never authorization.
EXIT_OK: Final = 0
EXIT_ARGUMENTS: Final = 2
EXIT_RETRYABLE_UNCERTAINTY: Final = 3
EXIT_CONTRACT_MISMATCH: Final = 4

CONFIRMATION_FLAG: Final = "--confirm-nonpersistent-calculate"
GIFT_CARD_CALCULATION_NOT_CONFIRMED: Final = "gift_card_calculation_not_confirmed"

# Repeated on every run, green included, so a green line can never be read on
# its own as a permission.
SEND_AUTHORIZATION_NOTICE: Final = "calculation_evidence_is_not_send_authorization"


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="easyweek_voucher_contract_preflight",
        description=(
            "One non-persistent EasyWeek voucher calculation as operator evidence. "
            "This is not permission to issue a voucher or to send a message."
        ),
        # Abbreviations are off on purpose: argparse would otherwise accept
        # `--confirm` as this flag, and a half-typed word must not authorise a
        # POST against production.
        allow_abbrev=False,
    )
    parser.add_argument(
        CONFIRMATION_FLAG,
        dest="confirmed",
        action="store_true",
        help="Required. Without it the command makes no HTTP request at all.",
    )
    return parser


def _print_json(payload: dict[str, Any]) -> None:
    # stdout carries the safe report and nothing else; library logs go to stderr.
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2))


def _report(evidence: VoucherContractEvidence) -> dict[str, Any]:
    return {**evidence.as_safe_dict(), "send_authorization": SEND_AUTHORIZATION_NOTICE}


def _nothing_proven_report(reason: str) -> dict[str, Any]:
    """A run that never reached the API: the same shape, every proof absent.

    Built from the same dataclass as a real report rather than hand-written, so
    a field added to the evidence cannot go missing here.
    """
    return _report(
        VoucherContractEvidence(
            workspace_proven=False,
            location_proven=False,
            template_proven=False,
            template_pristine=False,
            template_counters_before=None,
            template_counters_after=None,
            template_counters_unchanged=False,
            calculation_contract_ready=False,
            account_paid_amount_observed=None,
            reasons=(reason,),
        )
    )


def exit_code_for(evidence: VoucherContractEvidence) -> int:
    """Green, "we do not know", or "the contract does not hold" — never merged."""
    if evidence.calculation_contract_ready:
        return EXIT_OK
    if GIFT_CARD_CALCULATION_RETRYABLE_UNCERTAINTY in evidence.reasons:
        return EXIT_RETRYABLE_UNCERTAINTY
    return EXIT_CONTRACT_MISMATCH


async def run_preflight(
    reader: VoucherTemplateReader,
    calculator: VoucherCalculator,
) -> VoucherContractEvidence:
    """The testable core: no ORM session, no argument parsing, no printing."""
    return await probe_voucher_calculation_contract(reader, calculator)


async def _run_with_real_clients() -> VoucherContractEvidence:
    # Two separate clients on purpose: the GET-only one keeps the reads, and the
    # calculate-only one is the sole POST surface.
    async with EasyWeekClient() as reader, EasyWeekVoucherCalculationClient() as calculator:
        return await run_preflight(reader, calculator)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )
    args = _build_parser().parse_args(argv)

    if not args.confirmed:
        # Checked before any client is constructed: an unconfirmed run must not
        # reach the network even to read.
        _print_json(_nothing_proven_report(GIFT_CARD_CALCULATION_NOT_CONFIRMED))
        return EXIT_ARGUMENTS

    try:
        evidence = asyncio.run(_run_with_real_clients())
    except EasyWeekConfigError:
        # A missing key or slug is an operator configuration problem, reported
        # as a stable reason — never as the provider's own message.
        _print_json(_nothing_proven_report(GIFT_CARD_CALCULATION_CONFIGURATION_UNAVAILABLE))
        return EXIT_ARGUMENTS

    _print_json(_report(evidence))
    return exit_code_for(evidence)


if __name__ == "__main__":
    raise SystemExit(main())
