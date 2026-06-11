"""Read-only Chatwoot API latency probe.

Sends N GET requests to the contacts/search endpoint and prints latency
stats (count/avg/median/min/max) plus a status-code summary. Used by the
internal-route runbook (docs/ops/chatwoot_internal_route.md) to compare the
public Chatwoot URL against internal Docker route candidates.

Reads CHATWOOT_BASE_URL, CHATWOOT_ACCOUNT_ID and CHATWOOT_API_TOKEN from the
environment; --base-url overrides the env URL for probing candidates without
touching .env. GET-only: no Chatwoot mutations, no production writes. The
API token is never printed.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import statistics
import time
from collections import Counter

import httpx

DEFAULT_REQUESTS = 15
DEFAULT_TIMEOUT_SEC = 10.0
DEFAULT_QUERY = "+490000000000"


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="probe_chatwoot_latency",
        description="Read-only latency probe for the Chatwoot contacts/search endpoint.",
    )
    parser.add_argument(
        "--base-url",
        default=None,
        help="Chatwoot base URL to probe (default: CHATWOOT_BASE_URL from env)",
    )
    parser.add_argument(
        "--query",
        default=DEFAULT_QUERY,
        help=f"contacts/search query, e.g. a phone number (default: {DEFAULT_QUERY})",
    )
    parser.add_argument(
        "--requests",
        type=int,
        default=DEFAULT_REQUESTS,
        help=f"number of requests to send (default: {DEFAULT_REQUESTS})",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT_SEC,
        help=f"per-request timeout in seconds (default: {DEFAULT_TIMEOUT_SEC})",
    )
    return parser


def format_stats(durations: list[float], statuses: list[str]) -> str:
    """Format latency/status summary lines. Pure helper, unit-tested."""
    lines = [f"count={len(statuses)}"]
    if durations:
        lines.extend(
            [
                f"avg={statistics.fmean(durations):.3f}s",
                f"median={statistics.median(durations):.3f}s",
                f"min={min(durations):.3f}s",
                f"max={max(durations):.3f}s",
            ]
        )
    status_summary = " ".join(f"{status}x{count}" for status, count in sorted(Counter(statuses).items()))
    lines.append(f"statuses: {status_summary or '<none>'}")
    return "\n".join(lines)


async def _run(args: argparse.Namespace) -> int:
    base_url = (args.base_url or os.getenv("CHATWOOT_BASE_URL", "")).strip().rstrip("/")
    account_id = os.getenv("CHATWOOT_ACCOUNT_ID", "").strip()
    api_token = os.getenv("CHATWOOT_API_TOKEN", "").strip()

    if not base_url:
        print("ERROR: no base URL (set CHATWOOT_BASE_URL or pass --base-url)")
        return 2
    if not account_id:
        print("ERROR: CHATWOOT_ACCOUNT_ID is not set")
        return 2
    if not api_token:
        print("ERROR: CHATWOOT_API_TOKEN is not set")
        return 2

    url = f"{base_url}/api/v1/accounts/{account_id}/contacts/search"
    print(f"base_url={base_url}")
    print(f"requests={args.requests} timeout={args.timeout}s query={args.query}")

    durations: list[float] = []
    statuses: list[str] = []
    async with httpx.AsyncClient(timeout=args.timeout) as client:
        for _ in range(args.requests):
            started = time.perf_counter()
            try:
                res = await client.get(
                    url,
                    headers={"api_access_token": api_token},
                    params={"q": args.query},
                )
            except httpx.HTTPError as exc:
                statuses.append(type(exc).__name__)
                continue
            durations.append(time.perf_counter() - started)
            statuses.append(str(res.status_code))

    print(format_stats(durations, statuses))
    return 0 if statuses and all(status == "200" for status in statuses) else 1


def main() -> None:
    raise SystemExit(asyncio.run(_run(_build_parser().parse_args())))


if __name__ == "__main__":
    main()
