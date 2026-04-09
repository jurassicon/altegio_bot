"""Smart integration test for the monthly newsletter WhatsApp template.

Steps:
  A) Verify the Meta template exists and is APPROVED in WABA.
  B) Idempotency check — skip if a successful test ran within 24 h (unless --force).
  C) Issue a loyalty card via Altegio API.
  D) Send a WhatsApp message using the Meta template.
  E) Poll ``whatsapp_events`` for webhook delivery status.
  F) Cleanup — delete the loyalty card if --cleanup is set.

Exit codes:
  0 = PASS
  2 = FAIL

Usage::

    python -m altegio_bot.scripts.run_test_newsletter_smart \\
      --phone 381638400431 \\
      --company-id 758285 \\
      --booking-link https://n813709.alteg.io/ \\
      --template kitilash_ka_newsletter_new_clients_monthly_v1 \\
      --expect-status delivered \\
      --timeout 180 \\
      --cleanup
"""

from __future__ import annotations

import asyncio
import logging
import random
import string
import sys
from datetime import datetime, timedelta
from typing import Any

import httpx
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.altegio_loyalty import AltegioLoyaltyClient
from altegio_bot.db import SessionLocal
from altegio_bot.models.models import OutboxMessage, SmartTestRun
from altegio_bot.settings import settings
from altegio_bot.utils import utcnow

logger = logging.getLogger("smart_test")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TEST_CODE = "newsletter_smart_test"
IDEMPOTENCY_WINDOW_HOURS = 24
TEMPLATE_LANGUAGE = "de"
EXPECTED_TEMPLATE_ID = "1331641148769327"
DEFAULT_CARD_TYPE_TITLE = "Kundenkarte - 10 %"
_LOYALTY_CARD_PREFIX = "Kundenkarte #"

# Statuses that count as PASS for each --expect-status value.
_PASS_FOR_DELIVERED: frozenset[str] = frozenset({"delivered", "read"})
_PASS_FOR_SENT: frozenset[str] = frozenset({"sent", "delivered", "read"})
_FAIL_STATUS = "failed"


# ---------------------------------------------------------------------------
# A) Meta WABA template verification
# ---------------------------------------------------------------------------


async def check_meta_template(
    template_name: str,
    *,
    access_token: str,
    graph_url: str,
    api_version: str,
    waba_id: str,
    expected_template_id: str | None = None,
) -> tuple[bool, str | None]:
    """Verify *template_name* exists and is APPROVED in the Meta WABA.

    Returns ``(ok, error_message)``.  *ok* is ``True`` iff the template is
    found with ``status == APPROVED``.
    """
    url = f"{graph_url.rstrip('/')}/{api_version}/{waba_id}/message_templates"
    params = {
        "fields": "name,id,status,language,category",
        "name": template_name,
        "access_token": access_token,
    }
    async with httpx.AsyncClient(timeout=20.0) as client:
        resp = await client.get(url, params=params)

    if resp.status_code >= 400:
        return False, (
            f"Meta API returned HTTP {resp.status_code} when checking template {template_name!r}: {resp.text[:300]}"
        )

    data: Any = {}
    try:
        data = resp.json()
    except Exception:
        pass

    templates = []
    if isinstance(data, dict):
        templates = data.get("data", []) or []

    matched = [t for t in templates if isinstance(t, dict) and t.get("name") == template_name]
    if not matched:
        return False, (f"Template {template_name!r} not found in WABA {waba_id!r}.")

    tmpl = matched[0]
    status = (tmpl.get("status") or "").upper()
    if status != "APPROVED":
        return False, (f"Template {template_name!r} status is {status!r}, expected APPROVED.")

    if expected_template_id and str(tmpl.get("id")) != expected_template_id:
        logger.warning(
            "Template id mismatch: got %s, expected %s (not critical, sending by name)",
            tmpl.get("id"),
            expected_template_id,
        )

    logger.info(
        "Template %r APPROVED (id=%s, language=%s, category=%s)",
        template_name,
        tmpl.get("id"),
        tmpl.get("language"),
        tmpl.get("category"),
    )
    return True, None


# ---------------------------------------------------------------------------
# B) Idempotency helpers
# ---------------------------------------------------------------------------


async def _find_recent_success(
    session: AsyncSession,
    *,
    phone_e164: str,
    test_code: str,
    within_hours: int,
) -> SmartTestRun | None:
    """Return the most recent PASS run within *within_hours*, or None."""
    cutoff = utcnow() - timedelta(hours=within_hours)
    stmt = (
        select(SmartTestRun)
        .where(SmartTestRun.phone_e164 == phone_e164)
        .where(SmartTestRun.test_code == test_code)
        .where(SmartTestRun.outcome == "pass")
        .where(SmartTestRun.created_at >= cutoff)
        .order_by(SmartTestRun.created_at.desc())
        .limit(1)
    )
    res = await session.execute(stmt)
    return res.scalar_one_or_none()


# ---------------------------------------------------------------------------
# C) Card number generation
# ---------------------------------------------------------------------------


def _gen_card_number() -> str:
    """Generate a 16-digit test card number.

    Format: ``99`` + ``YYMMDD`` (6 digits) + 8 random digits = 16 total.
    The ``99`` prefix makes test cards easy to identify.
    """
    now = utcnow()
    yymmdd = now.strftime("%y%m%d")
    random_part = "".join(random.choices(string.digits, k=8))
    return f"99{yymmdd}{random_part}"


# ---------------------------------------------------------------------------
# D) Direct Meta template send (bypasses DB sender lookup)
# ---------------------------------------------------------------------------


async def _send_template_direct(
    *,
    phone_e164: str,
    template_name: str,
    language: str,
    params: list[str],
    access_token: str,
    graph_url: str,
    api_version: str,
    phone_number_id: str,
) -> str:
    """Send a Meta WhatsApp template message directly, returning the message id."""
    to_number = phone_e164.lstrip("+").strip()
    url = f"{graph_url.rstrip('/')}/{api_version}/{phone_number_id}/messages"
    payload: dict[str, Any] = {
        "messaging_product": "whatsapp",
        "to": to_number,
        "type": "template",
        "template": {
            "name": template_name,
            "language": {"code": language},
            "components": [
                {
                    "type": "body",
                    "parameters": [{"type": "text", "text": p} for p in params],
                }
            ],
        },
    }
    headers = {"Authorization": f"Bearer {access_token}"}
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(url, headers=headers, json=payload)

    data: Any = {}
    try:
        data = resp.json()
    except Exception:
        data = {}

    if resp.status_code >= 400:
        err = data.get("error") if isinstance(data, dict) else None
        msg = err.get("message") if isinstance(err, dict) else str(data)
        raise RuntimeError(f"Meta send_template failed HTTP {resp.status_code}: {msg}")

    messages = data.get("messages") if isinstance(data, dict) else None
    if isinstance(messages, list) and messages:
        first = messages[0]
        if isinstance(first, dict) and first.get("id"):
            return str(first["id"])

    raise RuntimeError(f"Unexpected Meta response: {data}")


# ---------------------------------------------------------------------------
# E) Status polling
# ---------------------------------------------------------------------------


def _evaluate_outcome(
    statuses_seen: list[str],
    *,
    expect_status: str,
) -> str | None:
    """Return 'pass', 'fail', or None (still waiting).

    A ``failed`` status anywhere is an immediate FAIL.
    """
    if _FAIL_STATUS in statuses_seen:
        return "fail"

    pass_set = _PASS_FOR_DELIVERED if expect_status == "delivered" else _PASS_FOR_SENT
    if any(s in pass_set for s in statuses_seen):
        return "pass"

    return None


async def _poll_statuses(
    provider_message_id: str,
    *,
    expect_status: str,
    timeout_sec: int,
    poll_interval: float = 2.0,
) -> dict[str, Any]:
    """Poll ``whatsapp_events`` until a terminal status is found or timeout.

    Returns a result dict:
      outcome: 'pass' | 'fail' | 'timeout'
      statuses_seen: list of status strings (de-duped, ordered)
      first_seen_at: datetime | None
      last_seen_at: datetime | None
      err_code: str | None
      err_details: str | None
    """
    deadline = utcnow() + timedelta(seconds=timeout_sec)
    statuses_seen: list[str] = []
    first_seen_at: datetime | None = None
    last_seen_at: datetime | None = None
    err_code: str | None = None
    err_details: str | None = None

    sql = text(
        """
        SELECT
          received_at,
          payload #>> \'{entry,0,changes,0,value,statuses,0,status}\' AS status_value,
          payload #>> \'{entry,0,changes,0,value,statuses,0,errors,0,code}\' AS err_code,
          payload #>> \'{entry,0,changes,0,value,statuses,0,errors,0,error_data,details}\'
            AS err_details
        FROM whatsapp_events
        WHERE payload #>> \'{entry,0,changes,0,value,statuses,0,id}\' = :msg_id
        ORDER BY received_at ASC
        """
    )

    while utcnow() < deadline:
        async with SessionLocal() as session:
            res = await session.execute(sql, {"msg_id": provider_message_id})
            rows = res.fetchall()

        for row in rows:
            sv = row.status_value
            if sv and sv not in statuses_seen:
                statuses_seen.append(sv)
                ts = row.received_at
                if first_seen_at is None:
                    first_seen_at = ts
                last_seen_at = ts

            if row.err_code:
                err_code = row.err_code
            if row.err_details:
                err_details = row.err_details

        outcome = _evaluate_outcome(statuses_seen, expect_status=expect_status)
        if outcome is not None:
            return {
                "outcome": outcome,
                "statuses_seen": statuses_seen,
                "first_seen_at": first_seen_at,
                "last_seen_at": last_seen_at,
                "err_code": err_code,
                "err_details": err_details,
            }

        remaining = (deadline - utcnow()).total_seconds()
        if remaining <= 0:
            break

        await asyncio.sleep(min(poll_interval, remaining))

    return {
        "outcome": "timeout",
        "statuses_seen": statuses_seen,
        "first_seen_at": first_seen_at,
        "last_seen_at": last_seen_at,
        "err_code": err_code,
        "err_details": err_details,
    }


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------


async def run_smart_test(
    *,
    phone: str,
    company_id: int,
    location_id: int,
    booking_link: str,
    template_name: str,
    expect_status: str,
    timeout_sec: int,
    cleanup: bool,
    cleanup_on_fail: bool,
    force: bool,
    card_type_id: str | None,
    client_name: str,
    expected_template_id: str | None = None,
) -> int:
    """Orchestrate the full smart test.  Returns exit code (0=PASS, 2=FAIL)."""

    # Normalise phone to e164 (with leading +)
    phone_stripped = phone.lstrip("+").strip()
    phone_e164 = f"+{phone_stripped}"
    phone_int = int(phone_stripped)

    logger.info(
        "Smart test START phone=%s company=%s template=%s expect=%s",
        phone_e164,
        company_id,
        template_name,
        expect_status,
    )

    # -----------------------------------------------------------------------
    # A) Template verification
    # -----------------------------------------------------------------------
    access_token = settings.whatsapp_access_token
    graph_url = settings.whatsapp_graph_url
    api_version = settings.whatsapp_api_version
    waba_id = settings.meta_waba_id
    phone_number_id = settings.meta_wa_phone_number_id

    if not access_token:
        logger.error("FAIL: WHATSAPP_ACCESS_TOKEN is not set")
        return 2
    if not waba_id:
        logger.error("FAIL: META_WABA_ID is not set")
        return 2
    if not phone_number_id:
        logger.error("FAIL: META_WA_PHONE_NUMBER_ID is not set")
        return 2

    ok, err_msg = await check_meta_template(
        template_name,
        access_token=access_token,
        graph_url=graph_url,
        api_version=api_version,
        waba_id=waba_id,
        expected_template_id=expected_template_id or EXPECTED_TEMPLATE_ID,
    )
    if not ok:
        logger.error("FAIL (template check): %s", err_msg)
        return 2

    # -----------------------------------------------------------------------
    # B) Idempotency
    # -----------------------------------------------------------------------
    if not force:
        async with SessionLocal() as session:
            recent = await _find_recent_success(
                session,
                phone_e164=phone_e164,
                test_code=TEST_CODE,
                within_hours=IDEMPOTENCY_WINDOW_HOURS,
            )
        if recent is not None:
            logger.info(
                "PASS (already passed recently): run_id=%s created_at=%s provider_message_id=%s",
                recent.id,
                recent.created_at,
                recent.provider_message_id,
            )
            return 0

    # -----------------------------------------------------------------------
    # C) Issue loyalty card
    # -----------------------------------------------------------------------
    resolved_card_type_id = card_type_id or settings.loyalty_card_type_id or None
    loyalty = AltegioLoyaltyClient()
    card_id: str | None = None
    card_number: str | None = None
    resolved_type_id: str | None = None

    try:
        if not resolved_card_type_id:
            card_types = await loyalty.get_card_types(location_id)
            # First try to find by title
            match = next(
                (t for t in card_types if isinstance(t, dict) and t.get("title") == DEFAULT_CARD_TYPE_TITLE),
                None,
            )
            if match is None and card_types:
                match = card_types[0]
            if match is None:
                await loyalty.aclose()
                logger.error(
                    "FAIL: no loyalty card types found for location_id=%s",
                    location_id,
                )
                return 2
            resolved_card_type_id = str(match.get("id") or match.get("loyalty_card_type_id") or "")
            logger.info(
                "Resolved card_type_id=%s (title=%s)",
                resolved_card_type_id,
                match.get("title"),
            )

        resolved_type_id = resolved_card_type_id
        card_number = _gen_card_number()
        loyalty_card_text = f"{_LOYALTY_CARD_PREFIX}{card_number}"

        logger.info(
            "Issuing loyalty card number=%s type_id=%s phone=%s",
            card_number,
            resolved_type_id,
            phone_int,
        )
        card_resp = await loyalty.issue_card(
            location_id,
            loyalty_card_number=card_number,
            loyalty_card_type_id=resolved_type_id,
            phone=phone_int,
        )
        card_id = str(card_resp.get("id") or card_resp.get("loyalty_card_id") or "")
        issued_number = str(card_resp.get("loyalty_card_number") or card_number)
        card_number = issued_number
        loyalty_card_text = f"{_LOYALTY_CARD_PREFIX}{card_number}"
        logger.info("Card issued: id=%s number=%s", card_id, card_number)

    except Exception as exc:
        await loyalty.aclose()
        logger.error("FAIL (issue card): %s", exc)
        return 2

    # -----------------------------------------------------------------------
    # Create SmartTestRun record (pending)
    # -----------------------------------------------------------------------
    run_record: SmartTestRun | None = None
    async with SessionLocal() as session:
        async with session.begin():
            run_record = SmartTestRun(
                test_code=TEST_CODE,
                phone_e164=phone_e164,
                company_id=company_id,
                location_id=location_id,
                loyalty_card_id=card_id,
                loyalty_card_number=card_number,
                loyalty_card_type_id=resolved_type_id,
                template_name=template_name,
                outcome="pending",
                meta={
                    "booking_link": booking_link,
                    "client_name": client_name,
                    "expect_status": expect_status,
                },
            )
            session.add(run_record)
            await session.flush()
            run_id = run_record.id
            logger.info("SmartTestRun created id=%s", run_id)

    # -----------------------------------------------------------------------
    # D) Send WhatsApp template message
    # -----------------------------------------------------------------------
    template_params = [client_name, booking_link, loyalty_card_text]
    provider_message_id: str | None = None

    try:
        provider_message_id = await _send_template_direct(
            phone_e164=phone_e164,
            template_name=template_name,
            language=TEMPLATE_LANGUAGE,
            params=template_params,
            access_token=access_token,
            graph_url=graph_url,
            api_version=api_version,
            phone_number_id=phone_number_id,
        )
        logger.info("Message sent: provider_message_id=%s", provider_message_id)
    except Exception as exc:
        logger.error("FAIL (send template): %s", exc)
        # Update run record
        async with SessionLocal() as session:
            async with session.begin():
                rec = await session.get(SmartTestRun, run_id)
                if rec is not None:
                    rec.outcome = "fail"
                    rec.meta = dict(rec.meta or {}, send_error=str(exc))
        await _maybe_cleanup(
            loyalty=loyalty,
            location_id=location_id,
            card_id=card_id,
            run_id=run_id,
            should_cleanup=cleanup or cleanup_on_fail,
        )
        await loyalty.aclose()
        return 2

    # Save provider_message_id and create outbox record
    async with SessionLocal() as session:
        async with session.begin():
            rec = await session.get(SmartTestRun, run_id)
            if rec is not None:
                rec.provider_message_id = provider_message_id

            outbox = OutboxMessage(
                company_id=company_id,
                phone_e164=phone_e164,
                template_code=template_name,
                language=TEMPLATE_LANGUAGE,
                body="",
                status="sent",
                provider_message_id=provider_message_id,
                scheduled_at=utcnow(),
                sent_at=utcnow(),
                meta={
                    "send_type": "template",
                    "template": template_name,
                    "params": template_params,
                    "test_code": TEST_CODE,
                },
            )
            session.add(outbox)

    # -----------------------------------------------------------------------
    # E) Poll for delivery status
    # -----------------------------------------------------------------------
    logger.info(
        "Polling for status provider_message_id=%s expect=%s timeout=%ss",
        provider_message_id,
        expect_status,
        timeout_sec,
    )
    poll_result = await _poll_statuses(
        provider_message_id,
        expect_status=expect_status,
        timeout_sec=timeout_sec,
    )

    outcome = poll_result["outcome"]
    statuses_seen = poll_result["statuses_seen"]
    first_seen_at = poll_result["first_seen_at"]
    last_seen_at = poll_result["last_seen_at"]
    poll_err_code = poll_result["err_code"]
    poll_err_details = poll_result["err_details"]

    logger.info(
        "Poll complete: outcome=%s statuses_seen=%s first_seen_at=%s last_seen_at=%s err_code=%s err_details=%s",
        outcome,
        statuses_seen,
        first_seen_at,
        last_seen_at,
        poll_err_code,
        poll_err_details,
    )

    # Map outcome to pass/fail
    final_outcome = "pass" if outcome == "pass" else "fail"
    exit_code = 0 if final_outcome == "pass" else 2

    # Update SmartTestRun
    async with SessionLocal() as session:
        async with session.begin():
            rec = await session.get(SmartTestRun, run_id)
            if rec is not None:
                rec.outcome = final_outcome
                rec.meta = dict(
                    rec.meta or {},
                    poll_result={
                        "statuses_seen": statuses_seen,
                        "first_seen_at": (first_seen_at.isoformat() if first_seen_at else None),
                        "last_seen_at": (last_seen_at.isoformat() if last_seen_at else None),
                        "err_code": poll_err_code,
                        "err_details": poll_err_details,
                        "poll_outcome": outcome,
                    },
                )

    # Update outbox record status
    async with SessionLocal() as session:
        async with session.begin():
            outbox_stmt = (
                select(OutboxMessage)
                .where(OutboxMessage.provider_message_id == provider_message_id)
                .order_by(OutboxMessage.id.desc())
                .limit(1)
            )
            outbox_res = await session.execute(outbox_stmt)
            outbox_row = outbox_res.scalar_one_or_none()
            if outbox_row is not None:
                if "delivered" in statuses_seen:
                    outbox_row.status = "delivered"
                elif "read" in statuses_seen:
                    outbox_row.status = "read"
                elif "sent" in statuses_seen:
                    outbox_row.status = "sent"
                elif outcome == "timeout":
                    outbox_row.status = "sent"  # keep as sent on timeout
                else:
                    outbox_row.status = "failed"
                    outbox_row.error = f"err_code={poll_err_code} err_details={poll_err_details}"

    # -----------------------------------------------------------------------
    # F) Cleanup
    # -----------------------------------------------------------------------
    should_cleanup = cleanup or (cleanup_on_fail and final_outcome == "fail")
    await _maybe_cleanup(
        loyalty=loyalty,
        location_id=location_id,
        card_id=card_id,
        run_id=run_id,
        should_cleanup=should_cleanup,
    )
    await loyalty.aclose()

    # Final summary
    logger.info(
        "=== SUMMARY ===\n"
        "  provider_message_id : %s\n"
        "  statuses_seen       : %s\n"
        "  first_seen_at       : %s\n"
        "  last_seen_at        : %s\n"
        "  outcome             : %s",
        provider_message_id,
        statuses_seen,
        first_seen_at,
        last_seen_at,
        final_outcome.upper(),
    )

    if exit_code == 0:
        logger.info("=== RESULT: PASS ===")
    else:
        logger.error(
            "=== RESULT: FAIL (poll_outcome=%s err_code=%s) ===",
            outcome,
            poll_err_code,
        )

    return exit_code


# ---------------------------------------------------------------------------
# Cleanup helper
# ---------------------------------------------------------------------------


async def _maybe_cleanup(
    *,
    loyalty: AltegioLoyaltyClient,
    location_id: int,
    card_id: str | None,
    run_id: int,
    should_cleanup: bool,
) -> None:
    if not should_cleanup or not card_id:
        return

    try:
        await loyalty.delete_card(location_id, int(card_id))
        logger.info("Card deleted: card_id=%s", card_id)
        delete_status = "deleted"
    except Exception as exc:
        logger.warning("Failed to delete card card_id=%s: %s", card_id, exc)
        delete_status = f"error: {exc}"

    async with SessionLocal() as session:
        async with session.begin():
            rec = await session.get(SmartTestRun, run_id)
            if rec is not None:
                rec.deleted_at = utcnow()
                rec.delete_status = delete_status


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


async def main() -> None:
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(description="Smart integration test for WhatsApp newsletter template.")
    parser.add_argument(
        "--phone",
        required=True,
        help="Recipient phone number (e.g. 381638400431 or +381638400431)",
    )
    parser.add_argument("--company-id", type=int, required=True)
    parser.add_argument(
        "--location-id",
        type=int,
        default=None,
        help="Altegio location id (defaults to --company-id)",
    )
    parser.add_argument(
        "--booking-link",
        default="https://n813709.alteg.io/",
    )
    parser.add_argument(
        "--template",
        default="kitilash_ka_newsletter_new_clients_monthly_v1",
    )
    parser.add_argument(
        "--expect-status",
        choices=["sent", "delivered"],
        default="delivered",
    )
    parser.add_argument("--timeout", type=int, default=180)
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Delete the loyalty card after a PASS.",
    )
    parser.add_argument(
        "--cleanup-on-fail",
        action="store_true",
        help="Also delete the loyalty card after a FAIL.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Ignore idempotency check and always run.",
    )
    parser.add_argument(
        "--card-type-id",
        default=None,
        help="Altegio loyalty_card_type_id (overrides env LOYALTY_CARD_TYPE_ID).",
    )
    parser.add_argument(
        "--client-name",
        default="KitiLash Kunde",
        help="client_name parameter for the template.",
    )
    parser.add_argument(
        "--expected-template-id",
        default=EXPECTED_TEMPLATE_ID,
        help="Expected Meta template id for validation warning (default: %(default)s).",
    )
    args = parser.parse_args()

    location_id = args.location_id if args.location_id is not None else args.company_id

    exit_code = await run_smart_test(
        phone=args.phone,
        company_id=args.company_id,
        location_id=location_id,
        booking_link=args.booking_link,
        template_name=args.template,
        expect_status=args.expect_status,
        timeout_sec=args.timeout,
        cleanup=args.cleanup,
        cleanup_on_fail=args.cleanup_on_fail,
        force=args.force,
        card_type_id=args.card_type_id,
        client_name=args.client_name,
        expected_template_id=args.expected_template_id,
    )
    sys.exit(exit_code)


if __name__ == "__main__":
    asyncio.run(main())
