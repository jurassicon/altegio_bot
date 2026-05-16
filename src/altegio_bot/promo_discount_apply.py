"""Apply a promo discount to a booked Altegio visit.

Two apply modes are supported (see Settings.promo_apply_mode):

record_price_override (default, confirmed working — smoke-tested May 2026):
  GET /record/{location_id}/{record_id}   — fetch fresh record
  PUT /record/{location_id}/{record_id}   — update service price + audit comment
  Simple case  (1 record for client that day, 1 allowed service):
    price override applied automatically, customer notification queued.
  Complex case (multiple records same day or multiple allowed services):
    admin comment written with manual-review marker, lead set to booked,
    no automatic price change and no customer notification.

legacy loyalty-program path (NOT used for automatic apply — kept for
backward compatibility with existing direct-wrapper tests and smoke scripts):
  POST /visit/loyalty/apply_discount_program/{location_id}/{card_id}/{program_id}
  UNCONFIRMED endpoint — source: developer discussion, not OpenAPI spec.

Confirmed Altegio endpoints used elsewhere in this project (NOT this module):
  POST   /loyalty/cards/{location_id}           — issue card
  DELETE /loyalty/cards/{location_id}/{card_id} — delete card
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import httpx
from sqlalchemy import func, or_, select
from sqlalchemy.exc import IntegrityError as SAIntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.altegio_record_update import (
    AltegioRecordUpdateError,
    build_minimal_service_for_put,
    fetch_altegio_record_for_update,
    update_altegio_record_price_and_comment,
)
from altegio_bot.models.models import Client, MessageJob, PromoLead, Record, RecordService
from altegio_bot.settings import Settings, settings

_LOCAL_TZ = ZoneInfo("Europe/Belgrade")

# Regex matching both promo comment markers:
#   [PromoLead:<id>]         — simple automatic price override
#   [PromoLead:<id>:manual]  — complex manual-review annotation
_PROMO_MARKER_RE = re.compile(r"\[PromoLead:\d+(?::\w+)?\]")

# record_updated webhooks triggered by our own promo PUT are suppressed for this
# many seconds after the PUT timestamp stored in PromoLead.meta.
_SUPPRESS_WINDOW_SEC = 300  # 5 minutes

logger = logging.getLogger(__name__)


class PromoDiscountApplyError(Exception):
    """Raised when the Altegio apply_discount_program API call fails."""


@dataclass
class PromoDiscountApplyResult:
    applied: bool
    raw: dict


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _auth_header() -> str:
    return f"Bearer {settings.altegio_partner_token},{settings.altegio_user_token}"


def _headers() -> dict[str, str]:
    return {
        "Authorization": _auth_header(),
        "Accept": settings.altegio_api_accept,
        "Content-Type": "application/json",
    }


async def apply_promo_discount_to_visit(
    *,
    location_id: int,
    card_id: int,
    program_id: int | str,
    record_id: int | str,
) -> PromoDiscountApplyResult:
    """Apply a loyalty discount program to a visit via Altegio API.

    UNCONFIRMED endpoint — see module docstring. Requires
    promo_apply_discount_api_verified=True before calling.

    Raises PromoDiscountApplyError on HTTP, network, JSON, or shape failures.
    """
    if not settings.promo_apply_discount_api_verified:
        raise PromoDiscountApplyError(
            "apply_promo_discount_api_verified=False — API call blocked until endpoint is verified"
        )

    base = settings.altegio_api_base_url.rstrip("/")
    url = f"{base}/visit/loyalty/apply_discount_program/{location_id}/{card_id}/{program_id}"

    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            resp = await client.post(
                url,
                headers=_headers(),
                json={"record_id": int(record_id)},
            )
            resp.raise_for_status()
    except httpx.HTTPError as exc:
        raise PromoDiscountApplyError(
            f"apply_discount_program HTTP error: location={location_id} card={card_id} program={program_id}: {exc}"
        ) from exc

    try:
        data = resp.json()
    except Exception as exc:
        raise PromoDiscountApplyError(f"apply_discount_program invalid JSON: location={location_id}: {exc}") from exc

    if not isinstance(data, dict):
        raise PromoDiscountApplyError(
            f"apply_discount_program unexpected response shape {type(data).__name__}: {data!r}"
        )

    if data.get("success") is not True:
        raise PromoDiscountApplyError(
            f"apply_discount_program unsuccessful response:"
            f" location={location_id} card={card_id} program={program_id}: {data!r}"
        )

    return PromoDiscountApplyResult(applied=True, raw=data)


def _phone_variants(phone_e164: str) -> list[str]:
    digits = re.sub(r"\D+", "", phone_e164)
    variants = {phone_e164, digits, f"+{digits}"}
    return [v for v in variants if v]


def get_promo_allowed_service_ids() -> set[int]:
    """Parse promo_allowed_service_ids setting into a set of int service IDs."""
    raw = (settings.promo_allowed_service_ids or "").strip()
    if not raw:
        return set()
    result: set[int] = set()
    for part in raw.split(","):
        part = part.strip()
        if part:
            try:
                result.add(int(part))
            except ValueError:
                logger.warning("promo_discount: invalid service_id in promo_allowed_service_ids: %r", part)
    return result


def get_promo_network_company_ids() -> set[int]:
    """Parse promo_network_company_ids into a set of int IDs.

    Fail-closed on any invalid value: logs a warning and returns an empty
    set so cross-company apply is blocked rather than silently allowed.
    """
    raw = (settings.promo_network_company_ids or "").strip()
    if not raw:
        return set()
    result: set[int] = set()
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            result.add(int(part))
        except ValueError:
            logger.warning(
                "promo_discount: invalid company_id in promo_network_company_ids: %r — fail-closed",
                part,
            )
            return set()
    return result


def _get_location_id_for_company(company_id: int) -> int | None:
    """Look up location_id for company_id from promo_location_id_by_company."""
    try:
        loc_map = json.loads(settings.promo_location_id_by_company or "{}")
    except Exception:
        logger.warning(
            "promo_discount: invalid JSON in promo_location_id_by_company — fail-closed",
        )
        return None
    val = loc_map.get(str(company_id))
    if val is None:
        logger.warning(
            "promo_discount: no location_id configured for company_id=%d in promo_location_id_by_company",
            company_id,
        )
        return None
    # bool must be checked before int because bool is a subclass of int.
    if isinstance(val, bool):
        logger.warning(
            "promo_discount: invalid location_id %r (boolean not allowed)"
            " for company_id=%d in promo_location_id_by_company",
            val,
            company_id,
        )
        return None
    if isinstance(val, int):
        if val <= 0:
            logger.warning(
                "promo_discount: invalid location_id %d (must be > 0)"
                " for company_id=%d in promo_location_id_by_company",
                val,
                company_id,
            )
            return None
        return val
    if isinstance(val, str):
        try:
            parsed = int(val)
        except ValueError:
            logger.warning(
                "promo_discount: invalid location_id value %r for company_id=%d in promo_location_id_by_company",
                val,
                company_id,
            )
            return None
        if parsed <= 0:
            logger.warning(
                "promo_discount: invalid location_id %d (must be > 0)"
                " for company_id=%d in promo_location_id_by_company",
                parsed,
                company_id,
            )
            return None
        return parsed
    logger.warning(
        "promo_discount: unexpected location_id type %s for company_id=%d in promo_location_id_by_company",
        type(val).__name__,
        company_id,
    )
    return None


def _get_company_bindings(lead: PromoLead) -> dict:
    """Return the company_bindings dict from lead.meta, or an empty dict."""
    meta = lead.meta or {}
    bindings = meta.get("company_bindings")
    return bindings if isinstance(bindings, dict) else {}


def _set_company_binding(
    lead: PromoLead,
    company_id: int,
    data: dict,
) -> None:
    """Write or update a per-company binding entry in lead.meta."""
    meta = lead.meta or {}
    bindings = meta.get("company_bindings")
    if not isinstance(bindings, dict):
        bindings = {}
    bindings[str(company_id)] = data
    lead.meta = {**meta, "company_bindings": bindings}


async def ensure_promo_binding_for_record_company(
    lead: PromoLead,
    *,
    company_id: int,
    phone_e164: str,
    now: datetime,
) -> None:
    """Ensure a per-company binding for company_id exists in lead.meta.

    No-op if the binding already exists.  Attempts client and loyalty card
    provisioning according to settings, recording errors in meta without
    blocking record_price_override apply (fail-open for non-critical steps).

    For loyalty_program mode a missing card binding IS a hard block; callers
    are responsible for checking promo_apply_mode before proceeding.
    """
    company_id_str = str(company_id)
    if company_id_str in _get_company_bindings(lead):
        return

    binding: dict = {
        "source": "network_apply",
        "created_at": now.isoformat(),
    }

    location_id = _get_location_id_for_company(company_id)
    if location_id is not None:
        binding["location_id"] = location_id

    # Client provisioning — gated by promo_altegio_client_api_verified
    if settings.promo_altegio_client_api_verified:
        try:
            from altegio_bot.promo_loyalty import get_or_create_altegio_client

            async with httpx.AsyncClient(timeout=20.0) as _http:
                client_id = await get_or_create_altegio_client(
                    _http,
                    company_id=company_id,
                    phone_e164=phone_e164,
                )
            binding["altegio_client_id"] = client_id
        except Exception as exc:
            binding["altegio_client_error"] = str(exc)
            logger.warning(
                "promo_discount: client provisioning failed company=%d lead_id=%s: %s",
                company_id,
                lead.id,
                exc,
            )
    else:
        binding["client_provisioning_skipped"] = "promo_altegio_client_api_verified=False"

    # Loyalty card provisioning — gated by promo_issue_loyalty_card_enabled
    if settings.promo_issue_loyalty_card_enabled:
        loc_id = binding.get("location_id")
        card_type_id = settings.promo_loyalty_card_type_id
        if loc_id and card_type_id:
            if settings.promo_loyalty_card_api_verified:
                try:
                    from altegio_bot.promo_loyalty import (
                        issue_promo_loyalty_card,
                    )

                    card = await issue_promo_loyalty_card(
                        phone_e164=phone_e164,
                        location_id=int(loc_id),
                        card_type_id=card_type_id,
                    )
                    binding["loyalty_card_id"] = card.loyalty_card_id
                    binding["loyalty_card_number"] = card.loyalty_card_number
                    binding["card_type_id"] = card.card_type_id
                    if card.altegio_client_id is not None:
                        binding["altegio_client_id"] = card.altegio_client_id
                except Exception as exc:
                    binding["loyalty_card_error"] = str(exc)
                    logger.warning(
                        "promo_discount: loyalty card failed company=%d lead_id=%s: %s",
                        company_id,
                        lead.id,
                        exc,
                    )
            else:
                binding["loyalty_card_skipped"] = "promo_loyalty_card_api_verified=False"
        else:
            binding["loyalty_card_skipped"] = f"missing location_id={loc_id} or card_type_id={card_type_id!r}"

    _set_company_binding(lead, company_id, binding)
    logger.info(
        "promo_discount: company binding set company=%d lead_id=%s",
        company_id,
        lead.id,
    )


def is_promo_origin_comment(comment: str | None) -> bool:
    """Return True if ``comment`` contains a promo price-override marker.

    Markers written by this module:
      [PromoLead:<id>]         — simple automatic price override (applied)
      [PromoLead:<id>:manual]  — complex manual-review annotation (booked)

    Used by inbox_worker to suppress the normal record_updated customer
    notification when the update was triggered by our own promo PUT.
    """
    if not comment:
        return False
    return bool(_PROMO_MARKER_RE.search(comment))


def parse_promo_marker(comment: str | None) -> dict[str, object] | None:
    """Parse a promo marker from a comment string.

    Returns ``{'lead_id': int, 'kind': str}`` or ``None`` if no marker found.

    ``kind`` is ``'simple'`` for ``[PromoLead:<id>]`` and ``'manual'`` for
    ``[PromoLead:<id>:manual]``.  Unknown suffixes are passed through as-is
    so callers can treat unrecognised kinds as fail-closed.

    Used by ``_apply_via_record_price_override`` to distinguish between:
    - Our own marker (same lead_id) → recover lead state, skip re-PUT.
    - A different lead's marker → fail-closed (apply_failed).
    """
    if not comment:
        return None
    m = _PROMO_MARKER_RE.search(comment)
    if not m:
        return None
    marker = m.group()
    inner = marker[len("[PromoLead:") : -1]  # strips "[PromoLead:" prefix and "]" suffix
    parts = inner.split(":")
    try:
        lead_id = int(parts[0])
    except (ValueError, IndexError):
        return None
    kind = parts[1] if len(parts) > 1 else "simple"
    return {"lead_id": lead_id, "kind": kind}


async def should_suppress_promo_origin_record_update(
    session: AsyncSession,
    record: Record,
    event: object,
) -> bool:
    """Return True if this record_updated webhook should be suppressed as a promo-PUT echo.

    Called from inbox_worker on every record ``update`` webhook.  Returns True only
    when a PromoLead that triggered a price-override PUT for this record is found and
    the webhook arrived within the 5-minute suppression window.

    Two look-up paths:

    Fast path (comment has marker):
      Parse the lead_id from the ``[PromoLead:<id>]`` or ``[PromoLead:<id>:manual]``
      marker in ``record.comment``, load the matching PromoLead, verify it owns
      this altegio_record_id, then check the PUT timestamp in meta.

      Note: if the marker belongs to a *different* PromoLead (wrong
      ``altegio_record_id`` or unknown lead) the fast path returns False
      immediately — it does NOT fall through to the slow path, because a
      cross-lead marker indicates a state mismatch that deserves human review,
      not silent suppression.

    Slow path (no comment marker):
      Scan PromoLeads by altegio_record_id for any that have a
      ``promo_record_put_at`` within the suppression window.  Covers the edge
      case where Altegio's record_updated webhook is delivered before our own
      DB write of the comment is visible to the record sync.  Each candidate
      is additionally checked for ``promo_record_put_altegio_record_id`` and
      ``promo_record_put_kind`` to guard against stale or mismatched metadata.

    Never suppresses updates that arrive AFTER the 5-minute window, so future
    legitimate edits to a promo-annotated record are not silenced.
    """
    if not record.altegio_record_id:
        return False

    event_received_at = getattr(event, "received_at", None)
    if not isinstance(event_received_at, datetime):
        return False

    # Normalise to UTC for consistent arithmetic
    if event_received_at.tzinfo is None:
        event_received_at = event_received_at.replace(tzinfo=timezone.utc)
    else:
        event_received_at = event_received_at.astimezone(timezone.utc)

    # ── Fast path: extract lead_id from promo marker in comment ──────────────
    comment = record.comment
    if comment:
        m = _PROMO_MARKER_RE.search(comment)
        if m:
            # "[PromoLead:42]" → inner="42"   "[PromoLead:42:manual]" → inner="42:manual"
            marker = m.group()
            inner = marker[len("[PromoLead:") : -1]
            lead_id_str = inner.split(":")[0]
            try:
                lead_id = int(lead_id_str)
                fast_lead = await session.get(PromoLead, lead_id)
                if fast_lead is not None and fast_lead.altegio_record_id == record.altegio_record_id:
                    put_at_str = (fast_lead.meta or {}).get("promo_record_put_at")
                    if put_at_str:
                        try:
                            promo_put_at = datetime.fromisoformat(put_at_str)
                            if promo_put_at.tzinfo is None:
                                promo_put_at = promo_put_at.replace(tzinfo=timezone.utc)
                            delta = (event_received_at - promo_put_at).total_seconds()
                            if 0 <= delta <= _SUPPRESS_WINDOW_SEC:
                                logger.info(
                                    "promo_discount: suppress record_updated (marker path) "
                                    "record_id=%s lead_id=%s delta=%.0fs",
                                    record.id,
                                    fast_lead.id,
                                    delta,
                                )
                                return True
                        except (ValueError, TypeError):
                            pass
                # Lead found via marker but outside window (or wrong record) → do NOT suppress
                return False
            except (ValueError, TypeError):
                pass

    # ── Slow path: scan leads by altegio_record_id ───────────────────────────
    # Limit to 5 candidates: one promo per record is the normal case; a small
    # cap avoids a full-table scan while still covering rare retry scenarios.
    stmt = (
        select(PromoLead)
        .where(PromoLead.altegio_record_id == record.altegio_record_id)
        .where(PromoLead.meta["promo_record_put_at"].astext.is_not(None))
        .limit(5)
    )
    result = await session.execute(stmt)
    candidates = result.scalars().all()

    record_company_id = getattr(record, "company_id", None)
    window_start = event_received_at - timedelta(seconds=_SUPPRESS_WINDOW_SEC)
    for candidate in candidates:
        # Guard: same company — prevents cross-location false suppression.
        if record_company_id is not None and candidate.company_id != record_company_id:
            continue
        # Guard: candidate must reference the same record (by DB id or Altegio id).
        same_record = (candidate.record_id is not None and candidate.record_id == record.id) or (
            candidate.altegio_record_id is not None and candidate.altegio_record_id == record.altegio_record_id
        )
        if not same_record:
            continue
        cmeta = candidate.meta or {}
        put_at_str = cmeta.get("promo_record_put_at")
        if not put_at_str:
            continue
        # Guard: stored altegio_record_id in meta must match (normalized to int
        # to handle cases where it was stored as a string).
        stored_altegio_id_raw = cmeta.get("promo_record_put_altegio_record_id")
        if stored_altegio_id_raw is not None:
            try:
                stored_altegio_id = int(stored_altegio_id_raw)
            except (TypeError, ValueError):
                continue
            if stored_altegio_id != record.altegio_record_id:
                continue
        # Guard: kind must be a known value written by the current code path.
        if cmeta.get("promo_record_put_kind") not in ("simple", "manual"):
            continue
        try:
            promo_put_at = datetime.fromisoformat(put_at_str)
            if promo_put_at.tzinfo is None:
                promo_put_at = promo_put_at.replace(tzinfo=timezone.utc)
            if window_start <= promo_put_at <= event_received_at:
                logger.info(
                    "promo_discount: suppress record_updated (scan path) record_id=%s lead_id=%s",
                    record.id,
                    candidate.id,
                )
                return True
        except (ValueError, TypeError):
            continue

    return False


def get_service_cost_for_discount(service: dict) -> float:
    """Return the effective service cost for discount calculation.

    Priority: ``cost`` field first (if not None), then ``manual_cost``, else 0.0.
    A cost of 0 is a valid price (free service) — it is NOT treated as 'unknown'.
    This prevents the ``or``-chaining bug where a zero cost silently falls through
    to ``manual_cost`` and produces a misleading original price.
    """
    cost = service.get("cost")
    if cost is not None:
        try:
            return float(cost)
        except (TypeError, ValueError):
            pass
    manual_cost = service.get("manual_cost")
    if manual_cost is not None:
        try:
            return float(manual_cost)
        except (TypeError, ValueError):
            pass
    return 0.0


def parse_service_id(value: object) -> int | None:
    """Coerce a service ``id`` value (int or str) to int; return None on failure.

    Altegio sometimes returns service ids as strings in GET responses even when
    the allowlist is configured with integers.  This coercion ensures a string
    ``"12345"`` matches the integer ``12345`` from ``promo_allowed_service_ids``.
    """
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


async def _count_same_day_records_for_client(
    session: AsyncSession,
    *,
    client_id: int | None,
    company_id: int,
    reference_starts_at: datetime | None,
) -> int:
    """Count non-deleted records for the client on the same local calendar day.

    The local timezone is Europe/Belgrade, matching the rest of the project.

    Returns 2 (forces complex case) when ``client_id`` or
    ``reference_starts_at`` is None so the caller treats an unknown date
    context as ambiguous and does not auto-apply a price override.
    """
    if client_id is None or reference_starts_at is None:
        return 2  # unknown → force complex/manual case (fail-closed)

    # Compute day boundaries in local time, then convert to UTC for SQL comparison.
    reference_local = reference_starts_at.astimezone(_LOCAL_TZ)
    local_date = reference_local.date()
    day_start_local = datetime(local_date.year, local_date.month, local_date.day, tzinfo=_LOCAL_TZ)
    day_end_local = day_start_local + timedelta(days=1)
    day_start_utc = day_start_local.astimezone(timezone.utc)
    day_end_utc = day_end_local.astimezone(timezone.utc)

    stmt = (
        select(func.count())
        .select_from(Record)
        .where(Record.client_id == client_id)
        .where(Record.company_id == company_id)
        .where(Record.is_deleted.is_(False))
        .where(Record.starts_at >= day_start_utc)
        .where(Record.starts_at < day_end_utc)
    )
    result = await session.execute(stmt)
    return int(result.scalar_one())


def _build_simple_apply_comment(
    lead: PromoLead,
    *,
    original_cost: float,
    new_cost: float,
    discount_amount: float,
) -> str:
    """Build the audit comment appended to the record on a successful simple apply.

    The closing ``[PromoLead:<id>]`` token is the idempotency marker — its
    presence in the record comment prevents a duplicate PUT on retry.
    """
    campaign = settings.promo_campaign_name
    return (
        f"Promo {campaign}: Neukundenrabatt {discount_amount:g} € automatisch angewendet.\n"
        f"PromoLead ID: {lead.id}\n"
        f"Code: {lead.secret_code}\n"
        f"Original price: {original_cost:g} €\n"
        f"New price: {new_cost:g} €\n"
        f"[PromoLead:{lead.id}]"
    )


def _build_manual_review_comment(lead: PromoLead) -> str:
    """Build the admin annotation for the complex / manual-review case.

    The closing ``[PromoLead:<id>:manual]`` token is the idempotency marker.
    No customer notification is sent when this marker is used.
    """
    campaign = settings.promo_campaign_name
    discount_amount = settings.promo_discount_amount
    return (
        f"Promo {campaign}: Neukundenrabatt {discount_amount:g} € reserviert.\n"
        f"Bitte manuell prüfen/anwenden.\n"
        f"PromoLead ID: {lead.id}\n"
        f"Code: {lead.secret_code}\n"
        f"[PromoLead:{lead.id}:manual]"
    )


async def _recover_lead_from_existing_marker(
    session: AsyncSession,
    lead: PromoLead,
    client: Client,
    record: Record,
    phone_e164: str,
    now: datetime,
    *,
    kind: str,
    source: str,
) -> None:
    """Recover PromoLead state from an existing promo marker confirmed by Altegio.

    Called when GET /record confirms the Altegio comment already carries this
    lead's marker, meaning a prior PUT succeeded but the lead state was not
    persisted (e.g. the process crashed between PUT and DB commit).

    ``source`` should be ``'altegio_comment'`` — recovery only happens after
    fresh Altegio confirmation (P1.1).

    ``kind`` values:
      'simple'  → lead→applied, stale error meta cleared, notification ensured.
      'manual'  → lead→booked, stale error meta cleared, no notification.
      other     → fail-closed (unknown marker format, no state change except
                  apply_failed).

    Stale meta keys cleared on success: ``apply_skip_reason``,
    ``discount_apply_error``, ``discount_apply_attempted_at``.
    """
    meta = lead.meta or {}
    # Strip stale error/skip fields produced by a failed prior attempt.
    cleaned_meta = {
        k: v
        for k, v in meta.items()
        if k not in ("apply_skip_reason", "discount_apply_error", "discount_apply_attempted_at")
    }

    if kind == "simple":
        lead.status = "applied"
        lead.applied_at = lead.applied_at if lead.applied_at is not None else now
        lead.record_id = record.id
        lead.altegio_record_id = record.altegio_record_id
        lead.meta = {
            **cleaned_meta,
            "discount_apply_method": "record_price_override",
            "recovered_from_existing_marker": True,
            "recovered_from_existing_marker_source": source,
            "recovered_from_existing_marker_at": now.isoformat(),
        }
        await _ensure_promo_discount_notification_job(session, lead, client, record, phone_e164, now)
    elif kind == "manual":
        lead.status = "booked"
        lead.record_id = record.id
        lead.altegio_record_id = record.altegio_record_id
        lead.meta = {
            **cleaned_meta,
            "manual_review_required": True,
            "recovered_from_existing_marker": True,
            "recovered_from_existing_marker_source": source,
            "recovered_from_existing_marker_at": now.isoformat(),
        }
    else:
        err = f"unknown promo marker kind={kind!r} in {source} — fail-closed"
        lead.status = "apply_failed"
        lead.meta = {
            **cleaned_meta,
            "discount_apply_error": err,
            "discount_apply_attempted_at": now.isoformat(),
        }
        logger.warning("promo_discount: %s lead_id=%s", err, lead.id)


async def _apply_via_record_price_override(
    session: AsyncSession,
    record: Record,
    lead: PromoLead,
    client: Client,
    phone_e164: str,
    now: datetime,
    cfg: Settings,
    *,
    effective_location_id: int | None = None,
) -> None:
    """Apply promo discount by modifying the service price on the Altegio record.

    Simple case (1 same-day record, 1 allowed service):
      - Fetches fresh record via GET /record.
      - Builds modified services list with discounted price.
      - Sends price + audit comment in one PUT /record.
      - Transitions lead → applied, queues customer notification.

    Complex case (multiple same-day records OR multiple allowed services):
      - Fetches fresh record via GET /record.
      - Sends admin annotation comment in one PUT /record (no price change).
      - Transitions lead → booked with manual_review_required=True.
      - No customer notification is created.

    Both cases are idempotent: if the record comment already contains a promo
    marker the function returns without a second PUT.

    effective_location_id overrides lead.location_id for cross-company apply
    (the record's company may use a different Altegio location than the
    company where the PromoLead was originally issued).
    """
    meta = lead.meta or {}

    # Use caller-supplied location for cross-company, fall back to lead field.
    location_id = effective_location_id if effective_location_id is not None else lead.location_id
    altegio_record_id = record.altegio_record_id

    if not location_id or not altegio_record_id:
        err = f"missing required fields: location_id={location_id} altegio_record_id={altegio_record_id}"
        lead.status = "apply_failed"
        lead.meta = {**meta, "discount_apply_error": err, "discount_apply_attempted_at": now.isoformat()}
        logger.warning("promo_discount: missing fields lead_id=%s %s", lead.id, err)
        return

    # ── Local comment: guard against different-lead markers ───────────────────
    # If the local record comment contains a marker from a *different* lead we
    # fail-closed immediately (no GET) to avoid double-discount.
    # If the marker is ours, we note it and fall through to GET /record —
    # recovery only happens after fresh Altegio confirmation (P1.1): the local
    # DB may be stale (admin may have deleted the discount comment in Altegio).
    local_has_our_marker = False
    parsed_local = parse_promo_marker(record.comment)
    if parsed_local is not None:
        marker_lead_id = int(parsed_local["lead_id"])
        if marker_lead_id == lead.id:
            local_has_our_marker = True  # confirmed below after GET
        else:
            err = f"promo marker in local comment belongs to different lead_id={marker_lead_id}"
            lead.status = "apply_failed"
            lead.meta = {**meta, "discount_apply_error": err, "discount_apply_attempted_at": now.isoformat()}
            logger.warning("promo_discount: %s lead_id=%s", err, lead.id)
            return

    # ── Attendance guard: do not price-override attended / completed records ──
    if (record.attendance or 0) == 1 or (record.visit_attendance or 0) == 1:
        err = "record already attended — price override skipped"
        lead.meta = {**meta, "apply_skip_reason": err}
        logger.info("promo_discount: skip lead_id=%s %s", lead.id, err)
        return

    # allowed_service_ids is config — read once per apply attempt intentionally.
    # Mutable DB state (record services, same-day count) is re-read post-GET.
    allowed_service_ids = get_promo_allowed_service_ids()

    # ── Fetch fresh Altegio record ────────────────────────────────────────────
    try:
        altegio_data = await fetch_altegio_record_for_update(
            location_id=location_id,
            record_id=altegio_record_id,
        )
    except AltegioRecordUpdateError as exc:
        err = str(exc)
        lead.status = "apply_failed"
        lead.meta = {**meta, "discount_apply_error": err, "discount_apply_attempted_at": now.isoformat()}
        logger.warning("promo_discount: GET /record failed lead_id=%s: %s", lead.id, exc)
        return

    # ── Idempotency / recovery: fresh Altegio comment check ──────────────────
    # This is the authoritative idempotency check.  Only recover if Altegio
    # itself confirms the marker is present — local DB alone is not trusted.
    parsed_altegio = parse_promo_marker(altegio_data.get("comment"))
    if parsed_altegio is not None:
        marker_lead_id = int(parsed_altegio["lead_id"])
        if marker_lead_id == lead.id:
            kind = str(parsed_altegio["kind"])
            await _recover_lead_from_existing_marker(
                session,
                lead,
                client,
                record,
                phone_e164,
                now,
                kind=kind,
                source="altegio_comment",
            )
            logger.info(
                "promo_discount: this lead's marker in Altegio comment, recovered lead_id=%s kind=%s",
                lead.id,
                kind,
            )
        else:
            err = f"promo marker in Altegio comment belongs to different lead_id={marker_lead_id}"
            lead.status = "apply_failed"
            lead.meta = {**meta, "discount_apply_error": err, "discount_apply_attempted_at": now.isoformat()}
            logger.warning("promo_discount: %s lead_id=%s", err, lead.id)
        return

    # Local comment had our marker but Altegio does not confirm it.
    # Admin may have edited/deleted the comment in Altegio — fail-closed to
    # prevent a stale recovery that does not reflect the true Altegio state.
    if local_has_our_marker:
        lead.meta = {**meta, "apply_skip_reason": "local_marker_not_confirmed_by_altegio"}
        logger.warning(
            "promo_discount: local marker not confirmed by fresh Altegio record lead_id=%s",
            lead.id,
        )
        return

    # ── Attendance re-check against fresh Altegio data ───────────────────────
    fresh_attendance = int(altegio_data.get("attendance") or 0)
    fresh_visit_attendance = int(altegio_data.get("visit_attendance") or 0)
    if fresh_attendance == 1 or fresh_visit_attendance == 1:
        err = "record already attended in Altegio — price override skipped"
        lead.meta = {**meta, "apply_skip_reason": err}
        logger.info("promo_discount: skip lead_id=%s %s (fresh attendance check)", lead.id, err)
        return

    existing_comment = altegio_data.get("comment") or ""
    altegio_services = altegio_data.get("services") or []

    # ── Re-run mutable guards against fresh local + Altegio data ─────────────
    # allowed_service_ids is config read above (intentionally not re-read here).
    # record_service_ids and same_day_count are mutable DB state — always fresh.
    record_service_ids = await _get_record_service_ids(session, record.id)
    matching_service_ids = record_service_ids.intersection(allowed_service_ids)

    if not matching_service_ids:
        err = "no allowed service in local record after revalidation (fail-closed)"
        lead.status = "apply_failed"
        lead.meta = {**meta, "discount_apply_error": err, "discount_apply_attempted_at": now.isoformat()}
        logger.warning("promo_discount: %s lead_id=%s", err, lead.id)
        return

    same_day_count = await _count_same_day_records_for_client(
        session,
        client_id=record.client_id,
        company_id=record.company_id,
        reference_starts_at=record.starts_at,
    )

    # Cross-check: which allowed services appear in the *fresh Altegio* services
    # list?  If Altegio's view diverges from local (late sync, concurrent edit),
    # we fail-closed rather than applying a discount to the wrong service.
    fresh_allowed_service_ids: set[int] = set()
    for svc in altegio_services:
        if not isinstance(svc, dict):
            continue
        svc_id = parse_service_id(svc.get("id"))
        if svc_id is not None and svc_id in allowed_service_ids:
            fresh_allowed_service_ids.add(svc_id)

    if not fresh_allowed_service_ids:
        err = "no allowed service in fresh Altegio record services (fail-closed)"
        lead.status = "apply_failed"
        lead.meta = {**meta, "discount_apply_error": err, "discount_apply_attempted_at": now.isoformat()}
        logger.warning("promo_discount: no allowed service in fresh Altegio record lead_id=%s", lead.id)
        return

    # Simple case requires all four conditions simultaneously:
    #   1. Exactly one same-day record for this client (local DB, fresh).
    #   2. Exactly one allowed service in the local record (fresh).
    #   3. Exactly one allowed service in the fresh Altegio record.
    #   4. Local and Altegio agree on which service it is.
    is_simple = (
        same_day_count == 1
        and len(matching_service_ids) == 1
        and len(fresh_allowed_service_ids) == 1
        and matching_service_ids == fresh_allowed_service_ids
    )

    if is_simple:
        # ── Simple case: automatic price override ─────────────────────────────
        service_id = next(iter(matching_service_ids))

        target_svc: dict | None = None
        for svc in altegio_services:
            if isinstance(svc, dict) and parse_service_id(svc.get("id")) == service_id:
                target_svc = svc
                break

        if target_svc is None:
            err = f"allowed service id={service_id} not found in fresh Altegio record services"
            lead.status = "apply_failed"
            lead.meta = {**meta, "discount_apply_error": err, "discount_apply_attempted_at": now.isoformat()}
            logger.warning("promo_discount: service missing in Altegio record lead_id=%s", lead.id)
            return

        original_cost = get_service_cost_for_discount(target_svc)
        discount_amount = float(cfg.promo_discount_amount)
        new_cost = max(0.0, original_cost - discount_amount)

        new_services: list[dict] = []
        for svc in altegio_services:
            if not isinstance(svc, dict):
                continue
            if parse_service_id(svc.get("id")) == service_id:
                new_services.append(
                    build_minimal_service_for_put(
                        svc,
                        override_cost=new_cost,
                        override_first_cost=original_cost,
                        override_discount=discount_amount,
                    )
                )
            else:
                new_services.append(build_minimal_service_for_put(svc))

        append_note = _build_simple_apply_comment(
            lead, original_cost=original_cost, new_cost=new_cost, discount_amount=discount_amount
        )
        new_comment = (existing_comment + "\n" + append_note).strip() if existing_comment else append_note

        try:
            put_result = await update_altegio_record_price_and_comment(
                location_id=location_id,
                record_id=altegio_record_id,
                record_data=altegio_data,
                new_services=new_services,
                new_comment=new_comment,
            )
        except AltegioRecordUpdateError as exc:
            err = str(exc)
            lead.status = "apply_failed"
            lead.meta = {**meta, "discount_apply_error": err, "discount_apply_attempted_at": now.isoformat()}
            logger.warning("promo_discount: PUT /record failed lead_id=%s: %s", lead.id, exc)
            return

        # Capture timestamp immediately after the PUT so the suppression window
        # starts from when Altegio actually processed the request, not when we
        # entered this function (which may be seconds earlier after DB queries).
        put_at = _utcnow()

        # Extract Altegio-computed discount percentage (percentage, not our € amount)
        returned_discount: float | None = None
        put_data = put_result.get("data") or {}
        for svc in put_data.get("services") or []:
            if isinstance(svc, dict) and svc.get("id") == service_id:
                raw = svc.get("discount")
                if raw is not None:
                    try:
                        returned_discount = float(raw)
                    except (TypeError, ValueError):
                        pass
                break

        lead.status = "applied"
        lead.applied_at = now
        lead.record_id = record.id
        lead.altegio_record_id = record.altegio_record_id
        lead.meta = {
            **meta,
            "discount_apply_method": "record_price_override",
            "original_cost": original_cost,
            "discounted_cost": new_cost,
            "discount_amount": discount_amount,
            "altegio_record_update_status": "success",
            "discount_apply_attempted_at": now.isoformat(),
            # Suppression window metadata: used by inbox_worker to decide whether
            # to suppress the record_updated echo webhook from this PUT.
            # put_at is captured after the successful PUT (not now) so the window
            # starts as close to the Altegio processing time as possible.
            "promo_record_put_at": put_at.isoformat(),
            "promo_record_put_marker": f"[PromoLead:{lead.id}]",
            "promo_record_put_record_id": record.id,
            "promo_record_put_altegio_record_id": altegio_record_id,
            "promo_record_put_kind": "simple",
            **({"altegio_returned_discount": returned_discount} if returned_discount is not None else {}),
        }

        await _ensure_promo_discount_notification_job(session, lead, client, record, phone_e164, now)
        logger.info(
            "promo_discount: price_override applied lead_id=%s record_id=%s original=%.2f new=%.2f",
            lead.id,
            record.id,
            original_cost,
            new_cost,
        )

    else:
        # ── Complex case: manual review ───────────────────────────────────────
        skip_reason = "multiple_records_same_day" if same_day_count != 1 else "multiple_allowed_services_in_record"

        append_note = _build_manual_review_comment(lead)
        new_comment = (existing_comment + "\n" + append_note).strip() if existing_comment else append_note

        complex_services = [build_minimal_service_for_put(svc) for svc in altegio_services if isinstance(svc, dict)]
        try:
            await update_altegio_record_price_and_comment(
                location_id=location_id,
                record_id=altegio_record_id,
                record_data=altegio_data,
                new_services=complex_services,
                new_comment=new_comment,
            )
        except AltegioRecordUpdateError as exc:
            err = str(exc)
            lead.status = "apply_failed"
            lead.meta = {**meta, "discount_apply_error": err, "discount_apply_attempted_at": now.isoformat()}
            logger.warning("promo_discount: complex PUT /record failed lead_id=%s: %s", lead.id, exc)
            return

        # Capture timestamp immediately after PUT (see simple-case comment above).
        put_at = _utcnow()

        lead.status = "booked"
        lead.record_id = record.id
        lead.altegio_record_id = record.altegio_record_id
        lead.meta = {
            **meta,
            "manual_review_required": True,
            "discount_apply_skip_reason": skip_reason,
            "discount_apply_attempted_at": now.isoformat(),
            # Suppression window metadata: used by inbox_worker to decide whether
            # to suppress the record_updated echo webhook from this PUT.
            "promo_record_put_at": put_at.isoformat(),
            "promo_record_put_marker": f"[PromoLead:{lead.id}:manual]",
            "promo_record_put_record_id": record.id,
            "promo_record_put_altegio_record_id": altegio_record_id,
            "promo_record_put_kind": "manual",
        }
        logger.info(
            "promo_discount: complex case, manual review required lead_id=%s skip_reason=%s",
            lead.id,
            skip_reason,
        )


def _build_notification_body() -> str:
    return (
        "Gute Nachricht 🎁\n\n"
        "Ihr Neukundenrabatt wurde Ihrer Buchung zugeordnet.\n\n"
        "Bitte beachten Sie: In der Online-Buchung und in der ersten Bestätigung "
        "können noch reguläre Preise angezeigt werden. Unser Team sieht den Rabatt "
        "in Ihrer Buchung.\n\n"
        "Wir freuen uns auf Ihren Besuch 💙"
    )


async def _ensure_promo_discount_notification_job(
    session: AsyncSession,
    lead: PromoLead,
    client: Client,
    record: Record,
    phone_e164: str,
    now: datetime,
) -> None:
    """Idempotent: create or find the customer notification MessageJob for this lead.

    Reads existing job by dedupe_key before inserting to prevent duplicate-key errors
    on webhook retries where the Altegio apply already succeeded.

    Writes to lead.meta (without overwriting existing apply metadata):
      customer_notification          = 'queued'
      customer_notification_job_id   = <job.id>
      customer_notification_created_at = <now ISO>
      customer_notification_dedupe_key = <dedupe_key>
    """
    dedupe_key = f"promo_discount_applied:{lead.id}"

    existing = (
        await session.execute(select(MessageJob).where(MessageJob.dedupe_key == dedupe_key))
    ).scalar_one_or_none()

    current_meta = lead.meta or {}

    notification_company_id = record.company_id

    if existing is not None:
        lead.meta = {
            **current_meta,
            "customer_notification": "queued",
            "customer_notification_job_id": existing.id,
            "customer_notification_created_at": now.isoformat(),
            "customer_notification_dedupe_key": dedupe_key,
            "customer_notification_company_id": notification_company_id,
        }
        logger.info(
            "promo_discount: notification job already exists job_id=%s lead_id=%s",
            existing.id,
            lead.id,
        )
        return

    notification_body = _build_notification_body()

    try:
        async with session.begin_nested():
            job = MessageJob(
                company_id=notification_company_id,
                client_id=client.id,
                record_id=record.id,
                job_type="promo_discount_applied",
                run_at=now,
                dedupe_key=dedupe_key,
                payload={
                    "body": notification_body,
                    "phone_e164": phone_e164,
                    "promo_lead_id": lead.id,
                },
            )
            session.add(job)
            await session.flush()
    except SAIntegrityError:
        # Concurrent handler inserted the same job between our SELECT and INSERT.
        # Savepoint was rolled back; re-read the winner and update meta.
        existing = (
            await session.execute(select(MessageJob).where(MessageJob.dedupe_key == dedupe_key))
        ).scalar_one_or_none()
        if existing is None:
            raise
        lead.meta = {
            **current_meta,
            "customer_notification": "queued",
            "customer_notification_job_id": existing.id,
            "customer_notification_created_at": now.isoformat(),
            "customer_notification_dedupe_key": dedupe_key,
            "customer_notification_company_id": notification_company_id,
        }
        logger.warning(
            "promo_discount: concurrent insert detected, using existing job_id=%s lead_id=%s",
            existing.id,
            lead.id,
        )
        return

    lead.meta = {
        **current_meta,
        "customer_notification": "queued",
        "customer_notification_job_id": job.id,
        "customer_notification_created_at": now.isoformat(),
        "customer_notification_dedupe_key": dedupe_key,
        "customer_notification_company_id": notification_company_id,
    }
    logger.info(
        "promo_discount: queued notification job_id=%s lead_id=%s",
        job.id,
        lead.id,
    )


def _lead_same_record(lead: PromoLead, record: Record) -> bool:
    """Return True if lead is bound to the same Altegio record as record."""
    by_pk = lead.record_id is not None and record.id is not None and lead.record_id == record.id
    by_altegio_id = (
        lead.altegio_record_id is not None
        and record.altegio_record_id is not None
        and lead.altegio_record_id == record.altegio_record_id
    )
    return by_pk or by_altegio_id


async def find_applicable_promo_lead_for_record(
    session: AsyncSession,
    *,
    company_id: int,
    phone_e164: str,
    now: datetime,
    record: Record,
    for_update: bool = False,
    expected_lead_id: int | None = None,
) -> PromoLead | None:
    """Return an active PromoLead eligible for discount application.

    Step 1 — same-company lookup (unchanged behaviour):
      Filters: company_id, phone_e164, campaign_name, status in ('issued',
      'booked'), not expired, loyalty_card_id/location_id/discount_program_id
      not null, meta.loyalty_card_issued==true, meta.promo_card_deleted_at
      null.  Returns the lead if found and the booked-lead guard passes.

    Step 2 — network-mode cross-company lookup (promo_network_apply_enabled):
      If step 1 finds nothing and the setting is True, searches across all
      company IDs listed in promo_network_company_ids.  Requires both
      lead.company_id and record.company_id to be in the allowed set.
      Fail-closed when multiple candidates are found.

    Booked-lead rebinding guard (both steps):
      A lead in status 'booked' is returned only when it is already bound to
      the same record (by record_id or altegio_record_id).  A booked lead
      bound to a different record is silently skipped.

    for_update=True locks and refreshes the row for post-I/O revalidation.
    expected_lead_id pins revalidation to the exact candidate found before I/O.
    """
    if not phone_e164:
        return None

    campaign = settings.promo_campaign_name

    # ── Step 1: same-company lookup ───────────────────────────────────────────
    stmt = (
        select(PromoLead)
        .where(PromoLead.company_id == company_id)
        .where(PromoLead.phone_e164 == phone_e164)
        .where(PromoLead.campaign_name == campaign)
        .where(PromoLead.status.in_(["issued", "booked"]))
        .where(PromoLead.expires_at > now)
        .where(PromoLead.loyalty_card_id.is_not(None))
        .where(PromoLead.location_id.is_not(None))
        .where(PromoLead.discount_program_id.is_not(None))
        .where(PromoLead.meta["loyalty_card_issued"].astext == "true")
        .where(PromoLead.meta["promo_card_deleted_at"].astext.is_(None))
        .order_by(PromoLead.created_at.desc())
        .limit(1)
    )
    if expected_lead_id is not None:
        stmt = stmt.where(PromoLead.id == expected_lead_id)
    if for_update:
        stmt = stmt.with_for_update().execution_options(populate_existing=True)

    result = await session.execute(stmt)
    lead = result.scalar_one_or_none()

    if lead is not None and lead.status == "booked":
        if not _lead_same_record(lead, record):
            logger.warning(
                "promo_discount: booked lead_id=%s bound to record_id=%s/%s, skipping current record_id=%s/%s",
                lead.id,
                lead.record_id,
                lead.altegio_record_id,
                record.id,
                record.altegio_record_id,
            )
            lead = None

    if lead is not None:
        return lead

    # ── Step 2: network-mode cross-company lookup ─────────────────────────────
    if not settings.promo_network_apply_enabled:
        return None

    network_ids = get_promo_network_company_ids()
    if not network_ids:
        logger.warning(
            "promo_discount: promo_network_apply_enabled=True but promo_network_company_ids is empty — fail-closed",
        )
        return None

    if company_id not in network_ids:
        logger.info(
            "promo_discount: record company_id=%d not in network_ids=%s — skipping cross-company lookup",
            company_id,
            network_ids,
        )
        return None

    cross_stmt = (
        select(PromoLead)
        .where(PromoLead.company_id.in_(network_ids))
        .where(PromoLead.company_id != company_id)  # never return same-company
        .where(PromoLead.phone_e164 == phone_e164)
        .where(PromoLead.campaign_name == campaign)
        .where(PromoLead.status.in_(["issued", "booked"]))
        .where(PromoLead.expires_at > now)
        .where(PromoLead.loyalty_card_id.is_not(None))
        .where(PromoLead.location_id.is_not(None))
        .where(PromoLead.discount_program_id.is_not(None))
        .where(PromoLead.meta["loyalty_card_issued"].astext == "true")
        .where(PromoLead.meta["promo_card_deleted_at"].astext.is_(None))
    )
    if expected_lead_id is not None:
        cross_stmt = cross_stmt.where(PromoLead.id == expected_lead_id)
    if for_update:
        cross_stmt = cross_stmt.with_for_update().execution_options(populate_existing=True)

    result = await session.execute(cross_stmt)
    candidates = list(result.scalars().all())

    if not candidates:
        return None

    if len(candidates) > 1:
        logger.warning(
            "promo_discount: %d active leads in network mode for phone=%s campaign=%s — fail-closed",
            len(candidates),
            phone_e164,
            campaign,
        )
        return None

    candidate = candidates[0]

    if candidate.status == "booked":
        if not _lead_same_record(candidate, record):
            logger.warning(
                "promo_discount: booked lead_id=%s bound to record_id=%s/%s,"
                " skipping current record_id=%s/%s (network)",
                candidate.id,
                candidate.record_id,
                candidate.altegio_record_id,
                record.id,
                record.altegio_record_id,
            )
            return None

    logger.info(
        "promo_discount: cross-company lead found lead_id=%s lead.company_id=%d record.company_id=%d",
        candidate.id,
        candidate.company_id,
        company_id,
    )
    return candidate


async def _has_prior_attended_visits(
    session: AsyncSession,
    phone_e164: str,
    exclude_record_id: int,
) -> bool:
    """Return True if the client has prior attended visits, excluding the current record.

    Uses only locally synced records (Client + Record tables).
    TODO: A full Altegio CRM API check is deferred to a future PR.
    """
    variants = _phone_variants(phone_e164)
    stmt = (
        select(Record.id)
        .join(Client, Client.id == Record.client_id)
        .where(Client.phone_e164.in_(variants))
        .where(Record.is_deleted.is_(False))
        .where(or_(Record.attendance == 1, Record.visit_attendance == 1))
        .where(Record.id != exclude_record_id)
        .limit(1)
    )
    result = await session.execute(stmt)
    return result.scalar_one_or_none() is not None


async def _get_record_service_ids(session: AsyncSession, record_pk: int) -> set[int]:
    """Return the set of service_ids for a record."""
    stmt = select(RecordService.service_id).where(RecordService.record_id == record_pk)
    result = await session.execute(stmt)
    return set(result.scalars().all())


async def try_apply_promo_discount(
    session: AsyncSession,
    record: Record,
    company_id: int,
    *,
    booking_created_at: datetime | None = None,
    booking_created_at_resolver: Callable[[], Awaitable[datetime | None]] | None = None,
    allow_existing_booking_before_promo: bool = False,
) -> None:
    """Attempt to apply a promo discount to a newly created Altegio visit.

    Called from inbox_worker.handle_event on record create webhooks only.
    Update webhooks are intentionally ignored to avoid applying promo to
    bookings created before the promo was issued.

    booking_created_at must be the confirmed booking creation time from Altegio
    (not the webhook received time). If it is not supplied directly,
    booking_created_at_resolver may be provided; it is called lazily only after
    local PromoLead, service allowlist, and prior-visit checks pass. If the
    timestamp is still None or earlier than PromoLead.issued_at, the discount is
    skipped (fail-closed) to prevent applying a promo to a booking that predates
    the promo campaign.

    Fail-closed: controlled failures are recorded in PromoLead.meta and do not
    propagate as exceptions. Unexpected exceptions propagate to the caller so
    the event can be retried.

    Flow:
    1.  Feature gate check (promo_apply_discount_enabled).
    2.  Resolve client phone from record.
    3.  Find matching PromoLead (filtered by company_id + phone).
    4.  Service allowlist check (promo_allowed_service_ids).
    5.  New-client guard (no prior attended visits, local DB only).
    6.  Resolve booking_created_at lazily, revalidate/lock the same PromoLead,
        re-run mutable local guards, then guard timestamp.
    7.  Transition issued → booked (booking confirmed).
    8.  API gate check (promo_apply_discount_api_verified).
    9.  Route by promo_apply_mode:
          'record_price_override' → _apply_via_record_price_override() (GET+PUT /record).
          other                   → legacy loyalty-program path (steps 10-12).
    10. Validate required fields for loyalty-program path.
    11. Call Altegio apply_discount_program API (legacy path).
    12. Update PromoLead status → applied (legacy path).
    13. Queue a customer WhatsApp notification (MessageJob, job_type='promo_discount_applied').
    """
    cfg = settings

    if not cfg.promo_apply_discount_enabled:
        return

    now = _utcnow()

    if record.client_id is None:
        return

    client = await session.get(Client, record.client_id)
    if client is None or not client.phone_e164:
        return

    phone_e164 = client.phone_e164

    async def _passes_mutable_local_guards(current_lead: PromoLead) -> bool:
        current_meta = current_lead.meta or {}

        # Service allowlist check. This is intentionally re-run after any
        # awaited timestamp lookup because local record services can change.
        allowed_service_ids = get_promo_allowed_service_ids()
        if not allowed_service_ids:
            err = "promo_allowed_service_ids empty — discount not applied automatically"
            current_lead.meta = {**current_meta, "apply_error": err, "apply_skip_reason": err}
            logger.info("promo_discount: skip lead_id=%s %s", current_lead.id, err)
            return False

        record_service_ids = await _get_record_service_ids(session, record.id)
        if not record_service_ids.intersection(allowed_service_ids):
            err = f"no allowed service in record: record_services={record_service_ids} allowed={allowed_service_ids}"
            current_lead.meta = {**current_meta, "apply_error": err, "apply_skip_reason": err}
            logger.info("promo_discount: skip lead_id=%s service not allowed", current_lead.id)
            return False

        # New-client guard. This is local DB only and is also re-run after
        # timestamp lookup because another sync can add attended visits.
        if await _has_prior_attended_visits(session, phone_e164, exclude_record_id=record.id):
            err = "client has prior attended visits — discount not applied"
            current_lead.meta = {**current_meta, "apply_error": err, "apply_skip_reason": err}
            logger.info("promo_discount: skip lead_id=%s prior visited client", current_lead.id)
            return False

        return True

    # ── 3. Find matching PromoLead ────────────────────────────────────────────
    lead = await find_applicable_promo_lead_for_record(
        session,
        company_id=company_id,
        phone_e164=phone_e164,
        now=now,
        record=record,
    )
    if lead is None:
        return

    meta = lead.meta or {}

    # ── 4–5. Mutable local guards ─────────────────────────────────────────────
    # TODO: The prior-visit check uses local DB only and may miss visits not yet
    # synced from Altegio. A full CRM API history check is deferred to a future PR.
    if not await _passes_mutable_local_guards(lead):
        return

    candidate_lead_id = lead.id

    # ── 6. Booking created timestamp guard ────────────────────────────────────
    # Resolve only after cheap local checks. A missing or pre-promo timestamp
    # means the booking may predate this promo campaign, so apply stays blocked.
    if booking_created_at is None and booking_created_at_resolver is not None:
        booking_created_at = await booking_created_at_resolver()
        now = _utcnow()
        lead = await find_applicable_promo_lead_for_record(
            session,
            company_id=company_id,
            phone_e164=phone_e164,
            now=now,
            record=record,
            for_update=True,
            expected_lead_id=candidate_lead_id,
        )
        if lead is None:
            logger.info(
                "promo_discount: lead no longer applicable after booking_created_at lookup record_id=%s",
                record.id,
            )
            return
        if not await _passes_mutable_local_guards(lead):
            return
        meta = lead.meta or {}

    if booking_created_at is None:
        err = "missing booking created timestamp"
        lead.meta = {**meta, "apply_skip_reason": err}
        logger.info("promo_discount: skip lead_id=%s %s", lead.id, err)
        return

    if booking_created_at < lead.issued_at:
        if not allow_existing_booking_before_promo:
            err = "booking predates promo lead"
            lead.meta = {
                **meta,
                "apply_skip_reason": err,
                "booking_created_at": booking_created_at.isoformat(),
                "promo_issued_at": lead.issued_at.isoformat(),
            }
            logger.info("promo_discount: skip lead_id=%s %s", lead.id, err)
            return
        meta = {
            **meta,
            "booking_created_before_promo_allowed": True,
            "booking_created_at": booking_created_at.isoformat(),
            "promo_issued_at": lead.issued_at.isoformat(),
        }
        lead.meta = meta

    # ── 7-pre. Cross-company gate checks (before state transition) ───────────
    # For cross-company leads, validate location_id and the API gate BEFORE
    # issuing the issued → booked transition.  This prevents a lead from
    # becoming 'booked' due to a failed attempt when the apply is impossible.
    # Same-company leads skip this block (is_cross_company=False) and continue
    # with unchanged behaviour through steps 7 and 8.
    is_cross_company = lead.company_id != company_id
    effective_location_id: int | None = None

    if is_cross_company:
        # Only record_price_override is supported for cross-company apply.
        # Check first — cheapest gate, no external calls or lookups.
        if cfg.promo_apply_mode != "record_price_override":
            err = (
                f"cross-company apply not supported for"
                f" promo_apply_mode={cfg.promo_apply_mode!r}"
                " — only 'record_price_override' is supported"
            )
            lead.meta = {
                **meta,
                "discount_apply_error": err,
                "discount_apply_attempted_at": now.isoformat(),
            }
            logger.warning(
                "promo_discount: unsupported mode %r for cross-company apply lead_id=%s",
                cfg.promo_apply_mode,
                lead.id,
            )
            return  # lead stays 'issued'

        effective_location_id = _get_location_id_for_company(company_id)
        if effective_location_id is None:
            err = (
                f"cross-company apply: no location_id for"
                f" record company_id={company_id}"
                " in promo_location_id_by_company — fail-closed"
            )
            lead.meta = {**meta, "apply_skip_reason": err}
            logger.warning("promo_discount: %s lead_id=%s", err, lead.id)
            return  # lead stays 'issued'

        if not cfg.promo_apply_discount_api_verified:
            err = "promo_apply_discount_api_verified=False — cross-company discount apply blocked"
            lead.meta = {
                **meta,
                "discount_apply_error": err,
                "discount_apply_attempted_at": now.isoformat(),
            }
            logger.warning(
                "promo_discount: api not verified, blocking cross-company apply for lead_id=%s",
                lead.id,
            )
            return  # lead stays 'issued'

    # ── 7. Transition issued → booked ─────────────────────────────────────────
    if lead.status == "issued":
        meta = {
            **meta,
            "booked_at": now.isoformat(),
            "booked_record_id": record.id,
            "booked_altegio_record_id": record.altegio_record_id,
        }
        lead.status = "booked"
        lead.meta = meta
        lead.record_id = record.id
        lead.altegio_record_id = record.altegio_record_id
        logger.info(
            "promo_discount: issued→booked lead_id=%s record_id=%s",
            lead.id,
            record.id,
        )

    # ── 7b. Cross-company binding (external calls, after API gate) ────────────
    # ensure_promo_binding_for_record_company may call get_or_create_altegio_client
    # or issue_promo_loyalty_card — only reached after the API gate passed above.
    if is_cross_company:
        await ensure_promo_binding_for_record_company(
            lead,
            company_id=company_id,
            phone_e164=phone_e164,
            now=now,
        )
        meta = lead.meta or {}
        meta = {
            **meta,
            "network_apply": {
                "source_company_id": lead.company_id,
                "applied_company_id": company_id,
                "cross_company": True,
            },
        }
        lead.meta = meta

    # ── 8. API gate (same-company path) ───────────────────────────────────────
    # Cross-company already checked at step 7-pre; this covers same-company.
    if not cfg.promo_apply_discount_api_verified:
        err = "promo_apply_discount_api_verified=False — discount apply blocked until endpoint is verified"
        lead.meta = {
            **meta,
            "discount_apply_error": err,
            "discount_apply_attempted_at": now.isoformat(),
        }
        logger.warning(
            "promo_discount: api not verified, blocking apply for lead_id=%s",
            lead.id,
        )
        return

    # ── 9. Route by apply mode ────────────────────────────────────────────────
    if cfg.promo_apply_mode == "record_price_override":
        await _apply_via_record_price_override(
            session,
            record,
            lead,
            client,
            phone_e164,
            now,
            cfg,
            effective_location_id=effective_location_id,
        )
        return

    # ── Legacy loyalty-program path ───────────────────────────────────────────
    # Cross-company apply is not supported here: the lead's loyalty card and
    # location belong to the source company, not the record's company.
    if is_cross_company:
        err = (
            f"cross-company apply not supported in loyalty_program mode:"
            f" lead.company_id={lead.company_id}"
            f" record.company_id={company_id}"
        )
        lead.status = "apply_failed"
        lead.meta = {
            **meta,
            "discount_apply_error": err,
            "discount_apply_attempted_at": now.isoformat(),
        }
        logger.warning("promo_discount: %s lead_id=%s", err, lead.id)
        return
    # ── 10. Validate required fields ─────────────────────────────────────────
    location_id = lead.location_id
    card_id_raw = lead.loyalty_card_id
    program_id_raw = lead.discount_program_id
    altegio_record_id = record.altegio_record_id

    if not location_id or not card_id_raw or not program_id_raw or not altegio_record_id:
        err = (
            f"missing required fields: location_id={location_id} "
            f"card_id={card_id_raw} program_id={program_id_raw} "
            f"altegio_record_id={altegio_record_id}"
        )
        lead.status = "apply_failed"
        lead.meta = {
            **meta,
            "discount_apply_error": err,
            "discount_apply_attempted_at": now.isoformat(),
        }
        logger.warning("promo_discount: missing fields lead_id=%s %s", lead.id, err)
        return

    try:
        card_id = int(card_id_raw)
    except (ValueError, TypeError) as exc:
        err = f"invalid card_id: {exc}"
        lead.status = "apply_failed"
        lead.meta = {
            **meta,
            "discount_apply_error": err,
            "discount_apply_attempted_at": now.isoformat(),
        }
        logger.warning("promo_discount: invalid card_id lead_id=%s %s", lead.id, err)
        return

    program_id: int | str = program_id_raw

    # ── 11. Call Altegio API (legacy loyalty-program path) ───────────────────
    try:
        api_result = await apply_promo_discount_to_visit(
            location_id=location_id,
            card_id=card_id,
            program_id=program_id,
            record_id=altegio_record_id,
        )
    except PromoDiscountApplyError as exc:
        err = str(exc)
        lead.status = "apply_failed"
        lead.meta = {
            **meta,
            "discount_apply_error": err,
            "discount_apply_attempted_at": now.isoformat(),
        }
        logger.warning("promo_discount: API failed lead_id=%s: %s", lead.id, exc)
        return

    # ── 12. Update PromoLead → applied ───────────────────────────────────────
    lead.status = "applied"
    lead.applied_at = now
    lead.record_id = record.id
    lead.altegio_record_id = record.altegio_record_id
    lead.meta = {
        **meta,
        "discount_applied_at": now.isoformat(),
        "discount_apply_result": api_result.raw,
        "discount_apply_record_id": record.id,
        "discount_apply_altegio_record_id": record.altegio_record_id,
        "discount_apply_location_id": location_id,
        "discount_apply_card_id": card_id,
        "discount_apply_program_id": program_id,
    }

    await _ensure_promo_discount_notification_job(session, lead, client, record, phone_e164, now)
    logger.info(
        "promo_discount: applied lead_id=%s record_id=%s card_id=%s",
        lead.id,
        record.id,
        card_id,
    )


async def process_promo_apply_existing_booking_job(
    session: AsyncSession,
    job: MessageJob,
) -> None:
    """Apply promo discount to a pre-existing future booking.

    Called from outbox_worker for promo_apply_existing_booking jobs.
    Searches local DB only (no external Altegio API calls) for the first
    future non-attended record matching this phone, then attempts to apply
    the promo discount.

    The booking may predate the promo issuance; allow_existing_booking_before_promo
    is passed to try_apply_promo_discount to permit this case.
    """
    now = _utcnow()
    payload = job.payload or {}
    promo_lead_id = payload.get("promo_lead_id")

    if promo_lead_id is None:
        job.status = "failed"
        job.locked_at = None
        job.last_error = "promo_apply_existing_booking: missing promo_lead_id"
        return

    stmt = select(PromoLead).where(PromoLead.id == int(promo_lead_id)).with_for_update()
    result = await session.execute(stmt)
    lead = result.scalar_one_or_none()

    if lead is None:
        job.status = "failed"
        job.locked_at = None
        job.last_error = f"promo_apply_existing_booking: PromoLead not found id={promo_lead_id}"
        return

    if lead.status != "issued":
        job.status = "done"
        job.locked_at = None
        job.last_error = None
        return

    if lead.expires_at <= now:
        job.status = "done"
        job.locked_at = None
        job.last_error = None
        return

    phone_e164 = lead.phone_e164
    variants = _phone_variants(phone_e164)

    # Determine company scope: same-company by default, network if enabled.
    cfg = settings
    if cfg.promo_network_apply_enabled:
        network_ids = get_promo_network_company_ids()
        if network_ids and lead.company_id in network_ids:
            company_filter = Record.company_id.in_(network_ids)
        else:
            company_filter = Record.company_id == lead.company_id
    else:
        company_filter = Record.company_id == lead.company_id

    # Find the first two future non-attended records to detect ambiguity.
    # LIMIT 2: if two records share the same earliest starts_at → ambiguous → fail-closed.
    cand_stmt = (
        select(Record)
        .join(Client, Client.id == Record.client_id)
        .where(Client.phone_e164.in_(variants))
        .where(company_filter)
        .where(Record.is_deleted.is_(False))
        .where(Record.starts_at > now)
        .where(or_(Record.attendance.is_(None), Record.attendance != 1))
        .where(or_(Record.visit_attendance.is_(None), Record.visit_attendance != 1))
        .order_by(Record.starts_at.asc(), Record.id.asc())
        .limit(2)
    )
    result = await session.execute(cand_stmt)
    candidates = list(result.scalars().all())

    if not candidates:
        lead.meta = {**(lead.meta or {}), "existing_booking_skip_reason": "no_future_booking"}
        job.status = "done"
        job.locked_at = None
        job.last_error = None
        return

    if len(candidates) == 2 and candidates[0].starts_at == candidates[1].starts_at:
        lead.meta = {**(lead.meta or {}), "existing_booking_skip_reason": "ambiguous_candidates"}
        job.status = "done"
        job.locked_at = None
        job.last_error = "promo_apply_existing_booking: ambiguous candidates (same starts_at)"
        return

    candidate = candidates[0]

    # Fail-closed: empty allowlist means no automatic apply.
    allowed_service_ids = get_promo_allowed_service_ids()
    if not allowed_service_ids:
        lead.meta = {**(lead.meta or {}), "existing_booking_skip_reason": "service_allowlist_empty"}
        job.status = "done"
        job.locked_at = None
        job.last_error = None
        return

    record_service_ids = await _get_record_service_ids(session, candidate.id)
    if not record_service_ids.intersection(allowed_service_ids):
        lead.meta = {**(lead.meta or {}), "existing_booking_skip_reason": "service_not_allowed"}
        job.status = "done"
        job.locked_at = None
        job.last_error = None
        return

    # Apply — allow existing bookings regardless of whether they predate the promo.
    await try_apply_promo_discount(
        session,
        candidate,
        candidate.company_id,
        booking_created_at=candidate.starts_at,
        allow_existing_booking_before_promo=True,
    )

    job.status = "done"
    job.locked_at = None
    job.last_error = None
