"""Local UUID-first identities for operator-assigned EasyWeek recipients.

Workspace customer lookup does not prove a branch. A new identity can only be
assigned explicitly to Karlsruhe, and never moves an existing EasyWeek card.
A matching opt-out anywhere in the local database vetoes the operation.
"""

from __future__ import annotations

import hashlib
import uuid

from sqlalchemy import or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.models.models import Client
from altegio_bot.utils import utcnow

IDENTITY_CONFLICT = "manual_recipient_identity_conflict"
BRANCH_ASSIGNMENT_REQUIRED = "manual_recipient_branch_assignment_required"
CLIENT_AMBIGUOUS = "manual_recipient_local_client_ambiguous"
CLIENT_OPTED_OUT = "manual_recipient_opted_out"
KARLSRUHE_COMPANY_ID = 322579


async def lock_workspace_identity(session: AsyncSession) -> None:
    """Serialize absent-row and cross-branch identity writes, without HTTP."""
    await session.execute(text("SELECT pg_advisory_xact_lock(724410210071::bigint)"))


async def lock_identity(session: AsyncSession, *, phone: str, customer_uuid: str) -> None:
    """Serialize cross-preview creation, including absent-row races.

    Lock both keys in stable order. The worker uses this same lock when attaching
    the first real booking to an identity that has no numeric customer ID yet.
    """
    await lock_workspace_identity(session)
    keys = sorted({f"easyweek-identity-phone:{phone}", f"easyweek-identity-uuid:{customer_uuid}"})
    for value in keys:
        key = int.from_bytes(hashlib.sha256(value.encode()).digest()[:8], "big", signed=True)
        await session.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": key})


async def local_identity(
    session: AsyncSession,
    *,
    company_id: int,
    phone: str,
    customer_uuid: str | None = None,
    lock: bool = False,
) -> tuple[Client | None, str | None]:
    """Read all matching identities. ``None, None`` means creation is possible.

    Altegio rows are only read for opt-out and ambiguity: they never provide an
    EasyWeek numeric ID, branch proof, visit proof, or template name.
    """
    identity = uuid.UUID(customer_uuid) if customer_uuid else None
    where = Client.phone_e164 == phone
    if identity is not None:
        where = or_(where, (Client.provider == "easyweek") & (Client.easyweek_customer_uuid == identity))
    query = select(Client).where(where).order_by(Client.id)
    if lock:
        query = query.with_for_update()
    rows = list((await session.scalars(query)).all())
    if any(row.wa_opted_out for row in rows):
        return None, CLIENT_OPTED_OUT
    easyweek = [row for row in rows if row.provider == "easyweek"]
    if len(easyweek) > 1:
        identities = {row.easyweek_customer_uuid for row in easyweek}
        numeric_ids = {row.altegio_client_id for row in easyweek if row.altegio_client_id is not None}
        if None in identities or len(identities) != 1 or len(numeric_ids) > 1:
            return None, CLIENT_AMBIGUOUS
    # Several legacy identities for the same phone cannot establish one person.
    if len([row for row in rows if row.provider != "easyweek"]) > 1:
        return None, IDENTITY_CONFLICT
    if not easyweek:
        # An unaddressable numeric card cannot be proven to be a different
        # person. Refuse a second manual card until an ordinary captured phone
        # or independent UUID binding resolves it. The worker still ingests
        # ordinary phone-less bookings when no UUID adoption is in question.
        unresolved = await session.scalar(
            select(Client.id)
            .where(
                Client.provider == "easyweek",
                Client.company_id == company_id,
                Client.easyweek_customer_uuid.is_(None),
                Client.phone_e164.is_(None),
            )
            .limit(1)
        )
        if unresolved is not None:
            return None, IDENTITY_CONFLICT
        return None, None
    branch_clients = [row for row in easyweek if row.company_id == company_id]
    # A real booking may establish another branch card; a workspace customer
    # alone cannot authorise the manual import to infer branch membership.
    if len(branch_clients) != 1:
        return None, IDENTITY_CONFLICT
    client = branch_clients[0]
    if client.phone_e164 != phone:
        return None, IDENTITY_CONFLICT
    if identity is not None and client.easyweek_customer_uuid not in (None, identity):
        return None, IDENTITY_CONFLICT
    return client, None


async def ensure_local_identity(
    session: AsyncSession,
    *,
    company_id: int,
    phone: str,
    customer_uuid: str,
    first_name: str,
    assign_karlsruhe: bool,
) -> tuple[Client | None, str | None]:
    """Apply only after live proof, inside the recipient write transaction."""
    await lock_identity(session, phone=phone, customer_uuid=customer_uuid)
    client, reason = await local_identity(
        session, company_id=company_id, phone=phone, customer_uuid=customer_uuid, lock=True
    )
    if reason is not None:
        return None, reason
    identity = uuid.UUID(customer_uuid)
    if client is not None:
        client.easyweek_customer_uuid = identity
        return client, None
    if company_id != KARLSRUHE_COMPANY_ID or not assign_karlsruhe:
        return None, BRANCH_ASSIGNMENT_REQUIRED
    client = Client(
        provider="easyweek",
        company_id=company_id,
        altegio_client_id=None,
        easyweek_customer_uuid=identity,
        easyweek_identity_assigned_at=utcnow(),
        phone_e164=phone,
        display_name=first_name,
        raw={},
    )
    session.add(client)
    await session.flush()
    return client, None
