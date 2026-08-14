"""Seed phase-1 EasyWeek templates and senders for every registry location.

The seed is provider-scoped, idempotent and delete-free.  Before the first
database write it checks every configured UUID against live ``GET /locations``
and prints the corresponding API name, making the API an independent identity
source instead of comparing one environment value with another.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.db import SessionLocal
from altegio_bot.easyweek_branches import (
    BranchContent,
    BranchProfile,
    branch_profile_for_slug,
    branch_template_contract,
    normalize_api_name,
)
from altegio_bot.easyweek_client import EasyWeekClient, EasyWeekError
from altegio_bot.easyweek_locations import EasyWeekLocation, configured_easyweek_locations
from altegio_bot.easyweek_policy import (
    RECORD_CANCELED,
    RECORD_CREATED,
    RECORD_CREATED_NEW_CLIENT,
    RECORD_UPDATED,
    REMINDER_2H,
    REMINDER_24H,
    validate_static_booking_page,
)
from altegio_bot.models.models import PROVIDER_EASYWEEK, MessageTemplate, WhatsAppSender
from altegio_bot.settings import settings

LANGUAGE = "de"
# PR-8 adds the two reminder codes here rather than in a second seeding path:
# a reminder row must be bound to its branch by exactly the contract the
# lifecycle rows already use — one branch's Meta name, body and footer.
TEMPLATE_CODES = (
    RECORD_CREATED,
    RECORD_CREATED_NEW_CLIENT,
    RECORD_UPDATED,
    RECORD_CANCELED,
    REMINDER_24H,
    REMINDER_2H,
)


class SeedConfigError(RuntimeError):
    """The environment or API identity check is not safe enough to write."""


@dataclass(frozen=True)
class VerifiedBranch:
    """A branch whose registry entry, API identity and profile all agree."""

    location: EasyWeekLocation
    profile: BranchProfile
    api_name: str

    @property
    def content(self) -> BranchContent:
        # Content comes from the PROFILE, never from the operator-supplied
        # prefix: that indirection is what let Durlach content be seeded under
        # Rastatt's company id.
        return self.profile.content


@dataclass(frozen=True)
class SeedPlan:
    branches: tuple[VerifiedBranch, ...]
    language: str
    phone_number_id: str


@dataclass(frozen=True)
class SeedResult:
    templates_created: int = 0
    templates_updated: int = 0
    template_duplicates: int = 0
    senders_created: int = 0
    senders_updated: int = 0


def _resolve_language() -> str:
    language = (getattr(settings, "easyweek_default_language", "") or "").strip().lower()
    if language != LANGUAGE:
        raise SeedConfigError(
            f"EASYWEEK_DEFAULT_LANGUAGE must be {LANGUAGE!r}; all seeded bodies and Meta templates are German."
        )
    return language


def _resolve_phone_number_id() -> str:
    phone_number_id = (getattr(settings, "meta_wa_phone_number_id", "") or "").strip()
    if not phone_number_id:
        raise SeedConfigError(
            "META_WA_PHONE_NUMBER_ID is not configured; sender rows would point at no WhatsApp number."
        )
    return phone_number_id


def _configured_seed_locations() -> tuple[tuple[EasyWeekLocation, BranchProfile], ...]:
    """Bind every registry entry to a trusted source-controlled profile.

    The registry's TOP-LEVEL KEY selects the profile — a stable branch slug an
    operator cannot silently repurpose — and the operator-supplied
    ``meta_template_prefix`` must then agree with it. Deriving the profile from
    the prefix instead (the old behaviour) is exactly how Durlach content ended
    up seeded under Rastatt's company id: the prefix is data, not identity.
    """
    registry = configured_easyweek_locations()
    if not registry.configured:
        raise SeedConfigError("EASYWEEK_LOCATION_MAP is empty; refusing to seed an unconfigured deployment.")
    if not registry.valid:
        raise SeedConfigError("EASYWEEK_LOCATION_MAP is invalid; refusing to seed a partial or ambiguous registry.")

    result: list[tuple[EasyWeekLocation, BranchProfile]] = []
    for location in registry.locations.values():
        profile = branch_profile_for_slug(location.name)
        if profile is None:
            raise SeedConfigError(
                f"EASYWEEK_LOCATION_MAP branch {location.name!r} has no source-controlled profile; "
                "add an approved branch profile before seeding it."
            )
        if location.meta_template_prefix != profile.meta_template_prefix:
            raise SeedConfigError(
                f"EASYWEEK_LOCATION_MAP branch {location.name!r} declares meta_template_prefix "
                f"{location.meta_template_prefix!r}, but that branch owns {profile.meta_template_prefix!r}."
            )
        if validate_static_booking_page(location.booking_page_url) is None:
            raise SeedConfigError("A registry booking page is invalid or outside EASYWEEK_BOOKING_PAGE_ALLOWED_HOSTS.")
        result.append((location, profile))
    return tuple(sorted(result, key=lambda item: item[0].location_id))


async def build_seed_plan(*, client_factory: Callable[[], Any] | None = None) -> SeedPlan:
    """Validate local config and every UUID via live GET /locations.

    The returned immutable plan is the only input accepted by the write loop.
    An unavailable/misconfigured API, missing UUID or duplicate API UUID aborts
    before a database statement is issued.
    """
    configured = _configured_seed_locations()
    language = _resolve_language()
    phone_number_id = _resolve_phone_number_id()
    factory = client_factory or EasyWeekClient

    try:
        async with factory() as client:
            api_locations = await client.list_locations()
    except EasyWeekError as exc:
        raise SeedConfigError("GET /locations identity check failed; refusing to seed.") from exc
    except Exception as exc:
        raise SeedConfigError("GET /locations identity check was unavailable; refusing to seed.") from exc

    api_by_uuid: dict[str, str] = {}
    for item in api_locations:
        raw_uuid = item.get("uuid") if isinstance(item, dict) else None
        raw_name = item.get("name") if isinstance(item, dict) else None
        if not isinstance(raw_uuid, str) or not isinstance(raw_name, str) or not raw_name.strip():
            raise SeedConfigError("GET /locations returned an unusable location; refusing to seed.")
        try:
            canonical_uuid = str(uuid.UUID(raw_uuid))
        except (ValueError, AttributeError, TypeError):
            raise SeedConfigError("GET /locations returned an unusable location; refusing to seed.") from None
        if canonical_uuid in api_by_uuid:
            raise SeedConfigError("GET /locations returned a duplicate UUID; refusing to seed.")
        api_by_uuid[canonical_uuid] = raw_name

    verified: list[VerifiedBranch] = []
    for location, profile in configured:
        api_name = api_by_uuid.get(location.location_uuid)
        if api_name is None:
            raise SeedConfigError("A registry UUID is absent from GET /locations; refusing to seed.")

        # THE check §10 was missing. Printing the name and continuing proves
        # nothing: it confirms *a* location exists, not that it is the branch
        # whose content is about to be written. EasyWeek is the independent
        # third party here — the registry claims this UUID is `profile.slug`,
        # and only the API can confirm or refute it.
        if normalize_api_name(api_name) != normalize_api_name(profile.api_name):
            raise SeedConfigError(
                f"EasyWeek API identity mismatch for branch {profile.slug!r}: the configured UUID belongs to "
                f"{json.dumps(api_name, ensure_ascii=False)}, expected "
                f"{json.dumps(profile.api_name, ensure_ascii=False)}."
            )

        # JSON quoting keeps terminal control characters escaped while showing
        # the exact human-readable API name that was matched.
        print(
            "verified EasyWeek location: branch={} company_id={} name={}".format(
                profile.slug,
                location.company_id,
                json.dumps(api_name, ensure_ascii=False),
            )
        )
        verified.append(VerifiedBranch(location=location, profile=profile, api_name=api_name))

    return SeedPlan(branches=tuple(verified), language=language, phone_number_id=phone_number_id)


async def _upsert_template(
    session: AsyncSession,
    *,
    branch: VerifiedBranch,
    language: str,
    code: str,
    result: SeedResult,
) -> SeedResult:
    location = branch.location
    stmt = (
        select(MessageTemplate)
        .where(MessageTemplate.provider == PROVIDER_EASYWEEK)
        .where(MessageTemplate.company_id == location.company_id)
        .where(MessageTemplate.code == code)
        .where(MessageTemplate.language == language)
        .order_by(MessageTemplate.id.asc())
    )
    existing = list((await session.execute(stmt)).scalars().all())
    # Body AND template name come from one source-owned contract. Building
    # either separately would let seed and runtime silently disagree.
    contract = branch_template_contract(branch.profile, code)
    if contract is None:
        raise SeedConfigError(f"Unsupported EasyWeek template code for branch {branch.profile.slug!r}.")

    if not existing:
        session.add(
            MessageTemplate(
                provider=PROVIDER_EASYWEEK,
                company_id=location.company_id,
                code=code,
                language=language,
                body=contract.raw_body,
                meta_template_name=contract.meta_template_name,
                is_active=True,
            )
        )
        return SeedResult(
            templates_created=result.templates_created + 1,
            templates_updated=result.templates_updated,
            template_duplicates=result.template_duplicates,
            senders_created=result.senders_created,
            senders_updated=result.senders_updated,
        )

    row = existing[0]
    row.body = contract.raw_body
    row.meta_template_name = contract.meta_template_name
    row.is_active = True
    return SeedResult(
        templates_created=result.templates_created,
        templates_updated=result.templates_updated + 1,
        template_duplicates=result.template_duplicates + len(existing) - 1,
        senders_created=result.senders_created,
        senders_updated=result.senders_updated,
    )


async def _upsert_sender(
    session: AsyncSession,
    *,
    branch: VerifiedBranch,
    phone_number_id: str,
    result: SeedResult,
) -> SeedResult:
    company_id = branch.location.company_id
    existed = (
        await session.execute(
            select(WhatsAppSender.id)
            .where(WhatsAppSender.provider == PROVIDER_EASYWEEK)
            .where(WhatsAppSender.company_id == company_id)
            .where(WhatsAppSender.sender_code == "default")
        )
    ).scalar_one_or_none() is not None

    stmt = pg_insert(WhatsAppSender).values(
        provider=PROVIDER_EASYWEEK,
        company_id=company_id,
        sender_code="default",
        phone_number_id=phone_number_id,
        display_phone=branch.content.contact_phone,
        is_active=True,
    )
    stmt = stmt.on_conflict_do_update(
        constraint="uq_whatsapp_senders_provider_company_code",
        set_={
            "phone_number_id": phone_number_id,
            "display_phone": branch.content.contact_phone,
            "is_active": True,
        },
    )
    await session.execute(stmt)
    return SeedResult(
        templates_created=result.templates_created,
        templates_updated=result.templates_updated,
        template_duplicates=result.template_duplicates,
        senders_created=result.senders_created + (not existed),
        senders_updated=result.senders_updated + existed,
    )


async def seed(
    session: AsyncSession,
    *,
    plan: SeedPlan | None = None,
    client_factory: Callable[[], Any] | None = None,
) -> SeedResult:
    """Converge all registry locations without deleting any existing row."""
    verified_plan = plan or await build_seed_plan(client_factory=client_factory)
    result = SeedResult()
    for branch in verified_plan.branches:
        for code in TEMPLATE_CODES:
            result = await _upsert_template(
                session,
                branch=branch,
                language=verified_plan.language,
                code=code,
                result=result,
            )
        result = await _upsert_sender(
            session,
            branch=branch,
            phone_number_id=verified_plan.phone_number_id,
            result=result,
        )
    return result


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Seed EasyWeek templates and senders for the configured registry.")
    return parser.parse_args(argv)


async def main(argv: list[str] | None = None) -> None:
    _parse_args(argv)
    # Live identity confirmation finishes before the database transaction opens.
    plan = await build_seed_plan()
    async with SessionLocal() as session:
        async with session.begin():
            result = await seed(session, plan=plan)

    print(
        "seeded easyweek templates: created={} updated={} senders_created={} senders_updated={}".format(
            result.templates_created,
            result.templates_updated,
            result.senders_created,
            result.senders_updated,
        )
    )
    if result.template_duplicates:
        print(
            f"WARNING: {result.template_duplicates} extra template row(s) share a seeded key. "
            "Nothing was deleted; review them manually."
        )


if __name__ == "__main__":
    asyncio.run(main())
