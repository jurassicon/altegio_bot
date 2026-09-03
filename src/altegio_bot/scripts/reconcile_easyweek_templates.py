"""Align selected EasyWeek ``message_templates`` rows with the approved Meta content.

A read-only production audit (2026-09-02) found the three EasyWeek marketing
codes — ``review_3d``, ``repeat_10d``, ``comeback_3d`` — out of step in three
different ways at once: Durlach had one active row of three, Rastatt had one,
Karlsruhe had none and no active sender, and every one of the seven approved
Meta templates disagreed with the body this codebase declared. The runtime
body-equality guard therefore refused all of them, and the two rows that did
exist were wrong in exactly the same way.

This command closes that gap for a NAMED set of branches and codes, and does
nothing else.

**Why not `seed_easyweek_templates`.** That command converges every branch and
every one of the nine codes, and it is the right tool for standing a branch up.
Here the operator needs the opposite property: touch three codes, on the
branches they name, after proving each one against Meta — and leave every other
row, sender and branch untouched. Widening the seed to take selectors would have
given one command two very different safety stories.

**What it proves before it writes anything.** Registry validity and branch
profile agreement; live branch identity through the existing ``GET /locations``
path; then, for every selected (branch, code): exactly one Meta template with
the expected name and language, APPROVED, MARKETING, positional, BODY-only, and
a body that equals the source-owned contract after the one unambiguous
``{{n}}`` conversion. A missing, PENDING, REJECTED, duplicated or unreadable
template blocks the WHOLE selected apply — not just its own row. Two of the
seven templates are expected to be missing today (Rastatt has no approved
retention pair yet); that is an external prerequisite for a human to satisfy,
never a reason to relax a check or to borrow the Karlsruhe template.

**What it refuses to do.** It creates no Meta template, sends no message,
touches no job, outbox row, event, feature flag or environment. It never
rewrites a sender that points at a different line. And a green template audit is
not permission to send: eligibility, consent, the queue and actual delivery are
proven separately by the existing rollout.

Dry-run is the default. ``--apply`` is the only way to reach a write, and every
selected write happens in one transaction.
"""

from __future__ import annotations

import argparse
import asyncio
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any, Final

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.db import SessionLocal
from altegio_bot.easyweek_branches import (
    BranchProfile,
    branch_template_contract,
    meta_positional_body,
)
from altegio_bot.easyweek_policy import COMEBACK_3D, REPEAT_10D, REVIEW_3D
from altegio_bot.models.models import PROVIDER_EASYWEEK, MessageTemplate, WhatsAppSender
from altegio_bot.scripts.clone_meta_templates_for_location import MetaTemplateClient, ScriptError
from altegio_bot.scripts.seed_easyweek_templates import (
    SeedConfigError,
    VerifiedBranch,
    build_seed_plan,
)
from altegio_bot.settings import settings

# The ONLY codes this command may touch. Deliberately a closed set rather than a
# parameter with a default: the six lifecycle and reminder codes carry branch
# footers and a different approval history, and a selector that could reach them
# would make this a general template-management tool.
RECONCILABLE_CODES: Final[tuple[str, ...]] = (REVIEW_3D, REPEAT_10D, COMEBACK_3D)

# EasyWeek never runs service-based sender routing; the outbox resolves this code
# and only this one.
EASYWEEK_SENDER_CODE: Final = "default"

ACTION_UNCHANGED: Final = "unchanged"
ACTION_CREATE: Final = "create"
ACTION_UPDATE: Final = "update"
ACTION_ACTIVATE: Final = "activate"
# `update` and `activate` are distinct on purpose: reactivating a row someone
# deliberately switched off is a different operator decision from correcting the
# text of a live one, and a plan that folded them together would hide it.
ACTION_UPDATE_AND_ACTIVATE: Final = "update_and_activate"

SENDER_ACTION_UNCHANGED: Final = "unchanged"
SENDER_ACTION_CREATE: Final = "create"
SENDER_ACTION_ACTIVATE: Final = "activate"

BLOCK_META_MISSING: Final = "meta_template_missing"
BLOCK_META_DUPLICATE: Final = "meta_template_duplicated"
BLOCK_META_NOT_APPROVED: Final = "meta_template_not_approved"
BLOCK_META_NOT_MARKETING: Final = "meta_template_not_marketing"
BLOCK_META_UNSUPPORTED_COMPONENTS: Final = "meta_template_components_unsupported"
BLOCK_META_BODY_MISMATCH: Final = "meta_body_does_not_match_contract"
BLOCK_CONTRACT_UNCONVERTIBLE: Final = "contract_body_not_convertible"
BLOCK_DB_DUPLICATE: Final = "db_rows_duplicated"
BLOCK_SENDER_OTHER_LINE: Final = "sender_points_at_another_line"
BLOCK_SENDER_LINE_UNCONFIGURED: Final = "sender_line_not_configured"


class ReconcileError(RuntimeError):
    """Configuration or evidence is not safe enough to write. Never carries API text."""


@dataclass(frozen=True)
class TemplatePlan:
    """One selected (branch, code): what was proven, and what would change."""

    branch: str
    company_id: int
    code: str
    meta_template_name: str
    action: str
    blocked_by: str | None = None

    @property
    def writes(self) -> bool:
        return self.blocked_by is None and self.action != ACTION_UNCHANGED


@dataclass(frozen=True)
class SenderPlan:
    branch: str
    company_id: int
    action: str
    blocked_by: str | None = None

    @property
    def writes(self) -> bool:
        return self.blocked_by is None and self.action != SENDER_ACTION_UNCHANGED


@dataclass
class ReconcileReport:
    """Counts, safe identifiers and stable reason codes — nothing else.

    No credentials, no raw Meta error text, no customer data and no Meta
    ``example`` values: an approved template's examples are free text a person
    typed, and they have carried real names before.
    """

    apply: bool = False
    branches: list[str] = field(default_factory=list)
    codes: list[str] = field(default_factory=list)
    templates: list[TemplatePlan] = field(default_factory=list)
    senders: list[SenderPlan] = field(default_factory=list)
    mutations_attempted: int = 0

    @property
    def blockers(self) -> list[str]:
        seen: list[str] = []
        for item in (*self.templates, *self.senders):
            if item.blocked_by is not None and item.blocked_by not in seen:
                seen.append(item.blocked_by)
        return seen

    @property
    def blocked(self) -> bool:
        return bool(self.blockers)

    def as_safe_dict(self) -> dict[str, Any]:
        actions: dict[str, int] = {}
        for plan in self.templates:
            key = plan.blocked_by or plan.action
            actions[key] = actions.get(key, 0) + 1
        sender_actions: dict[str, int] = {}
        for sender in self.senders:
            key = sender.blocked_by or sender.action
            sender_actions[key] = sender_actions.get(key, 0) + 1
        return {
            "mode": "apply" if self.apply else "dry-run",
            "send_authorized": False,
            "mutations_attempted": self.mutations_attempted,
            "scope": {"branches": sorted(self.branches), "codes": sorted(self.codes)},
            "template_actions": dict(sorted(actions.items())),
            "sender_actions": dict(sorted(sender_actions.items())),
            "blockers": self.blockers,
            "templates": [
                {
                    "branch": plan.branch,
                    "company_id": plan.company_id,
                    "code": plan.code,
                    "meta_template_name": plan.meta_template_name,
                    "action": plan.blocked_by or plan.action,
                }
                for plan in self.templates
            ],
            "senders": [
                {
                    "branch": sender.branch,
                    "company_id": sender.company_id,
                    "action": sender.blocked_by or sender.action,
                }
                for sender in self.senders
            ],
            # Stated rather than implied: a green template audit is not a send
            # authorisation, and this report is quoted in tickets.
            "note": "template contract only; eligibility, consent, queue and delivery are proven separately",
        }


# ---------------------------------------------------------------------------
# Meta evidence
# ---------------------------------------------------------------------------


def _components(template: dict[str, Any]) -> list[dict[str, Any]]:
    raw = template.get("components")
    return [item for item in raw if isinstance(item, dict)] if isinstance(raw, list) else []


def meta_template_blocker(template: dict[str, Any], *, expected_body: str) -> str | None:
    """Why this Meta template may not be used, or ``None``.

    Everything is checked positively. A template is usable only when it is
    APPROVED, MARKETING, carries exactly one BODY component and no other
    component at all, and its text equals the source-owned contract after the
    positional conversion. Anything else — a HEADER we do not render, a FOOTER
    that would print text nobody reviewed here, a BUTTONS block, a named-variable
    template — is a refusal rather than something to tolerate.
    """
    if str(template.get("status", "")).upper() != "APPROVED":
        return BLOCK_META_NOT_APPROVED
    if str(template.get("category", "")).upper() != "MARKETING":
        return BLOCK_META_NOT_MARKETING

    parameter_format = template.get("parameter_format")
    if parameter_format is not None and str(parameter_format).upper() != "POSITIONAL":
        return BLOCK_META_UNSUPPORTED_COMPONENTS

    components = _components(template)
    bodies = [item for item in components if str(item.get("type", "")).upper() == "BODY"]
    if len(components) != 1 or len(bodies) != 1:
        return BLOCK_META_UNSUPPORTED_COMPONENTS

    text = bodies[0].get("text")
    if not isinstance(text, str) or text != expected_body:
        return BLOCK_META_BODY_MISMATCH
    return None


def select_meta_templates(templates: Sequence[dict[str, Any]], *, name: str, language: str) -> list[dict[str, Any]]:
    """Every Meta row for this exact name and language.

    Exact, not prefix or case-insensitive: `kitilash_ka_repeat_10d_v1` must never
    be able to answer for `kitilash_ra_repeat_10d_v1`, which is precisely the
    fallback this whole contract exists to prevent.
    """
    return [
        item
        for item in templates
        if isinstance(item, dict) and item.get("name") == name and item.get("language") == language
    ]


# ---------------------------------------------------------------------------
# Planning
# ---------------------------------------------------------------------------


async def _existing_rows(session: AsyncSession, *, company_id: int, code: str, language: str) -> list[MessageTemplate]:
    """Every row for the key, ACTIVE OR NOT.

    Inactive rows are the reason this reads more than the send path does: "zero
    active rows" is not "no row". Creating a second one beside a deactivated one
    would leave two rows for a key that must have one, and the duplicate check
    below would then block every future run.
    """
    stmt = (
        select(MessageTemplate)
        .where(MessageTemplate.provider == PROVIDER_EASYWEEK)
        .where(MessageTemplate.company_id == company_id)
        .where(MessageTemplate.code == code)
        .where(MessageTemplate.language == language)
        .order_by(MessageTemplate.id.asc())
    )
    return list((await session.execute(stmt)).scalars().all())


def _template_action(row: MessageTemplate, *, name: str, body: str) -> str:
    needs_update = (row.meta_template_name or "").strip() != name or row.body != body
    needs_activation = not bool(row.is_active)
    if needs_update and needs_activation:
        return ACTION_UPDATE_AND_ACTIVATE
    if needs_update:
        return ACTION_UPDATE
    if needs_activation:
        return ACTION_ACTIVATE
    return ACTION_UNCHANGED


async def plan_templates(
    session: AsyncSession,
    *,
    branches: Sequence[VerifiedBranch],
    codes: Sequence[str],
    meta_templates: Sequence[dict[str, Any]],
    language: str,
) -> list[TemplatePlan]:
    plans: list[TemplatePlan] = []
    for branch in branches:
        profile: BranchProfile = branch.profile
        company_id = branch.location.company_id
        for code in codes:
            contract = branch_template_contract(profile, code)
            if contract is None:  # pragma: no cover - RECONCILABLE_CODES are all contracted
                raise ReconcileError(f"no source-owned contract for {profile.slug}/{code}")
            expected_body = meta_positional_body(profile, code)
            if expected_body is None:
                plans.append(
                    TemplatePlan(
                        branch=profile.slug,
                        company_id=company_id,
                        code=code,
                        meta_template_name=contract.meta_template_name,
                        action=ACTION_UNCHANGED,
                        blocked_by=BLOCK_CONTRACT_UNCONVERTIBLE,
                    )
                )
                continue

            matches = select_meta_templates(meta_templates, name=contract.meta_template_name, language=language)
            blocker: str | None = None
            if not matches:
                blocker = BLOCK_META_MISSING
            elif len(matches) > 1:
                blocker = BLOCK_META_DUPLICATE
            else:
                blocker = meta_template_blocker(matches[0], expected_body=expected_body)

            rows = await _existing_rows(session, company_id=company_id, code=code, language=language)
            if blocker is None and len(rows) > 1:
                # Never pick "the first" and never delete: which of two rows the
                # send path would resolve is already ambiguous, and guessing here
                # would silently bless one of them.
                blocker = BLOCK_DB_DUPLICATE

            if blocker is not None:
                action = ACTION_UNCHANGED
            elif not rows:
                action = ACTION_CREATE
            else:
                action = _template_action(rows[0], name=contract.meta_template_name, body=contract.raw_body)

            plans.append(
                TemplatePlan(
                    branch=profile.slug,
                    company_id=company_id,
                    code=code,
                    meta_template_name=contract.meta_template_name,
                    action=action,
                    blocked_by=blocker,
                )
            )
    return plans


async def plan_senders(
    session: AsyncSession,
    *,
    branches: Sequence[VerifiedBranch],
    phone_number_id: str,
) -> list[SenderPlan]:
    """What the ``default`` sender of each selected branch would need.

    Only ever reached behind an explicit option: a body reconciliation must not
    quietly activate a sender, because activating one can make ALREADY QUEUED
    ordinary messages sendable — an effect nobody asked for while fixing text.
    """
    plans: list[SenderPlan] = []
    for branch in branches:
        company_id = branch.location.company_id
        if not phone_number_id:
            plans.append(
                SenderPlan(
                    branch=branch.profile.slug,
                    company_id=company_id,
                    action=SENDER_ACTION_UNCHANGED,
                    blocked_by=BLOCK_SENDER_LINE_UNCONFIGURED,
                )
            )
            continue

        sender = (
            (
                await session.execute(
                    select(WhatsAppSender)
                    .where(WhatsAppSender.provider == PROVIDER_EASYWEEK)
                    .where(WhatsAppSender.company_id == company_id)
                    .where(WhatsAppSender.sender_code == EASYWEEK_SENDER_CODE)
                    .order_by(WhatsAppSender.id.asc())
                )
            )
            .scalars()
            .first()
        )
        if sender is None:
            plans.append(SenderPlan(branch=branch.profile.slug, company_id=company_id, action=SENDER_ACTION_CREATE))
            continue
        if (sender.phone_number_id or "").strip() != phone_number_id:
            # A sender pointing at another WhatsApp line is a decision someone
            # made. Overwriting it would silently move a branch's outbound number.
            plans.append(
                SenderPlan(
                    branch=branch.profile.slug,
                    company_id=company_id,
                    action=SENDER_ACTION_UNCHANGED,
                    blocked_by=BLOCK_SENDER_OTHER_LINE,
                )
            )
            continue
        action = SENDER_ACTION_UNCHANGED if sender.is_active else SENDER_ACTION_ACTIVATE
        plans.append(SenderPlan(branch=branch.profile.slug, company_id=company_id, action=action))
    return plans


# ---------------------------------------------------------------------------
# Applying
# ---------------------------------------------------------------------------


async def apply_templates(
    session: AsyncSession,
    *,
    branches: Sequence[VerifiedBranch],
    plans: Sequence[TemplatePlan],
    language: str,
) -> int:
    """Write the planned template changes. Caller owns the transaction."""
    by_company = {branch.location.company_id: branch.profile for branch in branches}
    written = 0
    for plan in plans:
        if not plan.writes:
            continue
        profile = by_company[plan.company_id]
        contract = branch_template_contract(profile, plan.code)
        assert contract is not None  # proven during planning
        rows = await _existing_rows(session, company_id=plan.company_id, code=plan.code, language=language)
        if not rows:
            session.add(
                MessageTemplate(
                    provider=PROVIDER_EASYWEEK,
                    company_id=plan.company_id,
                    code=plan.code,
                    language=language,
                    body=contract.raw_body,
                    meta_template_name=contract.meta_template_name,
                    is_active=True,
                )
            )
        else:
            row = rows[0]
            row.body = contract.raw_body
            row.meta_template_name = contract.meta_template_name
            row.is_active = True
        written += 1
    return written


async def apply_senders(
    session: AsyncSession,
    *,
    plans: Sequence[SenderPlan],
    phone_number_id: str,
    display_by_company: dict[int, str],
) -> int:
    written = 0
    for plan in plans:
        if not plan.writes:
            continue
        sender = (
            (
                await session.execute(
                    select(WhatsAppSender)
                    .where(WhatsAppSender.provider == PROVIDER_EASYWEEK)
                    .where(WhatsAppSender.company_id == plan.company_id)
                    .where(WhatsAppSender.sender_code == EASYWEEK_SENDER_CODE)
                    .order_by(WhatsAppSender.id.asc())
                )
            )
            .scalars()
            .first()
        )
        if sender is None:
            session.add(
                WhatsAppSender(
                    provider=PROVIDER_EASYWEEK,
                    company_id=plan.company_id,
                    sender_code=EASYWEEK_SENDER_CODE,
                    phone_number_id=phone_number_id,
                    display_phone=display_by_company.get(plan.company_id, ""),
                    is_active=True,
                )
            )
        else:
            sender.is_active = True
        written += 1
    return written


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def _select_branches(plan_branches: Sequence[VerifiedBranch], selected: Sequence[str]) -> list[VerifiedBranch]:
    known = {branch.profile.slug: branch for branch in plan_branches}
    missing = [slug for slug in selected if slug not in known]
    if missing:
        raise ReconcileError(f"selected branch not in the verified registry: {sorted(missing)}")
    return [known[slug] for slug in selected]


async def run_reconcile(
    session: AsyncSession,
    *,
    branches: Sequence[str],
    codes: Sequence[str],
    apply: bool = False,
    include_sender: bool = False,
    client_factory: Callable[[], Any] | None = None,
    meta_client_factory: Callable[[], Any] | None = None,
) -> ReconcileReport:
    """Prove, plan, and — only with ``apply`` — write, all in one transaction."""
    unknown = [code for code in codes if code not in RECONCILABLE_CODES]
    if unknown:
        raise ReconcileError(f"code outside this command's scope: {sorted(unknown)}")

    # The same live identity path the seed uses: registry validity, branch
    # profile agreement, and GET /locations confirming each configured UUID is
    # the branch we think it is. Reused rather than re-implemented so the two can
    # never disagree about what a proven branch is.
    plan = await build_seed_plan(client_factory=client_factory)
    selected = _select_branches(plan.branches, branches)

    factory = meta_client_factory or _default_meta_client
    async with factory() as meta:
        meta_templates = await meta.list_templates()

    report = ReconcileReport(
        apply=apply,
        branches=[branch.profile.slug for branch in selected],
        codes=list(codes),
    )
    report.templates = await plan_templates(
        session,
        branches=selected,
        codes=codes,
        meta_templates=meta_templates,
        language=plan.language,
    )
    if include_sender:
        report.senders = await plan_senders(session, branches=selected, phone_number_id=plan.phone_number_id)

    if not apply:
        return report
    if report.blocked:
        # One blocked pair blocks the whole selected apply. Writing "the good
        # ones" would leave the operator with a half-aligned set and no single
        # state to re-audit.
        return report

    report.mutations_attempted = await apply_templates(
        session, branches=selected, plans=report.templates, language=plan.language
    )
    if include_sender:
        report.mutations_attempted += await apply_senders(
            session,
            plans=report.senders,
            phone_number_id=plan.phone_number_id,
            display_by_company={branch.location.company_id: branch.content.contact_phone for branch in selected},
        )
    return report


def _default_meta_client() -> MetaTemplateClient:
    token = (settings.whatsapp_access_token or "").strip()
    waba_id = (settings.meta_waba_id or "").strip()
    if not token or not waba_id:
        raise ReconcileError("WHATSAPP_ACCESS_TOKEN and META_WABA_ID are required to read Meta templates")
    return MetaTemplateClient(
        token=token,
        waba_id=waba_id,
        graph_url=settings.whatsapp_graph_url,
        api_version=settings.whatsapp_api_version,
        timeout_seconds=30.0,
    )


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Align selected EasyWeek message_templates rows with the approved Meta content. "
            "Dry-run by default; --apply is the only path to a write."
        )
    )
    parser.add_argument(
        "--branch",
        action="append",
        dest="branches",
        required=True,
        metavar="SLUG",
        help="Registry branch slug to reconcile. Repeat for several; there is no 'all'.",
    )
    parser.add_argument(
        "--code",
        action="append",
        dest="codes",
        required=True,
        choices=RECONCILABLE_CODES,
        help="Template code to reconcile. Repeat for several; there is no 'all'.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write the planned changes. Without it nothing is written.",
    )
    parser.add_argument(
        "--include-sender",
        action="store_true",
        help="Also plan the branch's default sender. Never implied by a body reconciliation.",
    )
    return parser.parse_args(argv)


async def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    try:
        async with SessionLocal() as session:
            async with session.begin():
                report = await run_reconcile(
                    session,
                    branches=args.branches,
                    codes=args.codes,
                    apply=args.apply,
                    include_sender=args.include_sender,
                )
                if not args.apply or report.blocked:
                    # Dry-run and blocked applies leave nothing behind. A rollback
                    # is cheaper than trusting that no branch above wrote.
                    await session.rollback()
    except (ReconcileError, SeedConfigError, ScriptError) as exc:
        # Our own messages only. A raw Meta or driver error renders tokens and
        # bound parameters, and this output is pasted into tickets.
        print({"mode": "apply" if args.apply else "dry-run", "send_authorized": False, "error": str(exc)})
        return 1
    except Exception as exc:  # noqa: BLE001 — class name only, never the text
        print(
            {
                "mode": "apply" if args.apply else "dry-run",
                "send_authorized": False,
                "error": type(exc).__name__,
            }
        )
        return 1

    print(report.as_safe_dict())
    return 1 if report.blocked else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
