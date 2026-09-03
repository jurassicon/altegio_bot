"""PostgreSQL contract for the targeted EasyWeek template reconciliation CLI.

The command exists because a production audit compared the approved Meta content
with this codebase's contract and found three branches out of step in three
different ways at once. Its safety story is narrow and every part of it is pinned
here:

* it writes NOTHING without ``--apply``;
* it touches only the branches and codes named on the command line;
* one unusable Meta template blocks the WHOLE selected apply, so an operator
  never ends up with half a queue aligned;
* the sender is a separate, explicit decision — never a side effect of fixing
  text — and a sender pointing at another line is refused rather than rewritten.

No test performs a real send or reaches a real API: the EasyWeek locations and
the Meta template list are both supplied as fixtures.
"""

from __future__ import annotations

import json
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.easyweek_branches import BRANCH_PROFILES, branch_template_contract
from altegio_bot.models.models import (
    PROVIDER_ALTEGIO,
    PROVIDER_EASYWEEK,
    MessageTemplate,
    WhatsAppSender,
)
from altegio_bot.scripts import reconcile_easyweek_templates as cli
from altegio_bot.settings import settings
from altegio_bot.tests.easyweek_approved_meta_fixtures import (
    APPROVED_META_BODIES,
    approved_set,
    meta_template,
)

pytestmark = pytest.mark.asyncio

DURLACH_ID = 999501
RASTATT_ID = 999502
KARLSRUHE_ID = 999503
DURLACH_UUID = "dddddddd-eeee-4fff-8000-000000000001"
RASTATT_UUID = "dddddddd-eeee-4fff-8000-000000000002"
KARLSRUHE_UUID = "dddddddd-eeee-4fff-8000-000000000003"
BOOKING_HOST = "book.kitilash.invalid"
PHONE_NUMBER_ID = "shared-bot-phone-number-id"
OTHER_PHONE_NUMBER_ID = "a-different-whatsapp-line"

API_LOCATIONS = [
    {"uuid": DURLACH_UUID, "name": "KitiLash Durlach"},
    {"uuid": RASTATT_UUID, "name": "KitiLash Rastatt"},
    {"uuid": KARLSRUHE_UUID, "name": "KitiLash Karlsruhe"},
]

ALL_APPROVED = [*approved_set("du"), *approved_set("ra"), *approved_set("ka")]
# Production today: Rastatt has review only, so its retention pair is absent.
PRODUCTION_SHAPED = [*approved_set("du"), *approved_set("ra", ("review_3d",)), *approved_set("ka")]


@pytest.fixture(autouse=True)
def _registry(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "easyweek_default_language", "de", raising=False)
    monkeypatch.setattr(settings, "meta_wa_phone_number_id", PHONE_NUMBER_ID, raising=False)
    monkeypatch.setattr(settings, "easyweek_booking_page_allowed_hosts", BOOKING_HOST, raising=False)
    monkeypatch.setattr(
        settings,
        "easyweek_location_map",
        json.dumps(
            {
                "durlach": _entry(DURLACH_ID, DURLACH_UUID, "du"),
                "rastatt": _entry(RASTATT_ID, RASTATT_UUID, "ra"),
                "karlsruhe": _entry(KARLSRUHE_ID, KARLSRUHE_UUID, "ka"),
            }
        ),
        raising=False,
    )


def _entry(location_id: int, location_uuid: str, prefix: str) -> dict[str, Any]:
    return {
        "location_id": location_id,
        "location_uuid": location_uuid,
        "meta_template_prefix": prefix,
        "booking_page_url": f"https://{BOOKING_HOST}/{prefix}",
    }


class _FakeLocations:
    """The EasyWeek client surface `build_seed_plan` uses. No network."""

    def __init__(self, locations: list[dict[str, Any]]) -> None:
        self._locations = locations

    async def __aenter__(self) -> "_FakeLocations":
        return self

    async def __aexit__(self, *_exc: object) -> None:
        return None

    async def list_locations(self) -> list[dict[str, Any]]:
        return self._locations


class _FakeMeta:
    """The Meta template surface, plus a record of what was asked of it."""

    def __init__(self, templates: list[dict[str, Any]], *, fail: Exception | None = None) -> None:
        self._templates = templates
        self._fail = fail
        self.created: list[dict[str, Any]] = []

    async def __aenter__(self) -> "_FakeMeta":
        return self

    async def __aexit__(self, *_exc: object) -> None:
        return None

    async def list_templates(self) -> list[dict[str, Any]]:
        if self._fail is not None:
            raise self._fail
        return self._templates

    async def create_template(self, payload: dict[str, Any]) -> dict[str, Any]:  # pragma: no cover
        raise AssertionError("the reconcile CLI must never create a Meta template")


@pytest_asyncio.fixture
async def db(session_maker: async_sessionmaker[AsyncSession]) -> AsyncSession:
    async with session_maker() as session:
        yield session


async def _run(
    db: AsyncSession,
    *,
    branches: tuple[str, ...] = ("durlach",),
    codes: tuple[str, ...] = ("review_3d", "repeat_10d", "comeback_3d"),
    apply: bool = False,
    include_sender: bool = False,
    templates: list[dict[str, Any]] | None = None,
    locations: list[dict[str, Any]] | None = None,
    meta_fail: Exception | None = None,
) -> cli.ReconcileReport:
    meta = _FakeMeta(ALL_APPROVED if templates is None else templates, fail=meta_fail)
    report = await cli.run_reconcile(
        db,
        branches=branches,
        codes=codes,
        apply=apply,
        include_sender=include_sender,
        client_factory=lambda: _FakeLocations(API_LOCATIONS if locations is None else locations),
        meta_client_factory=lambda: meta,
    )
    await db.flush()
    return report


async def _rows(db: AsyncSession, *, company_id: int | None = None) -> list[MessageTemplate]:
    stmt = select(MessageTemplate).where(MessageTemplate.provider == PROVIDER_EASYWEEK)
    if company_id is not None:
        stmt = stmt.where(MessageTemplate.company_id == company_id)
    return list((await db.execute(stmt.order_by(MessageTemplate.id))).scalars().all())


async def _all_templates(db: AsyncSession) -> list[MessageTemplate]:
    return list((await db.execute(select(MessageTemplate).order_by(MessageTemplate.id))).scalars().all())


async def _senders(db: AsyncSession) -> list[WhatsAppSender]:
    return list((await db.execute(select(WhatsAppSender).order_by(WhatsAppSender.id))).scalars().all())


def _seed_row(
    *,
    company_id: int,
    code: str,
    prefix: str,
    body: str | None = None,
    name: str | None = None,
    is_active: bool = True,
    language: str = "de",
    provider: str = PROVIDER_EASYWEEK,
) -> MessageTemplate:
    contract = branch_template_contract(
        BRANCH_PROFILES[{"du": "durlach", "ra": "rastatt", "ka": "karlsruhe"}[prefix]], code
    )
    return MessageTemplate(
        provider=provider,
        company_id=company_id,
        code=code,
        language=language,
        body=contract.raw_body if body is None else body,
        meta_template_name=contract.meta_template_name if name is None else name,
        is_active=is_active,
    )


# ===========================================================================
# Dry-run is the default
# ===========================================================================


async def test_a_dry_run_writes_nothing(db: AsyncSession) -> None:
    report = await _run(db)

    assert report.apply is False
    assert report.mutations_attempted == 0
    assert await _all_templates(db) == []
    assert {plan.action for plan in report.templates} == {cli.ACTION_CREATE}
    assert report.as_safe_dict()["mode"] == "dry-run"
    assert report.as_safe_dict()["send_authorized"] is False


async def test_selectors_are_required() -> None:
    """No implicit "all branches" and no implicit "all codes"."""
    with pytest.raises(SystemExit):
        cli._parse_args(["--code", "review_3d"])
    with pytest.raises(SystemExit):
        cli._parse_args(["--branch", "durlach"])


async def test_a_code_outside_this_commands_scope_is_refused(db: AsyncSession) -> None:
    """The six lifecycle and reminder codes are not reachable from here."""
    with pytest.raises(SystemExit):
        cli._parse_args(["--branch", "durlach", "--code", "record_created"])

    with pytest.raises(cli.ReconcileError):
        await _run(db, codes=("reminder_24h",))


async def test_an_unknown_branch_is_refused(db: AsyncSession) -> None:
    with pytest.raises(cli.ReconcileError):
        await _run(db, branches=("ettlingen",))


# ===========================================================================
# Apply
# ===========================================================================


async def test_apply_creates_exactly_the_selected_rows(db: AsyncSession) -> None:
    report = await _run(db, branches=("durlach",), apply=True)

    rows = await _all_templates(db)
    assert len(rows) == 3
    assert {row.company_id for row in rows} == {DURLACH_ID}
    assert report.mutations_attempted == 3
    for row in rows:
        contract = branch_template_contract(BRANCH_PROFILES["durlach"], row.code)
        assert row.meta_template_name == contract.meta_template_name
        assert row.body == contract.raw_body
        assert row.is_active is True
        assert row.language == "de"
        assert row.provider == PROVIDER_EASYWEEK


async def test_apply_touches_no_other_branch_or_code(db: AsyncSession) -> None:
    """Everything not selected must be byte-identical afterwards."""
    db.add_all(
        [
            _seed_row(company_id=RASTATT_ID, code="review_3d", prefix="ra", body="RASTATT UNTOUCHED"),
            _seed_row(company_id=DURLACH_ID, code="reminder_24h", prefix="du", body="REMINDER UNTOUCHED"),
            MessageTemplate(
                provider=PROVIDER_ALTEGIO,
                company_id=DURLACH_ID,
                code="review_3d",
                language="de",
                body="ALTEGIO UNTOUCHED",
                meta_template_name="kitilash_ka_review_3d_v1",
                is_active=True,
            ),
        ]
    )
    await db.flush()

    await _run(db, branches=("durlach",), codes=("review_3d",), apply=True)

    untouched = {
        (row.provider, row.company_id, row.code): row.body
        for row in await _all_templates(db)
        if row.body.endswith("UNTOUCHED")
    }
    assert untouched == {
        (PROVIDER_EASYWEEK, RASTATT_ID, "review_3d"): "RASTATT UNTOUCHED",
        (PROVIDER_EASYWEEK, DURLACH_ID, "reminder_24h"): "REMINDER UNTOUCHED",
        (PROVIDER_ALTEGIO, DURLACH_ID, "review_3d"): "ALTEGIO UNTOUCHED",
    }


async def test_apply_is_idempotent(db: AsyncSession) -> None:
    first = await _run(db, apply=True)
    rows_after_first = {(row.id, row.body, row.meta_template_name) for row in await _all_templates(db)}

    second = await _run(db, apply=True)

    assert first.mutations_attempted == 3
    assert second.mutations_attempted == 0
    assert {plan.action for plan in second.templates} == {cli.ACTION_UNCHANGED}
    assert {(row.id, row.body, row.meta_template_name) for row in await _all_templates(db)} == rows_after_first


async def test_the_four_actions_are_distinguished(db: AsyncSession) -> None:
    """create, update, activate and unchanged are different operator facts."""
    db.add_all(
        [
            # unchanged
            _seed_row(company_id=DURLACH_ID, code="review_3d", prefix="du"),
            # update: right name, stale body
            _seed_row(company_id=DURLACH_ID, code="repeat_10d", prefix="du", body="OLD BODY"),
            # activate: correct content, switched off
            _seed_row(company_id=DURLACH_ID, code="comeback_3d", prefix="du", is_active=False),
        ]
    )
    await db.flush()

    report = await _run(db, branches=("durlach",))

    by_code = {plan.code: plan.action for plan in report.templates}
    assert by_code == {
        "review_3d": cli.ACTION_UNCHANGED,
        "repeat_10d": cli.ACTION_UPDATE,
        "comeback_3d": cli.ACTION_ACTIVATE,
    }


async def test_an_inactive_row_is_updated_not_duplicated(db: AsyncSession) -> None:
    """ "Zero ACTIVE rows" is not "no row" — a second row would be the real bug."""
    db.add(_seed_row(company_id=DURLACH_ID, code="review_3d", prefix="du", body="OLD", is_active=False))
    await db.flush()

    report = await _run(db, branches=("durlach",), codes=("review_3d",), apply=True)

    rows = await _rows(db, company_id=DURLACH_ID)
    assert len(rows) == 1, "the deactivated row is reused, never duplicated"
    assert rows[0].is_active is True
    assert rows[0].body == branch_template_contract(BRANCH_PROFILES["durlach"], "review_3d").raw_body
    assert [plan.action for plan in report.templates] == [cli.ACTION_UPDATE_AND_ACTIVATE]


# ===========================================================================
# Blockers: one bad pair blocks the whole selected apply
# ===========================================================================


async def test_a_missing_meta_template_blocks_the_whole_apply(db: AsyncSession) -> None:
    """Rastatt's real state today: review approved, retention absent."""
    report = await _run(
        db,
        branches=("rastatt",),
        apply=True,
        templates=PRODUCTION_SHAPED,
    )

    assert cli.BLOCK_META_MISSING in report.blockers
    assert report.mutations_attempted == 0
    assert await _all_templates(db) == [], "not even the provable review row is written"


async def test_a_missing_template_for_one_branch_does_not_block_another(db: AsyncSession) -> None:
    """The block is scoped to the SELECTED set, not to the whole registry."""
    report = await _run(db, branches=("durlach",), apply=True, templates=PRODUCTION_SHAPED)

    assert report.blockers == []
    assert len(await _all_templates(db)) == 3


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        ("PENDING", cli.BLOCK_META_NOT_APPROVED),
        ("REJECTED", cli.BLOCK_META_NOT_APPROVED),
        ("PAUSED", cli.BLOCK_META_NOT_APPROVED),
    ],
)
async def test_a_template_that_is_not_approved_blocks(db: AsyncSession, status: str, expected: str) -> None:
    templates = [meta_template(name="kitilash_du_review_3d_v1", code="review_3d", status=status)]
    report = await _run(db, branches=("durlach",), codes=("review_3d",), apply=True, templates=templates)

    assert report.blockers == [expected]
    assert await _all_templates(db) == []


async def test_a_non_marketing_template_blocks(db: AsyncSession) -> None:
    templates = [meta_template(name="kitilash_du_review_3d_v1", code="review_3d", category="UTILITY")]
    report = await _run(db, branches=("durlach",), codes=("review_3d",), apply=True, templates=templates)

    assert report.blockers == [cli.BLOCK_META_NOT_MARKETING]
    assert await _all_templates(db) == []


async def test_a_duplicated_meta_template_blocks(db: AsyncSession) -> None:
    """Two rows for one name means the send would resolve one by chance."""
    templates = [
        meta_template(name="kitilash_du_review_3d_v1", code="review_3d"),
        meta_template(name="kitilash_du_review_3d_v1", code="review_3d"),
    ]
    report = await _run(db, branches=("durlach",), codes=("review_3d",), apply=True, templates=templates)

    assert report.blockers == [cli.BLOCK_META_DUPLICATE]
    assert await _all_templates(db) == []


async def test_a_changed_meta_body_blocks(db: AsyncSession) -> None:
    """Meta drifting from the approved contract is a STOP, not a new contract."""
    drifted = APPROVED_META_BODIES["review_3d"].replace("Danke.", "Danke!")
    templates = [meta_template(name="kitilash_du_review_3d_v1", code="review_3d", body=drifted)]
    report = await _run(db, branches=("durlach",), codes=("review_3d",), apply=True, templates=templates)

    assert report.blockers == [cli.BLOCK_META_BODY_MISMATCH]
    assert await _all_templates(db) == []


async def test_a_collapsed_double_space_in_meta_blocks(db: AsyncSession) -> None:
    collapsed = APPROVED_META_BODIES["comeback_3d"].replace("bei uns,  KitiLash", "bei uns, KitiLash")
    templates = [meta_template(name="kitilash_du_comeback_3d_v1", code="comeback_3d", body=collapsed)]
    report = await _run(db, branches=("durlach",), codes=("comeback_3d",), apply=True, templates=templates)

    assert report.blockers == [cli.BLOCK_META_BODY_MISMATCH]


@pytest.mark.parametrize(
    "components",
    [
        pytest.param(
            [
                {"type": "BODY", "text": APPROVED_META_BODIES["review_3d"]},
                {"type": "FOOTER", "text": "Nobody reviewed this line here"},
            ],
            id="extra_footer",
        ),
        pytest.param(
            [
                {"type": "HEADER", "format": "IMAGE"},
                {"type": "BODY", "text": APPROVED_META_BODIES["review_3d"]},
            ],
            id="image_header",
        ),
        pytest.param([{"type": "BODY", "text": APPROVED_META_BODIES["review_3d"]}, {"type": "BUTTONS"}], id="buttons"),
    ],
)
async def test_unsupported_component_sets_block(db: AsyncSession, components: list[dict[str, Any]]) -> None:
    templates = [meta_template(name="kitilash_du_review_3d_v1", code="review_3d", components=components)]
    report = await _run(db, branches=("durlach",), codes=("review_3d",), apply=True, templates=templates)

    assert report.blockers == [cli.BLOCK_META_UNSUPPORTED_COMPONENTS]
    assert await _all_templates(db) == []


async def test_a_named_parameter_format_blocks(db: AsyncSession) -> None:
    templates = [
        meta_template(name="kitilash_du_review_3d_v1", code="review_3d", parameter_format="NAMED"),
    ]
    report = await _run(db, branches=("durlach",), codes=("review_3d",), apply=True, templates=templates)

    assert report.blockers == [cli.BLOCK_META_UNSUPPORTED_COMPONENTS]


async def test_another_branchs_approved_template_is_not_accepted(db: AsyncSession) -> None:
    """The Karlsruhe pair must not stand in for the missing Rastatt one."""
    templates = [*approved_set("ka"), *approved_set("ra", ("review_3d",))]
    report = await _run(db, branches=("rastatt",), apply=True, templates=templates)

    assert cli.BLOCK_META_MISSING in report.blockers
    assert await _all_templates(db) == []


async def test_duplicated_db_rows_block_rather_than_being_deduplicated(db: AsyncSession) -> None:
    db.add_all(
        [
            _seed_row(company_id=DURLACH_ID, code="review_3d", prefix="du", body="ONE"),
            _seed_row(company_id=DURLACH_ID, code="review_3d", prefix="du", body="TWO", is_active=False),
        ]
    )
    await db.flush()

    report = await _run(db, branches=("durlach",), codes=("review_3d",), apply=True)

    assert report.blockers == [cli.BLOCK_DB_DUPLICATE]
    bodies = {row.body for row in await _rows(db, company_id=DURLACH_ID)}
    assert bodies == {"ONE", "TWO"}, "neither row is edited and neither is deleted"


async def test_a_meta_read_failure_blocks_before_any_write(db: AsyncSession) -> None:
    with pytest.raises(RuntimeError):
        await _run(db, apply=True, meta_fail=RuntimeError("meta unavailable"))

    assert await _all_templates(db) == []


async def test_a_failed_live_identity_check_blocks_before_any_write(db: AsyncSession) -> None:
    """The branch UUID must still be confirmed by GET /locations."""
    wrong = [{"uuid": DURLACH_UUID, "name": "Somebody Else"}, *API_LOCATIONS[1:]]
    with pytest.raises(Exception):
        await _run(db, apply=True, locations=wrong)

    assert await _all_templates(db) == []


async def test_a_blocked_apply_writes_nothing_even_when_others_are_fine(db: AsyncSession) -> None:
    """Atomicity across the selected set, not per row."""
    templates = [
        *approved_set("du", ("review_3d", "repeat_10d")),
        meta_template(name="kitilash_du_comeback_3d_v1", code="comeback_3d", status="PENDING"),
    ]
    report = await _run(db, branches=("durlach",), apply=True, templates=templates)

    assert report.blockers == [cli.BLOCK_META_NOT_APPROVED]
    assert report.mutations_attempted == 0
    assert await _all_templates(db) == []


# ===========================================================================
# The sender is a separate decision
# ===========================================================================


async def test_the_sender_is_untouched_without_the_explicit_option(db: AsyncSession) -> None:
    report = await _run(db, branches=("karlsruhe",), apply=True)

    assert report.senders == []
    assert await _senders(db) == []


async def test_the_sender_is_created_only_with_the_option(db: AsyncSession) -> None:
    report = await _run(db, branches=("karlsruhe",), apply=True, include_sender=True)

    senders = await _senders(db)
    assert [plan.action for plan in report.senders] == [cli.SENDER_ACTION_CREATE]
    assert len(senders) == 1
    assert senders[0].provider == PROVIDER_EASYWEEK
    assert senders[0].company_id == KARLSRUHE_ID
    assert senders[0].sender_code == "default"
    assert senders[0].phone_number_id == PHONE_NUMBER_ID
    assert senders[0].is_active is True


async def test_an_inactive_sender_is_activated_as_its_own_action(db: AsyncSession) -> None:
    db.add(
        WhatsAppSender(
            provider=PROVIDER_EASYWEEK,
            company_id=KARLSRUHE_ID,
            sender_code="default",
            phone_number_id=PHONE_NUMBER_ID,
            is_active=False,
        )
    )
    await db.flush()

    report = await _run(db, branches=("karlsruhe",), apply=True, include_sender=True)

    assert [plan.action for plan in report.senders] == [cli.SENDER_ACTION_ACTIVATE]
    senders = await _senders(db)
    assert len(senders) == 1 and senders[0].is_active is True


async def test_a_sender_on_another_line_is_refused_not_rewritten(db: AsyncSession) -> None:
    """Moving a branch's outbound number is never a side effect of a text fix."""
    db.add(
        WhatsAppSender(
            provider=PROVIDER_EASYWEEK,
            company_id=KARLSRUHE_ID,
            sender_code="default",
            phone_number_id=OTHER_PHONE_NUMBER_ID,
            is_active=True,
        )
    )
    await db.flush()

    report = await _run(db, branches=("karlsruhe",), apply=True, include_sender=True)

    assert report.blockers == [cli.BLOCK_SENDER_OTHER_LINE]
    senders = await _senders(db)
    assert senders[0].phone_number_id == OTHER_PHONE_NUMBER_ID
    assert await _all_templates(db) == [], "and the blocked sender blocks the templates too"


async def test_altegio_senders_and_other_codes_are_untouched(db: AsyncSession) -> None:
    db.add_all(
        [
            WhatsAppSender(
                provider=PROVIDER_ALTEGIO,
                company_id=KARLSRUHE_ID,
                sender_code="default",
                phone_number_id=OTHER_PHONE_NUMBER_ID,
                is_active=True,
            ),
            WhatsAppSender(
                provider=PROVIDER_EASYWEEK,
                company_id=KARLSRUHE_ID,
                sender_code="vip",
                phone_number_id=OTHER_PHONE_NUMBER_ID,
                is_active=False,
            ),
        ]
    )
    await db.flush()

    await _run(db, branches=("karlsruhe",), apply=True, include_sender=True)

    senders = {(s.provider, s.sender_code): (s.phone_number_id, s.is_active) for s in await _senders(db)}
    assert senders[(PROVIDER_ALTEGIO, "default")] == (OTHER_PHONE_NUMBER_ID, True)
    assert senders[(PROVIDER_EASYWEEK, "vip")] == (OTHER_PHONE_NUMBER_ID, False)
    assert senders[(PROVIDER_EASYWEEK, "default")] == (PHONE_NUMBER_ID, True)


# ===========================================================================
# The report
# ===========================================================================


async def test_the_report_carries_no_credentials_or_customer_data(db: AsyncSession) -> None:
    templates = [
        meta_template(
            name="kitilash_du_review_3d_v1",
            code="review_3d",
            components=[
                {
                    "type": "BODY",
                    "text": APPROVED_META_BODIES["review_3d"],
                    "example": {"body_text": [["Anna Müller", "https://g.page/r/secret/review"]]},
                }
            ],
        )
    ]
    report = await _run(db, branches=("durlach",), codes=("review_3d",), templates=templates)

    rendered = json.dumps(report.as_safe_dict(), ensure_ascii=False)
    for secret in ("Anna Müller", "g.page/r/secret", PHONE_NUMBER_ID, DURLACH_UUID):
        assert secret not in rendered, secret


async def test_the_report_states_that_it_authorises_nothing(db: AsyncSession) -> None:
    report = await _run(db, apply=True)
    payload = report.as_safe_dict()

    assert payload["send_authorized"] is False
    assert payload["mode"] == "apply"
    assert "eligibility" in payload["note"]
    assert payload["scope"]["branches"] == ["durlach"]


async def test_the_report_names_the_selected_scope_only(db: AsyncSession) -> None:
    report = await _run(db, branches=("durlach", "karlsruhe"), codes=("repeat_10d",))
    payload = report.as_safe_dict()

    assert payload["scope"] == {"branches": ["durlach", "karlsruhe"], "codes": ["repeat_10d"]}
    assert {plan.code for plan in report.templates} == {"repeat_10d"}
    assert {plan.meta_template_name for plan in report.templates} == {
        "kitilash_du_repeat_10d_v1",
        "kitilash_ka_repeat_10d_v1",
    }


# ===========================================================================
# The CLI error boundary
#
# `MetaTemplateClient` builds its ScriptError from Meta's own `error.message`,
# and the paging guard interpolates a server-supplied cursor. Printing `str(exc)`
# put both into a report that gets pasted into tickets. These tests drive main()
# — the actual print path — rather than the report object.
# ===========================================================================


class _RaisingMeta:
    def __init__(self, exc: Exception) -> None:
        self._exc = exc

    async def __aenter__(self) -> "_RaisingMeta":
        return self

    async def __aexit__(self, *_exc: object) -> None:
        return None

    async def list_templates(self) -> list[dict[str, Any]]:
        raise self._exc


async def _run_main(
    monkeypatch: pytest.MonkeyPatch,
    session_maker: async_sessionmaker[AsyncSession],
    *,
    meta_exc: Exception,
    argv: list[str],
) -> int:
    monkeypatch.setattr(cli, "SessionLocal", session_maker, raising=False)
    monkeypatch.setattr(cli, "_default_meta_client", lambda: _RaisingMeta(meta_exc))

    real = cli.run_reconcile

    async def _with_fake_locations(session, **kwargs):
        kwargs["client_factory"] = lambda: _FakeLocations(API_LOCATIONS)
        return await real(session, **kwargs)

    monkeypatch.setattr(cli, "run_reconcile", _with_fake_locations)
    return await cli.main(argv)


def _meta_http_error() -> Exception:
    """The exception `MetaTemplateClient` raises for an HTTP 400 from Meta."""
    return cli.ScriptError(
        "cannot read templates: HTTP 400 SECRET_PROVIDER_MARKER_9f31 "
        "(https://graph.facebook.com/v20.0/12345/message_templates?after=CURSOR_MARKER_7b2a)"
    )


def _meta_paging_error() -> Exception:
    return cli.ScriptError("Meta paging cursor repeated ('CURSOR_MARKER_7b2a'); refusing to loop")


@pytest.mark.parametrize(
    ("factory", "markers"),
    [
        pytest.param(_meta_http_error, ("SECRET_PROVIDER_MARKER_9f31", "CURSOR_MARKER_7b2a"), id="http_400"),
        pytest.param(_meta_paging_error, ("CURSOR_MARKER_7b2a",), id="paging_cursor"),
    ],
)
async def test_external_error_text_never_reaches_the_output(
    session_maker: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
    factory,
    markers: tuple[str, ...],
) -> None:
    with caplog.at_level("DEBUG"):
        code = await _run_main(
            monkeypatch,
            session_maker,
            meta_exc=factory(),
            argv=["--branch", "durlach", "--code", "review_3d"],
        )

    captured = capsys.readouterr()
    logged = "\n".join(record.getMessage() for record in caplog.records)
    haystack = captured.out + captured.err + logged

    assert code == 1, "a failure must exit non-zero"
    for marker in markers:
        assert marker not in haystack, f"external text leaked: {marker}"
    assert cli.ERROR_META_READ_FAILED in captured.out
    assert "'send_authorized': False" in captured.out


async def test_a_failed_meta_read_leaves_the_database_untouched(
    session_maker: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
    capsys: pytest.CaptureFixture[str],
) -> None:
    code = await _run_main(
        monkeypatch,
        session_maker,
        meta_exc=_meta_http_error(),
        argv=[
            "--branch",
            "durlach",
            "--code",
            "review_3d",
            "--apply",
            "--snapshot",
            str(tmp_path / "snap.json"),
        ],
    )

    assert code == 1
    assert "SECRET_PROVIDER_MARKER_9f31" not in capsys.readouterr().out
    async with session_maker() as session:
        rows = list((await session.execute(select(MessageTemplate))).scalars().all())
    assert rows == [], "an apply that failed before its writes leaves nothing behind"


async def test_our_own_configuration_errors_are_still_printed(
    session_maker: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Our messages carry no external value, so they stay the operator's diagnosis."""
    monkeypatch.setattr(cli, "SessionLocal", session_maker, raising=False)

    async def _boom(*_args: Any, **_kwargs: Any) -> None:
        raise cli.ReconcileError("selected branch not in the verified registry: ['ettlingen']")

    monkeypatch.setattr(cli, "run_reconcile", _boom)

    code = await cli.main(["--branch", "durlach", "--code", "review_3d"])
    out = capsys.readouterr().out

    assert code == 1
    assert cli.ERROR_CONFIGURATION in out
    assert "ettlingen" in out
