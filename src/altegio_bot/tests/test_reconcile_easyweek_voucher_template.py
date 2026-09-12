"""Regression tests for the narrow §36 Meta-to-DB reconciler.

Two production defects live here, and both needed a real database to catch.

The first was a transaction boundary: the audit SELECT autobegan a transaction
on the AsyncSession, and the code then opened a second one to write in, so
`--apply` died with `A transaction is already begun on this Session` before it
could create the missing row. A mocked session has no autobegin and reported
success, which is exactly why these tests use the real PostgreSQL session maker
for anything that touches the transaction lifecycle.

The second was fail-open: two rows carrying one code arrive from the audit as
"no row", and the write path read "no row" as "create one" — so the command
whose job is to fix an ambiguity would have added a third row to it.
"""

from __future__ import annotations

from typing import Any

import pytest
from sqlalchemy import select
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from altegio_bot.campaigns.easyweek_voucher_delivery import template_contract
from altegio_bot.models.models import PROVIDER_EASYWEEK, MessageTemplate
from altegio_bot.scripts import reconcile_easyweek_voucher_template as reconciler
from altegio_bot.settings import settings

COMPANY_ID = 322579

# A string that must never reach an operator's terminal. It stands in for the
# template body and the connection parameters a SQLAlchemy error carries.
LEAKY_PARAMETER = "SECRET-BOUND-PARAMETER-do-not-print"


def _meta_template(**changes: Any) -> dict[str, Any]:
    template: dict[str, Any] = {
        "name": template_contract.VOUCHER_META_TEMPLATE_NAME,
        "language": template_contract.VOUCHER_TEMPLATE_LANGUAGE,
        "status": "APPROVED",
        "category": template_contract.VOUCHER_TEMPLATE_CATEGORY,
        "parameter_format": template_contract.VOUCHER_TEMPLATE_PARAMETER_FORMAT,
        "components": [{"type": "BODY", "text": template_contract.positional_body()}],
    }
    template.update(changes)
    return template


class _MetaClient:
    """Read-only by construction: there is no method here that writes."""

    def __init__(self, templates: list[dict[str, Any]]) -> None:
        self.templates = templates
        self.calls: list[str] = []

    async def __aenter__(self) -> "_MetaClient":
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None

    async def list_templates(self) -> list[dict[str, Any]]:
        self.calls.append("list_templates")
        return self.templates


@pytest.fixture
def meta(monkeypatch: pytest.MonkeyPatch) -> _MetaClient:
    """An APPROVED Meta template, and the credentials to go looking for it."""
    client = _MetaClient([_meta_template()])
    monkeypatch.setattr(reconciler, "MetaTemplateClient", lambda **_kwargs: client)
    monkeypatch.setattr(settings, "whatsapp_access_token", "test-token", raising=False)
    monkeypatch.setattr(settings, "meta_waba_id", "test-waba", raising=False)
    return client


@pytest.fixture
def database(monkeypatch: pytest.MonkeyPatch, session_maker) -> Any:
    monkeypatch.setattr(reconciler, "SessionLocal", session_maker)
    return session_maker


def _query():
    return (
        select(MessageTemplate)
        .where(MessageTemplate.provider == PROVIDER_EASYWEEK)
        .where(MessageTemplate.company_id == COMPANY_ID)
        .where(MessageTemplate.code == template_contract.VOUCHER_TEMPLATE_CODE)
        .where(MessageTemplate.language == template_contract.VOUCHER_TEMPLATE_LANGUAGE)
    )


async def _rows(session_maker) -> list[MessageTemplate]:
    """What a SEPARATE session sees — the only proof a commit really happened."""
    async with session_maker() as session:
        return list((await session.execute(_query().order_by(MessageTemplate.id))).scalars().all())


async def _seed(session_maker, count: int = 1, **changes: Any) -> list[int]:
    fields: dict[str, Any] = {
        "provider": PROVIDER_EASYWEEK,
        "company_id": COMPANY_ID,
        "code": template_contract.VOUCHER_TEMPLATE_CODE,
        "language": template_contract.VOUCHER_TEMPLATE_LANGUAGE,
        "body": template_contract.VOUCHER_TEMPLATE_BODY,
        "meta_template_name": template_contract.VOUCHER_META_TEMPLATE_NAME,
        "is_active": True,
    }
    fields.update(changes)
    async with session_maker() as session:
        rows = [MessageTemplate(**fields) for _ in range(count)]
        session.add_all(rows)
        await session.commit()
        return [row.id for row in rows]


def _snapshot(rows: list[MessageTemplate]) -> list[tuple[Any, ...]]:
    return [(row.id, row.body, row.meta_template_name, row.is_active) for row in rows]


# ---------------------------------------------------------------------------
# The missing row: the case the production apply died on
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_missing_row_is_reported_but_not_created_without_apply(database, meta) -> None:
    report, code = await reconciler.reconcile(company_id=COMPANY_ID, apply=False)

    assert code == reconciler.EXIT_BLOCKED
    assert report["db_row_blocker"] == reconciler.BLOCKER_ROW_MISSING
    assert report["would_apply"] is True
    assert report["applied"] is False
    assert await _rows(database) == []


@pytest.mark.asyncio
async def test_apply_creates_the_missing_row_after_the_audit_select(database, meta) -> None:
    """The blocker: the audit SELECT autobegan, and the write never happened.

    `InvalidRequestError: A transaction is already begun on this Session` is
    what production got instead of a row.
    """
    report, code = await reconciler.reconcile(company_id=COMPANY_ID, apply=True)

    assert code == reconciler.EXIT_OK
    assert report["applied"] is True
    assert report["db_row_blocker"] == reconciler.BLOCKER_ROW_MISSING
    assert report["meta"]["template_proven"] is True

    # A separate session: an uncommitted row would be invisible here.
    rows = await _rows(database)
    assert len(rows) == 1
    assert rows[0].body == template_contract.VOUCHER_TEMPLATE_BODY
    assert rows[0].meta_template_name == template_contract.VOUCHER_META_TEMPLATE_NAME
    assert rows[0].provider == PROVIDER_EASYWEEK
    assert rows[0].company_id == COMPANY_ID
    assert rows[0].code == template_contract.VOUCHER_TEMPLATE_CODE
    assert rows[0].language == template_contract.VOUCHER_TEMPLATE_LANGUAGE
    assert rows[0].is_active is True

    # And the audit that follows an apply is the green one the runbook asks for.
    follow_up, follow_up_code = await reconciler.reconcile(company_id=COMPANY_ID, apply=False)
    assert follow_up["db_row_blocker"] is None
    assert follow_up_code == reconciler.EXIT_OK


# ---------------------------------------------------------------------------
# One row that disagrees with the contract
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_mismatched_row_is_left_alone_without_apply(database, meta) -> None:
    await _seed(database, body="something nobody approved")
    before = _snapshot(await _rows(database))

    report, code = await reconciler.reconcile(company_id=COMPANY_ID, apply=False)

    assert code == reconciler.EXIT_BLOCKED
    assert report["db_row_blocker"] is not None
    assert report["would_apply"] is True
    assert report["applied"] is False
    assert _snapshot(await _rows(database)) == before


@pytest.mark.asyncio
async def test_apply_updates_the_one_row_instead_of_adding_another(database, meta) -> None:
    [seeded] = await _seed(database, body="something nobody approved", is_active=False)

    report, code = await reconciler.reconcile(company_id=COMPANY_ID, apply=True)

    assert code == reconciler.EXIT_OK
    assert report["applied"] is True
    rows = await _rows(database)
    assert len(rows) == 1
    # The same row, brought to the contract — not a second one beside it.
    assert rows[0].id == seeded
    assert rows[0].body == template_contract.VOUCHER_TEMPLATE_BODY
    assert rows[0].meta_template_name == template_contract.VOUCHER_META_TEMPLATE_NAME
    assert rows[0].is_active is True

    follow_up, follow_up_code = await reconciler.reconcile(company_id=COMPANY_ID, apply=False)
    assert follow_up["db_row_blocker"] is None
    assert follow_up_code == reconciler.EXIT_OK


@pytest.mark.asyncio
async def test_an_already_exact_row_is_a_no_op(database, meta) -> None:
    await _seed(database)
    before = _snapshot(await _rows(database))

    report, code = await reconciler.reconcile(company_id=COMPANY_ID, apply=True)

    assert code == reconciler.EXIT_OK
    assert report["applied"] is False
    assert report["db_row_blocker"] is None
    assert "would_apply" not in report
    assert _snapshot(await _rows(database)) == before


# ---------------------------------------------------------------------------
# Two rows for one code: fail-closed, in both directions
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_two_rows_for_one_code_are_reported_and_not_touched(database, meta) -> None:
    await _seed(database, count=2, body="something nobody approved")
    before = _snapshot(await _rows(database))

    report, code = await reconciler.reconcile(company_id=COMPANY_ID, apply=False)

    assert code == reconciler.EXIT_BLOCKED
    assert report["db_row_blocker"] == reconciler.BLOCKER_MULTIPLE_ROWS
    assert report["applied"] is False
    # Never a promise that --apply resolves this.
    assert report.get("would_apply") is False
    assert _snapshot(await _rows(database)) == before


@pytest.mark.asyncio
async def test_apply_over_two_rows_writes_nothing_at_all(database, meta) -> None:
    """The fail-open blocker: "no row" used to mean "insert one".

    An ambiguity between two rows was answered with a third row.
    """
    await _seed(database, count=2, body="something nobody approved")
    before = _snapshot(await _rows(database))

    report, code = await reconciler.reconcile(company_id=COMPANY_ID, apply=True)

    assert code == reconciler.EXIT_BLOCKED
    assert report["applied"] is False
    assert report["db_row_blocker"] == reconciler.BLOCKER_MULTIPLE_ROWS
    after = await _rows(database)
    assert len(after) == 2
    assert _snapshot(after) == before


def test_an_unknown_blocker_without_a_row_never_becomes_an_insert() -> None:
    """The rule, not one instance of it.

    `multiple_rows_for_one_code` is today's way to arrive with a blocker and no
    row. A blocker added later must not silently inherit an INSERT.
    """
    assert reconciler._repair_for(reconciler.BLOCKER_ROW_MISSING, None) == reconciler.REPAIR_INSERT
    assert reconciler._repair_for(reconciler.BLOCKER_MULTIPLE_ROWS, None) == reconciler.REPAIR_NONE
    assert reconciler._repair_for("a_blocker_nobody_has_written_yet", None) == reconciler.REPAIR_NONE


@pytest.mark.asyncio
async def test_an_unknown_blocker_without_a_row_writes_nothing(database, meta, monkeypatch) -> None:
    real_audit = reconciler.audit

    async def unknown_blocker(session, *, company_id, templates):
        proof, _row, _blocker = await real_audit(session, company_id=company_id, templates=templates)
        return proof, None, "a_blocker_nobody_has_written_yet"

    monkeypatch.setattr(reconciler, "audit", unknown_blocker)

    report, code = await reconciler.reconcile(company_id=COMPANY_ID, apply=True)

    assert code == reconciler.EXIT_BLOCKED
    assert report["applied"] is False
    assert await _rows(database) == []


# ---------------------------------------------------------------------------
# Meta is the authority, and an unproven Meta writes nothing
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "templates",
    [
        pytest.param([], id="missing"),
        pytest.param([_meta_template(status="PENDING")], id="pending"),
        pytest.param([_meta_template(status="REJECTED")], id="rejected"),
        pytest.param([_meta_template(category="UTILITY")], id="wrong_category"),
        pytest.param([_meta_template(components=[{"type": "BODY", "text": "etwas anderes"}])], id="wrong_body"),
        pytest.param([_meta_template(), _meta_template()], id="duplicated"),
    ],
)
async def test_an_unproven_meta_template_blocks_before_any_write(
    database, monkeypatch, templates: list[dict[str, Any]]
) -> None:
    monkeypatch.setattr(reconciler, "MetaTemplateClient", lambda **_kwargs: _MetaClient(templates))
    monkeypatch.setattr(settings, "whatsapp_access_token", "test-token", raising=False)
    monkeypatch.setattr(settings, "meta_waba_id", "test-waba", raising=False)

    report, code = await reconciler.reconcile(company_id=COMPANY_ID, apply=True)

    assert code == reconciler.EXIT_BLOCKED
    assert report["applied"] is False
    assert report["meta"]["template_proven"] is False
    assert await _rows(database) == []


# ---------------------------------------------------------------------------
# A database that fails says so, and says nothing else
# ---------------------------------------------------------------------------


def _database_error() -> OperationalError:
    """Shaped like the real thing: a statement and its bound parameters."""
    return OperationalError(
        "INSERT INTO message_templates (body) VALUES (%(body)s)",
        {"body": LEAKY_PARAMETER},
        Exception("connection to server was lost"),
    )


@pytest.mark.asyncio
async def test_a_database_error_during_the_audit_is_reported_safely(
    database, meta, monkeypatch, capsys, configuration
) -> None:
    async def failing_execute(self, *args: Any, **kwargs: Any):
        raise _database_error()

    monkeypatch.setattr(AsyncSession, "execute", failing_execute)

    code = await reconciler.main(["--company-id", str(COMPANY_ID), "--apply"])

    assert code == reconciler.EXIT_BLOCKED
    captured = capsys.readouterr()
    assert captured.out.strip() == '{"ok": false, "reason": "voucher_template_database_unavailable"}'
    for stream in (captured.out, captured.err):
        assert LEAKY_PARAMETER not in stream
        assert "Traceback" not in stream
        assert "OperationalError" not in stream
        assert "INSERT INTO" not in stream


@pytest.mark.asyncio
async def test_a_database_error_while_committing_leaves_nothing_behind(
    database, meta, monkeypatch, capsys, configuration
) -> None:
    """The flush that fails must take the whole write with it."""
    # The SYNC Session is where the flush actually happens: AsyncSession.commit
    # hands the work to it, so patching the async facade would patch nothing.
    failing = {"on": True}
    real_flush = Session.flush

    def failing_flush(self, *args: Any, **kwargs: Any):
        if failing["on"]:
            raise _database_error()
        return real_flush(self, *args, **kwargs)

    monkeypatch.setattr(Session, "flush", failing_flush)

    code = await reconciler.main(["--company-id", str(COMPANY_ID), "--apply"])

    assert code == reconciler.EXIT_BLOCKED
    captured = capsys.readouterr()
    # Never `applied: true` over a transaction that rolled back.
    assert "applied" not in captured.out
    assert captured.out.strip() == '{"ok": false, "reason": "voucher_template_database_unavailable"}'
    assert LEAKY_PARAMETER not in captured.out
    assert LEAKY_PARAMETER not in captured.err

    failing["on"] = False
    assert await _rows(database) == []


# ---------------------------------------------------------------------------
# The command surface itself
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_cli_prints_safe_json_and_exits_zero_on_a_successful_apply(
    database, meta, capsys, configuration
) -> None:
    import json

    code = await reconciler.main(["--company-id", str(COMPANY_ID), "--apply"])

    assert code == reconciler.EXIT_OK
    captured = capsys.readouterr()
    report = json.loads(captured.out)
    assert report["applied"] is True
    assert report["company_id"] == COMPANY_ID
    assert "Traceback" not in captured.err
    # No phone number, no customer, no code, no token — this command knows none.
    for secret in ("test-token", "test-waba", "+49", template_contract.VOUCHER_TEMPLATE_BODY):
        assert secret not in captured.out
    assert len(await _rows(database)) == 1


@pytest.mark.asyncio
async def test_the_meta_client_is_only_ever_read(database, meta) -> None:
    await reconciler.reconcile(company_id=COMPANY_ID, apply=True)

    assert meta.calls == ["list_templates"]
    assert not hasattr(meta, "create_template")


def test_the_reconciler_reaches_no_easyweek_surface_and_sends_nothing() -> None:
    """Structural: the imports are the whole reachable surface."""
    source = __import__("inspect").getsource(reconciler)

    for forbidden in (
        "EasyWeekClient",
        "EasyWeekVoucherMutationClient",
        "VoucherDeliveryClient",
        "create_template",
        "send_message",
        "OutboxMessage",
        "MessageJob",
    ):
        assert forbidden not in source
