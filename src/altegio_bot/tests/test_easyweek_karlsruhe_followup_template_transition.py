"""Exercise the administrator's actual SQL on PostgreSQL, including repeated rollout."""

from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from altegio_bot.campaigns.easyweek_voucher_followup.preview import local_template_row_proven
from altegio_bot.models.models import MessageTemplate

SQL = (Path(__file__).resolve().parents[3] / "docs/easyweek/karlsruhe_followup_template.sql").read_text()
PREVIOUS = (
    "Hallo {client_name}, für Sie ist bei uns ein Gutschein über 10 € hinterlegt. "
    "Termin buchen: {booking_link}. Wenn Sie keine Nachrichten mehr von uns möchten, antworten Sie mit STOP (СТОП)."
)


def template(**changes):
    fields = dict(
        provider="easyweek",
        company_id=322579,
        code="new_client_voucher_followup_v1",
        language="de",
        body=PREVIOUS,
        meta_template_name="kitilash_ka_new_client_voucher_followup_v1",
        is_active=True,
    )
    return MessageTemplate(**(fields | changes))


async def run_sql(session_maker, action):
    async with session_maker.begin() as session:
        await session.execute(text("SELECT set_config('easyweek.template_action', :action, true)"), {"action": action})
        await (await session.connection()).exec_driver_sql(SQL)


async def versions(session_maker):
    async with session_maker() as session:
        # An UPDATE that only rewrites identical values still gets a new xmin.
        return dict((await session.execute(text("SELECT id, xmin::text FROM message_templates"))).all())


async def test_known_previous_body_transitions_once_and_only_for_karlsruhe(session_maker):
    async with session_maker.begin() as session:
        target = template()
        other = template(company_id=322580)
        session.add_all([target, other])
        await session.flush()
        assert not await local_template_row_proven(session, company_id=322579)
    before = await versions(session_maker)
    await run_sql(session_maker, "transition")
    after = await versions(session_maker)
    assert after.keys() == before.keys()
    assert after[target.id] != before[target.id]
    assert after[other.id] == before[other.id]
    async with session_maker() as session:
        assert await local_template_row_proven(session, company_id=322579)
    await run_sql(session_maker, "transition")
    assert await versions(session_maker) == after


async def test_absent_template_requires_explicit_first_install(session_maker):
    with pytest.raises(DBAPIError, match="template missing"):
        await run_sql(session_maker, "transition")
    assert await versions(session_maker) == {}
    await run_sql(session_maker, "install")
    async with session_maker() as session:
        assert await local_template_row_proven(session, company_id=322579)
    before = await versions(session_maker)
    await run_sql(session_maker, "install")
    assert await versions(session_maker) == before


@pytest.mark.parametrize(
    "rows, action, error",
    [
        ([{}, {"is_active": False}], "transition", "Multiple Karlsruhe"),
        ([{"is_active": False}], "transition", "inactive row"),
        ([{"meta_template_name": "another"}], "transition", "identity"),
        ([{"body": "unknown"}], "transition", "Unexpected existing"),
        ([{}], "install", "Unexpected existing"),
    ],
)
async def test_conflicts_leave_all_rows_untouched(session_maker, rows, action, error):
    async with session_maker.begin() as session:
        session.add_all(template(**changes) for changes in rows)
    before = await versions(session_maker)
    with pytest.raises(DBAPIError, match=error):
        await run_sql(session_maker, action)
    assert await versions(session_maker) == before
