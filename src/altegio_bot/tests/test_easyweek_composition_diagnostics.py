"""Legacy Altegio cards are not an identity conflict, and a refusal names its rows.

Two confirmed problems from one production check of preview #44, which stopped on
``manual_recipient_identity_conflict``:

**The false conflict.** A correct earned EasyWeek recipient of Karlsruhe also had
two Altegio cards on the same phone, in other branches. The COUNT of those legacy
cards alone refused the whole composition. The owner states the business rule: one
person may legitimately hold several Altegio cards across branches and several
within one branch, and the number of them proves nothing about identity.

**The lost detail.** ``ProductionComposition`` already knows which member failed and
why, and the UI contract threw that away, so an operator saw one global error and no
way to find the row behind it.

Everything here is synthetic. The counts and sums used below are scenario data, not
production constants.
"""

from __future__ import annotations

import asyncio
import json
import uuid as uuid_module

from sqlalchemy import select

from altegio_bot.campaigns import easyweek_manual_identity as manual
from altegio_bot.campaigns.easyweek_voucher_production import dispatch as dispatch_module
from altegio_bot.campaigns.easyweek_voucher_production import ledger as ledger_module
from altegio_bot.models.models import CampaignRecipient, CampaignRun, Client
from altegio_bot.tests import easyweek_voucher_10eur_fixtures as new
from altegio_bot.tests import easyweek_voucher_production_fixtures as old
from altegio_bot.utils import utcnow

PHONE = "+4915100000777"
CUSTOMER_UUID = "7a1d0b00-0000-4000-8000-00000000a001"
KARLSRUHE = manual.KARLSRUHE_COMPANY_ID


async def _karlsruhe_identity(session, *, phone: str = PHONE, customer_uuid: str = CUSTOMER_UUID) -> Client:
    """The one proven EasyWeek card this person is addressed through."""
    client = Client(
        provider="easyweek",
        company_id=KARLSRUHE,
        altegio_client_id=None,
        easyweek_customer_uuid=uuid_module.UUID(customer_uuid),
        easyweek_identity_assigned_at=None,
        phone_e164=phone,
        display_name="Synthetic Karlsruhe",
        raw={},
    )
    session.add(client)
    await session.flush()
    return client


async def _altegio_cards(session, *, companies: list[int], phone: str = PHONE, opted_out: bool = False) -> list[Client]:
    """Legacy Altegio cards on the same number. Never an EasyWeek identity."""
    cards: list[Client] = []
    for index, company_id in enumerate(companies):
        card = Client(
            provider="altegio",
            company_id=company_id,
            altegio_client_id=900100 + index,
            phone_e164=phone,
            display_name=f"Synthetic Altegio {index}",
            wa_opted_out=opted_out,
            raw={},
        )
        session.add(card)
        cards.append(card)
    await session.flush()
    return cards


async def test_legacy_cards_in_several_branches_are_not_an_identity_conflict(session_maker):
    """The reported case. Two Altegio cards elsewhere, one correct Karlsruhe card."""
    async with session_maker() as session, session.begin():
        expected = await _karlsruhe_identity(session)
        await _altegio_cards(session, companies=[111111, 222222])

    async with session_maker() as session:
        client, reason = await manual.local_identity(
            session, company_id=KARLSRUHE, phone=PHONE, customer_uuid=CUSTOMER_UUID
        )
    assert reason is None, reason
    assert client is not None and client.id == expected.id


async def test_many_legacy_cards_in_one_branch_are_not_an_identity_conflict(session_maker):
    """No arbitrary ceiling: the same rule holds for four cards in one branch."""
    async with session_maker() as session, session.begin():
        expected = await _karlsruhe_identity(session)
        await _altegio_cards(session, companies=[111111, 111111, 111111, 111111])

    async with session_maker() as session:
        client, reason = await manual.local_identity(
            session, company_id=KARLSRUHE, phone=PHONE, customer_uuid=CUSTOMER_UUID
        )
    assert reason is None, reason
    assert client is not None and client.id == expected.id


async def test_legacy_cards_are_left_exactly_as_they_were(session_maker):
    """Nothing is merged, renamed, rebound or backfilled to clear the refusal."""
    async with session_maker() as session, session.begin():
        await _karlsruhe_identity(session)
        await _altegio_cards(session, companies=[111111, 222222])

    async with session_maker() as session:
        before = [
            (row.id, row.provider, row.company_id, row.altegio_client_id, row.easyweek_customer_uuid, row.display_name)
            for row in (await session.scalars(select(Client).order_by(Client.id))).all()
        ]
        client, reason = await manual.local_identity(
            session, company_id=KARLSRUHE, phone=PHONE, customer_uuid=CUSTOMER_UUID
        )
    assert reason is None and client is not None
    async with session_maker() as session:
        after = [
            (row.id, row.provider, row.company_id, row.altegio_client_id, row.easyweek_customer_uuid, row.display_name)
            for row in (await session.scalars(select(Client).order_by(Client.id))).all()
        ]
    assert after == before
    # No UUID was invented for a legacy card to get past the check.
    assert all(entry[4] is None for entry in after if entry[1] == "altegio")


async def test_an_opt_out_on_any_matched_legacy_card_still_refuses(session_maker):
    """The one thing Altegio rows ARE read for keeps working."""
    async with session_maker() as session, session.begin():
        await _karlsruhe_identity(session)
        await _altegio_cards(session, companies=[111111, 222222], opted_out=True)

    async with session_maker() as session:
        client, reason = await manual.local_identity(
            session, company_id=KARLSRUHE, phone=PHONE, customer_uuid=CUSTOMER_UUID
        )
    assert client is None and reason == manual.CLIENT_OPTED_OUT


async def test_two_easyweek_identities_on_one_phone_are_still_ambiguous(session_maker):
    """A real EasyWeek ambiguity is not relaxed by any of this."""
    async with session_maker() as session, session.begin():
        await _karlsruhe_identity(session)
        await _karlsruhe_identity(session, customer_uuid="7a1d0b00-0000-4000-8000-00000000a002")
        await _altegio_cards(session, companies=[111111, 222222])

    async with session_maker() as session:
        client, reason = await manual.local_identity(
            session, company_id=KARLSRUHE, phone=PHONE, customer_uuid=CUSTOMER_UUID
        )
    assert client is None and reason == manual.CLIENT_AMBIGUOUS


async def test_an_easyweek_card_of_another_branch_only_still_conflicts(session_maker):
    """§44.2 unchanged: a workspace customer does not prove branch membership."""
    async with session_maker() as session, session.begin():
        client = Client(
            provider="easyweek",
            company_id=KARLSRUHE + 1,
            altegio_client_id=None,
            easyweek_customer_uuid=uuid_module.UUID(CUSTOMER_UUID),
            phone_e164=PHONE,
            display_name="Synthetic Other Branch",
            raw={},
        )
        session.add(client)
        await _altegio_cards(session, companies=[111111, 222222])

    async with session_maker() as session:
        found, reason = await manual.local_identity(
            session, company_id=KARLSRUHE, phone=PHONE, customer_uuid=CUSTOMER_UUID
        )
    assert found is None and reason == manual.IDENTITY_CONFLICT


async def test_a_different_phone_on_the_branch_card_still_conflicts(session_maker):
    """The phone/branch/identity binding is untouched."""
    async with session_maker() as session, session.begin():
        await _karlsruhe_identity(session, phone="+4915100000888")
        await _altegio_cards(session, companies=[111111, 222222])

    async with session_maker() as session:
        found, reason = await manual.local_identity(
            session, company_id=KARLSRUHE, phone=PHONE, customer_uuid=CUSTOMER_UUID
        )
    assert found is None and reason == manual.IDENTITY_CONFLICT


# ===========================================================================
# The private UI contract carries WHICH row failed and WHY
# ===========================================================================

COMPOSITION_URL = "/ops/voucher-mailings/api/composition"
EXCLUDE_URL = "/ops/voucher-mailings/api/exclude-recipient"


async def _preview(session_maker, *, count: int):
    """One completed Karlsruhe preview of *count* manual candidates, and its reader."""
    run_id, recipient_ids = await old.seed_production_preview(session_maker, count=count)
    await new.seed_template_and_sender(session_maker)
    return run_id, recipient_ids, new.FakeReader(count=count)


async def _check(client, run_id: int) -> tuple[int, dict]:
    response = await client.post(COMPOSITION_URL, json={"preview_run_id": run_id})
    return response.status_code, response.json()


def _row(body: dict, recipient_id: int) -> dict:
    found = [row for row in body["recipients"] if row["campaign_recipient_id"] == recipient_id]
    assert len(found) == 1, (recipient_id, body["recipients"])
    return found[0]


async def test_the_whole_audience_is_reported_even_when_one_row_refuses(
    session_maker, production_configuration, binding_key, ui_client, transports
):
    """The defect, from the operator's side: one bad row used to hide the other two.

    The check refuses as a whole — a partial composition is a different approval —
    and it now says so while still showing every active row, each with its own
    state, so the operator can find the one to act on.
    """
    run_id, recipient_ids, reader = await _preview(session_maker, count=3)
    async with session_maker() as session, session.begin():
        recipient = await session.get(CampaignRecipient, recipient_ids[1])
        client = await session.get(Client, recipient.client_id)
        client.wa_opted_out = True
    transports.use(reader=reader)

    status, body = await _check(ui_client, run_id)
    assert status == 200
    assert body["composition_known"] is True
    assert body["composition_proven"] is False
    assert len(body["recipients"]) == 3
    assert body["observed_active_count"] == 3
    assert body["proven_count"] == 2
    assert body["refused_count"] == 1
    assert body["unchecked_count"] == 0
    refused = _row(body, recipient_ids[1])
    assert refused["state"] == "refused"
    assert "voucher_production_recipient_opted_out" in refused["reasons"]
    # The other two are not tarred with it.
    for recipient_id in (recipient_ids[0], recipient_ids[2]):
        assert _row(body, recipient_id)["state"] == "proven"
        assert _row(body, recipient_id)["reasons"] == []
    # And the refusal is attributed to the row, so it is not also a batch blocker.
    assert "voucher_production_recipient_opted_out" not in body["blockers"]


async def test_several_rows_refuse_for_different_reasons(
    session_maker, production_configuration, binding_key, ui_client, transports
):
    """Two different problems must read as two different problems."""
    run_id, recipient_ids, reader = await _preview(session_maker, count=3)
    async with session_maker() as session, session.begin():
        recipient = await session.get(CampaignRecipient, recipient_ids[0])
        client = await session.get(Client, recipient.client_id)
        client.wa_opted_out = True
    # The second person's EasyWeek card no longer carries a usable name.
    reader.customers[old.CUSTOMER_UUIDS[1]] = old.customer_payload(1, first_name="")
    reader.customer_pages[old.PHONES[1]] = [old.customers_page([old.customer_payload(1, first_name="")])]
    transports.use(reader=reader)

    status, body = await _check(ui_client, run_id)
    assert status == 200
    assert body["refused_count"] == 2 and body["proven_count"] == 1
    first = set(_row(body, recipient_ids[0])["reasons"])
    second = set(_row(body, recipient_ids[1])["reasons"])
    assert "voucher_production_recipient_opted_out" in first
    assert second and second != first, (first, second)


async def test_a_batch_blocker_is_not_attributed_to_any_client(
    session_maker, production_configuration, binding_key, ui_client, transports
):
    """An entitlement taken in another batch belongs to the batch, not to a row.

    Nothing about the other batch's people is revealed either: the operator reads a
    reason code and their own rows, and no identity from another campaign appears.
    """
    from altegio_bot.tests.test_easyweek_voucher_production_mailing import _freeze

    first_run, _ = await old.seed_production_preview(session_maker, count=1)
    await new.seed_template_and_sender(session_maker)
    frozen = await _freeze(session_maker, old.FakeReader(count=1), old.production_request(run_id=first_run), count=1)
    assert frozen.outcome == "frozen", frozen.reasons

    run_id, _recipient_ids, reader = await _preview(session_maker, count=1)
    transports.use(reader=reader)
    status, body = await _check(ui_client, run_id)

    assert status == 200
    assert body["composition_proven"] is False
    assert "voucher_production_entitlement_already_exists" in body["blockers"]
    # Shown, proven on its own terms, and not blamed for the batch's problem.
    assert len(body["recipients"]) == 1
    assert body["recipients"][0]["reasons"] == []
    assert body["recipients"][0]["state"] == "proven"


async def test_an_early_refusal_lists_unchecked_rows_from_the_preview(
    session_maker, production_configuration, binding_key, ui_client, transports
):
    """A check that stopped before the live reads is not an empty audience.

    The rows are listed from the local preview so the operator can see how many
    there are and which they are — marked UNCHECKED and flagged as named from the
    preview, because nothing here proved an identity.
    """
    run_id, recipient_ids, reader = await _preview(session_maker, count=2)
    async with session_maker() as session, session.begin():
        # An owner-test basis refuses the whole composition before any live read.
        # Its typed binding is what the schema requires of that basis, so the row is
        # a real one production could hold rather than a shape only a test can make.
        recipient = await session.get(CampaignRecipient, recipient_ids[1])
        recipient.recipient_basis = "owner_test_account"
        recipient.easyweek_customer_uuid = None
        recipient.easyweek_test_customer_uuid = uuid_module.UUID(old.CUSTOMER_UUIDS[1])
    transports.use(reader=reader)

    status, body = await _check(ui_client, run_id)
    assert status == 200
    assert body["composition_known"] is True
    assert body["composition_proven"] is False
    assert "voucher_production_composition_mixed_basis" in body["blockers"]
    assert body["observed_active_count"] == 2
    assert body["unchecked_count"] == 2 and body["proven_count"] == 0
    assert len(body["recipients"]) == 2
    for row in body["recipients"]:
        assert row["state"] == "unchecked"
        assert row["name_from_preview"] is True
        assert row["reasons"] == []


async def test_a_lost_provider_read_is_not_an_audience_of_nobody(
    session_maker, production_configuration, binding_key, ui_client, transports
):
    """A failure to reach EasyWeek says the audience is unknown, never that it is 0."""
    run_id, _recipient_ids, reader = await _preview(session_maker, count=3)
    reader.customer_pages[old.PHONES[0]] = TimeoutError("synthetic")
    transports.use(reader=reader)

    status, body = await _check(ui_client, run_id)
    assert status == 200
    # A per-member read failure still describes the audience, with that row refused.
    assert body["composition_known"] is True
    assert len(body["recipients"]) == 3
    assert body["refused_count"] >= 1


async def test_a_closed_fence_reports_an_unknown_audience_rather_than_zero(
    session_maker, production_configuration, binding_key, ui_client, transports, monkeypatch
):
    """``composition_known=false`` is the answer that contains no audience at all."""
    from altegio_bot.settings import settings as live

    run_id, _recipient_ids, reader = await _preview(session_maker, count=2)
    transports.use(reader=reader)
    monkeypatch.setattr(live, "easyweek_voucher_production_mailing_enabled", False, raising=False)

    status, body = await _check(ui_client, run_id)
    assert status == 200
    assert body["composition_known"] is False
    assert body["composition_proven"] is False
    assert "voucher_production_disabled" in body["blockers"]
    # Zero is reported as zero RECIPIENTS COUNTED, and `known=false` is what tells
    # the page not to believe it. The two together are the honest answer.
    assert body["recipients"] == []


# ===========================================================================
# Excluding one problematic recipient, from the page that found them
# ===========================================================================


async def _exclude(client, run_id: int, recipient_id: int) -> tuple[int, dict]:
    response = await client.post(EXCLUDE_URL, json={"preview_run_id": run_id, "campaign_recipient_id": recipient_id})
    return response.status_code, response.json()


async def _recipient(session_maker, recipient_id: int) -> CampaignRecipient:
    async with session_maker() as session:
        row = await session.get(CampaignRecipient, recipient_id)
        assert row is not None
        return row


async def test_excluding_a_problem_row_changes_the_composition_and_its_money(
    session_maker, production_configuration, binding_key, ui_client, transports
):
    """The whole operator loop: find the row, exclude it, check again, read the sum.

    The numbers here are scenario data for a synthetic preview, not production
    constants: four candidates at 1000 minor units each become three.
    """
    run_id, recipient_ids, reader = await _preview(session_maker, count=4)
    async with session_maker() as session, session.begin():
        recipient = await session.get(CampaignRecipient, recipient_ids[2])
        client = await session.get(Client, recipient.client_id)
        client.wa_opted_out = True
    transports.use(reader=reader)

    _status, before = await _check(ui_client, run_id)
    assert before["composition_proven"] is False
    assert before["recipient_count"] == 4
    assert before["total_exposure_minor"] == 4 * 1000
    assert _row(before, recipient_ids[2])["state"] == "refused"

    status, outcome = await _exclude(ui_client, run_id, recipient_ids[2])
    assert status == 200, outcome
    assert outcome["applied"] is True
    assert outcome["status"] == "skipped"
    assert outcome["excluded_reason"] == "manual_removed"
    assert outcome["already_excluded"] is False

    # Soft removal: the row is still there, with its basis and its audit trail.
    row = await _recipient(session_maker, recipient_ids[2])
    assert row.status == "skipped" and row.excluded_reason == "manual_removed"
    assert row.recipient_basis == "operator_manual_selection"
    assert row.easyweek_customer_uuid is not None
    assert (row.meta or {}).get("manually_removed_at")
    # The client card is untouched — exclusion is not deletion.
    async with session_maker() as session:
        assert await session.get(Client, row.client_id) is not None

    # Counters were recomputed in the same transaction as the edit.
    async with session_maker() as session:
        run = await session.get(CampaignRun, run_id)
        assert run.candidates_count == 3

    # A fresh check: three people, and the money follows.
    _status, after = await _check(ui_client, run_id)
    assert after["composition_proven"] is True
    assert after["recipient_count"] == 3
    assert after["observed_active_count"] == 3
    assert after["proven_count"] == 3
    assert after["total_exposure_minor"] == 3 * 1000
    assert all(entry["campaign_recipient_id"] != recipient_ids[2] for entry in after["recipients"])


async def test_an_exclusion_reaches_no_provider_at_all(
    session_maker, production_configuration, binding_key, ui_client, transports
):
    """No EasyWeek or Meta mutation, and no order, voucher or ledger row."""
    from altegio_bot.models.models import EasyWeekVoucherProductionBatch

    run_id, recipient_ids, reader = await _preview(session_maker, count=2)
    mutator = old.FakeMutator()
    sender = old.FakeSender()
    transports.use(reader=reader, mutator=mutator, sender=sender)

    status, outcome = await _exclude(ui_client, run_id, recipient_ids[0])
    assert status == 200 and outcome["applied"] is True
    assert mutator.calls == []
    assert sender.calls == 0
    assert reader.order_calls == []
    async with session_maker() as session:
        assert await session.scalar(select(EasyWeekVoucherProductionBatch.id)) is None


async def test_excluding_the_same_row_twice_is_idempotent(
    session_maker, production_configuration, binding_key, ui_client, transports
):
    """A second press is the same exclusion, answered as such rather than as an error."""
    run_id, recipient_ids, reader = await _preview(session_maker, count=2)
    transports.use(reader=reader)

    first_status, first = await _exclude(ui_client, run_id, recipient_ids[0])
    assert first_status == 200 and first["already_excluded"] is False
    second_status, second = await _exclude(ui_client, run_id, recipient_ids[0])
    assert second_status == 200, second
    assert second["applied"] is True and second["already_excluded"] is True

    # One exclusion, not two: the remaining candidate count moved once.
    async with session_maker() as session:
        run = await session.get(CampaignRun, run_id)
        assert run.candidates_count == 1
    row = await _recipient(session_maker, recipient_ids[0])
    assert row.status == "skipped" and row.excluded_reason == "manual_removed"


async def test_a_frozen_preview_refuses_the_exclusion(
    session_maker, production_configuration, binding_key, ui_client, transports
):
    """The §42/§43 lock holds at the write boundary, not only on the page.

    A batch frozen onto this preview addresses each slot by (run id, recipient id)
    and re-proves the whole composition before every external step, so editing the
    snapshot underneath it would strand real money.
    """
    from altegio_bot.tests.test_easyweek_voucher_production_mailing import _freeze

    run_id, recipient_ids = await old.seed_production_preview(session_maker, count=2)
    await new.seed_template_and_sender(session_maker)
    reader = old.FakeReader(count=2)
    frozen = await _freeze(session_maker, reader, old.production_request(run_id=run_id), count=2)
    assert frozen.outcome == "frozen", frozen.reasons
    transports.use(reader=reader)

    status, outcome = await _exclude(ui_client, run_id, recipient_ids[0])
    assert status == 409, outcome
    assert outcome["applied"] is False
    assert outcome["reason"] == "voucher_production_recipient_not_excludable"
    row = await _recipient(session_maker, recipient_ids[0])
    assert row.status == "candidate" and row.excluded_reason is None


async def test_a_recipient_of_another_run_is_refused(
    session_maker, production_configuration, binding_key, ui_client, transports
):
    """The row has to belong to the preview the request names."""
    run_id, _ids = await old.seed_production_preview(session_maker, count=1)
    other_run, other_ids = await old.seed_production_preview(session_maker, count=1, offset=4)
    await new.seed_template_and_sender(session_maker)
    transports.use(reader=new.FakeReader(count=1))

    status, outcome = await _exclude(ui_client, run_id, other_ids[0])
    assert status == 409, outcome
    assert outcome["reason"] == "voucher_production_recipient_not_excludable"
    row = await _recipient(session_maker, other_ids[0])
    assert row.status == "candidate"
    assert row.campaign_run_id == other_run


async def test_a_preview_of_another_provider_is_refused(
    session_maker, production_configuration, binding_key, ui_client, transports
):
    """An Altegio preview is not this page's to edit, whatever the ids say."""
    run_id, recipient_ids = await old.seed_production_preview(session_maker, count=1, provider="altegio")
    await new.seed_template_and_sender(session_maker)
    transports.use(reader=new.FakeReader(count=1))

    status, outcome = await _exclude(ui_client, run_id, recipient_ids[0])
    assert status == 409, outcome
    assert outcome["reason"] == "voucher_production_recipient_not_excludable"
    row = await _recipient(session_maker, recipient_ids[0])
    assert row.status == "candidate"


async def test_a_preview_of_another_branch_is_refused(
    session_maker, production_configuration, binding_key, ui_client, transports
):
    """Karlsruhe only. The scope is re-verified under the run's row lock."""
    run_id, recipient_ids = await old.seed_production_preview(session_maker, count=1, company_id=old.COMPANY_ID + 1)
    await new.seed_template_and_sender(session_maker)
    transports.use(reader=new.FakeReader(count=1))

    status, outcome = await _exclude(ui_client, run_id, recipient_ids[0])
    assert status == 409, outcome
    assert outcome["reason"] == "voucher_production_recipient_not_excludable"
    assert (await _recipient(session_maker, recipient_ids[0])).status == "candidate"


async def test_an_exclusion_without_a_session_or_csrf_or_origin_is_refused(
    session_maker, production_configuration, binding_key, ui_client, anon_client, transports
):
    """Three doors, each shut on its own, and the row is untouched by all of them."""
    run_id, recipient_ids, reader = await _preview(session_maker, count=1)
    transports.use(reader=reader)
    payload = {"preview_run_id": run_id, "campaign_recipient_id": recipient_ids[0]}

    # No Ops session at all.
    assert (await anon_client.post(EXCLUDE_URL, json=payload)).status_code in (401, 403)
    # A session, no CSRF header.
    no_csrf = await ui_client.post(EXCLUDE_URL, json=payload, headers={"X-Ops-CSRF": ""})
    assert no_csrf.status_code in (401, 403), no_csrf.text
    # A session and a token, from somewhere else.
    bad_origin = await ui_client.post(EXCLUDE_URL, json=payload, headers={"Origin": "https://evil.invalid"})
    assert bad_origin.status_code in (401, 403), bad_origin.text

    assert (await _recipient(session_maker, recipient_ids[0])).status == "candidate"


async def test_the_browser_cannot_widen_the_request_with_extra_fields(
    session_maker, production_configuration, binding_key, ui_client, transports
):
    """No UUID, no identity proof, no "the check passed" — the model forbids extras."""
    run_id, recipient_ids, reader = await _preview(session_maker, count=1)
    transports.use(reader=reader)

    response = await ui_client.post(
        EXCLUDE_URL,
        json={
            "preview_run_id": run_id,
            "campaign_recipient_id": recipient_ids[0],
            "easyweek_customer_uuid": old.CUSTOMER_UUIDS[0],
            "composition_proven": True,
        },
    )
    assert response.status_code == 422, response.text
    assert (await _recipient(session_maker, recipient_ids[0])).status == "candidate"


async def test_a_freeze_racing_an_exclusion_leaves_one_consistent_answer(
    session_maker, production_configuration, binding_key, ui_client, transports
):
    """Both orders, and neither leaves a frozen batch disagreeing with its preview."""
    from altegio_bot.tests.test_easyweek_voucher_production_mailing import _freeze

    # Exclusion first, then the freeze: the batch is frozen on what is left.
    run_id, recipient_ids = await old.seed_production_preview(session_maker, count=3)
    await new.seed_template_and_sender(session_maker)
    reader = old.FakeReader(count=3)
    transports.use(reader=reader)
    status, outcome = await _exclude(ui_client, run_id, recipient_ids[0])
    assert status == 200 and outcome["applied"] is True
    frozen = await _freeze(session_maker, reader, old.production_request(run_id=run_id), count=2)
    assert frozen.outcome == "frozen", frozen.reasons
    assert frozen.batch["recipient_count"] == 2
    assert {item["slot"] for item in frozen.batch["items"]} == {1, 2}

    # Freeze first, then the exclusion: refused, and the batch keeps its snapshot.
    status, outcome = await _exclude(ui_client, run_id, recipient_ids[1])
    assert status == 409, outcome
    row = await _recipient(session_maker, recipient_ids[1])
    assert row.status == "candidate"
    batch = await ledger_module.load_for_preview(session_maker, campaign_run_id=run_id)
    assert batch.recipient_count == 2


async def test_the_exclusion_audit_row_names_no_person(
    session_maker, production_configuration, binding_key, ui_client, transports
):
    """The action is audited; the audit stays free of names, phones and UUIDs."""
    from altegio_bot.models.models import EasyWeekVoucherProductionAudit

    run_id, recipient_ids, reader = await _preview(session_maker, count=2)
    transports.use(reader=reader)
    status, _outcome = await _exclude(ui_client, run_id, recipient_ids[0])
    assert status == 200

    async with session_maker() as session:
        rows = list((await session.scalars(select(EasyWeekVoucherProductionAudit))).all())
    excluded = [row for row in rows if row.action == "exclude_recipient"]
    assert len(excluded) == 1
    serialised = json.dumps([row.detail for row in excluded], default=str)
    assert str(recipient_ids[0]) in serialised
    for secret in (old.CUSTOMER_NAMES[0], old.PHONES[0], old.CUSTOMER_UUIDS[0]):
        assert secret not in serialised, secret


async def test_the_diagnostic_answers_are_never_cached(
    session_maker, production_configuration, binding_key, ui_client, transports
):
    """These payloads name real people, for one operator, in one moment."""
    run_id, recipient_ids, reader = await _preview(session_maker, count=1)
    transports.use(reader=reader)

    for response in (
        await ui_client.post(COMPOSITION_URL, json={"preview_run_id": run_id}),
        await ui_client.post(EXCLUDE_URL, json={"preview_run_id": run_id, "campaign_recipient_id": recipient_ids[0]}),
    ):
        cache = response.headers.get("cache-control", "")
        assert "no-store" in cache, cache


async def test_the_new_diagnostic_fields_stay_out_of_every_safe_report(
    session_maker, production_configuration, binding_key, ui_client, transports
):
    """The private UI contract grew; the PII-free surfaces did not.

    ``as_ui_dict`` carries names and now per-row reasons. ``as_safe_dict`` is what
    goes into approval payloads, audit rows, HMAC bindings and status dumps, and it
    must still be nameless — so the two are asserted against each other on the same
    composition rather than only inspected separately.
    """
    from altegio_bot.campaigns.easyweek_voucher_production.composition import (
        prove_production_composition,
    )

    run_id, recipient_ids, reader = await _preview(session_maker, count=2)
    async with session_maker() as session, session.begin():
        recipient = await session.get(CampaignRecipient, recipient_ids[0])
        client = await session.get(Client, recipient.client_id)
        client.wa_opted_out = True
    transports.use(reader=reader)

    _status, ui = await _check(ui_client, run_id)
    # The operator's own screen names the person, which is the point of it.
    assert any(old.CUSTOMER_NAMES[1] in str(row["display_name"]) for row in ui["recipients"])

    async with session_maker() as session:
        composition = await prove_production_composition(
            session,
            preview_run_id=run_id,
            client_reader=reader,
            now=utcnow(),
            approval=None,
            schema_version="3",
        )
    safe = json.dumps(
        [member.as_safe_dict(preview_run_id=run_id, schema_version="3") for member in composition.members],
        default=str,
    )
    for secret in (*old.CUSTOMER_NAMES[:2], *old.PHONES[:2], *old.CUSTOMER_UUIDS[:2]):
        assert secret not in safe, secret
    # The reason codes themselves are safe to carry: they are a fixed vocabulary.
    assert "voucher_production_recipient_opted_out" in safe


async def test_a_hostile_display_name_is_carried_as_data_not_markup(
    session_maker, production_configuration, binding_key, ui_client, transports
):
    """The API answers with the stored characters; the page is what escapes them.

    Asserted here so a future "helpful" sanitisation in the API cannot silently
    replace the escaping the browser test proves — and so the JSON contract is
    known to be data rather than markup.
    """
    hostile = '<script>alert("xss")</script>'
    run_id, recipient_ids, reader = await _preview(session_maker, count=1)
    reader.customers[old.CUSTOMER_UUIDS[0]] = old.customer_payload(0, first_name=hostile)
    reader.customer_pages[old.PHONES[0]] = [old.customers_page([old.customer_payload(0, first_name=hostile)])]
    async with session_maker() as session, session.begin():
        recipient = await session.get(CampaignRecipient, recipient_ids[0])
        recipient.display_name = hostile
    transports.use(reader=reader)

    status, body = await _check(ui_client, run_id)
    assert status == 200
    assert body["recipients"][0]["display_name"] == hostile
    # And the HTML page itself embeds no recipient data at render time at all: the
    # table is built in the browser from this JSON, so there is nothing to escape
    # server-side and nothing for a stored name to break out of.
    page = await ui_client.get(f"/ops/voucher-mailings/prepare?preview_run_id={run_id}")
    assert page.status_code == 200
    assert hostile not in page.text


# ===========================================================================
# F1 — the browser's wait and the server's read have to be the same policy
# ===========================================================================


def test_the_read_budget_covers_the_measured_cost_of_a_real_list():
    """F1. The 20-second constant was shorter than the read it was waiting for.

    Reported: thirty-four manual recipients, sixty-eight successful reads at 350 ms,
    ``composition_proven=true`` after 24.73 s — and a browser that stopped listening
    at 20. The budget is now derived from the audience, and the numbers it has to
    cover are asserted here rather than left to a comment.
    """
    from altegio_bot.campaigns.easyweek_voucher_production import dispatch as dispatch_module

    # The reported case, with room for a provider slower than the one measured.
    measured_seconds = 24.73
    assert dispatch_module.composition_read_budget_seconds(34) > measured_seconds
    assert dispatch_module.composition_read_budget_seconds(34) >= 2 * measured_seconds

    # It scales with the audience, so a one-person preview does not inherit a
    # three-minute wait and a large one is not cut off.
    assert dispatch_module.composition_read_budget_seconds(1) < dispatch_module.composition_read_budget_seconds(34)
    assert dispatch_module.composition_read_budget_seconds(34) < dispatch_module.composition_read_budget_seconds(100)

    # Bounded, never open ended, whatever is asked of it.
    ceiling = dispatch_module.COMPOSITION_READ_BUDGET_CEILING_SECONDS
    assert dispatch_module.composition_read_budget_seconds(10_000) == ceiling
    assert dispatch_module.composition_read_budget_seconds(0) > 0
    assert dispatch_module.composition_read_budget_seconds(-5) == dispatch_module.composition_read_budget_seconds(0)


async def test_a_realistic_thirty_four_recipient_check_answers_inside_its_budget(
    session_maker, production_configuration, binding_key, ui_client, transports
):
    """F1, end to end: the reported list, with the reported per-read latency.

    Thirty-four recipients, every provider read delayed, and the answer still
    reaches the UI with ``composition_proven=true`` — once, without a second press.
    The delay is scaled down from the reported 350 ms so the suite stays usable; what
    the test pins is that the BUDGET exceeds the measured cost, which the unit test
    above asserts against the real numbers.
    """
    count = 34
    run_id, _recipient_ids, reader = await _preview(session_maker, count=count)
    reads: list[str] = []
    per_read_seconds = 0.01

    class SlowReader(type(reader)):  # type: ignore[misc]
        async def list_customers(self, *, params):
            reads.append("list")
            await asyncio.sleep(per_read_seconds)
            return await super().list_customers(params=params)

        async def get_customer(self, customer_uuid):
            reads.append("get")
            await asyncio.sleep(per_read_seconds)
            return await super().get_customer(customer_uuid)

    slow = SlowReader(count=count)
    transports.use(reader=slow)

    status, body = await _check(ui_client, run_id)
    assert status == 200
    assert body["composition_proven"] is True, body["reasons"]
    assert body["recipient_count"] == count
    assert body["proven_count"] == count
    assert body["total_exposure_minor"] == count * 1000
    # Two provider reads per manual recipient, which is what the budget is built on.
    assert len(reads) == 2 * count, len(reads)
    # And the answer tells the page which bound it was given, so the page waits for
    # the same policy instead of a constant of its own.
    assert body["read_budget_seconds"] == dispatch_module.composition_read_budget_seconds(count)


async def test_a_mixed_earned_and_manual_check_answers_inside_its_budget(
    session_maker, production_configuration, binding_key, ui_client, transports, monkeypatch
):
    """F1, the heavier read path: earned proof plus a manual zero-booking policy.

    Earned costs four reads — the source customer, the source booking, and the two
    of ``prove_customer`` — and the manual policy adds a history read. That is the
    shape the budget's per-recipient allowance has to cover, so it is exercised
    rather than assumed.
    """
    from altegio_bot.tests.test_easyweek_voucher_10eur_lifecycle import _mixed_preview

    run_id, reader = await _mixed_preview(session_maker, monkeypatch)
    transports.use(reader=reader)

    status, body = await _check(ui_client, run_id)
    assert status == 200, body
    assert body["composition_known"] is True
    assert body["recipient_count"] == 2
    assert body["proven_count"] == 2, body["recipients"]
    assert body["composition_proven"] is True, body["reasons"]
    assert {row["recipient_basis"] for row in body["recipients"]} == {
        "earned_first_visit",
        "operator_manual_selection",
    }
    assert body["read_budget_seconds"] == dispatch_module.composition_read_budget_seconds(2)


async def test_a_read_that_never_finishes_returns_control_with_a_named_timeout(
    session_maker, production_configuration, binding_key, ui_client, transports, monkeypatch
):
    """F1. A genuinely hung provider is bounded, and says so.

    The budget is squeezed for the test; what matters is that exceeding it produces
    a structured answer rather than an open-ended wait — the composition is UNKNOWN,
    not empty, nothing was written, and the operator can press again.
    """
    run_id, _recipient_ids, reader = await _preview(session_maker, count=2)

    class HangingReader(type(reader)):  # type: ignore[misc]
        async def list_customers(self, *, params):
            await asyncio.sleep(30)
            return await super().list_customers(params=params)

    transports.use(reader=HangingReader(count=2))
    monkeypatch.setattr(dispatch_module, "COMPOSITION_FIXED_BUDGET_SECONDS", 0)
    monkeypatch.setattr(dispatch_module, "COMPOSITION_PER_RECIPIENT_BUDGET_SECONDS", 0)
    monkeypatch.setattr(dispatch_module, "COMPOSITION_READ_BUDGET_CEILING_SECONDS", 1)

    status, body = await _check(ui_client, run_id)
    assert status == 200
    assert body["composition_known"] is False
    assert body["composition_proven"] is False
    assert "voucher_production_composition_read_timeout" in body["blockers"]
    # No audience is claimed either way, and no batch was created by a read.
    assert body["recipients"] == []
    assert await ledger_module.load_for_preview(session_maker, campaign_run_id=run_id) is not None
    assert (await ledger_module.load_for_preview(session_maker, campaign_run_id=run_id)).exists is False
    # The rows are untouched: a timeout is not a quiet narrowing of the audience.
    assert await dispatch_module.active_recipient_count(session_maker, run_id) == 2


async def test_the_preparation_page_renders_one_waiting_policy_and_no_duplicates(
    session_maker, production_configuration, binding_key, ui_client, transports
):
    """The page's wait comes from the server's policy, and from nowhere else.

    It is the CEILING plus the transport margin, not this preview's own budget:
    a number derived from the audience would go stale the moment somebody edited
    the preview in another tab, and the page could not learn the new one without a
    reload. Asserted against the policy function rather than a literal, so the two
    cannot be changed apart.
    """
    count = 4
    run_id, _recipient_ids, reader = await _preview(session_maker, count=count)
    transports.use(reader=reader)

    page = await ui_client.get(f"/ops/voucher-mailings/prepare?preview_run_id={run_id}")
    assert page.status_code == 200
    expected = dispatch_module.composition_browser_wait_seconds()
    assert f"const COMPOSITION_BROWSER_WAIT_SECONDS = {expected};" in page.text
    # It covers every bound the read policy can produce, for any audience.
    assert expected > dispatch_module.composition_read_budget_seconds(10_000)
    # The constants the two earlier defects lived in are gone, not merely enlarged.
    assert "COMPOSITION_READ_TIMEOUT_MS = 20000" not in page.text
    assert "INITIAL_COMPOSITION_READ_BUDGET_SECONDS" not in page.text
    # And the margin is not a second, independent number in the page script.
    assert "COMPOSITION_READ_MARGIN_MS" not in page.text


# ===========================================================================
# F3 — an early refusal still names the person the operator curated
# ===========================================================================


async def test_an_early_opt_out_refusal_shows_the_name_from_the_preview(
    session_maker, production_configuration, binding_key, ui_client, transports
):
    """F3. The row refused before any live read, so the proof carries no name.

    It used to print «без имени (строка preview …)» and claim
    ``name_from_preview=true`` without having read the preview at all. The curated
    name is now read from the preview row — for display only — so the operator can
    tell which person to act on.
    """
    run_id, recipient_ids, reader = await _preview(session_maker, count=3)
    async with session_maker() as session, session.begin():
        recipient = await session.get(CampaignRecipient, recipient_ids[1])
        recipient.display_name = "Куратор Назвал Это Имя"
        client = await session.get(Client, recipient.client_id)
        client.wa_opted_out = True
    transports.use(reader=reader)

    status, body = await _check(ui_client, run_id)
    assert status == 200
    row = _row(body, recipient_ids[1])
    assert row["state"] == "refused"
    assert row["display_name"] == "Куратор Назвал Это Имя"
    assert row["name_from_preview"] is True
    assert "voucher_production_recipient_opted_out" in row["reasons"]
    assert row["campaign_recipient_id"] == recipient_ids[1]
    # The proven rows keep the name the identity proof established.
    for recipient_id in (recipient_ids[0], recipient_ids[2]):
        other = _row(body, recipient_id)
        assert other["name_from_preview"] is False
        assert other["display_name"] != "Куратор Назвал Это Имя"


async def test_several_early_refusals_do_not_swap_their_names(
    session_maker, production_configuration, binding_key, ui_client, transports
):
    """F3. One query for the fallbacks, and every row still gets its own."""
    run_id, recipient_ids, reader = await _preview(session_maker, count=3)
    names = {recipient_ids[0]: "Первая Строка", recipient_ids[1]: "Вторая Строка"}
    async with session_maker() as session, session.begin():
        for recipient_id, name in names.items():
            recipient = await session.get(CampaignRecipient, recipient_id)
            recipient.display_name = name
            client = await session.get(Client, recipient.client_id)
            client.wa_opted_out = True
    transports.use(reader=reader)

    status, body = await _check(ui_client, run_id)
    assert status == 200
    assert body["refused_count"] == 2
    for recipient_id, name in names.items():
        row = _row(body, recipient_id)
        assert row["display_name"] == name, row
        assert row["name_from_preview"] is True


async def test_a_row_with_no_name_anywhere_gets_an_honest_placeholder(
    session_maker, production_configuration, binding_key, ui_client, transports
):
    """F3. The placeholder stays for the case it was written for, and says so.

    ``name_from_preview`` is false then: the shown text came from neither the proof
    nor the preview, and claiming the preview supplied it would be a claim about
    data nobody has.
    """
    run_id, recipient_ids, reader = await _preview(session_maker, count=1)
    async with session_maker() as session, session.begin():
        recipient = await session.get(CampaignRecipient, recipient_ids[0])
        recipient.display_name = "   "
        client = await session.get(Client, recipient.client_id)
        client.wa_opted_out = True
    transports.use(reader=reader)

    status, body = await _check(ui_client, run_id)
    assert status == 200
    row = _row(body, recipient_ids[0])
    assert row["state"] == "refused"
    assert row["display_name"] == f"без имени (строка preview {recipient_ids[0]})"
    assert row["name_from_preview"] is False


async def test_a_fallback_name_never_comes_from_another_preview(
    session_maker, production_configuration, binding_key, ui_client, transports
):
    """F3. The fallback query is scoped to this preview and this provider.

    A row id that belongs to another run must not lend its name to a row here —
    showing the wrong person's name is worse than the placeholder it replaces.
    """
    run_id, recipient_ids, reader = await _preview(session_maker, count=1)
    other_run, other_ids = await old.seed_production_preview(session_maker, count=1, offset=6)
    async with session_maker() as session, session.begin():
        stranger = await session.get(CampaignRecipient, other_ids[0])
        stranger.display_name = "Чужая Кампания"
        recipient = await session.get(CampaignRecipient, recipient_ids[0])
        recipient.display_name = ""
        client = await session.get(Client, recipient.client_id)
        client.wa_opted_out = True
    transports.use(reader=reader)

    status, body = await _check(ui_client, run_id)
    assert status == 200
    assert other_run != run_id
    row = _row(body, recipient_ids[0])
    assert row["display_name"] == f"без имени (строка preview {recipient_ids[0]})"
    assert "Чужая Кампания" not in json.dumps(body, ensure_ascii=False)


async def test_a_hostile_preview_name_is_carried_as_data_in_the_fallback(
    session_maker, production_configuration, binding_key, ui_client, transports
):
    """F3. The fallback is a name like any other: data, escaped by the page."""
    hostile = '<script>alert("preview")</script>'
    run_id, recipient_ids, reader = await _preview(session_maker, count=1)
    async with session_maker() as session, session.begin():
        recipient = await session.get(CampaignRecipient, recipient_ids[0])
        recipient.display_name = hostile
        client = await session.get(Client, recipient.client_id)
        client.wa_opted_out = True
    transports.use(reader=reader)

    status, body = await _check(ui_client, run_id)
    assert status == 200
    assert _row(body, recipient_ids[0])["display_name"] == hostile
    page = await ui_client.get(f"/ops/voucher-mailings/prepare?preview_run_id={run_id}")
    assert hostile not in page.text


async def test_the_fallback_name_stays_out_of_every_safe_report(
    session_maker, production_configuration, binding_key, ui_client, transports
):
    """F3. Display only: the preview name reaches the screen and nothing else."""
    from altegio_bot.models.models import EasyWeekVoucherProductionAudit

    curated = "Только Для Экрана"
    run_id, recipient_ids, reader = await _preview(session_maker, count=2)
    async with session_maker() as session, session.begin():
        recipient = await session.get(CampaignRecipient, recipient_ids[0])
        recipient.display_name = curated
        client = await session.get(Client, recipient.client_id)
        client.wa_opted_out = True
    transports.use(reader=reader)

    _status, body = await _check(ui_client, run_id)
    assert _row(body, recipient_ids[0])["display_name"] == curated

    status, _outcome = await _exclude(ui_client, run_id, recipient_ids[0])
    assert status == 200
    async with session_maker() as session:
        rows = list((await session.scalars(select(EasyWeekVoucherProductionAudit))).all())
    assert curated not in json.dumps([row.detail for row in rows], default=str)
