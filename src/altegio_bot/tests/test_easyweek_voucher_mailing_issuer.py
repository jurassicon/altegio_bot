"""One approved issuer for every production voucher (§43.9, §43.7 group C).

The owner's rule: every new production voucher is sold by ONE EasyWeek staffer,
whoever the client is, whoever served them, and whoever is logged into Ops. The
risk it closes is specific — eight people work at the Karlsruhe branch, and seven
of them have a UUID that passes every other check in this phase. A configured
value that merely *parses* is not an answer.

So these tests ask: does anything other than the approved identity reach a CREATE,
and does any failure of that identity cost more than zero external calls?

No real staffer UUID appears here. The synthetic issuer is pinned through the
single narrow seam ``expected_issuer_fingerprint``, which cannot be made to accept
an arbitrary value: the comparison stays exact, it is only told which fingerprint
is approved in this synthetic world. There is no bypass flag and no environment
variable, and nothing in this file introduces one.
"""

from __future__ import annotations

from typing import Any

import pytest

from altegio_bot.campaigns.easyweek_voucher_production import issuer as issuer_module
from altegio_bot.campaigns.easyweek_voucher_production import ledger as ledger_module
from altegio_bot.campaigns.easyweek_voucher_production import operations as operations_module
from altegio_bot.easyweek_client import EasyWeekRetryableError
from altegio_bot.easyweek_voucher_identity import KARLSRUHE_LOCATION_UUID
from altegio_bot.easyweek_voucher_mutation import VoucherMutationResponse
from altegio_bot.settings import settings
from altegio_bot.tests.easyweek_voucher_10eur_fixtures import (
    FakeReader,
    marker_orders,
    seed_template_and_sender,
)
from altegio_bot.tests.easyweek_voucher_production_fixtures import (
    ORDER_UUIDS,
    OTHER_STAFFER_UUID,
    STAFFER_UUID,
    FakeMutator,
    seed_production_preview,
    staffers_page,
)
from altegio_bot.workers import easyweek_voucher_production_worker as worker_module

PLAN_URL = "/ops/voucher-mailings/api/plan"
CONFIRM_URL = "/ops/voucher-mailings/api/confirm"


def _ok(index: int) -> VoucherMutationResponse:
    return VoucherMutationResponse(http_status=200, envelope={"uuid": ORDER_UUIDS[index]})


# ===========================================================================
# The pin itself
# ===========================================================================


def test_the_expected_fingerprint_is_the_documented_derivation():
    """The constant is SHA-256 over the domain string and the canonical UUID.

    Checked against an independently written derivation rather than against itself,
    so a change to either half is a failing test rather than a silent re-pin.
    """
    import hashlib
    import uuid as uuid_module

    # Obviously fabricated, and deliberately sharing no digits with the real
    # staffer UUID — not even its prefix. A probe that looked like the
    # production value would put part of it in the repository forever.
    probe = "0a0a0a0a-1b1b-4c4c-8d8d-2e2e2e2e2e2e"
    expected = hashlib.sha256(
        ("easyweek-voucher-production-issuer-v1:" + str(uuid_module.UUID(probe))).encode("utf-8")
    ).hexdigest()
    assert issuer_module.issuer_fingerprint(probe) == expected
    # And the production constant is a full SHA-256, not a truncation.
    assert len(issuer_module.APPROVED_ISSUER_FINGERPRINT) == 64
    assert issuer_module.expected_issuer_fingerprint() == issuer_module.APPROVED_ISSUER_FINGERPRINT


def test_a_cosmetic_rewrite_of_the_configured_uuid_is_not_a_drift():
    """Upper case, braces and a hyphenless form are the SAME identity.

    Canonicalised through ``uuid.UUID`` first, so an administrator who retypes the
    setting in a different style has not changed the issuer — and a report does not
    claim a drift that did not happen.
    """
    canonical = issuer_module.issuer_fingerprint(STAFFER_UUID)
    assert canonical is not None
    for spelling in (
        STAFFER_UUID.upper(),
        "{" + STAFFER_UUID + "}",
        STAFFER_UUID.replace("-", ""),
        f"  {STAFFER_UUID}  ",
    ):
        assert issuer_module.issuer_fingerprint(spelling) == canonical, spelling


@pytest.mark.parametrize(
    ("configured", "expected_reason"),
    [
        ("", "voucher_production_staffer_unconfigured"),
        ("   ", "voucher_production_staffer_unconfigured"),
        ("not-a-uuid", "voucher_production_issuer_not_approved"),
        (OTHER_STAFFER_UUID, "voucher_production_issuer_not_approved"),
    ],
)
def test_only_the_approved_identity_is_pinned(monkeypatch, configured: str, expected_reason: str):
    """Empty, malformed and "a valid UUID of somebody else" are three refusals.

    The third is the one that matters: it is a real employee of the real branch,
    and every other check in this phase would pass for them.
    """
    from altegio_bot.tests.easyweek_voucher_production_fixtures import pin_synthetic_issuer

    pin_synthetic_issuer(monkeypatch)
    pinned = issuer_module.pinned_issuer(configured)
    assert pinned.pinned is False
    assert pinned.reason == expected_reason
    assert pinned.uuid is None
    # And no report built from it ever carries a UUID.
    safe = pinned.as_safe_dict()
    assert OTHER_STAFFER_UUID not in str(safe)
    assert safe["issuer_selectable_by_operator"] is False


def test_the_pinned_issuer_reports_booleans_and_never_a_uuid(monkeypatch):
    from altegio_bot.tests.easyweek_voucher_production_fixtures import pin_synthetic_issuer

    pin_synthetic_issuer(monkeypatch)
    pinned = issuer_module.pinned_issuer(STAFFER_UUID)
    assert pinned.pinned is True
    safe = pinned.as_safe_dict()
    assert safe["issuer_pinned"] is True
    assert STAFFER_UUID not in str(safe)
    # The fingerprint is not secret, but it is not printed either: the boolean is
    # sufficient precisely because the pin admits exactly one value.
    assert issuer_module.APPROVED_ISSUER_FINGERPRINT not in str(safe)


# ===========================================================================
# Live membership of the location
# ===========================================================================


class _StafferReader:
    def __init__(self, pages: list[dict[str, Any]] | Exception) -> None:
        self.pages = pages
        self.calls: list[int] = []

    async def list_location_staffers(self, location_uuid: str, *, page: int) -> dict[str, Any]:
        self.calls.append(page)
        if isinstance(self.pages, Exception):
            raise self.pages
        if page > len(self.pages):
            raise AssertionError(f"the walk asked for page {page} beyond the fixture")
        return self.pages[page - 1]


async def test_membership_is_proven_from_a_complete_walk():
    reader = _StafferReader([staffers_page([OTHER_STAFFER_UUID, STAFFER_UUID])])
    membership = await issuer_module.prove_issuer_membership(
        reader, location_uuid=KARLSRUHE_LOCATION_UUID, issuer_uuid=STAFFER_UUID
    )
    assert membership.proven is True
    assert membership.pagination_complete is True
    assert membership.matches == 1


async def test_membership_is_proven_across_pages():
    """The approved issuer on page two is still a member."""
    reader = _StafferReader(
        [
            staffers_page([OTHER_STAFFER_UUID], current=1, last=2),
            staffers_page([STAFFER_UUID], current=2, last=2),
        ]
    )
    membership = await issuer_module.prove_issuer_membership(
        reader, location_uuid=KARLSRUHE_LOCATION_UUID, issuer_uuid=STAFFER_UUID
    )
    assert membership.proven is True
    assert reader.calls == [1, 2]


async def test_a_complete_walk_without_the_issuer_is_missing_not_unknown():
    reader = _StafferReader([staffers_page([OTHER_STAFFER_UUID])])
    membership = await issuer_module.prove_issuer_membership(
        reader, location_uuid=KARLSRUHE_LOCATION_UUID, issuer_uuid=STAFFER_UUID
    )
    assert membership.proven is False
    assert membership.reason == "voucher_production_issuer_membership_missing"
    assert membership.pagination_complete is True


async def test_two_rows_for_one_uuid_are_ambiguous_not_a_first_match():
    reader = _StafferReader([staffers_page([STAFFER_UUID, STAFFER_UUID])])
    membership = await issuer_module.prove_issuer_membership(
        reader, location_uuid=KARLSRUHE_LOCATION_UUID, issuer_uuid=STAFFER_UUID
    )
    assert membership.proven is False
    assert membership.reason == "voucher_production_issuer_membership_ambiguous"
    assert membership.matches == 2


@pytest.mark.parametrize(
    "pages",
    [
        # No pagination metadata at all.
        [{"data": [{"uuid": STAFFER_UUID}]}],
        # A server answering page 2 with page 1: a walk that counted it would
        # "finish" without ever reaching the end.
        [{"data": [], "meta": {"current_page": 2, "last_page": 3, "per_page": 100}}],
        # The page size is not the one that was requested.
        [{"data": [{"uuid": STAFFER_UUID}], "meta": {"current_page": 1, "last_page": 1, "per_page": 25}}],
    ],
)
async def test_an_unprovable_walk_is_incomplete_and_never_absence(pages: list[dict[str, Any]]):
    """Unknown is not "missing". Treating it as absence would block a correct mailing."""
    reader = _StafferReader(pages)
    membership = await issuer_module.prove_issuer_membership(
        reader, location_uuid=KARLSRUHE_LOCATION_UUID, issuer_uuid=STAFFER_UUID
    )
    assert membership.proven is False
    assert membership.reason == "voucher_production_issuer_membership_incomplete"


async def test_a_failing_catalogue_is_incomplete_not_missing():
    reader = _StafferReader(EasyWeekRetryableError("503", status_code=503))
    membership = await issuer_module.prove_issuer_membership(
        reader, location_uuid=KARLSRUHE_LOCATION_UUID, issuer_uuid=STAFFER_UUID
    )
    assert membership.proven is False
    assert membership.reason == "voucher_production_issuer_membership_incomplete"


async def test_membership_never_matches_by_name_or_by_position():
    """The UUID goes in and an answer about that UUID comes out. Nothing else.

    A catalogue whose first entry carries the right NAME and the wrong UUID must
    not satisfy the proof — the name is for finding a candidate, never for runtime.
    """
    reader = _StafferReader(
        [
            {
                "data": [
                    {"uuid": OTHER_STAFFER_UUID, "first_name": "Julia", "last_name": "Müller"},
                ],
                "meta": {"current_page": 1, "last_page": 1, "per_page": 100},
            }
        ]
    )
    membership = await issuer_module.prove_issuer_membership(
        reader, location_uuid=KARLSRUHE_LOCATION_UUID, issuer_uuid=STAFFER_UUID
    )
    assert membership.proven is False
    assert membership.reason == "voucher_production_issuer_membership_missing"


# ===========================================================================
# Through the UI: the pin decides whether a CREATE may happen at all
# ===========================================================================


async def _frozen(client, session_maker, transports, *, count: int, offset: int = 0) -> tuple[int, int, FakeReader]:
    run_id, _ = await seed_production_preview(session_maker, count=count, offset=offset)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(indices=list(range(offset, offset + count)))
    transports.use(reader=reader)
    offer = (
        await client.post(
            PLAN_URL,
            json={
                "stage": "freeze",
                "preview_run_id": run_id,
                "expected_recipient_count": count,
                "approved_exposure_minor": count * 1000,
            },
        )
    ).json()
    assert offer["ready"], offer["reasons"]
    await client.post(
        CONFIRM_URL,
        json={
            "approval_id": offer["approval"]["approval_id"],
            "confirmed_count": offer["targets"]["stage_target_count"],
            "confirmed_amount_minor": offer["targets"]["stage_amount_minor"],
        },
    )
    assert await worker_module.run_once(session_maker, owner="test-executor") is not None
    snapshot = await ledger_module.load_for_preview(session_maker, campaign_run_id=run_id)
    batch_id = int(snapshot.batch_id or 0)
    reader.orders.update(await marker_orders(session_maker, batch_id=batch_id, offset=offset))
    return run_id, batch_id, reader


async def test_every_client_and_every_batch_is_issued_by_the_one_approved_staffer(
    session_maker, production_configuration, binding_key, executor_enabled, ui_client, transports
):
    """Three clients in one mailing and a second mailing: one issuer throughout."""
    count = 3
    run_a, batch_a, reader_a = await _frozen(ui_client, session_maker, transports, count=count)
    mutator_a = FakeMutator(create_sequence=[_ok(i) for i in range(count)])
    transports.use(reader=reader_a, mutator=mutator_a)
    offer = (
        await ui_client.post(PLAN_URL, json={"stage": "create", "preview_run_id": run_a, "batch_id": batch_a})
    ).json()
    assert offer["ready"], offer["reasons"]
    await ui_client.post(
        CONFIRM_URL,
        json={
            "approval_id": offer["approval"]["approval_id"],
            "confirmed_count": offer["targets"]["stage_target_count"],
            "confirmed_amount_minor": offer["targets"]["stage_amount_minor"],
        },
    )
    assert await worker_module.run_once(session_maker, owner="test-executor") is not None

    # A second, independent mailing with its own people.
    run_b, batch_b, reader_b = await _frozen(ui_client, session_maker, transports, count=2, offset=10)
    mutator_b = FakeMutator(create_sequence=[_ok(10), _ok(11)])
    transports.use(reader=reader_b, mutator=mutator_b)
    offer_b = (
        await ui_client.post(PLAN_URL, json={"stage": "create", "preview_run_id": run_b, "batch_id": batch_b})
    ).json()
    assert offer_b["ready"], offer_b["reasons"]
    await ui_client.post(
        CONFIRM_URL,
        json={
            "approval_id": offer_b["approval"]["approval_id"],
            "confirmed_count": offer_b["targets"]["stage_target_count"],
            "confirmed_amount_minor": offer_b["targets"]["stage_amount_minor"],
        },
    )
    assert await worker_module.run_once(session_maker, owner="test-executor") is not None

    staffers = {call["staffer_uuid"] for call in (*mutator_a.create_calls, *mutator_b.create_calls)}
    assert staffers == {STAFFER_UUID}, staffers
    assert len(mutator_a.create_calls) == count
    assert len(mutator_b.create_calls) == 2
    # Every customer is different; the issuer is not.
    customers = {call["customer_uuid"] for call in (*mutator_a.create_calls, *mutator_b.create_calls)}
    assert len(customers) == count + 2


async def test_the_operator_account_does_not_change_who_issues(
    session_maker, production_configuration, binding_key, executor_enabled, ui_client, transports, monkeypatch
):
    """A different Ops operator confirming changes the audit, never the issuer."""
    from altegio_bot.ops.auth import SESSION_COOKIE, make_session_token
    from altegio_bot.tests.easyweek_voucher_mailing_ui_fixtures import OPS_SECRET, OTHER_OPS_USER, csrf_for

    count = 1
    run_id, batch_id, reader = await _frozen(ui_client, session_maker, transports, count=count)

    monkeypatch.setattr(settings, "ops_user", OTHER_OPS_USER, raising=False)
    cookie = make_session_token(OTHER_OPS_USER, OPS_SECRET)
    ui_client.cookies.set(SESSION_COOKIE, cookie)
    ui_client.headers.update({"X-Ops-CSRF": csrf_for(cookie)})

    mutator = FakeMutator(create_sequence=[_ok(0)])
    transports.use(reader=reader, mutator=mutator)
    offer = (
        await ui_client.post(PLAN_URL, json={"stage": "create", "preview_run_id": run_id, "batch_id": batch_id})
    ).json()
    assert offer["ready"], offer["reasons"]
    await ui_client.post(
        CONFIRM_URL,
        json={
            "approval_id": offer["approval"]["approval_id"],
            "confirmed_count": offer["targets"]["stage_target_count"],
            "confirmed_amount_minor": offer["targets"]["stage_amount_minor"],
        },
    )
    assert await worker_module.run_once(session_maker, owner="test-executor") is not None

    assert [call["staffer_uuid"] for call in mutator.create_calls] == [STAFFER_UUID]
    # The audit, separately, records WHO authorised it.
    approval = await operations_module.load_approval(session_maker, approval_id=offer["approval"]["approval_id"])
    assert approval is not None and approval.principal == OTHER_OPS_USER


@pytest.mark.parametrize(
    ("configured", "reason"),
    [
        ("", "voucher_production_staffer_unconfigured"),
        ("not-a-uuid", "voucher_production_issuer_not_approved"),
        (OTHER_STAFFER_UUID, "voucher_production_issuer_not_approved"),
    ],
)
async def test_a_wrong_issuer_configuration_gives_zero_creates(
    session_maker,
    production_configuration,
    binding_key,
    executor_enabled,
    ui_client,
    transports,
    monkeypatch,
    configured: str,
    reason: str,
):
    """Empty, invalid, or another valid staffer: all three refuse before any POST."""
    count = 1
    run_id, batch_id, reader = await _frozen(ui_client, session_maker, transports, count=count)

    monkeypatch.setattr(settings, "easyweek_voucher_production_mailing_staffer_uuid", configured, raising=False)
    mutator = FakeMutator(create_sequence=[_ok(0)])
    transports.use(reader=reader, mutator=mutator)
    offer = (
        await ui_client.post(PLAN_URL, json={"stage": "create", "preview_run_id": run_id, "batch_id": batch_id})
    ).json()

    assert offer["ready"] is False
    assert reason in offer["reasons"]
    # No approval, so nothing to confirm, so no POST.
    assert mutator.calls == []
    creates = [entry for entry in await operations_module.list_operations(session_maker) if entry.stage == "create"]
    assert creates == []


async def test_an_issuer_who_left_the_branch_blocks_new_creates(
    session_maker, production_configuration, binding_key, executor_enabled, ui_client, transports
):
    """Being the approved identity and still working here are two different facts."""
    count = 1
    run_id, batch_id, reader = await _frozen(ui_client, session_maker, transports, count=count)

    # The catalogue no longer lists them.
    reader.staffer_pages = [staffers_page([OTHER_STAFFER_UUID])]
    mutator = FakeMutator(create_sequence=[_ok(0)])
    transports.use(reader=reader, mutator=mutator)
    offer = (
        await ui_client.post(PLAN_URL, json={"stage": "create", "preview_run_id": run_id, "batch_id": batch_id})
    ).json()
    assert offer["ready"] is False
    assert "voucher_production_issuer_membership_missing" in offer["reasons"]
    assert mutator.calls == []


async def test_an_issuer_drift_after_the_confirmation_blocks_execution(
    session_maker, production_configuration, binding_key, executor_enabled, ui_client, transports, monkeypatch
):
    """The configuration changes between the confirmation and the stage. Zero POSTs.

    The plan signed the verdict "the configured staffer is the approved one", which
    the executor re-derives live. A deploy that repointed the setting in between
    cannot be executed under the old approval.
    """
    count = 2
    run_id, batch_id, reader = await _frozen(ui_client, session_maker, transports, count=count)
    mutator = FakeMutator(create_sequence=[_ok(0), _ok(1)])
    transports.use(reader=reader, mutator=mutator)
    offer = (
        await ui_client.post(PLAN_URL, json={"stage": "create", "preview_run_id": run_id, "batch_id": batch_id})
    ).json()
    assert offer["ready"], offer["reasons"]
    confirmed = await ui_client.post(
        CONFIRM_URL,
        json={
            "approval_id": offer["approval"]["approval_id"],
            "confirmed_count": offer["targets"]["stage_target_count"],
            "confirmed_amount_minor": offer["targets"]["stage_amount_minor"],
        },
    )
    assert confirmed.status_code == 200

    # Somebody repoints the server's staffer setting at another employee.
    monkeypatch.setattr(settings, "easyweek_voucher_production_mailing_staffer_uuid", OTHER_STAFFER_UUID, raising=False)

    finished = await worker_module.run_once(session_maker, owner="test-executor")
    assert finished is not None
    assert finished.status == "refused"
    assert mutator.calls == []
    snapshot = await ledger_module.load(session_maker, batch_id=batch_id)
    assert all(entry.status == "planned" for entry in snapshot.items)


async def test_the_catalogue_is_walked_once_per_stage_not_once_per_recipient(
    session_maker, production_configuration, binding_key, executor_enabled, ui_client, transports
):
    """A mailing of twelve does one catalogue walk, which keeps the stage linear."""
    count = 12
    run_id, batch_id, reader = await _frozen(ui_client, session_maker, transports, count=count)
    reader.staffer_calls.clear()

    transports.use(reader=reader)
    offer = (
        await ui_client.post(PLAN_URL, json={"stage": "create", "preview_run_id": run_id, "batch_id": batch_id})
    ).json()
    assert offer["ready"], offer["reasons"]
    # One page, read once — not twelve times, and not once per slot.
    assert reader.staffer_calls == [(KARLSRUHE_LOCATION_UUID, 1)], reader.staffer_calls


async def test_the_historical_staffer_binding_of_a_frozen_batch_is_not_rewritten(
    session_maker, production_configuration, binding_key, executor_enabled, ui_client, transports, monkeypatch
):
    """A frozen batch keeps the staffer it was frozen with, whatever changes later."""
    count = 1
    _run_id, batch_id, _reader = await _frozen(ui_client, session_maker, transports, count=count)
    before = await ledger_module.load(session_maker, batch_id=batch_id)
    assert before.staffer_uuid == STAFFER_UUID

    monkeypatch.setattr(settings, "easyweek_voucher_production_mailing_staffer_uuid", OTHER_STAFFER_UUID, raising=False)
    after = await ledger_module.load(session_maker, batch_id=batch_id)
    assert after.staffer_uuid == STAFFER_UUID


async def test_an_allowed_refund_does_not_depend_on_the_current_issuer(
    session_maker, production_configuration, binding_key, executor_enabled, ui_client, transports, monkeypatch
):
    """§43.9: the issuer rule must not strand a pre-send refund.

    A paid slot with nothing ever sent for it must still be refundable after the
    staffer setting is emptied, corrected, or pointed at somebody new. The money
    should come back either way — the frozen order, the payment account, the fence,
    the binding and the absence of a send claim are what protect it, and none of
    them moved.
    """
    count = 1
    run_id, batch_id, reader = await _frozen(ui_client, session_maker, transports, count=count)

    # Create and pay, with the issuer correctly configured.
    transports.use(reader=reader, mutator=FakeMutator(create_sequence=[_ok(0)]))
    offer = (
        await ui_client.post(PLAN_URL, json={"stage": "create", "preview_run_id": run_id, "batch_id": batch_id})
    ).json()
    await ui_client.post(
        CONFIRM_URL,
        json={
            "approval_id": offer["approval"]["approval_id"],
            "confirmed_count": offer["targets"]["stage_target_count"],
            "confirmed_amount_minor": offer["targets"]["stage_amount_minor"],
        },
    )
    assert await worker_module.run_once(session_maker, owner="test-executor") is not None
    settles = await marker_orders(session_maker, batch_id=batch_id, status="paid")
    transports.use(reader=reader, mutator=FakeMutator(pay_sequence=[_ok(0)], reader=reader, settles=settles))
    offer = (
        await ui_client.post(PLAN_URL, json={"stage": "pay", "preview_run_id": run_id, "batch_id": batch_id})
    ).json()
    assert offer["ready"], offer["reasons"]
    await ui_client.post(
        CONFIRM_URL,
        json={
            "approval_id": offer["approval"]["approval_id"],
            "confirmed_count": offer["targets"]["stage_target_count"],
            "confirmed_amount_minor": offer["targets"]["stage_amount_minor"],
        },
    )
    assert await worker_module.run_once(session_maker, owner="test-executor") is not None

    # Now the staffer configuration disappears entirely.
    monkeypatch.setattr(settings, "easyweek_voucher_production_mailing_staffer_uuid", "", raising=False)

    # A CREATE would be refused — correctly.
    transports.use(reader=reader)
    blocked = (
        await ui_client.post(PLAN_URL, json={"stage": "create", "preview_run_id": run_id, "batch_id": batch_id})
    ).json()
    assert blocked["ready"] is False

    # The refund is not.
    refunded = await marker_orders(session_maker, batch_id=batch_id, status="refunded")
    mutator = FakeMutator(refund=_ok(0), reader=reader, settles=refunded)
    transports.use(reader=reader, mutator=mutator)
    refund_offer = (
        await ui_client.post(
            PLAN_URL, json={"stage": "refund", "preview_run_id": run_id, "batch_id": batch_id, "slot": 1}
        )
    ).json()
    assert refund_offer["ready"] is True, refund_offer["reasons"]
    await ui_client.post(
        CONFIRM_URL,
        json={
            "approval_id": refund_offer["approval"]["approval_id"],
            "confirmed_count": refund_offer["targets"]["stage_target_count"],
            "confirmed_amount_minor": refund_offer["targets"]["stage_amount_minor"],
        },
    )
    finished = await worker_module.run_once(session_maker, owner="test-executor")
    assert finished is not None and finished.status == "completed", finished.result
    assert mutator.calls.count("refund") == 1
    snapshot = await ledger_module.load(session_maker, batch_id=batch_id)
    assert snapshot.item(1).status == "refunded"
    # And the batch still records the staffer it was frozen with.
    assert snapshot.staffer_uuid == STAFFER_UUID


async def test_status_stays_available_when_the_issuer_pin_fails(
    session_maker, production_configuration, binding_key, executor_enabled, ui_client, transports, monkeypatch
):
    """An identity problem must not take the state away from the operator."""
    _run_id, batch_id, _reader = await _frozen(ui_client, session_maker, transports, count=1)
    monkeypatch.setattr(settings, "easyweek_voucher_production_mailing_staffer_uuid", "", raising=False)

    page = await ui_client.get(f"/ops/voucher-mailings/{batch_id}")
    assert page.status_code == 200
    state = (await ui_client.get(f"/ops/voucher-mailings/api/status?batch_id={batch_id}")).json()
    assert state["batch"]["exists"] is True
    # And the readiness panel says which fact is missing.
    assert "voucher_production_staffer_unconfigured" in page.text


async def test_a_page_render_does_not_claim_an_unchecked_membership(
    session_maker, production_configuration, binding_key, executor_enabled, ui_client, transports
):
    """The readiness panel must not report a blocker nobody checked.

    Proving membership costs a live EasyWeek walk, which a page render must not
    make on every load. So the panel reports the PIN — a local comparison — and
    says the membership is verified when a step is prepared. A panel that printed
    `issuer_membership_incomplete` on every page would train an operator to ignore
    it, which is worse than not showing it.
    """
    _run_id, batch_id, reader = await _frozen(ui_client, session_maker, transports, count=1)
    reader.staffer_calls.clear()

    page = await ui_client.get(f"/ops/voucher-mailings/{batch_id}")
    assert page.status_code == 200
    assert "voucher_production_issuer_membership_incomplete" not in page.text
    assert "voucher_production_issuer_not_approved" not in page.text
    # And it did not reach EasyWeek to find that out.
    assert reader.staffer_calls == []

    state = (await ui_client.get(f"/ops/voucher-mailings/api/status?batch_id={batch_id}")).json()
    assert state["readiness"]["issuer_membership_checked_here"] is False
    assert state["readiness"]["issuer_pinned"] is True


async def test_preparing_a_step_still_requires_a_proven_membership(
    session_maker, production_configuration, binding_key, executor_enabled, ui_client, transports
):
    """What the page defers, the plan insists on."""
    run_id, batch_id, reader = await _frozen(ui_client, session_maker, transports, count=1)
    # A catalogue that cannot be proven complete.
    reader.staffer_pages = [{"data": [{"uuid": STAFFER_UUID}]}]
    transports.use(reader=reader)
    offer = (
        await ui_client.post(PLAN_URL, json={"stage": "create", "preview_run_id": run_id, "batch_id": batch_id})
    ).json()
    assert offer["ready"] is False
    assert "voucher_production_issuer_membership_incomplete" in offer["reasons"]
    assert reader.staffer_calls, "the plan did not even try to prove membership"


async def test_a_staffer_change_after_a_refund_plan_still_lets_the_money_come_back(
    session_maker, production_configuration, binding_key, executor_enabled, ui_client, transports, monkeypatch
):
    """The drift case §43.9 forbids stranding: plan a refund, THEN lose the issuer.

    Distinct from ``test_an_allowed_refund_does_not_depend_on_the_current_issuer``,
    which empties the setting before planning. Here the plan is taken while the
    configuration is correct and the setting changes before the confirmation — the
    moment a digest over the issuer verdict would turn a returnable €15 into an
    unreturnable one, for a reason that has nothing to do with returning it.
    """
    count = 1
    run_id, batch_id, reader = await _frozen(ui_client, session_maker, transports, count=count)

    transports.use(reader=reader, mutator=FakeMutator(create_sequence=[_ok(0)]))
    offer = (
        await ui_client.post(PLAN_URL, json={"stage": "create", "preview_run_id": run_id, "batch_id": batch_id})
    ).json()
    await ui_client.post(
        CONFIRM_URL,
        json={
            "approval_id": offer["approval"]["approval_id"],
            "confirmed_count": offer["targets"]["stage_target_count"],
            "confirmed_amount_minor": offer["targets"]["stage_amount_minor"],
        },
    )
    assert await worker_module.run_once(session_maker, owner="test-executor") is not None
    settles = await marker_orders(session_maker, batch_id=batch_id, status="paid")
    transports.use(reader=reader, mutator=FakeMutator(pay_sequence=[_ok(0)], reader=reader, settles=settles))
    offer = (
        await ui_client.post(PLAN_URL, json={"stage": "pay", "preview_run_id": run_id, "batch_id": batch_id})
    ).json()
    await ui_client.post(
        CONFIRM_URL,
        json={
            "approval_id": offer["approval"]["approval_id"],
            "confirmed_count": offer["targets"]["stage_target_count"],
            "confirmed_amount_minor": offer["targets"]["stage_amount_minor"],
        },
    )
    assert await worker_module.run_once(session_maker, owner="test-executor") is not None

    # The refund is planned while everything is configured correctly.
    refunded = await marker_orders(session_maker, batch_id=batch_id, status="refunded")
    mutator = FakeMutator(refund=_ok(0), reader=reader, settles=refunded)
    transports.use(reader=reader, mutator=mutator)
    refund_offer = (
        await ui_client.post(
            PLAN_URL, json={"stage": "refund", "preview_run_id": run_id, "batch_id": batch_id, "slot": 1}
        )
    ).json()
    assert refund_offer["ready"] is True, refund_offer["reasons"]

    # The mechanism, pinned rather than left to be inferred from the outcome: the
    # refund's SIGNED snapshot carries no issuer verdict, so the staffer setting is
    # not one of the facts its digest depends on. A create's does.
    refund_snapshot = refund_offer["plan"]["snapshot"]
    assert refund_snapshot["issuer_check_applied"] is False
    assert "issuer_pinned" not in refund_snapshot
    assert "issuer_membership_proven" not in refund_snapshot
    create_snapshot = (
        await ui_client.post(PLAN_URL, json={"stage": "create", "preview_run_id": run_id, "batch_id": batch_id})
    ).json()["plan"]["snapshot"]
    assert create_snapshot["issuer_check_applied"] is True
    assert create_snapshot["issuer_pinned"] is True

    # ONLY NOW does the staffer setting change, between the plan and the confirmation.
    monkeypatch.setattr(settings, "easyweek_voucher_production_mailing_staffer_uuid", OTHER_STAFFER_UUID, raising=False)

    await ui_client.post(
        CONFIRM_URL,
        json={
            "approval_id": refund_offer["approval"]["approval_id"],
            "confirmed_count": refund_offer["targets"]["stage_target_count"],
            "confirmed_amount_minor": refund_offer["targets"]["stage_amount_minor"],
        },
    )
    finished = await worker_module.run_once(session_maker, owner="test-executor")
    assert finished is not None and finished.status == "completed", finished.result
    assert mutator.calls.count("refund") == 1
    snapshot = await ledger_module.load(session_maker, batch_id=batch_id)
    assert snapshot.item(1).status == "refunded"
    # And the batch still records the staffer it was frozen with: nothing was
    # re-attributed by any of this.
    assert snapshot.staffer_uuid == STAFFER_UUID
