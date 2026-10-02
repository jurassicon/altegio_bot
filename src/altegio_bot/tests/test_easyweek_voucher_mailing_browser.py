"""Browser-level acceptance of the operator UI (review R8, §43.7).

Chromium, against the real pages, over real HTTP, with a real session cookie. Every
step here is the operator's: a click on the button they would click, a value typed
into the field they would type it into, a wait for the page to repaint, and
navigation followed where the page navigates. No step is replaced by an API POST, and
no screen is opened by a URL assembled from a database read — which is the difference
between proving the interface works and proving the contracts behind it do.

EasyWeek and Meta are faked at the dispatch seam. The executor is the real one and is
driven between operator actions, which is how it behaves in the deployment: the
browser confirms, the worker picks the operation up, the page notices.

Required, not skippable: ``ALTEGIO_REQUIRE_BROWSER_TESTS=1`` makes a missing browser
a failure. See ``easyweek_voucher_mailing_browser_fixtures``.
"""

from __future__ import annotations

import asyncio
import re

from altegio_bot.campaigns.easyweek_voucher_production import ledger as ledger_module
from altegio_bot.campaigns.easyweek_voucher_production import operations as operations_module
from altegio_bot.easyweek_voucher_mutation import VoucherMutationResponse
from altegio_bot.tests.easyweek_voucher_mailing_browser_fixtures import assert_no_page_errors
from altegio_bot.tests.easyweek_voucher_production_fixtures import (
    ORDER_UUIDS,
    VOUCHER_CODE_SENTINELS,
    FakeMutator,
    FakeReader,
    FakeSender,
    marker_orders,
    seed_production_preview,
    seed_template_and_sender,
    unknown_outcome,
)
from altegio_bot.workers import easyweek_voucher_production_worker as worker_module

# Only the numeric mailing page. A glob would also match the prepare screen the
# browser is already on, and `wait_for_url` would return without navigating.
_MAILING_URL = re.compile(r"/ops/voucher-mailings/\d+$")


def _ok(index: int) -> VoucherMutationResponse:
    return VoucherMutationResponse(http_status=200, envelope={"uuid": ORDER_UUIDS[index]})


async def _press_confirm(page) -> None:
    """Press the confirmation and wait for the request to have LANDED.

    ``click`` returns once the event is dispatched, not once the fetch it starts has
    completed. Everything after a confirmation — running the executor, claiming the
    operation, pressing a second tab's button — depends on the operation actually
    existing, so the test must wait for the page's own evidence that the answer came
    back. ``confirmStage`` closes the panel only after awaiting its POST, so the panel
    disappearing is exactly that evidence.
    """
    await page.click("#btn-confirm")
    await page.wait_for_selector("#confirm-panel", state="hidden")


async def _wait_for_slots_text(page, needle: str) -> None:
    """Wait for the slots table to show something, allowing for the poll interval.

    The page repaints on a timer, so the window is the poll period plus a render —
    longer than the default so a slow machine is not mistaken for a broken page.
    """
    await page.wait_for_function(
        "(needle) => { const t = document.querySelector('#slots-table'); return t && t.innerText.includes(needle); }",
        arg=needle,
        timeout=30_000,
    )


async def _drain(session_maker) -> operations_module.StoredOperation | None:
    """Let the real executor take whatever the browser just confirmed."""
    return await worker_module.run_once(session_maker, owner="browser-test-executor")


async def _seed(session_maker, *, count: int, offset: int = 0) -> tuple[int, FakeReader]:
    run_id, _ = await seed_production_preview(session_maker, count=count, offset=offset)
    await seed_template_and_sender(session_maker)
    return run_id, FakeReader(indices=list(range(offset, offset + count)))


async def _freeze_through_browser(page, session_maker, transports, *, run_id: int, count: int) -> int:
    """Everything an operator does to get a batch: look, read, type, confirm.

    Returns the batch id the page NAVIGATED to, read back out of the URL — never out
    of the database, because landing on the right screen is part of what is tested.
    """
    await page.goto(f"/ops/voucher-mailings/prepare?preview_run_id={run_id}")

    # "Проверить состав" — a read of the real audience (review R3).
    await page.click("#btn-load")
    await page.wait_for_selector("#composition-table")
    shown_count = (await page.inner_text("#c-count")).strip()
    shown_total = (await page.inner_text("#c-total")).strip()
    assert shown_count == str(count), shown_count
    assert "€" in shown_total

    # The confirmation fields only appear once a real composition was proven.
    await page.wait_for_selector("#freeze-panel:not(.d-none)")

    # The operator types what they read.
    euro = f"{count * 1500 / 100:.2f}"
    await page.fill("#f-count", str(count))
    await page.fill("#f-euro", euro)
    await page.click("#btn-plan-freeze")

    # A separate confirmation, which only now appears.
    await page.wait_for_selector("#confirm-panel:not(.d-none)")
    summary = await page.inner_text("#confirm-summary")
    assert str(count) in summary and euro in summary, summary

    await _press_confirm(page)
    # The freeze is asynchronous: the page watches the operation until a batch
    # exists, then navigates to it (review R5).
    await page.wait_for_selector("#tracked-operation")
    await _drain(session_maker)
    await page.wait_for_url(_MAILING_URL, timeout=20_000)
    batch_id = int(page.url.rstrip("/").rsplit("/", 1)[-1])
    assert batch_id > 0
    return batch_id


async def _run_stage_through_browser(page, session_maker, *, button: str) -> None:
    """Press a stage button, confirm, and let the executor finish it."""
    await page.wait_for_selector(f"#{button}:not([disabled])")
    await page.click(f"#{button}")
    await page.wait_for_selector("#confirm-panel:not(.d-none)")
    await _press_confirm(page)
    await _drain(session_maker)


# ===========================================================================
# The whole path, in a browser
# ===========================================================================


async def test_an_operator_walks_preview_to_deliver_in_a_browser(
    session_maker, production_configuration, binding_key, executor_enabled, page, transports
):
    """preview → composition → freeze → create → pay → deliver, by clicking."""
    count = 2
    run_id, reader = await _seed(session_maker, count=count)
    transports.use(reader=reader)

    # The entry point: the list page offers this preview.
    await page.goto("/ops/voucher-mailings")
    assert await page.is_visible(f"a[href='/ops/voucher-mailings/prepare?preview_run_id={run_id}']")
    assert "Юлии Мюллер" in await page.inner_text("body")

    batch_id = await _freeze_through_browser(page, session_maker, transports, run_id=run_id, count=count)
    reader.orders.update(await marker_orders(session_maker, batch_id=batch_id))

    # The slots table names the clients, not just ordinals (review R7).
    await page.wait_for_selector("#slots-table")
    table = await page.inner_text("#slots-table")
    assert "Synthetic Production 1" in table, table

    transports.use(reader=reader, mutator=FakeMutator(create_sequence=[_ok(i) for i in range(count)]))
    await _run_stage_through_browser(page, session_maker, button="btn-stage-create")
    await _wait_for_slots_text(page, "created")

    settles = await marker_orders(session_maker, batch_id=batch_id, status="paid")
    transports.use(
        reader=reader,
        mutator=FakeMutator(pay_sequence=[_ok(i) for i in range(count)], reader=reader, settles=settles),
    )
    await _run_stage_through_browser(page, session_maker, button="btn-stage-pay")
    await _wait_for_slots_text(page, "paid")

    sender = FakeSender()
    transports.use(reader=reader, sender=sender)
    await _run_stage_through_browser(page, session_maker, button="btn-stage-deliver")
    await _wait_for_slots_text(page, "provider_accepted")

    assert sender.calls == count
    # The four delivery facts are on screen and not merged.
    delivery = await page.inner_text("#delivery-panel")
    assert "Meta приняла" in delivery and f"{count} из {count}" in delivery
    assert "Прочитано" in delivery and "0 из" in delivery
    # No voucher code anywhere on the page.
    body = await page.inner_text("body")
    for sentinel in VOUCHER_CODE_SENTINELS[:count]:
        assert sentinel not in body
    assert_no_page_errors(page)


async def test_checking_the_composition_does_not_freeze_anything(
    session_maker, production_configuration, binding_key, executor_enabled, page, transports
):
    """Review R3, from the operator's side: the read is a read.

    Pressing "Проверить состав" must show the real list and reveal the fields — and
    must not create a batch, an approval or an operation.
    """
    count = 3
    run_id, reader = await _seed(session_maker, count=count)
    transports.use(reader=reader)

    await page.goto(f"/ops/voucher-mailings/prepare?preview_run_id={run_id}")
    await page.click("#btn-load")
    await page.wait_for_selector("#composition-table")

    rows = await page.locator("#composition-table tbody tr").count()
    assert rows == count
    assert (await page.inner_text("#c-count")).strip() == str(count)
    # The real period, not today's month.
    assert "2026-08-01..2026-08-31" in await page.inner_text("#c-period")
    # The fields are reachable...
    await page.wait_for_selector("#freeze-panel:not(.d-none)")
    # ...and the confirmation is NOT armed.
    assert await page.is_hidden("#confirm-panel")
    # The numbers are shown as an expectation, and the fields are left empty for the
    # operator to state them.
    assert str(count) in await page.inner_text("#approval-hint")
    assert await page.input_value("#f-count") == ""

    assert await ledger_module.load_for_preview(session_maker, campaign_run_id=run_id) is not None
    snapshot = await ledger_module.load_for_preview(session_maker, campaign_run_id=run_id)
    assert snapshot.exists is False
    assert await operations_module.list_operations(session_maker, campaign_run_id=run_id) == []
    assert_no_page_errors(page)


async def test_wrong_numbers_do_not_create_a_batch(
    session_maker, production_configuration, binding_key, executor_enabled, page, transports
):
    """A miscount refuses the whole freeze, from the browser."""
    count = 3
    run_id, reader = await _seed(session_maker, count=count)
    transports.use(reader=reader)
    await page.goto(f"/ops/voucher-mailings/prepare?preview_run_id={run_id}")
    await page.click("#btn-load")
    await page.wait_for_selector("#freeze-panel:not(.d-none)")

    # Approving four people for a list of three.
    await page.fill("#f-count", str(count + 1))
    await page.fill("#f-euro", f"{(count + 1) * 1500 / 100:.2f}")
    await page.click("#btn-plan-freeze")
    # By TEXT, not by presence: the composition step already left an alert there, so
    # waiting for "an alert" would pass instantly on the previous one.
    await page.wait_for_function(
        "() => { const el = document.querySelector('#alert-area'); return el && el.innerText.includes('недоступно'); }"
    )
    assert await page.is_hidden("#confirm-panel")
    assert (await ledger_module.load_for_preview(session_maker, campaign_run_id=run_id)).exists is False

    # The right numbers then open a confirmation.
    await page.fill("#f-count", str(count))
    await page.fill("#f-euro", f"{count * 1500 / 100:.2f}")
    await page.click("#btn-plan-freeze")
    await page.wait_for_selector("#confirm-panel:not(.d-none)")
    # Still nothing created: the confirmation has not been pressed.
    assert (await ledger_module.load_for_preview(session_maker, campaign_run_id=run_id)).exists is False
    assert_no_page_errors(page)


async def test_a_pending_freeze_survives_a_refresh_and_a_relogin(
    session_maker, production_configuration, binding_key, executor_enabled, page, transports
):
    """Review R5: a confirmed FREEZE has no batch yet, and the page must track it.

    The executor is deliberately NOT run until after the reload, so the operation is
    genuinely still queued while the browser is restarted.
    """
    count = 2
    run_id, reader = await _seed(session_maker, count=count)
    transports.use(reader=reader)
    await page.goto(f"/ops/voucher-mailings/prepare?preview_run_id={run_id}")
    await page.click("#btn-load")
    await page.wait_for_selector("#freeze-panel:not(.d-none)")
    await page.fill("#f-count", str(count))
    await page.fill("#f-euro", f"{count * 1500 / 100:.2f}")
    await page.click("#btn-plan-freeze")
    await page.wait_for_selector("#confirm-panel:not(.d-none)")
    await page.click("#btn-confirm")

    # Queued, not finished: the page says it is waiting rather than offering a freeze.
    await page.wait_for_selector("#tracked-operation")
    waiting = await page.inner_text("#tracked-operation")
    assert "очереди" in waiting or "выполняется" in waiting, waiting
    operations = await operations_module.list_operations(session_maker, campaign_run_id=run_id)
    assert len(operations) == 1 and operations[0].batch_id is None

    # A refresh — the operator's F5, and the same thing a fresh login lands on.
    await page.reload()
    await page.wait_for_selector("#tracked-operation")
    assert len(await operations_module.list_operations(session_maker, campaign_run_id=run_id)) == 1

    # Now the worker runs, and the page follows to the batch it created.
    await _drain(session_maker)
    await page.wait_for_url(_MAILING_URL, timeout=20_000)
    batch_id = int(page.url.rstrip("/").rsplit("/", 1)[-1])
    snapshot = await ledger_module.load_for_preview(session_maker, campaign_run_id=run_id)
    assert snapshot.batch_id == batch_id
    # Exactly one freeze happened.
    assert len(await operations_module.list_operations(session_maker, batch_id=batch_id)) == 1
    assert_no_page_errors(page)


async def test_a_refused_freeze_is_shown_and_not_retried_silently(
    session_maker, production_configuration, binding_key, executor_enabled, page, transports
):
    """Review R5: the page reports a worker refusal rather than spinning forever."""
    count = 2
    run_id, reader = await _seed(session_maker, count=count)
    transports.use(reader=reader)
    await page.goto(f"/ops/voucher-mailings/prepare?preview_run_id={run_id}")
    await page.click("#btn-load")
    await page.wait_for_selector("#freeze-panel:not(.d-none)")
    await page.fill("#f-count", str(count))
    await page.fill("#f-euro", f"{count * 1500 / 100:.2f}")
    await page.click("#btn-plan-freeze")
    await page.wait_for_selector("#confirm-panel:not(.d-none)")
    await _press_confirm(page)
    await page.wait_for_selector("#tracked-operation")

    # The audience changes under the confirmed plan, so the executor refuses it.
    async with session_maker() as session:
        from sqlalchemy import update

        from altegio_bot.models.models import CampaignRecipient

        await session.execute(
            update(CampaignRecipient).where(CampaignRecipient.campaign_run_id == run_id).values(status="excluded")
        )
        await session.commit()
    finished = await _drain(session_maker)
    assert finished is not None and finished.status == "refused"

    await page.wait_for_function(
        "() => { const el = document.querySelector('#tracked-operation');"
        " return el && el.innerText.includes('отказано'); }"
    )
    assert (await ledger_module.load_for_preview(session_maker, campaign_run_id=run_id)).exists is False
    assert_no_page_errors(page)


async def test_two_browser_tabs_confirming_one_offer_act_once(
    session_maker, production_configuration, binding_key, executor_enabled, page, transports, ops_server
):
    """Two real tabs, one offer, one effect."""
    count = 2
    run_id, reader = await _seed(session_maker, count=count)
    transports.use(reader=reader)
    batch_id = await _freeze_through_browser(page, session_maker, transports, run_id=run_id, count=count)
    reader.orders.update(await marker_orders(session_maker, batch_id=batch_id))

    mutator = FakeMutator(create_sequence=[_ok(i) for i in range(count)])
    transports.use(reader=reader, mutator=mutator)

    other = await page.context.new_page()
    try:
        await other.goto(f"/ops/voucher-mailings/{batch_id}")
        for tab in (page, other):
            await tab.wait_for_selector("#btn-stage-create:not([disabled])")
            await tab.click("#btn-stage-create")
            await tab.wait_for_selector("#confirm-panel:not(.d-none)")
        # Both tabs press their own confirmation.
        # The first tab's confirmation lands first, so the second one is attempted
        # while an operation really is in flight — which is the race R1 is about.
        await _press_confirm(page)
        await _press_confirm(other)
        await _drain(session_maker)
        assert await _drain(session_maker) is None, "a second operation was queued"
    finally:
        await other.close()

    assert mutator.calls.count("create") == count
    creates = [
        entry
        for entry in await operations_module.list_operations(session_maker, batch_id=batch_id)
        if entry.stage == "create"
    ]
    assert len(creates) == 1, creates
    assert_no_page_errors(page)


async def test_stop_and_a_fresh_confirmation_from_the_browser(
    session_maker, production_configuration, binding_key, executor_enabled, page, transports
):
    """Stop is pressed in the browser, then continuing is a fresh decision (R1)."""
    count = 3
    run_id, reader = await _seed(session_maker, count=count)
    transports.use(reader=reader)
    batch_id = await _freeze_through_browser(page, session_maker, transports, run_id=run_id, count=count)
    reader.orders.update(await marker_orders(session_maker, batch_id=batch_id))

    # Press stop before the stage runs, from the page.
    await page.click("#btn-stop")
    await page.wait_for_function(
        "() => { const el = document.querySelector('#stop-banner');"
        " return el && el.innerText.includes('Остановлено'); }"
    )
    assert (await ledger_module.stop_state(session_maker, batch_id=batch_id)).active is True

    # A plan prepared now knows about the stop, so confirming it is the continuation.
    mutator = FakeMutator(create_sequence=[_ok(i) for i in range(count)])
    transports.use(reader=reader, mutator=mutator)
    await page.wait_for_selector("#btn-stage-create:not([disabled])")
    await page.click("#btn-stage-create")
    await page.wait_for_selector("#confirm-panel:not(.d-none)")
    await _press_confirm(page)
    finished = await _drain(session_maker)

    # The work first, so a failure distinguishes "the stage did not run" from "the
    # page had not repainted yet".
    assert finished is not None and finished.outcome_code == "applied", finished
    assert mutator.calls.count("create") == count
    assert (await ledger_module.stop_state(session_maker, batch_id=batch_id)).active is False
    await _wait_for_slots_text(page, "created")
    assert_no_page_errors(page)


async def test_a_reconcile_during_an_active_send_is_refused_in_the_browser(
    session_maker, production_configuration, binding_key, executor_enabled, page, transports
):
    """Review R2 from the operator's side: the button answers busy, not success."""
    count = 1
    run_id, reader = await _seed(session_maker, count=count)
    transports.use(reader=reader)
    batch_id = await _freeze_through_browser(page, session_maker, transports, run_id=run_id, count=count)
    reader.orders.update(await marker_orders(session_maker, batch_id=batch_id))

    transports.use(reader=reader, mutator=FakeMutator(create_sequence=[_ok(0)]))
    await _run_stage_through_browser(page, session_maker, button="btn-stage-create")
    settles = await marker_orders(session_maker, batch_id=batch_id, status="paid")
    transports.use(reader=reader, mutator=FakeMutator(pay_sequence=[_ok(0)], reader=reader, settles=settles))
    await _run_stage_through_browser(page, session_maker, button="btn-stage-pay")

    # The send is confirmed and the operation is claimed but not finished, so a
    # stage is genuinely executing while the operator presses "Сверить".
    sender = FakeSender()
    transports.use(reader=reader, sender=sender)
    await page.wait_for_selector("#btn-stage-deliver:not([disabled])")
    await page.click("#btn-stage-deliver")
    await page.wait_for_selector("#confirm-panel:not(.d-none)")
    await _press_confirm(page)
    claimed = await operations_module.claim_next_operation(session_maker, owner="busy-executor")
    assert claimed is not None

    await page.click("#btn-reconcile")
    await page.wait_for_function(
        "() => { const el = document.querySelector('#alert-area'); return el && el.innerText.includes('недоступна'); }"
    )
    assert sender.calls == 0
    assert_no_page_errors(page)


async def test_unknown_then_reconcile_then_continue_in_the_browser(
    session_maker, production_configuration, binding_key, executor_enabled, page, transports
):
    """An ambiguous send halts the rest, the page says so, and the readback is offered."""
    count = 2
    run_id, reader = await _seed(session_maker, count=count)
    transports.use(reader=reader)
    batch_id = await _freeze_through_browser(page, session_maker, transports, run_id=run_id, count=count)
    reader.orders.update(await marker_orders(session_maker, batch_id=batch_id))
    transports.use(reader=reader, mutator=FakeMutator(create_sequence=[_ok(i) for i in range(count)]))
    await _run_stage_through_browser(page, session_maker, button="btn-stage-create")
    settles = await marker_orders(session_maker, batch_id=batch_id, status="paid")
    transports.use(
        reader=reader,
        mutator=FakeMutator(pay_sequence=[_ok(i) for i in range(count)], reader=reader, settles=settles),
    )
    await _run_stage_through_browser(page, session_maker, button="btn-stage-pay")

    sender = FakeSender(outcomes=[unknown_outcome(), unknown_outcome()])
    transports.use(reader=reader, sender=sender)
    await _run_stage_through_browser(page, session_maker, button="btn-stage-deliver")

    await page.wait_for_function(
        "() => { const el = document.querySelector('#stop-banner'); return el && el.innerText.includes('сверка'); }"
    )
    # One send, and the second slot was never attempted.
    assert sender.calls == 1
    # The next stage is not offered while an outcome is in doubt.
    assert await page.is_disabled("#btn-stage-deliver")
    # The readback is.
    transports.use(reader=reader)
    await page.click("#btn-reconcile")
    await page.wait_for_function(
        "() => { const el = document.querySelector('#alert-area');"
        " return el && (el.innerText.includes('выполнена') || el.innerText.includes('недоступна')); }"
    )
    assert_no_page_errors(page)


async def test_an_allowed_refund_names_its_client_and_runs_from_the_browser(
    session_maker, production_configuration, binding_key, executor_enabled, page, transports
):
    """Reviews R6 and R7: the right rows offer a refund, and it says whose it is."""
    count = 2
    run_id, reader = await _seed(session_maker, count=count)
    transports.use(reader=reader)
    batch_id = await _freeze_through_browser(page, session_maker, transports, run_id=run_id, count=count)
    reader.orders.update(await marker_orders(session_maker, batch_id=batch_id))
    transports.use(reader=reader, mutator=FakeMutator(create_sequence=[_ok(i) for i in range(count)]))
    await _run_stage_through_browser(page, session_maker, button="btn-stage-create")
    settles = await marker_orders(session_maker, batch_id=batch_id, status="paid")
    transports.use(
        reader=reader,
        mutator=FakeMutator(pay_sequence=[_ok(i) for i in range(count)], reader=reader, settles=settles),
    )
    await _run_stage_through_browser(page, session_maker, button="btn-stage-pay")

    await page.wait_for_selector("#slots-table button[data-slot='1']")
    refunded = await marker_orders(session_maker, batch_id=batch_id, status="refunded")
    mutator = FakeMutator(refund=_ok(0), reader=reader, settles=refunded)
    transports.use(reader=reader, mutator=mutator)

    await page.click("#slots-table button[data-slot='1']")
    await page.wait_for_selector("#confirm-panel:not(.d-none)")
    summary = await page.inner_text("#confirm-summary")
    # The confirmation names the person, not only the ordinal (review R7).
    assert "Synthetic Production 1" in summary, summary
    await _press_confirm(page)
    finished = await _drain(session_maker)
    assert finished is not None and finished.status == "completed", finished
    assert mutator.calls.count("refund") == 1
    await _wait_for_slots_text(page, "refunded")
    # The refunded row no longer offers a refund; the other is untouched.
    assert await page.locator("#slots-table button[data-slot='1']").count() == 0
    assert_no_page_errors(page)


async def test_no_refund_is_offered_after_a_send_attempt(
    session_maker, production_configuration, binding_key, executor_enabled, page, transports
):
    """Review R6: the UI must not offer what the server forbids."""
    count = 1
    run_id, reader = await _seed(session_maker, count=count)
    transports.use(reader=reader)
    batch_id = await _freeze_through_browser(page, session_maker, transports, run_id=run_id, count=count)
    reader.orders.update(await marker_orders(session_maker, batch_id=batch_id))
    transports.use(reader=reader, mutator=FakeMutator(create_sequence=[_ok(0)]))
    await _run_stage_through_browser(page, session_maker, button="btn-stage-create")
    settles = await marker_orders(session_maker, batch_id=batch_id, status="paid")
    transports.use(reader=reader, mutator=FakeMutator(pay_sequence=[_ok(0)], reader=reader, settles=settles))
    await _run_stage_through_browser(page, session_maker, button="btn-stage-pay")
    await page.wait_for_selector("#slots-table button[data-slot='1']")

    transports.use(reader=reader, sender=FakeSender())
    await _run_stage_through_browser(page, session_maker, button="btn-stage-deliver")
    await _wait_for_slots_text(page, "provider_accepted")
    assert await page.locator("#slots-table button[data-slot='1']").count() == 0
    assert_no_page_errors(page)


async def test_two_clients_with_the_same_name_stay_distinguishable(
    session_maker, production_configuration, binding_key, executor_enabled, page, transports
):
    """Review R7: identical names must still identify different preview rows."""
    count = 2
    run_id, reader = await _seed(session_maker, count=count)
    async with session_maker() as session:
        from sqlalchemy import update

        from altegio_bot.models.models import CampaignRecipient

        await session.execute(
            update(CampaignRecipient)
            .where(CampaignRecipient.campaign_run_id == run_id)
            .values(display_name="Анна <b>Иванова</b>")
        )
        await session.commit()
    transports.use(reader=reader)
    await _freeze_through_browser(page, session_maker, transports, run_id=run_id, count=count)

    await page.wait_for_selector("#slots-table")
    rows = page.locator("#slots-table tbody tr")
    assert await rows.count() == count
    links = await page.locator("#slots-table tbody tr td:nth-child(3)").all_inner_texts()
    # Same name, different preview rows — and the two are told apart by them.
    assert len(set(links)) == count, links
    # The angle brackets are text, not markup: nothing was injected.
    assert await page.locator("#slots-table b").count() == 0
    assert "Анна <b>Иванова</b>" in await page.inner_text("#slots-table")
    assert_no_page_errors(page)


async def test_two_previews_do_not_show_each_others_operations(
    session_maker, production_configuration, binding_key, executor_enabled, page, transports
):
    """Review R5: a page watching one preview is scoped to it."""
    run_a, reader_a = await _seed(session_maker, count=1)
    run_b, reader_b = await _seed(session_maker, count=1, offset=10)
    transports.use(reader=reader_a)
    batch_a = await _freeze_through_browser(page, session_maker, transports, run_id=run_a, count=1)

    # The second preview's own page, before it has any operation of its own.
    await page.goto(f"/ops/voucher-mailings/prepare?preview_run_id={run_b}")
    transports.use(reader=reader_b)
    scoped = await page.evaluate(
        "async (runId) => {"
        " const r = await fetch('/ops/voucher-mailings/api/status?preview_run_id=' + runId,"
        " {credentials: 'same-origin'});"
        " return await r.json(); }",
        run_b,
    )
    # Preview A's freeze must not appear here.
    assert scoped["operations"] == [], scoped["operations"]
    assert scoped["active_operation"] is None
    assert (scoped.get("batch") or {}).get("exists") is not True

    # And preview A's own page still shows its own.
    await page.goto(f"/ops/voucher-mailings/{batch_a}")
    await page.wait_for_selector("#operation-panel")
    own = await page.evaluate(
        "async (batchId) => {"
        " const r = await fetch('/ops/voucher-mailings/api/status?batch_id=' + batchId,"
        " {credentials: 'same-origin'});"
        " return await r.json(); }",
        batch_a,
    )
    assert [entry["stage"] for entry in own["operations"]] == ["freeze"]
    assert_no_page_errors(page)


async def test_a_closed_fence_blocks_the_buttons_and_says_why(
    session_maker, production_configuration, binding_key, executor_enabled, page, transports, monkeypatch
):
    """With the fence shut the pages render, explain, and start nothing."""
    count = 1
    run_id, reader = await _seed(session_maker, count=count)
    transports.use(reader=reader)
    from altegio_bot.settings import settings as live

    monkeypatch.setattr(live, "easyweek_voucher_production_mailing_enabled", False, raising=False)

    await page.goto(f"/ops/voucher-mailings/prepare?preview_run_id={run_id}")
    assert "voucher_production_disabled" in await page.inner_text("body")
    await page.click("#btn-load")
    await page.wait_for_selector("#alert-area .alert")
    assert await page.is_hidden("#freeze-panel")
    assert (await ledger_module.load_for_preview(session_maker, campaign_run_id=run_id)).exists is False
    assert await operations_module.list_operations(session_maker, campaign_run_id=run_id) == []
    assert_no_page_errors(page)


async def test_an_unauthenticated_browser_is_sent_to_the_login_form(
    session_maker, production_configuration, binding_key, executor_enabled, ops_server
):
    """No session, no pages — checked in a browser with no cookie at all."""
    try:
        from playwright.async_api import async_playwright
    except ImportError:  # pragma: no cover - the fixture already enforces this
        return
    async with async_playwright() as pw:
        browser = await pw.chromium.launch()
        try:
            context = await browser.new_context(base_url=ops_server)
            anonymous = await context.new_page()
            await anonymous.goto("/ops/voucher-mailings")
            assert "/ops/login" in anonymous.url, anonymous.url
            # And the API refuses rather than acting.
            answer = await anonymous.evaluate(
                "async () => {"
                " const r = await fetch('/ops/voucher-mailings/api/stop',"
                "  {method: 'POST', headers: {'Content-Type': 'application/json'},"
                "   body: JSON.stringify({batch_id: 1})});"
                " return r.status; }"
            )
            assert answer in (401, 403), answer
        finally:
            await browser.close()
    assert await operations_module.list_operations(session_maker) == []


async def test_concurrent_browser_confirmations_of_two_stages_do_not_overlap(
    session_maker, production_configuration, binding_key, executor_enabled, page, transports
):
    """Review R1 in a browser: a second stage cannot start while one is in flight."""
    count = 2
    run_id, reader = await _seed(session_maker, count=count)
    transports.use(reader=reader)
    batch_id = await _freeze_through_browser(page, session_maker, transports, run_id=run_id, count=count)
    reader.orders.update(await marker_orders(session_maker, batch_id=batch_id))

    transports.use(reader=reader, mutator=FakeMutator(create_sequence=[_ok(i) for i in range(count)]))
    await page.wait_for_selector("#btn-stage-create:not([disabled])")
    await page.click("#btn-stage-create")
    await page.wait_for_selector("#confirm-panel:not(.d-none)")
    await _press_confirm(page)

    # A second confirmation attempted through the page's own API, while the first
    # operation is still queued.
    refusal = await page.evaluate(
        "async (args) => {"
        " const plan = await fetch('/ops/voucher-mailings/api/plan',"
        "  {method: 'POST', credentials: 'same-origin',"
        "   headers: {'Content-Type': 'application/json', 'X-Ops-CSRF': CSRF},"
        "   body: JSON.stringify({stage: 'create', preview_run_id: args.run, batch_id: args.batch})});"
        " const offer = await plan.json();"
        " if (!offer.ready) return {stage: 'plan', reasons: offer.reasons};"
        " const confirm = await fetch('/ops/voucher-mailings/api/confirm',"
        "  {method: 'POST', credentials: 'same-origin',"
        "   headers: {'Content-Type': 'application/json', 'X-Ops-CSRF': CSRF},"
        "   body: JSON.stringify({approval_id: offer.approval.approval_id,"
        "     confirmed_count: offer.targets.stage_target_count,"
        "     confirmed_amount_minor: offer.targets.stage_amount_minor})});"
        " return {stage: 'confirm', status: confirm.status, body: await confirm.json()}; }",
        {"run": run_id, "batch": batch_id},
    )
    if refusal["stage"] == "confirm":
        assert refusal["status"] == 409, refusal
        assert "voucher_production_operation_in_flight" in refusal["body"]["reasons"]

    await _drain(session_maker)
    assert await _drain(session_maker) is None
    creates = [
        entry
        for entry in await operations_module.list_operations(session_maker, batch_id=batch_id)
        if entry.stage == "create"
    ]
    assert len(creates) == 1, creates
    assert_no_page_errors(page)


async def test_the_executor_runs_the_work_without_any_manual_exec(
    session_maker, production_configuration, binding_key, executor_enabled, page, transports
):
    """Review R4: a UI-confirmed stage is executed by the worker loop itself.

    Driven through ``run_worker`` — the deployed entrypoint's own loop — rather than a
    single hand-called pass, so what is proven is that the supervised service would
    pick the work up.
    """
    count = 2
    run_id, reader = await _seed(session_maker, count=count)
    transports.use(reader=reader)
    batch_id = await _freeze_through_browser(page, session_maker, transports, run_id=run_id, count=count)
    reader.orders.update(await marker_orders(session_maker, batch_id=batch_id))

    mutator = FakeMutator(create_sequence=[_ok(i) for i in range(count)])
    transports.use(reader=reader, mutator=mutator)
    await page.wait_for_selector("#btn-stage-create:not([disabled])")
    await page.click("#btn-stage-create")
    await page.wait_for_selector("#confirm-panel:not(.d-none)")
    await _press_confirm(page)

    stop_event = asyncio.Event()
    worker = asyncio.create_task(
        worker_module.run_worker(session_maker, owner="supervised", poll_sec=0.05, stop_event=stop_event)
    )
    try:
        await _wait_for_slots_text(page, "created")
    finally:
        stop_event.set()
        await asyncio.wait_for(worker, timeout=10)

    assert mutator.calls.count("create") == count
    assert_no_page_errors(page)
