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
import contextlib
import re

from altegio_bot.campaigns.easyweek_voucher_production import ledger as ledger_module
from altegio_bot.campaigns.easyweek_voucher_production import operations as operations_module
from altegio_bot.campaigns.easyweek_voucher_production import runner as production_runner
from altegio_bot.easyweek_voucher_mutation import VoucherMutationResponse
from altegio_bot.tests.easyweek_voucher_10eur_fixtures import (
    FakeReader,
    marker_orders,
    seed_template_and_sender,
)
from altegio_bot.tests.easyweek_voucher_mailing_browser_fixtures import (
    assert_no_page_errors,
    tab_with_no_cache,
    watch_for_errors,
)
from altegio_bot.tests.easyweek_voucher_production_fixtures import (
    ORDER_UUIDS,
    VOUCHER_CODE_SENTINELS,
    FakeMutator,
    FakeSender,
    seed_production_preview,
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
    assert f"{count * 10:.2f}" in shown_total
    terms = await page.inner_text(".voucher-terms")
    assert "10 EUR" in terms and "Одноразовый" in terms and "календарный месяц с активации" in terms
    assert "einen Monat" in await page.inner_text("pre")

    # The confirmation fields only appear once a real composition was proven.
    await page.wait_for_selector("#freeze-panel:not(.d-none)")

    # The operator types what they read.
    euro = f"{count * 1000 / 100:.2f}"
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
    session_maker,
    production_configuration,
    binding_key,
    executor_enabled,
    page,
    transports,
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
    await page.fill("#f-euro", f"{(count + 1) * 1000 / 100:.2f}")
    await page.click("#btn-plan-freeze")
    # By TEXT, not by presence: the composition step already left an alert there, so
    # waiting for "an alert" would pass instantly on the previous one.
    await page.wait_for_function(
        "() => { const el = document.querySelector('#alert-area'); return el && el.innerText.includes('недоступно'); }"
    )
    assert await page.is_hidden("#confirm-panel")
    assert (await ledger_module.load_for_preview(session_maker, campaign_run_id=run_id)).exists is False
    # The refusal also says the shown list is no longer known to be current, because
    # a reason code cannot distinguish a miscount from an edited preview (review F1).
    await page.wait_for_selector("#composition-stale")

    # The right numbers alone are not enough now: the audience is proven again first.
    await page.click("#btn-load")
    await page.wait_for_selector("#composition-stale", state="hidden")
    await page.fill("#f-count", str(count))
    await page.fill("#f-euro", f"{count * 1000 / 100:.2f}")
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
    await page.fill("#f-euro", f"{count * 1000 / 100:.2f}")
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
    await page.fill("#f-euro", f"{count * 1000 / 100:.2f}")
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
    session_maker,
    production_configuration,
    binding_key,
    executor_enabled,
    page,
    transports,
    ops_server,
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


async def test_a_terminal_stop_from_the_browser_offers_no_continuation(
    session_maker, production_configuration, binding_key, executor_enabled, page, transports
):
    """§45.4. The stop an operator presses here ends this €10 mailing's execution.

    Supersedes ``test_stop_and_a_fresh_confirmation_from_the_browser``, whose whole
    point was that a fresh confirmation RESUMED the batch. That is the §43.6 rule the
    owner replaced for this contract; schemas 1 and 2 keep it, and the API suites
    cover the server's side of the refusal.

    What the page has to get right is the honesty: it says execution stopped, it does
    not say the issued vouchers were annulled or the money returned, and it keeps
    offering the two things that still work — reading and reconciling.
    """
    count = 3
    run_id, reader = await _seed(session_maker, count=count)
    transports.use(reader=reader)
    batch_id = await _freeze_through_browser(page, session_maker, transports, run_id=run_id, count=count)
    reader.orders.update(await marker_orders(session_maker, batch_id=batch_id))

    # The press is guarded by a dialog that spells the terminality out. Accepting it
    # explicitly is the test: a browser that silently dismissed it would prove that
    # an operator can stop a mailing for good without being told.
    warnings: list[str] = []

    async def _accept(dialog):
        warnings.append(dialog.message)
        await dialog.accept()

    page.on("dialog", _accept)
    await page.wait_for_selector("#btn-stop:not([disabled])")
    assert "окончательно" in await page.inner_text("#btn-stop")
    await page.click("#btn-stop")
    await page.wait_for_function(
        "() => { const el = document.querySelector('#stop-banner');"
        " return el && el.innerText.includes('окончательно'); }"
    )
    assert warnings and "окончательно" in warnings[0]
    # Said before the write, not after: what it stops, and what it does not do.
    assert "не аннулир" in warnings[0] and "возвра" in warnings[0]

    state = await ledger_module.stop_state(session_maker, batch_id=batch_id)
    assert state.active is True and state.terminal is True

    banner = await page.inner_text("#stop-banner")
    assert "не означает" in banner and "аннулированы" in banner
    # No stage is offered any more, and the stop cannot be pressed a second time.
    for stage in ("create", "pay", "deliver"):
        assert await page.get_attribute(f"#btn-stage-{stage}", "disabled") is not None
    assert await page.get_attribute("#btn-stop", "disabled") is not None
    # Reading the outside world back is still an operator action.
    assert await page.get_attribute("#btn-reconcile", "disabled") is None
    await page.click("#btn-reconcile")
    await page.wait_for_function(
        "() => { const el = document.querySelector('#alert-area'); return el && el.innerText.trim().length > 0; }"
    )

    # Nothing was bought, and the executor has nothing to take.
    mutator = FakeMutator(create_sequence=[_ok(i) for i in range(count)])
    transports.use(reader=reader, mutator=mutator)
    assert await _drain(session_maker) is None
    assert mutator.calls == []
    snapshot = await ledger_module.load(session_maker, batch_id=batch_id)
    assert {item.status for item in snapshot.items} == {"planned"}
    assert (await ledger_module.stop_state(session_maker, batch_id=batch_id)).active is True
    assert_no_page_errors(page)


async def test_reconcile_and_an_allowed_refund_still_work_after_a_terminal_stop(
    session_maker, production_configuration, binding_key, executor_enabled, page, transports
):
    """§45.4. Stopping takes away spending, not access to what already happened.

    The batch is paid before the stop, so there is real money out there — which is
    exactly when an operator must still be able to read the outside world back and
    get one slot's money returned, from the same interface, with its own separate
    confirmation.
    """
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
    await _wait_for_slots_text(page, "paid")

    page.on("dialog", lambda dialog: asyncio.ensure_future(dialog.accept()))
    await page.click("#btn-stop")
    await page.wait_for_function(
        "() => { const el = document.querySelector('#stop-banner');"
        " return el && el.innerText.includes('окончательно'); }"
    )
    assert (await ledger_module.stop_state(session_maker, batch_id=batch_id)).terminal is True

    # Reconciliation: still an operator action, and it reopens nothing.
    transports.use(reader=reader)
    await page.click("#btn-reconcile")
    await page.wait_for_function(
        "() => { const el = document.querySelector('#alert-area'); return el && el.innerText.trim().length > 0; }"
    )

    # One slot's money back, separately confirmed, through the same page.
    refunded = await marker_orders(session_maker, batch_id=batch_id, status="refunded")
    mutator = FakeMutator(refund=_ok(0), reader=reader, settles=refunded)
    transports.use(reader=reader, mutator=mutator)
    await page.wait_for_selector("#slots-table button[data-slot='1']")
    await page.click("#slots-table button[data-slot='1']")
    await page.wait_for_selector("#confirm-panel:not(.d-none)")
    await _press_confirm(page)
    finished = await _drain(session_maker)
    assert finished is not None and finished.status == "completed", finished
    assert mutator.calls.count("refund") == 1
    await _wait_for_slots_text(page, "refunded")

    # And the stop is still terminal: no stage came back with the refund.
    state = await ledger_module.stop_state(session_maker, batch_id=batch_id)
    assert state.active is True and state.terminal is True
    for stage in ("create", "pay", "deliver"):
        assert await page.get_attribute(f"#btn-stage-{stage}", "disabled") is not None
    assert_no_page_errors(page)


async def test_a_reconcile_during_an_active_send_is_refused_in_the_browser(
    session_maker,
    production_configuration,
    binding_key,
    executor_enabled,
    page,
    transports,
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
    session_maker,
    production_configuration,
    binding_key,
    executor_enabled,
    page,
    transports,
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
    session_maker,
    production_configuration,
    binding_key,
    executor_enabled,
    page,
    transports,
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


# ===========================================================================
# F1 — the composition and the plan answer are different payloads
# ===========================================================================


async def _composition_on_screen(page) -> dict[str, object]:
    """What the composition panel is telling the operator, right now."""
    return {
        "period": (await page.inner_text("#c-period")).strip(),
        "count": (await page.inner_text("#c-count")).strip(),
        "total": (await page.inner_text("#c-total")).strip(),
        "rows": await page.locator("#composition-table tbody tr").count(),
    }


async def test_the_composition_survives_preparing_the_confirmation(
    session_maker, production_configuration, binding_key, executor_enabled, page, transports
):
    """Review F1, exactly as reported.

    The reviewed page passed the ``/api/plan`` answer — a stage offer — to the
    composition renderer, which reads top-level fields that payload does not have. So
    the moment the operator pressed "Проверить и зафиксировать", the list they had
    just read was repainted as 0 recipients, 0,00 €, "—" and "Состав пуст", while the
    confirmation dialog beside it still said two recipients and 30 €.

    The composition is therefore read BEFORE and AFTER the dialog appears and has to
    be the same both times, which is the assertion the earlier browser tests were
    missing: they checked the panel before pressing and never looked again.
    """
    count = 2
    run_id, reader = await _seed(session_maker, count=count)
    transports.use(reader=reader)
    await page.goto(f"/ops/voucher-mailings/prepare?preview_run_id={run_id}")

    await page.click("#btn-load")
    await page.wait_for_selector("#composition-table")
    before = await _composition_on_screen(page)
    assert before["rows"] == count
    assert before["count"] == str(count)
    assert "20.00" in str(before["total"]), before
    assert "2026-08-01..2026-08-31" in str(before["period"]), before

    euro = f"{count * 1000 / 100:.2f}"
    await page.fill("#f-count", str(count))
    await page.fill("#f-euro", euro)
    await page.click("#btn-plan-freeze")
    await page.wait_for_selector("#confirm-panel:not(.d-none)")

    after = await _composition_on_screen(page)
    assert after == before, f"the composition changed while the confirmation was prepared: {before} -> {after}"
    # And the two panels agree, which is the whole point: the dialog's numbers are
    # the plan's, the table's are the composition's, and they are the same numbers.
    summary = await page.inner_text("#confirm-summary")
    assert str(count) in summary and euro in summary, summary
    assert await page.is_hidden("#composition-stale")
    assert_no_page_errors(page)


async def test_a_refused_plan_marks_the_composition_instead_of_zeroing_it(
    session_maker, production_configuration, binding_key, executor_enabled, page, transports
):
    """A refusal does not become an empty audience, and does not become a freeze.

    Two things have to be true at once: the list stays on screen with its real
    numbers, and it stops counting as current — because a refused freeze plan cannot
    say whether the operator miscounted or the preview changed underneath them.
    """
    count = 3
    run_id, reader = await _seed(session_maker, count=count)
    transports.use(reader=reader)
    await page.goto(f"/ops/voucher-mailings/prepare?preview_run_id={run_id}")
    await page.click("#btn-load")
    await page.wait_for_selector("#composition-table")
    before = await _composition_on_screen(page)

    # One recipient too many.
    await page.fill("#f-count", str(count + 1))
    await page.fill("#f-euro", f"{(count + 1) * 1000 / 100:.2f}")
    await page.click("#btn-plan-freeze")
    await page.wait_for_selector("#composition-stale")

    assert await _composition_on_screen(page) == before, "a refusal repainted the composition"
    assert await page.is_hidden("#confirm-panel")

    # And the stale list cannot be frozen on: the numbers mean nothing until the
    # audience is proven again.
    await page.fill("#f-count", str(count))
    await page.fill("#f-euro", f"{count * 1000 / 100:.2f}")
    await page.click("#btn-plan-freeze")
    await page.wait_for_function(
        "() => { const el = document.querySelector('#alert-area');"
        " return el && el.innerText.includes('Сначала проверьте состав'); }"
    )
    assert await page.is_hidden("#confirm-panel")

    # Nothing was created by any of it.
    assert (await ledger_module.load_for_preview(session_maker, campaign_run_id=run_id)).exists is False
    assert await operations_module.list_operations(session_maker, campaign_run_id=run_id) == []

    # Re-checking the list clears the mark and the right numbers then arm the step.
    await page.click("#btn-load")
    await page.wait_for_selector("#composition-stale", state="hidden")
    await page.fill("#f-count", str(count))
    await page.fill("#f-euro", f"{count * 1000 / 100:.2f}")
    await page.click("#btn-plan-freeze")
    await page.wait_for_selector("#confirm-panel:not(.d-none)")
    assert (await ledger_module.load_for_preview(session_maker, campaign_run_id=run_id)).exists is False
    assert_no_page_errors(page)


async def test_a_composition_that_changed_after_the_check_is_not_confirmable(
    session_maker, production_configuration, binding_key, executor_enabled, page, transports
):
    """A swap keeps the count and the money identical, so the digest decides.

    One recipient excluded and another admitted leaves "3 people, 45 €" true, the
    operator's typed numbers correct and the plan ready — while the list on screen
    names somebody who is no longer in it. Numbers cannot catch that; the composition
    digest the plan re-proved can.
    """
    count = 3
    run_id, recipient_ids = await seed_production_preview(session_maker, count=count + 1)
    await seed_template_and_sender(session_maker)
    reader = FakeReader(indices=list(range(count + 1)))
    transports.use(reader=reader)

    async def include(ids: list[int], *, status: str) -> None:
        async with session_maker() as session:
            from sqlalchemy import update

            from altegio_bot.models.models import CampaignRecipient

            await session.execute(update(CampaignRecipient).where(CampaignRecipient.id.in_(ids)).values(status=status))
            await session.commit()

    # Three of the four are in the mailing when the operator looks.
    await include([recipient_ids[-1]], status="excluded")
    await page.goto(f"/ops/voucher-mailings/prepare?preview_run_id={run_id}")
    await page.click("#btn-load")
    await page.wait_for_selector("#composition-table")
    shown = await _composition_on_screen(page)
    assert shown["rows"] == count and shown["count"] == str(count), shown

    # Now the first is dropped and the fourth admitted: three people and 45 € are
    # still both true, so the operator's typed numbers will match a ready plan.
    await include([recipient_ids[0]], status="excluded")
    await include([recipient_ids[-1]], status="candidate")

    await page.fill("#f-count", str(count))
    await page.fill("#f-euro", f"{count * 1000 / 100:.2f}")
    await page.click("#btn-plan-freeze")
    await page.wait_for_selector("#composition-stale")

    assert await page.is_hidden("#confirm-panel")
    assert await _composition_on_screen(page) == shown, "the stale list was repainted as the new one"
    assert (await ledger_module.load_for_preview(session_maker, campaign_run_id=run_id)).exists is False
    assert_no_page_errors(page)


# ===========================================================================
# F2 — the page restores itself from the server
# ===========================================================================


async def _confirm_a_freeze(page, session_maker, *, run_id: int, count: int) -> None:
    """Everything up to and including the confirmation, with nothing drained."""
    await page.goto(f"/ops/voucher-mailings/prepare?preview_run_id={run_id}")
    await page.click("#btn-load")
    await page.wait_for_selector("#freeze-panel:not(.d-none)")
    await page.fill("#f-count", str(count))
    await page.fill("#f-euro", f"{count * 1000 / 100:.2f}")
    await page.click("#btn-plan-freeze")
    await page.wait_for_selector("#confirm-panel:not(.d-none)")
    await _press_confirm(page)
    await page.wait_for_selector("#tracked-operation")


async def test_a_queued_freeze_is_found_by_a_tab_that_stored_nothing(
    session_maker, production_configuration, binding_key, executor_enabled, page, transports, ops_server
):
    """Review F2, cases 1 and 2.

    A new tab has its own empty sessionStorage, and so does a fresh login. The
    reviewed page resumed only from that store, so both of them showed a preview with
    a confirmed, queued FREEZE as a mailing nobody had started — and would have
    offered a second freeze.
    """
    count = 2
    run_id, reader = await _seed(session_maker, count=count)
    transports.use(reader=reader)
    await _confirm_a_freeze(page, session_maker, run_id=run_id, count=count)
    queued = await operations_module.list_operations(session_maker, campaign_run_id=run_id)
    assert len(queued) == 1 and queued[0].batch_id is None

    # Case 1: a tab whose sessionStorage is emptied before any page script runs, so
    # the only possible source of what it shows is the server.
    other = await tab_with_no_cache(page, ops_server)
    try:
        await other.goto(f"/ops/voucher-mailings/prepare?preview_run_id={run_id}")
        await other.wait_for_selector("#tracked-operation")
        shown = await other.inner_text("#tracked-operation")
        assert "очереди" in shown or "выполняется" in shown, shown
        assert_no_page_errors(other)
    finally:
        await other.context.close()

    # Case 2: a fresh login, in a context that was never on this page at all.
    relogged = await tab_with_no_cache(page, ops_server)
    try:
        await relogged.goto(f"/ops/voucher-mailings/prepare?preview_run_id={run_id}")
        await relogged.wait_for_selector("#tracked-operation")
        assert "очереди" in await relogged.inner_text("#tracked-operation")
        # Still one operation: watching is a read.
        assert len(await operations_module.list_operations(session_maker, campaign_run_id=run_id)) == 1

        # The executor runs, and the tab that stored nothing follows to the batch.
        await _drain(session_maker)
        await relogged.wait_for_url(_MAILING_URL, timeout=20_000)
        batch_id = int(relogged.url.rstrip("/").rsplit("/", 1)[-1])
        snapshot = await ledger_module.load_for_preview(session_maker, campaign_run_id=run_id)
        assert snapshot.batch_id == batch_id
        assert len(await operations_module.list_operations(session_maker, batch_id=batch_id)) == 1
        assert_no_page_errors(relogged)
    finally:
        await relogged.context.close()


async def test_a_lost_confirm_answer_is_resolved_from_the_server(
    session_maker, production_configuration, binding_key, executor_enabled, page, transports
):
    """Review F2, case 3: the request is carried out, the answer never arrives.

    The route lets the POST reach the server and then drops the response, so the
    operation really is committed while the browser sees a failed fetch. The page must
    not press again, must not report that nothing happened, and must find the
    operation that exists.
    """
    count = 2
    run_id, reader = await _seed(session_maker, count=count)
    transports.use(reader=reader)

    async def swallow_the_answer(route):
        await route.fetch()
        await route.abort()

    await page.route("**/api/confirm", swallow_the_answer)
    await page.goto(f"/ops/voucher-mailings/prepare?preview_run_id={run_id}")
    await page.click("#btn-load")
    await page.wait_for_selector("#freeze-panel:not(.d-none)")
    await page.fill("#f-count", str(count))
    await page.fill("#f-euro", f"{count * 1000 / 100:.2f}")
    await page.click("#btn-plan-freeze")
    await page.wait_for_selector("#confirm-panel:not(.d-none)")
    await _press_confirm(page)

    # It says the answer was lost, and that it is not confirming again.
    await page.wait_for_function(
        "() => { const el = document.querySelector('#alert-area');"
        " return el && el.innerText.includes('Ответ не получен'); }"
    )
    # ...and then shows the operation the server actually has.
    await page.wait_for_selector("#tracked-operation")
    committed = await operations_module.list_operations(session_maker, campaign_run_id=run_id)
    assert len(committed) == 1, committed

    # Reopening is the same answer, not a second freeze.
    await page.unroute("**/api/confirm")
    await page.reload()
    await page.wait_for_selector("#tracked-operation")
    assert len(await operations_module.list_operations(session_maker, campaign_run_id=run_id)) == 1

    await _drain(session_maker)
    await page.wait_for_url(_MAILING_URL, timeout=20_000)
    batch_id = int(page.url.rstrip("/").rsplit("/", 1)[-1])
    assert len(await operations_module.list_operations(session_maker, batch_id=batch_id)) == 1
    assert await _drain(session_maker) is None, "a second freeze was queued"
    assert (await ledger_module.load(session_maker, batch_id=batch_id)).recipient_count == count
    assert_no_page_errors(page)


async def test_a_failing_read_is_shown_as_a_lost_connection_and_recovers(
    session_maker, production_configuration, binding_key, executor_enabled, page, transports
):
    """Review F2, case 4: a temporary read failure must not end the watch.

    The reviewed code returned silently when the fetch threw or the body would not
    parse, which stopped the polling for good — the screen then looked like a mailing
    nobody had started while a confirmed stage was running on the server.
    """
    count = 2
    run_id, reader = await _seed(session_maker, count=count)
    transports.use(reader=reader)

    failures = {"left": 2}

    async def flaky(route):
        if failures["left"] > 0:
            failures["left"] -= 1
            await route.abort()
            return
        await route.continue_()

    await page.route("**/api/operation*", flaky)
    # Deliberately NOT _confirm_a_freeze: that helper waits for the operation to be
    # on screen, which would swallow the very phase under test.
    await page.goto(f"/ops/voucher-mailings/prepare?preview_run_id={run_id}")
    await page.click("#btn-load")
    await page.wait_for_selector("#freeze-panel:not(.d-none)")
    await page.fill("#f-count", str(count))
    await page.fill("#f-euro", f"{count * 1000 / 100:.2f}")
    await page.click("#btn-plan-freeze")
    await page.wait_for_selector("#confirm-panel:not(.d-none)")
    await _press_confirm(page)

    # Not knowing is its own state, and it is not "nothing is happening".
    await page.wait_for_selector("#operation-unreachable")
    assert "Связь" in await page.inner_text("#operation-unreachable")
    assert len(await operations_module.list_operations(session_maker, campaign_run_id=run_id)) == 1

    # The reads start working again and the watch picks the operation back up.
    await page.wait_for_selector("#tracked-operation")
    assert failures["left"] == 0
    await _drain(session_maker)
    await page.wait_for_url(_MAILING_URL, timeout=20_000)
    batch_id = int(page.url.rstrip("/").rsplit("/", 1)[-1])
    assert len(await operations_module.list_operations(session_maker, batch_id=batch_id)) == 1
    assert_no_page_errors(page)


async def test_a_refusal_is_still_there_when_the_operator_comes_back(
    session_maker, production_configuration, binding_key, executor_enabled, page, transports, ops_server
):
    """Review F2, case 5: a refused freeze is visible from a tab that stored nothing."""
    count = 2
    run_id, reader = await _seed(session_maker, count=count)
    transports.use(reader=reader)
    await _confirm_a_freeze(page, session_maker, run_id=run_id, count=count)

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

    other = await tab_with_no_cache(page, ops_server)
    try:
        await other.goto(f"/ops/voucher-mailings/prepare?preview_run_id={run_id}")
        await other.wait_for_function(
            "() => { const el = document.querySelector('#tracked-operation');"
            " return el && el.innerText.includes('отказано'); }"
        )
        assert (await ledger_module.load_for_preview(session_maker, campaign_run_id=run_id)).exists is False
        assert_no_page_errors(other)
    finally:
        await other.context.close()


async def test_a_batch_created_while_nobody_watched_opens_its_own_page(
    session_maker, production_configuration, binding_key, executor_enabled, page, transports, ops_server
):
    """Review F2, case 7: the freeze finished before the operator came back."""
    count = 2
    run_id, reader = await _seed(session_maker, count=count)
    transports.use(reader=reader)
    await _confirm_a_freeze(page, session_maker, run_id=run_id, count=count)
    await _drain(session_maker)
    snapshot = await ledger_module.load_for_preview(session_maker, campaign_run_id=run_id)
    assert snapshot.exists and snapshot.batch_id is not None

    other = await tab_with_no_cache(page, ops_server)
    try:
        await other.goto(f"/ops/voucher-mailings/prepare?preview_run_id={run_id}")
        await other.wait_for_url(_MAILING_URL, timeout=20_000)
        assert other.url.rstrip("/").endswith(f"/{snapshot.batch_id}")
        # Arriving at the mailing is not a new freeze.
        assert len(await operations_module.list_operations(session_maker, batch_id=snapshot.batch_id)) == 1
        assert_no_page_errors(other)
    finally:
        await other.context.close()


async def test_another_previews_page_never_shows_this_operation(
    session_maker, production_configuration, binding_key, executor_enabled, page, transports
):
    """Review F2, case 6, now that the restore comes from the server.

    The resume is scoped by preview, so the preview next door stays empty — and a
    cached id belonging to another preview is dropped rather than displayed.
    """
    count = 1
    run_a, reader_a = await _seed(session_maker, count=count)
    run_b, _reader_b = await _seed(session_maker, count=count, offset=10)
    transports.use(reader=reader_a)
    await _confirm_a_freeze(page, session_maker, run_id=run_a, count=count)
    mine = await operations_module.list_operations(session_maker, campaign_run_id=run_a)
    assert len(mine) == 1

    other = watch_for_errors(await page.context.new_page())
    try:
        # Plant preview A's operation id in preview B's cache slot — the only way a
        # cache could ever cross previews — and then open preview B.
        await other.goto(f"/ops/voucher-mailings/prepare?preview_run_id={run_b}")
        await other.evaluate(
            "(args) => window.sessionStorage.setItem('ew-voucher-op-' + args.run, String(args.id))",
            {"run": run_b, "id": mine[0].id},
        )
        await other.reload()
        # The planted id is DROPPED rather than displayed, because the server — not the
        # store — decides what this preview has.
        await other.wait_for_function(
            "(run) => window.sessionStorage.getItem('ew-voucher-op-' + run) === null",
            arg=run_b,
        )
        assert await other.is_hidden("#tracked-operation")
        assert await other.is_hidden("#operation-unreachable")
        assert_no_page_errors(other)
    finally:
        await other.close()


# ===========================================================================
# An undecided result is neither an absent operation nor a refusal
# ===========================================================================
#
# The last review finding: everything that could not be read was being reported as
# something that HAD been read. A database error answered 200 with an empty snapshot,
# so a page reloading during the hiccup concluded there was no operation and threw
# away the id of a freeze that was queued in that very database; and a 502, a
# truncated body or an incomplete envelope reached the confirmation as
# "Отказ: неизвестно" — a refusal nobody had issued, about a stage that had in fact
# been committed.
#
# The substitutions below are made at the test boundary only: the runner's status read
# is replaced with one that raises, and the confirmation's RESPONSE is replaced after
# the real request has reached the real server. Every operator action, and everything
# asserted about what they see, goes through the browser.


@contextlib.contextmanager
def _status_read_broken():
    """A database that cannot answer the status read, for the length of the block.

    Substituted at the runner seam and restored by hand rather than through
    ``monkeypatch.undo()``: that would also undo what the fixtures patched for this
    test — the session factory the in-process app uses, and the production
    configuration — and the page would then be looking at a different world rather
    than at a database that had recovered.
    """
    from sqlalchemy.exc import OperationalError

    async def boom(*_args, **_kwargs):
        raise OperationalError("SELECT 1", {}, Exception("connection reset"))

    original = production_runner.run_status
    production_runner.run_status = boom
    try:
        yield
    finally:
        production_runner.run_status = original


async def _confirm_answer_replaced_by(page, replacement) -> dict[str, int]:
    """Let the confirmation reach the server, then hand the browser *replacement*.

    ``route.fetch()`` performs the real request — same cookie, same CSRF header, same
    body — so the operation really is committed. Only the answer is substituted, which
    is exactly the failure being reproduced: the work happened and the browser cannot
    tell.
    """
    seen = {"posts": 0}

    async def handler(route):
        seen["posts"] += 1
        await route.fetch()
        await route.fulfill(**replacement)

    await page.route("**/api/confirm", handler)
    return seen


async def _freeze_up_to_the_confirmation(page, *, run_id: int, count: int) -> None:
    await page.goto(f"/ops/voucher-mailings/prepare?preview_run_id={run_id}")
    await page.click("#btn-load")
    await page.wait_for_selector("#freeze-panel:not(.d-none)")
    await page.fill("#f-count", str(count))
    await page.fill("#f-euro", f"{count * 1000 / 100:.2f}")
    await page.click("#btn-plan-freeze")
    await page.wait_for_selector("#confirm-panel:not(.d-none)")


async def _unconfirmed_result_is_shown(page) -> None:
    """The page says the result is unknown — never that it was refused."""
    await page.wait_for_function(
        "() => { const el = document.querySelector('#alert-area');"
        " return el && el.innerText.includes('Результат не подтверждён'); }"
    )
    assert "Отказ" not in await page.inner_text("#alert-area")


async def _one_operation_recovered_and_finished(page, session_maker, *, run_id: int, count: int, seen) -> None:
    """One confirm, one operation, found by a READ, and the right batch afterwards."""
    await page.wait_for_selector("#tracked-operation")
    assert seen["posts"] == 1, f"the confirmation was sent {seen['posts']} times"
    operations = await operations_module.list_operations(session_maker, campaign_run_id=run_id)
    assert len(operations) == 1, operations

    await page.unroute("**/api/confirm")
    await _drain(session_maker)
    await page.wait_for_url(_MAILING_URL, timeout=20_000)
    batch_id = int(page.url.rstrip("/").rsplit("/", 1)[-1])
    snapshot = await ledger_module.load(session_maker, batch_id=batch_id)
    assert snapshot.recipient_count == count
    assert len(await operations_module.list_operations(session_maker, batch_id=batch_id)) == 1
    # Nothing was queued a second time, so nothing can reach EasyWeek or Meta twice.
    assert await _drain(session_maker) is None, "a second operation was queued"
    assert_no_page_errors(page)


async def test_an_unreadable_status_is_not_an_absent_operation(
    session_maker, production_configuration, binding_key, executor_enabled, page, transports, ops_server
):
    """Scenario A: a queued FREEZE, and the status read fails.

    The reviewed endpoint answered 200 with empty ``batch`` and ``operations``, which a
    page cannot tell from "this preview has nothing" — so it cleared its tracking and
    showed an empty panel while the operation sat in the database.
    """
    count = 2
    run_id, reader = await _seed(session_maker, count=count)
    quiet_run_id, _quiet_reader = await _seed(session_maker, count=1, offset=30)
    transports.use(reader=reader)
    await _freeze_up_to_the_confirmation(page, run_id=run_id, count=count)
    await _press_confirm(page)
    await page.wait_for_selector("#tracked-operation")
    assert len(await operations_module.list_operations(session_maker, campaign_run_id=run_id)) == 1

    with _status_read_broken():
        # A tab with nothing of its own cannot know the id, and must not pretend to
        # know that there is none.
        blind = await tab_with_no_cache(page, ops_server)
        try:
            await blind.goto(f"/ops/voucher-mailings/prepare?preview_run_id={run_id}")
            await blind.wait_for_selector("#operation-unreachable")
            assert await blind.is_hidden("#tracked-operation")
            assert_no_page_errors(blind)
        finally:
            await blind.context.close()
        # The operation is exactly where it was.
        assert len(await operations_module.list_operations(session_maker, campaign_run_id=run_id)) == 1

        # The tab that does know the id keeps it, and finds the same operation again.
        await page.reload()
        await page.wait_for_selector("#tracked-operation")
        assert "очереди" in await page.inner_text("#tracked-operation")
        kept = await page.evaluate("(run) => window.sessionStorage.getItem('ew-voucher-op-' + run)", run_id)
        assert kept is not None and int(kept) > 0, "the known operation id was thrown away"

    # The database comes back: the same operation, then its batch.
    await page.reload()
    await page.wait_for_selector("#tracked-operation")
    await _drain(session_maker)
    await page.wait_for_url(_MAILING_URL, timeout=20_000)
    batch_id = int(page.url.rstrip("/").rsplit("/", 1)[-1])
    assert (await ledger_module.load_for_preview(session_maker, campaign_run_id=run_id)).batch_id == batch_id
    assert len(await operations_module.list_operations(session_maker, batch_id=batch_id)) == 1

    # And a genuinely empty preview still reads as empty — unavailability and absence
    # are two different answers, not one.
    empty = await tab_with_no_cache(page, ops_server)
    try:
        await empty.goto(f"/ops/voucher-mailings/prepare?preview_run_id={quiet_run_id}")
        await empty.wait_for_selector("#composition-panel")
        await empty.wait_for_function(
            "(run) => window.sessionStorage.getItem('ew-voucher-op-' + run) === null",
            arg=quiet_run_id,
        )
        assert await empty.is_hidden("#tracked-operation")
        assert await empty.is_hidden("#operation-unreachable")
        assert_no_page_errors(empty)
    finally:
        await empty.context.close()
    assert_no_page_errors(page)


async def test_a_502_page_in_place_of_a_confirm_answer_is_not_a_refusal(
    session_maker, production_configuration, binding_key, executor_enabled, page, transports
):
    """Scenario B: the operation is committed and the browser is handed 502 HTML."""
    count = 2
    run_id, reader = await _seed(session_maker, count=count)
    transports.use(reader=reader)
    seen = await _confirm_answer_replaced_by(
        page,
        {
            "status": 502,
            "content_type": "text/html",
            "body": "<html><head><title>502</title></head><body>Bad Gateway</body></html>",
        },
    )
    await _freeze_up_to_the_confirmation(page, run_id=run_id, count=count)
    await _press_confirm(page)

    await _unconfirmed_result_is_shown(page)
    await _one_operation_recovered_and_finished(page, session_maker, run_id=run_id, count=count, seen=seen)


async def test_a_truncated_confirm_answer_is_not_a_refusal(
    session_maker, production_configuration, binding_key, executor_enabled, page, transports
):
    """Scenario C: HTTP 200, and a JSON body that stops in the middle."""
    count = 2
    run_id, reader = await _seed(session_maker, count=count)
    transports.use(reader=reader)
    seen = await _confirm_answer_replaced_by(
        page,
        {
            "status": 200,
            "content_type": "application/json",
            "body": '{"accepted": true, "created": true, "operation": {"operation_id"',
        },
    )
    await _freeze_up_to_the_confirmation(page, run_id=run_id, count=count)
    await _press_confirm(page)

    await _unconfirmed_result_is_shown(page)
    await _one_operation_recovered_and_finished(page, session_maker, run_id=run_id, count=count, seen=seen)


async def test_an_incomplete_success_envelope_is_not_believed_either_way(
    session_maker, production_configuration, binding_key, executor_enabled, page, transports
):
    """Scenario D: valid JSON, ``accepted: true``, and no operation to point at.

    Neither a refusal nor a success: there is no operation id in it, so the page has
    nothing to watch and must go and read what exists rather than guess which.
    """
    count = 2
    run_id, reader = await _seed(session_maker, count=count)
    transports.use(reader=reader)
    seen = await _confirm_answer_replaced_by(
        page,
        {
            "status": 200,
            "content_type": "application/json",
            "body": '{"accepted": true, "created": true, "reasons": [], "operation": {"stage": "freeze"}}',
        },
    )
    await _freeze_up_to_the_confirmation(page, run_id=run_id, count=count)
    await _press_confirm(page)

    await _unconfirmed_result_is_shown(page)
    await _one_operation_recovered_and_finished(page, session_maker, run_id=run_id, count=count, seen=seen)


async def test_a_real_refusal_is_still_reported_as_a_refusal(
    session_maker, production_configuration, binding_key, executor_enabled, page, transports
):
    """The other half of the distinction, and the one it must not swallow.

    The refusal here is the server's own: the confirmation's REQUEST is altered so it
    states a count the approval does not authorise, and the real
    :func:`confirm_stage` refuses it with its own reason. Nothing is faked about the
    answer — and nothing must be created.
    """
    count = 2
    run_id, reader = await _seed(session_maker, count=count)
    transports.use(reader=reader)

    async def miscount(route):
        import json as json_module

        payload = json_module.loads(route.request.post_data or "{}")
        payload["confirmed_count"] = int(payload.get("confirmed_count") or 0) + 1
        await route.continue_(post_data=json_module.dumps(payload))

    await page.route("**/api/confirm", miscount)
    await _freeze_up_to_the_confirmation(page, run_id=run_id, count=count)
    await _press_confirm(page)

    await page.wait_for_function(
        "() => { const el = document.querySelector('#alert-area'); return el && el.innerText.includes('Отказ'); }"
    )
    shown = await page.inner_text("#alert-area")
    assert "voucher_production_approval_count_unconfirmed" in shown, shown
    assert "не подтверждён" not in shown, shown
    # A refusal creates nothing, and leaves nothing to watch.
    assert await operations_module.list_operations(session_maker, campaign_run_id=run_id) == []
    assert (await ledger_module.load_for_preview(session_maker, campaign_run_id=run_id)).exists is False
    assert await page.is_hidden("#tracked-operation")
    assert_no_page_errors(page)


async def test_a_status_error_does_not_blank_a_mailings_progress(
    session_maker, production_configuration, binding_key, executor_enabled, page, transports
):
    """The mailing page keeps what was proven when the next read fails.

    Repainting from an unavailable answer would empty the slots table and zero the
    delivery counters of a mailing that is running — the same confusion as scenario A,
    on the screen where the money is.
    """
    count = 2
    run_id, reader = await _seed(session_maker, count=count)
    transports.use(reader=reader)
    batch_id = await _freeze_through_browser(page, session_maker, transports, run_id=run_id, count=count)
    reader.orders.update(await marker_orders(session_maker, batch_id=batch_id))
    mutator = FakeMutator(create_sequence=[_ok(i) for i in range(count)])
    transports.use(reader=reader, mutator=mutator)
    await _run_stage_through_browser(page, session_maker, button="btn-stage-create")
    await _wait_for_slots_text(page, "created")
    rows_before = await page.locator("#slots-table tbody tr").count()
    assert rows_before == count

    with _status_read_broken():
        await page.wait_for_selector("#status-unavailable")

        # The progress is still the progress.
        assert await page.locator("#slots-table tbody tr").count() == rows_before
        assert "created" in await page.inner_text("#slots-table")
        assert "Получателей нет" not in await page.inner_text("#slots-panel")

    # And it repaints again once the read works.
    await page.wait_for_selector("#status-unavailable", state="hidden")
    assert await page.locator("#slots-table tbody tr").count() == rows_before
    assert mutator.calls.count("create") == count
    assert_no_page_errors(page)


async def test_operator_adds_checked_subset_to_earned_preview_and_delivers_mixed_mailing(
    session_maker,
    production_configuration,
    binding_key,
    executor_enabled,
    page,
    transports,
    monkeypatch,
):
    """Paste → read results → explicit subset → four stage confirmations, all in UI."""
    from sqlalchemy import func, select

    import altegio_bot.ops.voucher_mailing as voucher_ops
    from altegio_bot.models.models import CampaignRecipient, Client
    from altegio_bot.settings import settings
    from altegio_bot.tests.easyweek_voucher_mixed_ui_fixtures import MixedReader as HistoricalMixedReader
    from altegio_bot.tests.easyweek_voucher_mixed_ui_fixtures import seed_mixed_editor
    from altegio_bot.tests.easyweek_voucher_production_fixtures import CUSTOMER_UUIDS, PHONES

    monkeypatch.setattr(settings, "easyweek_allowed_service_categories", '["Wimpernverlängerung"]')
    run_id, earned_id = await seed_mixed_editor(session_maker)

    class MixedReader(HistoricalMixedReader, FakeReader):
        """Reuse mixed audience proof with the explicit new product reader."""

    await seed_template_and_sender(session_maker)
    reader = MixedReader(count=3)
    # Third contact's malformed history is never interpreted as empty.
    reader.history[CUSTOMER_UUIDS[2]] = {"data": [], "meta": {"total": 0}}
    transports.use(reader=reader)
    monkeypatch.setattr(voucher_ops, "EasyWeekClient", lambda: reader)
    await page.goto(f"/ops/campaigns/{run_id}")
    assert "Automatic: 1" in await page.inner_text("#preview-basis-counts")
    async with session_maker() as session:
        initial_client_count = await session.scalar(select(func.count()).select_from(Client))
    await page.fill("#bulk-phones", "\n".join([PHONES[0], PHONES[1], PHONES[1], PHONES[2]]))
    await page.check("#bulk-altegio")
    await page.check("#bulk-karlsruhe")
    await page.click("#bulk-check")
    await page.wait_for_selector("#bulk-result-table")
    results = await page.inner_text("#bulk-result-table")
    assert "Можно добавить" in results and "Уже присутствует" in results and "Повтор строки" in results
    assert "Историю EasyWeek не удалось доказать" in results
    assert await page.is_disabled("#bulk-confirm")
    async with session_maker() as session:
        assert await session.scalar(select(func.count()).select_from(Client)) == initial_client_count
        assert await session.scalar(select(func.count()).select_from(CampaignRecipient)) == 1
    await page.check("#bulk-subset-confirmed")
    await page.click("#bulk-confirm")
    await page.wait_for_function("document.querySelector('#bulk-status').innerText.includes('Состав добавлен: 1')")
    await page.reload()
    assert await page.is_hidden("#bulk-confirm-panel")
    assert "manual: 1" in await page.inner_text("#preview-basis-counts")
    async with session_maker() as session:
        earned = await session.get(CampaignRecipient, earned_id)
        assert earned.recipient_basis == "earned_first_visit"
        assert earned.source_booking_uuid is not None
        assert await session.scalar(select(func.count()).select_from(CampaignRecipient)) == 2

    # Navigate by the page's actual action; do not hand-copy ids into the mailing URL.
    await page.click("#preview-vouchers-link")
    await page.wait_for_url(re.compile(r"/ops/voucher-mailings/prepare\?preview_run_id=\d+$"))
    await page.click("#btn-load")
    await page.wait_for_selector("#composition-table")
    assert (await page.inner_text("#c-count")).strip() == "2"
    composition_text = await page.inner_text("#composition-table")
    assert "доказанный первый визит" in composition_text and "заявление о визите Altegio" in composition_text
    await page.fill("#f-count", "2")
    await page.fill("#f-euro", "20.00")
    await page.click("#btn-plan-freeze")
    await page.wait_for_selector("#confirm-panel:not(.d-none)")
    await _press_confirm(page)
    await page.wait_for_selector("#tracked-operation")
    await _drain(session_maker)
    await page.wait_for_url(_MAILING_URL, timeout=20_000)
    batch_id = int(page.url.rstrip("/").rsplit("/", 1)[-1])
    reader.orders.update(await marker_orders(session_maker, batch_id=batch_id))
    transports.use(reader=reader, mutator=FakeMutator(create_sequence=[_ok(0), _ok(1)]))
    await _run_stage_through_browser(page, session_maker, button="btn-stage-create")
    await _wait_for_slots_text(page, "created")
    settles = await marker_orders(session_maker, batch_id=batch_id, status="paid")
    transports.use(reader=reader, mutator=FakeMutator(pay_sequence=[_ok(0), _ok(1)], reader=reader, settles=settles))
    await _run_stage_through_browser(page, session_maker, button="btn-stage-pay")
    await _wait_for_slots_text(page, "paid")
    sender = FakeSender()
    transports.use(reader=reader, sender=sender)
    await _run_stage_through_browser(page, session_maker, button="btn-stage-deliver")
    await _wait_for_slots_text(page, "provider_accepted")
    assert sender.calls == 2
    await page.reload()
    await _wait_for_slots_text(page, "provider_accepted")
    assert sender.calls == 2
    delivery = await page.inner_text("#delivery-panel")
    assert "Meta приняла" in delivery and "Прочитано" in delivery and "0 из" in delivery
    assert_no_page_errors(page)


async def test_the_browser_states_the_terms_and_claims_no_proof_of_its_own(
    session_maker, production_configuration, binding_key, executor_enabled, page, transports
):
    """§45.4, in the operator's real browser, on the real default guard.

    Supersedes ``test_the_browser_refuses_to_issue_a_voucher_it_could_not_deliver``.
    That test's refusal is the one the owner removed, so it could not simply be
    reworded — what it protected is kept and asserted here instead: the terms an
    operator reads before buying, the honest arithmetic on the confirmation, and the
    page never claiming a proof this application does not perform.

    Nothing is bought: the confirmation is cancelled rather than approved, which is
    also the proof that reaching it is not itself an effect.
    """
    run_id, reader = await _seed(session_maker, count=1)
    transports.use(reader=reader)
    batch_id = await _freeze_through_browser(page, session_maker, transports, run_id=run_id, count=1)
    terms = await page.inner_text(".voucher-terms")
    assert "10 EUR" in terms
    assert "календарный месяц с активации" in terms
    assert "не с получения WhatsApp" in terms
    assert "10 €" in await page.inner_text("pre")
    body = await page.inner_text("body")
    assert "kitilash_ka_new_client_voucher_10eur_v2" in body
    # Who answers for the term, said out loud — and no fictional proof anywhere.
    assert "Срок действия и погашение контролируются EasyWeek" in body
    assert "не подтверждает дату активации" in body
    assert "voucher_production_validity_capability_unproven" not in await page.content()
    assert "issued_validity_capability_proven" not in await page.content()
    assert "issued_validity_proven" not in await page.content()

    reader.orders.update(await marker_orders(session_maker, batch_id=batch_id))
    # A mutator is installed so a create that DID happen would show in its call list
    # rather than failing for want of a transport.
    mutator = FakeMutator(create_sequence=[_ok(0)])
    transports.use(reader=reader, mutator=mutator)
    await page.wait_for_selector("#btn-stage-create:not([disabled])")
    await page.click("#btn-stage-create")
    # The step an operator may now take, with the money it commits spelled out.
    await page.wait_for_selector("#confirm-panel:not(.d-none)")
    assert "10.00 €" in await page.inner_text("#confirm-summary")
    await page.click("text=Отмена")
    await page.wait_for_selector("#confirm-panel.d-none", state="attached")

    # Cancelled, so nothing was bought and nothing needs reconciling.
    assert mutator.create_calls == [] and mutator.pay_calls == []
    snapshot = await ledger_module.load_for_preview(session_maker, campaign_run_id=run_id)
    assert snapshot.items[0].status == "planned"
    assert snapshot.items[0].send_attempt_count == 0
    assert not snapshot.halted and not snapshot.reconciliation_required
    assert_no_page_errors(page)


async def test_a_voucher_easyweek_calls_expired_stays_paid_and_unsent_in_browser(
    session_maker, production_configuration, binding_key, executor_enabled, page, transports
):
    """§45.4. The invalidity signal EasyWeek actually publishes still stops a send.

    Replaces ``test_an_issued_voucher_with_an_unproven_term_stays_paid_and_unsent_in_browser``,
    whose premise — an artifact with no dates — is now the normal case. The invariant
    it was protecting is unchanged: the browser refuses DELIVER, the attempt is not
    spent, and the money stays recoverable through the refund the page still offers.
    """
    run_id, reader = await _seed(session_maker, count=1)
    transports.use(reader=reader)
    batch_id = await _freeze_through_browser(page, session_maker, transports, run_id=run_id, count=1)
    reader.orders.update(await marker_orders(session_maker, batch_id=batch_id))
    transports.use(reader=reader, mutator=FakeMutator(create_sequence=[_ok(0)]))
    await _run_stage_through_browser(page, session_maker, button="btn-stage-create")
    settles = await marker_orders(session_maker, batch_id=batch_id, status="paid")
    transports.use(reader=reader, mutator=FakeMutator(pay_sequence=[_ok(0)], reader=reader, settles=settles))
    await _run_stage_through_browser(page, session_maker, button="btn-stage-pay")
    await _wait_for_slots_text(page, "paid")

    # EasyWeek now says this voucher is unusable.
    for order in reader.orders.values():
        if isinstance(order, dict):
            for voucher in order.get("vouchers") or []:
                voucher["is_expired"] = True

    sender = FakeSender()
    transports.use(reader=reader, sender=sender)
    await page.wait_for_selector("#btn-stage-deliver:not([disabled])")
    await page.click("#btn-stage-deliver")
    await page.wait_for_function(
        "document.querySelector('#alert-area').innerText.includes('EasyWeek сообщает, что ваучер недействителен')"
    )
    assert await page.is_hidden("#confirm-panel")
    assert sender.calls == 0
    snapshot = await ledger_module.load_for_preview(session_maker, campaign_run_id=run_id)
    assert snapshot.items[0].status == "paid"
    assert snapshot.items[0].send_attempt_count == 0
    # The money is still recoverable: the slot still offers its refund.
    assert await page.locator("#slots-table button[data-slot='1']").count() == 1
    assert_no_page_errors(page)
