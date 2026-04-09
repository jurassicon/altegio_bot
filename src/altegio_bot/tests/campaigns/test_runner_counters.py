"""Тесты идемпотентности счётчиков в runner._update_run_exclusion_counters."""

from __future__ import annotations

from types import SimpleNamespace

from altegio_bot.campaigns.runner import _update_run_exclusion_counters
from altegio_bot.campaigns.segment import ClientCandidate


def _make_run() -> SimpleNamespace:
    """Создать простой объект-заглушку CampaignRun для unit-теста.

    Используем SimpleNamespace вместо CampaignRun, т.к. SQLAlchemy-модели
    требуют инициализации через сессию (_sa_instance_state).
    _update_run_exclusion_counters работает только с атрибутами счётчиков,
    поэтому SimpleNamespace полностью достаточен.
    """
    return SimpleNamespace(
        total_clients_seen=0,
        candidates_count=0,
        excluded_opted_out=0,
        excluded_no_phone=0,
        excluded_has_records_before=0,
        excluded_multiple_records=0,
        excluded_more_than_one_record=0,
        excluded_no_confirmed_record=0,
        excluded_crm_unavailable=0,
    )


def _make_candidate(excluded_reason: str | None) -> ClientCandidate:
    client = SimpleNamespace(wa_opted_out=False, phone_e164="+491234567890")
    return ClientCandidate(
        client=client,
        total_records_in_period=1,
        confirmed_records_in_period=1,
        lash_records_in_period=1,
        confirmed_lash_records_in_period=1,
        service_titles_in_period=[],
        records_before_period=0,
        local_client_found=True,
        excluded_reason=excluded_reason,
    )


def test_counters_basic_mapping() -> None:
    """Счётчики правильно маппируются на причины исключения."""
    run = _make_run()
    candidates = [
        _make_candidate(None),  # eligible
        _make_candidate("opted_out"),
        _make_candidate("no_phone"),
        _make_candidate("has_records_before_period"),
        _make_candidate("multiple_lash_records_in_period"),
        _make_candidate("no_lash_record_in_period"),
        _make_candidate("no_confirmed_lash_record_in_period"),
        _make_candidate("crm_history_unavailable"),
    ]
    _update_run_exclusion_counters(run, candidates)

    assert run.total_clients_seen == 8
    assert run.candidates_count == 1
    assert run.excluded_opted_out == 1
    assert run.excluded_no_phone == 1
    assert run.excluded_has_records_before == 1
    assert run.excluded_multiple_records == 1
    assert run.excluded_more_than_one_record == 1
    assert run.excluded_no_confirmed_record == 2  # no_lash + no_confirmed_lash
    assert run.excluded_crm_unavailable == 1


def test_counters_idempotent_double_call() -> None:
    """Повторный вызов с теми же candidates не удваивает счётчики."""
    run = _make_run()
    candidates = [
        _make_candidate(None),
        _make_candidate("opted_out"),
        _make_candidate("has_records_before_period"),
    ]

    _update_run_exclusion_counters(run, candidates)
    # Запомнить значения после первого вызова
    first_candidates = run.candidates_count
    first_opted_out = run.excluded_opted_out
    first_before = run.excluded_has_records_before
    first_total = run.total_clients_seen

    # Повторный вызов с теми же candidates
    _update_run_exclusion_counters(run, candidates)

    assert run.candidates_count == first_candidates
    assert run.excluded_opted_out == first_opted_out
    assert run.excluded_has_records_before == first_before
    assert run.total_clients_seen == first_total


def test_counters_reset_before_recalculate() -> None:
    """При повторном вызове с другими candidates — правильно пересчитывает."""
    run = _make_run()
    # Первый набор: 3 eligible
    first_batch = [_make_candidate(None)] * 3
    _update_run_exclusion_counters(run, first_batch)
    assert run.candidates_count == 3

    # Второй набор: 0 eligible, 2 opted_out
    second_batch = [_make_candidate("opted_out")] * 2
    _update_run_exclusion_counters(run, second_batch)

    assert run.candidates_count == 0  # Сброшено и пересчитано
    assert run.excluded_opted_out == 2
    assert run.total_clients_seen == 2


def test_counters_crm_unavailable_mapped() -> None:
    """crm_history_unavailable корректно попадает в excluded_crm_unavailable."""
    run = _make_run()
    candidates = [
        _make_candidate("crm_history_unavailable"),
        _make_candidate("crm_history_unavailable"),
    ]
    _update_run_exclusion_counters(run, candidates)

    assert run.excluded_crm_unavailable == 2
    assert run.candidates_count == 0
    assert run.excluded_has_records_before == 0  # Не смешивается с has_records_before
