"""Тесты LRU-кеша в service_filter.py.

Проверяет:
- При достижении лимита вытесняется самая старая запись (LRU), а не весь кеш.
- Доступ к записи обновляет её приоритет (MRU).
- Кеш ограничен по размеру и предсказуем.
"""

from __future__ import annotations

from altegio_bot.service_filter import _CACHE_MAX_SIZE, _LRU_CACHE, _cache_get, _cache_put


def _clear_cache() -> None:
    """Очистить кеш перед тестом."""
    _LRU_CACHE.clear()


def test_cache_basic_put_get() -> None:
    """Базовый put/get."""
    _clear_cache()
    _cache_put((1, 100), 999)
    assert _cache_get((1, 100)) == 999


def test_cache_miss_returns_none() -> None:
    """Отсутствующий ключ возвращает None."""
    _clear_cache()
    assert _cache_get((1, 9999)) is None


def test_cache_evicts_lru_not_full_clear() -> None:
    """При переполнении вытесняется самая старая запись, а не весь кеш."""
    _clear_cache()
    # Заполнить кеш до лимита
    for i in range(_CACHE_MAX_SIZE):
        _cache_put((1, i), i * 10)

    assert len(_LRU_CACHE) == _CACHE_MAX_SIZE

    # Первая добавленная запись (key=(1,0)) должна быть самой старой
    # Обновим все кроме (1,0), чтобы она осталась LRU
    # (уже достигнут лимит, следующий put вытеснит (1,0))

    # Добавляем новую запись
    _cache_put((2, 99999), 12345)

    # Кеш не должен был очиститься полностью
    assert len(_LRU_CACHE) == _CACHE_MAX_SIZE

    # Должна быть вытеснена самая старая (1,0)
    assert _cache_get((1, 0)) is None

    # Все остальные должны остаться
    assert _cache_get((1, 1)) == 10
    assert _cache_get((1, _CACHE_MAX_SIZE - 1)) is not None

    # Новый ключ тоже должен быть в кеше
    assert _cache_get((2, 99999)) == 12345


def test_cache_access_updates_lru_order() -> None:
    """Доступ к записи делает её MRU (не вытесняется первой)."""
    _clear_cache()

    # Добавить 3 записи: (1,1), (1,2), (1,3)
    _cache_put((1, 1), 11)
    _cache_put((1, 2), 22)
    _cache_put((1, 3), 33)

    # Обратиться к (1,1) — теперь она MRU
    # Порядок LRU→MRU: (1,2), (1,3), (1,1)
    assert _cache_get((1, 1)) == 11

    # Заполнить кеш ровно до лимита
    # Уже есть 3 записи, нужно ещё _CACHE_MAX_SIZE - 3
    for i in range(_CACHE_MAX_SIZE - 3):
        _cache_put((2, i), i)

    assert len(_LRU_CACHE) == _CACHE_MAX_SIZE

    # Добавить ещё одну — должна вытеснить самую старую (1,2)
    _cache_put((3, 999), 9999)

    assert len(_LRU_CACHE) == _CACHE_MAX_SIZE

    # (1,2) должна быть вытеснена как LRU
    assert _cache_get((1, 2)) is None

    # (1,1) должна остаться (обращались к ней после (1,2))
    assert _cache_get((1, 1)) == 11


def test_cache_update_existing_key() -> None:
    """Обновление существующего ключа меняет значение без дублирования."""
    _clear_cache()
    _cache_put((1, 42), 100)
    _cache_put((1, 42), 200)

    assert _cache_get((1, 42)) == 200
    assert len(_LRU_CACHE) == 1


def test_cache_size_bounded() -> None:
    """Размер кеша не превышает _CACHE_MAX_SIZE."""
    _clear_cache()
    for i in range(_CACHE_MAX_SIZE + 100):
        _cache_put((1, i), i)

    assert len(_LRU_CACHE) <= _CACHE_MAX_SIZE
