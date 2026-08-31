"""Exact money and duration reading for the cutover (PR-11.1, revision 16).

The first version of the classifier read prices through a helper that returned
``None`` for anything that was not a *positive* number. That collapsed two
completely different facts into one:

* "this service has no price recorded" — nothing to compare, and
* "this booking costs the customer **0**" — a full discount, which is exactly
  the per-booking override the migration must refuse to carry.

So ``cost=90, cost_to_pay=0`` read as "no override" and sailed through as a
90 € booking. The customer had been promised it for free. That is the failure
this module exists to make impossible: **zero is a value, absence is not.**

Two rules follow from it, and both are load-bearing:

1. **Money is compared as :class:`~decimal.Decimal`, never as ``float``.**
   ``0.1 + 0.2 != 0.3`` in binary floating point, and two prices that differ by
   a cent are exactly the case a custom-price check has to catch. Values arrive
   as JSON numbers or numeric strings and are converted through ``str`` so no
   binary rounding happens on the way in.
2. **Anything not a finite, real number is refused, not skipped.** ``NaN``,
   ``±Infinity``, ``bool`` and malformed text are not "missing"; they are a
   source we do not understand, and the booking blocks.

``bool`` deserves its own sentence: ``True == 1`` in Python, so a sloppy check
would read ``"cost": true`` as a one-euro service. ``type(x) is bool`` is tested
before anything else, everywhere.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Context, Decimal, Inexact, InvalidOperation, Rounded
from typing import Any, Final

# Duration is stored and sent in whole minutes, so a source duration that is not
# a whole number of minutes cannot be represented and must not be rounded.
SECONDS_PER_MINUTE: Final = 60


class AmountError(ValueError):
    """A numeric field could not be read as an exact, finite value."""


@dataclass(frozen=True)
class Amount:
    """One exact monetary value, or the explicit fact that there was none.

    ``present=False`` means the field was absent or JSON ``null``. It never means
    zero, and no arithmetic or comparison is allowed against it — callers must
    branch on ``present`` first, which is what makes the missing-baseline case
    impossible to ignore.
    """

    present: bool
    value: Decimal | None = None

    @property
    def is_zero(self) -> bool:
        return self.present and self.value == Decimal(0)


ABSENT: Final = Amount(present=False)


def read_amount(raw: object) -> Amount:
    """Read one money field exactly, or raise :class:`AmountError`.

    Accepts JSON numbers and numeric strings. Returns :data:`ABSENT` only for a
    genuinely missing field (``None`` / key not present) — **never** for ``0``.

    Raises for ``bool``, ``NaN``, ``±Infinity``, negative amounts, and anything
    unparseable. Those are not "no price"; they are a price we cannot trust, and
    the caller turns them into a blocked row rather than a migrated booking.
    """
    if raw is None:
        return ABSENT
    if type(raw) is bool:
        # `True == 1`. A boolean is never a price.
        raise AmountError("amount is a boolean")

    if isinstance(raw, int):
        value = Decimal(raw)
    elif isinstance(raw, float):
        # str() first: Decimal(0.1) is 0.1000000000000000055511151231257827.
        if raw != raw or raw in (float("inf"), float("-inf")):
            raise AmountError("amount is not finite")
        value = Decimal(str(raw))
    elif isinstance(raw, str):
        text = raw.strip()
        if not text:
            return ABSENT
        try:
            value = Decimal(text)
        except InvalidOperation:
            raise AmountError("amount is not a number") from None
    elif isinstance(raw, Decimal):
        value = raw
    else:
        raise AmountError("amount is not a supported type")

    if not value.is_finite():
        raise AmountError("amount is not finite")
    if value < 0:
        # A negative booking price is not a discount we understand.
        raise AmountError("amount is negative")
    return Amount(present=True, value=value)


def amounts_differ(left: Amount, right: Amount) -> bool:
    """True when both amounts are present and numerically different.

    ``Decimal("90") == Decimal("90.00")`` is True, which is what we want: the
    same money written two ways is not an override.
    """
    if not (left.present and right.present):
        return False
    assert left.value is not None and right.value is not None
    return left.value != right.value


# Currencies this migration can express in minor units, and the exponent each
# one uses. A closed list on purpose: "multiply by 100" is an assumption about
# the currency, and EasyWeek returns `price` as an integer of minor units with
# no exponent alongside it. A currency that is not listed here is not a currency
# we can compare exactly, so it fails closed rather than being guessed at.
MINOR_UNIT_EXPONENT: Final[dict[str, int]] = {"EUR": 2}


def to_minor_units(amount: Amount, *, currency: str) -> int:
    """Exact minor-unit integer for *amount* in *currency*, or raise.

    EasyWeek states catalogue and booking prices as an integer number of minor
    units (``12000`` for €120.00); the manifest states them as an exact decimal
    string (``"120.00"``). Comparing the two needs one conversion, and it has to
    be exact in both directions — a price that rounds is a price that could hide
    a per-booking override of less than a cent's worth of difference, which is
    the whole class of change this migration refuses to migrate silently.

    Raises :class:`AmountError` for an absent amount, an unsupported currency, or
    a value with more precision than the currency has (``"1.005"`` in EUR).
    Decimal arithmetic throughout — never float.
    """
    if not amount.present or amount.value is None:
        raise AmountError("amount is absent")
    code = currency.strip().upper() if isinstance(currency, str) else ""
    exponent = MINOR_UNIT_EXPONENT.get(code)
    if exponent is None:
        raise AmountError("currency is not supported for minor-unit comparison")

    scaled = amount.value.scaleb(exponent)
    try:
        # `to_integral_exact` raises Inexact rather than rounding, which is
        # exactly the behaviour we want: "1.005 EUR" has no minor-unit form.
        minor = scaled.to_integral_exact(context=Context(traps=[Inexact, Rounded]))
    except (Inexact, Rounded, InvalidOperation):
        raise AmountError("amount has more precision than the currency") from None
    return int(minor)


@dataclass(frozen=True)
class Duration:
    """A duration in whole minutes, or the explicit absence of one."""

    present: bool
    minutes: int | None = None


DURATION_ABSENT: Final = Duration(present=False)


class DurationError(ValueError):
    """A duration field could not be read as a whole number of minutes."""


def read_duration_seconds(raw: object) -> Duration:
    """Read a duration given in seconds and return it in whole minutes.

    Raises :class:`DurationError` for zero, negative, non-finite, boolean,
    fractional-second and unparseable values, and for any duration that is not a
    whole number of minutes. A booking whose length we would have to round is a
    booking whose length we do not actually know.

    ``present=False`` is returned only for a genuinely absent field, and the
    caller must treat that as "no baseline", never as "no override".
    """
    if raw is None:
        return DURATION_ABSENT
    if type(raw) is bool:
        raise DurationError("duration is a boolean")

    if isinstance(raw, int):
        seconds = Decimal(raw)
    elif isinstance(raw, float):
        if raw != raw or raw in (float("inf"), float("-inf")):
            raise DurationError("duration is not finite")
        seconds = Decimal(str(raw))
    elif isinstance(raw, str):
        text = raw.strip()
        if not text:
            return DURATION_ABSENT
        try:
            seconds = Decimal(text)
        except InvalidOperation:
            raise DurationError("duration is not a number") from None
    else:
        raise DurationError("duration is not a supported type")

    if not seconds.is_finite():
        raise DurationError("duration is not finite")
    if seconds <= 0:
        raise DurationError("duration is not positive")
    if seconds % SECONDS_PER_MINUTE != 0:
        raise DurationError("duration is not a whole number of minutes")

    minutes = int(seconds / SECONDS_PER_MINUTE)
    return Duration(present=True, minutes=minutes)


def read_duration_minutes(raw: object) -> Duration:
    """Read a duration already expressed in whole minutes (manifest catalogue)."""
    if raw is None:
        return DURATION_ABSENT
    if type(raw) is bool:
        raise DurationError("duration is a boolean")
    if not isinstance(raw, int):
        raise DurationError("duration is not an integer number of minutes")
    if raw <= 0:
        raise DurationError("duration is not positive")
    return Duration(present=True, minutes=raw)


def safe_amount_repr(amount: Amount) -> Any:
    """A report-safe rendering. Prices are not PII, but they are still data."""
    if not amount.present:
        return None
    assert amount.value is not None
    return str(amount.value)
