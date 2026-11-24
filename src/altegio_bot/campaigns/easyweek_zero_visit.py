"""One narrow diagnostic: a ``booking-succeeded`` that reported ``visits_total`` 0.

What production showed
----------------------
A processed ``booking-succeeded`` delivery whose body carried the JSON integer
``0`` in ``visits_total``. The local counter and its timestamp stayed NULL,
because the worker refuses to store a count a finished visit contradicts. The
same booking's full customer history, read later through the API, showed one
completed non-canceled visit.

Why EasyWeek sent a zero is **not known**, and nothing here guesses. This module
does not assume a delayed counter update, does not assume zero means "first
visit", and does not substitute 1 anywhere.

What this changes, and what it deliberately does not
----------------------------------------------------
Before, the segmenter counted ``visits_total_out_of_range`` in the aggregate and
moved on, so the person never appeared in the preview at all: an operator could
see that four deliveries had been refused and could not see **who** to look at.

So this adds exactly one thing: for such a delivery, when the chain
Event -> Record -> Client is provable inside one provider and company and the
Record starts inside the campaign period, the preview keeps **one excluded row**
with a stable reason. The row is reviewed-and-excluded. It does not join the
active audience, does not change the voucher nominal, and gets **no**
:class:`~altegio_bot.campaigns.easyweek_segment.EasyWeekSourceProof`.

The normalizer and the inbox worker are untouched: ``visits_total=0`` is still a
hard, non-retryable rejection there, and ``Client.easyweek_visits_total`` still
has to be at least 1 or NULL. This is a reporting path, not a second contract.

Strictly the integer zero
-------------------------
``False``, ``"0"``, ``0.0``, ``None``, an absent field, a negative number and a
number too large for the column are **not** this case. Each of them says the
producer changed the field's type, encoding or meaning, and each keeps its own
existing refusal. The shared ``visits_total_out_of_range`` code is not enough to
select this branch either: it also covers negatives and overflow.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any, Final, Mapping

from altegio_bot.easyweek_locations import EasyWeekLocation
from altegio_bot.easyweek_normalizer import (
    NormalizationError,
    normalize_succeeded_customer_id,
    normalize_succeeded_event,
)

# The excluded row's stable reason. Fits `campaign_recipients.excluded_reason`
# (and `auto_excluded_reason`, where it is kept as audit after an operator
# includes the person anyway). PII-free by construction: a fixed identifier.
ZERO_VISITS_REPORTED: Final = "easyweek_campaign_source_visits_total_zero"

# What an operator reads instead of the code. One sentence of fact and one of
# consequence; no hypothesis about why the counter was zero.
ZERO_VISITS_REPORTED_TEXT: Final = (
    "EasyWeek подтвердил завершение визита, но передал счётчик 0. Первый визит требует проверки."
)


def payload_reports_zero_visits_total(payload: object) -> bool:
    """Is ``visits_total`` present and exactly the JSON integer ``0``?

    ``type(value) is int`` and not ``isinstance``: ``True`` is an ``int``
    subclass and would otherwise pass, and a boolean in a count field is a
    contract change, not a zero. Everything else — a float, a numeric string,
    ``None``, an absent key, a negative value — is likewise refused here.
    """
    if not isinstance(payload, dict) or "visits_total" not in payload:
        return False
    value = payload["visits_total"]
    return type(value) is int and value == 0


@dataclass(frozen=True)
class ZeroVisitReport:
    """A proven ``booking-succeeded`` whose counter was reported as zero.

    Carries the same four identity fields :class:`BookingIdentity` asks for, so
    the existing record resolution and the existing Event -> Record -> Client
    rule apply to it unchanged.

    ``reported_visits_total`` is always literally ``0``. It is kept as a field
    rather than implied so an operator screen can state the number the delivery
    carried beside the number the API answers, and so nothing in this path can
    be mistaken for a usable counter: there is no ``visits_total`` here, and
    :class:`~altegio_bot.easyweek_normalizer.SucceededVisit` — which guarantees
    at least 1 — is deliberately not reused.
    """

    booking_uuid: uuid.UUID
    booking_id: int
    company_id: int
    customer_id: int
    reported_visits_total: int = 0


def normalize_zero_visit_event(
    *,
    event_hint: str | None,
    payload: Any,
    body_truncated: bool,
    location_registry: Mapping[int, EasyWeekLocation],
) -> ZeroVisitReport:
    """Validate one zero-counter ``booking-succeeded``, reusing the full contract.

    Runs exactly the checks a succeeded delivery already gets — trigger,
    truncation, payload shape, the ``location_id`` + ``location_uuid`` pair as
    one registry identity, the canonical ``uid``, the numeric booking id — and
    then the numeric customer id, through the very functions the counter path
    calls. Nothing is relaxed: a truncated body, a foreign location or a missing
    customer id refuses here as it refuses there.

    Raises :class:`NormalizationError` with
    ``visits_total_out_of_range`` when the payload's ``visits_total`` is not the
    integer zero, so a caller cannot reach this path with any other shape.
    """
    if not payload_reports_zero_visits_total(payload):
        raise NormalizationError(NormalizationError.VISITS_TOTAL_OUT_OF_RANGE)
    succeeded = normalize_succeeded_event(
        event_hint=event_hint,
        payload=payload,
        body_truncated=body_truncated,
        location_registry=location_registry,
    )
    customer_id = normalize_succeeded_customer_id(payload)
    return ZeroVisitReport(
        booking_uuid=succeeded.booking_uuid,
        booking_id=succeeded.booking_id,
        company_id=succeeded.company_id,
        customer_id=customer_id,
    )


__all__ = [
    "ZERO_VISITS_REPORTED",
    "ZERO_VISITS_REPORTED_TEXT",
    "ZeroVisitReport",
    "normalize_zero_visit_event",
    "payload_reports_zero_visits_total",
]
