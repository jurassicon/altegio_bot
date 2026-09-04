"""What a migrated booking is made of, as one immutable structure (plan §30.12).

Why this exists
---------------
Until the cart contract, a migratable booking had exactly one service, so the
decision carried it as a handful of loose scalars: one service uuid, one
duration, one price. Adding a second service by adding a second set of scalars
would have been the wrong shape twice over — the two sets could drift apart, and
every consumer would have had to remember which of them applied.

So a decision now carries a **sequence of bindings**. One binding is everything
proven about one service on one booking: what Altegio called it, which EasyWeek
catalogue entry it maps to, and the reviewed baseline that mapping was accepted
under. A single-service booking has one; a cart booking has two.

The order is the contract
-------------------------
Bindings are ordered as the SOURCE lists them, and that order is canonical
everywhere: in the fingerprint, in the plan digest, in the request body, in the
report and in reconciliation. It is not sorted, because the request sends the
services in a sequence and a booking whose two services swapped places is a
different request — one that a previously reviewed plan never authorised.

Nothing here decides anything. It records what was proven, and refuses to answer
questions that do not apply to the shape it is holding.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Final

# The two mutation contracts this migration can execute. Each one is authorised
# by its own canary proof: a single-service canary says nothing about the cart
# endpoint, and a cart canary says nothing about the plain one.
MUTATION_SINGLE: Final = "single"
MUTATION_CART_TWO: Final = "cart_two"

MUTATION_KINDS: Final[frozenset[str]] = frozenset({MUTATION_SINGLE, MUTATION_CART_TWO})

# How many services each contract carries. A booking that does not match its
# kind's count is not a booking this migration knows how to write.
SERVICES_PER_KIND: Final[dict[str, int]] = {MUTATION_SINGLE: 1, MUTATION_CART_TWO: 2}


class BindingError(ValueError):
    """A question asked of the wrong shape. Never a reason to guess an answer."""


@dataclass(frozen=True)
class ServiceBinding:
    """One service on one booking: source id, target uuid, reviewed baseline.

    Every field is proven before it gets here — the mapping comes from the
    manifest an operator reviewed, and the baseline is what the live catalogue
    was checked against. Nothing in it is derived at write time.

    PII-free: an Altegio service id, an EasyWeek uuid, a normalised catalogue
    name, a currency and two numbers. A catalogue name is a salon's own price
    list, not a customer's data, and it appears only in operator-facing output.
    """

    altegio_service_id: int
    easyweek_service_uuid: str
    normalized_name: str
    currency: str
    catalog_price_minor: int
    catalog_duration_minutes: int
    # The master who performs THIS service. Held per binding rather than per
    # booking because the request states it per service — and because the one
    # thing the cart canary proved is a single staffer across both lines, which
    # is a fact worth being able to check rather than to assume.
    staffer_uuid: str

    def digest_material(self) -> tuple[str, ...]:
        """The parts a fingerprint or a plan digest folds in, in fixed order."""
        return (
            str(self.altegio_service_id),
            self.easyweek_service_uuid,
            self.normalized_name,
            self.currency,
            str(self.catalog_price_minor),
            str(self.catalog_duration_minutes),
            self.staffer_uuid,
        )

    def as_safe_dict(self) -> dict[str, Any]:
        """Ids, a uuid and numbers. Safe for a machine report."""
        return {
            "altegio_service_id": self.altegio_service_id,
            "easyweek_service_uuid": self.easyweek_service_uuid,
            "currency": self.currency,
            "catalog_price_minor_units": self.catalog_price_minor,
            "catalog_duration_minutes": self.catalog_duration_minutes,
            "easyweek_staffer_uuid": self.staffer_uuid,
        }

    def as_operator_dict(self) -> dict[str, Any]:
        """The same, plus the catalogue name a person needs to recognise it."""
        payload = self.as_safe_dict()
        payload["service_name"] = self.normalized_name
        return payload


def validate_bindings(kind: str, bindings: tuple[ServiceBinding, ...]) -> None:
    """Refuse a binding set that does not match its own contract.

    Checked here rather than trusted from the caller: these are the conditions
    the cart canary actually proved, and a set that violates one of them is a
    booking nobody has evidence for.
    """
    if kind not in MUTATION_KINDS:
        raise BindingError(f"unknown mutation kind: {kind!r}")
    expected = SERVICES_PER_KIND[kind]
    if len(bindings) != expected:
        raise BindingError(f"{kind} needs exactly {expected} service(s), got {len(bindings)}")

    if kind == MUTATION_CART_TWO:
        if bindings[0].easyweek_service_uuid == bindings[1].easyweek_service_uuid:
            # Two lines pointing at one catalogue entry. The canary proved two
            # DIFFERENT services; a doubled one is a quantity question nobody
            # has an answer for.
            raise BindingError("a cart booking needs two different services")
        if bindings[0].altegio_service_id == bindings[1].altegio_service_id:
            raise BindingError("a cart booking needs two different source services")
        if bindings[0].staffer_uuid != bindings[1].staffer_uuid:
            # The canary proved one staffer across both lines and nothing else.
            raise BindingError("a cart booking needs the same staffer for both services")
        if bindings[0].currency != bindings[1].currency:
            raise BindingError("a cart booking needs one currency")


def total_duration_minutes(bindings: tuple[ServiceBinding, ...]) -> int:
    return sum(item.catalog_duration_minutes for item in bindings)


def total_price_minor(bindings: tuple[ServiceBinding, ...]) -> int:
    return sum(item.catalog_price_minor for item in bindings)


def service_signatures(bindings: tuple[ServiceBinding, ...]) -> tuple[tuple[str, int, int], ...]:
    """What a readback has to find, per service, in the order they were sent.

    ``(normalized name, price minor, duration minutes)`` — the same triple the
    single-service proof already compares, because ``ordered_services[*].uuid``
    is the ORDER LINE's identifier and not the catalogue service's (plan §28).
    """
    return tuple((item.normalized_name, item.catalog_price_minor, item.catalog_duration_minutes) for item in bindings)
