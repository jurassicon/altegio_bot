"""No value, and no digest of a value, escapes the canary (§35, finding 7).

A plain SHA-256 of a low-entropy secret is not a safeguard. A twelve-character
voucher code, a phone number, an e-mail address and a postcode are all
brute-forceable from their digests in seconds, so a "fingerprint" of one is the
value wearing a hat. This suite therefore asserts, for every class of sensitive
data the canary could meet, that NEITHER the raw value NOR its plain or
truncated SHA-256 reaches an observation, the ledger, stdout, a log record, a
repr or an exception.

The one place a digest is still used is a runtime UUID, and that is deliberate:
122 bits of entropy cannot be walked back from a hash, which is exactly the
property a voucher code does not have.
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import timedelta
from typing import Any

import pytest

from altegio_bot.easyweek_voucher_canary import ledger as ledger_module
from altegio_bot.easyweek_voucher_canary.artifact import (
    PII_SCALAR_KEYS,
    REDACTED_CONTAINER_KEYS,
    observe_artifact,
)
from altegio_bot.easyweek_voucher_canary.plan import RuntimeIdentity, canary_marker, identity_fingerprint
from altegio_bot.easyweek_voucher_identity import EASYWEEK_VOUCHER_TEMPLATE_UUID, SUPPORTED_VOUCHER_PRICE_MINOR
from altegio_bot.tests.easyweek_voucher_canary_fixtures import (
    ACCOUNT_UUID,
    CUSTOMER_UUID,
    ORDER_UUID,
    STAFFER_UUID,
    issued_voucher,
    open_order,
    paid_order,
)
from altegio_bot.utils import utcnow

MARKER = canary_marker()
IDENTITY = RuntimeIdentity(
    customer_uuid=CUSTOMER_UUID,
    staffer_uuid=STAFFER_UUID,
    account_uuid=ACCOUNT_UUID,
)

# Real classes of sensitive data, as they would actually appear.
SENSITIVE: dict[str, str] = {
    "first_name": "Jurij",
    "last_name": "Tscherkassow",
    "phone": "+491701234567",
    "email": "kundin@example.invalid",
    "address": "Kaiserstrasse 12",
    "zip_code": "76133",
    "notes": "Kundin bittet um Rueckruf",
    "voucher_code": "GC-7H2K-9QX4",
    "public_url": "https://kitilash.example.invalid/v/GC-7H2K-9QX4",
    "token": "eyJhbGciOiJIUzI1NiJ9.payload.signature",
}


def _digest_forms(value: str) -> list[str]:
    """Every digest shape a careless implementation might have produced."""
    full = hashlib.sha256(value.encode("utf-8")).hexdigest()
    quoted = hashlib.sha256(json.dumps(value).encode("utf-8")).hexdigest()
    forms = [full, full[:16], full[:12], full[:8], quoted, quoted[:16]]
    return forms


def _forbidden_strings() -> list[str]:
    forbidden: list[str] = []
    for value in SENSITIVE.values():
        forbidden.append(value)
        forbidden.extend(_digest_forms(value))
    return forbidden


def _leaky_order() -> dict[str, Any]:
    return paid_order(
        marker=MARKER,
        customer={
            "uuid": CUSTOMER_UUID,
            "first_name": SENSITIVE["first_name"],
            "last_name": SENSITIVE["last_name"],
            "phone": SENSITIVE["phone"],
            "email": SENSITIVE["email"],
            "address": SENSITIVE["address"],
            "zip_code": SENSITIVE["zip_code"],
        },
        notes=SENSITIVE["notes"],
        vouchers=[
            {
                "uuid": ORDER_UUID,
                "code": SENSITIVE["voucher_code"],
                "public_url": SENSITIVE["public_url"],
                "token": SENSITIVE["token"],
                "customer": {"phone": SENSITIVE["phone"], "email": SENSITIVE["email"]},
            }
        ],
    )


def _observe(payload: Any):
    return observe_artifact(
        payload,
        stage="pii_regression",
        expected_customer_uuid=CUSTOMER_UUID,
        expected_template_uuid=EASYWEEK_VOUCHER_TEMPLATE_UUID,
        expected_price_minor=SUPPORTED_VOUCHER_PRICE_MINOR,
    )


# ---------------------------------------------------------------------------
# The observation itself
# ---------------------------------------------------------------------------


def test_no_sensitive_value_or_its_digest_reaches_an_observation() -> None:
    safe = _observe(_leaky_order()).as_safe_dict()
    printed = json.dumps(safe, sort_keys=True)

    for forbidden in _forbidden_strings():
        assert forbidden not in printed, forbidden


def test_no_sensitive_value_or_its_digest_reaches_a_repr_or_a_string() -> None:
    observation = _observe(_leaky_order())

    for surface in (repr(observation), str(observation), repr(observation.fields)):
        for forbidden in _forbidden_strings():
            assert forbidden not in surface, forbidden


def test_a_customer_subtree_is_marked_redacted_and_never_walked_into() -> None:
    safe = _observe(_leaky_order()).as_safe_dict()
    paths = [entry["path"] for entry in safe["fields"]]

    customer_field = next(entry for entry in safe["fields"] if entry["path"] == "customer")
    assert customer_field["subtree_redacted"] is True
    assert customer_field["json_type"] == "object"
    # Presence and type, and nothing else: no length, and nothing from inside.
    assert "value_length" not in customer_field
    assert not any(path.startswith("customer.") for path in paths)


def test_a_nested_customer_inside_a_voucher_is_redacted_too() -> None:
    safe = _observe(_leaky_order()).as_safe_dict()
    paths = [entry["path"] for entry in safe["fields"]]

    nested = next(entry for entry in safe["fields"] if entry["path"].endswith("].customer"))
    assert nested["subtree_redacted"] is True
    assert not any(".customer." in path for path in paths)


@pytest.mark.parametrize("key", sorted(PII_SCALAR_KEYS))
def test_a_personal_scalar_never_carries_even_a_length(key) -> None:
    """The length of a phone number or a postcode narrows it all by itself."""
    order = open_order(marker=MARKER)
    order[key] = "0123456789"

    safe = _observe(order).as_safe_dict()
    field = next(entry for entry in safe["fields"] if entry["path"] == key)

    assert "value_length" not in field
    assert "value_fingerprint" not in field
    assert field["json_type"] == "string"


@pytest.mark.parametrize("key", sorted(REDACTED_CONTAINER_KEYS))
def test_every_person_container_is_redacted(key) -> None:
    order = open_order(marker=MARKER)
    order[key] = {"phone": SENSITIVE["phone"], "email": SENSITIVE["email"]}

    safe = _observe(order).as_safe_dict()
    printed = json.dumps(safe, sort_keys=True)

    field = next(entry for entry in safe["fields"] if entry["path"] == key)
    assert field["subtree_redacted"] is True
    assert SENSITIVE["phone"] not in printed
    assert SENSITIVE["email"] not in printed


def test_a_voucher_code_keeps_only_its_shape() -> None:
    """Enough to research the artifact; not enough to reconstruct it."""
    order = open_order(marker=MARKER, vouchers=[{"code": SENSITIVE["voucher_code"]}])
    safe = _observe(order).as_safe_dict()

    field = next(entry for entry in safe["fields"] if entry["path"].endswith("code"))
    assert field["json_type"] == "string"
    assert field["value_length"] == len(SENSITIVE["voucher_code"])
    assert "value_fingerprint" not in field


def test_an_unknown_scalar_keeps_neither_a_length_nor_a_digest() -> None:
    """An unrecognised field could be anything, including somebody's number."""
    order = open_order(marker=MARKER, vouchers=[{"mystery_field": SENSITIVE["phone"]}])
    safe = _observe(order).as_safe_dict()
    printed = json.dumps(safe, sort_keys=True)

    field = next(entry for entry in safe["fields"] if entry["path"].endswith("mystery_field"))
    assert field["known_key"] is False
    assert "value_length" not in field
    assert "value_fingerprint" not in field
    assert SENSITIVE["phone"] not in printed


def test_cross_stage_equality_is_reported_as_unproven_rather_than_faked() -> None:
    safe = _observe(_leaky_order()).as_safe_dict()
    assert safe["cross_stage_equality_proven"] is False
    assert safe["artifact_contract_proven"] is False


# ---------------------------------------------------------------------------
# The ledger, stdout and logs
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_nothing_sensitive_reaches_the_ledger_row(session_maker) -> None:
    observation = _observe(_leaky_order()).as_safe_dict()
    await ledger_module.claim_create(
        session_maker,
        create_plan_digest="a" * 64,
        template_config_digest="b" * 64,
        customer_fingerprint=identity_fingerprint("customer", CUSTOMER_UUID),
        staffer_fingerprint=identity_fingerprint("staffer", STAFFER_UUID),
        account_fingerprint=identity_fingerprint("account", ACCOUNT_UUID),
        reconciliation_marker=MARKER,
        create_window_start=utcnow() - timedelta(minutes=10),
        create_window_end=utcnow() + timedelta(hours=6),
    )
    result = await ledger_module.record_outcome(
        session_maker,
        status=ledger_module.STATUS_CREATE_UNKNOWN,
        expected_statuses=frozenset({ledger_module.STATUS_CREATE_CLAIMED}),
        evidence={"paid_readback": observation},
    )

    stored = json.dumps(result.snapshot.as_safe_dict(), sort_keys=True)
    for forbidden in _forbidden_strings():
        assert forbidden not in stored, forbidden


def test_nothing_sensitive_reaches_a_log_record(caplog) -> None:
    caplog.set_level(logging.DEBUG)
    logger = logging.getLogger("easyweek_voucher_canary_pii_probe")

    observation = _observe(_leaky_order())
    # Whatever an operator or a future maintainer logs about an observation,
    # there is nothing sensitive in it to log.
    logger.info("observation=%s", observation.as_safe_dict())

    recorded = "\n".join(record.getMessage() for record in caplog.records)
    for forbidden in _forbidden_strings():
        assert forbidden not in recorded, forbidden


def test_an_exception_carrying_an_observation_still_leaks_nothing() -> None:
    observation = _observe(_leaky_order())
    error = RuntimeError(f"canary failed: {observation.as_safe_dict()}")

    for forbidden in _forbidden_strings():
        assert forbidden not in str(error), forbidden


def test_the_issued_artifact_shape_leaks_neither_its_code_nor_its_digest() -> None:
    """The shape production actually returned, through every safe surface.

    This body is the one the canary now accepts as proof of a single voucher,
    so the code inside it reaches the observation, the ledger evidence and the
    stage report. None of them may carry it, or any digest of it.
    """
    order = open_order(
        marker=MARKER,
        vouchers=[issued_voucher(code=SENSITIVE["voucher_code"], public_url=SENSITIVE["public_url"])],
    )
    observation = _observe(order)
    safe = observation.as_safe_dict()

    assert safe["voucher_line_proven"] is True
    assert safe["voucher_quantity_proof"] == "singleton_issued_artifact"
    for surface in (json.dumps(safe, sort_keys=True), repr(observation), str(observation)):
        for forbidden in _forbidden_strings():
            assert forbidden not in surface, forbidden


def test_the_proof_label_is_a_closed_vocabulary_not_a_value() -> None:
    """Only three strings can ever appear there, and none is data."""
    order = open_order(marker=MARKER, vouchers=[issued_voucher(code=SENSITIVE["voucher_code"])])

    label = _observe(order).as_safe_dict()["voucher_quantity_proof"]

    assert label in {"explicit_quantity", "singleton_issued_artifact", "unproven"}


def test_a_runtime_uuid_is_still_fingerprinted_and_that_is_deliberate() -> None:
    """122 bits of entropy cannot be walked back; a voucher code can."""
    digest = identity_fingerprint("customer", CUSTOMER_UUID)

    assert len(digest) == 64
    assert CUSTOMER_UUID not in digest
    # And the salt makes it useless outside this canary scope.
    assert digest != hashlib.sha256(CUSTOMER_UUID.encode("utf-8")).hexdigest()
