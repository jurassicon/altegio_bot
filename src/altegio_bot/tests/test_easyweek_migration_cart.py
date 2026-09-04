"""The two-service cart contract, exactly as a real canary proved it (plan §30.12).

The evidence this suite encodes is narrow and was obtained once, by hand, on the
live workspace: one Altegio record, two different services, one master, one
location, standard prices and standard durations, sent as a single cart item and
read back as a single booking with two order lines totalling 180 minutes and
18000 minor EUR.

Everything wider than that has no evidence, so everything wider than that fails
closed. Most of this file is that boundary: three services, the same service
twice, two masters, a discount, a stretched slot. Each is a shape somebody could
plausibly expect to work, and each would write a real appointment for a real
customer if it did.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from altegio_bot.easyweek_migration.bindings import (
    MUTATION_CART_TWO,
    MUTATION_SINGLE,
    BindingError,
    ServiceBinding,
    service_signatures,
    total_duration_minutes,
    total_price_minor,
    validate_bindings,
)
from altegio_bot.easyweek_migration.classify import (
    BLOCK_CART_UNSUPPORTED,
    BLOCK_CUSTOM_DURATION,
    BLOCK_CUSTOM_PRICE,
    BLOCK_MULTI_SERVICE,
    BLOCK_SERVICE_MAPPING_MISSING,
    BLOCKED,
    READY,
    classify_record,
    source_fingerprint,
)
from altegio_bot.easyweek_migration.cutover import parse_cutover
from altegio_bot.easyweek_migration.manifest import KARLSRUHE_COMPANY_ID, parse_manifest
from altegio_bot.tests.test_easyweek_migration_planning import (
    CUSTOMER_PHONE,
    KA_SERVICE_ID,
    KA_SERVICE_UUID,
    KA_STAFF_ID,
    KA_STAFF_UUID,
    directory_with,
    manifest_text,
    record,
)

# The second half of a cart: another service in the same branch, mapped to its
# own catalogue entry, performed by the same master.
SECOND_SERVICE_ID = 6007
SECOND_SERVICE_UUID = "aaaa1111-2222-4333-8444-555566667777"
SECOND_STAFF_UUID = "bbbb1111-2222-4333-8444-555566667777"
CUTOVER = "2026-09-01T00:00:00Z"


def cart_manifest(**service_overrides: Any) -> Any:
    """The planning manifest, plus a mapping for the cart's second service."""
    payload = json.loads(manifest_text())
    services = payload["branches"][str(KARLSRUHE_COMPANY_ID)]["services"]
    second = {
        "easyweek_service_uuid": SECOND_SERVICE_UUID,
        "catalog_duration_minutes": 120,
        "catalog_price": "90.00",
        "catalog_service_name": "Wimpernverlängerung 2D",
        "catalog_currency": "EUR",
    }
    second.update(service_overrides)
    services[str(SECOND_SERVICE_ID)] = second
    parsed = parse_manifest(json.dumps(payload))
    assert parsed.valid, parsed.reason
    return parsed


def cart_record(**overrides: Any) -> dict[str, Any]:
    """The proven shape: two different standard services, 60 + 120 minutes."""
    base = record(
        services=[
            {"id": KA_SERVICE_ID, "cost": 90.0, "cost_to_pay": 90.0},
            {"id": SECOND_SERVICE_ID, "cost": 90.0, "cost_to_pay": 90.0},
        ],
        seance_length=(60 + 120) * 60,
    )
    base.update(overrides)
    return base


def classify(record_payload: dict[str, Any], *, manifest: Any = None) -> Any:
    return classify_record(
        record_payload,
        company_id=KARLSRUHE_COMPANY_ID,
        manifest=manifest or cart_manifest(),
        directory=directory_with(),
        cutover=parse_cutover(CUTOVER),
        ledger=None,
    )


def binding(**overrides: Any) -> ServiceBinding:
    base: dict[str, Any] = {
        "altegio_service_id": KA_SERVICE_ID,
        "easyweek_service_uuid": KA_SERVICE_UUID,
        "normalized_name": "mascara effekt",
        "currency": "EUR",
        "catalog_price_minor": 9000,
        "catalog_duration_minutes": 60,
        "staffer_uuid": KA_STAFF_UUID,
    }
    base.update(overrides)
    return ServiceBinding(**base)


PAIR = (
    binding(),
    binding(
        altegio_service_id=SECOND_SERVICE_ID,
        easyweek_service_uuid=SECOND_SERVICE_UUID,
        normalized_name="wimpernverlängerung 2d",
        catalog_duration_minutes=120,
    ),
)


# ---------------------------------------------------------------------------
# The proven shape
# ---------------------------------------------------------------------------


def test_two_different_standard_services_are_a_cart_booking() -> None:
    decision = classify(cart_record())

    assert decision.outcome == READY
    assert decision.mutation_kind == MUTATION_CART_TWO
    assert len(decision.bindings) == 2
    assert decision.duration_minutes == 180, "the canary's 180 minutes, as the sum"


def test_a_cart_binding_carries_both_services_in_source_order() -> None:
    [first, second] = classify(cart_record()).bindings

    assert first.altegio_service_id == KA_SERVICE_ID
    assert second.altegio_service_id == SECOND_SERVICE_ID
    assert first.easyweek_service_uuid == KA_SERVICE_UUID
    assert second.easyweek_service_uuid == SECOND_SERVICE_UUID


def test_both_services_carry_the_same_master() -> None:
    """The one staffer arrangement the canary actually created."""
    bindings = classify(cart_record()).bindings

    assert {item.staffer_uuid for item in bindings} == {KA_STAFF_UUID}


def test_a_single_service_booking_is_still_single() -> None:
    """No regression: the existing contract keeps its own kind."""
    decision = classify(record())

    assert decision.outcome == READY
    assert decision.mutation_kind == MUTATION_SINGLE
    assert len(decision.bindings) == 1
    assert decision.easyweek_service_uuid == KA_SERVICE_UUID


def test_a_cart_decision_refuses_the_single_service_convenience() -> None:
    """A caller reaching for "the" service uuid has not been taught about carts."""
    decision = classify(cart_record())

    with pytest.raises(BindingError):
        _ = decision.easyweek_service_uuid


# ---------------------------------------------------------------------------
# Everything wider fails closed
# ---------------------------------------------------------------------------


def test_three_services_are_refused() -> None:
    payload = cart_record()
    payload["services"].append({"id": KA_SERVICE_ID, "cost": 90.0, "cost_to_pay": 90.0})
    payload["seance_length"] = (60 + 120 + 60) * 60

    decision = classify(payload)

    assert decision.outcome == BLOCKED
    assert decision.reason == BLOCK_MULTI_SERVICE


def test_the_same_service_twice_is_refused() -> None:
    """Two lines on one catalogue entry is a quantity question nobody answered."""
    payload = record(
        services=[
            {"id": KA_SERVICE_ID, "cost": 90.0, "cost_to_pay": 90.0},
            {"id": KA_SERVICE_ID, "cost": 90.0, "cost_to_pay": 90.0},
        ],
        seance_length=7200,
    )

    decision = classify(payload)

    assert decision.outcome == BLOCKED
    assert decision.reason == BLOCK_CART_UNSUPPORTED


def test_two_masters_are_refused() -> None:
    """The canary proved one staffer across both lines and nothing else."""
    payload = json.loads(manifest_text())
    branch = payload["branches"][str(KARLSRUHE_COMPANY_ID)]
    branch["selected_altegio_staff_ids"] = [KA_STAFF_ID]
    branch["services"][str(SECOND_SERVICE_ID)] = {
        "easyweek_service_uuid": SECOND_SERVICE_UUID,
        "catalog_duration_minutes": 120,
        "catalog_price": "90.00",
        "catalog_service_name": "Wimpernverlängerung 2D",
        "catalog_currency": "EUR",
    }
    manifest = parse_manifest(json.dumps(payload))

    # Proven at the binding level: the classifier reads one staff id per record,
    # so two masters on one booking can only arrive as a hand-built pair.
    with pytest.raises(BindingError):
        validate_bindings(
            MUTATION_CART_TWO,
            (PAIR[0], binding(**{**PAIR[1].__dict__, "staffer_uuid": SECOND_STAFF_UUID})),
        )
    assert manifest.valid


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("cost_to_pay", 70.0),
        ("discount", 10.0),
        ("first_cost", 120.0),
    ],
)
def test_a_price_override_on_either_service_is_refused(field: str, value: float) -> None:
    for index in (0, 1):
        payload = cart_record()
        payload["services"][index][field] = value

        decision = classify(payload)

        assert decision.outcome == BLOCKED, f"service {index} with {field}"
        assert decision.reason == BLOCK_CUSTOM_PRICE


def test_a_stretched_slot_is_refused() -> None:
    """record.seance_length must equal the SUM of the two catalogue durations."""
    payload = cart_record(seance_length=(60 + 120 + 30) * 60)

    decision = classify(payload)

    assert decision.outcome == BLOCKED
    assert decision.reason == BLOCK_CUSTOM_DURATION


def test_a_shortened_slot_is_refused() -> None:
    decision = classify(cart_record(seance_length=60 * 60))

    assert decision.outcome == BLOCKED
    assert decision.reason == BLOCK_CUSTOM_DURATION


def test_an_unmapped_second_service_is_refused() -> None:
    payload = cart_record()
    payload["services"][1]["id"] = 999999

    decision = classify(payload)

    assert decision.outcome == BLOCKED
    assert decision.reason == BLOCK_SERVICE_MAPPING_MISSING


def test_a_line_duration_disagreeing_with_the_catalogue_is_refused() -> None:
    """A stale manifest baseline, caught on the line rather than on the total."""
    payload = cart_record()
    payload["services"][1]["seance_length"] = 90 * 60

    decision = classify(payload)

    assert decision.outcome == BLOCKED
    assert decision.reason == BLOCK_CUSTOM_DURATION


# ---------------------------------------------------------------------------
# The binding model itself
# ---------------------------------------------------------------------------


def test_a_cart_needs_two_bindings() -> None:
    for wrong in ((), (PAIR[0],), (*PAIR, PAIR[0])):
        with pytest.raises(BindingError):
            validate_bindings(MUTATION_CART_TWO, wrong)


def test_a_single_needs_exactly_one_binding() -> None:
    validate_bindings(MUTATION_SINGLE, (PAIR[0],))
    with pytest.raises(BindingError):
        validate_bindings(MUTATION_SINGLE, PAIR)


def test_two_currencies_are_refused() -> None:
    other = binding(**{**PAIR[1].__dict__, "currency": "CHF"})

    with pytest.raises(BindingError):
        validate_bindings(MUTATION_CART_TWO, (PAIR[0], other))


def test_the_totals_are_the_sums() -> None:
    assert total_duration_minutes(PAIR) == 180
    assert total_price_minor(PAIR) == 18000, "the canary's 18000 minor EUR"


def test_the_signatures_keep_the_sent_order() -> None:
    assert service_signatures(PAIR) == (
        ("mascara effekt", 9000, 60),
        ("wimpernverlängerung 2d", 9000, 120),
    )


# ---------------------------------------------------------------------------
# The fingerprint covers everything that decides what would be written
# ---------------------------------------------------------------------------


def fingerprint_of(**overrides: Any) -> str:
    base: dict[str, Any] = {
        "company_id": KARLSRUHE_COMPANY_ID,
        "record_id": 900001,
        "starts_at_utc": parse_cutover(CUTOVER).at,
        "staff_uuid": KA_STAFF_UUID,
        "customer_uuid": "77777777-7777-4777-8777-777777777777",
        "mutation_kind": MUTATION_CART_TWO,
        "bindings": PAIR,
        "booked_duration_minutes": 180,
    }
    base.update(overrides)
    return source_fingerprint(**base)


def test_the_mutation_kind_is_inside_the_fingerprint() -> None:
    """A single and a cart are different requests, even over the same row."""
    assert fingerprint_of(mutation_kind=MUTATION_SINGLE, bindings=(PAIR[0],)) != fingerprint_of()


def test_swapping_the_two_services_changes_the_fingerprint() -> None:
    """The order is the request body's order, not a set."""
    assert fingerprint_of(bindings=(PAIR[1], PAIR[0])) != fingerprint_of()


@pytest.mark.parametrize(
    "change",
    [
        {"easyweek_service_uuid": "cccc1111-2222-4333-8444-555566667777"},
        {"catalog_price_minor": 9500},
        {"catalog_duration_minutes": 90},
        {"normalized_name": "etwas anderes"},
        {"currency": "CHF"},
        {"altegio_service_id": 6099},
        {"staffer_uuid": SECOND_STAFF_UUID},
    ],
    ids=["target_uuid", "price", "duration", "name", "currency", "source_id", "staffer"],
)
def test_changing_either_service_changes_the_fingerprint(change: dict[str, Any]) -> None:
    for index in (0, 1):
        moved = list(PAIR)
        moved[index] = binding(**{**PAIR[index].__dict__, **change})

        assert fingerprint_of(bindings=tuple(moved)) != fingerprint_of(), f"service {index}"


def test_the_booked_duration_is_inside_the_fingerprint() -> None:
    assert fingerprint_of(booked_duration_minutes=181) != fingerprint_of()


def test_the_same_data_always_digests_the_same() -> None:
    assert fingerprint_of() == fingerprint_of()


# ---------------------------------------------------------------------------
# The write client (plan §30.12)
# ---------------------------------------------------------------------------


LOCATION_UUID = "11111111-1111-4111-8111-111111111111"
BOOKING_A = "aaaaaaaa-0000-4000-8000-000000000001"
BOOKING_B = "bbbbbbbb-0000-4000-8000-000000000002"


def cart_body(**overrides: Any) -> dict[str, Any]:
    from altegio_bot.easyweek_migration.write_client import build_cart_booking_request

    base: dict[str, Any] = {
        "location_uuid": LOCATION_UUID,
        "customer_phone": CUSTOMER_PHONE,
        "customer_first_name": "Testkundin",
        "datetime_start_utc_iso": "2026-09-10T10:00:00Z",
        "comment": "altegio-migration:758285:900001",
        "services": [(KA_SERVICE_UUID, KA_STAFF_UUID), (SECOND_SERVICE_UUID, KA_STAFF_UUID)],
    }
    base.update(overrides)
    return build_cart_booking_request(**base)


def test_the_cart_body_is_exactly_the_proven_shape() -> None:
    """Field for field, the body the live canary was answered 200 for."""
    body = cart_body()

    assert set(body) == {
        "location_uuid",
        "timezone",
        "customer_phone",
        "customer_first_name",
        "booking_comment",
        "items",
    }
    assert body["timezone"] == "Europe/Berlin"
    assert len(body["items"]) == 1, "one item; the ledger keys one source to one target"

    [item] = body["items"]
    assert set(item) == {"datetime_start", "services"}
    assert item["datetime_start"] == "2026-09-10T10:00:00Z", "the ORIGINAL source start"
    assert [line["service_uuid"] for line in item["services"]] == [KA_SERVICE_UUID, SECOND_SERVICE_UUID]
    for line in item["services"]:
        assert set(line) == {"service_uuid", "staffer_uuid"}
        assert line["staffer_uuid"] == KA_STAFF_UUID


def test_the_cart_body_carries_no_price_or_duration() -> None:
    """Both come from the catalogue; sending either would be an unproven field."""
    blob = json.dumps(cart_body())

    for absent in ("price", "cost", "duration", "seance", "customer_uuid", "quantity"):
        assert absent not in blob, absent


@pytest.mark.parametrize(
    ("services", "why"),
    [
        ([(KA_SERVICE_UUID, KA_STAFF_UUID)], "one service"),
        ([(KA_SERVICE_UUID, KA_STAFF_UUID)] * 3, "three services"),
        ([(KA_SERVICE_UUID, KA_STAFF_UUID), (KA_SERVICE_UUID, KA_STAFF_UUID)], "the same service twice"),
        ([(KA_SERVICE_UUID, KA_STAFF_UUID), (SECOND_SERVICE_UUID, SECOND_STAFF_UUID)], "two staffers"),
    ],
)
def test_the_builder_refuses_every_unproven_shape(services: Any, why: str) -> None:
    from altegio_bot.easyweek_client import EasyWeekPermanentError

    with pytest.raises(EasyWeekPermanentError):
        cart_body(services=services)


def cart_client(handler: Any, **kwargs: Any) -> Any:
    import httpx

    from altegio_bot.easyweek_migration.write_client import EasyWeekMigrationWriteClient, RateLimiter

    async def _no_sleep(_seconds: float) -> None:
        """Tests must not spend the rate budget in wall-clock time."""

    return EasyWeekMigrationWriteClient(
        api_key="k",
        workspace_slug="s",
        transport=httpx.MockTransport(handler),
        sleep=_no_sleep,
        rate_limiter=RateLimiter(requests_per_minute=6000, sleep=_no_sleep),
        **kwargs,
    )


@pytest.mark.asyncio
async def test_one_uuid_in_the_response_is_a_proven_creation() -> None:
    import httpx

    async with cart_client(lambda request: httpx.Response(200, json={"uuid": BOOKING_A})) as client:
        created = await client.create_cart_booking(cart_body())

    assert created.booking_uuid == BOOKING_A
    assert created.attempts == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        {"data": {"uuid": BOOKING_A}},
        {"data": [{"uuid": BOOKING_A}]},
        {"bookings": [{"uuid": BOOKING_A}]},
        [{"booking_uuid": BOOKING_A}],
    ],
    ids=["data_object", "data_list", "bookings", "bare_list"],
)
async def test_the_single_uuid_is_found_in_every_envelope(payload: Any) -> None:
    import httpx

    async with cart_client(lambda request: httpx.Response(200, json=payload)) as client:
        assert (await client.create_cart_booking(cart_body())).booking_uuid == BOOKING_A


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [{}, {"data": {}}, {"uuid": "not-a-uuid"}, []],
    ids=["empty", "empty_data", "unusable_uuid", "empty_list"],
)
async def test_a_2xx_without_a_uuid_is_uncertain(payload: Any) -> None:
    import httpx

    from altegio_bot.easyweek_migration.write_client import EasyWeekUncertainMutation

    async with cart_client(lambda request: httpx.Response(200, json=payload)) as client:
        with pytest.raises(EasyWeekUncertainMutation):
            await client.create_cart_booking(cart_body())


@pytest.mark.asyncio
async def test_a_2xx_naming_several_bookings_is_uncertain() -> None:
    """One source record keys one target uuid; picking one of two loses the other."""
    import httpx

    from altegio_bot.easyweek_migration.write_client import EasyWeekUncertainMutation

    payload = {"data": [{"uuid": BOOKING_A}, {"uuid": BOOKING_B}]}
    async with cart_client(lambda request: httpx.Response(200, json=payload)) as client:
        with pytest.raises(EasyWeekUncertainMutation) as refused:
            await client.create_cart_booking(cart_body())

    assert "2 bookings" in str(refused.value)


@pytest.mark.asyncio
async def test_a_2xx_that_is_not_json_is_uncertain() -> None:
    import httpx

    from altegio_bot.easyweek_migration.write_client import EasyWeekUncertainMutation

    async with cart_client(lambda request: httpx.Response(201, content=b"<html>ok</html>")) as client:
        with pytest.raises(EasyWeekUncertainMutation):
            await client.create_cart_booking(cart_body())


@pytest.mark.asyncio
async def test_a_rate_limit_is_the_only_retried_status() -> None:
    import httpx

    posts: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        posts.append(1)
        if len(posts) == 1:
            return httpx.Response(429, headers={"Retry-After": "0"})
        return httpx.Response(200, json={"uuid": BOOKING_A})

    async with cart_client(handler) as client:
        created = await client.create_cart_booking(cart_body())

    assert len(posts) == 2, "a 429 is refused before the handler runs, so a retry is safe"
    assert created.booking_uuid == BOOKING_A


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [500, 502, 503, 504])
async def test_a_server_error_after_the_post_is_uncertain_and_never_retried(status: int) -> None:
    import httpx

    from altegio_bot.easyweek_migration.write_client import EasyWeekUncertainMutation

    posts: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        posts.append(1)
        return httpx.Response(status)

    async with cart_client(handler) as client:
        with pytest.raises(EasyWeekUncertainMutation):
            await client.create_cart_booking(cart_body())

    assert len(posts) == 1, "a 5xx does not prove the booking was not created"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    [
        httpx.TimeoutException("slow") if (httpx := __import__("httpx")) else None,
        __import__("httpx").ConnectError("gone"),
    ],
    ids=["timeout", "disconnect"],
)
async def test_a_timeout_or_disconnect_never_repeats_the_post(failure: Exception) -> None:

    from altegio_bot.easyweek_migration.write_client import EasyWeekUncertainMutation

    posts: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        posts.append(1)
        raise failure

    async with cart_client(handler) as client:
        with pytest.raises(EasyWeekUncertainMutation):
            await client.create_cart_booking(cart_body())

    assert len(posts) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [400, 409, 422])
async def test_a_client_error_is_a_permanent_refusal(status: int) -> None:
    """Includes the conflict a taken slot produces: a named refusal, not a guess."""
    import httpx

    from altegio_bot.easyweek_client import EasyWeekPermanentError

    posts: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        posts.append(1)
        return httpx.Response(status, json={"errors": {"items": ["slot taken"]}})

    async with cart_client(handler) as client:
        with pytest.raises(EasyWeekPermanentError):
            await client.create_cart_booking(cart_body())

    assert len(posts) == 1


@pytest.mark.asyncio
async def test_a_refusal_never_echoes_the_response_body() -> None:
    import httpx

    from altegio_bot.easyweek_client import EasyWeekPermanentError

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(422, json={"errors": {"customer_phone": [f"{CUSTOMER_PHONE} is invalid"]}})

    async with cart_client(handler) as client:
        with pytest.raises(EasyWeekPermanentError) as refused:
            await client.create_cart_booking(cart_body())

    message = str(refused.value)
    assert CUSTOMER_PHONE not in message
    assert "customer_phone" in message, "the field NAME is useful; its value is not"


# ---------------------------------------------------------------------------
# One canary licenses one contract (plan §30.12.4)
# ---------------------------------------------------------------------------


def binding_for(kind: str) -> Any:
    from altegio_bot.easyweek_migration.branch_identity import BranchIdentityResult
    from altegio_bot.easyweek_migration.canary import build_binding

    return build_binding(
        manifest_digest="d" * 64,
        staff_scope_digest="s" * 64,
        cutover_at=parse_cutover(CUTOVER).at,
        horizon_days=30,
        branch_result=BranchIdentityResult(proven=True, proven_branches={KARLSRUHE_COMPANY_ID: "karlsruhe"}),
        contract_kind=kind,
    )


def test_the_two_contracts_are_different_waves() -> None:
    """A wave identity that ignored the contract would let one canary license both."""
    assert binding_for(MUTATION_SINGLE).wave_identity != binding_for(MUTATION_CART_TWO).wave_identity


def test_the_contract_kind_is_reported() -> None:
    safe = binding_for(MUTATION_CART_TWO).as_safe_dict()

    assert safe["contract_kind"] == MUTATION_CART_TWO
    assert "wave_identity" in safe


def test_a_binding_defaults_to_the_single_contract() -> None:
    """Every caller that predates the cart keeps the contract it always had."""
    from altegio_bot.easyweek_migration.branch_identity import BranchIdentityResult
    from altegio_bot.easyweek_migration.canary import build_binding

    binding_obj = build_binding(
        manifest_digest="d" * 64,
        staff_scope_digest="s" * 64,
        cutover_at=parse_cutover(CUTOVER).at,
        horizon_days=30,
        branch_result=BranchIdentityResult(proven=True, proven_branches={KARLSRUHE_COMPANY_ID: "karlsruhe"}),
    )

    assert binding_obj.contract_kind == MUTATION_SINGLE


def test_an_unknown_contract_is_refused() -> None:
    from altegio_bot.easyweek_migration.branch_identity import BranchIdentityResult
    from altegio_bot.easyweek_migration.canary import build_binding

    with pytest.raises(ValueError):
        build_binding(
            manifest_digest="d" * 64,
            staff_scope_digest="s" * 64,
            cutover_at=parse_cutover(CUTOVER).at,
            horizon_days=30,
            branch_result=BranchIdentityResult(proven=True, proven_branches={KARLSRUHE_COMPANY_ID: "karlsruhe"}),
            contract_kind="cart_seventeen",
        )


def test_a_contract_mismatch_is_named_as_its_own_difference() -> None:
    """ "Run another canary" is a different instruction from "fix the manifest"."""
    from altegio_bot.easyweek_migration.canary import SCOPE_CONTRACT_MISMATCH, _first_difference

    difference = _first_difference(binding_for(MUTATION_SINGLE), binding_for(MUTATION_CART_TWO))

    assert difference == SCOPE_CONTRACT_MISMATCH


def test_the_canary_proof_identity_separates_the_contracts() -> None:
    """Two canaries over the same booking must be two rows, not one overwritten."""
    from altegio_bot.models.models import EasyWeekMigrationCanaryProof as Proof

    unique = next(
        constraint
        for constraint in Proof.__table__.constraints
        if getattr(constraint, "name", None) == "uq_easyweek_migration_canary_identity"
    )

    assert "contract_kind" in {column.name for column in unique.columns}


# ---------------------------------------------------------------------------
# The write gate: classified, reported, and not yet written
# ---------------------------------------------------------------------------


def test_the_apply_path_writes_only_contracts_it_can_prove() -> None:
    """`cart_two` is classified and reported, but not written yet.

    Its request builder, its write client and its canary isolation are done and
    tested. What is missing is the readback: `TargetSnapshot` projects one
    service, so a two-line booking cannot yet be proven after creation — and a
    real appointment nobody can check is worse than one nobody created.
    """
    from altegio_bot.easyweek_migration.runner import SUPPORTED_MUTATION_KINDS

    assert MUTATION_SINGLE in SUPPORTED_MUTATION_KINDS
    assert MUTATION_CART_TWO not in SUPPORTED_MUTATION_KINDS


def test_the_gate_is_a_named_refusal_not_a_crash() -> None:
    """The whole point: an unwritable contract stops as a row for a person."""
    from altegio_bot.easyweek_migration.runner import BLOCK_CONTRACT_UNSUPPORTED

    assert BLOCK_CONTRACT_UNSUPPORTED == "mutation_contract_unsupported"
    for leaked in ("phone", "@", "+49"):
        assert leaked not in BLOCK_CONTRACT_UNSUPPORTED


def test_a_cart_decision_still_reports_its_services_safely() -> None:
    """An operator has to see what was classified, without any customer data."""
    safe = classify(cart_record()).as_safe_dict()

    assert safe["mutation_kind"] == MUTATION_CART_TWO
    assert len(safe["services"]) == 2
    assert [item["altegio_service_id"] for item in safe["services"]] == [KA_SERVICE_ID, SECOND_SERVICE_ID]

    blob = json.dumps(safe, ensure_ascii=False)
    for leaked in (CUSTOMER_PHONE, "Testkundin", "@"):
        assert leaked not in blob, leaked


def test_the_safe_shape_carries_no_service_names() -> None:
    """Catalogue names are operator-facing; the machine report stays ids only."""
    for item in classify(cart_record()).as_safe_dict()["services"]:
        assert "service_name" not in item

    # The operator-facing shape does carry them, which is what it is for.
    binding_obj = classify(cart_record()).bindings[0]
    assert "service_name" in binding_obj.as_operator_dict()
