"""Safe, bounded projection of whatever a voucher sale actually returns (§35).

The canary exists because nobody knows what an issued EasyWeek voucher looks
like. That means this module must read a shape it has never seen — which is
exactly the situation where a "just summarise the response" reflex leaks a
voucher code, a customer subtree or a customer-facing URL into stdout, a log
record or a database column that outlives every reason to have it.

No values, and no digests of values
-----------------------------------
Nothing here returns, stores or prints a value, and — since the review — nothing
hashes one either. A plain SHA-256 of a low-entropy secret is not a safeguard:
a twelve-character voucher code, a phone number or an e-mail address is
brute-forceable from its digest in seconds, so a "fingerprint" of one is the
value in a costume. Order fields are therefore described by:

* where the field was seen (stage plus a sanitised dotted path);
* its JSON type;
* whether it was present;
* a bounded length, and only where a length is a count rather than content.

A customer subtree is not described at all beyond "it was there": the walker
records presence and type, marks ``subtree_redacted``, and does not descend.

What this costs, stated plainly
-------------------------------
Without value digests, this module cannot prove that the code seen in the pay
response is the same string as the code seen in the readback. That comparison is
simply not made, and every observation says so through
``cross_stage_equality_proven: false``. Proving it would need a keyed HMAC under
a secret kept away from the digests, which is a separate decision with its own
key-management story — not something to bolt onto a research canary.

Bounded on purpose
------------------
Traversal is capped by depth, node count, key length and breadth. A deeply
nested or enormous response is truncated and flagged, never followed: an unknown
remote shape is not allowed to decide how much memory this process uses.

Unknown fields are shape, not contract
--------------------------------------
A field nobody has seen before is recorded as key plus type and marked unproven.
It is never promoted to a contract, and a missing one is never inferred away.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Final

from altegio_bot.easyweek_voucher_canary.voucher_line import (
    QUANTITY_PROOF_UNPROVEN,
    prove_voucher_line,
)

# Traversal bounds. Generous enough for a real order, small enough that a
# hostile or broken response cannot turn a projection into an outage.
MAX_DEPTH: Final = 6
MAX_NODES: Final = 512
MAX_PATH_LENGTH: Final = 96
MAX_KEY_LENGTH: Final = 48
# Only ever applied to a count, never to a value's content.
MAX_REPORTED_LENGTH: Final = 4096

# Containers that hold a person. The walker records that one was present and
# stops: there is no field inside a customer subtree whose shape is worth the
# risk of touching its contents.
REDACTED_CONTAINER_KEYS: Final = frozenset(
    {
        "customer",
        "client",
        "recipient",
        "purchaser",
        "buyer",
        "contact",
        "owner",
        "author",
    }
)

# Scalar fields that ARE personal data wherever they appear. Presence and type
# only — not even a length, because the length of a phone number or a postcode
# is itself a narrowing fact.
PII_SCALAR_KEYS: Final = frozenset(
    {
        "address",
        "address_1",
        "address_2",
        "apt",
        "birth_date",
        "birthday",
        "city",
        "comment",
        "description",
        "email",
        "first_name",
        "full_name",
        "house",
        "last_name",
        "middle_name",
        "name",
        "note",
        "notes",
        "passport",
        "phone",
        "phone_number",
        "postal_code",
        "street",
        "tax_number",
        "zip_code",
    }
)

# The artifact fields this canary exists to look for. A bounded length IS
# reported for these — "the code is 12 characters" is a useful research fact and
# not a customer identifier — but never a value and never a digest of one.
_CODE_KEYS: Final = frozenset({"code", "voucher_code", "number", "pin", "token"})
_URL_KEYS: Final = frozenset({"url", "public_url", "public_purchase_url", "customer_url", "link", "share_url"})
ARTIFACT_SCALAR_KEYS: Final = _CODE_KEYS | _URL_KEYS

_VOUCHER_CONTAINER_KEYS: Final = frozenset({"voucher", "vouchers"})

# Everything above, plus the identity/state fields a POS order is expected to
# carry. Anything outside this set is recorded as an unlisted shape.
KNOWN_KEYS: Final = (
    _VOUCHER_CONTAINER_KEYS
    | ARTIFACT_SCALAR_KEYS
    | REDACTED_CONTAINER_KEYS
    | PII_SCALAR_KEYS
    | frozenset(
        {
            "uuid",
            "order_uuid",
            "customer_uuid",
            "client_uuid",
            "voucher_uuid",
            "status",
            "state",
            "is_paid",
            "is_reverted",
            "is_canceled",
            "is_cancelled",
            "is_refunded",
            "paid_at",
            "refunded_at",
            "created_at",
            "updated_at",
            "location_uuid",
            "staffer_uuid",
            "account_uuid",
            "invoice",
            "data",
            "meta",
            "total",
            "subtotal",
            "amount_due",
            "amount_paid",
            "price",
            "quantity",
            "voucher_template_uuid",
            "activated_at",
            "expires_at",
            "is_activated",
            "valid_until",
        }
    )
)

# Artifact kinds, in the order they are decided. A closed vocabulary so a report
# can be compared across runs.
ARTIFACT_NONE: Final = "none"
ARTIFACT_EMPTY_COLLECTION: Final = "empty_collection"
ARTIFACT_OBJECT: Final = "voucher_object"
ARTIFACT_COLLECTION: Final = "voucher_collection"
ARTIFACT_SCALAR: Final = "voucher_scalar"


def _json_type(value: object) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return "unknown"


def _safe_key(key: object) -> str:
    """A key name reduced to a bounded, printable slug.

    Key names are usually structure, but a remote system can key an object BY a
    value — an index of orders by phone number, say — so a name that is not a
    plain identifier is replaced by a fixed placeholder rather than by a digest
    of itself. A digest here would be the same mistake as a digest of a value.
    """
    if not isinstance(key, str) or not key:
        return "<non-string-key>"
    trimmed = key[:MAX_KEY_LENGTH]
    if trimmed and all(character.isalnum() or character in "_-." for character in trimmed):
        return trimmed
    return "<opaque-key>"


def _reportable_length(key: str, value: object) -> int | None:
    """A length, only where a length is a count rather than content.

    Arrays and objects: yes — "three vouchers" is a structural fact. Known
    artifact scalars: yes, and only those, because "the code is 12 characters"
    is what this canary is here to learn. Anything else, including every
    personal field and every unknown scalar: no.
    """
    if isinstance(value, (list, dict)):
        return min(len(value), MAX_REPORTED_LENGTH)
    if key in ARTIFACT_SCALAR_KEYS and isinstance(value, str):
        return min(len(value), MAX_REPORTED_LENGTH)
    return None


@dataclass(frozen=True)
class ObservedField:
    """One field, described by shape alone. No value, and no digest of one."""

    path: str
    json_type: str
    present: bool
    value_length: int | None
    known_key: bool
    subtree_redacted: bool = False

    def as_safe_dict(self) -> dict[str, Any]:
        safe: dict[str, Any] = {
            "path": self.path,
            "json_type": self.json_type,
            "present": self.present,
            "known_key": self.known_key,
        }
        if self.value_length is not None:
            safe["value_length"] = self.value_length
        if self.subtree_redacted:
            safe["subtree_redacted"] = True
        return safe


@dataclass(frozen=True)
class ArtifactObservation:
    """What one response or readback showed, as facts and nothing else."""

    stage: str
    truncated: bool
    order_customer_binding_proven: bool
    voucher_line_proven: bool
    # How the count of one was established, if it was: the closed vocabulary in
    # `voucher_line`. Always consistent with `voucher_line_proven`, because both
    # come from the same proof.
    voucher_quantity_proof: str
    voucher_line_shape_unknown: bool
    individual_voucher_artifact_observed: bool
    artifact_kind_observed: str
    artifact_customer_binding_proven: bool
    artifact_nested_in_customer_bound_order: bool
    refund_observed: bool
    fields: tuple[ObservedField, ...]

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "stage": self.stage,
            "truncated": self.truncated,
            "order_customer_binding_proven": self.order_customer_binding_proven,
            "voucher_line_proven": self.voucher_line_proven,
            "voucher_quantity_proof": self.voucher_quantity_proof,
            "voucher_line_shape_unknown": self.voucher_line_shape_unknown,
            "individual_voucher_artifact_observed": self.individual_voucher_artifact_observed,
            "artifact_kind_observed": self.artifact_kind_observed,
            "artifact_customer_binding_proven": self.artifact_customer_binding_proven,
            "artifact_nested_in_customer_bound_order": self.artifact_nested_in_customer_bound_order,
            "refund_observed": self.refund_observed,
            # Contract is never claimed from an unknown shape.
            "artifact_contract_proven": False,
            # No value digests exist, so no value was compared across stages.
            # Saying so is honest; a weak hash that implied otherwise was not.
            "cross_stage_equality_proven": False,
            "fields": [observed.as_safe_dict() for observed in self.fields],
        }


def _walk(node: Any, path: str, depth: int, budget: list[int], out: list[ObservedField]) -> bool:
    """Record shape for *node*, returning ``True`` when something was truncated."""
    if budget[0] <= 0 or depth > MAX_DEPTH or len(path) > MAX_PATH_LENGTH:
        return True

    truncated = False
    if isinstance(node, dict):
        items: Any = node.items()
    elif isinstance(node, list):
        items = ((index, value) for index, value in enumerate(node))
    else:
        return False

    for raw_key, value in items:
        if budget[0] <= 0:
            return True
        budget[0] -= 1

        if isinstance(node, list):
            safe_key = ""
            child_path = f"{path}[{raw_key}]"
            known = False
        else:
            safe_key = _safe_key(raw_key)
            child_path = f"{path}.{safe_key}" if path else safe_key
            known = safe_key in KNOWN_KEYS

        redacted = safe_key in REDACTED_CONTAINER_KEYS
        personal = safe_key in PII_SCALAR_KEYS
        out.append(
            ObservedField(
                path=child_path[:MAX_PATH_LENGTH],
                json_type=_json_type(value),
                present=True,
                # A person's field gets no length either: the length of a phone
                # number or a postcode narrows it all by itself.
                value_length=None if (redacted or personal) else _reportable_length(safe_key, value),
                known_key=known,
                subtree_redacted=redacted,
            )
        )
        if redacted:
            # Presence and type, then stop. There is nothing inside a customer
            # subtree whose shape is worth touching its contents for.
            continue
        if isinstance(value, (dict, list)):
            truncated = _walk(value, child_path, depth + 1, budget, out) or truncated
    return truncated


def _order_object(envelope: object) -> dict[str, Any] | None:
    """The order object, from the envelope or from a ``data`` wrapper."""
    if not isinstance(envelope, dict):
        return None
    inner = envelope.get("data")
    if isinstance(inner, dict) and ("uuid" in inner or "vouchers" in inner or "invoice" in inner):
        return inner
    return envelope


def matches_customer(node: Any, expected: str) -> bool:
    """True when *node* names *expected* through a customer reference.

    Compared in memory against a runtime value and thrown away: what survives is
    a boolean. Neither the expected UUID nor the observed one is stored.
    """
    if not isinstance(node, dict):
        return False
    for key in ("customer_uuid", "client_uuid", "recipient_uuid"):
        value = node.get(key)
        if isinstance(value, str) and value == expected:
            return True
    for key in REDACTED_CONTAINER_KEYS:
        container = node.get(key)
        if isinstance(container, dict):
            inner = container.get("uuid")
            if isinstance(inner, str) and inner == expected:
                return True
    return False


def _voucher_artifacts(order: dict[str, Any]) -> tuple[str, list[Any]]:
    """The artifact kind and the artifact nodes, if the order carries any."""
    single = order.get("voucher")
    collection = order.get("vouchers")

    if isinstance(collection, list) and collection:
        return ARTIFACT_COLLECTION, list(collection)
    if isinstance(single, dict) and single:
        return ARTIFACT_OBJECT, [single]
    if isinstance(single, str) and single:
        return ARTIFACT_SCALAR, [single]
    if isinstance(collection, list):
        return ARTIFACT_EMPTY_COLLECTION, []
    return ARTIFACT_NONE, []


def _carries_individual_artifact(nodes: list[Any]) -> bool:
    """True when at least one node looks like an ISSUED voucher, not a line item.

    A voucher line on an unpaid order is the request echoed back: a template
    UUID, a price and a quantity. An issued voucher is something more — its own
    identity, a code, a URL or an activation state. The difference is the whole
    question this canary was authorised to answer, so it is decided by presence
    of those fields, never by their content.
    """
    for node in nodes:
        if isinstance(node, str) and node:
            return True
        if not isinstance(node, dict):
            continue
        if any(isinstance(node.get(key), str) and node.get(key) for key in ARTIFACT_SCALAR_KEYS):
            return True
        if isinstance(node.get("uuid"), str) and node["uuid"]:
            return True
        if node.get("is_activated") is not None or node.get("activated_at") is not None:
            return True
    return False


def _refund_observed(order: dict[str, Any]) -> bool:
    """Only the documented reverted/refunded markers count as a rollback proof."""
    for key in ("is_reverted", "is_refunded"):
        if order.get(key) is True:
            return True
    status = order.get("status")
    if isinstance(status, str) and status.casefold() in {"refunded", "reverted"}:
        return True
    return False


def observe_artifact(
    envelope: object,
    *,
    stage: str,
    expected_customer_uuid: str,
    expected_template_uuid: str,
    expected_price_minor: int,
) -> ArtifactObservation:
    """Project one response or readback into safe, comparable facts.

    ``expected_customer_uuid`` is compared in memory and never stored: what
    survives is the boolean "the order named the customer we planned for".

    The order carrying the right customer is deliberately NOT reported as an
    artifact-level customer binding. A voucher nested inside a customer-bound
    order is a weaker fact than a voucher that names its own owner, and merging
    the two would let the canary claim a binding it never saw.
    """
    order = _order_object(envelope)
    if order is None:
        return ArtifactObservation(
            stage=stage,
            truncated=False,
            order_customer_binding_proven=False,
            voucher_line_proven=False,
            voucher_quantity_proof=QUANTITY_PROOF_UNPROVEN,
            voucher_line_shape_unknown=True,
            individual_voucher_artifact_observed=False,
            artifact_kind_observed=ARTIFACT_NONE,
            artifact_customer_binding_proven=False,
            artifact_nested_in_customer_bound_order=False,
            refund_observed=False,
            fields=(),
        )

    fields: list[ObservedField] = []
    truncated = _walk(order, "", 0, [MAX_NODES], fields)

    order_customer_binding = matches_customer(order, expected_customer_uuid)

    kind, nodes = _voucher_artifacts(order)
    # The SAME proof the payment pre-condition uses. Two implementations of
    # "is this one voucher for fifteen euros?" could disagree, and either
    # direction of that disagreement is a bug with money in it.
    line_proof = prove_voucher_line(
        order,
        expected_template_uuid=expected_template_uuid,
        expected_price_minor=expected_price_minor,
    )
    voucher_line_proven = line_proof.proven
    voucher_line_shape_unknown = not line_proof.shape_recognised

    individual = _carries_individual_artifact(nodes)
    artifact_customer_binding = any(
        matches_customer(node, expected_customer_uuid) for node in nodes if isinstance(node, dict)
    )

    return ArtifactObservation(
        stage=stage,
        truncated=truncated,
        order_customer_binding_proven=order_customer_binding,
        voucher_line_proven=voucher_line_proven,
        voucher_quantity_proof=line_proof.quantity_proof,
        voucher_line_shape_unknown=voucher_line_shape_unknown,
        individual_voucher_artifact_observed=individual,
        artifact_kind_observed=kind if individual else (ARTIFACT_EMPTY_COLLECTION if not nodes else kind),
        artifact_customer_binding_proven=artifact_customer_binding,
        artifact_nested_in_customer_bound_order=bool(nodes) and order_customer_binding,
        refund_observed=_refund_observed(order),
        fields=tuple(fields),
    )
