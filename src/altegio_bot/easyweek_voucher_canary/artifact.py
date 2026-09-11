"""Safe, bounded projection of whatever a voucher sale actually returns (§35).

The canary exists because nobody knows what an issued EasyWeek voucher looks
like. That means this module must read a shape it has never seen — which is
exactly the situation where a "just log the response" reflex leaks a voucher
code, a customer subtree or a customer-facing URL into stdout, a log record or a
database column that outlives every reason to have it.

So nothing here ever returns, stores or prints a VALUE. It returns:

* where a field was seen (stage plus a sanitised dotted path);
* the field's JSON type;
* whether it was present;
* a truncated SHA-256 fingerprint of its serialised value;
* a bounded length.

A fingerprint is enough to answer the questions the canary actually has — "did
the same code appear in the pay response and in the readback?", "did the value
change after the refund?" — without ever holding the code itself.

Bounded on purpose
------------------
Traversal is capped by depth, node count, key length and serialised value size.
A deeply nested or enormous response is truncated and flagged, never followed:
an unknown remote shape is not allowed to decide how much memory this process
uses or how long it runs.

Unknown fields are shape, not contract
--------------------------------------
A field nobody has seen before is recorded as key + type + fingerprint and
marked unproven. It is never promoted to a contract, and a missing one is never
inferred away.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Final

# Traversal bounds. Generous enough for a real order, small enough that a
# hostile or broken response cannot turn a projection into an outage.
MAX_DEPTH: Final = 6
MAX_NODES: Final = 512
MAX_PATH_LENGTH: Final = 96
MAX_KEY_LENGTH: Final = 48
# Values are serialised only to be hashed and measured, never kept.
MAX_SERIALISED_VALUE: Final = 4096

# Keys that would carry an individual voucher artifact, a customer binding or a
# customer-facing link. Their presence is what the canary is looking for; their
# content is what it must never keep.
_VOUCHER_CONTAINER_KEYS: Final = frozenset({"voucher", "vouchers"})
_CODE_KEYS: Final = frozenset({"code", "voucher_code", "number", "pin", "token"})
_URL_KEYS: Final = frozenset({"url", "public_url", "public_purchase_url", "customer_url", "link", "share_url"})
_CUSTOMER_KEYS: Final = frozenset({"customer", "customer_uuid", "client", "client_uuid", "recipient"})

# Everything above, plus the identity/state fields a POS order is expected to
# carry. Anything outside this set is recorded as an unlisted shape.
KNOWN_KEYS: Final = (
    _VOUCHER_CONTAINER_KEYS
    | _CODE_KEYS
    | _URL_KEYS
    | _CUSTOMER_KEYS
    | frozenset(
        {
            "uuid",
            "order_uuid",
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
            "comment",
            "invoice",
            "data",
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


def _fingerprint(value: object) -> str:
    """A short, stable, one-way digest of a value. Never reversible to the value.

    Serialisation is capped before hashing: a multi-megabyte string must not be
    copied through this process just to be summarised.
    """
    try:
        serialised = json.dumps(value, sort_keys=True, ensure_ascii=False, default=str)
    except Exception:
        serialised = repr(type(value).__name__)
    return hashlib.sha256(serialised[:MAX_SERIALISED_VALUE].encode("utf-8")).hexdigest()[:16]


def _value_length(value: object) -> int | None:
    """A bounded size, when size is a fact worth having. Never the content."""
    if isinstance(value, (str, bytes, list, tuple, dict, set)):
        return min(len(value), MAX_SERIALISED_VALUE)
    return None


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

    Key names are structure rather than content, but they come from a remote
    system, so they are still bounded and charset-restricted before anything
    prints them. A key that survives none of that becomes its own fingerprint.
    """
    if not isinstance(key, str) or not key:
        return "<non-string-key>"
    trimmed = key[:MAX_KEY_LENGTH]
    if all(character.isalnum() or character in "_-." for character in trimmed):
        return trimmed
    return f"<opaque:{_fingerprint(key)}>"


@dataclass(frozen=True)
class ObservedField:
    """One field, described by shape alone."""

    path: str
    json_type: str
    present: bool
    value_fingerprint: str | None
    value_length: int | None
    known_key: bool

    def as_safe_dict(self) -> dict[str, Any]:
        safe: dict[str, Any] = {
            "path": self.path,
            "json_type": self.json_type,
            "present": self.present,
            "known_key": self.known_key,
        }
        if self.value_fingerprint is not None:
            safe["value_fingerprint"] = self.value_fingerprint
        if self.value_length is not None:
            safe["value_length"] = self.value_length
        return safe


@dataclass(frozen=True)
class ArtifactObservation:
    """What one response or readback showed, as facts and nothing else."""

    stage: str
    truncated: bool
    order_customer_binding_proven: bool
    voucher_line_proven: bool
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
            "individual_voucher_artifact_observed": self.individual_voucher_artifact_observed,
            "artifact_kind_observed": self.artifact_kind_observed,
            "artifact_customer_binding_proven": self.artifact_customer_binding_proven,
            "artifact_nested_in_customer_bound_order": self.artifact_nested_in_customer_bound_order,
            "refund_observed": self.refund_observed,
            # Contract is never claimed from an unknown shape.
            "artifact_contract_proven": False,
            "fields": [observed.as_safe_dict() for observed in self.fields],
        }


def _walk(node: Any, path: str, depth: int, budget: list[int], out: list[ObservedField]) -> bool:
    """Record shape for *node*, returning ``True`` when something was truncated."""
    if budget[0] <= 0 or depth > MAX_DEPTH or len(path) > MAX_PATH_LENGTH:
        return True

    truncated = False
    if isinstance(node, dict):
        for key, value in node.items():
            if budget[0] <= 0:
                return True
            budget[0] -= 1
            safe_key = _safe_key(key)
            child_path = f"{path}.{safe_key}" if path else safe_key
            out.append(
                ObservedField(
                    path=child_path[:MAX_PATH_LENGTH],
                    json_type=_json_type(value),
                    present=True,
                    value_fingerprint=_fingerprint(value),
                    value_length=_value_length(value),
                    known_key=safe_key in KNOWN_KEYS,
                )
            )
            if isinstance(value, (dict, list)):
                truncated = _walk(value, child_path, depth + 1, budget, out) or truncated
    elif isinstance(node, list):
        for index, value in enumerate(node):
            if budget[0] <= 0:
                return True
            budget[0] -= 1
            child_path = f"{path}[{index}]"
            out.append(
                ObservedField(
                    path=child_path[:MAX_PATH_LENGTH],
                    json_type=_json_type(value),
                    present=True,
                    value_fingerprint=_fingerprint(value),
                    value_length=_value_length(value),
                    known_key=False,
                )
            )
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


def _matches_uuid(node: Any, keys: frozenset[str], expected: str) -> bool:
    """True when *node* names *expected* through one of *keys*.

    Compared in memory and thrown away: the expected UUID is runtime
    configuration and never reaches a report or the ledger.
    """
    if not isinstance(node, dict):
        return False
    for key in keys:
        value = node.get(key)
        if isinstance(value, str) and value == expected:
            return True
        if isinstance(value, dict) and isinstance(value.get("uuid"), str) and value["uuid"] == expected:
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
        if any(isinstance(node.get(key), str) and node.get(key) for key in _CODE_KEYS):
            return True
        if any(isinstance(node.get(key), str) and node.get(key) for key in _URL_KEYS):
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
            individual_voucher_artifact_observed=False,
            artifact_kind_observed=ARTIFACT_NONE,
            artifact_customer_binding_proven=False,
            artifact_nested_in_customer_bound_order=False,
            refund_observed=False,
            fields=(),
        )

    fields: list[ObservedField] = []
    truncated = _walk(order, "", 0, [MAX_NODES], fields)

    order_customer_binding = _matches_uuid(order, _CUSTOMER_KEYS, expected_customer_uuid)

    kind, nodes = _voucher_artifacts(order)
    voucher_line_proven = False
    if len(nodes) == 1 and isinstance(nodes[0], dict):
        line = nodes[0]
        voucher_line_proven = (
            line.get("voucher_template_uuid") == expected_template_uuid
            and type(line.get("price")) is int
            and line.get("price") == expected_price_minor
            and type(line.get("quantity")) is int
            and line.get("quantity") == 1
        )

    individual = _carries_individual_artifact(nodes)
    artifact_customer_binding = any(
        _matches_uuid(node, _CUSTOMER_KEYS, expected_customer_uuid) for node in nodes if isinstance(node, dict)
    )

    return ArtifactObservation(
        stage=stage,
        truncated=truncated,
        order_customer_binding_proven=order_customer_binding,
        voucher_line_proven=voucher_line_proven,
        individual_voucher_artifact_observed=individual,
        artifact_kind_observed=kind if individual else (ARTIFACT_EMPTY_COLLECTION if not nodes else kind),
        artifact_customer_binding_proven=artifact_customer_binding,
        artifact_nested_in_customer_bound_order=bool(nodes) and order_customer_binding,
        refund_observed=_refund_observed(order),
        fields=tuple(fields),
    )
