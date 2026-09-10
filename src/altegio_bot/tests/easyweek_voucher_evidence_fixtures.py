"""Shared payloads and source-inspection helpers for voucher-contract tests.

The payloads mirror the shapes observed on 10.09.2026 and carry no production
customer identity: no customer UUID, no order UUID, no voucher code, no
customer-facing URL and no dashboard identity.
"""

from __future__ import annotations

import ast
import inspect
from types import ModuleType
from typing import Any

WORKSPACE_UUID = "e66be240-362c-4fe4-9388-6ed187b27b93"
WORKSPACE_SLUG = "kitilash"
KARLSRUHE_UUID = "8395fab6-7ee8-4702-88d9-fd78f92539c1"
TEMPLATE_UUID = "49bc000c-c3a6-47c7-bdfd-b8ccd3ae2677"
PRICE_MINOR = 1500

WORKSPACE: dict[str, Any] = {
    "uuid": WORKSPACE_UUID,
    "slug": WORKSPACE_SLUG,
    "currency": "EUR",
}
LOCATIONS: list[dict[str, Any]] = [
    {"uuid": KARLSRUHE_UUID, "name": "KitiLash Karlsruhe", "timezone": "Europe/Berlin"},
]
TEMPLATE: dict[str, Any] = {
    "uuid": TEMPLATE_UUID,
    "cost": PRICE_MINOR,
    "value": PRICE_MINOR,
    "is_enabled": True,
    "is_online": False,
    "is_single_charge": True,
    "validity": None,
    "forces_activation": True,
    "is_connected_all_branches": True,
    "is_connected_all_services": True,
    "vouchers_count": 0,
    "activated_vouchers_count": 0,
}

# The canonical live response for price=1500 (matrix case 1).
CANONICAL_INVOICE: dict[str, Any] = {
    "base_amount": 1500,
    "base_price": 1500,
    "subtotal": 1500,
    "total": 1500,
    "amount_due": 1500,
    "discount_amount": 0,
    "amount_paid": 0,
    "voucher_paid_amount": 0,
    "account_paid_amount": -1500,
    "order_uuid": None,
    "status": None,
}


def canonical_response(**invoice_changes: Any) -> dict[str, Any]:
    return {"invoice": {**CANONICAL_INVOICE, **invoice_changes}}


def zero_total_response() -> dict[str, Any]:
    """Matrix case 3: price=0 calculated happily. Not a supported contract."""
    return canonical_response(
        base_amount=0,
        base_price=0,
        subtotal=0,
        total=0,
        amount_due=0,
        account_paid_amount=0,
    )


def arbitrary_price_response() -> dict[str, Any]:
    """Matrix case 4: the API invoiced 1499, exactly as asked, without checking."""
    return canonical_response(
        base_amount=1499,
        base_price=1499,
        subtotal=1499,
        total=1499,
        amount_due=1499,
        account_paid_amount=-1499,
    )


def fully_discounted_response() -> dict[str, Any]:
    """Matrix case 5: price=1500 plus discount_amount=1500 gives a zero total."""
    return canonical_response(
        base_amount=1500,
        base_price=1500,
        subtotal=0,
        total=0,
        amount_due=0,
        discount_amount=-1500,
        account_paid_amount=0,
    )


def code_without_docstrings(module: ModuleType) -> str:
    """Module source with every docstring removed.

    These modules deliberately NAME the endpoints and identifiers they refuse to
    implement, so a plain substring check over the source would flag the very
    prose that documents the refusal. Only real code is searched.
    """
    tree = ast.parse(inspect.getsource(module))
    for node in ast.walk(tree):
        body = getattr(node, "body", None)
        if not isinstance(body, list) or not body:
            continue
        if not isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        first = body[0]
        if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant) and isinstance(first.value.value, str):
            del body[0]
    return ast.unparse(tree)


def imported_modules(module: ModuleType) -> set[str]:
    """Every module name this module imports, as written in its import statements."""
    names: set[str] = set()
    for node in ast.walk(ast.parse(inspect.getsource(module))):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
    return names
