"""Synthetic payloads for the voucher-canary tests.

Every identity here is obviously fabricated. No production customer, staffer,
account or order UUID, no name, no contact detail and no real voucher artifact
appears in this repository — the canary reads those from named environment
variables at run time and keeps only fingerprints.
"""

from __future__ import annotations

import ast
import inspect
from types import ModuleType
from typing import Any

from altegio_bot.easyweek_voucher_identity import (
    EASYWEEK_VOUCHER_TEMPLATE_UUID,
    EASYWEEK_WORKSPACE_SLUG,
    EASYWEEK_WORKSPACE_UUID,
    FROZEN_TEMPLATE_FACTS,
    KARLSRUHE_LOCATION_UUID,
    SUPPORTED_VOUCHER_PRICE_MINOR,
)

# Synthetic runtime identities. Version-4 shaped and unmistakably fake.
CUSTOMER_UUID = "aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa"
STAFFER_UUID = "bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb"
ACCOUNT_UUID = "cccccccc-3333-4333-8333-cccccccccccc"
ORDER_UUID = "dddddddd-4444-4444-8444-dddddddddddd"
OTHER_UUID = "eeeeeeee-5555-4555-8555-eeeeeeeeeeee"

WORKSPACE: dict[str, Any] = {
    "uuid": EASYWEEK_WORKSPACE_UUID,
    "slug": EASYWEEK_WORKSPACE_SLUG,
    "currency": "EUR",
}
LOCATIONS: list[dict[str, Any]] = [
    {"uuid": KARLSRUHE_LOCATION_UUID, "name": "KitiLash Karlsruhe", "timezone": "Europe/Berlin"},
]
TEMPLATE: dict[str, Any] = {
    "uuid": EASYWEEK_VOUCHER_TEMPLATE_UUID,
    **FROZEN_TEMPLATE_FACTS,
    **{
        "vouchers_count": 0,
        "activated_vouchers_count": 0,
    },
}
CUSTOMER: dict[str, Any] = {
    "uuid": CUSTOMER_UUID,
    "first_name": "Synthetic",
    "last_name": "Fixture",
    "email": "fixture@example.invalid",
    "phone": "+490000000000",
}
# Staffers are paginated and publish a `last_page`; accounts are served whole
# under the location. Two shapes, because the documented API has two.
STAFFERS: dict[str, Any] = {
    "data": [{"uuid": STAFFER_UUID, "name": "Synthetic Staffer"}],
    "meta": {"current_page": 1, "last_page": 1, "per_page": 100, "total": 1},
}
ACCOUNTS: list[dict[str, Any]] = [{"uuid": ACCOUNT_UUID, "name": "Synthetic Card"}]

CREATED_AT = "2026-09-11T10:00:00+00:00"


def orders_page(rows: list[dict[str, Any]], *, page: int = 1, last_page: int = 1) -> dict[str, Any]:
    """One page of `GET /orders`, with the pagination metadata the API publishes."""
    return {
        "data": rows,
        "meta": {"current_page": page, "last_page": last_page, "per_page": 100, "total": len(rows)},
    }


def voucher_line(**changes: Any) -> dict[str, Any]:
    return {
        "voucher_template_uuid": EASYWEEK_VOUCHER_TEMPLATE_UUID,
        "price": SUPPORTED_VOUCHER_PRICE_MINOR,
        "quantity": 1,
        **changes,
    }


def open_order(*, marker: str, **changes: Any) -> dict[str, Any]:
    """A freshly created, unpaid POS order, in the shape EasyWeek really returns.

    Deliberately WITHOUT a top-level ``location_uuid`` or ``staffer_uuid``: the
    observed body carries neither, and a fixture that invented them would have
    let a matcher requiring them pass here and fail against production. The
    customer arrives as a nested object, which is also what was observed.
    """
    order = {
        "uuid": ORDER_UUID,
        "status": "open",
        "is_paid": False,
        "is_reverted": False,
        "created_at": CREATED_AT,
        "comment": marker,
        "customer": {"uuid": CUSTOMER_UUID, "first_name": "Synthetic", "last_name": "Fixture"},
        "vouchers": [voucher_line()],
        "invoice": {"total": 1500, "amount_due": 1500, "amount_paid": 0},
    }
    order.update(changes)
    return order


def listed_order(*, marker: str, **changes: Any) -> dict[str, Any]:
    """A row as it appears in `GET /orders`, with no scope fields echoed back."""
    row = {
        "uuid": ORDER_UUID,
        "status": "open",
        "is_reverted": False,
        "created_at": CREATED_AT,
        "comment": marker,
        "customer": {"uuid": CUSTOMER_UUID},
        "vouchers": [voucher_line()],
    }
    row.update(changes)
    return row


def paid_order(*, marker: str, **changes: Any) -> dict[str, Any]:
    order = open_order(marker=marker)
    order.update(
        {
            "is_paid": True,
            "status": "paid",
            "invoice": {"total": 1500, "amount_due": 0, "amount_paid": 1500},
        }
    )
    order.update(changes)
    return order


def refunded_order(*, marker: str, **changes: Any) -> dict[str, Any]:
    order = paid_order(marker=marker)
    order.update({"is_reverted": True, "status": "refunded"})
    order.update(changes)
    return order


def cancelled_order(*, marker: str, **changes: Any) -> dict[str, Any]:
    order = open_order(marker=marker)
    order.update({"is_canceled": True, "status": "canceled"})
    order.update(changes)
    return order


def code_without_docstrings(module: ModuleType) -> str:
    """Module source with every docstring removed.

    These modules deliberately NAME the endpoints and identifiers they refuse to
    implement, so a plain substring check over the source would flag the very
    prose that documents the refusal.
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
    """Every module name this module imports, as written in its imports."""
    names: set[str] = set()
    for node in ast.walk(ast.parse(inspect.getsource(module))):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
    return names
