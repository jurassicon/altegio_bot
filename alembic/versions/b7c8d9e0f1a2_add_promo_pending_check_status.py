"""add promo pending_check status

Revision ID: b7c8d9e0f1a2
Revises: a3b4c5d6e7f8
Create Date: 2026-05-11 00:00:00.000000
"""

from typing import Sequence, Union

from alembic import op

revision: str = "b7c8d9e0f1a2"
down_revision: Union[str, Sequence[str], None] = "a3b4c5d6e7f8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_OLD_STATUSES = (
    "issued",
    "booked",
    "applied",
    "used",
    "expired",
    "cancelled",
    "rejected_not_new",
    "rejected_service_not_allowed",
    "apply_failed",
)

_NEW_STATUSES = (
    "issued",
    "pending_check",
    "booked",
    "applied",
    "used",
    "expired",
    "cancelled",
    "rejected_not_new",
    "rejected_service_not_allowed",
    "apply_failed",
)


def _status_check(statuses: tuple[str, ...]) -> str:
    return "status IN ({})".format(", ".join(f"'{status}'" for status in statuses))


def upgrade() -> None:
    op.drop_constraint("ck_promo_leads_status", "promo_leads", type_="check")
    op.create_check_constraint(
        "ck_promo_leads_status",
        "promo_leads",
        _status_check(_NEW_STATUSES),
    )


def downgrade() -> None:
    op.execute(
        """
        UPDATE promo_leads
        SET status = 'cancelled',
            reject_reason = COALESCE(reject_reason, 'pending_check_downgrade')
        WHERE status = 'pending_check'
        """
    )
    op.drop_constraint("ck_promo_leads_status", "promo_leads", type_="check")
    op.create_check_constraint(
        "ck_promo_leads_status",
        "promo_leads",
        _status_check(_OLD_STATUSES),
    )
