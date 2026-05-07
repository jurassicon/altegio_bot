"""add promo_leads table

Revision ID: a3b4c5d6e7f8
Revises: a2b3c4d5e6f7, f8e7d6c5b4a3
Create Date: 2026-05-07 00:00:00.000000

Adds the promo_leads table that tracks WhatsApp secret-word promo leads.

This is an additive migration — new table only, no existing tables touched.
Safe to apply with zero downtime.

Deploy order:
  1. alembic upgrade head   (this migration)
  2. Deploy / restart api + workers
"""

from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "a3b4c5d6e7f8"
down_revision: Union[str, Sequence[str], None] = ("a2b3c4d5e6f7", "f8e7d6c5b4a3")
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_VALID_STATUSES = (
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

_STATUS_CHECK = "status IN ({})".format(", ".join(f"'{s}'" for s in _VALID_STATUSES))
_DISCOUNT_TYPE_CHECK = "discount_type IN ('fixed', 'percent')"


def upgrade() -> None:
    op.create_table(
        "promo_leads",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        # Company / location context
        sa.Column("company_id", sa.Integer(), nullable=False),
        sa.Column("location_id", sa.BigInteger(), nullable=True),
        # Client identification
        sa.Column("phone_e164", sa.String(32), nullable=False),
        sa.Column("altegio_client_id", sa.BigInteger(), nullable=True),
        # Campaign / offer
        sa.Column("campaign_name", sa.String(128), nullable=False),
        sa.Column("secret_code", sa.String(64), nullable=False),
        sa.Column("discount_amount", sa.Numeric(10, 2), nullable=False),
        sa.Column("discount_type", sa.String(32), nullable=False),
        # Status — full lifecycle defined in PROMO_LEAD_STATUSES.
        sa.Column(
            "status",
            sa.String(64),
            server_default="issued",
            nullable=False,
        ),
        sa.Column("reject_reason", sa.Text(), nullable=True),
        # Core timestamps
        sa.Column("issued_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        # Attribution timestamps (future PRs)
        sa.Column("booked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("applied_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("used_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("cancelled_at", sa.DateTime(timezone=True), nullable=True),
        # Altegio loyalty integration (future PRs)
        sa.Column("loyalty_card_id", sa.String(128), nullable=True),
        sa.Column("loyalty_card_number", sa.String(64), nullable=True),
        sa.Column("card_type_id", sa.String(64), nullable=True),
        sa.Column("discount_program_id", sa.String(64), nullable=True),
        # Altegio booking attribution (future PRs)
        # record_id = local FK; altegio_record_id = external Altegio identifier
        sa.Column(
            "record_id",
            sa.BigInteger(),
            sa.ForeignKey("records.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("altegio_record_id", sa.BigInteger(), nullable=True),
        sa.Column("visit_id", sa.BigInteger(), nullable=True),
        # Audit
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "meta",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        # One active lead per phone + campaign (strict policy).
        sa.UniqueConstraint("phone_e164", "campaign_name", name="uq_promo_leads_phone_campaign"),
        sa.CheckConstraint(_STATUS_CHECK, name="ck_promo_leads_status"),
        sa.CheckConstraint(_DISCOUNT_TYPE_CHECK, name="ck_promo_leads_discount_type"),
    )

    # Individual column indexes
    op.create_index("ix_promo_leads_company_id", "promo_leads", ["company_id"])
    op.create_index("ix_promo_leads_phone_e164", "promo_leads", ["phone_e164"])
    op.create_index("ix_promo_leads_altegio_client_id", "promo_leads", ["altegio_client_id"])
    op.create_index("ix_promo_leads_campaign_name", "promo_leads", ["campaign_name"])
    op.create_index("ix_promo_leads_status", "promo_leads", ["status"])
    op.create_index("ix_promo_leads_issued_at", "promo_leads", ["issued_at"])
    op.create_index("ix_promo_leads_expires_at", "promo_leads", ["expires_at"])
    op.create_index("ix_promo_leads_created_at", "promo_leads", ["created_at"])
    op.create_index("ix_promo_leads_record_id", "promo_leads", ["record_id"])

    # Compound index for status + expiry lookups
    op.create_index(
        "ix_promo_leads_status_expires",
        "promo_leads",
        ["status", "expires_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_promo_leads_status_expires", table_name="promo_leads")
    op.drop_index("ix_promo_leads_record_id", table_name="promo_leads")
    op.drop_index("ix_promo_leads_created_at", table_name="promo_leads")
    op.drop_index("ix_promo_leads_expires_at", table_name="promo_leads")
    op.drop_index("ix_promo_leads_issued_at", table_name="promo_leads")
    op.drop_index("ix_promo_leads_status", table_name="promo_leads")
    op.drop_index("ix_promo_leads_campaign_name", table_name="promo_leads")
    op.drop_index("ix_promo_leads_altegio_client_id", table_name="promo_leads")
    op.drop_index("ix_promo_leads_phone_e164", table_name="promo_leads")
    op.drop_index("ix_promo_leads_company_id", table_name="promo_leads")
    op.drop_table("promo_leads")
