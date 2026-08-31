"""easyweek migration service baseline

Revision ID: e7b91d3a5c02
Revises: d5f0c8a91b47
Create Date: 2026-08-31 00:00:00.000000

Additive only: one new table, no change to any existing one.

It exists because the service a wave was reviewed against had nowhere to live.
The migration cannot read a booking's catalogue service back — EasyWeek returns
an order-line uuid — so plan §28 proves the service by its exact attributes
instead. That was re-derived from the current catalogue on every run, which made
it circular: rename a service between the canary and the bulk and the "expected"
attributes changed with it, so the check passed and the old canary kept
licensing the wave.

One row per (location, service): the agreed truth, written once before the first
booking for that service and only ever verified afterwards. Not a catalogue
history and not a manifest snapshot — no versions, no audit trail.
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "e7b91d3a5c02"
down_revision: Union[str, Sequence[str], None] = "d5f0c8a91b47"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "easyweek_migration_service_baseline",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("easyweek_location_uuid", sa.String(length=64), nullable=False),
        sa.Column("easyweek_service_uuid", sa.String(length=64), nullable=False),
        sa.Column("canonical_name", sa.String(length=255), nullable=False),
        sa.Column("currency", sa.String(length=8), nullable=False),
        sa.Column("price_minor", sa.Integer(), nullable=False),
        sa.Column("duration_minutes", sa.Integer(), nullable=False),
        sa.Column("proof_method", sa.String(length=64), nullable=False),
        sa.Column("proof_version", sa.String(length=16), nullable=False),
        sa.Column("wave_identity", sa.String(length=64), nullable=True),
        sa.Column("established_run_id", sa.String(length=64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "easyweek_location_uuid",
            "easyweek_service_uuid",
            name="uq_easyweek_service_baseline_identity",
        ),
    )


def downgrade() -> None:
    op.drop_table("easyweek_migration_service_baseline")
