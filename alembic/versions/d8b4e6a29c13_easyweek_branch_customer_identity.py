"""Allow one workspace customer to have distinct branch Clients (PR-21 R3)."""

import sqlalchemy as sa

from alembic import op

revision = "d8b4e6a29c13"
down_revision = "f6a8d2c91b47"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_unique_constraint(
        "uq_clients_provider_company_easyweek_uuid", "clients", ["provider", "company_id", "easyweek_customer_uuid"]
    )
    op.drop_constraint("uq_clients_provider_easyweek_uuid", "clients", type_="unique")


def downgrade() -> None:
    # Never merge or move branch cards to recreate the obsolete global key.
    occupied = (
        op.get_bind()
        .execute(
            sa.text("""
        SELECT EXISTS (
            SELECT 1 FROM clients WHERE easyweek_customer_uuid IS NOT NULL
            GROUP BY provider, easyweek_customer_uuid HAVING count(*) > 1
        )
    """)
        )
        .scalar_one()
    )
    if occupied:
        raise RuntimeError(
            "PR-21 branch downgrade refused: workspace customer has multiple branch Clients; preserve data"
        )
    op.create_unique_constraint("uq_clients_provider_easyweek_uuid", "clients", ["provider", "easyweek_customer_uuid"])
    op.drop_constraint("uq_clients_provider_company_easyweek_uuid", "clients", type_="unique")
