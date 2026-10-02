"""record which operator stop a voucher-mailing plan was built under (review R1)

A plan that was prepared BEFORE an operator pressed stop must not be able to
authorise carrying on after it. The approval therefore records the batch's stop
GENERATION at the moment the plan was built, and a confirmation is admitted while
a stop is active only when that number still matches — that is, only when the plan
was built by somebody who could see the stop.

A counter rather than a boolean, so that "no stop had ever been pressed", "the
first stop" and "the stop pressed again after a resume" are three different facts
rather than one. Without it, a stop pressed during a running stage could be lifted
by a second tab confirming a plan from before it, and the slots behind a request
still in flight would be created after all.

``server_default`` of 0 is correct for the rows that already exist: this phase has
no production batches, and 0 means "built when no stop had ever been pressed",
which is exactly the state of any approval written before this migration.

Nothing else changes. No historical table, row, constraint or binding is touched.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "c7e3b8a14f29"
down_revision = "a4f1c9d26b70"
branch_labels = None
depends_on = None

APPROVALS = "easyweek_voucher_production_approvals"


def upgrade() -> None:
    op.add_column(
        APPROVALS,
        sa.Column(
            "stop_generation_at_plan",
            sa.Integer(),
            server_default=sa.text("0"),
            nullable=False,
        ),
    )
    op.create_check_constraint(
        "ck_ew_voucher_production_approval_stop_gen",
        APPROVALS,
        "stop_generation_at_plan >= 0",
    )


def downgrade() -> None:
    op.drop_constraint("ck_ew_voucher_production_approval_stop_gen", APPROVALS, type_="check")
    op.drop_column(APPROVALS, "stop_generation_at_plan")
