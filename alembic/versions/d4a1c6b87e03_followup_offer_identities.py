"""Pin the exact offered identities on a follow-up approval (§46.8 review R2).

One additive, nullable column and nothing else.

The approval already carried a digest over the composition and the count the
operator confirmed. Neither was enough: the digest covered every INCLUDED row of
the preview while the count covered only the sendable subset, so a recipient who
regained eligibility between the offer and the confirmation could be added to the
run the operator never saw them in — and a count comparison cannot notice one
person being replaced by another at the same count either.

``offered_source_item_ids`` records which rows the offer actually covered, and
the digest beside it is recomputed over that same subset. A confirmation may
then only ever NARROW the offered set.

Nullable on purpose, and fail-closed at the far end: an approval written before
this column existed has no offered set, so it cannot be confirmed. Reconstructing
one from today's facts is precisely the widening this column exists to prevent,
and a pending offer is cheap to recreate.

The downgrade drops the column. That loses no fact whose absence could let a
second message reach anybody — a spent attempt and a terminal tombstone live in
``easyweek_voucher_followup_sends`` and are guarded by their own revision — but
it does re-open the widening above, so it refuses while any approval that is
still PENDING carries an offered set. A consumed approval is history: its run and
its rows already exist, and the single-attempt key is what protects them.
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "d4a1c6b87e03"
down_revision: Union[str, Sequence[str], None] = "c3f8a95d2e14"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

APPROVALS = "easyweek_voucher_followup_approvals"


def upgrade() -> None:
    op.add_column(
        APPROVALS,
        sa.Column("offered_source_item_ids", postgresql.JSONB(), nullable=True),
    )


def downgrade() -> None:
    """Refuse while a PENDING offer would lose its pinned identities."""
    bind = op.get_bind()
    bind.execute(sa.text(f"LOCK TABLE {APPROVALS} IN ACCESS EXCLUSIVE MODE"))
    pending = bind.execute(
        sa.text(f"SELECT count(*) FROM {APPROVALS} WHERE status = 'pending' AND offered_source_item_ids IS NOT NULL")
    ).scalar_one()
    if int(pending or 0):
        raise RuntimeError(
            "refusing to drop the follow-up offer identities: "
            f"{int(pending)} pending offer(s) carry one. Dropping the column would let a "
            "confirmation of those offers reach recipients they were never offered for. "
            "Let them expire or consume them first."
        )
    op.drop_column(APPROVALS, "offered_source_item_ids")
