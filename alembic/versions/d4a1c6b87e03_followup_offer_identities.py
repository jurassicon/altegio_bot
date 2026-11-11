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
it does re-open the widening above, so it refuses while an offer that could
still be CONFIRMED carries an offered set: pending and inside its TTL.

An offer whose TTL has run out does not block the rollback. It cannot be
confirmed in the first place, and it stays unconfirmable afterwards, so keeping
it would mean an abandoned plan pinning the schema for ever. A consumed approval
is history too: its run and its rows already exist, and the single-attempt key
is what protects them.
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
    """Refuse while a LIVE pending offer would lose its pinned identities.

    "Live" and not merely "pending", which was a defect: a TTL running out does
    not change ``status``, so an abandoned, unconfirmable plan pinned the column
    for ever — and the runbook's own advice, "let it expire", could never be
    followed. The only thing the guard protects is an offer that could still be
    confirmed, and an expired one cannot pass the confirmation's TTL check.

    An expired offer also stays unconfirmable after a downgrade and a fresh
    upgrade: its TTL is unchanged and the column comes back NULL. The original
    signed identities cannot be reconstructed; signature validation or the
    subsequent expiry/offer checks refuse the approval.

    The comparison is against the database's own clock, under the same
    ACCESS EXCLUSIVE lock the check and the DDL already share, so an offer being
    written concurrently cannot land between the count and the drop and cannot
    be judged against a different "now".
    """
    bind = op.get_bind()
    bind.execute(sa.text(f"LOCK TABLE {APPROVALS} IN ACCESS EXCLUSIVE MODE"))
    live = bind.execute(
        sa.text(
            f"SELECT count(*) FROM {APPROVALS} "
            "WHERE status = 'pending' "
            "AND offered_source_item_ids IS NOT NULL "
            "AND expires_at > now()"
        )
    ).scalar_one()
    if int(live or 0):
        raise RuntimeError(
            "refusing to drop the follow-up offer identities: "
            f"{int(live)} pending offer(s) are still within their TTL and carry one. "
            "Dropping the column would let a confirmation of those offers reach recipients "
            "they were never offered for. Let them expire — an expired offer cannot be "
            "confirmed and does not block this rollback — or consume them first."
        )
    op.drop_column(APPROVALS, "offered_source_item_ids")
