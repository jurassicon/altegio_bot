"""add the durable Chatwoot outbound mirror registry (WAMID -> Message.id)

An inbound WhatsApp reaction to an AUTOMATIC message has to find the private
mirror note that message became in Chatwoot. Until now the only way to find it
was to page the conversation and look for a marker in ``content_attributes``,
which has two problems this table removes:

* a long conversation pushes the note out of any bounded page window, so the
  native reply silently degrades to a visible quote;
* Chatwoot's messages endpoint pages by ``id`` but orders by ``created_at``, so a
  backdated or imported message can make an id cursor skip real history. No
  local heuristic can fix that, because the two orders are simply not the same
  order.

So the side that CREATES the note records what it created, after Chatwoot has
answered with a valid message id. The reaction path then resolves its target in
one indexed read and never depends on the pagination defect.

Why a standalone table and not a column on ``outbox_messages``:

* the Chatwoot mirror is a background task that races the Outbox row's own
  ``provider_message_id`` commit, so this write must not assume that row exists
  yet and must not lock it;
* ``outbox_messages.chatwoot_message_id`` already means something else — the
  operator-relay message id — and reusing it would make an accidentally
  populated value look like proof, which the reaction path deliberately refuses.

The WAMID is globally unique per Meta message, so it is both the idempotency key
(the write is ``ON CONFLICT DO NOTHING``) and the lookup index. Nothing is
backfilled: notes created before this revision have no row and keep using the
bounded legacy scan, which still fails closed.

Revision ID: c4e9a1b78d52
Revises: b3f7c2a90d14
Create Date: 2026-09-27
"""

import sqlalchemy as sa

from alembic import op

revision = "c4e9a1b78d52"
down_revision = "b3f7c2a90d14"
branch_labels = None
depends_on = None

MIRRORS = "chatwoot_outbound_mirrors"

# Spelled out rather than imported: a migration must keep meaning the same thing
# after the constants move.
_ROUTES = ("tenant", "general")
_ROUTE_SQL = ", ".join(f"'{value}'" for value in _ROUTES)


def upgrade() -> None:
    op.create_table(
        MIRRORS,
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        # -- the proof -------------------------------------------------------
        sa.Column("provider_message_id", sa.String(length=128), nullable=False),
        sa.Column("chatwoot_message_id", sa.BigInteger(), nullable=False),
        sa.Column("chatwoot_conversation_id", sa.BigInteger(), nullable=False),
        sa.Column("marker_version", sa.String(length=64), nullable=False),
        # -- routing provenance, descriptive only ----------------------------
        sa.Column("chatwoot_route", sa.String(length=16), nullable=False),
        sa.Column("chatwoot_inbox_id", sa.Integer(), nullable=True),
        sa.Column("tenant_provider", sa.String(length=32), nullable=True),
        sa.Column("company_id", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        # 1. One Meta message, one mirror note. This is the idempotency key for
        # the write and the index the reaction lookup reads.
        sa.UniqueConstraint("provider_message_id", name="uq_chatwoot_outbound_mirror_provider_message"),
        # 2-3. A zero or negative Chatwoot id is not a real message or
        # conversation, and storing one would hand the reaction path an unusable
        # "proof" that the application would then have to re-validate.
        sa.CheckConstraint("chatwoot_message_id > 0", name="ck_chatwoot_outbound_mirror_message_id"),
        sa.CheckConstraint("chatwoot_conversation_id > 0", name="ck_chatwoot_outbound_mirror_conversation_id"),
        # 4. An empty wamid cannot identify a Meta message.
        sa.CheckConstraint("length(provider_message_id) > 0", name="ck_chatwoot_outbound_mirror_wamid_present"),
        # 5. A closed route vocabulary.
        sa.CheckConstraint(f"chatwoot_route IN ({_ROUTE_SQL})", name="ck_chatwoot_outbound_mirror_route"),
    )


def downgrade() -> None:
    # Only the one object this revision created. Nothing else is touched, and
    # dropping it returns the reaction path to the bounded legacy scan.
    op.drop_table(MIRRORS)
