"""bind the Chatwoot outbound mirror registry to one installation/account/generation

``c4e9a1b78d52`` created the registry keyed on the Meta wamid alone. That key is
globally unique for a *Meta message*, but the values it points at — a Chatwoot
``Message.id`` and ``conversation_id`` — are only meaningful inside one Chatwoot
installation and one account. Replace the database behind the same URL, restore a
dump, or move to another account, and those numeric ids start again from low
numbers. A mapping row from the old installation could then match a completely
unrelated new message and turn a reaction into a reply to the wrong thing.

So the row gets a namespace. ``chatwoot_scope_id`` is composed by the
``ChatwootClient`` that actually talks to Chatwoot — normalized base URL + account
id + an operator-rotated generation token, never the API token — and the lookup
requires an exact match on it. Bumping the generation retires every row of the
previous generation without deleting anything.

This revision is a separate child rather than an edit of ``c4e9a1b78d52``, because
that revision may already be applied somewhere; rewriting it would leave those
databases on a revision whose recorded DDL no longer matches.

Three changes:

* add the nullable ``chatwoot_scope_id`` column. Nullable is deliberate: rows
  written by the previous revision have no scope, and they must stay un-trusted
  rather than be guessed into the current installation. The application's lookup
  requires an exact scope match, which NULL can never satisfy, so those rows fail
  closed into the legacy bounded scan;
* replace the global unique key on ``provider_message_id`` with a composite one on
  ``(chatwoot_scope_id, provider_message_id)``. PostgreSQL treats NULLs as distinct
  in a unique key, so legacy rows are neither deduplicated nor constrained, which
  is exactly right for rows nothing will ever read;
* nothing else. No Outbox, WhatsApp event or neighbouring table is touched, and no
  data is backfilled.

Downgrade deduplicates deterministically before restoring the global unique key,
because after this revision two scopes may legitimately hold the same wamid and the
old constraint cannot express that. It keeps the LOWEST ``id`` per
``provider_message_id`` and deletes the rest. That is safe for this table
specifically: every row is derived state that the application can lose without
consequence — a missing mapping row only means the reaction falls back to the
bounded scan and then to the visible quote. Doing it this way also means the
downgrade cannot fail on live data, which for a rollback path matters more than
keeping rows nothing can trust after the rollback anyway.

Revision ID: d7b2f6a4c318
Revises: c4e9a1b78d52
Create Date: 2026-09-27
"""

import sqlalchemy as sa

from alembic import op

revision = "d7b2f6a4c318"
down_revision = "c4e9a1b78d52"
branch_labels = None
depends_on = None

MIRRORS = "chatwoot_outbound_mirrors"

OLD_UNIQUE = "uq_chatwoot_outbound_mirror_provider_message"
NEW_UNIQUE = "uq_chatwoot_outbound_mirror_scope_provider_message"

# Matches CHATWOOT_SCOPE_ID_MAX_CHARS in chatwoot_client.py. Spelled out rather
# than imported: a migration must keep meaning the same thing after constants move.
_SCOPE_ID_MAX_CHARS = 200


def upgrade() -> None:
    op.add_column(
        MIRRORS,
        sa.Column("chatwoot_scope_id", sa.String(length=_SCOPE_ID_MAX_CHARS), nullable=True),
    )
    # Drop first, then add: the two keys overlap, and a wamid that is unique
    # globally is also unique per scope, so there is no window where a write that
    # was legal before becomes illegal.
    op.drop_constraint(OLD_UNIQUE, MIRRORS, type_="unique")
    op.create_unique_constraint(NEW_UNIQUE, MIRRORS, ["chatwoot_scope_id", "provider_message_id"])


def downgrade() -> None:
    op.drop_constraint(NEW_UNIQUE, MIRRORS, type_="unique")

    # Deterministic deduplication, required before the global key can come back:
    # after the upgrade the same wamid may exist once per scope. Keep the lowest
    # id, drop the rest. Losing a derived mapping row costs only a fallback.
    op.execute(
        sa.text(
            f"""
            DELETE FROM {MIRRORS}
            WHERE id NOT IN (
                SELECT MIN(id) FROM {MIRRORS} GROUP BY provider_message_id
            )
            """
        )
    )

    op.drop_column(MIRRORS, "chatwoot_scope_id")
    op.create_unique_constraint(OLD_UNIQUE, MIRRORS, ["provider_message_id"])
