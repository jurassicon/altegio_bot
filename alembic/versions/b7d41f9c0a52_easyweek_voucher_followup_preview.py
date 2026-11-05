"""EasyWeek voucher follow-up preview snapshot (§46.2, PR-22.2).

Additive and narrow. Two new tables and nothing else: no column of an existing
table is altered, no consent row, delivery fact, attempt counter, HMAC byte or
financial value is read or rewritten. The original mailing's ledger is the source
of truth a preview reads; a preview never writes back into it.

* ``easyweek_voucher_followup_previews`` — one immutable version of one
  proposal: the source batch it was computed from, the groups and interval the
  operator chose, the exact message contract, when the proofs were taken, how
  long they are treated as current, the counts and a deterministic digest of the
  included composition.
* ``easyweek_voucher_production_delivery_receipts`` — the durable proof §46.3
  asks of a POSITIVE status before it may become an audience: the verified
  signature and the route the callback named, written beside the slot's own
  transition. The slot's columns, counters and statistics are untouched, and a
  historical slot simply has no row, which is the honest answer.
* ``easyweek_voucher_followup_preview_rows`` — one row per source slot the
  selected groups reached, included or not, with a safe reason code. Its partial
  unique indexes ARE the source/dedupe contract PR-22.3 will claim against: one
  included row per source slot, per canonical customer UUID and per original
  entitlement.

Nothing in this revision opens a send path. There is no claim, no attempt, no
tombstone and no counter here; those belong to PR-22.3.
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "b7d41f9c0a52"
down_revision: Union[str, Sequence[str], None] = "a1c4e7b25d80"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

PREVIEWS = "easyweek_voucher_followup_previews"
ROWS = "easyweek_voucher_followup_preview_rows"
RECEIPTS = "easyweek_voucher_production_delivery_receipts"

# The three selectable audiences plus ``unproven``, which is how a stored row
# accounts for a slot none of the three can be proven for.
_GROUPS = "'read', 'delivered_unread', 'confirmed_failure', 'unproven'"
_DECISIONS = "'included', 'excluded', 'unknown'"

# The two positive statuses a receipt can prove, and the authenticity vocabulary
# shared with the inbound receipts of PR-22.1.
_RECEIPT_KINDS_SQL = "kind IN ('delivered', 'read')"
_RECEIPT_AUTHENTICITY_SQL = "authenticity IN ('signature_verified', 'unverified')"
_RECEIPT_MATCH_SQL = "match_state IN ('confirmed', 'unmatched', 'conflicting')"


def upgrade() -> None:
    op.create_table(
        PREVIEWS,
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("batch_id", sa.BigInteger(), nullable=False),
        sa.Column("campaign_run_id", sa.BigInteger(), nullable=False),
        sa.Column("company_id", sa.Integer(), nullable=False),
        sa.Column("location_uuid", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("source_request_schema_version", sa.String(16), nullable=False),
        sa.Column("source_frozen_digest", sa.String(64), nullable=False),
        sa.Column("stop_generation", sa.Integer(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("selected_groups", postgresql.JSONB(), nullable=False),
        sa.Column("min_interval_seconds", sa.BigInteger(), nullable=False),
        sa.Column("template_code", sa.String(64), nullable=False),
        sa.Column("meta_template_name", sa.String(128), nullable=False),
        sa.Column("template_language", sa.String(8), nullable=False),
        sa.Column("template_category", sa.String(32), nullable=False),
        sa.Column("template_parameter_order", postgresql.JSONB(), nullable=False),
        sa.Column("template_body_digest", sa.String(64), nullable=False),
        sa.Column("template_meta_verified", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("contract_version", sa.String(32), nullable=False),
        sa.Column("failure_policy_version", sa.String(32), nullable=False),
        sa.Column("checked_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("valid_until", sa.DateTime(timezone=True), nullable=False),
        sa.Column("included_count", sa.Integer(), nullable=False),
        sa.Column("excluded_count", sa.Integer(), nullable=False),
        sa.Column("unknown_count", sa.Integer(), nullable=False),
        sa.Column("composition_digest", sa.String(64), nullable=False),
        sa.Column("operator_principal", sa.String(128), nullable=False),
        sa.Column("evidence", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("batch_id", "version", name="uq_ew_followup_preview_batch_version"),
        sa.CheckConstraint("version >= 1", name="ck_ew_followup_preview_version"),
        sa.CheckConstraint("min_interval_seconds > 0", name="ck_ew_followup_preview_interval_positive"),
        sa.CheckConstraint(
            "jsonb_array_length(selected_groups) >= 1",
            name="ck_ew_followup_preview_groups_present",
        ),
        sa.CheckConstraint(
            "included_count >= 0 AND excluded_count >= 0 AND unknown_count >= 0",
            name="ck_ew_followup_preview_counts_non_negative",
        ),
        sa.CheckConstraint("valid_until > checked_at", name="ck_ew_followup_preview_validity_window"),
    )
    op.create_index("ix_ew_followup_preview_batch", PREVIEWS, ["batch_id", "version"])

    op.create_table(
        ROWS,
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("preview_id", sa.BigInteger(), nullable=False),
        sa.Column("source_batch_id", sa.BigInteger(), nullable=False),
        sa.Column("source_item_id", sa.BigInteger(), nullable=False),
        sa.Column("source_slot", sa.Integer(), nullable=False),
        sa.Column("campaign_recipient_id", sa.BigInteger(), nullable=False),
        sa.Column("easyweek_customer_uuid", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("source_entitlement_digest", sa.String(64), nullable=False),
        sa.Column("recipient_digest", sa.String(64), nullable=True),
        sa.Column("source_attempted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("group_code", sa.String(24), nullable=False),
        sa.Column("decision", sa.String(16), nullable=False),
        sa.Column("reason_code", sa.String(64), nullable=True),
        sa.Column("evidence", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(
            ["preview_id"],
            [f"{PREVIEWS}.id"],
            ondelete="CASCADE",
            name="fk_ew_followup_preview_row_preview",
        ),
        sa.UniqueConstraint("preview_id", "source_item_id", name="uq_ew_followup_preview_row_item"),
        sa.CheckConstraint(f"group_code IN ({_GROUPS})", name="ck_ew_followup_preview_row_group"),
        sa.CheckConstraint(f"decision IN ({_DECISIONS})", name="ck_ew_followup_preview_row_decision"),
        sa.CheckConstraint(
            "(decision = 'included') = (reason_code IS NULL)",
            name="ck_ew_followup_preview_row_reason_paired",
        ),
    )
    op.create_index(
        "uq_ew_followup_preview_row_included_customer",
        ROWS,
        ["preview_id", "easyweek_customer_uuid"],
        unique=True,
        postgresql_where=sa.text("decision = 'included'"),
    )
    op.create_index(
        "uq_ew_followup_preview_row_included_entitlement",
        ROWS,
        ["preview_id", "source_entitlement_digest"],
        unique=True,
        postgresql_where=sa.text("decision = 'included'"),
    )
    op.create_index("ix_ew_followup_preview_row_preview", ROWS, ["preview_id", "decision"])


    op.create_table(
        RECEIPTS,
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("fact_key", sa.String(64), nullable=False),
        sa.Column("kind", sa.String(16), nullable=False),
        sa.Column("authenticity", sa.String(24), nullable=False),
        sa.Column("match_state", sa.String(16), nullable=False, server_default=sa.text("'confirmed'")),
        sa.Column("item_id", sa.BigInteger(), nullable=True),
        sa.Column("attempt_id", sa.BigInteger(), nullable=True),
        sa.Column("batch_id", sa.BigInteger(), nullable=True),
        sa.Column("slot", sa.Integer(), nullable=True),
        sa.Column("provider_message_id", sa.String(128), nullable=False),
        sa.Column("observed_sender_digest", sa.String(64), nullable=True),
        sa.Column("observed_recipient_digest", sa.String(64), nullable=True),
        sa.Column("route_proven", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("whatsapp_event_id", sa.BigInteger(), nullable=True),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("event_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("evidence", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("fact_key", name="uq_ew_voucher_receipt_fact"),
        sa.ForeignKeyConstraint(
            ["item_id"],
            ["easyweek_voucher_production_batch_items.id"],
            name="fk_ew_voucher_receipt_item",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["attempt_id"],
            ["easyweek_voucher_production_batch_attempts.id"],
            name="fk_ew_voucher_receipt_attempt",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["whatsapp_event_id"],
            ["whatsapp_events.id"],
            name="fk_ew_voucher_receipt_event",
            ondelete="SET NULL",
        ),
        sa.CheckConstraint(_RECEIPT_KINDS_SQL, name="ck_ew_voucher_receipt_kind"),
        sa.CheckConstraint(_RECEIPT_AUTHENTICITY_SQL, name="ck_ew_voucher_receipt_authenticity"),
        sa.CheckConstraint(_RECEIPT_MATCH_SQL, name="ck_ew_voucher_receipt_match"),
        # A stored row IS the proof, so the database requires each part. The
        # signature is required of every row — it cannot be established later —
        # while the slot and the route are required of a CONFIRMED one.
        sa.CheckConstraint("authenticity = 'signature_verified'", name="ck_ew_voucher_receipt_signed"),
        sa.CheckConstraint(
            "match_state <> 'confirmed' OR ("
            "item_id IS NOT NULL AND batch_id IS NOT NULL AND slot IS NOT NULL AND route_proven"
            ")",
            name="ck_ew_voucher_receipt_confirmed_proof",
        ),
        sa.CheckConstraint(
            "match_state <> 'unmatched' OR ("
            "item_id IS NULL AND batch_id IS NULL AND slot IS NULL AND NOT route_proven"
            ")",
            name="ck_ew_voucher_receipt_unmatched_has_no_item",
        ),
        sa.CheckConstraint(
            "match_state <> 'conflicting' OR item_id IS NOT NULL",
            name="ck_ew_voucher_receipt_conflicting_has_item",
        ),
        sa.CheckConstraint(
            "NOT route_proven OR (observed_sender_digest IS NOT NULL AND observed_recipient_digest IS NOT NULL)",
            name="ck_ew_voucher_receipt_route_proven",
        ),
    )
    op.create_index("ix_ew_voucher_receipt_item_kind", RECEIPTS, ["item_id", "kind"])
    op.create_index(
        "ix_ew_voucher_receipt_unmatched",
        RECEIPTS,
        ["provider_message_id"],
        postgresql_where=sa.text("match_state = 'unmatched'"),
    )


def downgrade() -> None:
    """Refuse while any preview exists, and refuse BEFORE any DDL.

    A preview carries an operator's identity and the proposal they were looking
    at. It is recomputable from the original ledger, so losing one is not a loss
    of truth the way a refusal or a delivery fact would be — but it is still an
    audit record this project does not drop silently, and the check is free.

    The tables are locked first, so a preview being written concurrently cannot
    land between the count and the drop.
    """
    bind = op.get_bind()
    for table in (ROWS, PREVIEWS, RECEIPTS):
        bind.execute(sa.text(f"LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE"))
    stored = bind.execute(sa.text(f"SELECT count(*) FROM {PREVIEWS}")).scalar_one()
    if int(stored or 0):
        raise RuntimeError(
            "refusing to drop the follow-up preview tables: "
            f"{int(stored)} preview version(s) are stored. "
            "Remove them deliberately first if that is really intended."
        )
    # And the proven positive statuses. A receipt is evidence about a message
    # that really was delivered, recorded once and unrecoverable afterwards —
    # the ingress strips the signature it was proven with — so §46.9's rule
    # applies: a destructive downgrade refuses while such facts exist.
    receipts = bind.execute(sa.text(f"SELECT count(*) FROM {RECEIPTS}")).scalar_one()
    if int(receipts or 0):
        raise RuntimeError(
            "refusing to drop the delivery receipt table: "
            f"{int(receipts)} proven status fact(s) are stored, and their proof "
            "cannot be recovered once dropped."
        )

    op.drop_index("ix_ew_voucher_receipt_unmatched", table_name=RECEIPTS)
    op.drop_index("ix_ew_voucher_receipt_item_kind", table_name=RECEIPTS)
    op.drop_table(RECEIPTS)
    op.drop_index("ix_ew_followup_preview_row_preview", table_name=ROWS)
    op.drop_index("uq_ew_followup_preview_row_included_entitlement", table_name=ROWS)
    op.drop_index("uq_ew_followup_preview_row_included_customer", table_name=ROWS)
    op.drop_table(ROWS)
    op.drop_index("ix_ew_followup_preview_batch", table_name=PREVIEWS)
    op.drop_table(PREVIEWS)
