"""EasyWeek voucher follow-up SEND ledger (§46.7–46.8, PR-22.3).

Additive and isolated. Four new tables and nothing else: no column of an
existing table is altered, and no consent row, delivery fact, attempt counter,
HMAC byte, financial value or schema 3/4 rule is read or rewritten. The original
mailing's ``send_attempt_count <= 1`` CHECK, its attempts, its receipts and its
failures are untouched — this ledger refers to them and never writes into them.

* ``easyweek_voucher_followup_approvals`` — one immutable, single-use offer with
  its own expiry: the operator, the exact action, the preview version and its
  composition digest, the groups, the interval, the whole message contract, the
  branch sender, the source stop generation and the count.
* ``easyweek_voucher_followup_runs`` — the durable record that an offer was
  spent. UNIQUE on ``approval_id``, so a double click, a refresh, two tabs and
  two operators converge on ONE run. Carries the executor's lease and the run's
  own terminal stop.
* ``easyweek_voucher_followup_sends`` — one row per original recipient, and the
  single place the one-extra-attempt rule lives. Its three partial unique
  indexes are scoped to the ORIGINAL mailing and the ORIGINAL entitlement: the
  source slot, the canonical customer within that batch, and the entitlement
  digest (which already carries the batch and the slot). A terminal stop writes
  a row in the same key space, so a tombstone blocks a resend exactly as a spent
  attempt does — and nothing here bans a customer uuid for life, so a future
  independent primary mailing is outside this key space entirely.
* ``easyweek_voucher_followup_callbacks`` — delivered / read / failed evidence
  about the follow-up's OWN message ids, including a callback that arrived
  before the acceptance was committed. It never touches the original message.

The downgrade refuses BEFORE any destructive DDL while a spent attempt or a
terminal tombstone exists, because dropping either would allow a second message
to the same person or let a stop be bypassed (§46.9).
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "c3f8a95d2e14"
down_revision: Union[str, Sequence[str], None] = "b7d41f9c0a52"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

APPROVALS = "easyweek_voucher_followup_approvals"
RUNS = "easyweek_voucher_followup_runs"
SENDS = "easyweek_voucher_followup_sends"
CALLBACKS = "easyweek_voucher_followup_callbacks"

PREVIEWS = "easyweek_voucher_followup_previews"
ITEMS = "easyweek_voucher_production_batch_items"

_APPROVAL_STATUSES = "'pending', 'consumed'"
_ACTION = "followup_send"

_RUN_STATUSES = "'queued', 'running', 'completed', 'stopped', 'interrupted', 'refused'"

_SEND_STATUSES = (
    "'planned', 'skipped', 'claimed', 'accepted', 'delivered', 'read', 'rejected', 'failed', 'unknown', 'stopped'"
)
# The states that account for the one extra attempt: a spent one, or a terminal
# tombstone. This predicate IS the single-attempt key.
_SEND_BLOCKING = "status IN ('claimed', 'accepted', 'delivered', 'read', 'rejected', 'failed', 'unknown', 'stopped')"
# The narrower set in which an external attempt really was consumed.
_SEND_ATTEMPTED = "'claimed', 'accepted', 'delivered', 'read', 'rejected', 'failed', 'unknown'"

_CALLBACK_KINDS = "'delivered', 'read', 'failed'"
_CALLBACK_MATCH = "'confirmed', 'unmatched', 'conflicting'"


def upgrade() -> None:
    op.create_table(
        APPROVALS,
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("action", sa.String(32), nullable=False),
        sa.Column("preview_id", sa.BigInteger(), nullable=False),
        sa.Column("preview_version", sa.Integer(), nullable=False),
        sa.Column("source_batch_id", sa.BigInteger(), nullable=False),
        sa.Column("campaign_run_id", sa.BigInteger(), nullable=False),
        sa.Column("company_id", sa.Integer(), nullable=False),
        sa.Column("location_uuid", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("source_request_schema_version", sa.String(16), nullable=False),
        sa.Column("source_frozen_digest", sa.String(64), nullable=False),
        sa.Column("source_stop_generation", sa.Integer(), nullable=False),
        sa.Column("composition_digest", sa.String(64), nullable=False),
        sa.Column("selected_groups", postgresql.JSONB(), nullable=False),
        sa.Column("min_interval_seconds", sa.BigInteger(), nullable=False),
        sa.Column("approved_recipient_count", sa.Integer(), nullable=False),
        sa.Column("recipient_digest", sa.String(64), nullable=False),
        sa.Column("template_code", sa.String(64), nullable=False),
        sa.Column("meta_template_name", sa.String(128), nullable=False),
        sa.Column("template_language", sa.String(8), nullable=False),
        sa.Column("template_category", sa.String(32), nullable=False),
        sa.Column("template_parameter_order", postgresql.JSONB(), nullable=False),
        sa.Column("template_body_digest", sa.String(64), nullable=False),
        sa.Column("contract_version", sa.String(32), nullable=False),
        sa.Column("failure_policy_version", sa.String(32), nullable=False),
        sa.Column("sender_digest", sa.String(64), nullable=False),
        sa.Column("principal", sa.String(128), nullable=False),
        sa.Column("session_fingerprint", sa.String(64), nullable=False),
        sa.Column("identification_limit", sa.String(32), nullable=False),
        sa.Column("nonce", sa.String(64), nullable=False),
        sa.Column("plan_signature", sa.String(64), nullable=False),
        sa.Column("issued_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("consumed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("nonce", name="uq_ew_followup_approval_nonce"),
        sa.ForeignKeyConstraint(
            ["preview_id"],
            [f"{PREVIEWS}.id"],
            name="fk_ew_followup_approval_preview",
            ondelete="RESTRICT",
        ),
        sa.CheckConstraint(f"status IN ({_APPROVAL_STATUSES})", name="ck_ew_followup_approval_status"),
        sa.CheckConstraint(f"action = '{_ACTION}'", name="ck_ew_followup_approval_action"),
        sa.CheckConstraint("expires_at > issued_at", name="ck_ew_followup_approval_window"),
        sa.CheckConstraint("approved_recipient_count >= 0", name="ck_ew_followup_approval_count"),
        sa.CheckConstraint("min_interval_seconds > 0", name="ck_ew_followup_approval_interval"),
        sa.CheckConstraint(
            "jsonb_array_length(selected_groups) >= 1",
            name="ck_ew_followup_approval_groups_present",
        ),
        sa.CheckConstraint(
            "(status = 'consumed') = (consumed_at IS NOT NULL)",
            name="ck_ew_followup_approval_consumed_paired",
        ),
    )
    op.create_index("ix_ew_followup_approval_preview", APPROVALS, ["preview_id", "status"])

    op.create_table(
        RUNS,
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("approval_id", sa.BigInteger(), nullable=False),
        sa.Column("preview_id", sa.BigInteger(), nullable=False),
        sa.Column("preview_version", sa.Integer(), nullable=False),
        sa.Column("source_batch_id", sa.BigInteger(), nullable=False),
        sa.Column("campaign_run_id", sa.BigInteger(), nullable=False),
        sa.Column("company_id", sa.Integer(), nullable=False),
        sa.Column("composition_digest", sa.String(64), nullable=False),
        sa.Column("principal", sa.String(128), nullable=False),
        sa.Column("session_fingerprint", sa.String(64), nullable=False),
        sa.Column("identification_limit", sa.String(32), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("planned_count", sa.Integer(), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("lease_owner", sa.String(128), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("stop_requested_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("stop_requested_by", sa.String(128), nullable=True),
        sa.Column("queued_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("outcome_code", sa.String(64), nullable=True),
        sa.Column("reason_codes", postgresql.JSONB(), nullable=True),
        sa.Column("result", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("approval_id", name="uq_ew_followup_run_approval"),
        sa.ForeignKeyConstraint(
            ["approval_id"],
            [f"{APPROVALS}.id"],
            name="fk_ew_followup_run_approval",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["preview_id"],
            [f"{PREVIEWS}.id"],
            name="fk_ew_followup_run_preview",
            ondelete="RESTRICT",
        ),
        sa.CheckConstraint(f"status IN ({_RUN_STATUSES})", name="ck_ew_followup_run_status"),
        sa.CheckConstraint("planned_count >= 0", name="ck_ew_followup_run_planned_count"),
        sa.CheckConstraint(
            "(status = 'running') OR lease_owner IS NULL",
            name="ck_ew_followup_run_lease_only_while_running",
        ),
        sa.CheckConstraint(
            "stop_requested_at IS NOT NULL OR stop_requested_by IS NULL",
            name="ck_ew_followup_run_stop_paired",
        ),
    )
    op.create_index("ix_ew_followup_run_source", RUNS, ["source_batch_id", "id"])
    op.create_index(
        "ix_ew_followup_run_queued",
        RUNS,
        ["id"],
        postgresql_where=sa.text("status = 'queued'"),
    )

    op.create_table(
        SENDS,
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("run_id", sa.BigInteger(), nullable=False),
        sa.Column("source_batch_id", sa.BigInteger(), nullable=False),
        sa.Column("source_item_id", sa.BigInteger(), nullable=False),
        sa.Column("source_slot", sa.Integer(), nullable=False),
        sa.Column("campaign_recipient_id", sa.BigInteger(), nullable=False),
        sa.Column("easyweek_customer_uuid", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("source_entitlement_digest", sa.String(64), nullable=False),
        sa.Column("source_attempted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("group_code", sa.String(24), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("intent_uuid", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("answered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("stopped_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("provider_message_id", sa.String(128), nullable=True),
        sa.Column("http_status", sa.Integer(), nullable=True),
        sa.Column("reason_code", sa.String(64), nullable=True),
        sa.Column("sender_digest", sa.String(64), nullable=True),
        sa.Column("recipient_digest", sa.String(64), nullable=True),
        sa.Column("template_code", sa.String(64), nullable=False),
        sa.Column("meta_template_name", sa.String(128), nullable=False),
        sa.Column("template_language", sa.String(8), nullable=False),
        sa.Column("template_body_digest", sa.String(64), nullable=False),
        sa.Column("consent_fingerprint", sa.String(64), nullable=True),
        sa.Column("delivered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("read_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("failed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("contradiction_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("evidence", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("run_id", "source_item_id", name="uq_ew_followup_send_run_item"),
        sa.UniqueConstraint("intent_uuid", name="uq_ew_followup_send_intent"),
        sa.UniqueConstraint("provider_message_id", name="uq_ew_followup_send_message"),
        sa.ForeignKeyConstraint(
            ["run_id"],
            [f"{RUNS}.id"],
            name="fk_ew_followup_send_run",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["source_item_id"],
            [f"{ITEMS}.id"],
            name="fk_ew_followup_send_item",
            ondelete="RESTRICT",
        ),
        sa.CheckConstraint(f"status IN ({_SEND_STATUSES})", name="ck_ew_followup_send_status"),
        sa.CheckConstraint("attempt_count >= 0 AND attempt_count <= 1", name="ck_ew_followup_send_attempt_bound"),
        sa.CheckConstraint(
            f"(attempt_count = 1) = (status IN ({_SEND_ATTEMPTED}))",
            name="ck_ew_followup_send_attempt_paired",
        ),
        sa.CheckConstraint(
            f"(status IN ({_SEND_ATTEMPTED})) = (intent_uuid IS NOT NULL AND claimed_at IS NOT NULL)",
            name="ck_ew_followup_send_claim_paired",
        ),
        sa.CheckConstraint(
            "provider_message_id IS NULL OR status IN ('accepted', 'delivered', 'read', 'failed', 'unknown')",
            name="ck_ew_followup_send_message_state",
        ),
        sa.CheckConstraint(
            "status NOT IN ('accepted', 'delivered', 'read') OR provider_message_id IS NOT NULL",
            name="ck_ew_followup_send_positive_has_message",
        ),
        sa.CheckConstraint(
            "status <> 'skipped' OR reason_code IS NOT NULL",
            name="ck_ew_followup_send_skip_reason",
        ),
        sa.CheckConstraint(
            "status <> 'stopped' OR stopped_at IS NOT NULL",
            name="ck_ew_followup_send_stop_paired",
        ),
        sa.CheckConstraint(
            f"status NOT IN ({_SEND_ATTEMPTED}) OR (sender_digest IS NOT NULL AND recipient_digest IS NOT NULL)",
            name="ck_ew_followup_send_route_frozen",
        ),
    )
    op.create_index(
        "uq_ew_followup_send_source_slot",
        SENDS,
        ["source_batch_id", "source_slot"],
        unique=True,
        postgresql_where=sa.text(_SEND_BLOCKING),
    )
    op.create_index(
        "uq_ew_followup_send_source_customer",
        SENDS,
        ["source_batch_id", "easyweek_customer_uuid"],
        unique=True,
        postgresql_where=sa.text(_SEND_BLOCKING),
    )
    op.create_index(
        "uq_ew_followup_send_source_entitlement",
        SENDS,
        ["source_entitlement_digest"],
        unique=True,
        postgresql_where=sa.text(_SEND_BLOCKING),
    )
    op.create_index("ix_ew_followup_send_run_status", SENDS, ["run_id", "status"])
    op.create_index("ix_ew_followup_send_source_batch", SENDS, ["source_batch_id", "source_slot"])

    op.create_table(
        CALLBACKS,
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("fact_key", sa.String(64), nullable=False),
        sa.Column("kind", sa.String(16), nullable=False),
        sa.Column("authenticity", sa.String(24), nullable=False),
        sa.Column("match_state", sa.String(16), nullable=False),
        sa.Column("send_id", sa.BigInteger(), nullable=True),
        sa.Column("run_id", sa.BigInteger(), nullable=True),
        sa.Column("source_batch_id", sa.BigInteger(), nullable=True),
        sa.Column("source_slot", sa.Integer(), nullable=True),
        sa.Column("provider_message_id", sa.String(128), nullable=False),
        sa.Column("observed_sender_digest", sa.String(64), nullable=True),
        sa.Column("observed_recipient_digest", sa.String(64), nullable=True),
        sa.Column("route_proven", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("meta_error_code", sa.Integer(), nullable=True),
        sa.Column("reason_code", sa.String(64), nullable=True),
        sa.Column("whatsapp_event_id", sa.BigInteger(), nullable=True),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("event_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("evidence", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("fact_key", name="uq_ew_followup_callback_fact"),
        sa.ForeignKeyConstraint(
            ["send_id"],
            [f"{SENDS}.id"],
            name="fk_ew_followup_callback_send",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["whatsapp_event_id"],
            ["whatsapp_events.id"],
            name="fk_ew_followup_callback_event",
            ondelete="SET NULL",
        ),
        sa.CheckConstraint(f"kind IN ({_CALLBACK_KINDS})", name="ck_ew_followup_callback_kind"),
        sa.CheckConstraint(f"match_state IN ({_CALLBACK_MATCH})", name="ck_ew_followup_callback_match"),
        sa.CheckConstraint(
            "authenticity IN ('signature_verified', 'unverified')",
            name="ck_ew_followup_callback_authenticity",
        ),
        sa.CheckConstraint(
            "match_state <> 'confirmed' OR ("
            "send_id IS NOT NULL AND route_proven AND authenticity = 'signature_verified'"
            ")",
            name="ck_ew_followup_callback_confirmed_proof",
        ),
        sa.CheckConstraint(
            "match_state <> 'unmatched' OR (send_id IS NULL AND NOT route_proven)",
            name="ck_ew_followup_callback_unmatched_has_no_row",
        ),
        sa.CheckConstraint(
            "NOT route_proven OR (observed_sender_digest IS NOT NULL AND observed_recipient_digest IS NOT NULL)",
            name="ck_ew_followup_callback_route_proven",
        ),
    )
    op.create_index("ix_ew_followup_callback_send_kind", CALLBACKS, ["send_id", "kind"])
    op.create_index(
        "ix_ew_followup_callback_unmatched",
        CALLBACKS,
        ["provider_message_id"],
        postgresql_where=sa.text("match_state = 'unmatched'"),
    )


def downgrade() -> None:
    """Refuse BEFORE any destructive DDL while irreplaceable facts exist.

    Two of these facts cannot be recomputed from anything else, and losing
    either would let a second message reach somebody:

    * a SPENT attempt. The row recording that this original recipient's one
      extra message was claimed is the only thing that stops a new run claiming
      it again. Dropping it would re-open the attempt;
    * a terminal STOP tombstone. It is an operator's decision that this person
      gets no follow-up to this mailing, and §46.7 makes it survive a new run, a
      new preview and a new template. Dropping it would bypass the stop.

    A ``planned`` or ``skipped`` row is neither: the first is a reservation the
    section allows to be recomputed and the second spends nothing, so a ledger
    holding only those drops cleanly.

    The follow-up callbacks are checked for the same reason PR-22.2's receipts
    are: the signature they were proven with is stripped from the stored event
    and cannot be recovered afterwards.

    Every table is locked first, so a claim or a callback committing
    concurrently cannot land between the count and the drop.
    """
    bind = op.get_bind()
    for table in (CALLBACKS, SENDS, RUNS, APPROVALS):
        bind.execute(sa.text(f"LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE"))

    spent = bind.execute(sa.text(f"SELECT count(*) FROM {SENDS} WHERE status IN ({_SEND_ATTEMPTED})")).scalar_one()
    if int(spent or 0):
        raise RuntimeError(
            "refusing to drop the follow-up send ledger: "
            f"{int(spent)} spent follow-up attempt(s) are recorded. Dropping them would "
            "re-open an attempt that may already have reached a real person."
        )
    tombstones = bind.execute(sa.text(f"SELECT count(*) FROM {SENDS} WHERE status = 'stopped'")).scalar_one()
    if int(tombstones or 0):
        raise RuntimeError(
            "refusing to drop the follow-up send ledger: "
            f"{int(tombstones)} terminal stop tombstone(s) are recorded, and dropping them "
            "would let a new run send to recipients an operator stopped for good."
        )
    callbacks = bind.execute(sa.text(f"SELECT count(*) FROM {CALLBACKS}")).scalar_one()
    if int(callbacks or 0):
        raise RuntimeError(
            "refusing to drop the follow-up callback table: "
            f"{int(callbacks)} proven follow-up status fact(s) are stored, and their proof "
            "cannot be recovered once dropped."
        )

    op.drop_index("ix_ew_followup_callback_unmatched", table_name=CALLBACKS)
    op.drop_index("ix_ew_followup_callback_send_kind", table_name=CALLBACKS)
    op.drop_table(CALLBACKS)

    op.drop_index("ix_ew_followup_send_source_batch", table_name=SENDS)
    op.drop_index("ix_ew_followup_send_run_status", table_name=SENDS)
    op.drop_index("uq_ew_followup_send_source_entitlement", table_name=SENDS)
    op.drop_index("uq_ew_followup_send_source_customer", table_name=SENDS)
    op.drop_index("uq_ew_followup_send_source_slot", table_name=SENDS)
    op.drop_table(SENDS)

    op.drop_index("ix_ew_followup_run_queued", table_name=RUNS)
    op.drop_index("ix_ew_followup_run_source", table_name=RUNS)
    op.drop_table(RUNS)

    op.drop_index("ix_ew_followup_approval_preview", table_name=APPROVALS)
    op.drop_table(APPROVALS)
