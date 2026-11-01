"""Durable WhatsApp consent and proven voucher delivery failure evidence.

Three tables and three columns, each of them something this schema cannot
express today:

* ``whatsapp_inbound_receipts`` — what arrived, per message, with the
  authenticity the ingress could prove and a pending marker a send guard can
  read;
* ``whatsapp_consent_suppressions`` — the current consent answer per WhatsApp
  address, independent of any client card, with the monotonic version a final
  send guard orders itself against;
* ``easyweek_voucher_production_delivery_failures`` — proven failures of one
  original send, bindable to a slot after the fact;
* ``whatsapp_events.inbound_authenticity`` — whether this delivery's signature
  was checked, for the status-only payloads that carry no messages;
* two route digests on ``easyweek_voucher_production_batch_attempts``, frozen
  with the one attempt a slot ever gets.

Nothing historical is rewritten. No amount, no price, no attempt counter and no
binding is touched, and every new column is nullable, so an existing row keeps
saying exactly what it said before — which for authenticity and for the route
means "unknown", honestly, rather than a value derived from today's settings.

The downgrade refuses while any new consent or evidence fact exists, and it
refuses BEFORE any DDL: a refusal has to be undeletable even by a rollback, and
an operator who is told the rollback is unsafe must find the data still there.
"""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "a1c4e7b25d80"
down_revision = "c9e7a34d6210"
branch_labels = None
depends_on = None

RECEIPTS = "whatsapp_inbound_receipts"
SUPPRESSIONS = "whatsapp_consent_suppressions"
FAILURES = "easyweek_voucher_production_delivery_failures"
ATTEMPTS = "easyweek_voucher_production_batch_attempts"
EVENTS = "whatsapp_events"

_COMMANDS = "'stop', 'start', 'none'"
_AUTHENTICITIES = "'signature_verified', 'unverified'"
_RESOLUTIONS = "'applied', 'noop', 'refused', 'ignored_route'"
_STATES = "'opted_out', 'opted_in'"
_SOURCES = "'inbound_command', 'historical_projection'"
_FAILURE_SOURCES = "'meta_api_rejection', 'status_callback'"
_FAILURE_REASONS = "'meta_api_rejected', 'meta_callback_failed'"
_FAILURE_MATCHES = "'confirmed', 'unmatched', 'conflicting'"
_FAILURE_CLASSES = "'permanent', 'transient', 'unknown'"


def upgrade() -> None:
    op.add_column(EVENTS, sa.Column("inbound_authenticity", sa.String(24), nullable=True))
    op.add_column(ATTEMPTS, sa.Column("sender_phone_number_id_digest", sa.String(64), nullable=True))
    op.add_column(ATTEMPTS, sa.Column("recipient_digest", sa.String(64), nullable=True))

    op.create_table(
        RECEIPTS,
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("receipt_key", sa.String(64), nullable=False),
        sa.Column("whatsapp_event_id", sa.BigInteger(), nullable=True),
        sa.Column("message_index", sa.Integer(), nullable=False),
        sa.Column("wa_address", sa.String(32), nullable=False),
        sa.Column("command", sa.String(8), nullable=False),
        sa.Column("authenticity", sa.String(24), nullable=False),
        sa.Column("whatsapp_message_id", sa.String(128), nullable=True),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("event_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("resolution", sa.String(24), nullable=True),
        sa.Column("resolution_reason", sa.String(64), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(
            ["whatsapp_event_id"],
            [f"{EVENTS}.id"],
            ondelete="SET NULL",
        ),
        sa.UniqueConstraint("receipt_key", name="uq_wa_inbound_receipt_key"),
        sa.UniqueConstraint("whatsapp_event_id", "message_index", name="uq_wa_inbound_receipt_event_message"),
        sa.CheckConstraint(f"command IN ({_COMMANDS})", name="ck_wa_inbound_receipt_command"),
        sa.CheckConstraint(f"authenticity IN ({_AUTHENTICITIES})", name="ck_wa_inbound_receipt_authenticity"),
        sa.CheckConstraint(
            f"resolution IS NULL OR resolution IN ({_RESOLUTIONS})",
            name="ck_wa_inbound_receipt_resolution",
        ),
        sa.CheckConstraint(
            "(resolved_at IS NULL) = (resolution IS NULL)",
            name="ck_wa_inbound_receipt_resolution_paired",
        ),
        sa.CheckConstraint("message_index >= 0", name="ck_wa_inbound_receipt_index"),
    )
    op.create_index(
        "ix_wa_inbound_receipt_pending",
        RECEIPTS,
        ["wa_address"],
        postgresql_where=sa.text("resolved_at IS NULL"),
    )
    op.create_index("ix_wa_inbound_receipt_address_observed", RECEIPTS, ["wa_address", "observed_at"])
    op.create_index("ix_wa_inbound_receipt_event", RECEIPTS, ["whatsapp_event_id"])
    op.create_index(
        "ix_wa_inbound_receipt_message",
        RECEIPTS,
        ["whatsapp_message_id"],
        postgresql_where=sa.text("whatsapp_message_id IS NOT NULL"),
    )

    op.create_table(
        SUPPRESSIONS,
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("wa_address", sa.String(32), nullable=False),
        sa.Column("state", sa.String(16), nullable=False),
        sa.Column("consent_version", sa.BigInteger(), nullable=False),
        sa.Column("decision_command", sa.String(8), nullable=False),
        sa.Column("decision_source", sa.String(32), nullable=False),
        sa.Column("decision_authenticity", sa.String(24), nullable=False),
        sa.Column("decision_receipt_id", sa.BigInteger(), nullable=True),
        sa.Column("decided_observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("decided_event_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("opted_out_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_stop_observed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_stop_event_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_start_observed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_start_event_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("banned_easyweek_customer_uuid", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("banned_uuid_recorded_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("evidence", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(
            ["decision_receipt_id"],
            [f"{RECEIPTS}.id"],
            ondelete="SET NULL",
        ),
        sa.UniqueConstraint("wa_address", name="uq_wa_consent_address"),
        sa.CheckConstraint(f"state IN ({_STATES})", name="ck_wa_consent_state"),
        sa.CheckConstraint("decision_command IN ('stop', 'start')", name="ck_wa_consent_decision_command"),
        sa.CheckConstraint(
            f"decision_authenticity IN ({_AUTHENTICITIES})",
            name="ck_wa_consent_decision_authenticity",
        ),
        sa.CheckConstraint(f"decision_source IN ({_SOURCES})", name="ck_wa_consent_decision_source"),
        sa.CheckConstraint("consent_version >= 1", name="ck_wa_consent_version_positive"),
        sa.CheckConstraint("(state = 'opted_out') = (opted_out_at IS NOT NULL)", name="ck_wa_consent_opt_out_paired"),
        sa.CheckConstraint(
            "(banned_easyweek_customer_uuid IS NULL) = (banned_uuid_recorded_at IS NULL)",
            name="ck_wa_consent_banned_uuid_paired",
        ),
        # The rule that makes an unproven START harmless at the schema level:
        # an opted_in row whose decision was a START must carry a verified
        # signature and a proven event time, so an unsigned, replayed or
        # time-unknown START is unrepresentable rather than merely refused.
        sa.CheckConstraint(
            "decision_command <> 'start' OR ("
            "state = 'opted_in' "
            "AND decided_event_at IS NOT NULL "
            "AND decision_authenticity = 'signature_verified')",
            name="ck_wa_consent_start_needs_proof",
        ),
    )
    op.create_index(
        "ix_wa_consent_active",
        SUPPRESSIONS,
        ["wa_address"],
        postgresql_where=sa.text("state = 'opted_out'"),
    )
    op.create_index(
        "ix_wa_consent_banned_uuid",
        SUPPRESSIONS,
        ["banned_easyweek_customer_uuid"],
        postgresql_where=sa.text("banned_easyweek_customer_uuid IS NOT NULL"),
    )

    op.create_table(
        FAILURES,
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("fact_key", sa.String(64), nullable=False),
        sa.Column("source", sa.String(32), nullable=False),
        sa.Column("reason_code", sa.String(32), nullable=False),
        sa.Column("match_state", sa.String(16), nullable=False),
        sa.Column("failure_class", sa.String(16), nullable=False),
        sa.Column("authenticity", sa.String(24), nullable=False),
        sa.Column("item_id", sa.BigInteger(), nullable=True),
        sa.Column("attempt_id", sa.BigInteger(), nullable=True),
        sa.Column("batch_id", sa.BigInteger(), nullable=True),
        sa.Column("slot", sa.Integer(), nullable=True),
        sa.Column("intent_uuid", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("provider_message_id", sa.String(128), nullable=True),
        sa.Column("observed_sender_digest", sa.String(64), nullable=True),
        sa.Column("observed_recipient_digest", sa.String(64), nullable=True),
        sa.Column("route_proven", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("meta_error_code", sa.Integer(), nullable=True),
        sa.Column("http_status", sa.Integer(), nullable=True),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("event_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("whatsapp_event_id", sa.BigInteger(), nullable=True),
        sa.Column("evidence", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(
            ["item_id"],
            ["easyweek_voucher_production_batch_items.id"],
            name="fk_ew_voucher_failure_item",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["attempt_id"],
            [f"{ATTEMPTS}.id"],
            name="fk_ew_voucher_failure_attempt",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["whatsapp_event_id"],
            [f"{EVENTS}.id"],
            ondelete="SET NULL",
        ),
        sa.UniqueConstraint("fact_key", name="uq_ew_voucher_failure_fact"),
        sa.CheckConstraint(f"source IN ({_FAILURE_SOURCES})", name="ck_ew_voucher_failure_source"),
        sa.CheckConstraint(f"reason_code IN ({_FAILURE_REASONS})", name="ck_ew_voucher_failure_reason"),
        sa.CheckConstraint(f"match_state IN ({_FAILURE_MATCHES})", name="ck_ew_voucher_failure_match"),
        sa.CheckConstraint(f"failure_class IN ({_FAILURE_CLASSES})", name="ck_ew_voucher_failure_class"),
        sa.CheckConstraint(f"authenticity IN ({_AUTHENTICITIES})", name="ck_ew_voucher_failure_authenticity"),
        sa.CheckConstraint(
            "match_state <> 'confirmed' OR item_id IS NOT NULL",
            name="ck_ew_voucher_failure_confirmed_has_item",
        ),
        sa.CheckConstraint(
            "match_state <> 'unmatched' OR item_id IS NULL",
            name="ck_ew_voucher_failure_unmatched_has_no_item",
        ),
        sa.CheckConstraint(
            "source <> 'status_callback' OR provider_message_id IS NOT NULL",
            name="ck_ew_voucher_failure_callback_has_message",
        ),
        sa.CheckConstraint(
            "source <> 'meta_api_rejection' OR intent_uuid IS NOT NULL",
            name="ck_ew_voucher_failure_api_has_intent",
        ),
        sa.CheckConstraint(
            "NOT route_proven OR (observed_sender_digest IS NOT NULL AND observed_recipient_digest IS NOT NULL)",
            name="ck_ew_voucher_failure_route_proven",
        ),
    )
    op.create_index("ix_ew_voucher_failure_item", FAILURES, ["item_id"])
    op.create_index(
        "ix_ew_voucher_failure_message",
        FAILURES,
        ["provider_message_id"],
        postgresql_where=sa.text("provider_message_id IS NOT NULL"),
    )
    op.create_index(
        "ix_ew_voucher_failure_unmatched",
        FAILURES,
        ["provider_message_id"],
        postgresql_where=sa.text("match_state = 'unmatched'"),
    )


def downgrade() -> None:
    # The refusal to drop runs BEFORE any DDL, and under ACCESS EXCLUSIVE locks
    # taken first.
    #
    # Both halves matter. Checking emptiness without the locks would race a
    # concurrent STOP: the webhook could commit a refusal between the count and
    # the DROP, and the rollback would then delete a consent decision a real
    # person made seconds earlier. Dropping first and complaining afterwards
    # would be worse still — the data would already be gone.
    #
    # So: lock, prove empty, and only then touch the schema. An operator who is
    # told the rollback is unsafe finds every refusal and every failure fact
    # still exactly where it was.
    bind = op.get_bind()
    for table in (SUPPRESSIONS, RECEIPTS, FAILURES):
        op.execute(sa.text(f"LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE"))

    if bind.execute(sa.text(f"SELECT EXISTS (SELECT 1 FROM {SUPPRESSIONS})")).scalar_one():
        raise RuntimeError("WhatsApp consent refusals exist; destructive downgrade refused before DDL")
    if bind.execute(sa.text(f"SELECT EXISTS (SELECT 1 FROM {RECEIPTS})")).scalar_one():
        raise RuntimeError("WhatsApp inbound receipts exist; destructive downgrade refused before DDL")
    if bind.execute(sa.text(f"SELECT EXISTS (SELECT 1 FROM {FAILURES})")).scalar_one():
        raise RuntimeError("Voucher delivery failure evidence exists; destructive downgrade refused before DDL")

    op.drop_index("ix_ew_voucher_failure_unmatched", table_name=FAILURES)
    op.drop_index("ix_ew_voucher_failure_message", table_name=FAILURES)
    op.drop_index("ix_ew_voucher_failure_item", table_name=FAILURES)
    op.drop_table(FAILURES)

    op.drop_index("ix_wa_consent_banned_uuid", table_name=SUPPRESSIONS)
    op.drop_index("ix_wa_consent_active", table_name=SUPPRESSIONS)
    op.drop_table(SUPPRESSIONS)

    op.drop_index("ix_wa_inbound_receipt_message", table_name=RECEIPTS)
    op.drop_index("ix_wa_inbound_receipt_event", table_name=RECEIPTS)
    op.drop_index("ix_wa_inbound_receipt_address_observed", table_name=RECEIPTS)
    op.drop_index("ix_wa_inbound_receipt_pending", table_name=RECEIPTS)
    op.drop_table(RECEIPTS)

    # The two attempt digests and the event's authenticity are dropped last and
    # unconditionally: they are audit detail about rows that stay, not facts the
    # tables above hold, and nothing else reads them.
    op.drop_column(ATTEMPTS, "recipient_digest")
    op.drop_column(ATTEMPTS, "sender_phone_number_id_digest")
    op.drop_column(EVENTS, "inbound_authenticity")
