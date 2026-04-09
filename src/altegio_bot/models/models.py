from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class AltegioEvent(Base):
    __tablename__ = "altegio_events"

    id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
    )

    dedupe_key: Mapped[str] = mapped_column(
        String(128),
        unique=True,
        index=True,
    )

    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    processed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    # received/processing/processed/failed
    status: Mapped[str] = mapped_column(String(32), default="received")
    error: Mapped[str | None] = mapped_column(Text, nullable=True)

    company_id: Mapped[int | None] = mapped_column(
        Integer,
        index=True,
        nullable=True,
    )
    resource: Mapped[str | None] = mapped_column(
        String(32),
        index=True,
        nullable=True,
    )
    resource_id: Mapped[int | None] = mapped_column(
        BigInteger,
        index=True,
        nullable=True,
    )
    # create/update/delete
    event_status: Mapped[str | None] = mapped_column(
        String(32),
        index=True,
        nullable=True,
    )

    query: Mapped[dict] = mapped_column(JSONB, default=dict)
    headers: Mapped[dict] = mapped_column(JSONB, default=dict)
    payload: Mapped[dict] = mapped_column(JSONB, default=dict)


class SmartTestRun(Base):
    """Record of a smart-test execution for idempotency and auditing."""

    __tablename__ = "smart_test_runs"

    id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
    )

    test_code: Mapped[str] = mapped_column(String(128), index=True)
    phone_e164: Mapped[str] = mapped_column(String(32), index=True)
    company_id: Mapped[int] = mapped_column(Integer, index=True)
    location_id: Mapped[int | None] = mapped_column(Integer, nullable=True)

    loyalty_card_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    loyalty_card_number: Mapped[str | None] = mapped_column(String(64), nullable=True)
    loyalty_card_type_id: Mapped[str | None] = mapped_column(String(64), nullable=True)

    provider_message_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    template_name: Mapped[str | None] = mapped_column(String(128), nullable=True)

    # pending / pass / fail
    outcome: Mapped[str | None] = mapped_column(String(32), nullable=True)

    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    delete_status: Mapped[str | None] = mapped_column(String(32), nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        index=True,
    )

    meta: Mapped[dict] = mapped_column(JSONB, default=dict)


class Client(Base):
    """
    Клиент в контексте филиала (company_id).
    Уникальность: (company_id, altegio_client_id).
    """

    __tablename__ = "clients"
    __table_args__ = (
        UniqueConstraint(
            "company_id",
            "altegio_client_id",
            name="uq_clients_company_altegio_id",
        ),
    )

    id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
    )

    company_id: Mapped[int] = mapped_column(Integer, index=True)
    altegio_client_id: Mapped[int] = mapped_column(BigInteger, index=True)

    phone_e164: Mapped[str | None] = mapped_column(
        String(32),
        index=True,
        nullable=True,
    )
    display_name: Mapped[str | None] = mapped_column(String(256), nullable=True)
    email: Mapped[str | None] = mapped_column(String(256), nullable=True)

    raw: Mapped[dict] = mapped_column(JSONB, default=dict)

    wa_opted_out: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        server_default=text("false"),
        index=True,
    )
    wa_opted_out_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    wa_opt_out_reason: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )

    records: Mapped[list["Record"]] = relationship(back_populates="client")


class Record(Base):
    """
    Термин/запись в контексте филиала.
    Уникальность: (company_id, altegio_record_id).
    """

    __tablename__ = "records"
    __table_args__ = (
        UniqueConstraint(
            "company_id",
            "altegio_record_id",
            name="uq_records_company_altegio_id",
        ),
    )

    id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
    )

    company_id: Mapped[int] = mapped_column(Integer, index=True)
    altegio_record_id: Mapped[int] = mapped_column(BigInteger, index=True)

    client_id: Mapped[int | None] = mapped_column(
        BigInteger,
        ForeignKey("clients.id", ondelete="SET NULL"),
        index=True,
        nullable=True,
    )
    altegio_client_id: Mapped[int | None] = mapped_column(
        BigInteger,
        index=True,
        nullable=True,
    )

    staff_id: Mapped[int | None] = mapped_column(
        Integer,
        index=True,
        nullable=True,
    )
    staff_name: Mapped[str | None] = mapped_column(String(256), nullable=True)

    starts_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        index=True,
        nullable=True,
    )
    ends_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    duration_sec: Mapped[int | None] = mapped_column(Integer, nullable=True)

    comment: Mapped[str | None] = mapped_column(Text, nullable=True)
    short_link: Mapped[str | None] = mapped_column(Text, nullable=True)

    confirmed: Mapped[int | None] = mapped_column(Integer, nullable=True)
    attendance: Mapped[int | None] = mapped_column(Integer, nullable=True)
    visit_attendance: Mapped[int | None] = mapped_column(Integer, nullable=True)

    is_deleted: Mapped[bool] = mapped_column(
        Boolean,
        default=False,
        index=True,
    )

    total_cost: Mapped[Decimal | None] = mapped_column(
        Numeric(12, 2),
        nullable=True,
    )

    last_change_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    raw: Mapped[dict] = mapped_column(JSONB, default=dict)

    client: Mapped[Client | None] = relationship(back_populates="records")
    services: Mapped[list["RecordService"]] = relationship(
        back_populates="record",
        cascade="all, delete-orphan",
    )


class RecordService(Base):
    """
    Услуги внутри записи. Ключ: (record_id, service_id)
    """

    __tablename__ = "record_services"

    record_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("records.id", ondelete="CASCADE"),
        primary_key=True,
    )
    service_id: Mapped[int] = mapped_column(Integer, primary_key=True)

    title: Mapped[str | None] = mapped_column(String(512), nullable=True)
    amount: Mapped[int | None] = mapped_column(Integer, nullable=True)
    cost_to_pay: Mapped[Decimal | None] = mapped_column(
        Numeric(12, 2),
        nullable=True,
    )

    raw: Mapped[dict] = mapped_column(JSONB, default=dict)

    record: Mapped[Record] = relationship(back_populates="services")


class MessageTemplate(Base):
    __tablename__ = "message_templates"

    id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
    )
    company_id: Mapped[int] = mapped_column(Integer, index=True)

    # "record_created", "reminder_24h", ...
    code: Mapped[str] = mapped_column(String(64), index=True)

    # "de"
    language: Mapped[str] = mapped_column(String(8), default="de")

    # Текст шаблона с плейсхолдерами {client_name}, {date}, ...
    body: Mapped[str] = mapped_column(Text)

    is_active: Mapped[bool] = mapped_column(Boolean, default=True, index=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class MessageJob(Base):
    __tablename__ = "message_jobs"

    id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
    )

    company_id: Mapped[int] = mapped_column(Integer, index=True)
    record_id: Mapped[int | None] = mapped_column(
        BigInteger,
        ForeignKey("records.id", ondelete="CASCADE"),
        index=True,
        nullable=True,
    )
    client_id: Mapped[int | None] = mapped_column(
        BigInteger,
        ForeignKey("clients.id", ondelete="CASCADE"),
        index=True,
        nullable=True,
    )

    # тип задачи: record_created/reminder_24h/...
    job_type: Mapped[str] = mapped_column(String(64), index=True)

    # когда надо отправить
    run_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        index=True,
    )

    # queued/running/done/canceled/failed
    status: Mapped[str] = mapped_column(
        String(32),
        index=True,
        nullable=False,
        server_default=text("'queued'"),
    )
    attempts: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default=text("0"),
    )
    max_attempts: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default=text("5"),
    )
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    # полезно для аналитики и идемпотентности
    dedupe_key: Mapped[str] = mapped_column(
        String(128),
        unique=True,
        index=True,
    )

    payload: Mapped[dict] = mapped_column(JSONB, default=dict)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )

    locked_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        index=True,
    )


class OutboxMessage(Base):
    __tablename__ = "outbox_messages"

    id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
    )

    company_id: Mapped[int] = mapped_column(Integer, index=True)
    client_id: Mapped[int | None] = mapped_column(
        BigInteger,
        ForeignKey("clients.id", ondelete="SET NULL"),
        index=True,
        nullable=True,
    )
    record_id: Mapped[int | None] = mapped_column(
        BigInteger,
        ForeignKey("records.id", ondelete="SET NULL"),
        index=True,
        nullable=True,
    )
    job_id: Mapped[int | None] = mapped_column(
        BigInteger,
        ForeignKey("message_jobs.id", ondelete="SET NULL"),
        index=True,
        nullable=True,
    )

    # куда (wa phone)
    phone_e164: Mapped[str] = mapped_column(String(32), index=True)

    template_code: Mapped[str] = mapped_column(String(64), index=True)
    language: Mapped[str] = mapped_column(String(8), default="de")
    body: Mapped[str] = mapped_column(Text)

    # queued/sending/sent/delivered/read/failed
    status: Mapped[str] = mapped_column(String(32), default="queued", index=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)

    provider_message_id: Mapped[str | None] = mapped_column(
        String(128),
        index=True,
        nullable=True,
    )

    scheduled_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        index=True,
    )
    sent_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    meta: Mapped[dict] = mapped_column(JSONB, default=dict)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )

    sender_id: Mapped[int | None] = mapped_column(
        BigInteger,
        ForeignKey("whatsapp_senders.id"),
        index=True,
        nullable=True,
    )


class ContactRateLimit(Base):
    __tablename__ = "contact_rate_limits"

    phone_e164: Mapped[str] = mapped_column(String(32), primary_key=True)
    next_allowed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        index=True,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class WhatsAppSender(Base):
    __tablename__ = "whatsapp_senders"

    id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
    )
    company_id: Mapped[int] = mapped_column(Integer, index=True)
    sender_code: Mapped[str] = mapped_column(String(32), index=True)

    phone_number_id: Mapped[str] = mapped_column(String(64))
    display_phone: Mapped[str | None] = mapped_column(String(32), nullable=True)

    is_active: Mapped[bool] = mapped_column(Boolean, default=True)

    __table_args__ = (
        UniqueConstraint(
            "company_id",
            "sender_code",
            name="uq_whatsapp_senders_company_code",
        ),
    )


class ServiceSenderRule(Base):
    __tablename__ = "service_sender_rules"

    id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
    )

    company_id: Mapped[int] = mapped_column(Integer, index=True)
    service_id: Mapped[int] = mapped_column(Integer, index=True)

    sender_code: Mapped[str] = mapped_column(String(32))

    __table_args__ = (
        UniqueConstraint(
            "company_id",
            "service_id",
            name="uq_service_sender_rules_company_service",
        ),
    )


class WhatsAppEvent(Base):
    __tablename__ = "whatsapp_events"

    id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
    )

    dedupe_key: Mapped[str] = mapped_column(
        String(128),
        unique=True,
        index=True,
    )

    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        index=True,
    )
    processed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    status: Mapped[str] = mapped_column(
        String(32),
        default="received",
        index=True,
    )
    error: Mapped[str | None] = mapped_column(Text, nullable=True)

    company_id: Mapped[int | None] = mapped_column(
        Integer,
        index=True,
        nullable=True,
    )
    resource: Mapped[str | None] = mapped_column(
        String(32),
        index=True,
        nullable=True,
    )
    resource_id: Mapped[int | None] = mapped_column(
        BigInteger,
        index=True,
        nullable=True,
    )
    event_status: Mapped[str | None] = mapped_column(
        String(32),
        index=True,
        nullable=True,
    )

    query: Mapped[dict] = mapped_column(JSONB, default=dict)
    headers: Mapped[dict] = mapped_column(JSONB, default=dict)
    payload: Mapped[dict] = mapped_column(JSONB, default=dict)

    # Chatwoot conversation that originated this event (set when webhook comes
    # from Chatwoot instead of Meta directly)
    chatwoot_conversation_id: Mapped[int | None] = mapped_column(
        BigInteger,
        nullable=True,
        index=True,
    )


# ---------------------------------------------------------------------------
# Campaign models
# ---------------------------------------------------------------------------


class CampaignRun(Base):
    """Один запуск кампании: preview или send-real."""

    __tablename__ = "campaign_runs"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    # Код кампании, например 'new_clients_monthly'
    campaign_code: Mapped[str] = mapped_column(String(128), index=True)

    # 'preview' | 'send-real'
    mode: Mapped[str] = mapped_column(String(32))

    # Список company_id, охваченных рассылкой
    company_ids: Mapped[list] = mapped_column(JSONB, default=list)

    # Ссылка на preview-run, на базе которого запущен send-real.
    # Nullable: у preview этого поля нет.
    source_preview_run_id: Mapped[int | None] = mapped_column(
        BigInteger,
        ForeignKey("campaign_runs.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )

    # Параметры loyalty-карт (для send-real)
    location_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    card_type_id: Mapped[str | None] = mapped_column(String(64), nullable=True)

    period_start: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    period_end: Mapped[datetime] = mapped_column(DateTime(timezone=True))

    # running / completed / failed
    status: Mapped[str] = mapped_column(String(32), default="running")

    # Окно атрибуции в днях (по умолчанию 30)
    attribution_window_days: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("30"))

    # -----------------------------------------------------------------------
    # Follow-up policy
    # -----------------------------------------------------------------------
    followup_enabled: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        server_default=text("false"),
    )
    # Через сколько дней проверять follow-up (например, 7 или 14)
    followup_delay_days: Mapped[int | None] = mapped_column(Integer, nullable=True)
    # 'unread_only' | 'unread_or_not_booked'
    followup_policy: Mapped[str | None] = mapped_column(String(32), nullable=True)
    # WhatsApp template для follow-up (отдельный approved template)
    followup_template_name: Mapped[str | None] = mapped_column(String(128), nullable=True)

    # -----------------------------------------------------------------------
    # Счётчики сегментации
    # -----------------------------------------------------------------------
    total_clients_seen: Mapped[int] = mapped_column(Integer, default=0)
    candidates_count: Mapped[int] = mapped_column(Integer, default=0)

    # Исключения (legacy — оставлены для обратной совместимости)
    excluded_opted_out: Mapped[int] = mapped_column(Integer, default=0)
    excluded_more_than_one_record: Mapped[int] = mapped_column(Integer, default=0)
    excluded_has_arrived: Mapped[int] = mapped_column(Integer, default=0)
    excluded_no_phone: Mapped[int] = mapped_column(Integer, default=0)

    # Исключения (новые)
    excluded_multiple_records: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    excluded_no_confirmed_record: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    excluded_has_records_before: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    excluded_invalid_phone: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    excluded_no_whatsapp: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    # CRM API был недоступен — история клиента не проверена → исключён
    excluded_crm_unavailable: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    # Altegio service category API недоступен — ресничность услуги не определена → исключён
    excluded_service_category_unavailable: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("0")
    )

    # -----------------------------------------------------------------------
    # Счётчики доставки и атрибуции
    # -----------------------------------------------------------------------
    sent_count: Mapped[int] = mapped_column(Integer, default=0)
    failed_count: Mapped[int] = mapped_column(Integer, default=0)
    queued_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    provider_accepted_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    delivered_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    read_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    replied_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    booked_after_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    opted_out_after_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))

    # -----------------------------------------------------------------------
    # Счётчики loyalty-карт
    # -----------------------------------------------------------------------
    cleanup_failed_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    cards_deleted_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    cards_issued_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        index=True,
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    meta: Mapped[dict] = mapped_column(JSONB, default=dict)

    recipients: Mapped[list["CampaignRecipient"]] = relationship(
        back_populates="run",
        cascade="all, delete-orphan",
    )


class CampaignRecipient(Base):
    """
    Снимок сегментации и результат рассылки для одного клиента.
    Создаётся как для preview, так и для send-real.
    """

    __tablename__ = "campaign_recipients"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    campaign_run_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("campaign_runs.id", ondelete="CASCADE"),
        index=True,
    )

    company_id: Mapped[int] = mapped_column(Integer, index=True)

    client_id: Mapped[int | None] = mapped_column(
        BigInteger,
        ForeignKey("clients.id", ondelete="SET NULL"),
        index=True,
        nullable=True,
    )
    altegio_client_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    phone_e164: Mapped[str | None] = mapped_column(String(32), index=True, nullable=True)
    display_name: Mapped[str | None] = mapped_column(String(256), nullable=True)

    # -----------------------------------------------------------------------
    # Статус (полная цепочка)
    # candidate → skipped | cleanup_failed | cleanup_ok → card_issued
    #   → queue_failed | queued → provider_accepted → delivered
    #   → read → replied → booked_after_campaign
    # -----------------------------------------------------------------------
    status: Mapped[str] = mapped_column(String(64), default="candidate")

    # Причина исключения (если статус skipped/cleanup_failed).
    # Значения: opted_out / no_phone / invalid_phone / no_whatsapp /
    #           multiple_records_in_period / no_confirmed_record_in_period /
    #           has_records_before_period / cleanup_failed /
    #           provider_error / delivery_failed / card_issue_failed
    excluded_reason: Mapped[str | None] = mapped_column(String(64), nullable=True)

    # -----------------------------------------------------------------------
    # Снимок сегментации (заполняется при создании записи)
    # -----------------------------------------------------------------------
    # Все записи клиента в периоде (не удалённые)
    total_records_in_period: Mapped[int] = mapped_column(Integer, default=0)
    # Подтверждённые записи (confirmed == CONFIRMED_FLAG) в периоде
    confirmed_records_in_period: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    # Все записи клиента ДО начала периода (любой статус)
    records_before_period: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    # Legacy-поле (оставлено для обратной совместимости)
    arrived_records_in_period: Mapped[int] = mapped_column(Integer, default=0)
    is_opted_out: Mapped[bool] = mapped_column(Boolean, default=False)

    # -----------------------------------------------------------------------
    # CRM-диагностика (заполняется при сегментации через Altegio CRM API)
    # -----------------------------------------------------------------------
    # Ресничные записи в периоде (из local RecordService + category lookup)
    lash_records_in_period: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    # Подтверждённые ресничные записи в периоде
    confirmed_lash_records_in_period: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    # Названия услуг из периода (для диагностики)
    service_titles_in_period: Mapped[list] = mapped_column(
        JSONB, nullable=False, default=list, server_default=text("'[]'::jsonb")
    )
    # Все записи до начала периода по данным Altegio CRM (источник истины)
    total_records_before_period_any: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    # Найден ли локальный Client в нашей БД
    local_client_found: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))

    # -----------------------------------------------------------------------
    # Loyalty-карты
    # -----------------------------------------------------------------------
    # Выпущенная в этом run карта
    loyalty_card_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    loyalty_card_number: Mapped[str | None] = mapped_column(String(64), nullable=True)
    loyalty_card_type_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    # Список ID campaign-карт, удалённых перед выпуском новой
    cleanup_card_ids: Mapped[list] = mapped_column(
        JSONB,
        nullable=False,
        default=list,
        server_default=text("'[]'::jsonb"),
    )
    cleanup_failed_reason: Mapped[str | None] = mapped_column(String(256), nullable=True)

    # -----------------------------------------------------------------------
    # Tracking отправки
    # -----------------------------------------------------------------------
    message_job_id: Mapped[int | None] = mapped_column(
        BigInteger,
        ForeignKey("message_jobs.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    outbox_message_id: Mapped[int | None] = mapped_column(
        BigInteger,
        ForeignKey("outbox_messages.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    provider_message_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)

    # -----------------------------------------------------------------------
    # Attribution timestamps (заполняются по мере событий)
    # -----------------------------------------------------------------------
    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    read_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    replied_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    booked_after_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    opted_out_after_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # -----------------------------------------------------------------------
    # Follow-up
    # -----------------------------------------------------------------------
    followup_status: Mapped[str | None] = mapped_column(String(32), nullable=True)
    followup_message_job_id: Mapped[int | None] = mapped_column(
        BigInteger,
        ForeignKey("message_jobs.id", ondelete="SET NULL"),
        nullable=True,
    )
    followup_outbox_id: Mapped[int | None] = mapped_column(
        BigInteger,
        ForeignKey("outbox_messages.id", ondelete="SET NULL"),
        nullable=True,
    )
    followup_sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # Legacy-поля
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    delete_status: Mapped[str | None] = mapped_column(String(32), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    meta: Mapped[dict] = mapped_column(JSONB, default=dict)

    run: Mapped["CampaignRun"] = relationship(back_populates="recipients")
