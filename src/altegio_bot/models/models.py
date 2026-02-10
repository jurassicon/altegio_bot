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
    Enum,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class AltegioEvent(Base):
    __tablename__ = "altegio_events"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    dedupe_key: Mapped[str] = mapped_column(String(128), unique=True, index=True)

    received_at: Mapped[datetime] = mapped_column(DateTime(
        timezone=True), server_default=func.now()
    )
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # received/processing/processed/failed
    status: Mapped[str] = mapped_column(String(32), default="received")
    error: Mapped[str | None] = mapped_column(Text, nullable=True)

    company_id: Mapped[int | None] = mapped_column(Integer, index=True, nullable=True)
    resource: Mapped[str | None] = mapped_column(String(32), index=True, nullable=True)
    resource_id: Mapped[int | None] = mapped_column(BigInteger, index=True, nullable=True)
    # create/update/delete
    event_status: Mapped[str | None] = mapped_column(String(32), index=True, nullable=True)

    query: Mapped[dict] = mapped_column(JSONB, default=dict)
    headers: Mapped[dict] = mapped_column(JSONB, default=dict)
    payload: Mapped[dict] = mapped_column(JSONB, default=dict)


class Client(Base):
    """
    Клиент в контексте филиала (company_id).
    Уникальность: (company_id, altegio_client_id).
    """
    __tablename__ = "clients"
    __table_args__ = (
        UniqueConstraint("company_id", "altegio_client_id", name="uq_clients_company_altegio_id"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    company_id: Mapped[int] = mapped_column(Integer, index=True)
    altegio_client_id: Mapped[int] = mapped_column(BigInteger, index=True)

    phone_e164: Mapped[str | None] = mapped_column(String(32), index=True, nullable=True)
    display_name: Mapped[str | None] = mapped_column(String(256), nullable=True)
    email: Mapped[str | None] = mapped_column(String(256), nullable=True)

    raw: Mapped[dict] = mapped_column(JSONB, default=dict)

    records: Mapped[list["Record"]] = relationship(back_populates="client")


class Record(Base):
    """
    Термин/запись в контексте филиала.
    Уникальность: (company_id, altegio_record_id).
    """
    __tablename__ = "records"
    __table_args__ = (
        UniqueConstraint("company_id", "altegio_record_id", name="uq_records_company_altegio_id"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    company_id: Mapped[int] = mapped_column(Integer, index=True)
    altegio_record_id: Mapped[int] = mapped_column(BigInteger, index=True)

    client_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("clients.id", ondelete="SET NULL"), index=True, nullable=True
    )
    altegio_client_id: Mapped[int | None] = mapped_column(BigInteger, index=True, nullable=True)

    staff_id: Mapped[int | None] = mapped_column(Integer, index=True, nullable=True)
    staff_name: Mapped[str | None] = mapped_column(String(256), nullable=True)

    starts_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True, nullable=True)
    ends_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    duration_sec: Mapped[int | None] = mapped_column(Integer, nullable=True)

    comment: Mapped[str | None] = mapped_column(Text, nullable=True)
    short_link: Mapped[str | None] = mapped_column(Text, nullable=True)

    confirmed: Mapped[int | None] = mapped_column(Integer, nullable=True)
    attendance: Mapped[int | None] = mapped_column(Integer, nullable=True)
    visit_attendance: Mapped[int | None] = mapped_column(Integer, nullable=True)

    is_deleted: Mapped[bool] = mapped_column(Boolean, default=False, index=True)

    total_cost: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), nullable=True)

    last_change_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

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
        BigInteger, ForeignKey("records.id", ondelete="CASCADE"), primary_key=True
    )
    service_id: Mapped[int] = mapped_column(Integer, primary_key=True)

    title: Mapped[str | None] = mapped_column(String(512), nullable=True)
    amount: Mapped[int | None] = mapped_column(Integer, nullable=True)
    cost_to_pay: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), nullable=True)

    raw: Mapped[dict] = mapped_column(JSONB, default=dict)

    record: Mapped[Record] = relationship(back_populates="services")


class MessageTemplate(Base):
    __tablename__ = "message_templates"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    company_id: Mapped[int] = mapped_column(Integer, index=True)

    # "record_created", "reminder_24h", ...
    code: Mapped[str] = mapped_column(String(64), index=True)

    # "de"
    language: Mapped[str] = mapped_column(String(8), default="de")

    # Текст шаблона с плейсхолдерами {client_name}, {date}, ...
    body: Mapped[str] = mapped_column(Text)

    is_active: Mapped[bool] = mapped_column(Boolean, default=True, index=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class MessageJob(Base):
    __tablename__ = "message_jobs"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    company_id: Mapped[int] = mapped_column(Integer, index=True)
    record_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("records.id", ondelete="CASCADE"), index=True, nullable=True
    )
    client_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("clients.id", ondelete="CASCADE"), index=True, nullable=True
    )

    # тип задачи: record_created/reminder_24h/...
    job_type: Mapped[str] = mapped_column(String(64), index=True)

    # когда надо отправить
    run_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)

    # queued/running/done/canceled/failed
    status: Mapped[str] = mapped_column(String(32), default="queued", index=True)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, default=5)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    # полезно для аналитики и идемпотентности
    dedupe_key: Mapped[str] = mapped_column(String(128), unique=True, index=True)

    payload: Mapped[dict] = mapped_column(JSONB, default=dict)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class OutboxMessage(Base):
    __tablename__ = "outbox_messages"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    company_id: Mapped[int] = mapped_column(Integer, index=True)
    client_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("clients.id", ondelete="SET NULL"), index=True, nullable=True
    )
    record_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("records.id", ondelete="SET NULL"), index=True, nullable=True
    )
    job_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("message_jobs.id", ondelete="SET NULL"), index=True, nullable=True
    )

    # куда (wa phone)
    phone_e164: Mapped[str] = mapped_column(String(32), index=True)

    template_code: Mapped[str] = mapped_column(String(64), index=True)
    language: Mapped[str] = mapped_column(String(8), default="de")
    body: Mapped[str] = mapped_column(Text)

    # queued/sending/sent/delivered/read/failed
    status: Mapped[str] = mapped_column(String(32), default="queued", index=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)

    # метаданные Meta (message_id)
    provider_message_id: Mapped[str | None] = mapped_column(String(128), index=True, nullable=True)

    scheduled_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    meta: Mapped[dict] = mapped_column(JSONB, default=dict)

    created_at: Mapped[datetime] = mapped_column(DateTime(
        timezone=True), server_default=func.now()
    )

    sender_id: Mapped[int | None] = mapped_column(
        BigInteger,
        ForeignKey("whatsapp_senders.id"),
        index=True,
        nullable=True
    )


class ContactRateLimit(Base):
    __tablename__ = "contact_rate_limits"

    phone_e164: Mapped[str] = mapped_column(String(32), primary_key=True)
    next_allowed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class WhatsAppSender(Base):
    __tablename__ = "whatsapp_senders"

    id: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, autoincrement=True
    )
    company_id: Mapped[int] = mapped_column(Integer, index=True)
    sender_code: Mapped[str] = mapped_column(String(32), index=True)

    phone_number_id: Mapped[str] = mapped_column(String(64))
    display_phone: Mapped[str | None] = mapped_column(
        String(32), nullable=True
    )

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
        BigInteger, primary_key=True, autoincrement=True
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
