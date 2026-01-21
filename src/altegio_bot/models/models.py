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
