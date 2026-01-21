from __future__ import annotations

from datetime import datetime
from sqlalchemy import BigInteger, DateTime, Integer, String, Text, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class AltegioEvent(Base):
    __tablename__ = "altegio_events"

    id: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, autoincrement=True
    )

    # быстрый дедуп: один и тот же event не обработаем дважды
    dedupe_key: Mapped[str] = mapped_column(
        String(128), unique=True, index=True
    )

    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    processed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    # received/processing/processed/failed
    status: Mapped[str] = mapped_column(String(32), default="received")
    error: Mapped[str | None] = mapped_column(Text, nullable=True)

    # “плоские” поля для аналитики/фильтров
    company_id: Mapped[int | None] = mapped_column(
        Integer, index=True, nullable=True
    )
    # record/client/...
    resource: Mapped[str | None] = mapped_column(
        String(32), index=True, nullable=True
    )
    resource_id: Mapped[int | None] = mapped_column(
        BigInteger, index=True, nullable=True
    )
    # create/update/delete
    event_status: Mapped[str | None] = mapped_column(
        String(32), index=True, nullable=True)


    # сырьё (для восстановления и BI)
    query: Mapped[dict] = mapped_column(JSONB, default=dict)
    headers: Mapped[dict] = mapped_column(JSONB, default=dict)
    payload: Mapped[dict] = mapped_column(JSONB, default=dict)
