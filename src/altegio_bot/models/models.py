from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    LargeBinary,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PostgresUUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

# ---------------------------------------------------------------------------
# CRM provider scoping
# ---------------------------------------------------------------------------
# Rows from different CRMs live side by side in the same tables, so identity is
# scoped by ``provider`` rather than by ``company_id`` alone: EasyWeek numeric
# ids (location / booking / customer) share a namespace with Altegio ones and a
# collision is realistic, not theoretical.
#
# Deliberately a plain bounded string with a server default rather than an enum
# or a CHECK constraint: adding a third CRM later must be a code change, not a
# migration against a restrictive type.
PROVIDER_ALTEGIO = "altegio"
PROVIDER_EASYWEEK = "easyweek"

_PROVIDER_SERVER_DEFAULT = text(f"'{PROVIDER_ALTEGIO}'")


def _provider_column() -> Mapped[str]:
    """The identical ``provider`` column shared by every provider-scoped table.

    Existing Altegio constructors must keep working untouched, so the value is
    optional in Python (ORM default) and backfilled by the database for any
    INSERT that does not name it (server default).
    """
    return mapped_column(
        String(32),
        nullable=False,
        default=PROVIDER_ALTEGIO,
        server_default=_PROVIDER_SERVER_DEFAULT,
    )


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


class EasyWeekEvent(Base):
    """Сырая запись доставки вебхука EasyWeek.

    Research-grade: каждая аутентифицированная доставка — включая ретраи, Resend
    и не-JSON тела — становится отдельной строкой. Никакой обработки,
    нормализации и дедупликации. Повторы анализируются через НЕуникальный индекс
    по ``payload_hash``; unique-констрейнт отсутствует сознательно, чтобы ретраи
    сохранялись как данные.
    """

    __tablename__ = "easyweek_events"
    __table_args__ = (
        # PR-11. Exactly the recovery scan's predicate, so the partial index the
        # migration creates also exists in the schema tests build from the model.
        Index(
            "ix_easyweek_events_review_deferred",
            "review_deferred_at",
            postgresql_where=text("review_deferred_at IS NOT NULL"),
        ),
        # PR-12. Its own index for its own scan, matching its own predicate.
        Index(
            "ix_easyweek_events_retention_deferred",
            "retention_deferred_at",
            postgresql_where=text("retention_deferred_at IS NOT NULL"),
        ),
    )

    id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
    )

    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        index=True,
    )

    # Жизненный цикл: "captured" -> ... Задел под inbox-воркер PR-4, который
    # будет забирать строки со статусом "captured". Сейчас вебхук пишет только
    # значение по умолчанию. index + server_default повторяют миграцию
    # easyweek_events, чтобы схема, собранная из модели в тестах, совпадала с прод.
    status: Mapped[str] = mapped_column(
        String(32),
        default="captured",
        server_default=text("'captured'"),
        index=True,
    )

    # Значение query-параметра ?event= в том виде, как оно настроено в URL
    # вебхука. Валидации имён триггеров на этапе capture нет.
    event_hint: Mapped[str | None] = mapped_column(
        String(32),
        index=True,
        nullable=True,
    )

    # PR-11. Узкое durable continuation-state ТОЛЬКО для потребителей
    # `booking-succeeded`: момент, когда визит был доказан и счётчик записан, но
    # отзыв рассмотреть не удалось, потому что мастер-фенс уведомлений был
    # закрыт.
    #
    # Зачем отдельное поле, а не `captured`: visit counter должен быть записан и
    # закоммичен независимо от фенса уведомлений, а откат транзакции ради
    # удержания события уничтожил бы этот счётчик. Оставить строку нетерминальной
    # тоже нельзя — она стала бы predecessor и заблокировала бы последующие
    # lifecycle-события СВОЕЙ booking, чего PR-11 требует избежать.
    #
    # Поэтому строка становится `processed` (очередь свободна, hot loop
    # отсутствует), но несёт отметку «отзыв ещё должен быть рассмотрен».
    # `recover_deferred_reviews` снимает её, когда фенс открывается.
    # NULL — обязательства нет: либо отзыв уже рассмотрен, либо оператор их не
    # включал, либо это не `booking-succeeded`.
    #
    # Значение — это момент, **начиная с которого** обязательство можно
    # рассматривать, а не только момент его появления. Первая отметка ставится
    # «сейчас», то есть сразу eligible; если проход не смог принять решение
    # (невалидная карта ссылок, сломанный аллоулист, неожиданная транзиентная
    # ошибка), момент сдвигается вперёд. Выборка recovery это учитывает и
    # сортирует по нему, поэтому неразрешимая строка не занимает слот
    # ограниченного batch на каждом проходе и не морит голодом корректные
    # строки за собой. Отдельная колонка под retry не заводится: у обязательства
    # ровно одно расписание, и второе поле могло бы с ним разойтись.
    review_deferred_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    # PR-12. Тот же механизм для СВОЕГО обязательства — `repeat_10d`, — и
    # намеренно отдельная колонка, а не переиспользование review-отметки.
    #
    # Одна колонка на два обязательства выглядела экономией, но означала «эта
    # доставка что-то кому-то должна» без указания ЧТО. Восстановление тогда
    # вынуждено спрашивать текущие флаги, а не то, что было заработано в момент
    # события, и это даёт сразу два дефекта: выключенная в момент события
    # функция может получить сообщение задним числом, а действительно
    # заработанное — потеряться, когда чужой планировщик первым очистит общую
    # отметку.
    #
    # Тип обязательства обязан быть durable, потому что durable — это
    # единственное, что переживает изменение флагов между событием и
    # восстановлением. Каждая отметка ставится только своим включённым в тот
    # момент планировщиком, снимается или сдвигается только своим проходом
    # восстановления, и recoverable-ошибка одного обязательства не трогает
    # второе: у них разные транзакции.
    #
    # NULL — обязательства нет. Одно `booking-succeeded` может нести обе отметки
    # одновременно; это нормальное состояние, а не конфликт.
    retention_deferred_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    # Откуда пришёл валидный токен: "query" | "header".
    auth_via: Mapped[str | None] = mapped_column(String(16), nullable=True)

    # sha256 канонизированного JSON-payload (sort_keys, separators=(",", ":")).
    # NULL, когда тело не JSON. Индексируется, но НЕ уникален: повторные
    # доставки делят хэш и сохраняются обе.
    payload_hash: Mapped[str | None] = mapped_column(
        String(64),
        index=True,
        nullable=True,
    )

    content_type: Mapped[str | None] = mapped_column(String(128), nullable=True)

    # Источник истины по содержимому доставки: до 128 КиБ ИСХОДНЫХ байт, как их
    # прислал EasyWeek. Заполняется для каждой аутентифицированной доставки,
    # включая успешно разобранный JSON: JSONB — это уже разбор, он теряет
    # порядок ключей, пробелы, формат чисел, дубли ключей и невалидный UTF-8.
    # Nullable, чтобы ручная политика хранения могла обнулить байты, сохранив
    # метаданные строки.
    body_raw: Mapped[bytes | None] = mapped_column(LargeBinary, nullable=True)
    # Полный размер полученной доставки в байтах — включая ту часть, которая не
    # попала в body_raw из-за лимита.
    body_size_bytes: Mapped[int] = mapped_column(
        BigInteger,
        default=0,
        server_default=text("0"),
    )

    # Текстовая проекция тела для случаев, когда сохранить его как JSONB не
    # удалось (не JSON, NUL, суррогаты, слишком большое тело). Это удобство для
    # чтения: байтовый источник истины — body_raw. Ограничена теми же 128 КиБ.
    body_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    body_truncated: Mapped[bool] = mapped_column(
        Boolean,
        default=False,
        server_default=text("false"),
    )

    query: Mapped[dict] = mapped_column(JSONB, default=dict)  # секреты замаскированы
    headers: Mapped[dict] = mapped_column(JSONB, default=dict)  # только безопасные заголовки
    payload: Mapped[dict] = mapped_column(JSONB, default=dict)  # распарсенный JSON или {}

    # --- PR-4 causal ordering key --------------------------------------------
    # Канонический booking UUID доставки. ЕДИНСТВЕННЫЙ ключ, по которому claim
    # сериализует события одной записи.
    #
    # Почему не `payload ->> 'uid'`: это сырой текст, а нормализатор приводит его
    # к `uuid.UUID(raw.strip())`. Одна и та же UUID в lowercase, uppercase, в
    # фигурных скобках, без дефисов или с пробелами — это РАЗНЫЕ строки, но ОДНА
    # booking. По сырому тексту claim не увидел бы раннюю доставку как
    # предшественника поздней, и поздняя обогнала бы раннюю: после retry ранняя
    # легла бы сверху и откатила время, service snapshot, стоимость или client
    # link. Здесь identity уже является UUID, поэтому и ключ — UUID.
    #
    # NULL, когда `uid` отсутствует или синтаксически невалиден. Такая строка
    # никого не блокирует и сама не блокируется — она доходит до claim и
    # получает детерминированный отказ. Сырой захват (body_raw/payload/
    # payload_hash) при этом не меняется.
    booking_uuid: Mapped[uuid.UUID | None] = mapped_column(
        PostgresUUID(as_uuid=True),
        nullable=True,
    )

    # --- PR-4 runtime auditability -------------------------------------------
    # Когда строка достигла терминального статуса (processed/failed). NULL, пока
    # событие ещё captured.
    processed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    # Стабильный безопасный код причины отказа — НЕ str(exception).
    # Текст исключения драйвера/БД может содержать SQL-параметры, а значит и
    # телефон, e-mail и имя клиента; такой текст в БД был бы утечкой PII.
    # Допустимые значения перечислены в easyweek_normalizer.NormalizationError.
    error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)

    # --- per-event retry scheduling ------------------------------------------
    # Без них одна «отравленная» строка навсегда блокирует backlog: claim берёт
    # старейшую captured строку, поэтому после отката воркер выбирал бы ту же
    # самую снова и снова. next_retry_at выводит её из выборки на время, и
    # остальные готовые события обрабатываются.
    processing_attempts: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default=text("0"),
    )
    next_retry_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )


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
    Клиент в контексте филиала (provider, company_id).
    Уникальность: (provider, company_id, altegio_client_id).
    """

    __tablename__ = "clients"
    __table_args__ = (
        UniqueConstraint(
            "provider",
            "company_id",
            "altegio_client_id",
            name="uq_clients_provider_company_altegio_id",
        ),
        Index("ix_clients_provider_company_phone", "provider", "company_id", "phone_e164"),
        Index("ix_clients_wa_opted_out_at", "wa_opted_out", "wa_opted_out_at"),
        # PR-11. The EasyWeek visit counter is provider-scoped by construction,
        # not by convention: an Altegio row must never carry one. Altegio's own
        # count is answered live by `count_attended_client_visits`, so a stored
        # number there would be a second, silently diverging source of truth.
        CheckConstraint(
            "easyweek_visits_total IS NULL OR provider = 'easyweek'",
            name="ck_clients_easyweek_visits_total_provider",
        ),
        # `booking-succeeded` proves a finished visit, so the snapshot it
        # carries is at least 1. Zero or negative is a malformed delivery, and
        # the database refuses it even if a future caller forgets to.
        CheckConstraint(
            "easyweek_visits_total IS NULL OR easyweek_visits_total >= 1",
            name="ck_clients_easyweek_visits_total_positive",
        ),
        # The value and the moment it was accepted are one fact. A timestamp
        # without a count says a visit was recorded and lost the number; a count
        # without a timestamp cannot be audited against the delivery that set it.
        CheckConstraint(
            "(easyweek_visits_total IS NULL) = (easyweek_visits_total_updated_at IS NULL)",
            name="ck_clients_easyweek_visits_total_paired",
        ),
    )

    id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
    )

    provider: Mapped[str] = _provider_column()
    company_id: Mapped[int] = mapped_column(Integer, index=True)
    # Historically named for Altegio; conceptually the external client id of
    # whichever provider owns the row (EasyWeek supplies ``:customer_id``).
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

    # PR-11: the last PROVEN `visits_total` snapshot from a `booking-succeeded`
    # delivery, and the moment that value was accepted.
    #
    # A snapshot, never a tally. EasyWeek states the total; we never compute
    # `current + 1` from the fact that a webhook arrived, because a Resend, a
    # replay with a different payload hash and a genuine second visit are
    # indistinguishable at that level. Storing the number EasyWeek states makes
    # all three converge on the same value instead of drifting upwards.
    #
    # NULL on every Altegio row, enforced by a CHECK above.
    easyweek_visits_total: Mapped[int | None] = mapped_column(Integer, nullable=True)
    easyweek_visits_total_updated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    records: Mapped[list["Record"]] = relationship(back_populates="client")


class Record(Base):
    """
    Термин/запись в контексте филиала.
    Уникальность: (provider, company_id, altegio_record_id).
    """

    __tablename__ = "records"
    __table_args__ = (
        UniqueConstraint(
            "provider",
            "company_id",
            "altegio_record_id",
            name="uq_records_provider_company_altegio_id",
        ),
        # EasyWeek's booking UUID is the authoritative identifier (§1.6.2), so it
        # must be unique — but ONLY for EasyWeek rows. A partial index keeps the
        # constraint off every Altegio row (all NULL there) and, because NULLs
        # are distinct in a unique index, still allows many EasyWeek rows whose
        # UUID has not been captured yet.
        Index(
            "uq_records_easyweek_booking_uuid",
            "easyweek_booking_uuid",
            unique=True,
            postgresql_where=text("provider = 'easyweek' AND easyweek_booking_uuid IS NOT NULL"),
        ),
    )

    id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
    )

    provider: Mapped[str] = _provider_column()
    company_id: Mapped[int] = mapped_column(Integer, index=True)
    # Historically named for Altegio; conceptually the external record id
    # (EasyWeek supplies the numeric ``:id``).
    altegio_record_id: Mapped[int] = mapped_column(BigInteger, index=True)

    # EasyWeek identity (§1.6.2–3). Both stay NULL for Altegio rows.
    #
    # The UUID is the key for ``GET /bookings/{uuid}``. ``booking_hash_id`` is a
    # bounded STRING, not a BigInteger: the value is the number from the manage
    # link, and treating it as an integer would assume a purely numeric format
    # and silently destroy leading zeros. Its only sanctioned use is proving the
    # provenance of ``short_link`` — a link is never synthesised from it.
    easyweek_booking_uuid: Mapped[uuid.UUID | None] = mapped_column(
        PostgresUUID(as_uuid=True),
        nullable=True,
    )
    easyweek_booking_hash_id: Mapped[str | None] = mapped_column(String(64), nullable=True)

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

    __table_args__ = (
        # The lookup PR-5 will perform: provider first, so an EasyWeek template
        # can never be reached through an Altegio-shaped query.
        Index(
            "ix_message_templates_provider_company_code_lang",
            "provider",
            "company_id",
            "code",
            "language",
        ),
    )

    id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
    )
    provider: Mapped[str] = _provider_column()
    company_id: Mapped[int] = mapped_column(Integer, index=True)

    # "record_created", "reminder_24h", ...
    code: Mapped[str] = mapped_column(String(64), index=True)

    # "de"
    language: Mapped[str] = mapped_column(String(8), default="de")

    # Текст шаблона с плейсхолдерами {client_name}, {date}, ...
    body: Mapped[str] = mapped_column(Text)

    # Имя утверждённого Meta-шаблона для этой строки. DB-first замена
    # глобального хардкода META_TEMPLATE_MAP (§1.6.9): его нельзя расширять
    # numeric company_id из другой CRM. Резолвом займётся PR-5; здесь только
    # колонка.
    meta_template_name: Mapped[str | None] = mapped_column(String(128), nullable=True)

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

    __table_args__ = (
        Index("ix_message_jobs_status_run_at", "status", "run_at"),
        Index(
            "ix_message_jobs_provider_company_type_status",
            "provider",
            "company_id",
            "job_type",
            "status",
        ),
        Index("ix_message_jobs_status_locked_at", "status", "locked_at"),
    )

    id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
    )

    provider: Mapped[str] = _provider_column()
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

    __table_args__ = (
        Index("ix_outbox_messages_sent_at", "sent_at"),
        Index("ix_outbox_messages_company_sent_at", "company_id", "sent_at"),
        Index("ix_outbox_messages_created_at", "created_at"),
        Index(
            "ix_outbox_messages_company_created_at",
            "company_id",
            "created_at",
        ),
        # DB-level idempotency for operator relay: at most one Outbox intent per
        # source WhatsAppEvent. Partial so the many historical/bot rows that have
        # no source event (NULL) are unconstrained and can coexist freely.
        Index(
            "uq_outbox_source_whatsapp_event_id",
            "source_whatsapp_event_id",
            unique=True,
            postgresql_where=text("source_whatsapp_event_id IS NOT NULL"),
        ),
    )

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

    # Lifecycle: queued → sending → sent/delivered/read | failed | canceled |
    # unknown. For operator relay the transition queued → sending is committed
    # (with attempt_started_at) BEFORE the first Meta side effect, so a durable
    # send intent always exists on disk before the network call. 'unknown' marks
    # an attempt whose Meta outcome cannot be proven (crash/indeterminate) — it
    # is never auto-retried and requires manual review.
    status: Mapped[str] = mapped_column(String(32), default="queued", index=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Wall-clock time the queued → sending claim was committed. Distinct from
    # created_at (a row may sit in 'queued' first) and from sent_at (only set on
    # confirmed success). Stale-'sending' recovery keys off this timestamp.
    attempt_started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

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

    # 'bot' = sent by automation/campaign worker
    # 'operator' = relayed from Chatwoot human operator via altegio_bot → Meta
    message_source: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        server_default=text("'bot'"),
    )

    # Chatwoot ids of the operator message this row relays (also kept in meta
    # for backward compatibility).  Indexed so an inbound WhatsApp reply
    # (messages[0].context.id == provider_message_id) can resolve its native
    # Chatwoot reply target.
    chatwoot_conversation_id: Mapped[int | None] = mapped_column(
        BigInteger,
        nullable=True,
        index=True,
    )
    chatwoot_message_id: Mapped[int | None] = mapped_column(
        BigInteger,
        nullable=True,
        index=True,
    )

    # Operator-relay idempotency anchor: the WhatsAppEvent this row relays.
    # NULL for every historical row and every non-operator (bot/campaign) send.
    # Populated for operator relay and covered by the partial unique index above,
    # so one event can never spawn two send attempts even under concurrency or
    # crash-replay. SET NULL on event deletion keeps the audit row alive.
    source_whatsapp_event_id: Mapped[int | None] = mapped_column(
        BigInteger,
        ForeignKey("whatsapp_events.id", ondelete="SET NULL"),
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


class MetaCircuitBreaker(Base):
    """Global Meta send circuit state, one row per scope.

    Shared by outbox workers and the meta guard worker through Postgres.
    State names are intentionally inverted compared with the classic breaker:
    open means sends are allowed, closed means sends are paused, and half_open
    means a recovery probe is in progress.
    """

    __tablename__ = "meta_circuit_breaker"

    scope: Mapped[str] = mapped_column(String(64), primary_key=True)
    state: Mapped[str] = mapped_column(
        String(16),
        nullable=False,
        default="open",
        server_default=text("'open'"),
    )
    reason: Mapped[str | None] = mapped_column(String(256), nullable=True)
    opened_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )
    next_probe_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    probe_token: Mapped[str | None] = mapped_column(String(64), nullable=True)
    probe_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    probe_lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    probe_attempts: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default=text("0"),
    )
    last_error_kind: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)


class WhatsAppSender(Base):
    __tablename__ = "whatsapp_senders"

    id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
    )
    provider: Mapped[str] = _provider_column()
    company_id: Mapped[int] = mapped_column(Integer, index=True)
    sender_code: Mapped[str] = mapped_column(String(32), index=True)

    phone_number_id: Mapped[str] = mapped_column(String(64))
    display_phone: Mapped[str | None] = mapped_column(String(32), nullable=True)

    is_active: Mapped[bool] = mapped_column(Boolean, default=True)

    __table_args__ = (
        UniqueConstraint(
            "provider",
            "company_id",
            "sender_code",
            name="uq_whatsapp_senders_provider_company_code",
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

    # SOURCE Chatwoot conversation that originated this event (set only when
    # the webhook comes from Chatwoot instead of Meta directly).  Never holds
    # the destination conversation of a forwarded Meta-origin message — that
    # lives in forwarded_chatwoot_conversation_id below.
    chatwoot_conversation_id: Mapped[int | None] = mapped_column(
        BigInteger,
        nullable=True,
        index=True,
    )

    # Chatwoot Message.id linked to this event:
    # - Chatwoot-origin events: id of the source Chatwoot message;
    # - Meta-origin events: id of the message created in Chatwoot when the
    #   inbound text was forwarded there.
    chatwoot_message_id: Mapped[int | None] = mapped_column(
        BigInteger,
        nullable=True,
        index=True,
    )

    # DESTINATION Chatwoot conversation a Meta-origin inbound message was
    # forwarded to.  Kept separate from chatwoot_conversation_id (source
    # marker) so forwarding never flips a Meta event into "chatwoot-origin".
    forwarded_chatwoot_conversation_id: Mapped[int | None] = mapped_column(
        BigInteger,
        nullable=True,
        index=True,
    )

    # Meta wamid of the inbound message (payload messages[0].id).  Audit and
    # future native-reply mapping; NOT the Chatwoot message id and NOT a
    # replacement for dedupe_key.
    whatsapp_message_id: Mapped[str | None] = mapped_column(
        String(128),
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
    # Клиент уже вернулся в салон после окончания периода кампании → рассылка не нужна
    excluded_returned_after_visit: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))

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
    status: Mapped[str] = mapped_column(String(32), default="candidate")

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
    # Не удалённые записи ПОСЛЕ периода кампании по данным Altegio CRM.
    # Если > 0 → клиент уже вернулся сам, рассылка не нужна → excluded_reason='returned_after_first_visit'.
    records_after_period: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
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


# ---------------------------------------------------------------------------
# Promo / secret-word funnel
# ---------------------------------------------------------------------------

#: Full set of allowed PromoLead statuses.
#: All values are defined here so future PRs can advance the lifecycle
#: without schema changes.
PROMO_LEAD_STATUSES: frozenset[str] = frozenset(
    {
        "issued",  # discount linked, awaiting booking
        "pending_check",  # eligibility check queued, no discount promised yet
        "booked",  # client booked after receiving discount
        "applied",  # discount reserved against the booking
        "used",  # discount applied to the completed visit
        "expired",  # validity period elapsed without booking
        "cancelled",  # manually cancelled
        "rejected_not_new",  # client already has prior visits
        "rejected_service_not_allowed",  # service not eligible
        "apply_failed",  # discount application to visit failed
    }
)


_PROMO_STATUS_CHECK = "status IN ({})".format(
    ", ".join(
        f"'{s}'"
        for s in (
            "issued",
            "pending_check",
            "booked",
            "applied",
            "used",
            "expired",
            "cancelled",
            "rejected_not_new",
            "rejected_service_not_allowed",
            "apply_failed",
        )
    )
)


class PromoLead(Base):
    """WhatsApp secret-word promo lead.

    Created when a client sends a known secret word (e.g. 'aktion') via
    WhatsApp.  Tracks the lifecycle of a personal discount offer from first
    contact through eligibility checks, booking, and discount application.
    """

    __tablename__ = "promo_leads"
    __table_args__ = (
        # One active lead per phone + campaign enforced at DB level.
        UniqueConstraint("phone_e164", "campaign_name", name="uq_promo_leads_phone_campaign"),
        CheckConstraint(_PROMO_STATUS_CHECK, name="ck_promo_leads_status"),
        CheckConstraint("discount_type IN ('fixed', 'percent')", name="ck_promo_leads_discount_type"),
        Index("ix_promo_leads_status_expires", "status", "expires_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    # Company / location context (filled from WhatsAppSender at creation time)
    company_id: Mapped[int] = mapped_column(Integer, index=True)
    location_id: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # Client identification
    phone_e164: Mapped[str] = mapped_column(String(32), index=True)
    altegio_client_id: Mapped[int | None] = mapped_column(BigInteger, index=True, nullable=True)

    # Campaign / offer
    campaign_name: Mapped[str] = mapped_column(String(128), index=True)
    secret_code: Mapped[str] = mapped_column(String(64))

    discount_amount: Mapped[Decimal] = mapped_column(Numeric(10, 2))
    # 'fixed' (Euro) | 'percent'
    discount_type: Mapped[str] = mapped_column(String(32))

    # See PROMO_LEAD_STATUSES for the full lifecycle.
    status: Mapped[str] = mapped_column(
        String(64),
        index=True,
        server_default=text("'issued'"),
    )
    reject_reason: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Core timestamps
    issued_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)

    # Attribution timestamps (filled as lifecycle advances in future PRs)
    booked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    applied_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    used_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    cancelled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # Altegio loyalty integration (future PRs — not set in this PR)
    loyalty_card_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    loyalty_card_number: Mapped[str | None] = mapped_column(String(64), nullable=True)
    card_type_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    discount_program_id: Mapped[str | None] = mapped_column(String(64), nullable=True)

    # Altegio booking attribution (future PRs)
    # record_id = local FK; altegio_record_id = external Altegio identifier
    record_id: Mapped[int | None] = mapped_column(
        BigInteger,
        ForeignKey("records.id", ondelete="SET NULL"),
        index=True,
        nullable=True,
    )
    altegio_record_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    visit_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        index=True,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )

    meta: Mapped[dict] = mapped_column(JSONB, default=dict)


class EasyWeekMigrationServiceBaseline(Base):
    """The service a wave was reviewed against, kept so a later run cannot drift.

    The migration cannot read a booking's catalogue service back: EasyWeek
    returns an order-line uuid, not a catalogue one. Plan §28 authorises proving
    the service by its exact attributes instead, and that only means anything if
    the attributes being compared are the ones an operator actually reviewed.

    Without this table they were not. Each run re-derived the expectation from
    whatever the catalogue said at that moment, so renaming a service between the
    canary and the bulk silently produced a new "expectation" that the new
    catalogue trivially satisfied — the check compared the catalogue with itself
    and the old canary went on licensing the wave.

    So the expectation is written down, once, in the same transaction as the
    ledger claim that precedes the very first booking for that service. After
    that it is only ever **verified**: a later catalogue read can agree with it or
    fail closed, never overwrite it. That is what makes the chain
    ``reviewed dry-run → canary → apply → reconcile/rollback`` mean one thing
    rather than four.

    Deliberately NOT a catalogue history. One row per
    ``(location, service)`` — the current agreed truth — with no versions, no
    audit trail and no snapshot of the manifest. Changing it is an explicit
    operator act, not something a run does on its way past.
    """

    __tablename__ = "easyweek_migration_service_baseline"

    __table_args__ = (
        # One agreed truth per service per location. The unique constraint is the
        # mechanism: an INSERT that loses the race reads the winner back rather
        # than establishing a second, different expectation.
        UniqueConstraint(
            "easyweek_location_uuid",
            "easyweek_service_uuid",
            name="uq_easyweek_service_baseline_identity",
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    easyweek_location_uuid: Mapped[str] = mapped_column(String(64), nullable=False)
    easyweek_service_uuid: Mapped[str] = mapped_column(String(64), nullable=False)

    # The attributes identity is argued from. `canonical_name` is stored in the
    # normalised form the comparison uses (NFC, collapsed whitespace, casefold)
    # so a run cannot disagree with a stored baseline over an encoding.
    canonical_name: Mapped[str] = mapped_column(String(255), nullable=False)
    currency: Mapped[str] = mapped_column(String(8), nullable=False)
    price_minor: Mapped[int] = mapped_column(Integer, nullable=False)
    duration_minutes: Mapped[int] = mapped_column(Integer, nullable=False)

    # How it was proven. A baseline written under one method or version must not
    # be read as evidence for another; the reader refuses instead of adapting.
    proof_method: Mapped[str] = mapped_column(String(64), nullable=False)
    proof_version: Mapped[str] = mapped_column(String(16), nullable=False)

    # Which wave and run established it. Reported, never used to decide — the
    # decision is the attribute comparison — but an operator asking "where did
    # this expectation come from?" needs an answer that is not "some run".
    wave_identity: Mapped[str | None] = mapped_column(String(64), nullable=True)
    established_run_id: Mapped[str] = mapped_column(String(64), nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )


class EasyWeekMigrationLedger(Base):
    """One durable row per SOURCE booking a cutover migration has looked at (PR-11.1).

    The migration writes bookings into a live CRM. Its whole safety story rests
    on being able to answer, offline and forever, *"has this Altegio record
    already been created in EasyWeek?"* — so the answer lives in PostgreSQL under
    a unique constraint, not in a report file, not in a marker on the remote
    booking, and not in an in-memory set that dies with the process.

    Identity is **source-scoped**, not target-scoped: the natural key is
    ``(source_provider, source_company_id, source_record_id)``. A target UUID is
    the *result* of a migration attempt and is unknown for every row that has not
    succeeded yet — keying on it would leave exactly the uncertain rows unkeyed.

    ``status`` is a small closed vocabulary owned by
    :mod:`altegio_bot.easyweek_migration.ledger`; it is a plain string here for
    the same reason ``provider`` is (see above): a new outcome must be a code
    change, not a migration against a restrictive type.

    Nothing on this row is PII. Phones, names and payloads are deliberately
    absent: the ledger is read by operators, printed into reports and kept long
    after the cutover, and a customer identifier stored here would outlive every
    reason to have it. ``source_fingerprint`` is a digest of the source booking's
    *schedule identity*, which is what proves a row was migrated as planned and
    what detects a source that changed under us.
    """

    __tablename__ = "easyweek_migration_ledger"
    __table_args__ = (
        # The idempotency guarantee, enforced by the database rather than by the
        # tool: a second `apply` (a rerun, a parallel operator, a crashed run
        # picked up again) cannot insert a second row for the same source
        # booking, so it cannot create a second EasyWeek booking for it either.
        UniqueConstraint(
            "source_provider",
            "source_company_id",
            "source_record_id",
            name="uq_easyweek_migration_ledger_source_identity",
        ),
        Index("ix_easyweek_migration_ledger_run", "run_id"),
        Index("ix_easyweek_migration_ledger_status", "status"),
        # A created row must name what it created; a row that created nothing
        # must not claim a target. Without this, an interrupted apply could leave
        # `created` with a NULL target and reconciliation would report a booking
        # nobody can find or roll back.
        CheckConstraint(
            "(status <> 'created') OR (target_booking_uuid IS NOT NULL)",
            name="ck_easyweek_migration_ledger_created_has_target",
        ),
        CheckConstraint(
            "attempts >= 0",
            name="ck_easyweek_migration_ledger_attempts_non_negative",
        ),
        # A reminder handover marker is one fact in two columns: the instant it
        # happened and the plan that authorised it. Half a marker would be a row
        # that claims ownership moved without saying under what authority, or a
        # digest with no handover behind it — and the runtime fence reads the
        # instant while the apply compares the digest, so either half alone
        # would let one of them answer while the other could not.
        CheckConstraint(
            "(reminders_handed_over_at IS NULL) = (reminder_handover_plan_digest IS NULL)",
            name="ck_easyweek_migration_ledger_reminder_handover_complete",
        ),
        # The fence runs inside the Altegio planning transaction for every
        # create/update delivery, so it has to be an index-only lookup. Partial
        # on purpose: only handed-over rows are ever asked about, and the index
        # stays a fraction of the table.
        Index(
            "ix_easyweek_migration_ledger_reminder_handover",
            "source_provider",
            "source_company_id",
            "source_record_id",
            postgresql_where=text("reminders_handed_over_at IS NOT NULL"),
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    # -- source identity (Altegio) ----------------------------------------
    source_provider: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        server_default=_PROVIDER_SERVER_DEFAULT,
    )
    source_company_id: Mapped[int] = mapped_column(Integer, nullable=False)
    source_record_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    # Digest over the source booking's schedule identity (start, staff, service,
    # duration, customer key). Not a secret and not reversible to PII; it exists
    # so a later run can say "this source booking is no longer what we migrated".
    source_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)

    # -- target identity (EasyWeek) ---------------------------------------
    target_provider: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        server_default=text(f"'{PROVIDER_EASYWEEK}'"),
    )
    # NULL until a mutation is PROVEN to have created a booking. An uncertain
    # POST leaves this NULL on purpose: claiming a UUID we never read back would
    # be worse than admitting we do not know.
    target_booking_uuid: Mapped[str | None] = mapped_column(String(64), nullable=True)

    # PII-free digest of the LIVE target booking as it was written: location,
    # staff, service, customer uuid, start, duration, marker and active status.
    # Rollback compares a freshly fetched booking against this before cancelling
    # anything — the marker alone cannot tell a booking that was moved to another
    # day, master or customer from one nobody has touched.
    target_snapshot_fingerprint: Mapped[str | None] = mapped_column(String(64), nullable=True)

    # -- run bookkeeping ---------------------------------------------------
    # The run that first claimed this source booking. It never changes, because
    # it is what a rollback of THAT apply selects on: if a later reconciliation
    # overwrote it, the booking would silently drop out of its own run's rollback
    # set and become unrollbackable.
    run_id: Mapped[str] = mapped_column(String(64), nullable=False)
    # The run that last MOVED this row's status — a reconciliation, a resolution,
    # a rollback. Separate from `run_id` so bookkeeping about the row cannot
    # rewrite the row's origin.
    last_resolution_run_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    # The index lives in ``__table_args__`` above, next to the others; declaring
    # ``index=True`` here as well would emit the same CREATE INDEX twice.
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"), default=0)
    # Stable technical code (`mapping_missing`, `customer_ambiguous`, …). Never a
    # provider message, never a payload excerpt, never a phone number.
    reason_code: Mapped[str | None] = mapped_column(String(64), nullable=True)

    # -- reminder ownership (PR-11.2, plan §30.11) -------------------------
    # When the future reminders for this booking stopped being Altegio's and
    # became EasyWeek's, and under which reviewed plan.
    #
    # Why this cannot be inferred from anything already on the row. `status`
    # becomes `created` when the BOOKING was migrated, which happens long before
    # — and independently of — the reminder handover; a cancelled `MessageJob`
    # is not durable evidence either, because the Altegio planner's `add_job`
    # resurrects a cancelled job on the next delivery of the same fact. So a
    # late Altegio webhook had nothing to consult and would re-open a reminder
    # the handover had just withdrawn, leaving one appointment with open
    # reminders on both sides.
    #
    # Written atomically with the cancellation it describes, so a wave that
    # rolls back leaves no marker, and a marker always means the withdrawal
    # committed. Both columns move together — see the CHECK above.
    reminders_handed_over_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    # The handover plan digest an operator reviewed and authorised. Kept so a
    # repeat of the SAME snapshot is recognised as idempotent, and a different
    # one is refused rather than silently re-marking the row.
    reminder_handover_plan_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )


class EasyWeekMigrationCanaryProof(Base):
    """Durable evidence that ONE real booking was created and read back correctly.

    The canary exists because ``POST /bookings`` is a confirmed endpoint with an
    unconfirmed body schema (plan §1.1, §21.4). Before hundreds of real customers
    get appointments, exactly one must be created and then *re-read from EasyWeek*
    and compared field by field against what was sent.

    The first version treated ``--limit 1`` as that evidence. It was not: a limit
    proves only that one POST returned 2xx, it says nothing about whether the
    booking landed at the right branch, with the right master, at the right time,
    and it picked whichever row the Altegio API happened to return first — a
    different, arbitrary customer on every run.

    So the proof is a row, and a bulk apply requires one that still applies. The
    binding fields are what "still applies" means: change the manifest, change
    the branch mapping, change the request schema or the cutover, and the stored
    proof no longer matches the run being attempted — because it is no longer
    evidence about that run.

    PII-free by construction: source ids, UUIDs, digests and a timestamp.
    """

    __tablename__ = "easyweek_migration_canary_proof"
    __table_args__ = (
        # One proof per (manifest, schema, cutover, source booking). A re-run of
        # the same canary updates its row rather than accumulating history that
        # would make "which proof is current?" ambiguous.
        UniqueConstraint(
            "manifest_digest",
            "request_schema_version",
            "contract_kind",
            "cutover_at",
            "source_company_id",
            "source_record_id",
            name="uq_easyweek_migration_canary_identity",
        ),
        Index(
            "ix_easyweek_migration_canary_lookup",
            "manifest_digest",
            "request_schema_version",
            "contract_kind",
        ),
        # A proof row that did not verify is not a proof. It is still stored —
        # a failed canary is exactly what an operator needs to read — but the
        # bulk gate selects on `verified`, and a NULL target on a verified row
        # would be a proof of nothing.
        CheckConstraint(
            "(verified IS NOT TRUE) OR (target_booking_uuid IS NOT NULL)",
            name="ck_easyweek_migration_canary_verified_has_target",
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    # Which mutation contract this canary exercised: `single` for
    # ``POST /bookings``, `cart_two` for ``POST /bookings/cart`` (plan §30.12).
    #
    # Part of the identity, not a label. The two contracts are different
    # endpoints with different request bodies and different readback shapes, so
    # a canary that proved one has proven nothing about the other — and a bulk
    # run licensed by the wrong kind would write hundreds of bookings through a
    # path no real booking has ever gone down. Existing rows default to `single`
    # because that is the only contract that existed when they were written.
    contract_kind: Mapped[str] = mapped_column(
        String(16),
        nullable=False,
        server_default=text("'single'"),
        default="single",
    )

    # -- what was proven --------------------------------------------------
    source_company_id: Mapped[int] = mapped_column(Integer, nullable=False)
    source_record_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    source_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    target_booking_uuid: Mapped[str | None] = mapped_column(String(64), nullable=True)
    # Digest of the live booking as read back from EasyWeek after the write.
    target_snapshot_fingerprint: Mapped[str | None] = mapped_column(String(64), nullable=True)

    # -- what the proof is BOUND to ---------------------------------------
    manifest_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    # The wave selector on its own. It is already inside `manifest_digest`, but
    # keeping it separately is what lets a mismatch say "somebody moved a master
    # between waves" instead of the useless "the manifest changed" — and moving a
    # master is the one change that would hide her bookings from the very check
    # that is supposed to prove they landed.
    staff_scope_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)
    # How far ahead the wave looked. A narrower horizon at reconciliation time
    # would silently drop the far end of the wave out of the proof.
    horizon_days: Mapped[int | None] = mapped_column(Integer, nullable=True)
    # Bumped whenever the POST body changes shape. An old proof cannot vouch for
    # a request we no longer send.
    request_schema_version: Mapped[str] = mapped_column(String(16), nullable=False)
    cutover_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    branch_identity_digest: Mapped[str] = mapped_column(String(64), nullable=False)

    # -- the verdict -------------------------------------------------------
    verified: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"), default=False)
    # Stable code naming the first field that did not match, when it failed.
    failure_reason: Mapped[str | None] = mapped_column(String(128), nullable=True)
    run_id: Mapped[str] = mapped_column(String(64), nullable=False)
    verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )
