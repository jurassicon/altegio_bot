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
    ForeignKeyConstraint,
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
    Numeric identity and EasyWeek UUID are unique per provider/branch. The same
    workspace customer can have one distinct card in each supported branch.
    """

    __tablename__ = "clients"
    __table_args__ = (
        UniqueConstraint(
            "provider",
            "company_id",
            "altegio_client_id",
            name="uq_clients_provider_company_altegio_id",
        ),
        UniqueConstraint(
            "provider", "company_id", "easyweek_customer_uuid", name="uq_clients_provider_company_easyweek_uuid"
        ),
        CheckConstraint(
            "easyweek_customer_uuid IS NULL OR provider = 'easyweek'",
            name="ck_clients_easyweek_uuid_provider",
        ),
        CheckConstraint(
            "altegio_client_id IS NOT NULL OR (provider = 'easyweek' AND easyweek_customer_uuid IS NOT NULL)",
            name="ck_clients_external_identity",
        ),
        CheckConstraint(
            "easyweek_identity_assigned_at IS NULL OR (provider = 'easyweek' AND easyweek_customer_uuid IS NOT NULL)",
            name="ck_clients_easyweek_assignment",
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
    altegio_client_id: Mapped[int | None] = mapped_column(BigInteger, index=True, nullable=True)
    # A customer may be known by the proven public UUID before any booking
    # provides its numeric id. Never manufacture an id to populate the old field.
    easyweek_customer_uuid: Mapped[uuid.UUID | None] = mapped_column(PostgresUUID(as_uuid=True), nullable=True)
    easyweek_identity_assigned_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

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

    # §46.4 / PR-22.1 — was Meta's HMAC CHECKED for this delivery, over these
    # very bytes? Recorded by the ingress because only the ingress can know: the
    # signature header is deliberately stripped from `headers` above, so after
    # that point the proof is gone and no later process may reconstruct one from
    # a sanitised copy.
    #
    # Needed on the EVENT, not only on the per-message receipts, because a
    # delivery may carry no messages at all — a `statuses` payload has no
    # receipt to hang this on, and a `failed` callback still has to be recorded
    # with an honest statement of whether its delivery was authenticated.
    #
    # NULL on every row written before this phase, and NULL means unverified:
    # the absence of a recorded check is not a check that passed.
    inbound_authenticity: Mapped[str | None] = mapped_column(
        String(24),
        nullable=True,
    )


# ---------------------------------------------------------------------------
# Campaign models
# ---------------------------------------------------------------------------


class CampaignRun(Base):
    """Один запуск кампании: preview или send-real."""

    __tablename__ = "campaign_runs"
    __table_args__ = (
        UniqueConstraint("id", "provider", name="uq_campaign_runs_id_provider"),
        Index(
            "ix_campaign_runs_provider_campaign_created",
            "provider",
            "campaign_code",
            "created_at",
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    provider: Mapped[str] = _provider_column()

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


# On what grounds a campaign recipient is in a snapshot (§37.1). Three answers,
# and the difference between them is the difference between a proof, an approved
# stand-in and a human decision:
#
#   earned_first_visit        the segmenter proved a first visit (§33)
#   owner_test_account        the one pre-configured canary account (§36.11)
#   operator_manual_selection an operator typed a phone number (§37.1)
#
# The last one is NOT evidence of a first visit and must never be reported as
# one. It exists because the transitional August list has no EasyWeek data to be
# proven from — EasyWeek only started on 1 September — so a person decides, and
# the row says that a person decided.
RECIPIENT_BASIS_EARNED = "earned_first_visit"
RECIPIENT_BASIS_TEST = "owner_test_account"
RECIPIENT_BASIS_MANUAL = "operator_manual_selection"
RECIPIENT_BASES = (RECIPIENT_BASIS_EARNED, RECIPIENT_BASIS_TEST, RECIPIENT_BASIS_MANUAL)
_RECIPIENT_BASIS_SQL = ", ".join(f"'{value}'" for value in RECIPIENT_BASES)


class CampaignRecipient(Base):
    """
    Снимок сегментации и результат рассылки для одного клиента.
    Создаётся как для preview, так и для send-real.
    """

    __tablename__ = "campaign_recipients"
    __table_args__ = (
        ForeignKeyConstraint(
            ["campaign_run_id", "provider"],
            ["campaign_runs.id", "campaign_runs.provider"],
            name="fk_campaign_recipients_run_provider",
            ondelete="CASCADE",
        ),
        # Redundant on its own — `id` is already the primary key — and load
        # bearing anyway: it is what lets another table reference a recipient
        # TOGETHER WITH its provider, the same way runs are referenced. Without
        # it a composite foreign key cannot exist, and a referencing row could
        # name an EasyWeek recipient while claiming to be Altegio.
        UniqueConstraint("id", "provider", name="uq_campaign_recipients_id_provider"),
        Index(
            "ix_campaign_recipients_provider_run",
            "provider",
            "campaign_run_id",
        ),
        Index(
            "ix_campaign_recipients_provider_company_client",
            "provider",
            "company_id",
            "client_id",
        ),
        CheckConstraint(
            "((source_easyweek_event_id IS NULL) = (source_record_id IS NULL)) "
            "AND ((source_easyweek_event_id IS NULL) = (source_booking_uuid IS NULL)) "
            "AND ((source_easyweek_event_id IS NULL) = (source_visits_total IS NULL)) "
            "AND ((source_easyweek_event_id IS NULL) = (source_visits_total_updated_at IS NULL))",
            name="ck_campaign_recipients_easyweek_source_proof_complete",
        ),
        CheckConstraint(
            "source_easyweek_event_id IS NULL OR provider = 'easyweek'",
            name="ck_campaign_recipients_easyweek_source_proof_provider",
        ),
        CheckConstraint(
            "source_visits_total IS NULL OR source_visits_total = 1",
            name="ck_campaign_recipients_easyweek_source_visits_first",
        ),
        # §36.11. The owner-approved test identity, typed rather than hidden in
        # `meta`: an operator reading this row, and every constraint below, can
        # tell what basis it is on without parsing JSON.
        CheckConstraint(
            "easyweek_test_customer_uuid IS NULL OR provider = 'easyweek'",
            name="ck_campaign_recipients_test_customer_provider",
        ),
        # A test row must not be able to look like earned proof. Not "does not
        # happen to have one" — cannot have one: the source proof columns are
        # forbidden here, so no later code path can fill them in and turn a test
        # identity into a first visit nobody made.
        CheckConstraint(
            "easyweek_test_customer_uuid IS NULL OR ("
            "source_easyweek_event_id IS NULL "
            "AND source_record_id IS NULL "
            "AND source_booking_uuid IS NULL "
            "AND source_visits_total IS NULL "
            "AND source_visits_total_updated_at IS NULL)",
            name="ck_campaign_recipients_test_customer_has_no_source_proof",
        ),
        # One test recipient per customer per preview. Partial, because the rule
        # is about test rows only and the ordinary snapshot is untouched by it.
        Index(
            "uq_campaign_recipients_test_customer_per_run",
            "provider",
            "campaign_run_id",
            "easyweek_test_customer_uuid",
            unique=True,
            postgresql_where=text("easyweek_test_customer_uuid IS NOT NULL"),
        ),
        # §37.1. A closed basis vocabulary, and the invariants that keep the
        # three apart. Stated as equivalences, not implications: a basis without
        # its identity column is as wrong as an identity column without its
        # basis, and either one would let a row misdescribe how it got here.
        CheckConstraint(
            f"recipient_basis IN ({_RECIPIENT_BASIS_SQL})",
            name="ck_campaign_recipients_basis",
        ),
        CheckConstraint(
            f"recipient_basis = '{RECIPIENT_BASIS_EARNED}' OR provider = 'easyweek'",
            name="ck_campaign_recipients_basis_provider",
        ),
        CheckConstraint(
            f"(recipient_basis = '{RECIPIENT_BASIS_TEST}') = (easyweek_test_customer_uuid IS NOT NULL)",
            name="ck_campaign_recipients_basis_test_binding",
        ),
        CheckConstraint(
            f"(recipient_basis = '{RECIPIENT_BASIS_MANUAL}') = (easyweek_customer_uuid IS NOT NULL)",
            name="ck_campaign_recipients_basis_manual_binding",
        ),
        # A manual selection must not be able to look like earned proof. Not
        # "happens to have none" — cannot have any, so no later code path can
        # fill these in and turn an operator's decision into a first visit
        # nobody made.
        CheckConstraint(
            f"recipient_basis <> '{RECIPIENT_BASIS_MANUAL}' OR ("
            "source_easyweek_event_id IS NULL "
            "AND source_record_id IS NULL "
            "AND source_booking_uuid IS NULL "
            "AND source_visits_total IS NULL "
            "AND source_visits_total_updated_at IS NULL)",
            name="ck_campaign_recipients_manual_has_no_source_proof",
        ),
        # The original automatic verdict is audit, and audit belongs to the row
        # that actually overrode one: an operator's manual inclusion. Anywhere
        # else it would be a reason attached to a decision nobody made.
        CheckConstraint(
            f"auto_excluded_reason IS NULL OR (provider = 'easyweek' AND recipient_basis = '{RECIPIENT_BASIS_MANUAL}')",
            name="ck_campaign_recipients_auto_excluded_basis",
        ),
        # An ACTIVE earned EasyWeek candidate is a claim that the segmenter
        # proved a first visit, and the proof is those five columns together.
        # The segmenter already works this way — it attaches the proof exactly
        # when the row is eligible — so this states the rule rather than adding
        # one, and stops any later path from producing a proof-free row that
        # still says `earned`.
        CheckConstraint(
            f"NOT (provider = 'easyweek' AND recipient_basis = '{RECIPIENT_BASIS_EARNED}' "
            "AND status = 'candidate' AND excluded_reason IS NULL) OR ("
            "source_easyweek_event_id IS NOT NULL "
            "AND source_record_id IS NOT NULL "
            "AND source_booking_uuid IS NOT NULL "
            "AND source_visits_total IS NOT NULL "
            "AND source_visits_total_updated_at IS NOT NULL)",
            name="ck_campaign_recipients_active_earned_has_proof",
        ),
        CheckConstraint(
            "manual_policy IS NULL OR (provider = 'easyweek' AND recipient_basis = 'operator_manual_selection' "
            "AND manual_policy = 'altegio_visit_zero_easyweek_bookings')",
            name="ck_campaign_recipients_manual_policy",
        ),
        CheckConstraint(
            "(manual_policy IS NULL) = (manual_policy_checked_at IS NULL) AND "
            "(manual_policy IS NULL) = (manual_operator_attested_at IS NULL)",
            name="ck_campaign_recipients_manual_policy_proof",
        ),
        # One ACTIVE row per proven customer per run. Keyed on the customer UUID
        # rather than the phone: one number can legitimately belong to two
        # customers, which is an ambiguity the add path refuses outright — but
        # the database must not depend on that refusal being remembered.
        Index(
            "uq_campaign_recipients_manual_customer_per_run",
            "provider",
            "campaign_run_id",
            "easyweek_customer_uuid",
            unique=True,
            postgresql_where=text("easyweek_customer_uuid IS NOT NULL"),
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    provider: Mapped[str] = _provider_column()

    campaign_run_id: Mapped[int] = mapped_column(
        BigInteger,
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

    # PR-14: restart-safe evidence for an eligible EasyWeek proven-subset row.
    # Raw event payloads are intentionally not copied into recipient.meta.
    source_easyweek_event_id: Mapped[int | None] = mapped_column(
        BigInteger,
        ForeignKey("easyweek_events.id", name="fk_campaign_recipients_source_easyweek_event"),
        nullable=True,
        index=True,
    )
    source_record_id: Mapped[int | None] = mapped_column(
        BigInteger,
        ForeignKey("records.id", name="fk_campaign_recipients_source_record"),
        nullable=True,
        index=True,
    )
    source_booking_uuid: Mapped[uuid.UUID | None] = mapped_column(
        PostgresUUID(as_uuid=True),
        nullable=True,
    )
    source_visits_total: Mapped[int | None] = mapped_column(Integer, nullable=True)
    source_visits_total_updated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    # §36.11: the owner-approved test identity this row stands for, or NULL for
    # every ordinary recipient. Its presence IS the basis — there is no separate
    # boolean to disagree with it — and the constraints above make sure a row
    # carrying it can never also carry earned-first-visit proof.
    easyweek_test_customer_uuid: Mapped[uuid.UUID | None] = mapped_column(
        PostgresUUID(as_uuid=True),
        nullable=True,
    )

    # §37.1: how this row got into the snapshot. Typed, because every report
    # that shows a recipient has to be able to say whether a first visit was
    # proven, an approved test account was substituted, or a person decided.
    recipient_basis: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        server_default=text(f"'{RECIPIENT_BASIS_EARNED}'"),
    )

    # The EasyWeek customer an operator's manual selection was proven against,
    # live, before the row was written. NULL for every other basis.
    easyweek_customer_uuid: Mapped[uuid.UUID | None] = mapped_column(
        PostgresUUID(as_uuid=True),
        nullable=True,
    )

    # §44 is opt-in. NULL retains §37's historical manual-selection contract.
    manual_policy: Mapped[str | None] = mapped_column(String(64), nullable=True)
    manual_policy_checked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    manual_operator_attested_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # What the segmenter had decided before an operator included this person
    # anyway. Kept so an override never erases the reason it overrode: the row
    # still says "the automatic answer was X, and a human said yes regardless".
    auto_excluded_reason: Mapped[str | None] = mapped_column(String(64), nullable=True)

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
        # Intentionally unsupported service categories do not transfer reminder
        # ownership to EasyWeek. They carry a separate three-column marker:
        # when automation was suppressed, which plan authorised it, and the
        # closed business reason. All three fields are one fact.
        CheckConstraint(
            "((reminders_suppressed_at IS NULL) = "
            "(reminder_suppression_plan_digest IS NULL)) AND "
            "((reminders_suppressed_at IS NULL) = "
            "(reminder_suppression_reason_code IS NULL))",
            name="ck_easyweek_migration_ledger_reminder_suppression_complete",
        ),
        CheckConstraint(
            "reminder_suppression_reason_code IS NULL OR "
            "reminder_suppression_reason_code = 'service_category_not_allowed'",
            name="ck_easyweek_migration_ledger_reminder_suppression_reason",
        ),
        # Ownership and intentional suppression are opposite dispositions. A
        # row carrying both would let different runtime readers reach different
        # answers, so the database makes the state impossible.
        CheckConstraint(
            "NOT (reminders_handed_over_at IS NOT NULL AND reminders_suppressed_at IS NOT NULL)",
            name="ck_easyweek_migration_ledger_reminder_marker_exclusive",
        ),
        # A rollback attempt is one fact in two columns, for the same reason the
        # handover marker is: half of it would be an attempt with no run to
        # attribute it to, or a run id claiming an attempt that never happened.
        CheckConstraint(
            "(rollback_attempted_at IS NULL) = (rollback_attempt_run_id IS NULL)",
            name="ck_easyweek_migration_ledger_rollback_attempt_complete",
        ),
        # PR-12.1 (§31). The same shape as the reminder marker and deliberately
        # NOT the same columns: §30 proves that timed EasyWeek reminders were
        # created, while this proves that source marketing ownership was given
        # up WITHOUT creating any target obligation. Merging them would let one
        # proof stand in for the other.
        CheckConstraint(
            "(post_booking_jobs_handed_over_at IS NULL) = (post_booking_handover_plan_digest IS NULL)",
            name="ck_easyweek_migration_ledger_post_booking_handover_complete",
        ),
        # Read by the Altegio planner on every create/update/delete delivery and
        # by the outbox immediately before a send, so it has to be index-only.
        # Partial for the same reason as the reminder one: only handed-over rows
        # are ever asked about.
        Index(
            "ix_easyweek_migration_ledger_post_booking_handover",
            "source_provider",
            "source_company_id",
            "source_record_id",
            postgresql_where=text("post_booking_jobs_handed_over_at IS NOT NULL"),
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
        Index(
            "ix_easyweek_migration_ledger_reminder_suppression",
            "source_provider",
            "source_company_id",
            "source_record_id",
            postgresql_where=text("reminders_suppressed_at IS NOT NULL"),
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

    # -- intentional reminder suppression (PR-11.2, plan §30.13) ----------
    # A category_not_allowed row does not transfer reminder ownership to
    # EasyWeek: this bot intentionally owns no reminder obligation for it. The
    # marker is still durable so late Altegio events cannot recreate or requeue
    # a source reminder after the operator cancelled the frozen source set.
    reminders_suppressed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    reminder_suppression_plan_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)
    reminder_suppression_reason_code: Mapped[str | None] = mapped_column(String(64), nullable=True)

    # -- post-booking marketing ownership (PR-12.1, plan §31.6) ------------
    # When this booking's `review_3d` / `repeat_10d` / `comeback_3d` stopped
    # being Altegio's, and under which reviewed plan.
    #
    # A separate marker from the reminder one, and the difference is the whole
    # point. §30 says "the EasyWeek reminders for this booking exist"; this says
    # "the Altegio marketing obligations for this booking are withdrawn and no
    # target obligation was created in their place" — because a migrated future
    # booking proves no completed visit, and only a real EasyWeek outcome event
    # may ever create the EasyWeek equivalents.
    #
    # It is written for EVERY eligible row of the wave, including rows that
    # carry no such job right now: without it, a late Altegio delivery would
    # create the FIRST obligation after the handover, and there would be nothing
    # for the runtime fences to find.
    post_booking_jobs_handed_over_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    # The reviewed plan digest that authorised it. A repeat of the same snapshot
    # is idempotent; a different digest is refused rather than silently
    # overwriting somebody else's authorisation.
    post_booking_handover_plan_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)

    # -- rollback attempt (PR-11.2, plan §30.12) ---------------------------
    # When a confirmed rollback was about to send this row's cancel, and which
    # run was about to send it. Written and COMMITTED before the PUT leaves.
    #
    # It exists because a cancel can end in three states, and only one of them
    # is knowable from EasyWeek alone. A booking that reads as cancelled today
    # is either one this tool cancelled (whose response we may never have seen)
    # or one a person cancelled by hand — and those must not be treated the
    # same: the first may finish as `rolled_back`, the second is a target
    # somebody modified and is not ours to claim.
    #
    # The marker is the only durable difference between them, so it is written
    # BEFORE the mutation rather than after it. A crash between the write and
    # the PUT therefore leaves a row that says "a cancel may have been sent" —
    # which is exactly what happened, and which the next run resolves by
    # reading the booking rather than by sending a second PUT.
    rollback_attempted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    # Kept beside the instant so an operator can tell WHICH rollback run left an
    # unresolved attempt without joining anything.
    rollback_attempt_run_id: Mapped[str | None] = mapped_column(String(64), nullable=True)

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


class EasyWeekMigrationWaveClosure(Base):
    """One durable fact: this wave's reminders are EasyWeek's now.

    A wave is (source provider, source company, origin run). After a supervised
    reminder handover, no booking may be added to it: the handover proved and
    marked everything it could see, and a booking created afterwards would have
    its reminders withdrawn on the Altegio side and never created on the
    EasyWeek one.

    Why this is a row of its own rather than the per-ledger-row handover marker.
    The marker can only exist where a `created` row exists, so a company/run pair
    the snapshot named but that held no created row — an empty pair, or one with
    only `failed` rows — had nothing to carry it. `wave_handed_over` therefore
    answered "no" for that pair the moment the transaction's advisory lock was
    released, and a late migration retry under that run id could POST a booking
    into a wave that had already been closed. The advisory lock serialises; only
    a committed row can outlive the transaction that took it.

    Written in the SAME transaction as the reminder jobs, the cancellations and
    the row markers, so a rolled-back handover leaves no closure behind, and a
    committed one closes every pair it claimed — including the empty ones.

    PII-free by construction: ids, a run id, a plan digest and an instant.
    """

    __tablename__ = "easyweek_migration_wave_closure"
    __table_args__ = (
        # One closure per wave. A repeat of the same handover finds its own row
        # and changes nothing; a different plan digest is a conflict, not an
        # update, because two different authorisations cannot both have closed
        # the same wave.
        UniqueConstraint(
            "source_provider",
            "source_company_id",
            "run_id",
            name="uq_easyweek_migration_wave_closure_identity",
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    source_provider: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        server_default=_PROVIDER_SERVER_DEFAULT,
    )
    source_company_id: Mapped[int] = mapped_column(Integer, nullable=False)
    # The ORIGIN run of the migration wave, exactly as the ledger stores it.
    run_id: Mapped[str] = mapped_column(String(64), nullable=False)
    # The reviewed snapshot that authorised the closure. Kept so a repeat can be
    # recognised as the same handover rather than as a second one.
    plan_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    closed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
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


class EasyWeekVoucherCanaryLedger(Base):
    """The one durable row of the controlled voucher mutation canary (§35).

    The canary creates, pays for and refunds exactly one real EasyWeek voucher
    order. Every one of those three steps is a mutation nobody can take back by
    repeating it, and each may end with the request sent and the answer lost. So
    the question "did we already claim this step, and could it have reached
    EasyWeek?" lives in PostgreSQL under a unique constraint — not in a report
    file, not in a marker on the remote order, and not in memory that dies with
    the process.

    One row, ever
    -------------
    Identity is the canary SCOPE, not the order: a target UUID is the *result*
    of the first mutation and is unknown for exactly the rows that need the
    guarantee most. ``canary_scope`` is unique, so a second operator, a rerun and
    a resumed crash all collide on the same row rather than starting a second
    canary. Running another canary is deliberately a code change (a new scope
    constant) plus a review.

    Claim before request
    --------------------
    Every stage writes its ``*_claimed_at`` and commits BEFORE the POST leaves.
    A crash between that commit and the response is therefore recorded as
    "claimed, outcome unknown" rather than as "nothing happened", which is the
    only reading that cannot double-charge a real card.

    Nothing here is PII
    -------------------
    No customer name, phone or e-mail; no staffer or account name; no Bearer
    key; no headers; no request or response body; no voucher code; no
    customer-facing URL; no free-text note; no raw EasyWeek payload. The three
    operator-supplied identities survive only as salted SHA-256 fingerprints, and
    the voucher artifact survives only as shape: a key, a JSON type, a presence
    flag and a truncated fingerprint. ``target_order_uuid`` is the single
    exception, and it is here because a refund and a manual cleanup are
    impossible without it.
    """

    __tablename__ = "easyweek_voucher_canary_ledger"
    __table_args__ = (
        # The whole idempotency guarantee, arbitrated by the database rather
        # than by a read-then-write race in the tool.
        UniqueConstraint("canary_scope", name="uq_easyweek_voucher_canary_scope"),
        Index("ix_easyweek_voucher_canary_status", "status"),
        CheckConstraint(
            "status IN ("
            "'create_claimed','create_unknown','create_rejected','created',"
            "'pay_claimed','pay_unknown','pay_rejected','paid',"
            "'refund_claimed','refund_unknown','refund_rejected','refunded',"
            "'ambiguous','manually_cleaned')",
            name="ck_easyweek_voucher_canary_status",
        ),
        # A row that reached any post-create state must name the order it is
        # talking about. Without this, a resumed run could report a payment
        # against an order nobody can find, refund or cancel.
        CheckConstraint(
            "status IN ('create_claimed','create_unknown','create_rejected','ambiguous') "
            "OR target_order_uuid IS NOT NULL",
            name="ck_easyweek_voucher_canary_target_required",
        ),
        # An attempt that was never claimed would mean a request left without a
        # durable record in front of it — the exact failure the claim exists to
        # prevent. Likewise a verification with no attempt behind it.
        CheckConstraint(
            "(create_attempted_at IS NULL OR create_claimed_at IS NOT NULL) AND "
            "(pay_attempted_at IS NULL OR pay_claimed_at IS NOT NULL) AND "
            "(refund_attempted_at IS NULL OR refund_claimed_at IS NOT NULL)",
            name="ck_easyweek_voucher_canary_attempt_needs_claim",
        ),
        CheckConstraint(
            "(create_verified_at IS NULL OR create_attempted_at IS NOT NULL) AND "
            "(pay_verified_at IS NULL OR pay_attempted_at IS NOT NULL) AND "
            "(refund_verified_at IS NULL OR refund_attempted_at IS NOT NULL)",
            name="ck_easyweek_voucher_canary_verify_needs_attempt",
        ),
        # A later stage cannot be claimed before an earlier one was.
        CheckConstraint(
            "(pay_claimed_at IS NULL OR create_claimed_at IS NOT NULL) AND "
            "(refund_claimed_at IS NULL OR pay_claimed_at IS NOT NULL)",
            name="ck_easyweek_voucher_canary_stage_order",
        ),
        # Fingerprints are full SHA-256 hex digests, and so is every stage
        # authorisation. The later two are nullable because a canary that never
        # reached them legitimately has none.
        CheckConstraint(
            "char_length(customer_fingerprint) = 64 AND "
            "char_length(staffer_fingerprint) = 64 AND "
            "char_length(account_fingerprint) = 64 AND "
            "char_length(template_config_digest) = 64 AND "
            "char_length(create_plan_digest) = 64 AND "
            "(pay_plan_digest IS NULL OR char_length(pay_plan_digest) = 64) AND "
            "(refund_plan_digest IS NULL OR char_length(refund_plan_digest) = 64)",
            name="ck_easyweek_voucher_canary_digest_lengths",
        ),
        # A stage that was claimed must name the plan that authorised it.
        CheckConstraint(
            "(pay_claimed_at IS NULL) = (pay_plan_digest IS NULL) AND "
            "(refund_claimed_at IS NULL) = (refund_plan_digest IS NULL)",
            name="ck_easyweek_voucher_canary_stage_plan_recorded",
        ),
        # The reconciliation window is what bounds an unresolved create search.
        # An inverted or absent window would let that search widen silently.
        CheckConstraint(
            "create_window_end > create_window_start",
            name="ck_easyweek_voucher_canary_window_ordered",
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    # -- identity ----------------------------------------------------------
    canary_scope: Mapped[str] = mapped_column(String(64), nullable=False)
    request_schema_version: Mapped[str] = mapped_column(String(16), nullable=False)
    # The FROZEN product configuration — price, flags, branch and service counts
    # — and nothing that legitimately moves. Re-derived live before every stage;
    # a difference is a template edit and stops the canary.
    template_config_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    # One approved plan per stage. A single digest could only ever authorise the
    # first mutation: after `create` succeeds, the plan that required no marker
    # order to exist can never be satisfied again.
    create_plan_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    pay_plan_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)
    refund_plan_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)

    # -- who, as one-way digests only --------------------------------------
    customer_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    staffer_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    account_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    # Non-personal slug written into the order comment. It is how an unresolved
    # create is found again, by this tool and by a human in the dashboard.
    reconciliation_marker: Mapped[str] = mapped_column(String(64), nullable=False)

    # -- state -------------------------------------------------------------
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    reason_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    target_order_uuid: Mapped[uuid.UUID | None] = mapped_column(PostgresUUID(as_uuid=True), nullable=True)

    # -- the bounded search window for an unresolved create ----------------
    create_window_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    create_window_end: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    # -- per-stage timestamps ---------------------------------------------
    create_claimed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    create_attempted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    create_verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    pay_claimed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    pay_attempted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    pay_verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    refund_claimed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    refund_attempted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    refund_verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    # Set only when a human closed an open draft in the EasyWeek dashboard and a
    # later reconcile OBSERVED that. The tool never claims it did the rollback.
    manual_cleanup_observed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # -- safe evidence -----------------------------------------------------
    # Shape facts only: stage, key path, JSON type, presence, bounded structural
    # length. Never a value, never a digest of a value, never a body.
    evidence: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    # The voucher counters observed at each stage, as a per-stage baseline. They
    # are evidence, not authority: a counter that moved because a voucher was
    # issued is the product working, and is never read as a template edit.
    stage_counters: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))

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


# ---------------------------------------------------------------------------
# §36: controlled voucher DELIVERY canary
# ---------------------------------------------------------------------------

# The closed state vocabulary of the delivery canary, mirrored by CHECK
# constraints below. Every one of these is a durable answer to "what may still
# happen to a real customer and a real €15?".
VOUCHER_DELIVERY_PLANNED = "planned"
VOUCHER_DELIVERY_CREATE_CLAIMED = "create_claimed"
VOUCHER_DELIVERY_CREATE_UNKNOWN = "create_unknown"
VOUCHER_DELIVERY_CREATE_REJECTED = "create_rejected"
VOUCHER_DELIVERY_CREATED = "created"
VOUCHER_DELIVERY_PAY_CLAIMED = "pay_claimed"
VOUCHER_DELIVERY_PAY_UNKNOWN = "pay_unknown"
VOUCHER_DELIVERY_PAY_REJECTED = "pay_rejected"
VOUCHER_DELIVERY_PAID = "paid"
VOUCHER_DELIVERY_SEND_CLAIMED = "send_claimed"
VOUCHER_DELIVERY_SEND_UNKNOWN = "send_unknown"
VOUCHER_DELIVERY_SEND_REJECTED = "send_rejected"
VOUCHER_DELIVERY_PROVIDER_ACCEPTED = "provider_accepted"
VOUCHER_DELIVERY_DELIVERED = "delivered"
VOUCHER_DELIVERY_READ = "read"
VOUCHER_DELIVERY_REFUND_CLAIMED = "refund_claimed"
VOUCHER_DELIVERY_REFUND_UNKNOWN = "refund_unknown"
VOUCHER_DELIVERY_REFUND_REJECTED = "refund_rejected"
VOUCHER_DELIVERY_REFUNDED = "refunded"
VOUCHER_DELIVERY_MANUALLY_CLEANED = "manually_cleaned"
VOUCHER_DELIVERY_AMBIGUOUS = "ambiguous"

VOUCHER_DELIVERY_STATUSES = (
    VOUCHER_DELIVERY_PLANNED,
    VOUCHER_DELIVERY_CREATE_CLAIMED,
    VOUCHER_DELIVERY_CREATE_UNKNOWN,
    VOUCHER_DELIVERY_CREATE_REJECTED,
    VOUCHER_DELIVERY_CREATED,
    VOUCHER_DELIVERY_PAY_CLAIMED,
    VOUCHER_DELIVERY_PAY_UNKNOWN,
    VOUCHER_DELIVERY_PAY_REJECTED,
    VOUCHER_DELIVERY_PAID,
    VOUCHER_DELIVERY_SEND_CLAIMED,
    VOUCHER_DELIVERY_SEND_UNKNOWN,
    VOUCHER_DELIVERY_SEND_REJECTED,
    VOUCHER_DELIVERY_PROVIDER_ACCEPTED,
    VOUCHER_DELIVERY_DELIVERED,
    VOUCHER_DELIVERY_READ,
    VOUCHER_DELIVERY_REFUND_CLAIMED,
    VOUCHER_DELIVERY_REFUND_UNKNOWN,
    VOUCHER_DELIVERY_REFUND_REJECTED,
    VOUCHER_DELIVERY_REFUNDED,
    VOUCHER_DELIVERY_MANUALLY_CLEANED,
    VOUCHER_DELIVERY_AMBIGUOUS,
)

_VOUCHER_DELIVERY_STATUS_SQL = ", ".join(f"'{value}'" for value in VOUCHER_DELIVERY_STATUSES)

# On what grounds this canary may act for this person (§36.11). Two grounds, and
# the difference between them is not a detail: one is a voucher somebody earned,
# the other is the owner's test account standing in for one.
#
# `earned_first_visit` is the §36 contract unchanged — a real first visit, proven
# live, with the booking that earned it recorded and unique.
#
# `owner_test_account` is the one pre-configured identity the owner approved for
# testing. Its history cannot be cleared and a fresh account per attempt is not
# workable, so the first-visit proof is `not_applicable` for it — recorded as
# exactly that, never as a quiet `true`. It carries no booking at all, because
# there is no visit to point at and inventing one would make the ledger lie.
VOUCHER_DELIVERY_BASIS_EARNED = "earned_first_visit"
VOUCHER_DELIVERY_BASIS_TEST = "owner_test_account"
VOUCHER_DELIVERY_BASES = (VOUCHER_DELIVERY_BASIS_EARNED, VOUCHER_DELIVERY_BASIS_TEST)
_VOUCHER_DELIVERY_BASIS_SQL = ", ".join(f"'{value}'" for value in VOUCHER_DELIVERY_BASES)


class EasyWeekCampaignVoucherDeliveryLedger(Base):
    """The one durable row of the controlled voucher DELIVERY canary (§36).

    §35 proved that this application can create, pay for and refund one real
    voucher. This canary adds the step §35 deliberately refused: giving the
    resulting code to a real person over WhatsApp. That changes what the durable
    state has to answer. It is no longer only "could this POST have reached
    EasyWeek?" but also "could a customer already be holding a €15 code?" — and
    the second question has no undo at all.

    Two uniqueness rules, for two different mistakes
    ------------------------------------------------
    ``canary_scope`` is unique, so a second operator, a rerun and a resumed
    crash all collide on one row rather than starting a second canary. That
    stops the canary from happening twice.

    The entitlement key — provider, company, campaign code and the SOURCE
    BOOKING the entitlement was earned by — is unique independently of the
    preview run. That stops the same person from being given a second voucher
    through a fresh preview, which is the mistake a scope-only rule would miss
    entirely: a new run means new row ids for the same human being.

    Nothing here is the voucher
    ---------------------------
    The code is a bearer secret. It is never a column. What is stored is a keyed
    MAC of it, bound to this row and this order, plus the id of the key that
    produced the MAC — enough to prove before sending that the code in hand is
    the code the paid order issued, and useless to anyone who reads the table.

    No name, phone, e-mail, message text or template parameter is stored either.
    The recipient is addressed by id, and the addressing is re-proven live
    against the campaign tables before every external step.
    """

    __tablename__ = "easyweek_campaign_voucher_delivery_ledger"

    __table_args__ = (
        # 1. One canary, ever. A second scope is a code change plus a review.
        UniqueConstraint("canary_scope", name="uq_ew_voucher_delivery_scope"),
        # 2. One voucher per earned entitlement, whatever run proposes it.
        # Partial, because only an earned row HAS an entitlement to be unique
        # about: a test row carries no booking, and a plain unique constraint
        # over a nullable column would enforce nothing there anyway.
        Index(
            "uq_ew_voucher_delivery_entitlement",
            "provider",
            "company_id",
            "campaign_code",
            "source_booking_uuid",
            unique=True,
            postgresql_where=text(f"recipient_basis = '{VOUCHER_DELIVERY_BASIS_EARNED}'"),
        ),
        # 2b. And one canary per test identity, which is the same rule stated
        # about the only thing a test row can be identified by. Without it a
        # second preview could point a second canary at the same account.
        Index(
            "uq_ew_voucher_delivery_test_identity",
            "provider",
            "company_id",
            "campaign_code",
            "easyweek_customer_uuid",
            unique=True,
            postgresql_where=text(f"recipient_basis = '{VOUCHER_DELIVERY_BASIS_TEST}'"),
        ),
        # 3-5. A result may belong to exactly one canary row.
        UniqueConstraint("target_order_uuid", name="uq_ew_voucher_delivery_target_order"),
        UniqueConstraint("outbound_intent_uuid", name="uq_ew_voucher_delivery_intent"),
        UniqueConstraint("provider_message_id", name="uq_ew_voucher_delivery_provider_message"),
        # 6. The recipient must belong to the run this row names, under the same
        # provider. A composite FK, because two separate FKs would each be
        # satisfied by rows that have nothing to do with each other.
        ForeignKeyConstraint(
            ["campaign_run_id", "provider"],
            ["campaign_runs.id", "campaign_runs.provider"],
            name="fk_ew_voucher_delivery_run_provider",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["campaign_recipient_id", "provider"],
            ["campaign_recipients.id", "campaign_recipients.provider"],
            name="fk_ew_voucher_delivery_recipient_provider",
            ondelete="RESTRICT",
        ),
        # 7. A closed status vocabulary. An unknown string is not a state.
        CheckConstraint(
            f"status IN ({_VOUCHER_DELIVERY_STATUS_SQL})",
            name="ck_ew_voucher_delivery_status",
        ),
        # This canary is EasyWeek-only by construction.
        CheckConstraint("provider = 'easyweek'", name="ck_ew_voucher_delivery_provider"),
        # A closed basis vocabulary, and the one invariant that keeps the two
        # apart: an earned row names the booking it was earned by, and a test
        # row names none. That is what stops a test canary from being written
        # with a borrowed, random or customer-shaped "source booking" — the
        # column is not merely unused there, it is forbidden.
        CheckConstraint(
            f"recipient_basis IN ({_VOUCHER_DELIVERY_BASIS_SQL})",
            name="ck_ew_voucher_delivery_basis",
        ),
        CheckConstraint(
            f"(recipient_basis = '{VOUCHER_DELIVERY_BASIS_EARNED}') = (source_booking_uuid IS NOT NULL)",
            name="ck_ew_voucher_delivery_basis_source",
        ),
        # 8. Stage order. A stage cannot be attempted before it was claimed, and
        # cannot be verified before it was attempted.
        CheckConstraint(
            "(create_attempted_at IS NULL OR create_claimed_at IS NOT NULL) "
            "AND (create_verified_at IS NULL OR create_attempted_at IS NOT NULL) "
            "AND (pay_attempted_at IS NULL OR pay_claimed_at IS NOT NULL) "
            "AND (pay_verified_at IS NULL OR pay_attempted_at IS NOT NULL) "
            "AND (send_attempted_at IS NULL OR send_claimed_at IS NOT NULL) "
            "AND (refund_attempted_at IS NULL OR refund_claimed_at IS NOT NULL) "
            "AND (refund_verified_at IS NULL OR refund_attempted_at IS NOT NULL)",
            name="ck_ew_voucher_delivery_stage_order",
        ),
        # 9. Money cannot move before the order it pays for was proven to exist.
        CheckConstraint(
            "pay_claimed_at IS NULL OR (create_verified_at IS NOT NULL AND target_order_uuid IS NOT NULL)",
            name="ck_ew_voucher_delivery_pay_needs_created",
        ),
        # 10. Nothing may be sent before the voucher is proven paid for, proven
        # bound to this row by MAC, and proven still deliverable by a live guard
        # taken after the payment.
        CheckConstraint(
            "send_claimed_at IS NULL OR ("
            "pay_verified_at IS NOT NULL "
            "AND voucher_code_hmac IS NOT NULL "
            "AND hmac_key_id IS NOT NULL "
            "AND live_guard_reproven_at IS NOT NULL "
            "AND live_guard_reproven_at >= pay_verified_at)",
            name="ck_ew_voucher_delivery_send_needs_paid",
        ),
        # 11. Acceptance is something Meta said, so it needs both the attempt we
        # made and the identifier Meta answered with.
        CheckConstraint(
            "provider_accepted_at IS NULL OR (send_attempted_at IS NOT NULL AND provider_message_id IS NOT NULL)",
            name="ck_ew_voucher_delivery_accepted_needs_attempt",
        ),
        # 12-13. The webhook ladder. Delivered needs acceptance; read needs
        # delivery. A provider that reports them out of order proves nothing new.
        CheckConstraint(
            "delivered_at IS NULL OR provider_accepted_at IS NOT NULL",
            name="ck_ew_voucher_delivery_delivered_needs_accepted",
        ),
        CheckConstraint(
            "read_at IS NULL OR delivered_at IS NOT NULL",
            name="ck_ew_voucher_delivery_read_needs_delivered",
        ),
        # 14. A refund is only ever the pre-send escape hatch. Once a message
        # may be in a customer's hands the money stays where it is, and the
        # decision becomes a human one.
        CheckConstraint(
            "refund_claimed_at IS NULL OR ("
            "provider_accepted_at IS NULL "
            "AND delivered_at IS NULL "
            "AND read_at IS NULL "
            "AND send_attempted_at IS NULL)",
            name="ck_ew_voucher_delivery_refund_is_pre_send",
        ),
        CheckConstraint(
            f"status <> '{VOUCHER_DELIVERY_REFUNDED}' OR ("
            "provider_accepted_at IS NULL AND delivered_at IS NULL AND read_at IS NULL)",
            name="ck_ew_voucher_delivery_refunded_never_sent",
        ),
        # At most one delivery attempt in the lifetime of this row. Not a retry
        # budget: a counter that can only be zero or one.
        CheckConstraint(
            "send_attempt_count >= 0 AND send_attempt_count <= 1",
            name="ck_ew_voucher_delivery_single_attempt",
        ),
        CheckConstraint(
            "(send_attempt_count = 0) = (send_attempted_at IS NULL)",
            name="ck_ew_voucher_delivery_attempt_count_matches",
        ),
        # A MAC without the key that made it cannot be verified later.
        CheckConstraint(
            "(voucher_code_hmac IS NULL) = (hmac_key_id IS NULL)",
            name="ck_ew_voucher_delivery_hmac_pair",
        ),
        Index("ix_ew_voucher_delivery_status", "status"),
        Index("ix_ew_voucher_delivery_recipient", "campaign_recipient_id"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    # -- identity ----------------------------------------------------------
    canary_scope: Mapped[str] = mapped_column(String(128), nullable=False)
    request_schema_version: Mapped[str] = mapped_column(String(16), nullable=False)
    provider: Mapped[str] = _provider_column()
    company_id: Mapped[int] = mapped_column(Integer, nullable=False)
    campaign_code: Mapped[str] = mapped_column(String(128), nullable=False)

    # The exact preview recipient an operator chose, and the run it came from.
    campaign_run_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    campaign_recipient_id: Mapped[int] = mapped_column(BigInteger, nullable=False)

    # On what grounds this row exists: an earned first visit, or the owner's
    # approved test account. Not a flag — every stage plan, the identity match
    # and the operator report all read it, so a row cannot quietly change what
    # it is claiming about the person it names.
    recipient_basis: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        server_default=text(f"'{VOUCHER_DELIVERY_BASIS_EARNED}'"),
    )

    # The visit that earned the entitlement. Part of the uniqueness rule that
    # survives a new preview run — and NULL for a test row, which has no visit
    # to name. A CHECK ties the two together in both directions.
    source_booking_uuid: Mapped[uuid.UUID | None] = mapped_column(PostgresUUID(as_uuid=True), nullable=True)
    easyweek_customer_uuid: Mapped[uuid.UUID] = mapped_column(PostgresUUID(as_uuid=True), nullable=False)

    # -- the frozen EasyWeek identity this canary may act on ----------------
    location_uuid: Mapped[uuid.UUID] = mapped_column(PostgresUUID(as_uuid=True), nullable=False)
    staffer_uuid: Mapped[uuid.UUID] = mapped_column(PostgresUUID(as_uuid=True), nullable=False)
    payment_account_uuid: Mapped[uuid.UUID] = mapped_column(PostgresUUID(as_uuid=True), nullable=False)
    voucher_template_uuid: Mapped[uuid.UUID] = mapped_column(PostgresUUID(as_uuid=True), nullable=False)
    # Recomputable, non-personal, and findable in the EasyWeek dashboard.
    reconciliation_marker: Mapped[str] = mapped_column(String(64), nullable=False)

    # -- results -----------------------------------------------------------
    target_order_uuid: Mapped[uuid.UUID | None] = mapped_column(PostgresUUID(as_uuid=True), nullable=True)
    # Our own identifier for the one delivery attempt, created and committed
    # before the request leaves.
    outbound_intent_uuid: Mapped[uuid.UUID | None] = mapped_column(PostgresUUID(as_uuid=True), nullable=True)
    # What Meta answered with, and the only key a webhook may match on.
    provider_message_id: Mapped[str | None] = mapped_column(String(128), nullable=True)

    # -- the voucher, as a keyed proof and nothing else ---------------------
    voucher_code_hmac: Mapped[str | None] = mapped_column(String(64), nullable=True)
    hmac_key_id: Mapped[str | None] = mapped_column(String(64), nullable=True)

    # -- state -------------------------------------------------------------
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    reason_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    manual_cleanup_required: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    reconciliation_required: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    manual_cleanup_observed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # -- operator authorisation provenance ---------------------------------
    create_plan_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)
    pay_plan_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)
    deliver_plan_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)
    refund_plan_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)

    # -- claim / attempt / verification, per stage -------------------------
    create_claimed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    create_attempted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    create_verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    create_window_start: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    create_window_end: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    pay_claimed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    pay_attempted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    pay_verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # The live guard taken immediately before sending, after the payment.
    live_guard_reproven_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    send_claimed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    send_attempted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    send_attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    provider_accepted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    read_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    refund_claimed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    refund_attempted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    refund_verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # -- safe evidence -----------------------------------------------------
    # Shape facts, proof labels and reason codes. Never a value, never a body,
    # never a template parameter, never the code.
    evidence: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )


class EasyWeekCampaignVoucherDeliveryAttempt(Base):
    """One redacted outbound intent, written and committed BEFORE the send.

    Deliberately its own table rather than an ``OutboxMessage``. The outbox is
    swept by a generic worker whose whole purpose is to retry what it finds, and
    the one property this row must have is that nothing may ever pick it up and
    send it again. Guaranteeing that inside a shared table would mean proving a
    negative about every current and future query against it; a separate table
    the worker has never heard of needs no such proof.

    It stores no message. There is no rendered body, no parameter list and no
    voucher code here, so nothing downstream could re-render the message even if
    it tried: the code exists only in memory, between one read of the paid order
    and one POST.
    """

    __tablename__ = "easyweek_campaign_voucher_delivery_attempts"

    __table_args__ = (
        UniqueConstraint("intent_uuid", name="uq_ew_voucher_delivery_attempt_intent"),
        # Audit rows reference the ledger and must never be the reason it can be
        # deleted, nor disappear with it: RESTRICT in both directions of intent.
        ForeignKeyConstraint(
            ["ledger_id"],
            ["easyweek_campaign_voucher_delivery_ledger.id"],
            name="fk_ew_voucher_delivery_attempt_ledger",
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            "outcome IN ('claimed', 'provider_accepted', 'unknown', 'rejected')",
            name="ck_ew_voucher_delivery_attempt_outcome",
        ),
        CheckConstraint(
            "outcome <> 'provider_accepted' OR provider_message_id IS NOT NULL",
            name="ck_ew_voucher_delivery_attempt_accepted_has_id",
        ),
        Index("ix_ew_voucher_delivery_attempt_ledger", "ledger_id"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    ledger_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    intent_uuid: Mapped[uuid.UUID] = mapped_column(PostgresUUID(as_uuid=True), nullable=False)

    # Enough to audit WHICH approved template was used, and nothing about what
    # it said to whom.
    template_code: Mapped[str] = mapped_column(String(64), nullable=False)
    meta_template_name: Mapped[str] = mapped_column(String(128), nullable=False)
    template_language: Mapped[str] = mapped_column(String(8), nullable=False)
    # The sender line, by id. Not the number.
    sender_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    # The recipient by reference only — the campaign row already holds identity,
    # re-proven live before every send.
    campaign_recipient_id: Mapped[int] = mapped_column(BigInteger, nullable=False)

    outcome: Mapped[str] = mapped_column(String(32), nullable=False)
    reason_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    provider_message_id: Mapped[str | None] = mapped_column(String(128), nullable=True)

    claimed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


# ---------------------------------------------------------------------------
# §37.2: controlled MANUAL-BASIS voucher delivery canary
# ---------------------------------------------------------------------------
# §36 delivers to somebody whose first visit earned the voucher, or to the one
# pre-configured test account. This canary delivers to a real customer an
# OPERATOR picked by hand in a preview.
#
# The state vocabulary is the same shape, because the sequence of irreversible
# things that can happen to €15 and to a person's phone is the same. It is
# repeated rather than imported so that a future change to one canary cannot
# silently redefine the durable states of the other.
MANUAL_VOUCHER_PLANNED = "planned"
MANUAL_VOUCHER_CREATE_CLAIMED = "create_claimed"
MANUAL_VOUCHER_CREATE_UNKNOWN = "create_unknown"
MANUAL_VOUCHER_CREATE_REJECTED = "create_rejected"
MANUAL_VOUCHER_CREATED = "created"
MANUAL_VOUCHER_PAY_CLAIMED = "pay_claimed"
MANUAL_VOUCHER_PAY_UNKNOWN = "pay_unknown"
MANUAL_VOUCHER_PAY_REJECTED = "pay_rejected"
MANUAL_VOUCHER_PAID = "paid"
MANUAL_VOUCHER_SEND_CLAIMED = "send_claimed"
MANUAL_VOUCHER_SEND_UNKNOWN = "send_unknown"
MANUAL_VOUCHER_SEND_REJECTED = "send_rejected"
MANUAL_VOUCHER_PROVIDER_ACCEPTED = "provider_accepted"
MANUAL_VOUCHER_DELIVERED = "delivered"
MANUAL_VOUCHER_READ = "read"
MANUAL_VOUCHER_REFUND_CLAIMED = "refund_claimed"
MANUAL_VOUCHER_REFUND_UNKNOWN = "refund_unknown"
MANUAL_VOUCHER_REFUND_REJECTED = "refund_rejected"
MANUAL_VOUCHER_REFUNDED = "refunded"
MANUAL_VOUCHER_MANUALLY_CLEANED = "manually_cleaned"
MANUAL_VOUCHER_AMBIGUOUS = "ambiguous"

MANUAL_VOUCHER_STATUSES = (
    MANUAL_VOUCHER_PLANNED,
    MANUAL_VOUCHER_CREATE_CLAIMED,
    MANUAL_VOUCHER_CREATE_UNKNOWN,
    MANUAL_VOUCHER_CREATE_REJECTED,
    MANUAL_VOUCHER_CREATED,
    MANUAL_VOUCHER_PAY_CLAIMED,
    MANUAL_VOUCHER_PAY_UNKNOWN,
    MANUAL_VOUCHER_PAY_REJECTED,
    MANUAL_VOUCHER_PAID,
    MANUAL_VOUCHER_SEND_CLAIMED,
    MANUAL_VOUCHER_SEND_UNKNOWN,
    MANUAL_VOUCHER_SEND_REJECTED,
    MANUAL_VOUCHER_PROVIDER_ACCEPTED,
    MANUAL_VOUCHER_DELIVERED,
    MANUAL_VOUCHER_READ,
    MANUAL_VOUCHER_REFUND_CLAIMED,
    MANUAL_VOUCHER_REFUND_UNKNOWN,
    MANUAL_VOUCHER_REFUND_REJECTED,
    MANUAL_VOUCHER_REFUNDED,
    MANUAL_VOUCHER_MANUALLY_CLEANED,
    MANUAL_VOUCHER_AMBIGUOUS,
)

_MANUAL_VOUCHER_STATUS_SQL = ", ".join(f"'{value}'" for value in MANUAL_VOUCHER_STATUSES)

# The only basis this canary serves. A column rather than an implication,
# because every stage plan, the identity match and the operator report read it —
# and because a row must state, durably, that what stands behind it is a
# person's decision and not a proven first visit.
MANUAL_VOUCHER_BASIS = RECIPIENT_BASIS_MANUAL


class EasyWeekManualVoucherDeliveryLedger(Base):
    """The one durable row of the manual-basis voucher delivery canary (§37.2).

    What makes this different from §36 is not the machinery — it is what stands
    behind the row. §36 acts on an entitlement somebody earned by visiting, or
    on the owner's test account. Here an operator looked at a preview, typed a
    phone number, and decided. That decision is a legitimate reason to send one
    controlled voucher; it is not evidence of a first visit, and this table is
    built so it can never be mistaken for one.

    Hence: no ``source_booking_uuid`` column at all. There is no visit to point
    at, and a nullable column would be an invitation to fill it with a borrowed,
    random or customer-shaped value. The absence is structural.

    Uniqueness, for two different mistakes
    --------------------------------------
    ``canary_scope`` is unique, so a rerun, a second operator and a resumed
    crash all collide on one row rather than starting a second canary.

    The entitlement key is the customer plus the campaign PERIOD — provider,
    company, campaign code, EasyWeek customer UUID and both period bounds. A
    manual selection has no booking to be unique about, so the thing that must
    not repeat is "this person, this campaign, this month". A fresh preview of
    the same August wave produces new row ids for the same human being, and that
    is exactly the mistake a scope-only rule would miss.

    Nothing here is the voucher
    ---------------------------
    The code is a bearer secret and is never a column. What is stored is a keyed
    MAC bound to this row and this order, plus the id of the key that made it.
    No name, phone, e-mail, message text or template parameter is stored: the
    recipient is addressed by id, re-proven live before every external step.
    """

    __tablename__ = "easyweek_manual_voucher_delivery_ledger"

    __table_args__ = (
        # 1. One canary, ever.
        UniqueConstraint("canary_scope", name="uq_ew_manual_voucher_scope"),
        # 2. One voucher per person per campaign period, whatever run proposes
        # it. Not partial: every row of this table has a customer and a period,
        # because that is the only identity a manual selection has.
        UniqueConstraint(
            "provider",
            "company_id",
            "campaign_code",
            "easyweek_customer_uuid",
            "campaign_period_start",
            "campaign_period_end",
            name="uq_ew_manual_voucher_entitlement",
        ),
        # 3-5. A result may belong to exactly one canary row.
        UniqueConstraint("target_order_uuid", name="uq_ew_manual_voucher_target_order"),
        UniqueConstraint("outbound_intent_uuid", name="uq_ew_manual_voucher_intent"),
        UniqueConstraint("provider_message_id", name="uq_ew_manual_voucher_provider_message"),
        # 6. The recipient must belong to the run this row names, under the same
        # provider. Composite FKs, because two separate ones would each be
        # satisfied by rows that have nothing to do with each other.
        ForeignKeyConstraint(
            ["campaign_run_id", "provider"],
            ["campaign_runs.id", "campaign_runs.provider"],
            name="fk_ew_manual_voucher_run_provider",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["campaign_recipient_id", "provider"],
            ["campaign_recipients.id", "campaign_recipients.provider"],
            name="fk_ew_manual_voucher_recipient_provider",
            ondelete="RESTRICT",
        ),
        # 7. A closed status vocabulary. An unknown string is not a state.
        CheckConstraint(
            f"status IN ({_MANUAL_VOUCHER_STATUS_SQL})",
            name="ck_ew_manual_voucher_status",
        ),
        # 8. EasyWeek-only, Karlsruhe-only, manual-basis-only, by construction.
        # The company id is a literal here rather than configuration: this canary
        # was approved for one branch, and a misconfigured environment must not
        # be able to point a real payment at another one.
        CheckConstraint("provider = 'easyweek'", name="ck_ew_manual_voucher_provider"),
        CheckConstraint("company_id = 322579", name="ck_ew_manual_voucher_company"),
        CheckConstraint(
            f"recipient_basis = '{RECIPIENT_BASIS_MANUAL}'",
            name="ck_ew_manual_voucher_basis",
        ),
        # 9. A campaign period is an interval, and an entitlement key built on a
        # backwards one would be meaningless.
        CheckConstraint(
            "campaign_period_start < campaign_period_end",
            name="ck_ew_manual_voucher_period_order",
        ),
        # 10. Stage order. A stage cannot be attempted before it was claimed,
        # nor verified before it was attempted.
        CheckConstraint(
            "(create_attempted_at IS NULL OR create_claimed_at IS NOT NULL) "
            "AND (create_verified_at IS NULL OR create_attempted_at IS NOT NULL) "
            "AND (pay_attempted_at IS NULL OR pay_claimed_at IS NOT NULL) "
            "AND (pay_verified_at IS NULL OR pay_attempted_at IS NOT NULL) "
            "AND (send_attempted_at IS NULL OR send_claimed_at IS NOT NULL) "
            "AND (refund_attempted_at IS NULL OR refund_claimed_at IS NOT NULL) "
            "AND (refund_verified_at IS NULL OR refund_attempted_at IS NOT NULL)",
            name="ck_ew_manual_voucher_stage_order",
        ),
        # 11. Money cannot move before the order it pays for was proven to exist.
        CheckConstraint(
            "pay_claimed_at IS NULL OR (create_verified_at IS NOT NULL AND target_order_uuid IS NOT NULL)",
            name="ck_ew_manual_voucher_pay_needs_created",
        ),
        # 12. Nothing may be sent before the voucher is proven paid for, proven
        # bound to this row by MAC, and proven still deliverable by a live guard
        # taken AFTER the payment. The ordering is a constraint, not a comment:
        # a guard from before the payment says nothing about the person now.
        CheckConstraint(
            "send_claimed_at IS NULL OR ("
            "pay_verified_at IS NOT NULL "
            "AND voucher_code_hmac IS NOT NULL "
            "AND hmac_key_id IS NOT NULL "
            "AND live_guard_reproven_at IS NOT NULL "
            "AND live_guard_reproven_at >= pay_verified_at)",
            name="ck_ew_manual_voucher_send_needs_paid",
        ),
        # 13. Acceptance is something Meta said, so it needs both the attempt we
        # made and the identifier Meta answered with.
        CheckConstraint(
            "provider_accepted_at IS NULL OR (send_attempted_at IS NOT NULL AND provider_message_id IS NOT NULL)",
            name="ck_ew_manual_voucher_accepted_needs_attempt",
        ),
        # 14-15. The webhook ladder. Delivered needs acceptance; read needs
        # delivery. A provider reporting them out of order proves nothing new.
        CheckConstraint(
            "delivered_at IS NULL OR provider_accepted_at IS NOT NULL",
            name="ck_ew_manual_voucher_delivered_needs_accepted",
        ),
        CheckConstraint(
            "read_at IS NULL OR delivered_at IS NOT NULL",
            name="ck_ew_manual_voucher_read_needs_delivered",
        ),
        # 16-17. A refund is only ever the pre-send escape hatch. Once a message
        # may be in a customer's hands the money stays where it is.
        CheckConstraint(
            "refund_claimed_at IS NULL OR ("
            "provider_accepted_at IS NULL "
            "AND delivered_at IS NULL "
            "AND read_at IS NULL "
            "AND send_attempted_at IS NULL)",
            name="ck_ew_manual_voucher_refund_is_pre_send",
        ),
        CheckConstraint(
            f"status <> '{MANUAL_VOUCHER_REFUNDED}' OR ("
            "provider_accepted_at IS NULL AND delivered_at IS NULL AND read_at IS NULL)",
            name="ck_ew_manual_voucher_refunded_never_sent",
        ),
        # 18. At most one delivery attempt in the lifetime of this row. Not a
        # retry budget: a counter that can only be zero or one.
        CheckConstraint(
            "send_attempt_count >= 0 AND send_attempt_count <= 1",
            name="ck_ew_manual_voucher_single_attempt",
        ),
        CheckConstraint(
            "(send_attempt_count = 0) = (send_attempted_at IS NULL)",
            name="ck_ew_manual_voucher_attempt_count_matches",
        ),
        # 19. A MAC without the key that made it cannot be verified later.
        CheckConstraint(
            "(voucher_code_hmac IS NULL) = (hmac_key_id IS NULL)",
            name="ck_ew_manual_voucher_hmac_pair",
        ),
        Index("ix_ew_manual_voucher_status", "status"),
        Index("ix_ew_manual_voucher_recipient", "campaign_recipient_id"),
        Index("ix_ew_manual_voucher_run", "campaign_run_id"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    # -- identity ----------------------------------------------------------
    canary_scope: Mapped[str] = mapped_column(String(128), nullable=False)
    request_schema_version: Mapped[str] = mapped_column(String(16), nullable=False)
    # WHICH approved baseline this row was planned against. A later canary with
    # a different service count is a different configuration, and a row that
    # cannot say which one it trusted could not be audited afterwards.
    baseline_version: Mapped[str] = mapped_column(String(32), nullable=False)
    provider: Mapped[str] = _provider_column()
    company_id: Mapped[int] = mapped_column(Integer, nullable=False)
    campaign_code: Mapped[str] = mapped_column(String(128), nullable=False)
    recipient_basis: Mapped[str] = mapped_column(String(32), nullable=False)

    # The exact preview recipient an operator chose, and the run it came from.
    campaign_run_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    campaign_recipient_id: Mapped[int] = mapped_column(BigInteger, nullable=False)

    # The person, and the wave they were selected for. Together with the
    # campaign these are the entitlement key: a manual selection has no booking.
    easyweek_customer_uuid: Mapped[uuid.UUID] = mapped_column(PostgresUUID(as_uuid=True), nullable=False)
    campaign_period_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    campaign_period_end: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    # -- the frozen EasyWeek identity this canary may act on ----------------
    location_uuid: Mapped[uuid.UUID] = mapped_column(PostgresUUID(as_uuid=True), nullable=False)
    staffer_uuid: Mapped[uuid.UUID] = mapped_column(PostgresUUID(as_uuid=True), nullable=False)
    payment_account_uuid: Mapped[uuid.UUID] = mapped_column(PostgresUUID(as_uuid=True), nullable=False)
    voucher_template_uuid: Mapped[uuid.UUID] = mapped_column(PostgresUUID(as_uuid=True), nullable=False)
    # Recomputable, non-personal, and findable in the EasyWeek dashboard.
    reconciliation_marker: Mapped[str] = mapped_column(String(64), nullable=False)

    # -- results -----------------------------------------------------------
    target_order_uuid: Mapped[uuid.UUID | None] = mapped_column(PostgresUUID(as_uuid=True), nullable=True)
    outbound_intent_uuid: Mapped[uuid.UUID | None] = mapped_column(PostgresUUID(as_uuid=True), nullable=True)
    provider_message_id: Mapped[str | None] = mapped_column(String(128), nullable=True)

    # -- the voucher, as a keyed proof and nothing else ---------------------
    voucher_code_hmac: Mapped[str | None] = mapped_column(String(64), nullable=True)
    hmac_key_id: Mapped[str | None] = mapped_column(String(64), nullable=True)

    # -- state -------------------------------------------------------------
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    reason_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    manual_cleanup_required: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    reconciliation_required: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    manual_cleanup_observed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # -- operator authorisation provenance ---------------------------------
    create_plan_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)
    pay_plan_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)
    deliver_plan_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)
    refund_plan_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)

    # -- claim / attempt / verification, per stage -------------------------
    create_claimed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    create_attempted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    create_verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    create_window_start: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    create_window_end: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    pay_claimed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    pay_attempted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    pay_verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # The live guard taken immediately before sending, after the payment.
    live_guard_reproven_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    send_claimed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    send_attempted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    send_attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    provider_accepted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    read_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    refund_claimed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    refund_attempted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    refund_verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # -- safe evidence -----------------------------------------------------
    # Shape facts, proof labels and reason codes. Never a value, never a body,
    # never a template parameter, never the code.
    evidence: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )


class EasyWeekManualVoucherDeliveryAttempt(Base):
    """One redacted outbound intent, written and committed BEFORE the send.

    Its own table rather than an ``OutboxMessage``, for the same reason as §36:
    the outbox is swept by a generic worker whose purpose is to retry what it
    finds, and the one property this row must have is that nothing may ever pick
    it up and send it again. A separate table the worker has never heard of
    needs no proof of that.

    It stores no message. No rendered body, no parameter list, no voucher code —
    so nothing downstream could re-render the message even if it tried.
    """

    __tablename__ = "easyweek_manual_voucher_delivery_attempts"

    __table_args__ = (
        UniqueConstraint("intent_uuid", name="uq_ew_manual_voucher_attempt_intent"),
        ForeignKeyConstraint(
            ["ledger_id"],
            ["easyweek_manual_voucher_delivery_ledger.id"],
            name="fk_ew_manual_voucher_attempt_ledger",
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            "outcome IN ('claimed', 'provider_accepted', 'unknown', 'rejected')",
            name="ck_ew_manual_voucher_attempt_outcome",
        ),
        CheckConstraint(
            "outcome <> 'provider_accepted' OR provider_message_id IS NOT NULL",
            name="ck_ew_manual_voucher_attempt_accepted_has_id",
        ),
        Index("ix_ew_manual_voucher_attempt_ledger", "ledger_id"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    ledger_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    intent_uuid: Mapped[uuid.UUID] = mapped_column(PostgresUUID(as_uuid=True), nullable=False)

    # Enough to audit WHICH approved template was used, and nothing about what
    # it said to whom.
    template_code: Mapped[str] = mapped_column(String(64), nullable=False)
    meta_template_name: Mapped[str] = mapped_column(String(128), nullable=False)
    template_language: Mapped[str] = mapped_column(String(8), nullable=False)
    sender_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    campaign_recipient_id: Mapped[int] = mapped_column(BigInteger, nullable=False)

    outcome: Mapped[str] = mapped_column(String(32), nullable=False)
    reason_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    provider_message_id: Mapped[str | None] = mapped_column(String(128), nullable=True)

    claimed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


# ---------------------------------------------------------------------------
# §41: controlled EasyWeek voucher snapshot BATCH (PR-18)
# ---------------------------------------------------------------------------
# §37.2 proved the whole irreversible sequence — issue, pay, deliver, observe —
# for exactly one manually selected person. This phase repeats it for a bounded
# handful of them, and the only new question it has to answer is *how many*.
#
# The answer is the schema's, not the code's. A batch names its own size once,
# at freeze, and every row below is tied to that number: a sixth recipient has
# no slot to occupy, no foreign key to satisfy and no arithmetic that adds up.
# Five is the cap the owner approved and €75 is the exposure that follows from
# it, so both are literals a reviewer sees in a diff rather than configuration
# an environment could raise.
VOUCHER_BATCH_SCOPE = "easyweek_voucher_snapshot_batch_v1"
VOUCHER_BATCH_SCHEMA_VERSION = "1"

# The hard ceiling, in three places that must agree: the slot range, the
# recipient count and the money. None of them is configurable, here or anywhere.
VOUCHER_BATCH_MAX_RECIPIENTS = 5
VOUCHER_BATCH_UNIT_PRICE_MINOR = 1500
VOUCHER_BATCH_MAX_EXPOSURE_MINOR = VOUCHER_BATCH_MAX_RECIPIENTS * VOUCHER_BATCH_UNIT_PRICE_MINOR

# -- the batch header's own lifecycle ---------------------------------------
# Deliberately short. The header answers "may another stage start?", and the
# per-item ladder below answers everything else.
VOUCHER_BATCH_FROZEN = "frozen"
VOUCHER_BATCH_IN_PROGRESS = "in_progress"
VOUCHER_BATCH_HALTED = "halted"
VOUCHER_BATCH_COMPLETED = "completed"

VOUCHER_BATCH_STATUSES = (
    VOUCHER_BATCH_FROZEN,
    VOUCHER_BATCH_IN_PROGRESS,
    VOUCHER_BATCH_HALTED,
    VOUCHER_BATCH_COMPLETED,
)

_VOUCHER_BATCH_STATUS_SQL = ", ".join(f"'{value}'" for value in VOUCHER_BATCH_STATUSES)

# -- one item's ladder -------------------------------------------------------
# The same vocabulary as §§36 and 37.2, and repeated rather than imported for
# the same reason: these are durable strings in a database, and a future edit to
# one phase's states must not silently redefine another's.
VOUCHER_BATCH_ITEM_PLANNED = "planned"
VOUCHER_BATCH_ITEM_CREATE_CLAIMED = "create_claimed"
VOUCHER_BATCH_ITEM_CREATE_UNKNOWN = "create_unknown"
VOUCHER_BATCH_ITEM_CREATE_REJECTED = "create_rejected"
VOUCHER_BATCH_ITEM_CREATED = "created"
VOUCHER_BATCH_ITEM_PAY_CLAIMED = "pay_claimed"
VOUCHER_BATCH_ITEM_PAY_UNKNOWN = "pay_unknown"
VOUCHER_BATCH_ITEM_PAY_REJECTED = "pay_rejected"
VOUCHER_BATCH_ITEM_PAID = "paid"
VOUCHER_BATCH_ITEM_SEND_CLAIMED = "send_claimed"
VOUCHER_BATCH_ITEM_SEND_UNKNOWN = "send_unknown"
VOUCHER_BATCH_ITEM_SEND_REJECTED = "send_rejected"
VOUCHER_BATCH_ITEM_PROVIDER_ACCEPTED = "provider_accepted"
VOUCHER_BATCH_ITEM_DELIVERED = "delivered"
VOUCHER_BATCH_ITEM_READ = "read"
VOUCHER_BATCH_ITEM_REFUND_CLAIMED = "refund_claimed"
VOUCHER_BATCH_ITEM_REFUND_UNKNOWN = "refund_unknown"
VOUCHER_BATCH_ITEM_REFUND_REJECTED = "refund_rejected"
VOUCHER_BATCH_ITEM_REFUNDED = "refunded"
VOUCHER_BATCH_ITEM_MANUALLY_CLEANED = "manually_cleaned"
VOUCHER_BATCH_ITEM_AMBIGUOUS = "ambiguous"

VOUCHER_BATCH_ITEM_STATUSES = (
    VOUCHER_BATCH_ITEM_PLANNED,
    VOUCHER_BATCH_ITEM_CREATE_CLAIMED,
    VOUCHER_BATCH_ITEM_CREATE_UNKNOWN,
    VOUCHER_BATCH_ITEM_CREATE_REJECTED,
    VOUCHER_BATCH_ITEM_CREATED,
    VOUCHER_BATCH_ITEM_PAY_CLAIMED,
    VOUCHER_BATCH_ITEM_PAY_UNKNOWN,
    VOUCHER_BATCH_ITEM_PAY_REJECTED,
    VOUCHER_BATCH_ITEM_PAID,
    VOUCHER_BATCH_ITEM_SEND_CLAIMED,
    VOUCHER_BATCH_ITEM_SEND_UNKNOWN,
    VOUCHER_BATCH_ITEM_SEND_REJECTED,
    VOUCHER_BATCH_ITEM_PROVIDER_ACCEPTED,
    VOUCHER_BATCH_ITEM_DELIVERED,
    VOUCHER_BATCH_ITEM_READ,
    VOUCHER_BATCH_ITEM_REFUND_CLAIMED,
    VOUCHER_BATCH_ITEM_REFUND_UNKNOWN,
    VOUCHER_BATCH_ITEM_REFUND_REJECTED,
    VOUCHER_BATCH_ITEM_REFUNDED,
    VOUCHER_BATCH_ITEM_MANUALLY_CLEANED,
    VOUCHER_BATCH_ITEM_AMBIGUOUS,
)

_VOUCHER_BATCH_ITEM_STATUS_SQL = ", ".join(f"'{value}'" for value in VOUCHER_BATCH_ITEM_STATUSES)

# The one branch, the one campaign and the one basis this batch may ever name.
VOUCHER_BATCH_COMPANY_ID = 322579
VOUCHER_BATCH_CAMPAIGN_CODE = "new_clients_monthly"


class EasyWeekVoucherSnapshotBatch(Base):
    """The one frozen batch header of the controlled snapshot batch (§41).

    One row, ever
    -------------
    ``batch_scope`` is both unique AND pinned to a single literal by a CHECK, so
    this table can physically hold at most one row. A second batch is therefore
    not a flag somebody flips twice — it is a code change plus a migration plus
    a review, which is exactly the ceremony the owner asked for.

    The size is named once and then binds everything
    ------------------------------------------------
    ``recipient_count`` is written at freeze and never changes. Items reference
    it through a composite foreign key and compare their own slot against it, so
    "the sixth recipient" is not a case this schema rejects — it is a row that
    cannot be written at all.

    The money follows from the count
    --------------------------------
    ``total_exposure_minor`` is not a number somebody types: a CHECK requires it
    to equal ``voucher_unit_price_minor * recipient_count``, the unit price is
    pinned to 1500, and the count is capped at five. €75 is therefore the
    arithmetic ceiling of this table rather than a promise in a document.

    What is NOT here
    ----------------
    No phone, no name, no customer-facing text and no voucher code. The frozen
    digest below is a hash over the composition, so an operator can prove the
    batch they approved is the batch that ran without the header carrying any of
    the identities it is about.
    """

    __tablename__ = "easyweek_voucher_snapshot_batches"

    __table_args__ = (
        # 1-2. One batch, ever — unique AND pinned to the single approved scope.
        UniqueConstraint("batch_scope", name="uq_ew_voucher_batch_scope"),
        CheckConstraint(
            f"batch_scope = '{VOUCHER_BATCH_SCOPE}'",
            name="ck_ew_voucher_batch_single_scope",
        ),
        # 3. The FK target items use to prove their slot is inside this batch's
        # own declared size. Redundant next to the primary key, and load bearing:
        # without it the composite foreign key below cannot exist.
        UniqueConstraint("id", "recipient_count", name="uq_ew_voucher_batch_id_count"),
        # 4. The preview this batch was frozen from, under the same provider.
        ForeignKeyConstraint(
            ["campaign_run_id", "provider"],
            ["campaign_runs.id", "campaign_runs.provider"],
            name="fk_ew_voucher_batch_run_provider",
            ondelete="RESTRICT",
        ),
        # 5-8. The topology the owner approved, as literals rather than as
        # configuration a misconfigured environment could point elsewhere.
        CheckConstraint("provider = 'easyweek'", name="ck_ew_voucher_batch_provider"),
        CheckConstraint(
            f"company_id = {VOUCHER_BATCH_COMPANY_ID}",
            name="ck_ew_voucher_batch_company",
        ),
        CheckConstraint(
            f"campaign_code = '{VOUCHER_BATCH_CAMPAIGN_CODE}'",
            name="ck_ew_voucher_batch_campaign",
        ),
        CheckConstraint(
            f"recipient_basis = '{RECIPIENT_BASIS_MANUAL}'",
            name="ck_ew_voucher_batch_basis",
        ),
        # 9. One to five. The sixth slot has nowhere to go.
        CheckConstraint(
            f"recipient_count >= 1 AND recipient_count <= {VOUCHER_BATCH_MAX_RECIPIENTS}",
            name="ck_ew_voucher_batch_recipient_count",
        ),
        # 10-11. The money, derived rather than asserted. €15 each, and a total
        # that must be exactly the product — so €75 is the ceiling of the table.
        CheckConstraint(
            f"voucher_unit_price_minor = {VOUCHER_BATCH_UNIT_PRICE_MINOR}",
            name="ck_ew_voucher_batch_unit_price",
        ),
        CheckConstraint(
            "total_exposure_minor = voucher_unit_price_minor * recipient_count",
            name="ck_ew_voucher_batch_exposure_matches",
        ),
        # 12. A campaign period is an interval; an entitlement key built on a
        # backwards one would be meaningless.
        CheckConstraint(
            "campaign_period_start < campaign_period_end",
            name="ck_ew_voucher_batch_period_order",
        ),
        # 13. A closed status vocabulary.
        CheckConstraint(
            f"status IN ({_VOUCHER_BATCH_STATUS_SQL})",
            name="ck_ew_voucher_batch_status",
        ),
        # 14. A halt must say why, and only a halt may.
        CheckConstraint(
            f"(status = '{VOUCHER_BATCH_HALTED}') = (halted_reason_code IS NOT NULL)",
            name="ck_ew_voucher_batch_halt_has_reason",
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    # -- identity ----------------------------------------------------------
    batch_scope: Mapped[str] = mapped_column(String(128), nullable=False)
    request_schema_version: Mapped[str] = mapped_column(String(16), nullable=False)
    baseline_version: Mapped[str] = mapped_column(String(32), nullable=False)
    provider: Mapped[str] = _provider_column()
    company_id: Mapped[int] = mapped_column(Integer, nullable=False)
    campaign_code: Mapped[str] = mapped_column(String(128), nullable=False)
    recipient_basis: Mapped[str] = mapped_column(String(32), nullable=False)

    # The exact completed preview this composition was frozen from.
    campaign_run_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    campaign_period_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    campaign_period_end: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    # -- the frozen EasyWeek identity this batch may act on -----------------
    location_uuid: Mapped[uuid.UUID] = mapped_column(PostgresUUID(as_uuid=True), nullable=False)
    staffer_uuid: Mapped[uuid.UUID] = mapped_column(PostgresUUID(as_uuid=True), nullable=False)
    payment_account_uuid: Mapped[uuid.UUID] = mapped_column(PostgresUUID(as_uuid=True), nullable=False)
    voucher_template_uuid: Mapped[uuid.UUID] = mapped_column(PostgresUUID(as_uuid=True), nullable=False)

    # -- the frozen composition --------------------------------------------
    # A hash over the ordered slots, their recipients and the money. It carries
    # no identity of its own, so it can be printed in a report and pasted into a
    # ticket, and it is what an operator compares before every later stage.
    frozen_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    recipient_count: Mapped[int] = mapped_column(Integer, nullable=False)
    voucher_unit_price_minor: Mapped[int] = mapped_column(Integer, nullable=False)
    total_exposure_minor: Mapped[int] = mapped_column(Integer, nullable=False)

    # -- state -------------------------------------------------------------
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    halted_reason_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    reconciliation_required: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))

    # -- operator authorisation provenance ---------------------------------
    freeze_plan_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)

    frozen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    halted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # Shape facts, proof labels and reason codes. Never a value, never a body.
    evidence: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )


class EasyWeekVoucherSnapshotBatchItem(Base):
    """One slot of the frozen batch: one person, one €15 voucher, one message.

    Its slot cannot escape the batch
    --------------------------------
    ``batch_recipient_count`` is not a convenience copy. It is half of a
    composite foreign key into ``(id, recipient_count)`` of the header, so the
    database itself knows what this batch's declared size is, and a CHECK then
    requires ``1 <= slot <= batch_recipient_count <= 5``. Together with the
    uniqueness of ``(batch_id, slot)`` that makes a sixth item unrepresentable
    rather than merely refused.

    One person, once
    ----------------
    Three separate uniqueness rules, for three different mistakes:

    * ``(batch_id, campaign_recipient_id)`` — the same preview row twice;
    * ``(batch_id, easyweek_customer_uuid)`` — two preview rows, one human;
    * the entitlement key — provider, company, campaign, customer and both
      period bounds — which is table-wide, so a later batch (were one ever
      approved) could not hand the same person a second €15 for the same wave.

    Nothing here is the voucher
    ---------------------------
    The code is a bearer secret and is never a column: what is stored is a keyed
    MAC bound to this row and this order, plus the id of the key that made it.
    """

    __tablename__ = "easyweek_voucher_snapshot_batch_items"

    __table_args__ = (
        # 1. The slot belongs to this batch and to this batch's declared size.
        ForeignKeyConstraint(
            ["batch_id", "batch_recipient_count"],
            [
                "easyweek_voucher_snapshot_batches.id",
                "easyweek_voucher_snapshot_batches.recipient_count",
            ],
            name="fk_ew_voucher_batch_item_batch_size",
            ondelete="RESTRICT",
        ),
        # 2. 1 <= slot <= the batch's own size <= 5. The sixth slot has no home.
        CheckConstraint(
            "slot >= 1 AND slot <= batch_recipient_count "
            f"AND batch_recipient_count >= 1 AND batch_recipient_count <= {VOUCHER_BATCH_MAX_RECIPIENTS}",
            name="ck_ew_voucher_batch_item_slot_range",
        ),
        # 3-5. Three uniqueness rules for three different mistakes.
        UniqueConstraint("batch_id", "slot", name="uq_ew_voucher_batch_item_slot"),
        UniqueConstraint("batch_id", "campaign_recipient_id", name="uq_ew_voucher_batch_item_recipient"),
        UniqueConstraint("batch_id", "easyweek_customer_uuid", name="uq_ew_voucher_batch_item_customer"),
        # 6. One voucher per person per campaign period, table-wide.
        UniqueConstraint(
            "provider",
            "company_id",
            "campaign_code",
            "easyweek_customer_uuid",
            "campaign_period_start",
            "campaign_period_end",
            name="uq_ew_voucher_batch_item_entitlement",
        ),
        # 7-10. A result may belong to exactly one slot.
        UniqueConstraint("reconciliation_marker", name="uq_ew_voucher_batch_item_marker"),
        UniqueConstraint("target_order_uuid", name="uq_ew_voucher_batch_item_target_order"),
        UniqueConstraint("outbound_intent_uuid", name="uq_ew_voucher_batch_item_intent"),
        UniqueConstraint("provider_message_id", name="uq_ew_voucher_batch_item_provider_message"),
        # 11. The recipient must belong to the run this row names, under the
        # same provider. A composite FK, because two separate ones would each be
        # satisfied by rows that have nothing to do with each other.
        ForeignKeyConstraint(
            ["campaign_recipient_id", "provider"],
            ["campaign_recipients.id", "campaign_recipients.provider"],
            name="fk_ew_voucher_batch_item_recipient_provider",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["campaign_run_id", "provider"],
            ["campaign_runs.id", "campaign_runs.provider"],
            name="fk_ew_voucher_batch_item_run_provider",
            ondelete="RESTRICT",
        ),
        # 12-15. The approved topology and a closed status vocabulary.
        CheckConstraint(
            f"status IN ({_VOUCHER_BATCH_ITEM_STATUS_SQL})",
            name="ck_ew_voucher_batch_item_status",
        ),
        CheckConstraint("provider = 'easyweek'", name="ck_ew_voucher_batch_item_provider"),
        CheckConstraint(
            f"company_id = {VOUCHER_BATCH_COMPANY_ID}",
            name="ck_ew_voucher_batch_item_company",
        ),
        CheckConstraint(
            f"campaign_code = '{VOUCHER_BATCH_CAMPAIGN_CODE}'",
            name="ck_ew_voucher_batch_item_campaign",
        ),
        CheckConstraint(
            f"recipient_basis = '{RECIPIENT_BASIS_MANUAL}'",
            name="ck_ew_voucher_batch_item_basis",
        ),
        # 16. Exactly one voucher of exactly €15. Not a default and not a
        # maximum: the two numbers this slot is allowed to be worth.
        CheckConstraint(
            f"voucher_value_minor = {VOUCHER_BATCH_UNIT_PRICE_MINOR} AND voucher_quantity = 1",
            name="ck_ew_voucher_batch_item_exact_voucher",
        ),
        CheckConstraint(
            "campaign_period_start < campaign_period_end",
            name="ck_ew_voucher_batch_item_period_order",
        ),
        # 17. Stage order. A stage cannot be attempted before it was claimed,
        # nor verified before it was attempted.
        CheckConstraint(
            "(create_attempted_at IS NULL OR create_claimed_at IS NOT NULL) "
            "AND (create_verified_at IS NULL OR create_attempted_at IS NOT NULL) "
            "AND (pay_attempted_at IS NULL OR pay_claimed_at IS NOT NULL) "
            "AND (pay_verified_at IS NULL OR pay_attempted_at IS NOT NULL) "
            "AND (send_attempted_at IS NULL OR send_claimed_at IS NOT NULL) "
            "AND (refund_attempted_at IS NULL OR refund_claimed_at IS NOT NULL) "
            "AND (refund_verified_at IS NULL OR refund_attempted_at IS NOT NULL)",
            name="ck_ew_voucher_batch_item_stage_order",
        ),
        # 18. Money cannot move before the order it pays for was proven to exist.
        CheckConstraint(
            "pay_claimed_at IS NULL OR (create_verified_at IS NOT NULL AND target_order_uuid IS NOT NULL)",
            name="ck_ew_voucher_batch_item_pay_needs_created",
        ),
        # 19. Nothing may be sent before the voucher is proven paid for, bound to
        # this row by MAC, and proven still deliverable by a guard taken AFTER
        # the payment. A guard from before the payment says nothing about now.
        CheckConstraint(
            "send_claimed_at IS NULL OR ("
            "pay_verified_at IS NOT NULL "
            "AND voucher_code_hmac IS NOT NULL "
            "AND hmac_key_id IS NOT NULL "
            "AND live_guard_reproven_at IS NOT NULL "
            "AND live_guard_reproven_at >= pay_verified_at)",
            name="ck_ew_voucher_batch_item_send_needs_paid",
        ),
        # 20-22. Acceptance needs both the attempt and Meta's identifier; the
        # webhook ladder may not be climbed out of order.
        CheckConstraint(
            "provider_accepted_at IS NULL OR (send_attempted_at IS NOT NULL AND provider_message_id IS NOT NULL)",
            name="ck_ew_voucher_batch_item_accepted_needs_attempt",
        ),
        CheckConstraint(
            "delivered_at IS NULL OR provider_accepted_at IS NOT NULL",
            name="ck_ew_voucher_batch_item_delivered_needs_accepted",
        ),
        CheckConstraint(
            "read_at IS NULL OR delivered_at IS NOT NULL",
            name="ck_ew_voucher_batch_item_read_needs_delivered",
        ),
        # 23-24. A refund is only ever the pre-send escape hatch.
        CheckConstraint(
            "refund_claimed_at IS NULL OR ("
            "provider_accepted_at IS NULL "
            "AND delivered_at IS NULL "
            "AND read_at IS NULL "
            "AND send_claimed_at IS NULL "
            "AND send_attempted_at IS NULL)",
            name="ck_ew_voucher_batch_item_refund_is_pre_send",
        ),
        CheckConstraint(
            f"status <> '{VOUCHER_BATCH_ITEM_REFUNDED}' OR ("
            "provider_accepted_at IS NULL AND delivered_at IS NULL AND read_at IS NULL)",
            name="ck_ew_voucher_batch_item_refunded_never_sent",
        ),
        # 25-26. At most one delivery attempt in the lifetime of this slot. Not
        # a retry budget: a counter that can only be zero or one.
        CheckConstraint(
            "send_attempt_count >= 0 AND send_attempt_count <= 1",
            name="ck_ew_voucher_batch_item_single_attempt",
        ),
        CheckConstraint(
            "(send_attempt_count = 0) = (send_attempted_at IS NULL)",
            name="ck_ew_voucher_batch_item_attempt_count_matches",
        ),
        # 27. A MAC without the key that made it cannot be verified later.
        CheckConstraint(
            "(voucher_code_hmac IS NULL) = (hmac_key_id IS NULL)",
            name="ck_ew_voucher_batch_item_hmac_pair",
        ),
        Index("ix_ew_voucher_batch_item_batch", "batch_id"),
        Index("ix_ew_voucher_batch_item_status", "status"),
        Index("ix_ew_voucher_batch_item_run", "campaign_run_id"),
        Index("ix_ew_voucher_batch_item_recipient", "campaign_recipient_id"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    # -- membership --------------------------------------------------------
    batch_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    # The batch's declared size, carried here so the foreign key above can
    # anchor this slot to it. Never edited independently: the composite FK makes
    # a disagreement unrepresentable.
    batch_recipient_count: Mapped[int] = mapped_column(Integer, nullable=False)
    slot: Mapped[int] = mapped_column(Integer, nullable=False)

    # -- identity ----------------------------------------------------------
    provider: Mapped[str] = _provider_column()
    company_id: Mapped[int] = mapped_column(Integer, nullable=False)
    campaign_code: Mapped[str] = mapped_column(String(128), nullable=False)
    recipient_basis: Mapped[str] = mapped_column(String(32), nullable=False)
    campaign_run_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    campaign_recipient_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    easyweek_customer_uuid: Mapped[uuid.UUID] = mapped_column(PostgresUUID(as_uuid=True), nullable=False)
    campaign_period_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    campaign_period_end: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    # -- the exact sale this slot authorises -------------------------------
    voucher_value_minor: Mapped[int] = mapped_column(Integer, nullable=False)
    voucher_quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    # Recomputable, non-personal, and findable in the EasyWeek dashboard.
    reconciliation_marker: Mapped[str] = mapped_column(String(64), nullable=False)

    # -- results -----------------------------------------------------------
    target_order_uuid: Mapped[uuid.UUID | None] = mapped_column(PostgresUUID(as_uuid=True), nullable=True)
    outbound_intent_uuid: Mapped[uuid.UUID | None] = mapped_column(PostgresUUID(as_uuid=True), nullable=True)
    provider_message_id: Mapped[str | None] = mapped_column(String(128), nullable=True)

    # -- the voucher, as a keyed proof and nothing else ---------------------
    voucher_code_hmac: Mapped[str | None] = mapped_column(String(64), nullable=True)
    hmac_key_id: Mapped[str | None] = mapped_column(String(64), nullable=True)

    # -- state -------------------------------------------------------------
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    reason_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    manual_cleanup_required: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    reconciliation_required: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    manual_cleanup_observed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # -- operator authorisation provenance ---------------------------------
    create_plan_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)
    pay_plan_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)
    deliver_plan_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)
    refund_plan_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)

    # -- claim / attempt / verification, per stage -------------------------
    create_claimed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    create_attempted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    create_verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    create_window_start: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    create_window_end: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    pay_claimed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    pay_attempted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    pay_verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    live_guard_reproven_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    send_claimed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    send_attempted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    send_attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    provider_accepted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    read_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    refund_claimed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    refund_attempted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    refund_verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # -- safe evidence -----------------------------------------------------
    evidence: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )


class EasyWeekVoucherSnapshotBatchAttempt(Base):
    """One redacted outbound intent, written and committed BEFORE a send.

    Its own table rather than an ``OutboxMessage``, for the same reason as §§36
    and 37.2: the outbox is swept by a generic worker whose purpose is to retry
    what it finds, and the one property this row must have is that nothing may
    ever pick it up and send it again.

    It stores no message. No rendered body, no parameter list, no voucher code,
    no phone number and no name — so nothing downstream could re-render the
    message even if it tried.
    """

    __tablename__ = "easyweek_voucher_snapshot_batch_attempts"

    __table_args__ = (
        UniqueConstraint("intent_uuid", name="uq_ew_voucher_batch_attempt_intent"),
        ForeignKeyConstraint(
            ["item_id"],
            ["easyweek_voucher_snapshot_batch_items.id"],
            name="fk_ew_voucher_batch_attempt_item",
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            "outcome IN ('claimed', 'provider_accepted', 'unknown', 'rejected')",
            name="ck_ew_voucher_batch_attempt_outcome",
        ),
        CheckConstraint(
            "outcome <> 'provider_accepted' OR provider_message_id IS NOT NULL",
            name="ck_ew_voucher_batch_attempt_accepted_has_id",
        ),
        Index("ix_ew_voucher_batch_attempt_item", "item_id"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    item_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    intent_uuid: Mapped[uuid.UUID] = mapped_column(PostgresUUID(as_uuid=True), nullable=False)

    # Enough to audit WHICH approved template was used, and nothing about what
    # it said to whom.
    template_code: Mapped[str] = mapped_column(String(64), nullable=False)
    meta_template_name: Mapped[str] = mapped_column(String(128), nullable=False)
    template_language: Mapped[str] = mapped_column(String(8), nullable=False)
    sender_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    campaign_recipient_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    slot: Mapped[int] = mapped_column(Integer, nullable=False)

    outcome: Mapped[str] = mapped_column(String(32), nullable=False)
    reason_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    provider_message_id: Mapped[str | None] = mapped_column(String(128), nullable=True)

    claimed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


# ---------------------------------------------------------------------------
# §42: production manual-snapshot voucher MAILING (PR-19)
# ---------------------------------------------------------------------------
# §41 proved the whole irreversible sequence — issue, pay, deliver, observe —
# for a bounded handful of manually selected people, in production, on
# 28.09.2026. This phase is the working mode: a real operator-curated list, of
# whatever size that list honestly is, mailed one confirmed stage at a time.
#
# What changes from §41, and what deliberately does not
# -----------------------------------------------------
# §41 is a SINGLETON by construction: its scope is pinned to one literal, so
# its table physically holds one row. That was the right shape for a controlled
# experiment and it is the wrong shape for the working mode, which has to run
# again next month. So this phase gets its own three tables and its own
# identity, and §41's tables — rows, constraints and historical HMAC bindings
# alike — are left exactly as they are.
#
# What does NOT change is every safety property that made §41 trustworthy: the
# claim is committed before the request leaves, a stage cannot be climbed out of
# order, a delivery has one lifetime attempt, a refund is pre-send only, and one
# person gets one voucher per campaign period.
#
# The size is the operator's, and the schema makes them say it
# ------------------------------------------------------------
# §41's ceiling was five recipients and €75, as literals. Carrying that into
# production would be wrong, and replacing it with an invented number — ten,
# fifty, a hundred — would be no better: it would be this schema deciding how
# many real customers a real campaign may have.
#
# So there is no ceiling here. What there is instead is an ARITHMETIC IDENTITY
# the operator has to state up front and the database then refuses to let drift:
# the count they approved must equal the count that was frozen, and the money
# they approved must equal the product of that count and €15. A batch whose
# approved numbers do not describe its own composition is not a batch this
# schema can store.
VOUCHER_PRODUCTION_SCOPE = "easyweek_voucher_production_mailing_v1"
VOUCHER_PRODUCTION_SCHEMA_VERSION = "2"

# €15 per recipient, exactly. Not a default and not a maximum: the one value a
# slot of this phase is allowed to be worth.
VOUCHER_PRODUCTION_UNIT_PRICE_MINOR = 1500

# The one branch, the one campaign and the one basis this phase may ever name.
VOUCHER_PRODUCTION_COMPANY_ID = 322579
VOUCHER_PRODUCTION_CAMPAIGN_CODE = "new_clients_monthly"

# -- the batch header's own lifecycle ---------------------------------------
VOUCHER_PRODUCTION_FROZEN = "frozen"
VOUCHER_PRODUCTION_IN_PROGRESS = "in_progress"
VOUCHER_PRODUCTION_HALTED = "halted"
VOUCHER_PRODUCTION_COMPLETED = "completed"

VOUCHER_PRODUCTION_STATUSES = (
    VOUCHER_PRODUCTION_FROZEN,
    VOUCHER_PRODUCTION_IN_PROGRESS,
    VOUCHER_PRODUCTION_HALTED,
    VOUCHER_PRODUCTION_COMPLETED,
)

_VOUCHER_PRODUCTION_STATUS_SQL = ", ".join(f"'{value}'" for value in VOUCHER_PRODUCTION_STATUSES)

# -- one item's ladder -------------------------------------------------------
# The same vocabulary as §§36, 37.2 and 41, and repeated rather than imported
# for the same reason: these are durable strings in a database, and a future
# edit to one phase's states must not silently redefine another's.
VOUCHER_PRODUCTION_ITEM_PLANNED = "planned"
VOUCHER_PRODUCTION_ITEM_CREATE_CLAIMED = "create_claimed"
VOUCHER_PRODUCTION_ITEM_CREATE_UNKNOWN = "create_unknown"
VOUCHER_PRODUCTION_ITEM_CREATE_REJECTED = "create_rejected"
VOUCHER_PRODUCTION_ITEM_CREATED = "created"
VOUCHER_PRODUCTION_ITEM_PAY_CLAIMED = "pay_claimed"
VOUCHER_PRODUCTION_ITEM_PAY_UNKNOWN = "pay_unknown"
VOUCHER_PRODUCTION_ITEM_PAY_REJECTED = "pay_rejected"
VOUCHER_PRODUCTION_ITEM_PAID = "paid"
VOUCHER_PRODUCTION_ITEM_SEND_CLAIMED = "send_claimed"
VOUCHER_PRODUCTION_ITEM_SEND_UNKNOWN = "send_unknown"
VOUCHER_PRODUCTION_ITEM_SEND_REJECTED = "send_rejected"
VOUCHER_PRODUCTION_ITEM_PROVIDER_ACCEPTED = "provider_accepted"
VOUCHER_PRODUCTION_ITEM_DELIVERED = "delivered"
VOUCHER_PRODUCTION_ITEM_READ = "read"
VOUCHER_PRODUCTION_ITEM_REFUND_CLAIMED = "refund_claimed"
VOUCHER_PRODUCTION_ITEM_REFUND_UNKNOWN = "refund_unknown"
VOUCHER_PRODUCTION_ITEM_REFUND_REJECTED = "refund_rejected"
VOUCHER_PRODUCTION_ITEM_REFUNDED = "refunded"
VOUCHER_PRODUCTION_ITEM_MANUALLY_CLEANED = "manually_cleaned"
VOUCHER_PRODUCTION_ITEM_AMBIGUOUS = "ambiguous"

VOUCHER_PRODUCTION_ITEM_STATUSES = (
    VOUCHER_PRODUCTION_ITEM_PLANNED,
    VOUCHER_PRODUCTION_ITEM_CREATE_CLAIMED,
    VOUCHER_PRODUCTION_ITEM_CREATE_UNKNOWN,
    VOUCHER_PRODUCTION_ITEM_CREATE_REJECTED,
    VOUCHER_PRODUCTION_ITEM_CREATED,
    VOUCHER_PRODUCTION_ITEM_PAY_CLAIMED,
    VOUCHER_PRODUCTION_ITEM_PAY_UNKNOWN,
    VOUCHER_PRODUCTION_ITEM_PAY_REJECTED,
    VOUCHER_PRODUCTION_ITEM_PAID,
    VOUCHER_PRODUCTION_ITEM_SEND_CLAIMED,
    VOUCHER_PRODUCTION_ITEM_SEND_UNKNOWN,
    VOUCHER_PRODUCTION_ITEM_SEND_REJECTED,
    VOUCHER_PRODUCTION_ITEM_PROVIDER_ACCEPTED,
    VOUCHER_PRODUCTION_ITEM_DELIVERED,
    VOUCHER_PRODUCTION_ITEM_READ,
    VOUCHER_PRODUCTION_ITEM_REFUND_CLAIMED,
    VOUCHER_PRODUCTION_ITEM_REFUND_UNKNOWN,
    VOUCHER_PRODUCTION_ITEM_REFUND_REJECTED,
    VOUCHER_PRODUCTION_ITEM_REFUNDED,
    VOUCHER_PRODUCTION_ITEM_MANUALLY_CLEANED,
    VOUCHER_PRODUCTION_ITEM_AMBIGUOUS,
)

_VOUCHER_PRODUCTION_ITEM_STATUS_SQL = ", ".join(f"'{value}'" for value in VOUCHER_PRODUCTION_ITEM_STATUSES)


class EasyWeekVoucherProductionBatch(Base):
    """One frozen production mailing batch (§42).

    Many batches, each addressed by its own id
    ------------------------------------------
    Unlike §41 this table holds more than one row, and that is the whole point
    of the phase: the working mode has to run again next month. What replaces
    §41's singleton constraint is a durable per-batch identity — the primary key
    — plus one rule that keeps the batches from overlapping: ``campaign_run_id``
    is UNIQUE, so one preview can be frozen into at most one batch, ever. There
    is no "the current batch" and no "the latest batch"; every stage after the
    freeze names the id it means.

    The approved numbers are checked against the composition, by the database
    ----------------------------------------------------------------------------
    Four columns and three CHECKs carry §42.5 in full:

    * ``approved_recipient_count`` is what the operator typed, and it must equal
      ``recipient_count``, which is what the freeze actually counted;
    * ``approved_exposure_minor`` is the money they approved, and it must equal
      ``total_exposure_minor``;
    * ``total_exposure_minor`` must itself equal
      ``voucher_unit_price_minor * recipient_count``, with the unit price pinned
      to the versioned product (€15 historically, €10 for schema 3).

    Storing the approved values rather than merely validating them is the point.
    A batch that has been frozen carries, forever, the two numbers a human
    agreed to — and the constraints mean those numbers cannot describe anything
    other than the batch's own composition.

    There is deliberately no upper bound on ``recipient_count``. §41's ceiling
    of five was right for an experiment; inventing a replacement ceiling here
    would be this table deciding how many real customers a real campaign may
    have. What is bounded is not the size but the operator's ignorance of it:
    they cannot freeze a batch without stating its size and its cost correctly.

    What is NOT here
    ----------------
    No phone, no name, no customer-facing text and no voucher code. The frozen
    digest is a hash over the composition, so an operator can prove the batch
    they approved is the batch that ran without the header carrying any of the
    identities it is about.
    """

    __tablename__ = "easyweek_voucher_production_batches"

    __table_args__ = (
        # 1. One production batch per preview, ever. This is what replaces
        # §41's singleton scope: batches are plural, previews are not reusable.
        UniqueConstraint("campaign_run_id", name="uq_ew_voucher_production_batch_run"),
        # 2. The FK target items use to prove their slot is inside this batch's
        # own declared size. Redundant next to the primary key, and load
        # bearing: without it the composite foreign key on items cannot exist.
        UniqueConstraint("id", "recipient_count", name="uq_ew_voucher_production_batch_id_count"),
        UniqueConstraint("id", "voucher_unit_price_minor", name="uq_ew_voucher_production_batch_id_price"),
        # 3. The preview this batch was frozen from, under the same provider.
        ForeignKeyConstraint(
            ["campaign_run_id", "provider"],
            ["campaign_runs.id", "campaign_runs.provider"],
            name="fk_ew_voucher_production_batch_run_provider",
            ondelete="RESTRICT",
        ),
        # 4-8. The topology the owner approved, as literals rather than as
        # configuration a misconfigured environment could point elsewhere.
        CheckConstraint("provider = 'easyweek'", name="ck_ew_voucher_production_batch_provider"),
        CheckConstraint(
            f"batch_scope = '{VOUCHER_PRODUCTION_SCOPE}'",
            name="ck_ew_voucher_production_batch_scope",
        ),
        CheckConstraint(
            f"company_id = {VOUCHER_PRODUCTION_COMPANY_ID}",
            name="ck_ew_voucher_production_batch_company",
        ),
        CheckConstraint(
            f"campaign_code = '{VOUCHER_PRODUCTION_CAMPAIGN_CODE}'",
            name="ck_ew_voucher_production_batch_campaign",
        ),
        CheckConstraint(
            "(request_schema_version = '1' AND recipient_basis = 'operator_manual_selection') OR "
            "(request_schema_version IN ('2', '3', '4') AND recipient_basis IN "
            "('earned_first_visit', 'operator_manual_selection', 'mixed'))",
            name="ck_ew_voucher_production_batch_basis",
        ),
        # 9. At least one recipient. An empty batch is not a small batch: there
        # is nothing to approve. Deliberately no upper bound — see the docstring.
        CheckConstraint("recipient_count >= 1", name="ck_ew_voucher_production_batch_recipient_count"),
        # 10-13. Product version and arithmetic the database will not let drift:
        # a total that is exactly the product, and the two numbers the operator
        # approved equal to the two the freeze computed.
        CheckConstraint(
            "(request_schema_version IN ('1', '2') "
            "AND product_contract_version = 'easyweek-production-15eur-v1' "
            "AND message_contract_code = 'new_client_voucher' "
            "AND voucher_unit_price_minor = 1500) OR "
            "(request_schema_version = '3' "
            "AND product_contract_version = 'easyweek-production-10eur-v1' "
            "AND message_contract_code = 'new_client_voucher_10eur_v2' "
            "AND voucher_template_uuid = '0ffb0346-57b8-475e-9c22-152dd23e25ca'::uuid "
            "AND voucher_unit_price_minor = 1000) OR "
            # The free gift certificate. Same template and same message as the paid
            # 10 EUR product, same face value — and an issue price of exactly zero,
            # which is the one thing that makes it a different contract.
            "(request_schema_version = '4' "
            "AND product_contract_version = 'easyweek-production-gift-10eur-v1' "
            "AND message_contract_code = 'new_client_voucher_10eur_v2' "
            "AND voucher_template_uuid = '0ffb0346-57b8-475e-9c22-152dd23e25ca'::uuid "
            "AND voucher_unit_price_minor = 1000 "
            "AND voucher_issue_price_minor = 0)",
            name="ck_ew_voucher_production_batch_unit_price",
        ),
        CheckConstraint(
            "total_exposure_minor = voucher_unit_price_minor * recipient_count",
            name="ck_ew_voucher_production_batch_exposure_matches",
        ),
        # The money side of the same arithmetic, kept apart from the nominal one.
        # ``voucher_unit_price_minor`` and ``total_exposure_minor`` are the FACE
        # VALUE and the nominal total — equal to the money for every paid contract,
        # which is why their historical rows and digests are untouched. These two
        # are what issuing actually costs.
        CheckConstraint(
            "total_issue_price_minor = voucher_issue_price_minor * recipient_count",
            name="ck_ew_voucher_production_batch_issue_price_matches",
        ),
        # A paid contract's two sums are the same number; only the free one may
        # differ, and only by being zero. This is what stops a schema 3 batch from
        # being written as if it had cost nothing.
        CheckConstraint(
            "(request_schema_version IN ('1', '2', '3') "
            "AND voucher_issue_price_minor = voucher_unit_price_minor) OR "
            "(request_schema_version = '4' AND voucher_issue_price_minor = 0)",
            name="ck_ew_voucher_production_batch_issue_price_contract",
        ),
        CheckConstraint(
            "approved_recipient_count = recipient_count",
            name="ck_ew_voucher_production_batch_count_approved",
        ),
        CheckConstraint(
            "approved_exposure_minor = total_exposure_minor",
            name="ck_ew_voucher_production_batch_exposure_approved",
        ),
        # 14. A campaign period is an interval; an entitlement key built on a
        # backwards one would be meaningless.
        CheckConstraint(
            "campaign_period_start < campaign_period_end",
            name="ck_ew_voucher_production_batch_period_order",
        ),
        # 15. A closed status vocabulary. An unknown string is not a state.
        CheckConstraint(
            f"status IN ({_VOUCHER_PRODUCTION_STATUS_SQL})",
            name="ck_ew_voucher_production_batch_status",
        ),
        # 16. A halt must say why, and only a halt may.
        CheckConstraint(
            f"(status = '{VOUCHER_PRODUCTION_HALTED}') = (halted_reason_code IS NOT NULL)",
            name="ck_ew_voucher_production_batch_halt_has_reason",
        ),
        Index("ix_ew_voucher_production_batch_status", "status"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    # -- identity ----------------------------------------------------------
    batch_scope: Mapped[str] = mapped_column(String(128), nullable=False)
    request_schema_version: Mapped[str] = mapped_column(String(16), nullable=False)
    product_contract_version: Mapped[str] = mapped_column(
        String(64), nullable=False, server_default=text("'easyweek-production-15eur-v1'")
    )
    message_contract_code: Mapped[str] = mapped_column(
        String(64), nullable=False, server_default=text("'new_client_voucher'")
    )
    baseline_version: Mapped[str] = mapped_column(String(32), nullable=False)
    provider: Mapped[str] = _provider_column()
    company_id: Mapped[int] = mapped_column(Integer, nullable=False)
    campaign_code: Mapped[str] = mapped_column(String(128), nullable=False)
    recipient_basis: Mapped[str] = mapped_column(String(32), nullable=False)

    # The exact completed preview this composition was frozen from.
    campaign_run_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    campaign_period_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    campaign_period_end: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    # -- the frozen EasyWeek identity this batch may act on -----------------
    location_uuid: Mapped[uuid.UUID] = mapped_column(PostgresUUID(as_uuid=True), nullable=False)
    staffer_uuid: Mapped[uuid.UUID] = mapped_column(PostgresUUID(as_uuid=True), nullable=False)
    payment_account_uuid: Mapped[uuid.UUID] = mapped_column(PostgresUUID(as_uuid=True), nullable=False)
    voucher_template_uuid: Mapped[uuid.UUID] = mapped_column(PostgresUUID(as_uuid=True), nullable=False)

    # -- the frozen composition --------------------------------------------
    frozen_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    recipient_count: Mapped[int] = mapped_column(Integer, nullable=False)
    # The FACE VALUE of one voucher, and the nominal total. Named as they were
    # written, because for every paid contract they are also the money, and every
    # historical row and signature already means exactly that.
    voucher_unit_price_minor: Mapped[int] = mapped_column(Integer, nullable=False)
    total_exposure_minor: Mapped[int] = mapped_column(Integer, nullable=False)
    # What issuing actually costs: zero for the free certificate, and the face
    # value for every paid contract.
    voucher_issue_price_minor: Mapped[int] = mapped_column(Integer, nullable=False)
    total_issue_price_minor: Mapped[int] = mapped_column(Integer, nullable=False)

    # -- what the operator explicitly approved, kept for the record ---------
    # Not validation leftovers. These are the two numbers a human agreed to
    # before any money moved, and the CHECKs above mean they cannot describe
    # anything other than this batch's own composition.
    approved_recipient_count: Mapped[int] = mapped_column(Integer, nullable=False)
    approved_exposure_minor: Mapped[int] = mapped_column(Integer, nullable=False)

    # -- state -------------------------------------------------------------
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    halted_reason_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    reconciliation_required: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))

    # -- operator authorisation provenance ---------------------------------
    freeze_plan_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)

    frozen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    halted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # Shape facts, proof labels and reason codes. Never a value, never a body.
    evidence: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )


class EasyWeekVoucherProductionBatchItem(Base):
    """One slot of a production batch: one person, one fixed voucher, one message.

    Its slot cannot escape its batch
    --------------------------------
    ``batch_recipient_count`` is not a convenience copy. It is half of a
    composite foreign key into ``(id, recipient_count)`` of the header, so the
    database itself knows what this batch's declared size is, and a CHECK then
    requires ``1 <= slot <= batch_recipient_count``. Together with the
    uniqueness of ``(batch_id, slot)`` that makes an out-of-range slot
    unrepresentable rather than merely refused.

    Slot numbers are per batch, deliberately. Two independent batches both
    holding a slot 1 is normal and correct: a slot is a position inside one
    frozen composition, never a global identifier, which is why nothing in this
    phase may look an item up by slot alone.

    One person, once
    ----------------
    Three separate uniqueness rules, for three different mistakes:

    * ``(batch_id, campaign_recipient_id)`` — the same preview row twice;
    * ``(batch_id, easyweek_customer_uuid)`` — two preview rows, one human;
    * the entitlement key — provider, company, campaign, customer and both
      period bounds — which is TABLE-WIDE, so a second batch built from a
      different preview cannot hand the same person a second €15 for the same
      wave. This is the constraint that makes "one voucher per client per
      campaign period" true across batches rather than only inside one, and it
      is enforced by PostgreSQL rather than by a check somebody could forget.

    Nothing here is the voucher
    ---------------------------
    The code is a bearer secret and is never a column: what is stored is a keyed
    MAC bound to this row and this order, plus the id of the key that made it.
    """

    __tablename__ = "easyweek_voucher_production_batch_items"

    __table_args__ = (
        # 1. The slot belongs to this batch and to this batch's declared size.
        ForeignKeyConstraint(
            ["batch_id", "batch_recipient_count"],
            [
                "easyweek_voucher_production_batches.id",
                "easyweek_voucher_production_batches.recipient_count",
            ],
            name="fk_ew_voucher_production_item_batch_size",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["batch_id", "voucher_value_minor"],
            ["easyweek_voucher_production_batches.id", "easyweek_voucher_production_batches.voucher_unit_price_minor"],
            name="fk_ew_voucher_production_item_batch_price",
            ondelete="RESTRICT",
        ),
        # 2. 1 <= slot <= the batch's own declared size. No global ceiling.
        CheckConstraint(
            "slot >= 1 AND slot <= batch_recipient_count AND batch_recipient_count >= 1",
            name="ck_ew_voucher_production_item_slot_range",
        ),
        # 3-5. Three uniqueness rules for three different mistakes, all of them
        # scoped to ONE batch: slot numbers repeat across batches by design.
        UniqueConstraint("batch_id", "slot", name="uq_ew_voucher_production_item_slot"),
        UniqueConstraint("batch_id", "campaign_recipient_id", name="uq_ew_voucher_production_item_recipient"),
        UniqueConstraint("batch_id", "easyweek_customer_uuid", name="uq_ew_voucher_production_item_customer"),
        # 6. One voucher per person per campaign period, TABLE-WIDE. The rule
        # that survives a second preview and a second batch.
        UniqueConstraint(
            "provider",
            "company_id",
            "campaign_code",
            "easyweek_customer_uuid",
            "campaign_period_start",
            "campaign_period_end",
            name="uq_ew_voucher_production_item_entitlement",
        ),
        # 7-10. A result may belong to exactly one slot, table-wide.
        UniqueConstraint("reconciliation_marker", name="uq_ew_voucher_production_item_marker"),
        UniqueConstraint("target_order_uuid", name="uq_ew_voucher_production_item_target_order"),
        UniqueConstraint("outbound_intent_uuid", name="uq_ew_voucher_production_item_intent"),
        UniqueConstraint("provider_message_id", name="uq_ew_voucher_production_item_provider_message"),
        # 11. The recipient and the run must belong together, under the same
        # provider. Composite FKs, because two separate ones would each be
        # satisfied by rows that have nothing to do with each other.
        ForeignKeyConstraint(
            ["campaign_recipient_id", "provider"],
            ["campaign_recipients.id", "campaign_recipients.provider"],
            name="fk_ew_voucher_production_item_recipient_provider",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["campaign_run_id", "provider"],
            ["campaign_runs.id", "campaign_runs.provider"],
            name="fk_ew_voucher_production_item_run_provider",
            ondelete="RESTRICT",
        ),
        # 12-16. The approved topology and a closed status vocabulary.
        CheckConstraint(
            f"status IN ({_VOUCHER_PRODUCTION_ITEM_STATUS_SQL})",
            name="ck_ew_voucher_production_item_status",
        ),
        CheckConstraint("provider = 'easyweek'", name="ck_ew_voucher_production_item_provider"),
        CheckConstraint(
            f"company_id = {VOUCHER_PRODUCTION_COMPANY_ID}",
            name="ck_ew_voucher_production_item_company",
        ),
        CheckConstraint(
            f"campaign_code = '{VOUCHER_PRODUCTION_CAMPAIGN_CODE}'",
            name="ck_ew_voucher_production_item_campaign",
        ),
        CheckConstraint(
            "recipient_basis IN ('earned_first_visit', 'operator_manual_selection')",
            name="ck_ew_voucher_production_item_basis",
        ),
        CheckConstraint(
            "(recipient_basis = 'earned_first_visit' AND source_booking_uuid IS NOT NULL "
            "AND source_proof_digest IS NOT NULL AND manual_policy IS NULL) OR "
            "(recipient_basis = 'operator_manual_selection' AND source_booking_uuid IS NULL "
            "AND source_proof_digest IS NULL)",
            name="ck_ew_voucher_production_item_source",
        ),
        CheckConstraint(
            "manual_policy IS NULL OR (recipient_basis = 'operator_manual_selection' "
            "AND manual_policy = 'altegio_visit_zero_easyweek_bookings')",
            name="ck_ew_voucher_production_item_policy",
        ),
        # 17. One fixed voucher, with its amount additionally bound to the
        # header product through fk_ew_voucher_production_item_batch_price.
        CheckConstraint(
            "voucher_value_minor IN (1500, 1000) AND voucher_quantity = 1",
            name="ck_ew_voucher_production_item_exact_voucher",
        ),
        CheckConstraint(
            "campaign_period_start < campaign_period_end",
            name="ck_ew_voucher_production_item_period_order",
        ),
        # 18. Stage order. A stage cannot be attempted before it was claimed,
        # nor verified before it was attempted.
        CheckConstraint(
            "(create_attempted_at IS NULL OR create_claimed_at IS NOT NULL) "
            "AND (create_verified_at IS NULL OR create_attempted_at IS NOT NULL) "
            "AND (pay_attempted_at IS NULL OR pay_claimed_at IS NOT NULL) "
            "AND (pay_verified_at IS NULL OR pay_attempted_at IS NOT NULL) "
            "AND (send_attempted_at IS NULL OR send_claimed_at IS NOT NULL) "
            "AND (refund_attempted_at IS NULL OR refund_claimed_at IS NOT NULL) "
            "AND (refund_verified_at IS NULL OR refund_attempted_at IS NOT NULL)",
            name="ck_ew_voucher_production_item_stage_order",
        ),
        # 19. Money cannot move before the order it pays for was proven to exist.
        CheckConstraint(
            "pay_claimed_at IS NULL OR (create_verified_at IS NOT NULL AND target_order_uuid IS NOT NULL)",
            name="ck_ew_voucher_production_item_pay_needs_created",
        ),
        # 20. Nothing may be sent before the voucher is proven paid for, bound
        # to this row by MAC, and proven still deliverable by a guard taken
        # AFTER the payment. A guard from before the payment says nothing.
        CheckConstraint(
            "send_claimed_at IS NULL OR ("
            "pay_verified_at IS NOT NULL "
            "AND voucher_code_hmac IS NOT NULL "
            "AND hmac_key_id IS NOT NULL "
            "AND live_guard_reproven_at IS NOT NULL "
            "AND live_guard_reproven_at >= pay_verified_at)",
            name="ck_ew_voucher_production_item_send_needs_paid",
        ),
        # 21-23. Acceptance needs both the attempt and Meta's identifier; the
        # webhook ladder may not be climbed out of order. This is what makes
        # "completed is not read" a fact about the schema and not a caveat in a
        # report.
        CheckConstraint(
            "provider_accepted_at IS NULL OR (send_attempted_at IS NOT NULL AND provider_message_id IS NOT NULL)",
            name="ck_ew_voucher_production_item_accepted_needs_attempt",
        ),
        CheckConstraint(
            "delivered_at IS NULL OR provider_accepted_at IS NOT NULL",
            name="ck_ew_voucher_production_item_delivered_needs_accepted",
        ),
        CheckConstraint(
            "read_at IS NULL OR delivered_at IS NOT NULL",
            name="ck_ew_voucher_production_item_read_needs_delivered",
        ),
        # 24-25. A refund is only ever the pre-send escape hatch.
        CheckConstraint(
            "refund_claimed_at IS NULL OR ("
            "provider_accepted_at IS NULL "
            "AND delivered_at IS NULL "
            "AND read_at IS NULL "
            "AND send_claimed_at IS NULL "
            "AND send_attempted_at IS NULL)",
            name="ck_ew_voucher_production_item_refund_is_pre_send",
        ),
        CheckConstraint(
            f"status <> '{VOUCHER_PRODUCTION_ITEM_REFUNDED}' OR ("
            "provider_accepted_at IS NULL AND delivered_at IS NULL AND read_at IS NULL)",
            name="ck_ew_voucher_production_item_refunded_never_sent",
        ),
        # 26-27. At most one delivery attempt in the lifetime of this slot. Not
        # a retry budget: a counter that can only be zero or one.
        CheckConstraint(
            "send_attempt_count >= 0 AND send_attempt_count <= 1",
            name="ck_ew_voucher_production_item_single_attempt",
        ),
        CheckConstraint(
            "(send_attempt_count = 0) = (send_attempted_at IS NULL)",
            name="ck_ew_voucher_production_item_attempt_count_matches",
        ),
        # 28. A MAC without the key that made it cannot be verified later.
        CheckConstraint(
            "(voucher_code_hmac IS NULL) = (hmac_key_id IS NULL)",
            name="ck_ew_voucher_production_item_hmac_pair",
        ),
        Index("ix_ew_voucher_production_item_batch", "batch_id"),
        Index("ix_ew_voucher_production_item_status", "status"),
        Index("ix_ew_voucher_production_item_run", "campaign_run_id"),
        Index("ix_ew_voucher_production_item_recipient", "campaign_recipient_id"),
        # The header settle reads its items by (batch, status) and the stage
        # walk reads them by (batch, slot). Both are per batch, which is what
        # keeps a batch of fifty from costing fifty full-table reads.
        Index("ix_ew_voucher_production_item_batch_status", "batch_id", "status"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    # -- membership --------------------------------------------------------
    batch_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    # The batch's declared size, carried here so the foreign key above can
    # anchor this slot to it. Never edited independently: the composite FK makes
    # a disagreement unrepresentable.
    batch_recipient_count: Mapped[int] = mapped_column(Integer, nullable=False)
    slot: Mapped[int] = mapped_column(Integer, nullable=False)

    # -- identity ----------------------------------------------------------
    provider: Mapped[str] = _provider_column()
    company_id: Mapped[int] = mapped_column(Integer, nullable=False)
    campaign_code: Mapped[str] = mapped_column(String(128), nullable=False)
    recipient_basis: Mapped[str] = mapped_column(String(32), nullable=False)
    manual_policy: Mapped[str | None] = mapped_column(String(64), nullable=True)
    source_booking_uuid: Mapped[uuid.UUID | None] = mapped_column(PostgresUUID(as_uuid=True), nullable=True)
    source_proof_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)
    campaign_run_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    campaign_recipient_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    easyweek_customer_uuid: Mapped[uuid.UUID] = mapped_column(PostgresUUID(as_uuid=True), nullable=False)
    campaign_period_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    campaign_period_end: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    # -- the exact sale this slot authorises -------------------------------
    voucher_value_minor: Mapped[int] = mapped_column(Integer, nullable=False)
    voucher_quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    # Recomputable, non-personal, and findable in the EasyWeek dashboard.
    reconciliation_marker: Mapped[str] = mapped_column(String(64), nullable=False)

    # -- results -----------------------------------------------------------
    target_order_uuid: Mapped[uuid.UUID | None] = mapped_column(PostgresUUID(as_uuid=True), nullable=True)
    outbound_intent_uuid: Mapped[uuid.UUID | None] = mapped_column(PostgresUUID(as_uuid=True), nullable=True)
    provider_message_id: Mapped[str | None] = mapped_column(String(128), nullable=True)

    # -- the voucher, as a keyed proof and nothing else ---------------------
    voucher_code_hmac: Mapped[str | None] = mapped_column(String(64), nullable=True)
    hmac_key_id: Mapped[str | None] = mapped_column(String(64), nullable=True)

    # -- state -------------------------------------------------------------
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    reason_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    manual_cleanup_required: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    reconciliation_required: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    manual_cleanup_observed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # -- operator authorisation provenance ---------------------------------
    create_plan_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)
    pay_plan_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)
    deliver_plan_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)
    refund_plan_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)

    # -- claim / attempt / verification, per stage -------------------------
    create_claimed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    create_attempted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    create_verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    create_window_start: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    create_window_end: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    pay_claimed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    pay_attempted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    pay_verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    live_guard_reproven_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    send_claimed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    send_attempted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    send_attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    provider_accepted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    read_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    refund_claimed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    refund_attempted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    refund_verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # -- safe evidence -----------------------------------------------------
    evidence: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )


class EasyWeekVoucherProductionBatchAttempt(Base):
    """One redacted outbound intent, written and committed BEFORE a send.

    Its own table rather than an ``OutboxMessage``, for the same reason as
    §§36, 37.2 and 41: the outbox is swept by a generic worker whose purpose is
    to retry what it finds, and the one property this row must have is that
    nothing may ever pick it up and send it again.

    It stores no message. No rendered body, no parameter list, no voucher code,
    no phone number and no name — so nothing downstream could re-render the
    message even if it tried.
    """

    __tablename__ = "easyweek_voucher_production_batch_attempts"

    __table_args__ = (
        UniqueConstraint("intent_uuid", name="uq_ew_voucher_production_attempt_intent"),
        # RESTRICT in both directions of intent: the audit of a real send
        # attempt must not disappear with the slot, nor allow it to be deleted.
        ForeignKeyConstraint(
            ["item_id"],
            ["easyweek_voucher_production_batch_items.id"],
            name="fk_ew_voucher_production_attempt_item",
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            "outcome IN ('claimed', 'provider_accepted', 'unknown', 'rejected')",
            name="ck_ew_voucher_production_attempt_outcome",
        ),
        CheckConstraint(
            "outcome <> 'provider_accepted' OR provider_message_id IS NOT NULL",
            name="ck_ew_voucher_production_attempt_accepted_has_id",
        ),
        Index("ix_ew_voucher_production_attempt_item", "item_id"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    item_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    intent_uuid: Mapped[uuid.UUID] = mapped_column(PostgresUUID(as_uuid=True), nullable=False)

    # Enough to audit WHICH approved template was used, and nothing about what
    # it said to whom.
    template_code: Mapped[str] = mapped_column(String(64), nullable=False)
    meta_template_name: Mapped[str] = mapped_column(String(128), nullable=False)
    template_language: Mapped[str] = mapped_column(String(8), nullable=False)
    sender_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    campaign_recipient_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    batch_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    slot: Mapped[int] = mapped_column(Integer, nullable=False)

    # -- the route this attempt actually used, frozen at the claim ----------
    # Digests, because the values are a Meta phone number id and a customer's
    # phone number and this table stores neither. Written once, in the same
    # committed transaction that spends the slot's one attempt, and never
    # updated afterwards: a later failure fact has to be checked against the
    # route the message really took, not against whichever sender the settings
    # name today or whichever number the card carries now.
    #
    # NULL on every row written before this phase existed, and deliberately so.
    # A historical attempt proved no route, and inventing one from today's
    # configuration would manufacture evidence; its facts stay unknown instead.
    sender_phone_number_id_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)
    recipient_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)

    outcome: Mapped[str] = mapped_column(String(32), nullable=False)
    reason_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    provider_message_id: Mapped[str | None] = mapped_column(String(128), nullable=True)

    claimed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


# ---------------------------------------------------------------------------
# §43 — browser-authorised operation of the production voucher mailing (PR-20)
# ---------------------------------------------------------------------------
# §42 proved the stages; it drove them from a terminal, where the operator
# carried the digest and the timestamp by hand. The owner's decision of
# 28.09.2026 is that the real mailing happens from the interface, which moves
# exactly one thing: WHERE the authorisation lives.
#
# It cannot live in the browser. A request body is not an approval — it is
# whatever the last page happened to post, twice if the operator double-clicked,
# and again if a tab was left open. So the server builds the plan, stores it
# immutably with its own expiry, and hands the browser nothing but its id. The
# browser confirms THAT plan; it never assembles permission out of fields.
#
# Three tables, for three different jobs:
#
# * ``..._approvals`` — one immutable plan per offer: who, which batch, which
#   stage, which exact slots, how many and how much. Signed by the same digest
#   §42 already used, so every drift check it earned still applies.
# * ``..._operations`` — the durable record that an approval was spent. UNIQUE on
#   ``approval_id``, which is what makes a double POST, a refresh, two tabs and
#   two operators converge on ONE operation instead of two mailings.
# * ``..._stop_requests`` — the operator's "stop after the current request",
#   durable and per batch, checked inside the same transaction as the next claim.

VOUCHER_PRODUCTION_APPROVAL_PENDING = "pending"
VOUCHER_PRODUCTION_APPROVAL_CONSUMED = "consumed"

VOUCHER_PRODUCTION_APPROVAL_STATUSES = (
    VOUCHER_PRODUCTION_APPROVAL_PENDING,
    VOUCHER_PRODUCTION_APPROVAL_CONSUMED,
)

_VOUCHER_PRODUCTION_APPROVAL_STATUS_SQL = ", ".join(f"'{value}'" for value in VOUCHER_PRODUCTION_APPROVAL_STATUSES)

VOUCHER_PRODUCTION_OPERATION_QUEUED = "queued"
VOUCHER_PRODUCTION_OPERATION_RUNNING = "running"
VOUCHER_PRODUCTION_OPERATION_COMPLETED = "completed"
VOUCHER_PRODUCTION_OPERATION_REFUSED = "refused"
VOUCHER_PRODUCTION_OPERATION_EXPIRED = "expired"
# The executor died while this operation held the lease. NOT a retryable state
# and deliberately never returned to ``queued``: something may already have
# reached EasyWeek or Meta, and the only safe next step is a readback and a
# fresh human decision.
VOUCHER_PRODUCTION_OPERATION_INTERRUPTED = "interrupted"

VOUCHER_PRODUCTION_OPERATION_STATUSES = (
    VOUCHER_PRODUCTION_OPERATION_QUEUED,
    VOUCHER_PRODUCTION_OPERATION_RUNNING,
    VOUCHER_PRODUCTION_OPERATION_COMPLETED,
    VOUCHER_PRODUCTION_OPERATION_REFUSED,
    VOUCHER_PRODUCTION_OPERATION_EXPIRED,
    VOUCHER_PRODUCTION_OPERATION_INTERRUPTED,
)

_VOUCHER_PRODUCTION_OPERATION_STATUS_SQL = ", ".join(f"'{value}'" for value in VOUCHER_PRODUCTION_OPERATION_STATUSES)

VOUCHER_PRODUCTION_OPERATION_TERMINAL = (
    VOUCHER_PRODUCTION_OPERATION_COMPLETED,
    VOUCHER_PRODUCTION_OPERATION_REFUSED,
    VOUCHER_PRODUCTION_OPERATION_EXPIRED,
    VOUCHER_PRODUCTION_OPERATION_INTERRUPTED,
)

_VOUCHER_PRODUCTION_OPERATION_TERMINAL_SQL = ", ".join(f"'{value}'" for value in VOUCHER_PRODUCTION_OPERATION_TERMINAL)


class EasyWeekVoucherProductionApproval(Base):
    """One immutable server-side plan an operator may confirm, once (§43.4).

    What makes it an approval rather than a form submission
    ------------------------------------------------------
    Everything that decides what may happen is written HERE, by the server, at
    the moment the plan was built: the authenticated principal, the branch, the
    preview, the batch, the stage, the exact ``target_slots``, the count and the
    money, and the verdicts about the pinned issuer and the frozen identity. The
    browser receives an id.

    So the confirm request cannot widen anything. There is no field in it that
    names a slot, a staffer, an amount nobody computed or a batch the plan is not
    about; the two numbers it does carry are compared against this row and a
    mismatch refuses. An operator who edits the preview between reading a plan
    and confirming it changes the digest, and the stage refuses before anything
    leaves the process.

    ``target_slots`` is the PR-19 fix, stored
    -----------------------------------------
    §42.7 requires that execution act on exactly the slots the approval covered —
    a CREATE that lands between the plan and the act makes its slot eligible for
    the NEXT plan, not for this one's payment. That list is signed into
    ``plan_digest`` and kept here verbatim, so the executor reads the approved
    set rather than re-deriving a larger one.

    It expires, and the queue does not extend it
    --------------------------------------------
    ``expires_at`` is 30 minutes after the plan was issued, exactly as §42's TTL.
    Waiting in the executor's queue is not reading time: an operation picked up
    after this moment is refused with zero external effects and needs a new plan
    and a new confirmation. No worker re-approves anything.

    PII-free, like every other row of this phase: slots, ids, counts, booleans
    and reason codes. No phone, no name, no customer UUID, no voucher code, no
    staffer UUID and no session token.
    """

    __tablename__ = "easyweek_voucher_production_approvals"

    __table_args__ = (
        CheckConstraint(
            "(request_schema_version IN ('1', '2') AND product_contract_version = 'easyweek-production-15eur-v1') "
            "OR (request_schema_version = '3' AND product_contract_version = 'easyweek-production-10eur-v1') "
            "OR (request_schema_version = '4' "
            "AND product_contract_version = 'easyweek-production-gift-10eur-v1')",
            name="ck_ew_voucher_production_approval_product",
        ),
        # What the operator confirmed, for both of the 10 EUR contracts. The sums in
        # ``stage_amount_minor``/``batch_exposure_minor`` are NOMINAL — the size of
        # the giveaway — which is also the money for the paid contract, so schema 3's
        # rule is reproduced verbatim and schema 4 is held to the same arithmetic.
        # A free issue does not get to skip confirming how many vouchers, or how
        # much they are worth; what it changes is only what is charged.
        CheckConstraint(
            "request_schema_version NOT IN ('3', '4') OR (batch_recipient_count >= 1 "
            "AND batch_exposure_minor = batch_recipient_count * 1000 "
            "AND stage_amount_minor = CASE WHEN stage = 'freeze' THEN batch_exposure_minor "
            "WHEN stage IN ('create', 'pay', 'refund') THEN stage_target_count * 1000 ELSE 0 END)",
            name="ck_ew_voucher_production_approval_product_amount",
        ),
        # And the money side, stored beside it rather than instead of it. Zero for
        # the free contract, the nominal for every paid one — so an approval can
        # never claim a free batch cost something, or a paid batch cost nothing.
        CheckConstraint(
            "(request_schema_version IN ('1', '2', '3') "
            "AND stage_issue_price_minor = stage_amount_minor "
            "AND batch_issue_price_minor = batch_exposure_minor) OR "
            "(request_schema_version = '4' "
            "AND stage_issue_price_minor = 0 AND batch_issue_price_minor = 0)",
            name="ck_ew_voucher_production_approval_issue_price",
        ),
        CheckConstraint("provider = 'easyweek'", name="ck_ew_voucher_production_approval_provider"),
        CheckConstraint(
            f"batch_scope = '{VOUCHER_PRODUCTION_SCOPE}'",
            name="ck_ew_voucher_production_approval_scope",
        ),
        CheckConstraint(
            f"company_id = {VOUCHER_PRODUCTION_COMPANY_ID}",
            name="ck_ew_voucher_production_approval_company",
        ),
        CheckConstraint(
            "stage IN ('freeze', 'create', 'pay', 'deliver', 'refund')",
            name="ck_ew_voucher_production_approval_stage",
        ),
        CheckConstraint(
            f"status IN ({_VOUCHER_PRODUCTION_APPROVAL_STATUS_SQL})",
            name="ck_ew_voucher_production_approval_status",
        ),
        # Consumed exactly when there is a moment it was consumed at. The two
        # cannot drift, so "spent" is one fact rather than two that might disagree.
        CheckConstraint(
            f"(status = '{VOUCHER_PRODUCTION_APPROVAL_CONSUMED}') = (consumed_at IS NOT NULL)",
            name="ck_ew_voucher_production_approval_consumed",
        ),
        # An approval that covers no slot authorises nothing, and storing one
        # would invite an executor to read "no restriction" out of an empty list.
        CheckConstraint("target_slot_count >= 1", name="ck_ew_voucher_production_approval_slots"),
        CheckConstraint("stop_generation_at_plan >= 0", name="ck_ew_voucher_production_approval_stop_gen"),
        # What THIS stage is about to do, which is not the same as what the batch
        # costs in total. A freeze and a deliver move no money; a create and a pay
        # move €15 per slot they are about to touch.
        CheckConstraint("stage_target_count >= 1", name="ck_ew_voucher_production_approval_targets"),
        CheckConstraint("stage_amount_minor >= 0", name="ck_ew_voucher_production_approval_amount"),
        CheckConstraint(
            "stage_issue_price_minor >= 0 AND batch_issue_price_minor >= 0",
            name="ck_ew_voucher_production_approval_issue_price_sign",
        ),
        CheckConstraint("expires_at > plan_issued_at", name="ck_ew_voucher_production_approval_ttl"),
        # Every stage after the freeze is about one existing batch, by id. A row
        # without one could only be a freeze, which is the stage that creates it.
        CheckConstraint(
            "(stage = 'freeze') OR (batch_id IS NOT NULL)",
            name="ck_ew_voucher_production_approval_batch",
        ),
        ForeignKeyConstraint(
            ["campaign_run_id", "provider"],
            ["campaign_runs.id", "campaign_runs.provider"],
            name="fk_ew_voucher_production_approval_run_provider",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["batch_id"],
            ["easyweek_voucher_production_batches.id"],
            name="fk_ew_voucher_production_approval_batch",
            ondelete="RESTRICT",
        ),
        Index("ix_ew_voucher_production_approval_batch_stage", "batch_id", "stage"),
        Index("ix_ew_voucher_production_approval_status", "status"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    batch_scope: Mapped[str] = mapped_column(String(128), nullable=False)
    request_schema_version: Mapped[str] = mapped_column(String(16), nullable=False)
    product_contract_version: Mapped[str] = mapped_column(
        String(64), nullable=False, server_default=text("'easyweek-production-15eur-v1'")
    )
    provider: Mapped[str] = _provider_column()
    company_id: Mapped[int] = mapped_column(Integer, nullable=False)
    stage: Mapped[str] = mapped_column(String(32), nullable=False)

    # -- who ---------------------------------------------------------------
    # The server-resolved Ops account, never an actor named in a payload.
    principal: Mapped[str] = mapped_column(String(128), nullable=False)
    # A keyed digest of the session that created it. Enough to tell two sessions
    # of the same account apart in an audit; never the token itself.
    session_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    # Said out loud rather than left to be assumed: with one shared Ops account,
    # an audit row identifies the account and the session, not a human being.
    identification_limit: Mapped[str] = mapped_column(String(32), nullable=False)

    # -- what ---------------------------------------------------------------
    campaign_run_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    batch_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    target_slots: Mapped[list] = mapped_column(JSONB, nullable=False)
    target_slot_count: Mapped[int] = mapped_column(Integer, nullable=False)
    stage_target_count: Mapped[int] = mapped_column(Integer, nullable=False)
    # The NOMINAL the operator confirmed for this stage. For every paid contract
    # that is also the money, which is why historical rows keep their meaning.
    stage_amount_minor: Mapped[int] = mapped_column(Integer, nullable=False)
    # What this stage will actually charge, and what the whole batch will.
    stage_issue_price_minor: Mapped[int] = mapped_column(Integer, nullable=False)
    batch_issue_price_minor: Mapped[int] = mapped_column(Integer, nullable=False)
    batch_recipient_count: Mapped[int] = mapped_column(Integer, nullable=False)
    batch_exposure_minor: Mapped[int] = mapped_column(Integer, nullable=False)
    campaign_period_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    campaign_period_end: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    # Which operator stop this plan was built in knowledge of (review R1).
    #
    # 0 means "no stop had ever been pressed for this batch". A confirmation is
    # admitted while a stop is ACTIVE only when this equals the batch's current
    # generation — that is, only when the plan was built after that stop, by an
    # operator who could see it. A plan from before the stop carries the older
    # number and is refused, so a second tab cannot lift somebody else's stop.
    stop_generation_at_plan: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))

    # -- the authorisation itself -------------------------------------------
    plan_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    plan_issued_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    # -- the bindings this plan was proven against --------------------------
    issuer_pinned: Mapped[bool] = mapped_column(Boolean, nullable=False)
    issuer_membership_proven: Mapped[bool] = mapped_column(Boolean, nullable=False)
    runtime_identity_bound: Mapped[bool] = mapped_column(Boolean, nullable=False)
    baseline_version: Mapped[str] = mapped_column(String(32), nullable=False)
    frozen_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)

    status: Mapped[str] = mapped_column(String(32), nullable=False)
    consumed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )


class EasyWeekVoucherProductionOperation(Base):
    """One durable, confirmed stage of one mailing (§43.5).

    Written and COMMITTED before the browser gets its answer, which is what makes
    the work independent of the tab that started it. A refresh, a second login or
    a closed laptop changes nothing: the operation is in PostgreSQL and the
    dedicated executor is what runs it.

    One approval, one operation
    ---------------------------
    ``approval_id`` is UNIQUE. That single constraint is what a double-click, a
    retried POST whose response was lost, two open tabs and two operators all
    collide on — in the database, not in a disabled button. The loser of the race
    is handed the operation the winner created, so the answer to "did my click
    work?" is always the same operation rather than a second payment.

    A crash is not a retry
    ----------------------
    There is no path from ``running`` back to ``queued``. An operation whose
    executor died becomes ``interrupted``, which is terminal: the per-item ledger
    already distinguishes "never claimed" from "may have reached the outside
    world", and continuing means a readback and a NEW approval for whatever is
    provably still untouched. Nothing here re-authorises itself.

    ``result`` holds the stage's own PII-free report — the same dictionary the CLI
    printed in §42. No voucher code, no phone, no name, no provider payload and no
    raw Meta id has ever been in it.
    """

    __tablename__ = "easyweek_voucher_production_operations"

    __table_args__ = (
        # The idempotency key of the whole phase.
        UniqueConstraint("approval_id", name="uq_ew_voucher_production_operation_approval"),
        CheckConstraint("provider = 'easyweek'", name="ck_ew_voucher_production_operation_provider"),
        CheckConstraint(
            f"batch_scope = '{VOUCHER_PRODUCTION_SCOPE}'",
            name="ck_ew_voucher_production_operation_scope",
        ),
        CheckConstraint(
            "stage IN ('freeze', 'create', 'pay', 'deliver', 'refund')",
            name="ck_ew_voucher_production_operation_stage",
        ),
        CheckConstraint(
            f"status IN ({_VOUCHER_PRODUCTION_OPERATION_STATUS_SQL})",
            name="ck_ew_voucher_production_operation_status",
        ),
        # Finished exactly when it reached a terminal state.
        CheckConstraint(
            f"(status IN ({_VOUCHER_PRODUCTION_OPERATION_TERMINAL_SQL})) = (finished_at IS NOT NULL)",
            name="ck_ew_voucher_production_operation_finished",
        ),
        # A queued operation has not started and holds no lease. Anything else
        # would let a sweep mistake a waiting row for an abandoned one.
        CheckConstraint(
            f"status <> '{VOUCHER_PRODUCTION_OPERATION_QUEUED}'"
            " OR (started_at IS NULL AND lease_owner IS NULL AND lease_expires_at IS NULL)",
            name="ck_ew_voucher_production_operation_queued_idle",
        ),
        CheckConstraint("attempts >= 0", name="ck_ew_voucher_production_operation_attempts"),
        ForeignKeyConstraint(
            ["approval_id"],
            ["easyweek_voucher_production_approvals.id"],
            name="fk_ew_voucher_production_operation_approval",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["batch_id"],
            ["easyweek_voucher_production_batches.id"],
            name="fk_ew_voucher_production_operation_batch",
            ondelete="RESTRICT",
        ),
        Index("ix_ew_voucher_production_operation_status", "status"),
        Index("ix_ew_voucher_production_operation_batch", "batch_id"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    approval_id: Mapped[int] = mapped_column(BigInteger, nullable=False)

    batch_scope: Mapped[str] = mapped_column(String(128), nullable=False)
    provider: Mapped[str] = _provider_column()
    company_id: Mapped[int] = mapped_column(Integer, nullable=False)
    stage: Mapped[str] = mapped_column(String(32), nullable=False)
    campaign_run_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    batch_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    # Only ever set for a refund, which is the one stage about a single slot.
    slot: Mapped[int | None] = mapped_column(Integer, nullable=True)

    principal: Mapped[str] = mapped_column(String(128), nullable=False)
    session_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    identification_limit: Mapped[str] = mapped_column(String(32), nullable=False)

    status: Mapped[str] = mapped_column(String(32), nullable=False)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    lease_owner: Mapped[str | None] = mapped_column(String(128), nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    queued_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    outcome_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    reason_codes: Mapped[list] = mapped_column(JSONB, nullable=False, server_default=text("'[]'::jsonb"))
    result: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )


class EasyWeekVoucherProductionStopRequest(Base):
    """ "Stop after the current request", durable, for ONE batch (§43.6).

    One row per batch, ever — ``batch_id`` is UNIQUE — so pressing stop twice is
    the same request rather than a second one, and a continuation clears it
    instead of deleting history.

    What it is NOT: a cancellation. A request already on the wire cannot be
    un-sent, so a stop never claims one did not happen; the per-item ledger
    records whatever that request turned out to be, including unknown. What the
    stop does is refuse the NEXT per-item claim, inside the very transaction that
    would have granted it — see ``ledger._claim``. Both this writer and that
    claim take the batch header's row lock first, so they serialise and there is
    no window in which one more slot slips through.

    It does not refund anything, it does not halt the batch, and it blocks
    neither status, nor delivery webhooks, nor reconciliation, nor an allowed
    pre-send refund. Stopping is how an operator stops spending, not how they
    lose access to what already happened.
    """

    __tablename__ = "easyweek_voucher_production_stop_requests"

    __table_args__ = (
        UniqueConstraint("batch_id", name="uq_ew_voucher_production_stop_batch"),
        ForeignKeyConstraint(
            ["batch_id"],
            ["easyweek_voucher_production_batches.id"],
            name="fk_ew_voucher_production_stop_batch",
            ondelete="RESTRICT",
        ),
        Index("ix_ew_voucher_production_stop_active", "batch_id", "cleared_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    batch_id: Mapped[int] = mapped_column(BigInteger, nullable=False)

    requested_by: Mapped[str] = mapped_column(String(128), nullable=False)
    requested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    # Set when an operator confirmed a fresh plan for this batch, which is the
    # only thing that lifts a stop. Keeping the row makes "it was stopped once"
    # a readable fact rather than an absence.
    cleared_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    cleared_by: Mapped[str | None] = mapped_column(String(128), nullable=True)
    stop_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("1"))

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )


class EasyWeekVoucherProductionAudit(Base):
    """Who did what, when, to which plan and operation — and what came back.

    Separate from the EasyWeek staffer on the voucher, deliberately (§43.9): the
    issuer is fixed and says nothing about who authorised anything, so the Ops
    account that confirmed an action is recorded here and nowhere else.

    ``identification_limit`` is the honest part. One shared Ops credential cannot
    distinguish two people, and a log that implied otherwise would be worse than
    one that says so: the row names the account and a digest of the session, and
    states what that does and does not prove.

    Never stored: a cookie, a session token, a CSRF token, a voucher code, a raw
    provider payload or a raw Meta id. ``detail`` carries counts, slots, booleans
    and reason codes.
    """

    __tablename__ = "easyweek_voucher_production_audit"

    __table_args__ = (
        CheckConstraint("provider = 'easyweek'", name="ck_ew_voucher_production_audit_provider"),
        Index("ix_ew_voucher_production_audit_at", "at"),
        Index("ix_ew_voucher_production_audit_batch", "batch_id"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    provider: Mapped[str] = _provider_column()

    principal: Mapped[str] = mapped_column(String(128), nullable=False)
    session_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    identification_limit: Mapped[str] = mapped_column(String(32), nullable=False)

    action: Mapped[str] = mapped_column(String(32), nullable=False)
    stage: Mapped[str | None] = mapped_column(String(32), nullable=True)
    campaign_run_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    batch_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    approval_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    operation_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    outcome: Mapped[str] = mapped_column(String(64), nullable=False)
    detail: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


# ---------------------------------------------------------------------------
# Chatwoot outbound mirror registry
# ---------------------------------------------------------------------------
# Durable WAMID → Chatwoot Message.id link for the private mirror note of an
# automatic (bot) WhatsApp send. It exists so an inbound WhatsApp reaction can
# resolve its native reply target in ONE indexed read, instead of paging the
# conversation history and hoping the note is still inside the walked window.
#
# Written only by the side that CREATED the note, only after Chatwoot answered
# with a valid message id — never by searching Chatwoot for a plausible match.
# The row is therefore a record of our own action, not an inference.
#
# Deliberately a standalone table rather than a column on ``outbox_messages``:
# the Chatwoot mirror runs as a background task that races the Outbox row's own
# ``provider_message_id`` commit, so this write must not depend on that row
# existing yet, and it must never take a lock on it.
CHATWOOT_MIRROR_ROUTES = ("tenant", "general")
_CHATWOOT_MIRROR_ROUTE_SQL = ", ".join(f"'{value}'" for value in CHATWOOT_MIRROR_ROUTES)


class ChatwootOutboundMirror(Base):
    __tablename__ = "chatwoot_outbound_mirrors"

    __table_args__ = (
        # One Meta message has exactly one mirror note PER CHATWOOT INSTALLATION.
        # Chatwoot message/conversation ids restart with a new database, so the
        # namespace has to be part of the key: the same wamid may legitimately map
        # to a different Chatwoot message in another installation, and neither row
        # may hide the other. This is also the idempotency key for the write
        # (INSERT ... ON CONFLICT DO NOTHING) and the lookup index.
        #
        # Legacy rows carry a NULL scope. PostgreSQL treats NULLs as distinct here,
        # so they are neither deduplicated nor trusted — the lookup requires an
        # exact scope match, which a NULL can never satisfy.
        UniqueConstraint(
            "chatwoot_scope_id",
            "provider_message_id",
            name="uq_chatwoot_outbound_mirror_scope_provider_message",
        ),
        # A zero or negative Chatwoot id is never a real message/conversation,
        # and storing one would hand the reaction path an unusable "proof".
        CheckConstraint("chatwoot_message_id > 0", name="ck_chatwoot_outbound_mirror_message_id"),
        CheckConstraint("chatwoot_conversation_id > 0", name="ck_chatwoot_outbound_mirror_conversation_id"),
        CheckConstraint("length(provider_message_id) > 0", name="ck_chatwoot_outbound_mirror_wamid_present"),
        CheckConstraint(
            f"chatwoot_route IN ({_CHATWOOT_MIRROR_ROUTE_SQL})",
            name="ck_chatwoot_outbound_mirror_route",
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    # Which Chatwoot installation/account/generation these numeric ids belong to.
    # Composed once by the ChatwootClient that talks to Chatwoot
    # (``build_chatwoot_scope_id``: normalized base URL + account id + the operator's
    # rotatable generation token — never the API token). Nullable ONLY so rows
    # written before this column existed can stay; they are never trusted.
    chatwoot_scope_id: Mapped[str | None] = mapped_column(String(200), nullable=True)

    # The exact Meta wamid of the outbound message this note mirrors, and the
    # Chatwoot message/conversation the note actually became. Together with the
    # scope above these are the whole proof the reaction path needs.
    provider_message_id: Mapped[str] = mapped_column(String(128), nullable=False)
    chatwoot_message_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    chatwoot_conversation_id: Mapped[int] = mapped_column(BigInteger, nullable=False)

    # The marker contract version in force when the note was written. A lookup
    # accepts only the version it expects, so bumping the marker retires old
    # rows instead of silently trusting a different shape.
    marker_version: Mapped[str] = mapped_column(String(64), nullable=False)

    # Routing provenance, recorded for ops and audit. The isolation the reaction
    # path enforces is the conversation itself — a Chatwoot conversation belongs
    # to exactly one inbox, and the inbox map binds an inbox to exactly one
    # provider/company pair — so these columns are descriptive, which is why
    # they stay nullable for the legacy single-inbox client.
    chatwoot_route: Mapped[str] = mapped_column(String(16), nullable=False)
    chatwoot_inbox_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    tenant_provider: Mapped[str | None] = mapped_column(String(32), nullable=True)
    company_id: Mapped[int | None] = mapped_column(Integer, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class EasyWeekVoucherOwnerTest(Base):
    """One-ever isolated gift issuance; not a campaign or customer entitlement."""

    __tablename__ = "easyweek_voucher_owner_test"
    __table_args__ = (
        CheckConstraint("id = 1", name="ck_ew_owner_test_singleton"),
        CheckConstraint(
            "state IN ('new', 'create_queued', 'create_running', 'open', "
            "'pay_queued', 'pay_running', 'paid', 'unknown', 'blocked')",
            name="ck_ew_owner_test_state",
        ),
        CheckConstraint("NOT pay_attempted OR create_attempted", name="ck_ew_owner_test_attempts"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    customer_uuid: Mapped[uuid.UUID] = mapped_column(PostgresUUID(as_uuid=True), nullable=False)
    binding_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    marker: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    state: Mapped[str] = mapped_column(String(32), nullable=False, default="new")
    stopped: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    create_attempted: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    pay_attempted: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    order_uuid: Mapped[uuid.UUID | None] = mapped_column(PostgresUUID(as_uuid=True), nullable=True)
    code_present: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    reason: Mapped[str | None] = mapped_column(String(96), nullable=True)
    approval: Mapped[dict | None] = mapped_column(JSONB(none_as_null=True), nullable=True)
    audit: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class EasyWeekManualRecipientPlan(Base):
    """Short-lived, private operator/session-bound list check (§44)."""

    __tablename__ = "easyweek_manual_recipient_plans"
    __table_args__ = (
        CheckConstraint("policy = 'altegio_visit_zero_easyweek_bookings'", name="ck_ew_manual_plan_policy"),
        CheckConstraint("expires_at > created_at", name="ck_ew_manual_plan_expiry"),
        CheckConstraint("(applied_at IS NULL) = (result IS NULL)", name="ck_ew_manual_plan_applied"),
        Index("ix_ew_manual_plan_expiry", "expires_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(PostgresUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    run_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("campaign_runs.id", ondelete="RESTRICT"), nullable=False)
    operator_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    session_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    policy: Mapped[str] = mapped_column(String(64), nullable=False)
    proof_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    applied_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    result: Mapped[dict | None] = mapped_column(JSONB(none_as_null=True), nullable=True)


# ---------------------------------------------------------------------------
# §46.4 / PR-22.1 — durable inbound consent and proven delivery failure
# ---------------------------------------------------------------------------
# Three facts this phase has nowhere to put, and one reason each.
#
# * ``whatsapp_inbound_receipts`` — WHAT ARRIVED, recorded where the signature
#   was actually checked. The ingress deliberately drops the Meta signature
#   header before it stores the event, so after that boundary authenticity can
#   never be recovered: it has to be written down at the moment it is known.
#   The row is also the durable "not resolved yet" marker, per message rather
#   than per event, which is what lets a consent guard ask "is anything from
#   this address still unprocessed?" without parsing stored payloads.
#
# * ``whatsapp_consent_suppressions`` — WHAT WE MAY DO, per normalised WhatsApp
#   address, independent of any ``Client`` card. A refusal has to survive a
#   person having no card at all, a card being created, imported or replaced,
#   and their phone number changing; none of that is expressible as a column on
#   ``clients``. It also carries the monotonic ``consent_version`` a later final
#   send guard orders itself against.
#
# * ``easyweek_voucher_production_delivery_failures`` — WHAT WENT WRONG with one
#   original send. Several contradictory facts may exist about one slot (an
#   early callback, a duplicate, one naming a foreign sender), so they cannot
#   collapse into a column on the slot, and one may arrive BEFORE the slot's
#   message id is committed, so it has to be storable unmatched and bound later.
#
# A missing row in any of them means "not proven", never "allowed" and never
# "no such event".

CONSENT_COMMAND_STOP = "stop"
CONSENT_COMMAND_START = "start"
# An ordinary inbound message. Recorded because it still has to be resolved
# before a send may proceed — a reply is not consent, but an unread reply may
# be carrying a STOP this process has not looked at yet.
CONSENT_COMMAND_NONE = "none"

INBOUND_CONSENT_COMMANDS = (
    CONSENT_COMMAND_STOP,
    CONSENT_COMMAND_START,
    CONSENT_COMMAND_NONE,
)

_INBOUND_CONSENT_COMMAND_SQL = ", ".join(f"'{value}'" for value in INBOUND_CONSENT_COMMANDS)

# Meta's HMAC was configured AND matched the raw body of this very request.
INBOUND_AUTHENTICITY_SIGNED = "signature_verified"
# Everything else, and deliberately one single bucket: a request that carried no
# signature, a deployment with no app secret configured, a replayed payload and
# a historical row whose headers were already sanitised are all equally
# unproven, and naming them apart would invite treating some of them as proof.
INBOUND_AUTHENTICITY_UNVERIFIED = "unverified"

INBOUND_AUTHENTICITIES = (
    INBOUND_AUTHENTICITY_SIGNED,
    INBOUND_AUTHENTICITY_UNVERIFIED,
)

_INBOUND_AUTHENTICITY_SQL = ", ".join(f"'{value}'" for value in INBOUND_AUTHENTICITIES)

# What the consent writer did with this receipt. ``applied`` moved the ledger,
# ``noop`` was already satisfied, ``refused`` carries a reason code and left the
# ledger alone, and ``ignored_route`` is stamped by the ingress itself for an
# event no worker will ever process.
INBOUND_RESOLUTION_APPLIED = "applied"
INBOUND_RESOLUTION_NOOP = "noop"
INBOUND_RESOLUTION_REFUSED = "refused"
INBOUND_RESOLUTION_IGNORED_ROUTE = "ignored_route"

INBOUND_RESOLUTIONS = (
    INBOUND_RESOLUTION_APPLIED,
    INBOUND_RESOLUTION_NOOP,
    INBOUND_RESOLUTION_REFUSED,
    INBOUND_RESOLUTION_IGNORED_ROUTE,
)

_INBOUND_RESOLUTION_SQL = ", ".join(f"'{value}'" for value in INBOUND_RESOLUTIONS)

CONSENT_STATE_OPTED_OUT = "opted_out"
CONSENT_STATE_OPTED_IN = "opted_in"

CONSENT_STATES = (CONSENT_STATE_OPTED_OUT, CONSENT_STATE_OPTED_IN)

_CONSENT_STATE_SQL = ", ".join(f"'{value}'" for value in CONSENT_STATES)

CONSENT_SOURCE_INBOUND = "inbound_command"
CONSENT_SOURCE_PROJECTION = "historical_projection"

CONSENT_SOURCES = (CONSENT_SOURCE_INBOUND, CONSENT_SOURCE_PROJECTION)

_CONSENT_SOURCE_SQL = ", ".join(f"'{value}'" for value in CONSENT_SOURCES)


class WhatsAppInboundReceipt(Base):
    """One inbound WhatsApp message, as the ingress durably accepted it.

    Written in the SAME transaction as its ``WhatsAppEvent``, before the 200 the
    sender sees. That is what makes it a boundary: once Meta has been told the
    delivery was accepted, the fact that this address said something is on disk
    whatever happens to the worker afterwards.

    It records authenticity because nothing downstream can. ``whatsapp_ingest``
    verifies Meta's HMAC against the raw request body and then deliberately
    strips the signature header out of the stored copy, so by the time any
    worker, report or reconciliation looks at the event the proof is gone. A
    later process must never "recover" it from a sanitised header, and with this
    column it has no reason to try.

    Resolution, not deletion
    ------------------------
    ``resolved_at`` is the pending marker. A row with it NULL means this address
    has said something that consent has not accounted for yet, and a send guard
    treats that as a hold rather than as permission. Rows are never removed:
    the pending set drains by being resolved, which is a fact, not a cleanup.

    ``receipt_key`` is its own durable identity, derived from the event's dedupe
    key and this message's position. The foreign key is SET NULL rather than
    RESTRICT, matching ``OutboxMessage.whatsapp_event_id``, so removing a stored
    event cannot be blocked by this table — and cannot silently take the record
    of what arrived with it either.
    """

    __tablename__ = "whatsapp_inbound_receipts"

    __table_args__ = (
        # Identity that survives the event row it came from.
        UniqueConstraint("receipt_key", name="uq_wa_inbound_receipt_key"),
        # And identity within one stored event, so re-ingesting the same payload
        # cannot write a second receipt for the same message position.
        UniqueConstraint("whatsapp_event_id", "message_index", name="uq_wa_inbound_receipt_event_message"),
        CheckConstraint(
            f"command IN ({_INBOUND_CONSENT_COMMAND_SQL})",
            name="ck_wa_inbound_receipt_command",
        ),
        CheckConstraint(
            f"authenticity IN ({_INBOUND_AUTHENTICITY_SQL})",
            name="ck_wa_inbound_receipt_authenticity",
        ),
        CheckConstraint(
            f"resolution IS NULL OR resolution IN ({_INBOUND_RESOLUTION_SQL})",
            name="ck_wa_inbound_receipt_resolution",
        ),
        # Resolved and its outcome are one fact. A resolved row with no
        # resolution could not be audited; a resolution with no timestamp could
        # not be ordered against anything.
        CheckConstraint(
            "(resolved_at IS NULL) = (resolution IS NULL)",
            name="ck_wa_inbound_receipt_resolution_paired",
        ),
        CheckConstraint("message_index >= 0", name="ck_wa_inbound_receipt_index"),
        # The pending lookup: every send guard asks exactly this question.
        Index(
            "ix_wa_inbound_receipt_pending",
            "wa_address",
            postgresql_where=text("resolved_at IS NULL"),
        ),
        Index("ix_wa_inbound_receipt_address_observed", "wa_address", "observed_at"),
        Index("ix_wa_inbound_receipt_event", "whatsapp_event_id"),
        # A refused route is resolved the moment it is written, so the two are
        # one fact and a row cannot claim to be both ignored and pending.
        CheckConstraint(
            "route_allowed IS NOT FALSE OR resolved_at IS NOT NULL",
            name="ck_wa_inbound_receipt_refused_route_resolved",
        ),
        CheckConstraint(
            "(linked_easyweek_customer_uuid IS NULL) = (linked_uuid_recorded_at IS NULL)",
            name="ck_wa_inbound_receipt_linked_uuid_paired",
        ),
        # The guard's other pending lookup: everything still unaccounted for
        # from a number this UUID was proven to use, including numbers no card
        # carries any more.
        Index(
            "ix_wa_inbound_receipt_pending_uuid",
            "linked_easyweek_customer_uuid",
            postgresql_where=text("resolved_at IS NULL AND linked_easyweek_customer_uuid IS NOT NULL"),
        ),
        # Conflicting reuse of one Meta message identifier is a thing this
        # phase must be able to SEE rather than a thing it forbids: two
        # different payloads naming one wamid are both recorded, and the consent
        # writer refuses to lift a ban on either.
        Index(
            "ix_wa_inbound_receipt_message",
            "whatsapp_message_id",
            postgresql_where=text("whatsapp_message_id IS NOT NULL"),
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    receipt_key: Mapped[str] = mapped_column(String(64), nullable=False)
    whatsapp_event_id: Mapped[int | None] = mapped_column(
        BigInteger,
        ForeignKey("whatsapp_events.id", ondelete="SET NULL"),
        nullable=True,
    )
    message_index: Mapped[int] = mapped_column(Integer, nullable=False)

    # The normalised sender address of THIS message, never of the payload's
    # first one: a payload may carry several messages from several people.
    wa_address: Mapped[str] = mapped_column(String(32), nullable=False)
    command: Mapped[str] = mapped_column(String(8), nullable=False)
    authenticity: Mapped[str] = mapped_column(String(24), nullable=False)

    # The sender route THIS message arrived on, as a one-way digest, and the
    # verdict the ingress reached about it at the time.
    #
    # Both are provenance, and the second one is why the first is not enough:
    # the allow list is configuration and can be changed, so re-deriving an old
    # message's verdict from today's settings would silently rewrite history —
    # a route that was refused when the message arrived would become an
    # executable command, and one that was accepted would become a refusal. A
    # NULL ``route_allowed`` is an honest "not recorded per message", which is
    # what every row written before this column existed really is.
    sender_route_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)
    route_allowed: Mapped[bool | None] = mapped_column(Boolean, nullable=True)

    # Meta's own wamid for this message, when it supplied a usable one.
    whatsapp_message_id: Mapped[str | None] = mapped_column(String(128), nullable=True)

    # A proven NEGATIVE-CONSENT link, and nothing else.
    #
    # Written only by the booking bridge, at the one moment an EasyWeek customer
    # UUID has just been proven for this person — and only onto a receipt that
    # is still unresolved. It exists because the card's number may change before
    # the inbox worker reaches this message: the refusal would then be recorded
    # against a number no card names, and nothing would connect it to the new
    # one. With this column the decision the worker eventually makes carries the
    # link onto the suppression row, and a send guard for the new number can see
    # that something from the old one is still unaccounted for.
    #
    # Never read as positive identity. Like ``banned_easyweek_customer_uuid`` it
    # widens who must NOT be messaged (§46.4.2) and proves nothing about whose
    # card this is, which branch they belong to, or whether they are an EasyWeek
    # customer at all.
    linked_easyweek_customer_uuid: Mapped[uuid.UUID | None] = mapped_column(PostgresUUID(as_uuid=True), nullable=True)
    linked_uuid_recorded_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # Two different times, deliberately never merged. ``observed_at`` is when
    # this process accepted the delivery and is always known. ``event_at`` is
    # what the sender said happened and is NULL unless it parsed as a plausible
    # unix second — an unparseable, absent or absurd timestamp is not a time.
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    event_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    resolution: Mapped[str | None] = mapped_column(String(24), nullable=True)
    resolution_reason: Mapped[str | None] = mapped_column(String(64), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )


class WhatsAppConsentSuppression(Base):
    """The current marketing consent answer for one WhatsApp address.

    Keyed by the address and nothing else
    -------------------------------------
    A ``Client`` card is the wrong place for this and always was. A person can
    say STOP before any card exists, from a number that belongs to two cards in
    two branches, or from a number that later moves to a different card — and in
    every one of those cases the refusal has to be the thing that survives. So
    the row is about the address, and a card being created, imported, merged or
    replaced cannot touch it.

    ``banned_easyweek_customer_uuid`` is a BAN EXTENSION, not an identity claim
    ---------------------------------------------------------------------------
    When the STOP arrives, any EasyWeek card matching that phone may name a
    customer UUID, and that UUID is recorded here so the refusal follows the
    person through a change of number. It must never be read the other way
    round: a phone match does not prove who somebody is, which branch they
    belong to, or that they are an EasyWeek customer at all. It is written only
    when the match is unambiguous, and ``evidence`` says on what basis.

    One direction is free, the other needs proof
    --------------------------------------------
    Moving to ``opted_out`` is always allowed: the cost of an unnecessary ban is
    one marketing message nobody sent. Moving to ``opted_in`` is not, and the
    last CHECK below enforces it in the database rather than in the writer: an
    ``opted_in`` row whose decision was a START must carry a verified signature
    and a proven event time, so an unsigned, replayed or time-unknown START is
    not merely refused by code — it is unrepresentable.

    ``consent_version`` is monotonic per address and bumps on every accepted
    decision. A later final send guard reads it, and the executor re-reads it at
    its claim: equal means nothing moved, higher means a decision landed in
    between and the claim must refuse.
    """

    __tablename__ = "whatsapp_consent_suppressions"

    __table_args__ = (
        UniqueConstraint("wa_address", name="uq_wa_consent_address"),
        CheckConstraint(f"state IN ({_CONSENT_STATE_SQL})", name="ck_wa_consent_state"),
        CheckConstraint(
            f"decision_command IN ('{CONSENT_COMMAND_STOP}', '{CONSENT_COMMAND_START}')",
            name="ck_wa_consent_decision_command",
        ),
        CheckConstraint(
            f"decision_authenticity IN ({_INBOUND_AUTHENTICITY_SQL})",
            name="ck_wa_consent_decision_authenticity",
        ),
        CheckConstraint(f"decision_source IN ({_CONSENT_SOURCE_SQL})", name="ck_wa_consent_decision_source"),
        CheckConstraint("consent_version >= 1", name="ck_wa_consent_version_positive"),
        # A ban and the moment it started are one fact.
        CheckConstraint(
            f"(state = '{CONSENT_STATE_OPTED_OUT}') = (opted_out_at IS NOT NULL)",
            name="ck_wa_consent_opt_out_paired",
        ),
        CheckConstraint(
            "(banned_easyweek_customer_uuid IS NULL) = (banned_uuid_recorded_at IS NULL)",
            name="ck_wa_consent_banned_uuid_paired",
        ),
        # The rule that makes an unproven START harmless at the schema level.
        CheckConstraint(
            f"decision_command <> '{CONSENT_COMMAND_START}' OR ("
            f"state = '{CONSENT_STATE_OPTED_IN}' "
            "AND decided_event_at IS NOT NULL "
            f"AND decision_authenticity = '{INBOUND_AUTHENTICITY_SIGNED}')",
            name="ck_wa_consent_start_needs_proof",
        ),
        Index(
            "ix_wa_consent_active",
            "wa_address",
            postgresql_where=text(f"state = '{CONSENT_STATE_OPTED_OUT}'"),
        ),
        Index(
            "ix_wa_consent_banned_uuid",
            "banned_easyweek_customer_uuid",
            postgresql_where=text("banned_easyweek_customer_uuid IS NOT NULL"),
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    wa_address: Mapped[str] = mapped_column(String(32), nullable=False)
    state: Mapped[str] = mapped_column(String(16), nullable=False)
    consent_version: Mapped[int] = mapped_column(BigInteger, nullable=False)

    # -- the decision this state rests on, denormalised on purpose ----------
    # The receipt is SET NULL on delete, so these columns are what keeps the
    # refusal auditable when the stored event behind it is gone.
    decision_command: Mapped[str] = mapped_column(String(8), nullable=False)
    decision_source: Mapped[str] = mapped_column(String(32), nullable=False)
    decision_authenticity: Mapped[str] = mapped_column(String(24), nullable=False)
    decision_receipt_id: Mapped[int | None] = mapped_column(
        BigInteger,
        ForeignKey("whatsapp_inbound_receipts.id", ondelete="SET NULL"),
        nullable=True,
    )
    decided_observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    decided_event_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    opted_out_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # The high-water marks each command has reached, kept apart from the current
    # decision so an out-of-order replay can be recognised rather than guessed.
    #
    # ``last_start_*`` is AUDIT, and deliberately nothing else: it moves only
    # for a START this ledger actually honoured, and no rule reads it. Deriving
    # the opt-in boundary from it was a defect — the mark used to move for every
    # START, rejected ones included, so an unsigned attempt could raise the time
    # a later genuine STOP had to beat. That boundary is ``decided_event_at`` of
    # the decision in force, and only that.
    last_stop_observed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_stop_event_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_start_observed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_start_event_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # When a STOP was accepted whose OWN event time could not be proven.
    #
    # Load-bearing, and the reason is an ordering one. ``last_stop_event_at``
    # only moves forward, so a later STOP with an unprovable time used to leave
    # it reading the older, provable one — and a START merely had to beat THAT
    # to lift a refusal nobody could order it against. With this column the
    # unprovable STOP stays visible: "strictly newer" has no answer while it is
    # set, and §46.4.4 says an unproven order is not a permission.
    stop_event_time_unknown_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    banned_easyweek_customer_uuid: Mapped[uuid.UUID | None] = mapped_column(PostgresUUID(as_uuid=True), nullable=True)
    banned_uuid_recorded_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # Reason labels and shape facts only. Never a message body, never a name.
    evidence: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )


DELIVERY_FAILURE_SOURCE_META_API = "meta_api_rejection"
DELIVERY_FAILURE_SOURCE_CALLBACK = "status_callback"

DELIVERY_FAILURE_SOURCES = (
    DELIVERY_FAILURE_SOURCE_META_API,
    DELIVERY_FAILURE_SOURCE_CALLBACK,
)

_DELIVERY_FAILURE_SOURCE_SQL = ", ".join(f"'{value}'" for value in DELIVERY_FAILURE_SOURCES)

# Bound to one proven original attempt of one slot.
DELIVERY_FAILURE_MATCH_CONFIRMED = "confirmed"
# Nobody owns this identifier yet. Kept so a callback that overtook the commit
# recording the message id is not lost, and bindable later on the exact id.
DELIVERY_FAILURE_MATCH_UNMATCHED = "unmatched"
# It names a slot but disagrees with it — a foreign sender or recipient, or a
# failure arriving after delivered/read. Stored for audit; changes nothing.
DELIVERY_FAILURE_MATCH_CONFLICTING = "conflicting"
# It names the right slot and contradicts nothing, but the proof its SOURCE
# requires is incomplete — an unsigned callback, or one about an attempt that
# froze no route. Kept for audit, and deliberately not confirmed: "nothing
# contradicts it" is not the same fact as "it is proven", and only the second
# one may mark a slot.
DELIVERY_FAILURE_MATCH_UNPROVEN = "unproven"

DELIVERY_FAILURE_MATCH_STATES = (
    DELIVERY_FAILURE_MATCH_CONFIRMED,
    DELIVERY_FAILURE_MATCH_UNMATCHED,
    DELIVERY_FAILURE_MATCH_CONFLICTING,
    DELIVERY_FAILURE_MATCH_UNPROVEN,
)

_DELIVERY_FAILURE_MATCH_SQL = ", ".join(f"'{value}'" for value in DELIVERY_FAILURE_MATCH_STATES)

DELIVERY_FAILURE_CLASS_PERMANENT = "permanent"
DELIVERY_FAILURE_CLASS_TRANSIENT = "transient"
DELIVERY_FAILURE_CLASS_UNKNOWN = "unknown"

DELIVERY_FAILURE_CLASSES = (
    DELIVERY_FAILURE_CLASS_PERMANENT,
    DELIVERY_FAILURE_CLASS_TRANSIENT,
    DELIVERY_FAILURE_CLASS_UNKNOWN,
)

_DELIVERY_FAILURE_CLASS_SQL = ", ".join(f"'{value}'" for value in DELIVERY_FAILURE_CLASSES)

# Meta itself refused the POST, with its own error envelope, on a status it
# decides before acting on the message.
DELIVERY_FAILURE_REASON_API_REJECTED = "meta_api_rejected"
# A `failed` delivery status webhook naming this exact message id.
DELIVERY_FAILURE_REASON_CALLBACK_FAILED = "meta_callback_failed"

DELIVERY_FAILURE_REASONS = (
    DELIVERY_FAILURE_REASON_API_REJECTED,
    DELIVERY_FAILURE_REASON_CALLBACK_FAILED,
)

_DELIVERY_FAILURE_REASON_SQL = ", ".join(f"'{value}'" for value in DELIVERY_FAILURE_REASONS)


class EasyWeekVoucherProductionDeliveryFailure(Base):
    """One PROVEN failure fact about one original production voucher send.

    What it is not
    --------------
    It is not permission to send again. Nothing in this table re-opens a slot;
    the single-attempt CHECK on the slot is untouched and this row is written
    beside it, never instead of it. §46.3 needs the FACT so a later phase can
    offer an audience with a reason; whether a reason allows a follow-up at all
    is a separate policy that does not live here.

    It is also not a place for doubt. A timeout, a 429, an unreadable 2xx, a
    2xx without a message id, a missing callback, accepted-only, an attempt that
    never happened, a CREATE/PAY error and a local parameter refusal all mean
    the same thing — unknown — and none of them writes a row. Only two things
    do: Meta's own refusal of the request, and a ``failed`` status webhook
    naming the exact message id.

    Why the external error is reduced to numbers and labels
    ------------------------------------------------------
    A Meta error title, message or detail string can quote the request it
    refused, which for this phase means a voucher code, a rendered body or a
    customer's name. So the untrusted side of the fact is narrowed to one
    integer: ``meta_error_code``, set only when the value really parsed as an
    integer, NULL otherwise. The classification is derived from that integer
    alone — never from the free text — which is why an unknown code produces
    ``unknown`` rather than a guess.

    Why it can exist before the slot does
    -------------------------------------
    Meta can deliver a status for a message id while the transaction that
    records that id is still in flight. Such a callback arrives owned by nobody,
    and dropping it would lose a real failure. It is therefore stored
    ``unmatched`` on the identifier alone and bound when the acceptance commits.
    Matching is only ever by exact proven identity — message id, intent, item —
    and never by phone or by how close two timestamps happen to be.
    """

    __tablename__ = "easyweek_voucher_production_delivery_failures"

    __table_args__ = (
        # Idempotency. The same fact delivered twice is one row, and the key is
        # computed from the proven contents of the fact rather than from when it
        # arrived, so a duplicate is recognised however late it is.
        UniqueConstraint("fact_key", name="uq_ew_voucher_failure_fact"),
        ForeignKeyConstraint(
            ["item_id"],
            ["easyweek_voucher_production_batch_items.id"],
            name="fk_ew_voucher_failure_item",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["attempt_id"],
            ["easyweek_voucher_production_batch_attempts.id"],
            name="fk_ew_voucher_failure_attempt",
            ondelete="RESTRICT",
        ),
        CheckConstraint(f"source IN ({_DELIVERY_FAILURE_SOURCE_SQL})", name="ck_ew_voucher_failure_source"),
        CheckConstraint(f"reason_code IN ({_DELIVERY_FAILURE_REASON_SQL})", name="ck_ew_voucher_failure_reason"),
        CheckConstraint(f"match_state IN ({_DELIVERY_FAILURE_MATCH_SQL})", name="ck_ew_voucher_failure_match"),
        CheckConstraint(f"failure_class IN ({_DELIVERY_FAILURE_CLASS_SQL})", name="ck_ew_voucher_failure_class"),
        CheckConstraint(
            f"authenticity IN ({_INBOUND_AUTHENTICITY_SQL})",
            name="ck_ew_voucher_failure_authenticity",
        ),
        # A confirmed fact names the slot it is about. An unmatched one cannot,
        # by definition — that is what makes it unmatched.
        CheckConstraint(
            f"match_state <> '{DELIVERY_FAILURE_MATCH_CONFIRMED}' OR item_id IS NOT NULL",
            name="ck_ew_voucher_failure_confirmed_has_item",
        ),
        CheckConstraint(
            f"match_state <> '{DELIVERY_FAILURE_MATCH_UNMATCHED}' OR item_id IS NULL",
            name="ck_ew_voucher_failure_unmatched_has_no_item",
        ),
        # An unproven fact knows which slot it was tested against — that is the
        # difference between it and an unmatched one.
        CheckConstraint(
            f"match_state <> '{DELIVERY_FAILURE_MATCH_UNPROVEN}' OR item_id IS NOT NULL",
            name="ck_ew_voucher_failure_unproven_has_item",
        ),
        # And a confirmed CALLBACK carries the two proofs §46.3 asks of it: the
        # signature the ingress checked, and the route the attempt froze. An
        # API rejection has its own contract — this process observed Meta refuse
        # its own request, so no webhook signature exists to require.
        CheckConstraint(
            f"match_state <> '{DELIVERY_FAILURE_MATCH_CONFIRMED}' "
            f"OR source <> '{DELIVERY_FAILURE_SOURCE_CALLBACK}' "
            f"OR (authenticity = '{INBOUND_AUTHENTICITY_SIGNED}' AND route_proven)",
            name="ck_ew_voucher_failure_callback_confirmed_proof",
        ),
        # A callback is about a message id; an API refusal happens before any id
        # exists and is tied to the committed intent instead.
        CheckConstraint(
            f"source <> '{DELIVERY_FAILURE_SOURCE_CALLBACK}' OR provider_message_id IS NOT NULL",
            name="ck_ew_voucher_failure_callback_has_message",
        ),
        CheckConstraint(
            f"source <> '{DELIVERY_FAILURE_SOURCE_META_API}' OR intent_uuid IS NOT NULL",
            name="ck_ew_voucher_failure_api_has_intent",
        ),
        # Route proof is all-or-nothing per side, and its absence is honest:
        # a historical attempt stored no route, so its facts say unknown rather
        # than claiming the sender the settings happen to name today.
        CheckConstraint(
            "NOT route_proven OR (observed_sender_digest IS NOT NULL AND observed_recipient_digest IS NOT NULL)",
            name="ck_ew_voucher_failure_route_proven",
        ),
        Index("ix_ew_voucher_failure_item", "item_id"),
        Index(
            "ix_ew_voucher_failure_message",
            "provider_message_id",
            postgresql_where=text("provider_message_id IS NOT NULL"),
        ),
        Index(
            "ix_ew_voucher_failure_unmatched",
            "provider_message_id",
            postgresql_where=text(f"match_state = '{DELIVERY_FAILURE_MATCH_UNMATCHED}'"),
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    fact_key: Mapped[str] = mapped_column(String(64), nullable=False)
    source: Mapped[str] = mapped_column(String(32), nullable=False)
    reason_code: Mapped[str] = mapped_column(String(32), nullable=False)
    match_state: Mapped[str] = mapped_column(String(16), nullable=False)
    failure_class: Mapped[str] = mapped_column(String(16), nullable=False)
    authenticity: Mapped[str] = mapped_column(String(24), nullable=False)

    # -- the exact original attempt this is about ---------------------------
    item_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    attempt_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    batch_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    slot: Mapped[int | None] = mapped_column(Integer, nullable=True)
    intent_uuid: Mapped[uuid.UUID | None] = mapped_column(PostgresUUID(as_uuid=True), nullable=True)
    provider_message_id: Mapped[str | None] = mapped_column(String(128), nullable=True)

    # -- the route the FACT names, as digests, and whether it was provable --
    observed_sender_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)
    observed_recipient_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)
    route_proven: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))

    # -- the untrusted side, reduced to one integer -------------------------
    meta_error_code: Mapped[int | None] = mapped_column(Integer, nullable=True)
    http_status: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # Two different times, never merged: when we saw it, and when the event is
    # PROVEN to have happened. The second is NULL whenever it cannot be proven.
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    event_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    whatsapp_event_id: Mapped[int | None] = mapped_column(
        BigInteger,
        ForeignKey("whatsapp_events.id", ondelete="SET NULL"),
        nullable=True,
    )

    # Reason labels and shape facts only. No raw provider response, no body, no
    # voucher code, no personal parameter and no Meta error text.
    evidence: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )


# ---------------------------------------------------------------------------
# §46.3 — durable evidence for a PROVEN positive status (PR-22.2)
# ---------------------------------------------------------------------------
# The production ledger has always moved a slot to ``delivered``/``read`` on a
# callback matched by exact message id, and that is the right contract for the
# MAILING's own record: it is a fact about a message already sent, and losing it
# would corrupt the history of a mailing that really happened.
#
# §46.3 asks a stricter question of anything that wants to SEND again: a status
# counts only "из проверенного inbound-контекста с точным sender и message ID".
# The transition itself records neither, and the proof is gone by the time any
# later reader looks — the ingress verifies Meta's HMAC against the raw body and
# then strips the signature header out of the stored copy.
#
# So the proof is written down beside the transition, in its own table, and the
# slot's own columns, counters and statistics are left exactly as they were. A
# historical slot has no row here, which is the honest answer: its status is the
# mailing's record and not evidence for a new send.

DELIVERY_RECEIPT_KIND_DELIVERED = "delivered"
DELIVERY_RECEIPT_KIND_READ = "read"

DELIVERY_RECEIPT_KINDS = (DELIVERY_RECEIPT_KIND_DELIVERED, DELIVERY_RECEIPT_KIND_READ)

_DELIVERY_RECEIPT_KIND_SQL = ", ".join(f"'{value}'" for value in DELIVERY_RECEIPT_KINDS)


class EasyWeekVoucherProductionDeliveryReceipt(Base):
    """One PROVEN positive status about one original production voucher send.

    What makes a row here, and nothing less: a ``delivered`` or ``read`` status
    whose delivery signature the ingress verified, naming the exact message id
    of this slot, on a route equal to the one that slot's single attempt froze.
    A callback missing any of the three is not written — it is not evidence —
    while the slot's own transition is applied exactly as before.

    It carries no authority of its own. Nothing here re-opens a slot, changes a
    counter, or claims a send: it records that a status was proven, and whether
    a later phase may act on that is a separate decision elsewhere.
    """

    __tablename__ = "easyweek_voucher_production_delivery_receipts"

    __table_args__ = (
        # Idempotency, computed from the proven contents of the fact rather than
        # from when it arrived, so a duplicate delivery is recognised however
        # late it is and a Resend adds nothing.
        UniqueConstraint("fact_key", name="uq_ew_voucher_receipt_fact"),
        ForeignKeyConstraint(
            ["item_id"],
            ["easyweek_voucher_production_batch_items.id"],
            name="fk_ew_voucher_receipt_item",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["attempt_id"],
            ["easyweek_voucher_production_batch_attempts.id"],
            name="fk_ew_voucher_receipt_attempt",
            ondelete="RESTRICT",
        ),
        # Named here rather than inline on the column, so the ORM and the
        # migration agree on the constraint's NAME as well as its shape — the
        # drift guard compares names, and an auto-generated one would differ.
        ForeignKeyConstraint(
            ["whatsapp_event_id"],
            ["whatsapp_events.id"],
            name="fk_ew_voucher_receipt_event",
            ondelete="SET NULL",
        ),
        CheckConstraint(f"kind IN ({_DELIVERY_RECEIPT_KIND_SQL})", name="ck_ew_voucher_receipt_kind"),
        CheckConstraint(
            f"authenticity IN ({_INBOUND_AUTHENTICITY_SQL})",
            name="ck_ew_voucher_receipt_authenticity",
        ),
        # A stored row IS the proof, so the three parts §46.3 asks for are
        # required by the database rather than by whoever writes it: the
        # verified signature, and both halves of a route that matched.
        CheckConstraint(
            f"authenticity = '{INBOUND_AUTHENTICITY_SIGNED}'",
            name="ck_ew_voucher_receipt_signed",
        ),
        CheckConstraint(
            "route_proven AND observed_sender_digest IS NOT NULL AND observed_recipient_digest IS NOT NULL",
            name="ck_ew_voucher_receipt_route_proven",
        ),
        Index("ix_ew_voucher_receipt_item_kind", "item_id", "kind"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    fact_key: Mapped[str] = mapped_column(String(64), nullable=False)
    kind: Mapped[str] = mapped_column(String(16), nullable=False)
    authenticity: Mapped[str] = mapped_column(String(24), nullable=False)

    item_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    attempt_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    batch_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    slot: Mapped[int] = mapped_column(Integer, nullable=False)
    provider_message_id: Mapped[str] = mapped_column(String(128), nullable=False)

    # The route the CALLBACK named, as one-way digests, proven equal to the one
    # the attempt froze. Never the values: this table holds no phone number and
    # no sender secret.
    observed_sender_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    observed_recipient_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    route_proven: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"))

    whatsapp_event_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    # Two different times, never merged: when we saw it, and when the event is
    # proven to have happened. The second is NULL when it cannot be proven.
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    event_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # Labels and shape facts only. No raw provider response, no body, no code.
    evidence: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


# ---------------------------------------------------------------------------
# §46.2 / §46.10 — the EasyWeek voucher follow-up PREVIEW contract (PR-22.2)
# ---------------------------------------------------------------------------
# A preview is a proposal, not a permission. Nothing here reserves or spends a
# send attempt, and nothing here is a claim: the execution ledger, its global
# single-attempt key and its terminal tombstones belong to PR-22.3. What these
# two tables do carry is the source/dedupe contract that stage will build on, so
# that it can be written without inventing identity after the fact.

FOLLOWUP_GROUP_READ = "read"
FOLLOWUP_GROUP_DELIVERED = "delivered_unread"
FOLLOWUP_GROUP_FAILED = "confirmed_failure"

FOLLOWUP_GROUPS = (
    FOLLOWUP_GROUP_READ,
    FOLLOWUP_GROUP_DELIVERED,
    FOLLOWUP_GROUP_FAILED,
)

# A slot of the original mailing that belongs to NONE of the three groups: never
# attempted, accepted and never heard of again, locally refused, timed out, or
# holding only an unproven failure fact. Deliberately NOT in ``FOLLOWUP_GROUPS``,
# because it is not selectable and not an audience — it is how a stored snapshot
# accounts for a slot it could prove nothing about, instead of filing that slot
# under a group whose evidence it does not have.
FOLLOWUP_GROUP_UNPROVEN = "unproven"

FOLLOWUP_ROW_GROUP_CODES = FOLLOWUP_GROUPS + (FOLLOWUP_GROUP_UNPROVEN,)

_FOLLOWUP_GROUP_SQL = ", ".join(f"'{value}'" for value in FOLLOWUP_ROW_GROUP_CODES)

# What a preview says about one source slot. ``unknown`` is its own answer and
# never folded into ``excluded``: "we could not establish it" is a different
# fact from "it does not qualify", and §46.5 requires an operator to see which.
FOLLOWUP_ROW_INCLUDED = "included"
FOLLOWUP_ROW_EXCLUDED = "excluded"
FOLLOWUP_ROW_UNKNOWN = "unknown"

FOLLOWUP_ROW_DECISIONS = (
    FOLLOWUP_ROW_INCLUDED,
    FOLLOWUP_ROW_EXCLUDED,
    FOLLOWUP_ROW_UNKNOWN,
)

_FOLLOWUP_ROW_DECISION_SQL = ", ".join(f"'{value}'" for value in FOLLOWUP_ROW_DECISIONS)


class EasyWeekVoucherFollowupPreview(Base):
    """One immutable version of a follow-up preview for one original batch.

    Immutable by construction: a row is written once and never updated. Looking
    at a preview again does not touch it, and an explicit recalculation inserts
    the NEXT version beside it — so an operator can always see what the earlier
    proposal actually said, and a later stage can tell which proposal it is
    acting on.

    It binds itself to the exact source it was computed from. ``batch_id`` plus
    ``source_frozen_digest`` name one frozen original mailing; ``stop_generation``
    names the stop counter that batch had when the evidence was read, so a
    preview built before a terminal operator STOP can never read as one built
    after it. ``checked_at`` and ``valid_until`` say when the proofs were taken
    and how long the contract is treated as current: a snapshot is not a
    standing permission to send.

    Nothing personal is stored. Reason codes, counts and one-way digests only —
    no phone number, no name, no voucher code, no rendered BODY, no raw provider
    identifier.
    """

    __tablename__ = "easyweek_voucher_followup_previews"

    __table_args__ = (
        # One version number per source batch, so two concurrent operators
        # cannot both believe they wrote version N.
        UniqueConstraint("batch_id", "version", name="uq_ew_followup_preview_batch_version"),
        CheckConstraint("version >= 1", name="ck_ew_followup_preview_version"),
        # §46.2 — the interval is the operator's explicit decision. No default is
        # invented anywhere, and a missing, zero or negative value cannot be
        # represented at all.
        CheckConstraint("min_interval_seconds > 0", name="ck_ew_followup_preview_interval_positive"),
        # §46.3 — an empty selection is not "all". A preview without a group is
        # not a narrower preview; it is not a preview.
        CheckConstraint("jsonb_array_length(selected_groups) >= 1", name="ck_ew_followup_preview_groups_present"),
        CheckConstraint(
            "included_count >= 0 AND excluded_count >= 0 AND unknown_count >= 0",
            name="ck_ew_followup_preview_counts_non_negative",
        ),
        CheckConstraint("valid_until > checked_at", name="ck_ew_followup_preview_validity_window"),
        Index("ix_ew_followup_preview_batch", "batch_id", "version"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    # -- the source this preview is about ----------------------------------
    #
    # Deliberately NO ``provider`` column, for the same reason
    # ``easyweek_voucher_production_stop_requests`` has none: this row names a
    # batch, and that batch already says which provider it belongs to. A second
    # copy here would be decoration that could disagree with it, and the §PR-3
    # boundary test guards exactly that.
    batch_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    campaign_run_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    company_id: Mapped[int] = mapped_column(Integer, nullable=False)
    location_uuid: Mapped[uuid.UUID] = mapped_column(PostgresUUID(as_uuid=True), nullable=False)
    # The original request schema (3 or 4) and the frozen digest of that exact
    # batch. A preview is only ever valid for the snapshot it read.
    source_request_schema_version: Mapped[str] = mapped_column(String(16), nullable=False)
    source_frozen_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    # The batch's stop counter when the evidence was read (§45.4).
    stop_generation: Mapped[int] = mapped_column(Integer, nullable=False)

    version: Mapped[int] = mapped_column(Integer, nullable=False)

    # -- what the operator chose -------------------------------------------
    selected_groups: Mapped[list] = mapped_column(JSONB, nullable=False)
    min_interval_seconds: Mapped[int] = mapped_column(BigInteger, nullable=False)

    # -- the exact message contract ----------------------------------------
    # Identity, language, category, parameter order and a digest of the BODY.
    # The text itself is a local contract; this records WHICH contract, never a
    # rendered message.
    template_code: Mapped[str] = mapped_column(String(64), nullable=False)
    meta_template_name: Mapped[str] = mapped_column(String(128), nullable=False)
    template_language: Mapped[str] = mapped_column(String(8), nullable=False)
    template_category: Mapped[str] = mapped_column(String(32), nullable=False)
    template_parameter_order: Mapped[list] = mapped_column(JSONB, nullable=False)
    template_body_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    # Did a live Meta read prove the APPROVED status in THIS evaluation?
    template_meta_verified: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))

    # -- how it was decided -------------------------------------------------
    contract_version: Mapped[str] = mapped_column(String(32), nullable=False)
    failure_policy_version: Mapped[str] = mapped_column(String(32), nullable=False)
    checked_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    valid_until: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    # -- what it found ------------------------------------------------------
    included_count: Mapped[int] = mapped_column(Integer, nullable=False)
    excluded_count: Mapped[int] = mapped_column(Integer, nullable=False)
    unknown_count: Mapped[int] = mapped_column(Integer, nullable=False)
    # Deterministic over the INCLUDED composition and the contract that produced
    # it, so a later stage can prove it is acting on this exact proposal.
    composition_digest: Mapped[str] = mapped_column(String(64), nullable=False)

    # -- who asked ----------------------------------------------------------
    operator_principal: Mapped[str] = mapped_column(String(128), nullable=False)

    # Safe aggregates only: reason histograms and counts.
    evidence: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class EasyWeekVoucherFollowupPreviewRow(Base):
    """One source slot as one preview version judged it.

    Every slot the selected groups reached is written down, included or not,
    because the excluded ones are the half an operator has to be able to read:
    §46.8 asks for counts by group AND the reasons, and a row that simply
    vanished would be indistinguishable from one nobody looked at.

    The uniqueness here is the source/dedupe contract PR-22.3 will claim
    against. Within one version an included recipient appears once per source
    slot, once per canonical customer UUID and once per original entitlement, so
    a duplicated card, a second representation of the same person or a re-read of
    the same voucher cannot become two follow-up messages later.
    """

    __tablename__ = "easyweek_voucher_followup_preview_rows"

    __table_args__ = (
        ForeignKeyConstraint(
            ["preview_id"],
            ["easyweek_voucher_followup_previews.id"],
            ondelete="CASCADE",
            name="fk_ew_followup_preview_row_preview",
        ),
        UniqueConstraint("preview_id", "source_item_id", name="uq_ew_followup_preview_row_item"),
        CheckConstraint(f"group_code IN ({_FOLLOWUP_GROUP_SQL})", name="ck_ew_followup_preview_row_group"),
        CheckConstraint(f"decision IN ({_FOLLOWUP_ROW_DECISION_SQL})", name="ck_ew_followup_preview_row_decision"),
        # An included row needs no reason; everything else must say why.
        CheckConstraint(
            f"(decision = '{FOLLOWUP_ROW_INCLUDED}') = (reason_code IS NULL)",
            name="ck_ew_followup_preview_row_reason_paired",
        ),
        # The dedupe contract, enforced by the database rather than by the
        # writer: one included row per person and per original entitlement.
        Index(
            "uq_ew_followup_preview_row_included_customer",
            "preview_id",
            "easyweek_customer_uuid",
            unique=True,
            postgresql_where=text(f"decision = '{FOLLOWUP_ROW_INCLUDED}'"),
        ),
        Index(
            "uq_ew_followup_preview_row_included_entitlement",
            "preview_id",
            "source_entitlement_digest",
            unique=True,
            postgresql_where=text(f"decision = '{FOLLOWUP_ROW_INCLUDED}'"),
        ),
        Index("ix_ew_followup_preview_row_preview", "preview_id", "decision"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    preview_id: Mapped[int] = mapped_column(BigInteger, nullable=False)

    # -- the immutable source links ----------------------------------------
    source_batch_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    source_item_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    source_slot: Mapped[int] = mapped_column(Integer, nullable=False)
    campaign_recipient_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    easyweek_customer_uuid: Mapped[uuid.UUID] = mapped_column(PostgresUUID(as_uuid=True), nullable=False)
    # A one-way handle over the ORIGINAL voucher artifact's identity — its HMAC
    # and key id — never the code and never a new entitlement.
    source_entitlement_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    # A one-way handle over the proven recipient, so a row can be compared
    # without storing a number.
    # NULL where the original attempt froze no route, which a historical slot
    # honestly did. An empty string would claim a digest that does not exist.
    recipient_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)
    # The proven moment the original attempt happened: the interval anchor.
    source_attempted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    group_code: Mapped[str] = mapped_column(String(24), nullable=False)
    decision: Mapped[str] = mapped_column(String(16), nullable=False)
    reason_code: Mapped[str | None] = mapped_column(String(64), nullable=True)

    # Safe facts only: booleans, reason labels and counts.
    evidence: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
