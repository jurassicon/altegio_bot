from typing import Any

from pydantic import SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Hard allowlist of job types that may be routed to text inside the 24h window.
# This must only contain appointment / service notification types — never campaign
# or marketing job types — because the text-success path returns early before the
# shared campaign post-send backfill logic that runs at the bottom of _run_job_logic.
BOT_TEXT_INSIDE_24H_ALLOWED_JOB_TYPES: frozenset[str] = frozenset(
    {
        "record_created",
        "record_updated",
        "record_canceled",
        "reminder_24h",
        "reminder_2h",
    }
)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        # Отсутствующий файл pydantic-settings молча пропускает, поэтому
        # easyweek.env опционален. Правее — выше приоритет: значения из
        # easyweek.env перекрывают одноимённые из .env.
        env_file=(".env", "easyweek.env"),
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    app_name: str = "altegio_bot"
    env: str = "dev"

    database_url: str
    altegio_webhook_secret: str

    whatsapp_provider: str = "dummy"
    allow_real_send: bool = False
    stop_worker_on_token_expired: bool = False

    whatsapp_access_token: str = ""
    meta_wa_phone_number_id: str = ""
    meta_waba_id: str = ""

    whatsapp_graph_url: str = "https://graph.facebook.com"
    whatsapp_api_version: str = "v20.0"

    whatsapp_webhook_verify_token: str = ""

    whatsapp_allowed_phone_number_ids: str = ""
    meta_app_secret: str = ""

    altegio_api_base_url: str = "https://api.alteg.io/api/v1"
    altegio_api_accept: str = "application/vnd.api.v2+json"
    altegio_partner_token: str = ""
    altegio_user_token: str = ""
    loyalty_card_type_id: str = ""

    # Ops-cabinet access
    ops_token: str = ""
    ops_user: str = ""
    ops_pass: str = ""
    # Secret used to sign session cookies. Falls back to ops_pass when not set.
    ops_secret: str = ""

    # marketing_only | all
    wa_optout_policy: str = "marketing_only"

    # template | text | auto
    # template: always use Meta templates (except STOP/START acks)
    # text:     always use free-form text (dev/testing only)
    # auto:     templates for business-initiated, text for conversational
    whatsapp_send_mode: str = "auto"

    # Meta circuit breaker outage fallback.
    # closed means Meta sends are paused; open means sends are allowed.
    # The guard worker opens the circuit only after a successful Meta metadata
    # probe. Delivery retry below is separate and only handles WhatsApp
    # status=failed webhooks after Meta already accepted a message.
    meta_circuit_breaker_enabled: bool = True
    meta_circuit_probe_initial_delay_seconds: int = 300
    meta_circuit_probe_backoff_seconds: list[int] = [300, 600, 900, 1800]
    meta_circuit_probe_max_delay_seconds: int = 1800
    meta_circuit_pause_requeue_delay_seconds: int = 300
    meta_circuit_probe_timeout_seconds: int = 10
    meta_circuit_probe_lease_seconds: int = 60

    outbox_delivery_retry_enabled: bool = True

    @field_validator("meta_circuit_probe_backoff_seconds", mode="before")
    @classmethod
    def parse_meta_circuit_probe_backoff_seconds(cls, v: Any) -> list[int] | Any:
        if isinstance(v, str):
            raw = v.strip()
            if not raw:
                return []
            if raw.startswith("["):
                return v
            return [int(part.strip()) for part in raw.split(",") if part.strip()]
        return v

    # Minutes after which a "processing" job is considered stuck
    ops_stuck_minutes: int = 15

    # Warn if failed outbox messages in 24h exceed this number
    ops_failed_warning_threshold: int = 10

    # Local timezone for display (IANA name)
    ops_local_tz: str = "Europe/Berlin"

    # Temporary auto-suppression for wa error code 131026
    # (undeliverable recipient / WA unreachable).
    # After `threshold` failures within `window_days`, automated sends to
    # that phone are skipped and logged as status='canceled'.
    wa_131026_suppression_enabled: bool = True
    wa_131026_suppression_threshold: int = 2
    wa_131026_suppression_window_days: int = 14

    # Stricter marketing/follow-up suppression. On top of the transactional
    # 131026 threshold above, marketing job types (MARKETING_JOB_TYPES in the
    # outbox worker, incl. newsletter_new_clients_followup) are suppressed if the
    # phone has ANY undeliverable/suppression history within a longer cooldown:
    #   * a 131026 or 131049 WhatsApp delivery failure;
    #   * a previous suppressed_131026 / suppressed_131049 canceled outbox row.
    # A single prior occurrence is enough (conservative). Transactional reminders
    # keep the 14-day threshold rule above. Override: MARKETING_SUPPRESSION_*.
    marketing_suppression_enabled: bool = True
    marketing_suppression_cooldown_days: int = 90

    # Максимальное число параллельных CRM-запросов при сегментации кампании.
    # Защищает от перегрузки Altegio API. Переопределяется через env:
    #   CAMPAIGN_CRM_MAX_CONCURRENCY=8
    # Значение обязано быть >= 1; 0 или отрицательное — ошибка при старте.
    campaign_crm_max_concurrency: int = 8

    @field_validator("campaign_crm_max_concurrency")
    @classmethod
    def validate_campaign_crm_max_concurrency(cls, v: int) -> int:
        if v < 1:
            raise ValueError(
                f"campaign_crm_max_concurrency должен быть >= 1, получено {v}. "
                "Проверьте переменную окружения CAMPAIGN_CRM_MAX_CONCURRENCY."
            )
        return v

    # Auto-follow-up historical safety window (worker-only).
    # The automatic follow-up worker must not backfill very old due runs after
    # first deployment. If a run's followup_due_at (= completed_at +
    # followup_delay_days) is older than this many days, the worker stamps it
    # `skipped_historical` instead of planning/sending.
    # Manual /followup/plan and /followup/run-now endpoints are NOT affected.
    # Override via env: AUTO_FOLLOWUP_MAX_DUE_AGE_DAYS=7
    auto_followup_max_due_age_days: int = 7

    # Chatwoot integration
    chatwoot_enabled: bool = True
    chatwoot_base_url: str = ""
    chatwoot_api_token: str = ""
    chatwoot_inbox_id: int = 0
    chatwoot_account_id: int = 0
    chatwoot_webhook_secret: str = ""

    # Optional X-Forwarded-Proto value for outgoing Chatwoot API calls.
    # Empty (default) — header is not sent, behaviour unchanged.
    # "https" — required for verified internal Docker routes (e.g.
    # CHATWOOT_BASE_URL=http://rails:3000): Rails answers 301 to plain
    # internal HTTP without this header. Allowed values: "http", "https".
    # Invalid values never add the header (warning is logged instead),
    # so a typo cannot change request semantics.
    chatwoot_api_forwarded_proto: str = ""

    # Controls whether an inbound WhatsApp reply gets a visible text quote in the
    # Chatwoot message body, in addition to the native reply metadata sent via API.
    # fallback_only (default) — rely on Chatwoot's native reply UI when native
    #   metadata is sent (same-conversation target with a Chatwoot message id);
    #   add the visible quote only in fallback cases: bot/automation targets
    #   without a native Chatwoot id, cross-conversation targets, and missing
    #   targets. (A bot/automation row that does carry a native id in the same
    #   conversation is native, not fallback.)
    # always — always add a visible body quote, even when native metadata is sent.
    chatwoot_reply_context_visible_quote_mode: str = "fallback_only"

    # Meta-first cutover: route Chatwoot operator replies through
    # altegio_bot → Meta instead of relying on Chatwoot WhatsApp inbox.
    # Default False (safe). Enable only after verifying no double-send risk.
    chatwoot_operator_relay_enabled: bool = False

    # One provider-scoped JSON mapping for both Chatwoot directions:
    # inbox_id -> {provider, company_id}. Required when multiple tenants share
    # one WA phone_number_id. Integer-only values are recognized as the legacy
    # schema but fail closed for branch routing because they cannot prove CRM
    # provider identity.
    # Example: {"8":{"provider":"altegio","company_id":758285}}
    # If empty (default) — routing falls back to phone_number_id only.
    # If non-empty and inbox_id not found — relay is fail-closed.
    chatwoot_inbox_company_map: str = "{}"

    # ---------------------------------------------------------------------------
    # Operator relay closed-window mode (24h WhatsApp customer service window)
    # ---------------------------------------------------------------------------
    # Controls behaviour when the 24h Meta customer service window is closed.
    #
    # private_note_only (default) — blocks the Meta send, adds a Chatwoot private
    #   note to the operator, and creates a canceled OutboxMessage for audit.
    #   Safe default: no risk of violating Meta's session rules.
    #
    # reopen_template — sends an approved Meta template instead of the free-form
    #   text, allowing the operator to re-engage. Requires REOPEN_TEMPLATE_NAME.
    chatwoot_operator_closed_window_mode: str = "private_note_only"
    # Name of the approved Meta template to send when mode=reopen_template.
    # Must be non-empty when chatwoot_operator_closed_window_mode=reopen_template.
    chatwoot_operator_reopen_template_name: str = ""
    chatwoot_operator_reopen_template_language: str = "de"
    # Controls which params are sent to the template.
    # none         — params = []
    # contact_name — params = [contact_name]
    chatwoot_operator_reopen_template_param_mode: str = "contact_name"
    # When True, adds a private Chatwoot note explaining the window was closed
    # and the original message was not delivered directly.
    chatwoot_operator_reopen_private_note_enabled: bool = True

    # An operator-relay Outbox left in 'sending' longer than this is treated as
    # a stale/interrupted attempt: recovery moves it to 'unknown' (manual review,
    # never an automatic resend). Kept generous so a slow-but-live Meta call is
    # not misclassified; tests lower it to force the stale path.
    chatwoot_operator_relay_stale_sending_seconds: int = 900

    # An operator-relay event stuck in 'processing' with NO Outbox for longer
    # than this had its durable prepare interrupted before any provider side
    # effect; recovery returns it to 'received' so the next poll re-prepares it.
    # Must be > 0 in production so a currently-processing event is never reset.
    chatwoot_operator_relay_stale_processing_seconds: int = 300

    # Bounded batch size for each operator-relay recovery scan (stale processing,
    # queued resume, stale sending), so recovery never becomes a tight full scan.
    chatwoot_operator_relay_recovery_batch_size: int = 50

    # How often the production poll loop runs the operator-relay recovery cycle
    # (in addition to once at worker startup).
    chatwoot_operator_relay_recovery_interval_seconds: int = 60

    @field_validator("chatwoot_reply_context_visible_quote_mode")
    @classmethod
    def validate_reply_context_visible_quote_mode(cls, v: str) -> str:
        allowed = {"fallback_only", "always"}
        if v not in allowed:
            raise ValueError(
                f"chatwoot_reply_context_visible_quote_mode must be one of "
                f"{sorted(allowed)!r}, got {v!r}. "
                "Check CHATWOOT_REPLY_CONTEXT_VISIBLE_QUOTE_MODE."
            )
        return v

    # PR-7.2. Where a customer's inbound WhatsApp message is shown, and whether
    # an operator reply typed in General may resolve a branch sender.
    #   context  — today's behaviour: reply/reaction context decides, otherwise
    #              General; a General operator reply stays blocked.
    #   affinity — context first, then the proven-tenant resolver; only real
    #              NO_EVIDENCE falls back to General.
    #   general  — one-inbox display rollback. Inbound is shown in General, but
    #              an operator reply still has to PROVE a tenant before a sender
    #              is chosen; it never means "pick the first sender".
    # Consumed by altegio-whatsapp-inbox-worker only.
    chatwoot_inbound_routing_mode: str = "context"

    @field_validator("chatwoot_inbound_routing_mode")
    @classmethod
    def validate_inbound_routing_mode(cls, v: str) -> str:
        allowed = {"context", "affinity", "general"}
        if v not in allowed:
            # Fail fast: a silent fallback would route customer messages to a
            # branch nobody chose, and the operator would never know.
            raise ValueError(
                f"chatwoot_inbound_routing_mode must be one of "
                f"{sorted(allowed)!r}, got {v!r}. "
                "Check CHATWOOT_INBOUND_ROUTING_MODE."
            )
        return v

    @field_validator("chatwoot_operator_closed_window_mode")
    @classmethod
    def validate_closed_window_mode(cls, v: str) -> str:
        allowed = {"private_note_only", "reopen_template"}
        if v not in allowed:
            raise ValueError(
                f"chatwoot_operator_closed_window_mode must be one of "
                f"{sorted(allowed)!r}, got {v!r}. "
                "Check CHATWOOT_OPERATOR_CLOSED_WINDOW_MODE."
            )
        return v

    @field_validator("chatwoot_operator_reopen_template_param_mode")
    @classmethod
    def validate_reopen_param_mode(cls, v: str) -> str:
        allowed = {"none", "contact_name"}
        if v not in allowed:
            raise ValueError(
                f"chatwoot_operator_reopen_template_param_mode must be one of "
                f"{sorted(allowed)!r}, got {v!r}. "
                "Check CHATWOOT_OPERATOR_REOPEN_TEMPLATE_PARAM_MODE."
            )
        return v

    @model_validator(mode="after")
    def validate_reopen_template_config(self) -> "Settings":
        if self.chatwoot_operator_closed_window_mode == "reopen_template":
            if not self.chatwoot_operator_reopen_template_name:
                raise ValueError(
                    "CHATWOOT_OPERATOR_REOPEN_TEMPLATE_NAME must be non-empty "
                    "when CHATWOOT_OPERATOR_CLOSED_WINDOW_MODE=reopen_template"
                )
            if not self.chatwoot_operator_reopen_template_language:
                raise ValueError(
                    "CHATWOOT_OPERATOR_REOPEN_TEMPLATE_LANGUAGE must be non-empty "
                    "when CHATWOOT_OPERATOR_CLOSED_WINDOW_MODE=reopen_template"
                )
        return self

    # ---------------------------------------------------------------------------
    # WhatsApp promo / secret-word funnel
    # ---------------------------------------------------------------------------
    # Comma-separated secret words that trigger the promo flow.
    # Matched after normalisation: strip, lowercase, punctuation removed.
    promo_secret_words: str = "aktion,angebot,rabatt"
    # Internal campaign identifier stored on every PromoLead row.
    promo_campaign_name: str = "welcome_discount"
    # Discount value shown to the customer.
    promo_discount_amount: float = 15.0
    # 'fixed' (Euro) | 'percent'
    promo_discount_type: str = "fixed"
    # Expiration mode: 'issued_plus_days' | 'calendar_month'
    promo_validity_mode: str = "issued_plus_days"
    promo_validity_days: int = 30
    # Online booking URL included in every promo reply.
    promo_booking_url: str = "https://n813709.alteg.io/"
    # Gate for the full promo lead funnel (PromoLead creation + loyalty API).
    # Default False: sends a safe informational reply only, no DB lead row.
    # Enable only after loyalty API integration is production-ready.
    promo_lead_funnel_enabled: bool = False
    # Optional external new-client eligibility check via Altegio CRM history.
    # Default False: keep local-only behaviour and make no Altegio API call.
    # When True, any Altegio visit/record for the WhatsApp phone blocks issuance.
    promo_check_new_client_in_altegio: bool = False
    # Optional async promo eligibility flow.
    # Default False keeps the current sync MVP-1 promo lead behaviour.
    promo_async_eligibility_check_enabled: bool = False

    # ---------------------------------------------------------------------------
    # Promo loyalty card issuance (requires promo_lead_funnel_enabled=True)
    # ---------------------------------------------------------------------------
    # Altegio card type ID to assign to promo loyalty cards.
    promo_loyalty_card_type_id: str = ""
    # Altegio discount program ID stored on the PromoLead for future visit apply.
    promo_discount_program_id: str = ""
    # JSON mapping company_id (str) → location_id (int).
    # Example: PROMO_LOCATION_ID_BY_COMPANY={"1": 12345, "42": 67890}
    promo_location_id_by_company: str = "{}"
    # Gate for loyalty card issuance via Altegio API.
    # Default False: PromoLead created but no Altegio loyalty card issued.
    # Requires promo_lead_funnel_enabled=True and all promo_loyalty_* settings set.
    promo_issue_loyalty_card_enabled: bool = False
    # Smoke-test gate for the confirmed card issuance endpoint:
    #   POST /loyalty/cards/{location_id}  (response: id, number)
    # Default False: _attempt_loyalty_card_issue() is blocked until True.
    # Set True only after a successful smoke test against a real Altegio account
    # in a non-production environment.
    # This flag is SEPARATE from promo_altegio_client_api_verified, which guards
    # the unconfirmed client lookup/create endpoints.
    promo_loyalty_card_api_verified: bool = False
    # Gate for the UNCONFIRMED client lookup/create endpoints:
    #   GET  /clients/{company_id}?phone=...
    #   POST /clients/{company_id}
    # Default False: get_or_create_altegio_client() raises before making any HTTP
    # call until this is explicitly set True.
    # Set True only after manually verifying endpoint shape and payload in a
    # non-production environment.
    promo_altegio_client_api_verified: bool = False

    # ---------------------------------------------------------------------------
    # Promo discount application to visits (requires promo_lead_funnel_enabled=True)
    # ---------------------------------------------------------------------------
    # Master gate. Default False: no discount is applied to bookings.
    # Enable only after promo_apply_discount_api_verified is also True.
    promo_apply_discount_enabled: bool = False
    # Smoke-test gate for discount-apply API calls.
    # Default False: API call is blocked even when promo_apply_discount_enabled=True.
    # Set True only after completing a smoke test in a non-production environment.
    #
    # record_price_override mode: gates PUT /record (smoke-tested May 2026 ✓).
    # loyalty_program mode:       gates POST /visit/loyalty/apply_discount_program
    #                             (UNCONFIRMED endpoint — source: developer discussion).
    promo_apply_discount_api_verified: bool = False
    # Comma-separated Altegio service IDs eligible for the promo discount.
    # If empty: discount is never applied automatically (fail-closed).
    # Example: PROMO_ALLOWED_SERVICE_IDS=12345,67890
    promo_allowed_service_ids: str = ""
    # Discount-apply implementation mode.
    # 'record_price_override': PUT /record to change service price directly.
    #   This is the confirmed-working approach (smoke-tested May 2026).
    # 'loyalty_program': legacy POST /visit/loyalty/apply_discount_program endpoint
    #   (kept for backward compatibility with existing tests and smoke scripts).
    # Any other value raises ValueError at startup.
    promo_apply_mode: str = "record_price_override"
    # Gate for the existing-booking apply automation.
    # Default False: promo_apply_existing_booking jobs are never created.
    # Requires promo_apply_discount_enabled=True to have any effect when True.
    promo_apply_existing_booking_enabled: bool = False

    # ---------------------------------------------------------------------------
    # Network-aware promo lead application
    # ---------------------------------------------------------------------------
    # When True: if no same-company PromoLead is found for a booking, search for
    # an active lead across all companies listed in promo_network_company_ids.
    # Default False: preserves the existing same-company-only behaviour.
    promo_network_apply_enabled: bool = False
    # Comma-separated list of Altegio company IDs allowed for cross-company apply.
    # Both the lead's company and the record's company must be in this list.
    # If empty while promo_network_apply_enabled=True: cross-company apply is
    # fail-closed (no discount applied).
    # Example: PROMO_NETWORK_COMPANY_IDS=758285,1271200
    promo_network_company_ids: str = ""

    @field_validator("promo_apply_mode")
    @classmethod
    def validate_promo_apply_mode(cls, v: str) -> str:
        allowed = {"record_price_override", "loyalty_program"}
        if v not in allowed:
            raise ValueError(
                f"promo_apply_mode must be one of {sorted(allowed)!r}, got {v!r}. "
                "Check the PROMO_APPLY_MODE environment variable."
            )
        return v

    # Human-readable list of services eligible for the promo discount.
    # Shown in issued/already-issued WhatsApp replies to guide the customer.
    # Leave empty → service block omitted (backward-compatible).
    # Example: PROMO_ALLOWED_SERVICES_DISPLAY_TEXT=Haarschnitt, Coloration, Keratin
    promo_allowed_services_display_text: str = ""

    # Publicly accessible image URLs for newsletter template IMAGE HEADER components.
    # Meta Cloud API requires a permanent URL it can fetch and cache at send time.
    # Leave empty → worker fails the job fast (no silent blank-header send).
    meta_newsletter_monthly_header_image_url: str = ""
    meta_newsletter_followup_header_image_url: str = ""

    # ---------------------------------------------------------------------------
    # Bot templates: send text inside open 24h WhatsApp customer window
    # ---------------------------------------------------------------------------
    # When enabled, bot notifications of whitelisted job types are sent as
    # free-form text if the customer wrote within the last 24 hours.
    # Otherwise Meta templates are used as before.
    # Default False — safe: no behaviour change until explicitly enabled.
    bot_template_text_inside_24h_enabled: bool = False
    # Comma-separated job types eligible for text-inside-24h routing.
    # Must be non-empty when bot_template_text_inside_24h_enabled=True.
    bot_template_text_inside_24h_job_types: str = (
        "record_created,record_updated,record_canceled,reminder_24h,reminder_2h"
    )
    # When True: if the text send fails with a deterministic Meta policy/window
    # error, fall back to the original Meta template automatically.
    # When False: no automatic fallback — use normal retry behaviour.
    bot_template_text_inside_24h_fallback_enabled: bool = True

    @model_validator(mode="after")
    def validate_bot_text_inside_24h_config(self) -> "Settings":
        tokens = [t.strip() for t in self.bot_template_text_inside_24h_job_types.split(",") if t.strip()]
        if self.bot_template_text_inside_24h_enabled and not tokens:
            raise ValueError(
                "BOT_TEMPLATE_TEXT_INSIDE_24H_JOB_TYPES must be non-empty "
                "when BOT_TEMPLATE_TEXT_INSIDE_24H_ENABLED=true"
            )
        # Hard allowlist — validate always so misconfiguration is caught before rollout,
        # regardless of whether the feature is currently enabled.
        unsupported = set(tokens) - BOT_TEXT_INSIDE_24H_ALLOWED_JOB_TYPES
        if unsupported:
            raise ValueError(
                f"BOT_TEMPLATE_TEXT_INSIDE_24H_JOB_TYPES contains unsupported job types: "
                f"{sorted(unsupported)!r}. "
                f"Allowed: {sorted(BOT_TEXT_INSIDE_24H_ALLOWED_JOB_TYPES)!r}"
            )
        return self

    # ---------------------------------------------------------------------------
    # EasyWeek integration (PR-1: сырой capture вебхуков, без обработки)
    # ---------------------------------------------------------------------------
    # Мастер-флаг поверхности. False (по умолчанию) — POST /webhooks/easyweek
    # отвечает 404: до go-live эндпоинт неотличим от несуществующего маршрута.
    easyweek_enabled: bool = False
    # Общий секрет из query-параметра ?token= в URL вебхука. Пустое значение
    # держит эндпоинт закрытым (403) даже при easyweek_enabled=true — fail-closed.
    easyweek_webhook_secret: str = ""

    # --- PR-2: read-only Public API v2 (клиент + операторская проба) ----------
    # Все четыре значения намеренно имеют безопасные пустые/дефолтные значения:
    # easyweek.env опционален, поэтому его отсутствие НЕ должно ломать импорт
    # приложения, API-контейнер или Altegio-воркеры. Наличие ключа и slug
    # проверяется только в момент создания клиента / запуска пробы.
    #
    # Bearer-ключ. SecretStr, а не str: обычный repr/str модели настроек попадает
    # в дампы конфигурации, трейсбеки и диагностический вывод, и обычная строка
    # утекла бы туда целиком. SecretStr печатается как '**********', а реальное
    # значение достаётся только явным .get_secret_value() в клиенте.
    easyweek_api_key: SecretStr = SecretStr("")
    # Значение обязательного заголовка ``Workspace``.
    easyweek_workspace_slug: str = ""
    # Pinned base URL публичного API v2 (см. INTEGRATION_PLAN §1.1).
    easyweek_api_base_url: str = "https://my.easyweek.io/api/public/v2"
    # --- PR-4: normalizer / easyweek_inbox_worker ----------------------------
    # THREE independent gates, all fail-closed by default. They are separate on
    # purpose: production already runs with EASYWEEK_ENABLED=true so the capture
    # endpoint works, and a worker gated on that same flag would start chewing
    # through the whole captured backlog the moment PR-4 is deployed.
    #
    #   easyweek_enabled                -> ONLY the public capture endpoint
    #   easyweek_processing_enabled     -> ONLY claiming/normalising captured rows
    #   easyweek_notifications_enabled  -> ONLY creating EasyWeek MessageJob rows
    #
    # Turning processing off never turns capture off: deliveries keep being
    # stored, they simply stay `captured` until processing is enabled again.
    easyweek_processing_enabled: bool = False
    # Creating queue-consumable EasyWeek jobs. Production default stays false
    # until PR-6; with it off the normalizer still keeps Client/Record current.
    easyweek_notifications_enabled: bool = False

    # PR-7.1: exact EasyWeek service-category allowlist as a JSON array of
    # strings. Parsing is deliberately deferred to the shared eligibility
    # helper so malformed input suppresses EasyWeek jobs without preventing the
    # API or the shared outbox worker from starting. Empty permits nothing.
    easyweek_allowed_service_categories: str = ""

    # Strict JSON registry of every EasyWeek branch this deployment owns.
    # Parsed at the security boundary by ``easyweek_locations``; malformed and
    # empty maps both keep processing off, while remaining distinguishable.
    easyweek_location_map: str = "{}"

    easyweek_inbox_worker_poll_sec: float = 1.0

    # --- PR-5: outbox rendering ----------------------------------------------
    # Comma-separated hosts every registry booking page is allowed to point at.
    #
    # Empty by default, and empty REJECTS EVERYTHING rather than allowing
    # everything: until the approved Durlach host is confirmed, a typo in the
    # booking page would otherwise pass every syntactic check and reach a
    # customer as the link they tap after a cancellation. Fail-closed stops the
    # activation instead; see easyweek_policy.validate_static_booking_page.
    # Example: EASYWEEK_BOOKING_PAGE_ALLOWED_HOSTS=book.example.com
    easyweek_booking_page_allowed_hosts: str = ""

    # Language used to look up EasyWeek message templates. Separate from the
    # Altegio per-company map: EasyWeek has its own location and its own
    # approved templates, and must not inherit an Altegio branch's language.
    easyweek_default_language: str = "de"

    # ---------------------------------------------------------------------------
    # Worker polling intervals
    # ---------------------------------------------------------------------------
    # How long each worker sleeps (in seconds) when no events/jobs are found.
    # Valid range: 0.05 – 60.0.  Override via env to reduce latency in prod:
    #   INBOX_WORKER_POLL_SEC=0.2
    #   OUTBOX_WORKER_POLL_SEC=0.2
    #   WHATSAPP_INBOX_WORKER_POLL_SEC=0.5
    inbox_worker_poll_sec: float = 1.0
    outbox_worker_poll_sec: float = 1.0
    whatsapp_inbox_worker_poll_sec: float = 1.0

    @field_validator(
        "inbox_worker_poll_sec",
        "outbox_worker_poll_sec",
        "whatsapp_inbox_worker_poll_sec",
        "easyweek_inbox_worker_poll_sec",
    )
    @classmethod
    def validate_worker_poll_sec(cls, v: float) -> float:
        if v < 0.05 or v > 60.0:
            raise ValueError(
                f"Worker poll_sec must be >= 0.05 and <= 60.0, "
                f"got {v}. Check INBOX_WORKER_POLL_SEC, "
                f"OUTBOX_WORKER_POLL_SEC, or "
                f"WHATSAPP_INBOX_WORKER_POLL_SEC."
            )
        return v


settings = Settings()
