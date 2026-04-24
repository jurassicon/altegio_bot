from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
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

    # Chatwoot integration
    chatwoot_enabled: bool = True
    chatwoot_base_url: str = ""
    chatwoot_api_token: str = ""
    chatwoot_inbox_id: int = 0
    chatwoot_account_id: int = 0
    chatwoot_webhook_secret: str = ""

    # Meta-first cutover: route Chatwoot operator replies through
    # altegio_bot → Meta instead of relying on Chatwoot WhatsApp inbox.
    # Default False (safe). Enable only after verifying no double-send risk.
    chatwoot_operator_relay_enabled: bool = False

    # JSON mapping Chatwoot inbox_id -> company_id for operator relay.
    # Required when multiple company_ids share the same WA phone_number_id.
    # Example: CHATWOOT_INBOX_COMPANY_MAP={"8": 758285, "7": 1271200}
    # If empty (default) — routing falls back to phone_number_id only.
    # If non-empty and inbox_id not found — relay is fail-closed.
    chatwoot_inbox_company_map: str = "{}"


settings = Settings()
