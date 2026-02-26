from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        case_sensitive=False,
        extra='ignore',
    )

    app_name: str = 'altegio_bot'
    env: str = 'dev'

    database_url: str
    altegio_webhook_secret: str

    whatsapp_provider: str = 'dummy'
    allow_real_send: bool = False
    stop_worker_on_token_expired: bool = False

    whatsapp_access_token: str = ''
    meta_wa_phone_number_id: str = ''
    meta_waba_id: str = ''

    whatsapp_graph_url: str = 'https://graph.facebook.com'
    whatsapp_api_version: str = 'v20.0'

    whatsapp_webhook_verify_token: str = ''

    whatsapp_allowed_phone_number_ids: str = ''
    meta_app_secret: str = ''

    altegio_api_base_url: str = 'https://api.alteg.io/api/v1'
    altegio_api_accept: str = 'application/vnd.api.v2+json'
    altegio_partner_token: str = ''
    altegio_user_token: str = ''

    # Ops-cabinet access
    ops_token: str = ''
    ops_user: str = ''
    ops_pass: str = ''

    # marketing_only | all
    wa_optout_policy: str = 'marketing_only'

    # Minutes after which a "processing" job is considered stuck
    ops_stuck_minutes: int = 15

    # Warn if failed outbox messages in 24h exceed this number
    ops_failed_warning_threshold: int = 10

    # Local timezone for display (IANA name)
    ops_local_tz: str = 'Europe/Berlin'


settings = Settings()