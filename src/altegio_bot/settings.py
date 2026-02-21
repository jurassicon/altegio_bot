from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', extra='ignore')

    app_name: str = 'altegio_bot'
    env: str = 'dev'

    database_url: str
    altegio_webhook_secret: str

    whatsapp_provider: str = 'dummy'
    allow_real_send: bool = False
    stop_worker_on_token_expired: bool = True

    whatsapp_access_token: str | None = None
    whatsapp_webhook_verify_token: str | None = None

    meta_wa_phone_number_id: str | None = None
    meta_waba_id: str | None = None

    whatsapp_graph_url: str = 'https://graph.facebook.com'
    whatsapp_api_version: str = 'v20.0'

    altegio_api_base_url: str = 'https://api.alteg.io/api/v1'
    altegio_api_accept: str = 'application/vnd.api.v2+json'
    altegio_partner_token: str | None = None
    altegio_user_token: str | None = None


settings = Settings()