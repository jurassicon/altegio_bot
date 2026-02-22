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

    altegio_api_base_url: str = 'https://api.alteg.io/api/v1'
    altegio_api_accept: str = 'application/vnd.api.v2+json'
    altegio_partner_token: str = ''
    altegio_user_token: str = ''


settings = Settings()
