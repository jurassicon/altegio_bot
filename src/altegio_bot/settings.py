from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8"
    )

    app_name: str = "altegio_bot"
    env: str = "dev"

    database_url: str
    altegio_webhook_secret: str

    whatsapp_provider: str = 'dummy'
    allow_real_send: bool = False

    whatsapp_access_token: str = ''
    meta_wa_phone_number_id: str = ''
    meta_waba_id: str = ''

    whatsapp_graph_url: str = 'https://graph.facebook.com'
    whatsapp_api_version: str = 'v20.0'


settings = Settings()
