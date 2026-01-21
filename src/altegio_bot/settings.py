from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8"
    )

    app_name: str = "altegio_bot"
    env: str = "dev"

    database_url: str
    altegio_webhook_secret: str


settings = Settings()
