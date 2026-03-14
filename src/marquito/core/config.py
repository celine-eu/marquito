from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="MARQUITO_", env_file=".env")

    # Database
    db_host: str = "172.17.0.1"
    db_port: int = 45432
    db_name: str = "marquito"
    db_user: str = "marquito"
    db_password: str = "marquito"

    @property
    def database_url(self) -> str:
        return (
            f"postgresql+asyncpg://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )

    @property
    def sync_database_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )

    # API
    api_title: str = "Marquito"
    api_description: str = "Open source metadata service for data pipelines"
    api_version: str = "0.50.0"

    # Pagination
    default_limit: int = 100
    max_limit: int = 1000


settings = Settings()
