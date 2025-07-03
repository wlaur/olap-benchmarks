from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    client_file_transfer: bool = True

    model_config = SettingsConfigDict(env_file=".env", env_prefix="TSDB_BENCHMARKS_MONETDB_", extra="ignore")


SETTINGS = Settings()  # type: ignore[call-arg]
