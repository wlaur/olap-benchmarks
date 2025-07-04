from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    client_file_transfer: bool = True

    # JSON can also be read as pl.Object and pl.String, but pl.Struct is usually the most useful
    json_polars_dtype: Literal["string", "struct", "object"] = "struct"

    model_config = SettingsConfigDict(env_file=".env", env_prefix="TSDB_BENCHMARKS_MONETDB_", extra="ignore")


SETTINGS = Settings()  # type: ignore[call-arg]
