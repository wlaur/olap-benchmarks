from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # do not use binary fetch as default, this requires that the schema is supplied beforehand
    # otherwise it will fetch a single row to determine the schema (this is not suitable for benchmarks)
    default_fetch_method: Literal["binary", "pymonetdb"] = "pymonetdb"

    # file transfer over http is not comparable with insert methods for other databases
    client_file_transfer: bool = False

    # JSON can also be read as pl.Object and pl.String, but pl.Struct is usually the most useful
    json_polars_dtype: Literal["string", "struct", "object"] = "struct"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="OLAP_BENCHMARKS_MONETDB_",
        extra="ignore",
    )


SETTINGS = Settings()  # type: ignore[call-arg]
