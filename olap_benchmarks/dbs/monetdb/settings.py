from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # do not use binary fetch as default, this requires that the schema is supplied beforehand
    # otherwise it will fetch a single row to determine the schema (this is not suitable for benchmarks)
    default_fetch_method: Literal["binary", "pymonetdb"] = "pymonetdb"

    # set to False to use copy ... on server (for binary export and csv or binary import)
    # this is faster if the client and server are on the same host
    client_file_transfer: bool = True

    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="OLAP_BENCHMARKS_MONETDB_",
        extra="ignore",
    )


SETTINGS = Settings()  # type: ignore[call-arg]
