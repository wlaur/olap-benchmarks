from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # binary fetch has a slight overhead (needs to infer the schema via PREPARE ... and process temporary files)
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
