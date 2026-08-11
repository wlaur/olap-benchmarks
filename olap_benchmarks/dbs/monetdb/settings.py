from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    write_window_bytes: int | None = None
    wire_compression: Literal["none", "auto", "lz4"] = "auto"
    constrained_append: Literal["auto", "direct"] = "auto"
    gdk_debug: int | None = None

    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="OLAP_BENCHMARKS_MONETDB_",
        extra="ignore",
    )


SETTINGS = Settings.model_validate({})
