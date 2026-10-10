from functools import cache
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict

from ...settings import ENV_FILE


class MonetDBSettings(BaseSettings):
    write_window_bytes: int | None = None
    wire_compression: Literal["none", "auto", "lz4"] = "auto"
    constrained_append: Literal["auto", "direct"] = "auto"

    model_config = SettingsConfigDict(
        env_file=ENV_FILE,
        env_prefix="OLAP_BENCHMARKS_MONETDB_",
        extra="ignore",
    )


@cache
def get_monetdb_settings() -> MonetDBSettings:
    return MonetDBSettings.model_validate({})
