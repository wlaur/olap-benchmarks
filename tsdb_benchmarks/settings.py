from typing import Annotated

from pydantic import DirectoryPath
from pydantic_settings import BaseSettings, SettingsConfigDict

DatabaseName = Annotated[str, "Database name"]
TableName = Annotated[str, "Table name"]


class Settings(BaseSettings):
    input_data_directory: DirectoryPath
    database_directory: DirectoryPath
    results_directory: DirectoryPath

    model_config = SettingsConfigDict(env_prefix="TSDB_BENCHMARKS_", extra="ignore")


SETTINGS = Settings()  # type: ignore[call-arg]
