from typing import Annotated

from pydantic import DirectoryPath
from pydantic_settings import BaseSettings, SettingsConfigDict

DatabaseName = Annotated[str, "Database name"]
TableName = Annotated[str, "Table name"]


class Settings(BaseSettings):
    input_data_directory: DirectoryPath
    database_directory: DirectoryPath
    temporary_directory: DirectoryPath

    results_directory: DirectoryPath

    insert_iterations: int = 1
    query_iterations: int = 10

    append_iterations: int = 1
    upsert_iterations: int = 1

    model_config = SettingsConfigDict(env_prefix="TSDB_BENCHMARKS_", extra="ignore")


SETTINGS = Settings()  # type: ignore[call-arg]
