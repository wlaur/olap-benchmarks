from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import DirectoryPath


class Settings(BaseSettings):
    input_data_directory: DirectoryPath
    database_directory: DirectoryPath

    model_config = SettingsConfigDict(env_prefix="TSDB_BENCHMARKS_", extra="ignore")


SETTINGS = Settings()