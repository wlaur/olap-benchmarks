import logging
import sys
from pathlib import Path
from typing import Annotated, Literal

from colorama import Fore, Style
from colorama import init as colorama_init
from pydantic import DirectoryPath
from pydantic_settings import BaseSettings, SettingsConfigDict

TableName = Annotated[str, "Table name"]

DatabaseName = Literal["monetdb", "clickhouse", "timescaledb", "duckdb"]
SuiteName = Literal["rtabench", "time_series"]
Operation = Literal["populate", "run"]

REPO_ROOT = Path(__file__).parent.parent.resolve()

MAIN_PROCESS_TITLE = "tsdb-benchmark-main"


class Settings(BaseSettings):
    input_data_directory: DirectoryPath
    results_directory: DirectoryPath

    database_directory: DirectoryPath
    temporary_directory: DirectoryPath

    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="TSDB_BENCHMARKS_",
        extra="ignore",
    )


SETTINGS = Settings()  # type: ignore[call-arg]


def setup_stdout_logging(level: int = logging.INFO) -> None:
    colorama_init()

    class ColoredFormatter(logging.Formatter):
        LEVEL_COLORS = {
            logging.DEBUG: Fore.CYAN,
            logging.INFO: Fore.GREEN,
            logging.WARNING: Fore.YELLOW,
            logging.ERROR: Fore.RED,
            logging.CRITICAL: Fore.MAGENTA + Style.BRIGHT,
        }

        def format(self, record: logging.LogRecord) -> str:
            color = self.LEVEL_COLORS.get(record.levelno, "")
            reset = Style.RESET_ALL
            message = super().format(record)
            return f"{color}{message}{reset}"

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)

    formatter = ColoredFormatter(
        "%(asctime)s.%(msecs)03d %(process)d %(levelname)s %(name)s %(threadName)s : %(message)s", "%H:%M:%S"
    )

    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()
    root.addHandler(handler)
