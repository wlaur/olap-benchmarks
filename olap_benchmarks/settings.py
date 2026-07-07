import logging
import sys
from pathlib import Path
from typing import Annotated, Literal, get_args

from colorama import Fore, Style
from colorama import init as colorama_init
from pydantic import DirectoryPath
from pydantic_settings import BaseSettings, SettingsConfigDict

TableName = Annotated[str, "Table name"]

DatabaseName = Literal[
    "monetdb",
    "clickhouse",
    "timescaledb",
    "duckdb",
    "questdb",
    "postgres",
    "starrocks",
]

SuiteName = Literal["rtabench", "time_series", "clickbench", "kaggle_airbnb", "tpc_h", "tpc_ds"]
Operation = Literal["populate", "select", "mutate"]
Revision = Annotated[str, "Results database revision"]

type DatabaseArg = DatabaseName | Literal["all"]
type SuiteArg = SuiteName | Literal["all"]

DEFAULT_SUITE_SCALE_FACTORS: dict[SuiteName, int] = {
    "rtabench": 1,
    "time_series": 1,
    "clickbench": 1,
    "kaggle_airbnb": 1,
    "tpc_h": 10,
    "tpc_ds": 1,
}

SCALE_FACTOR_SUITES: frozenset[SuiteName] = frozenset({"time_series", "tpc_h", "tpc_ds"})

assert set(DEFAULT_SUITE_SCALE_FACTORS) == set(get_args(SuiteName))


def resolve_suite_scale_factor(suite: SuiteName, scale_factor: int | None = None) -> int:
    resolved_scale_factor = DEFAULT_SUITE_SCALE_FACTORS[suite] if scale_factor is None else scale_factor

    if resolved_scale_factor < 1:
        raise ValueError(f"Suite scale factor must be >= 1 for {suite}: {resolved_scale_factor}")

    if suite not in SCALE_FACTOR_SUITES and resolved_scale_factor != 1:
        raise ValueError(f"Suite {suite} has a fixed scale factor of 1")

    return resolved_scale_factor


def format_suite_data_directory_name(suite: SuiteName, scale_factor: int) -> str:
    scale_factor = resolve_suite_scale_factor(suite, scale_factor)
    if suite in SCALE_FACTOR_SUITES:
        return f"{suite}_sf{scale_factor}"
    return suite


def get_suite_scale_factor(suite: SuiteName) -> int:
    scale_factor = resolve_suite_scale_factor(suite)
    if scale_factor < 1:
        raise ValueError(f"Suite scale factor must be >= 1 for {suite}: {scale_factor}")
    return scale_factor


def resolve_dbs(arg: DatabaseArg) -> list[DatabaseName]:
    if arg == "all":
        return list(get_args(DatabaseName))
    return [arg]


def resolve_suites(arg: SuiteArg) -> list[SuiteName]:
    if arg == "all":
        return list(get_args(SuiteName))
    return [arg]


REPO_ROOT = Path(__file__).parent.parent.resolve()

MAIN_PROCESS_TITLE = "olap-benchmarks-main"


class Settings(BaseSettings):
    input_data_directory: DirectoryPath
    results_directory: DirectoryPath

    database_directory: Path
    temporary_directory: DirectoryPath

    system: str

    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="OLAP_BENCHMARKS_",
        extra="ignore",
    )


SETTINGS = Settings.model_validate({})


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
