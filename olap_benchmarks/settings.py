import logging
import sys
from pathlib import Path
from typing import Annotated, Literal, cast, get_args

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
    "polars",
    "questdb",
    "postgres",
    "starrocks",
    "doris",
]

SuiteName = Literal["rtabench", "time_series", "clickbench", "jsonbench", "kaggle_airbnb", "tpc_h", "tpc_ds"]
Operation = Literal["populate", "select", "mutate", "concurrent"]
Revision = Annotated[str, "Results database revision"]
ContainerPlatform = Literal["linux/amd64", "linux/arm64"]

type DatabaseArg = DatabaseName | Literal["all"]
type SuiteArg = SuiteName | Literal["all"]
SuitePublicRole = Literal["benchmark", "smoke"]

SUITE_NAMES = cast(tuple[SuiteName, ...], get_args(SuiteName))

SUITE_DISPLAY_ORDER: tuple[SuiteName, ...] = (
    "time_series",
    "rtabench",
    "clickbench",
    "jsonbench",
    "kaggle_airbnb",
    "tpc_h",
    "tpc_ds",
)

DEFAULT_SUITE_SCALE_FACTORS: dict[SuiteName, int] = {
    "rtabench": 1,
    "time_series": 1,
    "clickbench": 1,
    "jsonbench": 10,
    "kaggle_airbnb": 1,
    "tpc_h": 10,
    "tpc_ds": 1,
}

TIME_SERIES_SCALE_FACTORS = (1, 10)
JSONBENCH_SCALE_FACTORS = (10,)
TPCH_SCALE_FACTORS = (10, 50)

ALL_SUITE_SCALE_FACTORS: dict[SuiteName, tuple[int, ...]] = {
    suite: (scale_factor,) for suite, scale_factor in DEFAULT_SUITE_SCALE_FACTORS.items()
}
ALL_SUITE_SCALE_FACTORS["time_series"] = TIME_SERIES_SCALE_FACTORS
ALL_SUITE_SCALE_FACTORS["jsonbench"] = JSONBENCH_SCALE_FACTORS
ALL_SUITE_SCALE_FACTORS["tpc_h"] = TPCH_SCALE_FACTORS

SCALE_FACTOR_SUITES: frozenset[SuiteName] = frozenset({"time_series", "jsonbench", "tpc_h", "tpc_ds"})

SUITE_LABELS: dict[SuiteName, str] = {
    "rtabench": "RTABench",
    "time_series": "Time Series",
    "clickbench": "ClickBench",
    "jsonbench": "JSONBench",
    "kaggle_airbnb": "Kaggle Airbnb",
    "tpc_h": "TPC-H",
    "tpc_ds": "TPC-DS",
}

SUITE_NAV_LABELS: dict[SuiteName, str] = SUITE_LABELS.copy()

SUITE_OPERATIONS: dict[SuiteName, tuple[Operation, ...]] = dict.fromkeys(SUITE_NAMES, ("populate", "select"))
SUITE_OPERATIONS["time_series"] = ("populate", "select", "mutate", "concurrent")

SuiteQueryNameParser = Literal["generic", "time_series"]

SUITE_QUERY_NAME_PARSERS: dict[SuiteName, SuiteQueryNameParser] = dict.fromkeys(SUITE_NAMES, "generic")
SUITE_QUERY_NAME_PARSERS["time_series"] = "time_series"

SUITE_PUBLIC_ROLES = cast(dict[SuiteName, SuitePublicRole], dict.fromkeys(SUITE_NAMES, "benchmark"))
SUITE_PUBLIC_ROLES["kaggle_airbnb"] = "smoke"

ROW_STORE_DATABASES: frozenset[DatabaseName] = frozenset({"postgres", "timescaledb"})
ROW_STORE_OPTIONAL_SUITE_NAMES: frozenset[SuiteName] = frozenset({"tpc_h", "tpc_ds"})

assert set(DEFAULT_SUITE_SCALE_FACTORS) == set(SUITE_NAMES)
assert set(ALL_SUITE_SCALE_FACTORS) == set(SUITE_NAMES)
assert set(SUITE_DISPLAY_ORDER) == set(SUITE_NAMES)
assert set(SUITE_LABELS) == set(SUITE_NAMES)
assert set(SUITE_NAV_LABELS) == set(SUITE_NAMES)
assert set(SUITE_OPERATIONS) == set(SUITE_NAMES)
assert set(SUITE_QUERY_NAME_PARSERS) == set(SUITE_NAMES)
assert set(SUITE_PUBLIC_ROLES) == set(SUITE_NAMES)
assert ROW_STORE_DATABASES.issubset(set(get_args(DatabaseName)))
assert ROW_STORE_OPTIONAL_SUITE_NAMES.issubset(set(SUITE_NAMES))


def resolve_suite_scale_factor(suite: SuiteName, scale_factor: int | None = None) -> int:
    resolved_scale_factor = DEFAULT_SUITE_SCALE_FACTORS[suite] if scale_factor is None else scale_factor

    if resolved_scale_factor < 1:
        raise ValueError(f"Suite scale factor must be >= 1 for {suite}: {resolved_scale_factor}")

    if suite not in SCALE_FACTOR_SUITES and resolved_scale_factor != 1:
        raise ValueError(f"Suite {suite} has a fixed scale factor of 1")

    if suite == "time_series" and resolved_scale_factor not in TIME_SERIES_SCALE_FACTORS:
        valid = ", ".join(str(value) for value in TIME_SERIES_SCALE_FACTORS)
        raise ValueError(f"Suite time_series supports scale factors: {valid}")

    if suite == "jsonbench" and resolved_scale_factor not in JSONBENCH_SCALE_FACTORS:
        valid = ", ".join(str(value) for value in JSONBENCH_SCALE_FACTORS)
        raise ValueError(f"Suite jsonbench supports scale factors: {valid}")

    return resolved_scale_factor


def resolve_suite_scale_factors(
    suite: SuiteName,
    scale_factor: int | None = None,
    *,
    include_all_supported: bool = False,
    allow_fixed_default: bool = False,
) -> tuple[int, ...]:
    if scale_factor is None:
        if include_all_supported:
            return ALL_SUITE_SCALE_FACTORS[suite]
        return (DEFAULT_SUITE_SCALE_FACTORS[suite],)

    try:
        return (resolve_suite_scale_factor(suite, scale_factor),)
    except ValueError:
        if allow_fixed_default and suite not in SCALE_FACTOR_SUITES:
            return (DEFAULT_SUITE_SCALE_FACTORS[suite],)
        raise


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
        return list(SUITE_NAMES)
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
