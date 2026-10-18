import logging
import sys
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from typing import Annotated, ClassVar, Literal, cast, get_args

from colorama import Fore, Style
from colorama import init as colorama_init
from pydantic import DirectoryPath, PositiveInt, field_validator
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
    "arc",
    "tdengine",
]

SuiteName = Literal[
    "rtabench",
    "time_series",
    "time_series_metrics",
    "time_series_ranges",
    "clickbench",
    "jsonbench",
    "chat_threads",
    "kaggle_airbnb",
    "tpc_h",
    "tpc_ds",
]
Operation = Literal["populate", "select", "mutate", "concurrent"]
Revision = Annotated[str, "Results database revision"]
ContainerPlatform = Literal["linux/amd64", "linux/arm64"]

type DatabaseArg = DatabaseName | Literal["all"]
type SuiteArg = SuiteName | Literal["all"]
SuitePublicRole = Literal["benchmark", "smoke"]

SuiteQueryNameParser = Literal["generic", "time_series"]


@dataclass(frozen=True, slots=True)
class SuiteSpec:
    label: str
    scale_factors: tuple[int, ...]
    default_scale_factor: int
    # whether the scale factor is part of the data directory name and of `--scale-factor`
    scalable: bool
    operations: tuple[Operation, ...] = ("populate", "select")
    query_name_parser: SuiteQueryNameParser = "generic"
    public_role: SuitePublicRole = "benchmark"
    # row-store engines skip the suite under `olap benchmark <db> all` unless it is named explicitly
    row_store_optional: bool = False

    def __post_init__(self) -> None:
        if not self.scale_factors or any(scale_factor < 1 for scale_factor in self.scale_factors):
            raise ValueError(f"{self.label}: scale factors must be >= 1: {self.scale_factors}")
        if self.default_scale_factor not in self.scale_factors:
            raise ValueError(f"{self.label}: default scale factor {self.default_scale_factor} is not supported")
        if not self.scalable and self.scale_factors != (1,):
            raise ValueError(f"{self.label}: a fixed-size suite has the single scale factor 1")

    @property
    def nav_label(self) -> str:
        return self.label


_ALL_OPERATIONS: tuple[Operation, ...] = ("populate", "select", "mutate", "concurrent")

# one entry per suite, in display order
SUITE_SPECS: dict[SuiteName, SuiteSpec] = {
    "time_series": SuiteSpec(
        "Time Series",
        scale_factors=(1, 10),
        default_scale_factor=1,
        scalable=True,
        operations=_ALL_OPERATIONS,
        query_name_parser="time_series",
    ),
    "time_series_metrics": SuiteSpec(
        "Time Series Metrics",
        scale_factors=(1,),
        default_scale_factor=1,
        scalable=False,
        query_name_parser="time_series",
    ),
    "time_series_ranges": SuiteSpec(
        "Time Series Ranges",
        scale_factors=(1,),
        default_scale_factor=1,
        scalable=False,
        query_name_parser="time_series",
    ),
    "rtabench": SuiteSpec("RTABench", scale_factors=(1,), default_scale_factor=1, scalable=False),
    "clickbench": SuiteSpec("ClickBench", scale_factors=(1,), default_scale_factor=1, scalable=False),
    "jsonbench": SuiteSpec("JSONBench", scale_factors=(10,), default_scale_factor=10, scalable=True),
    "chat_threads": SuiteSpec(
        "Chat Threads", scale_factors=(1, 10), default_scale_factor=1, scalable=True, operations=_ALL_OPERATIONS
    ),
    "kaggle_airbnb": SuiteSpec(
        "Kaggle Airbnb", scale_factors=(1,), default_scale_factor=1, scalable=False, public_role="smoke"
    ),
    "tpc_h": SuiteSpec(
        "TPC-H", scale_factors=(10, 50), default_scale_factor=10, scalable=True, row_store_optional=True
    ),
    "tpc_ds": SuiteSpec("TPC-DS", scale_factors=(1,), default_scale_factor=1, scalable=True, row_store_optional=True),
}

SUITE_NAMES = cast(tuple[SuiteName, ...], get_args(SuiteName))

ROW_STORE_DATABASES: frozenset[DatabaseName] = frozenset({"postgres", "timescaledb"})

# These engines have no server: they execute inside the benchmark client process, so their whole
# footprint is recorded as client memory and the `run_metric.server_*` columns are always 0 for them.
# Kept in sync with `Database.execution_mode` by
# test_results_resource_usage.test_in_process_databases_match_database_execution_mode.
IN_PROCESS_DATABASES: frozenset[DatabaseName] = frozenset({"duckdb", "polars"})

assert set(SUITE_SPECS) == set(SUITE_NAMES)


def resolve_suite_scale_factor(suite: SuiteName, scale_factor: int | None = None) -> int:
    spec = SUITE_SPECS[suite]
    resolved_scale_factor = spec.default_scale_factor if scale_factor is None else scale_factor

    if resolved_scale_factor not in spec.scale_factors:
        if not spec.scalable:
            raise ValueError(f"Suite {suite} has a fixed scale factor of 1")
        valid = ", ".join(str(value) for value in spec.scale_factors)
        raise ValueError(f"Suite {suite} supports scale factors: {valid}")

    return resolved_scale_factor


def resolve_suite_scale_factors(
    suite: SuiteName,
    scale_factor: int | None = None,
    *,
    include_all_supported: bool = False,
    allow_fixed_default: bool = False,
) -> tuple[int, ...]:
    spec = SUITE_SPECS[suite]
    if scale_factor is None:
        if include_all_supported:
            return spec.scale_factors
        return (spec.default_scale_factor,)

    try:
        return (resolve_suite_scale_factor(suite, scale_factor),)
    except ValueError:
        if allow_fixed_default and len(spec.scale_factors) == 1:
            return (spec.default_scale_factor,)
        raise


def format_suite_data_directory_name(suite: SuiteName, scale_factor: int) -> str:
    scale_factor = resolve_suite_scale_factor(suite, scale_factor)
    if SUITE_SPECS[suite].scalable:
        return f"{suite}_sf{scale_factor}"
    return suite


def resolve_dbs(arg: DatabaseArg) -> list[DatabaseName]:
    if arg == "all":
        return list(get_args(DatabaseName))
    return [arg]


def resolve_suites(arg: SuiteArg) -> list[SuiteName]:
    if arg == "all":
        return list(SUITE_NAMES)
    return [arg]


REPO_ROOT = Path(__file__).parent.parent.resolve()
ENV_FILE = REPO_ROOT / ".env"

MAIN_PROCESS_TITLE = "olap-benchmarks-main"


# host ports the benchmark containers bind, keyed by a stable logical name. Override any
# subset via OLAP_BENCHMARKS_HOST_PORTS, e.g. '{"monetdb": 50010}', to avoid colliding with
# other local containers. Container-side ports are fixed by the images and are not remapped.
DEFAULT_HOST_PORTS: dict[str, int] = {
    "monetdb": 50000,
    "clickhouse_http": 18123,
    "clickhouse_native": 19000,
    "postgres": 5433,
    "timescaledb": 5432,
    "questdb_http": 9000,
    "questdb_pg": 8812,
    "doris_fe_mysql": 9030,
    "doris_fe_http": 8030,
    "doris_be_http": 8040,
    "starrocks_fe_mysql": 9030,
    "starrocks_fe_http": 8030,
    "starrocks_be_http": 8040,
    "arc": 8000,
    "tdengine_ws": 16041,
}


class Settings(BaseSettings):
    input_data_directory: DirectoryPath
    results_directory: DirectoryPath

    database_directory: Path
    temporary_directory: DirectoryPath

    system: str
    benchmark_threads: PositiveInt | None = None
    benchmark_memory_gb: PositiveInt | None = None
    run_note: str | None = None
    minimum_free_disk_gb: PositiveInt = 40
    operation_timeout_seconds: PositiveInt | None = None
    query_timeout_seconds: PositiveInt | None = None
    client_memory_limit_gb: PositiveInt | None = None
    query_include: tuple[str, ...] = ()
    query_exclude: tuple[str, ...] = ()
    tables: tuple[str, ...] = ()

    host_ports: dict[str, int] = {}

    @field_validator("host_ports")
    @classmethod
    def _validate_host_ports(cls, value: dict[str, int]) -> dict[str, int]:
        unknown = sorted(set(value) - set(DEFAULT_HOST_PORTS))
        if unknown:
            raise ValueError(
                f"Unknown host port name(s): {', '.join(unknown)}; valid names are "
                f"{', '.join(sorted(DEFAULT_HOST_PORTS))}"
            )
        return value

    model_config = SettingsConfigDict(
        env_file=ENV_FILE,
        env_prefix="OLAP_BENCHMARKS_",
        extra="ignore",
    )


@cache
def get_settings() -> Settings:
    return Settings.model_validate({})


def effective_settings(settings: BaseSettings) -> dict[str, object]:
    prefix = settings.model_config.get("env_prefix", "")
    return {f"{prefix}{name}".upper(): value for name, value in settings.model_dump().items()}


def host_port(name: str) -> int:
    if name not in DEFAULT_HOST_PORTS:
        raise KeyError(f"Unknown host port name: {name}")
    return get_settings().host_ports.get(name, DEFAULT_HOST_PORTS[name])


def setup_stdout_logging(level: int = logging.INFO) -> None:
    colorama_init()

    class ColoredFormatter(logging.Formatter):
        LEVEL_COLORS: ClassVar[dict[int, str]] = {
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
