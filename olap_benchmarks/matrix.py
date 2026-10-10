from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass

from .settings import SUITE_SPECS, DatabaseName, Operation, SuiteName


@dataclass(frozen=True, slots=True)
class SuitePlan:
    suite: SuiteName
    scale_factor: int
    # None means every operation the engine registers for the suite
    operations: tuple[Operation, ...] | None = None


@dataclass(frozen=True, slots=True)
class EnginePlan:
    published: bool
    suites: tuple[SuitePlan, ...]


@dataclass(frozen=True, slots=True, order=True)
class MatrixCell:
    db: DatabaseName
    suite: SuiteName
    scale_factor: int
    operation: Operation

    @property
    def label(self) -> str:
        return f"{self.db}:{self.suite}:sf{self.scale_factor}:{self.operation}"


def _default_scale(*suites: SuiteName) -> tuple[SuitePlan, ...]:
    return tuple(SuitePlan(suite, SUITE_SPECS[suite].default_scale_factor) for suite in suites)


_COLUMN_STORE_SUITES: tuple[SuitePlan, ...] = (
    SuitePlan("time_series", 1),
    SuitePlan("chat_threads", 1),
    SuitePlan("rtabench", 1),
    SuitePlan("kaggle_airbnb", 1),
    SuitePlan("tpc_ds", 1),
    SuitePlan("tpc_h", 10),
    SuitePlan("jsonbench", 10),
    SuitePlan("clickbench", 1),
)

# The intended benchmark matrix. A new engine gets one entry here, listing only the suites it is
# meant to run (an engine does not have to cover every suite).
MATRIX: dict[DatabaseName, EnginePlan] = {
    "clickhouse": EnginePlan(published=True, suites=_COLUMN_STORE_SUITES),
    "duckdb": EnginePlan(published=True, suites=_COLUMN_STORE_SUITES),
    # no JSONBench queries
    "monetdb": EnginePlan(
        published=True,
        suites=tuple(plan for plan in _COLUMN_STORE_SUITES if plan.suite != "jsonbench"),
    ),
    "polars": EnginePlan(
        published=True,
        suites=(SuitePlan("tpc_h", 10), SuitePlan("jsonbench", 10), SuitePlan("clickbench", 1)),
    ),
    "postgres": EnginePlan(
        published=False,
        suites=_default_scale(
            "time_series", "chat_threads", "rtabench", "kaggle_airbnb", "tpc_ds", "tpc_h", "jsonbench", "clickbench"
        ),
    ),
    "timescaledb": EnginePlan(
        published=False,
        suites=_default_scale(
            "time_series", "chat_threads", "rtabench", "kaggle_airbnb", "tpc_ds", "tpc_h", "jsonbench", "clickbench"
        ),
    ),
    "questdb": EnginePlan(
        published=False,
        suites=_default_scale("time_series", "rtabench", "kaggle_airbnb", "clickbench"),
    ),
    "starrocks": EnginePlan(
        published=False,
        suites=_default_scale("time_series", "rtabench", "kaggle_airbnb", "tpc_ds", "tpc_h", "jsonbench", "clickbench"),
    ),
    "arc": EnginePlan(
        published=False,
        suites=_default_scale("time_series", "chat_threads", "clickbench"),
    ),
    "doris": EnginePlan(
        published=False,
        suites=_default_scale("rtabench", "jsonbench", "clickbench"),
    ),
}


def registered_operations() -> dict[DatabaseName, dict[SuiteName, tuple[Operation, ...]]]:
    from .dbs import get_databases

    return {
        db_name: {suite: suite_class.supported_operations for suite, suite_class in database.suite_registry().items()}
        for db_name, database in get_databases().items()
    }


def expand_matrix(
    matrix: Mapping[DatabaseName, EnginePlan] = MATRIX,
    *,
    dbs: Iterable[DatabaseName] | None = None,
    published_only: bool = False,
    registry: Mapping[DatabaseName, Mapping[SuiteName, tuple[Operation, ...]]] | None = None,
) -> list[MatrixCell]:
    selected = set(dbs) if dbs is not None else None
    operations_by_db = registry if registry is not None else registered_operations()
    cells: list[MatrixCell] = []

    for db_name, engine in matrix.items():
        if selected is not None and db_name not in selected:
            continue
        if published_only and not engine.published:
            continue

        for plan in engine.suites:
            registered = operations_by_db[db_name].get(plan.suite)
            if registered is None:
                raise ValueError(f"{db_name} declares {plan.suite} in the matrix but does not register the suite")
            operations = plan.operations if plan.operations is not None else registered
            unsupported = [operation for operation in operations if operation not in registered]
            if unsupported:
                raise ValueError(f"{db_name} declares unsupported {plan.suite} operation(s): {', '.join(unsupported)}")
            cells.extend(MatrixCell(db_name, plan.suite, plan.scale_factor, operation) for operation in operations)

    return cells


def published_databases(matrix: Mapping[DatabaseName, EnginePlan] = MATRIX) -> list[DatabaseName]:
    return [db_name for db_name, engine in matrix.items() if engine.published]


def current_pins() -> dict[str, str]:
    from .dbs import get_databases

    return {db_name: database.version for db_name, database in get_databases().items()}


def current_drivers() -> dict[str, str | None]:
    from .dbs import get_databases

    return {db_name: database.db_driver for db_name, database in get_databases().items()}
