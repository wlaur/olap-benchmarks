import logging
from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar

import polars as pl
from pydantic import BaseModel, Field

from ..dbs import Database
from ..run_metadata import StepResultStatus
from ..settings import (
    SETTINGS,
    Operation,
    SuiteName,
    TableName,
    format_suite_data_directory_name,
)

_LOGGER = logging.getLogger(__name__)


class ManualPreparationRequired(RuntimeError):
    pass


@dataclass(frozen=True)
class TableRowCountCheck:
    table_name: TableName
    expected_row_count: int
    actual_row_count: int | None
    error: str | None = None

    @property
    def matches(self) -> bool:
        return self.actual_row_count == self.expected_row_count

    @property
    def is_missing(self) -> bool:
        return self.actual_row_count is None


class BenchmarkSuite[DBT: Database](BaseModel, ABC):
    supported_operations: ClassVar[tuple[Operation, ...]] = ("populate", "select")
    db: DBT
    name: SuiteName
    scale_factor: int = Field(default=1, ge=1)

    @property
    def data_directory_name(self) -> str:
        return format_suite_data_directory_name(self.name, self.scale_factor)

    @property
    def input_data_directory(self) -> Path:
        return SETTINGS.input_data_directory / self.data_directory_name

    @staticmethod
    def parquet_row_count(fpath: Path) -> int:
        return int(pl.scan_parquet(fpath).select(pl.len()).collect().item(0, 0))

    def expected_table_row_counts(self) -> Mapping[TableName, int]:
        return {}

    def _collect_table_row_count_checks(self) -> list[TableRowCountCheck]:
        checks: list[TableRowCountCheck] = []

        for table_name, expected_row_count in self.expected_table_row_counts().items():
            try:
                actual_row_count = self.db.get_row_count(table_name)
            except Exception as exc:
                self.db.rollback()
                checks.append(
                    TableRowCountCheck(
                        table_name=table_name,
                        expected_row_count=expected_row_count,
                        actual_row_count=None,
                        error=f"{type(exc).__name__}: {exc}",
                    )
                )
            else:
                checks.append(
                    TableRowCountCheck(
                        table_name=table_name,
                        expected_row_count=expected_row_count,
                        actual_row_count=actual_row_count,
                    )
                )

        return checks

    def _format_table_row_count_checks(self, checks: list[TableRowCountCheck]) -> str:
        return "; ".join(
            f"{check.table_name}: expected={check.expected_row_count:_}, actual={check.actual_row_count:_}"
            if check.actual_row_count is not None
            else f"{check.table_name}: expected={check.expected_row_count:_}, missing ({check.error})"
            for check in checks
        )

    def _format_table_names(self, table_names: set[TableName]) -> str:
        if not table_names:
            return "(none)"
        return ", ".join(sorted(table_names))

    def should_populate(self) -> bool:
        checks = self._collect_table_row_count_checks()

        if not checks:
            return True

        if all(check.matches for check in checks):
            _LOGGER.info(
                f"Skipping populate for {self.name} on {self.db.name}; existing data matches expected row counts: "
                f"{self._format_table_row_count_checks(checks)}"
            )
            return False

        if all(check.is_missing for check in checks):
            existing_tables = self.db.get_table_names()

            if not existing_tables:
                return True

            raise RuntimeError(
                f"Refusing to populate {self.name} on {self.db.name}; expected tables are missing but the database "
                f"is not empty. Existing tables: {self._format_table_names(existing_tables)}. "
                f"Row-count checks: {self._format_table_row_count_checks(checks)}"
            )

        raise RuntimeError(
            f"Refusing to populate {self.name} on {self.db.name}; existing data does not match expected row counts: "
            f"{self._format_table_row_count_checks(checks)}"
        )

    def verify_populated_data(self) -> None:
        checks = self._collect_table_row_count_checks()

        if checks and not all(check.matches for check in checks):
            raise RuntimeError(
                f"Populated data verification failed for {self.name} on {self.db.name}: "
                f"{self._format_table_row_count_checks(checks)}"
            )

        if checks:
            _LOGGER.info(
                f"Verified populated data for {self.name} on {self.db.name}: "
                f"{self._format_table_row_count_checks(checks)}"
            )

    @abstractmethod
    def populate(self) -> None: ...

    @abstractmethod
    def select(self) -> None: ...

    def record_skipped_query_steps(
        self,
        query_name: str,
        iterations: int,
        *,
        result_status: StepResultStatus,
        reason: str,
        start_iteration: int = 1,
    ) -> None:
        if result_status not in ("skipped", "unsupported"):
            raise ValueError(f"Skipped query steps cannot use result_status={result_status!r}")

        for iteration in range(start_iteration, iterations + 1):
            self.db.record_skipped_query_step(
                query_name=query_name,
                iteration=iteration,
                reason=reason,
                result_status=result_status,
            )

    def execute_query_with_isolation(
        self,
        *,
        query_name: str,
        iterations: int,
        query_loader: Callable[[], str],
        fetch_kwargs_factory: Callable[[], Mapping[str, Any]],
        progress_label: str,
        log_success: Callable[[int, pl.DataFrame, float], None],
    ) -> bool:
        failed_iteration: int | None = None

        try:
            with self.db.query_context(query_name):
                query = query_loader()
                fetch_kwargs = fetch_kwargs_factory()

                for iteration in range(1, iterations + 1):
                    failed_iteration = iteration
                    df, duration_seconds = self.db.execute_query_iteration(
                        query_name=query_name,
                        iteration=iteration,
                        query=query,
                        fetch_kwargs=fetch_kwargs,
                    )
                    log_success(iteration, df, duration_seconds)
                    failed_iteration = None
        except Exception as exc:
            self.db.rollback()
            start_iteration = 1 if failed_iteration is None else failed_iteration + 1
            if start_iteration <= iterations:
                self.record_skipped_query_steps(
                    query_name,
                    iterations,
                    result_status="skipped",
                    reason=f"query aborted after {type(exc).__name__}: {exc}",
                    start_iteration=start_iteration,
                )
            _LOGGER.exception(
                f"Failed {query_name} {progress_label} on {self.db.name}; continuing with remaining queries: {exc}"
            )
            return False

        return True

    def mutate(self) -> None:
        raise NotImplementedError(f"{type(self).__name__} does not support the mutate operation")

    def concurrent(self) -> None:
        raise NotImplementedError(f"{type(self).__name__} does not support the concurrent operation")


def get_suite_preparer(suite: SuiteName, scale_factor: int) -> Callable[[], None]:
    match suite:
        case "rtabench":
            from .rtabench.config import prepare_data as prepare_static_data

            return prepare_static_data
        case "clickbench":
            from .clickbench.config import prepare_data as prepare_static_data

            return prepare_static_data
        case "jsonbench":
            from .jsonbench.config import prepare_data as prepare_scaled_data

            return lambda: prepare_scaled_data(scale_factor)
        case "chat_threads":
            from .chat_threads.config import prepare_data as prepare_scaled_data

            return lambda: prepare_scaled_data(scale_factor)
        case "time_series":
            from .time_series.config import prepare_data as prepare_scaled_data

            return lambda: prepare_scaled_data(scale_factor)
        case "kaggle_airbnb":
            from .kaggle_airbnb.config import prepare_data as prepare_static_data

            return prepare_static_data
        case "tpc_h":
            from .tpc_h.config import prepare_data as prepare_scaled_data

            return lambda: prepare_scaled_data(scale_factor)
        case "tpc_ds":
            from .tpc_ds.config import prepare_data as prepare_scaled_data

            return lambda: prepare_scaled_data(scale_factor)
