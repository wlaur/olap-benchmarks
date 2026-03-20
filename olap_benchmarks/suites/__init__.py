import logging
from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

import polars as pl
from pydantic import BaseModel

from ..dbs import Database
from ..settings import SuiteName, TableName

_LOGGER = logging.getLogger(__name__)


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
    db: DBT
    name: SuiteName

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

    def mutate(self) -> None:
        raise NotImplementedError(f"{type(self).__name__} does not support the mutate operation")
