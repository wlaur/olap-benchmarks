import logging
from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping
from typing import Literal

import polars as pl
from pydantic import BaseModel
from sqlalchemy import Connection, text

from ..settings import REPO_ROOT, SETTINGS, DatabaseName, SuiteName, TableName
from ..suites.rtabench.generate import RTA_BENCH_SCHEMAS

_LOGGER = logging.getLogger(__name__)


class Database(BaseModel, ABC):
    name: DatabaseName
    _connection: Connection | None = None

    @property
    @abstractmethod
    def start(self) -> str: ...

    @property
    def stop(self) -> str:
        return f"docker stop {self.name}-benchmark"

    @property
    def restart(self) -> str:
        return f"docker restart {self.name}-benchmark"

    def setup(self) -> None:
        pass

    def populate_rtabench(self) -> None:
        with (REPO_ROOT / f"tsdb_benchmarks/suites/rtabench/schemas/{self.name}.sql").open() as f:
            sql = f.read()

        con = self.connect()

        for stmt in sql.split(";"):
            if not stmt.strip():
                continue

            con.execute(text(stmt))

        con.commit()
        _LOGGER.info(f"Created RTABench tables for {self.name}")

        for table_name in RTA_BENCH_SCHEMAS:
            df = pl.read_parquet(SETTINGS.input_data_directory / f"rtabench/{table_name}.parquet")
            self.insert(df, table_name)
            print(f"Inserted {table_name} for {self.name}")

    def run_rtabench(self) -> None:
        pass

    def populate_time_series(self) -> None:
        pass

    def run_time_series(self) -> None:
        pass

    def benchmark(self, suite: SuiteName, operation: Literal["populate", "run"]) -> None:
        operations: dict[SuiteName, dict[Literal["populate", "run"], Callable[[], None]]] = {
            "rtabench": {
                "populate": self.populate_rtabench,
                "run": self.run_rtabench,
            },
            "time_series": {
                "populate": self.populate_time_series,
                "run": self.run_time_series,
            },
        }

        operations[suite][operation]()

    @abstractmethod
    def connect(self, reconnect: bool = False) -> Connection: ...

    def rollback(self) -> None:
        if self._connection is None:
            return
        self._connection.rollback()

    @abstractmethod
    def fetch(
        self, query: str, schema: Mapping[str, pl.DataType | type[pl.DataType]] | None = None
    ) -> pl.DataFrame: ...

    @abstractmethod
    def insert(self, df: pl.DataFrame, table: TableName, primary_key: str | list[str] | None = None) -> None: ...

    @abstractmethod
    def upsert(self, df: pl.DataFrame, table: TableName, primary_key: str | list[str]) -> None: ...
