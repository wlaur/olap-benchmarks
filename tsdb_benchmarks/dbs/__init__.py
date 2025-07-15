import logging
from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping
from time import perf_counter
from typing import Any, Literal

import polars as pl
from pydantic import BaseModel
from sqlalchemy import Connection, text

from ..settings import REPO_ROOT, SETTINGS, DatabaseName, SuiteName, TableName
from ..suites.rtabench.generate import RTABENCH_QUERY_NAMES, RTABENCH_SCHEMAS

_LOGGER = logging.getLogger(__name__)


RTABENCH_QUERIES_DIRECTORY = REPO_ROOT / "tsdb_benchmarks/suites/rtabench/queries"


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

        for table_name in RTABENCH_SCHEMAS:
            df = pl.read_parquet(SETTINGS.input_data_directory / f"rtabench/{table_name}.parquet")
            self.insert(df, table_name)
            _LOGGER.info(f"Inserted {table_name} for {self.name}")

    @property
    def rtabench_fetch_kwargs(self) -> dict[str, Any]:
        return {}

    def load_rtabench_query(self, query_name: str) -> str:
        with (RTABENCH_QUERIES_DIRECTORY / f"{self.name}/{query_name}.sql").open() as f:
            return f.read()

    def run_rtabench(self) -> None:
        total_seconds = 0.0
        for idx, n in enumerate(RTABENCH_QUERY_NAMES):
            query = self.load_rtabench_query(n)

            t0 = perf_counter()
            df = self.fetch(query, **self.rtabench_fetch_kwargs)
            t = perf_counter() - t0

            _LOGGER.info(f"Executed {n} ({idx + 1:_}/{len(RTABENCH_QUERY_NAMES):_}) in {1_000 * (t):_.2f} ms\ndf={df}")

            total_seconds += t

        _LOGGER.info(f"Executed {len(RTABENCH_QUERY_NAMES):_} queries in {total_seconds:_.2f} seconds")

    def populate_time_series(self) -> None:
        raise NotImplementedError

    def run_time_series(self) -> None:
        raise NotImplementedError

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
