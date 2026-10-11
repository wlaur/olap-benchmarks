from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any, ClassVar, cast

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import taosws
from pydantic import Field
from sqlalchemy import Connection, create_engine, text

from ...settings import DatabaseName, SuiteName, TableName, get_settings, host_port
from ...suites import BenchmarkSuite
from ...suites.time_series_ranges.config import TimeSeriesRanges
from .. import Database

_WS = cast(Any, taosws)
DATABASE = "benchmark"
BATCH_ROWS = 480
TABLE_BATCH_SIZE = 128


class TDengineTimeSeriesRanges(TimeSeriesRanges["TDengine"]):
    def populate_table(self, table: str, path: Path) -> None:
        self.db.load_time_series_table(table, path)

    def finish_populate(self) -> None:
        self.db.execute(f"FLUSH DATABASE {DATABASE}")


class TDengine(Database):
    name: DatabaseName = "tdengine"
    supports_arm64_containers: ClassVar[bool] = True
    wal_path_patterns: ClassVar[tuple[str, ...]] = ("data/vnode/*/wal",)
    connection_string: str = Field(
        default_factory=lambda: f"taosws://root:taosdata@localhost:{host_port('tdengine_ws')}/{DATABASE}?timezone=UTC"
    )

    @property
    def db_driver(self) -> str:
        return "websocket"

    @property
    def run_package_names(self) -> tuple[str, ...]:
        return ("olap-benchmarks", "taos-ws-py")

    @property
    def run_options(self) -> Mapping[str, object]:
        return {
            **super().run_options,
            "eav_layout": "supertable with one timestamp/value subtable per metric; metric_name is a tag",
            "wide_layout": "one timestamp plus typed FLOAT columns in a basic table",
            "insert": "WebSocket STMT2 column batches; EAV generated from the identical wide Parquet",
            "database_vgroups": 2,
            "database_buffer_mb_per_vgroup": 32,
            "database_keep_days": 36_500,
        }

    @property
    def start(self) -> str:
        return self.docker_run_command(
            self.container_images["server"],
            ports={str(host_port("tdengine_ws")): "6041"},
            mounts={self.database_directory.as_posix(): "/var/lib/taos"},
            env={"TZ": "UTC", "TAOS_FQDN": "localhost", "GOMAXPROCS": str(get_settings().benchmark_threads or 2)},
        )

    def connect(self, reconnect: bool = False) -> Connection:
        if reconnect:
            self.close_connection()
        if self._connection is None:
            bootstrap = _WS.connect(f"ws://root:taosdata@localhost:{host_port('tdengine_ws')}?timezone=UTC")
            try:
                bootstrap.execute(f"CREATE DATABASE IF NOT EXISTS {DATABASE} KEEP 36500 VGROUPS 2 BUFFER 32")
            finally:
                bootstrap.close()
            engine = create_engine(self.connection_string, pool_reset_on_return=None)
            connection = engine.connect()
            self._connection = self.bind_query_recorder(connection)
        return self._connection

    def get_runtime_version(self) -> str:
        return str(self.fetch("SELECT server_version() AS version").item())

    def fetch(self, query: str, schema: Mapping[str, pl.DataType | type[pl.DataType]] | None = None) -> pl.DataFrame:
        with self.record_query_execution(query):
            result = self.connect().execute(text(query.strip().removesuffix(";")))
            frame = pl.DataFrame(result.fetchall(), schema=list(result.keys()), orient="row", infer_schema_length=None)
            times = [name for name in ("time", "hr") if name in frame.columns and frame.schema[name] == pl.String]
            if times:
                frame = frame.with_columns(pl.col(times).str.strip_suffix(" UTC").str.to_datetime(time_unit="ms"))
        if schema is not None:
            frame = frame.cast(cast(pl.Schema, schema))
        return frame

    def get_table_names(self) -> set[TableName]:
        frame = self.fetch(
            "SELECT table_name AS name FROM information_schema.ins_tables "
            f"WHERE db_name = '{DATABASE}' AND stable_name IS NULL "
            "UNION ALL SELECT stable_name AS name FROM information_schema.ins_stables "
            f"WHERE db_name = '{DATABASE}'"
        )
        return set(cast(list[str], frame["name"].to_list()))

    def load_time_series_table(self, table: str, path: Path) -> None:
        is_eav = table.startswith("eav")
        source = path.with_name(path.name.replace("eav", "wide", 1)) if is_eav else path
        reader = pq.ParquetFile(source)
        columns = reader.schema_arrow.names[1:]
        if is_eav:
            self.execute(f"CREATE STABLE {table} (time TIMESTAMP, value FLOAT) TAGS (metric_name VARCHAR(11))")
            sql = f"INSERT INTO ? USING {table} TAGS (?) VALUES (?, ?)"
        else:
            definitions = ", ".join(f"{column} FLOAT" for column in columns)
            self.execute(f"CREATE TABLE {table} (time TIMESTAMP, {definitions})")
            sql = f"INSERT INTO {table} VALUES ({', '.join('?' for _ in range(len(columns) + 1))})"

        connection = cast(Any, self.connect().connection.driver_connection)
        statement = connection.stmt2_statement()
        try:
            statement.prepare(sql)
            batches = cast(Iterator[pa.RecordBatch], cast(Any, reader).iter_batches(batch_size=BATCH_ROWS))
            for batch in batches:
                timestamps = _WS.millis_timestamps_to_column(batch.column(0).cast(pa.int64()).to_pylist())
                if is_eav:
                    for start in range(0, len(columns), TABLE_BATCH_SIZE):
                        params: list[Any] = []
                        for index in range(start, min(start + TABLE_BATCH_SIZE, len(columns))):
                            values = _WS.floats_to_column(batch.column(index + 1).to_pylist())
                            params.append(
                                _WS.stmt2_bind_param_view(
                                    f"{table}_{columns[index]}",
                                    [_WS.varchar_to_tag(columns[index])],
                                    [timestamps, values],
                                )
                            )
                        statement.bind(params)
                        with self.record_query_execution(sql):
                            statement.execute()
                else:
                    values = [
                        _WS.floats_to_column(batch.column(index + 1).to_pylist()) for index in range(len(columns))
                    ]
                    statement.bind([_WS.stmt2_bind_param_view(table, None, [timestamps, *values])])
                    with self.record_query_execution(sql):
                        statement.execute()
        finally:
            statement.close()

    def insert(
        self,
        df: pl.DataFrame | pl.LazyFrame,
        table: TableName,
        primary_key: str | list[str] | None = None,
        not_null: str | list[str] | None = None,
    ) -> None:
        raise NotImplementedError("TDengine registers the Parquet-backed time_series_ranges suite only")

    def upsert(self, df: pl.DataFrame, table: TableName, primary_key: str | list[str]) -> None:
        raise NotImplementedError("TDengine mutation benchmarks are not registered")

    def delete(self, table: TableName, primary_key: str | list[str], keys: pl.DataFrame) -> None:
        raise NotImplementedError("TDengine mutation benchmarks are not registered")

    def suite_registry(self) -> Mapping[SuiteName, type[BenchmarkSuite[Any]]]:
        return {"time_series_ranges": TDengineTimeSeriesRanges}
