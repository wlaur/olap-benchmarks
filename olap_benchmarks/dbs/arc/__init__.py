import io
import logging
import subprocess
import uuid
from collections.abc import Iterator, Mapping
from datetime import datetime
from time import perf_counter, sleep
from typing import Any, ClassVar, cast

import httpx
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import ipc
from pydantic import Field, PrivateAttr
from sqlalchemy import Connection

from ...settings import DatabaseName, Operation, SuiteName, TableName, get_settings, host_port
from ...suites import BenchmarkSuite
from ...suites.chat_threads.config import ChatThreads, get_chat_threads_input_files
from ...suites.clickbench.config import Clickbench
from ...suites.time_series.config import TimeSeries
from ...suites.time_series_metrics.config import METRIC_COUNTS, TimeSeriesMetrics
from ...suites.time_series_ranges.config import METRIC_COUNTS as RANGE_METRIC_COUNTS
from ...suites.time_series_ranges.config import TimeSeriesRanges
from .. import Database
from ..utils import require_columns

_LOGGER = logging.getLogger(__name__)


ARC_DATABASE = "olap"

ARC_WRITE_BATCH_CELLS = 8_000_000

# Arc rejects a Parquet import larger than 500 MiB (HTTP 413); wide JSON batches exceed it well
# within the cell budget, so encoded batches above this size are split in half until they fit.
ARC_IMPORT_MAX_BYTES = 256 * 1024 * 1024

ARC_VISIBILITY_TIMEOUT_SECONDS = 600.0

ARC_COMPACTION_TIMEOUT_SECONDS = 3_600.0

# Arc stores every measurement partitioned by a non-null `time` column. A table whose own
# timestamp has another name is imported with that column as `time`, which Arc stores in its
# place; the Arc query overrides refer to it as `time`. Arc has no other timestamp or date type:
# any further timestamp column is stored as BIGINT epoch microseconds, and the overrides convert
# it back with make_timestamp() where a query returns it.
ARC_TIME_COLUMNS: Mapping[SuiteName, Mapping[TableName, str]] = {
    "clickbench": {"hits": "EventTime"},
    "chat_threads": {"chat_thread": "created_at", "chat_message": "created_at"},
}


class ArcError(RuntimeError):
    pass


def iter_write_batches(
    df: pl.DataFrame | pl.LazyFrame, batch_cells: int = ARC_WRITE_BATCH_CELLS
) -> Iterator[pl.DataFrame]:
    width = len(df.collect_schema())
    batch_rows = max(1, batch_cells // max(1, width))

    if isinstance(df, pl.LazyFrame):
        yield from df.collect_batches(chunk_size=batch_rows)
        return

    yield from df.iter_slices(batch_rows)


def encode_import_chunks(batch: pl.DataFrame, max_bytes: int = ARC_IMPORT_MAX_BYTES) -> Iterator[bytes]:
    encoded = encode_parquet(batch)
    if len(encoded) <= max_bytes or batch.height == 1:
        yield encoded
        return
    half = batch.height // 2
    yield from encode_import_chunks(batch.slice(0, half), max_bytes)
    yield from encode_import_chunks(batch.slice(half), max_bytes)


def to_arc_arrow(df: pl.DataFrame) -> pa.Table:
    # Arc's Parquet import rejects DATE and large_utf8/large_binary, which is what Polars writes.
    # A date becomes midnight UTC, as QuestDB stores ClickBench's EventDate.
    table = df.with_columns(pl.selectors.date().cast(pl.Datetime("us"))).to_arrow()
    types = [
        pa.string() if pa.types.is_large_string(dtype) else pa.binary() if pa.types.is_large_binary(dtype) else dtype
        for dtype in table.schema.types
    ]
    return table.cast(pa.schema(list(zip(table.schema.names, types, strict=True))))


def encode_parquet(df: pl.DataFrame) -> bytes:
    buffer = io.BytesIO()
    cast(Any, pq).write_table(to_arc_arrow(df), buffer, compression="none", write_statistics=False)
    return buffer.getvalue()


def sql_literal(value: object) -> str:
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    if isinstance(value, datetime):
        return f"'{value.isoformat(sep=' ')}'"
    if isinstance(value, int) and not isinstance(value, bool):
        return str(value)
    raise ValueError(f"Unsupported Arc delete key value: {value!r}")


def build_key_predicate(keys: pl.DataFrame, primary_key: list[str], time_column: str) -> str:
    if len(primary_key) != 1:
        raise ValueError(f"Arc deletes support a single key column, got {primary_key}")

    (key,) = primary_key
    values = keys.get_column(key)
    if isinstance(values.dtype, pl.Datetime):
        values = values.cast(pl.Datetime("us"))
    column = "time" if key == time_column else key
    literals = ", ".join(sql_literal(value) for value in values.unique().sort().to_list())
    return f"{column} IN ({literals})"


def decode_arrow_stream(payload: bytes) -> pl.DataFrame:
    table = ipc.open_stream(pa.py_buffer(payload)).read_all()
    df = cast(pl.DataFrame, cast(Any, pl).from_arrow(table))
    return df.with_columns(
        pl.col(name).dt.convert_time_zone("UTC").dt.replace_time_zone(None)
        for name, dtype in df.schema.items()
        if isinstance(dtype, pl.Datetime) and dtype.time_zone is not None
    )


def finish_populate(suite: BenchmarkSuite["Arc"], restart: bool) -> None:
    with suite.db.phase_context("compact"):
        suite.db.compact()

    with suite.db.phase_context("verify_compacted"):
        suite.verify_populated_data()

    if restart:
        suite.db.restart_event()


class ArcTimeSeries(TimeSeries["Arc"]):
    # Mutate and concurrent are not measured. Every row-targeted upsert or delete rewrites the
    # immutable hourly Parquet files it touches: the time_series SF1 mutate run took 88 minutes
    # (10k-row iterations up to ~6 minutes each, slowing as files fragment), and the concurrent run
    # that followed was OOM-killed within 40 s (Arc 26.09.3).
    supported_operations: ClassVar[tuple[Operation, ...]] = ("populate", "select")

    def populate(self, restart: bool = True) -> None:
        super().populate(restart=False)
        finish_populate(self, restart)


class ArcClickbench(Clickbench["Arc"]):
    def populate(self, restart: bool = True) -> None:
        with self.db.phase_context("verify_existing_data"):
            if not self.should_populate():
                return

        self.db.insert_sorted(self.load_dataset(), "hits")

        with self.db.phase_context("verify_populate"):
            self.verify_populated_data()

        finish_populate(self, restart)


class ArcTimeSeriesMetrics(TimeSeriesMetrics["Arc"]):
    pass


class ArcChatThreads(ChatThreads["Arc"]):
    # Not measured for the same reason as ArcTimeSeries.
    supported_operations: ClassVar[tuple[Operation, ...]] = ("populate", "select")

    def populate(self, restart: bool = True) -> None:
        with self.db.phase_context("verify_existing_data"):
            if not self.should_populate():
                return

        for table_name, fpath in get_chat_threads_input_files(self.scale_factor).items():
            self.db.insert_sorted(pl.scan_parquet(fpath), table_name)

        with self.db.phase_context("verify_populate"):
            self.verify_populated_data()

        finish_populate(self, restart)


class Arc(Database):
    name: DatabaseName = "arc"
    supports_arm64_containers: ClassVar[bool] = True
    BOUNDED_INGEST_ENV: ClassVar[dict[str, str]] = {
        "ARC_INGEST_FLUSH_WORKERS": "2",
        "GOMEMLIMIT": "1GiB",
    }

    connection_string: str = Field(default_factory=lambda: f"http://localhost:{host_port('arc')}")

    _client: httpx.Client | None = PrivateAttr(default=None)

    @property
    def start(self) -> str:
        env = {
            "ARC_AUTH_ENABLED": "false",
            "ARC_TELEMETRY_ENABLED": "false",
            "ARC_DELETE_ENABLED": "true",
        }
        if self.current_suite in ("time_series_metrics", "time_series_ranges"):
            env["ARC_INGEST_SORT_KEYS"] = self.metric_sort_keys
        if self.current_suite in ("time_series", "time_series_metrics", "time_series_ranges"):
            env.update(self.BOUNDED_INGEST_ENV)
        return self.docker_run_command(
            self.container_images["server"],
            ports={str(host_port("arc")): "8000"},
            mounts={self.database_directory.as_posix(): "/app/data"},
            env=env,
        )

    @property
    def metric_sort_keys(self) -> str:
        counts = RANGE_METRIC_COUNTS if self.current_suite == "time_series_ranges" else METRIC_COUNTS
        return " ".join(f"eav{count}:metric_name" for count in counts)

    @property
    def benchmark_settings(self) -> Mapping[str, object]:
        settings = dict(super().benchmark_settings)
        if self.current_suite in ("time_series_metrics", "time_series_ranges"):
            settings["ARC_INGEST_SORT_KEYS"] = self.metric_sort_keys
        if self.current_suite in ("time_series", "time_series_metrics", "time_series_ranges"):
            settings.update(self.BOUNDED_INGEST_ENV)
        return settings

    @property
    def client(self) -> httpx.Client:
        if self._client is None:
            self._client = httpx.Client(
                base_url=self.connection_string,
                headers={"x-arc-database": ARC_DATABASE},
                timeout=httpx.Timeout(None, connect=10.0),
            )
        return self._client

    def connect(self, reconnect: bool = False) -> Connection:
        _ = reconnect
        raise NotImplementedError("Arc is accessed over its HTTP API, not a SQL connection")

    def close_connection(self) -> None:
        client = self._client
        self._client = None
        if client is not None:
            client.close()

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, str] | None = None,
        json: Mapping[str, object] | None = None,
        files: Mapping[str, tuple[str, bytes, str]] | None = None,
    ) -> httpx.Response:
        response = self.client.request(method, path, params=params, json=json, files=files)
        if response.is_error:
            raise ArcError(f"Arc {method} {path} returned HTTP {response.status_code}: {response.text[:2_000]}")
        return response

    def wait_until_accessible(self, timeout_seconds: float = 300.0, interval_seconds: float = 1.0) -> None:
        _LOGGER.info(f"Waiting for database {self.name} (timeout: {timeout_seconds:.0f}s)...")

        deadline = perf_counter() + timeout_seconds
        attempts = 0

        while perf_counter() < deadline:
            attempts += 1
            try:
                self.close_connection()
                self._request("GET", "/ready")
                self.fetch("select 1")
                _LOGGER.info(f"Database {self.name} is ready (after {attempts} attempt(s))")
                return
            except Exception as e:
                elapsed = perf_counter() + timeout_seconds - deadline
                _LOGGER.info(f"Database {self.name} not ready ({elapsed:.0f}s elapsed): {e}")
                sleep(interval_seconds)

        raise TimeoutError(f"Timed out after {timeout_seconds:.0f}s waiting for database {self.name}")

    def get_runtime_version(self) -> str:
        # Arc exposes its version only in the startup log and the image's VERSION file
        result = subprocess.run(
            ["docker", "exec", f"{self.name}-benchmark", "cat", "/app/VERSION"],
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()

    def fetch(self, query: str, schema: Mapping[str, pl.DataType | type[pl.DataType]] | None = None) -> pl.DataFrame:
        with self.record_query_execution(query):
            response = self._request("POST", "/api/v1/query/arrow", json={"sql": query.strip().removesuffix(";")})
            df = decode_arrow_stream(response.content)

        if schema is not None:
            df = df.cast(cast(pl.Schema, schema))

        return df

    def get_table_names(self) -> set[TableName]:
        response = self.client.get(f"/api/v1/databases/{ARC_DATABASE}/measurements")
        if response.status_code == 404:
            return set()
        if response.is_error:
            raise ArcError(f"Arc measurement listing returned HTTP {response.status_code}: {response.text[:2_000]}")
        measurements = cast(list[dict[str, Any]], response.json().get("measurements") or [])
        return {str(measurement["name"]) for measurement in measurements}

    def get_count(self, table: TableName) -> int:
        df = self.fetch(f"select count(*) as row_count from {table}")
        return int(df.item(0, 0))

    def get_row_count(self, table: TableName) -> int:
        if table not in self.get_table_names():
            raise ArcError(f"Arc measurement '{table}' does not exist")
        return self.get_count(table)

    def wait_until_count(self, table: TableName, count: int) -> None:
        deadline = perf_counter() + ARC_VISIBILITY_TIMEOUT_SECONDS
        interval_seconds = 0.01

        while True:
            current = self.get_count(table)
            if current == count:
                return
            if current > count:
                raise ArcError(f"Arc measurement '{table}' has {current:_} rows, expected {count:_}")
            if perf_counter() > deadline:
                raise TimeoutError(f"Arc measurement '{table}' has {current:_}/{count:_} visible rows")
            sleep(interval_seconds)
            interval_seconds = min(1.0, 2 * interval_seconds)

    def time_column(self, table: TableName) -> str:
        return ARC_TIME_COLUMNS.get(self.current_suite, {}).get(table, "time")

    def insert_sorted(self, source: pl.LazyFrame, table: TableName) -> None:
        # Arc writes one file per hour partition per flush, so unsorted input scatters every batch
        # over the whole time range. Sort on the host first, as the QuestDB connector does.
        staged = get_settings().temporary_directory / "arc" / f"{table}_{uuid.uuid4().hex}.parquet"
        staged.parent.mkdir(parents=True, exist_ok=True)
        try:
            with self.phase_context("sort_input", table_name=table):
                source.sort(self.time_column(table)).sink_parquet(staged)
            with self.phase_context("insert", table_name=table):
                self.insert(pl.scan_parquet(staged), table)
        finally:
            staged.unlink(missing_ok=True)

    def _write(self, df: pl.DataFrame | pl.LazyFrame, table: TableName) -> int:
        # The typed Parquet import is used instead of the MessagePack write API: MessagePack
        # infers types per write (an all-null column becomes VARCHAR for the whole measurement)
        # and guesses the epoch unit from magnitude, misreading timestamps past year ~2286.
        rows = 0
        for batch in iter_write_batches(df):
            if batch.is_empty():
                continue
            for chunk in encode_import_chunks(batch):
                self._request(
                    "POST",
                    "/api/v1/import/parquet",
                    params={"measurement": table, "time_column": self.time_column(table)},
                    files={"file": (f"{table}.parquet", chunk, "application/vnd.apache.parquet")},
                )
            rows += batch.height
        return rows

    def insert(
        self,
        df: pl.DataFrame | pl.LazyFrame,
        table: TableName,
        primary_key: str | list[str] | None = None,
        not_null: str | list[str] | None = None,
    ) -> None:
        _ = primary_key, not_null
        initial_count = self.get_count(table) if table in self.get_table_names() else 0
        rows = self._write(df, table)
        if self.current_suite in ("time_series", "time_series_metrics", "time_series_ranges"):
            with self.phase_context("compact", table_name=table):
                self.compact()
        self.wait_until_count(table, initial_count + rows)
        _LOGGER.info(f"Inserted {rows:_} rows into Arc measurement '{table}'")

    def _delete(self, table: TableName, primary_key: str | list[str], keys: pl.DataFrame) -> int:
        response = self._request(
            "POST",
            "/api/v1/delete",
            json={
                "database": ARC_DATABASE,
                "measurement": table,
                "where": build_key_predicate(keys, require_columns(primary_key), self.time_column(table)),
                "confirm": True,
            },
        )
        body = cast(dict[str, Any], response.json())
        if not body.get("success"):
            raise ArcError(f"Arc delete failed: {body}")
        return int(body["deleted_count"])

    def upsert(self, df: pl.DataFrame, table: TableName, primary_key: str | list[str]) -> None:
        # Arc has no upsert; the delete API rewrites the affected Parquet files synchronously,
        # so delete-then-insert leaves exactly one row per key once the insert is visible
        initial_count = self.get_count(table)
        deleted = self._delete(table, primary_key, df.select(require_columns(primary_key)))
        rows = self._write(df, table)
        self.wait_until_count(table, initial_count - deleted + rows)

    def delete(self, table: TableName, primary_key: str | list[str], keys: pl.DataFrame) -> None:
        self._delete(table, primary_key, keys)

    def _run_compaction_cycle(self) -> dict[str, Any]:
        triggered = cast(
            dict[str, Any],
            self._request("POST", "/api/v1/compaction/trigger", params={"database": ARC_DATABASE}).json(),
        )
        cycle_id = int(triggered["cycle_id"])
        deadline = perf_counter() + ARC_COMPACTION_TIMEOUT_SECONDS
        while True:
            stats = cast(dict[str, Any], self._request("GET", "/api/v1/compaction/stats").json())
            last_cycle = cast(dict[str, Any], stats["last_cycle"])
            if int(last_cycle["cycle_id"]) >= cycle_id and not stats["cycle_running"]:
                return last_cycle
            if perf_counter() > deadline:
                raise TimeoutError(f"Timed out waiting for Arc compaction cycle {cycle_id}")
            sleep(1.0)

    def compact(self) -> None:
        # Arc writes one Parquet file per hour partition and flush; its scheduled compaction
        # (hourly, and daily at 03:00) merges them. Run it once after loading so queries see the
        # steady-state layout, and so a scheduled cycle does not start in the middle of a run.
        cycle = self._run_compaction_cycle()
        if int(cycle["failed_batches"]):
            raise ArcError(f"Arc compaction failed: {cycle}")
        # Arc 26.09.3 sometimes fails to read a partition's compaction manifest and skips that
        # partition (a discovery error) instead of compacting it; a second cycle usually picks it up.
        if int(cycle["discovery_errors"]):
            cycle = self._run_compaction_cycle()
            if int(cycle["failed_batches"]):
                raise ArcError(f"Arc compaction failed: {cycle}")
            if int(cycle["discovery_errors"]):
                _LOGGER.warning(f"Arc left {cycle['discovery_errors']} partition(s) uncompacted: {cycle}")

    def suite_registry(self) -> Mapping[SuiteName, type[BenchmarkSuite[Any]]]:
        # Arc stores a measurement only with a non-null `time` column: its Parquet import rejects
        # a table without one or with a null in it, and its MessagePack API fills a missing `time`
        # with the ingestion time, adding a column the suite does not have. Not registered:
        # - rtabench: products and order_items have no timestamp column
        # - kaggle_airbnb: neighbourhoods has no timestamp column, and listings has no non-null one
        # - tpc_h, tpc_ds: the dimension tables have no timestamp column
        # - jsonbench: the loader keeps the row count by inserting `{}` for each repaired split
        #   record, and those rows have no time_us
        return {
            "time_series": ArcTimeSeries,
            "time_series_metrics": ArcTimeSeriesMetrics,
            "time_series_ranges": TimeSeriesRanges,
            "clickbench": ArcClickbench,
            "chat_threads": ArcChatThreads,
        }
