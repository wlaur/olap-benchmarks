import io
import logging
import subprocess
from collections.abc import Iterator, Mapping
from time import perf_counter, sleep
from typing import Any, ClassVar, cast

import httpx
import polars as pl
import pyarrow as pa
from pyarrow import ipc
from pydantic import PrivateAttr
from sqlalchemy import Connection

from ...settings import DatabaseName, SuiteName, TableName, host_port
from ...suites import BenchmarkSuite
from ...suites.time_series.config import TimeSeries
from .. import Database
from ..utils import require_columns

_LOGGER = logging.getLogger(__name__)

VERSION = "26.09.3"

DOCKER_IMAGE = f"basekicklabs/arc:{VERSION}"

ARC_PORT = host_port("arc")

ARC_DATABASE = "olap"

ARC_WRITE_BATCH_CELLS = 8_000_000

ARC_VISIBILITY_TIMEOUT_SECONDS = 600.0

ARC_COMPACTION_TIMEOUT_SECONDS = 3_600.0


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


def encode_parquet(df: pl.DataFrame) -> bytes:
    buffer = io.BytesIO()
    df.write_parquet(buffer, compression="uncompressed", statistics=False)
    return buffer.getvalue()


def build_time_key_predicate(keys: pl.DataFrame, primary_key: list[str]) -> str:
    if primary_key != ["time"]:
        raise ValueError(f"Arc deletes are keyed on the time column only, got {primary_key}")

    values = keys.get_column("time").cast(pl.Datetime("us")).unique().sort()
    literals = ", ".join(f"'{value.isoformat(sep=' ')}'" for value in values.to_list())
    return f"time IN ({literals})"


def decode_arrow_stream(payload: bytes) -> pl.DataFrame:
    table = ipc.open_stream(pa.py_buffer(payload)).read_all()
    df = cast(pl.DataFrame, cast(Any, pl).from_arrow(table))
    return df.with_columns(
        pl.col(name).dt.convert_time_zone("UTC").dt.replace_time_zone(None)
        for name, dtype in df.schema.items()
        if isinstance(dtype, pl.Datetime) and dtype.time_zone is not None
    )


class ArcTimeSeries(TimeSeries["Arc"]):
    def populate(self, restart: bool = True) -> None:
        super().populate(restart=False)

        with self.db.phase_context("compact"):
            self.db.compact()

        with self.db.phase_context("verify_compacted"):
            self.verify_populated_data()

        if restart:
            self.db.restart_event()


class Arc(Database):
    name: DatabaseName = "arc"
    version: str = VERSION
    container_image: ClassVar[str | None] = DOCKER_IMAGE
    supports_arm64_containers: ClassVar[bool] = True

    connection_string: str = f"http://localhost:{ARC_PORT}"

    _client: httpx.Client | None = PrivateAttr(default=None)

    @property
    def start(self) -> str:
        return self.docker_run_command(
            DOCKER_IMAGE,
            ports={str(ARC_PORT): "8000"},
            mounts={self.database_directory.as_posix(): "/app/data"},
            env={
                "ARC_AUTH_ENABLED": "false",
                "ARC_TELEMETRY_ENABLED": "false",
                "ARC_DELETE_ENABLED": "true",
            },
        )

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

    def _write(self, df: pl.DataFrame | pl.LazyFrame, table: TableName) -> int:
        # The typed Parquet import is used instead of the MessagePack write API: MessagePack
        # infers types per write (an all-null column becomes VARCHAR for the whole measurement)
        # and guesses the epoch unit from magnitude, misreading timestamps past year ~2286.
        rows = 0
        for batch in iter_write_batches(df):
            if batch.is_empty():
                continue
            self._request(
                "POST",
                "/api/v1/import/parquet",
                params={"measurement": table},
                files={"file": (f"{table}.parquet", encode_parquet(batch), "application/vnd.apache.parquet")},
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
        self.wait_until_count(table, initial_count + rows)
        _LOGGER.info(f"Inserted {rows:_} rows into Arc measurement '{table}'")

    def _delete(self, table: TableName, primary_key: str | list[str], keys: pl.DataFrame) -> int:
        response = self._request(
            "POST",
            "/api/v1/delete",
            json={
                "database": ARC_DATABASE,
                "measurement": table,
                "where": build_time_key_predicate(keys, require_columns(primary_key)),
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
        deleted = self._delete(table, primary_key, df)
        rows = self._write(df, table)
        self.wait_until_count(table, initial_count - deleted + rows)

    def delete(self, table: TableName, primary_key: str | list[str], keys: pl.DataFrame) -> None:
        self._delete(table, primary_key, keys)

    def _compaction_pending(self) -> bool:
        candidates = cast(dict[str, Any], self._request("GET", "/api/v1/compaction/candidates").json())
        status = cast(dict[str, Any], self._request("GET", "/api/v1/compaction/status").json())
        pending = [
            candidate
            for candidate in cast(list[dict[str, Any]], candidates.get("candidates") or [])
            if candidate.get("database") == ARC_DATABASE
        ]
        return bool(pending) or bool(status["manager"].get("active_jobs"))

    def compact(self) -> None:
        # Arc writes one Parquet file per hour partition and flush; its scheduled compaction
        # (hourly, and daily at 03:00) merges them. Run it once after loading so queries see the
        # steady-state layout, and so a scheduled cycle does not start in the middle of a run.
        self._request("POST", "/api/v1/compaction/trigger", params={"database": ARC_DATABASE})

        deadline = perf_counter() + ARC_COMPACTION_TIMEOUT_SECONDS
        sleep(1.0)
        while self._compaction_pending():
            if perf_counter() > deadline:
                raise TimeoutError("Timed out waiting for Arc compaction")
            sleep(1.0)

    def suite_registry(self) -> Mapping[SuiteName, type[BenchmarkSuite[Any]]]:
        return {"time_series": ArcTimeSeries}
