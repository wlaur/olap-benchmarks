from __future__ import annotations

import logging
import multiprocessing
import shutil
from collections.abc import Callable, Iterator, Mapping
from contextlib import AbstractContextManager, nullcontext
from pathlib import Path
from typing import Any, Protocol, cast

import duckdb
import polars as pl
import pyarrow.parquet as pq
from sqlalchemy import Connection, text

from ..settings import TableName, get_settings

_LOGGER = logging.getLogger(__name__)


class SupportsCommit(Protocol):
    def commit(self) -> object | None: ...


type QueryRecorder = Callable[[str], AbstractContextManager[None]]


def record_query_execution_context(
    query: str,
    recorder_source: object | None,
) -> AbstractContextManager[None]:
    if recorder_source is None:
        return nullcontext()

    info = cast(Mapping[str, object] | None, getattr(recorder_source, "info", None))
    if not isinstance(info, Mapping):
        return nullcontext()

    recorder_obj = info.get("olap_query_recorder")
    if callable(recorder_obj):
        recorder = cast(QueryRecorder, recorder_obj)
        return recorder(query)

    return nullcontext()


def tracked_commit(connection: SupportsCommit, recorder_source: object | None = None) -> None:
    with record_query_execution_context("COMMIT", recorder_source or connection):
        connection.commit()


def normalize_columns(value: str | list[str] | None) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    return value


def require_columns(value: str | list[str], kind: str = "primary_key") -> list[str]:
    columns = normalize_columns(value)
    if not columns:
        raise ValueError(f"{kind} must be a non-empty string or list of strings")
    return columns


def iter_parquet_frames(fpath: Path, batch_size: int) -> Iterator[pl.DataFrame]:
    parquet_file = cast(Any, pq.ParquetFile(fpath))

    for batch in parquet_file.iter_batches(batch_size=batch_size):
        yield cast(pl.DataFrame, cast(Any, pl).from_arrow(batch))


def sort_parquet_file(source: Path, target: Path, column: str) -> None:
    # Out-of-core sort with a bounded share of the client memory budget; an in-memory sort of a
    # full-scale input can exceed the whole client-tree limit before writing a single row. DuckDB's
    # allocator keeps that memory resident after the connection closes, so the sort runs in a
    # short-lived child process.
    settings = get_settings()
    memory_gb = max(1, (settings.client_memory_limit_gb or 32) // 2)
    process = multiprocessing.get_context("spawn").Process(
        target=_sort_parquet_file,
        args=(source, target, column, memory_gb, settings.benchmark_threads),
        name="olap-sort-parquet",
    )
    process.start()
    process.join()
    if process.exitcode != 0:
        target.unlink(missing_ok=True)
        raise RuntimeError(f"Sorting {source} by {column} failed with exit code {process.exitcode}")


def _sort_parquet_file(source: Path, target: Path, column: str, memory_gb: int, threads: int | None) -> None:
    spill = target.parent / f".{target.stem}-sort-spill"
    config: dict[str, str | bool | int | float | list[str]] = {
        "memory_limit": f"{memory_gb}GiB",
        "temp_directory": str(spill),
    }
    if threads is not None:
        config["threads"] = threads
    try:
        with duckdb.connect(config=config) as connection:
            connection.execute("SET enable_progress_bar = false")
            source_literal = str(source).replace("'", "''")
            target_literal = str(target).replace("'", "''")
            connection.execute(
                f"COPY (SELECT * FROM read_parquet('{source_literal}') ORDER BY \"{column}\") "
                f"TO '{target_literal}' (FORMAT parquet, COMPRESSION snappy)"
            )
    finally:
        shutil.rmtree(spill, ignore_errors=True)


def drop_table(table: TableName, connection: Connection, commit: bool = True) -> None:
    connection.execute(text(f'drop table if exists "{table}"'))

    if commit:
        tracked_commit(connection)
