from __future__ import annotations

import logging
from collections.abc import Callable, Iterator, Mapping
from contextlib import AbstractContextManager, nullcontext
from pathlib import Path
from typing import Any, Protocol, cast

import polars as pl
import pyarrow.parquet as pq
from sqlalchemy import Connection, text

from ..settings import TableName

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


def drop_table(table: TableName, connection: Connection, commit: bool = True) -> None:
    connection.execute(text(f'drop table if exists "{table}"'))

    if commit:
        tracked_commit(connection)
