import logging
import uuid
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from queue import Full, Queue
from threading import Event, Thread
from time import perf_counter
from typing import Any, cast

import polars as pl
import pyarrow as pa
from sqlalchemy import Connection, text
from sqlalchemy_monetdb_adbc import fetch_arrow_table, ingest_arrow

from ...settings import TableName
from ..utils import drop_table, record_query_execution_context, tracked_commit
from .utils import create_table, get_table

_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class _ProducerError:
    error: BaseException


def fetch_adbc(
    query: str,
    connection: Connection,
    schema: Mapping[str, pl.DataType | type[pl.DataType]] | None = None,
) -> pl.DataFrame:
    with record_query_execution_context(query, connection):
        arrow_table = fetch_arrow_table(connection, query)

    frame = cast(pl.DataFrame, cast(Any, pl).from_arrow(arrow_table))
    if schema is not None:
        frame = frame.cast(cast(pl.Schema, schema))
    return frame


def _iter_arrow_batches(
    frame: pl.LazyFrame,
    row_counter: list[int],
) -> Iterator[pa.RecordBatch]:
    queue: Queue[pl.DataFrame | _ProducerError | None] = Queue(maxsize=1)
    consumed = Event()
    stopped = Event()

    def put(item: pl.DataFrame | _ProducerError | None) -> bool:
        while not stopped.is_set():
            try:
                queue.put(item, timeout=0.1)
                return True
            except Full:
                pass
        return False

    def receive(batch: pl.DataFrame) -> bool:
        if not put(batch):
            return True
        while not stopped.is_set():
            if consumed.wait(timeout=0.1):
                consumed.clear()
                return False
        return True

    def produce() -> None:
        try:
            frame.sink_batches(receive, lazy=False, engine="streaming")
        except BaseException as error:
            put(_ProducerError(error))
        finally:
            put(None)

    producer = Thread(target=produce, name="monetdb-adbc-producer", daemon=True)
    producer.start()
    try:
        while (item := queue.get()) is not None:
            if isinstance(item, _ProducerError):
                raise item.error
            row_counter[0] += item.height
            yield from item.to_arrow(compat_level=pl.CompatLevel.newest()).to_batches()
            consumed.set()
    finally:
        stopped.set()
        consumed.set()
        producer.join()


def insert_adbc(
    frame: pl.DataFrame | pl.LazyFrame,
    table: TableName,
    connection: Connection,
    primary_key: str | list[str] | None = None,
    not_null: str | list[str] | None = None,
    create: bool = True,
    commit: bool = True,
) -> None:
    started = perf_counter()
    schema = frame.schema if isinstance(frame, pl.DataFrame) else frame.collect_schema()
    destination = get_table(table, schema, primary_key=primary_key, not_null=not_null)

    row_counter = [0]
    if isinstance(frame, pl.DataFrame):
        expected_rows = frame.height
        data: pa.Table | pa.RecordBatchReader = frame.to_arrow()
    else:
        expected_rows = 0
        data = pa.RecordBatchReader.from_batches(
            schema.to_arrow(),
            _iter_arrow_batches(frame, row_counter),
        )

    with record_query_execution_context(f'ADBC INGEST INTO "{table}"', connection):
        inserted_rows = ingest_arrow(
            connection,
            destination,
            data,
            mode="append",
            create=create,
        )

    if isinstance(frame, pl.LazyFrame):
        expected_rows = row_counter[0]
    if inserted_rows != expected_rows:
        raise RuntimeError(f"ADBC reported {inserted_rows:_} inserted rows for a {expected_rows:_}-row dataset")

    if commit:
        tracked_commit(connection)

    _LOGGER.info(
        f"Inserted {inserted_rows:_} rows ({len(schema):_} columns) "
        f"into table {table} with ADBC in {perf_counter() - started:_.2f} seconds"
    )


def upsert_adbc(
    frame: pl.DataFrame,
    table: TableName,
    connection: Connection,
    primary_key: str | list[str],
) -> None:
    started = perf_counter()
    destination = get_table(table, frame.schema)
    source_name = f"_temporary_{uuid.uuid4().hex[:8]}"
    source = create_table(source_name, frame.schema, connection, primary_key=primary_key, temporary=True)

    try:
        with record_query_execution_context(f'ADBC INGEST INTO TEMPORARY "{source.name}"', connection):
            inserted_rows = ingest_arrow(
                connection,
                source,
                frame.to_arrow(),
                mode="append",
                temporary=True,
            )
        if inserted_rows != frame.height:
            raise RuntimeError(
                f"ADBC reported {inserted_rows:_} inserted rows for a {frame.height:_}-row upsert dataset"
            )

        primary_keys = [primary_key] if isinstance(primary_key, str) else list(primary_key)
        shared_columns = [column.name for column in destination.columns if column.name in source.columns]
        update_columns = [column for column in shared_columns if column not in primary_keys]
        if not update_columns:
            raise ValueError("No non-PK columns to upsert")

        on_clause = " and ".join(f'dest."{column}" = source."{column}"' for column in primary_keys)
        update_assignments = ", ".join(f'"{column}" = source."{column}"' for column in update_columns)
        insert_columns = ", ".join(f'"{column}"' for column in shared_columns)
        insert_values = ", ".join(f'source."{column}"' for column in shared_columns)
        merge_statement = (
            f'MERGE INTO "{destination.name}" AS dest '
            f'USING "{source.name}" AS source ON {on_clause} '
            f"WHEN MATCHED THEN UPDATE SET {update_assignments} "
            f"WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})"
        )

        with record_query_execution_context(merge_statement, connection):
            connection.execute(text(merge_statement))
        drop_table(source.name, connection, commit=False)
        tracked_commit(connection)
    except Exception:
        connection.rollback()
        try:
            drop_table(source.name, connection)
        except Exception:
            connection.rollback()
            _LOGGER.exception(f"Failed to clean up temporary ADBC upsert table {source.name}")
        raise

    _LOGGER.info(
        f"Upserted dataset with shape ({frame.height:_}, {frame.width:_}) "
        f"into table {table} with ADBC in {perf_counter() - started:_.2f} seconds"
    )


def delete_adbc(
    table: TableName,
    connection: Connection,
    primary_key: str | list[str],
    keys: pl.DataFrame,
) -> None:
    started = perf_counter()
    source_name = f"_temporary_{uuid.uuid4().hex[:8]}"
    source = create_table(source_name, keys.schema, connection, temporary=True)
    primary_keys = [primary_key] if isinstance(primary_key, str) else list(primary_key)

    try:
        with record_query_execution_context(f'ADBC INGEST INTO TEMPORARY "{source.name}"', connection):
            inserted_rows = ingest_arrow(
                connection,
                source,
                keys.to_arrow(),
                mode="append",
                temporary=True,
            )
        if inserted_rows != keys.height:
            raise RuntimeError(
                f"ADBC reported {inserted_rows:_} inserted rows for a {keys.height:_}-row delete key dataset"
            )

        predicate = " and ".join(f'dest."{column}" = source."{column}"' for column in primary_keys)
        statement = (
            f'DELETE FROM "{table}" AS dest WHERE EXISTS (SELECT 1 FROM "{source.name}" AS source WHERE {predicate})'
        )
        with record_query_execution_context(statement, connection):
            connection.execute(text(statement))
        drop_table(source.name, connection, commit=False)
        tracked_commit(connection)
    except Exception:
        connection.rollback()
        try:
            drop_table(source.name, connection)
        except Exception:
            connection.rollback()
            _LOGGER.exception(f"Failed to clean up temporary ADBC delete table {source.name}")
        raise

    _LOGGER.info(
        f"Deleted from table {table} using {keys.height:_} ADBC key rows in {perf_counter() - started:_.2f} seconds"
    )
