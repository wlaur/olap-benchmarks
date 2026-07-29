import logging
import uuid
from collections.abc import Mapping
from pathlib import Path
from time import perf_counter
from typing import Any, cast

import polars as pl
from adbc_driver_monetdb import ParquetArrowStream, PolarsArrowStream
from sqlalchemy import Connection, text
from sqlalchemy_monetdb_adbc import fetch_arrow_table, ingest_arrow
from sqlalchemy_monetdb_adbc.arrow import ArrowIngestData

from ...settings import TableName
from .. import ParquetEpochColumns
from ..utils import drop_table, record_query_execution_context, tracked_commit
from .utils import create_table, get_table

_LOGGER = logging.getLogger(__name__)


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


def _validated_ingest_row_count(reported_rows: int, expected_rows: int, context: str) -> int:
    if reported_rows < 0:
        raise RuntimeError(f"ADBC did not report an inserted row count for the {expected_rows:_}-row {context}")
    if reported_rows != expected_rows:
        raise RuntimeError(f"ADBC reported {reported_rows:_} inserted rows for a {expected_rows:_}-row {context}")
    return reported_rows


def insert_adbc(
    frame: pl.DataFrame | pl.LazyFrame,
    table: TableName,
    connection: Connection,
    primary_key: str | list[str] | None = None,
    not_null: str | list[str] | None = None,
    create: bool = True,
    commit: bool = True,
) -> None:
    schema = frame.schema if isinstance(frame, pl.DataFrame) else frame.collect_schema()

    lazy_stream: PolarsArrowStream | None = None
    if isinstance(frame, pl.DataFrame):
        expected_rows = frame.height
        data: pl.DataFrame | PolarsArrowStream = frame
    else:
        expected_rows = 0
        lazy_stream = PolarsArrowStream(frame)
        data = lazy_stream

    _insert_arrow_adbc(
        data,
        table,
        schema,
        connection,
        primary_key,
        not_null,
        expected_rows=expected_rows if lazy_stream is None else None,
        lazy_stream=lazy_stream,
        create=create,
        commit=commit,
    )


def insert_parquet_adbc(
    path: Path,
    table: TableName,
    connection: Connection,
    primary_key: str | list[str] | None = None,
    not_null: str | list[str] | None = None,
    *,
    create: bool = True,
    commit: bool = True,
    epoch_columns: ParquetEpochColumns | None = None,
) -> None:
    with ParquetArrowStream(path, epoch_columns=epoch_columns) as stream:
        _insert_arrow_adbc(
            stream,
            table,
            pl.Schema(stream.schema),
            connection,
            primary_key,
            not_null,
            expected_rows=stream.num_rows,
            create=create,
            commit=commit,
        )


def _insert_arrow_adbc(
    data: ArrowIngestData,
    table: TableName,
    schema: pl.Schema,
    connection: Connection,
    primary_key: str | list[str] | None,
    not_null: str | list[str] | None,
    *,
    expected_rows: int | None,
    create: bool,
    commit: bool,
    lazy_stream: PolarsArrowStream | None = None,
) -> None:
    started = perf_counter()
    destination = get_table(table, schema, primary_key=primary_key, not_null=not_null)

    with record_query_execution_context(f'ADBC INGEST INTO "{table}"', connection):
        inserted_rows = ingest_arrow(
            connection,
            destination,
            data,
            mode="append",
            create=create,
        )

    if lazy_stream is not None:
        expected_rows = lazy_stream.rows_read
    if expected_rows is None:
        raise RuntimeError("ADBC ingest completed without an expected row count")
    inserted_rows = _validated_ingest_row_count(inserted_rows, expected_rows, "dataset")

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
                frame,
                mode="append",
                temporary=True,
            )
        _validated_ingest_row_count(inserted_rows, frame.height, "upsert dataset")

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
                keys,
                mode="append",
                temporary=True,
            )
        _validated_ingest_row_count(inserted_rows, keys.height, "delete key dataset")

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
