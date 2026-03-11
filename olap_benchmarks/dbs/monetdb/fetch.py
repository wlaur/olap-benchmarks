import logging
import shutil
import uuid
from collections.abc import Mapping
from time import perf_counter
from typing import Any, Literal, Protocol, cast

import polars as pl
from pymonetdb.sql.cursors import Description
from sqlalchemy import Connection, text

from .binary import read_binary_column_data
from .settings import SETTINGS as MONETDB_SETTINGS
from .utils import (
    MONETDB_TEMPORARY_DIRECTORY,
    SchemaMeta,
    ensure_downloader_uploader,
    get_limit_query,
    get_polars_type,
    get_pymonetdb_connection,
    get_schema_meta,
)

_LOGGER = logging.getLogger(__name__)

SchemaMethod = Literal["infer", "fetch"]
DEFAULT_SCHEMA_METHOD: SchemaMethod = "infer"


class _MonetCursor(Protocol):
    description: list[Description] | None

    def execute(self, operation: str, parameters: dict[Any, Any] | None = None) -> int | None: ...

    def fetchall(self) -> list[tuple[Any, ...]]: ...


def _description_values(d: Description) -> tuple[str, str, int | None, int | None]:
    description = cast(Any, d)
    return (
        cast(str, description.name),
        cast(str, description.type_code),
        cast(int | None, description.precision),
        cast(int | None, description.scale),
    )


def fetch_pymonetdb(query: str, connection: Connection) -> pl.DataFrame:
    result = connection.execute(text(query.strip().removesuffix(";")))
    columns = list(result.keys())
    rows = result.fetchall()

    if not rows:
        return pl.DataFrame({col: [] for col in columns})

    return pl.DataFrame({col: [row[idx] for row in rows] for idx, col in enumerate(columns)})


def fetch_schema(query: str, connection: Connection) -> dict[str, tuple[pl.DataType | type[pl.DataType], SchemaMeta]]:
    t0 = perf_counter()

    query = get_limit_query(query)

    con = get_pymonetdb_connection(connection)
    c = cast(_MonetCursor, con.cursor())
    c.execute(query)

    description = c.description
    assert description is not None
    ret: dict[str, tuple[pl.DataType | type[pl.DataType], SchemaMeta]] = {}
    for col in description:
        name, type_code, precision, scale = _description_values(col)
        ret[name] = (get_polars_type(type_code, precision, scale), get_schema_meta(col))

    _LOGGER.info(f"Fetched schema with {len(ret):_} columns in {1_000 * (perf_counter() - t0):.2f} ms")

    return ret


def infer_schema(query: str, connection: Connection) -> dict[str, tuple[pl.DataType | type[pl.DataType], SchemaMeta]]:
    t0 = perf_counter()
    con = get_pymonetdb_connection(connection)
    c = cast(_MonetCursor, con.cursor())
    c.execute(f"PREPARE {query}")

    description = c.description
    assert description is not None
    ret = c.fetchall()

    # could also keep the prepared statement since we'll execute it shortly,
    # probably not worth the extra complexity though
    c.execute("DEALLOCATE ALL")

    schema: dict[str, pl.DataType | type[pl.DataType]] = {}
    for col in description:
        name, type_code, precision, scale = _description_values(col)
        schema[name] = get_polars_type(type_code, precision, scale)

    df = pl.DataFrame(ret, schema, orient="row")

    inferred_rows = df.to_dicts()
    ret = {cast(str, row["column"]): (get_polars_type(cast(str, row["type"])), SchemaMeta()) for row in inferred_rows}

    _LOGGER.info(f"Inferred schema with {len(ret):_} columns in {1_000 * (perf_counter() - t0):.2f} ms")

    return ret


def fetch_binary(
    query: str,
    connection: Connection,
    schema: Mapping[str, pl.DataType | type[pl.DataType] | tuple[pl.DataType | type[pl.DataType], SchemaMeta]]
    | SchemaMethod
    | None = None,
) -> pl.DataFrame:
    if schema is None:
        schema = DEFAULT_SCHEMA_METHOD

    con = get_pymonetdb_connection(connection)
    ensure_downloader_uploader(con)

    if isinstance(schema, dict):
        expanded_schema = {
            k: (v if not isinstance(v, tuple) else v[0], v[1] if isinstance(v, tuple) else SchemaMeta())
            for k, v in schema.items()
        }
    elif schema == "fetch":
        expanded_schema = fetch_schema(query, connection)
    elif schema == "infer":
        expanded_schema = infer_schema(query, connection)
    else:
        raise ValueError(f"Invalid value for schema: {schema}")

    temp_dir = MONETDB_TEMPORARY_DIRECTORY / "data" / str(uuid.uuid4())[:4]
    temp_dir.mkdir()

    path_prefix = "" if MONETDB_SETTINGS.client_file_transfer else "/"
    subdir = temp_dir.relative_to(MONETDB_TEMPORARY_DIRECTORY).as_posix()

    output_files = [temp_dir / f"{idx}.bin" for idx in range(len(expanded_schema))]

    files_clause = ",".join(f"'{path_prefix}{subdir}/{n.name}'" for n in output_files)

    query = query.strip().removesuffix(";")

    try:
        cast(Any, con).execute(
            f"copy {query} into little endian binary {files_clause} "
            f"on {'client' if MONETDB_SETTINGS.client_file_transfer else 'server'}"
        )

        columns: dict[str, pl.Series] = {}

        for (col_name, (dtype, meta)), path in zip(expanded_schema.items(), output_files, strict=True):
            columns[col_name] = read_binary_column_data(path, dtype, meta)

    finally:
        shutil.rmtree(temp_dir)

    df = pl.DataFrame(columns, orient="row")
    return df
