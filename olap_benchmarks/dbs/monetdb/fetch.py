import logging
import shutil
import uuid
from collections.abc import Mapping
from time import perf_counter
from typing import Literal

import polars as pl
from sqlalchemy import Connection

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


def fetch_pymonetdb(query: str, connection: Connection) -> pl.DataFrame:
    con = get_pymonetdb_connection(connection)
    c = con.cursor()
    c.execute(query)

    # TODO: bug with pymonetdb where the initial 100 rows are fetched using normal and the rest with binary
    # the behavior is not identical for JSON columns (binary fetch does not call json.loads)
    ret = c.fetchall()

    description = c.description
    assert description is not None
    schema = {n.name: get_polars_type(n.type_code, n.precision, n.scale) for n in description}

    df = pl.DataFrame(ret, schema, orient="row")

    return df


def fetch_schema(query: str, connection: Connection) -> dict[str, tuple[pl.DataType | type[pl.DataType], SchemaMeta]]:
    t0 = perf_counter()

    query = get_limit_query(query)

    con = get_pymonetdb_connection(connection)
    c = con.cursor()
    c.execute(query)

    description = c.description
    assert description is not None
    ret = {n.name: (get_polars_type(n.type_code, n.precision, n.scale), get_schema_meta(n)) for n in description}

    _LOGGER.info(f"Fetched schema with {len(ret):_} columns in {1_000 * (perf_counter() - t0):.2f} ms")

    return ret


def infer_schema(query: str, connection: Connection) -> dict[str, tuple[pl.DataType | type[pl.DataType], SchemaMeta]]:
    t0 = perf_counter()
    con = get_pymonetdb_connection(connection)
    c = con.cursor()
    c.execute(f"PREPARE {query}")

    description = c.description
    assert description is not None
    ret = c.fetchall()

    # could also keep the prepared statement since we'll execute it shortly,
    # probably not worth the extra complexity though
    c.execute("DEALLOCATE ALL")

    schema = {n.name: get_polars_type(n.type_code, n.precision, n.scale) for n in description}

    df = pl.DataFrame(ret, schema, orient="row")

    ret = {n["column"]: (get_polars_type(n["type"]), SchemaMeta()) for n in df.to_dicts()}

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
        con.execute(
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
