import contextlib
import shutil
import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import cast

import numpy as np
import polars as pl
import pyarrow as pa
from sqlalchemy import Connection

from .utils import (
    BOOLEAN_TRUE,
    UPLOAD_DOWNLOAD_DIRECTORY,
    SchemaMeta,
    ensure_downloader_uploader,
    get_limit_query,
    get_polars_type,
    get_pymonetdb_connection,
    get_schema_meta,
)


def fetch_pymonetdb(query: str, connection: Connection) -> pl.DataFrame:
    con = get_pymonetdb_connection(connection)
    c = con.cursor()
    c.execute(query)
    ret = c.fetchall()

    description = c.description
    assert description is not None
    return pl.DataFrame(ret, schema={n.name: get_polars_type(n.type_code) for n in description}, orient="row")


def fetch_schema(query: str, connection: Connection) -> dict[str, tuple[pl.DataType | type[pl.DataType], SchemaMeta]]:
    query = get_limit_query(query)

    con = get_pymonetdb_connection(connection)
    c = con.cursor()
    c.execute(query)

    description = c.description
    assert description is not None
    return {n.name: (get_polars_type(n.type_code), get_schema_meta(n)) for n in description}


def read_timestamp_column(path: Path) -> pl.Series:
    with path.open("rb") as f:
        data = f.read()

    record_dtype = np.dtype(
        [
            ("ms", "<u4"),
            ("seconds", "u1"),
            ("minutes", "u1"),
            ("hours", "u1"),
            ("padding", "u1"),
            ("day", "u1"),
            ("month", "u1"),
            ("year", "<i2"),
        ]
    )

    records = np.frombuffer(data, dtype=record_dtype)

    df = pl.DataFrame(
        {
            "year": records["year"],
            "month": records["month"],
            "day": records["day"],
            "hour": records["hours"],
            "minute": records["minutes"],
            "second": records["seconds"],
            "microsecond": records["ms"] * 1000,
        }
    )

    return df.select(
        pl.when(pl.col("year") == -1)
        .then(None)
        .otherwise(
            pl.datetime(
                "year", "month", "day", "hour", "minute", "second", "microsecond", time_unit="ms", time_zone=None
            )
        )
        .alias("time")
    ).get_column("time")


def read_text_column(path: Path, meta: SchemaMeta) -> pl.Series:
    data = path.read_bytes()
    nul_positions = np.flatnonzero(np.frombuffer(data, dtype=np.uint8) == 0x00)

    result: list[bytes | None] = []
    start = 0
    max_size = meta.size

    for end in nul_positions:
        if end - start == 1 and data[start] == 0x80:
            result.append(None)
        elif end == start:
            result.append(b"")
        else:
            slice_end = end
            if max_size is not None and (end - start) > max_size:
                slice_end = start + max_size
            result.append(data[start:slice_end])
        start = end + 1

    decoded_array = pa.array(result, type=pa.binary())
    string_array = pa.compute.cast(decoded_array, pa.string())
    return cast(pl.Series, pl.from_arrow(string_array))


def read_binary_column_data(path: Path, dtype: pl.DataType | type[pl.DataType], meta: SchemaMeta) -> pl.Series:
    if dtype == pl.Datetime("ms"):
        return read_timestamp_column(path)

    if dtype == pl.String:
        return read_text_column(path, meta)

    with path.open("rb") as f:
        data = f.read()

    np_dtype_map: dict[pl.DataType | type[pl.DataType], str] = {
        pl.Boolean: "<u1",
        pl.Int8: "<i1",
        pl.Int16: "<i2",
        pl.Int32: "<i4",
        pl.Int64: "<i8",
        pl.UInt8: "<u1",
        pl.UInt16: "<u2",
        pl.UInt32: "<u4",
        pl.UInt64: "<u8",
        pl.Float32: "<f4",
        pl.Float64: "<f8",
    }

    np_dtype = np_dtype_map.get(dtype)

    if np_dtype is None:
        raise ValueError(f"Unsupported dtype: {dtype}")

    if dtype == pl.Boolean:
        is_bool = True
        dtype = pl.UInt8
    else:
        is_bool = False

    values = np.frombuffer(data, dtype=np_dtype)

    s = pl.Series(values, dtype=dtype)

    if is_bool:
        dtype = pl.Boolean
        s = s.replace(BOOLEAN_TRUE, None).cast(dtype)

    if dtype in (pl.Int8, pl.Int16, pl.Int32, pl.Int64):
        sentinel = np.iinfo(values.dtype).min
        s = s.replace(sentinel, None)

    with contextlib.suppress(Exception):
        s = s.fill_nan(None)

    return s


def fetch_binary(
    query: str,
    connection: Connection,
    schema: Mapping[str, pl.DataType | type[pl.DataType] | tuple[pl.DataType | type[pl.DataType], SchemaMeta]]
    | None = None,
) -> pl.DataFrame:
    con = get_pymonetdb_connection(connection)
    ensure_downloader_uploader(con)

    if schema is None:
        expanded_schema = fetch_schema(query, connection)
    else:
        expanded_schema = {
            k: (v if not isinstance(v, tuple) else v[0], v[1] if isinstance(v, tuple) else SchemaMeta())
            for k, v in schema.items()
        }

    temp_dir = UPLOAD_DOWNLOAD_DIRECTORY / str(uuid.uuid4())[:4]
    temp_dir.mkdir()

    output_files = [temp_dir / f"{idx}.bin" for idx in range(len(expanded_schema))]
    output_files_repr = ",".join(f"'{temp_dir.name}/{n.name}'" for n in output_files)

    try:
        con.execute(f"copy {query} into little endian binary {output_files_repr} on client")

        columns: dict[str, pl.Series] = {}

        for (col_name, (dtype, meta)), path in zip(expanded_schema.items(), output_files, strict=True):
            columns[col_name] = read_binary_column_data(path, dtype, meta)

    finally:
        shutil.rmtree(temp_dir)

    df = pl.DataFrame(columns, orient="row")
    return df
