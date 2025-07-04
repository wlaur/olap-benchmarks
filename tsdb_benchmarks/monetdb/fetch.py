import contextlib
import json
import shutil
import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import cast

import numpy as np
import polars as pl
import pyarrow as pa
from sqlalchemy import Connection

from .settings import SETTINGS as MONETDB_SETTINGS
from .utils import (
    BOOLEAN_NULL,
    MONETDB_DATE_RECORD_TYPE,
    MONETDB_DATETIME_RECORD_TYPE,
    MONETDB_TEMPORARY_DIRECTORY,
    MONETDB_TIME_RECORD_TYPE,
    POLARS_NUMPY_STRUCT_PACKING_CODE_MAP,
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

    # TODO: bug with pymonetdb where the initial 100 rows are fetched using normal and the rest with binary
    # the behavior is not identical for JSON columns (binary fetch does not call json.loads)
    ret = c.fetchall()

    description = c.description
    assert description is not None
    schema = schema = {n.name: get_polars_type(n.type_code) for n in description}

    df = pl.DataFrame(ret, schema, orient="row")

    return df


def fetch_schema(query: str, connection: Connection) -> dict[str, tuple[pl.DataType | type[pl.DataType], SchemaMeta]]:
    query = get_limit_query(query)

    con = get_pymonetdb_connection(connection)
    c = con.cursor()
    c.execute(query)

    description = c.description
    assert description is not None
    return {n.name: (get_polars_type(n.type_code), get_schema_meta(n)) for n in description}


def read_datetime_column(path: Path, dtype: pl.DataType | type[pl.DataType]) -> pl.Series:
    with path.open("rb") as f:
        data = f.read()

    records = np.frombuffer(data, dtype=MONETDB_DATETIME_RECORD_TYPE)

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
        .cast(dtype)
        .alias("time")
    ).get_column("time")


def read_time_column(path: Path) -> pl.Series:
    with path.open("rb") as f:
        data = f.read()

    records = np.frombuffer(data, dtype=MONETDB_TIME_RECORD_TYPE)

    is_null = (
        (records["ms"] == 0xFFFFFFFF)
        | (records["seconds"] >= 60)
        | (records["minutes"] >= 60)
        | (records["hours"] >= 24)
    )

    nanos = (
        (records["hours"].astype(np.uint64) * 3600_000)
        + (records["minutes"].astype(np.uint64) * 60_000)
        + (records["seconds"].astype(np.uint64) * 1000)
        + records["ms"].astype(np.uint64)
    ) * 1_000_000

    series = pl.Series(nanos, dtype=pl.UInt64)

    result = (
        series.to_frame()
        .select(pl.when(pl.Series(is_null)).then(None).otherwise(pl.col(series.name)).cast(pl.Time))
        .to_series()
    )

    return result


def read_date_column(path: Path) -> pl.Series:
    with path.open("rb") as f:
        data = f.read()

    records = np.frombuffer(data, dtype=MONETDB_DATE_RECORD_TYPE)

    df = pl.DataFrame(
        {
            "year": records["year"],
            "month": records["month"],
            "day": records["day"],
        }
    )

    return df.select(
        pl.when(pl.col("year") == -1).then(None).otherwise(pl.date("year", "month", "day")).alias("date")
    ).get_column("date")


def read_string_column(path: Path) -> pl.Series:
    data = path.read_bytes()
    nul_positions = np.flatnonzero(np.frombuffer(data, dtype=np.uint8) == 0x00)

    n = len(nul_positions)
    result: list[bytes | None] = [None] * n

    start = 0

    for idx, end in enumerate(nul_positions):
        if end - start == 1 and data[start] == 0x80:
            result[idx] = None
        elif end == start:
            result[idx] = b""
        else:
            slice_end = end
            result[idx] = data[start:slice_end]

        start = end + 1

    decoded_array = pa.array(result, type=pa.binary())
    string_array = pa.compute.cast(decoded_array, pa.string())
    return cast(pl.Series, pl.from_arrow(string_array))


def read_json_column(path: Path) -> pl.Series:
    s = read_string_column(path).alias("json")

    # inefficient map with json.loads to match pymonetdb behavior
    return s.map_elements(json.loads, pl.Object)

    # maybe not safe to convert to pl.Struct, only makes sense if all JSON values are similar
    # TODO: make this a configurable setting
    # return s.str.json_decode(infer_schema_length=None)


def read_blob_column(path: Path) -> pl.Series:
    with path.open("rb") as f:
        data = f.read()

    result: list[bytes | None] = []

    offset = 0
    data_len = len(data)

    while offset + 8 <= data_len:
        length_bytes = data[offset : offset + 8]
        length = int.from_bytes(length_bytes, byteorder="little")
        offset += 8

        if length == 0xFFFFFFFFFFFFFFFF:
            result.append(None)
        else:
            if offset + length > data_len:
                raise ValueError("File ends prematurely while reading blob data")

            blob_data = data[offset : offset + length]
            result.append(blob_data)
            offset += length

    return pl.Series(result, dtype=pl.Binary)


def read_numeric_column(path: Path, dtype: pl.DataType | type[pl.DataType]) -> pl.Series:
    with path.open("rb") as f:
        data = f.read()

    np_dtype = POLARS_NUMPY_STRUCT_PACKING_CODE_MAP.get(dtype)

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
        s = s.replace(BOOLEAN_NULL, None).cast(dtype)

    if dtype in (pl.Int8, pl.Int16, pl.Int32, pl.Int64):
        sentinel = np.iinfo(values.dtype).min
        s = s.replace(sentinel, None)

    with contextlib.suppress(Exception):
        s = s.fill_nan(None)

    return s


def read_binary_column_data(path: Path, dtype: pl.DataType | type[pl.DataType], meta: SchemaMeta) -> pl.Series:
    match dtype:
        case pl.Time:
            return read_time_column(path)
        case pl.Date:
            return read_date_column(path)
        case pl.Datetime:
            return read_datetime_column(path, dtype)
        case pl.String:
            return read_string_column(path)
        case pl.Object:
            return read_json_column(path)
        case pl.Binary:
            return read_blob_column(path)
        case _:
            return read_numeric_column(path, dtype)


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

    temp_dir = MONETDB_TEMPORARY_DIRECTORY / "data" / str(uuid.uuid4())[:4]
    temp_dir.mkdir()
    path_prefix = "" if MONETDB_SETTINGS.client_file_transfer else "/"

    output_files = [temp_dir / f"{idx}.bin" for idx in range(len(expanded_schema))]
    files_clause = ",".join(
        f"'{path_prefix}{n.relative_to(MONETDB_TEMPORARY_DIRECTORY).as_posix()}'" for n in output_files
    )

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
