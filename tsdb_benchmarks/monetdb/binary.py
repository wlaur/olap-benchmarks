import contextlib
import json
from pathlib import Path
from typing import cast

import numpy as np
import polars as pl
import pyarrow as pa

from .utils import (
    BOOLEAN_NULL,
    MONETDB_DATE_RECORD_TYPE,
    MONETDB_DATETIME_RECORD_TYPE,
    MONETDB_DEFAULT_DECIMAL_PRECISION,
    MONETDB_DEFAULT_DECIMAL_SCALE,
    MONETDB_TIME_RECORD_TYPE,
    POLARS_NUMPY_TYPE_MAP,
)

BLOB_NULL_MARKER = (0xFFFFFFFFFFFFFFFF).to_bytes(8, byteorder="little")
STRING_NULL_MARKER = b"\x80\x00"

DATETIME_NULL_RECORD = {
    "ms": 0xFFFFFFFF,
    "seconds": 255,
    "minutes": 255,
    "hours": 255,
    "padding": 255,
    "day": 255,
    "month": 255,
    "year": -1,
}

DATE_NULL_RECORD = {
    "day": 255,
    "month": 255,
    "year": -1,
}

TIME_NULL_RECORD = {
    "ms": 0xFFFFFFFF,
    "seconds": 255,
    "minutes": 255,
    "hours": 255,
    "padding": 255,
}


def decimal_numpy_dtype(precision: int) -> np.dtype:
    if 1 <= precision <= 2:
        return np.int8  # type: ignore[return-value]
    if 3 <= precision <= 4:
        return np.int16  # type: ignore[return-value]
    if 5 <= precision <= 9:
        return np.int32  # type: ignore[return-value]
    if 10 <= precision <= 18:
        return np.int64  # type: ignore[return-value]
    raise ValueError(f"Decimal precision {precision} too large for integer-based encoding (needs 16 bytes)")


def numpy_to_polars_int_dtype(np_dtype: np.dtype) -> type[pl.DataType]:
    if np_dtype == np.int8:
        return pl.Int8
    if np_dtype == np.int16:
        return pl.Int16
    if np_dtype == np.int32:
        return pl.Int32
    if np_dtype == np.int64:
        return pl.Int64
    raise ValueError(f"Unsupported NumPy integer dtype: {np_dtype}")


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


def write_date_column(series: pl.Series, path: Path) -> None:
    null_mask = series.is_null().to_numpy()
    n = len(series)
    data = np.zeros(n, dtype=MONETDB_DATE_RECORD_TYPE)

    for k, v in DATE_NULL_RECORD.items():
        data[k][null_mask] = v

    valid_mask = ~null_mask

    if valid_mask.any():
        parts = (
            series.to_frame("dt")
            .with_columns(
                day=pl.col("dt").dt.day(),
                month=pl.col("dt").dt.month(),
                year=pl.col("dt").dt.year(),
            )
            .filter(pl.col("dt").is_not_null())
            .drop("dt")
        )
        parts_np = parts.to_numpy()

        data["day"][valid_mask] = parts_np[:, 0]
        data["month"][valid_mask] = parts_np[:, 1]
        data["year"][valid_mask] = parts_np[:, 2]

    path.write_bytes(data.tobytes())


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


def write_time_column(series: pl.Series, path: Path) -> None:
    null_mask = series.is_null().to_numpy()
    n = len(series)
    data = np.zeros(n, dtype=MONETDB_TIME_RECORD_TYPE)

    for k, v in TIME_NULL_RECORD.items():
        data[k][null_mask] = v

    valid_mask = ~null_mask
    if valid_mask.any():
        parts = (
            series.to_frame("dt")
            .with_columns(
                ms=pl.col("dt").dt.millisecond(),
                seconds=pl.col("dt").dt.second(),
                minutes=pl.col("dt").dt.minute(),
                hours=pl.col("dt").dt.hour(),
            )
            .filter(pl.col("dt").is_not_null())
            .drop("dt")
        )
        parts_np = parts.to_numpy()

        data["ms"][valid_mask] = parts_np[:, 0]
        data["seconds"][valid_mask] = parts_np[:, 1]
        data["minutes"][valid_mask] = parts_np[:, 2]
        data["hours"][valid_mask] = parts_np[:, 3]
        data["padding"][valid_mask] = 0

    path.write_bytes(data.tobytes())


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


def write_datetime_column(series: pl.Series, path: Path) -> None:
    series = series.cast(pl.Datetime("ms"))
    null_mask = series.is_null().to_numpy()
    n = len(series)

    data = np.zeros(n, dtype=MONETDB_DATETIME_RECORD_TYPE)
    for k, v in DATETIME_NULL_RECORD.items():
        data[k][null_mask] = v

    valid_mask = ~null_mask
    if valid_mask.any():
        parts = (
            series.to_frame("dt")
            .with_columns(
                ms=pl.col("dt").dt.millisecond(),
                seconds=pl.col("dt").dt.second(),
                minutes=pl.col("dt").dt.minute(),
                hours=pl.col("dt").dt.hour(),
                day=pl.col("dt").dt.day(),
                month=pl.col("dt").dt.month(),
                year=pl.col("dt").dt.year(),
            )
            .filter(pl.col("dt").is_not_null())
            .drop("dt")
        )

        parts_np = parts.to_numpy()

        data["ms"][valid_mask] = parts_np[:, 0]
        data["seconds"][valid_mask] = parts_np[:, 1]
        data["minutes"][valid_mask] = parts_np[:, 2]
        data["hours"][valid_mask] = parts_np[:, 3]
        data["padding"][valid_mask] = 0
        data["day"][valid_mask] = parts_np[:, 4]
        data["month"][valid_mask] = parts_np[:, 5]
        data["year"][valid_mask] = parts_np[:, 6]

    path.write_bytes(data.tobytes())


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


def write_string_column(series: pl.Series, path: Path) -> None:
    buffer = bytearray()

    for val in series:
        if val is None:
            buffer += STRING_NULL_MARKER
        else:
            buffer += val.encode("utf-8")
            buffer += b"\x00"

    path.write_bytes(buffer)


def read_json_column_object(path: Path) -> pl.Series:
    s = read_string_column(path).alias("json")
    # inefficient map with json.loads to match pymonetdb behavior
    return s.map_elements(json.loads, pl.Object)


def read_json_column_struct(path: Path) -> pl.Series:
    s = read_string_column(path).alias("json")
    return s.str.json_decode(infer_schema_length=None)


def write_json_column(series: pl.Series, path: Path) -> None:
    if series.dtype == pl.Object:
        series = series.map_elements(str, pl.String).str.replace_all("'", '"')
    elif series.dtype == pl.Struct:
        series = series.struct.json_encode()
    elif series.dtype == pl.String:
        raise ValueError(
            f"Cannot write string column {series.name} ({series.name=}) as JSON, "
            "convert to pl.Struct or pl.Object first"
        )
    else:
        raise ValueError(f"Invalid dtype for JSON column: {series.dtype} ({series.name=})")

    write_string_column(series, path)


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


def write_blob_column(series: pl.Series, path: Path) -> None:
    buffer = bytearray()

    for val in series:
        if val is None:
            buffer += BLOB_NULL_MARKER
        else:
            length = len(val)
            buffer += length.to_bytes(8, byteorder="little")
            buffer += val

    path.write_bytes(buffer)


def read_numeric_column(
    path: Path, dtype: pl.DataType | type[pl.DataType], np_dtype: np.dtype | None = None
) -> pl.Series:
    with path.open("rb") as f:
        data = f.read()

    if np_dtype is None:
        np_dtype = cast(np.dtype | None, POLARS_NUMPY_TYPE_MAP.get(dtype))

        if np_dtype is None:
            raise ValueError(f"Cannot determine corresponding Numpy type for {path}: {dtype}")

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


def write_numeric_column(series: pl.Series, path: Path, np_dtype: np.dtype | None = None) -> None:
    values: np.ndarray

    if series.dtype == pl.Boolean:
        values = (
            series.to_frame("value")
            .select(pl.when(pl.col.value.is_null()).then(128).when(pl.col.value).then(1).otherwise(0).cast(pl.UInt8))
            .to_series()
            .to_numpy()
        )

    else:
        if np_dtype is None:
            np_dtype = cast(np.dtype | None, POLARS_NUMPY_TYPE_MAP.get(series.dtype))

            if np_dtype is None:
                raise ValueError(f"Cannot determine corresponding Numpy type for {series.dtype} ({series.name=})")

        if np.issubdtype(np_dtype, np.integer):
            sentinel = np.iinfo(np_dtype).min
            values = series.fill_null(sentinel).to_numpy().astype(np_dtype)
        elif np.issubdtype(np_dtype, np.floating):
            values = series.fill_null(np.nan).to_numpy().astype(np_dtype)
        else:
            raise ValueError(f"Unsupported numeric type: {series.dtype}")

    with path.open("wb") as f:
        f.write(values.tobytes())


def read_decimal_column(path: Path, dtype: pl.Decimal | type[pl.Decimal]) -> pl.Series:
    precision = dtype.precision or MONETDB_DEFAULT_DECIMAL_PRECISION
    scale = dtype.scale or MONETDB_DEFAULT_DECIMAL_SCALE
    np_dtype = decimal_numpy_dtype(precision)
    values = read_numeric_column(path, numpy_to_polars_int_dtype(np_dtype), np_dtype)

    # avoid float conversion issues when dividing by scale
    return (values.cast(pl.Decimal(MONETDB_DEFAULT_DECIMAL_PRECISION, MONETDB_DEFAULT_DECIMAL_SCALE)) / 10**scale).cast(
        dtype
    )


def write_decimal_column(series: pl.Series, path: Path, dtype: pl.Decimal) -> None:
    precision = dtype.precision or MONETDB_DEFAULT_DECIMAL_PRECISION
    scale = dtype.scale or MONETDB_DEFAULT_DECIMAL_SCALE
    np_dtype = decimal_numpy_dtype(precision)

    scaled_int = (series * 10**scale).cast(pl.Int64)

    write_numeric_column(scaled_int, path, np_dtype=np_dtype)
