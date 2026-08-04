from __future__ import annotations

from hashlib import blake2b
from io import BytesIO
from typing import Any

import polars as pl

MAX_ANSWER_HASH_CELLS = 5_000_000
FLOAT_ROUND_DECIMALS = 10
ANSWER_HASH_VERSION = "canonical-v6"


def _canonicalize_answer_frame(df: pl.DataFrame) -> pl.DataFrame:
    expressions: list[pl.Expr] = []

    for name, dtype in df.schema.items():
        expression = pl.col(name)

        # Exact numerics render as their decimal string so that the hash does not depend on which
        # width or kind the driver reported. Drivers legitimately disagree here: MonetDB narrows an
        # aggregate's declared type by context, so the same value arrives as int8/int32/int64 or as
        # DECIMAL(38, 0) for hugeint, and a driver that erases width reports int64 throughout.
        # Casting to Int64 would also overflow hugeint, and casting to Float64 would corrupt any
        # integer beyond 2^53.
        if dtype == pl.Boolean or dtype.is_integer():
            expression = expression.cast(pl.Int64).cast(pl.String)
        elif isinstance(dtype, pl.Decimal) and (dtype.scale or 0) == 0:
            expression = expression.cast(pl.String)
        elif dtype.is_float() or isinstance(dtype, pl.Decimal):
            expression = expression.cast(pl.Float64).round(FLOAT_ROUND_DECIMALS)
        elif isinstance(dtype, pl.Datetime):
            if dtype.time_zone is not None:
                expression = expression.dt.convert_time_zone("UTC").dt.replace_time_zone(None)
            expression = expression.cast(pl.Datetime("ms"))
        elif isinstance(dtype, pl.Categorical | pl.Enum):
            expression = expression.cast(pl.String)
        # Fixed-width text arrives as bytes, not a string: ClickHouse declares TPC-H's CHAR(n)
        # columns as FixedString(n), which Arrow reports as binary and pads to the declared
        # width with NUL. Writing that to NDJSON panics polars-json outright, and even without
        # the panic b"BRAZIL\x00..." would never match a VARCHAR engine's "BRAZIL". The padding
        # is storage, not data, so decode to text and drop it. A trailing NUL cannot occur in a
        # legitimate text value, and genuinely non-UTF-8 binary raises rather than hashing as null.
        elif dtype == pl.Binary:
            expression = expression.cast(pl.String).str.strip_chars_end("\x00")

        expressions.append(expression.alias(name))

    canonical = df.select(expressions)
    canonical.columns = [f"c{idx}" for idx in range(canonical.width)]
    return canonical


def build_answer_metadata(df: pl.DataFrame) -> dict[str, Any]:
    row_count = df.height
    column_count = df.width
    cell_count = row_count * column_count
    metadata: dict[str, Any] = {
        "answer_rows": row_count,
        "answer_columns": column_count,
        "answer_cells": cell_count,
        "answer_hash_version": ANSWER_HASH_VERSION,
    }

    if cell_count > MAX_ANSWER_HASH_CELLS:
        metadata["answer_hash_skipped_reason"] = (
            f"result has {cell_count:_} cells, above the {MAX_ANSWER_HASH_CELLS:_}-cell hash limit"
        )
        return metadata

    sink = BytesIO()
    _canonicalize_answer_frame(df).write_ndjson(sink)
    metadata["answer_hash"] = f"blake2b128:{blake2b(sink.getbuffer(), digest_size=16).hexdigest()}"
    return metadata
