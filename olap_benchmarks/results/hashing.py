from __future__ import annotations

from hashlib import blake2b
from io import BytesIO
from typing import Any

import polars as pl

MAX_ANSWER_HASH_CELLS = 5_000_000
FLOAT_ROUND_DECIMALS = 10
ANSWER_HASH_VERSION = "canonical-v7"


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
        # NaN and null both mean "undefined" here and engines disagree on which they return for
        # an undefined statistic, e.g. stddev_samp() over a single row.
        elif dtype.is_float() or isinstance(dtype, pl.Decimal):
            expression = expression.cast(pl.Float64).fill_nan(None).round(FLOAT_ROUND_DECIMALS)
        elif isinstance(dtype, pl.Datetime):
            if dtype.time_zone is not None:
                expression = expression.dt.convert_time_zone("UTC").dt.replace_time_zone(None)
            expression = expression.cast(pl.Datetime("ms"))
        elif isinstance(dtype, pl.Categorical | pl.Enum):
            expression = expression.cast(pl.String)
        # Fixed-width text arrives as bytes: ClickHouse reports FixedString(n) as binary padded
        # to the declared width with NUL. That padding is storage rather than data, so decode to
        # text and drop it to match a VARCHAR engine. Non-UTF-8 binary raises rather than
        # hashing as null.
        elif dtype == pl.Binary:
            expression = expression.cast(pl.String).str.strip_chars_end("\x00")
        # An aggregated array renders as its bracketed elements, because MonetDB has no array type
        # and builds the same content as a string with group_concat. Element order is part of the
        # comparison, so a differently ordered or differently populated array still mismatches;
        # only the container's spelling is normalised. A list whose elements are all null, which is
        # what array_agg() returns for an unmatched outer join, canonicalises to null like
        # MonetDB's group_concat over no rows.
        elif isinstance(dtype, pl.List):
            expression = pl.concat_str(
                pl.lit("["),
                expression.cast(pl.List(pl.String)).list.join(",", ignore_nulls=False),
                pl.lit("]"),
            )

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
