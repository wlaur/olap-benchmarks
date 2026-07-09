from __future__ import annotations

from hashlib import blake2b
from io import BytesIO
from typing import Any

import polars as pl

MAX_ANSWER_HASH_CELLS = 5_000_000
FLOAT_ROUND_DECIMALS = 11
ANSWER_HASH_VERSION = "canonical-v2"


def _canonicalize_answer_frame(df: pl.DataFrame) -> pl.DataFrame:
    expressions: list[pl.Expr] = []

    for name, dtype in df.schema.items():
        expression = pl.col(name)

        if dtype == pl.Boolean or dtype.is_integer():
            expression = expression.cast(pl.Int64)
        elif dtype.is_float():
            expression = expression.cast(pl.Float64).round(FLOAT_ROUND_DECIMALS)
        elif isinstance(dtype, pl.Datetime):
            if dtype.time_zone is not None:
                expression = expression.dt.convert_time_zone("UTC").dt.replace_time_zone(None)
            expression = expression.cast(pl.Datetime("ms"))
        elif isinstance(dtype, pl.Categorical | pl.Enum):
            expression = expression.cast(pl.String)

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
