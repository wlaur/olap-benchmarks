from __future__ import annotations

from hashlib import blake2b
from io import BytesIO
from typing import Any

import polars as pl

MAX_ANSWER_HASH_CELLS = 5_000_000


def build_answer_metadata(df: pl.DataFrame) -> dict[str, Any]:
    row_count = df.height
    column_count = df.width
    cell_count = row_count * column_count
    metadata: dict[str, Any] = {
        "answer_rows": row_count,
        "answer_columns": column_count,
        "answer_cells": cell_count,
    }

    if cell_count > MAX_ANSWER_HASH_CELLS:
        metadata["answer_hash_skipped_reason"] = (
            f"result has {cell_count:_} cells, above the {MAX_ANSWER_HASH_CELLS:_}-cell hash limit"
        )
        return metadata

    sink = BytesIO()
    df.write_ipc(sink, compression="uncompressed")
    metadata["answer_hash"] = f"blake2b128:{blake2b(sink.getbuffer(), digest_size=16).hexdigest()}"
    return metadata
