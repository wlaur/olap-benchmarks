import os
from pathlib import Path
from typing import Any, cast

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from sqlalchemy import create_engine, text

from ..dbs.monetdb.adbc import _iter_parquet_batches, _validated_ingest_row_count, insert_adbc


def test_adbc_row_count_must_be_exact() -> None:
    assert _validated_ingest_row_count(42, 42, "dataset") == 42
    with pytest.raises(RuntimeError, match="did not report"):
        _validated_ingest_row_count(-1, 42, "dataset")
    with pytest.raises(RuntimeError, match="41 inserted rows"):
        _validated_ingest_row_count(41, 42, "dataset")


def test_parquet_arrow_stream_crosses_row_group_boundaries(tmp_path: Path) -> None:
    path = tmp_path / "input.parquet"
    cast(Any, pq).write_table(pa.table({"id": range(7), "value": ["x"] * 7}), path, row_group_size=2)

    batches = list(
        _iter_parquet_batches(
            path,
            batch_rows=3,
            row_groups=4,
            memory_pool=pa.system_memory_pool(),
        )
    )

    assert pa.Table.from_batches(batches).to_pydict() == {
        "id": list(range(7)),
        "value": ["x"] * 7,
    }


@pytest.mark.skipif(
    "MONETDB_TEST_SQLALCHEMY_URI" not in os.environ,
    reason="MONETDB_TEST_SQLALCHEMY_URI is not set",
)
def test_adbc_adapter_streams_a_lazy_frame_into_monetdb() -> None:
    engine = create_engine(os.environ["MONETDB_TEST_SQLALCHEMY_URI"])
    try:
        with engine.connect() as connection:
            connection.execute(text("DROP TABLE IF EXISTS olap_adbc_adapter"))
            connection.commit()
            insert_adbc(
                pl.LazyFrame({"id": range(10_000), "label": ["value"] * 10_000}),
                "olap_adbc_adapter",
                connection,
                primary_key="id",
            )
            assert connection.execute(text("SELECT COUNT(*), COUNT(DISTINCT label) FROM olap_adbc_adapter")).one() == (
                10_000,
                1,
            )
            connection.execute(text("DROP TABLE olap_adbc_adapter"))
            connection.commit()
    finally:
        engine.dispose()
