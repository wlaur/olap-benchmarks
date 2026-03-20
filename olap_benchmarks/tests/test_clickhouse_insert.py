from __future__ import annotations

from pathlib import Path

import polars as pl
from _pytest.monkeypatch import MonkeyPatch

from ..dbs.clickhouse import Clickhouse


def test_clickhouse_partitioned_lazy_staging_writes_partition_files(
    monkeypatch: MonkeyPatch,
    tmp_path: Path,
) -> None:
    db = Clickhouse()
    df = pl.DataFrame(
        {
            "id": list(range(10)),
            "value": [f"row-{idx}" for idx in range(10)],
        }
    ).lazy()

    def noop_wait_for_parquet_readable(self: Clickhouse, input_file: str, timeout_seconds: float = 10.0) -> None:
        _ = (self, input_file, timeout_seconds)

    monkeypatch.setattr(Clickhouse, "_wait_for_parquet_readable", noop_wait_for_parquet_readable)

    temp_path, input_file = db._write_temporary_parquet(df, tmp_path, partitions=3)

    assert temp_path.is_dir()
    assert input_file.endswith("/*.parquet")

    parquet_files = sorted(temp_path.glob("*.parquet"))
    assert len(parquet_files) == 3

    total_rows = sum(pl.read_parquet(path).height for path in parquet_files)
    assert total_rows == 10
