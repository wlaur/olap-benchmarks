from pathlib import Path

import pytest

from ..dbs.duckdb import DuckDB
from ..settings import get_settings


@pytest.mark.parametrize("memory_gb", [None, 6, 24])
def test_duckdb_memory_budget_uses_the_same_binary_units_as_containers(
    memory_gb: int | None, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = get_settings()
    monkeypatch.setattr(settings, "benchmark_memory_gb", memory_gb)
    monkeypatch.setattr(settings, "benchmark_threads", 2)
    monkeypatch.setattr(settings, "database_directory", tmp_path / "dbs")
    database = DuckDB()
    database._current_suite = "kaggle_airbnb"
    database._current_suite_scale_factor = 1
    try:
        if memory_gb is not None:
            actual = database.fetch("SELECT current_setting('memory_limit') AS memory_limit")
            assert actual.item(0, "memory_limit") == f"{memory_gb}.0 GiB"
        assert database.run_options["memory_limit_bytes"] == (memory_gb * 1024**3 if memory_gb is not None else None)
    finally:
        database.close_connection()
