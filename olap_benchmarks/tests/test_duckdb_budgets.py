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
    monkeypatch.setattr(settings, "temporary_directory", tmp_path)
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


@pytest.mark.parametrize("mode", ["native", "parquet"])
@pytest.mark.parametrize("threads", [None, 2, 10])
def test_duckdb_thread_pools_readback_reconnect_reset_and_worker_configuration(
    mode: str, threads: int | None, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from typing import cast

    from ..dbs.duckdb.storage import StorageMode

    settings = get_settings()
    monkeypatch.setattr(settings, "database_directory", tmp_path / "dbs")
    monkeypatch.setattr(settings, "temporary_directory", tmp_path)
    monkeypatch.setattr(settings, "duckdb_storage_mode", cast(StorageMode, mode))
    monkeypatch.setattr(settings, "benchmark_threads", threads)
    monkeypatch.setattr(settings, "benchmark_memory_gb", 24)
    monkeypatch.setattr(settings, "duckdb_async_threads", 10)
    monkeypatch.setattr(settings, "duckdb_read_ahead_depth", -1)
    database = DuckDB()
    database._current_suite = "kaggle_airbnb"
    database._current_suite_scale_factor = 1
    worker = DuckDB.model_construct()
    worker._current_suite = database.current_suite
    worker._current_suite_scale_factor = database.current_suite_scale_factor
    try:
        for connection_db in (database, worker):
            readback = connection_db.runtime_metadata()
            actual = cast(dict[str, str], readback["settings"])
            assert actual["threads"] == str(threads or 10)
            assert actual["async_threads"] == "10"
            assert actual["read_ahead_depth"] == "-1"
            assert actual["TimeZone"] == "UTC"
            assert actual["memory_limit"] == "24.0 GiB"
            assert actual["storage_compatibility_version"] == "v2.0.0"
            assert actual["disable_parquet_prefetching"] == "false"
            assert actual["storage_block_prefetch"] == "REMOTE_ONLY"
            assert actual["fsync_mode"] == "STANDARD"
            assert Path(actual["temp_directory"]).is_relative_to(tmp_path)
            assert readback["pragma_version"] == ["v2.0.0", "d54cbbe3df", "Cyanoptera"]
            build = cast(dict[str, str], readback["build_info"])
            assert build["package_version"] == "2.0.0"
            assert build["duckdb_commit"] == "d54cbbe3df"
            assert build["package_commit"]
            assert connection_db.database_tags()["storage_version"] == "v2.0.0+"
            assert connection_db.fetch("SELECT i FROM range(64) t(i)").get_column("i").to_list() == list(range(64))
        worker.close_connection()
        database.execute("SET threads=1")
        with pytest.raises(RuntimeError, match="setting threads"):
            database.runtime_metadata()
        database.reset_session()
        assert cast(dict[str, str], database.runtime_metadata()["settings"])["threads"] == str(threads or 10)
        database.connect(reconnect=True)
        assert cast(dict[str, str], database.runtime_metadata()["settings"])["async_threads"] == "10"
    finally:
        worker.close_connection()
        database.close_connection()
