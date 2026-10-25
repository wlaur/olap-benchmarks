from pathlib import Path
from shlex import split

import pytest

from .. import dbs
from ..dbs.questdb import QuestDB
from ..settings import SuiteName, get_settings


@pytest.mark.parametrize("seconds", [300, None])
@pytest.mark.parametrize("suite", ["rtabench", "time_series_metrics"])
def test_questdb_start_sets_native_timeout_in_seconds_without_changing_other_settings(
    seconds: int | None, suite: SuiteName, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = get_settings()
    monkeypatch.setattr(settings, "database_directory", tmp_path / "database")
    monkeypatch.setattr(settings, "temporary_directory", tmp_path)
    monkeypatch.setattr(settings, "query_timeout_seconds", seconds)
    monkeypatch.setattr(settings, "benchmark_threads", 10)
    monkeypatch.setattr(settings, "benchmark_memory_gb", 24)
    monkeypatch.setattr(dbs, "get_container_engine_platform", lambda: "linux/arm64")
    database = QuestDB()
    database._current_suite = suite
    database._current_suite_scale_factor = 1

    command = split(database.start)

    environment = dict(command[index + 1].split("=", 1) for index, value in enumerate(command) if value == "-e")
    expected = {"QDB_CAIRO_SQL_COPY_ROOT": "/import"}
    if seconds is not None:
        expected["QDB_QUERY_TIMEOUT"] = "300s"
    if suite == "time_series_metrics":
        expected["QDB_CAIRO_SQL_SMALL_MAP_PAGE_SIZE"] = "128K"
    assert environment == expected
    assert command[command.index("--cpus") + 1] == "10"
    assert command[command.index("--memory") + 1] == "24g"
    assert command[command.index("--memory-swap") + 1] == "24g"
    assert command[-1] == database.container_images["server"]
    assert (tmp_path / "questdb/data").is_dir()
