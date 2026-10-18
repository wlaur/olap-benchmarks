from pathlib import Path
from shlex import split

import pytest

from .. import dbs
from ..dbs.postgres import Postgres
from ..dbs.timescaledb import TimescaleDB
from ..settings import get_settings


@pytest.mark.parametrize("database_type", [Postgres, TimescaleDB])
def test_postgres_family_enforces_configured_resource_limits(
    database_type: type[Postgres], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    settings = get_settings()
    monkeypatch.setattr(settings, "database_directory", tmp_path)
    monkeypatch.setattr(settings, "benchmark_threads", 2)
    monkeypatch.setattr(settings, "benchmark_memory_gb", 6)
    monkeypatch.setattr(dbs, "get_container_engine_platform", lambda: "linux/arm64")
    database = database_type()
    database._current_suite = "time_series"
    database._current_suite_scale_factor = 1

    command = split(database.start)

    assert command[command.index("--cpus") + 1] == "2"
    assert command[command.index("--memory") + 1] == "6g"
    assert command[command.index("--memory-swap") + 1] == "6g"
    assert "shared_buffers=1536MB" in command
    assert "effective_cache_size=4608MB" in command
    assert "work_mem=128MB" in command
    assert "maintenance_work_mem=1536MB" in command
    assert "max_parallel_workers_per_gather=1" in command
    assert "max_parallel_workers=2" in command
    assert database.parallel_copy_workers == 2


def test_postgres_one_thread_budget_disables_parallel_gather(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = get_settings()
    monkeypatch.setattr(settings, "benchmark_threads", 1)
    monkeypatch.setattr(settings, "benchmark_memory_gb", 1)
    database = Postgres()

    assert database.server_settings["max_parallel_workers_per_gather"] == "0"
    assert database.server_settings["shared_buffers"] == "256MB"
    assert database.server_settings["work_mem"] == "32MB"
    assert database.parallel_copy_workers == 1


def test_postgres_parallel_copy_keeps_its_worker_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(get_settings(), "benchmark_threads", 32)

    assert Postgres().parallel_copy_workers == 12
