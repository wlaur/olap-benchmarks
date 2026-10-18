from pathlib import Path
from shlex import split
from urllib.parse import parse_qs, urlsplit

import polars as pl
import pytest

from .. import dbs
from ..dbs.postgres import Postgres
from ..dbs.timescaledb import TimescaleDB
from ..settings import Operation, get_settings


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


@pytest.mark.parametrize("database_type", [Postgres, TimescaleDB])
@pytest.mark.parametrize(
    ("operation", "seconds", "expected"),
    [("select", 45, "-cstatement_timeout=45000"), ("select", None, None), ("populate", 45, None)],
)
def test_postgres_connectorx_bounds_select_statements_only(
    database_type: type[Postgres],
    operation: Operation,
    seconds: int | None,
    expected: str | None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(get_settings(), "query_timeout_seconds", seconds)
    database = database_type()
    database._current_operation = operation
    statements: list[str | None] = []

    def read(query: str, uri: str) -> pl.DataFrame:
        assert query == "SELECT 1"
        parsed = urlsplit(uri)
        assert parsed.netloc == urlsplit(database.connection_string).netloc
        options = parse_qs(parsed.query).get("options", [None])[0]
        statements.append(options)
        assert "+" not in parsed.query
        return pl.DataFrame({"value": [1]})

    monkeypatch.setattr(pl, "read_database_uri", read)

    assert database.fetch_connectorx("SELECT 1;").item() == 1
    assert statements == [expected]


def test_postgres_statement_timeout_preserves_connection_options(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(get_settings(), "query_timeout_seconds", 45)
    database = Postgres(connection_string="postgresql://user:pass+word@localhost/postgres?options=-cwork_mem%3D64MB")
    database._current_operation = "select"
    uris: list[str] = []

    def read(_query: str, uri: str) -> pl.DataFrame:
        uris.append(uri)
        return pl.DataFrame({"value": [1]})

    monkeypatch.setattr(pl, "read_database_uri", read)

    database.fetch_connectorx("SELECT 1")

    assert uris == [
        "postgresql://user:pass+word@localhost/postgres?options=-cwork_mem%3D64MB%20-cstatement_timeout%3D45000"
    ]
