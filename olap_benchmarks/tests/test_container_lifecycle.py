import shlex
from typing import Literal

import pytest

from ..dbs import get_databases
from ..settings import SUITE_SPECS, DatabaseName


@pytest.mark.parametrize(
    "database",
    ["postgres", "timescaledb", "clickhouse", "monetdb", "questdb", "starrocks", "doris", "arc", "tdengine"],
)
@pytest.mark.parametrize("action", ["stop", "restart"])
def test_container_lifecycle_allows_and_records_shutdown_grace(
    database: DatabaseName, action: Literal["stop", "restart"]
) -> None:
    db = get_databases()[database]
    suite = next(iter(db.suite_registry()))
    db._current_suite = suite
    db._current_suite_scale_factor = SUITE_SPECS[suite].default_scale_factor
    commands = db.stop_commands if action == "stop" else db.restart_commands
    container_commands = [
        arguments for command in commands if (arguments := shlex.split(command))[:2] == ["docker", action]
    ]

    assert container_commands
    for arguments in container_commands:
        assert arguments[arguments.index("--timeout") + 1] == "120"
    assert db.run_options["container_shutdown_timeout_seconds"] == 120


@pytest.mark.parametrize("database", ["duckdb", "polars"])
def test_in_process_engines_have_no_container_shutdown_option(database: DatabaseName) -> None:
    assert "container_shutdown_timeout_seconds" not in get_databases()[database].run_options
