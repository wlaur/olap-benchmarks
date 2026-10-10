from __future__ import annotations

import os
import shutil
import subprocess
import sys
import uuid
from pathlib import Path
from typing import Any, cast

import duckdb
import pytest

from ..dbs import get_databases
from ..matrix import published_databases
from ..settings import REPO_ROOT, DatabaseName, Operation, SuiteName, format_suite_data_directory_name, get_settings

# The cheapest suite each engine registers: kaggle_airbnb is a 600 MB smoke suite; Polars only
# registers SF10-sized suites and ClickBench is a scan of the Parquet file it registers; Arc's
# smallest is time_series SF1.
SMOKE_SUITES: dict[DatabaseName, tuple[SuiteName, int]] = {
    "clickhouse": ("kaggle_airbnb", 1),
    "duckdb": ("kaggle_airbnb", 1),
    "monetdb": ("kaggle_airbnb", 1),
    "polars": ("clickbench", 1),
    "arc": ("time_series", 1),
}

REVISION = "live-smoke"


def test_every_published_engine_has_a_registered_smoke_suite() -> None:
    assert set(published_databases()) <= set(SMOKE_SUITES)
    for db_name, (suite, _) in SMOKE_SUITES.items():
        assert suite in get_databases()[db_name].suite_registry()


def _benchmark(
    db_name: DatabaseName, suite: SuiteName, scale_factor: int, operation: Operation, env: dict[str, str], cleanup: bool
) -> None:
    command = [
        sys.executable,
        "-m",
        "olap_benchmarks",
        "benchmark",
        db_name,
        suite,
        operation,
        "--revision",
        REVISION,
        "--scale-factor",
        str(scale_factor),
        *(["--cleanup"] if cleanup else []),
    ]
    completed = subprocess.run(command, cwd=REPO_ROOT, env=env, check=False)
    assert completed.returncode == 0, f"{' '.join(command)} exited with {completed.returncode}"


@pytest.mark.live
@pytest.mark.parametrize("db_name", list(SMOKE_SUITES))
def test_engine_runs_every_select_query(db_name: DatabaseName, tmp_path: Path) -> None:
    suite, scale_factor = SMOKE_SUITES[db_name]
    settings = get_settings()
    input_directory = settings.input_data_directory / format_suite_data_directory_name(suite, scale_factor)
    if not input_directory.is_dir() or not any(input_directory.iterdir()):
        pytest.skip(f"no input data at {input_directory}; run 'olap prepare {suite} --scale-factor {scale_factor}'")

    # a throwaway store under the configured temporary directory (which containers can mount),
    # so neither an existing store nor the configured results are touched
    database_directory = settings.temporary_directory / "live-smoke" / uuid.uuid4().hex
    results_directory = tmp_path / "results"
    database_directory.mkdir(parents=True)
    results_directory.mkdir()
    env = {
        **os.environ,
        "OLAP_BENCHMARKS_DATABASE_DIRECTORY": database_directory.as_posix(),
        "OLAP_BENCHMARKS_RESULTS_DIRECTORY": results_directory.as_posix(),
    }

    try:
        _benchmark(db_name, suite, scale_factor, "populate", env, cleanup=False)
        _benchmark(db_name, suite, scale_factor, "select", env, cleanup=True)
    finally:
        shutil.rmtree(database_directory, ignore_errors=True)

    connection = cast(Any, duckdb).connect(str(results_directory / f"{REVISION}.db"), read_only=True)
    try:
        runs = connection.execute(
            "select operation, status from run where db = ? and suite = ? order by id", [db_name, suite]
        ).fetchall()
        steps = connection.execute(
            """
            select s.query_name, s.status, s.result_status, s.error_message
            from run_step s join run r on r.id = s.run_id
            where r.operation = 'select' and s.step_type = 'query'
            """
        ).fetchall()
    finally:
        connection.close()

    assert ("select", "completed") in runs
    failed = [step for step in steps if step[1] != "completed" or step[2] not in ("ok", "unsupported")]
    assert steps
    assert not failed, failed
