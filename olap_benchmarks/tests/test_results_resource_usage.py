from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest
from sqlalchemy.orm import Session

from ..dbs import get_databases
from ..results import get_results_engine, migrate_results
from ..results.models import Run, RunMetric
from ..results.resource_usage import (
    ComparableMemoryUnavailableError,
    comparable_memory_source,
    describe_run_resource_usage,
    load_run_resource_usage,
    resolve_comparable_peak_memory_mb,
)
from ..settings import IN_PROCESS_DATABASES


def _seed(db_path: Path) -> None:
    migrate_results(db_path=db_path)
    engine = get_results_engine(read_only=False, db_path=db_path)

    try:
        with Session(engine) as session:
            containerised = Run(
                suite="clickbench",
                suite_scale_factor=1,
                db="monetdb",
                db_version="test",
                db_driver="adbc",
                operation="populate",
                system="test",
                status="completed",
                started_at=datetime(2026, 1, 1),
                finished_at=datetime(2026, 1, 1, 1),
            )
            in_process = Run(
                suite="clickbench",
                suite_scale_factor=1,
                db="duckdb",
                db_version="test",
                operation="populate",
                system="test",
                status="completed",
                started_at=datetime(2026, 1, 2),
                finished_at=datetime(2026, 1, 2, 1),
            )
            session.add_all([containerised, in_process])
            session.commit()

            session.add_all(
                [
                    RunMetric(
                        run_id=containerised.id,
                        time=datetime(2026, 1, 1, 0, 1),
                        cpu_percent=210.0,
                        server_mem_mb=8_000,
                        client_mem_mb=1_500,
                        client_uss_mb=1_400,
                        disk_mb=40_000,
                    ),
                    RunMetric(
                        run_id=containerised.id,
                        time=datetime(2026, 1, 1, 0, 2),
                        cpu_percent=180.0,
                        server_mem_mb=6_000,
                        client_mem_mb=2_500,
                        client_uss_mb=2_400,
                        disk_mb=52_000,
                    ),
                    RunMetric(
                        run_id=in_process.id,
                        time=datetime(2026, 1, 2, 0, 1),
                        cpu_percent=350.0,
                        server_mem_mb=0,
                        client_mem_mb=9_000,
                        client_uss_mb=8_800,
                        disk_mb=30_000,
                    ),
                ]
            )
            session.commit()
    finally:
        engine.dispose()


def _seed_lane_sampled_run(db_path: Path) -> None:
    migrate_results(db_path=db_path)
    engine = get_results_engine(read_only=False, db_path=db_path)

    try:
        with Session(engine) as session:
            run = Run(
                suite="clickbench",
                suite_scale_factor=1,
                db="clickhouse",
                db_version="test",
                operation="populate",
                system="test",
                status="completed",
                started_at=datetime(2026, 1, 1),
                finished_at=datetime(2026, 1, 1, 1),
            )
            session.add(run)
            session.commit()

            # the resource lane samples at 0 s, 1 s and 5 s while the slow disk lane
            # stretches its own interval, so no column is evenly spaced
            session.add_all(
                [
                    RunMetric(
                        run_id=run.id,
                        time=datetime(2026, 1, 1, 0, 0, second),
                        cpu_percent=cpu_percent,
                        server_mem_mb=1_000,
                        client_mem_mb=100,
                        client_uss_mb=90,
                    )
                    for second, cpu_percent in ((0, 100.0), (1, 200.0), (5, 300.0))
                ]
                + [
                    RunMetric(run_id=run.id, time=datetime(2026, 1, 1, 0, 0, second), disk_mb=disk_mb)
                    for second, disk_mb in ((0, 1_000), (6, 5_000))
                ]
            )
            session.commit()
    finally:
        engine.dispose()


def test_load_run_resource_usage_reads_columns_sampled_at_different_rates(tmp_path: Path) -> None:
    db_path = tmp_path / "results.db"
    _seed_lane_sampled_run(db_path)

    (usage,) = load_run_resource_usage(db_path=db_path)

    # rows that leave a column null must not mask the peaks recorded in other rows
    assert usage.peak_combined_cpu_percent == 300.0
    assert usage.peak_server_disk_mb == 5_000
    assert usage.peak_server_memory_mb == 1_000


def test_mean_cpu_weights_samples_by_elapsed_time_not_sample_count(tmp_path: Path) -> None:
    db_path = tmp_path / "results.db"
    _seed_lane_sampled_run(db_path)

    (usage,) = load_run_resource_usage(db_path=db_path)

    # 100% held for 1 s then 200% held for 4 s; counting samples equally would say 200%
    assert usage.mean_combined_cpu_percent == pytest.approx(180.0)
    assert usage.cpu_core_seconds == pytest.approx(9.0)


def test_in_process_databases_match_database_execution_mode() -> None:
    execution_modes = {name: db.execution_mode for name, db in get_databases().items()}

    assert {name for name, mode in execution_modes.items() if mode == "in_process"} == set(IN_PROCESS_DATABASES)


def test_comparable_memory_source_follows_execution_mode() -> None:
    assert comparable_memory_source("duckdb") == "client"
    assert comparable_memory_source("polars") == "client"
    assert comparable_memory_source("monetdb") == "server"


def test_comparable_peak_memory_uses_client_memory_for_in_process_engines() -> None:
    assert resolve_comparable_peak_memory_mb(db="duckdb", peak_server_memory_mb=0, peak_client_memory_mb=9_000) == 9_000
    assert (
        resolve_comparable_peak_memory_mb(db="monetdb", peak_server_memory_mb=8_000, peak_client_memory_mb=2_500)
        == 8_000
    )


def test_comparable_peak_memory_never_reports_zero_for_in_process_engines() -> None:
    with pytest.raises(ComparableMemoryUnavailableError, match="did not record"):
        resolve_comparable_peak_memory_mb(db="duckdb", peak_server_memory_mb=0, peak_client_memory_mb=None)

    with pytest.raises(ComparableMemoryUnavailableError, match="pre-split combined figure"):
        resolve_comparable_peak_memory_mb(db="polars", peak_server_memory_mb=29_190, peak_client_memory_mb=None)


def test_comparable_peak_memory_rejects_missing_server_memory_for_containers() -> None:
    with pytest.raises(ComparableMemoryUnavailableError, match="no server memory"):
        resolve_comparable_peak_memory_mb(db="monetdb", peak_server_memory_mb=0, peak_client_memory_mb=2_500)


def test_load_run_resource_usage_keeps_server_and_client_peaks_apart(tmp_path: Path) -> None:
    db_path = tmp_path / "results.db"
    _seed(db_path)

    usage = {row.db: row for row in load_run_resource_usage(db_path=db_path)}

    server_run = usage["monetdb"]
    assert server_run.peak_server_memory_mb == 8_000
    assert server_run.peak_client_memory_mb == 2_500
    assert server_run.peak_client_uss_memory_mb == 2_400
    assert server_run.peak_combined_cpu_percent == 210.0
    assert server_run.peak_server_disk_mb == 52_000
    assert server_run.comparable_memory_source == "server"
    assert server_run.comparable_peak_memory_mb() == 8_000

    client_run = usage["duckdb"]
    assert client_run.peak_server_memory_mb == 0
    assert client_run.comparable_memory_source == "client"
    assert client_run.comparable_peak_memory_mb() == 9_000


def test_load_run_resource_usage_filters_by_database(tmp_path: Path) -> None:
    db_path = tmp_path / "results.db"
    _seed(db_path)

    assert [row.db for row in load_run_resource_usage(db_path=db_path, db="duckdb")] == ["duckdb"]


def test_describe_run_resource_usage_reports_unavailable_reason(tmp_path: Path) -> None:
    db_path = tmp_path / "results.db"
    _seed(db_path)

    engine = get_results_engine(read_only=False, db_path=db_path)
    try:
        with Session(engine) as session:
            legacy = Run(
                suite="clickbench",
                suite_scale_factor=1,
                db="polars",
                db_version="test",
                operation="select",
                system="test",
                status="completed",
                started_at=datetime(2026, 1, 3),
                finished_at=datetime(2026, 1, 3, 1),
            )
            session.add(legacy)
            session.commit()
            session.add(
                RunMetric(
                    run_id=legacy.id,
                    time=datetime(2026, 1, 3, 0, 1),
                    cpu_percent=120.0,
                    server_mem_mb=4_096,
                    client_mem_mb=None,
                    client_uss_mb=None,
                    disk_mb=1_000,
                )
            )
            session.commit()
    finally:
        engine.dispose()

    described = {
        str(row["db"]): row for row in map(describe_run_resource_usage, load_run_resource_usage(db_path=db_path))
    }

    assert described["polars"]["comparable_peak_memory_mb"] is None
    assert "pre-split combined figure" in str(described["polars"]["comparable_memory_unavailable"])
    assert described["duckdb"]["comparable_peak_memory_mb"] == 9_000
    assert described["duckdb"]["comparable_memory_unavailable"] is None
    assert described["monetdb"]["comparable_peak_memory_mb"] == 8_000
