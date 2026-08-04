from __future__ import annotations

from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from typing import Any, cast, get_args

import polars as pl
import pytest
from sqlalchemy import Connection

from .. import dbs as dbs_module
from ..dbs import Database, apply_parquet_epoch_columns
from ..dbs.monetdb import MONETDB_RELEASE, MonetDB, MonetDBTimeSeries
from ..dbs.monetdb.settings import SETTINGS as MONETDB_SETTINGS
from ..settings import (
    DatabaseName,
    SuiteName,
    TableName,
    resolve_suite_scale_factor,
    resolve_suite_scale_factors,
)
from ..suites import BenchmarkSuite
from ..suites.rtabench.config import RTABENCH_QUERY_NAMES, RTABench
from ..suites.time_series.config import (
    BASE_TIME_SERIES_DATASET_SIZES,
    TimeSeries,
    get_time_series_batch_rows,
    get_time_series_column_counts,
    get_time_series_dataset_sizes,
    get_time_series_row_group_rows,
)


class DummyDatabase(Database):
    name: DatabaseName = "duckdb"
    version: str = "test"
    connection_string: str = "dummy://"

    @property
    def start(self) -> None:
        return None

    def connect(self, reconnect: bool = False) -> Connection:
        raise NotImplementedError

    def fetch(
        self,
        query: str,
        schema: Mapping[str, pl.DataType | type[pl.DataType]] | None = None,
    ) -> pl.DataFrame:
        raise NotImplementedError

    def get_table_names(self) -> set[TableName]:
        raise NotImplementedError

    def insert(
        self,
        df: pl.DataFrame | pl.LazyFrame,
        table: TableName,
        primary_key: str | list[str] | None = None,
        not_null: str | list[str] | None = None,
    ) -> None:
        raise NotImplementedError

    def upsert(self, df: pl.DataFrame, table: TableName, primary_key: str | list[str]) -> None:
        raise NotImplementedError

    def delete(self, table: TableName, primary_key: str | list[str], keys: pl.DataFrame) -> None:
        raise NotImplementedError


def test_apply_parquet_epoch_columns_preserves_values_and_nulls() -> None:
    frame = apply_parquet_epoch_columns(
        pl.LazyFrame(
            {
                "seconds": [0, 1, None, 86_400],
                "milliseconds": [0, 1_000, None, 86_400_000],
                "days": [0, 1, None, 20_000],
            }
        ),
        {"seconds": "s", "milliseconds": "ms", "days": "day"},
    ).collect()

    assert frame.schema == pl.Schema(
        {
            "seconds": pl.Datetime("us"),
            "milliseconds": pl.Datetime("us"),
            "days": pl.Date,
        }
    )
    assert frame.select(
        pl.col("seconds").dt.epoch("s"),
        pl.col("milliseconds").dt.epoch("ms"),
        pl.col("days").cast(pl.Int32),
    ).to_dict(as_series=False) == {
        "seconds": [0, 1, None, 86_400],
        "milliseconds": [0, 1_000, None, 86_400_000],
        "days": [0, 1, None, 20_000],
    }


class CountingDatabase(DummyDatabase):
    row_counts: dict[str, int] = {}
    row_count_errors: dict[str, str] = {}
    table_names: set[str] = set()
    rollback_calls: int = 0

    def get_row_count(self, table: TableName) -> int:
        if table in self.row_count_errors:
            raise RuntimeError(self.row_count_errors[table])

        return self.row_counts[table]

    def get_table_names(self) -> set[TableName]:
        return set(self.table_names)

    def rollback(self) -> None:
        self.rollback_calls += 1


class DummySuite(BenchmarkSuite[CountingDatabase]):
    name: SuiteName = "clickbench"

    def expected_table_row_counts(self) -> dict[TableName, int]:
        return {"hits": 10, "events": 20}

    def populate(self) -> None:
        raise NotImplementedError

    def select(self) -> None:
        raise NotImplementedError


class SkipPopulateSuite(BenchmarkSuite["SkipPopulateDatabase"]):
    name: SuiteName = "clickbench"

    def should_populate(self) -> bool:
        return False

    def populate(self) -> None:
        raise AssertionError("populate should not run when should_populate is false")

    def select(self) -> None:
        raise NotImplementedError


class SkipPopulateDatabase(DummyDatabase):
    def suite_registry(self) -> Mapping[SuiteName, type[BenchmarkSuite[Any]]]:
        return {"clickbench": SkipPopulateSuite}


class BrokenShouldPopulateSuite(BenchmarkSuite["BrokenShouldPopulateDatabase"]):
    name: SuiteName = "clickbench"

    def should_populate(self) -> bool:
        raise RuntimeError("schema probe failed")

    def populate(self) -> None:
        raise AssertionError("populate should not run when should_populate fails")

    def select(self) -> None:
        raise NotImplementedError


class BrokenShouldPopulateDatabase(DummyDatabase):
    def suite_registry(self) -> Mapping[SuiteName, type[BenchmarkSuite[Any]]]:
        return {"clickbench": BrokenShouldPopulateSuite}


class NoopSelectSuite(BenchmarkSuite["RuntimeVersionDatabase"]):
    name: SuiteName = "clickbench"

    def populate(self) -> None:
        raise NotImplementedError

    def select(self) -> None:
        raise AssertionError("select should not run before result storage is configured")


class RuntimeVersionDatabase(DummyDatabase):
    runtime_version: str | None = None

    def get_runtime_version(self) -> str | None:
        return self.runtime_version

    def suite_registry(self) -> Mapping[SuiteName, type[BenchmarkSuite[Any]]]:
        return {"clickbench": NoopSelectSuite}


class ClickHouseDummyDatabase(DummyDatabase):
    name: DatabaseName = "clickhouse"


def test_suite_supported_operations_defaults_to_populate_and_select() -> None:
    assert DummySuite.supported_operations == ("populate", "select")


def test_time_series_suite_declares_mutate_support() -> None:
    assert TimeSeries.supported_operations == ("populate", "select", "mutate", "concurrent")


def test_time_series_generation_bounds_batches_and_parquet_row_groups() -> None:
    assert get_time_series_batch_rows(785) == 25_477
    assert get_time_series_row_group_rows(785) == 10_658
    row_bytes = 8 + 785 * 4
    row_group_bytes = get_time_series_row_group_rows(785) * row_bytes
    assert 32 * 1024 * 1024 - row_bytes < row_group_bytes <= 32 * 1024 * 1024
    assert get_time_series_row_group_rows(2_000) == (32 * 1024 * 1024) // 8_008


def test_rtabench_schedules_preaggregated_upstream_queries() -> None:
    assert "1000_terminal_hourly_stats" in RTABENCH_QUERY_NAMES
    assert "1030_customers_with_most_orders_delivered" in RTABENCH_QUERY_NAMES


def test_rtabench_marks_missing_db_specific_queries_unsupported() -> None:
    duckdb_suite = cast(
        RTABench[DummyDatabase],
        RTABench.model_construct(db=DummyDatabase(), name="rtabench", scale_factor=1),
    )
    clickhouse_suite = cast(
        RTABench[ClickHouseDummyDatabase],
        RTABench.model_construct(db=ClickHouseDummyDatabase(), name="rtabench", scale_factor=1),
    )

    assert duckdb_suite.include_query("0000_terminal_hourly_stats")
    assert not duckdb_suite.include_query("1000_terminal_hourly_stats")
    assert clickhouse_suite.include_query("1000_terminal_hourly_stats")


def test_database_benchmarks_resolves_all_suites() -> None:
    db = DummyDatabase()

    benchmarks = db.benchmarks

    # jsonbench and chat_threads are registered per database rather than in the base registry,
    # because each engine needs its own json ingest path and query dialect
    assert set(benchmarks) == set(get_args(SuiteName)) - {"jsonbench", "chat_threads"}
    assert benchmarks["time_series"].db is db
    assert benchmarks["time_series"].scale_factor == 1
    assert benchmarks["tpc_h"].scale_factor == 10
    assert benchmarks["tpc_ds"].scale_factor == 1


def test_jsonbench_registered_for_initial_engines() -> None:
    from ..dbs.clickhouse import Clickhouse
    from ..dbs.doris import Doris
    from ..dbs.duckdb import DuckDB
    from ..dbs.postgres import Postgres
    from ..dbs.starrocks import StarRocks

    for db in (Clickhouse(), Doris(), DuckDB(), Postgres(), StarRocks()):
        benchmarks = db.benchmarks

        assert "jsonbench" in benchmarks
        assert benchmarks["jsonbench"].scale_factor == 10


def test_chat_threads_registered_for_json_capable_engines() -> None:
    from ..dbs.clickhouse import Clickhouse
    from ..dbs.duckdb import DuckDB
    from ..dbs.monetdb import MonetDB

    for db in (Clickhouse(), DuckDB(), MonetDB()):
        benchmarks = db.benchmarks

        assert "chat_threads" in benchmarks
        assert benchmarks["chat_threads"].scale_factor == 1

    for db in (Clickhouse(), DuckDB()):
        assert db.benchmarks["chat_threads"].supported_operations == ("populate", "select", "mutate", "concurrent")

    # the concurrent workload terminates mserver5, so MonetDB measures the rest of the suite
    # rather than failing the pair; see MonetDBChatThreads
    assert MonetDB().benchmarks["chat_threads"].supported_operations == ("populate", "select", "mutate")


def test_time_series_scale_factor_10_matches_reference_size() -> None:
    assert get_time_series_dataset_sizes(10) == BASE_TIME_SERIES_DATASET_SIZES


def test_time_series_scale_factor_1_is_tenth_size_with_reduced_rows_and_columns() -> None:
    sizes = get_time_series_dataset_sizes(1)

    for size, (rows, cols) in sizes.items():
        reference_rows, reference_cols = BASE_TIME_SERIES_DATASET_SIZES[size]
        assert rows < reference_rows
        assert cols < reference_cols
        assert rows * cols == pytest.approx(reference_rows * reference_cols * 0.1, rel=0.001)

    tall_counts = get_time_series_column_counts(sizes["tall"][1])
    assert tall_counts.binary >= 1
    assert tall_counts.ratio >= 1
    assert tall_counts.deviation >= 1
    assert tall_counts.process >= 5

    wide_counts = get_time_series_column_counts(sizes["wide"][1])
    assert wide_counts.binary >= 22
    assert wide_counts.ratio >= 12
    assert wide_counts.deviation >= 39
    assert wide_counts.process >= 667


def test_time_series_restricts_supported_scale_factors() -> None:
    with pytest.raises(ValueError, match="supports scale factors: 1, 10"):
        resolve_suite_scale_factor("time_series", 2)


def test_jsonbench_restricts_supported_scale_factors() -> None:
    assert resolve_suite_scale_factor("jsonbench") == 10

    with pytest.raises(ValueError, match="supports scale factors: 10"):
        resolve_suite_scale_factor("jsonbench", 100)


def test_all_suite_scale_factor_resolution_fans_out_time_series() -> None:
    assert resolve_suite_scale_factors("time_series", include_all_supported=True) == (1, 10)
    assert resolve_suite_scale_factors("jsonbench", include_all_supported=True) == (10,)
    assert resolve_suite_scale_factors("tpc_h", include_all_supported=True) == (10, 50)
    assert resolve_suite_scale_factors("clickbench", 10, allow_fixed_default=True) == (1,)
    assert resolve_suite_scale_factors("jsonbench", 1, allow_fixed_default=True) == (10,)


def test_current_suite_scale_factor_requires_explicit_value() -> None:
    db = DummyDatabase()
    db._current_suite = "time_series"

    with pytest.raises(ValueError, match="current_suite_scale_factor is not set"):
        _ = db.current_suite_scale_factor


def test_docker_run_command_uses_native_platform_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(dbs_module, "get_container_engine_platform", lambda: "linux/amd64")
    command = DummyDatabase().docker_run_command("example:latest")

    assert command.startswith("docker run --platform linux/amd64")


def test_docker_run_command_allows_explicit_platform_override() -> None:
    command = DummyDatabase().docker_run_command("example:latest", platform="linux/amd64")

    assert command.startswith("docker run --platform linux/amd64")


def test_monetdb_selects_image_for_engine_platform(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(dbs_module, "get_container_engine_platform", lambda: "linux/amd64")
    assert MonetDB().resolved_container_image == MONETDB_RELEASE.amd64_container_image

    monkeypatch.setattr(dbs_module, "get_container_engine_platform", lambda: "linux/arm64")
    assert MonetDB().resolved_container_image == MONETDB_RELEASE.arm64_container_image


def test_monetdb_accepts_numeric_runtime_version_for_release_pin() -> None:
    db = MonetDB()

    assert db.version == MONETDB_RELEASE.label
    assert db.is_runtime_version_expected(MONETDB_RELEASE.runtime_version)


def test_monetdb_adbc_uri_exposes_ingest_tuning(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(MONETDB_SETTINGS, "write_window_bytes", 268_435_456)
    monkeypatch.setattr(MONETDB_SETTINGS, "wire_compression", "none")
    monkeypatch.setattr(MONETDB_SETTINGS, "constrained_append", "direct")

    db = MonetDB()
    assert db.connection_string == (
        "monetdb+adbc://monetdb:monetdb@localhost:50000/benchmark"
        "?client_application=olap-benchmarks&write_window_bytes=268435456"
        "&wire_compression=none&constrained_append=direct"
    )
    assert db.run_options["constrained_append"] == "direct"


def test_monetdb_time_series_analyzes_time_after_adbc_ingest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    analyzed: list[tuple[str, list[str] | None]] = []

    def analyze_table(
        _self: MonetDB,
        table: str,
        columns: list[str] | None = None,
    ) -> None:
        analyzed.append((table, columns))

    monkeypatch.setattr(MonetDB, "analyze_table", analyze_table)
    suite = MonetDBTimeSeries.model_construct(db=MonetDB(), name="time_series", scale_factor=1)

    suite.finish_parquet_table("data_tall")

    assert analyzed == [("data_tall", ["time"])]


def test_arm_host_falls_back_to_amd64_with_warning(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(dbs_module, "get_container_engine_platform", lambda: "linux/arm64")
    monkeypatch.setattr(DummyDatabase, "container_image", "example:latest")
    db = DummyDatabase()

    assert db.container_platform == "linux/amd64"
    assert db.uses_container_emulation is True
    warning = db.container_platform_warning
    assert warning is not None
    assert "does not provide an ARM64 container image" in warning


def test_active_step_stack_is_thread_local() -> None:
    db = DummyDatabase()
    db._push_active_step(11)

    def worker() -> tuple[int | None, int | None]:
        db._push_active_step(22)
        active_inside_step = db.active_step_id
        db._pop_active_step(22)
        return active_inside_step, db.active_step_id

    with ThreadPoolExecutor(max_workers=1) as executor:
        assert executor.submit(worker).result() == (22, None)

    assert db.active_step_id == 11
    db._pop_active_step(11)
    assert db.active_step_id is None


def test_database_benchmark_rejects_unsupported_operation() -> None:
    db = DummyDatabase()

    with pytest.raises(ValueError, match="does not support operation 'mutate'"):
        db.benchmark("clickbench", "mutate")


def test_database_benchmark_does_not_record_skipped_populate() -> None:
    db = SkipPopulateDatabase()

    db.benchmark("clickbench", "populate")

    assert db._run_id is None


def test_database_benchmark_propagates_should_populate_errors() -> None:
    db = BrokenShouldPopulateDatabase()

    with pytest.raises(RuntimeError, match="schema probe failed"):
        db.benchmark("clickbench", "populate")

    assert db._run_id is None


def test_runtime_version_verification_accepts_matching_version() -> None:
    db = RuntimeVersionDatabase(runtime_version="test build 1")

    db.verify_runtime_version()


def test_runtime_version_verification_rejects_mismatched_version_before_recording() -> None:
    db = RuntimeVersionDatabase(runtime_version="other build")

    with pytest.raises(RuntimeError, match="runtime version does not match pinned version"):
        db.benchmark("clickbench", "select")

    assert db._run_id is None


def test_suite_should_skip_populate_when_expected_data_exists() -> None:
    suite = DummySuite(db=CountingDatabase(row_counts={"hits": 10, "events": 20}))

    assert suite.should_populate() is False


def test_suite_should_populate_when_expected_tables_are_missing() -> None:
    db = CountingDatabase(row_count_errors={"hits": "missing", "events": "missing"})
    suite = DummySuite(db=db)

    assert suite.should_populate() is True
    assert db.rollback_calls == 2


def test_suite_should_fail_when_expected_tables_are_missing_but_db_is_not_empty() -> None:
    suite = DummySuite(
        db=CountingDatabase(
            row_count_errors={"hits": "missing", "events": "missing"},
            table_names={"legacy_table"},
        )
    )

    with pytest.raises(RuntimeError, match="database is not empty"):
        suite.should_populate()


def test_suite_should_fail_when_existing_data_is_inconsistent() -> None:
    suite = DummySuite(db=CountingDatabase(row_counts={"hits": 10}, row_count_errors={"events": "missing"}))

    with pytest.raises(RuntimeError, match="existing data does not match expected row counts"):
        suite.should_populate()


def test_suite_verify_populated_data_requires_expected_counts() -> None:
    suite = DummySuite(db=CountingDatabase(row_counts={"hits": 10, "events": 19}))

    with pytest.raises(RuntimeError, match="Populated data verification failed"):
        suite.verify_populated_data()
