from collections.abc import Generator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Literal, cast

import polars as pl
import pytest
from polars.testing import assert_frame_equal
from pydantic import ConfigDict

from ..dbs.postgres import Postgres
from ..dbs.timescaledb import TimescaleDB, TimescaleTimeSeriesMetrics, TimescaleTimeSeriesRanges
from ..metrics.storage import Storage
from ..settings import TableName, get_settings
from ..suites.time_series_metrics.config import QUERY_NAMES as METRIC_QUERIES
from ..suites.time_series_metrics.config import generate_batch, to_eav
from ..suites.time_series_ranges.config import QUERY_NAMES as RANGE_QUERIES


class RecordingTelemetryDatabase(TimescaleDB):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    events: list[str] = []
    schemas: dict[str, pl.Schema] = {}
    frames: dict[str, pl.DataFrame] = {}
    failure: Literal["none", "copy", "compress"] = "none"

    def create_table(
        self,
        schema: pl.Schema,
        table: TableName,
        primary_key: str | list[str] | None = None,
        not_null: str | list[str] | None = None,
    ) -> None:
        assert primary_key is None
        assert not_null == (["time", "metric_name"] if table.startswith("eav") else "time")
        self.schemas[table] = schema
        self.events.append("create_table")

    def execute(self, statement: str, commit: bool = True, autocommit: bool = False, reconnect: bool = False) -> None:
        assert commit
        assert not reconnect
        self.events.append(statement)
        if "compress_chunk" in statement or "VACUUM" in statement:
            assert autocommit
        if "compress_chunk" in statement and self.failure == "compress":
            raise RuntimeError("compression failed")

    def insert(
        self,
        df: pl.DataFrame | pl.LazyFrame,
        table: TableName,
        primary_key: str | list[str] | None = None,
        not_null: str | list[str] | None = None,
    ) -> None:
        self.events.append("copy")
        if self.failure == "copy":
            raise RuntimeError("copy failed")
        assert primary_key is None
        assert not_null == (["time", "metric_name"] if table.startswith("eav") else "time")
        frame = df.collect() if isinstance(df, pl.LazyFrame) else df
        assert frame.schema == self.schemas[table]
        self.frames[table] = frame

    @contextmanager
    def phase_context(self, phase_name: str, table_name: str | None = None) -> Generator[None]:
        self.events.append(f"phase:{phase_name}:{table_name}")
        yield


@pytest.mark.parametrize(
    ("suite_name", "suite_type", "query_names"),
    [
        ("time_series_metrics", TimescaleTimeSeriesMetrics, METRIC_QUERIES),
        ("time_series_ranges", TimescaleTimeSeriesRanges, RANGE_QUERIES),
    ],
)
def test_native_telemetry_registration_preserves_queries_and_table_selection(
    monkeypatch: pytest.MonkeyPatch,
    suite_name: Literal["time_series_metrics", "time_series_ranges"],
    suite_type: type[TimescaleTimeSeriesMetrics] | type[TimescaleTimeSeriesRanges],
    query_names: tuple[str, ...],
) -> None:
    monkeypatch.setattr(get_settings(), "tables", ("wide100", "eav100"))
    suite = TimescaleDB().benchmarks[suite_name]
    reference = Postgres().benchmarks[suite_name]

    assert isinstance(suite, suite_type)
    assert suite.planned_query_names() == query_names
    assert list(suite.dataset_files()) == ["wide100", "eav100"]
    assert suite.expected_table_row_counts() == reference.expected_table_row_counts()
    for query_name in query_names:
        assert suite.load_query(query_name) == reference.load_query(query_name)
        assert suite.query_skip(query_name) == reference.query_skip(query_name)


@pytest.mark.parametrize("suite_name", ["time_series_metrics", "time_series_ranges"])
@pytest.mark.parametrize("layout", ["wide", "eav"])
def test_native_telemetry_configures_before_copy_and_compresses_original_data(
    tmp_path: Path,
    suite_name: Literal["time_series_metrics", "time_series_ranges"],
    layout: str,
) -> None:
    frame = generate_batch(0, 40, rows=40, metrics=100)
    if layout == "eav":
        frame = to_eav(frame)
    assert sum(frame.null_count().row(0)) > 0
    table = f"{layout}100"
    path = tmp_path / f"{table}.parquet"
    frame.write_parquet(path)
    database = RecordingTelemetryDatabase()
    suite = database.benchmarks[suite_name]
    assert isinstance(suite, TimescaleTimeSeriesMetrics)

    suite.populate_table(table, path)

    assert_frame_equal(database.frames[table], frame)
    events = database.events
    assert events[0:3] == [
        "create_table",
        f"phase:create_hypertable:{table}",
        f"SELECT create_hypertable('{table}', by_range('time', INTERVAL '7 days'))",
    ]
    assert "timescaledb.enable_columnstore = true" in events[3]
    assert "timescaledb.orderby = 'time DESC'" in events[3]
    segmentby = "metric_name" if layout == "eav" else ""
    assert f"timescaledb.segmentby = '{segmentby}'" in events[3]
    assert events[4:] == [
        "copy",
        f"phase:compress:{table}",
        f"SELECT compress_chunk(i, if_not_compressed => true) FROM show_chunks('{table}') i",
        f"phase:analyze:{table}",
        f'VACUUM FREEZE ANALYZE "{table}"',
    ]


@pytest.mark.parametrize("failure", ["copy", "compress"])
def test_native_telemetry_does_not_finalize_failed_preparation(
    tmp_path: Path, failure: Literal["copy", "compress"]
) -> None:
    path = tmp_path / "wide100.parquet"
    generate_batch(0, 2, rows=2, metrics=100).write_parquet(path)
    database = RecordingTelemetryDatabase(failure=failure)
    suite = database.benchmarks["time_series_metrics"]
    assert isinstance(suite, TimescaleTimeSeriesMetrics)

    with pytest.raises(RuntimeError, match="failed"):
        suite.populate_table("wide100", path)

    assert not any("VACUUM" in event for event in database.events)
    if failure == "copy":
        assert not any("compress_chunk" in event for event in database.events)


def test_wide1000_remains_a_rowstore_hypertable_because_compression_exceeds_tuple_limit(tmp_path: Path) -> None:
    path = tmp_path / "wide1000.parquet"
    generate_batch(0, 2, rows=2, metrics=1000).write_parquet(path)
    database = RecordingTelemetryDatabase()
    suite = database.benchmarks["time_series_metrics"]
    assert isinstance(suite, TimescaleTimeSeriesMetrics)
    suite.populate_table("wide1000", path)
    assert any("create_hypertable" in event for event in database.events)
    assert not any("enable_columnstore" in event or "compress_chunk" in event for event in database.events)
    assert database.events[-1] == 'VACUUM FREEZE ANALYZE "wide1000"'


@pytest.mark.parametrize("problem", [None, "missing", "incomplete", "wide_compressed"])
@pytest.mark.parametrize("recorded", [False, True])
def test_native_telemetry_rejects_count_complete_stores_with_unfinished_physical_preparation(
    problem: str | None,
    recorded: bool,
) -> None:
    class StorageDatabase(TimescaleDB):
        steps: list[dict[str, Any]] = []

        def _start_step(self, *args: object, **kwargs: object) -> int:
            return 1

        def _finish_step(self, step_id: int, *args: object, **kwargs: object) -> None:
            self.steps.append(kwargs)

        def fetch_python(
            self, query: str, schema: Mapping[str, pl.DataType | type[pl.DataType]] | None = None
        ) -> pl.DataFrame:
            return pl.DataFrame(
                {
                    "hypertable_name": ["absent" if problem == "missing" else "eav100", "wide1000"],
                    "compression_enabled": [True, problem == "wide_compressed"],
                    "chunks": [3, 3],
                    "compressed_chunks": [2 if problem == "incomplete" else 3, 0],
                }
            )

    database = StorageDatabase()
    if recorded:
        database._result_storage = cast(Storage, object())
        database._run_id = 1
    if problem is None:
        database.verify_telemetry_storage({"eav100": 100, "wide1000": 10})
    else:
        with pytest.raises(RuntimeError, match="Repopulate Timescale telemetry"):
            database.verify_telemetry_storage({"eav100": 100, "wide1000": 10})
    if recorded:
        assert database.steps[0]["status"] == ("completed" if problem is None else "failed")
        assert len(database.steps[0]["metadata"]["tables"]) == 2
    else:
        assert database.steps == []


def test_device_telemetry_select_requires_columnstore_only_for_readings() -> None:
    class ReadinessDatabase(TimescaleDB):
        verified: list[dict[str, int]] = []

        def verify_telemetry_storage(self, expected_tables: Mapping[str, int]) -> None:
            self.verified.append(dict(expected_tables))

    database = ReadinessDatabase()
    database._current_suite = "device_telemetry"
    database._current_suite_scale_factor = 10
    database._current_operation = "select"
    database.before_benchmark_operation()
    assert database.verified == [{"telemetry_reading": 4863454}]
