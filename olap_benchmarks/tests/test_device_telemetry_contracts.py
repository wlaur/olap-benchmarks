from collections.abc import Generator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Literal

import polars as pl
import pytest
from polars.testing import assert_frame_equal
from pydantic import ConfigDict

from ..dbs import ParquetEpochColumns
from ..dbs.postgres import Postgres
from ..settings import DatabaseName, TableName, get_settings
from ..suites.device_telemetry import config
from ..suites.device_telemetry.config import QUERIES_DIRECTORY, QUERY_NAMES, SUPPORTED_DATABASES, DeviceTelemetry
from ..suites.device_telemetry.dataset import (
    DEVICE_TABLE,
    END_SLOT,
    READING_TABLE,
    TelemetrySpec,
    dataset_files,
    expected_reading_count,
    generate_readings,
    write_dataset,
)


class RecordingTelemetryDatabase(Postgres):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    events: list[str] = []
    frames: dict[str, pl.DataFrame] = {}
    failure: Literal["none", "insert", "compress"] = "none"
    compressed: bool = False

    @contextmanager
    def phase_context(self, phase_name: str, table_name: str | None = None) -> Generator[None]:
        self.events.append(f"phase:{phase_name}:{table_name}")
        yield

    def initialize_schema(self, suite_directory: str) -> None:
        assert suite_directory == "device_telemetry"
        schema = QUERIES_DIRECTORY.parent / "schemas" / f"{self.name}.sql"
        assert schema.is_file()
        self.events.append("schema")

    def insert_parquet(
        self,
        path: Path,
        table: TableName,
        primary_key: str | list[str] | None = None,
        not_null: str | list[str] | None = None,
        *,
        epoch_columns: ParquetEpochColumns | None = None,
    ) -> None:
        assert epoch_columns is None
        assert primary_key is None
        assert not_null == ["device_id", "region", "tenant", "model"] + (
            ["cadence_slots", "rated_power"] if table == DEVICE_TABLE else ["time", "sample_no", "ingested_at"]
        )
        self.events.append(f"insert:{table}")
        if self.failure == "insert" and table == READING_TABLE:
            raise RuntimeError("fixture insert failed")
        self.frames[table] = pl.read_parquet(path)

    def get_row_count(self, table: TableName) -> int:
        if table not in self.frames:
            raise ValueError("missing fixture table")
        return self.frames[table].height

    def get_table_names(self) -> set[TableName]:
        return set(self.frames)

    def rollback(self) -> None:
        pass

    def execute(self, statement: str, commit: bool = True, autocommit: bool = False, reconnect: bool = False) -> None:
        assert commit and autocommit and not reconnect
        self.events.append(statement)
        if "compress_chunk" in statement:
            if self.failure == "compress":
                raise RuntimeError("fixture compression failed")
            self.compressed = True

    def verify_telemetry_storage(self, expected_tables: Mapping[str, int]) -> None:
        assert expected_tables == {READING_TABLE: self.frames[READING_TABLE].height}
        self.events.append("verify_columnstore")
        if not self.compressed:
            raise RuntimeError("fixture columnstore incomplete")

    def verify_storage_readiness(self) -> None:
        self.events.append("storage_ready")

    def restart_event(self) -> None:
        self.events.append("restart")


@pytest.fixture
def small_inputs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    spec = TelemetrySpec(60, 2)

    def fixture_scale(scale_factor: int) -> TelemetrySpec:
        assert scale_factor == 1
        return spec

    monkeypatch.setattr(config, "scale_spec", fixture_scale)
    monkeypatch.setattr(get_settings(), "input_data_directory", tmp_path)
    directory = tmp_path / "device_telemetry_sf1"
    write_dataset(directory, spec, batch_slots=144)
    return directory


@pytest.mark.parametrize("engine", sorted(SUPPORTED_DATABASES))
def test_population_reuses_loader_preserves_source_and_checks_before_restart(
    small_inputs: Path, engine: DatabaseName
) -> None:
    database = RecordingTelemetryDatabase(name=engine)
    suite = DeviceTelemetry(db=database)
    suite.populate()
    for table, path in dataset_files(small_inputs).items():
        assert_frame_equal(database.frames[table], pl.read_parquet(path))
    assert database.events.index("schema") < database.events.index(f"insert:{DEVICE_TABLE}")
    assert database.events.index(f"insert:{DEVICE_TABLE}") < database.events.index(f"insert:{READING_TABLE}")
    assert database.events.index("phase:verify_populate:None") < database.events.index("restart")
    if engine == "timescaledb":
        assert database.events.index(f"insert:{READING_TABLE}") < database.events.index(
            f"phase:compress:{READING_TABLE}"
        )
        assert database.compressed
        assert "verify_columnstore" in database.events
    else:
        assert not database.compressed
    if engine in ("postgres", "timescaledb"):
        assert all(f"VACUUM FREEZE ANALYZE {table}" in database.events for table in (DEVICE_TABLE, READING_TABLE))
    database.events.clear()
    suite.populate()
    assert "schema" not in database.events
    assert "restart" not in database.events
    assert "storage_ready" in database.events


@pytest.mark.parametrize("failure", ["insert", "compress"])
def test_failed_populate_cannot_restart_or_silently_reuse_incomplete_store(
    small_inputs: Path,
    failure: Literal["insert", "compress"],
) -> None:
    database = RecordingTelemetryDatabase(name="timescaledb", failure=failure)
    suite = DeviceTelemetry(db=database)
    with pytest.raises(RuntimeError, match="fixture"):
        suite.populate()
    assert "restart" not in database.events
    with pytest.raises(RuntimeError, match="does not match|columnstore incomplete"):
        suite.populate()
    assert "restart" not in database.events


def test_query_files_and_resolved_engine_contracts() -> None:
    assert set(QUERY_NAMES) == {path.stem for path in QUERIES_DIRECTORY.glob("*.sql")}
    assert len(QUERY_NAMES) == len(set(QUERY_NAMES)) == 18
    for engine in SUPPORTED_DATABASES:
        suite = DeviceTelemetry(db=RecordingTelemetryDatabase(name=engine))
        assert suite.planned_query_names() == QUERY_NAMES
        assert suite.supported_operations == ("populate", "select")
        for query in QUERY_NAMES:
            assert suite.load_query(query).strip()
        for override in (QUERIES_DIRECTORY / engine).glob("*.sql"):
            assert override.stem in QUERY_NAMES
            assert override.read_text() != (QUERIES_DIRECTORY / override.name).read_text()
        schema = (QUERIES_DIRECTORY.parent / "schemas" / f"{engine}.sql").read_text()
        assert "PRIMARY KEY (time)" not in schema
    ch = DeviceTelemetry(db=RecordingTelemetryDatabase(name="clickhouse"))
    for query in ("02_latest_per_device", "03_latest_region"):
        sql = ch.load_query(query)
        assert "argMax(tuple(time, cpu_user, memory_used), time)" in sql
    assert "avgOrNull(cpu_user)" in ch.load_query("04_tenant_hour")
    assert "lagInFrame(toNullable(sample_no), 1, NULL)" in ch.load_query("14_reporting_gaps")
    assert ch.fetch_kwargs["settings"]["join_use_nulls"] == 1
    assert "latest_arrival" in ch.fetch_kwargs["time_columns"]


@pytest.mark.parametrize("engine", ["questdb", "tdengine", "polars", "monetdb", "doris", "starrocks", "arc"])
def test_unimplemented_loaders_fail_closed(engine: DatabaseName) -> None:
    database = RecordingTelemetryDatabase(name=engine)
    suite = DeviceTelemetry(db=database)
    with pytest.raises(NotImplementedError, match="no verified loader/query contract"):
        suite.load_query(QUERY_NAMES[0])
    with pytest.raises(NotImplementedError, match="no verified loader/query contract"):
        suite.populate()
    assert database.events == []


def test_expected_counts_follow_generation_without_input_files() -> None:
    for scale, expected in ((1, 484318), (10, 4863454)):
        suite = DeviceTelemetry(db=RecordingTelemetryDatabase(), scale_factor=scale)
        assert suite.expected_table_row_counts() == {
            DEVICE_TABLE: 1000 if scale == 1 else 5000,
            READING_TABLE: expected,
        }
    for spec in (TelemetrySpec(1, 1), TelemetrySpec(17, 1), TelemetrySpec(60, 2)):
        assert expected_reading_count(spec) == generate_readings(spec, spec.start_slot, END_SLOT).height
