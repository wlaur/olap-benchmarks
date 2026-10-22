import os
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

import pytest

from ..dbs import Database, get_databases
from ..suites.device_telemetry.config import QUERY_NAMES, SUPPORTED_DATABASES, DeviceTelemetry
from ..suites.device_telemetry.dataset import DEVICE_TABLE, READING_TABLE, generate_devices, generate_readings
from .test_device_telemetry_queries import SPEC, oracle


@pytest.mark.live
def test_native_telemetry_answers_before_and_after_columnstore(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    selected = os.environ.get("OLAP_BENCHMARKS_TELEMETRY_FIXTURE_DATABASE")
    if selected is None:
        pytest.skip("Parent must provision one empty scratch engine and select TELEMETRY_FIXTURE_DATABASE")
    assert selected in SUPPORTED_DATABASES
    database = get_databases()[selected]
    database._current_suite = "device_telemetry"
    database._current_suite_scale_factor = 1
    if database.get_table_names():
        pytest.fail("Telemetry fixture requires an empty scratch database")

    @contextmanager
    def unmeasured_phase(_self: Database, phase_name: str, table_name: str | None = None) -> Generator[None]:
        yield

    monkeypatch.setattr(Database, "phase_context", unmeasured_phase)
    benchmark = DeviceTelemetry(db=database)
    try:
        database.initialize_schema("device_telemetry")
        frames = {
            DEVICE_TABLE: generate_devices(SPEC),
            READING_TABLE: generate_readings(SPEC, SPEC.start_slot, 4032),
        }
        for table, frame in frames.items():
            path = tmp_path / f"{table}.parquet"
            frame.write_parquet(path)
            database.insert_parquet(path, table)
        for compressed in (False, True) if selected == "timescaledb" else (False,):
            if compressed:
                database.execute(
                    "SELECT compress_chunk(i, if_not_compressed => true) FROM show_chunks('telemetry_reading') i",
                    autocommit=True,
                )
            for name in QUERY_NAMES:
                actual = database.fetch(benchmark.load_query(name), **benchmark.query_fetch_kwargs(name))
                assert actual.rows() == oracle(name), f"{selected}:{name}:compressed={compressed}"
    finally:
        for table in (READING_TABLE, DEVICE_TABLE):
            if table in database.get_table_names():
                database.execute(f"DROP TABLE {table}")
        database.close_connection()
