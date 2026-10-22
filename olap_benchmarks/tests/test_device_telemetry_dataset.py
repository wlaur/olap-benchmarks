import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, cast

import duckdb
import polars as pl
import pytest
from polars.testing import assert_frame_equal

from ..inputs import fingerprint_input_directory
from ..suites.device_telemetry.config import QUERIES_DIRECTORY
from ..suites.device_telemetry.dataset import (
    DEVICE_SCHEMA,
    DEVICE_TABLE,
    END,
    END_SLOT,
    METRICS,
    READING_SCHEMA,
    READING_TABLE,
    TelemetrySpec,
    dataset_files,
    generate_devices,
    generate_readings,
    prepare_dataset,
    scale_spec,
    write_dataset,
)
from ..suites.device_telemetry.visibility import visibility_batch


def test_shape_nulls_multiplicity_activity_and_synthetic_arrival() -> None:
    spec = TelemetrySpec(60, 2)
    devices = generate_devices(spec)
    readings = generate_readings(spec, spec.start_slot, END_SLOT)
    assert devices.schema == DEVICE_SCHEMA
    assert readings.schema == READING_SCHEMA
    assert devices["device_id"].n_unique() == spec.device_count
    assert readings.select("device_id", "time").n_unique() == readings.height
    assert readings["time"].n_unique() < readings.height
    assert all(readings[metric].null_count() > 0 for metric in METRICS)
    assert readings["rack"].null_count() > 0
    assert (
        readings.select("device_id", "time", "sample_no", "ingested_at", "region", "tenant", "model")
        .null_count()
        .row(0)
        == (0,) * 7
    )
    assert cast(datetime, readings.filter(pl.col("device_id") % 17 == 0)["time"].max()) < END - timedelta(days=1)
    assert cast(datetime, readings["time"].max()) < END
    assert cast(datetime, readings["ingested_at"].max()) > END
    lag = readings["ingested_at"] - readings["time"]
    assert lag.max() == timedelta(hours=2)
    assert lag.min() == timedelta(0)
    counts = readings.group_by("device_id").len()
    hot = counts.filter(pl.col("device_id") == 10)["len"].item()
    cold = counts.filter(pl.col("device_id") == 14)["len"].item()
    assert hot > cold * 10
    assert readings.filter(pl.col("device_id") == 42).tail(1).select("time", "cpu_user", "memory_used").row(0) == (
        END - timedelta(minutes=15),
        78.25,
        54.25,
    )


def test_scale_changes_cardinality_and_history_while_preserving_shared_readings() -> None:
    small, large = scale_spec(1), scale_spec(10)
    assert (small.device_count, small.history_days) == (1000, 7)
    assert (large.device_count, large.history_days) == (5000, 14)
    assert large.device_count * large.history_days == 10 * small.device_count * small.history_days
    assert generate_devices(large).head(small.device_count).equals(generate_devices(small))
    small_slice = generate_readings(small, END_SLOT - 24, END_SLOT)
    large_slice = generate_readings(large, END_SLOT - 24, END_SLOT)
    assert_frame_equal(small_slice, large_slice.filter(pl.col("device_id") < small.device_count))
    assert small_slice.width == large_slice.width == 20
    for frame in (small_slice, large_slice):
        assert frame.filter(pl.col("tenant") == 7).height > 0
        assert frame.filter(pl.col("region") == "region_1").height > 0


def test_partitioning_does_not_change_values_and_preparation_reuses_inputs(tmp_path: Path) -> None:
    spec = TelemetrySpec(60, 2)
    first, second = tmp_path / "first", tmp_path / "second"
    write_dataset(first, spec, batch_slots=71)
    write_dataset(second, spec, batch_slots=137)
    for table, path in dataset_files(first).items():
        assert_frame_equal(pl.read_parquet(path), pl.read_parquet(dataset_files(second)[table]))
    assert_frame_equal(
        pl.read_parquet(dataset_files(first)[READING_TABLE]), generate_readings(spec, spec.start_slot, END_SLOT)
    )
    before = fingerprint_input_directory(first, tmp_path / "hash-cache.json")
    stat = {path: path.stat().st_mtime_ns for path in first.iterdir()}
    prepare_dataset(first, spec)
    assert stat == {path: path.stat().st_mtime_ns for path in first.iterdir()}
    assert fingerprint_input_directory(first, tmp_path / "hash-cache.json") == before
    assert {path.name for path in first.iterdir()} == {
        "telemetry_device.parquet",
        "telemetry_reading.parquet",
        "generation.json",
    }


def test_preparation_rejects_partial_stale_or_changed_schema(tmp_path: Path) -> None:
    spec = TelemetrySpec(12, 1)
    write_dataset(tmp_path, spec)
    with pytest.raises(ValueError, match="Stale.*contract"):
        prepare_dataset(tmp_path, TelemetrySpec(13, 1))
    manifest = tmp_path / "generation.json"
    data = json.loads(manifest.read_text())
    data["seed"] = -1
    manifest.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="Stale.*contract"):
        prepare_dataset(tmp_path, spec)
    prepare_dataset(tmp_path, spec, overwrite=True)
    path = dataset_files(tmp_path)[READING_TABLE]
    pl.read_parquet(path).drop("cpu_user").write_parquet(path)
    with pytest.raises(ValueError, match="Stale.*schema"):
        prepare_dataset(tmp_path, spec)
    prepare_dataset(tmp_path, spec, overwrite=True)
    pl.read_parquet(path).head(1).write_parquet(path)
    with pytest.raises(ValueError, match="Stale.*row count"):
        prepare_dataset(tmp_path, spec)
    path.unlink()
    with pytest.raises(ValueError, match="Incomplete"):
        prepare_dataset(tmp_path, spec)


@pytest.mark.parametrize("scale", [0, 2, 100])
def test_invalid_scale_is_rejected(scale: int) -> None:
    with pytest.raises(ValueError, match="scale factors"):
        scale_spec(scale)


def test_batch_bounds_and_empty_intervals() -> None:
    spec = TelemetrySpec(1, 1)
    assert generate_readings(spec, END_SLOT, END_SLOT).schema == READING_SCHEMA
    assert generate_readings(spec, END_SLOT, END_SLOT).height == 0
    for start, stop in ((spec.start_slot - 1, spec.start_slot), (END_SLOT, END_SLOT + 1), (END_SLOT, END_SLOT - 1)):
        with pytest.raises(ValueError, match="outside"):
            generate_readings(spec, start, stop)
    with pytest.raises(ValueError, match="cardinality"):
        TelemetrySpec(0, 1)
    with pytest.raises(ValueError, match="history"):
        TelemetrySpec(1, 15)


def test_schema_and_append_visibility_preserve_exact_rows_in_memory() -> None:
    spec = TelemetrySpec(60, 2)
    con: duckdb.DuckDBPyConnection = cast(Any, duckdb).connect(database=":memory:", config={"threads": 2})
    try:
        con.execute((QUERIES_DIRECTORY.parent / "schemas" / "duckdb.sql").read_text())
        devices = generate_devices(spec)
        source = generate_readings(spec, END_SLOT - 24, END_SLOT)
        for table, frame in ((DEVICE_TABLE, devices), (READING_TABLE, source)):
            con.register("fixture", frame)
            con.execute(f"INSERT INTO {table} SELECT * FROM fixture")
        assert con.sql(f"SELECT COUNT(*) AS n FROM {READING_TABLE}").pl().item() == source.height
        batches = [visibility_batch(spec, index) for index in (0, 1)]
        for batch in batches:
            assert batch.frame.schema == READING_SCHEMA
            assert con.sql(batch.query).pl().height == 0
            con.register("fixture", batch.frame)
            con.execute(f"INSERT INTO {READING_TABLE} SELECT * FROM fixture")
            assert_frame_equal(con.sql(batch.query).pl(), batch.expected, check_dtypes=False)
        for batch in batches:
            assert_frame_equal(con.sql(batch.query).pl(), batch.expected, check_dtypes=False)
        assert batches[0].frame["time"].min() == END
        assert batches[0].expected.height == 8
        assert batches[0].frame.equals(visibility_batch(spec, 0).frame)
        assert_frame_equal(
            con.sql(f"SELECT * FROM {READING_TABLE} WHERE time < '2025-01-15' ORDER BY time, device_id").pl(),
            source,
            check_dtypes=False,
        )
    finally:
        con.close()
    with pytest.raises(ValueError, match="nonnegative"):
        visibility_batch(spec, -1)
