from collections import defaultdict
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, cast

import duckdb
import pytest

from ..suites.device_telemetry.config import QUERIES_DIRECTORY, QUERY_NAMES
from ..suites.device_telemetry.dataset import (
    DEVICE_TABLE,
    METRICS,
    READING_TABLE,
    TelemetrySpec,
    generate_devices,
    generate_readings,
)


@dataclass(frozen=True)
class ReferenceReading:
    device: int
    slot: int
    time: datetime
    arrival: datetime
    region: str
    tenant: int
    model: str
    rack: str | None
    values: tuple[float | None, ...]


SPEC = TelemetrySpec(60, 2)


def reference_readings() -> list[ReferenceReading]:
    readings: list[ReferenceReading] = []
    for slot in range(3456, 4032):
        for device in range(60):
            cadence = (1, 3, 3, 3, 12, 12, 12, 12, 12, 12)[device % 10]
            if slot % cadence or (slot + device * 13 + 20251022) % 97 < 3:
                continue
            if device % 17 == 0 and slot >= 3744:
                continue
            time = datetime(2025, 1, 1) + timedelta(minutes=slot * 5)
            delay = 7200 if (slot + device) % 20 == 0 else (slot + device) % 31
            values = tuple(
                None
                if (slot + device * 7 + index * 11 + 20251022) % 101 < 5
                else ((slot * (index * 2 + 1) + device * (index * 7 + 11) + 20251022) % 400) / 4
                for index in range(12)
            )
            readings.append(
                ReferenceReading(
                    device,
                    slot,
                    time,
                    time + timedelta(seconds=delay),
                    f"region_{device // 10 % 4}",
                    device % 100,
                    f"model_{device % 3}",
                    None if device % 11 == 0 else f"rack_{device % 16}",
                    values,
                )
            )
    return readings


REFERENCE = reference_readings()
DAY = [row for row in REFERENCE if row.time >= datetime(2025, 1, 14)]
HOUR = [row for row in DAY if row.time >= datetime(2025, 1, 14, 23)]


def mean(values: Sequence[float | None]) -> float | None:
    valid = [value for value in values if value is not None]
    return sum(valid) / len(valid) if valid else None


def latest_points(rows: list[ReferenceReading]) -> list[tuple[object, ...]]:
    latest = {row.device: row for row in rows}
    return [(device, row.time, row.values[0], row.values[2]) for device, row in sorted(latest.items())]


def oracle(query_name: str) -> list[tuple[object, ...]]:
    rows: list[tuple[object, ...]] = []
    if query_name in ("01_latest_device", "02_latest_per_device", "03_latest_region"):
        selected = REFERENCE
        if query_name.startswith("01"):
            selected = [row for row in selected if row.device == 42]
        elif query_name.startswith("03"):
            selected = [row for row in selected if row.region == "region_1"]
        return latest_points(selected)
    if query_name == "04_tenant_hour":
        selected = [row for row in HOUR if row.tenant == 7]
        values = [row.values[0] for row in selected if row.values[0] is not None]
        return [(len(selected), len(values), mean(values), min(values, default=None), max(values, default=None))]
    if query_name == "05_region_day":
        for model in ("model_0", "model_1", "model_2"):
            selected = [row for row in DAY if row.region == "region_1" and row.model == model]
            rows.append(
                (
                    model,
                    len(selected),
                    mean([row.values[0] for row in selected]),
                    mean([row.values[2] for row in selected]),
                    max((row.values[6] for row in selected if row.values[6] is not None), default=None),
                )
            )
        return rows
    if query_name == "06_fleet_hour_metrics":
        return [(len(HOUR), *[mean([row.values[index] for row in HOUR]) for index in range(12)])]
    if query_name in ("07_device_day_raw", "13_rolling_device"):
        selected = [row for row in DAY if row.device == 42]
        for index, row in enumerate(selected):
            if query_name.startswith("07"):
                rows.append((row.time, row.values[0], row.values[2], row.values[6], row.values[7]))
            else:
                rows.append(
                    (
                        row.time,
                        row.values[0],
                        mean([item.values[0] for item in selected[max(0, index - 5) : index + 1]]),
                    )
                )
        return rows
    if query_name in ("08_device_hourly", "09_region_model_hourly", "10_fleet_daily"):
        groups: dict[tuple[object, ...], list[ReferenceReading]] = defaultdict(list)
        for row in REFERENCE if query_name.startswith("10") else DAY:
            if query_name.startswith("08"):
                if row.device != 42:
                    continue
                key = (row.time.replace(minute=0),)
            elif query_name.startswith("09"):
                if row.region != "region_1":
                    continue
                key = (row.time.replace(minute=0), row.model)
            else:
                key = (row.time.replace(hour=0, minute=0), row.region)
            groups[key].append(row)
        for key, selected in groups.items():
            cpu = [row.values[0] for row in selected]
            memory = [row.values[2] for row in selected]
            if query_name.startswith("08"):
                rows.append((*key, len(selected), sum(value is not None for value in cpu), mean(cpu), mean(memory)))
            elif query_name.startswith("09"):
                rows.append((*key, len(selected), mean(cpu), max(v for v in memory if v is not None)))
            else:
                rows.append((*key, len(selected), mean(cpu), mean(memory)))
        return sorted(rows)
    if query_name == "11_busiest_devices":
        for device in range(60):
            values = [row.values[0] for row in DAY if row.device == device and row.values[0] is not None]
            if len(values) >= 12:
                rows.append((device, len(values), mean(values)))
        return sorted(rows, key=lambda row: (-cast(float, row[2]), cast(int, row[0])))[:20]
    if query_name in ("12_threshold_breaches", "15_null_coverage", "18_arrival_delay"):
        for region in ("region_0", "region_1", "region_2", "region_3"):
            selected = [row for row in (HOUR if query_name.startswith("18") else DAY) if row.region == region]
            if query_name.startswith("12"):
                cpu = [row for row in selected if row.values[0] is not None and row.values[0] >= 80]
                rows.append(
                    (
                        region,
                        len(selected),
                        len(cpu),
                        sum(row.values[2] is not None and row.values[2] >= 60 for row in cpu),
                    )
                )
            elif query_name.startswith("15"):
                rows.append(
                    (
                        region,
                        len(selected),
                        sum(row.rack is not None for row in selected),
                        sum(row.values[0] is not None for row in selected),
                        sum(row.values[7] is not None for row in selected),
                        sum(row.values[0] is None and row.values[7] is None for row in selected),
                    )
                )
            else:
                rows.append(
                    (
                        region,
                        len(selected),
                        sum(row.arrival >= datetime(2025, 1, 15) for row in selected),
                        max(row.arrival for row in selected),
                    )
                )
        return rows
    if query_name == "14_reporting_gaps":
        previous: dict[int, int] = {}
        for row in DAY:
            cadence = (1, 3, 3, 3, 12, 12, 12, 12, 12, 12)[row.device % 10]
            if row.device in previous and row.slot - previous[row.device] > cadence:
                rows.append((row.device, row.time, row.slot - previous[row.device]))
            previous[row.device] = row.slot
        return sorted(rows)
    if query_name == "16_metadata_power":
        power: dict[tuple[str, str | None], list[float | None]] = defaultdict(list)
        for row in DAY:
            value = row.values[0]
            power[row.region, row.rack].append(None if value is None else value * (100 + row.device % 5 * 25) / 100)
        return [
            (region, rack, len(values), mean(values))
            for (region, rack), values in sorted(power.items(), key=lambda item: (item[0][0], item[0][1] or ""))
        ]
    if query_name == "17_inactive_devices":
        latest = {row.device: row.time for row in REFERENCE}
        return [
            (device, latest.get(device))
            for device in range(60)
            if device not in latest or latest[device] < datetime(2025, 1, 14)
        ]
    raise AssertionError(f"Missing independent oracle for {query_name}")


@pytest.fixture(scope="module")
def telemetry_connection() -> Iterator[duckdb.DuckDBPyConnection]:
    con: duckdb.DuckDBPyConnection = cast(Any, duckdb).connect(database=":memory:", config={"threads": 2})
    con.register(DEVICE_TABLE, generate_devices(SPEC))
    con.register(READING_TABLE, generate_readings(SPEC, SPEC.start_slot, 4032))
    try:
        yield con
    finally:
        con.close()


@pytest.mark.parametrize("query_name", QUERY_NAMES)
@pytest.mark.parametrize("dialect", ["shared", "postgres", "timescaledb"])
def test_queries_have_exact_independent_answers(
    telemetry_connection: duckdb.DuckDBPyConnection,
    query_name: str,
    dialect: str,
) -> None:
    shared = QUERIES_DIRECTORY / f"{query_name}.sql"
    override = QUERIES_DIRECTORY / dialect / shared.name
    sql = (override if override.is_file() else shared).read_text()
    assert telemetry_connection.sql(sql).pl().rows() == oracle(query_name)


def test_reference_rows_match_every_generated_value() -> None:
    frame = generate_readings(SPEC, SPEC.start_slot, 4032)
    expected = [
        (row.time, row.device, row.slot, row.arrival, row.region, row.tenant, row.model, row.rack, *row.values)
        for row in REFERENCE
    ]
    assert frame.rows() == expected
    assert len(METRICS) == 12
    assert len(expected) > 5_000


@pytest.mark.parametrize("dialect", ["shared", "postgres", "timescaledb"])
def test_half_open_ranges_and_latest_row_keep_nulls(dialect: str) -> None:
    import polars as pl

    con: duckdb.DuckDBPyConnection = cast(Any, duckdb).connect(database=":memory:", config={"threads": 2})
    try:
        times = [
            datetime(2025, 1, 14, 22, 59, 59, 999000),
            datetime(2025, 1, 14, 23),
            datetime(2025, 1, 14, 23, 55),
            datetime(2025, 1, 15),
        ]
        frame = pl.DataFrame(
            {
                "device_id": [42] * 4,
                "tenant": [7] * 4,
                "region": ["region_1"] * 4,
                "time": times,
                "cpu_user": [10.0, 20.0, 30.0, None],
                "memory_used": [None] * 4,
            },
            schema_overrides={"cpu_user": pl.Float64, "memory_used": pl.Float64},
        )
        con.register(READING_TABLE, frame)

        def result(name: str) -> list[tuple[object, ...]]:
            shared = QUERIES_DIRECTORY / f"{name}.sql"
            override = QUERIES_DIRECTORY / dialect / shared.name
            return con.sql((override if override.is_file() else shared).read_text()).pl().rows()

        assert result("04_tenant_hour") == [(2, 2, 25.0, 20.0, 30.0)]
        for name in ("01_latest_device", "02_latest_per_device", "03_latest_region"):
            assert result(name) == [(42, datetime(2025, 1, 15), None, None)]
        con.register(READING_TABLE, frame.with_columns(pl.lit(None, dtype=pl.Float64).alias("cpu_user")))
        assert result("04_tenant_hour") == [(2, 0, None, None, None)]
        con.register(READING_TABLE, frame.head(0))
        assert result("04_tenant_hour") == [(0, 0, None, None, None)]
    finally:
        con.close()
