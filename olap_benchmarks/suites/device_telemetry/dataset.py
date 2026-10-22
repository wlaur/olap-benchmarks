import json
import logging
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Final

import polars as pl
import pyarrow.parquet as pq

SEED: Final = 20251022
END: Final = datetime(2025, 1, 15)
ORIGIN: Final = END - timedelta(days=14)
SLOTS_PER_DAY: Final = 288
END_SLOT: Final = 14 * SLOTS_PER_DAY
GENERATION_SLOTS: Final = 24
DEVICE_TABLE: Final = "telemetry_device"
READING_TABLE: Final = "telemetry_reading"
METRICS: Final = (
    "cpu_user",
    "cpu_system",
    "memory_used",
    "disk_used",
    "net_rx",
    "net_tx",
    "temperature",
    "battery",
    "voltage",
    "current",
    "signal",
    "errors",
)
DEVICE_SCHEMA: Final = pl.Schema(
    {
        "device_id": pl.Int32,
        "region": pl.String,
        "tenant": pl.Int32,
        "model": pl.String,
        "rack": pl.String,
        "cadence_slots": pl.Int32,
        "rated_power": pl.Float64,
    }
)
READING_SCHEMA: Final = pl.Schema(
    {
        "time": pl.Datetime("ms"),
        "device_id": pl.Int32,
        "sample_no": pl.Int64,
        "ingested_at": pl.Datetime("ms"),
        "region": pl.String,
        "tenant": pl.Int32,
        "model": pl.String,
        "rack": pl.String,
        **dict.fromkeys(METRICS, pl.Float64),
    }
)
_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class TelemetrySpec:
    device_count: int
    history_days: int

    def __post_init__(self) -> None:
        if self.device_count < 1 or not 1 <= self.history_days <= 14:
            raise ValueError("Telemetry requires positive device cardinality and 1–14 days of history")

    @property
    def start_slot(self) -> int:
        return END_SLOT - self.history_days * SLOTS_PER_DAY

    def manifest(self) -> dict[str, object]:
        return {"version": 1, "seed": SEED, "end": END.isoformat(), **asdict(self)}


def scale_spec(scale_factor: int) -> TelemetrySpec:
    match scale_factor:
        case 1:
            return TelemetrySpec(1_000, 7)
        case 10:
            return TelemetrySpec(5_000, 14)
        case _:
            raise ValueError("device_telemetry supports scale factors 1 and 10")


def expected_reading_count(spec: TelemetrySpec) -> int:
    total = 0
    for device in range(spec.device_count):
        cadence = 1 if device % 10 == 0 else 3 if device % 10 < 4 else 12
        stop = END_SLOT - (SLOTS_PER_DAY if device % 17 == 0 else 0)
        first = (spec.start_slot + cadence - 1) // cadence
        last = (stop - 1) // cadence
        if first > last:
            continue
        total += last - first + 1
        for missing in range(3):
            residue = (missing - device * 13 - SEED) * pow(cadence, -1, 97) % 97
            total -= (last - residue) // 97 - (first - 1 - residue) // 97
    return total


def generate_devices(spec: TelemetrySpec) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "device_id": range(spec.device_count),
            "region": [f"region_{device // 10 % 4}" for device in range(spec.device_count)],
            "tenant": [device % 100 for device in range(spec.device_count)],
            "model": [f"model_{device % 3}" for device in range(spec.device_count)],
            "rack": [None if device % 11 == 0 else f"rack_{device % 16}" for device in range(spec.device_count)],
            "cadence_slots": [
                1 if device % 10 == 0 else 3 if device % 10 < 4 else 12 for device in range(spec.device_count)
            ],
            "rated_power": [100.0 + device % 5 * 25.0 for device in range(spec.device_count)],
        },
        schema=DEVICE_SCHEMA,
    )


def _readings_at(devices: pl.DataFrame, start: int, stop: int, *, scheduled: bool = True) -> pl.DataFrame:
    frame = pl.DataFrame({"sample_no": range(start, stop)}, schema={"sample_no": pl.Int64}).join(devices, how="cross")
    if scheduled:
        frame = frame.filter(
            (pl.col("sample_no") % pl.col("cadence_slots") == 0)
            & ((pl.col("sample_no") + pl.col("device_id") * 13 + SEED) % 97 >= 3)
            & ~((pl.col("device_id") % 17 == 0) & (pl.col("sample_no") >= END_SLOT - SLOTS_PER_DAY))
        )
    frame = frame.with_columns(
        (pl.lit(ORIGIN) + pl.duration(minutes=pl.col("sample_no") * 5)).cast(pl.Datetime("ms")).alias("time"),
        *[
            pl.when((pl.col("sample_no") + pl.col("device_id") * 7 + index * 11 + SEED) % 101 < 5)
            .then(None)
            .otherwise(
                ((pl.col("sample_no") * (index * 2 + 1) + pl.col("device_id") * (index * 7 + 11) + SEED) % 400) / 4.0
            )
            .alias(metric)
            for index, metric in enumerate(METRICS)
        ],
    )
    delay = (
        pl.when((pl.col("sample_no") + pl.col("device_id")) % 20 == 0)
        .then(7200)
        .otherwise((pl.col("sample_no") + pl.col("device_id")) % 31)
    )
    return (
        frame.with_columns((pl.col("time") + pl.duration(seconds=delay)).cast(pl.Datetime("ms")).alias("ingested_at"))
        .select(READING_SCHEMA.names())
        .cast(READING_SCHEMA)
    )


def generate_readings(spec: TelemetrySpec, start: int, stop: int) -> pl.DataFrame:
    if not spec.start_slot <= start <= stop <= END_SLOT:
        raise ValueError("Reading batch lies outside the configured history")
    return _readings_at(generate_devices(spec), start, stop).sort("time", "device_id")


def dataset_files(directory: Path) -> dict[str, Path]:
    return {table: directory / f"{table}.parquet" for table in (DEVICE_TABLE, READING_TABLE)}


def write_dataset(directory: Path, spec: TelemetrySpec, *, batch_slots: int = GENERATION_SLOTS) -> None:
    if batch_slots < 1:
        raise ValueError("batch_slots must be positive")
    directory.mkdir(parents=True, exist_ok=True)
    files = dataset_files(directory)
    partials = {table: path.with_suffix(".parquet.part") for table, path in files.items()}
    manifest = directory / "generation.json"
    manifest.unlink(missing_ok=True)
    try:
        generate_devices(spec).write_parquet(partials[DEVICE_TABLE], compression="zstd")
        writer: pq.ParquetWriter | None = None
        try:
            for start in range(spec.start_slot, END_SLOT, batch_slots):
                frame = generate_readings(spec, start, min(start + batch_slots, END_SLOT))
                arrow = frame.to_arrow()
                if writer is None:
                    writer = pq.ParquetWriter(partials[READING_TABLE], arrow.schema, compression="zstd")
                writer.write_table(arrow, row_group_size=25_000)
        finally:
            if writer is not None:
                writer.close()
        for table, path in files.items():
            partials[table].replace(path)
        manifest.write_text(json.dumps(spec.manifest(), sort_keys=True, indent=2) + "\n")
        _LOGGER.info("Generated device telemetry: %s", spec)
    finally:
        for partial in partials.values():
            partial.unlink(missing_ok=True)


def prepare_dataset(directory: Path, spec: TelemetrySpec, *, overwrite: bool = False) -> None:
    files = dataset_files(directory)
    manifest = directory / "generation.json"
    if not overwrite and (manifest.exists() or any(path.exists() for path in files.values())):
        if not manifest.is_file() or not all(path.is_file() for path in files.values()):
            raise ValueError("Incomplete device telemetry inputs; prepare again with overwrite=True")
        if json.loads(manifest.read_text()) != spec.manifest():
            raise ValueError("Stale device telemetry generation contract; prepare again with overwrite=True")
        for table, schema in ((DEVICE_TABLE, DEVICE_SCHEMA), (READING_TABLE, READING_SCHEMA)):
            if pl.scan_parquet(files[table]).collect_schema() != schema:
                raise ValueError(f"Stale device telemetry schema for {table}; prepare again with overwrite=True")
            expected = spec.device_count if table == DEVICE_TABLE else expected_reading_count(spec)
            actual = pl.scan_parquet(files[table]).select(pl.len()).collect().item()
            if actual != expected:
                raise ValueError(f"Stale device telemetry row count for {table}; prepare again with overwrite=True")
        return
    write_dataset(directory, spec)
