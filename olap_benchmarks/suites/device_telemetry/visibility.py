from dataclasses import dataclass
from datetime import timedelta

import polars as pl

from .dataset import END, END_SLOT, READING_TABLE, TelemetrySpec, _readings_at, generate_devices


@dataclass(frozen=True)
class VisibilityBatch:
    frame: pl.DataFrame
    query: str
    expected: pl.DataFrame


def visibility_batch(spec: TelemetrySpec, batch_number: int) -> VisibilityBatch:
    if batch_number < 0:
        raise ValueError("Visibility batch_number must be nonnegative and never reused in a run")
    slot = END_SLOT + batch_number
    frame = _readings_at(generate_devices(spec).head(8), slot, slot + 1, scheduled=False).sort("device_id")
    frame = frame.with_columns((pl.col("time") + pl.duration(seconds=1)).alias("ingested_at"))
    timestamp = (END + timedelta(minutes=batch_number * 5)).isoformat(sep=" ")
    query = (
        f"SELECT device_id, time, sample_no, cpu_user FROM {READING_TABLE} "
        f"WHERE time = '{timestamp}' AND device_id < 8 ORDER BY device_id"
    )
    return VisibilityBatch(frame, query, frame.select("device_id", "time", "sample_no", "cpu_user"))
