from __future__ import annotations

from datetime import datetime
from multiprocessing.queues import Queue
from multiprocessing.synchronize import Event
from typing import cast

import pytest

from ..metrics import sampler
from ..metrics.measure import BenchmarkMetric
from ..metrics.storage import WriterMessage


class FakeStopEvent:
    def __init__(self) -> None:
        self._checks = 0

    def is_set(self) -> bool:
        self._checks += 1
        return self._checks > 2


class FakeStorage:
    inserted: list[tuple[int, datetime, float, int, int, int]] = []

    def __init__(self, _queue: object, _result_queue: object) -> None:
        self.inserted = []
        type(self).inserted = self.inserted

    def insert_metric(
        self,
        run_id: int,
        time: datetime,
        cpu_percent: float,
        mem_mb: int,
        client_mem_mb: int,
        disk_mb: int,
    ) -> None:
        self.inserted.append((run_id, time, cpu_percent, mem_mb, client_mem_mb, disk_mb))


class FakeFinalSampleTimeQueue:
    def __init__(self, final_sample_time: datetime) -> None:
        self.final_sample_time = final_sample_time

    def get(self) -> datetime:
        return self.final_sample_time


def test_sampling_loop_retries_after_transient_measurement_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    def get_metrics(
        _client_process_id: int,
        _container_names: object,
        _metric_directories: object,
    ) -> BenchmarkMetric:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise TimeoutError("Docker API stalled")
        return BenchmarkMetric(cpu_percent=10, mem_mb=20, client_mem_mb=5, disk_mb=30)

    monkeypatch.setattr(sampler, "setup_stdout_logging", lambda: None)
    monkeypatch.setattr(sampler, "Storage", FakeStorage)
    monkeypatch.setattr(sampler, "get_database_metrics", get_metrics)

    sampler.sampling_loop(
        client_process_id=42,
        container_names=("database",),
        metric_directories=(),
        run_id=7,
        stop_event=cast(Event, FakeStopEvent()),
        queue=cast(Queue[WriterMessage], object()),
        result_queue=cast(Queue[object], object()),
        final_sample_time_queue=cast(
            Queue[datetime],
            FakeFinalSampleTimeQueue(datetime(2026, 7, 28, 10, 0)),
        ),
        interval_seconds=None,
    )

    assert calls == 3
    assert FakeStorage.inserted == [
        (7, FakeStorage.inserted[0][1], 10.0, 20, 5, 30),
        (7, datetime(2026, 7, 28, 10, 0), 10.0, 20, 5, 30),
    ]
