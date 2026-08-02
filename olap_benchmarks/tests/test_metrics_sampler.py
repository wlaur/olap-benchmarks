from __future__ import annotations

from datetime import datetime
from multiprocessing import Value
from multiprocessing.queues import Queue
from multiprocessing.synchronize import Event
from types import SimpleNamespace
from typing import Any, cast

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
    inserted: list[tuple[int, datetime, float, int, int, int, int]] = []

    def __init__(self, _queue: object, _result_queue: object) -> None:
        self.inserted = []
        type(self).inserted = self.inserted

    def insert_metric(
        self,
        run_id: int,
        time: datetime,
        cpu_percent: float,
        server_mem_mb: int,
        client_mem_mb: int,
        client_uss_mb: int,
        disk_mb: int,
    ) -> None:
        self.inserted.append((run_id, time, cpu_percent, server_mem_mb, client_mem_mb, client_uss_mb, disk_mb))


class FakeFinalSampleTimeQueue:
    def __init__(self, final_sample_time: datetime) -> None:
        self.final_sample_time = final_sample_time

    def get(self) -> datetime:
        return self.final_sample_time


def test_consume_client_memory_peak_returns_and_resets_both_values() -> None:
    peak_rss = cast(Any, Value("q", 512))
    peak_uss = cast(Any, Value("q", 384))

    assert sampler.consume_client_memory_peak(peak_rss, peak_uss) == (512, 384)
    assert sampler.consume_client_memory_peak(peak_rss, peak_uss) == (0, 0)


def test_sample_client_memory_records_one_consistent_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    peak_rss = cast(Any, Value("q", 0))
    peak_uss = cast(Any, Value("q", 0))

    class FakeProcess:
        def memory_info(self) -> SimpleNamespace:
            return SimpleNamespace(rss=512 * 1024 * 1024)

        def memory_full_info(self) -> SimpleNamespace:
            return SimpleNamespace(uss=768 * 1024 * 1024)

    class StopAfterOneSample:
        def wait(self, interval_seconds: float) -> bool:
            assert interval_seconds == 0.01
            return True

    monkeypatch.setattr(sampler.psutil, "Process", FakeProcess)

    sampler.sample_client_memory(
        peak_rss,
        peak_uss,
        cast(Any, StopAfterOneSample()),
    )

    assert sampler.consume_client_memory_peak(peak_rss, peak_uss) == (512, 512)


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
        return BenchmarkMetric(
            cpu_percent=10,
            server_mem_mb=20,
            client_mem_mb=5,
            client_uss_mb=4,
            disk_mb=30,
        )

    monkeypatch.setattr(sampler, "setup_stdout_logging", lambda: None)
    monkeypatch.setattr(sampler, "Storage", FakeStorage)
    monkeypatch.setattr(sampler, "get_benchmark_metrics", get_metrics)

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
        (7, FakeStorage.inserted[0][1], 10.0, 20, 5, 4, 30),
        (7, datetime(2026, 7, 28, 10, 0), 10.0, 20, 5, 4, 30),
    ]
