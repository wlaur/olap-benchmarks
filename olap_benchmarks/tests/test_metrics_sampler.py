from __future__ import annotations

from datetime import datetime
from multiprocessing import Value
from multiprocessing.queues import Queue
from multiprocessing.synchronize import Event
from threading import Event as ThreadEvent
from threading import Lock
from types import SimpleNamespace
from typing import Any, ClassVar, cast

import pytest

from ..metrics import sampler
from ..metrics.sampler import Lane, LaneSchedule, MetricValues
from ..metrics.storage import WriterMessage


class FakeStopEvent:
    def __init__(self, checks_before_stop: int = 2) -> None:
        self._checks = 0
        self.checks_before_stop = checks_before_stop
        self.waits: list[float] = []

    def is_set(self) -> bool:
        self._checks += 1
        return self._checks > self.checks_before_stop

    def wait(self, timeout: float) -> bool:
        self.waits.append(timeout)
        return False


class FakeStorage:
    inserted: ClassVar[list[dict[str, Any]]] = []

    def __init__(self, _queue: object, _result_queue: object) -> None:
        # sampling_loop builds its own Storage, so the rows have to be reachable from the
        # class itself; each construction starts a fresh list
        type(self).inserted = []

    def insert_metric(
        self,
        run_id: int,
        time: datetime,
        cpu_percent: float | None = None,
        server_mem_mb: int | None = None,
        client_mem_mb: int | None = None,
        client_uss_mb: int | None = None,
        disk_mb: int | None = None,
    ) -> None:
        self.inserted.append(
            {
                "run_id": run_id,
                "time": time,
                "cpu_percent": cpu_percent,
                "server_mem_mb": server_mem_mb,
                "client_mem_mb": client_mem_mb,
                "client_uss_mb": client_uss_mb,
                "disk_mb": disk_mb,
            }
        )


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


def test_lane_schedule_advances_on_the_nominal_grid() -> None:
    schedule = LaneSchedule(lane=Lane(name="resource", interval_seconds=0.2, sample=MetricValues), due_at=100.0)

    schedule.reschedule(finished_at=100.01, duration_seconds=0.01)

    assert schedule.due_at == pytest.approx(100.2)


def test_lane_schedule_skips_ticks_missed_while_the_lane_was_busy() -> None:
    schedule = LaneSchedule(lane=Lane(name="resource", interval_seconds=0.2, sample=MetricValues), due_at=100.0)

    schedule.reschedule(finished_at=101.0, duration_seconds=1.0)

    # the four elapsed ticks are dropped rather than fired back to back
    assert schedule.due_at == pytest.approx(101.0)


def test_lane_schedule_stretches_interval_to_respect_max_duty_cycle() -> None:
    schedule = LaneSchedule(
        lane=Lane(name="disk", interval_seconds=5.0, sample=MetricValues, max_duty_cycle=0.1),
        due_at=100.0,
    )

    schedule.reschedule(finished_at=101.13, duration_seconds=1.13)

    # an 1.13 s du walk may only occupy a tenth of wall clock, so it waits 11.3 s
    assert schedule.due_at == pytest.approx(111.3)


def test_lane_schedule_keeps_nominal_interval_when_sampling_is_cheap() -> None:
    schedule = LaneSchedule(
        lane=Lane(name="disk", interval_seconds=5.0, sample=MetricValues, max_duty_cycle=0.1),
        due_at=100.0,
    )

    schedule.reschedule(finished_at=100.002, duration_seconds=0.002)

    assert schedule.due_at == pytest.approx(105.0)


def test_run_lane_writes_only_the_columns_it_measures() -> None:
    storage = FakeStorage(object(), object())
    resource = Lane(
        name="resource",
        interval_seconds=0.0,
        sample=lambda: MetricValues(cpu_percent=10.0, server_mem_mb=20, client_mem_mb=5, client_uss_mb=4),
    )
    disk = Lane(name="disk", interval_seconds=0.0, sample=lambda: MetricValues(disk_mb=30))

    sampler.run_lane(cast(Any, storage), 7, resource, cast(Event, FakeStopEvent(checks_before_stop=1)))
    sampler.run_lane(cast(Any, storage), 7, disk, cast(Event, FakeStopEvent(checks_before_stop=1)))

    assert [row["cpu_percent"] for row in storage.inserted] == [10.0, None]
    assert [row["server_mem_mb"] for row in storage.inserted] == [20, None]
    assert [row["disk_mb"] for row in storage.inserted] == [None, 30]
    assert all(row["run_id"] == 7 for row in storage.inserted)


def test_run_lane_samples_on_every_pass_once_it_is_due() -> None:
    storage = FakeStorage(object(), object())
    lane = Lane(name="resource", interval_seconds=0.0, sample=lambda: MetricValues(cpu_percent=1.0))

    sampler.run_lane(cast(Any, storage), 7, lane, cast(Event, FakeStopEvent(checks_before_stop=3)))

    assert len(storage.inserted) == 3


def test_run_lane_waits_out_its_own_interval_before_sampling_again() -> None:
    storage = FakeStorage(object(), object())
    stop_event = FakeStopEvent(checks_before_stop=6)
    lane = Lane(name="disk", interval_seconds=1_000.0, sample=lambda: MetricValues(disk_mb=1))

    sampler.run_lane(cast(Any, storage), 7, lane, cast(Event, stop_event))

    # the next tick is 1000 s out, so every later pass waits instead of sampling
    assert len(storage.inserted) == 1
    assert stop_event.waits and all(wait > 0 for wait in stop_event.waits)


def test_run_lane_keeps_sampling_after_a_transient_failure() -> None:
    storage = FakeStorage(object(), object())
    calls = 0

    def failing_sample() -> MetricValues:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise TimeoutError("Docker API stalled")
        return MetricValues(cpu_percent=10.0)

    lane = Lane(name="resource", interval_seconds=0.0, sample=failing_sample)

    sampler.run_lane(cast(Any, storage), 7, lane, cast(Event, FakeStopEvent(checks_before_stop=2)))

    assert calls == 2
    assert [row["cpu_percent"] for row in storage.inserted] == [10.0]


def test_run_lanes_does_not_let_a_slow_lane_block_a_fast_one() -> None:
    storage = FakeStorage(object(), object())
    stop_event = ThreadEvent()
    fast_samples_taken = ThreadEvent()
    fast_calls = 0
    lock = Lock()

    def fast_sample() -> MetricValues:
        nonlocal fast_calls
        with lock:
            fast_calls += 1
            if fast_calls >= 3:
                fast_samples_taken.set()
        return MetricValues(cpu_percent=1.0)

    def slow_sample() -> MetricValues:
        # only returns once the fast lane has sampled repeatedly, which cannot happen
        # unless the two lanes are genuinely running side by side
        assert fast_samples_taken.wait(timeout=10)
        stop_event.set()
        return MetricValues(disk_mb=1)

    lanes = (
        Lane(name="resource", interval_seconds=0.0, sample=fast_sample),
        Lane(name="disk", interval_seconds=0.0, sample=slow_sample),
    )

    sampler.run_lanes(cast(Any, storage), 7, lanes, cast(Event, stop_event))

    assert fast_calls >= 3
    assert any(row["disk_mb"] == 1 for row in storage.inserted)


def test_sampling_loop_ends_with_a_final_sample_from_every_lane(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    final_sample_time = datetime(2026, 7, 28, 10, 0)

    def build_lanes(*_args: object, **_kwargs: object) -> tuple[Lane, ...]:
        return (
            Lane(name="resource", interval_seconds=0.0, sample=lambda: MetricValues(cpu_percent=10.0)),
            Lane(name="disk", interval_seconds=0.0, sample=lambda: MetricValues(disk_mb=30)),
        )

    monkeypatch.setattr(sampler, "setup_stdout_logging", lambda: None)
    monkeypatch.setattr(sampler, "Storage", FakeStorage)
    monkeypatch.setattr(sampler, "build_lanes", build_lanes)

    sampler.sampling_loop(
        client_process_id=42,
        container_names=("database",),
        metric_directories=(),
        run_id=7,
        stop_event=cast(Event, FakeStopEvent(checks_before_stop=0)),
        queue=cast(Queue[WriterMessage], object()),
        result_queue=cast(Queue[object], object()),
        final_sample_time_queue=cast(Queue[datetime], FakeFinalSampleTimeQueue(final_sample_time)),
    )

    assert [(row["time"], row["cpu_percent"], row["disk_mb"]) for row in FakeStorage.inserted] == [
        (final_sample_time, 10.0, None),
        (final_sample_time, None, 30),
    ]
