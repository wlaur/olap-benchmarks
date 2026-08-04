from __future__ import annotations

import logging
import os
import time
from collections.abc import Callable, Sequence
from concurrent.futures import Executor, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime
from multiprocessing import Event as create_event
from multiprocessing import Process, Queue, Value
from multiprocessing.queues import Queue as ProcessQueue
from multiprocessing.synchronize import Event
from pathlib import Path
from threading import Event as ThreadEvent
from threading import Thread
from typing import Protocol, cast

import psutil

from ..settings import setup_stdout_logging
from .measure import get_disk_sample, get_resource_sample
from .storage import Storage, WriterMessage

_LOGGER = logging.getLogger(__name__)
CLIENT_RSS_SAMPLE_SECONDS = 0.01
CLIENT_USS_SAMPLE_SECONDS = 0.1
# CPU and memory come from one cheap docker stats call per container plus a
# non-blocking psutil read, so they can run fast. Container CPU is derived from
# /proc/stat jiffies, which quantise at 10 ms, so intervals far below this turn
# the CPU series into quantisation noise rather than extra detail.
RESOURCE_SAMPLE_SECONDS = 0.2
# du over a large store walks every file and is the only sampler that measurably
# competes with the database for CPU and page cache, so it gets its own slow lane.
DISK_SAMPLE_SECONDS = 5.0
# and a hard ceiling on the share of wall clock the lane may spend measuring,
# so a store with 200k files stretches the interval instead of stealing a core
DISK_MAX_DUTY_CYCLE = 0.1

# recorded in run metadata; the disk figure is a floor rather than a fixed period
SAMPLING_INTERVAL_SECONDS = {
    "resource": RESOURCE_SAMPLE_SECONDS,
    "disk": DISK_SAMPLE_SECONDS,
    "client_rss": CLIENT_RSS_SAMPLE_SECONDS,
    "client_uss": CLIENT_USS_SAMPLE_SECONDS,
}


class _Lock(Protocol):
    def __enter__(self) -> object: ...

    def __exit__(self, *_: object) -> None: ...


class _SharedInteger(Protocol):
    value: int

    def get_lock(self) -> _Lock: ...


@dataclass(frozen=True, slots=True)
class MetricValues:
    cpu_percent: float | None = None
    server_mem_mb: int | None = None
    client_mem_mb: int | None = None
    client_uss_mb: int | None = None
    disk_mb: int | None = None


@dataclass(frozen=True, slots=True)
class Lane:
    name: str
    interval_seconds: float
    sample: Callable[[], MetricValues]
    # None keeps the interval fixed; a value caps measurement cost as a share of wall clock
    max_duty_cycle: float | None = None


@dataclass(slots=True)
class LaneSchedule:
    lane: Lane
    due_at: float

    def reschedule(self, finished_at: float, duration_seconds: float) -> None:
        interval = self.lane.interval_seconds
        if self.lane.max_duty_cycle is not None:
            interval = max(interval, duration_seconds / self.lane.max_duty_cycle)

        # advance on the nominal grid, but never fire ticks that elapsed while the
        # lane was busy: a slow sample must not be followed by a catch-up burst
        self.due_at = max(finished_at, self.due_at + interval)


def sample_client_memory(
    peak_rss_mb: _SharedInteger,
    peak_uss_mb: _SharedInteger,
    stop_event: ThreadEvent,
    interval_seconds: float = CLIENT_RSS_SAMPLE_SECONDS,
    uss_interval_seconds: float = CLIENT_USS_SAMPLE_SECONDS,
) -> None:
    process = psutil.Process()
    next_uss_sample = 0.0
    while True:
        rss_mb = int(process.memory_info().rss / (1024 * 1024))
        now = time.monotonic()
        uss_mb = None
        if now >= next_uss_sample:
            uss_mb = min(rss_mb, int(process.memory_full_info().uss / (1024 * 1024)))
            next_uss_sample = now + uss_interval_seconds
        with peak_rss_mb.get_lock():
            peak_rss_mb.value = max(peak_rss_mb.value, rss_mb)
        if uss_mb is not None:
            with peak_uss_mb.get_lock():
                peak_uss_mb.value = max(peak_uss_mb.value, uss_mb)
        if stop_event.wait(interval_seconds):
            return


def consume_client_memory_peak(
    peak_rss_mb: _SharedInteger,
    peak_uss_mb: _SharedInteger,
) -> tuple[int, int]:
    with peak_rss_mb.get_lock(), peak_uss_mb.get_lock():
        value = (peak_rss_mb.value, peak_uss_mb.value)
        peak_rss_mb.value = 0
        peak_uss_mb.value = 0
        return value


def build_lanes(
    executor: Executor,
    client_process_id: int,
    container_names: Sequence[str],
    metric_directories: Sequence[Path],
    resource_interval_seconds: float = RESOURCE_SAMPLE_SECONDS,
    disk_interval_seconds: float = DISK_SAMPLE_SECONDS,
    client_rss_peak: _SharedInteger | None = None,
    client_uss_peak: _SharedInteger | None = None,
) -> tuple[Lane, ...]:
    def sample_resources() -> MetricValues:
        if client_rss_peak is None or client_uss_peak is None:
            client_mem_mb, client_uss_mb = None, None
        else:
            client_mem_mb, client_uss_mb = consume_client_memory_peak(client_rss_peak, client_uss_peak)

        sample = get_resource_sample(
            executor,
            client_process_id,
            container_names,
            client_mem_mb,
            client_uss_mb,
        )
        return MetricValues(
            cpu_percent=sample.cpu_percent,
            server_mem_mb=sample.server_mem_mb,
            client_mem_mb=sample.client_mem_mb,
            client_uss_mb=sample.client_uss_mb,
        )

    def sample_disk() -> MetricValues:
        return MetricValues(disk_mb=get_disk_sample(metric_directories))

    return (
        Lane(name="resource", interval_seconds=resource_interval_seconds, sample=sample_resources),
        Lane(
            name="disk",
            interval_seconds=disk_interval_seconds,
            sample=sample_disk,
            max_duty_cycle=DISK_MAX_DUTY_CYCLE,
        ),
    )


def record_lane_sample(storage: Storage, run_id: int, lane: Lane, sample_time: datetime) -> None:
    # every lane writes its own row and leaves the columns it does not measure null,
    # so a sample is never a blend of readings taken at different instants
    values = lane.sample()
    storage.insert_metric(
        run_id=run_id,
        time=sample_time,
        cpu_percent=values.cpu_percent,
        server_mem_mb=values.server_mem_mb,
        client_mem_mb=values.client_mem_mb,
        client_uss_mb=values.client_uss_mb,
        disk_mb=values.disk_mb,
    )


def run_lane(
    storage: Storage,
    run_id: int,
    lane: Lane,
    stop_event: Event,
    monotonic: Callable[[], float] = time.monotonic,
) -> None:
    schedule = LaneSchedule(lane=lane, due_at=monotonic())

    while not stop_event.is_set():
        now = monotonic()
        if schedule.due_at > now:
            stop_event.wait(schedule.due_at - now)
            continue

        started_at = monotonic()
        try:
            record_lane_sample(storage, run_id, lane, datetime.now(UTC).replace(tzinfo=None))
        except Exception:
            _LOGGER.exception(f"{lane.name} metric sample failed; retrying on the next interval")

        finished_at = monotonic()
        schedule.reschedule(finished_at, finished_at - started_at)


def run_lanes(storage: Storage, run_id: int, lanes: Sequence[Lane], stop_event: Event) -> None:
    # one thread per lane: a du walk over a large store takes seconds, and running it
    # inline would punch a matching hole in the CPU series exactly when the database is
    # busiest with disk work, which is the moment the sample is most worth having
    threads = [
        Thread(
            target=run_lane,
            args=(storage, run_id, lane, stop_event),
            name=f"metric-lane-{lane.name}",
        )
        for lane in lanes
    ]

    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()


def sampling_loop(
    client_process_id: int,
    container_names: Sequence[str],
    metric_directories: Sequence[Path],
    run_id: int,
    stop_event: Event,
    queue: Queue[WriterMessage],
    result_queue: Queue[object],
    final_sample_time_queue: ProcessQueue[datetime],
    resource_interval_seconds: float = RESOURCE_SAMPLE_SECONDS,
    disk_interval_seconds: float = DISK_SAMPLE_SECONDS,
    client_rss_peak: _SharedInteger | None = None,
    client_uss_peak: _SharedInteger | None = None,
) -> None:
    setup_stdout_logging()
    storage = Storage(queue, result_queue)

    with ThreadPoolExecutor(
        max_workers=len(container_names) + 1,
        thread_name_prefix="metric-sample",
    ) as executor:
        lanes = build_lanes(
            executor,
            client_process_id,
            container_names,
            metric_directories,
            resource_interval_seconds,
            disk_interval_seconds,
            client_rss_peak,
            client_uss_peak,
        )

        run_lanes(storage, run_id, lanes, stop_event)

        # every lane samples once more at the finish time, so the run always ends with
        # a fresh disk reading no matter where the slow lane sat in its cycle
        final_sample_time = final_sample_time_queue.get()
        for lane in lanes:
            try:
                record_lane_sample(storage, run_id, lane, final_sample_time)
            except Exception:
                _LOGGER.exception(f"Final {lane.name} metric sample failed")


@dataclass(frozen=True, slots=True)
class MetricSampler:
    process: Process
    stop_event: Event
    final_sample_time_queue: ProcessQueue[datetime]
    memory_thread: Thread
    memory_stop_event: ThreadEvent

    def finish(self, finished_at: datetime) -> None:
        self.final_sample_time_queue.put(finished_at)
        self.stop_event.set()
        self.process.join()
        self.memory_stop_event.set()
        self.memory_thread.join()


def start_metric_sampler(
    container_names: Sequence[str],
    metric_directories: Sequence[Path],
    run_id: int,
    storage: Storage,
    resource_interval_seconds: float = RESOURCE_SAMPLE_SECONDS,
    disk_interval_seconds: float = DISK_SAMPLE_SECONDS,
) -> MetricSampler:
    stop_event = create_event()
    final_sample_time_queue: ProcessQueue[datetime] = Queue(maxsize=1)
    client_rss_peak = cast(_SharedInteger, Value("q", 0))
    client_uss_peak = cast(_SharedInteger, Value("q", 0))
    memory_stop_event = ThreadEvent()
    memory_thread = Thread(
        target=sample_client_memory,
        args=(client_rss_peak, client_uss_peak, memory_stop_event),
        name="client-memory-sampler",
        daemon=True,
    )

    process = Process(
        target=sampling_loop,
        args=(
            os.getpid(),
            tuple(container_names),
            tuple(metric_directories),
            run_id,
            stop_event,
            storage.queue,
            storage.result_queue,
            final_sample_time_queue,
            resource_interval_seconds,
            disk_interval_seconds,
            client_rss_peak,
            client_uss_peak,
        ),
        daemon=False,
    )

    process.start()
    memory_thread.start()

    return MetricSampler(
        process=process,
        stop_event=stop_event,
        final_sample_time_queue=final_sample_time_queue,
        memory_thread=memory_thread,
        memory_stop_event=memory_stop_event,
    )
