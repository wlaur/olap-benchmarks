from __future__ import annotations

import logging
import os
import time
from collections.abc import Sequence
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
from .measure import get_database_metrics
from .storage import Storage, WriterMessage

_LOGGER = logging.getLogger(__name__)


class _Lock(Protocol):
    def __enter__(self) -> object: ...

    def __exit__(self, *_: object) -> None: ...


class _SharedInteger(Protocol):
    value: int

    def get_lock(self) -> _Lock: ...


def sample_client_memory(
    peak_rss_mb: _SharedInteger,
    peak_uss_mb: _SharedInteger,
    stop_event: ThreadEvent,
    interval_seconds: float = 0.01,
) -> None:
    process = psutil.Process()
    while True:
        memory = process.memory_full_info()
        rss_mb = int(memory.rss / (1024 * 1024))
        uss_mb = min(rss_mb, int(memory.uss / (1024 * 1024)))
        with peak_rss_mb.get_lock(), peak_uss_mb.get_lock():
            peak_rss_mb.value = max(peak_rss_mb.value, rss_mb)
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


def sampling_loop(
    client_process_id: int,
    container_names: Sequence[str],
    metric_directories: Sequence[Path],
    run_id: int,
    stop_event: Event,
    queue: Queue[WriterMessage],
    result_queue: Queue[object],
    final_sample_time_queue: ProcessQueue[datetime],
    interval_seconds: float | None = 1.0,
    client_rss_peak: _SharedInteger | None = None,
    client_uss_peak: _SharedInteger | None = None,
) -> None:
    setup_stdout_logging()
    storage = Storage(queue, result_queue)

    def sample_once(sample_time: datetime | None = None) -> None:
        now = sample_time or datetime.now(UTC).replace(tzinfo=None)
        if client_rss_peak is None or client_uss_peak is None:
            metric = get_database_metrics(client_process_id, container_names, metric_directories)
        else:
            client_mem_mb, client_uss_mb = consume_client_memory_peak(
                client_rss_peak,
                client_uss_peak,
            )
            metric = get_database_metrics(
                client_process_id,
                container_names,
                metric_directories,
                client_mem_mb,
                client_uss_mb,
            )

        storage.insert_metric(
            run_id=run_id,
            time=now,
            cpu_percent=metric.cpu_percent,
            mem_mb=metric.mem_mb,
            client_mem_mb=metric.client_mem_mb,
            client_uss_mb=metric.client_uss_mb,
            disk_mb=metric.disk_mb,
        )

        _LOGGER.info(f"Inserted metrics at {now}")

    while not stop_event.is_set():
        try:
            sample_once()
        except Exception:
            _LOGGER.exception("Metric sample failed; retrying on the next interval")

        if interval_seconds is not None:
            time.sleep(interval_seconds)

    try:
        sample_once(final_sample_time_queue.get())
    except Exception:
        _LOGGER.exception("Final metric sample failed")


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
    interval_seconds: float | None = 1.0,
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
            interval_seconds,
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
