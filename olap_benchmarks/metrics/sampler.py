from __future__ import annotations

import logging
import os
import time
from collections.abc import Sequence
from datetime import UTC, datetime
from multiprocessing import Event as create_event
from multiprocessing import Process, Queue
from multiprocessing.queues import Queue as ProcessQueue
from multiprocessing.synchronize import Event
from pathlib import Path

from ..settings import setup_stdout_logging
from .measure import get_database_metrics
from .storage import Storage, WriterMessage

_LOGGER = logging.getLogger(__name__)


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
) -> None:
    setup_stdout_logging()
    storage = Storage(queue, result_queue)

    def sample_once(sample_time: datetime | None = None) -> None:
        now = sample_time or datetime.now(UTC).replace(tzinfo=None)
        metric = get_database_metrics(client_process_id, container_names, metric_directories)

        storage.insert_metric(
            run_id=run_id,
            time=now,
            cpu_percent=metric.cpu_percent,
            mem_mb=metric.mem_mb,
            client_mem_mb=metric.client_mem_mb,
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


def start_metric_sampler(
    container_names: Sequence[str],
    metric_directories: Sequence[Path],
    run_id: int,
    storage: Storage,
    interval_seconds: float | None = 1.0,
) -> tuple[Process, Event, ProcessQueue[datetime]]:
    stop_event = create_event()
    final_sample_time_queue: ProcessQueue[datetime] = Queue(maxsize=1)

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
        ),
        daemon=False,
    )

    process.start()

    return process, stop_event, final_sample_time_queue
