from __future__ import annotations

import logging
import time
from collections.abc import Sequence
from datetime import UTC, datetime
from multiprocessing import Event as create_event
from multiprocessing import Process, Queue
from multiprocessing.synchronize import Event

from ..settings import DatabaseName, SuiteName, setup_stdout_logging
from .measure import get_database_metrics
from .storage import Storage, WriterMessage

_LOGGER = logging.getLogger(__name__)


def sampling_loop(
    db: DatabaseName,
    suite: SuiteName,
    suite_scale_factor: int,
    container_names: Sequence[str],
    run_id: int,
    stop_event: Event,
    queue: Queue[WriterMessage],
    result_queue: Queue[object],
    interval_seconds: float | None = 1.0,
) -> None:
    setup_stdout_logging()
    storage = Storage(queue, result_queue)

    while not stop_event.is_set():
        now = datetime.now(UTC).replace(tzinfo=None)
        metric = get_database_metrics(db, suite, suite_scale_factor, container_names)

        storage.insert_metric(
            run_id=run_id,
            time=now,
            cpu_percent=metric.cpu_percent,
            mem_mb=metric.mem_mb,
            disk_mb=metric.disk_mb,
        )

        _LOGGER.info(f"Inserted metrics at {now}")

        if interval_seconds is not None:
            time.sleep(interval_seconds)


def start_metric_sampler(
    db: DatabaseName,
    suite: SuiteName,
    suite_scale_factor: int,
    container_names: Sequence[str],
    run_id: int,
    storage: Storage,
    interval_seconds: float | None = 1.0,
) -> tuple[Process, Event]:
    stop_event = create_event()

    process = Process(
        target=sampling_loop,
        args=(
            db,
            suite,
            suite_scale_factor,
            tuple(container_names),
            run_id,
            stop_event,
            storage.queue,
            storage.result_queue,
            interval_seconds,
        ),
        daemon=False,
    )

    process.start()

    return process, stop_event
