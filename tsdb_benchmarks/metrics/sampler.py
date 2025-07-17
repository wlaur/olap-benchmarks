import logging
import time
from datetime import UTC, datetime
from multiprocessing import Event as create_event
from multiprocessing import Process
from multiprocessing.synchronize import Event
from queue import Queue

from ..settings import DatabaseName, Operation, SuiteName, setup_stdout_logging
from .measure import get_container_metrics
from .storage import Storage

_LOGGER = logging.getLogger(__name__)


def sampling_loop(
    name: DatabaseName,
    benchmark_id: int,
    stop_event: Event,
    queue: Queue,
    result_queue: Queue,
    interval_seconds: float | None = 1.0,
) -> None:
    setup_stdout_logging()
    storage = Storage(queue, result_queue)

    while not stop_event.is_set():
        now = datetime.now(UTC).replace(tzinfo=None)
        metric = get_container_metrics(name)

        storage.insert_metric(
            benchmark_id=benchmark_id,
            time=now,
            cpu_percent=metric.cpu_percent,
            mem_mb=metric.mem_mb,
            disk_mb=metric.disk_mb,
        )

        _LOGGER.info(f"Inserted metrics at {now}")

        if interval_seconds is not None:
            time.sleep(interval_seconds)

    finished_at = datetime.now(UTC).replace(tzinfo=None)

    storage.finish_benchmark(benchmark_id, finished_at)

    _LOGGER.info(f"Finished benchmark at {finished_at}")


def start_metric_sampler(
    suite: SuiteName,
    db: DatabaseName,
    operation: Operation,
    storage: Storage,
    interval_seconds: float | None = 1.0,
    notes: str | None = None,
) -> tuple[int, Process, Event]:
    stop_event = create_event()
    started_at = datetime.now(UTC).replace(tzinfo=None)

    benchmark_id = storage.insert_benchmark(suite=suite, db=db, operation=operation, started_at=started_at, notes=notes)

    process = Process(
        target=sampling_loop,
        args=(db, benchmark_id, stop_event, storage.queue, storage.result_queue, interval_seconds),
        daemon=True,
    )

    process.start()

    return benchmark_id, process, stop_event
