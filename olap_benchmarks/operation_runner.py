"""Run a single benchmark operation in its own process.

Client memory is sampled as peak RSS of the process executing the operation, and RSS is a
high-water mark that does not fall when Python or an allocator frees memory. Running several
operations of a (db, suite) pair in one process therefore charges every later operation with the
peak of the earlier ones: a select following a multi-GB populate reports the populate plateau as
its own peak, even when the result sets are a handful of rows. Isolating each operation in a fresh
process is the only way to make the figure mean what it says, since neither resetting the peak
counter nor measuring a delta can undo a high-water mark.

The child also runs the metric sampler, so `client_mem_mb` and `client_uss_mb` are recorded against
the operation's own process rather than the orchestrating one.
"""

import contextlib
import logging
import os
import shutil
import signal
from multiprocessing import Queue, get_context
from multiprocessing.process import BaseProcess
from pathlib import Path
from time import monotonic, sleep

import psutil

from .dbs import Database, get_databases
from .limits import check_disk_space, expired_query
from .metrics.storage import WriterMessage
from .settings import DatabaseName, Operation, SuiteName, get_settings

_LOGGER = logging.getLogger(__name__)

# Polars 2 spills out-of-core sorts (e.g. sorting ClickBench before a time-partitioned load) to the
# system temp directory under a small default disk budget and aborts the query when it runs out.
POLARS_SPILL_HEADROOM_MB = 30 * 1024


def configure_polars_spill() -> None:
    spill_directory = get_settings().temporary_directory / "polars"
    spill_directory.mkdir(parents=True, exist_ok=True)
    free_mb = shutil.disk_usage(spill_directory).free // (1024 * 1024)
    os.environ["POLARS_TEMP_DIR"] = spill_directory.as_posix()
    os.environ["POLARS_OOC_SPILL_DIR"] = spill_directory.as_posix()
    reserve_mb = max(POLARS_SPILL_HEADROOM_MB, get_settings().minimum_free_disk_gb * 1024)
    if free_mb <= reserve_mb:
        raise OSError(f"Not enough free disk for Polars spill: {free_mb} MiB free, reserve {reserve_mb} MiB")
    os.environ["POLARS_OOC_DISK_BUDGET_MB"] = str(free_mb - reserve_mb)


def _operation_entrypoint(
    db_name: DatabaseName,
    suite: SuiteName,
    operation: Operation,
    scale_factor: int,
    queue: "Queue[WriterMessage]",
    result_queue: "Queue[object]",
    start_command: str | None,
    page_cache_dropped: bool,
    query_watchdog_path: Path,
) -> None:
    os.setsid()
    db_instance = get_databases()[db_name]
    db_instance._current_suite = suite
    db_instance._current_suite_scale_factor = scale_factor
    db_instance._last_start_command = start_command
    db_instance._page_cache_dropped = page_cache_dropped
    if operation == "select":
        db_instance._query_watchdog_path = query_watchdog_path
    db_instance.set_queues(queue, result_queue)
    db_instance.benchmark(suite, operation, scale_factor=scale_factor)


def _stop_operation(process: BaseProcess) -> None:
    if process.pid is None:
        return
    if process.is_alive():
        with contextlib.suppress(ProcessLookupError):
            os.kill(process.pid, signal.SIGINT)
        process.join(timeout=5)

    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    else:
        deadline = monotonic() + 3
        while monotonic() < deadline:
            process.join(timeout=0)
            try:
                os.killpg(process.pid, 0)
            except ProcessLookupError:
                break
            sleep(0.05)
        else:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)

    if process.is_alive():
        process.kill()
    process.join(timeout=5)


def run_operation(
    db_instance: Database,
    suite: SuiteName,
    operation: Operation,
    scale_factor: int,
    queue: "Queue[WriterMessage]",
    result_queue: "Queue[object]",
) -> None:
    check_disk_space()
    configure_polars_spill()
    settings = get_settings()
    query_watchdog_path = settings.temporary_directory / f"query-watchdog-{os.getpid()}.json"
    query_watchdog_path.unlink(missing_ok=True)
    process = get_context("spawn").Process(
        target=_operation_entrypoint,
        args=(
            db_instance.name,
            suite,
            operation,
            scale_factor,
            queue,
            result_queue,
            db_instance._last_start_command,
            db_instance._page_cache_dropped,
            query_watchdog_path,
        ),
        name=f"olap-{db_instance.name}-{suite}-{operation}",
        daemon=False,
    )
    process.start()
    deadline = (
        monotonic() + settings.operation_timeout_seconds if settings.operation_timeout_seconds is not None else None
    )
    try:
        while process.is_alive():
            process.join(timeout=1)
            if not process.is_alive():
                break
            check_disk_space()
            query_name = expired_query(query_watchdog_path, settings.query_timeout_seconds)
            if query_name is not None:
                raise TimeoutError(f"Query {query_name} exceeded {settings.query_timeout_seconds} seconds")
            if deadline is not None and monotonic() > deadline:
                raise TimeoutError(f"{operation} on {db_instance.name}/{suite} exceeded operation time budget")
            if settings.client_memory_limit_gb is not None and process.pid is not None:
                try:
                    rss = psutil.Process(process.pid).memory_info().rss
                except psutil.NoSuchProcess:
                    continue
                if rss > settings.client_memory_limit_gb * 1024**3:
                    raise MemoryError(f"{operation} client exceeded {settings.client_memory_limit_gb} GiB RSS")
        if process.exitcode != 0:
            raise RuntimeError(
                f"{operation} on {db_instance.name}/{suite} (scale factor {scale_factor}) "
                f"failed in its benchmark process (exit code {process.exitcode})"
            )
    except BaseException:
        _stop_operation(process)
        raise
    finally:
        query_watchdog_path.unlink(missing_ok=True)
