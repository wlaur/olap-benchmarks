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

import logging
import os
import shutil
import signal
from multiprocessing import Queue, get_context
from multiprocessing.process import BaseProcess
from pathlib import Path
from tempfile import mkdtemp
from time import monotonic, sleep

import psutil

from .analytical_streams import AnalyticalConfig, validate_analytical_config
from .dbs import Database, get_databases
from .limits import check_disk_space, expired_query
from .metrics.storage import WriterMessage
from .settings import DatabaseName, Operation, SuiteName, get_settings

_LOGGER = logging.getLogger(__name__)

SELECT_QUERY_CANCELLATION_GRACE_SECONDS = 30

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
    analytical: AnalyticalConfig | None = None,
) -> None:
    os.setsid()
    db_instance = get_databases()[db_name]
    db_instance._current_suite = suite
    db_instance._current_suite_scale_factor = scale_factor
    db_instance._last_start_command = start_command
    db_instance._page_cache_dropped = page_cache_dropped
    if operation in ("select", "analytical"):
        db_instance._query_watchdog_path = query_watchdog_path
    db_instance._analytical_config = analytical
    db_instance.set_queues(queue, result_queue)
    try:
        db_instance.benchmark(suite, operation, scale_factor=scale_factor)
    finally:
        if operation == "analytical":
            try:
                db_instance.close_connection()
            finally:
                if db_instance.name == "clickhouse":
                    db_instance.reset_session()


def _stop_operation(process: BaseProcess) -> None:
    pid = process.pid
    if pid is None:
        return
    try:
        if process.is_alive():
            try:
                os.kill(pid, signal.SIGINT)
            except ProcessLookupError:
                pass
            except PermissionError:
                _LOGGER.warning("Permission denied interrupting operation process %s", pid, exc_info=True)
            process.join(timeout=5)

        try:
            os.killpg(pid, signal.SIGTERM)
        except ProcessLookupError:
            return
        except PermissionError:
            _LOGGER.warning("Permission denied terminating operation process group %s", pid, exc_info=True)
        else:
            deadline = monotonic() + 3
            while monotonic() < deadline:
                process.join(timeout=0)
                try:
                    os.killpg(pid, 0)
                except ProcessLookupError:
                    return
                except PermissionError:
                    _LOGGER.warning(
                        "Permission denied probing operation process group %s; cleanup is unconfirmed",
                        pid,
                        exc_info=True,
                    )
                    break
                sleep(0.05)
        try:
            os.killpg(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        except PermissionError:
            _LOGGER.warning(
                "Permission denied killing operation process group %s; cleanup is unconfirmed", pid, exc_info=True
            )
    finally:
        try:
            if process.is_alive():
                try:
                    process.kill()
                except ProcessLookupError:
                    pass
                except PermissionError:
                    _LOGGER.warning("Permission denied killing operation process %s", pid, exc_info=True)
        finally:
            process.join(timeout=5)
            if process.is_alive():
                _LOGGER.error("Operation process %s remains alive after cleanup", pid)


def run_operation(
    db_instance: Database,
    suite: SuiteName,
    operation: Operation,
    scale_factor: int,
    queue: "Queue[WriterMessage]",
    result_queue: "Queue[object]",
    *,
    analytical: AnalyticalConfig | None = None,
) -> None:
    if operation == "analytical":
        if analytical is None:
            raise ValueError("Analytical operation needs explicit stream configuration")
        validate_analytical_config(db_instance, analytical)
    elif analytical is not None:
        raise ValueError("Stream configuration is only valid for the analytical operation")
    check_disk_space()
    configure_polars_spill()
    settings = get_settings()
    query_watchdog_timeout = settings.query_timeout_seconds
    cancellation_grace = SELECT_QUERY_CANCELLATION_GRACE_SECONDS if operation == "select" else 0
    if query_watchdog_timeout is not None:
        query_watchdog_timeout += cancellation_grace
    query_watchdog_path = settings.temporary_directory / f"query-watchdog-{os.getpid()}.json"
    query_watchdog_path.unlink(missing_ok=True)
    if operation == "analytical":
        query_watchdog_path = Path(
            mkdtemp(prefix=f"analytical-watchdog-{os.getpid()}-", dir=settings.temporary_directory)
        )
    process: BaseProcess | None = None
    deadline = (
        monotonic() + settings.operation_timeout_seconds if settings.operation_timeout_seconds is not None else None
    )
    try:
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
                analytical,
            ),
            name=f"olap-{db_instance.name}-{suite}-{operation}",
            daemon=False,
        )
        process.start()
        while process.is_alive():
            process.join(timeout=1)
            if not process.is_alive():
                break
            check_disk_space()
            watchdogs = (
                query_watchdog_path.glob("client-*.json") if operation == "analytical" else (query_watchdog_path,)
            )
            for watchdog in watchdogs:
                query_name = expired_query(watchdog, query_watchdog_timeout)
                if query_name is not None:
                    message = f"Query {query_name} exceeded {settings.query_timeout_seconds} seconds"
                    if cancellation_grace:
                        message += f" and {cancellation_grace} seconds cancellation grace"
                    raise TimeoutError(message)
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
        if process is not None:
            try:
                _stop_operation(process)
            except BaseException:
                _LOGGER.exception("Failed to clean up benchmark operation process %s", process.pid)
        raise
    finally:
        if operation == "analytical":
            shutil.rmtree(query_watchdog_path)
        else:
            query_watchdog_path.unlink(missing_ok=True)
