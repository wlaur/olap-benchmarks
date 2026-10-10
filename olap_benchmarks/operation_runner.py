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
from multiprocessing import Process, Queue

from .dbs import Database, get_databases
from .metrics.storage import WriterMessage
from .settings import SETTINGS, DatabaseName, Operation, SuiteName

_LOGGER = logging.getLogger(__name__)

# Polars 2 spills out-of-core sorts (e.g. sorting ClickBench before a time-partitioned load) to the
# system temp directory under a small default disk budget and aborts the query when it runs out.
POLARS_SPILL_HEADROOM_MB = 30 * 1024


def configure_polars_spill() -> None:
    spill_directory = SETTINGS.temporary_directory / "polars"
    spill_directory.mkdir(parents=True, exist_ok=True)
    free_mb = shutil.disk_usage(spill_directory).free // (1024 * 1024)
    os.environ["POLARS_TEMP_DIR"] = spill_directory.as_posix()
    os.environ["POLARS_OOC_SPILL_DIR"] = spill_directory.as_posix()
    os.environ["POLARS_OOC_DISK_BUDGET_MB"] = str(max(free_mb - POLARS_SPILL_HEADROOM_MB, 1024))


def _operation_entrypoint(
    db_name: DatabaseName,
    suite: SuiteName,
    operation: Operation,
    scale_factor: int,
    queue: "Queue[WriterMessage]",
    result_queue: "Queue[object]",
    start_command: str | None,
    page_cache_dropped: bool,
) -> None:
    db_instance = get_databases()[db_name]
    db_instance._current_suite = suite
    db_instance._current_suite_scale_factor = scale_factor
    db_instance._last_start_command = start_command
    db_instance._page_cache_dropped = page_cache_dropped
    db_instance.set_queues(queue, result_queue)
    db_instance.benchmark(suite, operation, scale_factor=scale_factor)


def run_operation(
    db_instance: Database,
    suite: SuiteName,
    operation: Operation,
    scale_factor: int,
    queue: "Queue[WriterMessage]",
    result_queue: "Queue[object]",
) -> None:
    configure_polars_spill()
    process = Process(
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
        ),
        name=f"olap-{db_instance.name}-{suite}-{operation}",
        daemon=False,
    )
    process.start()
    process.join()

    if process.exitcode == 0:
        return

    # the child already recorded the failure and logged the traceback; surface it so a multi
    # operation run stops here exactly as an in-process exception used to
    raise RuntimeError(
        f"{operation} on {db_instance.name}/{suite} (scale factor {scale_factor}) "
        f"failed in its benchmark process (exit code {process.exitcode})"
    )
