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
from multiprocessing import Process, Queue

from .dbs import Database, get_databases
from .metrics.storage import WriterMessage
from .settings import DatabaseName, Operation, SuiteName

_LOGGER = logging.getLogger(__name__)


def _operation_entrypoint(
    db_name: DatabaseName,
    suite: SuiteName,
    operation: Operation,
    scale_factor: int,
    queue: "Queue[WriterMessage]",
    result_queue: "Queue[object]",
    start_command: str | None,
) -> None:
    db_instance = get_databases()[db_name]
    db_instance._current_suite = suite
    db_instance._current_suite_scale_factor = scale_factor
    db_instance._last_start_command = start_command
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
