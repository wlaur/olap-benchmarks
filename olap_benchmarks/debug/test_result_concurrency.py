import logging
import multiprocessing
import os
from queue import Queue

import fire  # type: ignore[import-untyped]

from ..metrics.storage import Storage, start_writer_process
from ..settings import setup_stdout_logging

_LOGGER = logging.getLogger(__name__)

setup_stdout_logging()

_LOGGER.warning("Module loading")


def func(q: Queue, rq: Queue) -> None:
    setup_stdout_logging()

    id = Storage(q, rq).debug(f"in proc {os.getpid()}")

    _LOGGER.info(f"Inserted id {id} from subprocess {os.getpid()}, {q.qsize()=}, {rq.qsize()=}")


def main(nproc: int = 10) -> None:
    _LOGGER.warning(f"Spawning {nproc:_} processes")

    _, q, rq = start_writer_process()
    s = Storage(q, rq)

    first = s.debug("first")
    _LOGGER.warning(f"Inserted first id {first}")

    processes: list[multiprocessing.Process] = []

    for idx in range(nproc):
        p = multiprocessing.Process(target=func, args=(q, rq), daemon=True)
        p.start()

        if idx % 10 == 0:
            mid = s.debug("mid")
            _LOGGER.warning(f"Inserted mid id {mid}")

        processes.append(p)

    for p in processes:
        p.join()

    last = s.debug("last")
    _LOGGER.warning(f"Inserted last id {last}")


if __name__ == "__main__":
    fire.Fire(main)
