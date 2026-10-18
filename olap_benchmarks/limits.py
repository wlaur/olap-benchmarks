import signal
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from shutil import disk_usage
from types import FrameType

from .settings import get_settings


def check_disk_space() -> None:
    settings = get_settings()
    for directory in {
        settings.database_directory,
        settings.temporary_directory,
        settings.results_directory,
        settings.input_data_directory,
    }:
        existing = directory
        while not existing.exists():
            existing = existing.parent
        free_gb = disk_usage(existing).free / 1024**3
        if free_gb < settings.minimum_free_disk_gb:
            raise OSError(
                f"Disk reserve reached at {Path(directory)}: {free_gb:.1f} GiB free; "
                f"require {settings.minimum_free_disk_gb} GiB"
            )


@contextmanager
def query_deadline(seconds: int | None) -> Generator[None]:
    if seconds is None:
        yield
        return

    def expire(_signum: int, _frame: FrameType | None) -> None:
        raise TimeoutError(f"Query exceeded {seconds} seconds")

    previous = signal.signal(signal.SIGALRM, expire)
    signal.setitimer(signal.ITIMER_REAL, seconds)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)
