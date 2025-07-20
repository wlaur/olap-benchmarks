import logging
import os
import platform
import subprocess
from pathlib import Path
from time import sleep

import docker
import psutil
from pydantic import BaseModel

from ..settings import MAIN_PROCESS_TITLE, SETTINGS, DatabaseName

_LOGGER = logging.getLogger(__name__)

IN_PROCESS_DBS: list[DatabaseName] = ["duckdb"]


def get_docker_socket() -> str:
    if "DOCKER_HOST" in os.environ:
        return os.environ["DOCKER_HOST"]

    system = platform.system()

    if system == "Darwin":
        # if using orbstack on macos
        try:
            result = subprocess.check_output(
                ["docker", "context", "inspect", "orbstack", "--format", "{{json .Endpoints.docker.Host}}"],
                stderr=subprocess.DEVNULL,
                text=True,
            ).strip('" \n')

            if result.startswith("unix://"):
                return result

        except (subprocess.CalledProcessError, FileNotFoundError):
            pass

    if system == "Linux":
        default_socket = Path("/var/run/docker.sock")
        if default_socket.exists():
            return f"unix://{default_socket.as_posix()}"

    raise RuntimeError(
        "Could not determine Docker socket path. "
        "Set environment variable DOCKER_HOST or ensure Docker is installed and running."
    )


DOCKER_CLIENT = docker.DockerClient(base_url=get_docker_socket())


class BenchmarkMetric(BaseModel):
    cpu_percent: float
    mem_mb: int
    disk_mb: int


def get_container_name(db: DatabaseName) -> str:
    return f"{db}-benchmark"


def get_database_directory(db: DatabaseName) -> Path:
    return SETTINGS.database_directory / db


def calculate_cpu_percent(cpu_stats: dict, precpu_stats: dict) -> float:
    cpu_delta = cpu_stats["cpu_usage"]["total_usage"] - precpu_stats["cpu_usage"]["total_usage"]
    system_delta = cpu_stats["system_cpu_usage"] - precpu_stats["system_cpu_usage"]
    online_cpus = cpu_stats["online_cpus"]

    if cpu_delta > 0 and system_delta > 0 and online_cpus > 0:
        return (cpu_delta / system_delta) * online_cpus * 100.0

    return 0.0


def find_main_process() -> psutil.Process:
    # this process title is set in __main__.py, so the result writer subprocess will not have it
    for proc in psutil.process_iter(attrs=["pid", "name", "cmdline"]):
        try:
            if proc.info["cmdline"] is not None and MAIN_PROCESS_TITLE in " ".join(proc.info["cmdline"]):
                return proc
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue

    raise RuntimeError(f"Process with title '{MAIN_PROCESS_TITLE}' not found")


def get_main_process_metrics(db: DatabaseName) -> BenchmarkMetric:
    proc = find_main_process()

    proc.cpu_percent(interval=None)  # snapshot baseline

    cpu_percent = proc.cpu_percent(interval=1.0)

    mem_info = proc.memory_info()
    mem_mb = int(mem_info.rss / (1024 * 1024))

    return BenchmarkMetric(
        cpu_percent=cpu_percent, mem_mb=mem_mb, disk_mb=get_directory_size_mb(get_database_directory(db))
    )


def get_container_metrics(db: DatabaseName) -> BenchmarkMetric:
    if db in IN_PROCESS_DBS:
        # contains potentially significant overhead from e.g. the insert methods
        # using docker stats only shows the resource usage from the database itself, not the main process that
        # reads and processes input Parquet files
        return get_main_process_metrics(db)

    container = DOCKER_CLIENT.containers.get(get_container_name(db))

    # this takes around ~1 sec, needs to collect cpu data before and after a sampling period of 1 second
    stats = container.stats(stream=False)

    try:
        cpu_percent = calculate_cpu_percent(stats["cpu_stats"], stats["precpu_stats"])
    except KeyError as e:
        _LOGGER.warning(f"docker stats output invalid (KeyError: {e}): {stats}, sleeping and retrying...")
        sleep(1)
        return get_container_metrics(db)

    mem_usage = stats["memory_stats"]["usage"]
    mem_mb = int(mem_usage / (1_024 * 1_024))

    return BenchmarkMetric(
        cpu_percent=cpu_percent,
        mem_mb=mem_mb,
        disk_mb=get_directory_size_mb(get_database_directory(db)),
    )


def get_directory_size_mb(path: Path) -> int:
    n_retries = 5
    output: str | None = None

    for _ in range(n_retries):
        try:
            output = subprocess.check_output(["du", "-sk", path.resolve().as_posix()], text=True)
            break

        # can fail if the db is deleting a file a the exact same instant, e.g. with clickhouse
        # du: /Users/williamlauren/repos/tsdb-benchmarks/data/dbs_time_series\
        # /clickhouse/store/bb1/bb1e5c5b-913d-4009-85a8-c015e07669a3/tmp_insert_all_304_304_0: No such file or directory
        except subprocess.CalledProcessError as e:
            _LOGGER.warning(f"Call to du failed: {e}, retrying in 1 second...")
            sleep(1)
            continue

    if output is None:
        raise RuntimeError(f"Call to du failed after {n_retries:_} retries")

    kilobytes = int(output.split()[0])
    return kilobytes // 1_024
