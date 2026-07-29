import logging
import os
import platform
import subprocess
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
from pathlib import Path
from threading import Lock
from time import sleep
from typing import Any, cast

import docker
import psutil
from pydantic import BaseModel

_LOGGER = logging.getLogger(__name__)
DOCKER_API_TIMEOUT_SECONDS = 5
_CONTAINER_CPU_SNAPSHOTS: dict[str, tuple[int, int]] = {}
_CONTAINER_CPU_SNAPSHOTS_LOCK = Lock()


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


@lru_cache(maxsize=1)
def get_docker_client() -> docker.DockerClient:
    return docker.DockerClient(
        base_url=get_docker_socket(),
        timeout=DOCKER_API_TIMEOUT_SECONDS,
    )


class BenchmarkMetric(BaseModel):
    cpu_percent: float
    mem_mb: int
    client_mem_mb: int
    client_uss_mb: int
    disk_mb: int


def calculate_cpu_percent(cpu_stats: dict[str, Any], precpu_stats: dict[str, Any]) -> float:
    return calculate_cpu_percent_from_totals(
        cpu_total=cpu_stats["cpu_usage"]["total_usage"],
        previous_cpu_total=precpu_stats["cpu_usage"]["total_usage"],
        system_total=cpu_stats["system_cpu_usage"],
        previous_system_total=precpu_stats["system_cpu_usage"],
        online_cpus=cpu_stats["online_cpus"],
    )


def calculate_cpu_percent_from_totals(
    cpu_total: int,
    previous_cpu_total: int,
    system_total: int,
    previous_system_total: int,
    online_cpus: int,
) -> float:
    cpu_delta = cpu_total - previous_cpu_total
    system_delta = system_total - previous_system_total

    if cpu_delta > 0 and system_delta > 0 and online_cpus > 0:
        return (cpu_delta / system_delta) * online_cpus * 100.0

    return 0.0


def calculate_container_cpu_percent(container_name: str, stats: dict[str, Any]) -> float:
    cpu_stats = cast(dict[str, Any], stats["cpu_stats"])
    precpu_stats = cast(dict[str, Any], stats["precpu_stats"])
    if "system_cpu_usage" in precpu_stats:
        return calculate_cpu_percent(cpu_stats, precpu_stats)

    cpu_total = cast(int, cpu_stats["cpu_usage"]["total_usage"])
    system_total = cast(int, cpu_stats["system_cpu_usage"])
    online_cpus = cast(int, cpu_stats["online_cpus"])
    with _CONTAINER_CPU_SNAPSHOTS_LOCK:
        previous = _CONTAINER_CPU_SNAPSHOTS.get(container_name)
        _CONTAINER_CPU_SNAPSHOTS[container_name] = (cpu_total, system_total)

    if previous is None:
        return 0.0
    return calculate_cpu_percent_from_totals(
        cpu_total=cpu_total,
        previous_cpu_total=previous[0],
        system_total=system_total,
        previous_system_total=previous[1],
        online_cpus=online_cpus,
    )


def get_main_process_metrics(
    process_id: int,
    client_mem_mb: int | None = None,
    client_uss_mb: int | None = None,
) -> BenchmarkMetric:
    proc = psutil.Process(process_id)

    proc.cpu_percent(interval=None)  # snapshot baseline

    cpu_percent = proc.cpu_percent(interval=1.0)

    if client_mem_mb is None or client_uss_mb is None:
        full_mem_info = proc.memory_full_info()
        if client_mem_mb is None:
            client_mem_mb = int(full_mem_info.rss / (1024 * 1024))
        if client_uss_mb is None:
            client_uss_mb = int(full_mem_info.uss / (1024 * 1024))

    return BenchmarkMetric(
        cpu_percent=cpu_percent,
        mem_mb=0,
        client_mem_mb=client_mem_mb,
        client_uss_mb=client_uss_mb,
        disk_mb=0,
    )


def get_container_metrics(container_name: str) -> BenchmarkMetric:
    container = cast(Any, get_docker_client().containers).get(container_name)

    stats = cast(dict[str, Any], container.stats(stream=False, one_shot=True))

    cpu_percent = calculate_container_cpu_percent(container_name, stats)

    mem_usage = stats["memory_stats"]["usage"]
    mem_mb = int(mem_usage / (1_024 * 1_024))

    return BenchmarkMetric(cpu_percent=cpu_percent, mem_mb=mem_mb, client_mem_mb=0, client_uss_mb=0, disk_mb=0)


def get_database_metrics(
    client_process_id: int,
    container_names: Sequence[str],
    metric_directories: Sequence[Path],
    client_mem_mb: int | None = None,
    client_uss_mb: int | None = None,
) -> BenchmarkMetric:
    # The Python client can dominate ingestion memory even when the database
    # runs in a container.
    with ThreadPoolExecutor(max_workers=len(container_names) + 1) as executor:
        main_future = (
            executor.submit(get_main_process_metrics, client_process_id)
            if client_mem_mb is None and client_uss_mb is None
            else executor.submit(
                get_main_process_metrics,
                client_process_id,
                client_mem_mb,
                client_uss_mb,
            )
        )
        container_metrics = list(executor.map(get_container_metrics, container_names))
        process_metrics = [main_future.result(), *container_metrics]

    return BenchmarkMetric(
        cpu_percent=sum(metric.cpu_percent for metric in process_metrics),
        mem_mb=sum(metric.mem_mb for metric in container_metrics),
        client_mem_mb=main_future.result().client_mem_mb,
        client_uss_mb=main_future.result().client_uss_mb,
        disk_mb=sum(get_directory_size_mb(path) for path in dict.fromkeys(metric_directories)),
    )


def get_directory_size_mb(path: Path) -> int:
    n_retries = 5
    output: str | None = None

    for _ in range(n_retries):
        try:
            output = subprocess.check_output(["du", "-sk", path.resolve().as_posix()], text=True)
            break

        # can fail if the db is deleting a file a the exact same instant, e.g. with clickhouse
        # du: /Users/williamlauren/repos/olap-benchmarks/data/dbs_time_series\
        # /clickhouse/store/bb1/bb1e5c5b-913d-4009-85a8-c015e07669a3/tmp_insert_all_304_304_0: No such file or directory
        except subprocess.CalledProcessError as e:
            _LOGGER.warning(f"Call to du failed: {e}, retrying in 1 second...")
            sleep(1)
            continue

    if output is None:
        raise RuntimeError(f"Call to du failed after {n_retries:_} retries")

    kilobytes = int(output.split()[0])
    return kilobytes // 1_024
