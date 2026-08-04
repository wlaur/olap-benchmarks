import logging
import os
import platform
import subprocess
from collections.abc import Sequence
from concurrent.futures import Executor
from functools import lru_cache
from pathlib import Path
from threading import Lock
from typing import Any, Protocol, cast

import docker
import psutil
from docker.errors import DockerException
from pydantic import BaseModel

_LOGGER = logging.getLogger(__name__)
DOCKER_API_TIMEOUT_SECONDS = 5
DIRECTORY_SIZE_ATTEMPTS = 3


class ContainerStatsSource(Protocol):
    def stats(self, stream: bool = False, one_shot: bool = False) -> dict[str, Any]: ...


_CONTAINER_CPU_SNAPSHOTS: dict[str, tuple[int, int]] = {}
_CONTAINER_CPU_SNAPSHOTS_LOCK = Lock()
_CONTAINERS: dict[str, ContainerStatsSource] = {}
_CONTAINERS_LOCK = Lock()
_CLIENT_PROCESSES: dict[int, psutil.Process] = {}
_CLIENT_PROCESSES_LOCK = Lock()


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


class ResourceSample(BaseModel):
    # one source's share in the per-source samplers, the client-plus-containers
    # total in the aggregate returned by get_resource_sample
    cpu_percent: float
    # database-container memory only; always 0 for in-process engines, which have no server
    server_mem_mb: int
    # benchmark client process memory; the real footprint of an in-process engine
    client_mem_mb: int
    client_uss_mb: int


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


def get_client_process(process_id: int) -> psutil.Process:
    with _CLIENT_PROCESSES_LOCK:
        process = _CLIENT_PROCESSES.get(process_id)
        if process is None:
            process = psutil.Process(process_id)
            # psutil measures CPU against the previous call on the same object, so the
            # first call only primes the baseline. Reusing the object is what lets the
            # sampler read CPU without blocking for a fixed measurement window.
            process.cpu_percent(interval=None)
            _CLIENT_PROCESSES[process_id] = process
        return process


def get_client_process_metrics(
    process_id: int,
    client_mem_mb: int | None = None,
    client_uss_mb: int | None = None,
) -> ResourceSample:
    process = get_client_process(process_id)

    cpu_percent = process.cpu_percent(interval=None)

    if client_mem_mb is None or client_uss_mb is None:
        full_mem_info = process.memory_full_info()
        if client_mem_mb is None:
            client_mem_mb = int(full_mem_info.rss / (1024 * 1024))
        if client_uss_mb is None:
            client_uss_mb = int(full_mem_info.uss / (1024 * 1024))

    return ResourceSample(
        cpu_percent=cpu_percent,
        server_mem_mb=0,
        client_mem_mb=client_mem_mb,
        client_uss_mb=client_uss_mb,
    )


def get_container(container_name: str) -> ContainerStatsSource:
    with _CONTAINERS_LOCK:
        container = _CONTAINERS.get(container_name)

    if container is None:
        container = cast(ContainerStatsSource, cast(Any, get_docker_client().containers).get(container_name))
        with _CONTAINERS_LOCK:
            _CONTAINERS[container_name] = container

    return container


def forget_container(container_name: str) -> None:
    with _CONTAINERS_LOCK:
        _CONTAINERS.pop(container_name, None)


def get_container_stats(container_name: str) -> dict[str, Any]:
    try:
        return get_container(container_name).stats(stream=False, one_shot=True)
    except DockerException:
        # a recreated container keeps its name but not its id, so drop the stale
        # handle and look the container up again before giving up on this sample
        forget_container(container_name)
        return get_container(container_name).stats(stream=False, one_shot=True)


def get_container_metrics(container_name: str) -> ResourceSample:
    stats = get_container_stats(container_name)

    cpu_percent = calculate_container_cpu_percent(container_name, stats)

    mem_usage = stats["memory_stats"]["usage"]
    server_mem_mb = int(mem_usage / (1_024 * 1_024))

    return ResourceSample(
        cpu_percent=cpu_percent,
        server_mem_mb=server_mem_mb,
        client_mem_mb=0,
        client_uss_mb=0,
    )


def get_resource_sample(
    executor: Executor,
    client_process_id: int,
    container_names: Sequence[str],
    client_mem_mb: int | None = None,
    client_uss_mb: int | None = None,
) -> ResourceSample:
    # The Python client can dominate ingestion memory even when the database
    # runs in a container, so server and client memory stay separate series.
    client_future = executor.submit(get_client_process_metrics, client_process_id, client_mem_mb, client_uss_mb)
    container_futures = [executor.submit(get_container_metrics, name) for name in container_names]

    client_metrics = client_future.result()
    container_metrics = [future.result() for future in container_futures]

    return ResourceSample(
        cpu_percent=client_metrics.cpu_percent + sum(metric.cpu_percent for metric in container_metrics),
        server_mem_mb=sum(metric.server_mem_mb for metric in container_metrics),
        client_mem_mb=client_metrics.client_mem_mb,
        client_uss_mb=client_metrics.client_uss_mb,
    )


def get_disk_sample(metric_directories: Sequence[Path]) -> int:
    return sum(get_directory_size_mb(path) for path in dict.fromkeys(metric_directories))


def get_directory_size_mb(path: Path) -> int:
    last_error: subprocess.CalledProcessError | None = None

    for _ in range(DIRECTORY_SIZE_ATTEMPTS):
        try:
            output = subprocess.check_output(["du", "-sk", path.resolve().as_posix()], text=True)

        # can fail if the db is deleting a file a the exact same instant, e.g. with clickhouse
        # du: /Users/williamlauren/repos/olap-benchmarks/data/dbs_time_series\
        # /clickhouse/store/bb1/bb1e5c5b-913d-4009-85a8-c015e07669a3/tmp_insert_all_304_304_0: No such file or directory
        # the retry restarts the walk, by which point the file is gone for good
        except subprocess.CalledProcessError as error:
            _LOGGER.warning(f"Call to du failed: {error}, retrying...")
            last_error = error
            continue

        kilobytes = int(output.split()[0])
        return kilobytes // 1_024

    raise RuntimeError(f"Call to du failed after {DIRECTORY_SIZE_ATTEMPTS} attempts") from last_error
