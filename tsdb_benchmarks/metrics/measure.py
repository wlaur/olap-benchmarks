import subprocess
from pathlib import Path

import docker
from pydantic import BaseModel

from ..settings import SETTINGS, DatabaseName

client = docker.from_env()


class BenchmarkMetric(BaseModel):
    cpu_percent: float
    mem_mb: int
    disk_mb: int


def get_container_name(name: DatabaseName) -> str:
    return f"{name}-benchmark"


def get_database_directory(name: DatabaseName) -> Path:
    return SETTINGS.database_directory / name


def get_container_metrics(name: DatabaseName) -> BenchmarkMetric:
    container = client.containers.get(get_container_name(name))
    stats = container.stats(stream=False)

    cpu_delta = stats["cpu_stats"]["cpu_usage"]["total_usage"] - stats["precpu_stats"]["cpu_usage"]["total_usage"]
    system_delta = stats["cpu_stats"]["system_cpu_usage"] - stats["precpu_stats"]["system_cpu_usage"]
    num_cpus = len(stats["cpu_stats"]["cpu_usage"].get("percpu_usage", []))

    cpu_percent = cpu_delta / system_delta * num_cpus * 100.0 if system_delta > 0.0 and cpu_delta > 0.0 else 0.0

    mem_usage = stats["memory_stats"]["usage"]
    mem_mb = int(mem_usage / (1_024 * 1_024))

    return BenchmarkMetric(
        cpu_percent=cpu_percent,
        mem_mb=mem_mb,
        disk_mb=get_directory_size_mb(get_database_directory(name)),
    )


def get_directory_size_mb(path: Path) -> int:
    output = subprocess.check_output(["du", "-sk", path.resolve().as_posix()], text=True)
    kilobytes = int(output.split()[0])
    return kilobytes // 1_024
