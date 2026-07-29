from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from ..metrics import measure


class FakeContainer:
    def __init__(self, stats: dict[str, Any]) -> None:
        self._stats = stats

    def stats(self, stream: bool = False, one_shot: bool = False) -> dict[str, Any]:
        assert stream is False
        assert one_shot is True
        return self._stats


class FakeContainers:
    def __init__(self, stats_by_name: dict[str, dict[str, Any]]) -> None:
        self._stats_by_name = stats_by_name

    def get(self, name: str) -> FakeContainer:
        return FakeContainer(self._stats_by_name[name])


class FakeDockerClient:
    def __init__(self, stats_by_name: dict[str, dict[str, Any]]) -> None:
        self.containers = FakeContainers(stats_by_name)


def docker_stats(cpu_delta: int, system_delta: int, online_cpus: int, mem_mb: int) -> dict[str, Any]:
    return {
        "cpu_stats": {
            "cpu_usage": {"total_usage": 1_000 + cpu_delta},
            "system_cpu_usage": 10_000 + system_delta,
            "online_cpus": online_cpus,
        },
        "precpu_stats": {
            "cpu_usage": {"total_usage": 1_000},
            "system_cpu_usage": 10_000,
        },
        "memory_stats": {"usage": mem_mb * 1_024 * 1_024},
    }


def test_database_metrics_sum_client_containers_and_all_storage(monkeypatch: pytest.MonkeyPatch) -> None:
    client = FakeDockerClient(
        {
            "doris-fe-benchmark": docker_stats(cpu_delta=20, system_delta=100, online_cpus=4, mem_mb=256),
            "doris-be-benchmark": docker_stats(cpu_delta=10, system_delta=100, online_cpus=4, mem_mb=512),
        }
    )

    def main_process_metrics(_process_id: int) -> measure.BenchmarkMetric:
        return measure.BenchmarkMetric(
            cpu_percent=25,
            mem_mb=0,
            client_mem_mb=128,
            client_uss_mb=96,
            disk_mb=0,
        )

    monkeypatch.setattr(measure, "get_docker_client", lambda: client)
    monkeypatch.setattr(measure, "get_main_process_metrics", main_process_metrics)
    sizes = {Path("/database"): 123, Path("/temporary"): 45}
    monkeypatch.setattr(measure, "get_directory_size_mb", sizes.__getitem__)

    metric = measure.get_database_metrics(
        42,
        ("doris-fe-benchmark", "doris-be-benchmark"),
        (Path("/database"), Path("/temporary")),
    )

    assert metric.cpu_percent == 145.0
    assert metric.mem_mb == 768
    assert metric.client_mem_mb == 128
    assert metric.client_uss_mb == 96
    assert metric.disk_mb == 168


def test_database_metrics_include_client_without_containers(monkeypatch: pytest.MonkeyPatch) -> None:
    def main_process_metrics(_process_id: int) -> measure.BenchmarkMetric:
        return measure.BenchmarkMetric(
            cpu_percent=25,
            mem_mb=0,
            client_mem_mb=128,
            client_uss_mb=96,
            disk_mb=0,
        )

    monkeypatch.setattr(measure, "get_main_process_metrics", main_process_metrics)

    def directory_size(_path: Path) -> int:
        return 10

    monkeypatch.setattr(measure, "get_directory_size_mb", directory_size)

    metric = measure.get_database_metrics(42, (), (Path("/database"), Path("/database")))

    assert metric == measure.BenchmarkMetric(
        cpu_percent=25,
        mem_mb=0,
        client_mem_mb=128,
        client_uss_mb=96,
        disk_mb=10,
    )


def test_database_metrics_use_high_frequency_client_memory_peaks(monkeypatch: pytest.MonkeyPatch) -> None:
    def main_process_metrics(
        _process_id: int,
        client_mem_mb: int,
        client_uss_mb: int,
    ) -> measure.BenchmarkMetric:
        return measure.BenchmarkMetric(
            cpu_percent=25,
            mem_mb=0,
            client_mem_mb=client_mem_mb,
            client_uss_mb=client_uss_mb,
            disk_mb=0,
        )

    monkeypatch.setattr(measure, "get_main_process_metrics", main_process_metrics)

    metric = measure.get_database_metrics(42, (), (), 512, 384)

    assert metric.client_mem_mb == 512
    assert metric.client_uss_mb == 384


def test_one_shot_container_cpu_uses_consecutive_samples() -> None:
    measure._CONTAINER_CPU_SNAPSHOTS.clear()
    first = docker_stats(cpu_delta=20, system_delta=100, online_cpus=4, mem_mb=256)
    second = docker_stats(cpu_delta=40, system_delta=200, online_cpus=4, mem_mb=256)
    first["precpu_stats"].pop("system_cpu_usage")
    second["precpu_stats"].pop("system_cpu_usage")

    assert measure.calculate_container_cpu_percent("database", first) == 0.0
    assert measure.calculate_container_cpu_percent("database", second) == 80.0
