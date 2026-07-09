from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from ..metrics import measure


class FakeContainer:
    def __init__(self, stats: dict[str, Any]) -> None:
        self._stats = stats

    def stats(self, stream: bool = False) -> dict[str, Any]:
        assert stream is False
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


def fake_directory_size_mb(path: Path) -> int:
    _ = path
    return 123


def test_database_metrics_sum_multiple_container_cpu_and_memory(monkeypatch: pytest.MonkeyPatch) -> None:
    client = FakeDockerClient(
        {
            "doris-fe-benchmark": docker_stats(cpu_delta=20, system_delta=100, online_cpus=4, mem_mb=256),
            "doris-be-benchmark": docker_stats(cpu_delta=10, system_delta=100, online_cpus=4, mem_mb=512),
        }
    )

    monkeypatch.setattr(measure, "get_docker_client", lambda: client)
    monkeypatch.setattr(measure, "get_directory_size_mb", fake_directory_size_mb)

    metric = measure.get_database_metrics(
        "doris",
        "jsonbench",
        10,
        ("doris-fe-benchmark", "doris-be-benchmark"),
    )

    assert metric.cpu_percent == 120.0
    assert metric.mem_mb == 768
    assert metric.disk_mb == 123
