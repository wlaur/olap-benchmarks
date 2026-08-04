from __future__ import annotations

import subprocess
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from docker.errors import NotFound

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


def client_sample(
    _process_id: int,
    client_mem_mb: int | None = None,
    client_uss_mb: int | None = None,
) -> measure.ResourceSample:
    return measure.ResourceSample(
        cpu_percent=25,
        server_mem_mb=0,
        client_mem_mb=128 if client_mem_mb is None else client_mem_mb,
        client_uss_mb=96 if client_uss_mb is None else client_uss_mb,
    )


def test_resource_sample_sums_client_and_containers(monkeypatch: pytest.MonkeyPatch) -> None:
    measure._CONTAINERS.clear()
    measure._CONTAINER_CPU_SNAPSHOTS.clear()
    client = FakeDockerClient(
        {
            "doris-fe-benchmark": docker_stats(cpu_delta=20, system_delta=100, online_cpus=4, mem_mb=256),
            "doris-be-benchmark": docker_stats(cpu_delta=10, system_delta=100, online_cpus=4, mem_mb=512),
        }
    )

    monkeypatch.setattr(measure, "get_docker_client", lambda: client)
    monkeypatch.setattr(measure, "get_client_process_metrics", client_sample)

    with ThreadPoolExecutor(max_workers=3) as executor:
        sample = measure.get_resource_sample(executor, 42, ("doris-fe-benchmark", "doris-be-benchmark"))

    assert sample.cpu_percent == 145.0
    assert sample.server_mem_mb == 768
    assert sample.client_mem_mb == 128
    assert sample.client_uss_mb == 96


def test_resource_sample_includes_client_without_containers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(measure, "get_client_process_metrics", client_sample)

    with ThreadPoolExecutor(max_workers=1) as executor:
        sample = measure.get_resource_sample(executor, 42, ())

    assert sample == measure.ResourceSample(cpu_percent=25, server_mem_mb=0, client_mem_mb=128, client_uss_mb=96)


def test_resource_sample_uses_high_frequency_client_memory_peaks(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(measure, "get_client_process_metrics", client_sample)

    with ThreadPoolExecutor(max_workers=1) as executor:
        sample = measure.get_resource_sample(executor, 42, (), 512, 384)

    assert sample.client_mem_mb == 512
    assert sample.client_uss_mb == 384


def test_client_cpu_is_read_without_blocking_for_a_measurement_window(monkeypatch: pytest.MonkeyPatch) -> None:
    intervals: list[float | None] = []

    class FakeProcess:
        def __init__(self, _process_id: int) -> None:
            pass

        def cpu_percent(self, interval: float | None = None) -> float:
            intervals.append(interval)
            return 42.0

        def memory_full_info(self) -> SimpleNamespace:
            return SimpleNamespace(rss=128 * 1024 * 1024, uss=96 * 1024 * 1024)

    measure._CLIENT_PROCESSES.clear()
    monkeypatch.setattr(measure.psutil, "Process", FakeProcess)

    first = measure.get_client_process_metrics(42)
    second = measure.get_client_process_metrics(42)

    # one priming call, then one read per sample, none of which blocks
    assert intervals == [None, None, None]
    assert first.cpu_percent == 42.0
    assert second.cpu_percent == 42.0


def test_client_process_object_is_reused_so_cpu_deltas_accumulate(monkeypatch: pytest.MonkeyPatch) -> None:
    constructions = 0

    class FakeProcess:
        def __init__(self, _process_id: int) -> None:
            nonlocal constructions
            constructions += 1

        def cpu_percent(self, interval: float | None = None) -> float:
            return 1.0

        def memory_full_info(self) -> SimpleNamespace:
            return SimpleNamespace(rss=0, uss=0)

    measure._CLIENT_PROCESSES.clear()
    monkeypatch.setattr(measure.psutil, "Process", FakeProcess)

    measure.get_client_process_metrics(42)
    measure.get_client_process_metrics(42)

    assert constructions == 1


def test_container_stats_refreshes_a_stale_container_handle(monkeypatch: pytest.MonkeyPatch) -> None:
    measure._CONTAINERS.clear()
    stats = docker_stats(cpu_delta=20, system_delta=100, online_cpus=4, mem_mb=256)
    lookups = 0

    class StaleContainer:
        def stats(self, stream: bool = False, one_shot: bool = False) -> dict[str, Any]:
            raise NotFound("no such container")

    class Containers:
        def get(self, _name: str) -> object:
            nonlocal lookups
            lookups += 1
            return StaleContainer() if lookups == 1 else FakeContainer(stats)

    monkeypatch.setattr(measure, "get_docker_client", lambda: SimpleNamespace(containers=Containers()))

    assert measure.get_container_stats("database") == stats
    assert lookups == 2


def test_disk_sample_sums_deduplicated_directories(monkeypatch: pytest.MonkeyPatch) -> None:
    sizes = {Path("/database"): 123, Path("/temporary"): 45}
    monkeypatch.setattr(measure, "get_directory_size_mb", sizes.__getitem__)

    assert measure.get_disk_sample((Path("/database"), Path("/temporary"), Path("/database"))) == 168


def test_directory_size_retries_after_a_transient_du_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = 0

    def check_output(args: list[str], text: bool) -> str:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise subprocess.CalledProcessError(1, args)
        return "2048\t/database\n"

    monkeypatch.setattr(measure.subprocess, "check_output", check_output)

    assert measure.get_directory_size_mb(Path("/database")) == 2
    assert calls == 2


def test_directory_size_raises_once_the_retries_are_exhausted(monkeypatch: pytest.MonkeyPatch) -> None:
    def check_output(args: list[str], text: bool) -> str:
        raise subprocess.CalledProcessError(1, args)

    monkeypatch.setattr(measure.subprocess, "check_output", check_output)

    with pytest.raises(RuntimeError, match="du failed after"):
        measure.get_directory_size_mb(Path("/database"))


def test_one_shot_container_cpu_uses_consecutive_samples() -> None:
    measure._CONTAINER_CPU_SNAPSHOTS.clear()
    first = docker_stats(cpu_delta=20, system_delta=100, online_cpus=4, mem_mb=256)
    second = docker_stats(cpu_delta=40, system_delta=200, online_cpus=4, mem_mb=256)
    first["precpu_stats"].pop("system_cpu_usage")
    second["precpu_stats"].pop("system_cpu_usage")

    assert measure.calculate_container_cpu_percent("database", first) == 0.0
    assert measure.calculate_container_cpu_percent("database", second) == 80.0


def test_restarting_container_reports_no_cpu_instead_of_raising() -> None:
    # the suites restart the database between operations, and docker reports no CPU accounting at
    # all while a container is coming up; raising here loses the whole sample, not just this column
    measure._CONTAINER_CPU_SNAPSHOTS.clear()
    stats = docker_stats(cpu_delta=20, system_delta=100, online_cpus=4, mem_mb=256)
    stats["precpu_stats"].pop("system_cpu_usage")
    stats["cpu_stats"].pop("system_cpu_usage")

    assert measure.calculate_container_cpu_percent("database", stats) == 0.0
