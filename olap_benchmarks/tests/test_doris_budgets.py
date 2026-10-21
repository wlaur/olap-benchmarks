import shlex
from pathlib import Path

import pytest

from .. import dbs
from ..dbs.doris import DORIS_BE_CONTAINER, DORIS_FE_CONTAINER, Doris
from ..settings import get_settings


@pytest.mark.parametrize("threads", [None, 1, 10, 14])
@pytest.mark.parametrize("memory_gb", [None, 1, 24, 25])
def test_doris_containers_share_one_recorded_resource_budget(
    threads: int | None, memory_gb: int | None, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = get_settings()
    monkeypatch.setattr(settings, "benchmark_threads", threads)
    monkeypatch.setattr(settings, "benchmark_memory_gb", memory_gb)
    monkeypatch.setattr(settings, "database_directory", tmp_path / "dbs")
    monkeypatch.setattr(settings, "temporary_directory", tmp_path / "temp")
    monkeypatch.setattr(dbs, "get_container_engine_platform", lambda: "linux/arm64")
    database = Doris()
    database._current_suite = "jsonbench"
    database._current_suite_scale_factor = 10
    commands = [shlex.split(command) for command in database.start_commands if command.startswith("docker run ")]
    assert len(commands) == 2
    cpu_limits: dict[str, float | None] = {}
    memory_limits: dict[str, int | None] = {}
    for command in commands:
        name = command[command.index("--name") + 1]
        if threads is None:
            assert "--cpus" not in command
            cpu_limits[name] = None
        else:
            cpu_limits[name] = float(command[command.index("--cpus") + 1])
        if memory_gb is None:
            assert "--memory" not in command
            assert "--memory-swap" not in command
            memory_limits[name] = None
        else:
            total_mb = memory_gb * 1024
            memory_limits[name] = total_mb // 4 if name == DORIS_FE_CONTAINER else total_mb - total_mb // 4
            expected_limit = f"{memory_limits[name]}m"
            assert command[command.index("--memory") + 1] == expected_limit
            assert command[command.index("--memory-swap") + 1] == expected_limit
    if threads is not None:
        assert cpu_limits[DORIS_FE_CONTAINER] == pytest.approx(threads / 10)
        assert cpu_limits[DORIS_BE_CONTAINER] == pytest.approx(threads * 0.9)
        assert sum(limit for limit in cpu_limits.values() if limit is not None) == pytest.approx(threads)
    if memory_gb is not None:
        assert sum(limit for limit in memory_limits.values() if limit is not None) == memory_gb * 1024
    assert database.run_options["container_budget_policy"] == "combined-fe25-be75-v2"
    assert database.run_options["container_limits"] == {
        name: {"cpu_limit": cpu, "memory_limit_mb": memory_limits[name]} for name, cpu in cpu_limits.items()
    }


def test_doris_24_gib_budget_and_heap_are_applied_to_the_correct_roles(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = get_settings()
    monkeypatch.setattr(settings, "benchmark_threads", 10)
    monkeypatch.setattr(settings, "benchmark_memory_gb", 24)
    monkeypatch.setattr(settings, "database_directory", tmp_path / "dbs")
    monkeypatch.setattr(settings, "temporary_directory", tmp_path / "temp")
    monkeypatch.setattr(dbs, "get_container_engine_platform", lambda: "linux/arm64")
    database = Doris()
    database._current_suite = "clickbench"
    database._current_suite_scale_factor = 1
    frontend, backend = (shlex.split(command) for command in database.start_commands[1:])
    assert frontend[frontend.index("--memory") + 1] == "6144m"
    assert frontend[frontend.index("--memory-swap") + 1] == "6144m"
    assert backend[backend.index("--memory") + 1] == "18432m"
    assert backend[backend.index("--memory-swap") + 1] == "18432m"
    assert "OLAP_DORIS_FE_HEAP_INITIAL_MB=1024" in frontend
    assert "OLAP_DORIS_FE_HEAP_MAXIMUM_MB=4096" in frontend
    assert not any("OLAP_DORIS_FE_HEAP" in argument for argument in backend)
    assert database.run_options["frontend_heap_policy"] == "fe-conf-jdk17-v1"
    assert database.run_options["frontend_heap_limits_mb"] == {"initial": 1024, "maximum": 4096}
    assert database.run_options["container_limits"] == {
        DORIS_FE_CONTAINER: {"cpu_limit": 1.0, "memory_limit_mb": 6144},
        DORIS_BE_CONTAINER: {"cpu_limit": 9.0, "memory_limit_mb": 18432},
    }


def test_doris_unlimited_memory_preserves_image_heap_defaults(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    settings = get_settings()
    monkeypatch.setattr(settings, "benchmark_memory_gb", None)
    monkeypatch.setattr(settings, "database_directory", tmp_path / "dbs")
    monkeypatch.setattr(settings, "temporary_directory", tmp_path / "temp")
    monkeypatch.setattr(dbs, "get_container_engine_platform", lambda: "linux/arm64")
    database = Doris()
    database._current_suite = "clickbench"
    database._current_suite_scale_factor = 1
    frontend = database.start_commands[1]
    assert "OLAP_DORIS_FE_HEAP" not in frontend
    assert database.run_options["frontend_heap_limits_mb"] is None
