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
        else:
            expected_limit = f"{memory_gb * 1024 // 2}m"
            assert command[command.index("--memory") + 1] == expected_limit
            assert command[command.index("--memory-swap") + 1] == expected_limit
    if threads is not None:
        assert cpu_limits[DORIS_FE_CONTAINER] == pytest.approx(threads / 10)
        assert cpu_limits[DORIS_BE_CONTAINER] == pytest.approx(threads * 0.9)
        assert sum(limit for limit in cpu_limits.values() if limit is not None) == pytest.approx(threads)
    assert database.run_options["container_budget_policy"] == "combined-v1"
    assert database.run_options["container_limits"] == {
        name: {"cpu_limit": cpu, "memory_limit_mb": memory_gb * 1024 // 2 if memory_gb is not None else None}
        for name, cpu in cpu_limits.items()
    }
