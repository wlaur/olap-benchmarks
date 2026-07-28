from __future__ import annotations

import pytest

from .. import run_metadata


def test_run_metadata_records_metric_semantics(monkeypatch: pytest.MonkeyPatch) -> None:
    def no_command(_args: list[str]) -> None:
        return None

    monkeypatch.setattr(run_metadata, "_run_optional_command", no_command)

    metadata = run_metadata.build_run_metadata(
        execution_mode="container",
        start_command="docker run database",
    )

    assert metadata["metrics"] == {
        "version": 2,
        "cpu_percent": "benchmark client plus all database containers",
        "mem_mb": "all database containers; excludes the benchmark client",
        "client_mem_mb": "benchmark client process RSS",
        "disk_mb": "database storage plus configured temporary and staging directories",
    }
