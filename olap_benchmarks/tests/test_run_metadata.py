from __future__ import annotations

from pathlib import Path
from typing import cast

import pytest

from .. import run_metadata


def test_run_metadata_records_metric_semantics(monkeypatch: pytest.MonkeyPatch) -> None:
    def no_command(_args: list[str]) -> None:
        return None

    monkeypatch.setattr(run_metadata, "_run_optional_command", no_command)

    metadata = run_metadata.build_run_metadata(
        execution_mode="container",
        start_command="docker run database",
        sampling_interval_seconds={"resource": 0.2, "disk": 5.0},
    )

    metrics = cast(dict[str, object], metadata["metrics"])

    assert metrics["version"] == 6
    assert set(metrics) == {
        "version",
        "sampling",
        "sampling_interval_seconds",
        "cpu_percent",
        "server_mem_mb",
        "client_mem_mb",
        "client_uss_mb",
        "disk_mb",
        "comparable_memory",
    }
    assert metrics["sampling_interval_seconds"] == {"resource": 0.2, "disk": 5.0}
    assert "mem_mb" not in metrics
    assert cast(str, metrics["server_mem_mb"]).startswith("server side:")
    assert cast(str, metrics["client_mem_mb"]).startswith("client side:")
    assert cast(str, metrics["client_uss_mb"]).startswith("client side:")
    assert cast(str, metrics["disk_mb"]).startswith("server side:")
    assert cast(str, metrics["cpu_percent"]).startswith("combined:")


def test_input_fingerprint_is_complete_and_changes_with_content(tmp_path: Path) -> None:
    nested = tmp_path / "nested"
    nested.mkdir()
    (tmp_path / "a.parquet").write_bytes(b"first")
    (nested / "b.parquet").write_bytes(b"second")

    first = run_metadata.fingerprint_input_directory(tmp_path)
    second = run_metadata.fingerprint_input_directory(tmp_path)

    assert first is second
    files = cast(list[dict[str, object]], first["files"])
    assert [file["path"] for file in files] == ["a.parquet", "nested/b.parquet"]

    (tmp_path / "a.parquet").write_bytes(b"changed")
    changed = run_metadata.fingerprint_input_directory(tmp_path)

    assert changed["manifest_sha256"] != first["manifest_sha256"]
