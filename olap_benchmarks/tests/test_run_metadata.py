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
    )

    assert metadata["metrics"] == {
        "version": 4,
        "cpu_percent": "benchmark client plus all database containers",
        "mem_mb": "all database containers; excludes the benchmark client",
        "client_mem_mb": "benchmark client process peak RSS sampled every 10 ms",
        "client_uss_mb": "benchmark client process peak USS sampled every 100 ms",
        "disk_mb": "database storage plus configured temporary and staging directories",
    }


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
