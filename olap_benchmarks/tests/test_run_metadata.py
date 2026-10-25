from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import cast

import pytest

from .. import run_metadata
from ..inputs import fingerprint_input_directory


@pytest.mark.parametrize(
    ("error_type", "message", "expected"),
    [
        ("TimeoutError", "Query exceeded 300 seconds", "timeout"),
        ("TimeoutExpired", "process deadline", "timeout"),
        ("RuntimeError", "db error: ERROR: timeout, query aborted [fd=1, runtime=60332ms, timeout=60000ms]", "timeout"),
        ("RuntimeError", "db error: ERROR: canceling statement due to statement timeout", "timeout"),
        ("OperationalError", "(psycopg2.errors.QueryCanceled) canceling statement due to statement timeout", "timeout"),
        ("QueryCanceled", "canceling statement due to user request", "error"),
        ("RuntimeError", "cancelled by user", "error"),
        ("OperationalError", "SQLSTATE 57014: query canceled", "error"),
        ("QueryCanceled", None, "error"),
        ("RuntimeError", "connection timeout", "error"),
        ("OperationalError", "canceling statement due to lock timeout", "error"),
        ("OperationalError", "syntax error\n[SQL: SELECT 'timeout, query aborted']", "error"),
        ("OperationalError", 'syntax error at or near "timeout, query aborted"', "error"),
        ("RuntimeError", "query aborted", "error"),
        ("OperationalError", "canceling statement due to user request\n[SQL: SELECT 'statement timeout']", "error"),
    ],
)
def test_step_timeout_classification_requires_deadline_evidence(
    error_type: str, message: str | None, expected: str
) -> None:
    assert (
        run_metadata.classify_step_result_status(
            step_type="query", status="failed", error_type=error_type, error_message=message
        )
        == expected
    )


def test_run_metadata_records_metric_semantics(monkeypatch: pytest.MonkeyPatch) -> None:
    def no_command(_args: list[str]) -> None:
        return None

    monkeypatch.setattr(run_metadata, "_run_optional_command", no_command)

    metadata = run_metadata.build_run_metadata(
        execution_mode="in_process",
        start_command=None,
        sampling_interval_seconds={"resource": 0.2, "disk": 5.0},
    )

    metrics = cast(dict[str, object], metadata["metrics"])

    assert metrics["version"] == 8
    assert set(metrics) == {
        "version",
        "sampling",
        "sampling_interval_seconds",
        "cpu_percent",
        "server_charge_mb",
        "server_anon_mb",
        "server_file_mb",
        "server_shmem_mb",
        "server_memory",
        "client_mem_mb",
        "client_uss_mb",
        "disk_mb",
        "disk_data_mb",
        "disk_wal_mb",
        "comparable_memory",
    }
    assert metrics["sampling_interval_seconds"] == {"resource": 0.2, "disk": 5.0}
    assert "mem_mb" not in metrics
    assert "server_mem_mb" not in metrics
    for column in ("server_charge_mb", "server_anon_mb", "server_file_mb", "server_shmem_mb", "server_memory"):
        assert cast(str, metrics[column]).startswith("server side:")
    assert cast(str, metrics["client_mem_mb"]).startswith("client side:")
    assert cast(str, metrics["client_uss_mb"]).startswith("client side:")
    for column in ("disk_mb", "disk_data_mb", "disk_wal_mb"):
        assert cast(str, metrics[column]).startswith("storage:")
    assert cast(str, metrics["cpu_percent"]).startswith("combined:")
    assert "allocated MiB from du -sk" in cast(str, metrics["disk_mb"])
    assert "options.storage_accounting" in cast(str, metrics["disk_mb"])


def test_input_fingerprint_is_complete_and_changes_with_content(tmp_path: Path) -> None:
    inputs = tmp_path / "inputs"
    nested = inputs / "nested"
    nested.mkdir(parents=True)
    (inputs / "a.parquet").write_bytes(b"first")
    (nested / "b.parquet").write_bytes(b"second")
    cache_path = tmp_path / "cache.json"

    first = fingerprint_input_directory(inputs, cache_path)
    second = fingerprint_input_directory(inputs, cache_path)

    assert first == second
    files = cast(list[dict[str, object]], first["files"])
    assert [file["path"] for file in files] == ["a.parquet", "nested/b.parquet"]

    (inputs / "a.parquet").write_bytes(b"changed")
    changed = fingerprint_input_directory(inputs, cache_path)

    assert changed["manifest_sha256"] != first["manifest_sha256"]


def test_input_fingerprint_reuses_cached_hashes_keyed_by_size_and_mtime(tmp_path: Path) -> None:
    inputs = tmp_path / "inputs"
    inputs.mkdir()
    source = inputs / "a.parquet"
    source.write_bytes(b"aaaa")
    cache_path = tmp_path / "cache.json"

    first = fingerprint_input_directory(inputs, cache_path)
    assert cache_path.is_file()

    stat = source.stat()
    source.write_bytes(b"bbbb")
    os.utime(source, ns=(stat.st_atime_ns, stat.st_mtime_ns))

    assert fingerprint_input_directory(inputs, cache_path) == first

    source.touch()
    assert fingerprint_input_directory(inputs, cache_path)["manifest_sha256"] != first["manifest_sha256"]


def _git(repository: Path, *args: str) -> None:
    subprocess.run(["git", "-C", str(repository), *args], check=True, capture_output=True)


def _init_repository(path: Path) -> Path:
    _git(path, "init", "-q")
    _git(path, "config", "user.email", "test@example.com")
    _git(path, "config", "user.name", "Test")
    (path / ".gitignore").write_text("ignored.txt\n")
    (path / "tracked.txt").write_text("first\n")
    _git(path, "add", ".")
    _git(path, "commit", "-q", "-m", "initial")
    return path


def test_repository_provenance_of_a_clean_checkout(tmp_path: Path) -> None:
    repository = _init_repository(tmp_path)
    (repository / "ignored.txt").write_text("not part of the tree\n")

    provenance = run_metadata.build_repository_provenance(repository)

    head = subprocess.run(
        ["git", "-C", str(repository), "rev-parse", "HEAD"], check=True, capture_output=True, text=True
    ).stdout.strip()
    assert provenance == {"commit": head, "dirty": False, "diff_sha256": None}


def test_repository_provenance_hashes_tracked_and_untracked_changes(tmp_path: Path) -> None:
    repository = _init_repository(tmp_path)

    (repository / "tracked.txt").write_text("second\n")
    modified = run_metadata.build_repository_provenance(repository)
    assert modified["dirty"] is True
    assert isinstance(modified["diff_sha256"], str)

    (repository / "new.txt").write_text("untracked\n")
    with_untracked = run_metadata.build_repository_provenance(repository)
    assert with_untracked["diff_sha256"] != modified["diff_sha256"]

    (repository / "new.txt").write_text("untracked, edited\n")
    edited_untracked = run_metadata.build_repository_provenance(repository)
    assert edited_untracked["diff_sha256"] not in (modified["diff_sha256"], with_untracked["diff_sha256"])


def test_repository_root_is_required(tmp_path: Path) -> None:
    outside = tmp_path / "not-a-repository"
    outside.mkdir()
    with pytest.raises(run_metadata.RepositoryNotFoundError, match="not inside a git repository"):
        run_metadata.find_repository_root(outside)


def test_run_metadata_records_the_repository_of_this_checkout() -> None:
    metadata = run_metadata.build_run_metadata(execution_mode="in_process", start_command=None)

    repository = cast(dict[str, object], metadata["repository"])
    assert set(repository) == {"commit", "dirty", "diff_sha256"}
    assert isinstance(repository["commit"], str)
    assert (repository["diff_sha256"] is None) is (repository["dirty"] is False)


def test_schema_hashes_cover_the_suite_schema_file_and_directory() -> None:
    duckdb_schema = run_metadata.hash_schema_files("tpc_h", "duckdb")
    assert list(cast(dict[str, str], duckdb_schema["files"])) == ["tpc_h/schemas/duckdb.sql"]

    timescaledb_schema = run_metadata.hash_schema_files("time_series", "timescaledb")
    assert sorted(cast(dict[str, str], timescaledb_schema["files"])) == [
        "time_series/schemas/timescaledb/large_pre_insert.sql",
        "time_series/schemas/timescaledb/tall_pre_insert.sql",
        "time_series/schemas/timescaledb/wide_pre_insert.sql",
    ]

    assert run_metadata.hash_schema_files("jsonbench", "duckdb")["files"] == {}
    assert duckdb_schema["digest"] != timescaledb_schema["digest"]


def test_run_metadata_records_the_benchmark_profile() -> None:
    metadata = run_metadata.build_run_metadata(execution_mode="in_process", start_command=None)

    profile = cast(dict[str, object], metadata["benchmark_profile"])
    assert profile["id"] == "resident-hot-v1"
    assert (profile["warmup_iterations"], profile["measured_iterations"]) == (1, 3)
