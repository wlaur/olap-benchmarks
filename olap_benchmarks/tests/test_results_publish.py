import hashlib
import json
import shutil
import subprocess
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any, cast

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from olap_benchmarks import results as results_module
from olap_benchmarks.matrix import MatrixCell
from olap_benchmarks.results import (
    _build_queries_manifest,
    _build_suites_manifest,
    get_results_engine,
    migrate_results,
)
from olap_benchmarks.results.models import Run, RunStep, SystemSnapshot
from olap_benchmarks.results.publishing import PublishValidationError, validate_publishable_runs
from olap_benchmarks.settings import (
    ALL_SUITE_SCALE_FACTORS,
    DEFAULT_SUITE_SCALE_FACTORS,
    SCALE_FACTOR_SUITES,
    SUITE_DISPLAY_ORDER,
    SUITE_LABELS,
    SUITE_NAMES,
    SUITE_OPERATIONS,
    SUITE_PUBLIC_ROLES,
    SUITE_QUERY_NAME_PARSERS,
)


def test_suites_manifest_matches_suite_settings() -> None:
    manifest = _build_suites_manifest()
    suites = cast(list[dict[str, Any]], manifest["suites"])

    assert [suite["id"] for suite in suites] == list(SUITE_DISPLAY_ORDER)

    suites_by_id = {suite["id"]: suite for suite in suites}
    for suite_name in SUITE_NAMES:
        suite = suites_by_id[suite_name]
        assert suite["title"] == SUITE_LABELS[suite_name]
        assert suite["queries_key"] == suite_name
        assert suite["default_scale_factor"] == DEFAULT_SUITE_SCALE_FACTORS[suite_name]
        assert suite["supported_scale_factors"] == list(ALL_SUITE_SCALE_FACTORS[suite_name])
        assert suite["scale_factor_supported"] == (suite_name in SCALE_FACTOR_SUITES)
        assert suite["operations"] == list(SUITE_OPERATIONS[suite_name])
        assert suite["query_name_parser"] == SUITE_QUERY_NAME_PARSERS[suite_name]
        assert suite["public_role"] == SUITE_PUBLIC_ROLES[suite_name]


def test_clickbench_q28_uses_engine_regex_backrefs() -> None:
    manifest = _build_queries_manifest()
    q28 = manifest["clickbench"]["Q28"]
    overrides = cast(dict[str, str], q28["db_overrides"])

    assert "'$1'" in overrides["monetdb"]
    assert "'$1'" in overrides["starrocks"]
    assert "'\\1'" in overrides["postgres"]


def test_queries_manifest_includes_registered_suites() -> None:
    manifest = _build_queries_manifest()

    assert set(SUITE_DISPLAY_ORDER).issubset(manifest)
    assert set(manifest["jsonbench"]) == {
        "01_events_by_collection",
        "02_create_events_by_collection",
        "03_create_events_by_hour",
        "04_first_post_users",
        "05_longest_post_activity",
    }
    assert set(cast(dict[str, str], manifest["jsonbench"]["01_events_by_collection"]["db_overrides"])) == {
        "clickhouse",
        "doris",
        "postgres",
        "starrocks",
    }


CELLS = [
    MatrixCell("duckdb", "rtabench", 1, "populate"),
    MatrixCell("duckdb", "rtabench", 1, "select"),
    MatrixCell("monetdb", "rtabench", 1, "populate"),
    MatrixCell("monetdb", "rtabench", 1, "select"),
]
PINS = {"duckdb": "1.0", "monetdb": "SP4"}
DRIVERS: dict[str, str | None] = {"duckdb": None, "monetdb": "adbc"}


def _add_run(
    session: Session,
    cell: MatrixCell,
    *,
    version: str | None = None,
    driver: str | None = "default",
    status: str = "completed",
    started_at: datetime = datetime(2026, 10, 1),
    commit: str = "c1",
    dirty: bool = False,
    snapshot_id: int = 1,
    answer_hash: str = "blake2b128:test",
) -> int:
    run = Run(
        suite=cell.suite,
        suite_scale_factor=cell.scale_factor,
        db=cell.db,
        db_version=version or PINS[cell.db],
        db_driver=DRIVERS[cell.db] if driver == "default" else driver,
        operation=cell.operation,
        system="test",
        system_snapshot_id=snapshot_id,
        status=status,
        started_at=started_at,
        finished_at=started_at,
        metadata_json={"repository": {"commit": commit, "dirty": dirty, "diff_sha256": "x" if dirty else None}},
    )
    session.add(run)
    session.flush()
    if cell.db == "monetdb":
        for step_name, row_count in (("session_baseline_before", 1), ("session_baseline_after", 0)):
            session.add(
                RunStep(
                    run_id=run.id,
                    step_type="phase",
                    step_name=step_name,
                    started_at=started_at,
                    finished_at=started_at,
                    status="completed",
                    row_count=row_count,
                    metadata_json={"temporary_ingest_tables": 0},
                )
            )
    if cell.operation == "select":
        for iteration in (1, 2):
            session.add(
                RunStep(
                    run_id=run.id,
                    step_type="query",
                    step_name="query",
                    query_name="q1",
                    iteration=iteration,
                    started_at=started_at,
                    finished_at=started_at,
                    status="completed",
                    result_status="ok",
                    row_count=1,
                    metadata_json={"answer_hash": answer_hash},
                )
            )
    return run.id


def _results_db(tmp_path: Path, add: Callable[[Session], None]) -> Path:
    db_path = migrate_results(db_path=tmp_path / "results.db")
    engine = get_results_engine(read_only=False, db_path=db_path)
    try:
        with Session(engine) as session:
            session.add(SystemSnapshot(id=1, system="test"))
            session.add(SystemSnapshot(id=2, system="test", os="other"))
            add(session)
            session.commit()
    finally:
        engine.dispose()
    return db_path


def _complete(session: Session) -> None:
    for cell in CELLS:
        _add_run(session, cell)


def _validate(db_path: Path) -> None:
    validate_publishable_runs(db_path, CELLS, pins=PINS, drivers=DRIVERS)


def test_publish_gate_accepts_a_complete_coherent_matrix(tmp_path: Path) -> None:
    _validate(_results_db(tmp_path, _complete))


def test_publish_gate_lists_missing_and_failed_cells(tmp_path: Path) -> None:
    def add(session: Session) -> None:
        _add_run(session, CELLS[0])
        _add_run(session, CELLS[1], status="failed")
        _add_run(session, CELLS[2])

    with pytest.raises(PublishValidationError) as excinfo:
        _validate(_results_db(tmp_path, add))

    message = str(excinfo.value)
    assert "monetdb:rtabench:sf1:select" in message
    assert "did not complete" in message
    assert "duckdb rtabench" not in message.split("did not complete")[0]


def test_publish_gate_rejects_mixed_commits_dirty_trees_and_snapshots(tmp_path: Path) -> None:
    def add(session: Session) -> None:
        _add_run(session, CELLS[0], commit="c2")
        _add_run(session, CELLS[1], dirty=True)
        _add_run(session, CELLS[2], snapshot_id=2)
        _add_run(session, CELLS[3])

    with pytest.raises(PublishValidationError) as excinfo:
        _validate(_results_db(tmp_path, add))

    message = str(excinfo.value)
    assert "exactly one repository commit" in message
    assert "dirty" in message
    assert "exactly one system snapshot" in message


def test_publish_gate_rejects_stale_versions_and_superseded_runs(tmp_path: Path) -> None:
    def add(session: Session) -> None:
        _complete(session)
        _add_run(session, CELLS[0], started_at=datetime(2026, 9, 1))
        _add_run(session, CELLS[2], version="SP3", started_at=datetime(2026, 11, 1))

    with pytest.raises(PublishValidationError) as excinfo:
        _validate(_results_db(tmp_path, add))

    message = str(excinfo.value)
    assert "superseded" in message
    assert "other than the current pin" in message


def test_publish_gate_rejects_wrong_results_and_unstable_answers(tmp_path: Path) -> None:
    def add(session: Session) -> None:
        _add_run(session, CELLS[0])
        _add_run(session, CELLS[1], answer_hash="blake2b128:a")
        _add_run(session, CELLS[2])
        run_id = _add_run(session, CELLS[3], answer_hash="blake2b128:b")
        session.flush()
        step = session.scalars(select(RunStep).where(RunStep.run_id == run_id, RunStep.iteration == 2)).one()
        step.result_status = "wrong_result"
        step.metadata_json = {"answer_hash": "blake2b128:c"}

    with pytest.raises(PublishValidationError) as excinfo:
        _validate(_results_db(tmp_path, add))

    message = str(excinfo.value)
    assert "marked wrong_result" in message
    assert "Answer-hash mismatches" in message
    assert "non-deterministic" in message


def test_publish_gate_rejects_missing_select_correctness(tmp_path: Path) -> None:
    def add(session: Session) -> None:
        _complete(session)
        session.flush()
        step = session.scalars(select(RunStep).where(RunStep.step_type == "query").limit(1)).one()
        step.metadata_json = None

    with pytest.raises(PublishValidationError, match="without a correctness result"):
        _validate(_results_db(tmp_path, add))


def test_publish_gate_keeps_monetdb_session_baselines(tmp_path: Path) -> None:
    def add(session: Session) -> None:
        _complete(session)
        session.flush()
        step = session.scalars(select(RunStep).where(RunStep.step_name == "session_baseline_after").limit(1)).one()
        step.row_count = 3

    with pytest.raises(PublishValidationError, match="clean session baselines"):
        _validate(_results_db(tmp_path, add))


def _patch_publish(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    results_directory = tmp_path / "results"
    results_directory.mkdir()
    published = tmp_path / "published"
    published.mkdir()

    def cells(published_only: bool = False) -> list[MatrixCell]:
        assert published_only
        return CELLS

    def pins() -> dict[str, str]:
        return PINS

    def drivers() -> dict[str, str | None]:
        return DRIVERS

    def manifest() -> dict[str, object]:
        return {}

    monkeypatch.setattr(results_module.SETTINGS, "results_directory", results_directory)
    monkeypatch.setattr(results_module, "PUBLISHED_DATA_DIRECTORY", published)
    monkeypatch.setattr(results_module, "expand_matrix", cells)
    monkeypatch.setattr(results_module, "current_pins", pins)
    monkeypatch.setattr(results_module, "current_drivers", drivers)
    monkeypatch.setattr(results_module, "_build_queries_manifest", manifest)
    return results_directory


def test_publish_writes_the_selected_compacted_set_with_its_checksum(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    results_directory = _patch_publish(monkeypatch, tmp_path)

    def add(session: Session) -> None:
        _complete(session)
        _add_run(session, CELLS[0], started_at=datetime(2026, 9, 1), commit="old")
        _add_run(session, MatrixCell("duckdb", "tpc_h", 10, "populate"))

    source = _results_db(tmp_path, add)
    shutil.move(source, results_directory / "candidate.db")

    output_dir, merge_stats = results_module.publish(revision="candidate")

    assert merge_stats is None
    output_db = output_dir / "results.db"
    manifest = json.loads((output_dir / "manifest.json").read_text())
    assert manifest["results_db_sha256"] == hashlib.sha256(output_db.read_bytes()).hexdigest()
    assert manifest["results_db_bytes"] == output_db.stat().st_size
    assert manifest["table_counts"]["run"] == len(CELLS)
    assert not (output_dir / "results.db.staging").exists()


def test_publish_refuses_an_incoherent_set_and_writes_nothing(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    results_directory = _patch_publish(monkeypatch, tmp_path)

    def add(session: Session) -> None:
        for cell in CELLS[:-1]:
            _add_run(session, cell)

    shutil.move(_results_db(tmp_path, add), results_directory / "candidate.db")

    with pytest.raises(PublishValidationError, match="monetdb:rtabench:sf1:select"):
        results_module.publish(revision="candidate")

    assert list((tmp_path / "published").iterdir()) == []


def test_upload_triggers_the_pages_workflow(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    calls: list[list[str]] = []

    def fake_run(args: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(args)
        return subprocess.CompletedProcess(args=args, returncode=0, stdout="", stderr="")

    def fake_require_gh(_tag: str) -> None:
        return None

    monkeypatch.setattr(results_module.subprocess, "run", fake_run)
    monkeypatch.setattr(results_module, "_require_gh", fake_require_gh)

    results_module.trigger_pages_deploy()

    assert calls == [["gh", "workflow", "run", "pages.yml", "--ref", "main"]]


def _missing_gh(_name: str) -> str | None:
    return None


def _present_gh(_name: str) -> str | None:
    return "/usr/bin/gh"


def test_require_gh_reports_missing_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(results_module.shutil, "which", _missing_gh)

    with pytest.raises(RuntimeError, match="gh not found on PATH"):
        results_module._require_gh("data")


def test_require_gh_reports_unauthenticated_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(results_module.shutil, "which", _present_gh)

    def unauthenticated(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(args=["gh", "auth", "status"], returncode=1, stdout="", stderr="")

    monkeypatch.setattr(results_module.subprocess, "run", unauthenticated)

    with pytest.raises(RuntimeError, match="not authenticated"):
        results_module._require_gh("data")


def test_require_gh_accepts_authenticated_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(results_module.shutil, "which", _present_gh)

    def authenticated(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(args=["gh", "auth", "status"], returncode=0, stdout="", stderr="")

    monkeypatch.setattr(results_module.subprocess, "run", authenticated)

    results_module._require_gh("data")


def test_upload_published_database_requires_a_published_file(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(results_module, "PUBLISHED_DATA_DIRECTORY", tmp_path)

    with pytest.raises(FileNotFoundError, match="run 'olap publish' first"):
        results_module.upload_published_database()
