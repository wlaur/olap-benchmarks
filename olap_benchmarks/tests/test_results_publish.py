import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any, cast

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from olap_benchmarks import results as results_module
from olap_benchmarks.dbs.monetdb.manifest import monetdb_benchmark_manifest
from olap_benchmarks.results import (
    _build_queries_manifest,
    _build_suites_manifest,
    _validate_publishable_runs,
    get_results_engine,
    migrate_results,
)
from olap_benchmarks.results.models import Run, RunStep
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


def _add_run(
    session: Session,
    *,
    driver: str,
    suite: str,
    scale_factor: int,
    operation: str,
    status: str = "completed",
    system: str = "test",
) -> None:
    run = Run(
        suite=suite,
        suite_scale_factor=scale_factor,
        db="monetdb",
        db_version="test",
        db_driver=driver,
        operation=operation,
        system=system,
        status=status,
        started_at=datetime(2026, 7, 29),
    )
    session.add(run)
    session.flush()
    for step_name, row_count in (("session_baseline_before", 1), ("session_baseline_after", 0)):
        session.add(
            RunStep(
                run_id=run.id,
                step_type="phase",
                step_name=step_name,
                started_at=datetime(2026, 7, 29),
                finished_at=datetime(2026, 7, 29),
                status="completed",
                row_count=row_count,
                metadata_json={"temporary_ingest_tables": 0},
            )
        )
    if operation == "select":
        session.add(
            RunStep(
                run_id=run.id,
                step_type="query",
                step_name="query",
                query_name="q1",
                iteration=1,
                started_at=datetime(2026, 7, 29),
                finished_at=datetime(2026, 7, 29),
                status="completed",
                result_status="ok",
                row_count=1,
                metadata_json={"answer_hash": "blake2b128:test"},
            )
        )


def test_publish_validation_rejects_running_and_incomplete_monetdb_results(
    tmp_path: Path,
) -> None:
    db_path = migrate_results(db_path=tmp_path / "results.db")
    engine = get_results_engine(read_only=False, db_path=db_path)
    try:
        with Session(engine) as session:
            _add_run(
                session,
                driver="adbc",
                suite="rtabench",
                scale_factor=1,
                operation="populate",
                status="running",
            )
            session.commit()
        engine.dispose()
        with pytest.raises(RuntimeError, match="running run"):
            _validate_publishable_runs(db_path)

        engine = get_results_engine(read_only=False, db_path=db_path)
        with Session(engine) as session:
            run = session.scalars(select(Run)).one()
            run.status = "completed"
            session.commit()
        engine.dispose()
        with pytest.raises(RuntimeError, match="lacks completed ADBC runs"):
            _validate_publishable_runs(db_path)
    finally:
        engine.dispose()


@pytest.mark.parametrize("driver", ["experimental", "staged"])
def test_publish_validation_rejects_unknown_monetdb_driver(tmp_path: Path, driver: str) -> None:
    db_path = migrate_results(db_path=tmp_path / "results.db")
    engine = get_results_engine(read_only=False, db_path=db_path)
    try:
        with Session(engine) as session:
            _add_run(
                session,
                driver=driver,
                suite="rtabench",
                scale_factor=1,
                operation="populate",
            )
            session.commit()
        engine.dispose()

        with pytest.raises(RuntimeError, match="Unknown MonetDB db_driver"):
            _validate_publishable_runs(db_path)
    finally:
        engine.dispose()


def test_publish_validation_accepts_the_complete_monetdb_release_matrix(
    tmp_path: Path,
) -> None:
    db_path = migrate_results(db_path=tmp_path / "results.db")
    engine = get_results_engine(read_only=False, db_path=db_path)
    try:
        with Session(engine) as session:
            for cell in monetdb_benchmark_manifest():
                _add_run(
                    session,
                    driver="adbc",
                    suite=cell.suite,
                    scale_factor=cell.scale_factor,
                    operation=cell.operation,
                )
            session.commit()
        engine.dispose()
        _validate_publishable_runs(db_path)
    finally:
        engine.dispose()


def test_publish_validation_accepts_a_run_that_skipped_some_scale_factors(tmp_path: Path) -> None:
    """A default-scale publish must not be blocked by scale factors nobody benchmarked.

    The gate exists to stop a partially validated MonetDB from being published, not to force every
    configured scale factor: the full fan-out needs tens of gigabytes of SF10 and SF50 input that a
    default-scale run has no reason to generate.
    """
    db_path = migrate_results(db_path=tmp_path / "results.db")
    engine = get_results_engine(read_only=False, db_path=db_path)
    try:
        with Session(engine) as session:
            for cell in monetdb_benchmark_manifest():
                if cell.scale_factor != min(
                    other.scale_factor for other in monetdb_benchmark_manifest() if other.suite == cell.suite
                ):
                    continue
                _add_run(
                    session,
                    driver="adbc",
                    suite=cell.suite,
                    scale_factor=cell.scale_factor,
                    operation=cell.operation,
                )
            session.commit()
        engine.dispose()

        _validate_publishable_runs(db_path)
    finally:
        engine.dispose()


def test_publish_validation_still_rejects_an_incomplete_scale_factor(tmp_path: Path) -> None:
    # scoping to what was run must not stop a half-finished suite from being caught
    db_path = migrate_results(db_path=tmp_path / "results.db")
    engine = get_results_engine(read_only=False, db_path=db_path)
    try:
        with Session(engine) as session:
            for cell in monetdb_benchmark_manifest():
                if cell.suite == "rtabench" and cell.scale_factor == 1 and cell.operation == "select":
                    continue
                _add_run(
                    session,
                    driver="adbc",
                    suite=cell.suite,
                    scale_factor=cell.scale_factor,
                    operation=cell.operation,
                )
            session.commit()
        engine.dispose()

        with pytest.raises(RuntimeError, match="rtabench:sf1:select"):
            _validate_publishable_runs(db_path)
    finally:
        engine.dispose()


def test_publish_validation_rejects_missing_select_correctness(tmp_path: Path) -> None:
    db_path = migrate_results(db_path=tmp_path / "results.db")
    engine = get_results_engine(read_only=False, db_path=db_path)
    try:
        with Session(engine) as session:
            for cell in monetdb_benchmark_manifest():
                _add_run(
                    session,
                    driver="adbc",
                    suite=cell.suite,
                    scale_factor=cell.scale_factor,
                    operation=cell.operation,
                )
            step = session.scalars(
                select(RunStep)
                .join(Run, Run.id == RunStep.run_id)
                .where(Run.db_driver == "adbc", Run.operation == "select", RunStep.step_type == "query")
                .limit(1)
            ).one()
            step.metadata_json = None
            session.commit()
        engine.dispose()

        with pytest.raises(RuntimeError, match="lack correctness results"):
            _validate_publishable_runs(db_path)
    finally:
        engine.dispose()


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
    monkeypatch.setattr(results_module, "REPO_ROOT", tmp_path)

    with pytest.raises(FileNotFoundError, match="run 'olap publish' first"):
        results_module.upload_published_database()
