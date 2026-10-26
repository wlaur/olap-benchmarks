from __future__ import annotations

import json
import shutil
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from ..results import certification as cert
from ..results import get_results_engine
from ..results.comparison import write_comparison_manifest
from ..results.models import QueryExecution, Run, RunMetric, RunStep
from .test_results_campaign import CELLS, DRIVERS, PINS, POPULATE, _add_run, _results_db


def _write(path: Path, value: object) -> dict[str, str]:
    path.write_text(json.dumps(value))
    return {"path": path.name, "sha256": cert.file_sha256(path)}


@pytest.fixture
def publication(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, dict[str, Any]]:
    monkeypatch.setattr(cert, "campaign_cells", lambda: CELLS)
    monkeypatch.setattr(cert, "current_pins", lambda: PINS)
    monkeypatch.setattr(cert, "current_drivers", lambda: DRIVERS)

    def add(session: Session) -> None:
        _add_run(session, POPULATE)
        _add_run(session, start=1, status="failed", recorded=("01_events_by_collection",))
        for run in session.scalars(select(Run)):
            run.metadata_json = {
                **(run.metadata_json or {}),
                "input": {"manifest_sha256": "a" * 64},
                "benchmark_profile": {"id": "resident-hot-v1", "warmup_iterations": 1, "measured_iterations": 3},
            }
            session.add(RunMetric(run_id=run.id, time=run.started_at, cpu_percent=1.5))
        population = session.get(Run, 1)
        assert population is not None
        session.add(
            RunStep(
                run_id=1,
                step_type="phase",
                step_name="populate",
                status="completed",
                table_name="bluesky",
                row_count=10_000_000,
                started_at=population.started_at,
                finished_at=population.finished_at,
            )
        )
        failed = session.get(Run, 2)
        assert failed is not None
        session.add(
            RunStep(
                run_id=2,
                step_type="phase",
                step_name="select",
                status="failed",
                started_at=failed.started_at,
                finished_at=failed.finished_at,
                error_type="FailedQueriesError",
                error_message="One query failed",
            )
        )
        step = session.scalars(select(RunStep).where(RunStep.run_id == 2)).first()
        assert step is not None and step.finished_at is not None
        session.add(
            QueryExecution(
                run_id=2, run_step_id=step.id, query="SELECT 1", start_time=step.started_at, end_time=step.finished_at
            )
        )

    raw = _results_db(tmp_path, add)
    raw.rename(tmp_path / "raw.db")
    raw = tmp_path / "raw.db"
    final = tmp_path / "results.db"
    shutil.copyfile(raw, final)
    digest = cert.file_sha256(final)
    _write(tmp_path / "manifest.json", {"results_db_sha256": digest, "results_db_bytes": final.stat().st_size})
    origin: dict[str, Any] = {"revision": "raw", "results_db_sha256": cert.file_sha256(raw), "run_id": 2}
    population_origin = {**origin, "run_id": 1}
    evidence = _write(tmp_path / "sealed.json", {"audit": "fixture evidence"})
    audit = _write(
        tmp_path / "population.json",
        {
            "selected_run_id": 1,
            "origin": population_origin,
            "canonical": True,
            "complete": True,
            "tables": [{"name": "bluesky", "expected_rows": 10_000_000, "actual_rows": 10_000_000}],
            "evidence": [evidence],
        },
    )
    lineage = _write(
        tmp_path / "lineage.json",
        {
            "selected_run_id": 2,
            "origin": origin,
            "population_origin": population_origin,
            "population_selected_run_id": 1,
            "full_query_plan": True,
            "all_query_sql_audited": True,
            "preparation_complete": True,
            "interrupted": False,
            "validation_ok": True,
            "evidence": [evidence],
        },
    )
    cert.write_validation_report(final, tmp_path / "validation.json")
    certificate: dict[str, Any] = {
        "selected_run_id": 2,
        "cell": {
            "system": "test",
            "db": "duckdb",
            "db_version": "1.0",
            "db_driver": None,
            "suite": "jsonbench",
            "scale_factor": 10,
            "benchmark_profile": "resident-hot-v1",
        },
        "origin": origin,
        "run_content_sha256": cert.run_content_sha256(final, 2),
        "population": {
            "selected_run_id": 1,
            "origin": population_origin,
            "run_content_sha256": cert.run_content_sha256(final, 1),
            "audit": audit,
        },
        "lineage": lineage,
        "validation": {"path": "validation.json", "sha256": cert.file_sha256(tmp_path / "validation.json")},
    }
    return tmp_path, {
        "version": 1,
        "policy_id": cert.POLICY,
        "results_db_sha256": digest,
        "certificates": [certificate],
    }


def _validate(directory: Path, bundle: dict[str, Any]) -> Path:
    _write(directory / "certificates.json", bundle)
    return write_comparison_manifest(
        directory, certificates_path=directory / "certificates.json", evidence_directory=directory
    )


def test_v2_certifies_whole_failed_run_without_modifying_measurements(publication: tuple[Path, dict[str, Any]]) -> None:
    directory, bundle = publication
    before = cert.file_sha256(directory / "results.db")
    output = json.loads(_validate(directory, bundle).read_text())
    assert output["version"] == 2
    assert output["overall_policy"] == "relative-speed-v2"
    assert output["certificate_policy"] == cert.POLICY
    assert output["certificates"] == bundle["certificates"]
    assert cert.file_sha256(directory / "results.db") == before
    baseline = json.loads(write_comparison_manifest(directory).read_text())
    assert baseline["version"] == 1
    assert "certificates" not in baseline


ALTERATIONS: list[Callable[[dict[str, Any]], object]] = [
    lambda b: b.update(results_db_sha256="0" * 64),
    lambda b: b["certificates"].append(b["certificates"][0]),
    lambda b: b["certificates"][0].update(selected_run_id=999),
    lambda b: b["certificates"][0].update(selected_run_id=1),
    lambda b: b["certificates"][0].update(run_content_sha256="0" * 64),
    lambda b: b["certificates"][0]["cell"].update(scale_factor=1),
    lambda b: b["certificates"][0]["origin"].update(run_id=999),
    lambda b: b["certificates"][0]["origin"].update(results_db_sha256="0" * 64),
    lambda b: b["certificates"][0]["population"].update(selected_run_id=999),
    lambda b: b["certificates"][0]["population"]["origin"].update(run_id=2),
    lambda b: b["certificates"][0]["lineage"].update(sha256="0" * 64),
    lambda b: b["certificates"][0].update(query_names=["01_events_by_collection"]),
    lambda b: b["certificates"][0].pop("population"),
    lambda b: b.update(extra=True),
]


@pytest.mark.parametrize("alter", ALTERATIONS)
def test_stale_unknown_duplicate_and_subset_certificates_fail_closed(
    publication: tuple[Path, dict[str, Any]], alter: Callable[[dict[str, Any]], object]
) -> None:
    directory, bundle = publication
    alter(bundle)
    (directory / "comparison.json").write_text("prior metadata")
    with pytest.raises(ValueError):
        _validate(directory, bundle)
    assert (directory / "comparison.json").read_text() == "prior metadata"


@pytest.mark.parametrize(
    ("report", "field", "value"),
    [
        ("lineage", "interrupted", True),
        ("lineage", "validation_ok", False),
        ("lineage", "preparation_complete", False),
        ("lineage", "population_selected_run_id", 999),
        ("population", "tables", [{"name": "bluesky", "expected_rows": 1, "actual_rows": 1}]),
        ("population", "tables", []),
        ("validation", "scope", "successful-subset"),
        ("validation", "iteration_inconsistencies", ["wrong answer"]),
        ("validation", "include_failed", False),
        ("validation", "extra", True),
    ],
)
def test_proof_reports_require_full_clean_bound_evidence(
    publication: tuple[Path, dict[str, Any]], report: str, field: str, value: object
) -> None:
    directory, bundle = publication
    path = directory / f"{report}.json"
    content = json.loads(path.read_text())
    content[field] = value
    reference = _write(path, content)
    certificate = bundle["certificates"][0]
    if report == "population":
        certificate["population"]["audit"] = reference
    else:
        certificate[report] = reference
    with pytest.raises(ValueError):
        _validate(directory, bundle)


@pytest.mark.parametrize("defect", ["wrong_result", "invalidation", "unfinished", "preparation", "subset", "iteration"])
def test_database_defects_cannot_be_overridden_by_clean_claims(
    publication: tuple[Path, dict[str, Any]], defect: str
) -> None:
    directory, bundle = publication
    path = directory / "results.db"
    engine = get_results_engine(db_path=path, read_only=False)
    try:
        with Session(engine) as session:
            run = session.get(Run, 2)
            assert run is not None
            step = session.scalars(select(RunStep).where(RunStep.run_id == 2)).first()
            assert step is not None
            if defect == "wrong_result":
                step.result_status = "wrong_result"
            elif defect == "unfinished":
                run.finished_at = None
            elif defect == "iteration":
                step.iteration = 99
            elif defect == "preparation":
                pop = session.get(Run, 1)
                assert pop is not None
                pop.status = "failed"
            else:
                run.metadata_json = {
                    **(run.metadata_json or {}),
                    **(
                        {"invalidation": {"reason": "bad"}}
                        if defect == "invalidation"
                        else {"settings": {"OLAP_BENCHMARKS_QUERY_INCLUDE": ["01*"]}}
                    ),
                }
            session.commit()
    finally:
        engine.dispose()
    bundle["results_db_sha256"] = cert.file_sha256(path)
    _write(
        directory / "manifest.json",
        {"results_db_sha256": bundle["results_db_sha256"], "results_db_bytes": path.stat().st_size},
    )
    with pytest.raises((ValueError, RuntimeError)):
        _validate(directory, bundle)


def test_fingerprint_includes_metrics(publication: tuple[Path, dict[str, Any]]) -> None:
    directory, bundle = publication
    path = directory / "results.db"
    before = bundle["certificates"][0]["run_content_sha256"]
    engine = get_results_engine(db_path=path, read_only=False)
    try:
        with Session(engine) as session:
            metric = session.scalars(select(RunMetric).where(RunMetric.run_id == 2)).first()
            assert metric is not None
            metric.cpu_percent = 99
            session.commit()
    finally:
        engine.dispose()
    assert cert.run_content_sha256(path, 2) != before
