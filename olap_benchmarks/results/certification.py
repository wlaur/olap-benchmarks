from __future__ import annotations

import hashlib
import json
from dataclasses import asdict
from pathlib import Path
from typing import Annotated, Any, Literal, cast

import duckdb
from pydantic import BaseModel, ConfigDict, Field

from ..matrix import current_drivers, current_pins
from ..profile import BENCHMARK_PROFILE
from ..settings import DatabaseName, SuiteName
from .campaign import campaign_cells
from .publishing import validate_publishable_runs
from .validation import validate_results

POLICY = "certified-terminal-partial-v1"
type Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
type RunId = Annotated[int, Field(gt=0)]


class Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class Origin(Contract):
    revision: Annotated[str, Field(pattern=r"^[a-zA-Z0-9][a-zA-Z0-9_.-]*$")]
    results_db_sha256: Digest
    run_id: RunId


class Artifact(Contract):
    path: Annotated[str, Field(min_length=1)]
    sha256: Digest


class Cell(Contract):
    system: Annotated[str, Field(min_length=1)]
    db: DatabaseName
    db_version: Annotated[str, Field(min_length=1)]
    db_driver: str | None
    suite: SuiteName
    scale_factor: Annotated[int, Field(gt=0)]
    benchmark_profile: Annotated[str, Field(min_length=1)]


class Population(Contract):
    selected_run_id: RunId
    origin: Origin
    run_content_sha256: Digest
    audit: Artifact


class Certificate(Contract):
    selected_run_id: RunId
    cell: Cell
    origin: Origin
    run_content_sha256: Digest
    population: Population
    lineage: Artifact
    validation: Artifact


class Certificates(Contract):
    version: Literal[1]
    policy_id: Literal["certified-terminal-partial-v1"]
    results_db_sha256: Digest
    certificates: list[Certificate]


class TableAudit(Contract):
    name: str
    expected_rows: Annotated[int, Field(ge=0)]
    actual_rows: Annotated[int, Field(ge=0)]


class PopulationAudit(Contract):
    selected_run_id: RunId
    origin: Origin
    canonical: Literal[True]
    complete: Literal[True]
    tables: list[TableAudit]
    evidence: Annotated[list[Artifact], Field(min_length=1)]


class Lineage(Contract):
    selected_run_id: RunId
    origin: Origin
    population_origin: Origin
    population_selected_run_id: RunId
    full_query_plan: Literal[True]
    all_query_sql_audited: Literal[True]
    preparation_complete: Literal[True]
    interrupted: Literal[False]
    validation_ok: Literal[True]
    evidence: Annotated[list[Artifact], Field(min_length=1)]


def file_sha256(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def _json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _rows(con: duckdb.DuckDBPyConnection, table: str, run_id: int) -> list[dict[str, Any]]:
    key = "id" if table == "run" else "run_id"
    return [
        json.loads(row[0])
        for row in con.execute(f"SELECT to_json(t) FROM {table} t WHERE {key}=?", [run_id]).fetchall()
    ]


def run_content_sha256(db_path: Path, run_id: int) -> str:
    con: duckdb.DuckDBPyConnection = cast(Any, duckdb).connect(str(db_path), read_only=True)
    try:
        runs = _rows(con, "run", run_id)
        if len(runs) != 1:
            raise ValueError(f"Unknown or duplicate run ID: {run_id}")
        steps = _rows(con, "run_step", run_id)
        step_keys = {step["id"]: index for index, step in enumerate(sorted(steps, key=lambda row: row["id"]))}
        content: dict[str, object] = {}
        for table, rows in (
            ("run", runs),
            ("run_step", steps),
            ("query_execution", _rows(con, "query_execution", run_id)),
            ("run_metric", _rows(con, "run_metric", run_id)),
        ):
            for row in rows:
                row.pop("id")
                row.pop("run_id", None)
                row.pop("system_snapshot_id", None)
                if "run_step_id" in row and row["run_step_id"] is not None:
                    row["run_step_id"] = step_keys[row["run_step_id"]]
            content[table] = sorted(rows, key=_json)
        snapshot = con.execute(
            "SELECT to_json(s) FROM system_snapshot s JOIN run r ON r.system_snapshot_id=s.id WHERE r.id=?", [run_id]
        ).fetchall()
        if len(snapshot) != 1:
            raise ValueError(f"Run {run_id}: missing or duplicate system snapshot")
        snapshot_row = json.loads(snapshot[0][0])
        del snapshot_row["id"]
        content["system_snapshot"] = snapshot_row
        return hashlib.sha256(_json(content).encode()).hexdigest()
    finally:
        con.close()


def _validation_report(db_path: Path) -> dict[str, object]:
    report = validate_results(db_path=db_path, include_failed=True, mark=False)
    if not report.ok:
        raise ValueError(f"Whole-publication answer/iteration validation failed: {report.format()}")
    return {
        **asdict(report),
        "results_db_sha256": file_sha256(db_path),
        "scope": "all-answers-and-iterations",
    }


def write_validation_report(db_path: Path, path: Path) -> None:
    path.write_text(json.dumps(_validation_report(db_path), indent=2) + "\n")


def _artifact_path(artifact: Artifact, directory: Path) -> Path:
    relative = Path(artifact.path)
    if relative.is_absolute() or ".." in relative.parts:
        raise ValueError("Evidence paths must be relative without parent traversal")
    path = directory / relative
    if file_sha256(path) != artifact.sha256:
        raise ValueError(f"Stale evidence digest: {artifact.path}")
    return path


def _origin_path(origin: Origin, directory: Path, checked: set[tuple[str, str]]) -> Path:
    path = directory / f"{origin.revision}.db"
    key = (origin.revision, origin.results_db_sha256)
    if key not in checked:
        if file_sha256(path) != origin.results_db_sha256:
            raise ValueError(f"Raw database SHA-256 mismatch: {origin.revision}")
        checked.add(key)
    return path


def _check_run(con: duckdb.DuckDBPyConnection, run_id: int, cell: Cell, operation: str) -> dict[str, Any]:
    rows = _rows(con, "run", run_id)
    if len(rows) != 1:
        raise ValueError(f"Unknown or duplicate selected run: {run_id}")
    run = rows[0]
    actual = {key: run["suite_scale_factor" if key == "scale_factor" else key] for key in Cell.model_fields}
    if actual != cell.model_dump() or run["operation"] != operation:
        raise ValueError(f"Wrong certificate cell for run {run_id}")
    metadata: dict[str, Any] = run["metadata"] or {}
    repository: dict[str, Any] = metadata.get("repository") or {}
    settings: dict[str, Any] = metadata.get("settings") or {}
    input_metadata: dict[str, Any] = metadata.get("input") or {}
    if (
        not run["finished_at"]
        or run["status"] != ("failed" if operation == "select" else "completed")
        or metadata.get("invalidation") is not None
        or repository.get("dirty") is not False
        or not repository.get("commit")
        or not input_metadata.get("manifest_sha256")
        or cell.benchmark_profile != BENCHMARK_PROFILE.id
    ):
        raise ValueError(f"Run {run_id}: status, provenance or invalidation forbids certification")
    if any(settings.get(f"OLAP_BENCHMARKS_{key}") for key in ("QUERY_INCLUDE", "QUERY_EXCLUDE", "TABLES")):
        raise ValueError(f"Run {run_id}: query/table subsets cannot be certified")
    steps = _rows(con, "run_step", run_id)
    if not steps or any(
        step["result_status"] == "wrong_result"
        or not step["finished_at"]
        or (
            step["step_type"] != "query"
            and step["status"] != "completed"
            and not (
                operation == "select"
                and step["step_type"] == "phase"
                and step["step_name"] == "select"
                and step["status"] == "failed"
                and step["error_type"]
                and step["error_message"] is not None
            )
        )
        for step in steps
    ):
        raise ValueError(f"Run {run_id}: wrong answers or incomplete preparation")
    if operation == "populate" and (
        run["error_type"] is not None
        or run["error_message"] is not None
        or any(step["status"] != "completed" or step["error_type"] or step["error_message"] for step in steps)
    ):
        raise ValueError(f"Run {run_id}: population did not complete cleanly")
    return run


def validate_certificates(
    db_path: Path, certificates_path: Path, evidence_directory: Path, annotations: object
) -> list[dict[str, Any]]:
    bundle = Certificates.model_validate_json(certificates_path.read_text())
    if bundle.results_db_sha256 != file_sha256(db_path):
        raise ValueError("Certificate final database SHA-256 mismatch")
    ids = [certificate.selected_run_id for certificate in bundle.certificates]
    if len(ids) != len(set(ids)):
        raise ValueError("Duplicate certificate selected run ID")
    origins = [(cert.origin.revision, cert.origin.run_id) for cert in bundle.certificates]
    if len(origins) != len(set(origins)):
        raise ValueError("Duplicate certificate origin")
    validate_publishable_runs(
        db_path,
        campaign_cells(),
        pins=current_pins(),
        drivers=current_drivers(),
        allow_mixed_commits=True,
        campaign=True,
    )
    validation = _validation_report(db_path)
    checked: set[tuple[str, str]] = set()
    con: duckdb.DuckDBPyConnection = cast(Any, duckdb).connect(str(db_path), read_only=True)
    try:
        for cert in bundle.certificates:
            selected = _check_run(con, cert.selected_run_id, cert.cell, "select")
            population = _check_run(con, cert.population.selected_run_id, cert.cell, "populate")
            duplicates = con.execute(
                "SELECT count(*) FROM run WHERE system=? AND db=? AND suite=? AND suite_scale_factor=? "
                "AND operation='select'",
                [cert.cell.system, cert.cell.db, cert.cell.suite, cert.cell.scale_factor],
            ).fetchone()
            if duplicates is None or duplicates[0] != 1:
                raise ValueError("Certificate requires one whole selected SELECT per cell")
            if population["finished_at"] > selected["started_at"]:
                raise ValueError("Population postdates the certified SELECT")
            if population["metadata"]["input"] != selected["metadata"]["input"]:
                raise ValueError("Population and SELECT input fingerprints differ")
            for run_id, origin, fingerprint in (
                (cert.selected_run_id, cert.origin, cert.run_content_sha256),
                (cert.population.selected_run_id, cert.population.origin, cert.population.run_content_sha256),
            ):
                raw = _origin_path(origin, evidence_directory, checked)
                if (
                    run_content_sha256(db_path, run_id) != fingerprint
                    or run_content_sha256(raw, origin.run_id) != fingerprint
                ):
                    raise ValueError(f"Run {run_id}: raw/selected content fingerprint mismatch")
            for annotation in cast(list[dict[str, object]], annotations):
                if annotation["run_id"] == cert.selected_run_id and annotation["preparation"] == "incomplete":
                    raise ValueError("Incomplete preparation cannot be certified")
            audit = PopulationAudit.model_validate_json(
                _artifact_path(cert.population.audit, evidence_directory).read_text()
            )
            lineage = Lineage.model_validate_json(_artifact_path(cert.lineage, evidence_directory).read_text())
            if (
                audit.selected_run_id != cert.population.selected_run_id
                or audit.origin != cert.population.origin
                or lineage.selected_run_id != cert.selected_run_id
                or lineage.origin != cert.origin
                or lineage.population_origin != cert.population.origin
                or lineage.population_selected_run_id != cert.population.selected_run_id
            ):
                raise ValueError("Population/lineage report identity mismatch")
            from ..dbs import get_databases

            database = get_databases()[cert.cell.db]
            suite = database.suite_registry()[cert.cell.suite](
                db=database, name=cert.cell.suite, scale_factor=cert.cell.scale_factor
            )
            expected = dict(suite.expected_table_row_counts())
            actual = {table.name: table.actual_rows for table in audit.tables}
            if (
                not expected
                or len(actual) != len(audit.tables)
                or actual != expected
                or any(table.actual_rows != table.expected_rows for table in audit.tables)
            ):
                raise ValueError("Population audit does not cover every canonical table and row")
            for evidence in [*audit.evidence, *lineage.evidence]:
                _artifact_path(evidence, evidence_directory)
            report = json.loads(_artifact_path(cert.validation, evidence_directory).read_text())
            if _json(report) != _json(validation):
                raise ValueError("Validation report must match clean ALL-answer/iteration validation of the final DB")
    finally:
        con.close()
    return [cert.model_dump() for cert in bundle.certificates]
