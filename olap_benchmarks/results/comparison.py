from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any, TypedDict, cast

import duckdb

from ..matrix import registered_operations
from ..query_registry import ANSWER_COMPARISON_EXCLUSIONS
from ..settings import REPO_ROOT, SUITE_SPECS


class ComparisonAnnotation(TypedDict):
    db: str
    suite: str
    scale_factor: int | None
    run_id: int | None
    preparation: str
    tuning: str | None
    summary: str
    evidence_url: str


def comparison_families(
    plans: Mapping[str, Mapping[int, tuple[str, ...]]],
) -> list[dict[str, object]]:
    families: list[dict[str, object]] = []
    for suite, spec in SUITE_SPECS.items():
        scales = plans.get(suite)
        if not scales:
            continue
        query_sets = set(scales.values())
        if len(query_sets) != 1:
            raise ValueError(f"{suite}: recorded query plans differ across scales")
        names = next(iter(query_sets))
        excluded = {
            name
            for (excluded_suite, name, db) in ANSWER_COMPARISON_EXCLUSIONS
            if excluded_suite == suite and db is None
        }
        tracks: list[tuple[str, str, list[str]]] = [
            (suite, "smoke" if spec.public_role == "smoke" else "raw", list(names))
        ]
        if suite == "rtabench":
            tracks = [
                (suite, "raw", [name for name in names if name.startswith("0")]),
                ("rtabench_prepared", "prepared", [name for name in names if name.startswith("1")]),
            ]
            if sum(len(track_names) for _, _, track_names in tracks) != len(names):
                raise ValueError("RTABench query plan contains an undeclared track")
        elif suite == "json_representation":
            tracks = [
                ("json_representation_raw", "raw", [name for name in names if name.startswith("raw_")]),
                ("json_representation_prepared", "prepared", [name for name in names if name.startswith("prepared_")]),
            ]
            if sum(len(track_names) for _, _, track_names in tracks) != len(names):
                raise ValueError("JSON representation query plan contains an undeclared track")
        for family_id, track, coverage_names in tracks:
            if not coverage_names:
                continue
            title = spec.label
            if track == "prepared":
                title += " prepared fields" if suite == "json_representation" else " prepared aggregates"
            families.append(
                {
                    "id": family_id,
                    "suite": suite,
                    "title": title,
                    "weight": 1 if track == "raw" and suite not in ("parquet_analytics", "json_representation") else 0,
                    "scale_factors": sorted(scales),
                    "query_names": [name for name in coverage_names if name not in excluded],
                    "coverage_query_names": coverage_names,
                    "track": track,
                }
            )
    return families


def _recorded_plans(db_path: Path) -> tuple[dict[str, dict[int, tuple[str, ...]]], dict[int, tuple[str, str, int]]]:
    plans: dict[str, dict[int, tuple[str, ...]]] = {}
    runs: dict[int, tuple[str, str, int]] = {}
    con: duckdb.DuckDBPyConnection = cast(Any, duckdb).connect(str(db_path), read_only=True)
    try:
        rows = con.execute(
            """
            SELECT id, db, suite, suite_scale_factor,
                cast(json_extract(metadata, '$.plan.query_names') AS varchar[])
            FROM run
            WHERE operation = 'select' AND status IN ('completed', 'failed') AND finished_at IS NOT NULL
                AND json_extract(metadata, '$.invalidation') IS NULL
            ORDER BY suite, suite_scale_factor, id
            """
        ).fetchall()
        for run_id, db, suite, scale, raw_names in rows:
            if suite not in SUITE_SPECS:
                raise ValueError(f"Unknown published suite: {suite}")
            names = tuple(cast(list[str], raw_names or []))
            if not names or len(names) != len(set(names)):
                raise ValueError(f"Run {run_id}: missing or duplicate recorded query plan")
            scales = plans.setdefault(str(suite), {})
            previous = scales.setdefault(int(scale), names)
            if previous != names:
                raise ValueError(f"{suite} SF{scale}: selected engines have different recorded query plans")
            runs[int(run_id)] = (str(db), str(suite), int(scale))
    finally:
        con.close()
    return plans, runs


def _annotations(path: Path | None, runs: Mapping[int, tuple[str, str, int]]) -> list[ComparisonAnnotation]:
    if path is None:
        return []
    raw: object = json.loads(path.read_text())
    if not isinstance(raw, list):
        raise TypeError("Comparison annotations must be an array")
    annotations: list[ComparisonAnnotation] = []
    required = set(ComparisonAnnotation.__required_keys__)
    for item in cast(list[object], raw):
        if not isinstance(item, dict):
            raise TypeError("Each comparison annotation must be an object")
        record = cast(dict[str, object], item)
        if set(record) != required:
            raise ValueError("Comparison annotation fields do not match the publication contract")
        if record["preparation"] not in ("incomplete", "observed", "unknown"):
            raise ValueError("Unknown preparation evidence level")
        if not all(isinstance(record[key], str) for key in ("db", "suite", "summary", "evidence_url")):
            raise ValueError("Comparison annotations require textual identities, summaries and sources")
        if not str(record["evidence_url"]).startswith("https://"):
            raise ValueError("Comparison annotation evidence must have an HTTPS source")
        run_id = record["run_id"]
        if not isinstance(run_id, int) or isinstance(run_id, bool):
            raise TypeError("Preparation and tuning annotations require an explicit integer run ID")
        if run_id not in runs:
            continue
        db, suite, scale = runs[run_id]
        if record["db"] != db or record["suite"] != suite or record["scale_factor"] != scale:
            raise ValueError(f"Annotation identity does not match selected run {run_id}")
        annotations.append(cast(ComparisonAnnotation, record))
    return annotations


def write_comparison_manifest(
    output_directory: Path,
    *,
    annotations_path: Path | None = None,
) -> Path:
    manifest = cast(dict[str, object], json.loads((output_directory / "manifest.json").read_text()))
    db_path = output_directory / "results.db"
    with db_path.open("rb") as handle:
        digest = hashlib.file_digest(handle, "sha256").hexdigest()
    if digest != manifest.get("results_db_sha256") or db_path.stat().st_size != manifest.get("results_db_bytes"):
        raise ValueError("Published results.db does not match manifest.json")
    plans, runs = _recorded_plans(db_path)
    registrations = [
        {
            "db": db,
            "suite": suite,
            "scale_factors": list(SUITE_SPECS[suite].scale_factors),
            "operations": list(operations),
        }
        for db, suites in sorted(registered_operations().items())
        for suite, operations in sorted(suites.items())
        if suite in plans
    ]
    baseline_annotations = REPO_ROOT / "docs/campaigns/2026-10-publication-annotations.json"
    annotation_source = annotations_path
    if annotation_source is None and manifest.get("revision") == "2026-10-expanded":
        annotation_source = baseline_annotations
    result: dict[str, object] = {
        "version": 1,
        "results_db_sha256": digest,
        "policy_id": "fixed-workloads-v1",
        "retrospective": True,
        "families": comparison_families(plans),
        "registrations": registrations,
        "answer_comparison_exclusions": [
            {"suite": suite, "query_name": name, "db": db, "reason": reason}
            for (suite, name, db), reason in sorted(
                ANSWER_COMPARISON_EXCLUSIONS.items(), key=lambda item: tuple(value or "" for value in item[0])
            )
            if suite in plans
        ],
        "annotations": _annotations(annotation_source, runs),
    }
    destination = output_directory / "comparison.json"
    destination.write_text(f"{json.dumps(result, indent=2)}\n")
    return destination
