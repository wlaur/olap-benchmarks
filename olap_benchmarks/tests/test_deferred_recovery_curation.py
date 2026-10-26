from __future__ import annotations

import copy
from pathlib import Path

import pytest

from scripts import curate_deferred_recovery as curator
from scripts.curate_timeout_recovery import Record, read_json, sha, write_json


def test_new_sql_generation_changes_only_the_eight_approved_overrides() -> None:
    baseline = read_json(curator.PUBLISHED / "queries.json")
    original = copy.deepcopy(baseline)
    changed, adjustments = curator.query_manifest(baseline)
    assert baseline == original
    assert {(a["query"], a["db"]) for a in adjustments} == {
        (query, database) for query in ("31", "49", "65", "85") for database in ("postgres", "timescaledb")
    }
    for query in ("31", "49", "65", "85"):
        for database in ("postgres", "timescaledb"):
            changed["tpc_ds"][query]["db_overrides"][database] = original["tpc_ds"][query]["db_overrides"].get(database)
            if database not in original["tpc_ds"][query]["db_overrides"]:
                del changed["tpc_ds"][query]["db_overrides"][database]
    assert changed == original


def certificate_fixture(root: Path) -> Record:
    write_json(
        root / "baseline-proof/lineage.json",
        {"origin": {"run_id": 685}, "evidence": [{"path": "raw/audit.json", "sha256": "evidence-digest"}]},
    )
    write_json(
        root / "baseline-proof/population.json",
        {
            "tables": [{"name": "hits", "actual_rows": 99_997_497}],
            "evidence": [{"path": "logs/populate.log", "sha256": "population-digest"}],
        },
    )
    write_json(root / "validation.json", {"results_db_sha256": "new-final-db"})
    return {
        "selected_run_id": 686,
        "origin": {"run_id": 685, "revision": "original"},
        "run_content_sha256": "scientific-content",
        "lineage": {"path": "lineage.json", "sha256": sha(root / "baseline-proof/lineage.json")},
        "population": {
            "selected_run_id": 685,
            "origin": {"run_id": 684},
            "audit": {"path": "population.json", "sha256": sha(root / "baseline-proof/population.json")},
        },
        "validation": {"path": "old-validation.json", "sha256": "old"},
    }


def test_retained_certificate_preserves_origin_and_original_proofs(tmp_path: Path) -> None:
    certificate = certificate_fixture(tmp_path)
    before = copy.deepcopy(certificate)
    result = curator.retain_certificate(certificate, tmp_path)
    assert certificate == before
    assert result["origin"] == certificate["origin"]
    assert result["run_content_sha256"] == "scientific-content"
    assert result["validation"]["sha256"] == sha(tmp_path / "validation.json")
    lineage = read_json(tmp_path / result["lineage"]["path"])
    assert lineage["evidence"] == [{"path": "baseline-proof/raw/audit.json", "sha256": "evidence-digest"}]
    population = read_json(tmp_path / result["population"]["audit"]["path"])
    assert population["tables"] == [{"name": "hits", "actual_rows": 99_997_497}]
    assert read_json(tmp_path / "baseline-proof/lineage.json")["evidence"][0]["path"] == "raw/audit.json"


def test_changed_retained_proof_is_rejected(tmp_path: Path) -> None:
    certificate = certificate_fixture(tmp_path)
    (tmp_path / "baseline-proof/lineage.json").write_text("{}")
    with pytest.raises(ValueError, match="Sealed source changed"):
        curator.retain_certificate(certificate, tmp_path)
