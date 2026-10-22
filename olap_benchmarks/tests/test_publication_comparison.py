import hashlib
import json
from pathlib import Path
from typing import Any, cast

import duckdb
import pytest

from olap_benchmarks.results.comparison import comparison_families, write_comparison_manifest


def _publication(
    tmp_path: Path, plans: list[tuple[int, str, int, list[str] | None]], *, status: str = "completed"
) -> str:
    con: duckdb.DuckDBPyConnection = cast(Any, duckdb).connect(str(tmp_path / "results.db"))
    try:
        con.execute(
            "CREATE TABLE run (id INTEGER, db VARCHAR, suite VARCHAR, suite_scale_factor INTEGER, "
            "operation VARCHAR, status VARCHAR, finished_at TIMESTAMP, metadata JSON)"
        )
        for run_id, suite, scale, names in plans:
            con.execute(
                "INSERT INTO run VALUES (?, 'duckdb', ?, ?, 'select', ?, '2026-10-21', ?)",
                [run_id, suite, scale, status, json.dumps({"plan": {"query_names": names}})],
            )
    finally:
        con.close()
    db_path = tmp_path / "results.db"
    digest = hashlib.sha256(db_path.read_bytes()).hexdigest()
    (tmp_path / "manifest.json").write_text(
        json.dumps({"revision": "test", "results_db_sha256": digest, "results_db_bytes": db_path.stat().st_size})
    )
    return digest


def test_frozen_tracks_keep_coverage_and_comparable_queries_separate() -> None:
    families = comparison_families(
        {
            "clickbench": {1: ("Q0", "Q28", "Q42")},
            "rtabench": {1: ("0000_raw", "0030_raw", "1000_prepared")},
            "kaggle_airbnb": {1: ("01_count",)},
            "tpc_h": {10: ("01", "02"), 50: ("01", "02")},
        }
    )
    indexed = {str(family["id"]): family for family in families}
    assert indexed["clickbench"]["query_names"] == ["Q0", "Q42"]
    assert indexed["clickbench"]["coverage_query_names"] == ["Q0", "Q28", "Q42"]
    assert indexed["rtabench"]["query_names"] == ["0000_raw", "0030_raw"]
    assert indexed["rtabench_prepared"]["query_names"] == ["1000_prepared"]
    assert indexed["rtabench_prepared"]["weight"] == 0
    assert indexed["kaggle_airbnb"]["weight"] == 0
    assert indexed["tpc_h"]["scale_factors"] == [10, 50]
    assert indexed["tpc_h"]["weight"] == indexed["clickbench"]["weight"] == 1


def test_different_scale_query_sets_require_an_explicit_policy() -> None:
    with pytest.raises(ValueError, match="plans differ across scales"):
        comparison_families({"tpc_h": {10: ("01",), 50: ("01", "02")}})


def test_representation_tracks_do_not_add_headline_family_votes() -> None:
    families = comparison_families(
        {
            "parquet_analytics": {10: ("01", "02")},
            "json_representation": {10: ("raw_01", "raw_02", "prepared_01", "prepared_02")},
        }
    )
    indexed = {str(family["id"]): family for family in families}
    assert indexed["json_representation_raw"]["query_names"] == ["raw_01", "raw_02"]
    assert indexed["json_representation_prepared"]["query_names"] == ["prepared_01", "prepared_02"]
    assert all(family["weight"] == 0 for family in families)


def test_unknown_json_representation_track_is_rejected() -> None:
    with pytest.raises(ValueError, match="undeclared track"):
        comparison_families({"json_representation": {10: ("raw_01", "other")}})


def test_unknown_rtabench_track_is_rejected() -> None:
    with pytest.raises(ValueError, match="undeclared track"):
        comparison_families({"rtabench": {1: ("0000_raw", "other")}})


def test_metadata_is_bound_to_unchanged_database_and_real_registrations(tmp_path: Path) -> None:
    digest = _publication(tmp_path, [(1, "tpc_h", 10, ["01", "02"]), (2, "tpc_h", 50, ["01", "02"])])
    before = (tmp_path / "results.db").read_bytes()
    destination = write_comparison_manifest(tmp_path)
    data = json.loads(destination.read_text())
    assert data["results_db_sha256"] == digest
    assert data["retrospective"] is True
    assert data["families"][0]["scale_factors"] == [10, 50]
    assert any(item["db"] == "polars" and item["suite"] == "tpc_h" for item in data["registrations"])
    assert not any(item["db"] == "tdengine" for item in data["registrations"])
    assert (tmp_path / "results.db").read_bytes() == before


def test_entirely_failed_new_suite_keeps_its_full_plan_and_registrations(tmp_path: Path) -> None:
    names = ["01_filter", "02_join", "03_aggregate"]
    _publication(tmp_path, [(1, "join_order", 10, names)], status="failed")
    data = json.loads(write_comparison_manifest(tmp_path).read_text())
    assert len(data["families"]) == 1
    assert data["families"][0]["coverage_query_names"] == names
    assert data["families"][0]["query_names"] == names
    assert data["families"][0]["scale_factors"] == [10]
    assert any(item["db"] == "duckdb" and item["suite"] == "join_order" for item in data["registrations"])


def test_hash_mismatch_preserves_previous_policy(tmp_path: Path) -> None:
    _publication(tmp_path, [(1, "tpc_h", 10, ["01"])])
    destination = tmp_path / "comparison.json"
    destination.write_text("previous policy")
    manifest_path = tmp_path / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["results_db_sha256"] = "0" * 64
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="does not match"):
        write_comparison_manifest(tmp_path)
    assert destination.read_text() == "previous policy"


@pytest.mark.parametrize("names", [None, [], ["01", "01"]])
def test_incomplete_recorded_plans_are_rejected(tmp_path: Path, names: list[str] | None) -> None:
    _publication(tmp_path, [(1, "tpc_h", 10, names)])
    with pytest.raises(ValueError, match="missing or duplicate"):
        write_comparison_manifest(tmp_path)


def test_differing_engine_plans_do_not_shrink_to_a_successful_intersection(tmp_path: Path) -> None:
    _publication(tmp_path, [(1, "tpc_h", 10, ["01", "02"]), (2, "tpc_h", 10, ["01"])])
    with pytest.raises(ValueError, match="different recorded query plans"):
        write_comparison_manifest(tmp_path)


def test_annotations_apply_only_to_the_documented_run(tmp_path: Path) -> None:
    _publication(tmp_path, [(10, "tpc_h", 10, ["01"])])
    annotations = [
        {
            "db": "duckdb",
            "suite": "tpc_h",
            "scale_factor": 10,
            "run_id": run_id,
            "preparation": "unknown",
            "tuning": None,
            "summary": "Documented limitation",
            "evidence_url": "https://example.com/report",
        }
        for run_id in (9, 10)
    ]
    path = tmp_path / "annotations.json"
    path.write_text(json.dumps(annotations))
    data = json.loads(write_comparison_manifest(tmp_path, annotations_path=path).read_text())
    assert data["annotations"] == [annotations[1]]


def test_sourced_annotation_cannot_be_attached_to_another_engine(tmp_path: Path) -> None:
    _publication(tmp_path, [(10, "tpc_h", 10, ["01"])])
    path = tmp_path / "annotations.json"
    path.write_text(
        json.dumps(
            [
                {
                    "db": "monetdb",
                    "suite": "tpc_h",
                    "scale_factor": 10,
                    "run_id": 10,
                    "preparation": "observed",
                    "tuning": None,
                    "summary": "A finding about another engine",
                    "evidence_url": "https://example.com/report",
                }
            ]
        )
    )
    with pytest.raises(ValueError, match="identity does not match"):
        write_comparison_manifest(tmp_path, annotations_path=path)
