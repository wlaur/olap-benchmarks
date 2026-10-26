from __future__ import annotations

import json
import re
from pathlib import Path
from typing import cast

import duckdb

from olap_benchmarks.results.certification import run_content_sha256, validate_certificates, write_validation_report
from olap_benchmarks.results.comparison import write_comparison_manifest
from olap_benchmarks.settings import REPO_ROOT
from scripts.curate_timeout_recovery import Record, read_json, require, rows, sha, write_json


def artifact(path: Path, output: Path) -> Record:
    return {"path": path.relative_to(output).as_posix(), "sha256": sha(path)}


def certify(output: Path, published: Path) -> None:
    database = published / "results.db"
    sql_audit = read_json(output / "sql-audit.json")
    associations = cast(list[Record], read_json(output / "population-associations.json"))
    candidates = [r for r in associations if r["operation"] == "select" and r["status"] == "failed"]
    certificates: list[Record] = []
    uncertified: list[Record] = []
    write_validation_report(database, output / "validation.json")
    with duckdb.connect(str(database), read_only=True) as con:
        for item in candidates:
            run_id = item["selected_run_id"]
            population_id = item["actual_population_selected_run_id"]
            run = rows(con, "select * from run where id=?", [run_id])[0]
            settings = json.loads(run["metadata"])["settings"]
            subsets = {
                key: settings.get(f"OLAP_BENCHMARKS_{key}")
                for key in ("QUERY_INCLUDE", "QUERY_EXCLUDE", "TABLES")
                if settings.get(f"OLAP_BENCHMARKS_{key}")
            }
            if subsets:
                uncertified.append(
                    {
                        "selected_run_id": run_id,
                        "origin": item["origin"],
                        "reason": "Explicit query/table subset fails whole-canonical certificate contract",
                        "subsets": subsets,
                    }
                )
                continue
            plan = read_json(output / item["plan_ref"]["path"])
            cell = next(
                c for c in plan["cells"] if c["key"] == f"{item['db']}:{item['suite']}:sf{item['scale_factor']}"
            )
            positions = [p for p in sql_audit["positions"] if p["selected_run_id"] == run_id]
            require([p["query_name"] for p in positions] == cell["query_names"], "Full query plan changed")
            require(
                all(
                    p["generation_sql_sha256"] == cell["sql_sha256"][p["query_name"]]
                    for p in positions
                    if p["query_name"] in cell["sql_sha256"]
                ),
                "Frozen SQL plan changed",
            )
            require(
                all(s["matches_loaded_sql"] for p in positions for s in p["recorded_sql"]),
                "Recorded SQL differs from the frozen plan",
            )
            require(item["validation_ok"] is True, "Sealed attempt validation did not pass")
            population_logs = [e for e in item["evidence"] if e["path"].endswith("/populate.log")]
            require(len(population_logs) == 1, "Missing or duplicate actual population log")
            log = (output / population_logs[0]["path"]).read_text()
            lines = [
                line
                for line in log.splitlines()
                if f"Verified populated data for {item['suite']} on {item['db']}:" in line
            ]
            require(len(lines) == 1, "Missing or duplicate full population verification")
            matches = re.findall(r"([a-zA-Z_][a-zA-Z_0-9]*): expected=([\d_]+), actual=([\d_]+)", lines[0])
            tables = [
                {
                    "name": name,
                    "expected_rows": int(expected.replace("_", "")),
                    "actual_rows": int(actual.replace("_", "")),
                }
                for name, expected, actual in matches
            ]
            require(
                bool(tables) and all(t["expected_rows"] == t["actual_rows"] for t in tables),
                "Population count mismatch",
            )
            root = output / "certificates" / f"run{run_id}"
            root.mkdir(parents=True, exist_ok=True)
            population_path = root / "population-audit.json"
            write_json(
                population_path,
                {
                    "selected_run_id": population_id,
                    "origin": item["actual_population_origin"],
                    "canonical": True,
                    "complete": True,
                    "tables": tables,
                    "evidence": population_logs,
                },
            )
            lineage_path = root / "lineage.json"
            write_json(
                lineage_path,
                {
                    "selected_run_id": run_id,
                    "origin": item["origin"],
                    "population_origin": item["actual_population_origin"],
                    "population_selected_run_id": population_id,
                    "full_query_plan": True,
                    "all_query_sql_audited": True,
                    "preparation_complete": True,
                    "interrupted": False,
                    "validation_ok": True,
                    "evidence": [
                        *item["evidence"],
                        item["plan_ref"],
                        item["journal_ref"],
                        artifact(output / "sql-audit.json", output),
                        artifact(output / "population-associations.json", output),
                    ],
                },
            )
            certificates.append(
                {
                    "selected_run_id": run_id,
                    "cell": {
                        "system": run["system"],
                        "db": run["db"],
                        "db_version": run["db_version"],
                        "db_driver": run["db_driver"],
                        "suite": run["suite"],
                        "scale_factor": run["suite_scale_factor"],
                        "benchmark_profile": run["benchmark_profile"],
                    },
                    "origin": item["origin"],
                    "run_content_sha256": run_content_sha256(database, run_id),
                    "population": {
                        "selected_run_id": population_id,
                        "origin": item["actual_population_origin"],
                        "run_content_sha256": run_content_sha256(database, population_id),
                        "audit": artifact(population_path, output),
                    },
                    "lineage": artifact(lineage_path, output),
                    "validation": artifact(output / "validation.json", output),
                }
            )
    accepted: list[Record] = []
    for candidate in certificates:
        candidate_path = output / "certificates" / f"run{candidate['selected_run_id']}" / "candidate.json"
        write_json(
            candidate_path,
            {
                "version": 1,
                "policy_id": "certified-terminal-partial-v1",
                "results_db_sha256": sha(database),
                "certificates": [candidate],
            },
        )
        try:
            validate_certificates(database, candidate_path, output, read_json(output / "annotations.json"))
        except ValueError as error:
            uncertified.append(
                {
                    "selected_run_id": candidate["selected_run_id"],
                    "origin": candidate["origin"],
                    "validator_error": str(error),
                    "reason": "The whole-attempt validator rejected this candidate; see validator_error and "
                    "the preserved candidate evidence. No certificate credit.",
                    "candidate": artifact(candidate_path, output),
                }
            )
        else:
            accepted.append(candidate)
    certificates = accepted
    certificate_path = output / "certificates.json"
    write_json(
        certificate_path,
        {
            "version": 1,
            "policy_id": "certified-terminal-partial-v1",
            "results_db_sha256": sha(database),
            "certificates": certificates,
        },
    )
    write_json(
        output / "certification-dispositions.json",
        {
            "certified": [{"selected_run_id": c["selected_run_id"], "origin": c["origin"]} for c in certificates],
            "uncertified": uncertified
            + [
                {
                    "selected_run_id": n,
                    "origin_revision": "baseline",
                    "reason": "No archived full canonical population proof located; no certificate or credit.",
                }
                for n in (658, 660)
            ],
            "deferred": "Entire new 479fab TPC-DS generation, including user-interrupted final/run688; "
            "unfinished validation is not evidence of wrong answers.",
        },
    )
    write_comparison_manifest(
        published,
        annotations_path=output / "annotations.json",
        certificates_path=certificate_path,
        evidence_directory=output,
    )
    print(f"Certified {len(certificates)} complete failed attempts against {sha(database)}")


if __name__ == "__main__":
    certify(REPO_ROOT / "data/temp/timeout-recovery-curated", REPO_ROOT / "site/public/data")
