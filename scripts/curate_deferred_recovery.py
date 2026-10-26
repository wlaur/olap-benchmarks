from __future__ import annotations

import argparse
import copy
import shutil
import subprocess
from dataclasses import asdict
from pathlib import Path
from typing import cast

import duckdb

from olap_benchmarks.matrix import current_drivers, current_pins
from olap_benchmarks.results.campaign import campaign_cells
from olap_benchmarks.results.certification import validate_certificates
from olap_benchmarks.results.comparison import write_comparison_manifest
from olap_benchmarks.results.merge import _sync_sequences
from olap_benchmarks.results.publishing import validate_publishable_runs
from olap_benchmarks.results.validation import validate_results
from olap_benchmarks.settings import REPO_ROOT
from scripts.certify_timeout_recovery import artifact, certify
from scripts.curate_timeout_recovery import (
    TABLES,
    Record,
    Source,
    baseline_audit,
    checked_source,
    evidence_copy,
    full_run_audit,
    import_source,
    integer,
    origin,
    read_json,
    require,
    rows,
    sha,
    sql_audit,
    sql_path,
    text_sha,
    write_json,
)

REVISION = "2026-10-timeout-recovery-deferred-publication"
BASELINE_SHA = "c47a7f7c04963684a2125a63578cf7fdca84c4c028ed2e05ceece1ed7ec4ac0c"
MEASUREMENT_HEAD = "743cc81f1b3d0e689d86aabbc2d6c1019ef7a962"
REPORT_URL = (
    "https://github.com/wlaur/olap-benchmarks/blob/main/docs/campaigns/2026-10-timeout-recovery-deferred-results.md"
)
OUTPUT = REPO_ROOT / "data/temp/deferred-publication"
PUBLISHED = REPO_ROOT / "site/public/data"
DESTINATION = REPO_ROOT / "results" / f"{REVISION}.db"
BASELINE = Path("/Users/williamlauren/repos/olap-benchmarks/site/public/data/results.db")


def sources() -> tuple[Source, ...]:
    return (
        Source(
            "2026-10-timeout-recovery-final",
            str(OUTPUT / "sources/prior-final"),
            "2d55a9915b80101885eccb92048401d75e02e23b279046d86f475f5dfb07000c",
            (685, 686),
            "2026-10-timeout-recovery-final-plan.json",
        ),
        Source(
            "2026-10-timeout-recovery-deferred-20261026",
            str(REPO_ROOT),
            "6e6baccfde803dc6a098f4bbdc7c90e269bf9b489f0d787eb51793aa55d6ffe2",
            tuple(range(685, 696)),
            "2026-10-timeout-recovery-deferred-plan.json",
        ),
    )


def query_manifest(baseline: Record) -> tuple[Record, list[Record]]:
    manifest = copy.deepcopy(baseline)
    adjustments: list[Record] = []
    for name in ("31", "49", "65", "85"):
        path = f"olap_benchmarks/suites/tpc_ds/queries/postgres/{name}.sql"
        sql = subprocess.check_output(["git", "show", f"{MEASUREMENT_HEAD}:{path}"], text=True)
        for database in ("postgres", "timescaledb"):
            manifest["tpc_ds"][name]["db_overrides"][database] = sql
            adjustments.append(
                {"suite": "tpc_ds", "query": name, "db": database, "sha256": text_sha(sql), "source": path}
            )
    return manifest, adjustments


def archive_entries() -> dict[str, Record]:
    old = read_json(REPO_ROOT / "data/temp/deferred-preparation/archive-index.json")["files"]
    new = read_json(REPO_ROOT / "data/temp/deferred-archive/archive-manifest.json")["files"]
    entries: dict[str, Record] = {e["original_path"]: e for e in new}
    prefix = "/Users/williamlauren/.t3/worktrees/olap-benchmarks/t3-69104cbd/"
    prior = sources()[0].root(REPO_ROOT)
    entries.update(
        {str(prior / e["original_path"].removeprefix(prefix)): e for e in old if e["original_path"].startswith(prefix)}
    )
    return entries


def source_evidence() -> dict[str, Record]:
    archive = archive_entries()
    result: dict[str, Record] = {}
    for source in sources():
        root = source.root(REPO_ROOT)
        checked_source(source.database(REPO_ROOT), source.sha256)
        journal_path = root / "data/temp/timeout-recovery/state.json"
        journal = read_json(journal_path)
        plan_path = root / "docs/campaigns" / source.plan_name
        checked_source(plan_path, journal["plan_sha256"])
        attempts: list[Record] = []
        for attempt in journal["attempts"]:
            if not set(source.runs).intersection(attempt["runs"].values()):
                continue
            require(attempt["validation_ok"] and attempt["cleanup_confirmed"], "Unvalidated or unclean attempt")
            directory = root / "data/temp/timeout-recovery" / attempt["id"]
            evidence = [
                evidence_copy(path, OUTPUT, archive)
                for path in sorted(directory.iterdir())
                if path.suffix in (".json", ".log", ".txt")
            ]
            attempts.append({**attempt, "evidence": evidence})
        result[source.revision] = {
            "journal_ref": evidence_copy(journal_path, OUTPUT, archive),
            "plan_ref": evidence_copy(plan_path, OUTPUT, archive),
            "attempts": attempts,
        }
        link = OUTPUT / f"{source.revision}.db"
        if not link.exists():
            link.symlink_to(source.database(REPO_ROOT))
    write_json(OUTPUT / "sealed-evidence.json", result)
    return result


def build() -> None:
    require(not DESTINATION.exists(), "Refusing to overwrite the new staging revision")
    checked_source(BASELINE, BASELINE_SHA)
    manifest, adjustments = query_manifest(read_json(PUBLISHED / "queries.json"))
    write_json(OUTPUT / "queries.json", manifest)
    write_json(OUTPUT / "query-manifest-adjustments.json", adjustments)
    comparison = read_json(PUBLISHED / "comparison.json")
    require(comparison["results_db_sha256"] == BASELINE_SHA, "Unbound publication baseline")
    info = source_evidence()
    shutil.copyfile(BASELINE, DESTINATION)
    replacements: list[Record] = []
    with duckdb.connect(str(DESTINATION)) as con:
        original_ids = [r["id"] for r in rows(con, "select id from run order by id")]
        for source in sources():
            con.execute(f"attach {sql_path(source.database(REPO_ROOT))} as src (read_only)")
            replacements.extend(import_source(con, source, OUTPUT))
            con.execute("detach src")
        replaced = [r["replaced_baseline_run_id"] for r in replacements]
        require(len(set(replaced)) == 13 and set(replaced) <= set(original_ids), "Duplicate or chained replacement")
        retained = sorted(set(original_ids) - set(replaced))
        _sync_sequences(con)
        write_json(OUTPUT / "selection.json", replacements)
        write_json(
            OUTPUT / "baseline-identity-map.json",
            {"origin_sha256": BASELINE_SHA, "retained_run_ids": retained, "replaced_run_ids": replaced},
        )
        baseline_audit(con, BASELINE, OUTPUT, expected_sha256=BASELINE_SHA)
        associations: list[Record] = []
        annotations = [a for a in comparison["annotations"] if a["run_id"] in retained]
        for item in replacements:
            source = next(s for s in sources() if s.revision == item["origin"]["revision"])
            source_info = info[source.revision]
            attempt = next(a for a in source_info["attempts"] if item["origin"]["run_id"] in a["runs"].values())
            population = attempt["runs"]["populate"]
            mapped_pop = next(r["selected_run_id"] for r in replacements if r["origin"] == origin(source, population))
            associations.append(
                {
                    **item,
                    "actual_population_origin": origin(source, population),
                    "actual_population_selected_run_id": mapped_pop,
                    "attempt": attempt["id"],
                    "validation_ok": attempt["validation_ok"],
                    "evidence": attempt["evidence"],
                    "plan_ref": source_info["plan_ref"],
                    "journal_ref": source_info["journal_ref"],
                }
            )
            if item["operation"] == "select":
                annotations.append(
                    {
                        "db": item["db"],
                        "suite": item["suite"],
                        "scale_factor": item["scale_factor"],
                        "run_id": item["selected_run_id"],
                        "preparation": "observed",
                        "tuning": None,
                        "summary": (
                            f"Whole canonical attempt {source.revision}/run{item['origin']['run_id']} "
                            f"used its own complete population run{population}. "
                            "All recorded successful answers and iterations validated. "
                            "Raw failures and missing measurements remain unchanged."
                        ),
                        "evidence_url": REPORT_URL,
                    }
                )
        write_json(OUTPUT / "population-associations.json", associations)
        write_json(OUTPUT / "annotations.json", annotations)
        full_run_audit(con, OUTPUT)
        audit = sql_audit(con, manifest, OUTPUT)
        audit["frozen_manifest_commit"] = MEASUREMENT_HEAD
        audit["generation_rule"] = (
            "Retained baseline SQL plus four approved PostgreSQL/Timescale overrides; "
            "every TPC-DS position checked against its actual measurement commit."
        )
        write_json(OUTPUT / "sql-audit.json", audit)
        require(integer(con, "select count(*) from system_snapshot") == 1, "Multiple snapshots")
    report = validate_results(db_path=DESTINATION, include_failed=True, mark=False)
    require(report.ok and report.marks_applied == report.marks_cleared == 0, report.format())
    write_json(OUTPUT / "readonly-validation.json", asdict(report))
    validate_publishable_runs(
        DESTINATION,
        campaign_cells(),
        pins=current_pins(),
        drivers=current_drivers(),
        allow_mixed_commits=True,
        campaign=True,
    )
    write_json(
        OUTPUT / "build-summary.json",
        {
            "revision": REVISION,
            "staging_sha256": sha(DESTINATION),
            "replacements": 13,
            "all_five_tables_preserved_except_known_relational_ids": True,
            "retained_baseline_rows_exact": True,
            "unexpected_marks": 0,
        },
    )


def export() -> None:
    require(sha(DESTINATION) == read_json(OUTPUT / "build-summary.json")["staging_sha256"], "Staging changed")
    subprocess.run(
        [str(REPO_ROOT / ".venv/bin/olap"), "publish", REVISION, "--campaign", "--report-url", REPORT_URL], check=True
    )
    shutil.copyfile(OUTPUT / "queries.json", PUBLISHED / "queries.json")
    with duckdb.connect(str(PUBLISHED / "results.db"), read_only=True) as con:
        con.execute(f"attach {sql_path(DESTINATION)} as staging (read_only)")
        for table in TABLES:
            for left, right in ((table, f"staging.{table}"), (f"staging.{table}", table)):
                require(
                    integer(con, f"select count(*) from ((select * from {left}) except all (select * from {right}))")
                    == 0,
                    f"Publisher changed {table}",
                )
    write_json(
        OUTPUT / "export-summary.json",
        {
            "results_db_sha256": sha(PUBLISHED / "results.db"),
            "all_five_tables_exactly_unchanged": True,
            "unexpected_marks": 0,
        },
    )


def retain_certificate(certificate: Record, output: Path) -> Record:
    retained = copy.deepcopy(certificate)
    for field, directory in (("lineage", retained), ("population-audit", retained["population"])):
        old_ref = directory["lineage" if field == "lineage" else "audit"]
        old_path = output / "baseline-proof" / old_ref["path"]
        checked_source(old_path, old_ref["sha256"])
        report = read_json(old_path)
        report["evidence"] = [{**entry, "path": "baseline-proof/" + entry["path"]} for entry in report["evidence"]]
        path = output / "retained-certificates" / f"run{retained['selected_run_id']}" / f"{field}.json"
        write_json(path, report)
        directory["lineage" if field == "lineage" else "audit"] = artifact(path, output)
    retained["validation"] = artifact(output / "validation.json", output)
    return retained


def certificates() -> None:
    certify(OUTPUT, PUBLISHED)
    pack = read_json(OUTPUT / "certificates.json")
    require(len(pack["certificates"]) == 5, "Every new failed SELECT must pass the unchanged certificate policy")
    old = read_json(OUTPUT / "baseline-proof/certificates.json")
    replaced = {r["replaced_baseline_run_id"] for r in cast(list[Record], read_json(OUTPUT / "selection.json"))}
    preserved = [retain_certificate(c, OUTPUT) for c in old["certificates"] if c["selected_run_id"] not in replaced]
    require({c["selected_run_id"] for c in preserved} == {686, 709, 711}, "Unexpected retained certificate set")
    pack["certificates"] = sorted([*preserved, *pack["certificates"]], key=lambda c: c["selected_run_id"])
    write_json(OUTPUT / "certificates.json", pack)
    validate_certificates(
        PUBLISHED / "results.db", OUTPUT / "certificates.json", OUTPUT, read_json(OUTPUT / "annotations.json")
    )
    with duckdb.connect(str(PUBLISHED / "results.db"), read_only=True) as con:
        failed_ids = {r["id"] for r in rows(con, "select id from run where operation='select' and status='failed'")}
    uncertified = failed_ids - {c["selected_run_id"] for c in pack["certificates"]}
    require(uncertified == {688}, "Unexpected uncertified failed attempt")
    write_json(
        OUTPUT / "certification-dispositions.json",
        {
            "certified": [c["selected_run_id"] for c in pack["certificates"]],
            "uncertified": [
                {
                    "selected_run_id": 688,
                    "reason": "Original MonetDB Chat Threads SF10 excluded Q15; whole cell remains zero.",
                }
            ],
            "analytical16": (
                "New warmup failed; no measurement and no further retry. "
                "Partial-query policy applies only to serial SELECT."
            ),
        },
    )
    write_comparison_manifest(
        PUBLISHED,
        annotations_path=OUTPUT / "annotations.json",
        certificates_path=OUTPUT / "certificates.json",
        evidence_directory=OUTPUT,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("build", "export", "certificates"))
    arguments = parser.parse_args()
    {"build": build, "export": export, "certificates": certificates}[cast(str, arguments.phase)]()
