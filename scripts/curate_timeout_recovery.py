from __future__ import annotations

import argparse
import csv
import hashlib
import json
import shutil
import subprocess
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, cast

import duckdb

from olap_benchmarks.dbs import get_databases
from olap_benchmarks.matrix import current_drivers, current_pins
from olap_benchmarks.results import get_results_head_revision
from olap_benchmarks.results.campaign import campaign_cells
from olap_benchmarks.results.comparison import write_comparison_manifest
from olap_benchmarks.results.merge import RUN_CELL_KEY, _sync_sequences
from olap_benchmarks.results.publishing import validate_publishable_runs
from olap_benchmarks.results.validation import validate_results
from olap_benchmarks.settings import REPO_ROOT, DatabaseName, SuiteName

REVISION = "2026-10-timeout-recovery-curated"
OLD_SOURCE = "efaae1c9f31a9c917b074e3c60da74c0572d34e6"
BASELINE_SHA = "832e28599501d125f2f4e0e35bb5251803955fbe1f6066f02c949c9df59f833f"
REPORT_URL = "https://github.com/wlaur/olap-benchmarks/blob/main/docs/campaigns/2026-10-timeout-recovery-curated.md"
TABLES = ("system_snapshot", "run", "run_step", "run_metric", "query_execution")
RELATIONS = {
    "system_snapshot": {"id": "system_snapshot"},
    "run": {"id": "run", "system_snapshot_id": "system_snapshot"},
    "run_step": {"id": "run_step", "run_id": "run"},
    "run_metric": {"id": "run_metric", "run_id": "run"},
    "query_execution": {"id": "query_execution", "run_id": "run", "run_step_id": "run_step"},
}
type Record = dict[str, Any]


@dataclass(frozen=True)
class Source:
    revision: str
    workspace: str
    sha256: str
    runs: tuple[int, ...]
    plan_name: str

    def root(self, worktrees: Path) -> Path:
        return worktrees / self.workspace

    def database(self, worktrees: Path) -> Path:
        return self.root(worktrees) / "results" / f"{self.revision}.db"


SOURCES = (
    Source(
        "2026-10-timeout-recovery",
        "t3-416ee4f7",
        "4ee1ce8950bd62375533a06535d719c1156469aa1869d29d6b4520f0fe788e57",
        (685, 686, 689, 690, 691, 692, 693),
        "2026-10-timeout-recovery-plan.json",
    ),
    Source(
        "2026-10-timeout-recovery-continuation",
        "t3-01c9b611",
        "8cb46edbd80a09d784a767359f3569454dd5064db9a89b147818140b119ebcae",
        (685, 686, 687, 688, 689),
        "2026-10-timeout-recovery-continuation-plan.json",
    ),
    Source(
        "2026-10-timeout-recovery-remaining",
        "t3-f4c709e8",
        "866d71728da75556a3c1dded5c62528b356c04b42f39c56b281b79d12f3b32a6",
        (689, 690, 691, 692),
        "2026-10-timeout-recovery-remaining-plan.json",
    ),
    Source(
        "2026-10-timeout-recovery-corrective",
        "t3-f9877fbb",
        "4f2a23a776effef7cb85c4b3cc3cc5492e555e2957117819db0d7d79be7e4dba",
        tuple(range(685, 698)),
        "2026-10-timeout-recovery-corrective-plan.json",
    ),
    Source(
        "2026-10-timeout-recovery-final",
        "t3-69104cbd",
        "2d55a9915b80101885eccb92048401d75e02e23b279046d86f475f5dfb07000c",
        (),
        "2026-10-timeout-recovery-final-plan.json",
    ),
)


ANNOTATION_DETAILS = {
    (
        "2026-10-timeout-recovery",
        690,
    ): "Canonical population completed, but SELECT explicitly excluded 15_part_type_distribution. "
    "Its 14 successful queries remain visible; this attempt has no whole-canonical certificate "
    "and earns zero overall credit. The later Q16 failure and raw outcomes are unchanged.",
    (
        "2026-10-timeout-recovery-corrective",
        691,
    ): "Canonical population completed. The post-SELECT session_baseline_after check failed when Q32 "
    "reached the 300-second query deadline plus 30-second outer grace. Independent cleanup succeeded "
    "and answer validation was clean; this is not a wrong-answer finding or quarantine. "
    "The 96 successful measurements remain visible, but the uncertified attempt earns zero overall credit.",
}


def sha(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def text_sha(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, default=str, allow_nan=False) + "\n")


def read_json(path: Path) -> Record:
    return cast(Record, json.loads(path.read_text()))


def rows(con: duckdb.DuckDBPyConnection, sql: str, parameters: Sequence[object] = ()) -> list[Record]:
    cursor = con.execute(sql, parameters)
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]


def integer(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    row = con.execute(sql).fetchone()
    assert row is not None
    return int(row[0])


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def sql_path(path: Path) -> str:
    return "'" + path.as_posix().replace("'", "''") + "'"


def origin(source: Source, run_id: int) -> Record:
    return {"revision": source.revision, "results_db_sha256": source.sha256, "run_id": run_id}


def checked_source(path: Path, digest: str) -> None:
    require(sha(path) == digest, f"Sealed source changed: {path}")


def evidence_copy(path: Path, output: Path, archive: dict[str, Record]) -> Record:
    entry = archive[str(path)]
    checked_source(path, entry["sha256"])
    dest = output / "evidence" / entry["sha256"] / path.name
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(path, dest)
    return {"path": dest.relative_to(output).as_posix(), "sha256": entry["sha256"]}


def make_maps(con: duckdb.DuckDBPyConnection, source: Source) -> None:
    ids = ",".join(map(str, source.runs))
    con.execute(f"create or replace temp table chosen as select * from src.run where id in ({ids})")
    require(integer(con, "select count(*) from chosen") == len(source.runs), "Missing selected run")
    require(
        integer(con, "select count(*) from chosen where status='running' or finished_at is null") == 0,
        "Unfinished selected run",
    )
    con.execute("""create or replace temp table wanted_snapshot as
        select distinct s.* from src.system_snapshot s join chosen r on r.system_snapshot_id=s.id""")
    fields = [row[0] for row in con.execute("describe system_snapshot").fetchall() if row[0] != "id"]
    require(len(fields) == 8, "Expected exactly eight non-ID snapshot fields")
    equality = " and ".join(f's."{field}" is not distinct from d."{field}"' for field in fields)
    con.execute(f"""create or replace temp table map_system_snapshot as
        select s.id old_id, min(d.id) new_id from wanted_snapshot s
        left join system_snapshot d on {equality} group by s.id""")
    missing = integer(con, "select count(*) from map_system_snapshot where new_id is null")
    require(missing == 0, f"Snapshots differ in exact scalar/JSON fields: {source.revision}; cannot collapse")
    for table in TABLES[1:]:
        offset = integer(con, f"select coalesce(max(id),0) from {table}")
        selection = (
            "select id from chosen"
            if table == "run"
            else (f"select id from src.{table} where run_id in (select id from chosen)")
        )
        con.execute(f"""create or replace temp table map_{table} as
            select id old_id, {offset} + row_number() over (order by id) new_id from ({selection})""")


def projected(table: str) -> tuple[str, str]:
    relations = RELATIONS[table]
    joins = " ".join(
        f'left join map_{target} m_{field} on s."{field}"=m_{field}.old_id' for field, target in relations.items()
    )
    return joins, ", ".join(f'm_{field}.new_id as "{field}"' for field in relations)


def import_source(con: duckdb.DuckDBPyConnection, source: Source, output: Path) -> list[Record]:
    make_maps(con, source)
    replacements: list[Record] = []
    for run in rows(con, "select * from chosen order by id"):
        condition = " and ".join(f'"{key}" is not distinct from ?' for key in RUN_CELL_KEY)
        values: list[object] = [run[key] for key in RUN_CELL_KEY]
        if run["operation"] == "analytical":
            condition += " and json_extract(metadata,'$.analytical.clients') = json_extract(?,'$.analytical.clients')"
            values.append(run["metadata"])
        old = rows(con, f"select id from run where {condition}", values)
        require(len(old) == 1, f"Expected one baseline replacement for {source.revision}/{run['id']}: {old}")
        old_id = int(old[0]["id"])
        new_id = integer(con, f"select new_id from map_run where old_id={run['id']}")
        replacements.append(
            {
                "origin": origin(source, run["id"]),
                "selected_run_id": new_id,
                "replaced_baseline_run_id": old_id,
                "db": run["db"],
                "suite": run["suite"],
                "scale_factor": run["suite_scale_factor"],
                "operation": run["operation"],
                "status": run["status"],
            }
        )
        for table in reversed(TABLES[1:]):
            con.execute(f"delete from {table} where {'id' if table == 'run' else 'run_id'}=?", [old_id])
    con.commit()
    for table in TABLES:
        mapping = rows(con, f"select * from map_{table} order by old_id")
        dest = output / "maps" / source.revision / f"{table}.csv"
        dest.parent.mkdir(parents=True, exist_ok=True)
        with dest.open("w") as handle:
            writer = csv.writer(handle)
            writer.writerow(["origin_id", "selected_id"])
            writer.writerows((row["old_id"], row["new_id"]) for row in mapping)
        joins, _ = projected(table)
        fields = [row[0] for row in con.execute(f"describe {table}").fetchall()]
        expressions = [f"m_{field}.new_id" if field in RELATIONS[table] else f's."{field}"' for field in fields]
        query = f"select {', '.join(expressions)} from src.{table} s {joins} where m_id.new_id is not null"
        for field in RELATIONS[table]:
            if field == "id":
                continue
            dangling = integer(
                con,
                f"select count(*) from src.{table} s {joins} "
                f'where m_id.new_id is not null and s."{field}" is not null '
                f"and m_{field}.new_id is null",
            )
            require(dangling == 0, f"Unmapped relational reference in {table}.{field}")
        if table != "system_snapshot":
            con.execute(f"insert into {table} {query}")
        difference = integer(
            con,
            f"select count(*) from (({query}) except all "
            f"(select * from {table} where id in (select new_id from map_{table})))",
        )
        reverse = integer(
            con,
            f"select count(*) from ((select * from {table} where id in "
            f"(select new_id from map_{table})) except all ({query}))",
        )
        require(difference == reverse == 0, f"Non-ID field mismatch: {source.revision}/{table}")
    return replacements


def sql_audit(con: duckdb.DuckDBPyConnection, manifest: Record, output: Path) -> Record:
    databases = get_databases()
    audit: list[Record] = []
    problems: list[Record] = []
    for run in rows(con, "select * from run where operation='select' order by id"):
        metadata: Record = json.loads(run["metadata"])
        names: list[str] = metadata["plan"]["query_names"]
        hashes: dict[str, str] = metadata.get("queries", {}).get("files", {})
        database = databases[cast(DatabaseName, run["db"])]
        suite = database.suite_registry()[cast(SuiteName, run["suite"])](
            db=database, name=run["suite"], scale_factor=run["suite_scale_factor"]
        )
        recorded = rows(
            con,
            """select s.query_name, e.query, count(*) executions from query_execution e
            join run_step s on e.run_step_id=s.id where e.run_id=? and s.step_type='query'
            group by s.query_name,e.query order by s.query_name,e.query""",
            [run["id"]],
        )
        for name in names:
            item: Record = {
                "selected_run_id": run["id"],
                "db": run["db"],
                "suite": run["suite"],
                "scale_factor": run["suite_scale_factor"],
                "query_name": name,
                "recorded_plan_sha256": hashes.get(name),
            }
            sql: str | None = None
            skip = suite.query_skip(name)
            if skip is None or name in hashes:
                if run["suite"] == "tpc_ds" or (run["db"] == "arc" and run["suite"] == "chat_threads"):
                    entry = manifest[run["suite"]][name]
                    sql = entry["db_overrides"].get(run["db"], entry["sql"])
                else:
                    sql = suite.load_query(name)
                assert isinstance(sql, str), f"Missing generation SQL: {item}"
                item["generation_sql_sha256"] = text_sha(sql)
                item["plan_matches_generation"] = None if name not in hashes else text_sha(sql) == hashes[name]
                if item["plan_matches_generation"] is False:
                    problems.append(item)
            else:
                item["plan_matches_generation"] = None
                item["registry_skip"] = skip
            if name not in hashes:
                item["recorded_hash_absent"] = True
                item["reason"] = "Run did not finalize query hashes, or position explicitly skipped/unsupported"
            item["recorded_sql"] = [
                {
                    "sha256": text_sha(r["query"]),
                    "executions": r["executions"],
                    "matches_loaded_sql": r["query"].strip().removesuffix(";").strip()
                    == (sql.strip().removesuffix(";").strip() if sql is not None else None),
                }
                for r in recorded
                if r["query_name"] == name
            ]
            if run["suite"] == "tpc_ds":
                commit = metadata["repository"]["commit"]
                roots = [run["db"]]
                if run["db"] == "timescaledb":
                    roots.append("postgres")
                paths = [f"olap_benchmarks/suites/tpc_ds/queries/{db}/{name}.sql" for db in roots]
                paths.append(f"olap_benchmarks/suites/tpc_ds/queries/{name}.sql")
                for path in paths:
                    loaded = subprocess.run(
                        ["git", "show", f"{commit}:{path}"], capture_output=True, text=True, check=False
                    )
                    if loaded.returncode == 0:
                        item["measured_source"] = {"commit": commit, "path": path, "sha256": text_sha(loaded.stdout)}
                        item["source_matches_generation"] = loaded.stdout == sql
                        if sql is not None and not item["source_matches_generation"]:
                            problems.append(item)
                        break
                else:
                    require(sql is None, f"Missing recorded source SQL: {item}")
            if any(not execution["matches_loaded_sql"] for execution in item["recorded_sql"]):
                problems.append(item)
            audit.append(item)
    result = {
        "frozen_manifest_commit": OLD_SOURCE,
        "query_positions": len(audit),
        "plan_mismatches": problems,
        "positions": audit,
    }
    write_json(output / "sql-audit.json", result)
    require(not problems, f"SQL generation mismatches ({len(problems)}); see sql-audit.json")
    return result


def baseline_audit(
    con: duckdb.DuckDBPyConnection, baseline: Path, output: Path, *, expected_sha256: str = BASELINE_SHA
) -> None:
    checked_source(baseline, expected_sha256)
    retained = read_json(output / "baseline-identity-map.json")["retained_run_ids"]
    con.execute(f"attach {sql_path(baseline)} as seed (read_only)")
    ids = ",".join(map(str, retained))
    summary: list[Record] = []
    for table in TABLES:
        predicate = "true" if table == "system_snapshot" else f"{'id' if table == 'run' else 'run_id'} in ({ids})"
        selection = f"select * from {{catalog}}{table} where {predicate}"
        actual, expected = selection.format(catalog=""), selection.format(catalog="seed.")
        for left, right in ((actual, expected), (expected, actual)):
            require(
                integer(con, f"select count(*) from (({left}) except all ({right}))") == 0,
                f"Retained baseline changed: {table}",
            )
        path = output / "maps" / "baseline" / f"{table}.csv"
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w") as handle:
            writer = csv.writer(handle)
            writer.writerow(["origin_id", "selected_id"])
            writer.writerows(
                (r[0], r[0]) for r in con.execute(f"select id from {table} where {predicate} order by id").fetchall()
            )
        summary.append(
            {
                "table": table,
                "rows": integer(con, f"select count(*) from ({actual})"),
                "map": path.relative_to(output).as_posix(),
                "sha256": sha(path),
                "exact_match": True,
            }
        )
    con.execute("detach seed")
    write_json(output / "baseline-preservation.json", summary)


def full_run_audit(con: duckdb.DuckDBPyConnection, output: Path) -> None:
    report: list[Record] = []
    for run in rows(con, "select * from run order by id"):
        run["metadata"] = json.loads(run["metadata"])
        steps = rows(con, "select * from run_step where run_id=? order by id", [run["id"]])
        for step in steps:
            step["metadata"] = json.loads(step["metadata"]) if step["metadata"] is not None else None
        report.append(
            {
                "run": run,
                "steps": steps,
                "query_execution_count": integer(con, f"select count(*) from query_execution where run_id={run['id']}"),
                "metric_count": integer(con, f"select count(*) from run_metric where run_id={run['id']}"),
            }
        )
    write_json(output / "full-run-audit.json", report)


def build(worktrees: Path, archive_manifest: Path, output: Path, destination: Path) -> None:
    require(not destination.exists(), f"Refusing to overwrite staging revision: {destination}")
    output.mkdir(parents=True, exist_ok=True)
    archive = {entry["original_path"]: entry for entry in read_json(archive_manifest)["files"]}
    baseline = worktrees / "t3-416ee4f7/site/public/data/results.db"
    checked_source(baseline, BASELINE_SHA)
    frozen_queries = subprocess.check_output(["git", "show", f"{OLD_SOURCE}:site/public/data/queries.json"])
    (output / "original-queries.json").write_bytes(frozen_queries)
    manifest = json.loads(frozen_queries)
    adjustments: list[Record] = []
    for name in ("46", "72", "78"):
        entry = manifest["tpc_ds"][name]
        entry["db_overrides"]["timescaledb"] = entry["db_overrides"]["postgres"]
        adjustments.append(
            {
                "suite": "tpc_ds",
                "query": name,
                "db": "timescaledb",
                "reason": "Materialize inherited frozen PostgreSQL override",
                "source_commit": OLD_SOURCE,
                "sha256": text_sha(entry["db_overrides"]["timescaledb"]),
            }
        )
    arc_commit = "e55b553407307d804d14e9d22fb0830e5fdfc4e2"
    arc_path = "olap_benchmarks/suites/chat_threads/queries/06_thread_fetch_payload.sql"
    arc_sql = subprocess.check_output(["git", "show", f"{arc_commit}:{arc_path}"], text=True)
    manifest["chat_threads"]["06_thread_fetch_payload"]["db_overrides"]["arc"] = arc_sql
    adjustments.append(
        {
            "suite": "chat_threads",
            "query": "06_thread_fetch_payload",
            "db": "arc",
            "reason": "Exact shared SQL used by both retained Arc scales",
            "source_commit": arc_commit,
            "source_path": arc_path,
            "sha256": text_sha(arc_sql),
        }
    )
    write_json(output / "queries.json", manifest)
    write_json(
        output / "query-manifest-adjustments.json",
        {
            "frozen_sha256": text_sha(frozen_queries.decode()),
            "selected_sha256": sha(output / "queries.json"),
            "adjustments": adjustments,
        },
    )
    baseline_comparison: Record = json.loads(
        subprocess.check_output(["git", "show", f"{OLD_SOURCE}:site/public/data/comparison.json"])
    )
    require(baseline_comparison["results_db_sha256"] == BASELINE_SHA, "Baseline metadata is unbound")
    evidence: list[Record] = []
    source_info: dict[str, Record] = {}
    for source in SOURCES:
        checked_source(source.database(worktrees), source.sha256)
        root = source.root(worktrees)
        journal_path = root / "data/temp/timeout-recovery/state.json"
        plan_path = root / "docs/campaigns" / source.plan_name
        journal: Record = read_json(journal_path)
        checked_source(plan_path, journal["plan_sha256"])
        journal_ref = evidence_copy(journal_path, output, archive)
        plan_ref = evidence_copy(plan_path, output, archive)
        info: Record = {
            "source": asdict(source),
            "journal": journal,
            "journal_ref": journal_ref,
            "plan_ref": plan_ref,
            "attempts": [],
        }
        for attempt in journal["attempts"]:
            selected = [run_id for run_id in attempt["runs"].values() if run_id in source.runs]
            record: Record = {**attempt, "selected_origin_run_ids": selected, "evidence": []}
            if selected:
                require(attempt["cleanup_confirmed"], "Selected attempt lacks confirmed cleanup")
                require(
                    attempt["validation_ok"] or attempt["status"] == "accepted_failed_populate",
                    "Selected SELECT attempt failed answer validation",
                )
                directory = root / "data/temp/timeout-recovery" / attempt["id"]
                for path in sorted(directory.glob("*")):
                    if path.suffix in (".json", ".log", ".txt"):
                        record["evidence"].append(evidence_copy(path, output, archive))
            info["attempts"].append(record)
        source_info[source.revision] = info
        evidence.append(info)
        link = output / f"{source.revision}.db"
        if not link.exists():
            link.symlink_to(source.database(worktrees))
    write_json(output / "sealed-evidence.json", evidence)
    shutil.copyfile(baseline, destination)
    replacements: list[Record] = []
    with duckdb.connect(str(destination)) as con:
        require(integer(con, "select count(*) from system_snapshot") == 1, "Baseline has multiple snapshots")
        require(
            con.execute("select version_num from alembic_version").fetchone() == (get_results_head_revision(),),
            "Schema mismatch",
        )
        original_ids = [r["id"] for r in rows(con, "select id from run order by id")]
        for source in SOURCES:
            if not source.runs:
                continue
            con.execute(f"attach {sql_path(source.database(worktrees))} as src (read_only)")
            replacements.extend(import_source(con, source, output))
            con.execute("detach src")
        require(
            all(r["replaced_baseline_run_id"] in original_ids for r in replacements),
            "A replacement would overwrite another selected namespace",
        )
        require(
            len({r["replaced_baseline_run_id"] for r in replacements}) == len(replacements),
            "Multiple replacements for one baseline attempt",
        )
        _sync_sequences(con)
        retained = [r["id"] for r in rows(con, "select id from run where id <= 684 order by id")]
        write_json(
            output / "baseline-identity-map.json",
            {
                "origin_sha256": BASELINE_SHA,
                "rule": "All retained baseline table PK/FKs retain their integer IDs; JSON is unchanged.",
                "baseline_run_ids": original_ids,
                "retained_run_ids": retained,
                "replaced_run_ids": sorted(r["replaced_baseline_run_id"] for r in replacements),
            },
        )
        write_json(output / "selection.json", replacements)
        for table in TABLES[2:]:
            require(
                integer(con, f"select count(*) from {table} c left join run r on c.run_id=r.id where r.id is null")
                == 0,
                f"Orphan in {table}",
            )
        require(
            integer(
                con,
                "select count(*) from query_execution e left join run_step s on e.run_step_id=s.id "
                "where e.run_step_id is not null and (s.id is null or s.run_id<>e.run_id)",
            )
            == 0,
            "Dangling or cross-run execution reference",
        )
        full_run_audit(con, output)
        sql_audit(con, manifest, output)
        annotations: list[Record] = []
        disposition: list[Record] = []
        replaced = {r["replaced_baseline_run_id"]: r for r in replacements}
        for annotation in baseline_comparison["annotations"]:
            old_id = annotation["run_id"]
            if old_id in replaced:
                replacement = replaced[old_id]
                require(replacement["operation"] == "select", "Non-SELECT annotation replacement")
                disposition.append(
                    {
                        "annotation": annotation,
                        "disposition": "replaced by whole-attempt evidence",
                        "selected_run_id": replacement["selected_run_id"],
                    }
                )
            else:
                require(old_id in retained, f"Retained annotation has no selected run: {old_id}")
                annotations.append(annotation)
                disposition.append({"annotation": annotation, "disposition": "retained exactly"})
        associations: list[Record] = []
        for item in replacements:
            source = next(s for s in SOURCES if s.revision == item["origin"]["revision"])
            info = source_info[source.revision]
            attempt = next(a for a in info["attempts"] if item["origin"]["run_id"] in a["runs"].values())
            population = attempt["runs"]["populate"]
            selected_pop = next(r["selected_run_id"] for r in replacements if r["origin"] == origin(source, population))
            association = {
                **item,
                "actual_population_origin": origin(source, population),
                "actual_population_selected_run_id": selected_pop,
                "attempt": attempt["id"],
                "validation_ok": attempt["validation_ok"],
                "population_certifies_retained_old_select": False,
                "evidence": attempt["evidence"],
                "plan_ref": info["plan_ref"],
                "journal_ref": info["journal_ref"],
            }
            associations.append(association)
            if item["operation"] == "select":
                annotations.append(
                    {
                        "db": item["db"],
                        "suite": item["suite"],
                        "scale_factor": item["scale_factor"],
                        "run_id": item["selected_run_id"],
                        "preparation": "observed",
                        "tuning": None,
                        "summary": f"Canonical full population {source.revision}/run{population} completed "
                        f"for this whole SELECT attempt ({source.revision}/run{item['origin']['run_id']}). "
                        "All recorded answers and iterations passed the sealed validation. "
                        "Failed and missing positions retain their recorded outcomes.",
                        "evidence_url": REPORT_URL,
                    }
                )
        write_json(output / "population-associations.json", associations)
        for annotation in annotations:
            replacement = next((r for r in replacements if r["selected_run_id"] == annotation["run_id"]), None)
            if replacement is not None:
                key = (replacement["origin"]["revision"], replacement["origin"]["run_id"])
                if key in ANNOTATION_DETAILS:
                    annotation["summary"] = ANNOTATION_DETAILS[key]
        write_json(output / "annotations.json", annotations)
        write_json(output / "annotation-dispositions.json", disposition)
        require(
            len(annotations)
            == len(baseline_comparison["annotations"])
            + sum(r["operation"] == "select" for r in replacements)
            - sum(r["disposition"] != "retained exactly" for r in disposition),
            "Annotation count mismatch",
        )
    report = validate_results(db_path=destination, include_failed=True, mark=False)
    write_json(output / "readonly-validation.json", asdict(report))
    require(report.ok and report.marks_applied == report.marks_cleared == 0, report.format())
    validate_publishable_runs(
        destination,
        campaign_cells(),
        pins=current_pins(),
        drivers=current_drivers(),
        allow_mixed_commits=True,
        campaign=True,
    )
    write_json(
        output / "build-summary.json",
        {
            "revision": REVISION,
            "db_sha256": sha(destination),
            "replacements": replacements,
            "exact_non_id_preservation": True,
            "system_snapshot_count": 1,
            "unexpected_marks": 0,
            "deferred": [f"{SOURCES[-1].revision}/run{n}" for n in (685, 686, 687, 688)],
            "deferred_unstarted": ["postgres:tpc_ds:sf10", "timescaledb:tpc_ds:sf10"],
            "excluded_validation_false": [
                "2026-10-timeout-recovery/run695",
                "2026-10-timeout-recovery-remaining/run694",
            ],
        },
    )
    print(f"Built and read-only validated {destination}")


def export(output: Path, destination: Path) -> None:
    require(read_json(output / "build-summary.json")["db_sha256"] == sha(destination), "Staging DB changed")
    report = validate_results(db_path=destination, include_failed=True, mark=False)
    require(report.ok and report.marks_applied == report.marks_cleared == 0, report.format())
    subprocess.run(
        [str(REPO_ROOT / ".venv/bin/olap"), "publish", REVISION, "--campaign", "--report-url", REPORT_URL],
        check=True,
        cwd=REPO_ROOT,
    )
    published = REPO_ROOT / "site/public/data"
    shutil.copyfile(output / "queries.json", published / "queries.json")
    write_comparison_manifest(published, annotations_path=output / "annotations.json")
    with duckdb.connect(str(published / "results.db"), read_only=True) as con:
        con.execute(f"attach {sql_path(destination)} as original (read_only)")
        for table in TABLES:
            for left, right in ((table, f"original.{table}"), (f"original.{table}", table)):
                require(
                    integer(con, f"select count(*) from ((select * from {left}) except all (select * from {right}))")
                    == 0,
                    f"Publisher mutated {table}",
                )
    write_json(
        output / "export-summary.json",
        {
            "results_db_sha256": sha(published / "results.db"),
            "queries_sha256": sha(published / "queries.json"),
            "unexpected_marks": 0,
            "all_five_tables_exactly_unchanged": True,
            "uploaded": False,
        },
    )
    print("Staging export complete; frozen queries restored; no scientific row changed")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("operation", choices=("build", "export", "audit"))
    parser.add_argument("--worktrees", type=Path, default=REPO_ROOT.parent)
    parser.add_argument(
        "--archive-manifest", type=Path, default=Path("/tmp/olap-recovery-closeout-20261026-0416/archive-manifest.json")
    )
    parser.add_argument("--output", type=Path, default=REPO_ROOT / "data/temp/timeout-recovery-curated")
    args = parser.parse_args()
    destination = REPO_ROOT / "results" / f"{REVISION}.db"
    if args.operation == "build":
        build(args.worktrees, args.archive_manifest, args.output, destination)
    elif args.operation == "export":
        export(args.output, destination)
    else:
        with duckdb.connect(str(destination), read_only=True) as con:
            baseline_audit(con, args.worktrees / "t3-416ee4f7/site/public/data/results.db", args.output)
            sql_audit(con, read_json(args.output / "queries.json"), args.output)
        print("All selected source SQL and retained baseline rows audited")


if __name__ == "__main__":
    main()
