from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest

from scripts import curate_timeout_recovery as curator


def database(path: Path, *, extra: bool = False, snapshot_metadata: str = '{"id":685}') -> None:
    with duckdb.connect(str(path)) as con:
        con.execute("""create table system_snapshot(id integer, system varchar, os varchar, os_release varchar,
            machine varchar, processor varchar, cpu_count_logical integer, memory_total_mb integer, metadata json)""")
        con.execute(
            "insert into system_snapshot values (1,'host','os','release','machine','cpu',8,1024,?)", [snapshot_metadata]
        )
        con.execute("""create table run(id integer, system varchar, db varchar, db_version varchar, db_driver varchar,
            suite varchar, suite_scale_factor integer, operation varchar, system_snapshot_id integer,
            status varchar, finished_at timestamp, metadata json)""")
        con.execute("""insert into run values
            (685,'host','postgres','18',null,'tpc_h',10,'select',1,'failed','2026-10-01','{"id":685}')""")
        if extra:
            con.execute("""insert into run values
                (700,'host','postgres','18',null,'tpc_h',50,'select',1,'failed','2026-10-01','{"id":700}')""")
        con.execute("create table run_step(id integer, run_id integer, metadata json, duration double)")
        con.execute("insert into run_step values (685,685,'{\"run_id\":685}',0.123456789012345)")
        con.execute("create table run_metric(id integer, run_id integer, value double)")
        con.execute("insert into run_metric values (685,685,123.456789012345)")
        con.execute("create table query_execution(id integer, run_id integer, run_step_id integer, query varchar)")
        con.execute("insert into query_execution values (685,685,685,'select 685')")


def source() -> curator.Source:
    return curator.Source("namespace-with-colliding-ids", "unused", "unused", (685,), "unused")


def test_whole_attempt_remaps_only_relational_ids_and_preserves_scientific_fields(tmp_path: Path) -> None:
    destination, raw = tmp_path / "destination.db", tmp_path / "raw.db"
    database(destination, extra=True)
    database(raw)
    raw_hash = curator.sha(raw)
    with duckdb.connect(str(destination)) as con:
        con.execute(f"attach {curator.sql_path(raw)} as src (read_only)")
        selected = curator.import_source(con, source(), tmp_path)
        assert selected[0]["selected_run_id"] == 701
        assert selected[0]["origin"]["run_id"] == 685
        assert con.execute("select metadata from run where id=701").fetchone() == ('{"id":685}',)
        assert con.execute("select run_id,metadata,duration from run_step").fetchone() == (
            701,
            '{"run_id":685}',
            0.123456789012345,
        )
        assert con.execute("select run_id,run_step_id,query from query_execution").fetchone() == (
            701,
            686,
            "select 685",
        )
        assert curator.integer(con, "select count(*) from system_snapshot") == 1
    assert curator.sha(raw) == raw_hash
    assert (tmp_path / "maps/namespace-with-colliding-ids/run.csv").read_text().splitlines() == [
        "origin_id,selected_id",
        "685,701",
    ]


def test_snapshot_json_must_match_exactly(tmp_path: Path) -> None:
    destination, raw = tmp_path / "destination.db", tmp_path / "raw.db"
    database(destination)
    database(raw, snapshot_metadata=json.dumps({"id": 686}))
    with duckdb.connect(str(destination)) as con:
        con.execute(f"attach {curator.sql_path(raw)} as src (read_only)")
        with pytest.raises(ValueError, match="Snapshots differ"):
            curator.import_source(con, source(), tmp_path)
        assert curator.integer(con, "select count(*) from run where id=685") == 1


def test_dangling_execution_step_is_rejected(tmp_path: Path) -> None:
    destination, raw = tmp_path / "destination.db", tmp_path / "raw.db"
    database(destination)
    database(raw)
    with duckdb.connect(str(raw)) as con:
        con.execute("update query_execution set run_step_id=999")
    with duckdb.connect(str(destination)) as con:
        con.execute(f"attach {curator.sql_path(raw)} as src (read_only)")
        with pytest.raises(ValueError, match="Unmapped relational reference"):
            curator.import_source(con, source(), tmp_path)
