from collections.abc import Mapping
from hashlib import sha256
from pathlib import Path
from typing import cast

import polars as pl
import pytest
from sqlalchemy import Connection

from ..dbs import Database
from ..dbs.postgres import (
    JOB_INDEX_COUNT,
    JOB_INDEX_POLICY,
    JOB_INDEX_READBACK_SQL,
    JOB_INDEX_SHA256,
    Postgres,
    PostgresFetchMethod,
    _job_indexes,
)
from ..dbs.timescaledb import TimescaleDB
from ..metrics.storage import Storage
from ..settings import DatabaseName, SuiteName, TableName, get_settings
from ..suites import BenchmarkSuite
from ..suites.join_order import schema as job_schema
from ..suites.join_order.config import JoinOrder


def _catalog(*, secondary: bool = True) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = [
        {
            "table_name": table,
            "index_name": f"{table}_pkey",
            "indisprimary": True,
            "indisvalid": True,
            "indisready": True,
            "definition": f"CREATE UNIQUE INDEX {table}_pkey ON public.{table} USING btree (id)",
            "column_name": "id",
            "bytes": 8192,
        }
        for table in job_schema.JOB_TABLES
    ]
    if secondary:
        rows.extend(
            {
                "table_name": table,
                "index_name": name,
                "indisprimary": False,
                "indisvalid": True,
                "indisready": True,
                "definition": f"CREATE INDEX {name} ON public.{table} USING btree ({column})",
                "column_name": column,
                "bytes": 8192,
            }
            for name, (table, column, _statement) in _job_indexes().items()
        )
    return rows


class _Recorder:
    def __init__(self) -> None:
        self.started: list[dict[str, object]] = []
        self.finished: list[dict[str, object]] = []
        self.queries: list[dict[str, object]] = []

    def start_step(self, **kwargs: object) -> int:
        step_id = len(self.started) + 1
        self.started.append({"step_id": step_id, **kwargs})
        return step_id

    def finish_step(self, **kwargs: object) -> None:
        self.finished.append(kwargs)

    def insert_query_execution(self, **kwargs: object) -> None:
        self.queries.append(kwargs)


class _OfflinePostgres(Postgres):
    catalog_rows: list[dict[str, object]] = []
    counts: dict[TableName, int] = dict.fromkeys(job_schema.JOB_TABLES, 1)
    events: list[str] = []
    statements: list[str] = []

    @property
    def start(self) -> str:
        raise AssertionError("Offline JOB tests must never start an engine")

    def connect(self, reconnect: bool = False) -> Connection:
        raise AssertionError("Offline JOB tests must never connect to an engine")

    def fetch(
        self,
        query: str,
        schema: Mapping[str, pl.DataType | type[pl.DataType]] | None = None,
        method: PostgresFetchMethod = "connectorx",
    ) -> pl.DataFrame:
        assert query == JOB_INDEX_READBACK_SQL and method == "python" and schema is None
        self.events.append("readback")
        with self.record_query_execution(query):
            return pl.DataFrame(
                self.catalog_rows,
                schema={
                    "table_name": pl.String,
                    "index_name": pl.String,
                    "indisprimary": pl.Boolean,
                    "indisvalid": pl.Boolean,
                    "indisready": pl.Boolean,
                    "definition": pl.String,
                    "column_name": pl.String,
                    "bytes": pl.Int64,
                },
            )

    def execute(self, statement: str, commit: bool = True, autocommit: bool = False, reconnect: bool = False) -> None:
        assert commit and not autocommit and not reconnect
        self.statements.append(statement)
        self.events.append(statement)
        with self.record_query_execution(statement):
            if statement.startswith("create index "):
                name = statement.split()[2]
                self.catalog_rows.extend(row for row in _catalog() if row["index_name"] == name)
            else:
                assert statement.startswith('ANALYZE public."')

    def get_table_names(self) -> set[TableName]:
        return set(self.counts)

    def get_row_count(self, table: TableName) -> int:
        self.events.append(f"count:{table}")
        return self.counts[table]

    def rollback(self) -> None:
        return


def _record(database: _OfflinePostgres) -> _Recorder:
    recorder = _Recorder()
    database._result_storage = cast(Storage, recorder)
    database._run_id = 1
    return recorder


@pytest.fixture
def database(monkeypatch: pytest.MonkeyPatch) -> _OfflinePostgres:
    def expected_counts(_self: JoinOrder[Database]) -> dict[TableName, int]:
        return dict.fromkeys(job_schema.JOB_TABLES, 1)

    monkeypatch.setattr(JoinOrder, "expected_table_row_counts", expected_counts)
    database = _OfflinePostgres(catalog_rows=_catalog())
    database._current_suite = "join_order"
    database._current_suite_scale_factor = 1
    return database


def test_job_index_policy_executes_only_the_exact_23_upstream_statements() -> None:
    source = (job_schema.DIRECTORY / "upstream/fkindexes.sql").read_bytes()
    indexes = _job_indexes()

    assert sha256(source).hexdigest() == JOB_INDEX_SHA256
    assert JOB_INDEX_SHA256 == "727909bbbb9b0ae2b7d5739cfcd066992fc4bf1e984b29d87bcc700c4e2583cc"
    assert len(indexes) == JOB_INDEX_COUNT == 23
    assert [statement for _table, _column, statement in indexes.values()] == source.decode().splitlines()
    assert indexes["person_role_id_cast_info"][:2] == ("cast_info", "person_role_id")


def test_job_index_policy_rejects_modified_source(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    directory = tmp_path / "upstream"
    directory.mkdir()
    source = (job_schema.DIRECTORY / "upstream/fkindexes.sql").read_bytes()
    (directory / "fkindexes.sql").write_bytes(source.replace(b"(company_id)", b"(id)", 1))
    monkeypatch.setattr(job_schema, "DIRECTORY", tmp_path)

    with pytest.raises(ValueError, match="upstream SHA-256 pin"):
        _job_indexes()


def test_population_charges_index_build_before_analyze_and_attests_observed_layout(database: _OfflinePostgres) -> None:
    database.catalog_rows = _catalog(secondary=False)
    recorder = _record(database)

    database.finalize_populate()

    creates = [statement for _table, _column, statement in _job_indexes().values()]
    analyzes = [f'ANALYZE public."{table}"' for table in sorted(job_schema.JOB_TABLES)]
    assert database.statements == creates + analyzes
    assert [(step["step_name"], step["table_name"]) for step in recorder.started] == [
        ("job_index_readiness", None),
        *(("index", table) for table, _column, _statement in _job_indexes().values()),
        *(("analyze", table) for table in sorted(job_schema.JOB_TABLES)),
        ("job_index_readiness", None),
    ]
    evidence = cast(dict[str, object], recorder.finished[-1]["metadata"])
    assert evidence["ready"] is True
    assert evidence["observed_secondary_index_count"] == 23
    assert evidence["observed_indexes"] == database.catalog_rows
    assert evidence["observed_layout"] == JOB_INDEX_POLICY
    assert [row["query"] for row in recorder.queries] == [
        JOB_INDEX_READBACK_SQL,
        *creates,
        *analyzes,
        JOB_INDEX_READBACK_SQL,
    ]
    assert all(row["run_step_id"] is not None for row in recorder.queries)
    assert not database._active_step_ids_by_thread

    database.finalize_populate()
    assert database.statements == creates + analyzes + analyzes


def test_matching_counts_check_indexes_without_result_storage_and_never_repair(database: _OfflinePostgres) -> None:
    benchmark = database.benchmarks["join_order"]
    assert database._result_storage is None
    assert not benchmark.should_populate()
    assert not benchmark.should_populate()
    assert not database.statements

    database.catalog_rows = _catalog(secondary=False)
    with pytest.raises(RuntimeError, match="missing or unexpected indexes"):
        benchmark.should_populate()
    assert database._result_storage is None
    assert not database.statements


def test_fresh_population_row_verification_requires_only_counts(database: _OfflinePostgres) -> None:
    database.catalog_rows = _catalog(secondary=False)

    database.benchmarks["join_order"].verify_populated_data()

    assert database.events == [f"count:{table}" for table in job_schema.JOB_TABLES]
    assert not database.statements


@pytest.mark.parametrize(
    "damage",
    [
        "missing-primary",
        "missing-secondary",
        "extra",
        "duplicate",
        "invalid",
        "not-ready",
        "table",
        "column",
        "definition",
    ],
)
def test_readiness_rejects_incomplete_or_different_catalogs_and_records_failure(
    database: _OfflinePostgres, damage: str
) -> None:
    row = database.catalog_rows[-1]
    match damage:
        case "missing-primary":
            database.catalog_rows.pop(0)
        case "missing-secondary":
            database.catalog_rows.pop()
        case "extra":
            database.catalog_rows.append({**row, "index_name": "extra_index"})
        case "duplicate":
            database.catalog_rows.append(dict(row))
        case "invalid":
            database.catalog_rows[0]["indisvalid"] = False
        case "not-ready":
            row["indisready"] = False
        case "table":
            row["table_name"] = "title"
        case "column":
            row["column_name"] = "id"
        case "definition":
            row["definition"] = str(row["definition"]) + " WHERE role_id IS NOT NULL"
        case _:
            raise AssertionError(damage)
    recorder = _record(database)

    with pytest.raises(RuntimeError, match="PostgreSQL JOB"):
        database.verify_storage_readiness()

    assert not database.statements
    assert recorder.finished[-1]["status"] == "failed"
    evidence = cast(dict[str, object], recorder.finished[-1]["metadata"])
    assert evidence["ready"] is False
    assert evidence["observed_indexes"] == database.catalog_rows
    assert recorder.queries[-1]["run_step_id"] == recorder.started[-1]["step_id"]
    assert not database._active_step_ids_by_thread


@pytest.mark.parametrize("damage", ["none", "counts", "indexes"])
def test_select_checks_all_counts_and_index_readiness_before_any_query(
    database: _OfflinePostgres, monkeypatch: pytest.MonkeyPatch, damage: str
) -> None:
    monkeypatch.setattr(get_settings(), "tables", ("title",))
    if damage == "counts":
        database.counts[job_schema.JOB_TABLES[-1]] = 0
    elif damage == "indexes":
        database.catalog_rows.pop()
    _record(database)

    def queries(_self: BenchmarkSuite[Database]) -> None:
        database.events.append("queries")

    monkeypatch.setattr(BenchmarkSuite, "run_select_queries", queries)
    benchmark = database.benchmarks["join_order"]
    if damage == "none":
        benchmark.select()
        assert database.events == [*(f"count:{table}" for table in job_schema.JOB_TABLES), "readback", "queries"]
    else:
        with pytest.raises(RuntimeError):
            benchmark.select()
        assert "queries" not in database.events
        assert ("readback" in database.events) == (damage == "indexes")
    assert not database.statements


@pytest.mark.parametrize(
    ("database_type", "suite_name", "scale", "enabled"),
    [(Postgres, "join_order", 1, True), (Postgres, "tpc_h", 10, False), (TimescaleDB, "tpc_h", 10, False)],
)
def test_index_metadata_is_intended_policy_and_scoped_to_postgres_job(
    database_type: type[Postgres], suite_name: SuiteName, scale: int, enabled: bool
) -> None:
    database = database_type()
    database._current_suite = suite_name
    database._current_suite_scale_factor = scale
    options = database.run_options

    assert ("job_index_policy" in options) == enabled
    assert not any("observed" in key for key in options)
    if enabled:
        assert options["job_index_policy"] == JOB_INDEX_POLICY
        assert options["job_upstream_index_sha256"] == JOB_INDEX_SHA256
        assert options["job_intended_secondary_index_count"] == 23
        assert options["job_index_readback_phase"] == "job_index_readiness"


@pytest.mark.parametrize(("name", "suite_name"), [("postgres", "tpc_h"), ("timescaledb", "join_order")])
def test_other_postgres_workloads_and_timescale_keep_existing_analyze_behavior(
    database: _OfflinePostgres, name: DatabaseName, suite_name: SuiteName
) -> None:
    database.name = name
    database._current_suite = suite_name
    database.catalog_rows = []
    _record(database)

    database.verify_storage_readiness()
    database.finalize_populate()

    assert database.events == [f'ANALYZE public."{table}"' for table in sorted(job_schema.JOB_TABLES)]


@pytest.mark.parametrize("name", ["duckdb", "clickhouse", "monetdb", "starrocks"])
def test_other_job_engines_keep_their_existing_select_path(
    database: _OfflinePostgres, monkeypatch: pytest.MonkeyPatch, name: DatabaseName
) -> None:
    database.name = name
    database.catalog_rows = []
    database.counts = {}

    def queries(_self: BenchmarkSuite[Database]) -> None:
        database.events.append("queries")

    monkeypatch.setattr(BenchmarkSuite, "run_select_queries", queries)
    database.benchmarks["join_order"].select()

    assert database.events == ["queries"]
