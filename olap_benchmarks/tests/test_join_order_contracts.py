from __future__ import annotations

import json
import re
import tarfile
from collections import Counter
from collections.abc import Generator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Literal, cast

import polars as pl
import pytest
from sqlalchemy import Connection

from ..dbs import Database
from ..settings import DatabaseName, TableName, get_settings
from ..suites.join_order import config as job_config
from ..suites.join_order.config import CANDIDATE_DATABASES, JoinOrder
from ..suites.join_order.prepare import (
    ARCHIVE_BYTES,
    ARCHIVE_SCHEMA,
    ARCHIVE_SHA256,
    SNAPSHOT_PIN,
    DatasetManifest,
    SnapshotPin,
    fingerprint,
    prepare_snapshot,
    verify_prepared_data,
)
from ..suites.join_order.schema import DIRECTORY, JOB_QUERY_NAMES, JOB_SCHEMA, JOB_TABLES, UPSTREAM_COMMIT
from . import test_join_order_prepare as prepare_fixtures

snapshot = prepare_fixtures.snapshot


class FixtureDatabase(Database):
    name: DatabaseName = "duckdb"
    version: str = "fixture"
    connection_string: str = "dummy://"
    inserted: list[tuple[Path, str]] = []
    counts: dict[str, int] = {}
    schemas: list[str] = []
    phases: list[str] = []
    restarts: int = 0

    @property
    def start(self) -> None:
        raise AssertionError("JOB static tests must never start an engine")

    def connect(self, reconnect: bool = False) -> Connection:
        raise AssertionError("JOB static tests must never connect to an engine")

    def fetch(self, query: str, schema: Mapping[str, pl.DataType | type[pl.DataType]] | None = None) -> pl.DataFrame:
        raise AssertionError("JOB static tests must never execute SQL")

    def get_table_names(self) -> set[TableName]:
        return set(self.counts)

    def get_row_count(self, table: TableName) -> int:
        return self.counts[table]

    def initialize_schema(self, suite_directory: str) -> None:
        self.schemas.append(suite_directory)

    def insert_parquet(
        self,
        path: Path,
        table: TableName,
        primary_key: str | list[str] | None = None,
        not_null: str | list[str] | None = None,
        *,
        epoch_columns: Mapping[str, Any] | None = None,
    ) -> None:
        assert primary_key is None and not_null is None and epoch_columns is None
        self.inserted.append((path, table))
        self.counts[table] = 1

    def insert(
        self,
        df: pl.DataFrame | pl.LazyFrame,
        table: TableName,
        primary_key: str | list[str] | None = None,
        not_null: str | list[str] | None = None,
    ) -> None:
        raise AssertionError("Suite must use the shared Parquet ingestion API")

    def upsert(self, df: pl.DataFrame, table: TableName, primary_key: str | list[str]) -> None:
        raise NotImplementedError

    def delete(self, table: TableName, primary_key: str | list[str], keys: pl.DataFrame) -> None:
        raise NotImplementedError

    def restart_event(self) -> None:
        self.restarts += 1

    def rollback(self) -> None:
        return

    def verify_storage_readiness(self) -> None:
        return

    @contextmanager
    def phase_context(self, phase_name: str, table_name: TableName | None = None) -> Generator[None]:
        self.phases.append(phase_name)
        yield


def suite(database: Database) -> JoinOrder[Database]:
    return cast(JoinOrder[Database], JoinOrder.model_construct(db=database, name="join_order", scale_factor=1))


def test_all_113_canonical_queries_and_source_files_are_pinned_byte_for_byte() -> None:
    provenance = cast(dict[str, Any], json.loads((DIRECTORY / "provenance.json").read_text()))
    assert provenance["commit"] == UPSTREAM_COMMIT
    assert provenance["query_names"] == list(JOB_QUERY_NAMES)
    assert len(JOB_QUERY_NAMES) == len(set(JOB_QUERY_NAMES)) == 113
    assert len(Counter(query[:-1] for query in JOB_QUERY_NAMES)) == 33
    assert {path.stem for path in (DIRECTORY / "queries").glob("*.sql")} == set(JOB_QUERY_NAMES)
    hashes = cast(dict[str, str], provenance["files"])
    assert len(hashes) == 116
    for path, digest in hashes.items():
        assert fingerprint(DIRECTORY / path)["sha256"] == digest


def test_archive_pin_records_initial_upstream_acquisition_without_independent_checksum_claim() -> None:
    provenance = cast(dict[str, Any], json.loads((DIRECTORY / "provenance.json").read_text()))
    assert provenance["archive_sha256"] == ARCHIVE_SHA256 == SNAPSHOT_PIN.sha256
    assert provenance["archive_bytes"] == ARCHIVE_BYTES == SNAPSHOT_PIN.bytes
    assert provenance["archive_schema"] == ARCHIVE_SCHEMA
    assert provenance["acquisition"]["date"] == "2026-10-22"
    assert provenance["acquisition"]["independent_checksum_available"] is False
    assert provenance["snapshot_url"] == "https://event.cwi.nl/da/job/imdb.tgz"


@pytest.mark.parametrize("query", JOB_QUERY_NAMES)
def test_query_contract_preserves_min_projections_aliases_and_connected_join_graph(query: str) -> None:
    sql = (DIRECTORY / "queries" / f"{query}.sql").read_text()
    assert sql.count(";") == 1 and sql.rstrip().endswith(";")
    projection, remainder = sql.removeprefix("SELECT ").split("\nFROM ", 1)
    from_clause, where = remainder.split("\nWHERE ", 1)
    selections = re.findall(r"MIN\((\w+)\.(\w+)\) AS (\w+)", projection)
    assert selections
    assert re.sub(r"MIN\(\w+\.\w+\) AS \w+|,|\s", "", projection) == ""
    aliases: dict[str, str] = {}
    for source in from_clause.split(","):
        table, alias = source.strip().split(" AS ")
        assert table in JOB_SCHEMA
        assert alias not in aliases
        aliases[alias] = table
    without_literals = re.sub(r"'(?:''|[^'])*'", "''", where)
    for alias, column in [*(match[:2] for match in selections), *re.findall(r"(\w+)\.(\w+)", without_literals)]:
        assert alias in aliases
        assert column in {definition.name for definition in JOB_SCHEMA[aliases[alias]]}
    edges = re.findall(r"(\w+)\.\w+\s*=\s*(\w+)\.\w+", without_literals)
    reached = {next(iter(aliases))}
    for _ in aliases:
        for left, right in edges:
            if left in reached or right in reached:
                reached.update((left, right))
    assert reached == set(aliases)
    assert "GROUP BY" not in sql and "LIMIT" not in sql and "DISTINCT" not in sql


def test_original_schema_retains_all_21_tables_int32_nullability_and_varchar_lengths() -> None:
    assert len(JOB_TABLES) == 21
    for columns in JOB_SCHEMA.values():
        assert columns[0].name == "id" and columns[0].sql_type == "integer" and not columns[0].nullable
        assert len({column.name for column in columns}) == len(columns)
        for column in columns:
            assert column.sql_type in {"integer", "text"} or column.max_characters is not None
    original = (DIRECTORY / "upstream/schema.sql").read_text()
    assert (DIRECTORY / "schemas/duckdb.sql").read_text() == original
    assert (DIRECTORY / "schemas/monetdb.sql").read_text() == original
    postgres = (DIRECTORY / "schemas/postgres.sql").read_text()
    assert postgres.replace(' COLLATE "C"', "") == original
    assert postgres.count('COLLATE "C"') == sum(
        column.sql_type != "integer" for columns in JOB_SCHEMA.values() for column in columns
    )


@pytest.mark.parametrize("database", ["clickhouse", "starrocks"])
def test_column_store_schema_maps_original_column_order_and_nullability(database: str) -> None:
    sql = (DIRECTORY / "schemas" / f"{database}.sql").read_text()
    tables = re.findall(r"CREATE TABLE (\w+) \(\n(.*?)\n\)", sql, re.DOTALL)
    assert tuple(table for table, _ in tables) == JOB_TABLES
    for table, definitions in tables:
        for definition, column in zip(definitions.split(",\n"), JOB_SCHEMA[table], strict=True):
            name, dtype = definition.strip().split(" ", 1)
            assert name == column.name
            if database == "clickhouse":
                base = "Int32" if column.sql_type == "integer" else "String"
                assert dtype == (f"Nullable({base})" if column.nullable else base)
            else:
                base = (
                    "INT"
                    if column.sql_type == "integer"
                    else (f"VARCHAR({column.max_characters * 4 if column.max_characters else 65533})")
                )
                assert dtype == base + (" NULL" if column.nullable else " NOT NULL")
    assert ("ORDER BY (id)" if database == "clickhouse" else "DUPLICATE KEY (id)") in sql
    assert "REPLACE" not in sql and "UNIQUE KEY" not in sql


@pytest.mark.parametrize("database", sorted(CANDIDATE_DATABASES))
def test_suite_resolves_all_canonical_queries_with_semantic_fetch_settings(database: DatabaseName) -> None:
    benchmark = suite(FixtureDatabase(name=database))
    assert benchmark.planned_query_names() == JOB_QUERY_NAMES
    for name in JOB_QUERY_NAMES:
        original = (DIRECTORY / "queries" / f"{name}.sql").read_text()
        if database == "duckdb" and name in {"15a", "15b", "15c", "15d"}:
            assert benchmark.load_query(name) == original.replace("aka_title AS at,", "aka_title AS aka,").replace(
                "at.", "aka."
            )
        elif database == "starrocks" and name in {"10b", "10c"}:
            assert benchmark.load_query(name) == original.replace("AS character,", "AS `character`,")
        else:
            assert benchmark.load_query(name) == original
        assert benchmark.query_fetch_kwargs(name) == benchmark.fetch_kwargs
    if database == "clickhouse":
        assert benchmark.fetch_kwargs == {
            "settings": {"aggregate_functions_null_for_empty": 1, "join_default_strictness": "ALL"}
        }
    elif database == "postgres":
        assert benchmark.fetch_kwargs == {"method": "python"}
    else:
        assert benchmark.fetch_kwargs == {}


def test_suite_rejects_unrecognized_query_names() -> None:
    with pytest.raises(ValueError, match="Unknown canonical"):
        suite(FixtureDatabase()).load_query("../upstream/schema")


@pytest.mark.parametrize("operation", ["populate", "select"])
def test_lifecycle_rejects_missing_job_provenance_before_reusing_matching_counts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, operation: Literal["populate", "select"]
) -> None:
    monkeypatch.setattr(get_settings(), "input_data_directory", tmp_path)
    (tmp_path / "join_order").mkdir()
    database = FixtureDatabase(counts=dict.fromkeys(JOB_TABLES, 1))
    with pytest.raises(ValueError, match="partial or unexpected JOB dataset"):
        database.benchmark("join_order", operation, 1)


def test_suite_rejects_synthetic_scale_factors() -> None:
    with pytest.raises(ValueError, match="fixed original"):
        JoinOrder(db=FixtureDatabase(), scale_factor=10)


@pytest.mark.parametrize("restart", [False, True])
def test_populate_uses_shared_ingestion_and_verifies_all_tables(
    tmp_path: Path,
    snapshot: tuple[Path, SnapshotPin],
    monkeypatch: pytest.MonkeyPatch,
    restart: bool,
) -> None:
    archive, pin = snapshot
    output = tmp_path / "prepared"
    prepare_snapshot(output, pin, archive=archive)

    def input_directory(_self: object) -> Path:
        return output

    def verify_fixture(directory: Path) -> DatasetManifest:
        return verify_prepared_data(directory, pin)

    monkeypatch.setattr(JoinOrder, "input_data_directory", property(input_directory))
    monkeypatch.setattr(job_config, "verify_prepared_data", verify_fixture)
    database = FixtureDatabase()
    benchmark = suite(database)
    benchmark.populate(restart=restart)
    assert database.schemas == ["join_order"]
    assert database.inserted == [(output / f"{table}.parquet", table) for table in JOB_TABLES]
    assert database.phases.count("insert") == 21
    assert database.restarts == int(restart)
    benchmark.populate(restart=restart)
    assert len(database.inserted) == 21
    assert database.restarts == int(restart)


def test_populate_rejects_unreviewed_engine_before_accessing_inputs() -> None:
    with pytest.raises(NotImplementedError, match="no reviewed schema"):
        suite(FixtureDatabase(name="questdb")).populate()


def test_starrocks_refuses_overlong_text_before_import_or_engine_access(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    entries = [
        (
            f"{table}.csv",
            b"1,1,1," + b"x" * 65_534 + b",\n" if table == "person_info" else prepare_fixtures.minimal_csv(table),
            tarfile.REGTYPE,
        )
        for table in JOB_TABLES
    ]
    archive = tmp_path / "overlong.tgz"
    prepare_fixtures.write_archive(archive, entries)
    digest = fingerprint(archive)
    pin = SnapshotPin(digest["sha256"], digest["bytes"])
    output = tmp_path / "join_order"
    prepare_snapshot(output, pin, archive=archive)

    def input_directory(_self: object) -> Path:
        return output

    def verify_fixture(directory: Path) -> DatasetManifest:
        return verify_prepared_data(directory, pin)

    monkeypatch.setattr(JoinOrder, "input_data_directory", property(input_directory))
    monkeypatch.setattr(job_config, "verify_prepared_data", verify_fixture)
    database = FixtureDatabase(name="starrocks")
    with pytest.raises(ValueError, match="person_info.info.*65533-byte"):
        suite(database).populate()
    assert not database.schemas and not database.inserted
