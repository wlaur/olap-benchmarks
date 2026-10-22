from __future__ import annotations

import os
from collections.abc import Generator, Mapping
from contextlib import contextmanager
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import duckdb
import polars as pl
import pytest
from pydantic import PrivateAttr

from ..dbs.clickhouse import Clickhouse
from ..dbs.duckdb import DuckDB
from ..dbs.polars import Polars
from ..settings import SUITE_SPECS, SuiteName, SuiteSpec, get_settings
from ..suites import BenchmarkSuite
from ..suites.json_representation.config import SUITE_NAME as JSON_REPRESENTATION
from ..suites.parquet_analytics.config import (
    SF10_ROW_COUNTS,
    SUITE_NAME,
    ParquetAnalytics,
    canonical_columns,
    inspect_sources,
    polars_dtype,
    prepare_data,
    queries_for_publication,
    source_directory,
)
from ..suites.parquet_analytics.contract import RepresentationManifest, manifest_metadata, required_external_files
from ..suites.tpc_h.config import TPCH_QUERY_NAMES, TPCH_TABLES, q11_fraction


class MemoryDuckDB(DuckDB):
    _fixture: duckdb.DuckDBPyConnection = PrivateAttr(default_factory=duckdb.connect)

    def raw_connection(self) -> duckdb.DuckDBPyConnection:
        return self._fixture

    def execute(self, statement: str, commit: bool = True, autocommit: bool = False, reconnect: bool = False) -> None:
        self._fixture.execute(statement)

    def fetch(self, query: str, schema: Mapping[str, pl.DataType | type[pl.DataType]] | None = None) -> pl.DataFrame:
        frame = self._fixture.sql(query).pl()
        return frame.cast(cast(pl.Schema, schema)) if schema is not None else frame

    def rollback(self) -> None:
        pass

    @contextmanager
    def phase_context(self, phase_name: str, table_name: str | None = None) -> Generator[None]:
        yield

    @property
    def benchmarks(self) -> dict[SuiteName, BenchmarkSuite[Any]]:
        return {SUITE_NAME: ParquetAnalytics(db=self, scale_factor=10)}


class FixturePolars(Polars):
    @contextmanager
    def phase_context(self, phase_name: str, table_name: str | None = None) -> Generator[None]:
        yield

    @property
    def benchmarks(self) -> dict[SuiteName, BenchmarkSuite[Any]]:
        return {SUITE_NAME: ParquetAnalytics(db=self, scale_factor=10)}


@pytest.fixture
def inputs(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    settings = get_settings()
    monkeypatch.setattr(settings, "input_data_directory", tmp_path / "input")
    monkeypatch.setattr(settings, "database_directory", tmp_path / "stores")
    monkeypatch.setattr(settings, "temporary_directory", tmp_path / "temporary")
    monkeypatch.setitem(SUITE_SPECS, SUITE_NAME, SuiteSpec("Parquet Analytics", (10,), 10, True))
    directory = source_directory(10)
    directory.mkdir(parents=True)
    for table, columns in canonical_columns().items():
        monkeypatch.setitem(SF10_ROW_COUNTS, table, 1)
        values: dict[str, list[object]] = {}
        for name, dtype, _ in columns:
            values[name] = [
                ""
                if dtype.startswith("VARCHAR")
                else date(1994, 1, 1)
                if dtype == "DATE"
                else Decimal("1.00")
                if dtype.startswith("DECIMAL")
                else 1
            ]
        pl.DataFrame(values, schema={name: polars_dtype(dtype) for name, dtype, _ in columns}).write_parquet(
            directory / f"{table}.parquet", row_group_size=1, compression="zstd"
        )
    return directory


def test_canonical_columns_cover_all_eight_source_schemas(inputs: Path) -> None:
    columns = canonical_columns()
    assert tuple(columns) == TPCH_TABLES
    assert sum(map(len, columns.values())) == 61
    assert columns["region"][-1] == ("r_comment", "VARCHAR(152)", False)
    assert columns["lineitem"][4] == ("l_quantity", "DECIMAL(15,2)", True)
    before = {path.name: path.read_bytes() for path in inputs.glob("*.parquet")}
    prepare_data(10)
    assert inspect_sources(10)["partitioning"] == "none"
    assert before == {path.name: path.read_bytes() for path in inputs.glob("*.parquet")}


@pytest.mark.parametrize("mutation", ["missing", "extra", "partitioned", "schema", "wrong_scale"])
def test_common_file_contract_rejects_alternative_inputs(inputs: Path, mutation: str) -> None:
    if mutation == "missing":
        (inputs / "region.parquet").unlink()
    elif mutation == "extra":
        (inputs / "extra.parquet").write_bytes((inputs / "region.parquet").read_bytes())
    elif mutation == "partitioned":
        (inputs / "partition").mkdir()
        (inputs / "region.parquet").rename(inputs / "partition/region.parquet")
    elif mutation == "schema":
        pl.DataFrame({"wrong": [1]}).write_parquet(inputs / "region.parquet")
    else:
        frame = pl.read_parquet(inputs / "region.parquet")
        pl.concat([frame, frame]).write_parquet(inputs / "region.parquet")
    with pytest.raises(RuntimeError, match="eight original|schema mismatch|SF10 row count"):
        prepare_data(10)


def test_sf10_only_and_effective_storage_modes() -> None:
    with pytest.raises(ValueError, match="SF10"):
        source_directory(1)
    db = DuckDB()
    db._current_suite = SUITE_NAME
    assert db.storage_mode == "parquet"
    db._current_suite = JSON_REPRESENTATION
    assert db.storage_mode == "native"


def test_duckdb_uses_only_external_views_and_rejects_source_changes(inputs: Path) -> None:
    db = MemoryDuckDB()
    db._current_suite = SUITE_NAME
    db._current_suite_scale_factor = 10
    db._current_operation = "populate"
    suite = ParquetAnalytics(db=db, scale_factor=10)
    try:
        suite.populate(restart=False)
        db.finalize_populate()
        db.verify_storage_readiness()
        assert db.storage.read().mode == "parquet"
        assert db.fetch("SELECT DISTINCT table_type FROM information_schema.tables").rows() == [("VIEW",)]
        assert len(db.storage.external_files()) == 8
        assert all(relation.view_sha256 for relation in db.storage.read().relations.values())
        assert manifest_metadata(db.database_directory)["state"] == "ready"
        (inputs / "region.parquet").touch()
        with pytest.raises(RuntimeError, match="source changed|source/layout changed"):
            db.verify_storage_readiness()
    finally:
        db.raw_connection().close()


def test_polars_external_dispatch_and_replaced_links(inputs: Path) -> None:
    db = FixturePolars()
    db._current_suite = SUITE_NAME
    db._current_suite_scale_factor = 10
    db._current_operation = "populate"
    suite = ParquetAnalytics(db=db, scale_factor=10)
    suite.populate(restart=False)
    db.verify_storage_readiness()
    assert all(db.table_path(table).is_symlink() for table in TPCH_TABLES)
    db.current_query_name = "06_revenue_forecast"
    result = db.fetch(suite.load_query("06_revenue_forecast"))
    assert result.shape == (1, 1)
    db.table_path("region").unlink()
    db.table_path("region").write_bytes((inputs / "region.parquet").read_bytes())
    with pytest.raises(RuntimeError, match="external Parquet link"):
        db.verify_storage_readiness()


def test_queries_resolve_original_files_and_q11_scale_factor() -> None:
    for db in (DuckDB(), Polars(), Clickhouse()):
        suite = ParquetAnalytics(db=db, scale_factor=10)
        assert suite.planned_query_names() == TPCH_QUERY_NAMES
        assert len(suite.planned_query_names()) == 22
        for name in TPCH_QUERY_NAMES:
            path = suite.query_path(name)
            assert path is not None and path.is_file()
            assert "tpc_h/queries" in path.as_posix()
        assert q11_fraction(10) in suite.load_specification_query("11_important_stock")
        assert suite.query_fetch_kwargs("17_small_quantity_revenue") == (
            {"settings": {"aggregate_functions_null_for_empty": 1}} if db.name == "clickhouse" else {}
        )
        assert suite.query_fetch_kwargs("05_local_supplier_volume") == (
            {"settings": {"aggregate_functions_null_for_empty": 1, "query_plan_optimize_join_order_limit": 0}}
            if db.name == "clickhouse"
            else {}
        )
        if db.name == "polars":
            assert suite.load_query("06_revenue_forecast") == "Polars LazyFrame expression 06_revenue_forecast"


def test_publication_helper_supplies_all_concrete_sql_at_sf10() -> None:
    manifest = queries_for_publication()
    assert tuple(manifest) == TPCH_QUERY_NAMES
    assert q11_fraction(10) in str(manifest["11_important_stock"]["sql"])
    assert q11_fraction(10) in str(manifest["11_important_stock"]["db_overrides"])
    assert "SELECT" in str(manifest["06_revenue_forecast"]["sql"])


def test_clickhouse_view_uses_original_files_with_canonical_types() -> None:
    suite = ParquetAnalytics(db=Clickhouse(), scale_factor=10)
    query = suite._clickhouse_view("lineitem")
    assert "file('parquet_analytics/lineitem.parquet', 'Parquet')" in query
    assert "'Decimal(15,2)'" in query
    assert "'Int64'" in query
    assert "CREATE VIEW lineitem" in query
    assert "MergeTree" not in query and "INSERT" not in query


def test_clickhouse_mounts_original_directory_read_only(inputs: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def command(self: Clickhouse, image: str, **kwargs: object) -> str:
        captured.update(kwargs)
        return "fixture-command"

    def images(self: Clickhouse) -> dict[str, str]:
        return {"server": "fixture-image"}

    monkeypatch.setattr(Clickhouse, "docker_run_command", command)
    monkeypatch.setattr(Clickhouse, "container_images", property(images))
    db = Clickhouse()
    db._current_suite = SUITE_NAME
    db._current_suite_scale_factor = 10
    assert db.start == "fixture-command"
    mounts = cast(dict[str, str], captured["mounts"])
    assert mounts[inputs.resolve().as_posix()] == "/benchmark_input/parquet_analytics:ro"
    assert captured["args"] == ("--", "--user_files_path=/benchmark_input")
    assert not mounts[inputs.resolve().as_posix()].startswith("/var/lib/clickhouse/")
    assert set(db.metric_directories[1:]) == {path.resolve() for path in inputs.glob("*.parquet")}


def test_manifest_and_required_disk_deduplicate_hardlinks_and_links(inputs: Path, tmp_path: Path) -> None:
    source = inputs / "region.parquet"
    store = tmp_path / "accounted-store"
    store.mkdir()
    alias = tmp_path / "alias.parquet"
    alias.symlink_to(source)
    assert required_external_files(store, [source, alias, source]) == (source.resolve(),)
    os.link(source, store / "required.parquet")
    assert required_external_files(store, [source, alias]) == ()
    manifest = RepresentationManifest.build("fixture", inputs, [source], {"layout": "external"})
    manifest.write(store)
    with pytest.raises(RuntimeError, match="incomplete"):
        RepresentationManifest.read(store).verify("fixture", inputs, [source], {"layout": "external"})
    manifest.state = "ready"
    manifest.write(store)
    RepresentationManifest.read(store).verify("fixture", inputs, [source], {"layout": "external"})
    with pytest.raises(RuntimeError, match="source/layout changed"):
        manifest.verify("fixture", inputs, [source], {"layout": "native-import"})
