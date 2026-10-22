from __future__ import annotations

import json
import re
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, cast

import polars as pl
from pyarrow.parquet import ParquetFile

from ...dbs import Database
from ...settings import DatabaseName, SuiteName, get_settings
from ..tpc_h.config import TPCH_QUERY_NAMES, TPCH_TABLES, TpcH
from .contract import RepresentationManifest, sql_literal

if TYPE_CHECKING:
    from ...dbs.clickhouse import Clickhouse
    from ...dbs.duckdb import DuckDB
    from ...dbs.polars import Polars

SUITE_NAME = cast(SuiteName, "parquet_analytics")
TPC_H_DIRECTORY = Path(__file__).parents[1] / "tpc_h"
SF10_ROW_COUNTS = {
    "region": 5,
    "nation": 25,
    "supplier": 100_000,
    "customer": 1_500_000,
    "part": 2_000_000,
    "partsupp": 8_000_000,
    "orders": 15_000_000,
    "lineitem": 59_986_052,
}


def source_directory(scale_factor: int) -> Path:
    if scale_factor != 10:
        raise ValueError("parquet_analytics supports only the canonical TPC-H SF10 files")
    return get_settings().input_data_directory / "tpc_h_sf10"


def canonical_columns() -> dict[str, list[tuple[str, str, bool]]]:
    schema = (TPC_H_DIRECTORY / "schemas/duckdb.sql").read_text()
    return {
        table: [
            (name, re.sub(r"\s+", "", dtype), bool(required))
            for name, dtype, required in re.findall(
                r"^\s+(\w+) (BIGINT|INTEGER|DATE|DECIMAL\(15, 2\)|VARCHAR\(\d+\))( NOT NULL)?[,]?$",
                body,
                re.MULTILINE,
            )
        ]
        for table, body in re.findall(r"CREATE TABLE (\w+) \((.*?)\n\);", schema, re.DOTALL)
    }


def polars_dtype(dtype: str) -> pl.DataType:
    if dtype.startswith("VARCHAR"):
        return pl.String()
    return {"BIGINT": pl.Int64(), "INTEGER": pl.Int32(), "DATE": pl.Date(), "DECIMAL(15,2)": pl.Decimal(15, 2)}[dtype]


def clickhouse_dtype(dtype: str, required: bool) -> str:
    mapped = (
        "String"
        if dtype.startswith("VARCHAR")
        else {"BIGINT": "Int64", "INTEGER": "Int32", "DATE": "Date", "DECIMAL(15,2)": "Decimal(15,2)"}[dtype]
    )
    return mapped if required else f"Nullable({mapped})"


def inspect_sources(scale_factor: int) -> dict[str, object]:
    directory = source_directory(scale_factor)
    files = sorted(directory.rglob("*.parquet"))
    if files != sorted(directory / f"{table}.parquet" for table in TPCH_TABLES):
        raise RuntimeError("parquet_analytics requires exactly the eight original, unpartitioned TPC-H files")
    tables: dict[str, object] = {}
    for table, columns in canonical_columns().items():
        path = directory / f"{table}.parquet"
        schema = pl.scan_parquet(path, hive_partitioning=False).collect_schema()
        expected = pl.Schema({name: polars_dtype(dtype) for name, dtype, _ in columns})
        if schema != expected or schema.names() != expected.names():
            raise RuntimeError(f"Canonical Parquet schema mismatch: {table}: {schema}")
        footer = ParquetFile(path).metadata
        if footer.num_rows != SF10_ROW_COUNTS[table]:
            raise RuntimeError(f"Expected canonical TPC-H SF10 row count for {table}: {SF10_ROW_COUNTS[table]:_}")
        tables[table] = {
            "schema": [(name, dtype, required) for name, dtype, required in columns],
            "rows": footer.num_rows,
            "row_groups": [footer.row_group(i).num_rows for i in range(footer.num_row_groups)],
            "compression": sorted(
                {
                    footer.row_group(i).column(j).compression
                    for i in range(footer.num_row_groups)
                    for j in range(footer.num_columns)
                }
            ),
        }
    return {"representation": "external-parquet", "partitioning": "none", "tables": tables}


def prepare_data(scale_factor: int) -> None:
    inspect_sources(scale_factor)


def queries_for_publication() -> dict[str, dict[str, str | None | dict[str, str]]]:
    from ...dbs.clickhouse import Clickhouse
    from ...dbs.duckdb import DuckDB

    shared = ParquetAnalytics(db=DuckDB(), scale_factor=10)
    clickhouse = ParquetAnalytics(db=Clickhouse(), scale_factor=10)
    return {
        name: {
            "sql": shared.load_specification_query(name),
            "db_overrides": {"clickhouse": clickhouse.load_specification_query(name)},
        }
        for name in TPCH_QUERY_NAMES
    }


class ParquetAnalytics[DBT: Database](TpcH[DBT]):
    name: SuiteName = SUITE_NAME
    candidate_databases: ClassVar[frozenset[DatabaseName]] = frozenset({"duckdb", "polars", "clickhouse"})

    @property
    def input_data_directory(self) -> Path:
        return source_directory(self.scale_factor)

    @property
    def queries_directory(self) -> Path:
        return TPC_H_DIRECTORY / "queries"

    def dataset_files(self) -> dict[str, Path]:
        return {table: self.input_data_directory / f"{table}.parquet" for table in TPCH_TABLES}

    def load_specification_query(self, query_name: str) -> str:
        return super().load_query(query_name)

    def load_query(self, query_name: str) -> str:
        if self.db.name == "polars":
            return f"Polars LazyFrame expression {query_name}"
        return self.load_specification_query(query_name)

    @property
    def fetch_kwargs(self) -> dict[str, Any]:
        return {"settings": {"aggregate_functions_null_for_empty": 1}} if self.db.name == "clickhouse" else {}

    def query_fetch_kwargs(self, query_name: str) -> dict[str, Any]:
        if self.db.name == "clickhouse" and query_name == "05_local_supplier_volume":
            return {"settings": {"aggregate_functions_null_for_empty": 1, "query_plan_optimize_join_order_limit": 0}}
        return self.fetch_kwargs

    def representation_layout(self) -> dict[str, object]:
        layout = cast(dict[str, object], json.loads(json.dumps(inspect_sources(self.scale_factor))))
        if self.db.name == "clickhouse" and self.db.get_table_names() == set(TPCH_TABLES):
            layout["view_definitions"] = {
                table: self.db.fetch(f"SHOW CREATE TABLE {table}").item() for table in TPCH_TABLES
            }
        return layout

    def verify_representation(self) -> None:
        RepresentationManifest.read(self.db.database_directory).verify(
            self.name, self.input_data_directory, list(self.dataset_files().values()), self.representation_layout()
        )
        if self.db.name == "polars":
            db = cast("Polars", self.db)
            for table, source in self.dataset_files().items():
                path = db.table_path(table)
                if not path.is_symlink() or path.resolve(strict=True) != source.resolve(strict=True):
                    raise RuntimeError(f"Expected external Parquet link: {table}")

    def _clickhouse_view(self, table: str) -> str:
        columns = canonical_columns()[table]
        projection = ", ".join(
            f"CAST({name}, {sql_literal(clickhouse_dtype(dtype, required))}) AS {name}"
            for name, dtype, required in columns
        )
        return f"CREATE VIEW {table} AS SELECT {projection} FROM file('parquet_analytics/{table}.parquet', 'Parquet')"

    def populate(self, restart: bool = True) -> None:
        if self.db.name not in self.candidate_databases:
            raise NotImplementedError(f"No common-file analytics contract for {self.db.name}")
        with self.db.phase_context("verify_existing_data"):
            if not self.should_populate():
                self.verify_representation()
                return
        with self.db.phase_context("source_contract"):
            manifest = RepresentationManifest.build(
                self.name, self.input_data_directory, list(self.dataset_files().values()), self.representation_layout()
            )
            manifest.write(self.db.database_directory)
        if self.db.name == "duckdb":
            db = cast("DuckDB", self.db)
            db.initialize_schema("tpc_h")
            for table, source in self.dataset_files().items():
                with db.phase_context("register", table_name=table):
                    db.insert_parquet(source, table)
        elif self.db.name == "polars":
            db = cast("Polars", self.db)
            for table, source in self.dataset_files().items():
                with db.phase_context("register", table_name=table):
                    db.register_table(table, source)
        else:
            db = cast("Clickhouse", self.db)
            for table in TPCH_TABLES:
                with db.phase_context("register", table_name=table):
                    db.run_sql(self._clickhouse_view(table))
            manifest.layout["view_definitions"] = {
                table: db.fetch(f"SHOW CREATE TABLE {table}").item() for table in TPCH_TABLES
            }
        with self.db.phase_context("verify_populate"):
            self.verify_populated_data()
            manifest.state = "ready"
            manifest.write(self.db.database_directory)
        if restart:
            self.db.restart_event()
