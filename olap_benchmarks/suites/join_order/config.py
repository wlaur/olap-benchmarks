from __future__ import annotations

from typing import Any, ClassVar

from pydantic import model_validator

from ...dbs import Database
from ...settings import DatabaseName, SuiteName, TableName
from .. import BenchmarkSuite
from .prepare import prepare_data, verify_prepared_data
from .schema import DIRECTORY, JOB_QUERY_NAMES, JOB_TABLES

__all__ = ["JoinOrder", "prepare_data"]

SUITE_NAME: SuiteName = "join_order"
CANDIDATE_DATABASES: frozenset[DatabaseName] = frozenset({"duckdb", "postgres", "monetdb", "clickhouse", "starrocks"})


class JoinOrder[DBT: Database](BenchmarkSuite[DBT]):
    name: SuiteName = SUITE_NAME
    candidate_databases: ClassVar[frozenset[DatabaseName]] = CANDIDATE_DATABASES

    def verify_input_data(self) -> None:
        verify_prepared_data(self.input_data_directory)

    @model_validator(mode="after")
    def _fixed_snapshot(self) -> JoinOrder[DBT]:
        if self.scale_factor != 1:
            raise ValueError("JOB uses the fixed original May 2013 snapshot; scale_factor must be 1")
        return self

    def expected_table_row_counts(self) -> dict[TableName, int]:
        return {table: self.parquet_row_count(self.input_data_directory / f"{table}.parquet") for table in JOB_TABLES}

    def populate(self, restart: bool = True) -> None:
        if self.db.name not in self.candidate_databases:
            raise NotImplementedError(f"JOB has no reviewed schema or query dialect for {self.db.name}")
        manifest = verify_prepared_data(self.input_data_directory)
        if self.db.name == "starrocks":
            for table, entry in manifest["tables"].items():
                for column, size in entry["max_utf8_bytes"].items():
                    if size > 65_533:
                        raise ValueError(f"JOB {table}.{column} exceeds StarRocks' 65533-byte text mapping")
        if not (DIRECTORY / "schemas" / f"{self.db.name}.sql").is_file():
            raise FileNotFoundError(f"Missing JOB schema for {self.db.name}")
        with self.db.phase_context("verify_existing_data"):
            if not self.should_populate():
                return
        self.db.initialize_schema("join_order")
        for table in JOB_TABLES:
            with self.db.phase_context("insert", table_name=table):
                self.db.insert_parquet(self.input_data_directory / f"{table}.parquet", table)
        with self.db.phase_context("verify_populate"):
            self.verify_populated_data()
        if restart:
            self.db.restart_event()

    @property
    def fetch_kwargs(self) -> dict[str, Any]:
        if self.db.name == "clickhouse":
            return {"settings": {"aggregate_functions_null_for_empty": 1, "join_default_strictness": "ALL"}}
        if self.db.name == "postgres":
            return {"method": "python"}
        return {}

    def load_query(self, query_name: str) -> str:
        if query_name not in JOB_QUERY_NAMES:
            raise ValueError(f"Unknown canonical JOB query: {query_name}")
        return super().load_query(query_name)

    def planned_query_names(self) -> tuple[str, ...]:
        return JOB_QUERY_NAMES
