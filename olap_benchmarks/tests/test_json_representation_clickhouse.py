from __future__ import annotations

import json
import uuid
from collections.abc import Sequence
from typing import Protocol, cast

import duckdb
import polars as pl
import pytest
from polars.testing import assert_frame_equal
from pyarrow import Table

from ..dbs.clickhouse import Clickhouse, get_clickhouse_client
from ..dbs.duckdb import DuckDB
from ..suites.json_representation.config import (
    JSON_SETTINGS,
    SCHEMAS_DIRECTORY,
    JSONRepresentation,
    prepared_projection,
)
from ..suites.jsonbench.config import JSONBENCH_QUERY_NAMES
from .test_json_representation_queries import fixture_rows


class FixtureClickhouseClient(Protocol):
    def command(self, sql: str, *, settings: dict[str, int | str] | None = None) -> object: ...

    def insert(self, table: str, rows: Sequence[tuple[str]], *, column_names: list[str]) -> None: ...

    def query_arrow(self, sql: str, *, settings: dict[str, int | str]) -> Table: ...

    def close(self) -> None: ...


@pytest.mark.live
@pytest.mark.parametrize("empty", [False, True])
def test_clickhouse_memory_fixture_matches_canonical_outputs_and_schema(empty: bool) -> None:
    client = cast(FixtureClickhouseClient, get_clickhouse_client())
    database = f"json_representation_fixture_{uuid.uuid4().hex}"
    clickhouse_suite = JSONRepresentation(db=Clickhouse(), scale_factor=10)
    canonical_suite = JSONRepresentation(db=DuckDB(), scale_factor=10)

    def qualified(sql: str) -> str:
        return sql.replace("json_raw", f"{database}.json_raw").replace("json_prepared", f"{database}.json_prepared")

    try:
        client.command(f"CREATE DATABASE {database}")
        for statement in (SCHEMAS_DIRECTORY / "clickhouse.sql").read_text().split(";"):
            if statement.strip():
                memory_ddl = statement.replace("ENGINE = MergeTree ORDER BY tuple()", "ENGINE = Memory")
                client.command(qualified(memory_ddl))
        with duckdb.connect() as con:
            con.execute((SCHEMAS_DIRECTORY / "duckdb.sql").read_text())
            if not empty:
                rows = [(json.dumps(row),) for row in fixture_rows()]
                con.executemany("INSERT INTO json_raw VALUES (?)", rows)
                client.insert(f"{database}.json_raw", rows, column_names=["j"])
            client.command(
                qualified("INSERT INTO json_prepared " + prepared_projection("clickhouse")), settings=JSON_SETTINGS
            )
            for name in JSONBENCH_QUERY_NAMES:
                expected = con.sql(canonical_suite.load_query(f"raw_{name}")).pl()
                if "first_post_ts" in expected.columns:
                    assert expected.select((pl.col("first_post_ts").dt.microsecond() % 1000 == 0).all()).item()
                    expected = expected.with_columns(pl.col("first_post_ts").cast(pl.Datetime("ms")))
                for track in ("raw", "prepared"):
                    arrow = client.query_arrow(
                        qualified(clickhouse_suite.load_query(f"{track}_{name}")), settings=JSON_SETTINGS
                    )
                    result = pl.DataFrame(arrow)
                    if "first_post_ts" in result.columns:
                        result = result.with_columns(
                            pl.col("first_post_ts").cast(pl.Datetime("ms")).dt.replace_time_zone(None)
                        )
                    assert_frame_equal(result, expected, check_exact=True)
    finally:
        client.command(f"DROP DATABASE IF EXISTS {database}")
        client.close()
