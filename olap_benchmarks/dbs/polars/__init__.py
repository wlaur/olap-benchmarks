from __future__ import annotations

from collections.abc import Mapping
from importlib.metadata import version as package_version
from pathlib import Path
from shutil import rmtree
from typing import Any, cast

import polars as pl
from sqlalchemy import Connection

from ...settings import SETTINGS, DatabaseName, SuiteName, TableName
from ...suites import BenchmarkSuite
from ...suites.jsonbench.config import JSONBench, get_jsonbench_input_files, write_jsonbench_input_file
from .. import Database

VERSION = package_version("polars")


class PolarsJSONBench(JSONBench["Polars"]):
    def _stage_input_files(self) -> Path:
        temp_dir = SETTINGS.temporary_directory / "polars/data"
        staging_dir = temp_dir / self.data_directory_name
        if staging_dir.exists():
            rmtree(staging_dir)
        staging_dir.mkdir(parents=True)

        for input_file in get_jsonbench_input_files(self.scale_factor):
            staged_file = staging_dir / input_file.name.removesuffix(".gz")
            write_jsonbench_input_file(input_file, staged_file)

        return staging_dir

    def populate(self, restart: bool = True) -> None:
        with self.db.phase_context("verify_existing_data"):
            if not self.should_populate():
                return

        table_path = self.db.table_path("bluesky")

        staging_dir = self._stage_input_files()
        try:
            input_files = [fpath.as_posix() for fpath in sorted(staging_dir.glob("file_*.json"))]

            with self.db.phase_context("insert", table_name="bluesky"):
                pl.scan_ndjson(input_files).sink_parquet(table_path)
        finally:
            rmtree(staging_dir)

        with self.db.phase_context("verify_populate"):
            self.verify_populated_data()

        if restart:
            self.db.restart_event()


class Polars(Database):
    name: DatabaseName = "polars"
    version: str = VERSION
    connection_string: str = ""

    @property
    def start(self) -> None:
        return None

    @property
    def stop(self) -> None:
        return None

    @property
    def restart(self) -> None:
        return None

    def get_runtime_version(self) -> str:
        return VERSION

    def table_path(self, table: TableName) -> Path:
        return self.database_directory / f"{table}.parquet"

    def _scan_table(self, table: TableName) -> pl.LazyFrame:
        fpath = self.table_path(table)
        if not fpath.is_file():
            raise RuntimeError(f"Polars table does not exist: {table}")
        return pl.scan_parquet(fpath)

    def connect(self, reconnect: bool = False) -> Connection:
        _ = reconnect
        raise NotImplementedError("Polars does not expose a SQL connection")

    def fetch(
        self,
        query: str,
        schema: Mapping[str, pl.DataType | type[pl.DataType]] | None = None,
    ) -> pl.DataFrame:
        _ = query

        if self.current_suite != "jsonbench" or self.current_query_name is None:
            raise NotImplementedError("Polars fetch is implemented for JSONBench query execution only")

        lf = self._execute_jsonbench_query(self.current_query_name)
        df = lf.collect()

        if schema is not None:
            df = df.cast(cast(pl.Schema, schema))

        return df

    def _jsonbench_source(self) -> pl.LazyFrame:
        return self._scan_table("bluesky")

    def _commit_collection(self) -> pl.Expr:
        return pl.col("commit").struct.field("collection")

    def _commit_operation(self) -> pl.Expr:
        return pl.col("commit").struct.field("operation")

    def _create_post_filter(self) -> pl.Expr:
        return (
            (pl.col("kind") == "commit")
            & (self._commit_operation() == "create")
            & (self._commit_collection() == "app.bsky.feed.post")
        )

    def _execute_jsonbench_query(self, query_name: str) -> pl.LazyFrame:
        lf = self._jsonbench_source()

        match query_name:
            case "01_events_by_collection":
                return (
                    lf.select(self._commit_collection().alias("event"))
                    .group_by("event")
                    .agg(pl.len().cast(pl.Int64).alias("count"))
                    .sort("count", descending=True)
                )
            case "02_create_events_by_collection":
                return (
                    lf.filter((pl.col("kind") == "commit") & (self._commit_operation() == "create"))
                    .select(self._commit_collection().alias("event"), pl.col("did"))
                    .group_by("event")
                    .agg(
                        pl.len().cast(pl.Int64).alias("count"),
                        pl.col("did").n_unique().cast(pl.Int64).alias("users"),
                    )
                    .sort("count", descending=True)
                )
            case "03_create_events_by_hour":
                return (
                    lf.filter(
                        (pl.col("kind") == "commit")
                        & (self._commit_operation() == "create")
                        & self._commit_collection().is_in(
                            ["app.bsky.feed.post", "app.bsky.feed.repost", "app.bsky.feed.like"]
                        )
                    )
                    .select(
                        self._commit_collection().alias("event"),
                        pl.from_epoch("time_us", time_unit="us").dt.hour().cast(pl.Int64).alias("hour_of_day"),
                    )
                    .group_by("event", "hour_of_day")
                    .agg(pl.len().cast(pl.Int64).alias("count"))
                    .sort("hour_of_day", "event")
                )
            case "04_first_post_users":
                return (
                    lf.filter(self._create_post_filter())
                    .group_by("did")
                    .agg(pl.col("time_us").min().alias("first_post_us"))
                    .select(
                        pl.col("did").alias("user_id"),
                        pl.from_epoch("first_post_us", time_unit="us").cast(pl.Datetime("ms")).alias("first_post_ts"),
                    )
                    .sort("first_post_ts")
                    .limit(3)
                )
            case "05_longest_post_activity":
                return (
                    lf.filter(self._create_post_filter())
                    .group_by("did")
                    .agg(((pl.col("time_us").max() - pl.col("time_us").min()) // 1000).alias("activity_span"))
                    .select(pl.col("did").alias("user_id"), pl.col("activity_span").cast(pl.Int64))
                    .sort("activity_span", descending=True)
                    .limit(3)
                )
            case _:
                raise ValueError(f"Unsupported Polars JSONBench query: {query_name}")

    def get_row_count(self, table: TableName) -> int:
        return int(self._scan_table(table).select(pl.len()).collect().item(0, 0))

    def get_table_names(self) -> set[TableName]:
        return {fpath.stem for fpath in self.database_directory.glob("*.parquet")}

    def insert(
        self,
        df: pl.DataFrame | pl.LazyFrame,
        table: TableName,
        primary_key: str | list[str] | None = None,
        not_null: str | list[str] | None = None,
    ) -> None:
        _ = df, table, primary_key, not_null
        raise NotImplementedError("Polars suites should write table files directly")

    def upsert(self, df: pl.DataFrame, table: TableName, primary_key: str | list[str]) -> None:
        _ = df, table, primary_key
        raise NotImplementedError("Polars does not support generic upsert")

    def delete(self, table: TableName, primary_key: str | list[str], keys: pl.DataFrame) -> None:
        _ = table, primary_key, keys
        raise NotImplementedError("Polars does not support generic delete")

    def suite_registry(self) -> Mapping[SuiteName, type[BenchmarkSuite[Any]]]:
        return {"jsonbench": PolarsJSONBench}
