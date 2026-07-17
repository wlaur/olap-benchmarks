# Derived from the TPC-H benchmark (http://www.tpc.org/tpch/); results are not
# comparable to published TPC-H results.
#
# Data is generated with tpchgen-cli (https://github.com/clflushopt/tpchgen-rs),
# which produces output byte-identical to the reference dbgen.
#
# Query provenance:
#   * base queries: DuckDB tpch extension (validation substitution parameters,
#     date arithmetic pre-evaluated, Q15 as a CTE), used by duckdb, postgres,
#     timescaledb and monetdb
#   * queries/clickhouse: official ClickHouse adaptations from
#     https://github.com/ClickHouse/ClickHouse/tree/master/tests/benchmarks/tpc-h
#   * queries/starrocks: official StarRocks adaptations from
#     https://docs.starrocks.io/docs/benchmarking/TPC-H_Benchmarking/

import logging
import shutil
import subprocess
from decimal import Decimal
from time import perf_counter
from typing import Any

import polars as pl

from ...dbs import Database
from ...settings import REPO_ROOT, SETTINGS, TableName, format_suite_data_directory_name, resolve_suite_scale_factor
from .. import BenchmarkSuite

_LOGGER = logging.getLogger(__name__)

TPC_H_QUERIES_DIRECTORY = REPO_ROOT / "olap_benchmarks/suites/tpc_h/queries"

TPCH_TABLES = (
    "region",
    "nation",
    "supplier",
    "customer",
    "part",
    "partsupp",
    "orders",
    "lineitem",
)

TPCH_QUERY_NAMES = {
    "01_pricing_summary": 3,
    "02_minimum_cost_supplier": 3,
    "03_shipping_priority": 3,
    "04_order_priority": 3,
    "05_local_supplier_volume": 3,
    "06_revenue_forecast": 3,
    "07_volume_shipping": 3,
    "08_market_share": 3,
    "09_product_profit": 3,
    "10_returned_items": 3,
    "11_important_stock": 3,
    "12_shipping_modes": 3,
    "13_customer_distribution": 3,
    "14_promotion_effect": 3,
    "15_top_supplier": 3,
    "16_parts_supplier": 3,
    "17_small_quantity_revenue": 3,
    "18_large_volume_customer": 3,
    "19_discounted_revenue": 3,
    "20_potential_promotion": 3,
    "21_suppliers_waiting": 3,
    "22_global_sales_opportunity": 3,
}

# The Q11 HAVING threshold is defined as 0.0001 / SF (spec section 2.4.11.3);
# the committed query files carry the SF = 1 value.
Q11_SF1_FRACTION = "0.0001"


def q11_fraction(scale_factor: int) -> str:
    return f"{Decimal(Q11_SF1_FRACTION) / scale_factor:f}"


def _prepare_tpc_h_data(scale_factor: int) -> None:
    scale_factor = resolve_suite_scale_factor("tpc_h", scale_factor)
    data_directory_name = format_suite_data_directory_name("tpc_h", scale_factor)
    output_directory = SETTINGS.input_data_directory / data_directory_name
    output_directory.mkdir(exist_ok=True, parents=True)

    existing = [table for table in TPCH_TABLES if (output_directory / f"{table}.parquet").is_file()]

    if len(existing) == len(TPCH_TABLES):
        _LOGGER.info(
            f"All {data_directory_name} Parquet files already exist in {output_directory}, skipping generation"
        )
        return

    if existing:
        raise ValueError(f"{output_directory} contains a partial dataset ({', '.join(existing)}); remove it first")

    if shutil.which("tpchgen-cli") is None:
        raise RuntimeError("tpchgen-cli not found on PATH; install it with 'cargo install tpchgen-cli'")

    command = [
        "tpchgen-cli",
        "parquet",
        "--scale-factor",
        str(scale_factor),
        "--output-dir",
        output_directory.as_posix(),
    ]

    _LOGGER.info(f"Generating TPC-H data at scale factor {scale_factor}: {' '.join(command)}")
    t0 = perf_counter()
    subprocess.run(command, check=True)
    _LOGGER.info(f"Generated {data_directory_name} data in {perf_counter() - t0:_.2f} seconds")


def prepare_data(scale_factor: int) -> None:
    _prepare_tpc_h_data(scale_factor)


class TpcH[DBT: Database](BenchmarkSuite[DBT]):
    def expected_table_row_counts(self) -> dict[TableName, int]:
        return {
            table_name: self.parquet_row_count(self.input_data_directory / f"{table_name}.parquet")
            for table_name in TPCH_TABLES
        }

    @property
    def populate_kwargs(self) -> dict[str, Any]:
        return {}

    def insert_table(self, df: pl.LazyFrame, table_name: TableName) -> None:
        self.db.insert(df, table_name, **self.populate_kwargs)

    def populate(self, restart: bool = True) -> None:
        with self.db.phase_context("verify_existing_data"):
            if not self.should_populate():
                return

        self.db.initialize_schema("tpc_h")

        for table_name in TPCH_TABLES:
            df = pl.scan_parquet(self.input_data_directory / f"{table_name}.parquet")

            with self.db.phase_context("insert", table_name=table_name):
                self.insert_table(df, table_name)
                _LOGGER.info(f"Inserted {table_name} for {self.name}")

        _LOGGER.info(f"Inserted all tpc_h tables for {self.name} scale factor {self.scale_factor}")

        with self.db.phase_context("verify_populate"):
            self.verify_populated_data()

        # restart db to ensure data is not kept in-memory by the db, and also
        # ensure that WAL is processed etc...
        if restart:
            self.db.restart_event()

    @property
    def fetch_kwargs(self) -> dict[str, Any]:
        return {}

    def load_tpch_query(self, query_name: str) -> str:
        db_specific = TPC_H_QUERIES_DIRECTORY / f"{self.db.name}/{query_name}.sql"
        common = TPC_H_QUERIES_DIRECTORY / f"{query_name}.sql"

        sql_source = db_specific if db_specific.is_file() else common

        with sql_source.open() as f:
            query = f.read()

        if query_name.startswith("11_"):
            assert query.count(Q11_SF1_FRACTION) == 1
            query = query.replace(Q11_SF1_FRACTION, q11_fraction(self.scale_factor))

        return query

    def include_query(self, query_name: str) -> bool:
        return True

    def select(self) -> None:
        t0 = perf_counter()
        failed_queries = 0
        for idx, (query_name, iterations) in enumerate(TPCH_QUERY_NAMES.items()):
            progress_label = f"({idx + 1:_}/{len(TPCH_QUERY_NAMES):_})"
            if not self.include_query(query_name):
                self.record_skipped_query_steps(
                    query_name,
                    iterations,
                    result_status="skipped",
                    reason="query excluded by suite/database",
                )
                continue

            def log_success(
                it: int,
                df: pl.DataFrame,
                t: float,
                *,
                query_name: str = query_name,
                progress_label: str = progress_label,
                iterations: int = iterations,
            ) -> None:
                _LOGGER.info(
                    f"Executed {query_name} {progress_label} "
                    f"iteration {it:_}/{iterations:_} "
                    f"in {1_000 * (t):_.2f} ms\ndf={df}"
                )

            ok = self.execute_query_with_isolation(
                query_name=query_name,
                iterations=iterations,
                query_loader=lambda query_name=query_name: self.load_tpch_query(query_name),
                fetch_kwargs_factory=lambda: self.fetch_kwargs,
                progress_label=progress_label,
                log_success=log_success,
            )
            if not ok:
                failed_queries += 1

        if failed_queries:
            _LOGGER.warning(
                f"TPC-H select completed on {self.db.name} with {failed_queries:_} failed "
                f"{'queries' if failed_queries != 1 else 'query'}"
            )

        _LOGGER.info(
            f"Executed {len(TPCH_QUERY_NAMES):_} queries (with repetitions) in {perf_counter() - t0:_.2f} seconds"
        )
