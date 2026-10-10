# Derived from the TPC-H benchmark (http://www.tpc.org/tpch/); results are not
# comparable to published TPC-H results.
#
# Data is generated with tpchgen-cli (https://github.com/clflushopt/tpchgen-rs),
# which produces output byte-identical to the reference dbgen. Pinned to the same
# commit as tpcgen-cli (see suites/tpc_ds/config.py) rather than the crates.io
# release, which lags main; install with 'cargo install --git
# https://github.com/clflushopt/tpchgen-rs
# --rev e53dea45345d3c934c724147e393983a53a40986 tpchgen-cli'.
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

TPCH_QUERY_NAMES = (
    "01_pricing_summary",
    "02_minimum_cost_supplier",
    "03_shipping_priority",
    "04_order_priority",
    "05_local_supplier_volume",
    "06_revenue_forecast",
    "07_volume_shipping",
    "08_market_share",
    "09_product_profit",
    "10_returned_items",
    "11_important_stock",
    "12_shipping_modes",
    "13_customer_distribution",
    "14_promotion_effect",
    "15_top_supplier",
    "16_parts_supplier",
    "17_small_quantity_revenue",
    "18_large_volume_customer",
    "19_discounted_revenue",
    "20_potential_promotion",
    "21_suppliers_waiting",
    "22_global_sales_opportunity",
)

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
        raise RuntimeError(
            "tpchgen-cli not found on PATH; install it with 'cargo install --git "
            "https://github.com/clflushopt/tpchgen-rs --rev e53dea45345d3c934c724147e393983a53a40986 tpchgen-cli'"
        )

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

    def populate(self, restart: bool = True) -> None:
        with self.db.phase_context("verify_existing_data"):
            if not self.should_populate():
                return

        self.db.initialize_schema("tpc_h")

        for table_name in TPCH_TABLES:
            source = self.input_data_directory / f"{table_name}.parquet"

            with self.db.phase_context("insert", table_name=table_name):
                self.db.insert_parquet(source, table_name)
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

    def query_fetch_kwargs(self, query_name: str) -> dict[str, Any]:
        """Fetch kwargs for one query, so a connector can special-case individual queries."""
        _ = query_name
        return self.fetch_kwargs

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

    def planned_query_names(self) -> tuple[str, ...]:
        return TPCH_QUERY_NAMES

    def load_query(self, query_name: str) -> str:
        return self.load_tpch_query(query_name)
