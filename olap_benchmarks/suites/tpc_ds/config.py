# Derived from the TPC-DS benchmark (http://www.tpc.org/tpcds/); results are not
# comparable to published TPC-DS results.
#
# Data is generated with tpcgen-cli from https://github.com/clflushopt/tpchgen-rs
# (commit 09d609d13b7b45a2aa06d2b86e5a4bfa29aacb1a, not yet on crates.io/PyPI;
# install with 'cargo install --git https://github.com/clflushopt/tpchgen-rs
# --rev 09d609d13b7b45a2aa06d2b86e5a4bfa29aacb1a tpcgen-cli') using
# '--compat c', which is conformance-tested against the reference dsdgen.
# The prepare step normalizes the generated Parquet in place: money columns are
# cast from DECIMAL(38,2) to the spec DECIMAL(7,2) (p_cost to DECIMAL(15,2)),
# and four columns are renamed to the spec names the queries reference
# (see SPEC_COLUMN_RENAMES).
#
# Queries are the 99 validation-parameter queries from the DuckDB tpcds
# extension (date arithmetic pre-evaluated, one statement per query), used
# unmodified by every database. Per-database overrides in queries/<db>/ are
# added only where the base form does not run; ClickHouse additionally gets the
# session settings from the official ClickHouse TPC-DS kit
# (https://github.com/ClickHouse/ClickHouse/tree/master/tests/benchmarks/tpc-ds)
# via fetch_kwargs.

import logging
import shutil
import subprocess
from pathlib import Path
from time import perf_counter
from typing import Any, ClassVar

import polars as pl

from ...dbs import Database
from ...settings import (
    REPO_ROOT,
    SETTINGS,
    DatabaseName,
    TableName,
    format_suite_data_directory_name,
    resolve_suite_scale_factor,
)
from .. import BenchmarkSuite

_LOGGER = logging.getLogger(__name__)

TPC_DS_QUERIES_DIRECTORY = REPO_ROOT / "olap_benchmarks/suites/tpc_ds/queries"

TPCDS_TABLES = (
    "call_center",
    "catalog_page",
    "catalog_returns",
    "catalog_sales",
    "customer",
    "customer_address",
    "customer_demographics",
    "date_dim",
    "household_demographics",
    "income_band",
    "inventory",
    "item",
    "promotion",
    "reason",
    "ship_mode",
    "store",
    "store_returns",
    "store_sales",
    "time_dim",
    "warehouse",
    "web_page",
    "web_returns",
    "web_sales",
    "web_site",
)

TPCDS_ITERATIONS = 3

TPCDS_QUERY_NAMES = tuple(f"{nr:02d}" for nr in range(1, 100))

BYTES_PER_GIB = 1024**3
BYTES_PER_MIB = 1024**2
TPCDS_PREPARE_FREE_SPACE_RESERVE_BYTES = 10 * BYTES_PER_GIB
TPCDS_PREPARE_BYTES_PER_SCALE_FACTOR = BYTES_PER_GIB

# tpcgen-cli column names that deviate from the TPC-DS spec / query references
SPEC_COLUMN_RENAMES: dict[TableName, dict[str, str]] = {
    "income_band": {"ib_income_band_id": "ib_income_band_sk"},
    "catalog_returns": {"cr_return_amount_inc_tax": "cr_return_amt_inc_tax"},
    "reason": {"r_reason_description": "r_reason_desc"},
    "web_returns": {"wr_store_credit": "wr_account_credit"},
}

# spec decimal precision for money columns; p_cost is DECIMAL(15,2) per spec
DECIMAL_PRECISION_OVERRIDES = {"p_cost": 15}
DECIMAL_PRECISION = 7
DECIMAL_SCALE = 2
TPCGEN_DECIMAL_PRECISION = 38
TPCGEN_DECIMAL_SCALE = 2


def _format_bytes(size: int) -> str:
    if size >= BYTES_PER_GIB:
        return f"{size / BYTES_PER_GIB:.1f} GiB"
    return f"{size / BYTES_PER_MIB:.1f} MiB"


def _free_disk_bytes(path: Path) -> int:
    return shutil.disk_usage(path).free


def _require_free_disk_space(path: Path, required_bytes: int, context: str) -> None:
    free_bytes = _free_disk_bytes(path)
    if free_bytes < required_bytes:
        raise RuntimeError(
            f"Refusing to {context}; need at least {_format_bytes(required_bytes)} free on "
            f"the volume containing {path}, found {_format_bytes(free_bytes)}"
        )


def _normalization_exprs(schema: pl.Schema, table_name: TableName) -> list[pl.Expr]:
    renames = SPEC_COLUMN_RENAMES.get(table_name, {})
    exprs: list[pl.Expr] = []

    for name, dtype in schema.items():
        expr = pl.col(name)

        if isinstance(dtype, pl.Decimal):
            expr = _normalize_decimal_expr(expr, name, dtype)

        if name in renames:
            expr = expr.alias(renames[name])

        exprs.append(expr)

    return exprs


def _normalize_decimal_expr(expr: pl.Expr, name: str, dtype: pl.Decimal) -> pl.Expr:
    target_precision = DECIMAL_PRECISION_OVERRIDES.get(name, DECIMAL_PRECISION)
    target_width = (target_precision, DECIMAL_SCALE)
    source_width = (TPCGEN_DECIMAL_PRECISION, TPCGEN_DECIMAL_SCALE)
    current_width = (dtype.precision, dtype.scale)

    if current_width == target_width:
        return expr

    if current_width != source_width:
        raise ValueError(
            f"Unexpected TPC-DS decimal width for {name}: DECIMAL{current_width}; "
            f"expected DECIMAL{source_width} from tpcgen-cli or normalized DECIMAL{target_width}"
        )

    return expr.cast(pl.Decimal(precision=target_precision, scale=DECIMAL_SCALE), strict=True)


def _schema_is_normalized(schema: pl.Schema, table_name: TableName) -> bool:
    for source_name, spec_name in SPEC_COLUMN_RENAMES.get(table_name, {}).items():
        if source_name in schema or spec_name not in schema:
            return False

    for name, dtype in schema.items():
        if not isinstance(dtype, pl.Decimal):
            continue

        precision = DECIMAL_PRECISION_OVERRIDES.get(name, DECIMAL_PRECISION)
        if (dtype.precision, dtype.scale) != (precision, DECIMAL_SCALE):
            return False

    return True


def _is_normalized(output_directory: Path) -> bool:
    return all(
        _schema_is_normalized(pl.scan_parquet(output_directory / f"{table_name}.parquet").collect_schema(), table_name)
        for table_name in TPCDS_TABLES
    )


def _normalize_tpc_ds_parquet(output_directory: Path) -> None:
    for table_name in TPCDS_TABLES:
        fpath = output_directory / f"{table_name}.parquet"
        lf = pl.scan_parquet(fpath)
        exprs = _normalization_exprs(lf.collect_schema(), table_name)

        tmp_path = fpath.with_suffix(".parquet.tmp")
        _require_free_disk_space(
            output_directory,
            TPCDS_PREPARE_FREE_SPACE_RESERVE_BYTES + fpath.stat().st_size,
            f"normalize {fpath.name}",
        )

        try:
            lf.select(exprs).sink_parquet(tmp_path)
            tmp_path.replace(fpath)
        finally:
            tmp_path.unlink(missing_ok=True)

    _LOGGER.info(f"Normalized tpc_ds Parquet files in {output_directory}")


def _prepare_tpc_ds_data(scale_factor: int) -> None:
    scale_factor = resolve_suite_scale_factor("tpc_ds", scale_factor)
    data_directory_name = format_suite_data_directory_name("tpc_ds", scale_factor)
    output_directory = SETTINGS.input_data_directory / data_directory_name
    output_directory.mkdir(exist_ok=True, parents=True)

    expected_filenames = {f"{table}.parquet" for table in TPCDS_TABLES}
    parquet_filenames = {path.name for path in output_directory.glob("*.parquet")}
    existing = [table for table in TPCDS_TABLES if f"{table}.parquet" in parquet_filenames]

    if len(existing) == len(TPCDS_TABLES):
        if not _is_normalized(output_directory):
            raise ValueError(f"{output_directory} contains an unnormalized dataset; remove it and rerun prepare")
        _LOGGER.info(
            f"All {data_directory_name} Parquet files already exist in {output_directory}, skipping generation"
        )
        return

    if parquet_filenames:
        unexpected = sorted(parquet_filenames - expected_filenames)
        existing_details = [*existing, *unexpected]
        raise ValueError(
            f"{output_directory} contains a partial dataset ({', '.join(existing_details)}); remove it first"
        )

    if shutil.which("tpcgen-cli") is None:
        raise RuntimeError(
            "tpcgen-cli not found on PATH; install it with 'cargo install --git "
            "https://github.com/clflushopt/tpchgen-rs --rev 09d609d13b7b45a2aa06d2b86e5a4bfa29aacb1a tpcgen-cli'"
        )

    _require_free_disk_space(
        output_directory,
        TPCDS_PREPARE_FREE_SPACE_RESERVE_BYTES + (TPCDS_PREPARE_BYTES_PER_SCALE_FACTOR * scale_factor),
        f"generate {data_directory_name} data",
    )

    command = [
        "tpcgen-cli",
        "tpcds",
        "parquet",
        "--compat",
        "c",
        "--scale-factor",
        str(scale_factor),
        "--no-progress",
        "--output-dir",
        output_directory.as_posix(),
    ]

    _LOGGER.info(f"Generating TPC-DS data at scale factor {scale_factor}: {' '.join(command)}")
    t0 = perf_counter()
    subprocess.run(command, check=True)
    _LOGGER.info(f"Generated {data_directory_name} data in {perf_counter() - t0:_.2f} seconds")

    _normalize_tpc_ds_parquet(output_directory)


def prepare_data(scale_factor: int) -> None:
    _prepare_tpc_ds_data(scale_factor)


class TpcDs[DBT: Database](BenchmarkSuite[DBT]):
    # queries an engine cannot run, with the error class documented next to
    # each entry; skipped instead of failing the whole select run
    UNSUPPORTED_QUERIES: ClassVar[dict[DatabaseName, frozenset[str]]] = {}

    def expected_table_row_counts(self) -> dict[TableName, int]:
        return {
            table_name: self.parquet_row_count(self.input_data_directory / f"{table_name}.parquet")
            for table_name in TPCDS_TABLES
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

        self.db.initialize_schema("tpc_ds")

        for table_name in TPCDS_TABLES:
            df = pl.scan_parquet(self.input_data_directory / f"{table_name}.parquet")

            with self.db.phase_context("insert", table_name=table_name):
                self.insert_table(df, table_name)
                _LOGGER.info(f"Inserted {table_name} for {self.name}")

        _LOGGER.info(f"Inserted all tpc_ds tables for {self.name} scale factor {self.scale_factor}")

        with self.db.phase_context("verify_populate"):
            self.verify_populated_data()

        # restart db to ensure data is not kept in-memory by the db, and also
        # ensure that WAL is processed etc...
        if restart:
            self.db.restart_event()

    @property
    def fetch_kwargs(self) -> dict[str, Any]:
        return {}

    def load_tpcds_query(self, query_name: str) -> str:
        db_specific = TPC_DS_QUERIES_DIRECTORY / f"{self.db.name}/{query_name}.sql"
        common = TPC_DS_QUERIES_DIRECTORY / f"{query_name}.sql"

        sql_source = db_specific if db_specific.is_file() else common

        with sql_source.open() as f:
            return f.read()

    def include_query(self, query_name: str) -> bool:
        return query_name not in self.UNSUPPORTED_QUERIES.get(self.db.name, frozenset())

    def select(self) -> None:
        t0 = perf_counter()
        failed_queries = 0
        for idx, query_name in enumerate(TPCDS_QUERY_NAMES):
            progress_label = f"({idx + 1:_}/{len(TPCDS_QUERY_NAMES):_})"
            if not self.include_query(query_name):
                _LOGGER.info(f"Skipping unsupported query {query_name} on {self.db.name}")
                self.record_skipped_query_steps(
                    query_name,
                    TPCDS_ITERATIONS,
                    result_status="unsupported",
                    reason=f"query is unsupported by {self.db.name}",
                )
                continue

            ok = self.execute_query_with_isolation(
                query_name=query_name,
                iterations=TPCDS_ITERATIONS,
                query_loader=lambda query_name=query_name: self.load_tpcds_query(query_name),
                fetch_kwargs_factory=lambda: self.fetch_kwargs,
                progress_label=progress_label,
                log_success=lambda it, df, t, query_name=query_name, progress_label=progress_label: _LOGGER.info(
                    f"Executed {query_name} {progress_label} "
                    f"iteration {it:_}/{TPCDS_ITERATIONS:_} "
                    f"in {1_000 * (t):_.2f} ms\ndf={df}"
                ),
            )
            if not ok:
                failed_queries += 1

        if failed_queries:
            _LOGGER.warning(
                f"TPC-DS select completed on {self.db.name} with {failed_queries:_} failed "
                f"{'queries' if failed_queries != 1 else 'query'}"
            )

        _LOGGER.info(
            f"Executed {len(TPCDS_QUERY_NAMES):_} queries (with repetitions) in {perf_counter() - t0:_.2f} seconds"
        )
