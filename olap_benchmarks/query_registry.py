from __future__ import annotations

from .settings import REPO_ROOT, DatabaseName, SuiteName

SUITES_DIRECTORY = REPO_ROOT / "olap_benchmarks/suites"

_MONETDB_ROLLING_AVG_WINDOW_BUG = (
    "MonetDB 11.55.7 through 11.55.9 return wrong values from avg() over a ROWS frame wider than 16 rows, "
    "from the 17th row onward. A 60-row moving average cannot be expressed correctly, so the query "
    "is not measured rather than publishing a runtime for a wrong answer."
)

_POSTGRES_NO_DATA_LARGE = (
    "data_large is not populated on PostgreSQL: stored as EAV it needs about 300 GB of heap plus a 70 GB "
    "(metric_name, time) index, beyond the per-(db, suite) disk budget (PostgresTimeSeries.SKIP_TABLES)"
)

_EAV_BATCH_EXPORT = "query requires wide-row export shape, but this database stores data_large as EAV"

_RTABENCH_INCREMENTAL_VIEWS = (
    "the 1000-series queries read the pre-aggregated incremental views of upstream RTABench (ClickHouse "
    "materialized views, TimescaleDB continuous aggregates); this engine defines no equivalent"
)

_RTABENCH_INCREMENTAL_VIEW_QUERIES = (
    "1000_terminal_hourly_stats",
    "1004_count_delayed_orders_per_day",
    "1008_most_week_delayed_order",
    "1012_max_satisfaction_for_order_per_week",
    "1013_satisfaction_with_without_backup",
    "1017_top_selling_month_product",
    "1023_top_sales_volume_product_from_terminal",
    "1025_product_category_performance",
    "1027_country_category_performance",
    "1030_customers_with_most_orders_delivered",
)


def _time_series_queries(prefix: str) -> list[str]:
    return sorted(path.stem for path in (SUITES_DIRECTORY / "time_series/queries").glob(f"{prefix}*.sql"))


# Queries an engine does not run, mapped to why. The select loop records every planned query listed
# here as `unsupported` with this reason instead of executing it. A query that is not listed must
# resolve to SQL (an engine override or the shared file); a missing file is an error, not a
# declaration.
UNSUPPORTED_QUERIES: dict[tuple[SuiteName, DatabaseName, str], str] = {
    **{
        ("time_series_metrics", "monetdb", f"{layout}{metrics}_09_rolling_avg"): _MONETDB_ROLLING_AVG_WINDOW_BUG
        for layout in ("wide", "eav")
        for metrics in (100, 1000, 2000, 5000)
    },
    **{
        ("time_series", "monetdb", query): _MONETDB_ROLLING_AVG_WINDOW_BUG
        for query in ("large_13_rolling_avg", "tall_13_rolling_avg", "wide_13_rolling_avg")
    },
    **{("time_series", "postgres", query): _POSTGRES_NO_DATA_LARGE for query in _time_series_queries("large_")},
    ("time_series", "timescaledb", "large_23_batch_export"): _EAV_BATCH_EXPORT,
    **{
        ("rtabench", db, query): _RTABENCH_INCREMENTAL_VIEWS
        for db in ("doris", "duckdb", "monetdb", "postgres", "questdb", "starrocks")
        for query in _RTABENCH_INCREMENTAL_VIEW_QUERIES
    },
}

# Queries whose answers are legitimately not comparable, mapped to why: (suite, query, None) skips
# the cross-engine comparison of the query entirely, (suite, query, db) leaves only that engine's
# answers out. These still run and are still timed. Keep this short: an entry is a claim that no
# engine is at fault, which is a much stronger claim than a failing query.
ANSWER_COMPARISON_EXCLUSIONS: dict[tuple[SuiteName, str, DatabaseName | None], str] = {
    ("clickbench", "Q28", None): (
        "DuckDB and ClickHouse apply different regex semantics to the roughly 1031 Referer "
        "values containing a newline: RE2's default leaves '.' not matching a newline and '$' "
        "meaning end of text, which DuckDB follows and ClickHouse does not. Neither is wrong "
        "and no rewrite reconciles them without changing what the query asks."
    ),
}


def unsupported_reason(suite: SuiteName, db: DatabaseName, query_name: str) -> str | None:
    return UNSUPPORTED_QUERIES.get((suite, db, query_name))


def unsupported_query_count(suite: SuiteName, db: DatabaseName) -> int:
    return sum(1 for entry_suite, entry_db, _ in UNSUPPORTED_QUERIES if (entry_suite, entry_db) == (suite, db))
