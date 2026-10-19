import json
from collections.abc import Generator
from datetime import datetime
from decimal import Decimal

import duckdb
import polars as pl
import pytest

from ..dbs.clickhouse import Clickhouse
from ..results.hashing import build_answer_metadata
from ..settings import REPO_ROOT
from ..suites.rtabench.config import RTABENCH_QUERY_NAMES, RTABENCH_SCHEMAS

SUITE = REPO_ROOT / "olap_benchmarks/suites/rtabench"
REFERENCES = SUITE / "reference_queries"
PREAGGREGATED = tuple(name for name in RTABENCH_QUERY_NAMES if name.startswith("1"))


def fixture_frames() -> dict[str, pl.DataFrame]:
    customers = [
        (1, "Alice", datetime(1980, 1, 1), "a", "address", "Berlin", "1", "state", "Switzerland"),
        (2, "Bob", datetime(1980, 1, 1), "b", "address", "Hamburg", "2", "state", "Germany"),
    ]
    products = [
        (1, "small", "description", "category1", Decimal("0.29"), 100),
        (2, "large", "description", "category2", Decimal("9998.41"), 100),
    ]
    orders = [(111, 1, datetime(2024, 1, 1)), (112, 2, datetime(2024, 1, 2)), (700, 1, datetime(2024, 1, 3))]
    items = [(111, 1, 3), (111, 2, 7), (112, 2, 3), (700, 1, 5)]
    events: list[dict[str, object]] = []

    def add(
        order: int,
        instant: str,
        block: int,
        *,
        backup: str | None = None,
        satisfaction: float = 20,
        kind: str = "Delivered",
        terminal: str = "Berlin",
        priority: bool = True,
        identifier: str | int | None = "duplicate",
        missing: bool = False,
    ) -> None:
        payload: dict[str, object] = {
            "terminal": terminal,
            "status": ["Delayed", "Priority"] if priority else ["Delayed"],
        }
        if not missing:
            payload["order_id"] = identifier
        events.append(
            {
                "order_id": order,
                "counter": len(events),
                "event_created": datetime.fromisoformat(instant),
                "event_type": kind,
                "satisfaction": satisfaction,
                "processor": "processor",
                "backup_processor": backup,
                "event_payload": json.dumps(payload),
                "ingest_block": block,
            }
        )

    add(111, "2023-12-31T23:00:00", 0)
    add(111, "2024-01-01T00:05:00", 0, satisfaction=10)
    add(111, "2024-01-01T00:15:00", 1, backup="backup", satisfaction=90)
    add(111, "2024-01-01T00:25:00", 1, backup="backup", satisfaction=70, identifier="", kind="Created")
    add(111, "2024-01-01T00:35:00", 2, backup="backup", satisfaction=60, identifier=None, kind="Departed")
    add(111, "2024-01-01T00:45:00", 0, backup="", satisfaction=200, missing=True, kind="Created")
    add(111, "2024-01-01T00:55:00", 2, backup="backup", satisfaction=20, identifier=7)
    add(111, "2024-01-02T01:05:00", 1, satisfaction=30, identifier=None)
    add(112, "2024-01-07T23:59:00", 0)
    add(112, "2024-01-08T00:00:00", 1)
    add(700, "2024-01-14T12:00:00", 0, satisfaction=80, kind="Created")
    add(700, "2024-01-15T12:00:00", 1, satisfaction=70, kind="Created")
    for order in (111, 112):
        for block, day in enumerate(("2024-01-29", "2024-01-30", "2024-02-04")):
            add(order, f"{day}T12:00:00", block)
    for _ in range(5):
        add(700, "2024-01-31T12:00:00", 0, priority=False)
    add(111, "2024-05-01T12:00:00", 0)
    add(111, "2024-05-01T13:00:00", 1)
    add(112, "2024-05-02T12:00:00", 2)
    add(111, "2024-06-30T23:59:00", 0)
    add(111, "2024-07-01T00:00:00", 0, kind="Created")
    add(111, "2024-07-01T00:01:00", 1)
    add(112, "2024-07-07T12:00:00", 2, kind="Departed")
    add(112, "2024-07-08T12:00:00", 1, terminal="Hamburg")
    add(700, "2024-12-31T23:59:00", 2)
    add(111, "2025-01-01T00:00:00", 0)
    frames = {
        "customers": pl.DataFrame(customers, schema=RTABENCH_SCHEMAS["customers"], orient="row"),
        "products": pl.DataFrame(products, schema=RTABENCH_SCHEMAS["products"], orient="row"),
        "orders": pl.DataFrame(orders, schema=RTABENCH_SCHEMAS["orders"], orient="row"),
        "order_items": pl.DataFrame(items, schema=RTABENCH_SCHEMAS["order_items"], orient="row"),
        "order_events": pl.DataFrame(events, schema={**RTABENCH_SCHEMAS["order_events"], "ingest_block": pl.Int32}),
    }
    return frames


def prepare_logical_aggregate_tables(connection: duckdb.DuckDBPyConnection) -> None:
    delayed = "list_has_all((event_payload -> 'status')::VARCHAR[], ['Delayed', 'Priority'])"
    terminal = "json_extract_string(event_payload, '$.terminal')"
    semester = (
        "date_trunc('year', event_created) + CASE WHEN month(event_created)>=7 "
        "THEN INTERVAL 6 MONTH ELSE INTERVAL 0 MONTH END"
    )
    joins = "products p JOIN order_items oi USING(product_id) JOIN order_events oe USING(order_id)"
    selections = {
        "order_events_q4": (
            "date_trunc('day',event_created) AS day, count(*) delayed_orders",
            "order_events",
            delayed,
            "day",
        ),
        "order_events_q8": (
            "date_trunc('day',event_created) AS day, order_id, count(*) delayed_orders",
            "order_events",
            delayed,
            "day,order_id",
        ),
        "order_events_q12": (
            "date_trunc('week',event_created) AS week, order_id, max(satisfaction) satisfaction",
            "order_events",
            "true",
            "week,order_id",
        ),
        "order_events_q13": (
            (
                "date_trunc('month',event_created) AS month, order_id, "
                "count(*) FILTER(WHERE backup_processor<>'') count_with_backup, "
                "count(*) FILTER(WHERE backup_processor IS NULL) count_without_backup, "
                "coalesce(sum(satisfaction) FILTER(WHERE backup_processor<>''),0) satisfaction_sum_with_backup, "
                "coalesce(sum(satisfaction) FILTER(WHERE backup_processor IS NULL),0) satisfaction_sum_without_backup, "
                "count(satisfaction) FILTER(WHERE backup_processor<>'') satisfaction_count_with_backup, "
                "count(satisfaction) FILTER(WHERE backup_processor IS NULL) satisfaction_count_without_backup"
            ),
            "order_events",
            "true",
            "month,order_id",
        ),
        "order_events_per_terminal_per_hour": (
            (
                f"date_trunc('hour',event_created) AS hour, {terminal} terminal, count(*) event_count, "
                "coalesce(list(DISTINCT json_extract_string(event_payload,'$.order_id')) "
                "FILTER(WHERE json_extract_string(event_payload,'$.order_id') IS NOT NULL),[]::VARCHAR[]) unique_orders"
            ),
            "order_events",
            "event_type IN ('Created','Departed','Delivered')",
            "hour,terminal",
        ),
        "top_selling_month_product_q17": (
            "date_trunc('month',event_created) event_created, product_id, p.name product_name, sum(amount) amount",
            joins,
            "event_type='Delivered'",
            "date_trunc('month',event_created),product_id,p.name",
        ),
        "top_sales_volume_product": (
            f"{semester} event_created, product_id, p.name product_name, sum(amount*price) volume, {terminal} terminal",
            joins,
            "true",
            f"{semester},product_id,p.name,terminal",
        ),
        "top_sales_volume_category_weekly": (
            "date_trunc('week',event_created) event_created, category, sum(amount*price) volume",
            joins,
            "event_type='Delivered'",
            "date_trunc('week',event_created),category",
        ),
        "country_category_monthly_performance": (
            (
                "date_trunc('month',oe.event_created) event_created, country, "
                "category product_category, sum(price*amount) revenue"
            ),
            joins + " JOIN orders o USING(order_id) JOIN customers c USING(customer_id)",
            "event_type='Delivered'",
            "date_trunc('month',oe.event_created),country,category",
        ),
        "customers_with_most_orders_delivered": (
            (
                "date_trunc('month',oe.event_created) event_created, customer_id, "
                "c.name customer_name, count(o.order_id) customer_orders"
            ),
            "customers c JOIN orders o USING(customer_id) JOIN order_events oe USING(order_id)",
            "event_type='Delivered'",
            "date_trunc('month',oe.event_created),customer_id,c.name",
        ),
    }
    for table, (columns, source, condition, group) in selections.items():
        connection.execute(
            f"CREATE TABLE {table} AS SELECT {columns} FROM {source} WHERE {condition} GROUP BY {group},ingest_block"
        )
    connection.execute("CREATE MACRO uniqExactMerge(states) AS list_unique(flatten(list(states)))")
    full = {
        "cagg_order_events_0004": (
            "date_trunc('day',event_created) AS day,count(*) count",
            "order_events",
            delayed,
            "day",
        ),
        "cagg_order_events_0008": (
            "date_trunc('week',event_created) AS week,order_id,count(*) count",
            "order_events",
            delayed,
            "week,order_id",
        ),
        "cagg_order_events_q12": (
            "date_trunc('week',event_created) AS week,order_id,max(satisfaction) satisfaction",
            "order_events",
            "true",
            "week,order_id",
        ),
        "cagg_order_events_q13": (
            (
                "date_trunc('month',event_created) AS month,order_id,"
                "count(*) FILTER(WHERE backup_processor<>'') count_with_backup, "
                "count(*) FILTER(WHERE backup_processor IS NULL) count_without_backup, "
                "avg(satisfaction) FILTER(WHERE backup_processor<>'') avg_satisfaction_with_backup, "
                "avg(satisfaction) FILTER(WHERE backup_processor IS NULL) avg_satisfaction_without_backup"
            ),
            "order_events",
            "true",
            "month,order_id",
        ),
        "cagg_order_events_per_terminal_per_hour": (
            (
                f"date_trunc('hour',event_created) AS hour,{terminal} terminal,count(*) event_count, "
                "count(DISTINCT json_extract_string(event_payload,'$.order_id')) unique_orders"
            ),
            "order_events",
            "event_type IN ('Created','Departed','Delivered')",
            "hour,terminal",
        ),
        "cagg_top_selling_month_product_q17": selections["top_selling_month_product_q17"],
        "cagg_top_selling_product_semester": selections["top_sales_volume_product"],
        "cagg_top_sales_volume_category_weekly": selections["top_sales_volume_category_weekly"],
        "cagg_country_category_monthly_performance": selections["country_category_monthly_performance"],
        "cagg_customers_with_most_orders_delivered": selections["customers_with_most_orders_delivered"],
    }
    for table, (columns, source, condition, group) in full.items():
        connection.execute(f"CREATE TABLE {table} AS SELECT {columns} FROM {source} WHERE {condition} GROUP BY {group}")


@pytest.fixture(scope="module")
def reference_database() -> Generator[duckdb.DuckDBPyConnection]:
    with duckdb.connect() as connection:
        for table, frame in fixture_frames().items():
            connection.register("input_frame", frame)
            connection.execute(f"CREATE TABLE {table} AS SELECT * FROM input_frame")
            connection.unregister("input_frame")
        prepare_logical_aggregate_tables(connection)
        yield connection


@pytest.mark.parametrize("engine", ["clickhouse", "timescaledb"])
@pytest.mark.parametrize("query_name", PREAGGREGATED)
def test_incremental_queries_match_raw_references_across_partial_insert_blocks(
    reference_database: duckdb.DuckDBPyConnection, engine: str, query_name: str
) -> None:
    expected = reference_database.execute((REFERENCES / f"{query_name}.sql").read_text()).pl()
    actual = reference_database.execute((SUITE / "queries" / engine / f"{query_name}.sql").read_text()).pl()
    assert expected.height > 0
    assert build_answer_metadata(actual)["answer_hash"] == build_answer_metadata(expected)["answer_hash"]


def test_all_incremental_variants_have_independent_raw_reference_queries() -> None:
    assert {path.stem for path in REFERENCES.glob("*.sql")} == set(PREAGGREGATED)
    assert "week" in Clickhouse().benchmarks["rtabench"].fetch_kwargs["time_columns"]
