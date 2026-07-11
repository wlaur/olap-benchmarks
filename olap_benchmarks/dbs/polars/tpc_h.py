from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from decimal import Decimal

import polars as pl

TpcHTables = Mapping[str, pl.LazyFrame]

DECIMAL_4 = pl.Decimal(38, 4)
DECIMAL_6 = pl.Decimal(38, 6)


def _volume() -> pl.Expr:
    return pl.col("l_extendedprice").cast(DECIMAL_4) * (1 - pl.col("l_discount").cast(DECIMAL_4))


def _average(column: str) -> pl.Expr:
    return pl.col(column).sum().cast(pl.Float64) / pl.len()


def _date_range(column: str, start: date, end: date) -> pl.Expr:
    return (pl.col(column) >= start) & (pl.col(column) < end)


def execute_tpc_h_query(tables: TpcHTables, query_name: str, scale_factor: int) -> pl.LazyFrame:
    customer = tables["customer"]
    lineitem = tables["lineitem"]
    nation = tables["nation"]
    orders = tables["orders"]
    part = tables["part"]
    partsupp = tables["partsupp"]
    region = tables["region"]
    supplier = tables["supplier"]

    match query_name:
        case "01_pricing_summary":
            return (
                lineitem.filter(pl.col("l_shipdate") <= date(1998, 9, 2))
                .group_by("l_returnflag", "l_linestatus")
                .agg(
                    pl.col("l_quantity").sum().alias("sum_qty"),
                    pl.col("l_extendedprice").sum().alias("sum_base_price"),
                    _volume().sum().alias("sum_disc_price"),
                    (_volume().cast(DECIMAL_6) * (1 + pl.col("l_tax").cast(DECIMAL_6))).sum().alias("sum_charge"),
                    _average("l_quantity").alias("avg_qty"),
                    _average("l_extendedprice").alias("avg_price"),
                    _average("l_discount").alias("avg_disc"),
                    pl.len().cast(pl.Int64).alias("count_order"),
                )
                .sort("l_returnflag", "l_linestatus")
            )
        case "02_minimum_cost_supplier":
            europe_nations = nation.join(
                region.filter(pl.col("r_name") == "EUROPE"),
                left_on="n_regionkey",
                right_on="r_regionkey",
            )
            candidates = (
                partsupp.join(supplier, left_on="ps_suppkey", right_on="s_suppkey")
                .join(europe_nations, left_on="s_nationkey", right_on="n_nationkey")
                .join(
                    part.filter((pl.col("p_size") == 15) & pl.col("p_type").str.ends_with("BRASS")),
                    left_on="ps_partkey",
                    right_on="p_partkey",
                )
            )
            return (
                candidates.with_columns(pl.col("ps_supplycost").min().over("ps_partkey").alias("min_supplycost"))
                .filter(pl.col("ps_supplycost") == pl.col("min_supplycost"))
                .select(
                    "s_acctbal",
                    "s_name",
                    "n_name",
                    pl.col("ps_partkey").alias("p_partkey"),
                    "p_mfgr",
                    "s_address",
                    "s_phone",
                    "s_comment",
                )
                .sort(
                    ["s_acctbal", "n_name", "s_name", "p_partkey"],
                    descending=[True, False, False, False],
                )
                .limit(100)
            )
        case "03_shipping_priority":
            return (
                customer.filter(pl.col("c_mktsegment") == "BUILDING")
                .join(
                    orders.filter(pl.col("o_orderdate") < date(1995, 3, 15)),
                    left_on="c_custkey",
                    right_on="o_custkey",
                )
                .join(
                    lineitem.filter(pl.col("l_shipdate") > date(1995, 3, 15)),
                    left_on="o_orderkey",
                    right_on="l_orderkey",
                )
                .group_by("o_orderkey", "o_orderdate", "o_shippriority")
                .agg(_volume().sum().alias("revenue"))
                .select(pl.col("o_orderkey").alias("l_orderkey"), "revenue", "o_orderdate", "o_shippriority")
                .sort(["revenue", "o_orderdate"], descending=[True, False])
                .limit(10)
            )
        case "04_order_priority":
            qualifying_orders = lineitem.filter(pl.col("l_commitdate") < pl.col("l_receiptdate")).select(
                pl.col("l_orderkey").unique()
            )
            return (
                orders.filter(_date_range("o_orderdate", date(1993, 7, 1), date(1993, 10, 1)))
                .join(qualifying_orders, left_on="o_orderkey", right_on="l_orderkey", how="semi")
                .group_by("o_orderpriority")
                .agg(pl.len().cast(pl.Int64).alias("order_count"))
                .sort("o_orderpriority")
            )
        case "05_local_supplier_volume":
            asia_nations = nation.join(
                region.filter(pl.col("r_name") == "ASIA"),
                left_on="n_regionkey",
                right_on="r_regionkey",
            )
            return (
                customer.join(
                    orders.filter(_date_range("o_orderdate", date(1994, 1, 1), date(1995, 1, 1))),
                    left_on="c_custkey",
                    right_on="o_custkey",
                )
                .join(lineitem, left_on="o_orderkey", right_on="l_orderkey")
                .join(supplier, left_on="l_suppkey", right_on="s_suppkey")
                .filter(pl.col("c_nationkey") == pl.col("s_nationkey"))
                .join(asia_nations, left_on="s_nationkey", right_on="n_nationkey")
                .group_by("n_name")
                .agg(_volume().sum().alias("revenue"))
                .sort("revenue", descending=True)
            )
        case "06_revenue_forecast":
            return lineitem.filter(
                _date_range("l_shipdate", date(1994, 1, 1), date(1995, 1, 1))
                & pl.col("l_discount").is_between(Decimal("0.05"), Decimal("0.07"), closed="both")
                & (pl.col("l_quantity") < 24)
            ).select(
                (pl.col("l_extendedprice").cast(DECIMAL_4) * pl.col("l_discount").cast(DECIMAL_4))
                .sum()
                .alias("revenue")
            )
        case "07_volume_shipping":
            supplier_nations = supplier.join(nation, left_on="s_nationkey", right_on="n_nationkey").select(
                pl.col("s_suppkey").alias("l_suppkey"), pl.col("n_name").alias("supp_nation")
            )
            customer_orders = (
                customer.join(nation, left_on="c_nationkey", right_on="n_nationkey")
                .join(orders, left_on="c_custkey", right_on="o_custkey")
                .select(pl.col("o_orderkey").alias("l_orderkey"), pl.col("n_name").alias("cust_nation"))
            )
            shipping = (
                lineitem.filter(pl.col("l_shipdate").is_between(date(1995, 1, 1), date(1996, 12, 31)))
                .join(supplier_nations, on="l_suppkey")
                .join(customer_orders, on="l_orderkey")
                .filter(
                    ((pl.col("supp_nation") == "FRANCE") & (pl.col("cust_nation") == "GERMANY"))
                    | ((pl.col("supp_nation") == "GERMANY") & (pl.col("cust_nation") == "FRANCE"))
                )
                .select("supp_nation", "cust_nation", pl.col("l_shipdate").dt.year().alias("l_year"), _volume())
            )
            return (
                shipping.group_by("supp_nation", "cust_nation", "l_year")
                .agg(pl.col("l_extendedprice").sum().alias("revenue"))
                .sort("supp_nation", "cust_nation", "l_year")
            )
        case "08_market_share":
            american_customers = (
                nation.join(
                    region.filter(pl.col("r_name") == "AMERICA"),
                    left_on="n_regionkey",
                    right_on="r_regionkey",
                )
                .join(customer, left_on="n_nationkey", right_on="c_nationkey")
                .join(
                    orders.filter(pl.col("o_orderdate").is_between(date(1995, 1, 1), date(1996, 12, 31))),
                    left_on="c_custkey",
                    right_on="o_custkey",
                )
                .select(pl.col("o_orderkey").alias("l_orderkey"), "o_orderdate")
            )
            supplier_nations = supplier.join(nation, left_on="s_nationkey", right_on="n_nationkey").select(
                pl.col("s_suppkey").alias("l_suppkey"), pl.col("n_name").alias("nation")
            )
            all_nations = (
                lineitem.join(american_customers, on="l_orderkey")
                .join(supplier_nations, on="l_suppkey")
                .join(
                    part.filter(pl.col("p_type") == "ECONOMY ANODIZED STEEL"),
                    left_on="l_partkey",
                    right_on="p_partkey",
                )
                .select(pl.col("o_orderdate").dt.year().alias("o_year"), _volume(), "nation")
            )
            return (
                all_nations.group_by("o_year")
                .agg(
                    (
                        pl.col("l_extendedprice").filter(pl.col("nation") == "BRAZIL").sum().cast(pl.Float64)
                        / pl.col("l_extendedprice").sum().cast(pl.Float64)
                    ).alias("mkt_share")
                )
                .sort("o_year")
            )
        case "09_product_profit":
            profit = (
                lineitem.join(
                    part.filter(pl.col("p_name").str.contains("green", literal=True)),
                    left_on="l_partkey",
                    right_on="p_partkey",
                )
                .join(
                    partsupp,
                    left_on=["l_partkey", "l_suppkey"],
                    right_on=["ps_partkey", "ps_suppkey"],
                )
                .join(supplier, left_on="l_suppkey", right_on="s_suppkey")
                .join(nation, left_on="s_nationkey", right_on="n_nationkey")
                .join(orders, left_on="l_orderkey", right_on="o_orderkey")
                .select(
                    pl.col("n_name").alias("nation"),
                    pl.col("o_orderdate").dt.year().alias("o_year"),
                    (_volume() - pl.col("ps_supplycost").cast(DECIMAL_4) * pl.col("l_quantity").cast(DECIMAL_4)).alias(
                        "amount"
                    ),
                )
            )
            return (
                profit.group_by("nation", "o_year")
                .agg(pl.col("amount").sum().alias("sum_profit"))
                .sort(["nation", "o_year"], descending=[False, True])
            )
        case "10_returned_items":
            return (
                customer.join(nation, left_on="c_nationkey", right_on="n_nationkey")
                .join(
                    orders.filter(_date_range("o_orderdate", date(1993, 10, 1), date(1994, 1, 1))),
                    left_on="c_custkey",
                    right_on="o_custkey",
                )
                .join(
                    lineitem.filter(pl.col("l_returnflag") == "R"),
                    left_on="o_orderkey",
                    right_on="l_orderkey",
                )
                .group_by("c_custkey", "c_name", "c_acctbal", "n_name", "c_address", "c_phone", "c_comment")
                .agg(_volume().sum().alias("revenue"))
                .select("c_custkey", "c_name", "revenue", "c_acctbal", "n_name", "c_address", "c_phone", "c_comment")
                .sort("revenue", descending=True)
                .limit(20)
            )
        case "11_important_stock":
            fraction = Decimal("0.0001") / scale_factor
            german_stock = (
                supplier.join(
                    nation.filter(pl.col("n_name") == "GERMANY"),
                    left_on="s_nationkey",
                    right_on="n_nationkey",
                )
                .join(partsupp, left_on="s_suppkey", right_on="ps_suppkey")
                .select("ps_partkey", (pl.col("ps_supplycost") * pl.col("ps_availqty")).alias("stock_value"))
            )
            threshold = german_stock.select((pl.col("stock_value").sum() * fraction).alias("threshold"))
            return (
                german_stock.group_by("ps_partkey")
                .agg(pl.col("stock_value").sum().alias("value"))
                .join(threshold, how="cross")
                .filter(pl.col("value") > pl.col("threshold"))
                .select("ps_partkey", "value")
                .sort("value", descending=True)
            )
        case "12_shipping_modes":
            return (
                lineitem.filter(
                    pl.col("l_shipmode").is_in(["MAIL", "SHIP"])
                    & (pl.col("l_commitdate") < pl.col("l_receiptdate"))
                    & (pl.col("l_shipdate") < pl.col("l_commitdate"))
                    & _date_range("l_receiptdate", date(1994, 1, 1), date(1995, 1, 1))
                )
                .join(orders, left_on="l_orderkey", right_on="o_orderkey")
                .group_by("l_shipmode")
                .agg(
                    pl.col("o_orderpriority")
                    .is_in(["1-URGENT", "2-HIGH"])
                    .cast(pl.Int64)
                    .sum()
                    .cast(pl.Float64)
                    .alias("high_line_count"),
                    (~pl.col("o_orderpriority").is_in(["1-URGENT", "2-HIGH"]))
                    .cast(pl.Int64)
                    .sum()
                    .cast(pl.Float64)
                    .alias("low_line_count"),
                )
                .sort("l_shipmode")
            )
        case "13_customer_distribution":
            order_counts = (
                customer.join(
                    orders.filter(~pl.col("o_comment").str.contains("special.*requests")),
                    left_on="c_custkey",
                    right_on="o_custkey",
                    how="left",
                )
                .group_by("c_custkey")
                .agg(pl.col("o_orderkey").count().cast(pl.Int64).alias("c_count"))
            )
            return (
                order_counts.group_by("c_count")
                .agg(pl.len().cast(pl.Int64).alias("custdist"))
                .sort(["custdist", "c_count"], descending=True)
            )
        case "14_promotion_effect":
            revenue = (
                lineitem.filter(_date_range("l_shipdate", date(1995, 9, 1), date(1995, 10, 1)))
                .join(part, left_on="l_partkey", right_on="p_partkey")
                .select(_volume(), "p_type")
            )
            return revenue.select(
                (
                    (100 * pl.col("l_extendedprice").filter(pl.col("p_type").str.starts_with("PROMO")).sum()).cast(
                        pl.Float64
                    )
                    / pl.col("l_extendedprice").sum().cast(pl.Float64)
                ).alias("promo_revenue")
            )
        case "15_top_supplier":
            revenue = (
                lineitem.filter(_date_range("l_shipdate", date(1996, 1, 1), date(1996, 4, 1)))
                .group_by("l_suppkey")
                .agg(_volume().sum().alias("total_revenue"))
                .filter(pl.col("total_revenue") == pl.col("total_revenue").max())
            )
            return (
                supplier.join(revenue, left_on="s_suppkey", right_on="l_suppkey")
                .select("s_suppkey", "s_name", "s_address", "s_phone", "total_revenue")
                .sort("s_suppkey")
            )
        case "16_parts_supplier":
            complaint_suppliers = supplier.filter(pl.col("s_comment").str.contains("Customer.*Complaints")).select(
                pl.col("s_suppkey")
            )
            return (
                partsupp.join(complaint_suppliers, left_on="ps_suppkey", right_on="s_suppkey", how="anti")
                .join(
                    part.filter(
                        (pl.col("p_brand") != "Brand#45")
                        & ~pl.col("p_type").str.starts_with("MEDIUM POLISHED")
                        & pl.col("p_size").is_in([49, 14, 23, 45, 19, 3, 36, 9])
                    ),
                    left_on="ps_partkey",
                    right_on="p_partkey",
                )
                .group_by("p_brand", "p_type", "p_size")
                .agg(pl.col("ps_suppkey").n_unique().cast(pl.Int64).alias("supplier_cnt"))
                .sort(
                    ["supplier_cnt", "p_brand", "p_type", "p_size"],
                    descending=[True, False, False, False],
                )
            )
        case "17_small_quantity_revenue":
            average_quantities = lineitem.group_by("l_partkey").agg(pl.col("l_quantity").mean().alias("avg_quantity"))
            return (
                lineitem.join(average_quantities, on="l_partkey")
                .join(
                    part.filter((pl.col("p_brand") == "Brand#23") & (pl.col("p_container") == "MED BOX")),
                    left_on="l_partkey",
                    right_on="p_partkey",
                )
                .filter(pl.col("l_quantity") < Decimal("0.2") * pl.col("avg_quantity"))
                .select((pl.col("l_extendedprice").sum().cast(pl.Float64) / 7.0).alias("avg_yearly"))
            )
        case "18_large_volume_customer":
            large_orders = (
                lineitem.group_by("l_orderkey")
                .agg(pl.col("l_quantity").sum().alias("order_quantity"))
                .filter(pl.col("order_quantity") > 300)
            )
            return (
                orders.join(large_orders, left_on="o_orderkey", right_on="l_orderkey", how="semi")
                .join(customer, left_on="o_custkey", right_on="c_custkey")
                .join(lineitem, left_on="o_orderkey", right_on="l_orderkey")
                .group_by("c_name", "o_custkey", "o_orderkey", "o_orderdate", "o_totalprice")
                .agg(pl.col("l_quantity").sum().alias("sum(l_quantity)"))
                .select(
                    "c_name",
                    pl.col("o_custkey").alias("c_custkey"),
                    "o_orderkey",
                    "o_orderdate",
                    "o_totalprice",
                    "sum(l_quantity)",
                )
                .sort(["o_totalprice", "o_orderdate"], descending=[True, False])
                .limit(100)
            )
        case "19_discounted_revenue":
            joined = lineitem.join(part, left_on="l_partkey", right_on="p_partkey")
            shipping_filter = pl.col("l_shipmode").is_in(["AIR", "AIR REG"]) & (
                pl.col("l_shipinstruct") == "DELIVER IN PERSON"
            )
            product_filter = (
                (
                    (pl.col("p_brand") == "Brand#12")
                    & pl.col("p_container").is_in(["SM CASE", "SM BOX", "SM PACK", "SM PKG"])
                    & pl.col("l_quantity").is_between(1, 11)
                    & pl.col("p_size").is_between(1, 5)
                )
                | (
                    (pl.col("p_brand") == "Brand#23")
                    & pl.col("p_container").is_in(["MED BAG", "MED BOX", "MED PKG", "MED PACK"])
                    & pl.col("l_quantity").is_between(10, 20)
                    & pl.col("p_size").is_between(1, 10)
                )
                | (
                    (pl.col("p_brand") == "Brand#34")
                    & pl.col("p_container").is_in(["LG CASE", "LG BOX", "LG PACK", "LG PKG"])
                    & pl.col("l_quantity").is_between(20, 30)
                    & pl.col("p_size").is_between(1, 15)
                )
            )
            return joined.filter(shipping_filter & product_filter).select(_volume().sum().alias("revenue"))
        case "20_potential_promotion":
            shipped = (
                lineitem.filter(_date_range("l_shipdate", date(1994, 1, 1), date(1995, 1, 1)))
                .group_by("l_partkey", "l_suppkey")
                .agg(pl.col("l_quantity").sum().alias("shipped_quantity"))
            )
            canada = supplier.join(
                nation.filter(pl.col("n_name") == "CANADA"),
                left_on="s_nationkey",
                right_on="n_nationkey",
            )
            return (
                partsupp.join(
                    part.filter(pl.col("p_name").str.starts_with("forest")),
                    left_on="ps_partkey",
                    right_on="p_partkey",
                )
                .join(
                    shipped,
                    left_on=["ps_partkey", "ps_suppkey"],
                    right_on=["l_partkey", "l_suppkey"],
                )
                .filter(pl.col("ps_availqty") > Decimal("0.5") * pl.col("shipped_quantity"))
                .join(canada, left_on="ps_suppkey", right_on="s_suppkey")
                .select("s_name", "s_address")
                .unique()
                .sort("s_name")
            )
        case "21_suppliers_waiting":
            all_supplier_counts = lineitem.group_by("l_orderkey").agg(
                pl.col("l_suppkey").n_unique().alias("supplier_count")
            )
            late_lines = lineitem.filter(pl.col("l_receiptdate") > pl.col("l_commitdate"))
            late_supplier_counts = late_lines.group_by("l_orderkey").agg(
                pl.col("l_suppkey").n_unique().alias("late_supplier_count")
            )
            return (
                late_lines.join(all_supplier_counts, on="l_orderkey")
                .join(late_supplier_counts, on="l_orderkey")
                .filter((pl.col("supplier_count") > 1) & (pl.col("late_supplier_count") == 1))
                .join(
                    orders.filter(pl.col("o_orderstatus") == "F"),
                    left_on="l_orderkey",
                    right_on="o_orderkey",
                )
                .join(supplier, left_on="l_suppkey", right_on="s_suppkey")
                .join(
                    nation.filter(pl.col("n_name") == "SAUDI ARABIA"),
                    left_on="s_nationkey",
                    right_on="n_nationkey",
                )
                .group_by("s_name")
                .agg(pl.len().cast(pl.Int64).alias("numwait"))
                .sort(["numwait", "s_name"], descending=[True, False])
                .limit(100)
            )
        case "22_global_sales_opportunity":
            country_codes = ["13", "31", "23", "29", "30", "18", "17"]
            customers = customer.with_columns(pl.col("c_phone").str.slice(0, 2).alias("cntrycode")).filter(
                pl.col("cntrycode").is_in(country_codes)
            )
            average_balance = customers.filter(pl.col("c_acctbal") > 0).select(
                pl.col("c_acctbal").mean().alias("average_balance")
            )
            return (
                customers.join(
                    orders.select("o_custkey").unique(), left_on="c_custkey", right_on="o_custkey", how="anti"
                )
                .join(average_balance, how="cross")
                .filter(pl.col("c_acctbal") > pl.col("average_balance"))
                .group_by("cntrycode")
                .agg(pl.len().cast(pl.Int64).alias("numcust"), pl.col("c_acctbal").sum().alias("totacctbal"))
                .sort("cntrycode")
            )
        case _:
            raise ValueError(f"Unsupported Polars TPC-H query: {query_name}")
