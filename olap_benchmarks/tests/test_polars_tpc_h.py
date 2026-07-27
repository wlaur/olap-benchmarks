from __future__ import annotations

from datetime import date
from decimal import Decimal

import polars as pl

from olap_benchmarks.dbs.polars.tpc_h import execute_tpc_h_query
from olap_benchmarks.suites.tpc_h.config import TPCH_QUERY_NAMES


def _money(value: str) -> Decimal:
    return Decimal(value)


def test_all_polars_tpc_h_queries_execute() -> None:
    tables = {
        "region": pl.DataFrame({"r_regionkey": [1], "r_name": ["EUROPE"], "r_comment": [""]}).lazy(),
        "nation": pl.DataFrame(
            {"n_nationkey": [1], "n_name": ["GERMANY"], "n_regionkey": [1], "n_comment": [""]}
        ).lazy(),
        "supplier": pl.DataFrame(
            {
                "s_suppkey": [1],
                "s_name": ["Supplier#1"],
                "s_address": ["Address"],
                "s_nationkey": [1],
                "s_phone": ["13-000"],
                "s_acctbal": [_money("100.00")],
                "s_comment": ["Comment"],
            }
        ).lazy(),
        "customer": pl.DataFrame(
            {
                "c_custkey": [1],
                "c_name": ["Customer#1"],
                "c_address": ["Address"],
                "c_nationkey": [1],
                "c_phone": ["13-000"],
                "c_acctbal": [_money("100.00")],
                "c_mktsegment": ["BUILDING"],
                "c_comment": ["Comment"],
            }
        ).lazy(),
        "part": pl.DataFrame(
            {
                "p_partkey": [1],
                "p_name": ["forest green"],
                "p_mfgr": ["Manufacturer"],
                "p_brand": ["Brand#23"],
                "p_type": ["ECONOMY ANODIZED STEEL"],
                "p_size": [15],
                "p_container": ["MED BOX"],
                "p_retailprice": [_money("100.00")],
                "p_comment": ["Comment"],
            }
        ).lazy(),
        "partsupp": pl.DataFrame(
            {
                "ps_partkey": [1],
                "ps_suppkey": [1],
                "ps_availqty": [100],
                "ps_supplycost": [_money("10.00")],
                "ps_comment": ["Comment"],
            }
        ).lazy(),
        "orders": pl.DataFrame(
            {
                "o_orderkey": [1],
                "o_custkey": [1],
                "o_orderstatus": ["F"],
                "o_totalprice": [_money("100.00")],
                "o_orderdate": [date(1994, 1, 1)],
                "o_orderpriority": ["1-URGENT"],
                "o_clerk": ["Clerk"],
                "o_shippriority": [0],
                "o_comment": ["Comment"],
            }
        ).lazy(),
        "lineitem": pl.DataFrame(
            {
                "l_orderkey": [1],
                "l_partkey": [1],
                "l_suppkey": [1],
                "l_linenumber": [1],
                "l_quantity": [_money("10.00")],
                "l_extendedprice": [_money("100.00")],
                "l_discount": [_money("0.05")],
                "l_tax": [_money("0.08")],
                "l_returnflag": ["R"],
                "l_linestatus": ["F"],
                "l_shipdate": [date(1994, 2, 1)],
                "l_commitdate": [date(1994, 2, 2)],
                "l_receiptdate": [date(1994, 2, 3)],
                "l_shipinstruct": ["DELIVER IN PERSON"],
                "l_shipmode": ["AIR"],
                "l_comment": ["Comment"],
            }
        ).lazy(),
    }

    results = [execute_tpc_h_query(tables, query_name, 1).collect() for query_name in TPCH_QUERY_NAMES]

    assert len(results) == len(TPCH_QUERY_NAMES)
