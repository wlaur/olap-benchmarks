from __future__ import annotations

from collections.abc import Iterator
from typing import Any, cast

import polars as pl
import pytest

from .. import analytical_streams
from ..dbs import Database, get_databases
from ..matrix import MatrixCell
from ..metrics.storage import Storage
from ..query_registry import (
    ANSWER_COMPARISON_EXCLUSIONS,
    ANSWER_ORDER_KEYS,
    SUITES_DIRECTORY,
    UNSUPPORTED_QUERIES,
    unsupported_query_count,
)
from ..results.cells import CellStatus, format_matrix_status
from ..results.hashing import build_answer_metadata
from ..settings import SUITE_NAMES, SUITE_SPECS, DatabaseName, SuiteName
from ..suites import BenchmarkSuite
from ..suites.tpc_ds.config import TpcDs
from .test_benchmark_profile import ProfileDatabase, RecordingStorage


def _suites() -> Iterator[tuple[DatabaseName, SuiteName, BenchmarkSuite[Any]]]:
    for db_name, database in get_databases().items():
        for suite_name, suite_class in database.suite_registry().items():
            yield (
                db_name,
                suite_name,
                suite_class(db=database, name=suite_name, scale_factor=SUITE_SPECS[suite_name].default_scale_factor),
            )


SUITES = list(_suites())
PLANNED = {(suite_name, db_name): set(suite.planned_query_names()) for db_name, suite_name, suite in SUITES}


@pytest.mark.parametrize(("db_name", "suite_name", "suite"), SUITES, ids=[f"{d}-{s}" for d, s, _ in SUITES])
def test_every_planned_query_resolves_or_is_declared_unsupported(
    db_name: DatabaseName, suite_name: SuiteName, suite: BenchmarkSuite[Any]
) -> None:
    for query_name in suite.planned_query_names():
        if (suite_name, db_name, query_name) in UNSUPPORTED_QUERIES:
            assert suite.query_skip(query_name) == ("unsupported", UNSUPPORTED_QUERIES[suite_name, db_name, query_name])
            continue
        assert suite.query_skip(query_name) is None
        assert suite.load_query(query_name).strip(), f"{db_name}:{suite_name}:{query_name} resolved to empty SQL"


def test_unsupported_queries_name_planned_queries_of_registered_suites() -> None:
    # a renamed query would silently become "supported" again
    for suite_name, db_name, query_name in UNSUPPORTED_QUERIES:
        assert (suite_name, db_name) in PLANNED, f"{db_name} does not register {suite_name}"
        assert query_name in PLANNED[suite_name, db_name], f"{suite_name}:{query_name} is not planned"


def test_every_override_belongs_to_a_planned_query_of_a_registered_suite() -> None:
    for suite_name in SUITE_NAMES:
        for override in sorted((SUITES_DIRECTORY / suite_name / "queries").glob("*/*.sql")):
            key = (suite_name, override.parent.name)
            assert key in PLANNED, f"{override}: {override.parent.name} does not register {suite_name}"
            assert override.stem in PLANNED[key], f"{override} is not a planned query"


def test_no_override_is_identical_to_its_shared_file() -> None:
    for suite_name in SUITE_NAMES:
        queries = SUITES_DIRECTORY / suite_name / "queries"
        for override in sorted(queries.glob("*/*.sql")):
            shared = queries / override.name
            if shared.is_file():
                assert override.read_bytes() != shared.read_bytes(), f"{override} duplicates {shared}"


def test_postgres_only_excludes_the_eav_batch_export_shape() -> None:
    declared = {query for suite, db, query in UNSUPPORTED_QUERIES if (suite, db) == ("time_series", "postgres")}

    assert declared == {"large_23_batch_export"}


def test_every_declaration_states_a_reason() -> None:
    for key, reason in (*UNSUPPORTED_QUERIES.items(), *ANSWER_COMPARISON_EXCLUSIONS.items()):
        assert len(reason) > 40, f"{key} needs a reason"


def test_answer_comparison_exclusions_name_planned_queries() -> None:
    for suite_name, query_name, db_name in ANSWER_COMPARISON_EXCLUSIONS:
        planned = {query for (suite, _), queries in PLANNED.items() if suite == suite_name for query in queries}
        assert query_name in planned
        if db_name is not None:
            assert (suite_name, db_name) in PLANNED


def test_clickbench_q28_is_the_only_answer_comparison_exclusion() -> None:
    # the list should stay short; a new entry deserves the same scrutiny Q28 got
    assert list(ANSWER_COMPARISON_EXCLUSIONS) == [("clickbench", "Q28", None)]


def test_answer_order_keys_are_scoped_to_four_tpcds_queries() -> None:
    assert ANSWER_ORDER_KEYS == {
        ("tpc_ds", "18"): (1, 2, 3, 0),
        ("tpc_ds", "44"): (0,),
        ("tpc_ds", "65"): (0, 1),
        ("tpc_ds", "71"): (4, 0, 2),
    }


@pytest.mark.parametrize(
    ("db_name", "suite"),
    [(db_name, suite) for db_name, suite_name, suite in SUITES if suite_name == "tpc_ds"],
)
@pytest.mark.parametrize(
    ("query_name", "projection", "order"),
    [
        (
            "18",
            "select i_item_id, ca_country, ca_state, ca_county,",
            "ca_country nulls first, ca_state nulls first, ca_county nulls first, i_item_id nulls first limit 100;",
        ),
        (
            "65",
            "select s_store_name, i_item_desc, sc.revenue, i_current_price, i_wholesale_cost, i_brand from ",
            "s_store_name nulls first, i_item_desc nulls first limit 100;",
        ),
        (
            "71",
            "select i_brand_id brand_id, i_brand brand, t_hour, t_minute, sum(ext_price) ext_price from ",
            "ext_price desc nulls first, i_brand_id nulls first, t_hour nulls first;",
        ),
    ],
)
def test_answer_order_keys_match_each_registered_engine_sql(
    db_name: DatabaseName, suite: BenchmarkSuite[Any], query_name: str, projection: str, order: str
) -> None:
    assert query_name in suite.planned_query_names()
    assert ("tpc_ds", db_name, query_name) not in UNSUPPORTED_QUERIES
    sql = " ".join(suite.load_query(query_name).lower().split())
    body, separator, actual_order = sql.rpartition(" order by ")
    assert separator, f"{db_name}:{query_name} needs an explicit ORDER BY contract"
    if query_name == "65" and db_name in ("postgres", "timescaledb"):
        order = 's_store_name collate "c" nulls first, i_item_desc collate "c" nulls first limit 100;'
    assert actual_order == order, f"{db_name}:{query_name} has a different ORDER BY contract"
    if (db_name, query_name) == ("doris", "18"):
        assert f"rollup_output as ({projection}" in body
        assert body.endswith(") select * from rollup_output")
    else:
        assert body.startswith(projection), f"{db_name}:{query_name} has different output positions"


@pytest.mark.parametrize(("db_name", "database"), list(get_databases().items()))
def test_tpcds_q44_sql_contract_for_each_registered_engine(db_name: DatabaseName, database: Database) -> None:
    suite_class = database.suite_registry().get("tpc_ds", TpcDs)
    suite = suite_class(db=database, name="tpc_ds", scale_factor=10)
    sql = " ".join(suite.load_query("44").lower().split())
    body, separator, actual_order = sql.rpartition(" order by ")

    assert body.startswith(
        "select asceding.rnk, i1.i_product_name best_performing, i2.i_product_name worst_performing from "
    ), f"{db_name}:44 has different output positions"
    assert separator, f"{db_name}:44 needs an explicit ORDER BY contract"
    assert actual_order == "asceding.rnk limit 100;", f"{db_name}:44 has a different ORDER BY contract"


@pytest.mark.parametrize("analytical", [False, True], ids=["select", "analytical"])
@pytest.mark.parametrize(
    ("suite_name", "query_name", "keys"),
    [
        ("tpc_ds", "18", (1, 2, 3, 0)),
        ("tpc_ds", "44", (0,)),
        ("tpc_ds", "65", (0, 1)),
        ("tpc_ds", "71", (4, 0, 2)),
        ("tpc_ds", "01", None),
        ("tpc_ds", "36", None),
        ("tpc_ds", "70", None),
        ("tpc_ds", "86", None),
        ("clickbench", "18", None),
        ("clickbench", "44", None),
        ("clickbench", "65", None),
        ("clickbench", "71", None),
        (None, "18", None),
        (None, "44", None),
    ],
)
def test_answer_order_policy_applies_in_both_execution_paths(
    suite_name: SuiteName | None,
    query_name: str,
    keys: tuple[int, ...] | None,
    analytical: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    frame = pl.DataFrame({**{f"key{index}": [1, 1] for index in range(5)}, "value": ["b", "a"]})
    sql = "select 'unchanged'"
    storage = RecordingStorage()
    database = ProfileDatabase()
    database._current_suite = suite_name
    database._current_operation = "analytical" if analytical else "select"
    database._run_id = 1
    database._result_storage = cast(Storage, storage)

    def fetch(_database: ProfileDatabase, query: str) -> pl.DataFrame:
        assert query == sql
        return frame

    monkeypatch.setattr(ProfileDatabase, "fetch", fetch)
    if analytical:
        sample = analytical_streams.execute_analytical_request(
            database, analytical_streams.AnalyticalQuery(query_name, sql, {}), 0, 1, "measured", None
        )
        assert sample is not None and sample.status == "completed"
    else:
        result, _ = database.execute_query_iteration(query_name, 1, sql)
        assert result is frame

    metadata = cast(dict[str, Any], storage.steps[1]["metadata"])
    expected_version = "canonical-v11" if keys is None else "canonical-v11-ordered-ties-v1"
    assert metadata["answer_hash_version"] == expected_version
    assert metadata["answer_hash"] == build_answer_metadata(frame, keys)["answer_hash"]
    assert ("answer_order_policy" in metadata) == (keys is not None)
    if keys is not None:
        assert metadata["answer_order_key_indices"] == list(keys)
        assert metadata["answer_hash"] == build_answer_metadata(frame.reverse(), keys)["answer_hash"]
    else:
        assert metadata["answer_hash"] != build_answer_metadata(frame.reverse())["answer_hash"]


def test_matrix_status_shows_declared_unsupported_queries_on_select_cells() -> None:
    statuses = [
        CellStatus(MatrixCell("monetdb", "time_series", 1, operation), "missing", None, "no run")
        for operation in ("populate", "select")
    ]
    populate, select = format_matrix_status(statuses).splitlines()[2:]

    assert unsupported_query_count("time_series", "monetdb") == 3
    assert populate.split()[5] == "no"
    assert select.split()[5] == "3"
