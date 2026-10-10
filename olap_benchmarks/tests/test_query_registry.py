from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest

from ..dbs import get_databases
from ..dbs.postgres import PostgresTimeSeries
from ..matrix import MatrixCell
from ..query_registry import (
    ANSWER_COMPARISON_EXCLUSIONS,
    SUITES_DIRECTORY,
    UNSUPPORTED_QUERIES,
    unsupported_query_count,
)
from ..results.cells import CellStatus, format_matrix_status
from ..settings import SUITE_NAMES, SUITE_SPECS, DatabaseName, SuiteName
from ..suites import BenchmarkSuite


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


def test_postgres_declares_every_query_of_the_tables_it_skips() -> None:
    skipped_sizes = {table.removeprefix("data_") for table in PostgresTimeSeries.SKIP_TABLES}
    declared = {query for suite, db, query in UNSUPPORTED_QUERIES if (suite, db) == ("time_series", "postgres")}

    assert declared == {query for query in PLANNED["time_series", "postgres"] if query.split("_")[0] in skipped_sizes}


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


def test_matrix_status_shows_declared_unsupported_queries_on_select_cells() -> None:
    statuses = [
        CellStatus(MatrixCell("monetdb", "time_series", 1, operation), "missing", None, "no run")
        for operation in ("populate", "select")
    ]
    populate, select = format_matrix_status(statuses).splitlines()[2:]

    assert unsupported_query_count("time_series", "monetdb") == 3
    assert populate.split()[5] == "no"
    assert select.split()[5] == "3"
