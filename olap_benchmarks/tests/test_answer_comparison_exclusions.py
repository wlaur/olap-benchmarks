from __future__ import annotations

from ..dbs import get_databases
from ..results.validation import ANSWER_COMPARISON_EXCLUSIONS, _excluded_query_names
from ..settings import SUITE_NAMES


def test_exclusions_name_real_suites() -> None:
    assert set(SUITE_NAMES) >= set(ANSWER_COMPARISON_EXCLUSIONS)


def test_every_exclusion_states_a_reason() -> None:
    # an entry here asserts that no engine is at fault, which needs justifying in the source
    for suite, queries in ANSWER_COMPARISON_EXCLUSIONS.items():
        for query_name, reason in queries.items():
            assert len(reason) > 40, f"{suite}/{query_name} needs a reason"


def test_scoping_to_a_suite_keeps_only_its_exclusions() -> None:
    assert _excluded_query_names("clickbench") == {"clickbench": ANSWER_COMPARISON_EXCLUSIONS["clickbench"]}
    assert _excluded_query_names("tpc_h") == {}
    assert _excluded_query_names(None) == ANSWER_COMPARISON_EXCLUSIONS


def test_clickbench_q28_is_the_only_exclusion() -> None:
    # the list should stay short; a new entry deserves the same scrutiny Q28 got
    assert list(ANSWER_COMPARISON_EXCLUSIONS) == ["clickbench"]
    assert list(ANSWER_COMPARISON_EXCLUSIONS["clickbench"]) == ["Q28"]


def test_monetdb_chat_threads_skips_the_crashing_concurrent_operation() -> None:
    databases = get_databases()

    monetdb = databases["monetdb"]
    monetdb._current_suite = "chat_threads"
    monetdb._current_suite_scale_factor = 1
    assert monetdb.benchmarks["chat_threads"].supported_operations == ("populate", "select", "mutate")

    for other in ("duckdb", "clickhouse"):
        database = databases[other]
        database._current_suite = "chat_threads"
        database._current_suite_scale_factor = 1
        assert "concurrent" in database.benchmarks["chat_threads"].supported_operations
