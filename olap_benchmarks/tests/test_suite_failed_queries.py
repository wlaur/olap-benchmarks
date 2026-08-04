from __future__ import annotations

import ast
from pathlib import Path

import pytest

from ..dbs import Database
from ..settings import REPO_ROOT, SUITE_NAMES
from ..suites import BenchmarkSuite, FailedQueriesError


class _Suite(BenchmarkSuite[Database]):
    def populate(self) -> None: ...

    def select(self) -> None: ...


def _suite(db_name: str = "clickhouse") -> _Suite:
    # model_construct skips validation, so a stub carrying just the attribute the message reads is
    # enough and avoids standing up a real connector
    return _Suite.model_construct(name="tpc_h", db=type("_Db", (), {"name": db_name})())


def test_no_failed_queries_is_a_no_op() -> None:
    _suite().assert_no_failed_queries(0, 22)


@pytest.mark.parametrize(("failed", "expected"), [(1, "1 of 22 tpc_h query failed"), (3, "3 of 22 tpc_h queries")])
def test_failed_queries_raise(failed: int, expected: str) -> None:
    with pytest.raises(FailedQueriesError, match=expected):
        _suite().assert_no_failed_queries(failed, 22)


def _select_calls_assert(source: Path) -> bool:
    """True if every select() in the module ends up calling assert_no_failed_queries."""
    tree = ast.parse(source.read_text())
    selects = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == "select" and node.args.args[:1]
    ]
    if not selects:
        return False

    return all(
        any(
            isinstance(call.func, ast.Attribute) and call.func.attr == "assert_no_failed_queries"
            for call in ast.walk(select)
            if isinstance(call, ast.Call)
        )
        for select in selects
    )


@pytest.mark.parametrize("suite", SUITE_NAMES)
def test_every_suite_fails_the_run_when_a_query_fails(suite: str) -> None:
    """execute_query_with_isolation keeps going after a failed query so the remaining ones are
    still measured. Every suite must then fail the run, or a query that fails on every single
    iteration is recorded as a completed run with nothing but a log line to show for it."""
    source = REPO_ROOT / f"olap_benchmarks/suites/{suite}/config.py"
    assert _select_calls_assert(source), (
        f"{suite}/config.py select() does not call assert_no_failed_queries, so failed queries "
        f"would be reported as a completed run"
    )
