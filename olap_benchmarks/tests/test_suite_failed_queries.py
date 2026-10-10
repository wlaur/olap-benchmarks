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


def _defines_select(source: Path) -> bool:
    tree = ast.parse(source.read_text())
    return any(isinstance(node, ast.FunctionDef) and node.name == "select" for node in ast.walk(tree))


@pytest.mark.parametrize(
    "source",
    [
        *(REPO_ROOT / f"olap_benchmarks/suites/{suite}/config.py" for suite in SUITE_NAMES),
        *sorted((REPO_ROOT / "olap_benchmarks/dbs").rglob("*.py")),
    ],
    ids=lambda path: path.relative_to(REPO_ROOT / "olap_benchmarks").as_posix(),
)
def test_select_always_runs_the_profile_loop(source: Path) -> None:
    """BenchmarkSuite.run_select_queries applies the benchmark profile's iteration policy and fails
    the run when a query fails after the remaining ones were measured. A suite or connector with its
    own select() would bypass both."""
    assert not _defines_select(source), f"{source.name} overrides select(); use run_select_queries instead"
