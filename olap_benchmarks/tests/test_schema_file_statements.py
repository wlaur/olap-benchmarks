from __future__ import annotations

from pathlib import Path
from typing import cast

import pytest

from ..dbs import Database


class _RecordingDatabase:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, stmt: str, reconnect: bool = False) -> None:
        _ = reconnect
        self.statements.append(stmt)


def _split(tmp_path: Path, sql: str) -> list[str]:
    fpath = tmp_path / "schema.sql"
    fpath.write_text(sql)
    db = _RecordingDatabase()
    # only execute() is reached, so a stub avoids standing up a real connector
    Database.execute_schema_file(cast(Database, db), fpath)
    return db.statements


def test_semicolon_in_a_comment_does_not_split_a_statement(tmp_path: Path) -> None:
    statements = _split(
        tmp_path,
        "CREATE TABLE t (\n"
        "    a integer not null,\n"
        "    -- text is not nullable; this is the only column holding NULLs\n"
        "    b Nullable(String)\n"
        ") ENGINE = MergeTree;\n",
    )

    assert len(statements) == 1
    assert statements[0].startswith("CREATE TABLE t (")
    assert statements[0].endswith(") ENGINE = MergeTree")
    assert statements[0].count("(") == statements[0].count(")")


def test_commented_out_statement_is_not_executed(tmp_path: Path) -> None:
    statements = _split(tmp_path, "CREATE TABLE t (a integer);\n-- CREATE TABLE u (b integer);\n")

    assert statements == ["CREATE TABLE t (a integer)"]


@pytest.mark.parametrize("suite_schema", sorted(Path("olap_benchmarks/suites").glob("*/schemas/*.sql")))
def test_shipped_schemas_have_balanced_parentheses_per_statement(suite_schema: Path, tmp_path: Path) -> None:
    for stmt in _split(tmp_path, suite_schema.read_text()):
        assert stmt.count("(") == stmt.count(")"), f"unbalanced parentheses in {suite_schema}:\n{stmt}"
