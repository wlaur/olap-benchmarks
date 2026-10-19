import re

import polars as pl

from ..dbs.duckdb import DuckDB
from ..dbs.questdb import QuestDB
from ..dbs.starrocks import StarRocks
from ..settings import REPO_ROOT


def test_questdb_clickbench_empty_string_filters_match_reference() -> None:
    reference = DuckDB().benchmarks["clickbench"]
    questdb = QuestDB().benchmarks["clickbench"]
    for index in range(43):
        name = f"Q{index}"
        expected = sorted(re.findall(r"(\w+) <> ''", reference.load_query(name)))
        query = questdb.load_query(name)
        assert sorted(re.findall(r"\((\w+) IS NOT NULL AND \1 <> ''\)", query)) == expected, name
        assert "AVG(length(" not in query, name


def test_questdb_clickbench_event_dates_are_dates() -> None:
    schema = (REPO_ROOT / "olap_benchmarks/suites/clickbench/schemas/questdb.sql").read_text()
    assert re.search(r"^\s+EventDate timestamp,", schema, re.MULTILINE)
    suite = QuestDB().benchmarks["clickbench"]
    for name in ("Q6", "Q23", "Q40"):
        schemas = suite.query_fetch_kwargs(name)["schema"]
        assert set(schemas.values()) == {pl.Date}
        assert all("EventDate" in column for column in schemas)
    assert suite.query_fetch_kwargs("Q0") == {}


def test_starrocks_clickbench_raw_rows_use_canonical_column_order() -> None:
    directory = REPO_ROOT / "olap_benchmarks/suites/clickbench/schemas"

    def schema_columns(engine: str) -> list[str]:
        schema = (directory / f"{engine}.sql").read_text().split("CREATE TABLE hits", 1)[1]
        return re.findall(r"^\s+(\w+)\s+\w+", schema, re.MULTILINE)

    canonical = schema_columns("duckdb")
    physical = schema_columns("starrocks")
    assert canonical != physical and set(canonical) == set(physical)
    query = StarRocks().benchmarks["clickbench"].load_query("Q23")
    projection = query.split("SELECT ", 1)[1].split(" FROM hits", 1)[0]
    assert projection.split(", ") == canonical
