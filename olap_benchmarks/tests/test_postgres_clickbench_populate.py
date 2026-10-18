from collections.abc import Generator
from contextlib import contextmanager
from typing import cast

import polars as pl
import pytest

from ..dbs.postgres import PostgresClickbench
from ..settings import SuiteName, TableName


class _ClickbenchDatabase:
    name = "postgres"

    def __init__(self, actual_rows: int, frame: pl.LazyFrame) -> None:
        self.actual_rows = actual_rows
        self.frame = frame
        self.events: list[str] = []

    def initialize_schema(self, suite: SuiteName) -> None:
        assert suite == "clickbench"
        self.events.append("schema")

    @contextmanager
    def phase_context(self, phase_name: str, table_name: str | None = None) -> Generator[None]:
        if phase_name == "insert":
            assert table_name == "hits"
        self.events.append(f"phase:{phase_name}")
        yield

    def insert(self, frame: pl.DataFrame | pl.LazyFrame, table: TableName, **kwargs: object) -> None:
        assert frame is self.frame
        assert table == "hits"
        assert not kwargs
        self.events.append("insert")

    def get_row_count(self, table: TableName) -> int:
        assert table == "hits"
        self.events.append("count")
        return self.actual_rows

    def restart_event(self) -> None:
        self.events.append("restart")


class _ClickbenchProjection:
    def __init__(self, frame: pl.LazyFrame) -> None:
        self.frame = frame
        self.columns: list[str] = []

    def select(self, columns: list[str]) -> pl.LazyFrame:
        self.columns = columns
        return self.frame


@pytest.mark.parametrize("actual_rows", [0, 2, 3])
def test_postgres_clickbench_verifies_all_loaded_rows_before_indexing(
    actual_rows: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    frame = pl.LazyFrame({"WatchID": [1, 2, 3]})
    database = _ClickbenchDatabase(actual_rows, frame)
    projection = _ClickbenchProjection(frame)
    suite = PostgresClickbench.model_construct(db=database, name="clickbench", scale_factor=1)

    def load_dataset(_self: PostgresClickbench) -> pl.LazyFrame:
        return cast(pl.LazyFrame, projection)

    def expected_rows(_self: PostgresClickbench) -> dict[TableName, int]:
        return {"hits": 3}

    def index_table(_self: PostgresClickbench) -> None:
        database.events.append("index")

    monkeypatch.setattr(PostgresClickbench, "load_dataset", load_dataset)
    monkeypatch.setattr(PostgresClickbench, "expected_table_row_counts", expected_rows)
    monkeypatch.setattr(PostgresClickbench, "index_table", index_table)

    if actual_rows == 3:
        suite.populate()
    else:
        with pytest.raises(RuntimeError, match=f"expected=3, actual={actual_rows}"):
            suite.populate()

    expected = ["schema", "phase:insert", "insert", "phase:verify_populate", "count"]
    if actual_rows == 3:
        expected.extend(["phase:index", "index", "restart"])
    assert database.events == expected
    assert len(projection.columns) == len(set(projection.columns)) == 105
    assert projection.columns[:3] == ["WatchID", "UserID", "FUniqID"]
    assert projection.columns[-1] == "HitColor"
