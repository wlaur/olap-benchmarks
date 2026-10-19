from __future__ import annotations

from contextlib import AbstractContextManager
from pathlib import Path

import polars as pl
import pytest

from ..dbs.utils import sort_parquet_file, tracked_commit
from ..settings import get_settings


class FakeRecorderContext(AbstractContextManager[None]):
    def __init__(self, events: list[str]) -> None:
        self.events = events

    def __enter__(self) -> None:
        self.events.append("enter")

    def __exit__(self, exc_type: object, exc: object, exc_tb: object) -> None:
        self.events.append("exit")


class FakeRecorderConnection:
    def __init__(self) -> None:
        self.events: list[str] = []
        self.recorded_queries: list[str] = []
        self.info: dict[str, object] = {"olap_query_recorder": self.record_query_execution}

    def record_query_execution(self, query: str) -> FakeRecorderContext:
        self.recorded_queries.append(query)
        return FakeRecorderContext(self.events)

    def commit(self) -> None:
        self.events.append("commit")


class FakeDBAPIConnection:
    def __init__(self) -> None:
        self.commit_calls = 0

    def commit(self) -> None:
        self.commit_calls += 1


def test_tracked_commit_records_commit_on_connection() -> None:
    connection = FakeRecorderConnection()

    tracked_commit(connection)

    assert connection.recorded_queries == ["COMMIT"]
    assert connection.events == ["enter", "commit", "exit"]


def test_tracked_commit_records_commit_for_raw_dbapi_connection() -> None:
    recorder_connection = FakeRecorderConnection()
    raw_connection = FakeDBAPIConnection()

    tracked_commit(raw_connection, recorder_source=recorder_connection)

    assert recorder_connection.recorded_queries == ["COMMIT"]
    assert recorder_connection.events == ["enter", "exit"]
    assert raw_connection.commit_calls == 1


def test_sort_parquet_file_preserves_types_and_rows(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    settings = get_settings()
    monkeypatch.setattr(settings, "client_memory_limit_gb", 2)
    monkeypatch.setattr(settings, "benchmark_threads", 2)
    source = tmp_path / "source.parquet"
    target = tmp_path / "sorted.parquet"
    frame = pl.DataFrame(
        {
            "EventTime": pl.Series([3, 1, 2, 1, 2**53 + 1], dtype=pl.Int64),
            "EventDate": pl.Series([15900, 15901, None, 15902, 15903], dtype=pl.UInt16),
            "Small": pl.Series([-1, 2, 3, None, 5], dtype=pl.Int16),
            "Medium": pl.Series([7, None, 9, 10, 11], dtype=pl.Int32),
            "URL": ["c\r\nline", "", None, "ä", "b'quote"],
        }
    )
    frame.write_parquet(source)
    sort_parquet_file(source, target, "EventTime")
    result = pl.read_parquet(target)
    assert result.schema == frame.schema
    assert result["EventTime"].to_list() == [1, 1, 2, 3, 2**53 + 1]
    assert result.sort(["EventTime", "EventDate"]).equals(frame.sort(["EventTime", "EventDate"]))
    assert sorted(path.name for path in tmp_path.iterdir()) == ["sorted.parquet", "source.parquet"]
