from __future__ import annotations

from contextlib import AbstractContextManager

from ..dbs.utils import tracked_commit


class FakeRecorderContext(AbstractContextManager[None]):
    def __init__(self, events: list[str]) -> None:
        self.events = events

    def __enter__(self) -> None:
        self.events.append("enter")
        return None

    def __exit__(self, exc_type: object, exc: object, exc_tb: object) -> None:
        self.events.append("exit")
        return None


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
