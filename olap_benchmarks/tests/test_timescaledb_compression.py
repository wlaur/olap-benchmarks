from pathlib import Path

import pytest

from ..dbs.timescaledb import TimescaleTimeSeries


class FakeConnection:
    def __init__(self) -> None:
        self.executed_sql: list[str] = []
        self.commit_calls = 0
        self.rollback_calls = 0

    def execute(self, statement: object) -> None:
        sql = str(statement)
        self.executed_sql.append(sql)

    def commit(self) -> None:
        self.commit_calls += 1

    def rollback(self) -> None:
        self.rollback_calls += 1

    def execution_options(self, **_: object) -> "FakeConnection":
        return self


class FakeTimescaleDB:
    name = "timescaledb"

    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection

    def connect(self, reconnect: bool = False) -> FakeConnection:
        return self.connection


def test_timescaledb_time_series_compression_skips_too_wide_tables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = FakeConnection()
    suite = TimescaleTimeSeries.model_construct(
        db=FakeTimescaleDB(connection),
        name="time_series",
    )

    monkeypatch.setattr(
        "olap_benchmarks.dbs.timescaledb.get_time_series_input_files",
        lambda: {
            "data_wide": Path("data_wide.parquet"),
            "data_tall": Path("data_tall.parquet"),
        },
    )

    suite.compress_tables()

    executed_sql = "\n".join(connection.executed_sql)

    assert "show_chunks('data_wide')" not in executed_sql
    assert "show_chunks('data_tall')" in executed_sql
    assert "vacuum freeze analyze data_wide" in executed_sql
    assert "vacuum freeze analyze data_tall" in executed_sql
    assert connection.commit_calls == 1
    assert connection.rollback_calls == 0
