from datetime import datetime
from pathlib import Path

import polars as pl
import pytest

from ..dbs.timescaledb import TimescaleDB, TimescaleTimeSeries


class FakeConnection:
    def __init__(self) -> None:
        self.executed_sql: list[str] = []
        self.commit_calls = 0

    def execute(self, statement: object) -> None:
        self.executed_sql.append(str(statement))

    def commit(self) -> None:
        self.commit_calls += 1


class FakeTimescaleDB:
    name = "timescaledb"

    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection

    def connect(self, reconnect: bool = False) -> FakeConnection:
        _ = reconnect
        return self.connection


def test_timescaledb_time_series_tables_have_no_primary_keys() -> None:
    suite = TimescaleTimeSeries.model_construct(
        db=FakeTimescaleDB(FakeConnection()),
        name="time_series",
        scale_factor=1,
    )

    assert suite.get_primary_key("data_tall") is None
    assert suite.get_primary_key("data_wide") is None
    assert suite.get_primary_key("data_large") is None

    assert suite.get_not_null("data_tall") == "time"
    assert suite.get_not_null("data_wide") == ["time", "metric_name"]
    assert suite.get_not_null("data_large") == ["time", "metric_name"]


def test_timescaledb_upsert_uses_on_conflict_instead_of_delete_insert(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    connection = FakeConnection()
    copied_tables: list[tuple[str, bool]] = []

    def fake_connect(self: TimescaleDB, reconnect: bool = False) -> FakeConnection:
        _ = (self, reconnect)
        return connection

    def fake_copy_csv_to_table(
        self: TimescaleDB,
        con: FakeConnection,
        table: str,
        csv_path: Path,
    ) -> None:
        _ = (self, con)
        copied_tables.append((table, csv_path.exists()))

    data_dir = tmp_path / "timescaledb" / "data"
    data_dir.mkdir(parents=True)

    monkeypatch.setattr("olap_benchmarks.settings.SETTINGS.temporary_directory", tmp_path)
    monkeypatch.setattr(TimescaleDB, "connect", fake_connect)
    monkeypatch.setattr(TimescaleDB, "_copy_csv_to_table", fake_copy_csv_to_table)

    db = TimescaleDB()
    df = pl.DataFrame({"time": [datetime(2025, 1, 1)], "value": [1.5]})

    db.upsert(df, "data_tall", primary_key="time")

    executed_sql = "\n".join(connection.executed_sql)

    assert "DELETE FROM data_tall" not in executed_sql
    assert 'ON CONFLICT ("time") DO UPDATE SET "value" = EXCLUDED."value"' in executed_sql
    assert copied_tables[0][0].startswith("_staging_data_tall_")
    assert copied_tables[0][1] is True
    assert connection.commit_calls == 1
