from collections.abc import Mapping
from contextlib import AbstractContextManager, nullcontext
from datetime import datetime
from pathlib import Path
from typing import Any, cast

import polars as pl
import pytest

from ..dbs import Database
from ..dbs.postgres import Postgres, PostgresTimeSeries
from ..dbs.timescaledb import TimescaleTimeSeries
from ..settings import TableName, get_settings
from ..suites.time_series.config import TIME_SERIES_QUERY_NAMES, TimeSeries


class RecordingTimeSeriesDB:
    name = "postgres"

    def __init__(self) -> None:
        self.expected_counts: dict[TableName, int] = {}
        self.loaded: set[TableName] = set()
        self.inserted: list[TableName] = []
        self.created: list[TableName] = []
        self.statements: list[str] = []
        self.schema_files: list[str] = []

    def phase_context(self, phase_name: str, table_name: str | None = None) -> AbstractContextManager[None]:
        _ = phase_name, table_name
        return nullcontext()

    def get_row_count(self, table: TableName) -> int:
        if table not in self.loaded:
            raise KeyError(table)
        return self.expected_counts[table]

    def get_table_names(self) -> set[TableName]:
        return self.loaded.copy()

    def rollback(self) -> None:
        pass

    def commit(self) -> None:
        pass

    def connect(self) -> "RecordingTimeSeriesDB":
        return self

    def initialize_schema(self, suite: str) -> None:
        assert suite == "time_series"

    def execute(self, statement: str, **kwargs: object) -> None:
        _ = kwargs
        self.statements.append(statement)

    def execute_schema_file(self, path: Path) -> None:
        self.schema_files.append(path.name)

    def create_table(
        self,
        schema: Mapping[str, pl.DataType | type[pl.DataType]],
        table: TableName,
        primary_key: str | list[str] | None = None,
        not_null: str | list[str] | None = None,
    ) -> None:
        _ = schema, primary_key, not_null
        self.created.append(table)

    def insert_parquet(self, path: Path, table: TableName, **kwargs: object) -> None:
        _ = path, kwargs
        self.inserted.append(table)
        self.loaded.add(table)

    def insert(self, frame: pl.DataFrame | pl.LazyFrame, table: TableName, **kwargs: object) -> None:
        _ = frame
        self.insert_parquet(Path(), table, **kwargs)


@pytest.mark.parametrize("suite_type", [TimeSeries[Database], PostgresTimeSeries[Postgres], TimescaleTimeSeries])
@pytest.mark.parametrize("selected_table", ["data_tall", "data_wide"])
def test_population_only_loads_and_prepares_selected_tables(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    suite_type: type[TimeSeries[Any]],
    selected_table: TableName,
) -> None:
    settings = get_settings()
    monkeypatch.setattr(settings, "tables", (selected_table,))
    monkeypatch.setattr(settings, "input_data_directory", tmp_path)
    db = RecordingTimeSeriesDB()
    suite = suite_type.model_construct(db=db, name="time_series", scale_factor=1)
    files = suite.dataset_files()
    assert set(files) == {selected_table}
    path = files[selected_table]
    path.parent.mkdir(parents=True)
    pl.DataFrame({"time": [datetime(2025, 1, 1)], "process_1": [1.0]}).write_parquet(path)
    db.expected_counts = dict(suite.expected_table_row_counts())

    def fake_eav_insert(self: PostgresTimeSeries[Any], table: TableName, source: Path) -> None:
        cast(RecordingTimeSeriesDB, self.db).insert_parquet(source, table)

    monkeypatch.setattr(PostgresTimeSeries, "_eav_streaming_insert", fake_eav_insert)
    monkeypatch.setattr(TimescaleTimeSeries, "_eav_streaming_insert", fake_eav_insert)

    suite.populate(restart=False)

    assert db.inserted == [selected_table]
    assert db.loaded == {selected_table}
    assert set(suite.expected_table_row_counts()) == {selected_table}
    for excluded in {"data_tall", "data_wide", "data_large"} - {selected_table}:
        assert all(excluded not in statement for statement in db.statements)
        assert excluded not in db.created
    if isinstance(suite, TimescaleTimeSeries):
        assert db.schema_files == [f"{selected_table.removeprefix('data_')}_pre_insert.sql"]
        assert any(f"show_chunks('{selected_table}')" in statement for statement in db.statements)
    elif isinstance(suite, PostgresTimeSeries):
        assert any(f"CREATE INDEX {selected_table}_" in statement for statement in db.statements)


def test_time_series_selection_keeps_the_full_query_plan(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(get_settings(), "tables", ("data_tall", "data_wide"))
    suite = PostgresTimeSeries[Postgres](db=Postgres(), name="time_series", scale_factor=10)

    assert suite.planned_query_names() == TIME_SERIES_QUERY_NAMES
    assert suite.query_skip("tall_01_max_time") is None
    assert suite.query_skip("wide_01_max_time") is None
    assert suite.query_skip("large_01_max_time") == ("skipped", "table excluded by campaign tables")
    assert suite.expected_table_row_counts() == {"data_tall": 2_000_000, "data_wide": 300_000_000}


@pytest.mark.parametrize("patterns", [("missing_table",), ("tall",)])
def test_time_series_rejects_table_selection_with_no_matches(
    monkeypatch: pytest.MonkeyPatch, patterns: tuple[str, ...]
) -> None:
    monkeypatch.setattr(get_settings(), "tables", patterns)
    suite = TimeSeries[Database].model_construct(db=RecordingTimeSeriesDB(), name="time_series", scale_factor=1)

    with pytest.raises(ValueError, match="matches no time_series tables"):
        suite.dataset_files()
    with pytest.raises(ValueError, match="matches no time_series tables"):
        suite.planned_query_names()
    with pytest.raises(ValueError, match="matches no time_series tables"):
        suite.get_mutate_steps()


@pytest.mark.parametrize(("scale_factor", "expected_large_rows"), [(1, 599_999_835), (10, 6_000_000_000)])
def test_postgres_large_table_support_depends_on_explicit_campaign_selection(
    monkeypatch: pytest.MonkeyPatch, scale_factor: int, expected_large_rows: int
) -> None:
    monkeypatch.setattr(get_settings(), "tables", ())
    db = Postgres()
    suite = PostgresTimeSeries[Postgres](db=db, name="time_series", scale_factor=scale_factor)

    assert suite.expected_table_row_counts()["data_large"] == expected_large_rows
    assert suite.query_skip("large_01_max_time") is None
    assert db.is_mutation_step_enabled("time_series", "insert_data_large_1")
    assert suite.query_skip("large_23_batch_export") == (
        "unsupported",
        "query requires wide-row export shape, but this database stores data_large as EAV",
    )
