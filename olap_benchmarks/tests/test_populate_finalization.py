from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any, Literal, cast

import pytest
from sqlalchemy import Connection

from .. import dbs as dbs_module
from ..dbs.postgres import Postgres
from ..dbs.timescaledb import TimescaleDB
from ..metrics.storage import Storage
from ..settings import SuiteName, TableName
from ..suites import BenchmarkSuite
from .test_benchmark_profile import ProfileDatabase, RecordingStorage


class FinalizationSuite(BenchmarkSuite["FinalizationDatabase"]):
    name: SuiteName = "clickbench"

    def should_populate(self) -> bool:
        return not self.db.skip_populate

    def populate(self) -> None:
        self.db.events.append("populate")
        if self.db.populate_error:
            raise ValueError("load failed")
        self.db.restart_event()

    def select(self) -> None:
        self.db.events.append("select")


class FinalizationDatabase(ProfileDatabase):
    events: list[str] = []
    skip_populate: bool = False
    populate_error: bool = False
    finalization_error: bool = False

    def suite_registry(self) -> Mapping[SuiteName, type[BenchmarkSuite[Any]]]:
        return {"clickbench": FinalizationSuite}

    def restart_event(self) -> None:
        self.events.append("restart")

    def before_benchmark_operation(self) -> None:
        self.events.append("before")

    def finalize_populate(self) -> None:
        self.events.append("finalize")
        if self.finalization_error:
            raise RuntimeError("statistics failed")

    def after_benchmark_operation(self) -> None:
        self.events.append("after")


class _Sampler:
    def finish(self, finished_at: datetime) -> None:
        _ = finished_at


@pytest.fixture
def lifecycle_storage(monkeypatch: pytest.MonkeyPatch) -> RecordingStorage:
    storage = RecordingStorage()

    def create_storage(_self: FinalizationDatabase) -> RecordingStorage:
        return storage

    def metadata(**_kwargs: object) -> dict[str, object]:
        return {}

    def sampler(**_kwargs: object) -> _Sampler:
        return _Sampler()

    monkeypatch.setattr(FinalizationDatabase, "create_result_storage", create_storage)
    monkeypatch.setattr(dbs_module, "build_system_metadata", dict)
    monkeypatch.setattr(dbs_module, "build_run_metadata", metadata)
    monkeypatch.setattr(dbs_module, "start_metric_sampler", sampler)
    return storage


@pytest.mark.parametrize("failure", ["none", "populate", "finalize"])
def test_populate_finalization_only_follows_successful_load_and_runs_before_cleanup(
    lifecycle_storage: RecordingStorage, failure: Literal["none", "populate", "finalize"]
) -> None:
    database = FinalizationDatabase(populate_error=failure == "populate", finalization_error=failure == "finalize")

    if failure == "none":
        database.benchmark("clickbench", "populate")
    else:
        error, message = (ValueError, "load failed") if failure == "populate" else (RuntimeError, "statistics failed")
        with pytest.raises(error, match=message):
            database.benchmark("clickbench", "populate")

    assert database.events == (
        ["before", "populate", "after"]
        if failure == "populate"
        else ["before", "populate", "restart", "finalize", "after"]
    )
    assert lifecycle_storage.run["status"] == ("completed" if failure == "none" else "failed")
    phases = [step for step in lifecycle_storage.steps.values() if step["step_type"] == "phase"]
    assert [step["step_name"] for step in phases] == ["populate"]
    assert phases[0]["status"] == lifecycle_storage.run["status"]


def test_select_does_not_finalize_populate(lifecycle_storage: RecordingStorage) -> None:
    database = FinalizationDatabase()

    database.benchmark("clickbench", "select")

    assert database.events == ["restart", "before", "select", "after"]
    assert lifecycle_storage.run["status"] == "completed"


def test_skipped_populate_does_not_finalize_or_record_an_unmeasured_operation(
    lifecycle_storage: RecordingStorage,
) -> None:
    database = FinalizationDatabase(skip_populate=True)

    database.benchmark("clickbench", "populate")

    assert database.events == []
    assert lifecycle_storage.run == {}


class QueryStorage(RecordingStorage):
    def __init__(self) -> None:
        super().__init__()
        self.queries: list[dict[str, object]] = []

    def insert_query_execution(self, **kwargs: object) -> None:
        self.queries.append(kwargs)


class AnalyzeConnection:
    def __init__(self, fail: bool = False) -> None:
        self.info: dict[str, object] = {}
        self.statements: list[str] = []
        self.commits = 0
        self.fail = fail

    def execute(self, statement: object) -> None:
        sql = str(statement)
        self.statements.append(sql)
        if self.fail:
            raise RuntimeError("ANALYZE failed")

    def commit(self) -> None:
        self.commits += 1


@pytest.mark.parametrize("database_type", [Postgres, TimescaleDB])
@pytest.mark.parametrize("fail", [False, True])
def test_postgres_family_finalization_analyzes_existing_tables_and_records_phase_and_sql(
    database_type: type[Postgres], fail: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = database_type()
    storage = QueryStorage()
    connection = AnalyzeConnection(fail=fail)
    database._run_id = 1
    database._current_operation = "populate"
    database._result_storage = cast(Storage, storage)

    def table_names(_self: Postgres) -> set[TableName]:
        return {'odd"name', "events"}

    def connect(self: Postgres, reconnect: bool = False) -> Connection:
        assert not reconnect
        return self.bind_query_recorder(cast(Connection, connection))

    monkeypatch.setattr(Postgres, "get_table_names", table_names)
    monkeypatch.setattr(Postgres, "connect", connect)

    if fail:
        with pytest.raises(RuntimeError, match="ANALYZE failed"):
            database.finalize_populate()
    else:
        database.finalize_populate()

    expected_sql = ['ANALYZE public."events"'] if fail else ['ANALYZE public."events"', 'ANALYZE public."odd""name"']
    assert connection.statements == expected_sql
    assert connection.commits == (0 if fail else 2)
    phases = list(storage.steps.values())
    assert [step["step_name"] for step in phases] == ["analyze"] * len(expected_sql)
    assert [step["table_name"] for step in phases] == (["events"] if fail else ["events", 'odd"name'])
    assert [step["status"] for step in phases] == (["failed"] if fail else ["completed", "completed"])
    queries = [query for query in storage.queries if str(query["query"]).startswith("ANALYZE")]
    assert [query["query"] for query in queries] == expected_sql
    assert [query["run_step_id"] for query in queries] == list(storage.steps)
    assert all(step["step_type"] == "phase" for step in phases)


@pytest.mark.parametrize("database_type", [Postgres, TimescaleDB])
def test_postgres_family_finalization_does_not_analyze_absent_tables(
    database_type: type[Postgres], monkeypatch: pytest.MonkeyPatch
) -> None:
    def no_tables(_self: Postgres) -> set[TableName]:
        return set()

    def unexpected_execute(
        _self: Postgres, _statement: str, commit: bool = True, autocommit: bool = False, reconnect: bool = False
    ) -> None:
        raise AssertionError("No existing table should be analyzed")

    monkeypatch.setattr(Postgres, "get_table_names", no_tables)
    monkeypatch.setattr(Postgres, "execute", unexpected_execute)

    database_type().finalize_populate()
