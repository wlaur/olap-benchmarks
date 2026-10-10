from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from hashlib import sha256
from typing import Any, cast

import polars as pl
import pytest
from sqlalchemy import Connection

from .. import dbs as dbs_module
from ..dbs import Database
from ..profile import BENCHMARK_PROFILE, BenchmarkProfile
from ..settings import DatabaseName, SuiteName, TableName
from ..suites import BenchmarkSuite


def test_resident_hot_profile_runs_one_warmup_and_three_measured_iterations() -> None:
    assert BENCHMARK_PROFILE.id == "resident-hot-v1"
    assert BENCHMARK_PROFILE.total_iterations == 4
    assert [BENCHMARK_PROFILE.iteration_role(iteration) for iteration in range(1, 5)] == [
        "warmup",
        "measured",
        "measured",
        "measured",
    ]
    with pytest.raises(ValueError, match="outside profile"):
        BENCHMARK_PROFILE.iteration_role(5)
    assert BENCHMARK_PROFILE.metadata()["id"] == "resident-hot-v1"


def test_profile_requires_a_measured_iteration() -> None:
    with pytest.raises(ValueError, match="measured"):
        BenchmarkProfile(
            id="broken",
            warmup_iterations=1,
            measured_iterations=0,
            restart_server_before_select=True,
            timed_boundary="",
            cache_policy="",
            scope="",
        )


class RecordingStorage:
    def __init__(self) -> None:
        self.steps: dict[int, dict[str, object]] = {}
        self.run: dict[str, object] = {}

    def insert_run(self, **kwargs: object) -> int:
        self.run.update(kwargs)
        return 1

    def finish_run(self, **kwargs: object) -> None:
        self.run.update(kwargs)

    def start_step(self, **kwargs: object) -> int:
        step_id = len(self.steps) + 1
        self.steps[step_id] = dict(kwargs)
        return step_id

    def finish_step(self, step_id: int, **kwargs: object) -> None:
        self.steps[step_id].update(kwargs)

    def insert_query_execution(self, **kwargs: object) -> None:
        _ = kwargs


class ProfileSuite(BenchmarkSuite["ProfileDatabase"]):
    name: SuiteName = "clickbench"

    def populate(self) -> None:
        raise NotImplementedError

    def planned_query_names(self) -> Sequence[str]:
        return ("a", "b", "c")

    def load_query(self, query_name: str) -> str:
        return f"select '{query_name}'"


class ProfileDatabase(Database):
    name: DatabaseName = "duckdb"
    version: str = "test"
    connection_string: str = "dummy://"
    restarts: int = 0
    abort_at: tuple[str, int] = ("b", 2)
    iteration_counter: dict[str, int] = {}

    @property
    def start(self) -> None:
        return None

    def restart_event(self) -> None:
        self.restarts += 1

    def connect(self, reconnect: bool = False) -> Connection:
        raise NotImplementedError

    def fetch(
        self,
        query: str,
        schema: Mapping[str, pl.DataType | type[pl.DataType]] | None = None,
    ) -> pl.DataFrame:
        query_name = query.split("'")[1]
        iteration = self.iteration_counter.get(query_name, 0) + 1
        self.iteration_counter[query_name] = iteration
        if (query_name, iteration) == self.abort_at:
            raise KeyboardInterrupt
        return pl.DataFrame({"value": [query_name]})

    def get_table_names(self) -> set[TableName]:
        return set()

    def insert(
        self,
        df: pl.DataFrame | pl.LazyFrame,
        table: TableName,
        primary_key: str | list[str] | None = None,
        not_null: str | list[str] | None = None,
    ) -> None:
        raise NotImplementedError

    def upsert(self, df: pl.DataFrame, table: TableName, primary_key: str | list[str]) -> None:
        raise NotImplementedError

    def delete(self, table: TableName, primary_key: str | list[str], keys: pl.DataFrame) -> None:
        raise NotImplementedError

    def suite_registry(self) -> Mapping[SuiteName, type[BenchmarkSuite[Any]]]:
        return {"clickbench": ProfileSuite}


class _Sampler:
    def finish(self, finished_at: datetime) -> None:
        _ = finished_at


def test_select_follows_the_profile_and_records_unstarted_planned_steps(monkeypatch: pytest.MonkeyPatch) -> None:
    storage = RecordingStorage()
    captured_metadata: dict[str, object] = {}

    def fake_run_metadata(**kwargs: object) -> dict[str, object]:
        captured_metadata.update(kwargs)
        return {}

    db = ProfileDatabase()

    def fake_storage(_self: ProfileDatabase) -> RecordingStorage:
        return storage

    def fake_sampler(**_kwargs: object) -> _Sampler:
        return _Sampler()

    monkeypatch.setattr(ProfileDatabase, "create_result_storage", fake_storage)
    monkeypatch.setattr(dbs_module, "build_system_metadata", dict)
    monkeypatch.setattr(dbs_module, "build_run_metadata", fake_run_metadata)
    monkeypatch.setattr(dbs_module, "start_metric_sampler", fake_sampler)

    with pytest.raises(KeyboardInterrupt):
        db.benchmark("clickbench", "select")

    assert db.restarts == 1
    assert storage.run["benchmark_profile"] == "resident-hot-v1"
    assert storage.run["status"] == "failed"
    assert captured_metadata["plan"] == {"query_names": ["a", "b", "c"], "iterations": 4}

    query_steps = [
        (step["query_name"], step["iteration"], step["iteration_role"], step.get("status"), step.get("result_status"))
        for step in storage.steps.values()
        if step["step_type"] == "query"
    ]
    assert query_steps[:6] == [
        ("a", 1, "warmup", "completed", "ok"),
        ("a", 2, "measured", "completed", "ok"),
        ("a", 3, "measured", "completed", "ok"),
        ("a", 4, "measured", "completed", "ok"),
        ("b", 1, "warmup", "completed", "ok"),
        ("b", 2, "measured", "failed", "error"),
    ]
    unstarted = query_steps[6:]
    assert [(name, iteration) for name, iteration, *_ in unstarted] == [
        ("b", 3),
        ("b", 4),
        ("c", 1),
        ("c", 2),
        ("c", 3),
        ("c", 4),
    ]
    assert {(status, result_status) for *_, status, result_status in unstarted} == {("completed", "skipped")}
    skipped = [step for step in storage.steps.values() if step.get("result_status") == "skipped"]
    assert all(str(step["error_message"]).startswith("never started: the run aborted") for step in skipped)

    metadata_update = cast(dict[str, dict[str, object]], storage.run["metadata_update"])
    assert metadata_update["queries"]["files"] == {
        "a": sha256(b"select 'a'").hexdigest(),
        "b": sha256(b"select 'b'").hexdigest(),
    }
