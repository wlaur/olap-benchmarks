from __future__ import annotations

import gzip
import json
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest
from pydantic import PrivateAttr

from ..settings import SUITE_SPECS, SuiteName, SuiteSpec, get_settings
from ..suites import BenchmarkSuite
from ..suites.json_representation.config import SUITE_NAME, JSONRepresentation, source_directory
from ..suites.jsonbench.config import get_jsonbench_input_files, iter_jsonbench_input_lines
from ..suites.parquet_analytics.contract import RepresentationManifest
from .test_parquet_analytics_contracts import MemoryDuckDB


class FixtureJSONRepresentation(JSONRepresentation["MemoryJSONDuckDB"]):
    def expected_table_row_counts(self) -> dict[str, int]:
        return {"json_raw": 40, "json_prepared": 40}


class MemoryJSONDuckDB(MemoryDuckDB):
    _phases: list[tuple[str, str | None]] = PrivateAttr(default_factory=list[tuple[str, str | None]])

    @contextmanager
    def phase_context(self, phase_name: str, table_name: str | None = None) -> Generator[None]:
        self._phases.append((phase_name, table_name))
        yield

    @property
    def benchmarks(self) -> dict[SuiteName, BenchmarkSuite[Any]]:
        return {SUITE_NAME: FixtureJSONRepresentation(db=self, scale_factor=10)}


def test_populate_retains_every_repaired_event_and_accounts_materialization(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    settings = get_settings()
    monkeypatch.setattr(settings, "input_data_directory", tmp_path / "input")
    monkeypatch.setattr(settings, "database_directory", tmp_path / "stores")
    monkeypatch.setattr(settings, "temporary_directory", tmp_path / "temporary")
    monkeypatch.setitem(SUITE_SPECS, SUITE_NAME, SuiteSpec("JSON Representation", (10,), 10, True))
    directory = source_directory(10)
    directory.mkdir(parents=True)
    for path in get_jsonbench_input_files(10):
        with gzip.open(path, "wt", encoding="utf-8") as output:
            output.writelines(
                [
                    (
                        '{"kind":"commit","did":"u","time_us":"9000",'
                        '"commit":{"operation":"create","collection":"app.bsky.feed.post"}}\n'
                    ),
                    '{"text":"first\n',
                    'second\\u0000"}\n',
                    '{"ok":true}\n',
                ]
            )
    expected = [json.loads(line) for path in get_jsonbench_input_files(10) for line in iter_jsonbench_input_lines(path)]
    db = MemoryJSONDuckDB()
    db._current_suite = SUITE_NAME
    db._current_suite_scale_factor = 10
    db._current_operation = "populate"
    suite = FixtureJSONRepresentation(db=db, scale_factor=10)
    try:
        suite.populate(restart=False)
        db.finalize_populate()
        db.verify_storage_readiness()
        actual = [json.loads(row) for row in db.fetch("SELECT j FROM json_raw").get_column("j")]
        assert actual == expected
        assert actual.count({}) == 10
        assert {"text": "first\nsecond"} in actual
        assert db.get_row_count("json_prepared") == len(expected)
        assert db.fetch("SELECT count(*) FROM json_prepared WHERE kind IS NULL").item() == 30
        assert ("repair_input", None) in db._phases
        assert ("materialize_fields", "json_prepared") in db._phases
        assert not (settings.temporary_directory / "duckdb/data/json_representation_sf10").exists()
        assert db.storage.external_files() == ()
        manifest = RepresentationManifest.read(db.database_directory)
        assert manifest.state == "ready"
        assert len(manifest.sources) == 10
        assert manifest.layout["timestamp_extrema"] == "lexical text before integer conversion; UTC milliseconds"
        db._phases.clear()
        suite.populate(restart=False)
        assert db._phases == [("verify_existing_data", None)]
        assert JSONRepresentation(db=db, scale_factor=10).expected_table_row_counts() == {
            "json_raw": 10_000_000,
            "json_prepared": 10_000_000,
        }
    finally:
        db.raw_connection().close()


def test_json_representation_rejects_other_scales() -> None:
    with pytest.raises(ValueError, match="SF10"):
        source_directory(1)
