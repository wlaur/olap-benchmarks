from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from datetime import date, datetime
from gzip import open as gzip_open
from pathlib import Path
from typing import cast

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from ..dbs.duckdb import DuckDB
from ..dbs.duckdb.storage import Column, StorageMode, identifier, literal
from ..settings import Operation, SuiteName, get_settings
from ..suites import BenchmarkSuite
from ..suites.clickbench.config import CLICKBENCH_TIMESTAMP_COLUMNS
from ..suites.jsonbench.config import get_jsonbench_input_files


class FixtureDuckDB(DuckDB):
    @contextmanager
    def phase_context(self, phase_name: str, table_name: str | None = None) -> Generator[None]:
        yield


@pytest.fixture(params=["native", "parquet"])
def db(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Generator[FixtureDuckDB]:
    settings = get_settings()
    monkeypatch.setattr(settings, "duckdb_storage_mode", cast(StorageMode, request.param))
    monkeypatch.setattr(settings, "database_directory", tmp_path / "store's directory")
    monkeypatch.setattr(settings, "input_data_directory", tmp_path / "input's directory")
    monkeypatch.setattr(settings, "temporary_directory", tmp_path / "temp's directory")
    monkeypatch.setattr(settings, "benchmark_threads", 2)
    monkeypatch.setattr(settings, "benchmark_memory_gb", 1)
    monkeypatch.setattr(settings, "duckdb_async_threads", 2)
    monkeypatch.setattr(settings, "tables", ())
    database = FixtureDuckDB()
    database._current_suite = "kaggle_airbnb"
    database._current_suite_scale_factor = 1
    database._current_operation = "populate"
    database.input_directory.mkdir(parents=True)
    yield database
    database.close_connection()


def set_suite(db: DuckDB, suite: SuiteName, sf: int = 1) -> None:
    db.close_connection()
    db._current_suite = suite
    db._current_suite_scale_factor = sf
    db.input_directory.mkdir(parents=True, exist_ok=True)


def patch_counts(monkeypatch: pytest.MonkeyPatch, suite: BenchmarkSuite[DuckDB], expected: dict[str, int]) -> None:
    def counts(self: BenchmarkSuite[DuckDB]) -> dict[str, int]:
        return expected

    monkeypatch.setattr(type(suite), "expected_table_row_counts", counts)


def prepare_single(db: DuckDB, monkeypatch: pytest.MonkeyPatch, frame: pl.DataFrame) -> BenchmarkSuite[DuckDB]:
    path = db.input_directory / "fixture.parquet"
    frame.write_parquet(path)
    suite = db.benchmarks[db.current_suite]
    patch_counts(monkeypatch, suite, {"fixture": frame.height})
    db.insert_parquet(path, "fixture", not_null="id")
    db.finalize_populate()
    return suite


def test_modes_full_fetch_schema_nulls_empty_and_reopen(db: DuckDB, monkeypatch: pytest.MonkeyPatch) -> None:
    frame = pl.DataFrame(
        {"id": [1, 2, 2], "value": [1.5, None, 1.5], "day": [date(2024, 1, 1), None, date(2024, 1, 2)]},
        schema={"id": pl.Int32, "value": pl.Float32, "day": pl.Date},
    )
    suite = prepare_single(db, monkeypatch, frame)
    assert db.get_table_names() == {"fixture"}
    assert not suite.should_populate()
    assert_frame_equal(
        db.fetch("SELECT * FROM fixture ORDER BY id, day NULLS FIRST"), frame.sort("id", "day", nulls_last=False)
    )
    empty = db.fetch("SELECT * FROM fixture WHERE false")
    assert empty.shape == (0, 3)
    assert empty.schema == frame.schema
    db.connect(reconnect=True)
    db.verify_storage_readiness()
    assert db.fetch("SELECT count(*) AS n FROM fixture").item() == 3
    manifest = db.storage.read()
    assert manifest.state == "ready"
    assert manifest.mode == db.storage_mode
    assert manifest.relations["fixture"].columns[0].not_null
    assert manifest.build[0] == "v2.0.0"


def test_count_complete_unmanifested_store_is_rejected(db: DuckDB, monkeypatch: pytest.MonkeyPatch) -> None:
    db.execute("CREATE TABLE fixture (id INTEGER)")
    db.execute("INSERT INTO fixture VALUES (1)")
    suite = db.benchmarks[db.current_suite]
    patch_counts(monkeypatch, suite, {"fixture": 1})
    with pytest.raises(RuntimeError, match="manifest missing"):
        suite.should_populate()


def test_readiness_rejects_wrong_mode_build_incomplete_and_changed_catalog(
    db: DuckDB, monkeypatch: pytest.MonkeyPatch
) -> None:
    suite = prepare_single(db, monkeypatch, pl.DataFrame({"id": [1]}))
    original = db.storage.read()
    changed = original.model_copy(deep=True)
    changed.mode = "parquet" if db.storage_mode == "native" else "native"
    db.storage.write(changed)
    with pytest.raises(RuntimeError, match="identity mismatch"):
        suite.should_populate()
    changed = original.model_copy(deep=True)
    changed.build[1] = "old-build"
    db.storage.write(changed)
    with pytest.raises(RuntimeError, match="identity mismatch"):
        db.verify_storage_readiness()
    changed = original.model_copy(deep=True)
    changed.state = "building"
    db.storage.write(changed)
    with pytest.raises(RuntimeError, match="incomplete"):
        suite.should_populate()
    db.storage.write(original)
    kind = "TABLE" if db.storage_mode == "native" else "VIEW"
    db.execute(f"DROP {kind} fixture")
    db.execute("CREATE VIEW fixture AS SELECT 1::BIGINT AS id")
    with pytest.raises(RuntimeError, match="layout/schema mismatch"):
        suite.should_populate()


def test_source_identity_change_rejects_ready_store(db: DuckDB, monkeypatch: pytest.MonkeyPatch) -> None:
    prepare_single(db, monkeypatch, pl.DataFrame({"id": [1]}))
    path = db.input_directory / "fixture.parquet"
    pl.DataFrame({"id": [2]}).write_parquet(path)
    with pytest.raises(RuntimeError, match="source changed"):
        db.verify_storage_readiness()


def test_physical_catalog_with_same_counts_cannot_replace_view(db: DuckDB, monkeypatch: pytest.MonkeyPatch) -> None:
    if db.storage_mode != "parquet":
        return
    suite = prepare_single(db, monkeypatch, pl.DataFrame({"id": [1]}))
    db.execute("DROP VIEW fixture")
    db.execute("CREATE TABLE fixture AS SELECT 1::BIGINT AS id")
    with pytest.raises(RuntimeError, match="layout/schema mismatch"):
        suite.should_populate()


@pytest.mark.parametrize("operation", ["mutate", "concurrent"])
def test_parquet_rejects_write_operations_before_result_storage(db: DuckDB, operation: Operation) -> None:
    if db.storage_mode != "parquet":
        return
    with pytest.raises(ValueError, match="populate/select"):
        db.benchmark("chat_threads", operation)
    with pytest.raises(ValueError, match="read-only"):
        db.insert(pl.DataFrame({"id": [1]}), "fixture")
    with pytest.raises(ValueError, match="read-only"):
        db.upsert(pl.DataFrame({"id": [1]}), "fixture", "id")
    with pytest.raises(ValueError, match="read-only"):
        db.delete("fixture", "id", pl.DataFrame({"id": [1]}))


def test_native_dml_and_dirty_store_require_pristine_select(db: DuckDB, monkeypatch: pytest.MonkeyPatch) -> None:
    if db.storage_mode != "native":
        return
    prepare_single(db, monkeypatch, pl.DataFrame({"id": [1], "value": ["a"]}))
    db._current_operation = "mutate"
    db.before_benchmark_operation()
    db.insert(pl.DataFrame({"id": [2], "value": ["b"]}), "fixture")
    db.upsert(pl.DataFrame({"id": [1], "value": ["updated"]}), "fixture", "id")
    db.delete("fixture", "id", pl.DataFrame({"id": [2]}))
    assert db.fetch("SELECT * FROM fixture").rows() == [(1, "updated")]
    db.after_benchmark_operation()
    db._current_operation = "select"
    with pytest.raises(RuntimeError, match="mutated"):
        db.before_benchmark_operation()


@pytest.mark.parametrize("failure", ["null", "duplicate", "invalid_cast", "wrong_order"])
def test_parquet_constraints_and_strict_casts(db: DuckDB, failure: str) -> None:
    if db.storage_mode != "parquet":
        return
    db.storage.building()
    db.execute("CREATE TABLE fixture (id INTEGER PRIMARY KEY, payload JSON NOT NULL)")
    values: dict[str, list[object]] = {"id": [1, 2], "payload": ["{}", '{"a":1}']}
    if failure == "null":
        values["payload"][1] = None
    if failure == "duplicate":
        values["id"][1] = 1
    if failure == "invalid_cast":
        values["payload"][1] = "invalid json"
    frame = pl.DataFrame(values)
    if failure == "wrong_order":
        frame = frame.select("payload", "id")
    path = db.input_directory / "fixture.parquet"
    frame.write_parquet(path)
    with pytest.raises(
        Exception,
        match={
            "null": "NULL violates",
            "duplicate": "Duplicate",
            "invalid_cast": "Malformed JSON",
            "wrong_order": "column order",
        }[failure],
    ):
        db.insert_parquet(path, "fixture")
    assert db.storage.read().state == "building"
    assert db.get_row_count("fixture") == 0


def test_dynamic_quoted_columns_and_path(db: DuckDB) -> None:
    path = db.input_directory / "quote's file.parquet"
    frame = pl.DataFrame({'a"b': [1], "empty": [None]}, schema={'a"b': pl.Int16, "empty": pl.Float32})
    frame.write_parquet(path)
    db.insert_parquet(path, "fixture")
    assert_frame_equal(db.fetch("SELECT * FROM fixture"), frame)


def test_storage_accounting_external_aliases_owned_and_initial_sources(
    db: DuckDB, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from ..dbs.duckdb import storage

    source = db.input_directory / "fixture.parquet"
    pl.DataFrame({"id": [1]}).write_parquet(source)

    def planned(database: DuckDB) -> dict[str, Path]:
        return {"fixture": source}

    monkeypatch.setattr(storage, "planned_sources", planned)
    before = db.metric_directories
    if db.storage_mode == "native":
        assert before == (db.database_directory.resolve(),)
        return
    assert before == (db.database_directory.resolve(), source)
    db.insert_parquet(source, "fixture")
    alias = tmp_path / "alias.parquet"
    alias.symlink_to(source)
    db.insert_parquet(alias, "alias")
    hardlink = tmp_path / "z-hardlink.parquet"
    hardlink.hardlink_to(source)
    db.insert_parquet(hardlink, "hardlink")
    assert db.metric_directories == before
    owned = db.database_directory / "owned.parquet"
    pl.DataFrame({"id": [1]}).write_parquet(owned)
    db.insert_parquet(owned, "owned")
    assert db.metric_directories == before
    options = db.run_options
    accounting = cast(dict[str, object], options["storage_accounting"])
    assert accounting["version"] == 1
    assert accounting["required_referenced_storage_paths"] == [source.as_posix()]
    assert accounting["required_referenced_storage_mb_before_operation"] is not None


def test_jsonbench_raw_repaired_json_roundtrip_and_owned_accounting(
    db: DuckDB, monkeypatch: pytest.MonkeyPatch
) -> None:
    set_suite(db, "jsonbench", 10)
    raw = '{ "dup": 1, "dup": 2, "n":1e+03, "null":null, "escape":"\\u0041\\u0000" }'
    split = '{"split":"first\nsecond"}\n'
    sources = get_jsonbench_input_files(10)
    for i, source in enumerate(sources):
        with gzip_open(source, "wt") as output:
            output.write(raw + "\n" if i == 0 else "{}\n")
            if i == 0:
                output.write(split)
    patch_counts(monkeypatch, db.benchmarks["jsonbench"], {"bluesky": 12})
    suite = db.benchmarks["jsonbench"]
    suite.populate()
    db.finalize_populate()
    db.verify_storage_readiness()
    rows = db.fetch("SELECT CAST(j AS VARCHAR) AS j FROM bluesky").get_column("j").to_list()
    assert raw.replace("\\u0000", "") in rows
    assert '{"split":"first\\nsecond"}' in rows
    assert rows.count("{}") == 10
    assert db.fetch("SELECT json_extract(j, '$.missing') IS NULL AS missing FROM bluesky").get_column("missing").all()
    assert db.storage.columns("bluesky")[0].dtype == "JSON"
    assert db.metric_directories == (db.database_directory.resolve(),)
    if db.storage_mode == "parquet":
        assert pl.read_parquet(db.database_directory / "bluesky.parquet")["j"].to_list() == rows
        assert db.storage.read().relations["bluesky"].parquet_metadata
    assert not (get_settings().temporary_directory / "duckdb/data/jsonbench_sf10").exists()


def canonical_fixture(db: DuckDB, table: str, *, clickbench: bool = False) -> None:
    columns = db.storage.columns(table)
    expressions: list[str] = []
    for col in columns:
        if clickbench and col.name in CLICKBENCH_TIMESTAMP_COLUMNS:
            expr = "CAST(-1 AS BIGINT)"
        elif clickbench and col.name == "EventDate":
            expr = "CAST(-1 AS INTEGER)"
        elif col.dtype == "DATE":
            expr = "TIMESTAMP '1980-02-03 23:59:59'" if col.name == "birthday" else "DATE '2024-01-01'"
        elif col.dtype.startswith("TIMESTAMP"):
            expr = "TIMESTAMP '2024-01-01 00:00:00'"
        elif col.dtype == "JSON":
            expr = literal('{ "k": null }')
        elif col.dtype == "VARCHAR":
            expr = "'fixture'"
        elif col.dtype == "BOOLEAN":
            expr = "true"
        else:
            expr = f"CAST(1 AS {col.dtype})"
        expressions.append(f"{expr} AS {identifier(col.name)}")
    db.execute(
        f"COPY (SELECT {', '.join(expressions)}) TO "
        f"{literal((db.input_directory / f'{table}.parquet').as_posix())} (FORMAT PARQUET)"
    )


@pytest.mark.parametrize(
    ("suite", "sf", "tables_count"),
    [
        ("rtabench", 1, 5),
        ("clickbench", 1, 1),
        ("chat_threads", 1, 2),
        ("tpc_h", 10, 8),
        ("tpc_ds", 1, 24),
    ],
)
def test_all_canonical_ddl_columns_types_constraints_and_real_load_hooks(
    db: DuckDB, monkeypatch: pytest.MonkeyPatch, suite: SuiteName, sf: int, tables_count: int
) -> None:
    set_suite(db, suite, sf)
    db.initialize_schema(suite)
    tables = sorted(db.get_table_names())
    expected_columns = {table: db.storage.columns(table) for table in tables}
    assert len(tables) == tables_count
    if suite == "clickbench":
        assert len(expected_columns["hits"]) == 105
    for table in tables:
        canonical_fixture(db, table, clickbench=suite == "clickbench")
    # Start population on a new catalog, retaining only the tiny prepared fixtures.
    db.close_connection()
    for file in db.database_directory.iterdir():
        file.unlink()
    benchmark = db.benchmarks[suite]
    patch_counts(monkeypatch, benchmark, dict.fromkeys(tables, 1))
    benchmark.populate()
    db.finalize_populate()
    db.verify_storage_readiness()
    for table in tables:
        recorded = db.storage.read().relations[table].columns
        assert recorded == expected_columns[table]
        assert db.get_row_count(table) == 1
        assert db.fetch(f"SELECT * FROM {identifier(table)}").width == len(recorded)
    if suite == "rtabench":
        assert db.fetch("SELECT birthday FROM customers").item() == date(1980, 2, 3)
        assert db.fetch("SELECT event_payload FROM order_events").item() == '{ "k": null }'
    if suite == "clickbench":
        assert db.fetch("SELECT EventTime, EventDate FROM hits").row(0) == (
            datetime(1969, 12, 31, 23, 59, 59),
            date(1969, 12, 31),
        )


@pytest.mark.parametrize("columns", [5000, 4094])
def test_full_dynamic_wide_shape_and_null_types(db: DuckDB, columns: int) -> None:
    frame = pl.DataFrame(
        {"time": [datetime(2024, 1, 1)], **{f"metric_{i:04d}": [None] for i in range(columns)}},
        schema={"time": pl.Datetime("ms"), **{f"metric_{i:04d}": pl.Float32 for i in range(columns)}},
    )
    path = db.input_directory / "wide.parquet"
    frame.write_parquet(path)
    db.insert_parquet(path, "wide", not_null="time")
    actual = db.fetch("SELECT * FROM wide")
    assert actual.width == columns + 1
    assert actual["time"].dtype == pl.Datetime("us")
    assert all(dtype == pl.Float32 for name, dtype in actual.schema.items() if name != "time")
    assert actual["metric_0000"].null_count() == 1


def test_empty_typed_input_and_runtime_operation_metadata(db: DuckDB, monkeypatch: pytest.MonkeyPatch) -> None:
    frame = pl.DataFrame(schema={"id": pl.Int32, "value": pl.Float32})
    suite = prepare_single(db, monkeypatch, frame)
    assert not suite.should_populate()
    assert_frame_equal(db.fetch("SELECT * FROM fixture"), frame)
    db._current_operation = "select"
    options = dict(db.run_options)
    db.before_benchmark_operation()
    db.fetch("SELECT * FROM fixture")
    db.after_benchmark_operation()
    finished = cast(dict[str, object], db._executed_query_metadata()["options"])
    assert finished["storage_accounting"] == options["storage_accounting"]
    runtime = cast(dict[str, object], finished["duckdb_runtime"])
    assert runtime["before"] == runtime["after"]
    assert runtime["before"] == db.runtime_metadata()


def test_airbnb_path_hook_preserves_dynamic_dates_booleans_nulls_and_arrays(db: DuckDB) -> None:
    from ..suites.kaggle_airbnb.config import KAGGLE_AIRBNB_TABLES

    frame = pl.DataFrame(
        {"id": [1, 1], "day": [date(2024, 1, 1), None], "available": [True, None], "text": ["a", "a"]},
        schema={"id": pl.Int32, "day": pl.Date, "available": pl.Boolean, "text": pl.String},
    )
    for table in KAGGLE_AIRBNB_TABLES:
        frame.write_parquet(db.input_directory / f"{table}.parquet")
    suite = db.benchmarks["kaggle_airbnb"]
    suite.populate()
    db.finalize_populate()
    db.verify_storage_readiness()
    assert len(db.storage.read().relations) == 6
    for table in KAGGLE_AIRBNB_TABLES:
        assert_frame_equal(
            db.fetch(f"SELECT * FROM {table} ORDER BY day NULLS FIRST"), frame.sort("day", nulls_last=False)
        )
    assert db.fetch("SELECT array_agg(text ORDER BY day) AS texts FROM reviews").item().to_list() == ["a", "a"]


def test_external_missing_broken_or_directory_reference_is_rejected(db: DuckDB, tmp_path: Path) -> None:
    if db.storage_mode != "parquet":
        return
    missing = tmp_path / "missing.parquet"
    for path in (missing, tmp_path / "broken.parquet"):
        if path != missing:
            path.symlink_to(missing)
        with pytest.raises(FileNotFoundError):
            db.insert_parquet(path, "fixture")
    with pytest.raises(ValueError, match="not a regular file"):
        db.storage.register(
            "fixture", tmp_path, [Column(name="id", dtype="INTEGER", not_null=False, primary_key=False)], "id"
        )


def test_canonical_integer_overflow_is_rejected_on_population(db: DuckDB) -> None:
    db.storage.building()
    db.execute("CREATE TABLE fixture (id BIGINT)")
    path = db.input_directory / "fixture.parquet"
    pl.DataFrame({"id": [2**64 - 1]}, schema={"id": pl.UInt64}).write_parquet(path)
    with pytest.raises(Exception, match="out of range"):
        db.insert_parquet(path, "fixture")
    db.rollback()
    assert not db.storage.read().relations


def test_readback_failure_retains_effective_after_settings(db: DuckDB, monkeypatch: pytest.MonkeyPatch) -> None:
    prepare_single(db, monkeypatch, pl.DataFrame({"id": [1]}))
    db._current_operation = "select"
    _ = db.run_options
    db.before_benchmark_operation()
    db.execute("SET threads=1")
    with pytest.raises(RuntimeError, match="setting threads"):
        db.after_benchmark_operation()
    options = cast(dict[str, object], db._executed_query_metadata()["options"])
    runtime = cast(dict[str, object], options["duckdb_runtime"])
    after = cast(dict[str, object], runtime["after"])
    assert cast(dict[str, str], after["settings"])["threads"] == "1"
