from __future__ import annotations

from pathlib import Path
from typing import cast

import pytest

from ..dbs import polars as polars_db
from ..dbs.polars import Polars
from ..settings import SuiteName, get_settings
from ..suites.tpc_h.config import TPCH_TABLES


@pytest.fixture
def storage_db(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Polars:
    monkeypatch.setattr(get_settings(), "database_directory", tmp_path / "dbs")
    monkeypatch.setattr(get_settings(), "input_data_directory", tmp_path / "inputs")
    monkeypatch.setattr(get_settings(), "tables", ("wide100",))
    db = Polars()
    db._current_suite = "clickbench"
    db._current_suite_scale_factor = 1
    db._current_operation = "populate"
    return db


@pytest.mark.parametrize(
    ("suite", "scale_factor", "tables"),
    [
        ("clickbench", 1, ("hits",)),
        ("tpc_h", 10, TPCH_TABLES),
        ("tpc_h", 50, TPCH_TABLES),
        ("time_series_metrics", 1, ("wide100",)),
        ("time_series_ranges", 1, ("wide100",)),
        ("jsonbench", 10, ()),
    ],
)
def test_population_paths_are_ready_before_registration(
    storage_db: Polars, suite: SuiteName, scale_factor: int, tables: tuple[str, ...]
) -> None:
    storage_db._current_suite = suite
    storage_db._current_suite_scale_factor = scale_factor
    storage_db.input_directory.mkdir(parents=True)
    sources = tuple(storage_db.input_directory / f"{table}.parquet" for table in tables)
    for source in sources:
        source.touch()
    (storage_db.input_directory / "unused.parquet").touch()

    sampler_paths = storage_db.metric_directories

    assert sampler_paths == (storage_db.database_directory.resolve(), *sorted(sources))
    assert not tuple(storage_db.database_directory.glob("*.parquet"))
    for table, source in zip(tables, sources, strict=True):
        storage_db.register_table(table, source)
    assert storage_db.metric_directories == sampler_paths


def test_selection_counts_retained_links_and_uses_their_actual_targets(storage_db: Polars, tmp_path: Path) -> None:
    storage_db._current_suite = "time_series_metrics"
    storage_db._current_operation = "select"
    old_source = tmp_path / "old-wide100.parquet"
    unselected_source = tmp_path / "old-eav100.parquet"
    old_source.touch()
    unselected_source.touch()
    storage_db.register_table("wide100", old_source)
    storage_db.register_table("eav100", unselected_source)

    assert storage_db.metric_directories == (
        storage_db.database_directory.resolve(),
        *sorted((old_source, unselected_source)),
    )
    storage_db._current_operation = "populate"
    assert storage_db.metric_directories[1:] == tuple(sorted((old_source, unselected_source)))


def test_select_without_a_store_does_not_claim_unregistered_inputs(storage_db: Polars) -> None:
    storage_db._current_operation = "select"
    storage_db.input_directory.mkdir(parents=True)
    (storage_db.input_directory / "hits.parquet").touch()

    assert storage_db.metric_directories == (storage_db.database_directory.resolve(),)


def test_external_aliases_are_resolved_and_counted_once(storage_db: Polars, tmp_path: Path) -> None:
    source = tmp_path / "external.parquet"
    source.touch()
    alias = tmp_path / "alias.parquet"
    alias.symlink_to(source.name)
    storage_db.register_table("hits", alias)
    storage_db.table_path("same_source").symlink_to("hits.parquet")

    assert storage_db.metric_directories == (storage_db.database_directory.resolve(), source)


def test_internal_targets_are_already_in_the_store_walk(storage_db: Polars) -> None:
    owned = storage_db.table_path("owned")
    owned.touch()
    storage_db.table_path("hits").symlink_to(owned.name)
    storage_db.table_path("alias").symlink_to("hits.parquet")

    assert storage_db.metric_directories == (storage_db.database_directory.resolve(),)


def test_existing_owned_table_does_not_add_the_prepared_source(storage_db: Polars) -> None:
    storage_db.table_path("hits").touch()

    assert storage_db.metric_directories == (storage_db.database_directory.resolve(),)


def test_jsonbench_counts_its_imported_parquet_without_the_staging_inputs(storage_db: Polars) -> None:
    storage_db._current_suite = "jsonbench"
    storage_db._current_suite_scale_factor = 10
    storage_db.input_directory.mkdir(parents=True)
    (storage_db.input_directory / "file_0001.json.gz").touch()
    paths_before_import = storage_db.metric_directories
    storage_db.table_path("bluesky").touch()

    assert paths_before_import == (storage_db.database_directory.resolve(),)
    assert storage_db.metric_directories == paths_before_import


def test_missing_prepared_source_raises_before_registration(storage_db: Polars) -> None:
    with pytest.raises(FileNotFoundError):
        _ = storage_db.metric_directories
    assert not storage_db.table_path("hits").is_symlink()


def test_dangling_store_link_does_not_fall_back_to_prepared_input(storage_db: Polars, tmp_path: Path) -> None:
    storage_db.input_directory.mkdir(parents=True)
    (storage_db.input_directory / "hits.parquet").touch()
    storage_db.table_path("hits").symlink_to(tmp_path / "missing.parquet")

    with pytest.raises(FileNotFoundError):
        _ = storage_db.metric_directories


def test_directory_reference_raises(storage_db: Polars, tmp_path: Path) -> None:
    storage_db.table_path("hits").symlink_to(tmp_path, target_is_directory=True)

    with pytest.raises(ValueError, match="not a regular file"):
        _ = storage_db.metric_directories


def test_provenance_separates_referenced_storage_from_the_imported_store(
    storage_db: Polars, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "required.parquet"
    source.touch()
    storage_db.register_table("hits", source)
    measured: list[tuple[Path, ...]] = []

    def measure(paths: tuple[Path, ...]) -> int:
        measured.append(paths)
        return 37

    monkeypatch.setattr(polars_db, "get_paths_size_mb", measure)

    options = storage_db.run_options
    accounting = cast(dict[str, object], options["storage_accounting"])

    assert options["note"] == get_settings().run_note
    assert accounting["version"] == 1
    assert accounting["additional_imported_store_path"] == storage_db.database_directory.as_posix()
    assert accounting["required_referenced_storage_paths"] == [source.as_posix()]
    assert accounting["required_referenced_storage_mb_before_operation"] == 37
    assert measured == [(source,)]


def test_reference_measurement_failure_is_not_recorded_as_zero(
    storage_db: Polars, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "required.parquet"
    source.touch()
    storage_db.register_table("hits", source)

    def fail(_paths: tuple[Path, ...]) -> int:
        raise RuntimeError("du failed")

    monkeypatch.setattr(polars_db, "get_paths_size_mb", fail)

    with pytest.raises(RuntimeError, match="du failed"):
        _ = storage_db.run_options
