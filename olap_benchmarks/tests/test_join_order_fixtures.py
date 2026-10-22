from __future__ import annotations

import csv
import io
import os
import sqlite3
import tarfile
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import cast

import duckdb
import polars as pl
import pytest

from ..dbs import Database, get_databases
from ..dbs.duckdb import DuckDB
from ..settings import get_settings
from ..suites.join_order import config as job_config
from ..suites.join_order import prepare
from ..suites.join_order.config import CANDIDATE_DATABASES, SUITE_NAME, JoinOrder
from ..suites.join_order.prepare import (
    DatasetManifest,
    SnapshotPin,
    fingerprint,
    prepare_snapshot,
    verify_prepared_data,
)
from ..suites.join_order.schema import DIRECTORY, JOB_QUERY_NAMES, JOB_SCHEMA, JOB_TABLES
from .test_join_order_prepare import write_archive

type Value = str | int | None
type Fixture = dict[str, list[dict[str, Value]]]


def adversarial_fixture() -> Fixture:
    tables: Fixture = {table: [] for table in JOB_TABLES}

    def add(table: str, **values: Value) -> None:
        row: dict[str, Value] = {
            column.name: None if column.nullable else (1 if column.sql_type == "integer" else "fixture")
            for column in JOB_SCHEMA[table]
        }
        assert set(values) <= row.keys()
        row.update(values)
        tables[table].append(row)

    for identifier, text in enumerate(
        ("top 250 rank", "rating", "countries", "genres", "release dates", "budget", "votes", "mini biography"),
        start=1,
    ):
        add("info_type", id=identifier, info=text)
    for identifier, kind in enumerate(("movie", "tv series", "episode"), start=1):
        add("kind_type", id=identifier, kind=kind)
    for identifier, kind in enumerate(("production companies", "distributors"), start=1):
        add("company_type", id=identifier, kind=kind)
    for identifier, kind in enumerate(("cast", "complete", "crew", "complete+verified"), start=1):
        add("comp_cast_type", id=identifier, kind=kind)
    for identifier, role in enumerate(("actor", "actress", "producer"), start=1):
        add("role_type", id=identifier, role=role)
    for identifier, link in enumerate(("follows", "features", "sequel", "followed by"), start=1):
        add("link_type", id=identifier, link=link)
    for identifier, keyword in enumerate(
        (
            "character-name-in-title",
            "sequel",
            "marvel-cinematic-universe",
            "murder",
            "10,000-mile-club",
            "computer-hacker",
        ),
        start=1,
    ):
        add("keyword", id=identifier, keyword=keyword)
    for identifier, (name, country) in enumerate(
        (
            ("Zeta Film", "[de]"),
            ("Alpha Warner", "[us]"),
            ("Unknown", None),
            ("Empty", ""),
            ("20th Century Fox", "[us]"),
            ("Japan Studio", "[jp]"),
        ),
        start=1,
    ):
        add("company_name", id=identifier, name=name, country_code=country)
    for identifier, (title, year) in enumerate(
        (
            ("Zulu", 2007),
            ("Alpha", 2012),
            ("Beta", 1990),
            ("Gamma", 2008),
            ("", 2008),
            ("omega", 2011),
            ("Boundary", 2005),
            ("Orphan", 2012),
            ("A", None),
            ("é", 2010),
        ),
        start=1,
    ):
        add("title", id=identifier, title=title, production_year=year, kind_id=1, episode_nr=50)
    add("aka_title", id=1, movie_id=1, title="Alternate Zulu", kind_id=1)
    for identifier, (movie, company, note) in enumerate(
        (
            (1, 1, "(presents)"),
            (2, 2, "(co-production)"),
            (3, 2, "(presents)"),
            (4, 1, ""),
            (5, 2, "(presents)"),
            (6, 2, "(presents) (as Metro-Goldwyn-Mayer Pictures)"),
            (1, 1, "(presents)"),
            (3, 2, None),
            (9, 2, "(presents)"),
            (10, 3, "(worldwide)"),
        ),
        start=1,
    ):
        add("movie_companies", id=identifier, movie_id=movie, company_id=company, company_type_id=1, note=note)
    for movie in (1, 2, 3, 4, 5, 6, 9):
        add("movie_info_idx", id=movie, movie_id=movie, info_type_id=1, info="100")
    for identifier, (movie, rating) in enumerate(((1, "9.0"), (2, "10.0"), (3, "8.0"), (4, "5.0")), start=20):
        add("movie_info_idx", id=identifier, movie_id=movie, info_type_id=2, info=rating)
    for identifier, (movie, keyword) in enumerate(
        ((1, 1), (1, 2), (1, 5), (1, 1), (2, 1), (2, 2), (2, 3), (3, 2), (4, 2), (6, 4), (8, 3)),
        start=1,
    ):
        add("movie_keyword", id=identifier, movie_id=movie, keyword_id=keyword)
    for movie in (1, 2, 3, 4):
        add("movie_info", id=movie, movie_id=movie, info_type_id=3, info="Sweden")
        add("movie_info", id=movie + 10, movie_id=movie, info_type_id=4, info="Drama")
        add("movie_info", id=movie + 20, movie_id=movie, info_type_id=5, info="USA: 2007", note="internet")
    add("movie_info", id=40, movie_id=1, info_type_id=6, info="$100")
    add("movie_info_idx", id=40, movie_id=1, info_type_id=7, info="1000")
    for identifier, (name, gender, pcode) in enumerate(
        (("Downey, Robert", "m", "D"), ("Zed B", "m", "F"), ("Angel, Beth", "f", "A"), ("Tim, Brian", "m", "C")),
        start=1,
    ):
        add("name", id=identifier, name=name, gender=gender, name_pcode_cf=pcode)
        add("aka_name", id=identifier, person_id=identifier, name=f"alias {name}")
    add("aka_name", id=5, person_id=1, name="")
    add("char_name", id=1, name="Tony Stark")
    add("char_name", id=2, name="Sherlock Holmes")
    for identifier, (movie, person, note) in enumerate(
        (
            (2, 1, "(voice)"),
            (2, 1, "(voice)"),
            (1, 2, None),
            (3, 2, "(voice) (uncredited)"),
            (1, 3, "(voice)"),
            (1, 4, "(producer)"),
            (8, 1, "(voice)"),
        ),
        start=1,
    ):
        add("cast_info", id=identifier, movie_id=movie, person_id=person, person_role_id=1, role_id=2, note=note)
    add("person_info", id=1, person_id=2, info_type_id=8, info="A biography", note="Volker Boehm")
    add("person_info", id=2, person_id=4, info_type_id=8, info="5' 9\"; path \\N; café\nsecond line", note="")
    add("complete_cast", id=1, movie_id=1, subject_id=1, status_id=2)
    add("complete_cast", id=2, movie_id=None, subject_id=1, status_id=2)
    add("movie_link", id=1, movie_id=1, linked_movie_id=2, link_type_id=1)
    add("movie_link", id=2, movie_id=2, linked_movie_id=1, link_type_id=3)
    add("movie_link", id=3, movie_id=3, linked_movie_id=3, link_type_id=2)
    add("movie_link", id=4, movie_id=1, linked_movie_id=2, link_type_id=1)
    return tables


def write_fixture_archive(path: Path) -> SnapshotPin:
    entries: list[tuple[str, bytes, bytes]] = []
    for table, rows in adversarial_fixture().items():
        stream = io.StringIO(newline="")
        writer = csv.writer(stream, quoting=csv.QUOTE_NOTNULL, doublequote=False, escapechar="\\", lineterminator="\n")
        writer.writerows(row.values() for row in rows)
        entries.append((f"{table}.csv", stream.getvalue().encode(), tarfile.REGTYPE))
    write_archive(path, entries)
    digest = fingerprint(path)
    return SnapshotPin(digest["sha256"], digest["bytes"])


def test_adversarial_fixture_roundtrips_all_columns_nulls_and_duplicate_rows(tmp_path: Path) -> None:
    archive = tmp_path / "fixture.tgz"
    pin = write_fixture_archive(archive)
    output = tmp_path / "join_order"
    prepare_snapshot(output, pin, archive=archive)
    manifest = verify_prepared_data(output, pin)
    for table, expected in adversarial_fixture().items():
        assert pl.read_parquet(output / f"{table}.parquet").to_dicts() == expected, table
        assert manifest["tables"][table]["rows"] == len(expected)


def test_fixture_has_original_primary_keys_and_adversarial_nullable_duplicate_values() -> None:
    fixture = adversarial_fixture()
    assert set(fixture) == set(JOB_TABLES)
    for table, rows in fixture.items():
        assert rows
        assert len({row["id"] for row in rows}) == len(rows)
        for row in rows:
            assert list(row) == [column.name for column in JOB_SCHEMA[table]]
            assert all(row[column.name] is not None for column in JOB_SCHEMA[table] if not column.nullable)
    assert {row["note"] for row in fixture["movie_companies"]} >= {None, "", "(presents)"}
    keys = [(row["movie_id"], row["keyword_id"]) for row in fixture["movie_keyword"]]
    assert len(set(keys)) < len(keys)
    assert any(row["production_year"] is None for row in fixture["title"])
    assert {row["info"] for row in fixture["movie_info_idx"]} >= {"9.0", "10.0"}


@contextmanager
def fixture_connections(empty: bool) -> Generator[tuple[duckdb.DuckDBPyConnection, sqlite3.Connection]]:
    with duckdb.connect(":memory:") as database, sqlite3.connect(":memory:") as reference:
        schema = (DIRECTORY / "upstream/schema.sql").read_text()
        database.execute(schema)
        reference.executescript(schema)
        reference.execute("PRAGMA case_sensitive_like=ON")
        if not empty:
            for table, rows in adversarial_fixture().items():
                statement = f"INSERT INTO {table} VALUES ({', '.join('?' for _ in JOB_SCHEMA[table])})"
                records = [tuple(row.values()) for row in rows]
                database.executemany(statement, records)
                reference.executemany(statement, records)
        yield database, reference


@pytest.mark.live
@pytest.mark.parametrize("empty", [False, True])
def test_all_113_queries_match_independent_fixture_oracle(empty: bool) -> None:
    benchmark = JoinOrder(db=DuckDB())
    with fixture_connections(empty) as (database, reference):
        for name in JOB_QUERY_NAMES:
            sql = (DIRECTORY / "queries" / f"{name}.sql").read_text()
            expected = cast(list[tuple[Value, ...]], reference.execute(sql).fetchall())
            actual = cast(list[tuple[Value, ...]], database.execute(benchmark.load_query(name)).fetchall())
            assert actual == expected, name
            assert len(actual) == 1, name
            if empty:
                assert all(value is None for value in actual[0]), name


@pytest.mark.live
def test_fixture_goldens_cover_independent_minima_lexical_ratings_and_duplicate_join_fanout() -> None:
    expected: dict[str, tuple[Value, ...]] = {
        "1a": ("(co-production)", "", 1990),
        "2a": ("Zulu",),
        "4a": ("9.0", "Zulu"),
        "6a": ("marvel-cinematic-universe", "Downey, Robert", "Alpha"),
        "32a": ("follows", "Zulu", "Alpha"),
    }
    with fixture_connections(empty=False) as (database, reference):
        for name, answer in expected.items():
            sql = (DIRECTORY / "queries" / f"{name}.sql").read_text()
            assert reference.execute(sql).fetchall() == [answer], name
            assert database.execute(sql).fetchall() == [answer], name
        sql = """
            SELECT COUNT(*) FROM title t, movie_companies mc, movie_keyword mk
            WHERE t.id = 1 AND t.id = mc.movie_id AND t.id = mk.movie_id AND mk.keyword_id = 1
        """
        assert reference.execute(sql).fetchone() == (4,)
        assert database.execute(sql).fetchone() == (4,)


@pytest.mark.live
def test_existing_engine_import_and_all_query_answers(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    selected = os.environ.get("OLAP_BENCHMARKS_JOB_FIXTURE_DATABASE")
    if selected is None:
        pytest.skip("Parent must select and prepare one isolated engine with OLAP_BENCHMARKS_JOB_FIXTURE_DATABASE")
    assert selected in CANDIDATE_DATABASES
    database = get_databases()[selected]
    database._current_suite = SUITE_NAME
    database._current_suite_scale_factor = 1
    if database.get_table_names():
        pytest.fail("JOB fixture gate requires an empty scratch database; refusing to replace existing tables")
    monkeypatch.setattr(get_settings(), "input_data_directory", tmp_path)
    monkeypatch.setattr(prepare, "PREPARE_FREE_SPACE_BYTES", 0)

    @contextmanager
    def unmeasured_phase(_self: Database, phase_name: str, table_name: str | None = None) -> Generator[None]:
        yield

    monkeypatch.setattr(Database, "phase_context", unmeasured_phase)
    archive = tmp_path / "fixture.tgz"
    pin = write_fixture_archive(archive)
    prepare_snapshot(tmp_path / "join_order", pin, archive=archive)

    def verify_fixture(directory: Path) -> DatasetManifest:
        return verify_prepared_data(directory, pin)

    monkeypatch.setattr(job_config, "verify_prepared_data", verify_fixture)
    benchmark = cast(JoinOrder[Database], database.benchmarks[SUITE_NAME])
    try:
        benchmark.populate(restart=False)
        database.finalize_populate()
        with sqlite3.connect(":memory:") as reference:
            reference.executescript((DIRECTORY / "upstream/schema.sql").read_text())
            reference.execute("PRAGMA case_sensitive_like=ON")
            for table, rows in adversarial_fixture().items():
                statement = f"INSERT INTO {table} VALUES ({', '.join('?' for _ in JOB_SCHEMA[table])})"
                reference.executemany(statement, [tuple(row.values()) for row in rows])
            for empty in (False, True):
                if empty:
                    for table in JOB_TABLES:
                        database.execute(f"TRUNCATE TABLE {table}")
                        reference.execute(f"DELETE FROM {table}")
                for name in JOB_QUERY_NAMES:
                    sql = benchmark.load_query(name)
                    expected = reference.execute(sql).fetchall()
                    actual = database.fetch(sql, **benchmark.query_fetch_kwargs(name))
                    assert actual.rows() == expected, f"{selected}:{name}:empty={empty}"
    finally:
        for table in reversed(JOB_TABLES):
            if table in database.get_table_names():
                database.execute(f"DROP TABLE {table}")
        database.close_connection()
