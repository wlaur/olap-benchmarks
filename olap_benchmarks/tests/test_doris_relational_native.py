import math
import os
from collections.abc import Generator
from contextlib import contextmanager
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest

from ..dbs import Database
from ..dbs.doris import Doris, DorisTpcDs, DorisTpcH
from ..dbs.duckdb import DuckDB
from ..settings import REPO_ROOT, SuiteName, get_settings


@pytest.mark.live
def test_doris_native_relational_queries_and_statistics(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    selected = os.environ.get("OLAP_BENCHMARKS_RELATIONAL_FIXTURE_SUITE")
    if selected not in ("tpc_h", "tpc_ds"):
        pytest.skip("Parent must provision empty Doris and select RELATIONAL_FIXTURE_SUITE")
    suite: SuiteName = selected
    scale = 10 if suite == "tpc_h" else 1
    database = Doris()
    database._current_suite = suite
    database._current_suite_scale_factor = scale
    if database.get_table_names():
        pytest.fail("Relational fixture requires an empty scratch Doris database")

    @contextmanager
    def unmeasured_phase(_self: Database, phase_name: str, table_name: str | None = None) -> Generator[None]:
        yield

    def start_step(_self: Database, *args: object, **kwargs: object) -> int:
        return 1

    def finish_step(_self: Database, *args: object, **kwargs: object) -> None:
        pass

    monkeypatch.setattr(Database, "phase_context", unmeasured_phase)
    monkeypatch.setattr(Database, "_start_step", start_step)
    monkeypatch.setattr(Database, "_finish_step", finish_step)
    monkeypatch.setattr(get_settings(), "input_data_directory", tmp_path)
    inputs = tmp_path / f"{suite}_sf{scale}"
    inputs.mkdir()
    benchmark = database.benchmarks[suite]
    assert isinstance(benchmark, (DorisTpcH, DorisTpcDs))
    database.execute("SET query_timeout = 45")
    singleton = database.fetch("SELECT STDDEV_SAMP(CAST(1 AS DOUBLE)) AS deviation", method="python").item()
    print(f"Doris singleton sample deviation: {singleton!r}", flush=True)
    assert (
        database.fetch(
            "SELECT CASE WHEN COUNT(value) < 2 THEN NULL ELSE STDDEV_SAMP(value) END AS deviation "
            "FROM (SELECT CAST(1 AS DOUBLE) AS value) fixture",
            method="python",
        ).item()
        is None
    )
    reference_database = DuckDB()
    reference_database._current_suite = suite
    reference_database._current_suite_scale_factor = scale
    reference = reference_database.benchmarks[suite]
    with duckdb.connect(config={"threads": 2}) as connection:
        extension, generator = ("tpch", "dbgen") if suite == "tpc_h" else ("tpcds", "dsdgen")
        connection.execute(f"LOAD {extension}")
        connection.execute(f"CALL {generator}(sf=0.01)")
        if suite == "tpc_ds":
            connection.execute("ALTER TABLE store RENAME COLUMN s_tax_percentage TO s_tax_precentage")
        tables = [str(row[0]) for row in connection.execute("SHOW TABLES").fetchall()]
        connection.execute("CREATE SCHEMA fixture")
        connection.execute("SET schema = 'fixture'")
        connection.execute((REPO_ROOT / f"olap_benchmarks/suites/{suite}/schemas/duckdb.sql").read_text())
        for table in tables:
            columns = [str(row[0]) for row in connection.execute(f"DESCRIBE fixture.{table}").fetchall()]
            names = ", ".join(f'"{column}"' for column in columns)
            connection.execute(f"INSERT INTO fixture.{table} SELECT {names} FROM main.{table}")
        for table in tables:
            path = (inputs / f"{table}.parquet").as_posix().replace("'", "''")
            connection.execute(f"COPY {table} TO '{path}' (FORMAT PARQUET)")
        try:
            benchmark.populate(restart=False)
            database.finalize_populate()
            problems: list[str] = []
            for empty in (False, True):
                if empty:
                    for table in tables:
                        database.execute(f"TRUNCATE TABLE {table}")
                        connection.execute(f"DELETE FROM {table}")
                for name in benchmark.planned_query_names():
                    try:
                        expected = connection.execute(reference.load_query(name)).fetchall()
                        actual = database.fetch(benchmark.load_query(name), **benchmark.query_fetch_kwargs(name)).rows()
                        assert len(actual) == len(expected), (
                            suite,
                            name,
                            empty,
                            len(actual),
                            len(expected),
                            actual[:2],
                            expected[:2],
                        )
                        for left, right in zip(actual, expected, strict=True):
                            assert len(left) == len(right), f"{suite}:{name}:empty={empty}"
                            for value, canonical in zip(left, right, strict=True):
                                if value == canonical:
                                    continue
                                if isinstance(value, (int, float, Decimal)) and isinstance(
                                    canonical, (int, float, Decimal)
                                ):
                                    assert math.isclose(float(value), float(canonical), rel_tol=1e-12, abs_tol=1e-9), (
                                        suite,
                                        name,
                                        empty,
                                        value,
                                        canonical,
                                    )
                                else:
                                    assert value == canonical, (suite, name, empty, value, canonical)
                    except Exception as exc:
                        problems.append(f"{suite}:{name}:empty={empty}: {type(exc).__name__}: {exc}")
                        print(problems[-1], flush=True)
                        database.rollback()
            assert not problems, "\n".join(problems)
        finally:
            for table in reversed(tables):
                if table in database.get_table_names():
                    database.execute(f"DROP TABLE {table}")
            database.close_connection()
