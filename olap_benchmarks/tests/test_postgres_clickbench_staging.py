from collections.abc import Generator
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Literal, cast

import polars as pl
import pytest
from sqlalchemy import Connection, Engine

from ..dbs import postgres as postgres_module
from ..dbs.postgres import Postgres
from ..dbs.timescaledb import TimescaleDB
from ..settings import SuiteName, get_settings


def _copy_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "WatchID": pl.Series([9007199254740993, 2, 3, 4, 5, 6, 7], dtype=pl.Int64),
            "EventTime": pl.Series([datetime(2025, 1, 1, 0, 0, index) for index in range(7)], dtype=pl.Datetime("ms")),
            "Title": [
                "café🙂",
                'quoted "text"',
                "two\nlines",
                "",
                "carriage\rreturn",
                'multi\n"quoted"\nline',
                "seven",
            ],
            "value": pl.Series([1.25, None, -2.5, 0.0, 3.75, None, -8.0], dtype=pl.Float32),
        }
    )


class CopyConnection:
    def __init__(self) -> None:
        self.info: dict[str, object] = {}
        self.begun = False
        self.committed = False

    def begin(self) -> None:
        self.begun = True

    def commit(self) -> None:
        assert self.begun
        self.committed = True


class CopyEngine:
    def __init__(self) -> None:
        self.connections: list[CopyConnection] = []

    @contextmanager
    def connect(self) -> Generator[CopyConnection]:
        connection = CopyConnection()
        self.connections.append(connection)
        yield connection


class ParentConnection:
    def __init__(self, engine: CopyEngine) -> None:
        self.engine = engine


def _staging_database(
    database_type: type[Postgres], suite: SuiteName, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> tuple[Postgres, CopyEngine]:
    monkeypatch.setattr(get_settings(), "temporary_directory", tmp_path)
    monkeypatch.setattr(get_settings(), "benchmark_threads", 2)
    monkeypatch.setattr(postgres_module, "COPY_BATCH_ROWS", 3)
    database = database_type()
    database._current_suite = suite
    engine = CopyEngine()

    def connect(_self: Postgres, reconnect: bool = False) -> Connection:
        assert not reconnect
        return cast(Connection, ParentConnection(engine))

    monkeypatch.setattr(Postgres, "connect", connect)

    def exists(_connection: Connection, _table: str) -> bool:
        return True

    monkeypatch.setattr(postgres_module, "table_exists", exists)
    return database, engine


@pytest.mark.parametrize("database_type", [Postgres, TimescaleDB])
@pytest.mark.parametrize("suite", ["clickbench", "tpc_ds"])
@pytest.mark.parametrize("lazy", [False, True])
def test_native_copy_preserves_csv_records_types_and_all_rows_in_bounded_batches(
    database_type: type[Postgres], suite: SuiteName, lazy: bool, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    database, engine = _staging_database(database_type, suite, monkeypatch, tmp_path)
    frame = _copy_frame()
    copied: list[pl.DataFrame] = []
    files: list[Path] = []
    lock = Lock()
    database._push_active_step(42)

    def copy(_self: Postgres, connection: Connection, table: str, csv: Path, columns: list[str] | None) -> None:
        assert cast(CopyConnection, connection).begun
        assert table == "hits"
        assert columns is None
        assert database.active_step_id == 42
        chunk = pl.read_csv(csv, schema_overrides=frame.schema, try_parse_dates=True)
        assert chunk.height <= 2
        with lock:
            copied.append(chunk)
            files.append(csv)
            assert len(list(csv.parent.glob("*.csv"))) <= 2

    monkeypatch.setattr(Postgres, "_copy_csv_to_table", copy)
    database.insert(frame.lazy() if lazy else frame, "hits")
    actual = pl.concat(copied).sort("WatchID")
    assert actual.equals(frame.sort("WatchID"))
    assert actual.schema == frame.schema
    assert all(connection.committed for connection in engine.connections)
    assert len(set(files)) == len(files)
    assert not list(database._staging_directory.glob("*.csv"))
    assert database.active_step_id == 42


@pytest.mark.parametrize("database_type", [Postgres, TimescaleDB])
@pytest.mark.parametrize("failure", ["serialization", "copy"])
def test_native_copy_reclaims_files_and_preserves_worker_failure(
    database_type: type[Postgres],
    failure: Literal["serialization", "copy"],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    database, engine = _staging_database(database_type, "clickbench", monkeypatch, tmp_path)
    database._push_active_step(42)

    def fail_serialization(_self: pl.DataFrame, path: Path) -> None:
        path.write_text("partly serialized")
        raise OSError("CSV serialization failed")

    def fail_copy(_self: Postgres, _connection: Connection, _table: str, csv: Path, _columns: list[str] | None) -> None:
        assert csv.exists()
        raise RuntimeError("native COPY rejected the input")

    if failure == "serialization":
        monkeypatch.setattr(pl.DataFrame, "write_csv", fail_serialization)
    monkeypatch.setattr(Postgres, "_copy_csv_to_table", fail_copy)
    with pytest.raises((OSError, RuntimeError), match="CSV serialization failed|native COPY rejected the input"):
        database.insert(_copy_frame().lazy(), "hits")
    assert not any(connection.committed for connection in engine.connections)
    assert not list(database._staging_directory.glob("*.csv"))
    assert database.active_step_id == 42


def test_native_copy_cell_budget_bounds_wide_batches(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    database, _engine = _staging_database(Postgres, "clickbench", monkeypatch, tmp_path)
    monkeypatch.setattr(postgres_module, "COPY_BATCH_ROWS", 100)
    monkeypatch.setattr(postgres_module, "COPY_BATCH_MAX_CELLS", 8)
    seen: list[int] = []

    def copy_partition(_self: Postgres, _engine: Engine, _table: str, frame: pl.DataFrame, _step: int | None) -> None:
        seen.append(frame.height)

    monkeypatch.setattr(Postgres, "_copy_insert_partition", copy_partition)
    database.insert(_copy_frame().lazy(), "hits")
    assert sum(seen) == 7
    assert max(seen) == 1


@pytest.mark.parametrize("columns", [None, ["WatchID", "Title"]])
def test_native_csv_stream_preserves_embedded_carriage_returns(tmp_path: Path, columns: list[str] | None) -> None:
    contents = 'WatchID,Title\n1,"bare\rcarriage\r\npaired\nline"\n'
    csv_path = tmp_path / "records.csv"
    csv_path.write_bytes(contents.encode("utf-8"))
    observed: list[tuple[str, str]] = []

    class Cursor:
        def copy_expert(self, query: str, stream: object) -> None:
            from typing import TextIO

            observed.append((query, cast(TextIO, stream).read()))

    class RawConnection:
        def cursor(self) -> Cursor:
            return Cursor()

    class PoolConnection:
        dbapi_connection = RawConnection()

    class SqlConnection:
        connection = PoolConnection()

    database = Postgres()
    database._copy_csv_to_table(cast(Connection, SqlConnection()), "hits", csv_path, columns)
    assert observed[0][1] == contents
    expected_columns = ' ("WatchID", "Title")' if columns is not None else ""
    assert observed[0][0] == f"COPY hits{expected_columns} FROM STDIN WITH (FORMAT csv, HEADER true)"
