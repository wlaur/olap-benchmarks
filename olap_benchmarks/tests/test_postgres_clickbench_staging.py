from datetime import datetime
from pathlib import Path
from subprocess import CalledProcessError, CompletedProcess
from typing import Literal, cast

import polars as pl
import pytest
from sqlalchemy import Connection

from ..dbs import postgres as postgres_module
from ..dbs.postgres import Postgres
from ..dbs.timescaledb import TimescaleDB
from ..settings import SuiteName, get_settings


def _clickbench_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "WatchID": pl.Series([9007199254740993, 2, 3, 4, 5, 6, 7], dtype=pl.Int64),
            "EventTime": pl.Series([datetime(2025, 1, 1, 0, 0, index) for index in range(7)], dtype=pl.Datetime("ms")),
            "Title": ["café🙂", 'quoted "text"', "two\nlines", "", "tail", "six", "seven"],
            "value": pl.Series([1.25, None, -2.5, 0.0, 3.75, None, -8.0], dtype=pl.Float32),
        }
    )


def _staging_database(
    database_type: type[Postgres], suite: SuiteName, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> Postgres:
    monkeypatch.setattr(get_settings(), "temporary_directory", tmp_path)
    monkeypatch.setattr(get_settings(), "benchmark_threads", 2)
    monkeypatch.setattr(postgres_module, "CLICKBENCH_COPY_BATCH_ROWS", 3)
    database = database_type()
    database._current_suite = suite

    def connect(_self: Postgres, reconnect: bool = False) -> Connection:
        assert not reconnect
        return cast(Connection, object())

    monkeypatch.setattr(Postgres, "connect", connect)

    def exists(_connection: Connection, _table: str) -> bool:
        return True

    monkeypatch.setattr(postgres_module, "table_exists", exists)
    return database


@pytest.mark.parametrize("database_type", [Postgres, TimescaleDB])
@pytest.mark.parametrize("suite", ["clickbench", "tpc_ds"])
def test_postgres_family_only_batches_clickbench_and_preserves_all_values_and_column_order(
    database_type: type[Postgres], suite: SuiteName, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    database = _staging_database(database_type, suite, monkeypatch, tmp_path)
    frame = _clickbench_frame()
    copied: list[pl.DataFrame] = []
    copy_files: list[Path] = []

    def copy(command: list[str], **kwargs: object) -> CompletedProcess[str]:
        assert kwargs == {"capture_output": True, "text": True, "check": True}
        assert command[command.index("--workers") + 1] == "2"
        assert command[command.index("--batch-size") + 1] == "50000"
        assert command[command.index("--table") + 1] == "hits"
        assert "--skip-header" in command
        csv = Path(command[command.index("--file") + 1])
        assert list(csv.parent.glob("*.csv")) == [csv]
        copied.append(pl.read_csv(csv, schema_overrides=frame.schema, try_parse_dates=True))
        copy_files.append(csv)
        return CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(postgres_module.subprocess, "run", copy)

    database.insert(frame.lazy(), "hits")

    assert [batch.height for batch in copied] == ([3, 3, 1] if suite == "clickbench" else [7])
    actual = pl.concat(copied)
    assert actual.equals(frame)
    assert actual.schema == frame.schema
    assert actual.columns == frame.columns
    assert len(set(copy_files)) == len(copy_files)
    assert not list(database._staging_directory.glob("*.csv"))


@pytest.mark.parametrize("database_type", [Postgres, TimescaleDB])
@pytest.mark.parametrize("suite", ["clickbench", "tpc_ds"])
@pytest.mark.parametrize("failure", ["serialization", "copy"])
def test_postgres_family_reclaims_partial_csv_and_preserves_the_original_failure(
    database_type: type[Postgres],
    suite: SuiteName,
    failure: Literal["serialization", "copy"],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    database = _staging_database(database_type, suite, monkeypatch, tmp_path)
    copy_calls = 0

    def fail_serialization(_self: pl.DataFrame | pl.LazyFrame, path: Path) -> None:
        path.write_text("partly serialized")
        raise OSError("CSV serialization failed")

    def copy(command: list[str], **_kwargs: object) -> CompletedProcess[str]:
        nonlocal copy_calls
        copy_calls += 1
        assert Path(command[command.index("--file") + 1]).exists()
        raise CalledProcessError(1, command, stderr="COPY failed")

    if failure == "serialization":
        monkeypatch.setattr(pl.DataFrame, "write_csv", fail_serialization)
        monkeypatch.setattr(pl.LazyFrame, "sink_csv", fail_serialization)
    monkeypatch.setattr(postgres_module.subprocess, "run", copy)

    if failure == "serialization":
        with pytest.raises(OSError, match="CSV serialization failed"):
            database.insert(_clickbench_frame().lazy(), "hits")
    else:
        with pytest.raises(CalledProcessError, match="returned non-zero exit status 1"):
            database.insert(_clickbench_frame().lazy(), "hits")

    assert copy_calls == (0 if failure == "serialization" else 1)
    assert not list(database._staging_directory.glob("*.csv"))
