import json
from dataclasses import replace
from datetime import datetime
from pathlib import Path

import polars as pl
import pytest

from ..dbs import Database
from ..dbs.timescaledb import TimescaleChatThreads, TimescaleDB, TimescaleTimeSeries
from ..settings import TableName, get_settings
from ..suites.chat_threads import config as chat_config
from ..suites.chat_threads.config import MESSAGE_TABLE, STREAM_UPDATE_REWRITES, ChatThreads
from ..suites.chat_threads.generator import (
    ANCHOR_USER_COUNT,
    ChatGenerationSpec,
    MessageRow,
    ThreadRow,
    generate_thread,
)


class FakeConnection:
    def __init__(self) -> None:
        self.executed_sql: list[str] = []
        self.commit_calls = 0

    def execute(self, statement: object) -> None:
        self.executed_sql.append(str(statement))

    def commit(self) -> None:
        self.commit_calls += 1


class FakeTimescaleDB:
    name = "timescaledb"

    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection
        self.upserts: list[tuple[pl.DataFrame, TableName, str | list[str]]] = []

    def connect(self, reconnect: bool = False) -> FakeConnection:
        _ = reconnect
        return self.connection

    def upsert(self, df: pl.DataFrame, table: TableName, primary_key: str | list[str]) -> None:
        self.upserts.append((df.clone(), table, primary_key))


def _set_chat_messages(monkeypatch: pytest.MonkeyPatch, thread: ThreadRow, messages: list[MessageRow]) -> None:
    def fake_generate_thread(
        spec: ChatGenerationSpec,
        user_index: int,
        thread_index: int,
        local_thread_index: int,
    ) -> tuple[ThreadRow, list[MessageRow]]:
        _ = (spec, user_index, thread_index, local_thread_index)
        return thread, messages

    monkeypatch.setattr(chat_config, "generate_thread", fake_generate_thread)


@pytest.mark.parametrize("trailing_role", ["tool", "user", "assistant"])
@pytest.mark.parametrize(
    ("use_timescale", "expected_key"),
    [(False, "message_id"), (True, ["message_id", "created_at"])],
)
def test_chat_stream_rewrites_preserve_message_identity(
    monkeypatch: pytest.MonkeyPatch,
    use_timescale: bool,
    expected_key: str | list[str],
    trailing_role: str,
) -> None:
    db = FakeTimescaleDB(FakeConnection())
    suite = (
        TimescaleChatThreads.model_construct(db=db, name="chat_threads", scale_factor=1)
        if use_timescale
        else ChatThreads[Database].model_construct(db=db, name="chat_threads", scale_factor=1)
    )
    seed = 7
    thread_index = suite._sacrificial_thread_index(seed * 31)
    thread, messages = generate_thread(suite.spec, ANCHOR_USER_COUNT + thread_index % 97, thread_index, 0)
    full_text = "A streamed assistant reply with Unicode café🙂. " * 4
    document = {
        "role": "assistant",
        "parts": [{"type": "thinking", "text": "Keep this unchanged"}, {"type": "text", "text": full_text}],
    }
    source = replace(messages[1], message_id="last-assistant", content=json.dumps(document))
    trailing = replace(
        messages[-1],
        message_id="trailing-message",
        content=json.dumps(
            {
                "role": trailing_role,
                "parts": [{"type": "text" if trailing_role == "user" else "tool_result", "text": "Do not stream this"}],
            }
        ),
    )
    _set_chat_messages(monkeypatch, thread, [messages[1], source, trailing])

    suite._apply_stream_update(seed)

    assert len(db.upserts) == STREAM_UPDATE_REWRITES == 20
    contents: list[str] = []
    for frame, table, primary_key in db.upserts:
        assert table == MESSAGE_TABLE
        assert primary_key == expected_key
        assert frame.height == 1
        assert frame.item(0, "message_id") == source.message_id
        assert frame.item(0, "created_at") == source.created_at
        assert frame.drop("content").equals(db.upserts[0][0].drop("content"))
        content = str(frame.item(0, "content"))
        rewritten = json.loads(content)
        assert rewritten["role"] == "assistant"
        assert rewritten["parts"][0] == document["parts"][0]
        assert full_text.startswith(rewritten["parts"][1]["text"])
        contents.append(content)

    assert len(set(contents)) == STREAM_UPDATE_REWRITES
    assert json.loads(contents[-1]) == document


@pytest.mark.parametrize(
    "document",
    [
        {"role": "user", "parts": [{"type": "text", "text": "A user message"}]},
        {"role": "assistant", "parts": [{"type": "thinking", "text": "No reply yet"}]},
        {"role": "assistant", "parts": [{"type": "text", "text": ""}]},
        {"role": "assistant", "parts": [{"type": "text", "text": None}]},
    ],
)
def test_chat_stream_rewrites_require_an_assistant_text_message(
    monkeypatch: pytest.MonkeyPatch,
    document: dict[str, object],
) -> None:
    db = FakeTimescaleDB(FakeConnection())
    suite = ChatThreads[Database].model_construct(db=db, name="chat_threads", scale_factor=1)
    thread, messages = generate_thread(suite.spec, ANCHOR_USER_COUNT, 19_000, 0)
    _set_chat_messages(monkeypatch, thread, [replace(messages[0], content=json.dumps(document))])

    with pytest.raises(ValueError, match="no assistant message with nonempty text"):
        suite._apply_stream_update(7)

    assert db.upserts == []


def test_timescaledb_time_series_tables_have_no_primary_keys() -> None:
    suite = TimescaleTimeSeries.model_construct(
        db=FakeTimescaleDB(FakeConnection()),
        name="time_series",
        scale_factor=1,
    )

    assert suite.get_primary_key("data_tall") is None
    assert suite.get_primary_key("data_wide") is None
    assert suite.get_primary_key("data_large") is None

    assert suite.get_not_null("data_tall") == "time"
    assert suite.get_not_null("data_wide") == ["time", "metric_name"]
    assert suite.get_not_null("data_large") == ["time", "metric_name"]


def test_timescaledb_time_series_marks_wide_export_shape_unsupported() -> None:
    suite = TimescaleTimeSeries.model_construct(
        db=FakeTimescaleDB(FakeConnection()),
        name="time_series",
        scale_factor=1,
    )

    assert suite.query_skip("large_23_batch_export") == (
        "unsupported",
        "query requires wide-row export shape, but this database stores data_large as EAV",
    )
    assert suite.query_skip("large_09_raw_filtered") is None


@pytest.mark.parametrize(
    ("table", "primary_key", "expected_conflict"),
    [
        ("data_tall", "time", 'ON CONFLICT ("time") DO UPDATE SET "value" = EXCLUDED."value"'),
        (
            "chat_message",
            ["message_id", "created_at"],
            'ON CONFLICT ("message_id", "created_at") DO UPDATE SET "content" = EXCLUDED."content"',
        ),
    ],
)
def test_timescaledb_upsert_uses_on_conflict_instead_of_delete_insert(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    table: TableName,
    primary_key: str | list[str],
    expected_conflict: str,
) -> None:
    connection = FakeConnection()
    copied_tables: list[tuple[str, bool, list[str]]] = []

    def fake_connect(self: TimescaleDB, reconnect: bool = False) -> FakeConnection:
        _ = (self, reconnect)
        return connection

    def fake_copy_csv_to_table(
        self: TimescaleDB,
        con: FakeConnection,
        table: str,
        csv_path: Path,
        columns: list[str],
    ) -> None:
        _ = (self, con)
        copied_tables.append((table, csv_path.exists(), columns))

    data_dir = tmp_path / "timescaledb" / "data"
    data_dir.mkdir(parents=True)

    monkeypatch.setattr(get_settings(), "temporary_directory", tmp_path)
    monkeypatch.setattr(TimescaleDB, "connect", fake_connect)
    monkeypatch.setattr(TimescaleDB, "_copy_csv_to_table", fake_copy_csv_to_table)

    db = TimescaleDB()
    df = (
        pl.DataFrame({"time": [datetime(2025, 1, 1)], "value": [1.5]})
        if table == "data_tall"
        else pl.DataFrame(
            {"content": ['{"parts":[]}'], "created_at": [datetime(2025, 1, 1)], "message_id": ["message-1"]}
        )
    )

    db.upsert(df, table, primary_key=primary_key)

    executed_sql = "\n".join(connection.executed_sql)
    insert_sql = next(
        statement for statement in connection.executed_sql if statement.startswith(f"INSERT INTO {table} ")
    )

    assert f"DELETE FROM {table}" not in executed_sql
    assert insert_sql.endswith(expected_conflict)
    assert copied_tables[0][0].startswith(f"_staging_{table}_")
    assert copied_tables[0][1] is True
    assert copied_tables[0][2] == df.columns
    assert connection.commit_calls == 1
