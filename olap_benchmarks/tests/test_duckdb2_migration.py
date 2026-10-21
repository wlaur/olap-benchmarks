import json

import duckdb

from ..query_registry import SUITES_DIRECTORY


def test_chat_payload_lambdas_preserve_attachment_scope_and_empty_parts() -> None:
    user = "fd71b9e4-4ea8-5b61-aeb6-cfd65f2da38e"
    thread = "85642b7f-067e-5388-8239-7ae73175d5b9"
    payload = {
        "parts": [
            {"text": "hello 🦆"},
            {"source": {"data": "YWJj"}},
            {"content": [{"source": {"data": "ZGVmZw=="}}, {"source": {"data": None}}]},
            {"spec": {"data": "must not count"}},
        ]
    }
    query = (SUITES_DIRECTORY / "chat_threads/queries/06_thread_fetch_payload.sql").read_text()
    with duckdb.connect(config={"threads": 1, "async_threads": 1}) as connection:
        connection.execute("CREATE TABLE chat_message (user_id VARCHAR, thread_id VARCHAR, seq BIGINT, content JSON)")
        connection.executemany(
            "INSERT INTO chat_message VALUES (?, ?, ?, ?)",
            [
                (user, thread, 2, '{"parts":[]}'),
                (user, thread, 1, json.dumps(payload)),
                (user, thread, 3, "{}"),
                (user, "other", 4, json.dumps(payload)),
                ("other", thread, 5, json.dumps(payload)),
            ],
        )
        result = connection.sql(query).pl()

    assert result.columns == ["seq", "text_part", "attachment_bytes"]
    assert result.rows() == [(1, "hello 🦆", 12), (2, None, 0), (3, None, 0)]
