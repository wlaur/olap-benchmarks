from __future__ import annotations

from collections.abc import Callable, Mapping
from io import BytesIO
from typing import Any, cast

import polars as pl
import pyarrow as pa
from clickhouse_connect.driver.common import StreamContext
from pytest import MonkeyPatch

from ..dbs.clickhouse import Clickhouse


class ArrowStreamClient:
    def __init__(self, stream_factory: Callable[[], StreamContext]) -> None:
        self.stream_factory = stream_factory
        self.queries: list[tuple[str, Mapping[str, Any] | None]] = []

    def query_arrow_stream(
        self,
        query: str,
        settings: Mapping[str, Any] | None = None,
    ) -> StreamContext:
        self.queries.append((query, settings))
        return self.stream_factory()


def stream_from_batches(schema: pa.Schema, batches: list[pa.RecordBatch]) -> StreamContext:
    reader = pa.RecordBatchReader.from_batches(schema, batches)
    return StreamContext(BytesIO(), cast(Any, reader))


def use_client(monkeypatch: MonkeyPatch, client: ArrowStreamClient) -> None:
    def get_client(_: Clickhouse) -> ArrowStreamClient:
        return client

    monkeypatch.setattr(Clickhouse, "get_client", get_client)


def test_clickhouse_fetch_concatenates_streamed_arrow_batches(monkeypatch: MonkeyPatch) -> None:
    schema = pa.schema([("id", pa.int64()), ("value", pa.string())])
    client = ArrowStreamClient(
        lambda: stream_from_batches(
            schema,
            [
                pa.record_batch([pa.array([1, 2], type=pa.int64()), pa.array(["a", "b"])], schema=schema),
                pa.record_batch([pa.array([3], type=pa.int64()), pa.array(["c"])], schema=schema),
            ],
        )
    )
    db = Clickhouse()
    use_client(monkeypatch, client)

    frame = db.fetch(" select id, value from test; ", settings={"max_threads": 2})

    assert frame.to_dict(as_series=False) == {"id": [1, 2, 3], "value": ["a", "b", "c"]}
    assert client.queries == [("select id, value from test", {"max_threads": 2})]
    assert frame.n_chunks() == 2


def test_clickhouse_fetch_preserves_empty_result_schema(monkeypatch: MonkeyPatch) -> None:
    schema = pa.schema([("id", pa.uint64()), ("value", pa.string())])
    client = ArrowStreamClient(lambda: stream_from_batches(schema, []))
    db = Clickhouse()
    use_client(monkeypatch, client)

    frame = db.fetch("select id, value from test where false")

    assert frame.shape == (0, 2)
    assert frame.schema == {"id": pl.UInt64, "value": pl.String}


def test_clickhouse_fetch_casts_schema_and_normalizes_time(monkeypatch: MonkeyPatch) -> None:
    timestamp_type = pa.timestamp("ms", tz="UTC")
    schema = pa.schema([("value", pa.int32()), ("time", timestamp_type)])
    client = ArrowStreamClient(
        lambda: stream_from_batches(
            schema,
            [
                pa.record_batch(
                    [pa.array([1], type=pa.int32()), pa.array([1_700_000_000_000], type=timestamp_type)],
                    schema=schema,
                )
            ],
        )
    )
    db = Clickhouse()
    use_client(monkeypatch, client)

    frame = db.fetch("select value, time from test", schema={"value": pl.Int64, "time": pl.Datetime("ms")})

    assert frame.schema == {"value": pl.Int64, "time": pl.Datetime("ms")}
    assert frame.item(0, "value") == 1
