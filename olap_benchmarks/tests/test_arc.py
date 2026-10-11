import io
from datetime import date, datetime

import polars as pl
import pyarrow as pa
import pytest
from pyarrow import ipc

from olap_benchmarks.dbs.arc import (
    Arc,
    ArcChatThreads,
    ArcClickbench,
    ArcTimeSeries,
    build_key_predicate,
    decode_arrow_stream,
    encode_import_chunks,
    encode_parquet,
    iter_write_batches,
    to_arc_arrow,
)


def _frame(rows: int) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "time": pl.datetime_range(
                datetime(2024, 1, 1), datetime(2024, 1, 1, 0, rows - 1), "1m", eager=True, time_unit="ms"
            ),
            "value": pl.Series([None] * rows, dtype=pl.Float32),
            "flag": [True] * rows,
        }
    )


def test_iter_write_batches_splits_by_cell_budget() -> None:
    df = _frame(10)

    eager = list(iter_write_batches(df, batch_cells=9))
    lazy = list(iter_write_batches(df.lazy(), batch_cells=9))

    assert [batch.height for batch in eager] == [3, 3, 3, 1]
    assert pl.concat(eager).equals(df)
    assert pl.concat(lazy).equals(df)


def test_encode_parquet_keeps_types_of_all_null_columns() -> None:
    df = _frame(3)

    decoded = pl.read_parquet(io.BytesIO(encode_parquet(df)))

    assert decoded.schema == df.schema
    assert decoded.equals(df)


def test_encode_parquet_keeps_timestamps_beyond_nanosecond_range() -> None:
    df = pl.DataFrame({"time": pl.Series([datetime(3927, 1, 1)], dtype=pl.Datetime("ms"))})

    assert pl.read_parquet(io.BytesIO(encode_parquet(df))).item(0, 0) == datetime(3927, 1, 1)


def test_build_key_predicate_deduplicates_and_sorts_time_keys() -> None:
    keys = pl.DataFrame(
        {"time": [datetime(2024, 10, 15, 12, 31), datetime(2024, 10, 15, 12, 30), datetime(2024, 10, 15, 12, 31)]}
    )

    assert build_key_predicate(keys, ["time"], "time") == "time IN ('2024-10-15 12:30:00', '2024-10-15 12:31:00')"


def test_build_key_predicate_maps_the_time_column_and_quotes_strings() -> None:
    created = pl.DataFrame({"created_at": [datetime(2025, 1, 1)]})
    threads = pl.DataFrame({"thread_id": ["b", "a'b"]})

    assert build_key_predicate(created, ["created_at"], "created_at") == "time IN ('2025-01-01 00:00:00')"
    assert build_key_predicate(threads, ["thread_id"], "created_at") == "thread_id IN ('a''b', 'b')"


def test_build_key_predicate_rejects_composite_keys() -> None:
    with pytest.raises(ValueError, match="single key column"):
        build_key_predicate(pl.DataFrame({"a": [1], "b": [2]}), ["a", "b"], "time")


def test_to_arc_arrow_uses_types_the_parquet_import_reads() -> None:
    df = pl.DataFrame({"day": [date(2013, 7, 1)], "text": ["x"], "raw": [b"y"]})

    schema = to_arc_arrow(df).schema

    assert schema.types == [pa.timestamp("us"), pa.string(), pa.binary()]


def test_decode_arrow_stream_returns_naive_utc_timestamps() -> None:
    table = pa.table(
        {
            "time": pa.array([1_704_067_200_000_000], type=pa.timestamp("us", tz="UTC")),
            "value": pa.array([1.5]),
        }
    )
    sink = io.BytesIO()
    with ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)

    df = decode_arrow_stream(sink.getvalue())

    assert df.schema == pl.Schema({"time": pl.Datetime("us"), "value": pl.Float64})
    assert df.item(0, 0) == datetime(2024, 1, 1)


def test_arc_registers_the_suites_it_can_store() -> None:
    db = Arc()

    assert dict(db.suite_registry()) == {
        "time_series": ArcTimeSeries,
        "clickbench": ArcClickbench,
        "chat_threads": ArcChatThreads,
    }
    assert ArcTimeSeries.supported_operations == ("populate", "select")
    assert ArcChatThreads.supported_operations == ("populate", "select")
    assert ArcClickbench.supported_operations == ("populate", "select")
    assert not db.DISABLED_MUTATION_STEPS


def test_arc_connect_is_not_a_sql_connection() -> None:
    with pytest.raises(NotImplementedError):
        Arc().connect()


def test_encode_import_chunks_splits_batches_above_the_size_limit() -> None:
    df = pl.DataFrame({"time": list(range(1_000)), "body": [f"{i:06d}" * 150 for i in range(1_000)]})
    chunks = list(encode_import_chunks(df, max_bytes=20_000))
    decoded = pl.concat([pl.read_parquet(io.BytesIO(chunk)) for chunk in chunks])
    assert len(chunks) > 1
    assert all(len(chunk) <= 20_000 for chunk in chunks)
    assert decoded.equals(df)


class _FakeResponse:
    def __init__(self, payload: dict[str, object]) -> None:
        self.payload = payload

    def json(self) -> dict[str, object]:
        return self.payload


def _cycle(cycle_id: int, discovery_errors: int = 0, failed_batches: int = 0) -> dict[str, object]:
    return {"cycle_id": cycle_id, "discovery_errors": discovery_errors, "failed_batches": failed_batches}


def _fake_arc(monkeypatch: pytest.MonkeyPatch, cycles: list[dict[str, object]]) -> list[str]:
    triggered: list[str] = []
    pending = iter(cycles)
    state: dict[str, object] = {"last": _cycle(0)}

    def request(self: Arc, method: str, path: str, **_: object) -> _FakeResponse:
        if path == "/api/v1/compaction/trigger":
            triggered.append(path)
            state["last"] = next(pending)
            return _FakeResponse({"cycle_id": state["last"]["cycle_id"]})  # type: ignore[index]
        return _FakeResponse({"last_cycle": state["last"], "cycle_running": False})

    monkeypatch.setattr(Arc, "_request", request)
    return triggered


def test_compact_retriggers_once_after_a_discovery_error(monkeypatch: pytest.MonkeyPatch) -> None:
    triggered = _fake_arc(monkeypatch, [_cycle(1, discovery_errors=1), _cycle(2)])
    Arc().compact()
    assert len(triggered) == 2


def test_compact_continues_when_the_retry_still_skips_a_partition(monkeypatch: pytest.MonkeyPatch) -> None:
    triggered = _fake_arc(monkeypatch, [_cycle(1, discovery_errors=1), _cycle(2, discovery_errors=1)])
    Arc().compact()
    assert len(triggered) == 2


def test_compact_raises_on_failed_batches(monkeypatch: pytest.MonkeyPatch) -> None:
    _fake_arc(monkeypatch, [_cycle(1, failed_batches=1)])
    with pytest.raises(RuntimeError, match="compaction failed"):
        Arc().compact()
