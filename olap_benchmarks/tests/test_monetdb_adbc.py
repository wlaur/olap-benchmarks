from collections.abc import Callable
from threading import Event
from unittest.mock import Mock

import polars as pl

from ..dbs.monetdb.adbc import _iter_arrow_batches


def test_lazy_arrow_batches_preserve_producer_boundaries() -> None:
    frame = pl.LazyFrame({"value": range(10)})
    row_counter = [0]

    batches = list(_iter_arrow_batches(frame, row_counter))

    assert sum(batch.num_rows for batch in batches) == 10
    assert row_counter == [10]


def test_lazy_arrow_batches_match_the_declared_arrow_schema() -> None:
    frame = pl.LazyFrame({"label": ["first", "second"]})

    batches = list(_iter_arrow_batches(frame, [0]))

    assert batches
    assert all(batch.schema == frame.collect_schema().to_arrow() for batch in batches)


def test_lazy_arrow_batches_leave_batch_sizing_to_polars_and_driver() -> None:
    batch = pl.DataFrame({"value": [1]}).to_arrow(compat_level=pl.CompatLevel.newest()).to_batches()[0]
    frame = Mock()

    def sink_batches(receive: Callable[[pl.DataFrame], bool], **_: object) -> bool:
        return receive(pl.DataFrame({"value": [1]}))

    frame.sink_batches.side_effect = sink_batches

    batches = list(_iter_arrow_batches(frame, [0]))

    frame.sink_batches.assert_called_once()
    assert frame.sink_batches.call_args.kwargs == {"lazy": False, "engine": "streaming"}
    assert batches == [batch]


def test_lazy_arrow_batches_apply_backpressure_until_each_batch_is_consumed() -> None:
    frame = Mock()
    produced_second = Event()

    def sink_batches(receive: Callable[[pl.DataFrame], bool], **_: object) -> None:
        assert not receive(pl.DataFrame({"value": [1]}))
        produced_second.set()
        assert not receive(pl.DataFrame({"value": [2]}))

    frame.sink_batches.side_effect = sink_batches
    batches = _iter_arrow_batches(frame, [0])

    assert next(batches).column("value").to_pylist() == [1]
    assert not produced_second.is_set()
    assert next(batches).column("value").to_pylist() == [2]
    assert produced_second.is_set()
    assert list(batches) == []
