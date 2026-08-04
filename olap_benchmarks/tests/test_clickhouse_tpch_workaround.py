from __future__ import annotations

from typing import Any, cast

from ..dbs.clickhouse import Clickhouse, ClickhouseTpcH
from ..suites.tpc_h.config import TPCH_QUERY_NAMES


def _suite() -> ClickhouseTpcH:
    return ClickhouseTpcH.model_construct(db=Clickhouse.model_construct(), name="tpc_h", scale_factor=10)


def test_only_the_affected_queries_override_the_join_algorithm() -> None:
    suite = _suite()
    overridden = {
        name for name in TPCH_QUERY_NAMES if "join_algorithm" in suite.query_fetch_kwargs(name).get("settings", {})
    }

    assert overridden == ClickhouseTpcH.PARALLEL_HASH_BUG_QUERIES


def test_affected_queries_force_the_non_parallel_hash_join() -> None:
    suite = _suite()

    for name in ClickhouseTpcH.PARALLEL_HASH_BUG_QUERIES:
        assert suite.query_fetch_kwargs(name)["settings"]["join_algorithm"] == "hash"


def test_workaround_targets_real_queries() -> None:
    # a renamed query would silently stop being worked around, and the run would start failing again
    assert set(TPCH_QUERY_NAMES) >= ClickhouseTpcH.PARALLEL_HASH_BUG_QUERIES


class _StubClient:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


def test_reset_session_closes_and_drops_the_client() -> None:
    db = Clickhouse.model_construct()
    client = _StubClient()
    db._clickhouse_client = cast(Any, client)

    db.reset_session()

    assert client.closed
    assert db._clickhouse_client is None


def test_reset_session_drops_the_client_even_if_closing_fails() -> None:
    # the session is typically already wedged, so close() failing must not stop the reset
    class _Failing(_StubClient):
        def close(self) -> None:
            raise RuntimeError("session is locked")

    db = Clickhouse.model_construct()
    db._clickhouse_client = cast(Any, _Failing())

    db.reset_session()

    assert db._clickhouse_client is None
