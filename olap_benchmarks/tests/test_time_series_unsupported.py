from __future__ import annotations

from ..dbs.monetdb import MonetDB, MonetDBTimeSeries
from ..suites.time_series.config import TIME_SERIES_QUERY_NAMES


def _suite() -> MonetDBTimeSeries:
    return MonetDBTimeSeries.model_construct(db=MonetDB.model_construct(), name="time_series", scale_factor=1)


def test_rolling_average_is_unsupported_on_monetdb() -> None:
    suite = _suite()

    for query_name in ("large_13_rolling_avg", "tall_13_rolling_avg", "wide_13_rolling_avg"):
        assert not suite.include_query(query_name)
        skip = suite.query_skip(query_name)
        assert skip is not None
        assert skip[0] == "unsupported"
        assert "ROWS frame" in skip[1]


def test_other_time_series_queries_stay_supported() -> None:
    suite = _suite()
    excluded = {name for name in TIME_SERIES_QUERY_NAMES if not suite.include_query(name)}

    assert excluded == set(MonetDBTimeSeries.UNSUPPORTED_QUERIES["monetdb"])


def test_unsupported_queries_name_real_queries() -> None:
    # a renamed query would silently become "supported" again and start publishing wrong values
    for database, queries in MonetDBTimeSeries.UNSUPPORTED_QUERIES.items():
        assert set(TIME_SERIES_QUERY_NAMES) >= set(queries), database
