import pytest

from ..dbs.starrocks import StarRocks, StarRocksTimeSeriesMetrics, StarRocksTimeSeriesRanges
from ..settings import SuiteName
from ..suites.time_series_metrics.config import QUERY_NAMES as METRIC_QUERY_NAMES
from ..suites.time_series_ranges.config import QUERY_NAMES as RANGE_QUERY_NAMES


@pytest.mark.parametrize(
    ("suite_name", "query_names"),
    [("time_series_metrics", METRIC_QUERY_NAMES), ("time_series_ranges", RANGE_QUERY_NAMES)],
)
def test_starrocks_time_series_queries_use_supported_double_type(
    suite_name: SuiteName,
    query_names: tuple[str, ...],
) -> None:
    suite = StarRocks().benchmarks[suite_name]

    assert isinstance(suite, StarRocksTimeSeriesMetrics)
    if suite_name == "time_series_ranges":
        assert isinstance(suite, StarRocksTimeSeriesRanges)
    assert suite.planned_query_names() == query_names
    assert suite.supported_operations == ("populate", "select")

    transformed_queries = 0
    for query_name in query_names:
        path = suite.query_path(query_name)
        assert path is not None
        original = path.read_text()
        loaded = suite.load_query(query_name)

        assert "DOUBLE PRECISION" not in loaded, query_name
        assert loaded == original.replace("DOUBLE PRECISION", "DOUBLE"), query_name
        if "DOUBLE PRECISION" in original:
            assert loaded != original
            transformed_queries += 1

    assert transformed_queries > 0
