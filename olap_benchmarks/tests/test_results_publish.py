from typing import Any, cast

from olap_benchmarks.results import _build_suites_manifest
from olap_benchmarks.settings import (
    ALL_SUITE_SCALE_FACTORS,
    DEFAULT_SUITE_SCALE_FACTORS,
    SCALE_FACTOR_SUITES,
    SUITE_DISPLAY_ORDER,
    SUITE_LABELS,
    SUITE_NAMES,
    SUITE_OPERATIONS,
    SUITE_QUERY_NAME_PARSERS,
)


def test_suites_manifest_matches_suite_settings() -> None:
    manifest = _build_suites_manifest()
    suites = cast(list[dict[str, Any]], manifest["suites"])

    assert [suite["id"] for suite in suites] == list(SUITE_DISPLAY_ORDER)

    suites_by_id = {suite["id"]: suite for suite in suites}
    for suite_name in SUITE_NAMES:
        suite = suites_by_id[suite_name]
        assert suite["title"] == SUITE_LABELS[suite_name]
        assert suite["queries_key"] == suite_name
        assert suite["default_scale_factor"] == DEFAULT_SUITE_SCALE_FACTORS[suite_name]
        assert suite["supported_scale_factors"] == list(ALL_SUITE_SCALE_FACTORS[suite_name])
        assert suite["scale_factor_supported"] == (suite_name in SCALE_FACTOR_SUITES)
        assert suite["operations"] == list(SUITE_OPERATIONS[suite_name])
        assert suite["query_name_parser"] == SUITE_QUERY_NAME_PARSERS[suite_name]
