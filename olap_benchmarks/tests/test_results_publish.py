from typing import Any, cast

from olap_benchmarks.results import _build_queries_manifest, _build_suites_manifest
from olap_benchmarks.settings import (
    ALL_SUITE_SCALE_FACTORS,
    DEFAULT_SUITE_SCALE_FACTORS,
    SCALE_FACTOR_SUITES,
    SUITE_DISPLAY_ORDER,
    SUITE_LABELS,
    SUITE_NAMES,
    SUITE_OPERATIONS,
    SUITE_PUBLIC_ROLES,
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
        assert suite["public_role"] == SUITE_PUBLIC_ROLES[suite_name]


def test_clickbench_q28_uses_engine_regex_backrefs() -> None:
    manifest = _build_queries_manifest()
    q28 = manifest["clickbench"]["Q28"]
    overrides = cast(dict[str, str], q28["db_overrides"])

    assert "'$1'" in overrides["monetdb"]
    assert "'$1'" in overrides["starrocks"]
    assert "'\\1'" in overrides["postgres"]


def test_queries_manifest_includes_registered_suites() -> None:
    manifest = _build_queries_manifest()

    assert set(SUITE_DISPLAY_ORDER).issubset(manifest)
    assert set(manifest["jsonbench"]) == {
        "01_events_by_collection",
        "02_create_events_by_collection",
        "03_create_events_by_hour",
        "04_first_post_users",
        "05_longest_post_activity",
    }
    assert set(cast(dict[str, str], manifest["jsonbench"]["01_events_by_collection"]["db_overrides"])) == {
        "clickhouse",
        "postgres",
        "starrocks",
    }
