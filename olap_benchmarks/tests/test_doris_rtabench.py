from olap_benchmarks.dbs.doris import Doris, DorisRTABench
from olap_benchmarks.settings import REPO_ROOT
from olap_benchmarks.suites.rtabench.config import RTABENCH_QUERY_NAMES

DORIS_QUERY_DIRECTORY = REPO_ROOT / "olap_benchmarks/suites/rtabench/queries/doris"


def test_doris_rtabench_query_coverage() -> None:
    supported = {path.stem for path in DORIS_QUERY_DIRECTORY.glob("*.sql")}
    baseline = {query_name for query_name in RTABENCH_QUERY_NAMES if query_name.startswith("00")}

    assert supported == baseline
    assert len(supported) == 31


def test_doris_rtabench_queries_use_native_case_aggregates() -> None:
    for query_name in ("0013_satisfaction_with_without_backup", "0028_sales_volume_by_age_group"):
        sql = (DORIS_QUERY_DIRECTORY / f"{query_name}.sql").read_text()
        assert " FILTER (" not in sql
        assert "CASE WHEN" in sql


def test_doris_registers_rtabench() -> None:
    assert Doris().suite_registry()["rtabench"] is DorisRTABench
