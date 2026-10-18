import re
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import cast

import pytest

from ..dbs.timescaledb import TimescaleDB, TimescaleRTABench
from ..run_metadata import hash_schema_files
from ..settings import REPO_ROOT, get_settings
from ..suites.rtabench.config import RTABENCH_QUERY_NAMES


def test_every_preaggregated_query_has_a_declared_and_fingerprinted_schema() -> None:
    suite = TimescaleDB().benchmarks["rtabench"]
    assert isinstance(suite, TimescaleRTABench)
    assert set(suite.CONTINUOUS_AGGREGATES) == {name for name in RTABENCH_QUERY_NAMES if name.startswith("1")}
    hashes = cast(dict[str, str], hash_schema_files("rtabench", "timescaledb")["files"])
    for query_name, view_name in suite.CONTINUOUS_AGGREGATES.items():
        relative = f"rtabench/schemas/timescaledb/{view_name}.sql"
        assert relative in hashes
        schema = (REPO_ROOT / "olap_benchmarks/suites" / relative).read_text()
        assert re.findall(r"CREATE MATERIALIZED VIEW (\w+)", schema) == [view_name]
        assert set(re.findall(r"\bcagg_\w+\b", suite.load_query(query_name))) == {view_name}


@pytest.mark.parametrize(
    ("included", "excluded", "expected"),
    [
        ((), (), 10),
        (("00*",), (), 0),
        (("00*", "1004_*", "1008_*"), (), 2),
        ((), ("1012_*",), 9),
    ],
)
def test_preparation_respects_the_same_query_selection_as_select(
    monkeypatch: pytest.MonkeyPatch, included: tuple[str, ...], excluded: tuple[str, ...], expected: int
) -> None:
    executed: list[Path] = []
    phases: list[str | None] = []

    class RecordingTimescaleDB(TimescaleDB):
        def execute_schema_file(self, fpath: Path, autocommit: bool = False) -> None:
            assert autocommit
            executed.append(fpath)

        @contextmanager
        def phase_context(self, phase_name: str, table_name: str | None = None) -> Generator[None]:
            assert phase_name == "continuous_aggregates"
            phases.append(table_name)
            yield

    monkeypatch.setattr(get_settings(), "query_include", included)
    monkeypatch.setattr(get_settings(), "query_exclude", excluded)
    suite = RecordingTimescaleDB().benchmarks["rtabench"]
    assert isinstance(suite, TimescaleRTABench)
    suite.prepare_continuous_aggregates()

    assert len(executed) == expected
    assert phases == [path.stem for path in executed]
    assert suite.planned_query_names() == RTABENCH_QUERY_NAMES
    selected = {view for query, view in suite.CONTINUOUS_AGGREGATES.items() if suite.query_skip(query) is None}
    assert {path.stem for path in executed} == selected
