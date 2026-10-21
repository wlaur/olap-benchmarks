from datetime import datetime
from pathlib import Path

import pytest
from sqlalchemy.orm import Session

from olap_benchmarks.dbs.doris import DORIS_RESULT_CACHE_VARIABLES
from olap_benchmarks.matrix import MatrixCell
from olap_benchmarks.results.models import Run, RunStep
from olap_benchmarks.results.publishing import PublishValidationError, validate_publishable_runs

from .test_results_publish import _add_run, _results_db


@pytest.mark.parametrize("problem", [None, "policy", "before", "after", "python", "connectorx"])
def test_publication_requires_observed_disabled_doris_caches(tmp_path: Path, problem: str | None) -> None:
    cell = MatrixCell("doris", "rtabench", 1, "select")

    def add(session: Session) -> None:
        run_id = _add_run(session, cell, version="4.1.5", driver=None)
        run = session.get_one(Run, run_id)
        assert run.metadata_json is not None
        run.metadata_json = {
            **run.metadata_json,
            "execution": {
                "mode": "container",
                "container_images": {"server": "apache/doris:4.1.5"},
                "container_image_digests": {"server": "apache/doris@sha256:" + "a" * 64},
            },
            "options": {"result_cache_policy": "unknown" if problem == "policy" else "disabled-v1"},
        }
        for phase in ("before", "after"):
            if problem == phase:
                continue
            snapshots = {
                method: dict.fromkeys(DORIS_RESULT_CACHE_VARIABLES, False) for method in ("python", "connectorx")
            }
            if problem in ("python", "connectorx"):
                snapshots[problem]["enable_sql_cache"] = True
            session.add(
                RunStep(
                    run_id=run_id,
                    step_type="phase",
                    step_name=f"doris_cache_settings_{phase}",
                    started_at=datetime(2026, 10, 21),
                    finished_at=datetime(2026, 10, 21),
                    status="completed",
                    metadata_json={"sessions": snapshots},
                )
            )

    db_path = _results_db(tmp_path, add)
    if problem is None:
        validate_publishable_runs(db_path, [cell], pins={"doris": "4.1.5"}, drivers={"doris": None})
    else:
        with pytest.raises(PublishValidationError, match="Doris select runs lack verified disabled result caches"):
            validate_publishable_runs(db_path, [cell], pins={"doris": "4.1.5"}, drivers={"doris": None})
