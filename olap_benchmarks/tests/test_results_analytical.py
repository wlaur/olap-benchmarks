from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl
import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from .. import results as results_module
from ..analytical_streams import (
    ANALYTICAL_PROFILE,
    CLICKHOUSE_REQUEST_SETTINGS,
    AnalyticalConfig,
    AnalyticalQuery,
    RequestSample,
    analytical_metadata,
    balanced_query_order,
    summarize_requests,
)
from ..dbs import get_databases
from ..matrix import MatrixCell
from ..results import get_results_engine
from ..results.campaign import select_campaign_runs
from ..results.hashing import build_answer_metadata
from ..results.models import Run, RunStep, SystemSnapshot
from ..results.publishing import PublishValidationError, validate_publishable_runs
from ..results.validation import validate_results
from ..run_metadata import sha256_text
from ..settings import DatabaseName, SuiteName, get_settings
from .test_results_campaign import DRIVERS, PINS, SELECT, _add_run, _results_db

ANALYTICAL_CELL = MatrixCell("duckdb", "tpc_h", 10, "select")
CELLS = [SELECT, ANALYTICAL_CELL]
ANSWER = build_answer_metadata(pl.DataFrame({"value": [1]}))
type Metadata = dict[str, Any]


def _input() -> Metadata:
    content = "a" * 64
    return {
        "algorithm": "sha256",
        "manifest_sha256": sha256_text(f"fixture\0{1}\0{content}\n"),
        "files": [{"path": "fixture", "bytes": 1, "sha256": content}],
    }


def _queries(db: DatabaseName = "duckdb", suite: SuiteName = "tpc_h") -> tuple[AnalyticalQuery, ...]:
    database = next(database for name, database in get_databases().items() if name == db)
    cls = database.suite_registry()[suite]
    benchmark = cls(db=database, name=suite, scale_factor=10)
    return tuple(AnalyticalQuery(name, benchmark.load_query(name), {}) for name in benchmark.planned_query_names())


def _normal(session: Session, *, db: DatabaseName = "duckdb", suite: SuiteName = "tpc_h") -> int:
    names = tuple(query.name for query in _queries(db, suite))
    run_id = _add_run(session, MatrixCell(db, suite, 10, "select"), plan=names, measured=names)
    run = session.get(Run, run_id)
    assert run is not None
    run.metadata_json = {**(run.metadata_json or {}), "input": _input()}
    for step in session.scalars(select(RunStep).where(RunStep.run_id == run_id)).all():
        step.metadata_json = deepcopy(ANSWER)
    return run_id


def _analytical(
    session: Session,
    *,
    start: int = 1,
    clients: int = 1,
    duration: float = 60,
    seed: int = 99,
    status: str = "completed",
    excluded: int = 0,
    db: DatabaseName = "duckdb",
    suite: SuiteName = "tpc_h",
) -> int:
    queries = _queries(db, suite)
    exclusions = {
        query.name: {"status": "skipped", "reason": "explicit fixture exclusion"} for query in queries[:excluded]
    }
    queries = queries[excluded:]
    config = AnalyticalConfig(clients, duration, seed)
    evidence = analytical_metadata(config, queries, exclusions)
    started = datetime(2026, 10, 19) + timedelta(seconds=start)
    measurement = started + timedelta(seconds=2)
    end = measurement + timedelta(seconds=duration + 0.2)
    metadata: Metadata = {
        "repository": {"commit": "analytical-commit", "dirty": False},
        "execution": {"mode": "in_process"},
        "benchmark_profile": ANALYTICAL_PROFILE.metadata(),
        "input": _input(),
        "settings": {
            "OLAP_BENCHMARKS_BENCHMARK_THREADS": 10,
            "OLAP_BENCHMARKS_BENCHMARK_MEMORY_GB": 24,
            "OLAP_BENCHMARKS_CLIENT_MEMORY_LIMIT_GB": 28,
            "OLAP_BENCHMARKS_OPERATION_TIMEOUT_SECONDS": 600,
            "OLAP_BENCHMARKS_QUERY_TIMEOUT_SECONDS": 90,
        },
        "analytical": evidence,
    }
    run = Run(
        suite=suite,
        suite_scale_factor=10,
        db=db,
        db_version=PINS[db],
        db_driver=DRIVERS[db],
        operation="analytical",
        system="test",
        system_snapshot_id=1,
        benchmark_profile=ANALYTICAL_PROFILE.id,
        status=status,
        started_at=started,
        finished_at=None if status == "running" else end + timedelta(seconds=1),
        error_type="TimeoutError" if status == "failed" else None,
        error_message="operation aborted" if status == "failed" else None,
        metadata_json=deepcopy(metadata),
    )
    session.add(run)
    session.flush()
    if status != "completed":
        return run.id
    samples: list[RequestSample] = []
    for client in range(clients):
        order = balanced_query_order([query.name for query in queries], seed, client)
        for phase in ("warmup", "measured"):
            for sequence, name in enumerate(order if phase == "warmup" else order[:1]):
                milliseconds = 1.0 if phase == "warmup" else (duration + 0.2) * 1_000
                time = started + timedelta(milliseconds=10 + sequence * 2) if phase == "warmup" else measurement
                session.add(
                    RunStep(
                        run_id=run.id,
                        step_type="query",
                        step_name="analytical_request",
                        query_name=name,
                        started_at=time,
                        finished_at=time + timedelta(milliseconds=milliseconds),
                        status="completed",
                        result_status="ok",
                        row_count=1,
                        metadata_json={
                            "request_id": f"client-{client}:{phase}:{sequence}",
                            "client_id": client,
                            "phase": phase,
                            "sequence": sequence,
                            "query_sha256": next(sha256_text(query.query) for query in queries if query.name == name),
                            "duration_ms": milliseconds,
                            **deepcopy(ANSWER),
                        },
                    )
                )
                samples.append(RequestSample(name, phase, "completed", milliseconds))
    effective: Metadata = {"settings": {"threads": "10", "memory_limit": "24.0 GiB"}}
    runtime: Metadata = {"runtime": effective, "cache_attestation": "cache hits are not instrumented"}
    if db == "polars":
        effective = {"thread_pool_size": 10, "engine_memory_limit_bytes": None}
        runtime = {"runtime": {"thread_pool_size": 10}, "cache_attestation": "cache hits are not instrumented"}
    elif db in ("postgres", "clickhouse"):
        effective = {"fixture-server": {"NanoCpus": 10_000_000_000, "Memory": 24 * 1024**3}}
        if db == "clickhouse":
            runtime = {
                "settings": {key: str(value) for key, value in CLICKHOUSE_REQUEST_SETTINGS.items()},
                "cache_attestation": "cache hits are not instrumented",
                "session_scope": "independent HTTP client",
            }
        else:
            settings = {
                name: {"setting": "1"}
                for name in (
                    "shared_buffers",
                    "effective_cache_size",
                    "work_mem",
                    "max_parallel_workers",
                    "max_parallel_workers_per_gather",
                    "max_worker_processes",
                )
            }
            settings.update(
                {name: {"setting": "on"} for name in ("default_transaction_read_only", "transaction_read_only")}
            )
            settings["statement_timeout"] = {"setting": "90000"}
            runtime = {"settings": settings, "backend_pid": 123, "cache_attestation": "cache hits are not instrumented"}
        metadata["execution"] = {
            "mode": "container",
            "container_images": {"server": "fixture:latest"},
            "container_image_digests": {"server": "fixture@sha256:" + "a" * 64},
        }
    measured = [sample for sample in samples if sample.phase == "measured"]
    evidence.update(
        budget={
            "configured_threads": 10,
            "configured_engine_memory_bytes": 24 * 1024**3,
            "client_rss_guard_bytes": 28 * 1024**3,
            "multiplied_by_clients": False,
            "scope": "shared engine budget",
            "client_rss_scope": "isolated operation RSS",
            "effective": effective,
        },
        worker_runtime={str(client): deepcopy(runtime) for client in range(clients)},
        measurement_started_at=measurement.replace(tzinfo=UTC).isoformat(),
        measurement_finished_at=end.replace(tzinfo=UTC).isoformat(),
        stop_issuing_at=(measurement + timedelta(seconds=duration)).replace(tzinfo=UTC).isoformat(),
        warmup_started_at=started.replace(tzinfo=UTC).isoformat(),
        warmup_finished_at=measurement.replace(tzinfo=UTC).isoformat(),
        actual_elapsed_seconds=duration + 0.2,
        drain_seconds=0.2,
        cancelled=False,
        worker_failures=[],
        overall=summarize_requests(measured, duration + 0.2),
        per_query={
            query.name: summarize_requests(
                [sample for sample in measured if sample.query_name == query.name], duration + 0.2
            )
            for query in queries
        },
        warmup_summary=summarize_requests([sample for sample in samples if sample.phase == "warmup"], None),
    )
    run.metadata_json = deepcopy(metadata)
    return run.id


def _db(tmp_path: Path, add: Callable[[Session], object]) -> Path:
    def with_select(session: Session) -> None:
        _add_run(session)
        _normal(session)
        add(session)

    return _results_db(tmp_path, with_select)


def _selected(path: Path) -> set[int]:
    return select_campaign_runs(path, CELLS, pins=PINS, drivers=DRIVERS)


def _validate(path: Path, *, campaign: bool = True, cells: list[MatrixCell] = CELLS) -> None:
    validate_publishable_runs(path, cells, pins=PINS, drivers=DRIVERS, campaign=campaign, allow_mixed_commits=campaign)


def _edit(session: Session, run_id: int, change: Callable[[Run, Metadata], object]) -> None:
    session.flush()
    run = session.get(Run, run_id)
    assert run is not None and run.metadata_json is not None
    metadata = deepcopy(run.metadata_json)
    change(run, metadata)
    run.metadata_json = metadata


def _step(session: Session, run_id: int, *, phase: str = "measured") -> RunStep:
    session.flush()
    return next(
        step
        for step in session.scalars(select(RunStep).where(RunStep.run_id == run_id).order_by(RunStep.id))
        if (step.metadata_json or {}).get("phase") == phase
    )


def _hard_abort(session: Session, *, start: int = 1, clients: int = 1, phase: str = "measured") -> int:
    run_id = _analytical(session, start=start, clients=clients)

    def change(run: Run, metadata: Metadata) -> None:
        run.status, run.error_type, run.error_message = "failed", "TimeoutError", "operation watchdog expired"
        previous = metadata["analytical"]
        evidence = analytical_metadata(AnalyticalConfig(clients, 60, 99), _queries(), {})
        if phase == "measured":
            for key in ("measurement_started_at", "stop_issuing_at", "warmup_started_at", "warmup_finished_at"):
                evidence[key] = previous[key]
        metadata["analytical"] = evidence

    _edit(session, run_id, change)
    for step in session.scalars(select(RunStep).where(RunStep.run_id == run_id)).all():
        metadata = step.metadata_json or {}
        if metadata.get("phase") == phase and metadata.get("sequence") == 0:
            step.status, step.result_status, step.finished_at, step.row_count = "running", None, None, None
            step.metadata_json = {
                key: value
                for key, value in metadata.items()
                if key in {"request_id", "client_id", "phase", "sequence", "query_sha256"}
            }
        elif phase == "warmup":
            session.delete(step)
    return run_id


def test_campaign_retains_separate_client_duration_seed_and_manifest_variants(tmp_path: Path) -> None:
    def add(session: Session) -> None:
        _analytical(session, start=1)
        _analytical(session, start=2, clients=4)
        _analytical(session, start=3, clients=16)
        _analytical(session, start=4, duration=30)
        _analytical(session, start=5, seed=0)
        _analytical(session, start=6, excluded=1)
        _analytical(session, start=7)

    path = _db(tmp_path, add)
    assert _selected(path) == {1, 2, 4, 5, 6, 7, 8, 9}
    results_module._restrict_to_publishable_runs(path, CELLS, DRIVERS, pins=PINS, campaign=True)
    _validate(path)


@pytest.mark.parametrize("status", ["failed", "completed"])
def test_latest_terminal_attempt_replaces_earlier_analytical_attempt(tmp_path: Path, status: str) -> None:
    def add(session: Session) -> None:
        _analytical(session)
        _analytical(session, start=2, status=status)

    path = _db(tmp_path, add)
    assert _selected(path) == {1, 2, 4}
    results_module._restrict_to_publishable_runs(path, CELLS, DRIVERS, pins=PINS, campaign=True)
    _validate(path)


@pytest.mark.parametrize("invalid", ["version", "driver", "profile", "dirty", "commit", "invalidation"])
def test_ineligible_analytical_attempts_do_not_hide_current_evidence(tmp_path: Path, invalid: str) -> None:
    def add(session: Session) -> None:
        _analytical(session)
        run_id = _analytical(session, start=2)

        def change(run: Run, metadata: Metadata) -> None:
            if invalid == "version":
                run.db_version = "old"
            elif invalid == "driver":
                run.db_driver = "other"
            elif invalid == "profile":
                run.benchmark_profile = "other"
            elif invalid == "dirty":
                metadata["repository"]["dirty"] = True
            elif invalid == "commit":
                metadata["repository"].pop("commit")
            else:
                metadata["invalidation"] = {"reason": "invalid input"}

        _edit(session, run_id, change)

    assert _selected(_db(tmp_path, add)) == {1, 2, 3}


@pytest.mark.parametrize("status", ["running", "completed", "failed"])
def test_newer_unfinished_analytical_attempt_blocks_publication(tmp_path: Path, status: str) -> None:
    def add(session: Session) -> None:
        _analytical(session)
        run_id = _analytical(session, start=2, status=status)
        run = session.get(Run, run_id)
        assert run is not None
        run.finished_at = None

    with pytest.raises(PublishValidationError, match="unfinished analytical"):
        _selected(_db(tmp_path, add))


def test_older_unfinished_analytical_attempt_does_not_hide_terminal_attempt(tmp_path: Path) -> None:
    def add(session: Session) -> None:
        _analytical(session, status="running")
        _analytical(session, start=2)

    assert _selected(_db(tmp_path, add)) == {1, 2, 4}


@pytest.mark.parametrize(
    "problem",
    [
        "profile",
        "manifest",
        "exclusions",
        "input",
        "budget",
        "worker",
        "summary",
        "elapsed",
        "cancelled",
        "missing_summary",
        "snapshot",
        "deadline",
    ],
)
def test_contradictory_analytical_metadata_is_rejected(tmp_path: Path, problem: str) -> None:
    def add(session: Session) -> None:
        run_id = _analytical(session)

        def change(run: Run, metadata: Metadata) -> None:
            evidence = metadata["analytical"]
            if problem == "profile":
                metadata["benchmark_profile"]["measured_iterations"] = 3
            elif problem == "manifest":
                evidence["query_manifest"]["files"].pop(next(iter(evidence["query_manifest"]["files"])))
            elif problem == "exclusions":
                evidence["excluded_queries"] = {"made_up": {"status": "skipped", "reason": "fixture"}}
            elif problem == "input":
                metadata["input"]["manifest_sha256"] = "a" * 64
            elif problem == "budget":
                evidence["budget"]["effective"]["settings"]["threads"] = "160"
            elif problem == "worker":
                evidence["worker_runtime"]["0"]["runtime"]["settings"]["memory_limit"] = "384.0 GiB"
            elif problem == "summary":
                evidence["overall"]["completed"] += 1
            elif problem == "elapsed":
                evidence["actual_elapsed_seconds"] = "60"
            elif problem == "cancelled":
                evidence["cancelled"] = True
            elif problem == "missing_summary":
                evidence.pop("overall")
            elif problem == "snapshot":
                run.system_snapshot_id = 999
            else:
                metadata["settings"]["OLAP_BENCHMARKS_OPERATION_TIMEOUT_SECONDS"] = 1

        _edit(session, run_id, change)

    with pytest.raises(PublishValidationError):
        _validate(_db(tmp_path, add))


@pytest.mark.parametrize(
    "problem",
    [
        "client",
        "phase",
        "request_id",
        "sequence",
        "query_hash",
        "role",
        "rows",
        "hash",
        "duration",
        "error",
        "unfinished",
        "wrong_result",
        "schedule",
    ],
)
def test_invalid_analytical_request_evidence_is_rejected(tmp_path: Path, problem: str) -> None:
    def add(session: Session) -> None:
        run_id = _analytical(session)
        step = _step(session, run_id)
        metadata = deepcopy(step.metadata_json or {})
        if problem in ("client", "phase", "request_id", "sequence", "query_hash"):
            key = {"client": "client_id", "query_hash": "query_sha256"}.get(problem, problem)
            metadata[key] = {
                "client": 4,
                "phase": "select",
                "request_id": "duplicate",
                "sequence": 2,
                "query_hash": "a" * 64,
            }[problem]
        elif problem == "role":
            step.iteration_role = "measured"
        elif problem == "rows":
            step.row_count = 2
        elif problem == "hash":
            metadata["answer_hash"] = "made_up"
        elif problem == "duration":
            metadata["duration_ms"] = -1
        elif problem == "error":
            step.error_type = "QueryError"
        elif problem == "unfinished":
            step.finished_at = None
        elif problem == "wrong_result":
            step.result_status = "wrong_result"
        else:
            step.query_name = next(query.name for query in _queries() if query.name != step.query_name)
            metadata["query_sha256"] = next(
                sha256_text(query.query) for query in _queries() if query.name == step.query_name
            )
        step.metadata_json = metadata

    with pytest.raises(PublishValidationError):
        _validate(_db(tmp_path, add))


def test_hard_aborted_requests_retain_unknown_outcomes_without_summaries(tmp_path: Path) -> None:
    def add(session: Session) -> None:
        run_id = _analytical(session)
        step = _step(session, run_id)

        def change(run: Run, metadata: Metadata) -> None:
            run.status = "failed"
            run.error_type, run.error_message = "KeyboardInterrupt", "interrupted"
            metadata["analytical"] = analytical_metadata(AnalyticalConfig(1, 60, 99), _queries(), {})

        _edit(session, run_id, change)
        step.status, step.result_status, step.row_count = "failed", None, None
        step.error_type, step.error_message = "KeyboardInterrupt", "interrupted"
        step.metadata_json = {
            key: value
            for key, value in (step.metadata_json or {}).items()
            if key in {"request_id", "client_id", "phase", "sequence", "query_sha256"}
        }

    path = _db(tmp_path, add)
    before = path.read_bytes()
    _validate(path)
    assert path.read_bytes() == before
    assert validate_results(db_path=path, pins=PINS, mark=True, include_failed=True).ok
    engine = get_results_engine(db_path=path)
    try:
        with Session(engine) as session:
            step = _step(session, 3)
            assert step.status == "failed" and step.result_status is None
            assert step.error_type == "KeyboardInterrupt"
    finally:
        engine.dispose()


@pytest.mark.parametrize("clients", [1, 16])
@pytest.mark.parametrize("phase", ["warmup", "measured"])
def test_failed_runs_retain_unfinished_requests_and_unknown_readback(tmp_path: Path, clients: int, phase: str) -> None:
    path = _db(tmp_path, lambda session: _hard_abort(session, clients=clients, phase=phase))
    before = path.read_bytes()
    assert _selected(path) == {1, 2, 3}
    _validate(path)
    assert path.read_bytes() == before
    engine = get_results_engine(db_path=path)
    try:
        with Session(engine) as session:
            run = session.get(Run, 3)
            assert run is not None and run.status == "failed" and run.finished_at is not None
            assert run.error_type == "TimeoutError" and run.error_message == "operation watchdog expired"
            evidence = (run.metadata_json or {})["analytical"]
            assert (
                not {
                    "overall",
                    "per_query",
                    "warmup_summary",
                    "actual_elapsed_seconds",
                    "drain_seconds",
                    "budget",
                    "worker_runtime",
                }
                & evidence.keys()
            )
            requests = session.scalars(
                select(RunStep).where(RunStep.run_id == run.id, RunStep.status == "running")
            ).all()
            assert len(requests) == clients
            for request in requests:
                assert request.result_status is None and request.finished_at is None and request.row_count is None
                assert request.error_type is None and request.error_message is None
                assert run.started_at <= request.started_at <= run.finished_at
                assert set(request.metadata_json or {}) == {
                    "request_id",
                    "client_id",
                    "phase",
                    "sequence",
                    "query_sha256",
                }
    finally:
        engine.dispose()


@pytest.mark.parametrize(
    "problem",
    [
        "completed_run",
        "running_run",
        "unfinished_run",
        "run_error_type",
        "run_error_message",
        "before_run",
        "after_run",
        "finished_request",
        "completed_request",
        "failed_request",
        "result",
        "rows",
        "error_type",
        "error_message",
        "duration",
        "null_duration",
        "answer",
        "null_answer",
        "request_status",
        "null_request_status",
        "measurement_end",
    ],
)
def test_unfinished_requests_reject_contradictory_outcomes_or_missing_terminal_failure(
    tmp_path: Path, problem: str
) -> None:
    def add(session: Session) -> None:
        run_id = _hard_abort(session)
        step = _step(session, run_id)

        def change(run: Run, metadata: Metadata) -> None:
            request = deepcopy(step.metadata_json or {})
            if problem == "completed_run":
                run.status, run.error_type, run.error_message = "completed", None, None
            elif problem == "running_run":
                run.status = "running"
            elif problem == "unfinished_run":
                run.finished_at = None
            elif problem == "run_error_type":
                run.error_type = None
            elif problem == "run_error_message":
                run.error_message = None
            elif problem == "before_run":
                step.started_at = run.started_at - timedelta(microseconds=1)
            elif problem == "after_run":
                assert run.finished_at is not None
                step.started_at = run.finished_at + timedelta(microseconds=1)
            elif problem == "finished_request":
                step.finished_at = run.finished_at
            elif problem == "completed_request":
                step.status = "completed"
            elif problem == "failed_request":
                step.status = "failed"
            elif problem == "result":
                step.result_status = "ok"
            elif problem == "rows":
                step.row_count = 0
            elif problem == "error_type":
                step.error_type = "TimeoutError"
            elif problem == "error_message":
                step.error_message = ""
            elif problem in ("duration", "null_duration"):
                request["duration_ms"] = 0 if problem == "duration" else None
            elif problem in ("answer", "null_answer"):
                request["answer_hash"] = ANSWER["answer_hash"] if problem == "answer" else None
            elif problem in ("request_status", "null_request_status"):
                request["request_status"] = "completed" if problem == "request_status" else None
            else:
                metadata["analytical"]["measurement_finished_at"] = step.started_at.replace(tzinfo=UTC).isoformat()
            step.metadata_json = request

        _edit(session, run_id, change)

    with pytest.raises(PublishValidationError, match="unfinished|terminal|explicit run error|analytical outcome"):
        _validate(_db(tmp_path, add))


@pytest.mark.parametrize(
    ("phase", "summary"),
    [("warmup", "warmup_summary"), ("measured", "overall"), ("measured", "per_query")],
)
@pytest.mark.parametrize("claim", ["empty", "zero", "qps", "latency"])
def test_unknown_request_phases_cannot_claim_partial_or_complete_summaries(
    tmp_path: Path, phase: str, summary: str, claim: str
) -> None:
    def add(session: Session) -> None:
        run_id = _hard_abort(session, phase=phase)
        step = _step(session, run_id, phase=phase)

        def change(_run: Run, metadata: Metadata) -> None:
            value: Metadata = {
                "empty": {},
                "zero": {"offered": 0, "completed": 0, "errors": 0, "timeouts": 0},
                "qps": {"throughput_per_second": 0},
                "latency": {"latency_ms": {"n": 0}},
            }[claim]
            metadata["analytical"][summary] = {str(step.query_name): value} if summary == "per_query" else value

        _edit(session, run_id, change)

    with pytest.raises(PublishValidationError, match=f"{summary} summary contradicts"):
        _validate(_db(tmp_path, add))


@pytest.mark.parametrize("problem", ["sequence", "schedule", "overlap_before", "overlap_after"])
def test_unfinished_requests_preserve_seeded_order_and_closed_loop_checks(tmp_path: Path, problem: str) -> None:
    def add(session: Session) -> None:
        run_id = _hard_abort(session)
        step = _step(session, run_id)
        order = balanced_query_order([query.name for query in _queries()], 99, 0)
        if problem == "sequence":
            step.metadata_json = {**(step.metadata_json or {}), "sequence": 1, "request_id": "client-0:measured:1"}
        elif problem == "schedule":
            step.query_name = order[1]
            step.metadata_json = {
                **(step.metadata_json or {}),
                "query_sha256": next(sha256_text(query.query) for query in _queries() if query.name == order[1]),
            }
        elif problem == "overlap_before":
            warmups = session.scalars(
                select(RunStep).where(RunStep.run_id == run_id, RunStep.status == "completed")
            ).all()
            last = max(warmups, key=lambda request: request.started_at)
            assert last.finished_at is not None
            step.started_at = last.finished_at - timedelta(microseconds=1)
            _edit(session, run_id, lambda _run, metadata: metadata["analytical"].update(measurement_started_at=None))
        else:
            session.add(
                RunStep(
                    run_id=run_id,
                    step_type="query",
                    step_name="analytical_request",
                    query_name=order[1],
                    status="running",
                    started_at=step.started_at + timedelta(milliseconds=1),
                    metadata_json={
                        **(step.metadata_json or {}),
                        "sequence": 1,
                        "request_id": "client-0:measured:1",
                        "query_sha256": next(
                            sha256_text(query.query) for query in _queries() if query.name == order[1]
                        ),
                    },
                )
            )

    with pytest.raises(PublishValidationError, match="sequences|seeded query schedule|requests overlap"):
        _validate(_db(tmp_path, add))


@pytest.mark.parametrize("boundary", ["measurement_started_at", "warmup_finished_at"])
@pytest.mark.parametrize("finished", [False, True])
def test_unknown_warmup_requests_reject_completion_boundaries(tmp_path: Path, boundary: str, finished: bool) -> None:
    def add(session: Session) -> None:
        run_id = _hard_abort(session, phase="warmup")
        step = _step(session, run_id, phase="warmup")
        if finished:
            step.status = "failed"
            step.finished_at = step.started_at + timedelta(milliseconds=1)
            step.error_type, step.error_message = "KeyboardInterrupt", "interrupted"

        def change(_run: Run, metadata: Metadata) -> None:
            time = step.started_at + timedelta(seconds=1)
            metadata["analytical"][boundary] = time.replace(tzinfo=UTC).isoformat()

        _edit(session, run_id, change)

    with pytest.raises(PublishValidationError, match="unfinished|warmup completion"):
        _validate(_db(tmp_path, add))


@pytest.mark.parametrize("boundary", ["measurement_started_at", "warmup_finished_at"])
@pytest.mark.parametrize("offset", [-1, 0])
def test_completed_warmup_requests_respect_completion_boundaries(tmp_path: Path, boundary: str, offset: int) -> None:
    def add(session: Session) -> None:
        run_id = _hard_abort(session)
        warmups = session.scalars(select(RunStep).where(RunStep.run_id == run_id, RunStep.status == "completed")).all()
        last = max(warmups, key=lambda request: request.started_at)
        assert last.finished_at is not None
        time = last.finished_at + timedelta(microseconds=offset)
        _edit(
            session,
            run_id,
            lambda _run, metadata: metadata["analytical"].update({boundary: time.replace(tzinfo=UTC).isoformat()}),
        )

    path = _db(tmp_path, add)
    before = path.read_bytes()
    if offset < 0:
        with pytest.raises(PublishValidationError, match="contradictory request timestamps"):
            _validate(path)
    else:
        _validate(path)
    assert path.read_bytes() == before


@pytest.mark.parametrize("phase", ["warmup", "measured"])
@pytest.mark.parametrize("unfinished", [False, True])
def test_client_requests_reject_reversed_sequence_history(tmp_path: Path, phase: str, unfinished: bool) -> None:
    def add(session: Session) -> None:
        run_id = _hard_abort(session, phase=phase)
        step = _step(session, run_id, phase=phase)
        queries = _queries()
        order = balanced_query_order([query.name for query in queries], 99, 0)
        time = step.started_at
        step.started_at = time + timedelta(seconds=2)
        if not unfinished:
            step.status, step.result_status, step.row_count = "completed", "ok", 1
            step.finished_at = step.started_at + timedelta(seconds=1)
            step.metadata_json = {**(step.metadata_json or {}), "duration_ms": 1_000.0, **deepcopy(ANSWER)}
        name = order[1]
        session.add(
            RunStep(
                run_id=run_id,
                step_type="query",
                step_name="analytical_request",
                query_name=name,
                status="completed",
                result_status="ok",
                started_at=time,
                finished_at=time + timedelta(seconds=1),
                row_count=1,
                metadata_json={
                    "request_id": f"client-0:{phase}:1",
                    "client_id": 0,
                    "phase": phase,
                    "sequence": 1,
                    "query_sha256": next(sha256_text(query.query) for query in queries if query.name == name),
                    "duration_ms": 1_000.0,
                    **deepcopy(ANSWER),
                },
            )
        )

    with pytest.raises(PublishValidationError, match="requests overlap"):
        _validate(_db(tmp_path, add))


@pytest.mark.parametrize("phase", ["warmup", "measured"])
@pytest.mark.parametrize("gap_microseconds", [-1, 0, 1])
def test_unfinished_final_requests_respect_logical_closed_loop_boundaries(
    tmp_path: Path, phase: str, gap_microseconds: int
) -> None:
    def add(session: Session) -> None:
        run_id = _hard_abort(session, phase=phase)
        step = _step(session, run_id, phase=phase)
        queries = _queries()
        order = balanced_query_order([query.name for query in queries], 99, 0)
        previous = deepcopy(step.metadata_json or {})
        time = step.started_at
        step.started_at = time + timedelta(seconds=1, microseconds=gap_microseconds)
        step.query_name = order[1]
        step.metadata_json = {
            **previous,
            "sequence": 1,
            "request_id": f"client-0:{phase}:1",
            "query_sha256": next(sha256_text(query.query) for query in queries if query.name == order[1]),
        }
        session.add(
            RunStep(
                run_id=run_id,
                step_type="query",
                step_name="analytical_request",
                query_name=order[0],
                status="completed",
                result_status="ok",
                started_at=time,
                finished_at=time + timedelta(seconds=1),
                row_count=1,
                metadata_json={**previous, "duration_ms": 1_000.0, **deepcopy(ANSWER)},
            )
        )

    path = _db(tmp_path, add)
    before = path.read_bytes()
    if gap_microseconds < 0:
        with pytest.raises(PublishValidationError, match="requests overlap"):
            _validate(path)
    else:
        _validate(path)
    assert path.read_bytes() == before


def test_client_requests_reject_measurement_before_warmup_without_phase_timestamps(tmp_path: Path) -> None:
    def add(session: Session) -> None:
        run_id = _hard_abort(session)
        step = _step(session, run_id)
        step.status, step.result_status, step.row_count = "completed", "ok", 1
        step.metadata_json = {**(step.metadata_json or {}), "duration_ms": 1.0, **deepcopy(ANSWER)}

        def change(run: Run, metadata: Metadata) -> None:
            step.started_at = run.started_at
            step.finished_at = run.started_at + timedelta(milliseconds=1)
            metadata["analytical"]["measurement_started_at"] = None
            metadata["analytical"]["warmup_finished_at"] = None

        _edit(session, run_id, change)

    with pytest.raises(PublishValidationError, match="requests overlap"):
        _validate(_db(tmp_path, add))


@pytest.mark.parametrize("conflict", ["hash", "rows"])
@pytest.mark.parametrize("within_stream", [False, True])
def test_answer_conflicts_block_analytical_publication(tmp_path: Path, conflict: str, within_stream: bool) -> None:
    def add(session: Session) -> None:
        run_id = _analytical(session)
        step = _step(session, run_id)
        if not within_stream:
            for request in session.scalars(select(RunStep).where(RunStep.run_id == run_id)).all():
                if request.query_name == step.query_name:
                    if conflict == "hash":
                        request.metadata_json = {
                            **(request.metadata_json or {}),
                            "answer_hash": "blake2b128:" + "a" * 32,
                        }
                    else:
                        request.row_count = 2
                        request.metadata_json = {**(request.metadata_json or {}), "answer_rows": 2, "answer_cells": 2}
        elif conflict == "hash":
            step.metadata_json = {**(step.metadata_json or {}), "answer_hash": "blake2b128:" + "a" * 32}
        else:
            step.row_count = 2
            step.metadata_json = {**(step.metadata_json or {}), "answer_rows": 2, "answer_cells": 2}

    path = _db(tmp_path, add)
    assert validate_results(db_path=path, pins=PINS).ok
    with pytest.raises(PublishValidationError, match="analytical answer conflict"):
        _validate(path)


@pytest.mark.parametrize("different", ["input", "hash_version"])
def test_different_input_or_hash_version_is_not_compared_to_select(tmp_path: Path, different: str) -> None:
    def add(session: Session) -> None:
        run_id = _analytical(session)
        if different == "input":

            def change(_run: Run, metadata: Metadata) -> None:
                metadata["input"]["files"][0]["bytes"] = 2
                metadata["input"]["manifest_sha256"] = sha256_text(f"fixture\0{2}\0{'a' * 64}\n")

            _edit(session, run_id, change)
        for step in session.scalars(select(RunStep).where(RunStep.run_id == run_id)).all():
            step.metadata_json = {
                **(step.metadata_json or {}),
                "answer_hash": "blake2b128:" + "a" * 32,
                **({"answer_hash_version": "canonical-other"} if different == "hash_version" else {}),
            }

    _validate(_db(tmp_path, add))


def test_campaign_publication_preserves_analytical_track_and_original_provenance(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def add(session: Session) -> None:
        _analytical(session, clients=1)
        _analytical(session, start=2, clients=4)
        _analytical(session, start=3, clients=16)
        _hard_abort(session, start=4)

    source = _db(tmp_path / "source", add)
    original = source.read_bytes()
    published = tmp_path / "published"
    monkeypatch.setattr(get_settings(), "results_directory", source.parent)
    monkeypatch.setattr(results_module, "PUBLISHED_DATA_DIRECTORY", published)
    monkeypatch.setattr(results_module, "campaign_cells", lambda: CELLS)
    monkeypatch.setattr(results_module, "current_pins", lambda: PINS)
    monkeypatch.setattr(results_module, "current_drivers", lambda: DRIVERS)
    results_module.publish(revision="campaign", campaign=True, report_url="https://example.com/report")
    assert source.read_bytes() == original
    manifest = json.loads((published / "manifest.json").read_text())
    assert manifest["publication"]["analytical_runs"] == 3
    assert manifest["publication"]["analytical_track"] == ANALYTICAL_PROFILE.id
    assert manifest["publication"]["select_cohorts"] == 2
    assert manifest["table_counts"]["run"] == 5
    assert manifest["results_db_sha256"] == hashlib.sha256((published / "results.db").read_bytes()).hexdigest()
    engine = get_results_engine(db_path=published / "results.db")
    try:
        with Session(engine) as session:
            runs = session.scalars(select(Run).where(Run.operation == "analytical").order_by(Run.id)).all()
            assert [(run.id, run.status) for run in runs] == [(4, "completed"), (5, "completed"), (6, "failed")]
            failed = runs[-1]
            evidence = (failed.metadata_json or {})["analytical"]
            assert "overall" not in evidence and "per_query" not in evidence and "worker_runtime" not in evidence
            request = _step(session, failed.id)
            assert request.status == "running" and request.result_status is None and request.finished_at is None
            assert request.row_count is None and request.error_type is None and request.error_message is None
            assert all(run.system_snapshot_id == 1 for run in runs)
            assert all(
                (run.metadata_json or {})["repository"] == {"commit": "analytical-commit", "dirty": False}
                for run in runs
            )
    finally:
        engine.dispose()


def test_matrix_publication_rejects_analytical_profile(tmp_path: Path) -> None:
    path = _db(tmp_path, lambda session: _analytical(session))
    with pytest.raises(PublishValidationError, match="exactly one benchmark profile"):
        _validate(path, campaign=False)


def test_analytical_runs_cannot_change_system_or_snapshot_policy(tmp_path: Path) -> None:
    def add(session: Session) -> None:
        session.add(SystemSnapshot(id=2, system="test"))
        run_id = _analytical(session)
        _edit(session, run_id, lambda run, _metadata: setattr(run, "system_snapshot_id", 2))

    with pytest.raises(PublishValidationError, match="exactly one system snapshot"):
        _validate(_db(tmp_path, add))


@pytest.mark.parametrize("db", ["polars", "postgres", "clickhouse"])
@pytest.mark.parametrize("problem", [None, "budget", "worker", "missing_worker"])
def test_all_supported_engines_require_actual_shared_budget_and_worker_readback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    db: DatabaseName,
    problem: str | None,
) -> None:
    monkeypatch.setitem(PINS, db, "fixture-version")
    monkeypatch.setitem(DRIVERS, db, None)

    def add(session: Session) -> None:
        _normal(session, db=db)
        run_id = _analytical(session, db=db)

        def change(_run: Run, metadata: Metadata) -> None:
            evidence = metadata["analytical"]
            if problem == "budget":
                if db == "polars":
                    evidence["budget"]["effective"]["engine_memory_limit_bytes"] = 24 * 1024**3
                else:
                    evidence["budget"]["effective"]["fixture-server"]["NanoCpus"] = 160_000_000_000
            elif problem == "worker":
                worker = evidence["worker_runtime"]["0"]
                if db == "polars":
                    worker["runtime"]["thread_pool_size"] = 16
                elif db == "postgres":
                    worker["settings"]["transaction_read_only"]["setting"] = "off"
                else:
                    worker["settings"]["use_query_cache"] = "1"
            elif problem == "missing_worker":
                evidence["worker_runtime"].pop("0")

        _edit(session, run_id, change)

    path = _db(tmp_path, add)
    cells = [*CELLS, MatrixCell(db, "tpc_h", 10, "select")]
    if problem is None:
        _validate(path, cells=cells)
    else:
        with pytest.raises(PublishValidationError, match="budget|worker runtime"):
            _validate(path, cells=cells)


@pytest.mark.parametrize("status", ["error", "timeout"])
def test_failed_analytical_request_summaries_match_errors_without_scoring_them(tmp_path: Path, status: str) -> None:
    def add(session: Session) -> None:
        run_id = _analytical(session)
        step = _step(session, run_id)
        step.status, step.result_status, step.row_count = "failed", status, None
        step.error_type, step.error_message = "TimeoutError" if status == "timeout" else "QueryError", "request failed"
        step.metadata_json = {
            key: value for key, value in (step.metadata_json or {}).items() if not key.startswith("answer_")
        }
        step.metadata_json = {**(step.metadata_json or {}), "request_status": status}

        def change(run: Run, metadata: Metadata) -> None:
            run.status, run.error_type, run.error_message = "failed", "AnalyticalWorkloadError", "request failed"
            evidence = metadata["analytical"]
            evidence["cancelled"] = True
            summary = {
                "offered": 1,
                "completed": 0,
                "errors": int(status == "error"),
                "timeouts": int(status == "timeout"),
            }
            evidence["overall"] = summary
            evidence["per_query"] = {str(step.query_name): summary}

        _edit(session, run_id, change)

    path = _db(tmp_path, add)
    before = path.read_bytes()
    _validate(path)
    assert path.read_bytes() == before


@pytest.mark.parametrize("variant", ["deadline", "options"])
def test_config_fingerprint_keeps_distinct_configuration_variants(tmp_path: Path, variant: str) -> None:
    def add(session: Session) -> None:
        _analytical(session)
        run_id = _analytical(session, start=2)

        def change(_run: Run, metadata: Metadata) -> None:
            if variant == "deadline":
                metadata["settings"]["OLAP_BENCHMARKS_QUERY_TIMEOUT_SECONDS"] = 120
            else:
                metadata["options"] = {"configuration_variant": "fixture"}

        _edit(session, run_id, change)

    path = _db(tmp_path, add)
    assert _selected(path) == {1, 2, 3, 4}


def test_analytical_wrong_result_marks_block_publication_before_select_mark_clearing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def add(session: Session) -> None:
        run_id = _analytical(session)
        step = _step(session, run_id)
        step.result_status, step.error_type, step.error_message = (
            "wrong_result",
            "AnswerHashMismatch",
            "explicit conflict",
        )

    source = _db(tmp_path / "source", add)
    original = source.read_bytes()
    published = tmp_path / "published"
    published.mkdir()
    for name in ("results.db", "manifest.json", "comparison.json"):
        (published / name).write_text("previous publication")
    monkeypatch.setattr(get_settings(), "results_directory", source.parent)
    monkeypatch.setattr(results_module, "PUBLISHED_DATA_DIRECTORY", published)
    monkeypatch.setattr(results_module, "campaign_cells", lambda: CELLS)
    monkeypatch.setattr(results_module, "current_pins", lambda: PINS)
    monkeypatch.setattr(results_module, "current_drivers", lambda: DRIVERS)
    with pytest.raises(PublishValidationError, match="invalid analytical outcome"):
        results_module.publish(revision="campaign", campaign=True, report_url="https://example.com/report")
    assert source.read_bytes() == original
    for name in ("results.db", "manifest.json", "comparison.json"):
        assert (published / name).read_text() == "previous publication"
    assert not (published / "results.db.staging").exists()


@pytest.mark.parametrize("query", ["Q27", "Q28"])
@pytest.mark.parametrize("same_engine_peer", [False, True])
def test_answer_comparison_exclusions_apply_only_across_engines(
    tmp_path: Path,
    query: str,
    same_engine_peer: bool,
) -> None:
    cells = [MatrixCell(db, "clickbench", 10, "select") for db in ("duckdb", "polars")]

    def add(session: Session) -> None:
        _normal(session, suite="clickbench")
        if same_engine_peer:
            _normal(session, db="polars", suite="clickbench")
        else:
            names = tuple(query.name for query in _queries("polars", "clickbench"))
            _add_run(session, cells[1], plan=names, status="failed", recorded=())
        run_id = _analytical(session, db="polars", suite="clickbench")
        for step in session.scalars(select(RunStep).where(RunStep.run_id == run_id)).all():
            if step.query_name == query:
                step.metadata_json = {**(step.metadata_json or {}), "answer_hash": "blake2b128:" + "a" * 32}

    path = _results_db(tmp_path, add)
    if query == "Q28" and not same_engine_peer:
        _validate(path, cells=cells)
    else:
        with pytest.raises(PublishValidationError, match="analytical answer conflict"):
            _validate(path, cells=cells)


def test_not_issued_deadline_request_is_retained_without_inflating_summary_counts(tmp_path: Path) -> None:
    def add(session: Session) -> None:
        run_id = _analytical(session)
        measured = _step(session, run_id)
        assert measured.finished_at is not None
        measured.finished_at -= timedelta(seconds=0.4)
        measured.metadata_json = {**(measured.metadata_json or {}), "duration_ms": 59_800.0}
        order = balanced_query_order([query.name for query in _queries()], 99, 0)
        name = order[1]
        time = measured.started_at + timedelta(seconds=59.9)
        session.add(
            RunStep(
                run_id=run_id,
                step_type="query",
                step_name="analytical_request",
                query_name=name,
                status="completed",
                result_status="skipped",
                started_at=time,
                finished_at=time + timedelta(seconds=0.1),
                metadata_json={
                    "request_id": "client-0:measured:1",
                    "client_id": 0,
                    "phase": "measured",
                    "sequence": 1,
                    "query_sha256": next(sha256_text(query.query) for query in _queries() if query.name == name),
                    "request_status": "not_issued",
                    "reason": "issuance deadline reached before fetch",
                },
            )
        )

        def change(_run: Run, metadata: Metadata) -> None:
            summary = summarize_requests(
                [RequestSample(str(measured.query_name), "measured", "completed", 59_800.0)], 60.2
            )
            metadata["analytical"]["overall"] = summary
            metadata["analytical"]["per_query"][str(measured.query_name)] = summary

        _edit(session, run_id, change)

    _validate(_db(tmp_path, add))
