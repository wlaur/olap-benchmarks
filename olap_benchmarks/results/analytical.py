from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from itertools import pairwise
from math import isclose, isfinite
from typing import TYPE_CHECKING, Any, TypeGuard, cast

import duckdb

from ..matrix import MatrixCell
from ..profile import BENCHMARK_PROFILE
from ..query_registry import ANSWER_COMPARISON_EXCLUSIONS
from ..run_metadata import digest_hashes, sha256_text
from ..settings import SuiteName
from .cells import StoredRun
from .hashing import MAX_ANSWER_HASH_CELLS
from .publishing import PublishValidationError

if TYPE_CHECKING:
    from ..analytical_streams import RequestSample

ANALYTICAL_PROFILE_ID = "analytical-streams-v1"
type Record = dict[str, Any]
type AnswerKey = tuple[str, str, int, str, str, str]


def _record(value: object) -> Record:
    return cast(Record, value) if isinstance(value, dict) else {}


def _text(value: object) -> TypeGuard[str]:
    return isinstance(value, str) and bool(value.strip())


def _number(value: object, *, positive: bool = False) -> TypeGuard[float]:
    return (
        type(value) in (int, float)
        and isfinite(cast(float, value))
        and (cast(float, value) > 0 if positive else cast(float, value) >= 0)
    )


def _hex(value: object, size: int = 64) -> TypeGuard[str]:
    return isinstance(value, str) and len(value) == size and all(char in "0123456789abcdef" for char in value)


def _timestamp(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed.astimezone(UTC).replace(tzinfo=None) if parsed.tzinfo is not None else None


def _metadata(con: duckdb.DuckDBPyConnection, ids: Sequence[int]) -> dict[int, Record]:
    return {
        int(row[0]): _record(json.loads(row[1] or "{}"))
        for row in con.execute("select id, metadata from run where id = any(?)", [list(ids)]).fetchall()
    }


def select_analytical_runs(
    con: duckdb.DuckDBPyConnection,
    runs: Sequence[StoredRun],
    cells: Sequence[MatrixCell],
    *,
    pins: Mapping[str, str],
    drivers: Mapping[str, str | None],
) -> set[int]:
    declared = {(cell.db, cell.suite, cell.scale_factor) for cell in cells if cell.operation == "select"}
    eligible = [
        run
        for run in runs
        if run.operation == "analytical"
        and (run.db, run.suite, run.scale_factor) in declared
        and run.benchmark_profile == ANALYTICAL_PROFILE_ID
        and run.db in pins
        and run.db_version == pins[run.db]
        and run.db in drivers
        and run.db_driver == drivers[run.db]
        and _text(run.commit)
        and run.dirty is False
    ]
    metadata = _metadata(con, [run.id for run in eligible])
    finished = {int(row[0]) for row in con.execute("select id from run where finished_at is not null").fetchall()}
    latest: dict[tuple[object, ...], StoredRun] = {}
    for run in eligible:
        recorded = metadata[run.id]
        repository = _record(recorded.get("repository"))
        if "invalidation" in recorded or repository.get("dirty") is not False or not _text(repository.get("commit")):
            continue
        evidence = _record(recorded.get("analytical"))
        clients, duration, seed = (
            evidence.get("clients"),
            evidence.get("duration_seconds"),
            evidence.get("workload_seed"),
        )
        if (
            type(clients) is not int
            or clients not in (1, 4, 16)
            or not _number(duration, positive=True)
            or type(seed) is not int
        ):
            raise PublishValidationError(f"{run.label}: invalid analytical configuration identity")
        config = {
            "manifest": evidence.get("query_manifest"),
            "excluded": evidence.get("excluded_queries"),
            "settings": recorded.get("settings"),
            "input": _record(recorded.get("input")).get("manifest_sha256"),
            "schema": recorded.get("schema"),
            "options": {
                key: value
                for key, value in _record(recorded.get("options")).items()
                if key not in ("note", "storage_accounting", "duckdb_runtime", "duckdb_storage")
            },
            "policies": {
                key: evidence.get(key)
                for key in (
                    "schedule",
                    "warmup",
                    "timed_boundary",
                    "load_model",
                    "timeout_policy",
                    "result_cache",
                    "error_policy",
                    "throughput_boundary",
                    "fetch_method",
                    "percentile_minimum_n",
                )
            },
            "storage": _record(_record(recorded.get("options")).get("duckdb_storage")).get("mode"),
            "runtime_requested": _record(_record(recorded.get("options")).get("duckdb_runtime")).get("requested"),
        }
        fingerprint = sha256_text(json.dumps(config, sort_keys=True, separators=(",", ":")))
        key = (
            run.system,
            run.db,
            run.db_version,
            run.db_driver,
            run.suite,
            run.scale_factor,
            clients,
            duration,
            seed,
            fingerprint,
        )
        previous = latest.get(key)
        if previous is None or (run.started_at, run.id) > (previous.started_at, previous.id):
            latest[key] = run
    unfinished = [
        run.label for run in latest.values() if run.id not in finished or run.status not in ("completed", "failed")
    ]
    if unfinished:
        raise PublishValidationError("Refusing to publish unfinished analytical runs:\n- " + "\n- ".join(unfinished))
    return {run.id for run in latest.values()}


def _current_queries(run: StoredRun, selected: set[str]) -> dict[str, str]:
    from ..dbs import get_databases

    database = next((db for name, db in get_databases().items() if name == run.db), None)
    if database is None or run.suite not in database.suite_registry():
        raise ValueError("unregistered analytical workload")
    benchmark = next(cls for name, cls in database.suite_registry().items() if name == run.suite)(
        db=database, name=cast(SuiteName, run.suite), scale_factor=run.scale_factor
    )
    return {
        name: sha256_text(benchmark.load_query(name)) if name in selected else ""
        for name in benchmark.planned_query_names()
    }


def _input_hash(metadata: Record) -> str | None:
    recorded = _record(metadata.get("input"))
    digest = recorded.get("manifest_sha256")
    files = recorded.get("files")
    if recorded.get("algorithm") != "sha256" or not _hex(digest) or not isinstance(files, list) or not files:
        return None
    hasher = sha256()
    names: list[str] = []
    for raw in cast(list[object], files):
        file = _record(raw)
        name, size, content = file.get("path"), file.get("bytes"), file.get("sha256")
        if not _text(name) or type(size) is not int or size < 0 or not _hex(content):
            return None
        names.append(name)
        hasher.update(f"{name}\0{size}\0{content}\n".encode())
    return digest if names == sorted(set(names)) and hasher.hexdigest() == digest else None


def _contract(run: StoredRun, metadata: Record) -> tuple[Record, tuple[str, ...]]:
    from ..analytical_streams import ANALYTICAL_DATABASES, ANALYTICAL_PROFILE, AnalyticalConfig, analytical_metadata

    evidence = _record(metadata.get("analytical"))
    if run.db not in ANALYTICAL_DATABASES or run.suite not in ("tpc_h", "tpc_ds", "clickbench"):
        raise ValueError("unsupported analytical database or suite")
    if not _matches(metadata.get("benchmark_profile"), ANALYTICAL_PROFILE.metadata()):
        raise ValueError("analytical benchmark profile metadata differs from the current contract")
    mode = "in_process" if run.db in ("duckdb", "polars") else "container"
    if _record(metadata.get("execution")).get("mode") != mode or metadata.get("plan") is not None:
        raise ValueError("analytical execution metadata contradicts its operation or engine")
    clients, duration, seed = evidence.get("clients"), evidence.get("duration_seconds"), evidence.get("workload_seed")
    if (
        type(clients) is not int
        or clients not in (1, 4, 16)
        or not _number(duration, positive=True)
        or type(seed) is not int
    ):
        raise ValueError("invalid analytical configuration")
    expected = analytical_metadata(AnalyticalConfig(clients, duration, seed), (), {})
    dynamic = {
        "clients",
        "duration_seconds",
        "workload_seed",
        "query_manifest",
        "excluded_queries",
        "measurement_started_at",
        "measurement_finished_at",
    }
    if any(not _matches(evidence.get(key), value) for key, value in expected.items() if key not in dynamic):
        raise ValueError("analytical metadata policies differ from the current contract")
    manifest = _record(evidence.get("query_manifest"))
    selected = _record(manifest.get("files"))
    current = _current_queries(run, set(selected))
    excluded = _record(evidence.get("excluded_queries"))
    if (
        not selected
        or not isinstance(evidence.get("excluded_queries"), dict)
        or set(selected) & set(excluded)
        or set(selected) | set(excluded) != set(current)
        or any(selected[name] != current.get(name) for name in selected)
        or manifest != digest_hashes(cast(dict[str, str], selected))
        or any(
            _record(value).get("status") not in ("skipped", "unsupported") or not _text(_record(value).get("reason"))
            for value in excluded.values()
        )
    ):
        raise ValueError("analytical query manifest must match all current queries or explicit exclusions")
    if _input_hash(metadata) is None:
        raise ValueError("analytical run lacks a valid recorded input fingerprint")
    settings = _record(metadata.get("settings"))
    required = {
        "OLAP_BENCHMARKS_BENCHMARK_THREADS": 10,
        "OLAP_BENCHMARKS_BENCHMARK_MEMORY_GB": 24,
        "OLAP_BENCHMARKS_CLIENT_MEMORY_LIMIT_GB": 28,
    }
    timeout = settings.get("OLAP_BENCHMARKS_OPERATION_TIMEOUT_SECONDS")
    query_timeout = settings.get("OLAP_BENCHMARKS_QUERY_TIMEOUT_SECONDS")
    if (
        any(type(settings.get(key)) is not int or settings[key] != value for key, value in required.items())
        or not _number(timeout, positive=True)
        or timeout <= duration
        or (query_timeout is not None and (type(query_timeout) is not int or query_timeout <= 0))
    ):
        raise ValueError("analytical configured budgets or deadlines are invalid")
    return evidence, tuple(name for name in current if name in selected)


def _duckdb_runtime(raw: object) -> bool:
    settings = _record(_record(raw).get("settings"))
    return settings.get("threads") == "10" and settings.get("memory_limit") == "24.0 GiB"


def _runtime_problems(run: StoredRun, metadata: Record, evidence: Record, issued_clients: set[int]) -> list[str]:
    from ..analytical_streams import CLICKHOUSE_REQUEST_SETTINGS

    problems: list[str] = []
    budget = _record(evidence.get("budget"))
    workers = _record(evidence.get("worker_runtime"))
    required = run.status == "completed"
    if not budget and not workers and not required:
        return problems
    if (
        budget.get("configured_threads") != 10
        or budget.get("configured_engine_memory_bytes") != 24 * 1024**3
        or budget.get("client_rss_guard_bytes") != 28 * 1024**3
        or budget.get("multiplied_by_clients") is not False
        or not _text(budget.get("scope"))
        or not _text(budget.get("client_rss_scope"))
    ):
        problems.append("missing or contradictory actual analytical budget evidence")
    effective = _record(budget.get("effective"))
    if run.db == "duckdb":
        valid = _duckdb_runtime(effective)
    elif run.db == "polars":
        valid = effective.get("thread_pool_size") == 10 and effective.get("engine_memory_limit_bytes", 0) is None
    else:
        cpus, memory = 0.0, 0.0
        valid = bool(effective)
        for value in effective.values():
            container = _record(value)
            nano, quota, period, limit = (container.get(key) for key in ("NanoCpus", "CpuQuota", "CpuPeriod", "Memory"))
            cpu = (
                nano / 1e9
                if _number(nano, positive=True)
                else (quota / period if _number(quota, positive=True) and _number(period, positive=True) else 0)
            )
            valid = valid and cpu > 0 and _number(limit, positive=True)
            cpus += cpu
            memory += limit if _number(limit) else 0
        valid = valid and cpus == 10 and memory == 24 * 1024**3
    if not valid:
        problems.append("actual engine budget does not match the shared analytical budget")
    clients = cast(int, evidence["clients"])
    expected_workers = {str(client) for client in range(clients)}
    if set(workers) - expected_workers or (run.status == "completed" and set(workers) != expected_workers):
        problems.append("worker runtime identities do not match the configured clients")
    required_workers = (
        set(range(clients)) if required else issued_clients & {int(key) for key in workers if key in expected_workers}
    )
    for client in sorted(required_workers | {int(key) for key in workers if key in expected_workers and workers[key]}):
        worker = _record(workers.get(str(client)))
        valid = _text(worker.get("cache_attestation"))
        if run.db == "duckdb":
            valid = valid and _duckdb_runtime(worker.get("runtime"))
        elif run.db == "polars":
            valid = valid and _record(worker.get("runtime")).get("thread_pool_size") == 10
        elif run.db == "clickhouse":
            settings = _record(worker.get("settings"))
            valid = valid and all(settings.get(key) == str(value) for key, value in CLICKHOUSE_REQUEST_SETTINGS.items())
            valid = valid and _text(worker.get("session_scope"))
        else:
            settings = _record(worker.get("settings"))
            names = {
                "default_transaction_read_only",
                "transaction_read_only",
                "statement_timeout",
                "shared_buffers",
                "effective_cache_size",
                "work_mem",
                "max_parallel_workers",
                "max_parallel_workers_per_gather",
                "max_worker_processes",
            }
            timeout = _record(metadata.get("settings")).get("OLAP_BENCHMARKS_QUERY_TIMEOUT_SECONDS") or 0
            valid = (
                valid
                and set(settings) == names
                and type(worker.get("backend_pid")) is int
                and all(_text(_record(settings.get(name)).get("setting")) for name in names)
                and all(
                    _record(settings.get(name)).get("setting") == "on"
                    for name in ("default_transaction_read_only", "transaction_read_only")
                )
                and _record(settings.get("statement_timeout")).get("setting") == str(timeout * 1000)
            )
        if not valid:
            problems.append(f"client {client} lacks valid actual worker runtime evidence")
    return problems


@dataclass(frozen=True)
class _Request:
    id: int
    name: str | None
    status: str
    result: str | None
    started: datetime
    finished: datetime | None
    rows: int | None
    error_type: str | None
    error_message: str | None
    metadata: Record
    legacy_identity: bool


def _requests(con: duckdb.DuckDBPyConnection, ids: Sequence[int]) -> dict[int, list[_Request]]:
    requests: dict[int, list[_Request]] = defaultdict(list)
    for row in con.execute(
        """
        select run_id, id, query_name, status, result_status, started_at, finished_at, row_count,
          error_type, error_message, metadata,
          step_name <> 'analytical_request' or iteration is not null or iteration_role is not null
        from run_step where run_id = any(?) and step_type = 'query' order by id
        """,
        [list(ids)],
    ).fetchall():
        requests[int(row[0])].append(
            _Request(
                int(row[1]),
                row[2],
                str(row[3]),
                row[4],
                row[5],
                row[6],
                row[7],
                row[8],
                row[9],
                _record(json.loads(row[10] or "{}")),
                bool(row[11]),
            )
        )
    return requests


def _answer_valid(request: _Request) -> bool:
    metadata = request.metadata
    rows, columns = metadata.get("answer_rows"), metadata.get("answer_columns")
    cells, schema = metadata.get("answer_cells"), metadata.get("answer_schema")
    answer = metadata.get("answer_hash")
    return (
        type(rows) is int
        and rows >= 0
        and rows == request.rows
        and type(columns) is int
        and columns >= 0
        and type(cells) is int
        and cells == rows * columns
        and isinstance(schema, list)
        and len(cast(list[object], schema)) == columns
        and all(
            isinstance(pair, list)
            and len(cast(list[object], pair)) == 2
            and all(isinstance(item, str) for item in cast(list[object], pair))
            for pair in cast(list[Any], schema)
        )
        and _text(metadata.get("answer_hash_version"))
        and (
            (
                isinstance(answer, str)
                and answer.startswith("blake2b128:")
                and _hex(answer[11:], 32)
                and metadata.get("answer_hash_skipped_reason") is None
            )
            or (answer is None and cells > MAX_ANSWER_HASH_CELLS and _text(metadata.get("answer_hash_skipped_reason")))
        )
    )


def _matches(actual: object, expected: object, *, partial: bool = False) -> bool:
    if isinstance(expected, dict):
        if not isinstance(actual, dict):
            return False
        left, right = cast(Record, actual), cast(Record, expected)
        return (set(left) <= set(right) if partial else set(left) == set(right)) and all(
            _matches(value, right[key], partial=partial) for key, value in left.items()
        )
    if isinstance(expected, float):
        return _number(actual) and isclose(actual, expected, rel_tol=1e-7, abs_tol=1e-6)
    return type(actual) is type(expected) and actual == expected


def _request_problems(
    run: StoredRun, evidence: Record, names: tuple[str, ...], requests: Sequence[_Request], finished: datetime
) -> tuple[list[str], set[int]]:
    from ..analytical_streams import RequestSample, balanced_query_order, summarize_requests

    problems: list[str] = []
    issued_clients: set[int] = set()
    positions: dict[tuple[int, str], list[int]] = defaultdict(list)
    successful_warmups: dict[int, set[int]] = defaultdict(set)
    request_times: dict[int, list[tuple[datetime, datetime]]] = defaultdict(list)
    samples: list[RequestSample] = []
    unknown_phases: set[str] = set()
    clients, seed = cast(int, evidence["clients"]), cast(int, evidence["workload_seed"])
    manifest = _record(_record(evidence["query_manifest"])["files"])
    start, end = _timestamp(evidence.get("measurement_started_at")), _timestamp(evidence.get("measurement_finished_at"))
    stop = _timestamp(evidence.get("stop_issuing_at"))
    for request in requests:
        meta = request.metadata
        client, phase, sequence = meta.get("client_id"), meta.get("phase"), meta.get("sequence")
        if (
            type(client) is not int
            or not 0 <= client < clients
            or phase not in ("warmup", "measured")
            or type(sequence) is not int
            or sequence < 0
            or request.legacy_identity
            or meta.get("request_id") != f"client-{client}:{phase}:{sequence}"
            or request.name not in names
            or meta.get("query_sha256") != manifest.get(request.name)
        ):
            problems.append(f"step {request.id} has an invalid analytical request identity or query hash")
            continue
        phase = cast(str, phase)
        positions[client, phase].append(sequence)
        order = balanced_query_order(names, seed, client)
        if request.name != order[sequence % len(order)] or (phase == "warmup" and sequence >= len(names)):
            problems.append(f"step {request.id} violates the recorded seeded query schedule")
        if (
            request.finished is None
            or not run.started_at <= request.started <= request.finished <= finished
            or (phase == "warmup" and start is not None and request.finished > start)
            or (phase == "measured" and start is not None and request.started < start)
            or (phase == "measured" and end is not None and request.finished > end)
        ):
            problems.append(f"step {request.id} has unfinished or contradictory request timestamps")
        else:
            request_times[client].append((request.started, request.finished))
        duration = meta.get("duration_ms")
        if (
            _number(duration)
            and request.finished is not None
            and duration > (request.finished - request.started).total_seconds() * 1000 + 10
        ):
            problems.append(f"step {request.id} fetch duration exceeds the recorded request interval")
        if request.result == "skipped":
            if (
                request.status != "completed"
                or meta.get("request_status") != "not_issued"
                or not _text(meta.get("reason"))
                or request.rows is not None
                or request.error_type is not None
                or request.error_message is not None
                or any(key.startswith("answer_") for key in meta)
                or "duration_ms" in meta
            ):
                problems.append(f"step {request.id} has an invalid not-issued outcome")
            continue
        issued_clients.add(client)
        if phase == "measured" and stop is not None and request.started >= stop:
            problems.append(f"step {request.id} was issued after the analytical deadline")
        if request.status == "completed" and request.result == "ok":
            if (
                not _answer_valid(request)
                or not _number(duration)
                or request.error_type is not None
                or request.error_message is not None
                or meta.get("request_status") not in (None, "completed")
            ):
                problems.append(f"step {request.id} has invalid answer, row count or duration evidence")
            else:
                samples.append(RequestSample(request.name, cast(Any, phase), "completed", duration))
                if phase == "warmup":
                    successful_warmups[client].add(sequence)
        elif request.status == "failed" and request.result in (None, "error", "timeout"):
            if (
                not _text(request.error_type)
                or request.error_message is None
                or request.rows is not None
                or any(key.startswith("answer_") for key in meta)
                or (duration is not None and not _number(duration))
                or meta.get("request_status") not in (None, request.result)
            ):
                problems.append(f"step {request.id} lacks an explicit, unscored request failure")
            if request.result is None:
                unknown_phases.add(phase)
            else:
                samples.append(RequestSample(request.name, cast(Any, phase), request.result, duration or 0.0))
            if run.status == "completed":
                problems.append(f"step {request.id} failed in a completed analytical run")
        else:
            problems.append(f"step {request.id} has an invalid analytical outcome ({request.status}/{request.result})")
    if any(sorted(values) != list(range(len(values))) for values in positions.values()):
        problems.append("analytical request sequences contain duplicates or gaps")
    if any(left[1] > right[0] for times in request_times.values() for left, right in pairwise(sorted(times))):
        problems.append("analytical client requests overlap despite the closed-loop load model")
    if (run.status == "completed" or any(phase == "measured" for _, phase in positions)) and any(
        successful_warmups.get(client, set()) != set(range(len(names))) for client in range(clients)
    ):
        problems.append("measured analytical streams require a full warmup manifest for every client")
    elapsed = evidence.get("actual_elapsed_seconds")
    duration = cast(float, evidence["duration_seconds"])
    if (run.status == "completed" or elapsed is not None) and (
        not _number(elapsed, positive=True)
        or start is None
        or end is None
        or stop is None
        or not run.started_at <= start <= end <= finished
        or not isclose((end - start).total_seconds(), elapsed, rel_tol=1e-4, abs_tol=0.01)
        or not isclose((stop - start).total_seconds(), duration, abs_tol=1e-5)
        or not _number(evidence.get("drain_seconds"))
        or not isclose(evidence["drain_seconds"], max(0, elapsed - duration), abs_tol=0.01)
        or (run.status == "completed" and elapsed < duration)
    ):
        problems.append("analytical elapsed time, measurement bounds or drain evidence is invalid")
    warmup_start, warmup_end = (
        _timestamp(evidence.get("warmup_started_at")),
        _timestamp(evidence.get("warmup_finished_at")),
    )
    for key in (
        "measurement_started_at",
        "measurement_finished_at",
        "stop_issuing_at",
        "warmup_started_at",
        "warmup_finished_at",
    ):
        value = evidence.get(key)
        parsed = _timestamp(value)
        if value is not None and (
            parsed is None or parsed < run.started_at or (key != "stop_issuing_at" and parsed > finished)
        ):
            problems.append(f"analytical {key} timestamp is invalid")
    if ("cancelled" in evidence and type(evidence["cancelled"]) is not bool) or (
        "worker_failures" in evidence
        and (
            not isinstance(evidence["worker_failures"], list)
            or not all(_text(failure) for failure in cast(list[object], evidence["worker_failures"]))
        )
    ):
        problems.append("analytical cancellation or worker failure evidence is invalid")
    if run.status == "completed" and (
        warmup_start is None or warmup_end != start or not run.started_at <= warmup_start <= cast(datetime, warmup_end)
    ):
        problems.append("analytical warmup timing evidence is invalid")
    measured = [sample for sample in samples if sample.phase == "measured"]
    if run.status == "completed" and (
        not measured or evidence.get("cancelled") is not False or evidence.get("worker_failures") != []
    ):
        problems.append("completed analytical run lacks measured requests or records cancellation/worker failures")
    summary_elapsed = elapsed if _number(elapsed, positive=True) else None
    summaries = {
        "overall": summarize_requests(measured, summary_elapsed),
        "warmup_summary": summarize_requests([sample for sample in samples if sample.phase == "warmup"], None),
        "per_query": {
            name: summarize_requests([sample for sample in measured if sample.query_name == name], summary_elapsed)
            for name in names
        },
    }
    for key, expected in summaries.items():
        if key not in evidence and run.status != "completed":
            continue
        phase = "warmup" if key == "warmup_summary" else "measured"
        if phase in unknown_phases or not _matches(evidence.get(key), expected, partial=run.status == "failed"):
            problems.append(f"analytical {key} summary contradicts the recorded requests")
    return problems, issued_clients


def _answer_problems(
    con: duckdb.DuckDBPyConnection,
    runs: Sequence[StoredRun],
    metadata: Mapping[int, Record],
    requests: Mapping[int, list[_Request]],
) -> list[str]:
    answers: dict[AnswerKey, dict[tuple[str, int], set[tuple[int, str | None]]]] = defaultdict(lambda: defaultdict(set))
    for run in runs:
        if run.operation != "analytical":
            continue
        input_hash = _input_hash(metadata[run.id])
        if input_hash is None:
            continue
        for request in requests.get(run.id, []):
            if request.status != "completed" or request.result != "ok" or request.rows is None or request.name is None:
                continue
            version = request.metadata.get("answer_hash_version")
            if not _text(version):
                continue
            key = (run.system, run.suite, run.scale_factor, input_hash, request.name, version)
            answers[key][run.db, run.id].add((request.rows, request.metadata.get("answer_hash")))
    select_ids = [run.id for run in runs if run.operation == "select" and run.benchmark_profile == BENCHMARK_PROFILE.id]
    by_id = {run.id: run for run in runs}
    for row in con.execute(
        """select run_id, query_name, row_count, json_extract_string(metadata, '$.answer_hash'),
          json_extract_string(metadata, '$.answer_hash_version') from run_step
        where run_id = any(?) and step_type = 'query' and status = 'completed'
          and result_status in ('ok', 'wrong_result') and row_count is not null""",
        [select_ids],
    ).fetchall():
        run = by_id[int(row[0])]
        input_hash = _input_hash(metadata[run.id])
        if input_hash is None or row[1] is None or not _text(row[4]):
            continue
        key = (run.system, run.suite, run.scale_factor, input_hash, str(row[1]), str(row[4]))
        if key in answers:
            answers[key][run.db, run.id].add((int(row[2]), row[3]))
    problems: list[str] = []
    for key, observations in answers.items():
        _, suite, scale, _, query, version = key
        entries = list(observations.items())
        for index, ((db, run_id), values) in enumerate(entries):
            if len({rows for rows, _ in values}) > 1 or len({answer for _, answer in values if answer is not None}) > 1:
                problems.append(
                    f"analytical answer conflict within run {run_id} {db} {suite}:sf{scale} {query} ({version})"
                )
            for (other_db, other_id), other_values in entries[index + 1 :]:
                if db != other_db and any(
                    (suite, query, excluded_db) in ANSWER_COMPARISON_EXCLUSIONS for excluded_db in (None, db, other_db)
                ):
                    continue
                combined = values | other_values
                if (
                    len({rows for rows, _ in combined}) > 1
                    or len({answer for _, answer in combined if answer is not None}) > 1
                ):
                    problems.append(
                        f"analytical answer conflict: runs {run_id}/{other_id} {suite}:sf{scale} {query} ({version})"
                    )
    return problems


def analytical_run_problems(con: duckdb.DuckDBPyConnection, runs: Sequence[StoredRun]) -> list[str]:
    analytical = [run for run in runs if run.operation == "analytical"]
    if not analytical:
        return []
    metadata = _metadata(con, [run.id for run in runs])
    requests = _requests(con, [run.id for run in analytical])
    terminal = {
        int(row[0]): row[1:]
        for row in con.execute(
            """select r.id, r.finished_at, r.error_type, r.error_message, s.system from run r
        left join system_snapshot s on s.id = r.system_snapshot_id where r.id = any(?)""",
            [[run.id for run in analytical]],
        ).fetchall()
    }
    problems: list[str] = []
    for run in analytical:
        finished, error_type, error_message, snapshot_system = terminal[run.id]
        try:
            evidence, names = _contract(run, metadata[run.id])
        except (ValueError, OSError) as exc:
            problems.append(f"{run.label}: {exc}")
            continue
        if snapshot_system != run.system:
            problems.append(f"{run.label}: missing or contradictory original system snapshot")
        if finished is None or finished < run.started_at:
            problems.append(f"{run.label}: missing or contradictory terminal timestamp")
            continue
        if run.status == "failed" and (not _text(error_type) or error_message is None):
            problems.append(f"{run.label}: failed analytical run requires an explicit run error")
        if run.status == "completed" and (error_type is not None or error_message is not None):
            problems.append(f"{run.label}: completed analytical run records a run error")
        request_problems, issued = _request_problems(run, evidence, names, requests.get(run.id, []), finished)
        problems.extend(f"{run.label}: {problem}" for problem in request_problems)
        problems.extend(
            f"{run.label}: {problem}" for problem in _runtime_problems(run, metadata[run.id], evidence, issued)
        )
    problems.extend(_answer_problems(con, runs, metadata, requests))
    return problems
