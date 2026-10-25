from __future__ import annotations

import logging
from collections.abc import Generator, Mapping, Sequence
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from math import ceil, isfinite
from pathlib import Path
from queue import SimpleQueue
from random import Random
from threading import Barrier, BrokenBarrierError, Event, Thread
from time import perf_counter
from typing import TYPE_CHECKING, Any, Literal, cast

import polars as pl
from sqlalchemy import text

from .limits import start_query_watchdog
from .profile import BenchmarkProfile
from .query_registry import ANSWER_ORDER_KEYS
from .results.hashing import build_answer_metadata
from .run_metadata import digest_hashes, is_query_timeout, sha256_text
from .settings import DatabaseName, get_settings

if TYPE_CHECKING:
    from .dbs import Database
    from .suites import BenchmarkSuite

_LOGGER = logging.getLogger(__name__)
type AnalyticalSuite = Literal["tpc_h", "tpc_ds", "clickbench"]
type RequestPhase = Literal["warmup", "measured"]
type RequestStatus = Literal["completed", "error", "timeout"]
ANALYTICAL_DATABASES: frozenset[DatabaseName] = frozenset({"duckdb", "polars", "postgres", "clickhouse"})
ANALYTICAL_THREADS = 10
ANALYTICAL_ENGINE_MEMORY_GB = 24
ANALYTICAL_CLIENT_RSS_GB = 28
CLICKHOUSE_REQUEST_SETTINGS = {
    "use_query_cache": 0,
    "aggregate_functions_null_for_empty": 1,
    "readonly": 2,
    "max_threads": ANALYTICAL_THREADS,
    "max_memory_usage": ANALYTICAL_ENGINE_MEMORY_GB * 1024**3,
}
PERCENTILE_MINIMUM_N = {50: 5, 95: 100, 99: 1_000}


class _AnalyticalProfile(BenchmarkProfile):
    def metadata(self) -> dict[str, object]:
        return {
            **super().metadata(),
            "measured_iterations": None,
            "total_iterations": None,
            "iteration_policy": "one warmup per query per client, then synchronized duration-based streams; "
            "request phase and ids live in step metadata, not resident-hot iteration roles",
        }


ANALYTICAL_PROFILE = _AnalyticalProfile(
    id="analytical-streams-v1",
    warmup_iterations=1,
    measured_iterations=1,
    restart_server_before_select=False,
    timed_boundary="perf_counter around native fetch until the full Polars DataFrame is materialized; "
    "answer hashing and request logging are outside latency, inside closed-loop throughput",
    cache_policy="immutable data; one full query-manifest warmup per client; resident data/predicate caches retained; "
    "no harness result cache; engine cache attestation only where runtime evidence records it",
    scope="opt-in analytical operation only; duration-based closed-loop streams, not fixed SELECT iterations",
)


@dataclass(frozen=True, slots=True)
class AnalyticalConfig:
    clients: int = 1
    duration_seconds: float = 60
    seed: int = 0

    def __post_init__(self) -> None:
        if self.clients not in (1, 4, 16):
            raise ValueError("Analytical clients must be 1, 4 or 16")
        if not isfinite(self.duration_seconds) or self.duration_seconds <= 0:
            raise ValueError("Analytical duration_seconds must be finite and positive")


def validate_analytical_config(database: Database, config: AnalyticalConfig) -> None:
    if database.name not in ANALYTICAL_DATABASES:
        raise ValueError(f"Analytical streams are not implemented for {database.name}")
    settings = get_settings()
    if settings.benchmark_threads != ANALYTICAL_THREADS or settings.benchmark_memory_gb != ANALYTICAL_ENGINE_MEMORY_GB:
        raise ValueError("Analytical streams require benchmark_threads=10 and benchmark_memory_gb=24 for all clients")
    if settings.client_memory_limit_gb != ANALYTICAL_CLIENT_RSS_GB:
        raise ValueError(
            "Analytical streams require client_memory_limit_gb=28 as a separate operation-process RSS guard"
        )
    if settings.operation_timeout_seconds is None or settings.operation_timeout_seconds <= config.duration_seconds:
        raise ValueError(
            "Analytical streams require operation_timeout_seconds greater than duration_seconds, "
            "with room for warmup and draining"
        )


def balanced_query_order(query_names: Sequence[str], seed: int, client_id: int) -> tuple[str, ...]:
    if not query_names or len(set(query_names)) != len(query_names):
        raise ValueError("Analytical query names must be nonempty and unique")
    names = list(query_names)
    Random(seed).shuffle(names)
    offset = client_id % len(names)
    return tuple(names[offset:] + names[:offset])


def latency_summary(durations_ms: Sequence[float]) -> dict[str, object]:
    ordered = sorted(durations_ms)
    n = len(ordered)
    return {
        "n": n,
        "percentile_method": "nearest_rank",
        **{
            f"p{percentile}": {
                "value_ms": ordered[ceil(n * percentile / 100) - 1] if n >= minimum else None,
                "status": "available" if n >= minimum else "insufficient_samples",
                "minimum_n": minimum,
            }
            for percentile, minimum in PERCENTILE_MINIMUM_N.items()
        },
    }


@dataclass(frozen=True, slots=True)
class AnalyticalQuery:
    name: str
    query: str
    fetch_kwargs: Mapping[str, Any]


def prepare_analytical_queries(benchmark: BenchmarkSuite[Any]) -> tuple[tuple[AnalyticalQuery, ...], dict[str, object]]:
    if benchmark.name not in ("tpc_h", "tpc_ds", "clickbench"):
        raise ValueError("Analytical streams support only tpc_h, tpc_ds and clickbench")
    queries: list[AnalyticalQuery] = []
    excluded: dict[str, object] = {}
    for name in benchmark.planned_query_names():
        skip = benchmark.query_skip(name)
        if skip is not None:
            excluded[name] = {"status": skip[0], "reason": skip[1]}
            continue
        kwargs = deepcopy(dict(benchmark.query_fetch_kwargs(name)))
        if benchmark.db.name == "postgres":
            kwargs["method"] = "python"
        elif benchmark.db.name == "clickhouse":
            settings = dict(cast(Mapping[str, Any], kwargs.get("settings", {})))
            settings.update(CLICKHOUSE_REQUEST_SETTINGS)
            kwargs["settings"] = settings
        queries.append(AnalyticalQuery(name, benchmark.load_query(name), kwargs))
    balanced_query_order([query.name for query in queries], 0, 0)
    return tuple(queries), excluded


def analytical_metadata(
    config: AnalyticalConfig, queries: Sequence[AnalyticalQuery], excluded: Mapping[str, object]
) -> dict[str, Any]:
    return {
        "version": 1,
        "track": ANALYTICAL_PROFILE.id,
        "clients": config.clients,
        "duration_seconds": config.duration_seconds,
        "measurement_started_at": None,
        "measurement_finished_at": None,
        "workload_seed": config.seed,
        "schedule": "per-client cyclic seeded permutation, rotated by client id; each complete cycle visits "
        "every selected query once; deterministic per-client order, timing-dependent request counts",
        "query_manifest": digest_hashes({query.name: sha256_text(query.query) for query in queries}),
        "excluded_queries": dict(excluded),
        "warmup": {"iterations_per_query_per_client": 1, "synchronized_measurement_after_all_clients": True},
        "timed_boundary": ANALYTICAL_PROFILE.timed_boundary,
        "load_model": "closed_loop; one outstanding fetch per client; no target request rate",
        "percentile_minimum_n": {str(k): v for k, v in PERCENTILE_MINIMUM_N.items()},
        "timeout_policy": "optional parent per-worker fetch watchdog, polled about once per second, "
        "aborts the whole operation; "
        "operation watchdog includes warmup and drain; no signal alarms in worker threads; "
        "hard-aborted requests remain running steps until the run is marked failed",
        "result_cache": {
            "harness": "none",
            "engine_attestation": "not inferred; see recorded runtime evidence",
            "clickhouse_request_settings": dict(CLICKHOUSE_REQUEST_SETTINGS),
        },
        "error_policy": "first request or worker error stops new requests across all clients; in-flight calls drain",
        "throughput_boundary": "synchronized release through the last client leaving its request loop; "
        "includes answer hashing and request logging, excludes session cleanup",
        "fetch_method": "existing native fetch; PostgreSQL requests explicitly use method=python (psycopg) "
        "for a persistent session; Polars uses independent expression state and the process-wide pool",
    }


def read_effective_budget(database: Database) -> dict[str, object]:
    from .dbs.duckdb import DuckDB
    from .metrics.measure import get_docker_client

    settings = get_settings()
    budget: dict[str, object] = {
        "configured_threads": settings.benchmark_threads,
        "configured_engine_memory_bytes": cast(int, settings.benchmark_memory_gb) * 1024**3,
        "client_rss_guard_bytes": settings.client_memory_limit_gb * 1024**3
        if settings.client_memory_limit_gb is not None
        else None,
        "client_rss_scope": "sampled RSS of the isolated operation process, all client threads included; "
        "not a cgroup limit and not whole-host memory",
        "multiplied_by_clients": False,
    }
    if isinstance(database, DuckDB):
        effective = database.runtime_metadata()
        budget.update(
            scope="one shared DuckDB database instance across connections; global engine thread and "
            "memory settings; RSS guard also covers Python/Arrow; auxiliary native threads are not a CPU quota",
            effective=effective,
        )
    elif database.name == "polars":
        threads = pl.thread_pool_size()
        if threads != settings.benchmark_threads:
            raise ValueError(
                f"Polars shared pool has {threads} threads; requested {settings.benchmark_threads}. "
                "Set POLARS_MAX_THREADS before starting olap"
            )
        budget.update(
            scope="shared process-wide Polars pool; native engine allocations have no separate hard "
            "memory cap; operation-process RSS guard enforces the configured client limit",
            effective={"thread_pool_size": threads, "engine_memory_limit_bytes": None},
        )
    else:
        containers: dict[str, object] = {}
        total_cpus = 0.0
        total_memory = 0
        for name in database.metric_container_names:
            host_config = cast(dict[str, Any], get_docker_client().containers.get(name).attrs["HostConfig"])
            nano_cpus = int(host_config.get("NanoCpus", 0))
            quota, period = int(host_config.get("CpuQuota", 0)), int(host_config.get("CpuPeriod", 0))
            cpus = nano_cpus / 1e9 if nano_cpus > 0 else quota / period if quota > 0 and period > 0 else 0
            memory = int(host_config["Memory"])
            if cpus <= 0 or memory <= 0:
                raise ValueError(f"Container {name} lacks a finite CPU/memory limit")
            total_cpus += cpus
            total_memory += memory
            containers[name] = {
                key: host_config.get(key)
                for key in ("NanoCpus", "CpuQuota", "CpuPeriod", "CpusetCpus", "Memory", "MemorySwap")
            }
        if total_cpus != settings.benchmark_threads or total_memory != budget["configured_engine_memory_bytes"]:
            raise ValueError("Actual container CPU/memory limits do not match the fixed analytical budget")
        budget.update(
            scope="server container CPU quota and memory cgroup charge including page cache; "
            "client RSS sampled separately; not whole-host memory",
            effective=containers,
        )
    return budget


def read_worker_runtime(worker: Database) -> dict[str, object]:
    from .dbs.clickhouse import Clickhouse
    from .dbs.duckdb import DuckDB

    if isinstance(worker, DuckDB):
        return {
            "runtime": worker.runtime_metadata(),
            "cache_attestation": "settings readback only; result-cache hits are not instrumented",
        }
    if worker.name == "polars":
        return {
            "runtime": {"thread_pool_size": pl.thread_pool_size()},
            "cache_attestation": "no harness result cache; engine cache hits are not instrumented",
        }
    if worker.name == "postgres":
        connection = worker.connect()
        settings = {
            str(row["name"]): dict(row)
            for row in connection.execute(
                text(
                    "SELECT name, setting, unit, source FROM pg_settings WHERE name IN "
                    "('default_transaction_read_only', 'transaction_read_only', 'statement_timeout', "
                    "'shared_buffers', 'effective_cache_size', 'work_mem', 'max_parallel_workers', "
                    "'max_parallel_workers_per_gather', 'max_worker_processes') ORDER BY name"
                )
            ).mappings()
        }
        if settings["default_transaction_read_only"]["setting"] != "on":
            raise ValueError("PostgreSQL analytical session is not read-only")
        if settings["transaction_read_only"]["setting"] != "on":
            raise ValueError("PostgreSQL analytical transaction is not read-only")
        expected_timeout = (get_settings().query_timeout_seconds or 0) * 1_000
        if int(settings["statement_timeout"]["setting"]) != expected_timeout:
            raise ValueError("PostgreSQL analytical statement_timeout does not match the configured deadline")
        return {
            "settings": settings,
            "backend_pid": connection.execute(text("SELECT pg_backend_pid()")).scalar_one(),
            "cache_attestation": "runtime memory/settings read back; cache hits are not instrumented",
        }
    if isinstance(worker, Clickhouse):
        frame = worker.fetch(
            "SELECT name, value, if(name = 'max_threads', toString(getSetting('max_threads')), value) "
            "AS effective_value FROM system.settings WHERE name IN "
            "('readonly', 'max_threads', 'max_memory_usage', 'max_execution_time', "
            "'aggregate_functions_null_for_empty') "
            "OR name LIKE '%cache%' ORDER BY name",
            settings=CLICKHOUSE_REQUEST_SETTINGS,
        )
        actual = {str(row["name"]): str(row["effective_value"]) for row in frame.iter_rows(named=True)}
        for name, value in CLICKHOUSE_REQUEST_SETTINGS.items():
            if actual.get(name) != str(value):
                raise ValueError(
                    f"ClickHouse analytical setting {name}: requested {value}, effective {actual.get(name)}"
                )
        return {
            "settings": actual,
            "settings_table": {str(row["name"]): str(row["value"]) for row in frame.iter_rows(named=True)},
            "cache_attestation": "settings probe on the same native client with the common request settings; "
            "max_threads uses numeric getSetting readback; query-log cache hits are not instrumented",
            "session_scope": "one independent native HTTP client per worker; server session ids are disabled",
        }
    raise ValueError(f"Analytical runtime readback is not implemented for {type(worker).__name__}")


@contextmanager
def analytical_worker(database: Database) -> Generator[Database]:
    worker = type(database).model_validate(database.model_dump())
    worker.current_query_name = None
    worker._current_suite = database.current_suite
    worker._current_suite_scale_factor = database.current_suite_scale_factor
    worker._current_operation = "analytical"
    worker._result_storage = database.result_storage
    worker._run_id = database.run_id
    try:
        if worker.name == "postgres":
            connection = worker.connect()
            connection.execute(text("SET default_transaction_read_only = on"))
            timeout = get_settings().query_timeout_seconds or 0
            connection.execute(text(f"SET statement_timeout = {timeout * 1_000}"))
            connection.commit()
        yield worker
    finally:
        try:
            worker.close_connection()
        finally:
            if worker.name == "clickhouse":
                worker.reset_session()


@dataclass(frozen=True, slots=True)
class RequestSample:
    query_name: str
    phase: RequestPhase
    status: RequestStatus
    duration_ms: float


def execute_analytical_request(
    worker: Database,
    query: AnalyticalQuery,
    client_id: int,
    sequence: int,
    phase: RequestPhase,
    watchdog: Path | None,
    *,
    deadline: float | None = None,
    cancel: Event | None = None,
) -> RequestSample | None:
    request_id = f"client-{client_id}:{phase}:{sequence}"
    metadata: dict[str, Any] = {
        "request_id": request_id,
        "client_id": client_id,
        "phase": phase,
        "sequence": sequence,
        "query_sha256": sha256_text(query.query),
    }
    step = worker._start_step("query", "analytical_request", query_name=query.name, metadata=metadata)
    worker._push_active_step(step)
    duration_ms = 0.0
    try:
        with worker.query_context(query.name):
            kwargs = deepcopy(dict(query.fetch_kwargs))
            start_query_watchdog(watchdog, request_id)
            t0 = perf_counter()
            try:
                if (cancel is not None and cancel.is_set()) or (deadline is not None and t0 >= deadline):
                    worker._finish_step(
                        step,
                        "completed",
                        "query",
                        result_status="skipped",
                        metadata={
                            **metadata,
                            "request_status": "not_issued",
                            "reason": "cancelled or issuance deadline reached before fetch",
                        },
                    )
                    return None
                frame = worker.fetch(query.query, **kwargs)
            finally:
                duration_ms = (perf_counter() - t0) * 1_000
                if watchdog is not None:
                    watchdog.unlink(missing_ok=True)
        order_keys = (
            ANSWER_ORDER_KEYS.get((worker._current_suite, query.name)) if worker._current_suite is not None else None
        )
        answer = (
            build_answer_metadata(frame) if order_keys is None else build_answer_metadata(frame, order_keys=order_keys)
        )
        worker._finish_timed_step(
            step, "completed", "query", row_count=frame.height, duration_ms=duration_ms, metadata={**metadata, **answer}
        )
        return RequestSample(query.name, phase, "completed", duration_ms)
    except BaseException as exc:
        timeout = isinstance(exc, TimeoutError) or is_query_timeout(
            error_type=type(exc).__name__, error_message=str(exc)
        )
        status = "timeout" if timeout else "error"
        worker._finish_timed_step(
            step,
            "failed",
            "query",
            duration_ms=duration_ms,
            error_type=type(exc).__name__,
            error_message=str(exc),
            result_status=status,
            metadata={**metadata, "request_status": status},
        )
        if not isinstance(exc, Exception):
            raise
        _LOGGER.exception("Analytical request %s failed", request_id)
        return RequestSample(query.name, phase, status, duration_ms)
    finally:
        worker._pop_active_step(step)


def summarize_requests(samples: Sequence[RequestSample], elapsed_seconds: float | None) -> dict[str, object]:
    completed = [sample.duration_ms for sample in samples if sample.status == "completed"]
    return {
        "offered": len(samples),
        "completed": len(completed),
        "errors": sum(sample.status == "error" for sample in samples),
        "timeouts": sum(sample.status == "timeout" for sample in samples),
        "throughput_per_second": len(completed) / elapsed_seconds if elapsed_seconds and elapsed_seconds > 0 else None,
        "latency_ms": latency_summary(completed),
    }


@dataclass(slots=True)
class _WorkerState:
    samples: list[RequestSample] = field(default_factory=lambda: list[RequestSample]())
    runtime: dict[str, object] = field(default_factory=lambda: dict[str, object]())
    finished_at: float | None = None
    finished_at_utc: str | None = None


class AnalyticalWorkloadError(RuntimeError):
    pass


def run_analytical_streams(
    database: Database,
    config: AnalyticalConfig,
    queries: Sequence[AnalyticalQuery],
    metadata: dict[str, Any],
    *,
    cancel: Event | None = None,
) -> None:
    balanced_query_order([query.name for query in queries], config.seed, 0)
    metadata["budget"] = read_effective_budget(database)
    stop = cancel if cancel is not None else Event()
    ready = Event()
    measurement_gate = Event()
    failures: SimpleQueue[BaseException] = SimpleQueue()
    states = [_WorkerState() for _ in range(config.clients)]
    by_name = {query.name: query for query in queries}
    measurement_start: float | None = None
    deadline: float | None = None
    warmup_start = datetime.now(UTC).isoformat()

    def begin_measurement() -> None:
        nonlocal measurement_start, deadline
        if stop.is_set():
            raise AnalyticalWorkloadError("Analytical warmup was cancelled")
        measurement_start = perf_counter()
        deadline = measurement_start + config.duration_seconds
        started_at = datetime.now(UTC)
        metadata["measurement_started_at"] = started_at.isoformat()
        metadata["stop_issuing_at"] = (started_at + timedelta(seconds=config.duration_seconds)).isoformat()
        metadata["warmup_finished_at"] = started_at.isoformat()

    barrier = Barrier(config.clients, action=ready.set)

    def client(client_id: int) -> None:
        try:
            with analytical_worker(database) as worker:
                state = states[client_id]
                try:
                    if stop.is_set():
                        barrier.abort()
                        return
                    state.runtime = read_worker_runtime(worker)
                    watchdog = (
                        database._query_watchdog_path / f"client-{client_id}.json"
                        if database._query_watchdog_path is not None
                        else None
                    )
                    order = balanced_query_order(tuple(by_name), config.seed, client_id)

                    def request(sequence: int, phase: RequestPhase) -> bool:
                        sample = execute_analytical_request(
                            worker,
                            by_name[order[sequence % len(order)]],
                            client_id,
                            sequence,
                            phase,
                            watchdog,
                            deadline=deadline if phase == "measured" else None,
                            cancel=stop,
                        )
                        if sample is None:
                            return False
                        state.samples.append(sample)
                        if sample.status != "completed":
                            stop.set()
                            return False
                        return True

                    for sequence in range(len(order)):
                        if stop.is_set() or not request(sequence, "warmup"):
                            barrier.abort()
                            return
                    barrier.wait()
                    measurement_gate.wait()
                    sequence = 0
                    while not stop.is_set() and deadline is not None and perf_counter() < deadline:
                        if not request(sequence, "measured"):
                            break
                        sequence += 1
                finally:
                    state.finished_at = perf_counter()
                    state.finished_at_utc = datetime.now(UTC).isoformat()
        except BrokenBarrierError as exc:
            if not stop.is_set():
                failures.put(exc)
                stop.set()
        except BaseException as exc:
            failures.put(exc)
            stop.set()
            barrier.abort()

    threads = [Thread(target=client, args=(i,), name=f"analytical-{i}", daemon=True) for i in range(config.clients)]
    try:
        with database.phase_context("analytical_warmup"):
            for thread in threads:
                thread.start()
            while not ready.wait(timeout=0.1):
                if stop.is_set():
                    raise AnalyticalWorkloadError("Analytical warmup failed or was cancelled")
            if stop.is_set():
                raise AnalyticalWorkloadError("Analytical warmup failed or was cancelled")
        with database.phase_context("analytical_measurement_and_drain"):
            begin_measurement()
            measurement_gate.set()
            for thread in threads:
                thread.join()
            if stop.is_set() or not failures.empty():
                raise AnalyticalWorkloadError("Analytical streams failed or were cancelled; see run metadata and steps")
    except BaseException:
        stop.set()
        barrier.abort()
        raise
    finally:
        measurement_gate.set()
        for thread in threads:
            if thread.ident is not None:
                thread.join()
        last_state = max(states, key=lambda state: state.finished_at or 0.0)
        end = last_state.finished_at if last_state.finished_at is not None else perf_counter()
        elapsed = end - measurement_start if measurement_start is not None else None
        samples = [sample for state in states for sample in state.samples]
        measured = [sample for sample in samples if sample.phase == "measured"]
        metadata.update(
            warmup_started_at=warmup_start,
            measurement_finished_at=last_state.finished_at_utc if measurement_start is not None else None,
            actual_elapsed_seconds=elapsed,
            drain_seconds=max(0.0, end - deadline) if deadline is not None else None,
            cancelled=stop.is_set(),
            overall=summarize_requests(measured, elapsed),
            per_query={
                name: summarize_requests([sample for sample in measured if sample.query_name == name], elapsed)
                for name in by_name
            },
            warmup_summary=summarize_requests([sample for sample in samples if sample.phase == "warmup"], None),
            worker_failures=[f"{type(exc).__name__}: {exc}" for exc in _drain_failures(failures)],
            worker_runtime={str(client_id): state.runtime for client_id, state in enumerate(states)},
        )
        _LOGGER.info("Analytical streams summary: %s", metadata)


def _drain_failures(failures: SimpleQueue[BaseException]) -> list[BaseException]:
    result: list[BaseException] = []
    while not failures.empty():
        result.append(failures.get())
    return result
