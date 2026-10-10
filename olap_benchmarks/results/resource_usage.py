from __future__ import annotations

from dataclasses import dataclass, fields
from pathlib import Path
from typing import Any, Literal, cast

import duckdb

from ..settings import IN_PROCESS_DATABASES, Revision, get_settings

MemorySource = Literal["server", "client"]
ResourceScope = Literal["client_only", "client_and_server"]


class ComparableMemoryUnavailableError(RuntimeError):
    pass


def comparable_memory_source(db: str) -> MemorySource:
    return "client" if db in IN_PROCESS_DATABASES else "server"


def resource_scope(db: str) -> ResourceScope:
    return "client_only" if db in IN_PROCESS_DATABASES else "client_and_server"


def resolve_comparable_peak_memory_mb(
    *,
    db: str,
    peak_server_memory_mb: int | None,
    peak_server_charge_mb: int | None,
    peak_client_memory_mb: int | None,
) -> int:
    if comparable_memory_source(db) == "client":
        if peak_server_charge_mb:
            raise ComparableMemoryUnavailableError(
                f"{db} executes inside the benchmark client process and cannot use server memory, but this run "
                f"recorded {peak_server_charge_mb} MB of it; the server column still holds a pre-split combined figure"
            )
        if peak_client_memory_mb is None:
            raise ComparableMemoryUnavailableError(
                f"{db} executes inside the benchmark client process, so its memory footprint is client_mem_mb, "
                "which this run did not record; it predates the server/client memory split (metrics version 2)"
            )
        return peak_client_memory_mb

    if peak_server_memory_mb is None and peak_server_charge_mb:
        raise ComparableMemoryUnavailableError(
            f"{db} recorded only its cgroup charge ({peak_server_charge_mb} MB), which includes page cache; "
            "the run predates the anon/file split (metrics version 7)"
        )
    if not peak_server_memory_mb:
        raise ComparableMemoryUnavailableError(
            f"{db} runs in one or more containers but this run recorded no server memory; "
            "container sampling did not produce a usable figure"
        )
    return peak_server_memory_mb


@dataclass(frozen=True)
class RunResourceUsage:
    run_id: int
    system: str
    db: str
    db_driver: str | None
    suite: str
    suite_scale_factor: int
    operation: str
    status: str
    # anon + shmem summed per sample across the database containers: memory the engine holds
    # and the kernel cannot reclaim by dropping clean page cache
    peak_server_memory_mb: int | None
    peak_server_file_mb: int | None
    peak_server_charge_mb: int | None
    peak_client_memory_mb: int | None
    peak_client_uss_memory_mb: int | None
    peak_combined_cpu_percent: float | None
    mean_combined_cpu_percent: float | None
    cpu_core_seconds: float | None
    peak_disk_mb: int | None
    final_disk_mb: int | None
    peak_disk_data_mb: int | None
    final_disk_data_mb: int | None
    peak_disk_wal_mb: int | None
    final_disk_wal_mb: int | None

    @property
    def comparable_memory_source(self) -> MemorySource:
        return comparable_memory_source(self.db)

    @property
    def resource_scope(self) -> ResourceScope:
        return resource_scope(self.db)

    def comparable_peak_memory_mb(self) -> int:
        return resolve_comparable_peak_memory_mb(
            db=self.db,
            peak_server_memory_mb=self.peak_server_memory_mb,
            peak_server_charge_mb=self.peak_server_charge_mb,
            peak_client_memory_mb=self.peak_client_memory_mb,
        )


SERVER_FIELDS = ("peak_server_memory_mb", "peak_server_file_mb", "peak_server_charge_mb")


def describe_run_resource_usage(usage: RunResourceUsage) -> dict[str, object]:
    try:
        comparable_peak_memory_mb: int | None = usage.comparable_peak_memory_mb()
        comparable_memory_unavailable: str | None = None
    except ComparableMemoryUnavailableError as exc:
        comparable_peak_memory_mb = None
        comparable_memory_unavailable = str(exc)

    # an in-process engine has no server, so its server figures are absent rather than zero
    client_only = usage.resource_scope == "client_only"
    described: dict[str, object] = {}
    for field in fields(usage):
        client_only_server_field = client_only and field.name in SERVER_FIELDS
        described[field.name] = None if client_only_server_field else getattr(usage, field.name)
        if field.name == "status":
            described["resource_scope"] = usage.resource_scope
    described.update(
        comparable_memory_source=usage.comparable_memory_source,
        comparable_peak_memory_mb=comparable_peak_memory_mb,
        comparable_memory_unavailable=comparable_memory_unavailable,
    )
    return described


def _resolve_results_path(revision: Revision, db_path: Path | None) -> Path:
    path = db_path or get_settings().results_directory / f"{revision}.db"
    if not path.is_file():
        raise FileNotFoundError(f"Results database does not exist: {path}")
    return path


def load_run_resource_usage(
    revision: Revision = "default",
    db_path: Path | None = None,
    system: str | None = None,
    db: str | None = None,
    suite: str | None = None,
    operation: str | None = None,
    status: str | None = None,
) -> list[RunResourceUsage]:
    path = _resolve_results_path(revision, db_path)

    where_clauses: list[str] = []
    params: list[object] = []
    for column, value in (
        ("system", system),
        ("db", db),
        ("suite", suite),
        ("operation", operation),
        ("status", status),
    ):
        if value is not None:
            where_clauses.append(f"r.{column} = ?")
            params.append(value)

    where_sql = f"where {' and '.join(where_clauses)}" if where_clauses else ""
    # Sampling lanes run at independent rates and each column is irregularly spaced, so
    # avg(cpu_percent) would weight a dense burst of samples the same as a sparse stretch.
    # Weighting each reading by the gap to the next one makes both figures depend only on
    # elapsed time. The final reading holds no interval and drops out of the weighted sum.
    sql = f"""
        with cpu_sample as (
          select
            run_id,
            cpu_percent,
            epoch(lead(time) over (partition by run_id order by time) - time) as weight_seconds
          from run_metric
          where cpu_percent is not null
        ),
        cpu_load as (
          select
            run_id,
            sum(cpu_percent * weight_seconds) / nullif(sum(weight_seconds), 0) as mean_combined_cpu_percent,
            sum(cpu_percent * weight_seconds) / 100.0 as cpu_core_seconds
          from cpu_sample
          where weight_seconds is not null
          group by run_id
        )
        select
          r.id as run_id,
          r.system,
          r.db,
          r.db_driver,
          r.suite,
          r.suite_scale_factor,
          r.operation,
          r.status,
          max(m.server_anon_mb + m.server_shmem_mb) as peak_server_memory_mb,
          max(m.server_file_mb) as peak_server_file_mb,
          max(m.server_charge_mb) as peak_server_charge_mb,
          max(m.client_mem_mb) as peak_client_memory_mb,
          max(m.client_uss_mb) as peak_client_uss_memory_mb,
          max(m.cpu_percent) as peak_combined_cpu_percent,
          c.mean_combined_cpu_percent,
          c.cpu_core_seconds,
          max(m.disk_mb) as peak_disk_mb,
          arg_max(m.disk_mb, m.time) filter (where m.disk_mb is not null) as final_disk_mb,
          max(m.disk_data_mb) as peak_disk_data_mb,
          arg_max(m.disk_data_mb, m.time) filter (where m.disk_mb is not null) as final_disk_data_mb,
          max(m.disk_wal_mb) as peak_disk_wal_mb,
          arg_max(m.disk_wal_mb, m.time) filter (where m.disk_mb is not null) as final_disk_wal_mb
        from run r
        left join run_metric m on m.run_id = r.id
        left join cpu_load c on c.run_id = r.id
        {where_sql}
        group by all
        order by r.id
    """

    con: duckdb.DuckDBPyConnection = cast(Any, duckdb).connect(str(path), read_only=True)
    try:
        cursor = con.execute(sql, params)
        names = [str(column[0]) for column in cast(list[tuple[object, ...]], cursor.description)]
        rows: list[tuple[Any, ...]] = cursor.fetchall()
    finally:
        con.close()

    return [RunResourceUsage(**dict(zip(names, row, strict=True))) for row in rows]
