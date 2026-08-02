from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast

import duckdb

from ..settings import IN_PROCESS_DATABASES, SETTINGS, Revision

MemorySource = Literal["server", "client"]


class ComparableMemoryUnavailableError(RuntimeError):
    pass


def comparable_memory_source(db: str) -> MemorySource:
    return "client" if db in IN_PROCESS_DATABASES else "server"


def resolve_comparable_peak_memory_mb(
    *,
    db: str,
    peak_server_memory_mb: int | None,
    peak_client_memory_mb: int | None,
) -> int:
    if comparable_memory_source(db) == "client":
        if peak_server_memory_mb:
            raise ComparableMemoryUnavailableError(
                f"{db} executes inside the benchmark client process and cannot use server memory, but this run "
                f"recorded {peak_server_memory_mb} MB of it; server_mem_mb still holds a pre-split combined figure"
            )
        if peak_client_memory_mb is None:
            raise ComparableMemoryUnavailableError(
                f"{db} executes inside the benchmark client process, so its memory footprint is client_mem_mb, "
                "which this run did not record; it predates the server/client memory split (metrics version 2)"
            )
        return peak_client_memory_mb

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
    peak_server_memory_mb: int | None
    peak_client_memory_mb: int | None
    peak_client_uss_memory_mb: int | None
    peak_combined_cpu_percent: float | None
    peak_server_disk_mb: int | None

    @property
    def comparable_memory_source(self) -> MemorySource:
        return comparable_memory_source(self.db)

    def comparable_peak_memory_mb(self) -> int:
        return resolve_comparable_peak_memory_mb(
            db=self.db,
            peak_server_memory_mb=self.peak_server_memory_mb,
            peak_client_memory_mb=self.peak_client_memory_mb,
        )


def describe_run_resource_usage(usage: RunResourceUsage) -> dict[str, object]:
    try:
        comparable_peak_memory_mb: int | None = usage.comparable_peak_memory_mb()
        comparable_memory_unavailable: str | None = None
    except ComparableMemoryUnavailableError as exc:
        comparable_peak_memory_mb = None
        comparable_memory_unavailable = str(exc)

    return {
        "run_id": usage.run_id,
        "system": usage.system,
        "db": usage.db,
        "db_driver": usage.db_driver,
        "suite": usage.suite,
        "suite_scale_factor": usage.suite_scale_factor,
        "operation": usage.operation,
        "status": usage.status,
        "comparable_memory_source": usage.comparable_memory_source,
        "comparable_peak_memory_mb": comparable_peak_memory_mb,
        "comparable_memory_unavailable": comparable_memory_unavailable,
        "peak_server_memory_mb": usage.peak_server_memory_mb,
        "peak_client_memory_mb": usage.peak_client_memory_mb,
        "peak_client_uss_memory_mb": usage.peak_client_uss_memory_mb,
        "peak_combined_cpu_percent": usage.peak_combined_cpu_percent,
        "peak_server_disk_mb": usage.peak_server_disk_mb,
    }


def _resolve_results_path(revision: Revision, db_path: Path | None) -> Path:
    path = db_path or SETTINGS.results_directory / f"{revision}.db"
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
    sql = f"""
        select
          r.id,
          r.system,
          r.db,
          r.db_driver,
          r.suite,
          r.suite_scale_factor,
          r.operation,
          r.status,
          max(m.server_mem_mb) as peak_server_memory_mb,
          max(m.client_mem_mb) as peak_client_memory_mb,
          max(m.client_uss_mb) as peak_client_uss_memory_mb,
          max(m.cpu_percent) as peak_combined_cpu_percent,
          max(m.disk_mb) as peak_server_disk_mb
        from run r
        left join run_metric m on m.run_id = r.id
        {where_sql}
        group by 1, 2, 3, 4, 5, 6, 7, 8
        order by r.id
    """

    con: duckdb.DuckDBPyConnection = cast(Any, duckdb).connect(str(path), read_only=True)
    try:
        rows = con.execute(sql, params).fetchall()
    finally:
        con.close()

    return [
        RunResourceUsage(
            run_id=int(row[0]),
            system=str(row[1]),
            db=str(row[2]),
            db_driver=None if row[3] is None else str(row[3]),
            suite=str(row[4]),
            suite_scale_factor=int(row[5]),
            operation=str(row[6]),
            status=str(row[7]),
            peak_server_memory_mb=None if row[8] is None else int(row[8]),
            peak_client_memory_mb=None if row[9] is None else int(row[9]),
            peak_client_uss_memory_mb=None if row[10] is None else int(row[10]),
            peak_combined_cpu_percent=None if row[11] is None else float(row[11]),
            peak_server_disk_mb=None if row[12] is None else int(row[12]),
        )
        for row in rows
    ]
