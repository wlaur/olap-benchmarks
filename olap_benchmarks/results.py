import json as json_module
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import duckdb

from .settings import SETTINGS, DatabaseName, SuiteName


def _get_connection() -> duckdb.DuckDBPyConnection:
    db_path = SETTINGS.results_directory / "results.db"
    return duckdb.connect(str(db_path), read_only=False)


def _parse_notes(notes: str | None) -> tuple[str | None, str | None]:
    if not notes:
        return None, None
    parts = notes.split(" | ", 1)
    version = parts[0].strip() if parts else None
    system = parts[1].strip() if len(parts) > 1 else None
    return version, system


def _format_duration(started_at: datetime | None, finished_at: datetime | None) -> str:
    if not started_at or not finished_at:
        return "-"
    delta = finished_at - started_at
    total_seconds = int(delta.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours}h {minutes}m {seconds}s"
    elif minutes:
        return f"{minutes}m {seconds}s"
    return f"{seconds}s"


def _format_status(finished_at: datetime | None, deleted_at: datetime | None) -> str:
    if deleted_at:
        return "deleted"
    if finished_at:
        return "completed"
    return "failed/running"


@dataclass
class Benchmark:
    id: int
    suite: str
    db: str
    operation: str
    started_at: datetime | None
    finished_at: datetime | None
    deleted_at: datetime | None
    notes: str | None

    @property
    def version(self) -> str | None:
        v, _ = _parse_notes(self.notes)
        return v

    @property
    def system(self) -> str | None:
        _, s = _parse_notes(self.notes)
        return s

    @property
    def status(self) -> str:
        return _format_status(self.finished_at, self.deleted_at)

    @property
    def duration(self) -> str:
        return _format_duration(self.started_at, self.finished_at)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "suite": self.suite,
            "db": self.db,
            "operation": self.operation,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "deleted_at": self.deleted_at.isoformat() if self.deleted_at else None,
            "version": self.version,
            "system": self.system,
            "status": self.status,
            "duration": self.duration,
        }


class ResultsCLI:
    """Manage benchmark results.

    Commands for listing, viewing, and deleting benchmark results
    stored in the results database.

    Examples:
        python -m olap_benchmarks results list
        python -m olap_benchmarks results list --db postgres
        python -m olap_benchmarks results show 42
        python -m olap_benchmarks results delete 42 43 --force
    """

    def list(
        self,
        db: DatabaseName | None = None,
        suite: SuiteName | None = None,
        version: str | None = None,
        system: str | None = None,
        status: str | None = None,
        limit: int = 50,
        json: bool = False,
    ) -> None:
        """List benchmark runs with optional filters.

        Args:
            db: Filter by database name (postgres, duckdb, etc.)
            suite: Filter by benchmark suite (rtabench, clickbench, etc.)
            version: Filter by database version (parsed from notes)
            system: Filter by system name (parsed from notes)
            status: Filter by status (completed, failed/running, deleted)
            limit: Maximum number of results to return (default: 50)
            json: Output as JSON instead of table
        """
        conn = _get_connection()

        query = """
            SELECT id, suite, db, operation, started_at, finished_at, deleted_at, notes
            FROM benchmark
            WHERE 1=1
        """
        params: list[Any] = []

        if db:
            query += " AND db = ?"
            params.append(db)

        if suite:
            query += " AND suite = ?"
            params.append(suite)

        if version:
            query += " AND notes LIKE ?"
            params.append(f"{version}%")

        if system:
            query += " AND notes LIKE ?"
            params.append(f"% | {system}%")

        if status == "completed":
            query += " AND finished_at IS NOT NULL AND deleted_at IS NULL"
        elif status == "failed/running":
            query += " AND finished_at IS NULL AND deleted_at IS NULL"
        elif status == "deleted":
            query += " AND deleted_at IS NOT NULL"

        query += " ORDER BY id DESC LIMIT ?"
        params.append(limit)

        rows = conn.execute(query, params).fetchall()
        benchmarks = [Benchmark(*row) for row in rows]

        if json:
            print(json_module.dumps([b.to_dict() for b in benchmarks], indent=2))
            return

        if not benchmarks:
            print("No benchmarks found.")
            return

        # Table header
        print(
            f"{'ID':>5} {'DB':<12} {'SUITE':<14} {'OP':<8} {'STATUS':<13} {'DURATION':<12} "
            f"{'VERSION':<16} {'SYSTEM':<20} {'STARTED':<20}"
        )
        print("-" * 130)

        for b in benchmarks:
            started = b.started_at.strftime("%Y-%m-%d %H:%M:%S") if b.started_at else "-"
            print(
                f"{b.id:>5} {b.db:<12} {b.suite:<14} {b.operation:<8} {b.status:<13} {b.duration:<12} "
                f"{(b.version or '-'):<16} {(b.system or '-'):<20} {started:<20}"
            )

        print(f"\nTotal: {len(benchmarks)} benchmark(s)")

    def show(self, benchmark_id: int, json: bool = False) -> None:
        """Show detailed information for a specific benchmark.

        Args:
            benchmark_id: The benchmark ID to show
            json: Output as JSON instead of formatted text
        """
        conn = _get_connection()

        # Get benchmark
        row = conn.execute(
            """
            SELECT id, suite, db, operation, started_at, finished_at, deleted_at, notes
            FROM benchmark WHERE id = ?
            """,
            [benchmark_id],
        ).fetchone()

        if not row:
            print(f"Benchmark {benchmark_id} not found.", file=sys.stderr)
            sys.exit(1)

        b = Benchmark(*row)

        # Get events
        events = conn.execute(
            """
            SELECT time, name, type FROM event
            WHERE benchmark_id = ?
            ORDER BY time
            """,
            [benchmark_id],
        ).fetchall()

        # Get metrics summary
        metrics = conn.execute(
            """
            SELECT
                COUNT(*) as count,
                MIN(cpu_percent) as min_cpu,
                AVG(cpu_percent) as avg_cpu,
                MAX(cpu_percent) as max_cpu,
                MIN(mem_mb) as min_mem,
                AVG(mem_mb) as avg_mem,
                MAX(mem_mb) as max_mem,
                MIN(disk_mb) as min_disk,
                MAX(disk_mb) as max_disk
            FROM metric WHERE benchmark_id = ?
            """,
            [benchmark_id],
        ).fetchone()

        if json:
            output = b.to_dict()
            output["events"] = [{"time": e[0].isoformat(), "name": e[1], "type": e[2]} for e in events]
            output["metrics_summary"] = (
                {
                    "count": metrics[0],
                    "cpu": {"min": metrics[1], "avg": metrics[2], "max": metrics[3]},
                    "mem_mb": {"min": metrics[4], "avg": metrics[5], "max": metrics[6]},
                    "disk_mb": {"min": metrics[7], "max": metrics[8]},
                }
                if metrics and metrics[0]
                else None
            )
            print(json_module.dumps(output, indent=2))
            return

        # Formatted output
        print(f"\n{'=' * 60}")
        print(f"BENCHMARK #{b.id}")
        print(f"{'=' * 60}")
        print(f"  Database:   {b.db}")
        print(f"  Suite:      {b.suite}")
        print(f"  Operation:  {b.operation}")
        print(f"  Status:     {b.status}")
        print(f"  Version:    {b.version or '-'}")
        print(f"  System:     {b.system or '-'}")
        print(f"  Started:    {b.started_at}")
        print(f"  Finished:   {b.finished_at or '-'}")
        print(f"  Duration:   {b.duration}")

        if events:
            print(f"\n{'EVENTS':-^60}")
            for time, name, type_ in events:
                print(f"  {time.strftime('%H:%M:%S.%f')[:-3]}  {type_:<5}  {name}")

        if metrics and metrics[0]:
            print(f"\n{'METRICS SUMMARY':-^60}")
            print(f"  Samples:    {metrics[0]}")
            print(f"  CPU (%):    min={metrics[1]:.1f}  avg={metrics[2]:.1f}  max={metrics[3]:.1f}")
            print(f"  Memory (MB): min={metrics[4]}  avg={metrics[5]:.0f}  max={metrics[6]}")
            print(f"  Disk (MB):  min={metrics[7]}  max={metrics[8]}")

        print()

    def delete(
        self,
        *ids: int,
        dry_run: bool = False,
        force: bool = False,
        failed: bool = False,
    ) -> None:
        """Delete benchmark(s) and their associated metrics/events.

        Args:
            *ids: One or more benchmark IDs to delete
            dry_run: Preview what would be deleted without actually deleting (--dry-run)
            force: Skip confirmation prompt (--force)
            failed: Delete all benchmarks without finished_at (--failed)
        """
        conn = _get_connection()

        # Determine which IDs to delete
        if failed:
            rows = conn.execute("SELECT id FROM benchmark WHERE finished_at IS NULL AND deleted_at IS NULL").fetchall()
            ids_to_delete = [row[0] for row in rows]
        else:
            ids_to_delete = list(ids)

        if not ids_to_delete:
            print("No benchmarks to delete.")
            return

        # Show what will be deleted
        placeholders = ",".join("?" * len(ids_to_delete))
        benchmarks = conn.execute(
            f"""
            SELECT id, suite, db, operation, started_at, finished_at, notes
            FROM benchmark WHERE id IN ({placeholders})
            """,
            ids_to_delete,
        ).fetchall()

        if not benchmarks:
            print("No matching benchmarks found.", file=sys.stderr)
            sys.exit(1)

        print(f"\nBenchmarks to delete ({len(benchmarks)}):")
        print(f"{'ID':>5} {'DB':<12} {'SUITE':<14} {'OP':<8} {'STARTED':<20}")
        print("-" * 65)
        for b in benchmarks:
            started = b[4].strftime("%Y-%m-%d %H:%M:%S") if b[4] else "-"
            print(f"{b[0]:>5} {b[2]:<12} {b[1]:<14} {b[3]:<8} {started:<20}")

        # Count related records
        metric_count = conn.execute(
            f"SELECT COUNT(*) FROM metric WHERE benchmark_id IN ({placeholders})",
            ids_to_delete,
        ).fetchone()[0]

        event_count = conn.execute(
            f"SELECT COUNT(*) FROM event WHERE benchmark_id IN ({placeholders})",
            ids_to_delete,
        ).fetchone()[0]

        print("\nThis will also delete:")
        print(f"  - {metric_count} metric records")
        print(f"  - {event_count} event records")

        if dry_run:
            print("\n[DRY RUN] No changes made.")
            return

        if not force:
            confirm = input("\nProceed with deletion? [y/N]: ").strip().lower()
            if confirm != "y":
                print("Aborted.")
                return

        # Perform deletion
        conn.execute(f"DELETE FROM metric WHERE benchmark_id IN ({placeholders})", ids_to_delete)
        conn.execute(f"DELETE FROM event WHERE benchmark_id IN ({placeholders})", ids_to_delete)
        conn.execute(f"DELETE FROM benchmark WHERE id IN ({placeholders})", ids_to_delete)

        print(f"\nDeleted {len(benchmarks)} benchmark(s), {metric_count} metrics, {event_count} events.")


def config(json: bool = False) -> None:
    """Show current configuration from .env file.

    Displays all settings loaded from the OLAP_BENCHMARKS_* environment variables.

    Args:
        json: Output as JSON instead of formatted text
    """
    settings_dict = {
        "input_data_directory": str(SETTINGS.input_data_directory),
        "results_directory": str(SETTINGS.results_directory),
        "database_directory": str(SETTINGS.database_directory),
        "temporary_directory": str(SETTINGS.temporary_directory),
        "system": SETTINGS.system,
    }

    if json:
        print(json_module.dumps(settings_dict, indent=2))
        return

    print("\nCurrent Configuration (from .env)")
    print("=" * 60)
    print(f"  OLAP_BENCHMARKS_INPUT_DATA_DIRECTORY:  {SETTINGS.input_data_directory}")
    print(f"  OLAP_BENCHMARKS_RESULTS_DIRECTORY:     {SETTINGS.results_directory}")
    print(f"  OLAP_BENCHMARKS_DATABASE_DIRECTORY:    {SETTINGS.database_directory}")
    print(f"  OLAP_BENCHMARKS_TEMPORARY_DIRECTORY:   {SETTINGS.temporary_directory}")
    print(f"  OLAP_BENCHMARKS_SYSTEM:                {SETTINGS.system}")
    print()
