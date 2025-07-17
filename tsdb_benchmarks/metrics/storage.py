from datetime import datetime

import duckdb

from ..settings import REPO_ROOT, SETTINGS, DatabaseName


class Storage:
    def __init__(self) -> None:
        self.db_path = SETTINGS.results_directory / "results.db"
        self.conn = duckdb.connect(self.db_path)
        self.init_schema()

    def init_schema(self) -> None:
        with (REPO_ROOT / "tsdb_benchmarks/metrics/schema.sql").open() as f:
            sql = f.read()
        self.conn.execute(sql)

    def insert_benchmark(self, name: DatabaseName, started_at: datetime, notes: str | None = None) -> int:
        result = self.conn.execute(
            """
            insert into benchmark (name, started_at, notes)
            values (?, ?, ?)
            returning id
            """,
            [name, started_at, notes],
        ).fetchone()
        assert result is not None
        return result[0]

    def finish_benchmark(self, benchmark_id: int, finished_at: datetime) -> None:
        self.conn.execute(
            """
            update benchmark
            set finished_at = ?
            where id = ?
            """,
            [finished_at, benchmark_id],
        )

    def insert_metric(self, benchmark_id: int, ts: datetime, cpu_percent: float, mem_mb: int, disk_mb: int) -> None:
        self.conn.execute(
            """
            insert into metric (
                benchmark_id, ts, cpu_percent, mem_mb, disk_mb
            )
            values (?, ?, ?, ?, ?)
            """,
            [benchmark_id, ts, cpu_percent, mem_mb, disk_mb],
        )

    def close(self) -> None:
        self.conn.close()
