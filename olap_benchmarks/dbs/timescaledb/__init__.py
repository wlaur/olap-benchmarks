import logging
import uuid
from collections.abc import Mapping
from concurrent.futures import Future, ThreadPoolExecutor, wait
from datetime import datetime
from pathlib import Path
from typing import Any, ClassVar, cast

import polars as pl
from sqlalchemy import Connection, Engine, create_engine, text

from ...settings import REPO_ROOT, DatabaseName, SuiteName, TableName, host_port
from ...suites import BenchmarkSuite
from ...suites.clickbench.config import Clickbench
from ...suites.rtabench.config import RTABench
from ...suites.time_series.config import (
    MutateStep,
    get_time_series_input_files,
)
from ..postgres import Postgres, PostgresTimeSeries
from ..utils import iter_parquet_frames, require_columns, tracked_commit

_LOGGER = logging.getLogger(__name__)

VERSION = "2.28.2"

DOCKER_IMAGE = f"timescale/timescaledb:{VERSION}-pg18"
TIMESCALEDB_HOST_PORT = host_port("timescaledb")

TIMESCALEDB_CONNECTION_STRING = f"postgresql://postgres:password@localhost:{TIMESCALEDB_HOST_PORT}/postgres"


class TimescaleRTABench(RTABench["TimescaleDB"]):
    def compress_tables(self) -> None:
        conn = self.db.connect()

        statement = "SELECT show_chunks('order_events')"
        with self.db.record_query_execution(statement):
            result = conn.execute(text(statement))
        chunks = [row[0] for row in result.fetchall()]

        if not chunks:
            raise RuntimeError

        _LOGGER.info(f"Found {len(chunks)} chunks to compress.")

        for idx, chunk in enumerate(chunks):
            _LOGGER.info(f"Compressing chunk {idx + 1:_}/{len(chunks):_}: {chunk}")
            self.db.execute(f"select compress_chunk('{chunk}'::regclass)", commit=False)

        tracked_commit(conn)

        self.db.execute("vacuum freeze analyze orders", autocommit=True)
        self.db.execute("vacuum freeze analyze order_events", autocommit=True)

    def populate(self, restart: bool = True) -> None:
        super().populate(restart=False)

        with self.db.phase_context("compress"):
            self.compress_tables()

        if restart:
            self.db.restart_event()


class TimescaleClickbench(Clickbench["TimescaleDB"]):
    def compress_table(self) -> None:
        self.db.execute("SELECT compress_chunk(i, if_not_compressed => true) FROM show_chunks('hits') i")
        self.db.execute("vacuum freeze analyze hits", autocommit=True)

    def populate(self, restart: bool = True) -> None:
        super().populate(restart=False)

        with self.db.phase_context("compress"):
            self.compress_table()

        if restart:
            self.db.restart_event()


class TimescaleTimeSeries(PostgresTimeSeries["TimescaleDB"]):
    # Unlike plain Postgres, the columnstore compression makes data_large fit
    # the disk budget, so nothing is skipped.
    SKIP_TABLES: ClassVar[frozenset[TableName]] = frozenset()

    PRE_INSERT_SCHEMA_FILES: ClassVar[dict[str, str]] = {
        "data_tall": "tall_pre_insert.sql",
        "data_wide": "wide_pre_insert.sql",
        "data_large": "large_pre_insert.sql",
    }

    # Chunk_time_interval per table -- must match the create_hypertable() calls
    # in the *_pre_insert.sql files. EAV inserts are flushed at this granularity
    # so just-completed chunks can be compressed before too much uncompressed
    # heap accumulates; otherwise data_large would build up ~300 GB of EAV heap.
    EAV_CHUNK_INTERVAL_DAYS: ClassVar[Mapping[str, int]] = {
        "data_wide": 7,
        "data_large": 30,
    }

    # Each compress_chunk() call is single-threaded inside TimescaleDB but
    # different chunks compress on independent backends. Two workers is the
    # sweet spot here -- more compress backends end up fighting the COPY
    # workers for the WALInsert / WALWrite LWLocks and slow the inserts down
    # enough to net negative on the EAV bulk load.
    MAX_PARALLEL_COMPRESSIONS: ClassVar[int] = 2

    _compress_engine: Engine | None = None

    def _get_compress_engine(self) -> Engine:
        # Compression runs on background threads, so it cannot share the main
        # SQLAlchemy connection. A QueuePool sized to the worker count avoids
        # repeated connect/auth overhead across the ~50 chunk compressions.
        if self._compress_engine is None:
            self._compress_engine = create_engine(
                self.db.connection_string,
                pool_size=self.MAX_PARALLEL_COMPRESSIONS,
                max_overflow=2,
                pool_pre_ping=False,
            )
        return self._compress_engine

    def _list_uncompressed_chunks_before(self, table_name: TableName, before: datetime) -> list[str]:
        engine = self._get_compress_engine()
        with engine.connect() as con:
            self.db.bind_query_recorder(con)
            before_str = before.strftime("%Y-%m-%d %H:%M:%S")
            list_sql = (
                "SELECT format('%I.%I', chunk_schema, chunk_name) "
                "FROM timescaledb_information.chunks "
                f"WHERE hypertable_name = '{table_name}' AND NOT is_compressed "
                f"AND range_end <= '{before_str}' "
                "ORDER BY range_end"
            )
            with self.db.record_query_execution(list_sql):
                rows = con.execute(text(list_sql)).fetchall()
        return [row[0] for row in rows]

    def _compress_chunk_threadsafe(self, chunk_name: str) -> None:
        engine = self._get_compress_engine()
        with engine.connect() as con:
            self.db.bind_query_recorder(con)
            stmt = f"SELECT compress_chunk('{chunk_name}'::regclass, if_not_compressed => true)"
            with self.db.record_query_execution(stmt):
                con.execute(text(stmt))
            tracked_commit(con)

    def _drain_completed(self, futures: list[Future[None]], max_in_flight: int) -> None:
        # Surface exceptions eagerly while bounding pool depth.
        while len(futures) > max_in_flight:
            done, _ = wait(futures, return_when="FIRST_COMPLETED")
            for fut in done:
                fut.result()
                futures.remove(fut)

    def _eav_streaming_insert(self, table_name: TableName, fpath: Path) -> None:
        """Single-pass stream of `fpath` → unpivot → COPY into the hypertable.
        Buffers wide rows up to one chunk_time_interval per flush and compresses
        completed chunks on a thread pool so compression of chunk N overlaps
        with the COPY for chunk N+1. The previous implementation re-scanned the
        9 GB parquet ~200 times and serialised the ~30 s/chunk compression."""
        days = self.EAV_CHUNK_INTERVAL_DAYS[table_name]
        target_wide_rows = days * 24 * 60  # one row per minute → one chunk's worth

        executor = ThreadPoolExecutor(
            max_workers=self.MAX_PARALLEL_COMPRESSIONS,
            thread_name_prefix=f"ts-compress-{table_name}",
        )
        in_flight: list[Future[None]] = []
        # Chunks already handed to the pool. The catalog query returns
        # not-yet-compressed chunks, so an in-flight compress would otherwise
        # keep showing up in successive flushes and get submitted twice.
        submitted_chunks: set[str] = set()

        buffer: list[pl.DataFrame] = []
        buffer_rows = 0
        batch_idx = 0

        def flush() -> None:
            nonlocal buffer_rows, batch_idx
            if not buffer:
                return
            batch_idx += 1
            df_wide = pl.concat(buffer, how="vertical") if len(buffer) > 1 else buffer[0]
            max_t = cast(datetime, df_wide["time"].max())
            df_eav = self._wide_to_eav(df_wide)
            self.db.insert(df_eav, table_name)
            _LOGGER.info(
                f"Inserted EAV batch {batch_idx} for {table_name}: "
                f"wide={df_wide.shape[0]:_}, eav={df_eav.shape[0]:_}, max_t={max_t}"
            )
            buffer.clear()
            buffer_rows = 0

            # Schedule compression of every newly-completed chunk independently
            # so the worker pool can parallelise them. Only chunks with
            # range_end <= max_t are fully filled; the chunk currently being
            # written (range_end > max_t) is left alone.
            new_chunks = [
                c for c in self._list_uncompressed_chunks_before(table_name, max_t) if c not in submitted_chunks
            ]
            for chunk in new_chunks:
                submitted_chunks.add(chunk)
                in_flight.append(executor.submit(self._compress_chunk_threadsafe, chunk))
            if new_chunks:
                _LOGGER.info(f"Submitted {len(new_chunks)} compress task(s) for {table_name}")
            self._drain_completed(in_flight, self.MAX_PARALLEL_COMPRESSIONS * 2)

        try:
            for df in iter_parquet_frames(fpath, self.PARQUET_STREAM_BATCH_ROWS):
                buffer.append(df)
                buffer_rows += df.height
                if buffer_rows >= target_wide_rows:
                    flush()
            flush()

            # Drain remaining compressions before declaring this table done so
            # the next phase doesn't race with background work.
            self._drain_completed(in_flight, 0)
        finally:
            executor.shutdown(wait=True)

    def compress_tables(self) -> None:
        # data_wide / data_large are already fully compressed by the streaming
        # insert path; the call below is a no-op for those (if_not_compressed
        # => true). data_tall is still loaded as one big batch and is
        # compressed here.
        for table_name in get_time_series_input_files(self.scale_factor):
            self.db.execute(
                f"SELECT compress_chunk(i, if_not_compressed => true) FROM show_chunks('{table_name}') i",
                reconnect=True,
            )
            _LOGGER.info(f"Compressed table {table_name}")

            self.db.execute(f"vacuum freeze analyze {table_name}", autocommit=True)
            _LOGGER.info(f"Vacuumed table {table_name}")

    def populate(self, restart: bool = True) -> None:
        with self.db.phase_context("verify_existing_data"):
            if not self.should_populate():
                return

        input_files = get_time_series_input_files(self.scale_factor)

        # 1. Create empty tables (regular wide for tall, EAV for wide/large).
        for table_name, fpath in input_files.items():
            if table_name in self.EAV_TABLES:
                self._eav_create_table(table_name)
            else:
                schema = cast(pl.Schema, pl.read_parquet_schema(fpath))
                self.db.create_table(schema, table_name, primary_key=None, not_null="time")

        # 2. Convert each table to a hypertable + columnstore config (empty
        #    table → no migrate_data needed, much faster than retro-conversion).
        for table_name, sql_file in self.PRE_INSERT_SCHEMA_FILES.items():
            with self.db.phase_context("create_hypertable", table_name=table_name):
                self.db.execute_schema_file(
                    REPO_ROOT / "olap_benchmarks/suites/time_series/schemas/timescaledb" / sql_file
                )

        # 3. Ingest data.
        for table_name, fpath in input_files.items():
            with self.db.phase_context("insert", table_name=table_name):
                if table_name in self.EAV_TABLES:
                    self._eav_streaming_insert(table_name, fpath)
                else:
                    df = pl.scan_parquet(fpath)
                    self.db.insert(df, table_name, primary_key=None, not_null="time")
            _LOGGER.info(f"Inserted {table_name} for {self.name}")

        # 4. Compress all chunks.
        with self.db.phase_context("compress"):
            self.compress_tables()

        with self.db.phase_context("verify_populate"):
            self.verify_populated_data()

        if restart:
            self.db.restart_event()

    def _apply_upsert(self, step: MutateStep, df: pl.DataFrame) -> None:
        # None of the time_series tables here have a unique constraint on
        # `time` (data_tall is wide with no PK; data_wide/data_large are EAV
        # with no (time, metric_name) unique). ON CONFLICT therefore cannot
        # bind, so we emulate upsert with DELETE-by-time + INSERT for every
        # table.
        keys = df.select("time").unique()
        self.db.delete(step.table, primary_key="time", keys=keys)
        primary_key = None if step.table in self.EAV_TABLES else self.get_primary_key(step.table)
        not_null = self.get_not_null(step.table)
        self.db.insert(df, step.table, primary_key=primary_key, not_null=not_null)


class TimescaleDB(Postgres):
    name: DatabaseName = "timescaledb"
    version: str = VERSION
    container_image: ClassVar[str | None] = DOCKER_IMAGE
    supports_arm64_containers: ClassVar[bool] = True

    connection_string: str = TIMESCALEDB_CONNECTION_STRING

    # Plain Postgres disables the data_large mutation steps because the table
    # is never populated there; with columnstore compression it is.
    DISABLED_MUTATION_STEPS: ClassVar[Mapping[SuiteName, frozenset[str]]] = {}

    @property
    def start(self) -> str:
        # macOS Finder writes .DS_Store into bind-mounted dirs as soon as it's
        # browsed; PG's initdb refuses to run on a non-empty PGDATA, so scrub
        # macOS metadata before handing the dir over.
        for junk in self.database_directory.glob(".DS_Store"):
            junk.unlink(missing_ok=True)
        for junk in self.database_directory.glob("._*"):
            junk.unlink(missing_ok=True)

        parts = [
            f"docker run --platform {self.container_platform} --name {self.name}-benchmark "
            f"--rm -d -p {TIMESCALEDB_HOST_PORT}:5432",
            f"-v {self.database_directory.as_posix()}:/var/lib/postgresql/data/",
            "-e POSTGRES_PASSWORD=password",
            "-e PGDATA=/var/lib/postgresql/data/",
            # Pin cluster timezone to UTC so TIMESTAMPTZ values written from
            # naive Polars datetimes round-trip without offset surprises.
            "-e TZ=UTC",
            "-e PGTZ=UTC",
            DOCKER_IMAGE,
            # Postgres settings tuned for the EAV bulk-load + columnstore
            # compression workload. shared_buffers / parallelism are already
            # auto-tuned by the Timescale image (timescaledb-tune); the rest
            # relax checkpoint / WAL pressure for the multi-billion-row insert
            # and let compress_chunk() sort 130 M-row chunks in memory instead
            # of spilling to BufFile temp files (the original bottleneck on
            # data_large -- per-chunk compress jumped from ~50 s to >300 s
            # once sorts spilled).
            "-c max_wal_size=16GB",
            "-c min_wal_size=2GB",
            # WAL compression saves disk but the CPU cost of zstd on every
            # WALInsert dominates under heavy concurrent COPY traffic; leaving
            # it off let the COPY workers spend less time waiting on the
            # WALInsert LWLock.
            "-c wal_compression=off",
            "-c synchronous_commit=off",
            "-c checkpoint_timeout=30min",
            "-c work_mem=4GB",
            "-c max_parallel_maintenance_workers=4",
            "-c bgwriter_lru_maxpages=1000",
            "-c bgwriter_delay=10ms",
        ]

        return " ".join(parts)

    def get_runtime_version(self) -> str:
        df = self.fetch(
            "select extversion as version from pg_extension where extname = 'timescaledb'",
            schema={"version": pl.String},
        )
        return str(df.item(0, 0))

    def _dml_session_setup(self, con: Connection) -> None:
        # Compressed chunks cap how many tuples a DML transaction may
        # decompress; the mutation benchmarks need that limit lifted.
        self.execute("SET timescaledb.max_tuples_decompressed_per_dml_transaction = 0", commit=False)

    def upsert(self, df: pl.DataFrame, table: TableName, primary_key: str | list[str]) -> None:
        primary_keys = require_columns(primary_key)

        for pk in primary_keys:
            if pk not in df.columns:
                raise ValueError(f"Primary key column '{pk}' not found in DataFrame columns")

        con = self.connect()
        pk_cols = ", ".join(f'"{pk}"' for pk in primary_keys)

        staging_table = f"_staging_{table}_{uuid.uuid4().hex[:8]}"
        self.execute(f"CREATE TEMP TABLE {staging_table} (LIKE {table} INCLUDING DEFAULTS)", commit=False)
        self._copy_dataframe(con, staging_table, df, f"{table}_upsert")
        self._dml_session_setup(con)

        all_columns = ", ".join(f'"{col}"' for col in df.columns)
        non_key_columns = [col for col in df.columns if col not in primary_keys]

        if non_key_columns:
            set_clause = ", ".join(f'"{col}" = EXCLUDED."{col}"' for col in non_key_columns)
            on_conflict_clause = f"ON CONFLICT ({pk_cols}) DO UPDATE SET {set_clause}"
        else:
            on_conflict_clause = f"ON CONFLICT ({pk_cols}) DO NOTHING"

        self.execute(
            f"INSERT INTO {table} ({all_columns}) SELECT {all_columns} FROM {staging_table} {on_conflict_clause}",
            commit=False,
        )

        self.execute(f"DROP TABLE {staging_table}", commit=False)
        tracked_commit(con)

        _LOGGER.info(f"Upserted {df.shape[0]:_} rows into {table}")

    def suite_registry(self) -> Mapping[SuiteName, type[BenchmarkSuite[Any]]]:
        return {
            **super().suite_registry(),
            "rtabench": TimescaleRTABench,
            "clickbench": TimescaleClickbench,
            "time_series": TimescaleTimeSeries,
        }
