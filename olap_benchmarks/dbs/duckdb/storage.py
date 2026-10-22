from __future__ import annotations

import json
import os
from pathlib import Path
from stat import S_ISREG
from typing import TYPE_CHECKING, Literal, cast

from pyarrow.parquet import ParquetFile
from pydantic import BaseModel, ConfigDict

from ...inputs import fingerprint_input_directory
from ...run_metadata import sha256_text
from ...settings import REPO_ROOT
from ...suites.join_order.schema import JOB_TABLES
from ...suites.kaggle_airbnb.config import KAGGLE_AIRBNB_TABLES
from ...suites.rtabench.config import RTABENCH_SCHEMAS
from ...suites.time_series.config import TimeSeries
from ...suites.time_series_metrics.config import TimeSeriesMetrics
from ...suites.tpc_ds.config import TPCDS_TABLES
from ...suites.tpc_h.config import TPCH_TABLES

if TYPE_CHECKING:
    from . import DuckDB

MANIFEST_VERSION = 1
MANIFEST_NAME = "storage-manifest.json"
type StorageMode = Literal["native", "parquet"]


def identifier(value: str) -> str:
    if "\0" in value:
        raise ValueError("NUL in SQL identifier")
    return '"' + value.replace('"', '""') + '"'


def literal(value: str) -> str:
    if "\0" in value:
        raise ValueError("NUL in SQL literal")
    return "'" + value.replace("'", "''") + "'"


class StorageModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Column(StorageModel):
    name: str
    dtype: str
    not_null: bool
    primary_key: bool


class SourceFile(StorageModel):
    path: str
    bytes: int
    mtime_ns: int

    @classmethod
    def inspect(cls, path: Path) -> SourceFile:
        resolved = path.resolve(strict=True)
        stat = resolved.stat()
        if not S_ISREG(stat.st_mode):
            raise ValueError(f"DuckDB source is not a regular file: {path}")
        return cls(path=resolved.as_posix(), bytes=stat.st_size, mtime_ns=stat.st_mtime_ns)


class Relation(StorageModel):
    columns: list[Column]
    sources: list[SourceFile]
    view_sha256: str | None = None
    projection: str | None = None
    parquet_metadata: list[dict[str, object]] | None = None
    parquet_file: dict[str, object] | None = None


class Manifest(StorageModel):
    version: int = MANIFEST_VERSION
    state: Literal["building", "ready"] = "building"
    suite: str
    scale_factor: int
    mode: StorageMode
    build: list[str]
    schema_sha256: str | None
    input_fingerprint: dict[str, object]
    input_directory: str
    inputs: list[SourceFile]
    storage_tags: dict[str, str]
    relations: dict[str, Relation] = {}
    dirty: bool = False


def schema_digest(db: DuckDB) -> str | None:
    path = REPO_ROOT / f"olap_benchmarks/suites/{db.current_suite}/schemas/duckdb.sql"
    return sha256_text(path.read_text()) if path.is_file() else None


def planned_sources(db: DuckDB) -> dict[str, Path]:
    suite = db.benchmarks[db.current_suite]
    match str(db.current_suite):
        case "time_series":
            return dict(cast("TimeSeries[DuckDB]", suite).dataset_files())
        case "time_series_metrics" | "time_series_ranges":
            return dict(cast("TimeSeriesMetrics[DuckDB]", suite).dataset_files())
        case "kaggle_airbnb":
            tables = KAGGLE_AIRBNB_TABLES
        case "rtabench":
            tables = tuple(RTABENCH_SCHEMAS)
        case "tpc_h" | "parquet_analytics":
            tables = TPCH_TABLES
        case "tpc_ds":
            tables = TPCDS_TABLES
        case "join_order":
            tables = JOB_TABLES
        case "device_telemetry":
            tables = ("telemetry_device", "telemetry_reading")
        case "clickbench":
            tables = ("hits",)
        case "chat_threads":
            tables = ("chat_thread", "chat_message")
        case "jsonbench" | "json_representation":
            return {}
        case _:
            raise NotImplementedError(f"No DuckDB planned source mapping for {db.current_suite}")
    return {table: db.input_directory / f"{table}.parquet" for table in tables}


class Storage:
    def __init__(self, db: DuckDB) -> None:
        self.db = db

    @property
    def path(self) -> Path:
        return self.db.database_directory / MANIFEST_NAME

    def read(self) -> Manifest:
        if not self.path.is_file():
            raise RuntimeError("DuckDB storage manifest missing; rebuild this benchmark store with DuckDB 2")
        return Manifest.model_validate_json(self.path.read_text())

    def write(self, manifest: Manifest) -> None:
        partial = self.path.with_suffix(f".{os.getpid()}.part")
        with partial.open("w") as output:
            output.write(manifest.model_dump_json(indent=2))
            output.flush()
            os.fsync(output.fileno())
        partial.replace(self.path)

    def building(self) -> Manifest:
        if self.path.exists():
            manifest = self.read()
            self.check_identity(manifest)
            if manifest.state != "building":
                raise RuntimeError("DuckDB store is already finalized; rebuild before population")
            return manifest
        if self.db.get_table_names():
            raise RuntimeError("Unmanifested DuckDB relations exist; rebuild this benchmark store")
        manifest = Manifest(
            suite=self.db.current_suite,
            scale_factor=self.db.current_suite_scale_factor,
            mode=self.db.storage_mode,
            build=self.db.runtime_build(),
            schema_sha256=schema_digest(self.db),
            input_fingerprint=fingerprint_input_directory(self.db.input_directory),
            input_directory=self.db.input_directory.resolve().as_posix(),
            inputs=[SourceFile.inspect(path) for path in sorted(self.db.input_directory.rglob("*")) if path.is_file()],
            storage_tags=self.db.database_tags(),
        )
        self.write(manifest)
        return manifest

    def check_identity(self, manifest: Manifest) -> None:
        if (
            manifest.version != MANIFEST_VERSION
            or manifest.suite != self.db.current_suite
            or manifest.scale_factor != self.db.current_suite_scale_factor
            or manifest.mode != self.db.storage_mode
            or manifest.build != self.db.runtime_build()
            or manifest.schema_sha256 != schema_digest(self.db)
            or manifest.input_directory != self.db.input_directory.resolve().as_posix()
            or manifest.storage_tags != self.db.database_tags()
        ):
            raise RuntimeError("DuckDB storage mode/build/schema identity mismatch; rebuild this benchmark store")

    def columns(self, table: str) -> list[Column]:
        rows = cast(
            list[tuple[int, str, str, bool, object, bool]],
            self.db.raw_connection().execute(f"PRAGMA table_info({literal(table)})").fetchall(),
        )
        return [Column(name=name, dtype=dtype, not_null=nn, primary_key=pk) for _, name, dtype, nn, _, pk in rows]

    def view_digest(self, table: str) -> str:
        rows = cast(
            list[tuple[str]],
            self.db.raw_connection()
            .execute(
                "SELECT sql FROM duckdb_views() WHERE database_name=current_database() "
                f"AND schema_name='main' AND view_name={literal(table)}"
            )
            .fetchall(),
        )
        if len(rows) != 1:
            raise RuntimeError(f"Expected one DuckDB view: {table}")
        return sha256_text(rows[0][0])

    def record_native(self, table: str, sources: list[Path] | None = None) -> None:
        manifest = self.read()
        previous = manifest.relations.get(table)
        manifest.relations[table] = Relation(
            columns=self.columns(table),
            sources=[SourceFile.inspect(path) for path in sources]
            if sources is not None
            else (previous.sources if previous else []),
        )
        self.write(manifest)

    def register(self, table: str, path: Path, columns: list[Column], projection: str) -> None:
        manifest = self.building()
        if table in manifest.relations:
            raise RuntimeError(f"Duplicate DuckDB Parquet registration: {table}")
        source = SourceFile.inspect(path)
        scan = f"read_parquet([{literal(source.path)}], hive_partitioning=false, union_by_name=false)"
        query = f"SELECT {projection} FROM {scan}"
        expected_names = [col.name for col in columns]
        source_names = [
            row[0]
            for row in cast(
                list[tuple[str, str, object, object, object, object]],
                self.db.raw_connection().execute(f"DESCRIBE SELECT * FROM {scan}").fetchall(),
            )
        ]
        if source_names != expected_names:
            raise RuntimeError(f"Parquet source column order/names differ from canonical schema for {table}")
        counts_sql = ", ".join(f"count({identifier(col.name)})" for col in columns)
        counts = cast(
            list[tuple[int, ...]],
            self.db.raw_connection().execute(f"SELECT count(*), {counts_sql} FROM ({query})").fetchall(),
        )
        for col, non_null_count in zip(columns, counts[0][1:], strict=True):
            if (col.not_null or col.primary_key) and non_null_count != counts[0][0]:
                raise ValueError(f"NULL violates canonical constraint: {table}.{col.name}")
        pk = ", ".join(identifier(col.name) for col in columns if col.primary_key)
        if (
            pk
            and self.db.raw_connection()
            .execute(f"SELECT 1 FROM ({query}) GROUP BY {pk} HAVING count(*) > 1 LIMIT 1")
            .fetchall()
        ):
            raise ValueError(f"Duplicate canonical primary key in {table}")
        if table in self.db.get_table_names():
            if self.db.get_row_count(table) != 0:
                raise RuntimeError(f"Refusing to replace populated relation: {table}")
            self.db.execute(f"DROP TABLE {identifier(table)}")
        self.db.execute(f"CREATE VIEW {identifier(table)} AS {query}")
        actual = self.columns(table)
        if [(col.name, col.dtype) for col in actual] != [(col.name, col.dtype) for col in columns]:
            raise RuntimeError(f"Parquet view schema mismatch: {table}")
        metadata = cast(
            list[tuple[object, ...]],
            self.db.raw_connection()
            .execute(
                f"SELECT compression, count(DISTINCT row_group_id), sum(num_values) "
                f"FROM parquet_metadata({literal(source.path)}) GROUP BY compression"
            )
            .fetchall(),
        )
        footer = ParquetFile(source.path).metadata
        manifest.relations[table] = Relation(
            columns=columns,
            sources=[source],
            view_sha256=self.view_digest(table),
            projection=projection,
            parquet_metadata=[dict(zip(("compression", "row_groups", "values"), row, strict=True)) for row in metadata],
            parquet_file={
                "created_by": footer.created_by,
                "format_version": footer.format_version,
                "rows": footer.num_rows,
                "row_groups": footer.num_row_groups,
            },
        )
        self.write(manifest)

    def verify(self, *, ready: bool = True) -> Manifest:
        manifest = self.read()
        self.check_identity(manifest)
        if ready and manifest.state != "ready":
            raise RuntimeError("DuckDB storage is incomplete; rebuild this benchmark store")
        if self.db._current_operation == "select" and manifest.dirty:
            raise RuntimeError("DuckDB store was mutated; rebuild pristine data before canonical SELECT")
        if not manifest.relations or self.db.get_table_names() != set(manifest.relations):
            raise RuntimeError("DuckDB catalog relations differ from manifest; rebuild this benchmark store")
        required = set(self.db.benchmarks[self.db.current_suite].expected_table_row_counts())
        if not required.issubset(manifest.relations):
            raise RuntimeError("DuckDB manifest is missing required suite tables; rebuild this benchmark store")
        planned = planned_sources(self.db)
        for source in manifest.inputs:
            if SourceFile.inspect(Path(source.path)) != source:
                raise RuntimeError(f"DuckDB input source changed: {source.path}; rebuild this benchmark store")
        kinds = dict(
            cast(
                list[tuple[str, str]],
                self.db.raw_connection()
                .execute(
                    "SELECT table_name, table_type FROM information_schema.tables "
                    "WHERE table_catalog=current_database() AND table_schema='main'"
                )
                .fetchall(),
            )
        )
        for table, relation in manifest.relations.items():
            if table in planned and [SourceFile.inspect(planned[table]).path] != [s.path for s in relation.sources]:
                raise RuntimeError(f"DuckDB source path identity mismatch: {table}; rebuild this benchmark store")
            actual = self.columns(table)
            expected = relation.columns
            if self.db.storage_mode == "native":
                valid = kinds.get(table) == "BASE TABLE" and actual == expected
            else:
                valid = (
                    kinds.get(table) == "VIEW"
                    and [(c.name, c.dtype) for c in actual] == [(c.name, c.dtype) for c in expected]
                    and self.view_digest(table) == relation.view_sha256
                )
            if not valid:
                raise RuntimeError(f"DuckDB physical layout/schema mismatch: {table}; rebuild this benchmark store")
            for source in relation.sources:
                if SourceFile.inspect(Path(source.path)) != source:
                    raise RuntimeError(f"DuckDB source changed: {source.path}; rebuild this benchmark store")
        return manifest

    def finalize(self) -> None:
        self.db.benchmarks[self.db.current_suite].verify_populated_data()
        manifest = self.verify(ready=False)
        required = set(self.db.benchmarks[self.db.current_suite].expected_table_row_counts())
        if not required or set(manifest.relations) != required:
            raise RuntimeError("DuckDB population manifest does not contain exactly the required suite tables")
        self.db.execute("CHECKPOINT")
        manifest.state = "ready"
        self.write(manifest)

    def external_files(self) -> tuple[Path, ...]:
        if self.db.storage_mode == "native":
            return ()
        references: set[Path] = set()
        if self.path.exists():
            for relation in self.read().relations.values():
                references.update(Path(source.path) for source in relation.sources)
        if self.db._current_operation == "populate":
            references.update(planned_sources(self.db).values())
        directory = self.db.database_directory.resolve()
        external: list[Path] = []
        inodes = {
            (stat.st_dev, stat.st_ino)
            for path in directory.rglob("*")
            if path.is_file() and not path.is_symlink()
            for stat in [path.stat()]
        }
        for reference in sorted(references):
            source = Path(SourceFile.inspect(reference).path)
            stat = source.stat()
            inode = (stat.st_dev, stat.st_ino)
            if not source.is_relative_to(directory) and inode not in inodes:
                external.append(source)
                inodes.add(inode)
        return tuple(external)

    def metadata(self) -> dict[str, object]:
        if not self.path.exists():
            return {"mode": self.db.storage_mode, "state": "uninitialized", "manifest_version": MANIFEST_VERSION}
        manifest = self.read()
        return {
            **cast(dict[str, object], json.loads(manifest.model_dump_json())),
            "manifest_sha256": sha256_text(manifest.model_dump_json()),
            "canonical_schema_sha256": sha256_text(
                json.dumps(
                    {
                        name: [column.model_dump() for column in relation.columns]
                        for name, relation in sorted(manifest.relations.items())
                    },
                    sort_keys=True,
                )
            ),
            "layout_revision": MANIFEST_VERSION,
        }
