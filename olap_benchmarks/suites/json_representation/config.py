from __future__ import annotations

import shutil
from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, cast

from ...dbs import Database
from ...settings import DatabaseName, SuiteName, get_settings
from .. import BenchmarkSuite
from ..jsonbench.config import (
    JSONBENCH_QUERY_NAMES,
    get_jsonbench_input_directory,
    get_jsonbench_input_files,
    write_jsonbench_input_file,
)
from ..jsonbench.config import (
    prepare_data as prepare_jsonbench_data,
)
from ..parquet_analytics.contract import RepresentationManifest, sql_literal

if TYPE_CHECKING:
    from ...dbs.clickhouse import Clickhouse
    from ...dbs.duckdb import DuckDB

SUITE_NAME = cast(SuiteName, "json_representation")
QUERY_NAMES = tuple(f"{track}_{name}" for track in ("raw", "prepared") for name in JSONBENCH_QUERY_NAMES)
SCHEMAS_DIRECTORY = Path(__file__).parent / "schemas"
JSON_SETTINGS: dict[str, int | str] = {
    "function_json_value_return_type_allow_nullable": 1,
    "function_json_value_return_type_allow_complex": 1,
    "cast_keep_nullable": 1,
    "short_circuit_function_evaluation": "enable",
}


def source_directory(scale_factor: int) -> Path:
    if scale_factor != 10:
        raise ValueError("json_representation supports only canonical JSONBench SF10")
    return get_jsonbench_input_directory(scale_factor)


def prepare_data(scale_factor: int) -> None:
    source_directory(scale_factor)
    prepare_jsonbench_data(scale_factor)


def prepared_projection(database: DatabaseName) -> str:
    fields = {
        "kind": ("kind",),
        "operation": ("commit", "operation"),
        "collection": ("commit", "collection"),
        "did": ("did",),
        "time_us_text": ("time_us",),
    }
    projection: list[str] = []
    for name, path in fields.items():
        jsonpath = sql_literal("$." + ".".join(path))
        if database == "duckdb":
            expression = f"json_extract_string(j, {jsonpath})"
        elif database == "clickhouse":
            components = ", ".join(sql_literal(component) for component in path)
            expression = f"if(JSONType(j, {components}) = 'Null', NULL, JSON_VALUE(j, {jsonpath}))"
        else:
            raise NotImplementedError(f"No prepared JSON extraction contract for {database}")
        projection.append(f"{expression} AS {name}")
    cast_time = "CAST(time_us_text AS BIGINT)" if database == "duckdb" else "toInt64(time_us_text)"
    return (
        "SELECT *, CASE WHEN kind = 'commit' AND operation = 'create' "
        "AND collection IN ('app.bsky.feed.post', 'app.bsky.feed.repost', 'app.bsky.feed.like') "
        f"THEN {cast_time} END AS time_us FROM (SELECT {', '.join(projection)} FROM json_raw)"
    )


class JSONRepresentation[DBT: Database](BenchmarkSuite[DBT]):
    name: SuiteName = SUITE_NAME
    candidate_databases: ClassVar[frozenset[DatabaseName]] = frozenset({"duckdb", "clickhouse"})

    @property
    def input_data_directory(self) -> Path:
        return source_directory(self.scale_factor)

    def expected_table_row_counts(self) -> dict[str, int]:
        return {"json_raw": 10_000_000, "json_prepared": 10_000_000}

    def planned_query_names(self) -> tuple[str, ...]:
        return QUERY_NAMES

    @property
    def fetch_kwargs(self) -> Mapping[str, Any]:
        if self.db.name == "clickhouse":
            return {"settings": JSON_SETTINGS, "time_columns": ["first_post_ts"]}
        return {}

    def representation_layout(self) -> dict[str, object]:
        schemas = (SCHEMAS_DIRECTORY / f"{self.db.name}.sql").read_text()
        layout: dict[str, object] = {
            "raw": "native unindexed JSON text",
            "prepared": "native unindexed extracted fields; no aggregation or precomputed answers",
            "schema_sql": schemas,
            "prepared_projection": prepared_projection(self.db.name),
            "timestamp_extrema": "lexical text before integer conversion; UTC milliseconds",
            "repair": "jsonbench.iter_jsonbench_input_lines; repaired fragments and empty-object rows retained",
            "required_source_files_after_populate": [],
            "storage_scope": "combined raw and prepared store; separate populate phases; no per-track disk attribution",
        }
        if self.db.name == "clickhouse" and self.db.get_table_names() == set(self.expected_table_row_counts()):
            layout["table_definitions"] = {
                table: self.db.fetch(f"SHOW CREATE TABLE {table}").item() for table in self.expected_table_row_counts()
            }
        return layout

    def verify_representation(self) -> None:
        RepresentationManifest.read(self.db.database_directory).verify(
            self.name,
            self.input_data_directory,
            get_jsonbench_input_files(self.scale_factor),
            self.representation_layout(),
        )
        if self.db.get_table_names() != set(self.expected_table_row_counts()):
            raise RuntimeError("JSON representation catalog differs from the two required tables")

    def _stage_input_files(self) -> Path:
        directory = get_settings().temporary_directory / self.db.name / "data" / "json_representation_sf10"
        if directory.exists():
            shutil.rmtree(directory)
        directory.mkdir(parents=True)
        for source in get_jsonbench_input_files(self.scale_factor):
            write_jsonbench_input_file(source, directory / source.name.removesuffix(".gz"))
        return directory

    def populate(self, restart: bool = True) -> None:
        source_directory(self.scale_factor)
        if self.db.name not in self.candidate_databases:
            raise NotImplementedError(f"No JSON representation contract for {self.db.name}")
        with self.db.phase_context("verify_existing_data"):
            if not self.should_populate():
                self.verify_representation()
                return
        with self.db.phase_context("source_contract"):
            manifest = RepresentationManifest.build(
                self.name,
                self.input_data_directory,
                get_jsonbench_input_files(self.scale_factor),
                self.representation_layout(),
            )
            manifest.write(self.db.database_directory)
        if self.db.name == "duckdb":
            db = cast("DuckDB", self.db)
            db.storage.building()
        with self.db.phase_context("schema"):
            self.db.execute_schema_file(SCHEMAS_DIRECTORY / f"{self.db.name}.sql")
        with self.db.phase_context("repair_input"):
            staging = self._stage_input_files()
        try:
            with self.db.phase_context("insert", table_name="json_raw"):
                if self.db.name == "duckdb":
                    files = ", ".join(sql_literal(path.as_posix()) for path in sorted(staging.glob("*.json")))
                    self.db.execute(
                        f"INSERT INTO json_raw SELECT CAST(json AS VARCHAR) FROM read_ndjson_objects([{files}])"
                    )
                else:
                    cast("Clickhouse", self.db).run_sql(
                        "INSERT INTO json_raw SELECT json FROM "
                        "file('json_representation_sf10/file_*.json', 'JSONAsString')"
                    )
        finally:
            shutil.rmtree(staging)
        with self.db.phase_context("materialize_fields", table_name="json_prepared"):
            sql = f"INSERT INTO json_prepared {prepared_projection(self.db.name)}"
            if self.db.name == "clickhouse":
                cast("Clickhouse", self.db).run_sql(sql, settings=JSON_SETTINGS)
            else:
                self.db.execute(sql)
                db = cast("DuckDB", self.db)
                for table in ("json_raw", "json_prepared"):
                    db.storage.record_native(table, get_jsonbench_input_files(self.scale_factor))
        with self.db.phase_context("verify_populate"):
            self.verify_populated_data()
            manifest.layout = self.representation_layout()
            manifest.state = "ready"
            manifest.write(self.db.database_directory)
        if restart:
            self.db.restart_event()
