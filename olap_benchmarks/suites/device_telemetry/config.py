import logging
from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Final, cast

from ...dbs import Database
from ...settings import DatabaseName, Operation, SuiteName, TableName, get_settings
from .. import BenchmarkSuite
from .dataset import DEVICE_TABLE, READING_TABLE, dataset_files, expected_reading_count, prepare_dataset, scale_spec

if TYPE_CHECKING:
    from ...dbs.timescaledb import TimescaleDB

SUITE_NAME: Final = cast(SuiteName, "device_telemetry")
SUPPORTED_DATABASES: Final[frozenset[DatabaseName]] = frozenset({"duckdb", "postgres", "timescaledb", "clickhouse"})
QUERIES_DIRECTORY: Final = Path(__file__).parent / "queries"
QUERY_NAMES: Final = (
    "01_latest_device",
    "02_latest_per_device",
    "03_latest_region",
    "04_tenant_hour",
    "05_region_day",
    "06_fleet_hour_metrics",
    "07_device_day_raw",
    "08_device_hourly",
    "09_region_model_hourly",
    "10_fleet_daily",
    "11_busiest_devices",
    "12_threshold_breaches",
    "13_rolling_device",
    "14_reporting_gaps",
    "15_null_coverage",
    "16_metadata_power",
    "17_inactive_devices",
    "18_arrival_delay",
)
_LOGGER = logging.getLogger(__name__)


def input_directory(scale_factor: int) -> Path:
    scale_spec(scale_factor)
    return get_settings().input_data_directory / f"device_telemetry_sf{scale_factor}"


def prepare_data(scale_factor: int = 1, overwrite: bool = False) -> None:
    prepare_dataset(input_directory(scale_factor), scale_spec(scale_factor), overwrite=overwrite)


class DeviceTelemetry[DBT: Database](BenchmarkSuite[DBT]):
    name: SuiteName = SUITE_NAME
    supported_operations: ClassVar[tuple[Operation, ...]] = ("populate", "select")
    candidate_databases: ClassVar[frozenset[DatabaseName]] = SUPPORTED_DATABASES

    def dataset_files(self) -> Mapping[TableName, Path]:
        return dataset_files(input_directory(self.scale_factor))

    def expected_table_row_counts(self) -> Mapping[TableName, int]:
        spec = scale_spec(self.scale_factor)
        return {DEVICE_TABLE: spec.device_count, READING_TABLE: expected_reading_count(spec)}

    def planned_query_names(self) -> tuple[str, ...]:
        scale_spec(self.scale_factor)
        return QUERY_NAMES

    def _require_supported_database(self) -> None:
        if self.db.name not in SUPPORTED_DATABASES:
            raise NotImplementedError(f"device_telemetry has no verified loader/query contract for {self.db.name}")

    def load_query(self, query_name: str) -> str:
        self._require_supported_database()
        return super().load_query(query_name)

    def _verify_storage(self) -> None:
        if self.db.name == "timescaledb":
            cast("TimescaleDB", self.db).verify_telemetry_storage(
                {READING_TABLE: self.expected_table_row_counts()[READING_TABLE]}
            )

    def should_populate(self) -> bool:
        needed = super().should_populate()
        if not needed:
            self._verify_storage()
        return needed

    def verify_populated_data(self) -> None:
        super().verify_populated_data()
        self._verify_storage()

    @property
    def fetch_kwargs(self) -> Mapping[str, Any]:
        if self.db.name == "clickhouse":
            return {
                "time_columns": ["time", "bucket", "latest_time", "latest_arrival"],
                "settings": {
                    "cast_keep_nullable": 1,
                    "join_use_nulls": 1,
                    "prefer_column_name_to_alias": 1,
                    "enable_extended_results_for_datetime_functions": 1,
                },
            }
        return {}

    def populate(self, restart: bool = True) -> None:
        self._require_supported_database()
        with self.db.phase_context("verify_existing_data"):
            if not self.should_populate():
                return
        self.db.initialize_schema("device_telemetry")
        for table, path in self.dataset_files().items():
            required = ["device_id", "region", "tenant", "model"]
            required += (
                ["cadence_slots", "rated_power"] if table == DEVICE_TABLE else ["time", "sample_no", "ingested_at"]
            )
            with self.db.phase_context("insert", table_name=table):
                self.db.insert_parquet(path, table, not_null=required)
                _LOGGER.info("Inserted %s for %s", table, self.name)
        if self.db.name == "timescaledb":
            with self.db.phase_context("compress", table_name=READING_TABLE):
                self.db.execute(
                    f"SELECT compress_chunk(i, if_not_compressed => true) FROM show_chunks('{READING_TABLE}') i",
                    autocommit=True,
                )
        if self.db.name in ("postgres", "timescaledb"):
            for table in self.dataset_files():
                with self.db.phase_context("analyze", table_name=table):
                    self.db.execute(f"VACUUM FREEZE ANALYZE {table}", autocommit=True)
        with self.db.phase_context("verify_populate"):
            self.verify_populated_data()
        if restart:
            self.db.restart_event()
