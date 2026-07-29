from ..dbs.monetdb.insert import ColumnGroupWrite, RowBatchWrite, staged_write_for_column_count
from ..dbs.monetdb.manifest import MonetDBBenchmarkCell, monetdb_benchmark_manifest


def test_monetdb_release_manifest_tracks_registered_suites_and_operations() -> None:
    manifest = monetdb_benchmark_manifest()

    assert len(manifest) == 20
    assert len(set(manifest)) == len(manifest)
    assert MonetDBBenchmarkCell("rtabench", 1, "populate") in manifest
    assert MonetDBBenchmarkCell("time_series", 10, "concurrent") in manifest
    assert MonetDBBenchmarkCell("tpc_h", 50, "select") in manifest
    assert all(cell.suite != "jsonbench" for cell in manifest)


def test_staged_parquet_policy_reserves_column_groups_for_wide_schemas() -> None:
    assert isinstance(staged_write_for_column_count(105), RowBatchWrite)
    assert isinstance(staged_write_for_column_count(511), RowBatchWrite)
    assert isinstance(staged_write_for_column_count(512), ColumnGroupWrite)
