from ..dbs.monetdb.manifest import MonetDBBenchmarkCell, monetdb_benchmark_manifest


def test_monetdb_release_manifest_tracks_registered_suites_and_operations() -> None:
    manifest = monetdb_benchmark_manifest()

    assert len(manifest) == 20
    assert len(set(manifest)) == len(manifest)
    assert MonetDBBenchmarkCell("rtabench", 1, "populate") in manifest
    assert MonetDBBenchmarkCell("time_series", 10, "concurrent") in manifest
    assert MonetDBBenchmarkCell("tpc_h", 50, "select") in manifest
    assert all(cell.suite != "jsonbench" for cell in manifest)
