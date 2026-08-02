from ..dbs.monetdb.manifest import MonetDBBenchmarkCell, monetdb_benchmark_manifest


def test_monetdb_release_manifest_tracks_registered_suites_and_operations() -> None:
    manifest = monetdb_benchmark_manifest()

    # chat_threads contributes 8 cells: two scale factors by four operations
    assert len(manifest) == 28
    assert len(set(manifest)) == len(manifest)
    assert MonetDBBenchmarkCell("rtabench", 1, "populate") in manifest
    assert MonetDBBenchmarkCell("time_series", 10, "concurrent") in manifest
    assert MonetDBBenchmarkCell("tpc_h", 50, "select") in manifest
    assert MonetDBBenchmarkCell("chat_threads", 1, "mutate") in manifest
    assert MonetDBBenchmarkCell("chat_threads", 10, "concurrent") in manifest
    assert all(cell.suite != "jsonbench" for cell in manifest)
