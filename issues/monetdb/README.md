# MonetDB

| Document | What it is |
| --- | --- |
| [`master-pipeline-evaluation.md`](master-pipeline-evaluation.md) | Build, benchmark and findings for the `pp_hashjoin` morsel-driven pipeline engine merged into MonetDB master, measured against the Dec2025-SP3 ARM64 baseline |
| [`master-pipeline-evaluation/`](master-pipeline-evaluation/) | The scripts that produced it |

The evaluation lives here because it is a benchmark-campaign result — suite timings, server
memory, ingest throughput — that only means anything next to this harness and the results
databases that produced it. Its §6 lists the server defects it turned up; those are written up
and tracked separately, with the driver that exercises the server.

## Reproducing the evaluation

Needs a MonetDB master image, which has to be built from source — there is no published image
for an arbitrary commit. The build is an ordinary CMake build of a `git archive` of a MonetDB
checkout, on Ubuntu 24.04; §2 of the evaluation lists the packages and CMake options the
`56.0.0` line adds over the Dec2025-SP3 image, and §2.1 the startup defect that made a patched
build necessary at the time.

With an image tagged and the `monetdb-master-pipeline` branch checked out for its repinned
`MONETDB_RELEASE` and `gdk_debug` setting, run both passes from the repository root:

```sh
issues/monetdb/master-pipeline-evaluation/run-both.sh
```

`run-matrix.sh REVISION` runs a single pass of the suite/scale matrix. The `.sql` files are
comparison queries against the results databases, run through the read-only CLI:

```sh
./.venv/bin/olap results query --revision master \
    "$(cat issues/monetdb/master-pipeline-evaluation/compare.sql)"
```

The `monetdb-master-pipeline` branch must not be merged as-is — its release pin points at an
image that exists only on the machine that built it.
