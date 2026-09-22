# MonetDB

**The MonetDB server bug reports are not here.** They live in
[`adbc-driver-monetdb`](https://github.com/wlaur/adbc-driver-monetdb) under
`docs/monetdb-issues/`, next to the driver that exercises the server hardest and alongside
`repro/`, which builds a server from any MonetDB commit and re-runs every check. That
directory's `README.md` holds the authoritative status table.

What remains here is the one MonetDB document that is about *this* suite rather than about a
server defect.

| Document | What it is |
| --- | --- |
| [`master-pipeline-evaluation.md`](master-pipeline-evaluation.md) | Build, benchmark and findings for the `pp_hashjoin` morsel-driven pipeline engine merged into MonetDB master, measured against the Dec2025-SP3 ARM64 baseline |
| [`master-pipeline-evaluation/`](master-pipeline-evaluation/) | The scripts that produced it |

The evaluation is kept in this repository because it is a benchmark-campaign result — suite
timings, server memory, ingest throughput — that only means anything next to the harness and
the results databases that produced it. Its §6 lists the defects it turned up; those were
written up individually in the driver repository, which carries their current status.

## Reproducing the evaluation

Needs a MonetDB master image. Build one with the driver repository's tooling:

```sh
docs/monetdb-issues/repro/build-image.sh ~/src/MonetDB monetdb-master:local bbc2d72f02
```

Then, from this repository's root:

```sh
# pipeline off, then on
issues/monetdb/master-pipeline-evaluation/run-both.sh
```

`run-matrix.sh REVISION` runs one pass of the suite/scale matrix. The `.sql` files are
comparison queries against the results databases, run through the read-only CLI from the
repository root:

```sh
./.venv/bin/olap results query --revision master \
    "$(cat issues/monetdb/master-pipeline-evaluation/compare.sql)"
```

`run-matrix.sh` needs the `monetdb-master-pipeline` branch for its repinned `MONETDB_RELEASE`
and the `gdk_debug` setting. That branch must not be merged as-is — the release pin points at a
local-only image.
