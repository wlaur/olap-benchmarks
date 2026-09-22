# MonetDB

| Document | What it is |
| --- | --- |
| [`master-pipeline-evaluation.md`](master-pipeline-evaluation.md) | Build, benchmark and findings for the `pp_hashjoin` morsel-driven pipeline engine merged into MonetDB master, measured against the Dec2025-SP3 ARM64 baseline |
| [`master-pipeline-evaluation/`](master-pipeline-evaluation/) | The scripts that produced it |

The evaluation lives here because it is a benchmark-campaign result — suite timings, server
memory, ingest throughput — that only means anything next to this harness and the results
databases that produced it. Its §6 lists the server defects it turned up; those are written up
and tracked separately, with the driver that exercises the server.

## Re-running against a new MonetDB build

The whole campaign is two commands once an image exists. Budget about an hour per pass on an
M4 Pro; `kaggle_airbnb` and `clickbench` are the long poles.

### 1. Get an image

A released version needs no build — use the published image and skip to step 2:

```text
monetdb/monetdb:<tag>                      linux/amd64
wlaur/monetdb-container:<version>-<rev>    native linux/arm64
```

An unreleased commit has to be built. The build is an ordinary CMake build of a `git archive`,
so the MonetDB checkout is never modified:

```sh
git -C ~/src/MonetDB fetch origin
git -C ~/src/MonetDB archive --format=tar --prefix=source/ origin/master \
    | gzip -1 > /tmp/ctx/MonetDB-src.tar.gz
docker buildx build --platform linux/arm64 --load \
    --tag wlaur/monetdb-container:<runtime_version>-<rev> /tmp/ctx
```

§2 of [the evaluation](master-pipeline-evaluation.md) lists what the `56.0.0` line needs beyond
the Dec2025-SP3 image — `libxxhash-dev` is a hard requirement, and the Parquet codecs
(`libsnappy`, `libzstd`, `libbrotli`) come with `-DWITH_SNAPPY=ON -DWITH_ZSTD=ON
-DWITH_BROTLI=ON`. §2.1 describes a startup defect that made a patched build necessary around
`bbc2d72f02`; it is fixed as of `ee305e491c`, so an unpatched build is the default now.

**The tag has to match what the harness derives**, which is
`wlaur/monetdb-container:{runtime_version}-{arm64_image_revision}`. Tagging a local build into
that scheme is the easiest way in:

```sh
docker tag my-build:latest wlaur/monetdb-container:56.0.0-3
```

### 2. Repin the harness

One line in `olap_benchmarks/dbs/monetdb/__init__.py`:

```python
MONETDB_RELEASE = MonetDBRelease(label="master-ee305e4", runtime_version="56.0.0", arm64_image_revision=3)
```

`label` only has to identify the build in the results; `runtime_version` and
`arm64_image_revision` are what build the ARM64 image name, and `expected_runtime_version`
asserts the server really is that version, so a stale image fails loudly instead of quietly
benchmarking the wrong build.

Do this on a branch. **A pin pointing at a local-only image must not reach `main`.**

### 3. Free port 50000

Other MonetDB containers — development ones from driver work, for instance — will hold it.
Stop them for the duration; the harness binds a fixed host port.

### 4. Run both passes

```sh
issues/monetdb/master-pipeline-evaluation/run-campaign.sh ee305e4
```

That runs the full suite matrix twice: once on MonetDB's stock default, once with the pipeline
engine on, writing `results/ee305e4.db` and `results/ee305e4-pp.db`. It refuses to overwrite an
existing revision and prints the pin it is about to benchmark. Pass a second argument to name
the pipeline-on revision yourself.

### 5. Compare

```sh
issues/monetdb/master-pipeline-evaluation/compare.sh ee305e4 ee305e4-pp
```

Suite-level warm totals, per-query deltas, and ingest/peak-memory, all against the Dec2025-SP3
baseline in `results/default.db`. Drop the second argument for a two-way comparison against the
pipeline-off pass alone.

### When the pipeline engine ships enabled by default

Most of this collapses. The second pass exists only because the engine is behind `GDKdebug`
bit 19 and off by default; once it is the default, run one pass and compare it against both SP3
and the `-pp` revisions recorded below. Check first, because the flag is the whole basis of the
split:

```sql
SELECT sys.debugflags();   -- reads without modifying; sys.debug() *sets* and returns the old value
```

`run-matrix.sh` and the `.sql` files stay useful either way; `run-campaign.sh` is the only piece
that assumes two passes.

## Revision ledger

Every campaign writes `results/<revision>.db`. These names are taken:

| Revision | Server | Pipeline | Notes |
| --- | --- | --- | --- |
| `default` | Dec2025-SP3, 11.55.7 | n/a | The baseline every comparison joins against |
| `master` | 56.0.0 @ `bbc2d72f02` | off | First pipeline-merge campaign |
| `master-pp` | 56.0.0 @ `bbc2d72f02` | on | |
| `master2` | 56.0.0 @ `76da78fba2` | off | |
| `master2_pipeline` | 56.0.0 @ `76da78fba2` | on | Partial |
| `tip` | 56.0.0 @ `ee305e491c` | off | |
| `tip-pp` | 56.0.0 @ `ee305e491c` | on | |

Name a new campaign after the build's short commit. `results/*.db` are not committed.

## Gotchas worth knowing before you start

- **The `-pp` pass can be slower overall.** The engine is violently bimodal: individual queries
  have gone 23× faster while whole suites went 5× slower, because a query shape the pipeline
  planner declines loses mitosis parallelism too. Do not read a slower total as a failed run.
- **ClickBench with the pipeline on has run to 42.9 GB on a 43.1 GB machine.** An OOM there is a
  plausible outcome, not necessarily a harness fault.
- **The resource metric lane drops roughly 4-7% of samples** in every campaign, including the
  SP3 baseline — `docker stats` takes seconds under load and the sampler times out and retries.
  Expected; not a reason to distrust a run.
- **Compare warm minimums over common queries only**, which is what these scripts do. A suite
  where one build fails a query has fewer rows to join, so totals across builds are not
  comparable unless the query sets match.
