# Overall database ranking

The homepage defaults to an overall index of **demonstrated speed and usable
coverage** across the published workloads. Higher is better on a 0–100 scale.
All publication databases remain listed, including those with partial or no
comparable evidence. This index describes the selected measurements and harness;
it does not establish a universal database winner or predict unmeasured capability.

Comparison metadata explicitly versions eligibility. Schema 1 uses
`relative-speed-v1`: a failed run earns zero for its entire cell. Schema 2 uses
`relative-speed-v2` with `certified-terminal-partial-v1`: individually usable
queries in a certified terminal failed SELECT may contribute; failed and missing
queries still earn zero. Neither policy changes the relative-speed formula,
fixed denominator, reference medians or strict workload comparison.

Both are retrospective indices. The dated campaign report's original “no overall
winner” statement remains historical. The frozen v1 acceptance results below
remain a v1 baseline, not a claim about a later v2 publication.

## Fixed workload and weights

The checksum-bound `site/public/data/comparison.json` supplies the canonical
queries, registered scales and existing family weights. The index includes its
11 positive-weight raw families, each with declared weight 1:

- Time Series: SF1 and SF10, 62 queries per scale.
- Time Series Metrics: SF1, 112 queries.
- Time Series Ranges: SF1, 120 queries.
- RTABench raw: SF1, 31 queries.
- ClickBench: SF1, 42 comparable queries.
- JSONBench: SF10, five queries.
- Chat Threads: SF1 and SF10, 23 queries per scale.
- TPC-H: SF10 and SF50, 22 queries per scale.
- TPC-DS: SF1 and SF10, 99 queries per scale.
- Device Telemetry: SF1 and SF10, 18 queries per scale.
- Join Order Benchmark: SF1, 113 queries.

These are **871 fixed query/scale positions**. Each family receives 1/11 of the
index, divided equally among its declared scales, then equally among its
canonical queries. More scales or queries do not give a family more influence.
Every database retains the entire denominator, including unmeasured positions.
The four time-series families, including Device Telemetry, plus RTABench account
for 5/11 of the weight. That workload mix is a choice, not a neutral sample of
all database uses.

RTABench's 10 prepared aggregates, both JSON Representation tracks (raw
extraction and prepared fields), Common Parquet Analytics and Airbnb have zero
weight. Common Parquet reuses TPC-H inputs; representation and preparation
variants do not create extra votes for the underlying work. JSONBench remains
included as its own raw family. ClickBench Q28 remains visible in workload
execution coverage but is outside the 871 comparable positions because its
regex answers differ across engines. Analytical throughput, preparation time,
memory and storage remain separate observations.

## Usable measurements and publication errors

Under v1, a database's selected run for a family/scale must be completed,
non-invalidated and free of a recorded run error, with a known input fingerprint
and no documented incomplete preparation. V2 additionally admits a failed run
only with a certificate for that entire selected SELECT. Its adapter and scale must be registered.
A query is usable when exactly one warmup and three measured
iterations succeed with the correct iteration identities, the measured fetch
durations are positive and finite, and no wrong-result flag, answer conflict or
documented semantic exclusion blocks comparison. Its timing `t` is the median
of those three measurements.

**Completed partial runs may contribute their individually usable queries.**
An uncertified failed run or otherwise blocked run contributes zero for its
entire cell. V2 certification requires the full canonical population and query
plan, clean provenance, exact raw and final database hashes, selected and origin
run identities, whole-run content fingerprints (including metrics and snapshot),
the actual population origin and mapped ID, hashed population and lineage audits,
and a clean report covering ALL answers and iterations. The publisher reruns the
existing publication gate and read-only answer validation including failed runs.
Invalidation, wrong results, conflicts, incomplete preparation and interrupted or
unsuccessful validation cannot be certified. Certificates never select a subset
of successful queries. The reader verifies database bytes and selected identity;
raw failure status and errors remain visible. Successful execution
alone is not independent correctness proof: peer hashes, repeat-only evidence,
hash limits and unknown correctness remain distinguishable in the explorer.

Publication verification fails for any of the following; the index must not
silently choose a favorable subset:

- Duplicate selected engine/version/driver variants for a database and cell.
- A selected run at an unregistered scale.
- Mixed input fingerprints among otherwise scorable runs in a family/scale.
- Unresolved answer conflicts.
- Fewer than two runs passing the run-level rules in any weighted family/scale.
- Fewer than two usable engine results for **any** weighted query/scale position.

In particular, insufficient query evidence is a publication error, not grounds
to drop the query, change its weight or award an uncontested reference. This
prevents withholding a competitor's result from silently deleting the fastest
database's credit. A revision that violates the rule requires resolution before
publication.

## Relative-speed points

For each canonical query/scale, `t*` is the fastest usable median across the
entire publication, including usable queries in eligible partial runs. A usable
timing `t` earns:

```text
Query points = 100 * t* / t
Query points = 0 when the position is not usable
```

The fastest result earns **100 points**, twice as slow earns **50**, ten times
as slow earns **10**, and 100 times as slow earns **1**. There is no logarithm,
tuned constant or cutoff. Missing, unsupported, skipped, timed-out or blocked
positions earn zero without an invented runtime. Positive usable timings retain
positive credit, apart from possible floating-point underflow at extreme ratios.

Average query points within each scale, then scales within each family, then
families using their published weights. Equivalently:

```text
w(f,s,q) = a(f) / sum_f a(f) / |S(f)| / |Q(f)|
Overall score(d) = sum_(f,s,q) w(f,s,q) * query_points(d,f,s,q)
```

Every engine retains the same fixed denominator. A specialist cannot earn a
perfect overall score from its successful subset. Scores satisfy
`0 <= score <= weighted usable coverage <= 100`. A score of 80 means 80 weighted
relative-speed points; it is neither 80% correctness nor a latency multiplier.
An engine with twice another's score need not finish the full workload twice
as fast.

## Overview and details

The initial table shows rank, database, score and the count of usable query/scale
positions out of the full 871. These counts are unweighted; families with more
queries contribute more positions but receive no extra score weight. “Queries
scored” does not mean independent correctness verification.

Expand a database to see published versions, per-workload points, measurement
gaps and contributing run IDs. Weighted coverage partitions the fixed workload:

- **Usable:** the query passes the measurement and comparison rules.
- **Recorded gap:** a selected registered run exists, but the query is unusable,
  including blocked runs and unstarted queries in that run.
- **No run:** no selected run exists, including unimplemented adapters.

These percentages sum to 100 before rounding and describe published evidence,
not underlying database capability. The workload count in the details means
families with at least one usable query.

Rank by descending score, treating scores equal at the displayed **0.1-point
precision** as ties. Ties share a competition rank (1, 1, 3); alphabetical order
within a tie does not break it. An engine with no usable evidence remains listed
with zero points and no rank. A positive score rounding to 0.0 remains ranked.
References, scores and ranks use the full publication; hiding engines changes
none of them.

The details retain a rank-sensitivity check: the minimum and maximum rank across
the full score and each leave-one-family-out calculation, renormalizing remaining
weights. An omission leaving no usable results is unranked and counted separately.
These are workload-mix sensitivity checks, not confidence intervals.

Concurrency measurements, campaign notes and measurement context are collapsed
by default and remain available on the overview.

## Difference from workload comparisons and the previous policy

The **By workload** view retains its strict family/scale latency comparison:
every required query must be usable, and the geometric mean of median-latency
ratios is lower-is-better. Eligible partial runs can earn overall points without
qualifying for that strict score.

Relative-speed points use an arithmetic average of reciprocal latency ratios.
This places more emphasis on queries close to the fastest result than the
previous logarithmic policy. It can reverse the strict geometric workload order:
ratios of 1× and 100× average 50.5 points, while two 4× results average 25 points,
even though their geometric ratios are 10× and 4× respectively. Neither is the
sum of query durations or elapsed run time.

On the frozen campaign, this policy differs from strict workload ordering in
**11 of 207 eligible engine-pair comparisons**. The previous logarithmic policy
was chosen retrospectively to avoid those differences. This revision explicitly
prioritizes a directly explainable formula; it is not selected to preserve a
preferred ranking. MonetDB overtakes StarRocks, and several middle positions
change. Scores from the two policies must not be compared as performance changes.
The canonical families, scales, queries and weights remain fixed; future changes
require a new policy identifier and pinned acceptance fixture.

The frozen-data acceptance fixture is
[site/tests/fixtures/overall.json](../site/tests/fixtures/overall.json). The
application-reader test in `site/tests/e2e/overall.spec.ts` checks scores, coverage,
rank sensitivity and the exact engine pairs whose workload order differs:

| Rank | Database | Score / 100 | Queries scored / 871 |
| --- | --- | ---: | ---: |
| 1 | DuckDB | 79.9 | 871 |
| 2 | ClickHouse | 41.1 | 871 |
| 3 | MonetDB | 29.8 | 788 |
| 4 | StarRocks | 28.5 | 787 |
| 5 | Polars | 15.1 | 323 |
| 6 | Doris | 11.7 | 319 |
| 7 | QuestDB | 10.4 | 426 |
| 8 | PostgreSQL | 8.2 | 622 |
| 9 | Arc | 7.4 | 421 |
| 10 | TimescaleDB | 6.7 | 509 |
| 11 | TDengine | 1.9 | 120 |

## Measurement limits and reproduction

The historical v1 acceptance evidence is revision `2026-10-followup`, selected release asset **143,929,344
bytes**, SHA-256
`832e28599501d125f2f4e0e35bb5251803955fbe1f6066f02c949c9df59f833f`.
The application calculates scores in `site/src/lib/overall.ts` using its shared
reader and query observations. Results databases are release assets, never
committed measurement files. Changing this score does not rerun benchmarks or
alter recorded observations.

This M4 Pro/macOS campaign uses warm caches and serial SELECT fetches. Timings
include planning, driver transport, transfer and materialization. Tiny queries
can favor embedded engines; no latency floor, deployment-cost adjustment or
10 ms offset is applied. Three measured fetches provide a median, not a
confidence interval. Adapter coverage, layouts and tuning effort are unequal;
preparation and resource-enforcement scopes differ. The score cannot isolate
execution kernels, predict production workloads or establish a general
memory-efficiency ranking. The four time-series families plus RTABench carry
5/11 of the weight, an explicit workload-mix choice.

In that historical v1 publication, PostgreSQL and TimescaleDB TPC-DS SF1 Q11
was explicitly excluded, allowing completed partial runs to score the other 98
positions. At SF10, an external Q11 watchdog failed the entire run, so v1 awarded
zero for all positions, including earlier completions.

In the curated recovery publication, PostgreSQL SF1 instead retains a terminal
failed run with an observed Q11 timeout and a whole-run certificate: its other
98 usable queries can score under v2. TimescaleDB SF1 retains its historical
explicit exclusion. Unproven SF10 failed cohorts have no certificate and remain
zero. The unfinished newer TPC-DS generation, including interrupted PostgreSQL
final/run688, is deferred. The [curated recovery report](campaigns/2026-10-timeout-recovery-curated.md)
records the selected generation and exact origins.

The [follow-up report](campaigns/2026-10-followup.md) records tuning, replacements
and limitations. Changes to the asset, query sets, weights or scoring policy can
change the ranking; a difference between editions alone does not demonstrate a
database speed improvement.

## Reproducing v2 metadata

Finish the normal publication/export first. Then generate the final validation
report with `olap_benchmarks.results.certification.write_validation_report` and
provide the certificate sidecar to the existing command:

```sh
uv run olap results publication-metadata --output-directory site/public/data \
  --annotations-path annotations.json --certificates-path certificates.json \
  --evidence-directory evidence
```

This writes comparison metadata only; it never rewrites `results.db`. Ordinary
publication and metadata generation without certificates remain explicitly v1.
The certificate models in `olap_benchmarks/results/certification.py` define exact
fields, rejecting missing or extra fields, duplicate IDs, stale hashes and wrong
cells. The evidence directory supplies `<origin.revision>.db` sealed raw archives
(symlinks are supported), the hashed reports and their referenced evidence.
Canonical input files must be available read-only through the normal input-data
setting to verify population row counts. Use `run_content_sha256(db_path, run_id)`
for the fingerprint; all scientific fields and embedded metadata are preserved,
while relational IDs are normalized for the curator's import mapping.
