# Overall database ranking

The homepage defaults to an overall index of **demonstrated speed and usable
coverage** across the published workloads. Higher is better on a 0–100 scale.
All publication databases remain listed, including those with partial or no
comparable evidence. This index describes the selected measurements and harness;
it does not establish a universal database winner or predict unmeasured capability.

The policy is `bounded-log-utility-v1`, with slowdown cap `K = 100`. It is a
retrospective index of existing measurements, not a measurement policy declared
before the campaign. The dated campaign report's “no overall winner” statement
remains a historical description of its original publication; this later index
adds an explicit aggregation policy without rerunning or changing those results.

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

A database's selected run for a family/scale must be completed, non-invalidated
and free of a recorded run error, with a known input fingerprint and no
documented incomplete preparation. Its adapter and scale must be registered.
A query is usable when exactly one warmup and three measured
iterations succeed with the correct iteration identities, the measured fetch
durations are positive and finite, and no wrong-result flag, answer conflict or
documented semantic exclusion blocks comparison. Its timing `t` is the median
of those three measurements.

**Completed partial runs may contribute their individually usable queries.**
A failed or otherwise blocked run contributes zero for its entire database /
family / scale cell, even if some earlier queries completed. Successful execution
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

## Bounded log credit

For each canonical query/scale, `t*` is the fastest usable median across the
entire publication, **including usable queries in completed partial runs**.
For a usable timing `t`, define:

```text
u(t) = clamp(1 - ln(t / t*) / ln(100), 0, 1)
u    = 0 when the position is not usable
```

The fastest observed result earns 100% query credit, a result taking 10 times
as long earns 50%, and one taking 100 times as long or more earns zero. A usable
result at or beyond the cap still counts toward usable coverage. Missing,
unsupported, skipped, timed-out or correctness-blocked positions earn zero
credit without an invented runtime or finite timeout penalty.

Let `a(f)` be the declared family weight, `S(f)` its fixed scale set and `Q(f)`
its fixed comparable-query set:

```text
w(f,s,q) = a(f) / sum_f a(f) / |S(f)| / |Q(f)|

Overall index(d) = 100 * sum_(f,s,q) w(f,s,q) * u(d,f,s,q)
```

This gives `0 <= index <= usable coverage <= 100`. An index of 80 means
80 weighted query-credit points. It is not 80% correctness, 80% coverage or a
runtime ratio to another database. Fixed weights prevent a specialist from
receiving a perfect overall score for only its successful subset. The bounded
curve limits the influence of extreme slowdowns; it deliberately assigns the
same speed credit to a usable result beyond the cap and an evidence gap, while
coverage and gap reasons preserve that distinction.

## Coverage, ranks and drill-down

Every canonical position belongs to exactly one coverage category, using the
same weights as the index:

- **Usable:** the query passes the measurement and comparison rules, including
  valid timings that receive zero credit at the slowdown cap.
- **Recorded gap:** a selected registered run exists, but this query is not
  usable. This includes blocked runs, incomplete measurements and unstarted
  positions within a selected run.
- **Not measured:** no selected run exists, including unimplemented adapters or
  registered adapters without a published run.

The three weighted percentages sum to 100 before display rounding. They measure
evidence in this publication, not the database's underlying capabilities.
Per-family details retain scores, coverage, query counts, gap reasons and
contributing run IDs. The workload count means families with at least one usable
query, rather than fully completed families.

Rank by descending index, treating scores equal at the displayed **0.1-point
precision** as ties. Ties share a competition rank (1, 1, 3); name ordering
within a tie does not break it. A database with no usable evidence stays listed
with zero index and coverage, without a rank. A database with usable evidence
can be ranked even if all its timings receive zero speed credit. References,
scores and ranks use the entire publication and do not change when engines are
hidden.

The rank-sensitivity band is the minimum and maximum rank across the **full
index plus all 11 leave-one-family-out calculations**. Each omission
renormalizes the remaining family weights and applies the same display-precision
tie rule. If an omission leaves a database with no usable evidence, that case is
unranked and counted separately, not assigned a worst numeric rank. TDengine,
measured in one weighted family, has such an omission. These bands describe
dependence on the workload mix; they are not confidence intervals.

The **By workload** view retains the strict family/scale latency comparison:
every required comparable query must be usable, and its score is the geometric
mean of median-latency ratios to the strict eligible reference set, with lower
being better. Partial completed runs can earn overall credit without qualifying
for that strict score. The two views have different inclusion rules and units;
the overall index is not an across-suite latency multiplier.

## Retrospective selection and method sensitivity

`K = 100` was chosen after inspecting the frozen measurements, as the smallest
power-of-ten cap preserving all 207 pairwise orders from the strict full-family
comparisons. Tested caps from 70 upward had zero inversions; `K = 30` had four,
and the earlier reciprocal-credit draft had 11. Agreement with those existing
comparisons is a retrospective selection criterion, not independent validation
of the resulting overall order. Freeze `K = 100` under this policy identifier
for future use; do not retune it to preserve a preferred winner.
The canonical families, query sets, scales and weights are also frozen under
this policy. Changing their definitions requires an explicit policy revision
and an updated pinned acceptance fixture.

Middle ranks depend materially on reasonable method choices. At tested caps
of 150 and above, PostgreSQL overtakes Polars; at 1,000, PostgreSQL is fifth and
TimescaleDB sixth. A domain-balanced alternative puts Doris fifth. An
exploratory query-bootstrap check gives overlapping rank ranges within 6–9 for
PostgreSQL, Doris, QuestDB and TimescaleDB. These are **method and workload-mix
sensitivity checks**, not measurement confidence intervals. Query resampling
does not supply independent campaign repetitions.

This is a custom scalar publication policy. A geometric mean over only
successful families compares different workload sets; assigning failures an
arbitrary duration invents timings. The chosen index instead retains every
canonical weight and makes its speed/coverage tradeoff explicit. Other utility
curves or domain weights can produce different defensible rankings.

The reviewed frozen-data ranking is reproduced by
[`site/tests/fixtures/overall.json`](../site/tests/fixtures/overall.json) and the
application-reader test in `site/tests/e2e/overall.spec.ts`:

| Rank | Database | Index | Usable / recorded gaps / no run (%) | Rank band |
| --- | --- | ---: | --- | --- |
| 1 | DuckDB | 88.9 | 100.0 / 0.0 / 0.0 | 1 |
| 2 | ClickHouse | 73.7 | 100.0 / 0.0 / 0.0 | 2 |
| 3 | StarRocks | 57.4 | 81.7 / 0.1 / 18.2 | 3–4 |
| 4 | MonetDB | 51.3 | 74.9 / 6.9 / 18.2 | 3–4 |
| 5 | Polars | 29.6 | 45.5 / 0.0 / 54.5 | 5–8 |
| 6 | PostgreSQL | 27.7 | 75.1 / 24.9 / 0.0 | 5–9 |
| 7 | Doris | 26.3 | 45.2 / 0.2 / 54.5 | 5–10 |
| 8 | QuestDB | 26.0 | 44.6 / 0.9 / 54.5 | 6–9 |
| 9 | TimescaleDB | 25.9 | 66.1 / 24.8 / 9.1 | 6–9 |
| 10 | Arc | 20.9 | 43.8 / 1.7 / 54.5 | 9–10 |
| 11 | TDengine | 5.5 | 9.1 / 0.0 / 90.9 | 11* |

`*` Dropping TDengine's sole measured family leaves it unranked. Displayed
coverage parts may differ from 100 by 0.1 because of rounding. Ranks 5–10 are
within nine index points; their order depends on the workload mix.

The following checks keep the same observations and denominators:

| Alternative | Ranks 5–10 | Strict within-workload order inversions |
| --- | --- | ---: |
| Cap 30 | Polars, QuestDB, Doris, PostgreSQL, Arc, TimescaleDB | 4 / 207 |
| Cap 100 (published) | Polars, PostgreSQL, Doris, QuestDB, TimescaleDB, Arc | 0 / 207 |
| Cap 1,000 | PostgreSQL, TimescaleDB, Polars, Doris, QuestDB, Arc | 0 / 207 |
| Four equal domain weights, cap 100 | Doris, Polars, PostgreSQL, TimescaleDB, QuestDB, Arc | 0 / 207 |

The domain check assigns one quarter each to time series (Time Series, Metrics,
Ranges, Device Telemetry), events (RTABench, ClickBench), documents (JSONBench,
Chat Threads), and relational work (TPC-H, TPC-DS, JOB), divided equally among
families within each domain. These groupings are another subjective choice.
The cap and domain checks retain the first four database positions and TDengine
last; the leave-one-family-out check allows StarRocks and MonetDB to exchange
third and fourth. The new index is checked in the Pages build before deployment.

## Measurement limits and reproduction

The evidence is revision `2026-10-followup`, selected release asset **143,929,344
bytes**, SHA-256
`832e28599501d125f2f4e0e35bb5251803955fbe1f6066f02c949c9df59f833f`.
The application calculates the index in `site/src/lib/overall.ts` using its
shared reader and query observations; the comparison manifest fixes its scope.
Results databases are release assets, not committed measurement files.

This M4 Pro/macOS campaign uses warm caches and serial SELECT fetches. Timings
include planning, driver transport, transfer and materialization. Tiny queries
can therefore favor embedded engines. No 10 ms offset, latency floor or
deployment-cost correction is applied. Three measured fetches provide a median,
not a confidence interval. Adapter coverage, layouts and tuning effort are
unequal; preparation and resource-enforcement scopes also differ. The index
cannot isolate execution kernels, predict production workloads or establish a
general memory-efficiency ranking.

One frozen campaign asymmetry affects PostgreSQL and TimescaleDB TPC-DS. At SF1,
Q11 was an explicit campaign exclusion and the completed partial runs can earn
credit for 98 of 99 positions. At SF10, the external Q11 watchdog failed the
whole run, so all positions receive zero under the run-level rule, including
earlier completed queries. Crediting those earlier SF10 completions instead
would change each affected overall index by less than one point. This release
accepts and documents that status asymmetry rather than rewriting the evidence.

The [follow-up report](campaigns/2026-10-followup.md) records the campaign's
tuning, replacements and limitations. Changing the asset, canonical queries,
weights or policy can change the index; a difference between editions alone
does not demonstrate a database speed improvement.
