# Bounded AI-assisted OLAP tuning

The comparison targets performance achievable with bounded AI-assisted tuning,
including complex engine-specific SQL. Every implemented database/suite pair
receives the same review criteria. Unimplemented suites are coverage gaps, not
measured execution failures. Repeated scale/width variants share a source review,
with their differences and deferred work recorded explicitly.

## Review and experiment budget

Review native storage and completed preparation, result-cache policy, filter
placement, repeated expressions and CTE reuse, joins/statistics, casts,
aggregations/sorts/windows, and driver/materialization costs. Record source/plan
evidence, uncertainty, and no-change conclusions for every pair.

Allow up to three distinct candidate patterns and twenty minutes of live
diagnostics per database/suite pair, across its scales. Prior MonetDB Q08 work
counts toward its chat budget. A reusable change may cover several queries;
the ledger must list them. Population and final measured reruns are outside
this exploration allowance. Cache comparability and unfinished required
preparation are corrections, recorded separately from performance tuning.

Use physical-plan gates to avoid timing candidates that do not change the
intended work. Reject an unchanged plan unless another concrete execution
mechanism is evidenced. A missing live store or exhausted allowance is a
documented deferral, never a claim that no improvement is possible.

## Allowed changes and correctness

Complex rewrites, engine-native syntax, and documented physical designs are
allowed. Preserve the full workload, values, NULL behavior, duplicates, output
types, deterministic tie breakers, and every applicable scale/shape. Validate
exact schemas and ordered answers as well as the harness answer hash. Include
empty and adversarial fixtures where query transformations could hide errors.
Retain existing justified cross-engine comparison exclusions; compare a changed
engine query against its own original form even when peer comparison is excluded.

Keep all query computation inside the normal timed fetch/materialization
boundary. Precomputed answers and aggregate tables are separate workload tracks.
Physical-design or statistics changes include preparation, ingestion, storage,
mutation and concurrency costs as applicable. No reduced inputs, approximate
answers, artificial intermediate row limits, or dataset-specific constants may
replace required work.

## Serial live protocol

One live engine/target statement at a time, including across subagents. Keep
ten CPUs, 24 GiB engine memory, no container swap, the client and disk guards,
45-second query and 900-second operation limits. Diagnostic clients also have
60-second external watchdogs. A client kill is not proof of server cancellation:
stop and inspect failures before continuing. No tests/builds or other measured
workloads may compete with live measurements.

Separate diagnostic comparisons from final benchmark samples. Use original /
candidate controls and reverse their order when repeating. Require matching
answers, the intended plan change and a repeatable practical improvement;
overlapping/noisy observations do not justify a performance claim. Keep rejected
experiments and reasons in the tuning ledger without promoting their timings.

## Adoption and publication

Assess latency alongside memory and storage. For each selected query report
sampled server anonymous/shared memory, mapped-file/page-cache charge, and client
RSS separately, with sample counts and observed windows. In-process engines use
client RSS. These are process/container footprints during execution, not the
query's exclusive allocation. Do not add independently sampled peak values or
rank a memory-mapped engine by heap alone. Client readings summarize the prior
sampling interval; exclude intervals crossing query boundaries from strict
per-query summaries. Sparse or absent samples mean unknown, never zero.

Report persistent database size, peak observed storage, and preparation time at
suite level. Whole-store disk samples do not establish query spill bytes or
temporary-file peaks; require engine plan/profile evidence for those claims.
Disk sampling excludes client staging and may miss short-lived files. Keep OOM,
disk-reserve and timeout outcomes visible. A latency improvement with materially
higher memory or disk use is a tradeoff, not an unconditional efficiency win.
Avoid combining these dimensions into one score without explicit weights.
File-scanning engines must count referenced input files required at query time;
the size of symlink entries alone is not their storage footprint. Imported
engines can discard source files after loading, so distinguish required query
storage from optional retained imports and staging.

The present policy limits container engines to 24 GiB and separately guards
client RSS at 28 GiB; this is not a uniform whole-job memory limit. DuckDB's
24 GiB internal allocator limit also differs from Polars's process RSS guard.
Report effective limits and observed footprints. A future memory-pressure suite
should use common whole-job limits and several data-to-memory ratios, with
engine spill profiles and cold/hot runs, before claiming a general memory-
efficiency ranking across server and in-process engines.

Commit validated changes before final reruns so every cohort has clean code
provenance. Query changes rerun the full affected select suites at applicable
scales; physical/common-setting changes also rerun all affected operations.
Retain bounded failure/capacity evidence. Delete completed old runs only when
validated replacements cover their query scope, inputs and configuration.
Do not lose useful extra observations merely because their run is older.

Publish tuning coverage, SQL, adopted/rejected proposals, budget limits and
remaining uncertainties alongside latency, resource use and completion coverage.
Three repeats support a campaign comparison, not general statistical confidence.
