# Remaining project work — 2026-10-19

The [expanded local campaign](docs/campaigns/2026-10-expanded.md) is complete:
99 combinations, 75 full supported groups and 24 measured partials. It fixed and
replayed the earlier StarRocks TPC-DS, Doris RTABench, Timescale mutation reporting,
planner-statistics and resource-budget issues. Larger scales now have measured
local coverage; specific resource exclusions replace the former blanket claim
that they cannot run here. The published site asset remains unchanged.

## Release and distribution

- [ ] **MonetDB amd64 SP4 image.** The verified native ARM64 override runs runtime
      11.55.9, but `monetdb/monetdb:Dec2025-SP4` is not published for amd64. Build
      and distribute an appropriate amd64 image, or wait for upstream, before a
      complete amd64 cloud campaign.
- [ ] **A new publishable campaign, when requested.** The local expansion spans
      multiple clean commits and snapshots and contains deliberate partials.
      Select and rerun a release cohort under one commit, snapshot and profile,
      then pass the existing validation/publication gates. Local completion does
      not authorize publishing these files.

## Coverage follow-ups

- [ ] **Larger-budget follow-ups, if useful.** The report records exact partial
      cohorts and resource evidence. In particular, Timescale ClickBench Q35–Q42
      and sixteen MonetDB chat SF10 tail queries lack complete four-iteration
      observations. Existing bounded partials remain valid; these are optional
      additions, not unfinished campaign combinations.
- [ ] **MonetDB JSONBench.** No suite is registered. Implement a complete adapter
      or document a demonstrated limitation; absence of implementation alone is
      not evidence that the engine cannot support it.
- [ ] **MonetDB rolling AVG.** Recheck the documented ROWS-frame correctness
      defect on a fixed engine, then retire the existing core/metrics registry
      exclusions only after equivalent answers are demonstrated.
- [ ] **Cloud campaign** per `CLOUD.md`, after the amd64 image issue is resolved.
      More resources can extend the documented local partials.
- [ ] **Timescale SQL duplication** (optional): consider a PostgreSQL fallback
      only if it preserves explicit Timescale-specific behavior and query hashes.

## Site and optional methodology

- [ ] **Show resource measurements.** Label container memory as server anon plus
      shmem and embedded-engine memory as client memory. PostgreSQL sampling has
      now been exercised; no blanket untested-sampling claim remains.
- [ ] **Arbitrary-query UI** over DuckDB-WASM, with the schema documented.
- [ ] **Engine-internal timings** as separate metrics, if wanted. End-to-end
      materialized client latency remains the benchmark measure.
- [ ] **Cold-cache profile** with a separate profile identifier and explicit cache
      treatment before each measured iteration.
- [ ] **JOB**, and **TSBS/InfluxDB 3** as complete future suite expansions.

Generated schema types are already checked in Pages CI, and the Explorer overflow
E2E assertion was corrected during the dependency refresh.
