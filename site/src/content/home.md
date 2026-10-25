## October 2026 · M4 Pro

- **Measurement:** 1 warmup and 3 measured runs per query; the median includes driver, transfer and conversion time. Serial runs, warm OS caches. [Documented layouts and query tuning](https://github.com/wlaur/olap-benchmarks/blob/main/docs/campaigns/2026-10-tuning-protocol.md) are allowed.
- **Overall score:** speed and usable coverage across equally weighted raw workloads. A failed or blocked run scores 0 for its whole workload and scale. [Full method](https://github.com/wlaur/olap-benchmarks/blob/main/docs/overall-ranking.md).
- **By workload:** strict comparison; every comparable query must be usable. Lower is better; different units and eligibility from the overall score.
- **Zero overall weight:** RTABench prepared aggregates, both JSON Representation tracks, Common Parquet Analytics and Kaggle Airbnb (smoke workload).
- **ClickBench Q28:** counted for coverage, excluded from performance; regex answers differ by engine.
- **Concurrency:** single measurement windows; query mixes differ by engine. Not part of any ranking.
- **Correctness:** successful execution is not independent verification. Result details show peer-hash agreement and exclusions.
- **Not comparable:** across systems, or with published TPC benchmarks.

Sources: [follow-up report](https://github.com/wlaur/olap-benchmarks/blob/main/docs/campaigns/2026-10-followup.md) · [suite catalog](#/catalog)
