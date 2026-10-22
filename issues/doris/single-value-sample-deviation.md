# Sample standard deviation of one value differs from the canonical workload

Observed with the pinned Doris **4.1.5** ARM64 FE/BE containers on 2026-10-22.
This is a query-semantics investigation; no upstream report has been filed.

```sql
SELECT STDDEV_SAMP(CAST(1 AS DOUBLE)) AS deviation;
```

Doris returned `0.0`. The canonical DuckDB 2.0 query returned NULL. Sample
variance divides by the number of non-NULL observations minus one; the benchmark
uses NULL when fewer than two values are available. Doris's
[function documentation](https://doris.apache.org/docs/4.x/sql-manual/sql-functions/aggregate-functions/stddev-samp/)
describes sample standard deviation and the empty-input case, but does not specify
the singleton case. This report does not infer an undocumented upstream guarantee.

The native TPC-DS fixture first exposed the difference in Q17 on a one-observation
group: the canonical deviation was NULL and Doris returned zero. It can also affect
Q39's coefficient of variation. The harness explicitly preserves its canonical
contract with:

```sql
CASE WHEN COUNT(value) < 2 THEN NULL ELSE STDDEV_SAMP(value) END
```

The guard counts non-NULL values. It retains duplicate observations, produces
NULL for empty/all-NULL/singleton groups, and leaves groups with at least two
values to the native aggregate. Both the guard and aggregate remain inside the
normal timed SQL. No answer is supplied by the client or prepared in advance.

Regression coverage includes empty, NULL, singleton, duplicate and two-value
groups, plus the parent-controlled native all-query TPC-DS fixture. The full-data
SF1/SF10 comparisons still require their own native measurements and answer checks.
