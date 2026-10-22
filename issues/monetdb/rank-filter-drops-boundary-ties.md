# Filtering RANK drops rows tied at the boundary

Observed on 2026-10-22 with MonetDB Dec2025-SP4 / 11.55.9, Docker image
`wlaur/monetdb-container:11.55.9-1`, native `linux/arm64`. The client packages
pinned in `pyproject.toml` are `adbc-driver-monetdb==0.13.1` and
`sqlalchemy-monetdb-adbc==0.4.1`. The reference engine is DuckDB 2.0.0.
The diagnostic container used ten CPUs and a 24 GiB memory limit with no swap.

A direct `WHERE rk <= 2` returns four rows from an eight-row input where six
qualify. The unfiltered ranks are correct: both partitions have ranks
`1, 2, 2, 4`. An equivalent CASE predicate returns all six qualifying rows.
The behavior affects both an ordinary partition key and a NULL partition key;
it does not require ROLLUP or a populated benchmark database. The underlying
optimizer mechanism has not been confirmed. CASE is a tested equivalent
predicate workaround, not proof of a materialization barrier or a particular
optimizer transformation.

## Self-contained reproduction

Run these three statements on the pinned server through a SQL client. Each
includes the complete ordering used in the diagnostic. The retained helper
appended `ORDER BY g NULLS FIRST, v DESC, label` when executing the SQL; its JSON
record's `sql` field omitted that suffix.

```sql
SELECT *
FROM (
    SELECT g, v, label, RANK() OVER (PARTITION BY g ORDER BY v DESC) rk
    FROM (VALUES
        ('a', 9, 1), ('a', 8, 2), ('a', 8, 3), ('a', 7, 4),
        (CAST(NULL AS VARCHAR(8)), 9, 5), (NULL, 8, 6),
        (NULL, 8, 7), (NULL, 7, 8)
    ) t(g, v, label)
) ranked
ORDER BY g NULLS FIRST, v DESC, label;

SELECT *
FROM (
    SELECT g, v, label, RANK() OVER (PARTITION BY g ORDER BY v DESC) rk
    FROM (VALUES
        ('a', 9, 1), ('a', 8, 2), ('a', 8, 3), ('a', 7, 4),
        (CAST(NULL AS VARCHAR(8)), 9, 5), (NULL, 8, 6),
        (NULL, 8, 7), (NULL, 7, 8)
    ) t(g, v, label)
) ranked
WHERE rk <= 2
ORDER BY g NULLS FIRST, v DESC, label;

SELECT *
FROM (
    SELECT g, v, label, RANK() OVER (PARTITION BY g ORDER BY v DESC) rk
    FROM (VALUES
        ('a', 9, 1), ('a', 8, 2), ('a', 8, 3), ('a', 7, 4),
        (CAST(NULL AS VARCHAR(8)), 9, 5), (NULL, 8, 6),
        (NULL, 8, 7), (NULL, 7, 8)
    ) t(g, v, label)
) ranked
WHERE CASE WHEN rk <= 2 THEN 1 ELSE 0 END = 1
ORDER BY g NULLS FIRST, v DESC, label;
```

The first statement returns all eight rows, including both rank-2 ties in
each partition and rank 4 for each value 7. The second and third statements
should both return the following sequence:

```text
g     v  label  rk
NULL  9      5   1
NULL  8      6   2
NULL  8      7   2
a     9      1   1
a     8      2   2
a     8      3   2
```

Observed with the direct predicate:

```text
g     v  label  rk
NULL  9      5   1
NULL  8      6   2
a     9      1   1
a     8      2   2
```

Labels 7 and 3 are missing even though the unfiltered query assigns both
rank 2. The CASE predicate returns the expected six rows, including both
boundary ties in each partition. Local evidence is retained in
`data/temp/monetdb-rank-minimal.json` and `data/temp/check_monetdb_rank.py`;
these ignored diagnostic artifacts are not prerequisites for the SQL above.

## TPC-DS Q67 evidence and workaround

At SF10, DuckDB's native ROLLUP query and an independent UNION ALL expansion
both produce a 1,107-row qualifying pool before the final `LIMIT 100`.
MonetDB's native query with direct `rk <= 100` produces only 1,100 rows.
Casting the rank in the predicate did not correct this result.

Before the rank filter, the NULL category partition has 13,084 rows in each
engine, with exactly equal row multisets, including multiplicities. All four
rows tied at rank 99 are present. Filtering this partition at rank 100 should
retain 102 rows; MonetDB's direct predicate retains only 100. Correct
unfiltered ranks and missing qualifying ties localize the discrepancy to the
filtered result without establishing its internal cause.

The adopted Q67 adaptation retains the original native ROLLUP, aggregation,
rank window, complete final ordering and `LIMIT 100`. Its sole change is:

```sql
WHERE CASE WHEN rk <= 100 THEN 1 ELSE 0 END = 1
```

With that predicate, MonetDB's 1,107-row pool matches DuckDB exactly in both
row multiset and sequence, checked independently of the top-100 hash
comparisons. The pool diagnostic JSON's hash comparison used the top-100
reference and does not establish full-pool equality. The adopted top-100
query also passes the normal correctness policy and multiset check at SF1
and SF10. These are correctness diagnostics, not new benchmark timings.

The other portability corrections tested alongside Q67 make Q12/Q45/Q50's
ascending NULL placement explicit to match the reference contract and cast
all six Q44 AVG inputs to DOUBLE. Different implicit NULL defaults are not an
engine defect; the AVG casts address precision portability. Neither is
evidence for the rank-filter defect. Existing correctness hashing policy and
the frozen Q18/Q44/Q65/Q71 ordering-policy entries remain unchanged.

Offline tests cover the full query contracts, all six AVG inputs, boundary
ties at ranks 2 and 99, NULL partitions, duplicate rows and native ROLLUP
multiplicities in an in-memory DuckDB database. They check equivalence and
preserve the workaround; they do not reproduce a MonetDB optimizer defect
without running the affected server.
