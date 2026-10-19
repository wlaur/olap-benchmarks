# Bucket-shuffle full join loses matching rows after analytic aggregation

Observed on StarRocks **4.1.6**, image `starrocks/allin1-ubuntu:4.1.6`, native
ARM64 under OrbStack, with one backend, 10 CPUs and a 24 GiB container limit.

A full outer join of two grouped cumulative sums returns both sides as unmatched,
even when their non-null item and date keys are equal. The plan chooses
`FULL OUTER JOIN (BUCKET_SHUFFLE)`. Forcing a partitioned shuffle join restores
the matches. The execution defect is isolated to this observed plan; its internal
cause has not been established.

## Reproduction

Start a disposable single-backend server, then connect with a MySQL client to port
19030 as `root`, without a password, once the backend is ready:

```sh
docker run --rm -d --name starrocks-q51-repro --cpus 10 --memory 24g \
  -p 19030:9030 starrocks/allin1-ubuntu:4.1.6
```

Run this SQL in the disposable server:

```sql
CREATE DATABASE q51_repro;
USE q51_repro;

CREATE TABLE q51_web (
    item_sk BIGINT, ticket BIGINT, d_date DATE, price DECIMAL(7, 2)
)
DUPLICATE KEY(item_sk, ticket)
DISTRIBUTED BY HASH(item_sk) BUCKETS 16
PROPERTIES ('replication_num' = '1');

CREATE TABLE q51_store (
    item_sk BIGINT, ticket BIGINT, d_date DATE, price DECIMAL(7, 2)
)
DUPLICATE KEY(item_sk, ticket)
DISTRIBUTED BY HASH(item_sk) BUCKETS 16
PROPERTIES ('replication_num' = '1');

INSERT INTO q51_web VALUES
    (19, 1, '2000-01-02', 56.96), (19, 2, '2000-01-03', 1.00),
    (37, 1, '2000-01-02', 31.75), (41, 1, '2000-03-17', 178.84);
INSERT INTO q51_store VALUES
    (19, 1, '2000-01-02', 49.68), (37, 1, '2000-01-02', 11.89),
    (41, 1, '2000-03-17', 150.76), (53, 1, '2000-01-02', 1.13);

WITH web_v1 AS (
    SELECT item_sk, d_date,
           sum(sum(price)) OVER (
               PARTITION BY item_sk ORDER BY d_date
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
           ) cume_sales
    FROM q51_web GROUP BY item_sk, d_date
), store_v1 AS (
    SELECT item_sk, d_date,
           sum(sum(price)) OVER (
               PARTITION BY item_sk ORDER BY d_date
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
           ) cume_sales
    FROM q51_store GROUP BY item_sk, d_date
)
SELECT coalesce(web.item_sk, store.item_sk) item_sk,
       coalesce(web.d_date, store.d_date) d_date,
       web.cume_sales web_sales, store.cume_sales store_sales
FROM web_v1 web
FULL OUTER JOIN store_v1 store
    ON web.item_sk = store.item_sk AND web.d_date = store.d_date
ORDER BY item_sk, d_date;
```

Expected: five rows, including three matched pairs:

| item_sk | d_date | web_sales | store_sales |
| --- | --- | --- | --- |
| 19 | 2000-01-02 | 56.96 | 49.68 |
| 19 | 2000-01-03 | 57.96 | NULL |
| 37 | 2000-01-02 | 31.75 | 11.89 |
| 41 | 2000-03-17 | 178.84 | 150.76 |
| 53 | 2000-01-02 | NULL | 1.13 |

Observed: eight rows, with all four web rows and all four store rows unmatched.
`EXPLAIN` reports `FULL OUTER JOIN (BUCKET_SHUFFLE)`.

Replace `FULL OUTER JOIN store_v1` with `FULL OUTER JOIN [SHUFFLE] store_v1` and
repeat. All five expected rows are returned; the plan reports
`FULL OUTER JOIN (PARTITIONED)`. This uses the documented
[join execution hint](https://docs.starrocks.io/docs/best_practices/query_tuning/query_hint).

Stop the disposable server afterward:

```sh
docker stop starrocks-q51-repro
```

## Effect on TPC-DS query 51

The same defect reproduces with the suite's generated SF1 data. Restricting the
two channel CTEs to items 19, 37, 41, 53 and 71 produces 80 web rows and 286 store
rows, with 18 matching pairs. The full join repeatedly returns 366 entirely
unmatched rows instead of 348 rows. No qualifying date is NULL.

Query 51's later `ROWS` windows then see duplicate item/date keys introduced by
the broken join, explaining their unstable output. Its 100-row answer differed
across all four benchmark iterations. This is not an original tie at the final
`LIMIT` boundary.

Both an equivalent outer-join emulation and the shuffle hint matched DuckDB's
complete answer in six repeated diagnostic executions. The suite uses the
[single-hint override](../../olap_benchmarks/suites/tpc_ds/queries/starrocks/51.sql),
preserving the original grouping, windows, filter, ordering and limit.
