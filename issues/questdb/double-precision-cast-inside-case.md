# DOUBLE PRECISION casts fail inside CASE

Reproduced on QuestDB 10.0.1, native Linux arm64, on 2026-10-18. The HTTP
SQL endpoint returns the error directly, so this does not depend on a driver.

Start a disposable server:

```sh
docker run --rm --init --name questdb-cast-repro -d -p 19000:9000 questdb/questdb:10.0.1
```

Submit each statement to `http://localhost:19000/exec` using its `query`
parameter, after the server becomes ready:

```sql
CREATE TABLE cast_probe (metric_name SYMBOL, value FLOAT);
INSERT INTO cast_probe VALUES ('m', 1.5);

SELECT CAST(value AS DOUBLE PRECISION) FROM cast_probe;

SELECT MAX(CASE WHEN metric_name = 'm'
                THEN CAST(value AS DOUBLE PRECISION) END)
FROM cast_probe;

SELECT MAX(CASE WHEN metric_name = 'm'
                THEN CAST(value AS DOUBLE) END)
FROM cast_probe;

SELECT CASE WHEN value IS NULL THEN CAST(NULL AS DOUBLE PRECISION)
            ELSE CAST(value AS DOUBLE PRECISION) END
FROM cast_probe;
```

The standalone cast succeeds and returns DOUBLE `1.5`. Both CASE statements
using `DOUBLE PRECISION` fail with HTTP 400 and `unsupported cast`. The
aggregate CASE using `DOUBLE` succeeds and returns DOUBLE `1.5`. Expected:
the accepted type spelling should work consistently inside CASE expressions.

The benchmark's QuestDB metrics/ranges loaders use `DOUBLE` for these casts.
A six-observation wide/EAV fixture verifies the calculation's seven output
columns, including null controls, null operands, positive and negative
branches, and an all-null hour. No answer-comparison exclusion is needed.

Remove the disposable server with `docker stop questdb-cast-repro`.
