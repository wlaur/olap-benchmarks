# MessagePack write with an all-null column stores it as VARCHAR and breaks the measurement

**Status: draft, not filed.** Reproduced on Arc 26.09.3 on 2026-10-10.

## Summary

The columnar MessagePack write API (`POST /api/v1/write/msgpack`) infers each column's type
from the values in a single request. When a column is `nil` in every row of a request, Arc writes
it as an all-null **string** column in that request's Parquet file(s). Every later query then
binds the column as `VARCHAR` across the whole measurement, so numeric aggregates such as
`avg(value)` fail with a binder error, including over rows that were written as floats.

A run of nulls inside one request is ordinary for time-series data: a sensor outage, a
sparse field, or a small batch that falls inside a gap. The source carries a comment (#337)
that defends string as a safe placeholder because readers union files by name. In practice the
union widens the column to `VARCHAR`.

## Versions

| | |
| --- | --- |
| Reproduces on | Arc `26.09.3` (`version=26.09.3` in the startup log, DuckDB v1.5.5) |
| Image | `basekicklabs/arc:26.09.3` (`sha256:fea780f6ac0f41714afb6a8f8d5af6c363681cf947f0f9ac4d48e6df82595378`) |
| Platform | `linux/arm64`, Apple Silicon (OrbStack) |
| Config | image defaults plus `ARC_AUTH_ENABLED=false`, `ARC_TELEMETRY_ENABLED=false` |

## Reproduction

```bash
docker run -d --rm --name arc-repro -p 18000:8000 \
  -e ARC_AUTH_ENABLED=false -e ARC_TELEMETRY_ENABLED=false basekicklabs/arc:26.09.3
sleep 5
uv run --no-project --with msgpack --with httpx python repro.py
docker rm -f arc-repro
```

`repro.py`:

```python
import httpx, msgpack

arc = httpx.Client(base_url="http://localhost:18000", headers={"x-arc-database": "repro"})

def write(columns: dict) -> None:
    body = msgpack.packb({"m": "readings", "columns": columns})
    arc.post("/api/v1/write/msgpack", content=body, headers={"content-type": "application/msgpack"}).raise_for_status()
    arc.post("/api/v1/write/line-protocol/flush").raise_for_status()

def query(sql: str) -> str:
    return arc.post("/api/v1/query", json={"sql": sql}).text

t0 = 1_704_067_200_000_000  # 2024-01-01T00:00:00Z in microseconds
write({"time": [t0, t0 + 60_000_000], "value": [1.5, 2.5]})
print(query("select avg(value) as avg_value from readings"))

# a later batch in which "value" happens to be null in every row (e.g. a sensor outage)
write({"time": [t0 + 7_200_000_000], "value": [None]})
print(query("select typeof(value) as value_type from readings where time >= '2024-01-01 02:00:00'"))
print(query("select avg(value) as avg_value from readings"))
```

## Observed

```
{"success":true,"columns":["avg_value"],"data":[[2]],...}
{"success":true,"columns":["value_type"],"data":[["VARCHAR"]],...}
{"success":false,...,"error":"arrow query failed: Binder Error: No function matches the given name and argument types 'avg(VARCHAR)'. You might need to add explicit type casts. ..."}
```

The Parquet file written for the second request stores `value` as `BYTE_ARRAY`. The files
from the first request keep `DOUBLE`.

## Expected

An all-null column in one request should not change the column's type for the measurement.
`avg(value)` should still return `2.0`, and the new row should read as `NULL`. Any of these
would work:

- take the type from the measurement's existing schema (the `_schema` anchor Arc already keeps)
  when a request carries no values for a column;
- leave an all-null column out of the written file, so union-by-name fills it with `NULL`;
- reject the write, rather than accept it and break every later read of the column.

## Workaround

- Clients can drop columns that are null in every row of a request before sending it. Probing
  confirmed that a file without the column reads back as `NULL` with the type taken from the
  other files, whichever order the files were written in.
- The olap-benchmarks harness instead loads through `POST /api/v1/import/parquet`, which takes
  column types from the Parquet schema, so all-null columns keep their declared type (see
  `olap_benchmarks/dbs/arc/__init__.py`). The harness's populate batches of the time_series data
  routinely contain columns that are null for a whole batch, so MessagePack ingest corrupted the
  column types.
- A measurement that is already affected stays broken until the offending files are deleted or
  rewritten.
