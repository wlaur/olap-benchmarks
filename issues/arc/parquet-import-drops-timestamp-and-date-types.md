# Parquet import stores extra timestamp columns as BIGINT and rejects DATE

**Status: draft, not filed.** Reproduced on Arc 26.09.3 on 2026-10-10.

## Summary

`POST /api/v1/import/parquet` keeps a Parquet `timestamp` type only for the measurement's
`time` column. Any other timestamp column is stored as `BIGINT` epoch microseconds
(`arrowColumnToTyped` turns `*array.Timestamp` into `[]int64`). Queries then see integers:
`updated_at > TIMESTAMP '2024-01-01'` fails to bind and `SELECT updated_at` returns a number. A
`date32` column is rejected outright, whether it is the time column or not. The MessagePack API
has no timestamp or date type either, so a table with more than one timestamp, or with any date,
cannot keep its schema in Arc.

Tables with several timestamps are common: `created_at`/`updated_at`, or event time next to
client time (ClickBench's `hits` has `EventTime`, `ClientEventTime`, `LocalEventTime` and
`EventDate`).

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
uv run --no-project --with pyarrow --with httpx python repro.py
docker rm -f arc-repro
```

`repro.py`:

```python
import io
from datetime import date, datetime

import httpx
import pyarrow as pa
import pyarrow.parquet as pq

arc = httpx.Client(base_url="http://localhost:18000", headers={"x-arc-database": "repro"})


def import_parquet(measurement: str, table: pa.Table) -> None:
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    response = arc.post(
        "/api/v1/import/parquet", params={"measurement": measurement}, files={"file": ("t.parquet", buffer.getvalue())}
    )
    print(response.status_code, response.text[:200])


times = pa.array([datetime(2024, 1, 1), datetime(2024, 1, 1, 1)], type=pa.timestamp("us"))
updated = pa.array([datetime(2024, 1, 2), datetime(2024, 1, 3)], type=pa.timestamp("us"))
days = pa.array([date(2024, 1, 1), date(2024, 1, 2)], type=pa.date32())

import_parquet("with_timestamp", pa.table({"time": times, "updated_at": updated}))
print(arc.post("/api/v1/query", json={"sql": "select typeof(updated_at) as t, updated_at from with_timestamp"}).text)
import_parquet("with_date", pa.table({"time": times, "day": days}))
```

## Observed

```
200 {"result":{"database":"repro","measurement":"with_timestamp","rows_imported":2,...}
{"success":true,"columns":["t","updated_at"],"data":[["BIGINT",1704240000000000],["BIGINT",1704153600000000]],...}
422 {"error":"unsupported parquet column \"day\": unsupported arrow type date32"}
```

## Expected

`updated_at` should read back as a `TIMESTAMP` with the value `2024-01-02 00:00:00`, and the
`date32` column should be stored as `DATE`, or at least as a timestamp. Arc's Parquet files are
written with arrow-go, which supports both types, so the gap appears to be only in the import's
type conversion.

## Workaround

The olap-benchmarks harness imports dates as midnight-UTC timestamps (the same mapping its
QuestDB connector uses), accepts the resulting `BIGINT` columns, and converts them back with
`make_timestamp()` in the Arc query overrides wherever a query returns one. Filters on such a
column compare against `epoch_us(TIMESTAMP '…')`. See `olap_benchmarks/dbs/arc/__init__.py` and
`olap_benchmarks/suites/clickbench/queries/arc.sql`.
