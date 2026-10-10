# MessagePack write guesses the epoch unit from magnitude and silently corrupts timestamps

**Status: draft, not filed.** Reproduced on Arc 26.09.3 on 2026-10-10.

## Summary

The columnar MessagePack write API documents `time` as an integer epoch, and Arc stores
microseconds. The unit, however, is not declared. `normalizeTimestampColumns`
(`internal/ingest/msgpack.go`) guesses it from the **first** value of each request's `time`
column:

| first value | treated as |
| --- | --- |
| `< 1e10` | seconds |
| `< 1e13` | milliseconds |
| `< 1e16` | microseconds |
| otherwise | nanoseconds (divided by 1000) |

A microsecond timestamp at or after about 2286-11-20 (`1e16` µs) is therefore divided by 1000
and stored in 1970. The write returns `204` and nothing is logged beyond a "timestamp is >7 days
old" warning. Millisecond and second timestamps hit the same problem at other dates, for example
milliseconds before 1970-04-26 (`< 1e10`) are read as seconds. A batch whose rows span a
boundary gets one unit applied to all of its rows, because only element 0 is checked.

Far-future timestamps are uncommon in production. They do appear in synthetic and benchmark
data, and in sentinel or "valid until" columns. The harm is that the data is corrupted
silently, with no error.

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

# microsecond epochs for 2024-01-01 and 2400-01-01; the docs say time is epoch microseconds
times = [1_704_067_200_000_000, 13_569_465_600_000_000]
for t in times:
    one = msgpack.packb({"m": "events", "columns": {"time": [t], "value": [float(t > 2e15)]}})
    arc.post("/api/v1/write/msgpack", content=one, headers={"content-type": "application/msgpack"}).raise_for_status()
arc.post("/api/v1/write/line-protocol/flush").raise_for_status()
print(arc.post("/api/v1/query", json={"sql": "select time, value from events order by value"}).text)
```

## Observed

```
{"success":true,"columns":["time","value"],"data":[["2024-01-01T00:00:00Z",0],["1970-06-07T01:17:45.6Z",1]],...}
```

## Expected

The second row should read `2400-01-01T00:00:00Z`. Either:

- let the client declare the unit (a header or payload field such as `"time_unit": "us"`, as
  the Parquet import does with `time_format`), and treat an undeclared integer as the documented
  microseconds; or
- at the least, reject values that a guessed unit cannot represent, rather than store them in
  1970.

## Workaround

- The olap-benchmarks harness writes through `POST /api/v1/import/parquet`. Parquet carries a
  typed `timestamp[unit]` column, which Arc converts exactly (`arrowTimestampToMicros`).
- Its time_series `concurrent` writer generates rows dated in the year 3926. With MessagePack,
  they landed in 1971 while the row counts still matched, which would have silently changed
  query answers.
- No MessagePack encoding can carry these values: in nanoseconds they overflow int64, and in any
  smaller unit they are misread.
