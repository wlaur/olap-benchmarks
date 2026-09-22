# Native JSON cast exhausts memory on a small tool-result document

Observed on 2026-09-22 with MonetDB 11.55.7 (Dec2025-SP3), using
`europe-north1-docker.pkg.dev/cloud-services-indmeas/docker/monetdb:linux-11.55.7`.
A single 289,083-byte JSON document exhausted a disposable database container's
4 GiB memory limit when cast to native `JSON`. Casting the identical bound value
to `TEXT` completed in 0.012 seconds. No table insert was necessary.

## Captured result

The original document represented one assistant message with 60 tool results,
each containing 100 nested numeric reading objects. The enclosing test fixture
contained eight display messages and 16 model messages, totalling 2,388,335 bytes.
The isolated cast used only one serialized message, not the whole fixture.

| Operation | Result |
| --- | --- |
| `SELECT LENGTH(CAST(:value AS TEXT))` | 289,083; 0.012 seconds |
| `SELECT LENGTH(CAST(:value AS JSON))` | Database process killed by the container memory limit |
| Container `memory.peak` | 4,294,967,296 bytes |
| Container `memory.events` | `oom_kill 1` |

These two casts were executed through a SQLAlchemy bound parameter. They establish
that the native JSON conversion can trigger the failure without a batch insert
or accumulated chat rows. They do not establish whether nesting, token count,
or another property of the document causes the allocation growth.

An earlier run against an unlimited local database was killed by the host OOM
killer at 09:32:08 EEST with approximately 59.3 GiB anonymous RSS
(`62202832 kB`). The preceding laptop freeze at approximately 09:27 has no
persisted OOM-kill record and cannot be attributed conclusively from that log.

## mclient reproduction

[reproducers/monetdb-json-cast.sql](reproducers/monetdb-json-cast.sql) contains a
single `SELECT LENGTH(CAST('<document>' AS JSON))`, with the synthetic original
289,083-byte document reconstructed from the fixture. It requires no Python,
application code, tables, or inserts. The document's SHA-256 is
`589bd8abda73255b7bc4132b30af28637f46bfb2ede2151b1665b9611e9833b4`.

**The failure was verified through a SQLAlchemy bound parameter, not by executing
this mclient SQL file.** Further exhaustion probes were stopped after the captured
failure. The SQL literal contains the same document; whether literal evaluation
changes the failure path remains unverified.

Use a disposable database with a hard memory limit and no swap. Do not use a
shared development or production database. This setup publishes no host port
and mounts no existing database volume:

```sh
docker run --detach --name monetdb-json-repro \
  --memory=4g --memory-swap=4g --pids-limit=128 \
  -e MONETDB_NAME=jsonrepro \
  europe-north1-docker.pkg.dev/cloud-services-indmeas/docker/monetdb:linux-11.55.7

docker logs monetdb-json-repro
docker exec monetdb-json-repro sh -c \
  'cat /sys/fs/cgroup/memory.max /sys/fs/cgroup/memory.swap.max'

docker cp reproducers/monetdb-json-cast.sql monetdb-json-repro:/tmp/json-cast.sql
docker exec monetdb-json-repro sh -c \
  'sed "s/AS JSON/AS TEXT/" /tmp/json-cast.sql > /tmp/text-cast.sql'
```

Wait for the database farm to start, confirm the limits print `4294967296` and
`0`, and enter the disposable image's default password, `monetdb`, at the prompt.
Run the control first:

```sh
docker exec -it monetdb-json-repro \
  mclient -d jsonrepro -u monetdb /tmp/text-cast.sql
```

Expected control result: `289083`. Then run the native JSON cast:

```sh
docker exec -it monetdb-json-repro \
  mclient -d jsonrepro -u monetdb /tmp/json-cast.sql
```

Inspect the cgroup counters after the query, even if the client disconnects.
The farm container can survive an OOM kill of its database subprocess, so
container uptime alone is insufficient evidence:

```sh
docker exec monetdb-json-repro sh -c \
  'cat /sys/fs/cgroup/memory.peak /sys/fs/cgroup/memory.events'
docker logs --tail 100 monetdb-json-repro
docker rm --force monetdb-json-repro
```

## Smaller token-density candidate

This smaller control was verified through `mclient` 11.55.7 and returned `100003`:

```sql
SELECT LENGTH(CAST('[' || REPEAT('0,', 50000) || '0]' AS TEXT));
```

Its disposable container peaked at 48,975,872 bytes, with all OOM counters zero,
and was removed. Changing `TEXT` to `JSON` would test token density independently
of nesting, but that variant has not been executed or confirmed to fail.

## Expected behavior and remaining work

A roughly 289 kB valid JSON value should not require more than 4 GiB merely to
cast and measure its length. Conversion should complete with bounded memory or
return a controlled error without killing the database process.

Using `TEXT` avoids native JSON parsing in the verified control, but changing
application storage types needs its own schema and query review. Reducing the
benchmark's tool payload would reduce exposure without explaining or fixing
the native JSON failure. No storage migration or benchmark reduction was made
as part of this report. An executed mclient reproduction, further minimization,
and allocation profile remain necessary before assigning a parser root cause.
