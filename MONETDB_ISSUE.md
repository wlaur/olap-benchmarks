# Server aborts when a client disconnects during binary result export

## Environment

- MonetDB 11.55.7 (Dec2025-SP3)
- Official `monetdb/monetdb:Dec2025-SP3` container
- Fresh database
- Reproduced with 4 GiB available to the container and no memory pressure

## Reproduction

Start MonetDB in one terminal. The example uses a disposable local database and deliberately omits authentication details; substitute the credentials from your local test setup.

```sh
docker run --rm --name monetdb-export-repro \
  -p 50000:50000 \
  --memory 4g \
  monetdb/monetdb:Dec2025-SP3
```

In another terminal, create a result large enough that it is still being transferred when the client is stopped:

```sh
mclient -h localhost -p 50000 -d demo -u monetdb \
  -s "SELECT value, value * 2, value * 3 FROM generate_series(1, 20000001) AS g(value)" \
  >/dev/null &

client_pid=$!
sleep 1
kill -KILL "$client_pid"
wait "$client_pid" || true
```

Repeat the query and interruption a few times if necessary. The timing depends on the machine:

```sh
for attempt in $(seq 1 20); do
  mclient -h localhost -p 50000 -d demo -u monetdb \
    -s "SELECT value, value * 2, value * 3 FROM generate_series(1, 20000001) AS g(value)" \
    >/dev/null &
  client_pid=$!
  sleep 0.2
  kill -KILL "$client_pid" 2>/dev/null || true
  wait "$client_pid" 2>/dev/null || true
done
```

The important condition is that the connection is closed while MonetDB is writing a binary result chunk. A client using binary result transfer negotiates that mode on the connection; no table or pre-existing data is required.

## Observed result

The database process aborts. Its log contains:

```text
MALException:sql.export_bin_column:42000!no error
mvc_export_bin_chunk: ERROR: MALException:sql.export_bin_column:42000!no error
free(): invalid pointer
database 'demo' has crashed with signal SIGABRT (dumped core)
```

Other connections to the same database are dropped while it restarts.

## Expected result

Closing a client connection during result transfer should cancel that transfer and clean up the result. It should not abort the database process or affect other clients.

## Notes

This does not appear to require memory pressure. In the fresh-container reproduction, the process was using roughly 120--140 MiB, the container limit was 4 GiB, and the cgroup OOM counters remained zero.

The abort follows a write failure in the binary export path. `mvc_export_bin_chunk()` receives an error from `dump_binary_column()` and frees the returned message with `GDKfree()`. The message is created through the query context's error allocator, so freeing it directly appears to be the invalid free. The unhelpful `42000!no error` text also suggests that the outer byte-counting stream is not retaining the error from the wrapped stream when the write fails.

