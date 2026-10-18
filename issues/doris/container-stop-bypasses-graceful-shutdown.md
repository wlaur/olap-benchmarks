# Container stop bypasses graceful engine shutdown

Tested with `apache/doris:fe-4.1.5` and `apache/doris:be-4.1.5`, native ARM64
under OrbStack on macOS, during the October 2026 expanded benchmark campaign.
This investigation concerns the images' process supervision, not a query defect.

## Reproduction

Start the original role images on a disposable Docker network:

```sh
docker network create --subnet 172.29.41.0/24 doris-stop-repro
docker run -d --name doris-stop-repro-fe --network doris-stop-repro --ip 172.29.41.2 \
  -e FE_SERVERS=fe1:172.29.41.2:9010 -e FE_ID=1 apache/doris:fe-4.1.5
docker run -d --name doris-stop-repro-be --network doris-stop-repro --ip 172.29.41.3 \
  -e FE_SERVERS=fe1:172.29.41.2:9010 -e BE_ADDR=172.29.41.3:9050 apache/doris:be-4.1.5
docker exec doris-stop-repro-fe mysql -h127.0.0.1 -P9030 -uroot -e 'SHOW BACKENDS'
```

Repeat the final command until the backend is alive, then inspect the shutdown:

```sh
docker stop --timeout 120 doris-stop-repro-be
docker inspect doris-stop-repro-be --format '{{.State.ExitCode}}'
docker stop --timeout 120 doris-stop-repro-fe
docker rm doris-stop-repro-be doris-stop-repro-fe
docker network rm doris-stop-repro
```

Expected: SIGTERM reaches the native service, the service finishes shutdown, and
the container exits without Docker escalating to SIGKILL.

Observed in the full RTABench load: backend SIGTERM at 18:18:43 UTC was followed
by SIGKILL at 18:20:44 and exit 137. The same sequence repeated before select and
at final cleanup. The backend continued running throughout the grace period.
Frontend exit 143 was quicker, but did not establish native graceful shutdown.

The backend process tree was:

```text
bash entry_point.sh (PID 1)
  bash entry_point.sh
    bash init_be.sh
      bash start_be.sh --console
        doris_be
```

The outer `entry_point.sh` starts `init_be.sh` in a background group and waits,
without forwarding SIGTERM. The inner initializer's trap therefore never runs.
Separately, both bundled `stop_fe.sh` and `stop_be.sh` default to signal 9;
`--grace` selects signal 15. The frontend initializer calls its stop script
without `--grace`.

## Harness correction and native verification

The harness mounts a small role wrapper that traps TERM/INT, invokes the native
`stop_<role>.sh --grace`, waits for the original initializer, and then exits.
It preserves initializer failures. Restart stops the backend, restarts the
frontend, then recreates the backend against the same persistent storage.

Stopping the backend first also avoids leaving live backend Thrift connections
while the frontend drains its workers. A frontend-first fixture logged
`graceful shutdown finished`, then waited in
`OssThriftServerStarter.stop -> ThriftServer.join`; its total restart took
82–100 seconds. This stack alone does not establish a permanent deadlock.

Two native backend-first restart cycles took approximately 42 and 44 seconds.
Each retained all 33 fixture events. All six
container exits across those cycles and final shutdown were code 0, with no
Docker SIGKILL events. These are lifecycle checks, not benchmark speedups.

Earlier benchmark attempts remain recorded. The campaign replays the two
affected Doris combinations, JSONBench and RTABench, after committing the fix.

Related upstream interfaces: [backend initializer](https://github.com/apache/doris/blob/master/docker/runtime/be/resource/init_be.sh),
[backend stop script](https://github.com/apache/doris/blob/master/bin/stop_be.sh),
and [Docker stop signal semantics](https://docs.docker.com/reference/cli/docker/container/stop/).
