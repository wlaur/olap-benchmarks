import os
import signal
import subprocess
import time
from pathlib import Path

import pytest

from ..dbs.doris import Doris

ENTRYPOINT = Path(__file__).parents[1] / "dbs/doris/container-entrypoint.sh"


@pytest.mark.parametrize("role", ["fe", "be"])
def test_container_signal_runs_graceful_native_stop_and_waits(tmp_path: Path, role: str) -> None:
    binary = tmp_path / role / "bin"
    binary.mkdir(parents=True)
    pidfile = binary / "native.pid"
    ready = tmp_path / "ready"
    finished = tmp_path / "finished"
    stop_arguments = tmp_path / "stop-arguments"
    initializer = tmp_path / "initialize.sh"
    initializer.write_text(
        f"sleep 60 &\nnative_pid=$!\nprintf '%s' \"$native_pid\" > '{pidfile}'\ntouch '{ready}'\nwait \"$native_pid\"\n"
    )
    stop = binary / f"stop_{role}.sh"
    stop.write_text(
        f"#!/usr/bin/env bash\nprintf '%s' \"$1\" > '{stop_arguments}'\n"
        f"kill -TERM \"$(cat '{pidfile}')\"\nsleep 0.1\ntouch '{finished}'\n"
    )
    stop.chmod(0o755)
    with subprocess.Popen(
        ["bash", str(ENTRYPOINT), role, str(initializer)],
        env={**os.environ, "DORIS_HOME": str(tmp_path)},
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    ) as process:
        try:
            deadline = time.monotonic() + 5
            while not ready.exists() and time.monotonic() < deadline:
                time.sleep(0.01)
            assert ready.exists()
            process.send_signal(signal.SIGTERM)
            assert process.wait(timeout=5) == 0
            assert finished.exists()
            assert stop_arguments.read_text() == "--grace"
        finally:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=5)


def test_container_initializer_failure_is_preserved(tmp_path: Path) -> None:
    initializer = tmp_path / "fail.sh"
    initializer.write_text("exit 17\n")
    completed = subprocess.run(["bash", str(ENTRYPOINT), "be", str(initializer)], check=False, timeout=5)
    assert completed.returncode == 17


def test_doris_mounts_signal_wrapper_for_both_roles() -> None:
    database = Doris()
    database._current_suite = "rtabench"
    database._current_suite_scale_factor = 1
    commands = database.start_commands
    frontend, backend = commands[1:]
    for command in (frontend, backend):
        assert "--entrypoint /bin/bash" in command
        assert "/benchmark-entrypoint.sh:ro" in command
    assert frontend.endswith("/benchmark-entrypoint.sh fe /usr/local/bin/init_fe.sh")
    assert backend.endswith("/benchmark-entrypoint.sh be /usr/local/bin/entry_point.sh")
    stop_backend, restart_frontend, start_backend = database.restart_commands
    assert stop_backend == "docker stop --timeout 120 doris-be-benchmark"
    assert restart_frontend == "docker restart --timeout 120 doris-fe-benchmark"
    assert start_backend == backend


@pytest.mark.parametrize("role", ["fe", "be"])
def test_heap_configuration_preserves_non_heap_jvm_options_and_is_idempotent(tmp_path: Path, role: str) -> None:
    config = tmp_path / "fe/conf/fe.conf"
    config.parent.mkdir(parents=True)
    original = (
        "LOG_DIR = ${DORIS_HOME}/log\n"
        'JAVA_OPTS_FOR_JDK_17="-Dfile.encoding=UTF-8 -Xmx8192m -Xms8192m '
        "-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=$LOG_DIR "
        "-Xlog:gc*:$LOG_DIR/fe.gc.log.$CUR_DATE:time,uptime:filecount=10,filesize=50M "
        '--add-opens=java.base/java.lang=ALL-UNNAMED"\n'
        "query_port = 9030\n"
    )
    config.write_text(original)
    initializer = tmp_path / "initialize.sh"
    initializer.write_text("exit 0\n")
    environment = {
        **os.environ,
        "DORIS_HOME": str(tmp_path),
        "OLAP_DORIS_FE_HEAP_INITIAL_MB": "1024",
        "OLAP_DORIS_FE_HEAP_MAXIMUM_MB": "4096",
    }
    for _ in range(2):
        completed = subprocess.run(
            ["bash", str(ENTRYPOINT), role, str(initializer)], env=environment, check=False, timeout=5
        )
        assert completed.returncode == 0
        expected = original.replace("-Xmx8192m", "-Xmx4096m").replace("-Xms8192m", "-Xms1024m")
        assert config.read_text() == (expected if role == "fe" else original)


@pytest.mark.parametrize(
    "options",
    [
        'JAVA_OPTS="-Xmx8192m -Xms8192m"',
        'JAVA_OPTS_FOR_JDK_17="-Xmx8192m"',
        'JAVA_OPTS_FOR_JDK_17="-Xmx8192m -Xmx4096m -Xms8192m"',
        'JAVA_OPTS_FOR_JDK_17="-Xmx8192m -Xms8192m"\nJAVA_OPTS_FOR_JDK_17="-Xmx8192m -Xms8192m"',
    ],
)
def test_unsupported_heap_configuration_stops_before_initializer(tmp_path: Path, options: str) -> None:
    config = tmp_path / "fe/conf/fe.conf"
    config.parent.mkdir(parents=True)
    config.write_text(options + "\n")
    initializer = tmp_path / "initialize.sh"
    initializer.write_text(f"touch '{tmp_path / 'initialized'}'\n")
    completed = subprocess.run(
        ["bash", str(ENTRYPOINT), "fe", str(initializer)],
        env={
            **os.environ,
            "DORIS_HOME": str(tmp_path),
            "OLAP_DORIS_FE_HEAP_INITIAL_MB": "1024",
            "OLAP_DORIS_FE_HEAP_MAXIMUM_MB": "4096",
        },
        check=False,
        capture_output=True,
        timeout=5,
    )
    assert completed.returncode == 2
    assert not (tmp_path / "initialized").exists()
    assert config.read_text() == options + "\n"
