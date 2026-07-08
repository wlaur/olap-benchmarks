from __future__ import annotations

import os
import platform
import shutil
import subprocess
import sys
from typing import Any, Literal

import psutil

ExecutionMode = Literal["container", "in_process"]
IterationRole = Literal["first_run", "warm", "steady_state"]
StepResultStatus = Literal["ok", "timeout", "unsupported", "wrong_result", "error", "skipped"]


def classify_iteration_role(iteration: int | None) -> IterationRole | None:
    if iteration is None:
        return None
    if iteration == 1:
        return "first_run"
    return "warm"


def classify_step_result_status(
    *,
    step_type: str,
    status: str,
    error_type: str | None = None,
) -> StepResultStatus | None:
    if step_type not in ("query", "mutation"):
        return None
    if status == "completed":
        return "ok"
    if status != "failed":
        return None
    if error_type in {"TimeoutError", "TimeoutExpired"}:
        return "timeout"
    return "error"


def _run_optional_command(args: list[str]) -> str | None:
    if shutil.which(args[0]) is None:
        return None

    try:
        result = subprocess.run(args, check=False, capture_output=True, text=True, timeout=5)
    except (OSError, subprocess.TimeoutExpired):
        return None

    if result.returncode != 0:
        return None

    value = result.stdout.strip()
    return value or None


def build_run_metadata(
    *,
    execution_mode: ExecutionMode,
    container_image: str | None,
    start_command: str | None,
) -> dict[str, Any]:
    docker_version = _run_optional_command(["docker", "version", "--format", "{{.Server.Version}}"])
    docker_platform = _run_optional_command(["docker", "version", "--format", "{{.Server.Os}}/{{.Server.Arch}}"])
    docker_context = _run_optional_command(["docker", "context", "show"])
    image_digest = (
        _run_optional_command(["docker", "image", "inspect", container_image, "--format", "{{index .RepoDigests 0}}"])
        if container_image is not None
        else None
    )

    return {
        "host": {
            "os": platform.system(),
            "os_release": platform.release(),
            "machine": platform.machine(),
            "processor": platform.processor(),
            "cpu_count_logical": os.cpu_count(),
            "memory_total_mb": int(psutil.virtual_memory().total / (1024 * 1024)),
        },
        "python": {
            "version": sys.version.split()[0],
        },
        "docker": {
            "version": docker_version,
            "context": docker_context,
            "server_platform": docker_platform,
        },
        "execution": {
            "mode": execution_mode,
            "container_image": container_image,
            "container_image_digest": image_digest,
            "container_platform": docker_platform if execution_mode == "container" else None,
            "start_command": start_command,
        },
        "methodology": {
            "timed_unit": "perf_counter around fetch() into Polars",
            "iteration_roles": "iteration 1 is first_run; later iterations are warm unless a suite records otherwise",
            "cache_policy": "database restart only where suite code calls restart_event; OS caches are not cleared",
        },
    }
