from __future__ import annotations

import os
import platform
import shutil
import subprocess
import sys
from collections.abc import Mapping
from functools import cache
from hashlib import sha256
from importlib.metadata import PackageNotFoundError, distribution
from pathlib import Path
from typing import Any, Literal, cast

import psutil

from .settings import ContainerPlatform

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


def _git_provenance(path: Path) -> dict[str, object] | None:
    commit = _run_optional_command(["git", "-C", str(path), "rev-parse", "HEAD"])
    if commit is None:
        return None
    status = _run_optional_command(["git", "-C", str(path), "status", "--porcelain"])
    return {"commit": commit, "dirty": bool(status)}


def build_package_provenance(package_names: tuple[str, ...]) -> dict[str, dict[str, object]]:
    packages: dict[str, dict[str, object]] = {}
    for package_name in package_names:
        try:
            installed = distribution(package_name)
        except PackageNotFoundError:
            packages[package_name] = {"installed": False}
            continue

        package: dict[str, object] = {"installed": True, "version": installed.version}
        direct_url_text = installed.read_text("direct_url.json")
        if direct_url_text is not None:
            import json

            direct_url = cast(dict[str, Any], json.loads(direct_url_text))
            vcs_info_value = direct_url.get("vcs_info")
            if isinstance(vcs_info_value, dict):
                vcs_info = cast(dict[str, Any], vcs_info_value)
                if isinstance(vcs_info.get("commit_id"), str):
                    package["commit"] = vcs_info["commit_id"]
            url = direct_url.get("url")
            dir_info_value = direct_url.get("dir_info")
            dir_info = cast(dict[str, Any], dir_info_value) if isinstance(dir_info_value, dict) else {}
            if dir_info.get("editable") is True and isinstance(url, str):
                from urllib.parse import unquote, urlparse

                parsed = urlparse(url)
                if parsed.scheme == "file":
                    provenance = _git_provenance(Path(unquote(parsed.path)))
                    if provenance is not None:
                        package.update(provenance)
                        package["editable"] = True
        packages[package_name] = package
    return packages


@cache
def _fingerprint_input_directory(
    path: Path,
    signature: tuple[tuple[str, int, int], ...],
) -> dict[str, object]:
    files: list[dict[str, object]] = []
    manifest_hash = sha256()
    for relative_path, size, modified_ns in signature:
        file_path = path / relative_path
        file_hash = sha256()
        with file_path.open("rb") as source:
            while chunk := source.read(8 * 1024 * 1024):
                file_hash.update(chunk)
        current_stat = file_path.stat()
        if (current_stat.st_size, current_stat.st_mtime_ns) != (size, modified_ns):
            raise RuntimeError(f"Benchmark input changed while it was being fingerprinted: {file_path}")
        digest = file_hash.hexdigest()
        manifest_hash.update(f"{relative_path}\0{size}\0{digest}\n".encode())
        files.append({"path": relative_path, "bytes": size, "sha256": digest})

    return {
        "algorithm": "sha256",
        "manifest_sha256": manifest_hash.hexdigest(),
        "files": files,
    }


def fingerprint_input_directory(path: Path) -> dict[str, object]:
    entries: list[tuple[str, int, int]] = []
    for file_path in sorted(candidate for candidate in path.rglob("*") if candidate.is_file()):
        stat = file_path.stat()
        entries.append((file_path.relative_to(path).as_posix(), stat.st_size, stat.st_mtime_ns))
    return _fingerprint_input_directory(path, tuple(entries))


def build_system_metadata() -> dict[str, Any]:
    docker_version = _run_optional_command(["docker", "version", "--format", "{{.Server.Version}}"])
    docker_platform = _run_optional_command(["docker", "version", "--format", "{{.Server.Os}}/{{.Server.Arch}}"])
    docker_context = _run_optional_command(["docker", "context", "show"])

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
        "methodology": {
            "timed_unit": "perf_counter around fetch() into Polars",
            "iteration_roles": "iteration 1 is first_run; later iterations are warm unless a suite records otherwise",
            "cache_policy": "database restart only where suite code calls restart_event; OS caches are not cleared",
        },
    }


def build_run_metadata(
    *,
    execution_mode: ExecutionMode,
    container_image: str | None = None,
    container_images: Mapping[str, str] | None = None,
    container_platform: ContainerPlatform | None = None,
    container_platform_emulated: bool = False,
    start_command: str | None,
    package_names: tuple[str, ...] = ("olap-benchmarks",),
    input_directory: Path | None = None,
    options: Mapping[str, object] | None = None,
    sampling_interval_seconds: Mapping[str, float] | None = None,
) -> dict[str, Any]:
    docker_platform = _run_optional_command(["docker", "version", "--format", "{{.Server.Os}}/{{.Server.Arch}}"])
    image_map = dict(container_images or {})
    if container_image is not None and not image_map:
        image_map = {"default": container_image}

    primary_container_image = container_image
    if primary_container_image is None and len(image_map) == 1:
        primary_container_image = next(iter(image_map.values()))

    image_digests = {
        name: _run_optional_command(["docker", "image", "inspect", image, "--format", "{{index .RepoDigests 0}}"])
        for name, image in image_map.items()
    }
    image_digest = (
        _run_optional_command(
            ["docker", "image", "inspect", primary_container_image, "--format", "{{index .RepoDigests 0}}"]
        )
        if primary_container_image is not None
        else None
    )
    image_ids = {
        name: _run_optional_command(["docker", "image", "inspect", image, "--format", "{{.Id}}"])
        for name, image in image_map.items()
    }
    image_id = (
        _run_optional_command(["docker", "image", "inspect", primary_container_image, "--format", "{{.Id}}"])
        if primary_container_image is not None
        else None
    )

    return {
        "execution": {
            "mode": execution_mode,
            "container_image": primary_container_image,
            "container_image_digest": image_digest,
            "container_image_id": image_id,
            "container_images": image_map or None,
            "container_image_digests": image_digests or None,
            "container_image_ids": image_ids or None,
            "container_platform": container_platform,
            "container_engine_platform": docker_platform if execution_mode == "container" else None,
            "container_platform_emulated": container_platform_emulated if execution_mode == "container" else False,
            "start_command": start_command,
        },
        "metrics": {
            "version": 6,
            "sampling": (
                "each metric family is sampled on its own schedule and stored as its own run_metric row, "
                "leaving the columns it does not measure null. Sample spacing is neither constant nor equal "
                "across columns, so aggregate with max() or weight each reading by the gap to the next "
                "non-null one; never assume a fixed period per row"
            ),
            "sampling_interval_seconds": dict(sampling_interval_seconds or {}),
            "cpu_percent": (
                "combined: benchmark client process plus all database containers; not decomposable into "
                "a server and a client share. Each reading is the average over the interval since the "
                "previous reading, so it covers the whole run with no gaps"
            ),
            "server_mem_mb": (
                "server side: all database containers, excluding the benchmark client. Always 0 when "
                "execution.mode is in_process, because there is no server process"
            ),
            "client_mem_mb": "client side: benchmark client process peak RSS, on the client_rss interval",
            "client_uss_mb": "client side: benchmark client process peak USS, on the client_uss interval",
            "disk_mb": (
                "server side: the database's own storage directories (Database.metric_directories). "
                "Client staging files and the benchmark client's own disk use are not measured. Sampled on "
                "the slow disk lane, whose interval stretches beyond its nominal value when a du walk is "
                "expensive, so disk readings are sparser and less regular than the other columns"
            ),
            "comparable_memory": (
                "server_mem_mb for container engines and client_mem_mb for in_process engines; "
                "server and client peaks are never summed because they do not coincide in time"
            ),
        },
        "packages": build_package_provenance(package_names),
        "input": fingerprint_input_directory(input_directory) if input_directory is not None else None,
        "options": dict(options or {}),
    }
