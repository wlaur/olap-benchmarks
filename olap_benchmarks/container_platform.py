from __future__ import annotations

import platform
import subprocess
from functools import lru_cache

from .settings import ContainerPlatform


def _normalize_container_platform(value: str) -> ContainerPlatform:
    normalized = value.strip().lower()
    aliases: dict[str, ContainerPlatform] = {
        "amd64": "linux/amd64",
        "x86_64": "linux/amd64",
        "linux/amd64": "linux/amd64",
        "linux/x86_64": "linux/amd64",
        "arm64": "linux/arm64",
        "aarch64": "linux/arm64",
        "linux/arm64": "linux/arm64",
        "linux/arm64/v8": "linux/arm64",
        "linux/aarch64": "linux/arm64",
    }
    try:
        return aliases[normalized]
    except KeyError as exc:
        raise RuntimeError(f"Unsupported container architecture: {value!r}") from exc


@lru_cache
def get_container_engine_platform() -> ContainerPlatform:
    try:
        result = subprocess.run(
            ["docker", "version", "--format", "{{.Server.Os}}/{{.Server.Arch}}"],
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired):
        result = None

    if result is not None and result.returncode == 0 and result.stdout.strip():
        return _normalize_container_platform(result.stdout)

    return _normalize_container_platform(platform.machine())
