"""Input fingerprints and page-cache control.

Hashing an input reads every byte of it, which leaves the file in the operating-system page cache.
Done right before populate, that favours engines that read the input files directly. The harness
therefore hashes inputs before any container starts, and caches each file's hash keyed by
(path, size, mtime_ns) so a repeat run only stats the files. On Linux `--drop-caches` then drops
the page cache before the container starts, so populate reads the inputs cold.
"""

from __future__ import annotations

import json
import logging
import os
import platform
from hashlib import sha256
from pathlib import Path
from typing import TypedDict, cast

from .settings import SETTINGS

_LOGGER = logging.getLogger(__name__)

INPUT_HASH_CACHE_FILE_NAME = "input-sha256-cache.json"
DROP_CACHES_PATH = Path("/proc/sys/vm/drop_caches")


class CachedFileHash(TypedDict):
    bytes: int
    mtime_ns: int
    sha256: str


def input_hash_cache_path() -> Path:
    return SETTINGS.temporary_directory / INPUT_HASH_CACHE_FILE_NAME


def _read_cache(path: Path) -> dict[str, CachedFileHash]:
    try:
        raw: object = json.loads(path.read_text())
    except FileNotFoundError:
        return {}
    except (OSError, ValueError) as exc:
        _LOGGER.warning(f"Ignoring unreadable input hash cache {path}: {exc}")
        return {}

    if not isinstance(raw, dict):
        return {}

    cache: dict[str, CachedFileHash] = {}
    for key, value in cast(dict[str, object], raw).items():
        if not isinstance(value, dict):
            continue
        entry = cast(dict[str, object], value)
        size, mtime_ns, digest = entry.get("bytes"), entry.get("mtime_ns"), entry.get("sha256")
        if isinstance(size, int) and isinstance(mtime_ns, int) and isinstance(digest, str):
            cache[key] = {"bytes": size, "mtime_ns": mtime_ns, "sha256": digest}
    return cache


def _write_cache(path: Path, cache: dict[str, CachedFileHash]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(f"{path.name}.{os.getpid()}.part")
    partial.write_text(json.dumps(cache, indent=1, sort_keys=True))
    partial.replace(path)


def _hash_file(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as source:
        while chunk := source.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def fingerprint_input_directory(path: Path, cache_path: Path | None = None) -> dict[str, object]:
    resolved_cache_path = cache_path or input_hash_cache_path()
    cache = _read_cache(resolved_cache_path)
    cache_changed = False

    files: list[dict[str, object]] = []
    manifest_hash = sha256()
    for file_path in sorted(candidate for candidate in path.rglob("*") if candidate.is_file()):
        stat = file_path.stat()
        key = file_path.resolve().as_posix()
        cached = cache.get(key)
        if cached is not None and (cached["bytes"], cached["mtime_ns"]) == (stat.st_size, stat.st_mtime_ns):
            digest = cached["sha256"]
        else:
            digest = _hash_file(file_path)
            current = file_path.stat()
            if (current.st_size, current.st_mtime_ns) != (stat.st_size, stat.st_mtime_ns):
                raise RuntimeError(f"Benchmark input changed while it was being fingerprinted: {file_path}")
            cache[key] = {"bytes": stat.st_size, "mtime_ns": stat.st_mtime_ns, "sha256": digest}
            cache_changed = True

        relative_path = file_path.relative_to(path).as_posix()
        manifest_hash.update(f"{relative_path}\0{stat.st_size}\0{digest}\n".encode())
        files.append({"path": relative_path, "bytes": stat.st_size, "sha256": digest})

    if cache_changed:
        _write_cache(resolved_cache_path, cache)

    return {
        "algorithm": "sha256",
        "manifest_sha256": manifest_hash.hexdigest(),
        "files": files,
    }


def drop_page_cache() -> bool:
    system = platform.system()
    if system != "Linux":
        _LOGGER.info(f"Dropping the page cache is only available on Linux, not {system}; leaving it as is")
        return False

    os.sync()
    try:
        DROP_CACHES_PATH.write_text("3\n")
    except OSError as exc:
        _LOGGER.warning(f"Could not drop the page cache through {DROP_CACHES_PATH} ({exc}); it needs root")
        return False

    _LOGGER.info("Dropped the page cache")
    return True
