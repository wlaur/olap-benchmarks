from __future__ import annotations

import re
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from importlib.metadata import PackageNotFoundError
from typing import Any, cast

import httpx

from .images import ImageStore, digests_for_repository, resolve_image
from .settings import ContainerPlatform, DatabaseName
from .versions import EnginePin, ImagePin, version_key

DOCKER_HUB_TAGS_URL = "https://hub.docker.com/v2/namespaces/{namespace}/repositories/{name}/tags"
PYPI_URL = "https://pypi.org/pypi/{package}/json"
DOCKER_HUB_PAGE_SIZE = 100
DOCKER_HUB_MAX_PAGES = 5


@dataclass(frozen=True, slots=True)
class ImageEntry:
    engine: DatabaseName
    role: str
    variant: ContainerPlatform | None
    pin: ImagePin
    used_here: bool

    @property
    def label(self) -> str:
        return self.role if self.variant is None else f"{self.role} ({self.variant.removeprefix('linux/')})"


def short_digest(digest: str) -> str:
    return digest[: len("sha256:") + 12]


def image_entries(
    pins: Mapping[DatabaseName, EnginePin],
    platforms: Mapping[DatabaseName, ContainerPlatform | None],
) -> list[ImageEntry]:
    return [
        ImageEntry(name, role, variant, pin, variant is None or variant == platforms[name])
        for name, engine in pins.items()
        for role, variant, pin in engine.all_images()
    ]


def _package_version(engine: EnginePin) -> str:
    try:
        return engine.resolved_version
    except PackageNotFoundError:
        return "not installed"


def inventory_rows(
    pins: Mapping[DatabaseName, EnginePin],
    platforms: Mapping[DatabaseName, ContainerPlatform | None],
    store: ImageStore,
) -> list[tuple[str, ...]]:
    rows: list[tuple[str, ...]] = []
    entries = image_entries(pins, platforms)
    for name, engine in pins.items():
        if engine.package is not None:
            rows.append((name, _package_version(engine), "package", engine.package, "yes", "installed", ""))
            continue
        for entry in (entry for entry in entries if entry.engine == name):
            local = store.local(entry.pin.reference)
            digests = digests_for_repository(local, entry.pin.repository) if local is not None else []
            if local is None:
                digest = ""
            elif digests:
                digest = short_digest(digests[0])
            else:
                digest = "none (local build)"
            rows.append(
                (
                    name,
                    engine.resolved_version,
                    entry.label,
                    entry.pin.reference,
                    "yes" if entry.used_here else "no",
                    "yes" if local is not None else "no",
                    digest,
                )
            )
    return rows


INVENTORY_HEADER = ("engine", "version", "image", "reference", "used here", "local", "registry digest")


def _check_entry(
    entry: ImageEntry, platforms: Mapping[DatabaseName, ContainerPlatform | None], store: ImageStore
) -> str:
    if entry.used_here:
        resolved = resolve_image(store, entry.pin, platforms[entry.engine])
        return f"ok, local {short_digest(resolved.digest)}"
    digest = store.registry_digest(entry.pin.reference)
    if entry.pin.digest is not None and digest != entry.pin.digest:
        raise RuntimeError(f"registry has {digest}, versions.toml pins {entry.pin.digest}")
    return f"ok, registry {short_digest(digest)}"


def check_rows(
    pins: Mapping[DatabaseName, EnginePin],
    platforms: Mapping[DatabaseName, ContainerPlatform | None],
    store: ImageStore,
) -> tuple[list[tuple[str, ...]], bool]:
    rows: list[tuple[str, ...]] = []
    ok = True
    for name, engine in pins.items():
        if engine.package is not None:
            version = _package_version(engine)
            ok &= version != "not installed"
            rows.append((name, "package", engine.package, version))
    for entry in image_entries(pins, platforms):
        try:
            outcome = _check_entry(entry, platforms, store)
        except Exception as exc:
            ok = False
            outcome = f"FAILED: {exc}"
        rows.append((entry.engine, entry.label, entry.pin.reference, outcome))
    return rows, ok


CHECK_HEADER = ("engine", "image", "reference", "result")


def docker_hub_repository(repository: str) -> tuple[str, str]:
    first, _, rest = repository.partition("/")
    if not rest:
        return "library", first
    if "." in first or ":" in first or first == "localhost":
        raise ValueError(f"{repository} is not on Docker Hub")
    return first, rest


def latest_docker_hub_tag(client: httpx.Client, repository: str, pattern: re.Pattern[str]) -> str | None:
    namespace, name = docker_hub_repository(repository)
    url: str | None = DOCKER_HUB_TAGS_URL.format(namespace=namespace, name=name)
    params: dict[str, str | int] | None = {"page_size": DOCKER_HUB_PAGE_SIZE, "ordering": "last_updated"}
    versions: list[str] = []
    for _ in range(DOCKER_HUB_MAX_PAGES):
        if url is None:
            break
        response = client.get(url, params=params)
        response.raise_for_status()
        page = cast(dict[str, Any], response.json())
        for tag in cast(list[dict[str, Any]], page.get("results") or []):
            match = pattern.fullmatch(str(tag.get("name", "")))
            if match is not None:
                versions.append(match["version"])
        url = cast(str | None, page.get("next"))
        params = None
    return max(versions, key=version_key) if versions else None


def latest_pypi_version(client: httpx.Client, package: str) -> str:
    response = client.get(PYPI_URL.format(package=package))
    response.raise_for_status()
    return str(cast(dict[str, Any], response.json())["info"]["version"])


def _upstream_status(pinned: str, latest: str | None) -> str:
    if latest is None:
        return "no matching upstream release"
    pinned_key, latest_key = version_key(pinned), version_key(latest)
    if latest_key > pinned_key:
        return "newer release available"
    if latest_key < pinned_key:
        return "pinned is newer than upstream"
    return "current"


def _outdated_row(client: httpx.Client, name: DatabaseName, engine: EnginePin) -> tuple[str, ...]:
    pinned = _package_version(engine) if engine.package is not None else engine.resolved_version
    source = f"pypi:{engine.package}" if engine.package is not None else f"hub:{engine.upstream_image.repository}"
    try:
        if engine.package is not None:
            latest = latest_pypi_version(client, engine.package)
        else:
            latest = latest_docker_hub_tag(client, engine.upstream_image.repository, engine.upstream_tag_pattern)
    except (httpx.HTTPError, ValueError) as exc:
        return (name, pinned, "", f"lookup failed: {type(exc).__name__}: {exc}", source)
    return (name, pinned, latest or "", _upstream_status(pinned, latest), source)


def outdated_rows(pins: Mapping[DatabaseName, EnginePin], client: httpx.Client) -> list[tuple[str, ...]]:
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(_outdated_row, client, name, engine) for name, engine in pins.items()]
        return [future.result() for future in futures]


OUTDATED_HEADER = ("engine", "pinned", "latest", "status", "source")
