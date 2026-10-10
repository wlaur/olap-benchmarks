from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol, cast

from .settings import ContainerPlatform
from .versions import ImagePin, normalize_repository, split_reference

_LOGGER = logging.getLogger(__name__)


class ImageDigestError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class LocalImage:
    id: str
    repo_digests: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ResolvedImage:
    reference: str
    repository: str
    digest: str
    image_id: str

    @property
    def repo_digest(self) -> str:
        return f"{self.repository}@{self.digest}"


class ImageStore(Protocol):
    def local(self, reference: str) -> LocalImage | None: ...

    def pull(self, reference: str, platform: ContainerPlatform | None) -> None: ...

    def registry_digest(self, reference: str) -> str: ...


class DockerImageStore:
    def __init__(self) -> None:
        import docker

        self._client = docker.from_env()

    def local(self, reference: str) -> LocalImage | None:
        from docker.errors import ImageNotFound

        try:
            image = self._client.images.get(reference)
        except ImageNotFound:
            return None
        attrs = image.attrs
        return LocalImage(id=str(image.id), repo_digests=tuple(cast(list[str], attrs.get("RepoDigests") or [])))

    def pull(self, reference: str, platform: ContainerPlatform | None) -> None:
        repository, tag = split_reference(reference)
        _LOGGER.info(f"Pulling {reference}" + (f" for {platform}" if platform else ""))
        self._client.images.pull(repository, tag=tag, platform=platform)

    def registry_digest(self, reference: str) -> str:
        registry_data = cast(Any, self._client.images).get_registry_data(reference)
        return str(registry_data.id)


def digests_for_repository(image: LocalImage, repository: str) -> list[str]:
    wanted = normalize_repository(repository)
    digests: list[str] = []
    for repo_digest in image.repo_digests:
        name, _, digest = repo_digest.partition("@")
        if normalize_repository(name) == wanted and digest:
            digests.append(digest)
    return digests


def _select_digest(image: LocalImage | None, pin: ImagePin) -> str | None:
    if image is None:
        return None
    digests = digests_for_repository(image, pin.repository)
    if pin.digest is not None:
        return pin.digest if pin.digest in digests else None
    return digests[0] if digests else None


def resolve_image(store: ImageStore, pin: ImagePin, platform: ContainerPlatform | None) -> ResolvedImage:
    image = store.local(pin.reference)
    digest = _select_digest(image, pin)
    if digest is None:
        try:
            store.pull(pin.reference, platform)
        except Exception as exc:
            raise ImageDigestError(f"Cannot pull {pin.reference} to determine its registry digest: {exc}") from exc
        image = store.local(pin.reference)
        if image is None:
            raise ImageDigestError(f"{pin.reference} is not present locally after pulling it")
        digest = _select_digest(image, pin)
        if digest is None:
            found = digests_for_repository(image, pin.repository)
            if pin.digest is not None and found:
                raise ImageDigestError(
                    f"{pin.reference} resolves to {', '.join(found)}, but versions.toml pins {pin.digest}"
                )
            raise ImageDigestError(f"{pin.reference} has no registry digest for {pin.repository} after pulling it")
    assert image is not None
    return ResolvedImage(reference=pin.reference, repository=pin.repository, digest=digest, image_id=image.id)


def resolve_images(
    pins: Mapping[str, ImagePin],
    platform: ContainerPlatform | None,
    store: ImageStore | None = None,
) -> dict[str, ResolvedImage]:
    if not pins:
        return {}
    image_store = store or DockerImageStore()
    return {role: resolve_image(image_store, pin, platform) for role, pin in pins.items()}
