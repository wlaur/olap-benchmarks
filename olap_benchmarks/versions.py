from __future__ import annotations

import re
import tomllib
from functools import cache
from importlib.metadata import version as package_version
from pathlib import Path
from typing import Self, cast

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, field_validator, model_validator

from .settings import ContainerPlatform, DatabaseName

VERSIONS_PATH = Path(__file__).with_name("versions.toml")

_DIGEST_PATTERN = r"^sha256:[0-9a-f]{64}$"
_NUMERIC_VERSION_PATTERN = r"(?P<version>\d+(?:\.\d+)*)"
_MONTHS = ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")
_RELEASE_LABEL = re.compile(r"(?P<month>[A-Z][a-z]{2})(?P<year>\d{4})(?:-SP(?P<service_pack>\d+))?")


def split_reference(reference: str) -> tuple[str, str]:
    name = reference.split("@", 1)[0]
    slash = name.rfind("/")
    colon = name.rfind(":")
    if colon > slash:
        return name[:colon], name[colon + 1 :]
    return name, "latest"


def normalize_repository(repository: str) -> str:
    for prefix in ("docker.io/", "index.docker.io/", "registry-1.docker.io/"):
        repository = repository.removeprefix(prefix)
    return repository.removeprefix("library/")


def version_key(version: str) -> tuple[int, ...]:
    label = _RELEASE_LABEL.fullmatch(version)
    if label is not None and label["month"] in _MONTHS:
        return (int(label["year"]), _MONTHS.index(label["month"]) + 1, int(label["service_pack"] or 0))
    return tuple(int(part) for part in re.findall(r"\d+", version))


class ImagePin(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    reference: str
    digest: str | None = Field(default=None, pattern=_DIGEST_PATTERN)

    @property
    def repository(self) -> str:
        return normalize_repository(split_reference(self.reference)[0])

    @property
    def tag(self) -> str:
        return split_reference(self.reference)[1]


def _image_pins(value: object) -> object:
    if not isinstance(value, dict):
        return value
    pins = cast(dict[str, object], value)
    return {role: {"reference": pin} if isinstance(pin, str) else pin for role, pin in pins.items()}


class EnginePin(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    version: str | None = None
    package: str | None = None
    expected_runtime_version: str | None = None
    images: dict[str, ImagePin] = Field(default_factory=dict[str, ImagePin])
    arm64_images: dict[str, ImagePin] = Field(default_factory=dict[str, ImagePin])
    upstream_tag: str | None = None

    _normalize_images = field_validator("images", "arm64_images", mode="before")(_image_pins)

    @model_validator(mode="after")
    def _check(self) -> Self:
        if (self.version is None) == (self.package is None):
            raise ValueError("set exactly one of `version` (container engine) or `package` (in-process engine)")
        if self.package is not None:
            if self.images or self.arm64_images or self.expected_runtime_version or self.upstream_tag:
                raise ValueError("an in-process `package` engine has no images or runtime version")
            return self
        if not self.images:
            raise ValueError("a container engine needs at least one image")
        unknown_roles = set(self.arm64_images) - set(self.images)
        if unknown_roles:
            raise ValueError(f"arm64_images roles without a default image: {sorted(unknown_roles)}")
        accepted = {self.version, self.expected_runtime_version} - {None}
        for pin in (*self.images.values(), *self.arm64_images.values()):
            if not any(version in pin.tag for version in accepted if version is not None):
                raise ValueError(f"image tag of {pin.reference!r} does not contain the pinned version")
        if self.upstream_tag is not None:
            if "version" not in re.compile(self.upstream_tag).groupindex:
                raise ValueError("upstream_tag needs a named `version` group")
        elif self.version is not None and self.version not in self.upstream_image.tag:
            raise ValueError("set upstream_tag: the pinned version is not part of the first image's tag")
        return self

    @property
    def resolved_version(self) -> str:
        if self.version is not None:
            return self.version
        assert self.package is not None
        return package_version(self.package)

    @property
    def is_container(self) -> bool:
        return bool(self.images)

    def images_for(self, platform: ContainerPlatform) -> dict[str, ImagePin]:
        if platform == "linux/arm64":
            return {role: self.arm64_images.get(role, pin) for role, pin in self.images.items()}
        return dict(self.images)

    def all_images(self) -> list[tuple[str, ContainerPlatform | None, ImagePin]]:
        listed: list[tuple[str, ContainerPlatform | None, ImagePin]] = []
        for role, pin in self.images.items():
            arm64_pin = self.arm64_images.get(role)
            if arm64_pin is None:
                listed.append((role, None, pin))
            else:
                listed.extend([(role, "linux/amd64", pin), (role, "linux/arm64", arm64_pin)])
        return listed

    @property
    def upstream_image(self) -> ImagePin:
        return next(iter(self.images.values()))

    @property
    def upstream_tag_pattern(self) -> re.Pattern[str]:
        if self.upstream_tag is not None:
            return re.compile(self.upstream_tag)
        assert self.version is not None
        prefix, _, suffix = self.upstream_image.tag.partition(self.version)
        return re.compile(f"^{re.escape(prefix)}{_NUMERIC_VERSION_PATTERN}{re.escape(suffix)}$")


_MANIFEST_ADAPTER = TypeAdapter(dict[DatabaseName, EnginePin])


def parse_versions(text: str) -> dict[DatabaseName, EnginePin]:
    return _MANIFEST_ADAPTER.validate_python(tomllib.loads(text))


@cache
def load_versions() -> dict[DatabaseName, EnginePin]:
    return parse_versions(VERSIONS_PATH.read_text())


def engine_pin(name: DatabaseName) -> EnginePin:
    try:
        return load_versions()[name]
    except KeyError as exc:
        raise KeyError(f"{name} has no entry in {VERSIONS_PATH}") from exc
