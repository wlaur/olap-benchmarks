from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Literal, cast

from pydantic import BaseModel, ConfigDict

from ...inputs import fingerprint_input_directory
from ...run_metadata import sha256_text

MANIFEST_NAME = "representation-manifest.json"


def sql_literal(value: str) -> str:
    if "\0" in value:
        raise ValueError("NUL in SQL literal")
    return "'" + value.replace("'", "''") + "'"


class SourceIdentity(BaseModel):
    model_config = ConfigDict(extra="forbid")
    path: str
    bytes: int
    mtime_ns: int

    @classmethod
    def inspect(cls, path: Path) -> SourceIdentity:
        resolved = path.resolve(strict=True)
        if not resolved.is_file():
            raise ValueError(f"Expected regular source file: {path}")
        stat = resolved.stat()
        return cls(path=resolved.as_posix(), bytes=stat.st_size, mtime_ns=stat.st_mtime_ns)


class RepresentationManifest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    version: Literal[1] = 1
    suite: str
    state: Literal["building", "ready"] = "building"
    source_directory: str
    source_fingerprint: dict[str, object]
    sources: list[SourceIdentity]
    layout: dict[str, object]

    @classmethod
    def build(cls, suite: str, directory: Path, files: list[Path], layout: dict[str, object]) -> RepresentationManifest:
        return cls(
            suite=suite,
            source_directory=directory.resolve(strict=True).as_posix(),
            source_fingerprint=fingerprint_input_directory(directory),
            sources=[SourceIdentity.inspect(path) for path in files],
            layout=layout,
        )

    def write(self, store: Path) -> None:
        path = store / MANIFEST_NAME
        partial = path.with_suffix(f".{os.getpid()}.part")
        with partial.open("w") as output:
            output.write(self.model_dump_json(indent=2))
            output.flush()
            os.fsync(output.fileno())
        partial.replace(path)

    @classmethod
    def read(cls, store: Path) -> RepresentationManifest:
        path = store / MANIFEST_NAME
        if not path.is_file():
            raise RuntimeError("Representation manifest missing; rebuild the suite store")
        return cls.model_validate_json(path.read_text())

    def verify(self, suite: str, directory: Path, files: list[Path], layout: dict[str, object]) -> None:
        if self.state != "ready":
            raise RuntimeError("Representation store is incomplete; rebuild the suite store")
        if (
            self.suite != suite
            or self.source_directory != directory.resolve(strict=True).as_posix()
            or self.sources != [SourceIdentity.inspect(path) for path in files]
            or self.source_fingerprint != fingerprint_input_directory(directory)
            or self.layout != layout
        ):
            raise RuntimeError("Representation source/layout changed; rebuild the suite store")


def manifest_metadata(store: Path) -> dict[str, object]:
    if not (store / MANIFEST_NAME).is_file():
        return {"version": 1, "state": "uninitialized"}
    manifest = RepresentationManifest.read(store)
    return {
        **cast(dict[str, object], json.loads(manifest.model_dump_json())),
        "manifest_sha256": sha256_text(manifest.model_dump_json()),
    }


def required_external_files(store: Path, sources: list[Path]) -> tuple[Path, ...]:
    directory = store.resolve()
    inodes = {
        (stat.st_dev, stat.st_ino)
        for path in directory.rglob("*")
        if path.is_file() and not path.is_symlink()
        for stat in [path.stat()]
    }
    result: list[Path] = []
    for source in sorted({path.resolve(strict=True) for path in sources}):
        stat = source.stat()
        inode = (stat.st_dev, stat.st_ino)
        if not source.is_relative_to(directory) and inode not in inodes:
            result.append(source)
            inodes.add(inode)
    return tuple(result)
