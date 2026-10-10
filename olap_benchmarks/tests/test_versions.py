from __future__ import annotations

import json
import tomllib
from collections.abc import Callable
from importlib.metadata import version as package_version
from pathlib import Path
from typing import cast, get_args

import duckdb
import httpx
import polars as pl
import pytest
from pydantic import ValidationError

from .. import run_metadata
from ..dbs import get_databases
from ..images import ImageDigestError, LocalImage, ResolvedImage, resolve_image
from ..settings import ContainerPlatform, DatabaseName
from ..version_report import (
    DOCKER_HUB_TAGS_URL,
    check_rows,
    docker_hub_repository,
    inventory_rows,
    latest_docker_hub_tag,
    outdated_rows,
)
from ..versions import ImagePin, load_versions, parse_versions, version_key

DIGEST_A = "sha256:" + "a" * 64
DIGEST_B = "sha256:" + "b" * 64


def test_manifest_covers_every_registered_database() -> None:
    assert set(load_versions()) == set(get_databases()) == set(get_args(DatabaseName))


def test_in_process_engines_are_pinned_exactly_in_pyproject() -> None:
    pyproject = Path(__file__).resolve().parents[2] / "pyproject.toml"
    dependencies = cast(list[str], tomllib.loads(pyproject.read_text())["project"]["dependencies"])
    packages = {name: pin.package for name, pin in load_versions().items() if pin.package is not None}

    assert packages == {"duckdb": "duckdb", "polars": "polars"}
    for package in packages.values():
        assert f"{package}=={package_version(package)}" in dependencies
    assert duckdb.__version__ == package_version("duckdb")
    assert pl.__version__ == package_version("polars")


def test_monetdb_selects_platform_images() -> None:
    monetdb = load_versions()["monetdb"]

    assert monetdb.images_for("linux/amd64")["server"].reference.startswith("monetdb/monetdb:")
    assert monetdb.images_for("linux/arm64")["server"].reference.startswith("wlaur/monetdb-container:")
    assert [(role, variant) for role, variant, _ in monetdb.all_images()] == [
        ("server", "linux/amd64"),
        ("server", "linux/arm64"),
    ]
    assert list(load_versions()["doris"].images) == ["fe", "be"]


@pytest.mark.parametrize(
    ("manifest", "message"),
    [
        ('[duckdb]\nversion = "1"\npackage = "duckdb"', "exactly one"),
        ('[clickhouse]\nversion = "1.0"', "at least one image"),
        ('[clickhouse]\nversion = "1.0"\nimages.server = "clickhouse:2.0"', "does not contain the pinned version"),
        (
            '[clickhouse]\nversion = "1.0"\nimages.server = "clickhouse:1.0"\narm64_images.other = "x:1.0"',
            "without a default image",
        ),
        ('[clickhouse]\nversion = "1.0"\nimages.server = { reference = "c:1.0", digest = "md5:x" }', "pattern"),
        ('[clickhouse]\nversion = "1.0"\nimages.server = "c:1.0"\nupstream_tag = "^.*$"', "named `version` group"),
        ('[nosuchdb]\nversion = "1.0"\nimages.server = "nosuchdb:1.0"', "nosuchdb"),
    ],
)
def test_manifest_rejects_invalid_entries(manifest: str, message: str) -> None:
    with pytest.raises(ValidationError, match=message):
        parse_versions(manifest)


def test_upstream_tag_pattern_is_derived_from_the_pinned_tag() -> None:
    pin = parse_versions('[timescaledb]\nversion = "2.30.2"\nimages.server = "timescale/timescaledb:2.30.2-pg18"')
    pattern = pin["timescaledb"].upstream_tag_pattern

    matched = [tag for tag in ("2.31.0-pg18", "2.31.0-pg17", "latest-pg18", "2.30.2-pg18-oss") if pattern.match(tag)]
    assert matched == ["2.31.0-pg18"]


def test_version_key_orders_numeric_versions_and_monetdb_labels() -> None:
    assert sorted(["26.10.1.1", "26.9.8.3", "26.9"], key=version_key) == ["26.9", "26.9.8.3", "26.10.1.1"]
    assert sorted(["Dec2025-SP4", "Mar2026", "Dec2025", "Aug2024-SP2"], key=version_key) == [
        "Aug2024-SP2",
        "Dec2025",
        "Dec2025-SP4",
        "Mar2026",
    ]


class FakeImageStore:
    def __init__(
        self,
        images: dict[str, LocalImage],
        after_pull: dict[str, LocalImage] | None = None,
        registry: dict[str, str] | None = None,
    ) -> None:
        self.images = dict(images)
        self.after_pull = after_pull or {}
        self.registry = registry or {}
        self.pulls: list[tuple[str, ContainerPlatform | None]] = []

    def local(self, reference: str) -> LocalImage | None:
        return self.images.get(reference)

    def pull(self, reference: str, platform: ContainerPlatform | None) -> None:
        self.pulls.append((reference, platform))
        if reference not in self.after_pull:
            raise RuntimeError("manifest unknown")
        self.images[reference] = self.after_pull[reference]

    def registry_digest(self, reference: str) -> str:
        if reference not in self.registry:
            raise RuntimeError("manifest unknown")
        return self.registry[reference]


def test_resolve_image_uses_a_local_registry_digest_without_pulling() -> None:
    store = FakeImageStore({"clickhouse:1.0": LocalImage("sha256:id", (f"clickhouse@{DIGEST_A}",))})

    resolved = resolve_image(store, ImagePin(reference="clickhouse:1.0"), "linux/arm64")

    assert resolved == ResolvedImage("clickhouse:1.0", "clickhouse", DIGEST_A, "sha256:id")
    assert resolved.repo_digest == f"clickhouse@{DIGEST_A}"
    assert store.pulls == []


def test_resolve_image_pulls_a_locally_built_image() -> None:
    reference = "wlaur/monetdb-container:11.55.9-1"
    store = FakeImageStore(
        {reference: LocalImage("sha256:built", ())},
        after_pull={reference: LocalImage("sha256:pulled", (f"docker.io/wlaur/monetdb-container@{DIGEST_A}",))},
    )

    resolved = resolve_image(store, ImagePin(reference=reference), "linux/arm64")

    assert (resolved.digest, resolved.image_id) == (DIGEST_A, "sha256:pulled")
    assert store.pulls == [(reference, "linux/arm64")]


def test_resolve_image_fails_without_a_registry_digest() -> None:
    with pytest.raises(ImageDigestError, match="Cannot pull"):
        resolve_image(FakeImageStore({}), ImagePin(reference="missing:1.0"), None)

    other_repository = FakeImageStore({}, after_pull={"a:1.0": LocalImage("sha256:id", (f"b@{DIGEST_A}",))})
    with pytest.raises(ImageDigestError, match="no registry digest"):
        resolve_image(other_repository, ImagePin(reference="a:1.0"), None)


def test_resolve_image_enforces_a_pinned_digest() -> None:
    pin = ImagePin(reference="a:1.0", digest=DIGEST_B)
    store = FakeImageStore(
        {"a:1.0": LocalImage("sha256:old", (f"a@{DIGEST_A}",))},
        after_pull={"a:1.0": LocalImage("sha256:new", (f"a@{DIGEST_A}",))},
    )

    with pytest.raises(ImageDigestError, match="versions.toml pins"):
        resolve_image(store, pin, None)

    matching = FakeImageStore({"a:1.0": LocalImage("sha256:id", (f"a@{DIGEST_A}", f"a@{DIGEST_B}"))})
    assert resolve_image(matching, pin, None).digest == DIGEST_B


def _no_command(_args: list[str]) -> None:
    return None


def test_run_metadata_records_every_image_digest(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(run_metadata, "_run_optional_command", _no_command)
    images = {
        "fe": ResolvedImage("apache/doris:fe-1", "apache/doris", DIGEST_A, "sha256:fe"),
        "be": ResolvedImage("apache/doris:be-1", "apache/doris", DIGEST_B, "sha256:be"),
    }

    execution = cast(
        dict[str, object],
        run_metadata.build_run_metadata(execution_mode="container", container_images=images, start_command=None)[
            "execution"
        ],
    )

    assert execution["container_images"] == {"fe": "apache/doris:fe-1", "be": "apache/doris:be-1"}
    assert execution["container_image_digests"] == {
        "fe": f"apache/doris@{DIGEST_A}",
        "be": f"apache/doris@{DIGEST_B}",
    }
    assert execution["container_image_ids"] == {"fe": "sha256:fe", "be": "sha256:be"}

    with pytest.raises(ValueError, match="registry digest"):
        run_metadata.build_run_metadata(execution_mode="container", start_command=None)


MANIFEST = parse_versions(
    """
[duckdb]
package = "duckdb"

[monetdb]
version = "Dec2025-SP4"
expected_runtime_version = "11.55.9"
images.server = "monetdb/monetdb:Dec2025-SP4"
arm64_images.server = "wlaur/monetdb-container:11.55.9-1"
upstream_tag = '^(?P<version>[A-Z][a-z]{2}\\d{4}(?:-SP\\d+)?)$'

[clickhouse]
version = "26.9.8.3"
images.server = "clickhouse:26.9.8.3-jammy"
"""
)
PLATFORMS: dict[DatabaseName, ContainerPlatform | None] = {
    "duckdb": None,
    "monetdb": "linux/arm64",
    "clickhouse": "linux/arm64",
}


def test_inventory_lists_images_presence_and_local_digests() -> None:
    store = FakeImageStore(
        {
            "wlaur/monetdb-container:11.55.9-1": LocalImage("sha256:built", ()),
            "clickhouse:26.9.8.3-jammy": LocalImage("sha256:ch", (f"clickhouse@{DIGEST_A}",)),
        }
    )

    rows = inventory_rows(MANIFEST, PLATFORMS, store)

    assert rows == [
        ("duckdb", package_version("duckdb"), "package", "duckdb", "yes", "installed", ""),
        ("monetdb", "Dec2025-SP4", "server (amd64)", "monetdb/monetdb:Dec2025-SP4", "no", "no", ""),
        (
            "monetdb",
            "Dec2025-SP4",
            "server (arm64)",
            "wlaur/monetdb-container:11.55.9-1",
            "yes",
            "yes",
            "none (local build)",
        ),
        ("clickhouse", "26.9.8.3", "server", "clickhouse:26.9.8.3-jammy", "yes", "yes", "sha256:aaaaaaaaaaaa"),
    ]


def test_check_pulls_host_images_and_queries_the_registry_for_others() -> None:
    store = FakeImageStore(
        {},
        after_pull={
            "wlaur/monetdb-container:11.55.9-1": LocalImage("sha256:m", (f"wlaur/monetdb-container@{DIGEST_A}",)),
            "clickhouse:26.9.8.3-jammy": LocalImage("sha256:c", (f"clickhouse@{DIGEST_B}",)),
        },
    )

    rows, ok = check_rows(MANIFEST, PLATFORMS, store)

    assert not ok
    assert rows[0] == ("duckdb", "package", "duckdb", package_version("duckdb"))
    results = {(row[0], row[1]): row[3] for row in rows[1:]}
    assert results[("monetdb", "server (amd64)")] == "FAILED: manifest unknown"
    assert results[("monetdb", "server (arm64)")] == "ok, local sha256:aaaaaaaaaaaa"
    assert results[("clickhouse", "server")] == "ok, local sha256:bbbbbbbbbbbb"
    assert store.pulls == [
        ("wlaur/monetdb-container:11.55.9-1", "linux/arm64"),
        ("clickhouse:26.9.8.3-jammy", "linux/arm64"),
    ]

    store.registry["monetdb/monetdb:Dec2025-SP4"] = DIGEST_A
    assert check_rows(MANIFEST, PLATFORMS, store)[1]


def _tags(*names: str, next_url: str | None = None) -> dict[str, object]:
    return {"results": [{"name": name} for name in names], "next": next_url}


def _client(handler: Callable[[httpx.Request], httpx.Response]) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler))


def test_latest_docker_hub_tag_follows_pages_and_filters_by_pattern() -> None:
    first_page = DOCKER_HUB_TAGS_URL.format(namespace="library", name="clickhouse")
    second_page = first_page + "?page=2"
    requested: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requested.append(str(request.url))
        if request.url.params.get("page") == "2":
            return httpx.Response(200, json=_tags("26.10.1.5-jammy"))
        return httpx.Response(200, json=_tags("latest", "26.9.8.3-jammy", "26.11-alpine", next_url=second_page))

    pattern = MANIFEST["clickhouse"].upstream_tag_pattern
    with _client(handler) as client:
        assert latest_docker_hub_tag(client, "clickhouse", pattern) == "26.10.1.5"

    assert requested[0].startswith(first_page)
    assert requested[1] == second_page


def test_docker_hub_repository_names() -> None:
    assert docker_hub_repository("clickhouse") == ("library", "clickhouse")
    assert docker_hub_repository("apache/doris") == ("apache", "doris")
    with pytest.raises(ValueError, match="not on Docker Hub"):
        docker_hub_repository("ghcr.io/basekick-labs/arc")


def test_outdated_reports_each_engine_and_isolates_failures() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "pypi.org":
            return httpx.Response(200, content=json.dumps({"info": {"version": "9.9.9"}}).encode())
        if "monetdb" in request.url.path:
            return httpx.Response(200, json=_tags("Dec2025-SP3", "Dec2025", "latest"))
        return httpx.Response(503)

    with _client(handler) as client:
        rows = {row[0]: row for row in outdated_rows(MANIFEST, client)}

    assert rows["duckdb"][2:] == ("9.9.9", "newer release available", "pypi:duckdb")
    assert rows["monetdb"][1:] == ("Dec2025-SP4", "Dec2025-SP3", "pinned is newer than upstream", "hub:monetdb/monetdb")
    assert rows["clickhouse"][3].startswith("lookup failed: HTTPStatusError")
