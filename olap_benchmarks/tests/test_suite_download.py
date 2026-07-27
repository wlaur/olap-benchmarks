from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import cast

import httpx
import pytest

from olap_benchmarks.suites.download import download_file


def test_download_file_resumes_an_interrupted_download(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    destination = tmp_path / "dataset.bin"
    partial = tmp_path / "dataset.bin.part"
    partial.write_bytes(b"first")
    request_headers: dict[str, str] = {}

    @contextmanager
    def stream(_method: str, _url: str, **kwargs: object) -> Generator[httpx.Response]:
        headers = kwargs["headers"]
        assert isinstance(headers, dict)
        request_headers.update(cast(dict[str, str], headers))
        yield httpx.Response(
            206,
            headers={"content-length": "7"},
            stream=httpx.ByteStream(b" second"),
            request=httpx.Request("GET", "https://example.invalid/dataset.bin"),
        )

    monkeypatch.setattr(httpx, "stream", stream)

    download_file("https://example.invalid/dataset.bin", destination)

    assert request_headers == {"Range": "bytes=5-"}
    assert destination.read_bytes() == b"first second"
    assert not partial.exists()


def test_download_file_replaces_a_partial_when_ranges_are_unsupported(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    destination = tmp_path / "dataset.bin"
    partial = tmp_path / "dataset.bin.part"
    partial.write_bytes(b"stale")

    @contextmanager
    def stream(_method: str, _url: str, **_kwargs: object) -> Generator[httpx.Response]:
        yield httpx.Response(
            200,
            headers={"content-length": "5"},
            stream=httpx.ByteStream(b"fresh"),
            request=httpx.Request("GET", "https://example.invalid/dataset.bin"),
        )

    monkeypatch.setattr(httpx, "stream", stream)

    download_file("https://example.invalid/dataset.bin", destination)

    assert destination.read_bytes() == b"fresh"
