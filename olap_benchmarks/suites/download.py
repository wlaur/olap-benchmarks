import logging
from pathlib import Path

import httpx

_LOGGER = logging.getLogger(__name__)
_DOWNLOAD_CHUNK_SIZE = 8 * 1024 * 1024


def _completed_partial_download(response: httpx.Response, offset: int) -> bool:
    if response.status_code != 416:
        return False

    content_range = response.headers.get("content-range", "")
    _, separator, total = content_range.rpartition("/")
    return bool(separator and total.isdigit() and int(total) == offset)


def download_file(url: str, destination: Path) -> None:
    if destination.is_file():
        _LOGGER.info("Reusing %s", destination)
        return

    destination.parent.mkdir(parents=True, exist_ok=True)
    partial = destination.with_name(f"{destination.name}.part")
    offset = partial.stat().st_size if partial.is_file() else 0
    headers = {"Range": f"bytes={offset}-"} if offset else {}

    _LOGGER.info("Downloading %s to %s", url, destination)
    with httpx.stream("GET", url, headers=headers, follow_redirects=True, timeout=None) as response:
        if _completed_partial_download(response, offset):
            partial.replace(destination)
            return

        response.raise_for_status()
        append = offset > 0 and response.status_code == 206
        content_length = response.headers.get("content-length")
        expected_size = int(content_length) + (offset if append else 0) if content_length is not None else None
        mode = "ab" if append else "wb"
        with partial.open(mode) as output:
            for chunk in response.iter_raw(_DOWNLOAD_CHUNK_SIZE):
                output.write(chunk)

    actual_size = partial.stat().st_size
    if expected_size is not None and actual_size != expected_size:
        raise RuntimeError(
            f"Incomplete download for {destination}: expected {expected_size:_} bytes, got {actual_size:_}"
        )

    partial.replace(destination)
