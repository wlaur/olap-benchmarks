from __future__ import annotations

import gzip
import json
import shutil
import tarfile
from pathlib import Path

from olap_benchmarks.settings import REPO_ROOT
from scripts.curate_timeout_recovery import BASELINE_SHA, SOURCES, Record, sha, write_json

ARCHIVE_SHA = "a8192c23b11807fe9d7733c0f7b8197494d1be9aa750b728628661d5fed147aa"
ARCHIVE_URL = (
    "https://github.com/wlaur/olap-benchmarks/releases/download/runs/"
    "timeout-recovery-20261026-evidence-a8192c23b118.tar.gz"
)


def package(output: Path, destination: Path) -> Path:
    destination.mkdir(parents=True, exist_ok=True)
    public = REPO_ROOT / "site/public/data"
    metadata = output / "publication"
    metadata.mkdir(exist_ok=True)
    for name in ("manifest.json", "comparison.json", "queries.json", "suites.json"):
        shutil.copyfile(public / name, metadata / name)
    shutil.copyfile(REPO_ROOT / "site/tests/fixtures/overall.json", metadata / "overall.json")
    raw_sources: list[Record] = [
        {
            "revision": source.revision,
            "sha256": source.sha256,
            "object": f"objects/{source.sha256}",
            "restore_as": f"{source.revision}.db",
        }
        for source in SOURCES
    ]
    raw_sources.append(
        {
            "revision": "baseline",
            "sha256": BASELINE_SHA,
            "object": f"objects/{BASELINE_SHA}",
            "restore_as": "baseline.db",
        }
    )
    write_json(
        output / "restore-index.json",
        {
            "version": 1,
            "raw_archive_url": ARCHIVE_URL,
            "raw_archive_sha256": ARCHIVE_SHA,
            "sources": raw_sources,
            "instructions": "Verify the original archive SHA, extract it separately, and COPY the listed objects "
            "to restore_as beside these proofs. Verify each object SHA. No original worktree path is required. "
            "Place the final release DB in publication/results.db for certificate verification. "
            "Canonical suite input Parquet metadata is required by the population validator; set "
            "OLAP_BENCHMARKS_INPUT_DATA_DIRECTORY to the retained canonical input directory. "
            "No raw database or symlink payload is included in this proof bundle.",
        },
    )
    files = sorted(
        p
        for p in output.rglob("*")
        if p.is_file() and not p.is_symlink() and p.suffix != ".db" and p.name != "artifact-index.json"
    )
    entries = [{"path": p.relative_to(output).as_posix(), "bytes": p.stat().st_size, "sha256": sha(p)} for p in files]
    write_json(output / "artifact-index.json", {"version": 1, "files": entries})
    files.append(output / "artifact-index.json")
    temporary = destination / "curation-proof.tar.gz"
    with (
        temporary.open("wb") as handle,
        gzip.GzipFile(filename="", mode="wb", fileobj=handle, mtime=0) as compressed,
        tarfile.open(fileobj=compressed, mode="w") as archive,
    ):
        for path in sorted(files):
            info = archive.gettarinfo(str(path), arcname=path.relative_to(output).as_posix())
            info.mtime = 0
            info.uid = info.gid = 0
            info.uname = info.gname = ""
            info.mode = 0o644
            with path.open("rb") as source:
                archive.addfile(info, source)
    digest = sha(temporary)
    archive_path = destination / f"timeout-recovery-curated-proof-{digest[:12]}.tar.gz"
    temporary.replace(archive_path)
    index = {
        "version": 1,
        "proof_archive": {
            "asset": archive_path.name,
            "bytes": archive_path.stat().st_size,
            "sha256": digest,
            "url": f"https://github.com/wlaur/olap-benchmarks/releases/download/runs/{archive_path.name}",
        },
        "original_archive": {"url": ARCHIVE_URL, "sha256": ARCHIVE_SHA},
        "sources": raw_sources,
        "publication": [
            {"path": f"site/public/data/{name}", "sha256": sha(public / name)}
            for name in ("results.db", "manifest.json", "comparison.json", "queries.json")
        ],
        "reports": [
            entry for entry in entries if "/" not in str(entry["path"]) and str(entry["path"]).endswith(".json")
        ],
    }
    write_json(REPO_ROOT / "docs/campaigns/2026-10-timeout-recovery-artifacts.json", index)
    with tarfile.open(archive_path, "r:gz") as archive:
        by_name = {entry["path"]: entry for entry in entries}
        for member in archive.getmembers():
            if not member.isfile():
                raise ValueError(f"Proof archive contains a non-file: {member.name}")
            if member.name == "artifact-index.json":
                continue
            handle = archive.extractfile(member)
            assert handle is not None
            import hashlib

            if hashlib.sha256(handle.read()).hexdigest() != by_name[member.name]["sha256"]:
                raise ValueError(f"Proof archive entry digest mismatch: {member.name}")
    print(json.dumps({"path": str(archive_path), "sha256": digest, "bytes": archive_path.stat().st_size}))
    return archive_path


if __name__ == "__main__":
    package(REPO_ROOT / "data/temp/timeout-recovery-curated", Path("/tmp/olap-curated-publication-20261026"))
