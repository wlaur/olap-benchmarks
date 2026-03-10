from __future__ import annotations

import tomllib
from importlib.metadata import version as package_version
from pathlib import Path

from ..dbs.duckdb import VERSION


def test_duckdb_runtime_matches_installed_package() -> None:
    assert package_version("duckdb") == VERSION


def test_pyproject_pins_same_duckdb_version() -> None:
    pyproject = Path(__file__).resolve().parents[2] / "pyproject.toml"
    data = tomllib.loads(pyproject.read_text())
    dependencies = data["project"]["dependencies"]

    assert f"duckdb=={VERSION}" in dependencies
