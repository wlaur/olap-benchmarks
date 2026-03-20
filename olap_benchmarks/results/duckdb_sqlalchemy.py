from __future__ import annotations

import importlib
from typing import Any, cast


def patch_duckdb_sqlalchemy_compat() -> None:

    try:
        typing_module = importlib.import_module("_duckdb.typing")
    except Exception:
        return

    duckdb_py_type = cast(Any, getattr(typing_module, "DuckDBPyType", None))

    if duckdb_py_type is None:
        return

    if getattr(duckdb_py_type, "__hash__", None) is None:

        def _hash(value: object) -> int:
            return hash(str(value))

        duckdb_py_type.__hash__ = _hash
