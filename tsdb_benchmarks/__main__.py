import os
from typing import Literal

from fire import Fire  # type: ignore[import-untyped]

from .settings import DatabaseName


def benchmark(name: DatabaseName) -> None:
    pass


def run(name: DatabaseName, command: Literal["start", "stop"]) -> None:
    match name:
        case "monetdb":
            from .monetdb import MonetDB

            cmd = getattr(MonetDB(), command)
            print(f"Running command {command}: {cmd}")
            os.system(cmd)

        case _:
            raise ValueError(f"Unknown database name: '{name}'")


def query(name: DatabaseName, query: str) -> None:
    match name:
        case "monetdb":
            from .monetdb import MonetDB

            print(MonetDB().fetch(query))

        case _:
            raise ValueError(f"Unknown database name: '{name}'")


if __name__ == "__main__":
    Fire({"benchmark": benchmark, "run": run, "query": query})
