from fire import Fire  # type: ignore[import-untyped]

from .settings import DatabaseName


def benchmark(name: DatabaseName) -> None:
    pass


if __name__ == "__main__":
    Fire(
        {
            "benchmark": benchmark,
        }
    )
