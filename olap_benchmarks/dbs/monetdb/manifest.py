from dataclasses import dataclass

from ...settings import ALL_SUITE_SCALE_FACTORS, Operation, SuiteName
from . import MonetDB


@dataclass(frozen=True, slots=True)
class MonetDBBenchmarkCell:
    suite: SuiteName
    scale_factor: int
    operation: Operation


def monetdb_benchmark_manifest() -> tuple[MonetDBBenchmarkCell, ...]:
    cells: list[MonetDBBenchmarkCell] = []
    for suite, suite_class in MonetDB().suite_registry().items():
        for scale_factor in ALL_SUITE_SCALE_FACTORS[suite]:
            for operation in suite_class.supported_operations:
                cells.append(
                    MonetDBBenchmarkCell(
                        suite=suite,
                        scale_factor=scale_factor,
                        operation=operation,
                    )
                )
    return tuple(cells)
