from typing import Literal, get_args

from pydantic import BaseModel

from .dbs import Database
from .settings import DatabaseName

BenchmarkStep = Literal["insert", "query", "append", "upsert"]

STEPS: list[BenchmarkStep] = list(get_args(BenchmarkStep))

# TODO: collect time series data for benchmarks, e.g. mem usage, cpu, disk usage vs. time
# can be used to find patterns and calculate aggregates later on


class Result(BaseModel):
    iterations: int = 1
    elapsed_sec: float
    peak_memory_mb: float


class ResultWithDiskUsage(Result):
    disk_usage_mb: float


class CombinedResult(BaseModel):
    iterations: int = 1
    insert: ResultWithDiskUsage
    query: Result


class BenchmarkResults(BaseModel):
    name: DatabaseName

    insert: ResultWithDiskUsage | None = None
    query: Result | None = None
    append: CombinedResult | None = None
    upsert: CombinedResult | None = None


def average(values: list[float]) -> float:
    assert values

    if len(values) == 1:
        return values[0]

    # remove the highest value (assume this is a cold start)
    values.sort()
    values.pop()

    return sum(values) / len(values)


def benchmark_insert(db: Database) -> ResultWithDiskUsage:
    elapsed: list[float] = []
    memory: list[float] = []
    disk: list[float] = []

    # TODO: determine based on dataset size
    iterations = 5

    for _ in range(iterations):
        elapsed.append(1)
        memory.append(1)
        disk.append(1)

    return ResultWithDiskUsage(
        iterations=iterations,
        elapsed_sec=average(elapsed),
        peak_memory_mb=average(memory),
        disk_usage_mb=average(disk),
    )


def benchmark_query(db: Database) -> Result:
    elapsed: list[float] = []
    memory: list[float] = []

    # TODO: determine based on query
    iterations = 5

    for _ in range(iterations):
        elapsed.append(1)
        memory.append(1)

    return Result(
        iterations=iterations,
        elapsed_sec=average(elapsed),
        peak_memory_mb=average(memory),
    )


def benchmark_append(db: Database) -> CombinedResult:
    insert_result = benchmark_insert(db)
    query_result = benchmark_query(db)

    # TODO: determine based on dataset size
    iterations = 1

    return CombinedResult(
        iterations=iterations,
        insert=insert_result,
        query=query_result,
    )


def benchmark_upsert(db: Database) -> CombinedResult:
    insert_result = benchmark_insert(db)
    query_result = benchmark_query(db)

    # TODO: determine based on dataset size
    iterations = 1

    return CombinedResult(
        iterations=iterations,
        insert=insert_result,
        query=query_result,
    )


def benchmark(db: Database, steps: BenchmarkStep | list[BenchmarkStep] | Literal["all"]) -> BenchmarkResults:
    if steps == "all":
        steps = STEPS

    if not isinstance(steps, list):
        steps = [steps]

    invalid = [s for s in steps if s not in STEPS]
    if invalid:
        raise ValueError(f"Invalid benchmark steps: {invalid}")

    steps = sorted(steps, key=lambda x: STEPS.index(x))
    results = BenchmarkResults(name=db.name)

    for step in steps:
        match step:
            case "insert":
                results.insert = benchmark_insert(db)
            case "query":
                results.query = benchmark_query(db)
            case "append":
                results.append = benchmark_append(db)
            case "upsert":
                results.upsert = benchmark_upsert(db)
            case _:
                raise ValueError(f"Invalid step: '{step}'")

    return results
