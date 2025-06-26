from typing import Literal, get_args

from pydantic import BaseModel

from .database import Database
from .settings import SETTINGS, DatabaseName

BenchmarkStep = Literal["insert", "query", "append", "upsert"]

STEPS: list[BenchmarkStep] = list(get_args(BenchmarkStep))


class QueryResult(BaseModel):
    iterations: int = 1
    elapsed_sec: float
    peak_memory_mb: float


class InsertResult(QueryResult):
    disk_usage_mb: float


class CombinedResult(BaseModel):
    iterations: int = 1
    insert: InsertResult
    query: QueryResult


class BenchmarkResults(BaseModel):
    name: DatabaseName

    insert: InsertResult | None = None
    query: QueryResult | None = None
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


def benchmark_insert(db: Database) -> InsertResult:
    elapsed: list[float] = []
    memory: list[float] = []
    disk: list[float] = []

    for _ in range(SETTINGS.insert_iterations):
        elapsed.append(1)
        memory.append(1)
        disk.append(1)

    return InsertResult(
        iterations=SETTINGS.insert_iterations,
        elapsed_sec=average(elapsed),
        peak_memory_mb=average(memory),
        disk_usage_mb=average(disk),
    )


def benchmark_query(db: Database) -> QueryResult:
    elapsed: list[float] = []
    memory: list[float] = []

    for _ in range(SETTINGS.query_iterations):
        elapsed.append(1)
        memory.append(1)

    return QueryResult(
        iterations=SETTINGS.query_iterations,
        elapsed_sec=average(elapsed),
        peak_memory_mb=average(memory),
    )


def benchmark_append(db: Database) -> CombinedResult:
    insert_result = benchmark_insert(db)
    query_result = benchmark_query(db)

    return CombinedResult(
        iterations=SETTINGS.append_iterations,
        insert=insert_result,
        query=query_result,
    )


def benchmark_upsert(db: Database) -> CombinedResult:
    insert_result = benchmark_insert(db)
    query_result = benchmark_query(db)

    return CombinedResult(
        iterations=SETTINGS.upsert_iterations,
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
