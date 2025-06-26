from typing import Literal, get_args

from pydantic import BaseModel

from .database import Database
from .settings import DatabaseName

BenchmarkStep = Literal["insert", "query", "append", "upsert"]

STEPS: list[BenchmarkStep] = list(get_args(BenchmarkStep))


class QueryResult(BaseModel):
    iterations: int = 1
    elapsed_sec: float
    peak_memory_mb: int


class InsertResult(QueryResult):
    disk_usage_mb: int


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


def benchmark_insert(db: Database) -> InsertResult:
    return InsertResult()


def benchmark_query(db: Database) -> QueryResult:
    return QueryResult()


def benchmark_append(db: Database) -> CombinedResult:
    return CombinedResult()


def benchmark_upsert(db: Database) -> CombinedResult:
    return CombinedResult()


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
