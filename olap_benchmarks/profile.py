from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

IterationRole = Literal["warmup", "measured"]


@dataclass(frozen=True, slots=True)
class BenchmarkProfile:
    id: str
    warmup_iterations: int
    measured_iterations: int
    restart_server_before_select: bool
    timed_boundary: str
    cache_policy: str
    scope: str

    def __post_init__(self) -> None:
        if self.warmup_iterations < 0 or self.measured_iterations < 1:
            raise ValueError(f"Profile {self.id} needs >= 0 warm-up and >= 1 measured iterations")

    @property
    def total_iterations(self) -> int:
        return self.warmup_iterations + self.measured_iterations

    def iteration_role(self, iteration: int) -> IterationRole:
        if not 1 <= iteration <= self.total_iterations:
            raise ValueError(f"Iteration {iteration} is outside profile {self.id} (1-{self.total_iterations})")
        return "warmup" if iteration <= self.warmup_iterations else "measured"

    def metadata(self) -> dict[str, object]:
        return {
            "id": self.id,
            "warmup_iterations": self.warmup_iterations,
            "measured_iterations": self.measured_iterations,
            "total_iterations": self.total_iterations,
            "iteration_policy": (
                f"each select query runs {self.warmup_iterations} unscored warm-up iteration(s) followed by "
                f"{self.measured_iterations} measured iteration(s); iteration_role is 'warmup' or 'measured'"
            ),
            "restart_server_before_select": self.restart_server_before_select,
            "timed_boundary": self.timed_boundary,
            "cache_policy": self.cache_policy,
            "scope": self.scope,
        }


BENCHMARK_PROFILE = BenchmarkProfile(
    id="resident-hot-v1",
    warmup_iterations=1,
    measured_iterations=3,
    restart_server_before_select=True,
    timed_boundary=(
        "end-to-end client latency: perf_counter around the connector's fetch(), from sending the query "
        "until the full result is materialised as a Polars DataFrame; driver, transfer and conversion "
        "included, engine-internal execution time not separated"
    ),
    cache_policy=(
        "server engines are restarted at the start of every select operation, after populate; embedded "
        "engines are reopened because every operation runs in a fresh process; operating-system page "
        "caches are not dropped between operations or iterations, so measured iterations read resident data"
    ),
    scope=(
        "select operations; populate, mutate and concurrent operations keep their own workload "
        "definitions and record the profile id only"
    ),
)
