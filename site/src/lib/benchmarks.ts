export type BenchmarkSuiteId = "time_series" | "rtabench" | "clickbench" | "kaggle_airbnb"

export interface BenchmarkDefinition {
  id: BenchmarkSuiteId
  navLabel: string
  title: string
  summary: string
}

export const benchmarkDefinitions: BenchmarkDefinition[] = [
  {
    id: "time_series",
    navLabel: "Time Series",
    title: "Time Series",
    summary:
      "Query-pattern and scale-sensitive latency comparisons for wide time-series workloads.",
  },
  {
    id: "rtabench",
    navLabel: "RTABench",
    title: "RTABench",
    summary:
      "Operational event-analytics workloads with a distinct visualization surface still to be designed.",
  },
  {
    id: "clickbench",
    navLabel: "ClickBench",
    title: "ClickBench",
    summary:
      "Analytical scan-heavy benchmark with a page scaffold ready for a dedicated presentation.",
  },
  {
    id: "kaggle_airbnb",
    navLabel: "Kaggle Airbnb",
    title: "Kaggle Airbnb",
    summary:
      "Join-oriented Airbnb analytics benchmark with a page scaffold ready for a dedicated presentation.",
  },
]

export const defaultBenchmarkId = benchmarkDefinitions[0]!.id

export function getBenchmarkDefinition(benchmarkId: BenchmarkSuiteId): BenchmarkDefinition {
  return (
    benchmarkDefinitions.find((benchmark) => benchmark.id === benchmarkId) ??
    benchmarkDefinitions[0]!
  )
}

export function getBenchmarkPath(benchmarkId: BenchmarkSuiteId): string {
  return `/benchmarks/${benchmarkId}`
}
