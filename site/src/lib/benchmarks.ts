export type BenchmarkSuiteId = "time_series" | "rtabench" | "clickbench" | "kaggle_airbnb"

export interface BenchmarkDefinition {
  id: BenchmarkSuiteId
  navLabel: string
  title: string
}

export const benchmarkDefinitions: BenchmarkDefinition[] = [
  {
    id: "time_series",
    navLabel: "Time Series",
    title: "Time Series",
  },
  {
    id: "rtabench",
    navLabel: "RTABench",
    title: "RTABench",
  },
  {
    id: "clickbench",
    navLabel: "ClickBench",
    title: "ClickBench",
  },
  {
    id: "kaggle_airbnb",
    navLabel: "Kaggle Airbnb",
    title: "Kaggle Airbnb",
  },
]

export const defaultBenchmarkId = benchmarkDefinitions[0]!.id
