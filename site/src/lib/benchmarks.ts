export type BenchmarkSuiteId =
  | "time_series"
  | "rtabench"
  | "clickbench"
  | "kaggle_airbnb"
  | "tpc_h"
  | "tpc_ds"

export interface BenchmarkDefinition {
  id: BenchmarkSuiteId
  navLabel: string
  title: string
  defaultScaleFactor: number
}

export const benchmarkDefinitions: BenchmarkDefinition[] = [
  {
    id: "time_series",
    navLabel: "Time Series",
    title: "Time Series",
    defaultScaleFactor: 1,
  },
  {
    id: "rtabench",
    navLabel: "RTABench",
    title: "RTABench",
    defaultScaleFactor: 1,
  },
  {
    id: "clickbench",
    navLabel: "ClickBench",
    title: "ClickBench",
    defaultScaleFactor: 1,
  },
  {
    id: "kaggle_airbnb",
    navLabel: "Kaggle Airbnb",
    title: "Kaggle Airbnb",
    defaultScaleFactor: 1,
  },
  {
    id: "tpc_h",
    navLabel: "TPC-H",
    title: "TPC-H",
    defaultScaleFactor: 10,
  },
  {
    id: "tpc_ds",
    navLabel: "TPC-DS",
    title: "TPC-DS",
    defaultScaleFactor: 1,
  },
]

export const defaultBenchmarkId = benchmarkDefinitions[0]!.id
