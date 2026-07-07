export type BenchmarkSuiteId =
  | "time_series"
  | "rtabench"
  | "clickbench"
  | "kaggle_airbnb"
  | "tpch_sf10"
  | "tpch_sf50"
  | "tpcds_sf1"

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
  {
    id: "tpch_sf10",
    navLabel: "TPC-H SF10",
    title: "TPC-H SF10",
  },
  {
    id: "tpch_sf50",
    navLabel: "TPC-H SF50",
    title: "TPC-H SF50",
  },
  {
    id: "tpcds_sf1",
    navLabel: "TPC-DS SF1",
    title: "TPC-DS SF1",
  },
]

export const defaultBenchmarkId = benchmarkDefinitions[0]!.id
