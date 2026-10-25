interface SuiteDescription {
  purpose: string
  source: string
}

export const suiteDescriptions: Record<string, SuiteDescription> = {
  time_series: {
    purpose:
      "Time-based lookups, aggregates and window queries over synthetic tall, wide and large datasets.",
    source: "olap_benchmarks/suites/time_series/config.py",
  },
  time_series_metrics: {
    purpose:
      "The same time-series operations across wide and entity–attribute–value layouts, varying the number of stored metrics.",
    source: "olap_benchmarks/suites/time_series_metrics/README.md",
  },
  time_series_ranges: {
    purpose:
      "Fetches, averages and derived calculations over 1, 10 and 100 days of selected metrics in wide and entity–attribute–value layouts.",
    source: "olap_benchmarks/suites/time_series_ranges/README.md",
  },
  rtabench: {
    purpose:
      "Order, customer and product analytics based on RTABench, with separate raw-query and prepared-aggregate tracks.",
    source: "olap_benchmarks/suites/rtabench/config.py",
  },
  clickbench: {
    purpose: "Analytical queries from ClickBench over its web-traffic hits dataset.",
    source: "olap_benchmarks/suites/clickbench/config.py",
  },
  jsonbench: {
    purpose:
      "JSON extraction and aggregation over Bluesky events, including collection counts and posting activity.",
    source: "olap_benchmarks/suites/jsonbench/config.py",
  },
  chat_threads: {
    purpose:
      "Queries over generated chat threads and messages, with thread metadata and message content stored separately.",
    source: "olap_benchmarks/suites/chat_threads/config.py",
  },
  kaggle_airbnb: {
    purpose:
      "A smoke workload of counts and joins over an Austin Inside Airbnb snapshot, including listings, calendars and reviews.",
    source: "olap_benchmarks/suites/kaggle_airbnb/config.py",
  },
  tpc_h: {
    purpose:
      "TPC-H-derived queries over generated orders, suppliers and line items. These are not official TPC-H results.",
    source: "olap_benchmarks/suites/tpc_h/config.py",
  },
  tpc_ds: {
    purpose:
      "TPC-DS-derived analytical queries over generated store, catalog and web-sales data. These are not official TPC-DS results.",
    source: "olap_benchmarks/suites/tpc_ds/config.py",
  },
  device_telemetry: {
    purpose:
      "Analytics over synthetic device readings with twelve nullable metrics, device tags and mixed reporting cadences.",
    source: "olap_benchmarks/suites/device_telemetry/README.md",
  },
  join_order: {
    purpose:
      "Join Order Benchmark queries over the fixed May 2013 IMDb snapshot and its 21 tables.",
    source: "olap_benchmarks/suites/join_order/README.md",
  },
  parquet_analytics: {
    purpose:
      "TPC-H-derived queries against the same external Parquet files, measuring a common file representation separately from native tables.",
    source: "olap_benchmarks/suites/parquet_analytics/README.md",
  },
  json_representation: {
    purpose:
      "Matching queries over raw JSON text and materialized fields from the same JSONBench events, published as separate tracks.",
    source: "olap_benchmarks/suites/json_representation/README.md",
  },
}
