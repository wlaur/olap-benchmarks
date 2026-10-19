# Raw references for incremental aggregates

These DuckDB queries define the logical answers of the ten native incremental-view
workloads. They are correctness oracles over the unchanged input tables, and do not
register raw-scan substitutes as benchmark support on other engines.

The 1000-series workloads have their own parameters and output shapes. In particular,
1008 selects the greatest Delayed-and-Priority count in the Monday week beginning
2024-01-29; it is distinct from the upstream raw 0008 query. Weeks start on Monday.
1023 totals every event in the July–December semester, while 1025 totals Delivered
events in the complete week beginning 2024-01-01.

Incremental insert blocks can produce several partial rows per key. Counts and sums
must be added, averages require their original numerator and denominator, and distinct
counts require mergeable exact states. The tests exercise multiple insert blocks,
boundaries, duplicate identifiers, empty strings, and nulls.
