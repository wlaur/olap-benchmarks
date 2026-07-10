# Explorer Comparison Architecture

## Product Model

The explorer mirrors the benchmark result dimensions directly:

- system
- database
- database version
- suite
- scale factor

One comparison mode varies exactly one dimension and fixes the remaining dimensions:

- Database comparison: fixed system, suite, and scale factor; each database uses its latest
  completed version.
- Scale factor comparison: fixed system, suite, database, and database version.
- Version comparison: fixed system, suite, database, and scale factor.
- System comparison: fixed suite, database, database version, and scale factor.

Suite is an analytical scope rather than a comparison series because suites have different query
sets and semantics.

## Routes

The production explorer lives at:

`/olap-benchmarks/#/explorer/:suite`

The catalog lives at:

`/olap-benchmarks/#/catalog`

The explorer stores comparison state in query parameters:

- `mode`
- `systems`
- `system`
- `databases`
- `database`
- `scales`
- `scale`
- `versions`
- `version`
- `query`
- `sql_database`

Example:

```text
/explorer/clickbench?mode=database&databases=clickhouse,duckdb&system=macbook-pro-m4&scale=1&query=Q22
```

## Data Rules

- Only completed select runs with successful query steps contribute comparison metrics.
- The latest completed run is selected for each exact system, suite, scale, database, and version
  variant.
- Rankings use the median query runtime across the query set shared by every selected series.
- The query breakdown shows missing results explicitly rather than treating them as zero.
- Database comparisons resolve the latest completed version independently for each database.
- SQL comes from the query manifest and automatically uses a database override when one exists.

## Layout

Desktop uses a setup column and a results canvas. Ranking, query breakdown, and SQL have bounded
internal scrolling where the result density requires it.

Mobile uses natural page scrolling. Dense query tables scroll horizontally within their own
container, and setup precedes results.

## Catalog

The catalog answers availability questions separately from performance ranking. It includes:

- suite navigation
- query inventory
- database and version availability
- coverage by scale factor and system
- links into the explorer at a valid suite and scale
