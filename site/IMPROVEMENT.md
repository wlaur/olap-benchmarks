# Explorer UI Improvement Plan

## Problem

The current explorer page combines too many related controls and visualizations in one dense viewport. The user has to reason about system, database, database version, suite, and scale factor at the same time, while panels imply relationships that are not always valid comparisons.

The improved UI should mirror the benchmark result dimensions directly:

- system
- database
- database version
- suite
- scale factor

The interface should make valid comparisons obvious and make invalid comparisons impossible or hard to create.

## Product Direction

Use a comparison-oriented explorer instead of a dashboard that shows every visualization at once.

The explorer should have one active comparison mode. Each mode varies exactly one dimension and fixes the remaining dimensions:

- Database comparison: fixed system, suite, scale factor; compare databases, each at a selected or latest valid version.
- Scale factor comparison: fixed system, suite, database, database version; compare scale factors.
- Version comparison: fixed system, suite, database, scale factor; compare database versions.
- System comparison: fixed suite, database, database version, scale factor; compare systems.

Suite should usually be an analytical scope, not a comparison series. Different suites often have different query sets and semantics, so suite-to-suite ranking should not be a primary explorer comparison.

## Prototype

The current prototype lives at:

`/olap-benchmarks/#/prototype/explorer`

It is intentionally disconnected from real benchmark data. It uses dummy data to validate layout, interaction, and comparison rules before changing the real explorer data flow.

The prototype currently includes:

- explicit comparison modes
- dimension-aware setup controls
- URL-addressable selection state
- responsive mobile and desktop layouts
- ranking and query breakdown panels
- SQL viewer for the selected query

## Deep Linking

The explorer should support direct links to a specific subset of dimensions.

Use plural query params for selectable subsets and singular query params for the active fixed value:

- `systems`
- `system`
- `suites`
- `suite`
- `databases`
- `database`
- `scales`
- `scale`
- `versions`
- `version`
- `mode`
- `query`

Example:

```text
/prototype/explorer?mode=database&databases=clickhouse,duckdb,doris&suites=clickbench,tpc_h&suite=clickbench&scale=100&query=Q22%20join
```

The URL can carry multiple suites for browsing context, but the active comparison should still use one active suite at a time.

## Main Explorer Layout

The page should have two conceptual areas:

- Comparison setup: dimension controls and selected comparison contract.
- Results canvas: ranking, query breakdown, SQL/query details, and supporting run metadata.

Desktop layout:

- left column for setup
- right area for active results
- bounded internal scrolling only where it improves density

Mobile layout:

- natural page scroll
- setup first, results after
- no desktop height constraints
- horizontal scrolling only inside dense tables, not on the whole page

## Query SQL

Users should be able to inspect the SQL behind every query shown in the breakdown.

The query breakdown should make query labels selectable. Selecting a query should update a SQL viewer panel using the existing `SqlCodeView` component style.

When real data is connected, SQL should come from the query manifest and database-specific overrides where available.

## Separate Catalog Page

Add a separate inventory/catalog page that is not part of the comparison explorer.

Purpose:

- browse all suites
- browse queries in each suite
- show which databases have results for each suite
- show available database versions
- show available scale factors
- expose coverage gaps and missing combinations

This page should answer availability questions, not performance ranking questions.

Suggested structure:

- suite list/table
- query list per selected suite
- coverage matrix by database and scale factor
- version availability summary
- quick links into the explorer with the relevant dimensions preselected

This avoids overloading the explorer with inventory concerns.

## Implementation Phases

1. Keep iterating on the dummy prototype until the interaction model is accepted.
2. Define the real dimension model from benchmark metadata and result rows.
3. Replace dummy data with derived availability data.
4. Connect each comparison mode to real result transforms.
5. Add the catalog page for suite/query/database coverage.
6. Retire or simplify old explorer panels that no longer fit the comparison model.

## Design Rules

- Vary one benchmark dimension at a time.
- Do not compare databases across different scale factors for the same suite.
- Do allow comparing scale factors for the same database/version/system/suite.
- Do allow comparing systems only when suite, database, database version, and scale factor are fixed.
- Keep suite as the scope for a comparison, not a plotted series.
- Prefer page scroll on mobile and internal scroll only inside dense tables or code viewers.
- Avoid non-interactive icons that look like controls.
- Preserve URL state for shareable analysis links.

## Open Questions

- For database comparison, should each database default to latest completed version, or should version be fixed across databases only when the same version family exists?
- Should SQL override selection be automatic from the selected database, or shown as tabs like the existing query detail panel?
- Should the catalog page be linked from the navbar or from the explorer setup panel first?
- Which existing explorer panels are still needed after the comparison model is implemented?
