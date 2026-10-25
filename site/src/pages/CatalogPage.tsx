import { ArrowUpRight, Layers3 } from "lucide-react"
import { useMemo } from "react"
import { Link, useSearchParams } from "react-router-dom"

import { CatalogCoverageMatrix } from "../components/catalog/CatalogCoverageMatrix"
import { CatalogQueryList } from "../components/catalog/CatalogQueryList"
import { QuietButton } from "../components/controls/Control"
import { ControlSelect, ControlSelectSkeleton } from "../components/controls/ControlSelect"
import { Disclosure } from "../components/layout/Disclosure"
import { PanelCard, PanelDisclosure, PanelHeader } from "../components/layout/Panel"
import { CampaignNotice } from "../components/methodology/ComparisonScopeNotices"
import { Skeleton } from "../components/Skeleton"
import { BodyText, DisplayTitle, MetaLabel, SectionTitle } from "../components/Typography"
import { useCatalogData } from "../hooks/useCatalogData"
import {
  buildCatalogSuiteSummary,
  catalogMissingAvailability,
  catalogRunAvailability,
  catalogTrackLabel,
  getSqlDialects,
} from "../lib/catalog"
import { formatDatabaseName } from "../lib/databaseNames"
import { suiteDescriptions } from "../lib/suiteDescriptions"
import { useAppStore } from "../stores/useAppStore"

export function CatalogPage() {
  const [searchParams, setSearchParams] = useSearchParams()
  const benchmarkDefinitions = useAppStore((state) => state.benchmarkDefinitions)
  const suitesLoading = useAppStore((state) => state.suitesLoading)
  const suitesError = useAppStore((state) => state.suitesError)
  const selectedSystem = useAppStore((state) => state.selectedSystem)
  const catalog = useCatalogData()
  const definition =
    benchmarkDefinitions.find((entry) => entry.id === searchParams.get("suite")) ??
    benchmarkDefinitions[0]
  const summary = useMemo(
    () =>
      definition && catalog.data
        ? buildCatalogSuiteSummary(definition, catalog.data.runs, catalog.data.manifest)
        : null,
    [definition, catalog.data],
  )
  const description = definition ? suiteDescriptions[definition.id] : undefined
  const manifestEntries = definition ? (catalog.queriesManifest[definition.queriesKey] ?? {}) : {}
  const requestedQuery = searchParams.get("query") ?? ""
  const selectedQuery = summary?.queryNames.includes(requestedQuery)
    ? requestedQuery
    : (summary?.queryNames[0] ?? "")
  const selectedSqlEntry = manifestEntries[selectedQuery]
  const sqlDialects = getSqlDialects(selectedSqlEntry)
  const requestedDialect = searchParams.get("sql_database") ?? ""
  const selectedDialect = sqlDialects.includes(requestedDialect)
    ? requestedDialect
    : (sqlDialects[0] ?? "")
  const loading = suitesLoading || catalog.loading
  const error = suitesError ?? catalog.error
  const family =
    summary?.families.find((entry) => entry.coverage_query_names.includes(requestedQuery)) ??
    summary?.families.find((entry) => entry.track === "raw") ??
    summary?.families[0]
  const explorerScale = family?.scale_factors[0] ?? summary?.scales[0]
  const explorerSystem = summary?.systems.includes(selectedSystem ?? "")
    ? selectedSystem
    : summary?.systems[0]
  const explorerParams = new URLSearchParams({ mode: "database" })
  if (family) explorerParams.set("family", family.id)
  if (explorerScale !== undefined) explorerParams.set("scale", String(explorerScale))
  if (explorerSystem) explorerParams.set("system", explorerSystem)
  if (requestedQuery && summary?.queryNames.includes(requestedQuery)) {
    explorerParams.set("query", requestedQuery)
    if (selectedDialect && selectedDialect !== "base") {
      explorerParams.set("sql_database", selectedDialect)
    }
  }
  const tracks = [...new Set(summary?.families.map((entry) => entry.track) ?? [])]
  const includedFamilies =
    summary?.families.filter((entry) => entry.track === "raw" && entry.weight > 0) ?? []
  const overallInclusion =
    includedFamilies.length === 0
      ? "Not included"
      : tracks.some((track) => track !== "raw")
        ? "Raw track only"
        : "Included"

  function handleSuiteSelect(suiteId: string) {
    setSearchParams({ suite: suiteId })
  }

  function handleQuerySelect(query: string) {
    const next = new URLSearchParams(searchParams)
    if (definition) next.set("suite", definition.id)
    next.set("query", query)
    next.delete("sql_database")
    setSearchParams(next, { replace: true })
  }

  function handleDialectSelect(dialect: string) {
    const next = new URLSearchParams(searchParams)
    if (definition) next.set("suite", definition.id)
    next.set("query", selectedQuery)
    if (dialect === "base") next.delete("sql_database")
    else next.set("sql_database", dialect)
    setSearchParams(next, { replace: true })
  }

  return (
    <div className="mx-auto w-full max-w-5xl space-y-5 pb-6 font-sans">
      <header className="flex flex-wrap items-center justify-between gap-4">
        <DisplayTitle as="h1">Benchmark catalog</DisplayTitle>
        {suitesLoading ? (
          <ControlSelectSkeleton
            label="Suite"
            labelMode="always"
            className="min-h-11 w-full sm:w-80"
          />
        ) : definition ? (
          <ControlSelect
            ariaLabel="Suite"
            label="Suite"
            value={definition.id}
            onChange={handleSuiteSelect}
            options={benchmarkDefinitions.map((entry) => ({
              value: entry.id,
              label: entry.title,
              description: suiteDescriptions[entry.id]?.purpose,
            }))}
            icon={<Layers3 className="size-4" aria-hidden="true" />}
            labelMode="always"
            className="min-h-11 w-full sm:w-80"
          />
        ) : null}
      </header>

      <section aria-label="Workload overview" aria-busy={loading && !error}>
        <PanelCard className="space-y-5 p-4 sm:p-6">
          <PanelHeader>
            <div className="min-w-0 space-y-2">
              <SectionTitle as="h2">{definition?.title ?? "Workload"}</SectionTitle>
              {description ? (
                <BodyText className="max-w-prose">{description.purpose}</BodyText>
              ) : null}
            </div>
          </PanelHeader>
          {error ? (
            <div className="space-y-3">
              <p role="alert" className="text-base text-red-200">
                Catalog results could not be loaded.
              </p>
              <QuietButton onClick={() => window.location.reload()}>
                Retry loading catalog
              </QuietButton>
            </div>
          ) : loading ? (
            <div role="status" className="space-y-4">
              <BodyText>Loading workload results…</BodyText>
              <Skeleton className="h-16 w-full" />
              <Skeleton className="h-12 w-full" />
              <Skeleton className="h-40 w-full" />
            </div>
          ) : !definition ? (
            <BodyText>No benchmark suites are configured.</BodyText>
          ) : !summary ? (
            <BodyText>No publication evidence is available for this workload.</BodyText>
          ) : (
            <>
              <dl className="grid grid-cols-2 gap-x-6 gap-y-3 text-sm sm:grid-cols-4">
                <div>
                  <dt className="text-slate-400">
                    {tracks.length > 1 ? "Queries across tracks" : "Queries"}
                  </dt>
                  <dd className="mt-1 font-semibold text-slate-100">{summary.queryNames.length}</dd>
                </div>
                <div>
                  <dt className="text-slate-400">Published scales</dt>
                  <dd className="mt-1 font-semibold text-slate-100">
                    {summary.scales.length > 0
                      ? summary.scales.map((scale) => `SF ${scale}`).join(", ")
                      : "None"}
                  </dd>
                </div>
                <div>
                  <dt className="text-slate-400">Tracks</dt>
                  <dd className="mt-1 font-semibold text-slate-100">
                    {tracks.length > 0
                      ? tracks.map((track) => track[0]!.toUpperCase() + track.slice(1)).join(", ")
                      : "None published"}
                  </dd>
                </div>
                <div>
                  <dt className="text-slate-400">Overall score</dt>
                  <dd className="mt-1 font-semibold text-slate-100">{overallInclusion}</dd>
                </div>
              </dl>

              <div className="space-y-3 border-t border-border-default pt-5">
                <SectionTitle as="h3">
                  {summary.recordedDatabaseCount === 0
                    ? "No recorded query runs for this workload"
                    : `Results recorded for ${summary.recordedDatabaseCount} of ${summary.databases.length} databases`}
                </SectionTitle>
                <BodyText className="max-w-prose text-sm">
                  Execution counts by track and scale. Complete execution does not establish
                  correctness or ranking eligibility. Run and preparation blockers are shown
                  separately from query completion.
                </BodyText>
                <Link
                  to={`/explorer/${definition.id}?${explorerParams}`}
                  className="inline-flex min-h-11 items-center gap-2 rounded-md border border-border-strong bg-surface-elevated px-4 py-2 text-base font-medium text-slate-100 outline-none hover:bg-surface-inset focus-visible:ring-2 focus-visible:ring-accent-300"
                >
                  Explore results
                  <ArrowUpRight className="size-4" aria-hidden="true" />
                </Link>
                {family ? (
                  <p className="text-sm text-slate-400">
                    {catalogTrackLabel(family)} · SF {explorerScale} · {explorerSystem}
                  </p>
                ) : null}
              </div>

              <p className="text-sm text-slate-400">
                {summary.systems.length === 1 ? "System" : "Systems"}:{" "}
                {summary.systems.join(", ") || "None recorded"}
              </p>
              <ul aria-label="Database availability" className="divide-y divide-border-subtle">
                {summary.databases.map((database) => (
                  <li
                    key={database.database}
                    data-catalog-engine={database.database}
                    className="grid gap-1 py-3 text-sm sm:grid-cols-[12rem_minmax(0,1fr)] sm:gap-4"
                  >
                    <span className="font-semibold text-slate-100">
                      {formatDatabaseName(database.database)}
                    </span>
                    <div className="min-w-0 space-y-3 text-slate-300">
                      {database.families.map(({ family: entry, runs }) => (
                        <div key={entry.id} data-catalog-family={entry.id} className="space-y-1">
                          <p className="font-medium text-slate-100">
                            {catalogTrackLabel(entry)} · {entry.coverage_query_names.length} queries
                          </p>
                          {entry.scale_factors.map((scale) => {
                            const scopedRuns = runs.filter((run) => run.scale === scale)
                            return scopedRuns.length > 0 ? (
                              scopedRuns.map((run) => (
                                <div key={run.runId} className="space-y-1">
                                  <p>
                                    SF {scale} · {catalogRunAvailability(run)}
                                  </p>
                                  {run.outcome === "blocked" ? (
                                    <p>{run.comparisonBlockers.join(" · ")}</p>
                                  ) : null}
                                  {run.outcome === "failed" ? (
                                    <Disclosure title="Failure reason" lazy>
                                      <BodyText className="text-sm [overflow-wrap:anywhere]">
                                        {run.failureReason ?? "Failure reason not recorded."}
                                      </BodyText>
                                    </Disclosure>
                                  ) : null}
                                  {summary.systems.length > 1 || scopedRuns.length > 1 ? (
                                    <p className="text-slate-400">
                                      {run.system} · {run.version}
                                    </p>
                                  ) : null}
                                </div>
                              ))
                            ) : (
                              <p key={scale}>
                                SF {scale} · {catalogMissingAvailability(database, scale)}
                              </p>
                            )
                          })}
                        </div>
                      ))}
                    </div>
                  </li>
                ))}
              </ul>
            </>
          )}
        </PanelCard>
      </section>

      {error ? (
        <PanelDisclosure title="Load error details">
          <BodyText className="break-words">{error}</BodyText>
        </PanelDisclosure>
      ) : null}

      {!loading && !error && summary ? (
        <div key={definition?.id} className="space-y-4">
          <PanelDisclosure title="Queries and SQL">
            <BodyText className="text-sm">
              Choose a query, then open its SQL. Query definitions do not establish that an engine
              executed them.
            </BodyText>
            <CatalogQueryList
              queryNames={summary.queryNames}
              selectedQuery={selectedQuery}
              selectedDialect={selectedDialect}
              sqlEntry={selectedSqlEntry}
              onSelectQuery={handleQuerySelect}
              onSelectDialect={handleDialectSelect}
            />
          </PanelDisclosure>
          <PanelDisclosure title="Coverage by scale and system">
            <CatalogCoverageMatrix summary={summary} />
          </PanelDisclosure>
          <PanelDisclosure title="Workload and publication context">
            <CampaignNotice />
            <BodyText className="max-w-prose">
              Each track uses its own published coverage queries, including queries excluded from
              performance comparisons. Complete execution requires recorded warmup and measured
              iterations for every query in that track. Partial execution retains missing,
              unsupported, skipped and failed query outcomes. Failed, invalidated and incomplete
              runs remain labelled as such even when query iterations completed. Documented
              incomplete preparation blocks comparison without erasing recorded execution counts.
              These labels do not replace performance eligibility or answer-validation evidence in
              Explorer.
            </BodyText>
            <BodyText className="max-w-prose">
              No recorded run means execution is unknown. Adapter unimplemented describes the
              harness registration snapshot, not the database’s capabilities. The catalog uses the
              selected publication’s latest run for each engine, version, driver, system and scale.
            </BodyText>
            {summary.families.map((entry) => (
              <div
                key={entry.id}
                className="space-y-1 border-t border-border-subtle pt-3 text-sm text-slate-300"
              >
                <MetaLabel>{entry.title}</MetaLabel>
                <p>
                  {entry.track} track · {entry.query_names.length} performance queries ·{" "}
                  {entry.coverage_query_names.length} coverage queries ·{" "}
                  {entry.track === "raw" && entry.weight > 0
                    ? `overall weight ${entry.weight}`
                    : "excluded from overall score"}
                </p>
              </div>
            ))}
            {description ? (
              <a
                href={`https://github.com/wlaur/olap-benchmarks/blob/main/${description.source}`}
                target="_blank"
                rel="noreferrer"
                className="inline-flex min-h-11 items-center text-base text-accent-300 underline underline-offset-4 focus-visible:outline-2 focus-visible:outline-accent-300"
              >
                Workload source
              </a>
            ) : null}
          </PanelDisclosure>
        </div>
      ) : null}
    </div>
  )
}
