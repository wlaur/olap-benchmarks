import {
  ChevronDown,
  ChevronUp,
  Database,
  FlaskConical,
  Gauge,
  GitBranch,
  Scale,
  Search,
  Server,
  Settings2,
  Trophy,
  type LucideIcon,
} from "lucide-react"
import { Fragment, useEffect, useMemo, useState, type ReactNode } from "react"
import { useNavigate, useSearchParams } from "react-router-dom"

import { ControlChip, QuietButton, SegmentedButton } from "../components/controls/Control"
import { ControlSelect } from "../components/controls/ControlSelect"
import { ChartFrame, PanelCard, PanelHeader } from "../components/layout/Panel"
import { Skeleton } from "../components/Skeleton"
import { SqlCodeView } from "../components/SqlCodeView"
import { MetaLabel, SectionTitle } from "../components/Typography"
import { useExplorerData } from "../hooks/useExplorerData"
import type { BenchmarkDefinition, BenchmarkSuiteId } from "../lib/benchmarks"
import { cn } from "../lib/cn"
import { getDatabaseColors } from "../lib/databaseColors"
import type { ExplorerQueryMetric, QuerySqlEntry } from "../lib/types"

type ComparisonMode = "database" | "scale" | "version" | "system"

const COMPARISON_MODES: {
  id: ComparisonMode
  label: string
  icon: LucideIcon
}[] = [
  { id: "database", label: "Database", icon: Database },
  { id: "scale", label: "Scale factor", icon: Scale },
  { id: "version", label: "Version", icon: GitBranch },
  { id: "system", label: "System", icon: Server },
]

const SERIES_COLORS = ["#5eead4", "#93c5fd", "#fbbf24", "#fb7185", "#c4b5fd"] as const

interface ExplorerPageProps {
  suiteId: BenchmarkSuiteId
  suiteDefinition: BenchmarkDefinition
  benchmarkDefinitions: readonly BenchmarkDefinition[]
  preferredSystem: string | null
}

interface RawSelection {
  system: string
  systems: string[]
  database: string
  databases: string[]
  scale: number | null
  scales: number[]
  version: string
  versions: string[]
}

interface ResolvedSelection {
  system: string
  systems: string[]
  systemOptions: string[]
  database: string
  databases: string[]
  databaseOptions: string[]
  scale: number
  scales: number[]
  scaleOptions: number[]
  version: string
  versions: string[]
  versionOptions: string[]
  latestVersions: Map<string, string>
  ready: boolean
}

interface RunVariant {
  runId: number
  system: string
  scale: number
  database: string
  version: string
  finishedAt: string
}

interface ComparisonRow {
  id: string
  label: string
  detail: string
  color: string
  valueMs: number
  database: string
  queryValues: Map<string, number>
}

interface QueryRow {
  query: string
  values: (number | null)[]
}

export function ExplorerPage({
  suiteId,
  suiteDefinition,
  benchmarkDefinitions,
  preferredSystem,
}: ExplorerPageProps) {
  const navigate = useNavigate()
  const [searchParams, setSearchParams] = useSearchParams()
  const [initialState] = useState(() => parseUrlState(searchParams, preferredSystem))
  const [mode, setMode] = useState<ComparisonMode>(() =>
    parseComparisonMode(searchParams.get("mode")),
  )
  const [rawSelection, setRawSelection] = useState<RawSelection>(initialState)
  const [requestedQuery, setRequestedQuery] = useState(searchParams.get("query") ?? "")
  const [requestedSqlDatabase, setRequestedSqlDatabase] = useState(
    searchParams.get("sql_database") ?? "",
  )
  const [showSetup, setShowSetup] = useState(false)
  const [showAllQueries, setShowAllQueries] = useState(false)
  const { metrics, queriesManifest, loading, error } = useExplorerData(suiteId)
  const variants = useMemo(() => makeRunVariants(metrics), [metrics])
  const selection = useMemo(
    () =>
      resolveSelection({
        mode,
        variants,
        raw: rawSelection,
        preferredSystem,
        defaultScale: suiteDefinition.defaultScaleFactor,
      }),
    [mode, preferredSystem, rawSelection, suiteDefinition.defaultScaleFactor, variants],
  )
  const databaseColors = useMemo(
    () => getDatabaseColors(unique(variants.map((variant) => variant.database))),
    [variants],
  )
  const comparisonRows = useMemo(
    () => buildComparisonRows({ mode, metrics, variants, selection, databaseColors }),
    [databaseColors, metrics, mode, selection, variants],
  )
  const sharedQueries = useMemo(() => findSharedQueries(comparisonRows), [comparisonRows])
  const rankedRows = useMemo(
    () =>
      comparisonRows
        .map((row) => ({
          ...row,
          valueMs: median(
            sharedQueries
              .map((query) => row.queryValues.get(query))
              .filter((value): value is number => value !== undefined),
          ),
        }))
        .sort((a, b) => a.valueMs - b.valueMs),
    [comparisonRows, sharedQueries],
  )
  const manifestEntries = useMemo(
    () => queriesManifest[suiteDefinition.queriesKey] ?? {},
    [queriesManifest, suiteDefinition.queriesKey],
  )
  const queryNames = useMemo(
    () => orderQueryNames(Object.keys(manifestEntries), rankedRows),
    [manifestEntries, rankedRows],
  )
  const selectedQuery = queryNames.includes(requestedQuery) ? requestedQuery : (queryNames[0] ?? "")
  const queryRows = useMemo(() => makeQueryRows(queryNames, rankedRows), [queryNames, rankedRows])
  const sqlDatabaseOptions = useMemo(
    () =>
      unique(
        mode === "database"
          ? rankedRows.map((row) => row.database)
          : selection.database
            ? [selection.database]
            : [],
      ),
    [mode, rankedRows, selection.database],
  )
  const sqlDatabase = sqlDatabaseOptions.includes(requestedSqlDatabase)
    ? requestedSqlDatabase
    : (sqlDatabaseOptions[0] ?? "")
  const selectedSqlEntry = manifestEntries[selectedQuery]
  const selectedSql = resolveSql(selectedSqlEntry, sqlDatabase)
  const selectedQueryRows = useMemo(
    () =>
      rankedRows
        .flatMap((row) => {
          const valueMs = row.queryValues.get(selectedQuery)
          return valueMs === undefined ? [] : [{ ...row, valueMs }]
        })
        .sort((a, b) => a.valueMs - b.valueMs),
    [rankedRows, selectedQuery],
  )
  const nextSearchParams = useMemo(() => {
    if (!selection.ready || !selectedQuery) return ""
    const params = new URLSearchParams()
    params.set("mode", mode)
    params.set("system", selection.system)
    params.set("systems", selection.systems.join(","))
    params.set("database", selection.database)
    params.set("databases", selection.databases.join(","))
    params.set("scale", String(selection.scale))
    params.set("scales", selection.scales.join(","))
    params.set("version", selection.version)
    params.set("versions", selection.versions.join(","))
    params.set("query", selectedQuery)
    if (sqlDatabase) params.set("sql_database", sqlDatabase)
    return params.toString()
  }, [mode, selectedQuery, selection, sqlDatabase])

  useEffect(() => {
    if (nextSearchParams && searchParams.toString() !== nextSearchParams) {
      setSearchParams(nextSearchParams, { replace: true })
    }
  }, [nextSearchParams, searchParams, setSearchParams])

  function updateRawSelection(update: Partial<RawSelection>) {
    setRawSelection((current) => ({ ...current, ...update }))
  }

  function handleSuiteChange(nextSuiteId: string) {
    navigate(`/explorer/${nextSuiteId}?mode=${mode}`)
  }

  const showLoading = loading

  return (
    <div className="flex min-h-full w-full max-w-full min-w-0 shrink-0 flex-col gap-4 overflow-x-clip pb-8">
      <header className="grid gap-4 xl:grid-cols-[minmax(0,1fr)_auto] xl:items-end">
        <div className="min-w-0">
          <MetaLabel>Benchmark explorer</MetaLabel>
          <h2 className="mt-1 text-2xl font-semibold text-slate-50 sm:text-3xl">
            {suiteDefinition.title} results
          </h2>
          <p className="mt-2 max-w-3xl text-sm leading-6 text-slate-400">
            See the overall result first, then inspect individual queries or adjust what is being
            compared.
          </p>
        </div>
        <div className="min-w-0">
          <MetaLabel className="mb-2 block">Compare by</MetaLabel>
          <div className="grid w-full min-w-0 grid-cols-2 gap-2 sm:grid-cols-4">
            {COMPARISON_MODES.map((comparisonMode) => {
              const Icon = comparisonMode.icon
              return (
                <SegmentedButton
                  key={comparisonMode.id}
                  selected={mode === comparisonMode.id}
                  aria-pressed={mode === comparisonMode.id}
                  onClick={() => setMode(comparisonMode.id)}
                  size="md"
                  className="min-h-10 min-w-0 gap-1.5 rounded-lg px-2 text-xs sm:gap-2 sm:px-3 sm:text-sm"
                >
                  <Icon className="h-4 w-4 shrink-0" strokeWidth={1.8} />
                  <span className="truncate">{comparisonMode.label}</span>
                </SegmentedButton>
              )
            })}
          </div>
        </div>
      </header>

      {showSetup ? (
        <PanelCard className="p-3 sm:p-4">
          <PanelHeader className="mb-4 flex-wrap">
            <div>
              <MetaLabel>Advanced controls</MetaLabel>
              <SectionTitle as="h3" className="mt-1">
                Adjust comparison
              </SectionTitle>
            </div>
            <QuietButton size="sm" onClick={() => setShowSetup(false)}>
              Done
            </QuietButton>
          </PanelHeader>

          {showLoading ? (
            <SetupSkeleton />
          ) : error ? (
            <div className="border-y border-red-500/30 bg-red-950/20 px-3 py-4 text-sm text-red-300">
              Failed to load explorer data: {error}
            </div>
          ) : !selection.ready ? (
            <div className="border-y border-border-subtle px-3 py-4 text-sm text-slate-500">
              No completed query results are available for this suite.
            </div>
          ) : (
            <div className="grid min-w-0 gap-3 md:grid-cols-2 2xl:grid-cols-5">
              <DimensionRow
                icon={Server}
                label="System"
                role={mode === "system" ? "varies" : "fixed"}
                summary={
                  mode === "system" ? `${selection.systems.length} selected` : selection.system
                }
              >
                {mode === "system" ? (
                  <ChoiceChips
                    options={selection.systemOptions.map((system, index) => ({
                      id: system,
                      label: system,
                      color: getSeriesColor(index),
                    }))}
                    selectedValues={selection.systems}
                    onToggle={(system) =>
                      updateRawSelection({ systems: toggleSelection(selection.systems, system) })
                    }
                  />
                ) : (
                  <DimensionSelect
                    ariaLabel="System"
                    label="System"
                    icon={Server}
                    value={selection.system}
                    options={selection.systemOptions.map((system) => ({
                      id: system,
                      label: system,
                    }))}
                    onChange={(system) => updateRawSelection({ system })}
                  />
                )}
              </DimensionRow>

              <DimensionRow
                icon={Database}
                label="Database"
                role={mode === "database" ? "varies" : "fixed"}
                summary={
                  mode === "database"
                    ? `${selection.databases.length} selected`
                    : formatDatabaseName(selection.database)
                }
              >
                {mode === "database" ? (
                  <ChoiceChips
                    options={selection.databaseOptions.map((database) => ({
                      id: database,
                      label: formatDatabaseName(database),
                      color: databaseColors[database] ?? getSeriesColor(0),
                    }))}
                    selectedValues={selection.databases}
                    onToggle={(database) =>
                      updateRawSelection({
                        databases: toggleSelection(selection.databases, database),
                      })
                    }
                  />
                ) : (
                  <DimensionSelect
                    ariaLabel="Database"
                    label="Database"
                    icon={Database}
                    value={selection.database}
                    options={selection.databaseOptions.map((database) => ({
                      id: database,
                      label: formatDatabaseName(database),
                    }))}
                    onChange={(database) => updateRawSelection({ database })}
                  />
                )}
              </DimensionRow>

              <DimensionRow
                icon={GitBranch}
                label="Database version"
                role={mode === "version" ? "varies" : "fixed"}
                summary={
                  mode === "version"
                    ? `${selection.versions.length} selected`
                    : mode === "database"
                      ? "Latest completed per database"
                      : selection.version
                }
              >
                {mode === "version" ? (
                  <ChoiceChips
                    options={selection.versionOptions.map((version, index) => ({
                      id: version,
                      label: version,
                      color: getSeriesColor(index),
                    }))}
                    selectedValues={selection.versions}
                    onToggle={(version) =>
                      updateRawSelection({
                        versions: toggleSelection(selection.versions, version),
                      })
                    }
                  />
                ) : mode === "database" ? (
                  <VersionPins
                    databaseIds={selection.databases}
                    latestVersions={selection.latestVersions}
                    databaseColors={databaseColors}
                  />
                ) : (
                  <DimensionSelect
                    ariaLabel="Database version"
                    label="Version"
                    icon={GitBranch}
                    value={selection.version}
                    options={selection.versionOptions.map((version) => ({
                      id: version,
                      label: version,
                    }))}
                    onChange={(version) => updateRawSelection({ version })}
                  />
                )}
              </DimensionRow>

              <DimensionRow
                icon={FlaskConical}
                label="Suite"
                role="fixed"
                summary={`${queryNames.length} queries with results`}
              >
                <DimensionSelect
                  ariaLabel="Suite"
                  label="Suite"
                  icon={FlaskConical}
                  value={suiteId}
                  options={benchmarkDefinitions.map((definition) => ({
                    id: definition.id,
                    label: definition.title,
                  }))}
                  onChange={handleSuiteChange}
                />
              </DimensionRow>

              <DimensionRow
                icon={Scale}
                label="Scale factor"
                role={mode === "scale" ? "varies" : "fixed"}
                summary={
                  mode === "scale" ? `${selection.scales.length} selected` : `SF ${selection.scale}`
                }
              >
                {mode === "scale" ? (
                  <ChoiceChips
                    options={selection.scaleOptions.map((scale, index) => ({
                      id: scale,
                      label: `SF ${scale}`,
                      color: getSeriesColor(index),
                    }))}
                    selectedValues={selection.scales}
                    onToggle={(scale) =>
                      updateRawSelection({ scales: toggleSelection(selection.scales, scale) })
                    }
                  />
                ) : (
                  <DimensionSelect
                    ariaLabel="Scale factor"
                    label="Scale"
                    icon={Scale}
                    value={String(selection.scale)}
                    options={selection.scaleOptions.map((scale) => ({
                      id: String(scale),
                      label: `SF ${scale}`,
                    }))}
                    onChange={(scale) => updateRawSelection({ scale: Number(scale) })}
                  />
                )}
              </DimensionRow>
            </div>
          )}
        </PanelCard>
      ) : null}

      <PanelCard className="min-w-0 space-y-4 p-3 sm:p-4">
        <PanelHeader className="flex-wrap">
          <div className="min-w-0">
            <MetaLabel>Overview</MetaLabel>
            <SectionTitle as="h3" className="mt-1 text-lg sm:text-xl">
              {getModeLabel(mode)}
            </SectionTitle>
            {!showLoading && selection.ready ? (
              <p className="mt-2 max-w-4xl text-sm leading-6 text-slate-400">
                {getComparisonDescription(
                  mode,
                  suiteDefinition.title,
                  selection,
                  rankedRows.length,
                )}
              </p>
            ) : null}
          </div>
          <QuietButton
            size="sm"
            onClick={() => setShowSetup((current) => !current)}
            className="gap-2 rounded-lg"
          >
            <Settings2 className="h-3.5 w-3.5" strokeWidth={1.8} />
            Adjust comparison
          </QuietButton>
        </PanelHeader>

        {showLoading ? (
          <ResultsSkeleton />
        ) : error ? (
          <div className="flex min-h-72 items-center justify-center border-y border-red-500/30 bg-red-950/20 px-4 text-sm text-red-300">
            Failed to load explorer data: {error}
          </div>
        ) : rankedRows.length === 0 ? (
          <div className="flex min-h-72 items-center justify-center border-y border-border-subtle text-sm text-slate-500">
            No completed query results match this comparison.
          </div>
        ) : (
          <>
            <OverviewStats rows={rankedRows} sharedQueryCount={sharedQueries.length} />

            <ChartFrame className="p-3 sm:p-4">
              <div className="mb-4 flex flex-wrap items-end justify-between gap-3">
                <div>
                  <MetaLabel>Overall ranking</MetaLabel>
                  <p className="mt-1 text-sm font-medium text-slate-200">
                    Median across {sharedQueries.length} shared queries
                  </p>
                </div>
                <div className="flex items-center gap-2 text-xs font-medium text-slate-400">
                  <Gauge className="h-3.5 w-3.5 text-accent-300" strokeWidth={1.8} />
                  Shorter bars are faster
                </div>
              </div>
              <RankingBars rows={rankedRows} />
            </ChartFrame>
          </>
        )}
      </PanelCard>

      {!showLoading && !error && rankedRows.length > 0 ? (
        <PanelCard className="min-w-0 space-y-4 p-3 sm:p-4">
          <PanelHeader className="flex-wrap">
            <div>
              <MetaLabel>Drill down</MetaLabel>
              <SectionTitle as="h3" className="mt-1 text-lg sm:text-xl">
                Explore one query
              </SectionTitle>
              <p className="mt-2 max-w-2xl text-sm leading-6 text-slate-400">
                Select a query to compare its runtime and inspect the SQL used by each database.
              </p>
            </div>
            <DimensionSelect
              ariaLabel="Query"
              label="Query"
              icon={Search}
              value={selectedQuery}
              options={queryNames.map((query) => ({ id: query, label: query }))}
              onChange={setRequestedQuery}
            />
          </PanelHeader>

          <div className="grid min-w-0 gap-4 xl:grid-cols-[minmax(18rem,0.72fr)_minmax(0,1.28fr)]">
            <ChartFrame className="p-3 sm:p-4">
              <div className="mb-4">
                <MetaLabel>Runtime for this query</MetaLabel>
                <p
                  className="mt-1 truncate text-sm font-medium text-slate-200"
                  title={selectedQuery}
                >
                  {selectedQuery}
                </p>
                <p className="mt-1 text-xs text-slate-500">Shorter bars are faster</p>
              </div>
              <RankingBars rows={selectedQueryRows} />
            </ChartFrame>

            <ChartFrame className="flex min-h-[24rem] flex-col overflow-hidden p-3 sm:p-4">
              <div className="mb-3 shrink-0 space-y-3">
                <div>
                  <MetaLabel>SQL used</MetaLabel>
                  <p className="mt-1 truncate text-sm font-medium text-slate-200">
                    {selectedQuery}
                  </p>
                </div>
                {sqlDatabaseOptions.length > 1 ? (
                  <div className="panel-scrollbar flex gap-1 overflow-x-auto pb-1">
                    {sqlDatabaseOptions.map((database) => (
                      <SegmentedButton
                        key={database}
                        selected={sqlDatabase === database}
                        aria-pressed={sqlDatabase === database}
                        onClick={() => setRequestedSqlDatabase(database)}
                        size="xs"
                        className="shrink-0 rounded-md"
                      >
                        {formatDatabaseName(database)}
                      </SegmentedButton>
                    ))}
                  </div>
                ) : null}
              </div>
              <div className="min-h-0 flex-1 overflow-hidden rounded-lg border border-border-subtle bg-surface-primary/40">
                <SqlCodeView code={selectedSql} wrapLines className="text-xs leading-relaxed" />
              </div>
            </ChartFrame>
          </div>

          <div className="border-t border-border-subtle pt-4">
            <QuietButton
              size="sm"
              onClick={() => setShowAllQueries((current) => !current)}
              aria-expanded={showAllQueries}
              className="gap-2 rounded-lg"
            >
              {showAllQueries ? (
                <ChevronUp className="h-3.5 w-3.5" strokeWidth={1.8} />
              ) : (
                <ChevronDown className="h-3.5 w-3.5" strokeWidth={1.8} />
              )}
              {showAllQueries ? "Hide" : "Show"} all {queryRows.length} query results
            </QuietButton>
            {showAllQueries ? (
              <ChartFrame className="mt-4 flex max-h-[36rem] min-h-[22rem] flex-col overflow-hidden p-3 sm:p-4">
                <div className="mb-3 shrink-0">
                  <MetaLabel>All queries</MetaLabel>
                  <p className="mt-1 text-sm font-medium text-slate-200">
                    Select a row to update the query detail above
                  </p>
                </div>
                <QueryBreakdown
                  rows={rankedRows}
                  queryRows={queryRows}
                  selectedQuery={selectedQuery}
                  onSelectQuery={setRequestedQuery}
                />
              </ChartFrame>
            ) : null}
          </div>
        </PanelCard>
      ) : null}
    </div>
  )
}

function DimensionRow({
  icon: Icon,
  label,
  role,
  summary,
  children,
}: {
  icon: LucideIcon
  label: string
  role: "fixed" | "varies"
  summary: string
  children: ReactNode
}) {
  return (
    <section className="min-w-0 rounded-lg bg-surface-inset p-3">
      <div className="flex min-w-0 items-start gap-2 sm:gap-3">
        <span className="flex h-7 w-7 shrink-0 items-center justify-center rounded-lg border border-border-default bg-surface-raised text-slate-300 sm:h-8 sm:w-8">
          <Icon className="h-3.5 w-3.5 sm:h-4 sm:w-4" strokeWidth={1.8} />
        </span>
        <div className="min-w-0 flex-1">
          <div className="flex min-w-0 items-center justify-between gap-2">
            <p className="truncate text-sm font-semibold text-slate-100">{label}</p>
            <RoleBadge role={role} />
          </div>
          <p className="mt-0.5 truncate text-xs text-slate-400">{summary}</p>
        </div>
      </div>
      <div className="mt-2.5 min-w-0">{children}</div>
    </section>
  )
}

function OverviewStats({
  rows,
  sharedQueryCount,
}: {
  rows: readonly ComparisonRow[]
  sharedQueryCount: number
}) {
  const fastest = rows[0]
  const slowest = rows.at(-1)
  if (!fastest || !slowest) return null
  const range = fastest.valueMs > 0 ? slowest.valueMs / fastest.valueMs : 0

  return (
    <div className="grid gap-2 sm:grid-cols-3">
      <div className="rounded-lg border border-border-subtle bg-surface-inset px-4 py-3">
        <div className="flex items-center gap-2 text-xs font-semibold tracking-wide text-slate-500 uppercase">
          <Trophy className="h-3.5 w-3.5 text-amber-300" strokeWidth={1.8} />
          Fastest overall
        </div>
        <p className="mt-2 truncate text-xl font-semibold text-slate-50">{fastest.label}</p>
        <p className="mt-1 text-xs text-slate-400">
          {formatRuntime(fastest.valueMs)} median runtime
        </p>
      </div>
      <div className="rounded-lg border border-border-subtle bg-surface-inset px-4 py-3">
        <p className="text-xs font-semibold tracking-wide text-slate-500 uppercase">
          Performance range
        </p>
        <p className="mt-2 text-xl font-semibold text-slate-50">{range.toFixed(1)}×</p>
        <p className="mt-1 truncate text-xs text-slate-400" title={slowest.label}>
          Fastest to slowest ({slowest.label})
        </p>
      </div>
      <div className="rounded-lg border border-border-subtle bg-surface-inset px-4 py-3">
        <p className="text-xs font-semibold tracking-wide text-slate-500 uppercase">
          Shared workload
        </p>
        <p className="mt-2 text-xl font-semibold text-slate-50">{sharedQueryCount} queries</p>
        <p className="mt-1 text-xs text-slate-400">Successful for every selected series</p>
      </div>
    </div>
  )
}

function RoleBadge({ role }: { role: "fixed" | "varies" }) {
  return (
    <span
      className={cn(
        "shrink-0 rounded-full border px-1.5 py-0.5 text-[0.58rem] font-semibold tracking-wide uppercase sm:px-2 sm:text-[0.62rem]",
        role === "varies"
          ? "border-accent-300/35 bg-accent-400/10 text-accent-200"
          : "border-border-default bg-surface-raised text-slate-400",
      )}
    >
      {role}
    </span>
  )
}

function DimensionSelect<T extends string>({
  ariaLabel,
  label,
  icon: Icon,
  value,
  options,
  onChange,
}: {
  ariaLabel: string
  label: string
  icon: LucideIcon
  value: string
  options: readonly { id: T; label: string }[]
  onChange: (value: T) => void
}) {
  return (
    <ControlSelect
      ariaLabel={ariaLabel}
      label={label}
      value={value}
      onChange={(nextValue) => onChange(nextValue as T)}
      options={options.map((option) => ({ value: option.id, label: option.label }))}
      icon={<Icon className="h-3 w-3" strokeWidth={1.8} />}
      labelMode="always"
      className="w-full max-w-full"
      menuClassName="min-w-[11rem] sm:min-w-[13rem]"
    />
  )
}

function ChoiceChips<T extends string | number>({
  options,
  selectedValues,
  onToggle,
}: {
  options: readonly { id: T; label: string; color: string }[]
  selectedValues: readonly T[]
  onToggle: (value: T) => void
}) {
  return (
    <div className="flex min-w-0 flex-wrap gap-2">
      {options.map((option) => {
        const selected = selectedValues.includes(option.id)
        return (
          <ControlChip
            key={String(option.id)}
            selected={selected}
            aria-pressed={selected}
            onClick={() => onToggle(option.id)}
            size="sm"
            className="min-h-8 max-w-full min-w-0 gap-1.5 rounded-lg"
          >
            <span
              className="h-2 w-2 shrink-0 rounded-full"
              style={{ backgroundColor: option.color }}
            />
            <span className="min-w-0 truncate">{option.label}</span>
          </ControlChip>
        )
      })}
    </div>
  )
}

function VersionPins({
  databaseIds,
  latestVersions,
  databaseColors,
}: {
  databaseIds: readonly string[]
  latestVersions: ReadonlyMap<string, string>
  databaseColors: Readonly<Record<string, string>>
}) {
  return (
    <div className="flex min-w-0 flex-wrap gap-x-3 gap-y-1.5">
      {databaseIds.map((database) => (
        <div key={database} className="flex min-w-0 items-center gap-2 text-xs">
          <span className="flex min-w-0 items-center gap-2 font-medium text-slate-300">
            <span
              className="h-2 w-2 shrink-0 rounded-full"
              style={{ backgroundColor: databaseColors[database] ?? getSeriesColor(0) }}
            />
            <span className="truncate">{formatDatabaseName(database)}</span>
          </span>
          <span className="shrink-0 text-slate-500">{latestVersions.get(database)}</span>
        </div>
      ))}
    </div>
  )
}

function RankingBars({ rows }: { rows: readonly ComparisonRow[] }) {
  const bestRuntime = Math.min(...rows.map((row) => row.valueMs))
  const slowestRuntime = Math.max(...rows.map((row) => row.valueMs))

  return (
    <div className="space-y-3">
      {rows.map((row, index) => {
        const runtimeWidth = slowestRuntime > 0 ? (row.valueMs / slowestRuntime) * 100 : 0
        return (
          <div key={row.id} className="grid gap-1.5">
            <div className="flex items-center justify-between gap-3">
              <div className="flex min-w-0 items-center gap-2">
                <span className="w-5 shrink-0 text-xs font-semibold text-slate-500">
                  #{index + 1}
                </span>
                <span
                  className="h-2.5 w-2.5 shrink-0 rounded-full"
                  style={{ backgroundColor: row.color }}
                />
                <span className="truncate text-sm font-medium text-slate-100">{row.label}</span>
              </div>
              <span className="shrink-0 text-sm font-semibold text-slate-100">
                {formatRuntime(row.valueMs)}
              </span>
            </div>
            <div
              role="meter"
              aria-label={`${row.label} runtime`}
              aria-valuemin={0}
              aria-valuemax={slowestRuntime}
              aria-valuenow={row.valueMs}
              aria-valuetext={formatRuntime(row.valueMs)}
              className="h-2 overflow-hidden rounded-full bg-surface-primary"
            >
              <div
                className="h-full rounded-full"
                style={{ width: `${Math.max(4, runtimeWidth)}%`, backgroundColor: row.color }}
              />
            </div>
            <div className="flex items-center justify-between gap-2 text-[0.7rem] text-slate-500">
              <span className="truncate">{row.detail}</span>
              <span>
                {index === 0 || bestRuntime <= 0
                  ? "Fastest"
                  : `${(row.valueMs / bestRuntime).toFixed(1)}× slower`}
              </span>
            </div>
          </div>
        )
      })}
    </div>
  )
}

function QueryBreakdown({
  rows,
  queryRows,
  selectedQuery,
  onSelectQuery,
}: {
  rows: readonly ComparisonRow[]
  queryRows: readonly QueryRow[]
  selectedQuery: string
  onSelectQuery: (query: string) => void
}) {
  const values = queryRows
    .flatMap((queryRow) => queryRow.values)
    .filter((value): value is number => value !== null)
  const maxValue = Math.max(...values, 1)

  return (
    <div className="panel-scrollbar overflow-auto xl:min-h-0 xl:flex-1">
      <div
        className="grid min-w-[30rem] gap-1 text-xs sm:min-w-[38rem]"
        style={{
          gridTemplateColumns: `minmax(6.5rem, 8rem) repeat(${rows.length}, minmax(6.25rem, 1fr))`,
        }}
      >
        <div className="sticky top-0 z-10 bg-surface-inset pb-1 text-slate-500">Query</div>
        {rows.map((row) => (
          <div
            key={row.id}
            className="sticky top-0 z-10 min-w-0 bg-surface-inset pb-1 font-medium text-slate-300"
          >
            <span className="block truncate">{row.label}</span>
          </div>
        ))}
        {queryRows.map((queryRow) => (
          <Fragment key={queryRow.query}>
            <div className="py-1">
              <button
                type="button"
                onClick={() => onSelectQuery(queryRow.query)}
                className={cn(
                  "w-full rounded-md px-2 py-1 text-left font-medium transition-colors outline-none",
                  "focus-visible:ring-2 focus-visible:ring-slate-300/20",
                  selectedQuery === queryRow.query
                    ? "bg-surface-raised text-slate-100"
                    : "text-slate-400 hover:bg-surface-raised/70 hover:text-slate-200",
                )}
              >
                {queryRow.query}
              </button>
            </div>
            {queryRow.values.map((value, index) => {
              const row = rows[index]
              if (!row) return null
              const intensity = value === null ? 0 : Math.round(14 + (value / maxValue) * 42)
              return (
                <div
                  key={`${queryRow.query}-${row.id}`}
                  className="rounded-md border border-border-subtle px-2 py-1.5 font-medium text-slate-100"
                  style={{
                    background:
                      value === null
                        ? "rgba(20, 24, 38, 0.5)"
                        : `linear-gradient(90deg, color-mix(in srgb, ${row.color} ${intensity}%, rgba(20, 24, 38, 0.92)), rgba(20, 24, 38, 0.68))`,
                  }}
                >
                  {value === null ? "-" : formatRuntime(value)}
                </div>
              )
            })}
          </Fragment>
        ))}
      </div>
    </div>
  )
}

function SetupSkeleton() {
  return (
    <div className="grid gap-3 md:grid-cols-2 2xl:grid-cols-5">
      {Array.from({ length: 5 }, (_, index) => (
        <div key={index} className="space-y-3 rounded-lg bg-surface-inset p-3">
          <div className="flex items-center gap-3">
            <Skeleton className="h-8 w-8 rounded-lg" />
            <div className="flex-1 space-y-2">
              <Skeleton className="h-4 w-28" />
              <Skeleton className="h-3 w-40" />
            </div>
          </div>
          <Skeleton className="h-9 w-full rounded-full" />
        </div>
      ))}
    </div>
  )
}

function ResultsSkeleton() {
  return (
    <>
      <div className="grid gap-2 sm:grid-cols-3">
        {Array.from({ length: 3 }, (_, index) => (
          <Skeleton key={index} className="h-24 w-full rounded-lg" />
        ))}
      </div>
      <Skeleton className="h-80 w-full rounded-lg" />
    </>
  )
}

function resolveSelection({
  mode,
  variants,
  raw,
  preferredSystem,
  defaultScale,
}: {
  mode: ComparisonMode
  variants: readonly RunVariant[]
  raw: RawSelection
  preferredSystem: string | null
  defaultScale: number
}): ResolvedSelection {
  if (variants.length === 0) return emptySelection()

  const allSystems = unique(variants.map((variant) => variant.system)).sort()
  const allDatabases = unique(variants.map((variant) => variant.database)).sort()

  if (mode === "system") {
    const database = choose(raw.database, allDatabases)
    const versionOptions = unique(
      variants.filter((variant) => variant.database === database).map((variant) => variant.version),
    ).sort()
    const version = choose(raw.version, versionOptions)
    const scaleOptions = unique(
      variants
        .filter((variant) => variant.database === database && variant.version === version)
        .map((variant) => variant.scale),
    ).sort((a, b) => a - b)
    const scale = chooseNumber(raw.scale, scaleOptions, defaultScale)
    const systemOptions = unique(
      variants
        .filter(
          (variant) =>
            variant.database === database && variant.version === version && variant.scale === scale,
        )
        .map((variant) => variant.system),
    ).sort()
    const systems = normalizeSubset(raw.systems, systemOptions, systemOptions)

    return makeResolvedSelection({
      system: choose(raw.system, systemOptions, preferredSystem),
      systems,
      systemOptions,
      database,
      databases: [database],
      databaseOptions: allDatabases,
      scale,
      scales: [scale],
      scaleOptions,
      version,
      versions: [version],
      versionOptions,
      variants,
    })
  }

  const system = choose(raw.system, allSystems, preferredSystem)
  const systemVariants = variants.filter((variant) => variant.system === system)
  const databaseOptions = unique(systemVariants.map((variant) => variant.database)).sort()

  if (mode === "database") {
    const scaleOptions = unique(systemVariants.map((variant) => variant.scale)).sort(
      (a, b) => a - b,
    )
    const scale = chooseNumber(raw.scale, scaleOptions, defaultScale)
    const availableVariants = systemVariants.filter((variant) => variant.scale === scale)
    const availableDatabases = unique(availableVariants.map((variant) => variant.database)).sort()
    const databases = normalizeSubset(raw.databases, availableDatabases, availableDatabases)
    const latestVersions = latestVersionsByDatabase(availableVariants, databases)
    const database = choose(raw.database, databases)
    const version = latestVersions.get(database) ?? ""

    return {
      system,
      systems: [system],
      systemOptions: allSystems,
      database,
      databases,
      databaseOptions: availableDatabases,
      scale,
      scales: [scale],
      scaleOptions,
      version,
      versions: version ? [version] : [],
      versionOptions: version ? [version] : [],
      latestVersions,
      ready: databases.length > 0,
    }
  }

  const database = choose(raw.database, databaseOptions)
  const databaseVariants = systemVariants.filter((variant) => variant.database === database)

  if (mode === "scale") {
    const versionOptions = unique(databaseVariants.map((variant) => variant.version)).sort()
    const version = choose(raw.version, versionOptions)
    const scaleOptions = unique(
      databaseVariants
        .filter((variant) => variant.version === version)
        .map((variant) => variant.scale),
    ).sort((a, b) => a - b)
    const scales = normalizeSubset(raw.scales, scaleOptions, scaleOptions)
    const scale = chooseNumber(raw.scale, scales, defaultScale)

    return makeResolvedSelection({
      system,
      systems: [system],
      systemOptions: allSystems,
      database,
      databases: [database],
      databaseOptions,
      scale,
      scales,
      scaleOptions,
      version,
      versions: [version],
      versionOptions,
      variants,
    })
  }

  const scaleOptions = unique(databaseVariants.map((variant) => variant.scale)).sort(
    (a, b) => a - b,
  )
  const scale = chooseNumber(raw.scale, scaleOptions, defaultScale)
  const versionOptions = unique(
    databaseVariants.filter((variant) => variant.scale === scale).map((variant) => variant.version),
  ).sort()
  const versions = normalizeSubset(raw.versions, versionOptions, versionOptions)
  const version = choose(raw.version, versions)

  return makeResolvedSelection({
    system,
    systems: [system],
    systemOptions: allSystems,
    database,
    databases: [database],
    databaseOptions,
    scale,
    scales: [scale],
    scaleOptions,
    version,
    versions,
    versionOptions,
    variants,
  })
}

function makeResolvedSelection(
  selection: Omit<ResolvedSelection, "latestVersions" | "ready"> & {
    variants: readonly RunVariant[]
  },
): ResolvedSelection {
  const { variants, ...resolved } = selection
  return {
    ...resolved,
    latestVersions: latestVersionsByDatabase(variants, resolved.databases),
    ready: Boolean(resolved.system && resolved.database && resolved.version),
  }
}

function emptySelection(): ResolvedSelection {
  return {
    system: "",
    systems: [],
    systemOptions: [],
    database: "",
    databases: [],
    databaseOptions: [],
    scale: 0,
    scales: [],
    scaleOptions: [],
    version: "",
    versions: [],
    versionOptions: [],
    latestVersions: new Map(),
    ready: false,
  }
}

function buildComparisonRows({
  mode,
  metrics,
  variants,
  selection,
  databaseColors,
}: {
  mode: ComparisonMode
  metrics: readonly ExplorerQueryMetric[]
  variants: readonly RunVariant[]
  selection: ResolvedSelection
  databaseColors: Readonly<Record<string, string>>
}): ComparisonRow[] {
  if (!selection.ready) return []

  const selectedVariants: { variant: RunVariant; label: string; detail: string; color: string }[] =
    []

  if (mode === "database") {
    for (const database of selection.databases) {
      const variant = findLatestVariant(
        variants.filter(
          (candidate) =>
            candidate.system === selection.system &&
            candidate.database === database &&
            candidate.scale === selection.scale,
        ),
      )
      if (variant) {
        selectedVariants.push({
          variant,
          label: formatDatabaseName(database),
          detail: `${variant.version} / SF ${variant.scale}`,
          color: databaseColors[database] ?? getSeriesColor(selectedVariants.length),
        })
      }
    }
  } else if (mode === "scale") {
    for (const scale of selection.scales) {
      const variant = findExactVariant(variants, {
        system: selection.system,
        database: selection.database,
        version: selection.version,
        scale,
      })
      if (variant) {
        selectedVariants.push({
          variant,
          label: `SF ${scale}`,
          detail: `${formatDatabaseName(variant.database)} ${variant.version}`,
          color: getSeriesColor(selectedVariants.length),
        })
      }
    }
  } else if (mode === "version") {
    for (const version of selection.versions) {
      const variant = findExactVariant(variants, {
        system: selection.system,
        database: selection.database,
        version,
        scale: selection.scale,
      })
      if (variant) {
        selectedVariants.push({
          variant,
          label: version,
          detail: `${formatDatabaseName(variant.database)} / SF ${variant.scale}`,
          color: getSeriesColor(selectedVariants.length),
        })
      }
    }
  } else {
    for (const system of selection.systems) {
      const variant = findExactVariant(variants, {
        system,
        database: selection.database,
        version: selection.version,
        scale: selection.scale,
      })
      if (variant) {
        selectedVariants.push({
          variant,
          label: system,
          detail: `${formatDatabaseName(variant.database)} ${variant.version} / SF ${variant.scale}`,
          color: getSeriesColor(selectedVariants.length),
        })
      }
    }
  }

  return selectedVariants.map(({ variant, label, detail, color }) => {
    const queryValues = new Map(
      metrics
        .filter((metric) => metric.run_id === variant.runId)
        .map((metric) => [metric.query_name, metric.median_duration_s * 1000] as const),
    )
    return {
      id: String(variant.runId),
      label,
      detail,
      color,
      valueMs: median([...queryValues.values()]),
      database: variant.database,
      queryValues,
    }
  })
}

function makeRunVariants(metrics: readonly ExplorerQueryMetric[]): RunVariant[] {
  const variants = new Map<number, RunVariant>()
  for (const metric of metrics) {
    variants.set(metric.run_id, {
      runId: metric.run_id,
      system: metric.system,
      scale: metric.suite_scale_factor,
      database: metric.db,
      version: metric.db_version,
      finishedAt: metric.finished_at,
    })
  }
  return [...variants.values()]
}

function findSharedQueries(rows: readonly ComparisonRow[]): string[] {
  const first = rows[0]
  if (!first) return []
  return [...first.queryValues.keys()].filter((query) =>
    rows.every((row) => row.queryValues.has(query)),
  )
}

function orderQueryNames(manifestNames: readonly string[], rows: readonly ComparisonRow[]) {
  const available = new Set(rows.flatMap((row) => [...row.queryValues.keys()]))
  const ordered = manifestNames.filter((query) => available.has(query))
  const unlisted = [...available].filter((query) => !manifestNames.includes(query)).sort()
  return [...ordered, ...unlisted]
}

function makeQueryRows(queryNames: readonly string[], rows: readonly ComparisonRow[]): QueryRow[] {
  return queryNames.map((query) => ({
    query,
    values: rows.map((row) => row.queryValues.get(query) ?? null),
  }))
}

function resolveSql(entry: QuerySqlEntry | undefined, database: string) {
  if (!entry) return "-- SQL is not available for this query."
  return entry.db_overrides[database] ?? entry.sql ?? "-- SQL is not available for this database."
}

function parseUrlState(
  searchParams: URLSearchParams,
  preferredSystem: string | null,
): RawSelection {
  return {
    system: searchParams.get("system") ?? preferredSystem ?? "",
    systems: splitParam(searchParams.get("systems")),
    database: searchParams.get("database") ?? "",
    databases: splitParam(searchParams.get("databases")),
    scale: parseNumber(searchParams.get("scale")),
    scales: splitParam(searchParams.get("scales")).map(Number).filter(Number.isFinite),
    version: searchParams.get("version") ?? "",
    versions: splitParam(searchParams.get("versions")),
  }
}

function parseComparisonMode(value: string | null): ComparisonMode {
  return COMPARISON_MODES.some((comparisonMode) => comparisonMode.id === value)
    ? (value as ComparisonMode)
    : "database"
}

function parseNumber(value: string | null) {
  if (value === null) return null
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : null
}

function splitParam(value: string | null) {
  return (
    value
      ?.split(",")
      .map((item) => item.trim())
      .filter(Boolean) ?? []
  )
}

function choose(value: string, options: readonly string[], alternate?: string | null) {
  if (options.includes(value)) return value
  if (alternate && options.includes(alternate)) return alternate
  return options[0] ?? ""
}

function chooseNumber(value: number | null, options: readonly number[], alternate?: number) {
  if (value !== null && options.includes(value)) return value
  if (alternate !== undefined && options.includes(alternate)) return alternate
  return options[0] ?? 0
}

function normalizeSubset<T>(values: readonly T[], options: readonly T[], fallback: readonly T[]) {
  const selected = options.filter((option) => values.includes(option))
  return selected.length > 0 ? selected : [...fallback]
}

function latestVersionsByDatabase(variants: readonly RunVariant[], databases: readonly string[]) {
  return new Map(
    databases.flatMap((database) => {
      const latest = findLatestVariant(variants.filter((variant) => variant.database === database))
      return latest ? [[database, latest.version] as const] : []
    }),
  )
}

function findLatestVariant(variants: readonly RunVariant[]) {
  return [...variants].sort((a, b) => b.finishedAt.localeCompare(a.finishedAt))[0]
}

function findExactVariant(
  variants: readonly RunVariant[],
  selection: Pick<RunVariant, "system" | "database" | "version" | "scale">,
) {
  return findLatestVariant(
    variants.filter(
      (variant) =>
        variant.system === selection.system &&
        variant.database === selection.database &&
        variant.version === selection.version &&
        variant.scale === selection.scale,
    ),
  )
}

function toggleSelection<T>(values: readonly T[], value: T): T[] {
  if (values.includes(value)) {
    return values.length <= 1 ? [...values] : values.filter((candidate) => candidate !== value)
  }
  return [...values, value]
}

function unique<T>(values: readonly T[]) {
  return [...new Set(values)]
}

function median(values: readonly number[]) {
  if (values.length === 0) return 0
  const sorted = [...values].sort((a, b) => a - b)
  const middle = Math.floor(sorted.length / 2)
  const upper = sorted[middle] ?? 0
  return sorted.length % 2 === 0 ? ((sorted[middle - 1] ?? upper) + upper) / 2 : upper
}

function getModeLabel(mode: ComparisonMode) {
  const selectedMode = COMPARISON_MODES.find((comparisonMode) => comparisonMode.id === mode)
  return `${selectedMode?.label ?? "Database"} comparison`
}

function getComparisonDescription(
  mode: ComparisonMode,
  suiteTitle: string,
  selection: ResolvedSelection,
  seriesCount: number,
) {
  if (mode === "database") {
    return `Comparing ${seriesCount} databases on ${selection.system} for ${suiteTitle} at SF ${selection.scale}. Each database uses its latest completed version.`
  }
  if (mode === "scale") {
    return `Comparing ${seriesCount} scale factors for ${formatDatabaseName(selection.database)} ${selection.version} on ${selection.system}.`
  }
  if (mode === "version") {
    return `Comparing ${seriesCount} versions of ${formatDatabaseName(selection.database)} on ${selection.system} for ${suiteTitle} at SF ${selection.scale}.`
  }
  return `Comparing ${seriesCount} systems running ${formatDatabaseName(selection.database)} ${selection.version} for ${suiteTitle} at SF ${selection.scale}.`
}

function formatRuntime(valueMs: number) {
  if (valueMs >= 1000) return `${(valueMs / 1000).toFixed(2)}s`
  if (valueMs >= 100) return `${Math.round(valueMs)}ms`
  if (valueMs >= 10) return `${valueMs.toFixed(1)}ms`
  return `${valueMs.toFixed(2)}ms`
}

function getSeriesColor(index: number) {
  return SERIES_COLORS[index % SERIES_COLORS.length] ?? SERIES_COLORS[0]
}

function formatDatabaseName(database: string) {
  const labels: Record<string, string> = {
    clickhouse: "ClickHouse",
    datafusion: "DataFusion",
    duckdb: "DuckDB",
    monetdb: "MonetDB",
    postgres: "PostgreSQL",
    questdb: "QuestDB",
    starrocks: "StarRocks",
    timescaledb: "TimescaleDB",
  }
  return labels[database] ?? database
}
