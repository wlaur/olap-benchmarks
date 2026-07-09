import {
  Database,
  FlaskConical,
  Gauge,
  GitBranch,
  Scale,
  Server,
  SlidersHorizontal,
  type LucideIcon,
} from "lucide-react"
import { Fragment, useEffect, useMemo, useState, type ReactNode } from "react"
import { useSearchParams } from "react-router-dom"

import { ControlChip, SegmentedButton } from "../components/controls/Control"
import { ControlSelect } from "../components/controls/ControlSelect"
import { ChartFrame, PanelCard, PanelHeader } from "../components/layout/Panel"
import { SqlCodeView } from "../components/SqlCodeView"
import { BodyText, MetaLabel, SectionTitle } from "../components/Typography"
import { cn } from "../lib/cn"

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

const SYSTEMS = [
  {
    id: "m3_max",
    label: "M3 Max",
    detail: "14c CPU / 96 GB",
    color: "#5eead4",
    factor: 1,
  },
  {
    id: "epyc_9754",
    label: "EPYC 9754",
    detail: "128c CPU / 512 GB",
    color: "#93c5fd",
    factor: 0.68,
  },
  {
    id: "c7i_4xlarge",
    label: "c7i.4xlarge",
    detail: "16 vCPU / 32 GB",
    color: "#fbbf24",
    factor: 1.18,
  },
] as const

const SUITES = [
  {
    id: "clickbench",
    label: "ClickBench",
    detail: "wide analytical queries",
    color: "#5eead4",
    scales: [10, 100, 1000],
    factor: 1,
  },
  {
    id: "tpc_h",
    label: "TPC-H",
    detail: "decision support",
    color: "#fbbf24",
    scales: [1, 10, 100],
    factor: 1.22,
  },
  {
    id: "tpc_ds",
    label: "TPC-DS",
    detail: "retail analytics",
    color: "#fb7185",
    scales: [10, 100, 1000],
    factor: 1.44,
  },
] as const

const DATABASES = [
  {
    id: "clickhouse",
    label: "ClickHouse",
    detail: "column store",
    color: "#facc15",
    baseMs: 56,
    versions: ["25.8.2", "25.3.5", "24.12.1"],
  },
  {
    id: "duckdb",
    label: "DuckDB",
    detail: "embedded engine",
    color: "#38bdf8",
    baseMs: 72,
    versions: ["1.4.2", "1.4.0", "1.3.2"],
  },
  {
    id: "doris",
    label: "Apache Doris",
    detail: "distributed MPP",
    color: "#fb7185",
    baseMs: 92,
    versions: ["3.1.0", "3.0.7", "2.1.9"],
  },
  {
    id: "datafusion",
    label: "DataFusion",
    detail: "Rust query engine",
    color: "#a7f3d0",
    baseMs: 118,
    versions: ["50.2.0", "49.0.2", "48.0.1"],
  },
] as const

const SCALE_COLORS = ["#5eead4", "#93c5fd", "#fbbf24", "#fb7185", "#c4b5fd"] as const

const QUERY_NAMES = ["Q01 scan", "Q04 filters", "Q13 group by", "Q22 join", "Q31 sort"] as const

type QueryName = (typeof QUERY_NAMES)[number]

const DEFAULT_QUERY_NAME: QueryName = "Q01 scan"

const QUERY_SQL: Record<QueryName, string> = {
  "Q01 scan": `SELECT
  toStartOfHour(created_at) AS hour,
  count(*) AS events,
  avg(duration_ms) AS avg_duration_ms
FROM benchmark_events
WHERE created_at >= TIMESTAMP '2026-01-01 00:00:00'
  AND created_at < TIMESTAMP '2026-01-08 00:00:00'
GROUP BY hour
ORDER BY hour;`,
  "Q04 filters": `SELECT
  region,
  device_type,
  count(*) AS sessions,
  quantile(0.95)(latency_ms) AS p95_latency_ms
FROM user_sessions
WHERE status = 'complete'
  AND device_type IN ('desktop', 'mobile')
  AND latency_ms BETWEEN 1 AND 30000
GROUP BY region, device_type
ORDER BY sessions DESC
LIMIT 50;`,
  "Q13 group by": `SELECT
  customer_segment,
  product_category,
  sum(order_total) AS revenue,
  count(DISTINCT customer_id) AS customers
FROM fact_orders
WHERE order_date >= DATE '2026-01-01'
GROUP BY customer_segment, product_category
HAVING revenue > 100000
ORDER BY revenue DESC;`,
  "Q22 join": `SELECT
  c.country,
  p.brand,
  sum(l.extended_price * (1 - l.discount)) AS net_revenue
FROM lineitem l
JOIN customer c ON c.customer_id = l.customer_id
JOIN product p ON p.product_id = l.product_id
WHERE l.ship_date BETWEEN DATE '2026-01-01' AND DATE '2026-03-31'
GROUP BY c.country, p.brand
ORDER BY net_revenue DESC
LIMIT 100;`,
  "Q31 sort": `SELECT
  query_id,
  database_name,
  median(runtime_ms) AS median_runtime_ms,
  max(memory_mb) AS peak_memory_mb
FROM benchmark_query_runs
WHERE suite = 'clickbench'
  AND scale_factor = 100
GROUP BY query_id, database_name
ORDER BY median_runtime_ms ASC, peak_memory_mb ASC;`,
}

type SystemId = (typeof SYSTEMS)[number]["id"]
type SuiteId = (typeof SUITES)[number]["id"]
type DatabaseId = (typeof DATABASES)[number]["id"]

interface ComparisonRow {
  id: string
  label: string
  detail: string
  color: string
  valueMs: number
}

interface DimensionContract {
  label: string
  role: "fixed" | "varies"
  value: string
}

interface PrototypeUrlState {
  mode: ComparisonMode
  systemId: SystemId
  selectedSystems: SystemId[]
  suiteId: SuiteId
  selectedSuites: SuiteId[]
  databaseId: DatabaseId
  selectedDatabases: DatabaseId[]
  scaleFactor: number
  selectedScaleFactors: number[]
  databaseVersion: string
  selectedVersions: string[]
  selectedQuery: QueryName
}

const DEFAULT_SELECTED_SYSTEMS: SystemId[] = ["m3_max", "epyc_9754"]
const DEFAULT_SELECTED_SUITES: SuiteId[] = ["clickbench"]
const DEFAULT_SELECTED_DATABASES: DatabaseId[] = ["clickhouse", "duckdb", "doris"]

function parsePrototypeUrlState(searchParams: URLSearchParams): PrototypeUrlState {
  const mode = parseComparisonMode(searchParams.get("mode"))
  const selectedSystems = parseIdSubset(
    searchParams.get("systems"),
    SYSTEMS,
    DEFAULT_SELECTED_SYSTEMS,
  )
  const systemId = parseIdValue(searchParams.get("system"), SYSTEMS, firstValue(selectedSystems))
  const selectedSuites = parseIdSubset(searchParams.get("suites"), SUITES, DEFAULT_SELECTED_SUITES)
  const suiteId = parseIdValue(searchParams.get("suite"), SUITES, firstValue(selectedSuites))
  const suite = getItem(SUITES, suiteId)
  const selectedDatabases = parseIdSubset(
    searchParams.get("databases"),
    DATABASES,
    DEFAULT_SELECTED_DATABASES,
  )
  const databaseId = parseIdValue(
    searchParams.get("database"),
    DATABASES,
    firstValue(selectedDatabases),
  )
  const database = getItem(DATABASES, databaseId)
  const selectedScaleFactors = parseNumberSubset(searchParams.get("scales"), suite.scales, [
    ...suite.scales,
  ])
  const scaleFactor = parseNumberValue(
    searchParams.get("scale"),
    suite.scales,
    firstValue(selectedScaleFactors),
  )
  const selectedVersions = parseStringSubset(searchParams.get("versions"), database.versions, [
    ...database.versions,
  ])
  const databaseVersion = parseStringValue(
    searchParams.get("version"),
    database.versions,
    firstValue(selectedVersions),
  )
  const selectedQuery = parseQueryName(searchParams.get("query"))

  return {
    mode,
    systemId,
    selectedSystems: ensureSelected(selectedSystems, systemId),
    suiteId,
    selectedSuites: ensureSelected(selectedSuites, suiteId),
    databaseId,
    selectedDatabases: ensureSelected(selectedDatabases, databaseId),
    scaleFactor,
    selectedScaleFactors: ensureSelected(selectedScaleFactors, scaleFactor),
    databaseVersion,
    selectedVersions: ensureSelected(selectedVersions, databaseVersion),
    selectedQuery,
  }
}

export function ExplorerPrototypePage() {
  const [searchParams, setSearchParams] = useSearchParams()
  const [initialState] = useState(() => parsePrototypeUrlState(searchParams))
  const [mode, setMode] = useState<ComparisonMode>(initialState.mode)
  const [systemId, setSystemId] = useState<SystemId>(initialState.systemId)
  const [suiteId, setSuiteId] = useState<SuiteId>(initialState.suiteId)
  const [databaseId, setDatabaseId] = useState<DatabaseId>(initialState.databaseId)
  const [scaleFactor, setScaleFactor] = useState(initialState.scaleFactor)
  const [databaseVersion, setDatabaseVersion] = useState(initialState.databaseVersion)
  const [selectedSystems, setSelectedSystems] = useState<SystemId[]>(initialState.selectedSystems)
  const [selectedSuites, setSelectedSuites] = useState<SuiteId[]>(initialState.selectedSuites)
  const [selectedDatabases, setSelectedDatabases] = useState<DatabaseId[]>(
    initialState.selectedDatabases,
  )
  const [selectedScaleFactors, setSelectedScaleFactors] = useState<number[]>(
    initialState.selectedScaleFactors,
  )
  const [selectedVersions, setSelectedVersions] = useState<string[]>(initialState.selectedVersions)
  const [selectedQuery, setSelectedQuery] = useState<QueryName>(initialState.selectedQuery)

  const selectedSystem = getItem(SYSTEMS, systemId)
  const selectedSuite = getItem(SUITES, suiteId)
  const selectedDatabase = getItem(DATABASES, databaseId)
  const availableScales = selectedSuite.scales
  const availableVersions = selectedDatabase.versions
  const normalizedVersion = includesString(availableVersions, databaseVersion)
    ? databaseVersion
    : firstValue(availableVersions)
  const normalizedScaleFactor = includesNumber(availableScales, scaleFactor)
    ? scaleFactor
    : firstValue(availableScales)
  const activeScaleFactors = useMemo(
    () => selectedScaleFactors.filter((scale) => includesNumber(availableScales, scale)),
    [availableScales, selectedScaleFactors],
  )
  const activeVersions = useMemo(
    () => selectedVersions.filter((version) => includesString(availableVersions, version)),
    [availableVersions, selectedVersions],
  )

  const comparisonRows = useMemo(
    () =>
      makeComparisonRows({
        mode,
        systemId,
        suiteId,
        databaseId,
        scaleFactor: normalizedScaleFactor,
        databaseVersion: normalizedVersion,
        selectedSystems,
        selectedDatabases,
        selectedScaleFactors: activeScaleFactors,
        selectedVersions: activeVersions,
      }),
    [
      activeScaleFactors,
      activeVersions,
      databaseId,
      mode,
      normalizedScaleFactor,
      normalizedVersion,
      selectedDatabases,
      selectedSystems,
      suiteId,
      systemId,
    ],
  )

  const rankedRows = useMemo(
    () => [...comparisonRows].sort((a, b) => a.valueMs - b.valueMs),
    [comparisonRows],
  )
  const queryRows = useMemo(() => makeQueryRows(rankedRows), [rankedRows])
  const dimensionContract = useMemo(
    () =>
      makeDimensionContract({
        mode,
        selectedSystem,
        selectedSuite,
        selectedDatabase,
        normalizedScaleFactor,
        normalizedVersion,
        selectedSystems,
        selectedSuites,
        selectedDatabases,
        selectedScaleFactors: activeScaleFactors,
        selectedVersions: activeVersions,
      }),
    [
      activeScaleFactors,
      activeVersions,
      mode,
      normalizedScaleFactor,
      normalizedVersion,
      selectedDatabase,
      selectedDatabases,
      selectedSuite,
      selectedSuites,
      selectedSystem,
      selectedSystems,
    ],
  )

  const nextSearchParams = useMemo(() => {
    const params = new URLSearchParams()
    params.set("mode", mode)
    params.set("system", systemId)
    params.set("systems", selectedSystems.join(","))
    params.set("suite", suiteId)
    params.set("suites", selectedSuites.join(","))
    params.set("database", databaseId)
    params.set("databases", selectedDatabases.join(","))
    params.set("scale", String(normalizedScaleFactor))
    params.set("scales", activeScaleFactors.join(","))
    params.set("version", normalizedVersion)
    params.set("versions", activeVersions.join(","))
    params.set("query", selectedQuery)
    return params.toString()
  }, [
    activeScaleFactors,
    activeVersions,
    databaseId,
    mode,
    normalizedScaleFactor,
    normalizedVersion,
    selectedDatabases,
    selectedQuery,
    selectedSystems,
    selectedSuites,
    suiteId,
    systemId,
  ])

  useEffect(() => {
    if (searchParams.toString() !== nextSearchParams) {
      setSearchParams(nextSearchParams, { replace: true })
    }
  }, [nextSearchParams, searchParams, setSearchParams])

  function handleModeChange(nextMode: ComparisonMode) {
    setMode(nextMode)
    if (nextMode === "system" && selectedSystems.length === 0) {
      setSelectedSystems(["m3_max", "epyc_9754"])
    }
    if (nextMode === "database" && selectedDatabases.length === 0) {
      setSelectedDatabases(["clickhouse", "duckdb"])
    }
    if (nextMode === "scale" && selectedScaleFactors.length === 0) {
      setSelectedScaleFactors([firstValue(availableScales)])
    }
    if (nextMode === "version" && selectedVersions.length === 0) {
      setSelectedVersions([firstValue(availableVersions)])
    }
  }

  function handleSuiteChange(nextSuiteId: SuiteId) {
    const nextSuite = getItem(SUITES, nextSuiteId)
    setSuiteId(nextSuiteId)
    setSelectedSuites((current) =>
      current.includes(nextSuiteId) ? current : [nextSuiteId, ...current],
    )
    setScaleFactor((current) =>
      includesNumber(nextSuite.scales, current) ? current : firstValue(nextSuite.scales),
    )
    setSelectedScaleFactors((current) => normalizeNumericSelection(current, nextSuite.scales))
  }

  function handleDatabaseChange(nextDatabaseId: DatabaseId) {
    const nextDatabase = getItem(DATABASES, nextDatabaseId)
    setDatabaseId(nextDatabaseId)
    setDatabaseVersion((current) =>
      includesString(nextDatabase.versions, current) ? current : firstValue(nextDatabase.versions),
    )
    setSelectedVersions((current) => normalizeStringSelection(current, nextDatabase.versions))
  }

  function handleSuiteToggle(nextSuiteId: SuiteId) {
    const nextSuites = toggleSelection(selectedSuites, nextSuiteId)
    setSelectedSuites(nextSuites)
    if (!nextSuites.includes(suiteId)) {
      handleSuiteChange(firstValue(nextSuites))
    }
  }

  return (
    <div className="flex min-h-full w-full max-w-full min-w-0 flex-col gap-4 overflow-x-hidden pb-5">
      <header className="flex flex-wrap items-end justify-between gap-3">
        <div className="min-w-0">
          <MetaLabel>Prototype</MetaLabel>
          <h2 className="mt-1 text-2xl font-semibold text-slate-50">Explore dimensions</h2>
        </div>
        <div className="grid w-full min-w-0 grid-cols-[repeat(2,minmax(0,1fr))] gap-2 sm:w-auto sm:grid-cols-4">
          {COMPARISON_MODES.map((comparisonMode) => {
            const Icon = comparisonMode.icon
            return (
              <SegmentedButton
                key={comparisonMode.id}
                selected={mode === comparisonMode.id}
                onClick={() => handleModeChange(comparisonMode.id)}
                size="md"
                className="min-h-10 min-w-0 gap-1.5 rounded-lg px-2 text-xs sm:gap-2 sm:px-3 sm:text-sm"
              >
                <Icon className="h-4 w-4 shrink-0" strokeWidth={1.8} />
                <span className="truncate">{comparisonMode.label}</span>
              </SegmentedButton>
            )
          })}
        </div>
      </header>

      <div className="grid max-w-full min-w-0 gap-4 xl:min-h-0 xl:flex-1 xl:grid-cols-[minmax(20rem,24rem)_minmax(0,1fr)]">
        <PanelCard className="flex flex-col gap-4 p-2.5 sm:p-3 xl:max-h-[min(48rem,calc(100dvh-11rem))] xl:p-4">
          <PanelHeader>
            <div>
              <SectionTitle as="h3">Comparison setup</SectionTitle>
              <BodyText className="mt-1 text-xs">
                One benchmark dimension varies at a time.
              </BodyText>
            </div>
            <SlidersHorizontal className="h-4 w-4 text-slate-500" strokeWidth={1.8} />
          </PanelHeader>

          <div className="panel-scrollbar space-y-3 overflow-visible xl:min-h-0 xl:overflow-y-auto xl:pr-1">
            <DimensionRow
              icon={Server}
              label="System"
              role={mode === "system" ? "varies" : "fixed"}
              summary={
                mode === "system"
                  ? `${selectedSystems.length} selected`
                  : `${selectedSystem.label} · ${selectedSystem.detail}`
              }
            >
              {mode === "system" ? (
                <ChoiceChips
                  options={SYSTEMS}
                  selectedValues={selectedSystems}
                  onToggle={(value) =>
                    setSelectedSystems((current) => toggleSelection(current, value))
                  }
                />
              ) : (
                <DimensionSelect
                  ariaLabel="System"
                  label="System"
                  icon={Server}
                  value={systemId}
                  options={SYSTEMS}
                  onChange={(value) => setSystemId(value as SystemId)}
                />
              )}
            </DimensionRow>

            <DimensionRow
              icon={Database}
              label="Database"
              role={mode === "database" ? "varies" : "fixed"}
              summary={
                mode === "database"
                  ? `${selectedDatabases.length} selected`
                  : `${selectedDatabase.label} · ${selectedDatabase.detail}`
              }
            >
              {mode === "database" ? (
                <ChoiceChips
                  options={DATABASES}
                  selectedValues={selectedDatabases}
                  onToggle={(value) =>
                    setSelectedDatabases((current) => toggleSelection(current, value))
                  }
                />
              ) : (
                <DimensionSelect
                  ariaLabel="Database"
                  label="Database"
                  icon={Database}
                  value={databaseId}
                  options={DATABASES}
                  onChange={(value) => handleDatabaseChange(value as DatabaseId)}
                />
              )}
            </DimensionRow>

            <DimensionRow
              icon={GitBranch}
              label="Database version"
              role={mode === "version" ? "varies" : "fixed"}
              summary={
                mode === "version"
                  ? `${selectedVersions.length} selected`
                  : mode === "database"
                    ? "Latest completed per database"
                    : normalizedVersion
              }
            >
              {mode === "version" ? (
                <ChoiceChips
                  options={availableVersions.map((version, index) => ({
                    id: version,
                    label: version,
                    detail: index === 0 ? "current" : "previous",
                    color: getScaleColor(index),
                  }))}
                  selectedValues={selectedVersions}
                  onToggle={(value) =>
                    setSelectedVersions((current) => toggleSelection(current, value))
                  }
                />
              ) : mode === "database" ? (
                <VersionPins databaseIds={selectedDatabases} />
              ) : (
                <DimensionSelect
                  ariaLabel="Database version"
                  label="Version"
                  icon={GitBranch}
                  value={normalizedVersion}
                  options={availableVersions.map((version, index) => ({
                    id: version,
                    label: version,
                    detail: index === 0 ? "current" : "previous",
                    color: getScaleColor(index),
                  }))}
                  onChange={setDatabaseVersion}
                />
              )}
            </DimensionRow>

            <DimensionRow
              icon={FlaskConical}
              label="Suite"
              role="fixed"
              summary={`${selectedSuite.label} · ${selectedSuites.length} linked`}
            >
              <div className="space-y-2">
                <ChoiceChips
                  options={SUITES}
                  selectedValues={selectedSuites}
                  onToggle={(value) => handleSuiteToggle(value)}
                />
                {selectedSuites.length > 1 ? (
                  <DimensionSelect
                    ariaLabel="Active suite"
                    label="Active"
                    icon={FlaskConical}
                    value={suiteId}
                    options={SUITES.filter((suite) => selectedSuites.includes(suite.id))}
                    onChange={(value) => handleSuiteChange(value as SuiteId)}
                  />
                ) : null}
              </div>
            </DimensionRow>

            <DimensionRow
              icon={Scale}
              label="Scale factor"
              role={mode === "scale" ? "varies" : "fixed"}
              summary={
                mode === "scale"
                  ? `${activeScaleFactors.length} selected`
                  : `SF ${normalizedScaleFactor}`
              }
            >
              {mode === "scale" ? (
                <ChoiceChips
                  options={availableScales.map((scale, index) => ({
                    id: scale,
                    label: `SF ${scale}`,
                    detail: "rows",
                    color: getScaleColor(index),
                  }))}
                  selectedValues={selectedScaleFactors}
                  onToggle={(value) =>
                    setSelectedScaleFactors((current) => toggleSelection(current, value))
                  }
                />
              ) : (
                <DimensionSelect
                  ariaLabel="Scale factor"
                  label="Scale"
                  icon={Scale}
                  value={String(normalizedScaleFactor)}
                  options={availableScales.map((scale, index) => ({
                    id: String(scale),
                    label: `SF ${scale}`,
                    detail: "rows",
                    color: getScaleColor(index),
                  }))}
                  onChange={(value) => setScaleFactor(Number(value))}
                />
              )}
            </DimensionRow>
          </div>
        </PanelCard>

        <PanelCard className="flex min-w-0 flex-col gap-4 p-2.5 sm:p-3 xl:grid xl:h-[min(42rem,calc(100dvh-11rem))] xl:min-h-0 xl:grid-rows-[auto_minmax(0,1fr)] xl:p-4">
          <PanelHeader className="flex-wrap">
            <div>
              <MetaLabel>Active comparison</MetaLabel>
              <SectionTitle as="h3" className="mt-1">
                {getModeLabel(mode)}
              </SectionTitle>
            </div>
            <div className="flex items-center gap-2 rounded-full border border-border-default bg-surface-inset px-3 py-1.5 text-xs font-medium text-slate-300">
              <Gauge className="h-3.5 w-3.5 text-accent-300" strokeWidth={1.8} />
              Lower runtime is better
            </div>
          </PanelHeader>

          <div className="grid gap-4 xl:min-h-0 xl:grid-cols-[minmax(0,1fr)_minmax(18rem,0.42fr)]">
            <div className="grid gap-4 xl:min-h-0 xl:grid-rows-[auto_minmax(0,1fr)]">
              <ChartFrame className="space-y-3 p-2.5 sm:p-3">
                <div className="flex items-center justify-between gap-3">
                  <div>
                    <MetaLabel>Ranking</MetaLabel>
                    <p className="mt-1 text-sm font-medium text-slate-200">Median query runtime</p>
                  </div>
                </div>
                <RankingBars rows={rankedRows} />
              </ChartFrame>

              <ChartFrame className="flex min-h-0 flex-col overflow-hidden p-2.5 sm:p-3">
                <div className="mb-3 flex shrink-0 items-center justify-between gap-3">
                  <div>
                    <MetaLabel>Query breakdown</MetaLabel>
                    <p className="mt-1 text-sm font-medium text-slate-200">
                      Runtime by representative query
                    </p>
                  </div>
                </div>
                <QueryBreakdown
                  rows={rankedRows}
                  queryRows={queryRows}
                  selectedQuery={selectedQuery}
                  onSelectQuery={setSelectedQuery}
                />
              </ChartFrame>
            </div>

            <div className="grid gap-4 xl:min-h-0 xl:grid-rows-[auto_minmax(0,1fr)]">
              <ChartFrame className="space-y-3 p-2.5 sm:p-3">
                <MetaLabel>Dimensions</MetaLabel>
                <DimensionContractList contract={dimensionContract} />
              </ChartFrame>

              <ChartFrame className="flex min-h-[18rem] flex-col overflow-hidden p-2.5 sm:p-3 xl:min-h-0">
                <div className="mb-3 shrink-0">
                  <MetaLabel>Query SQL</MetaLabel>
                  <p className="mt-1 truncate text-sm font-medium text-slate-200">
                    {selectedQuery}
                  </p>
                </div>
                <div className="min-h-0 flex-1 overflow-hidden rounded-lg border border-border-subtle bg-surface-primary/40">
                  <SqlCodeView
                    code={QUERY_SQL[selectedQuery]}
                    wrapLines
                    className="h-[18rem] text-xs leading-relaxed xl:h-full"
                  />
                </div>
              </ChartFrame>
            </div>
          </div>
        </PanelCard>
      </div>
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
    <section className="min-w-0 rounded-lg border border-border-subtle bg-surface-inset/55 p-2.5 sm:p-3">
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
      <div className="mt-3 min-w-0">{children}</div>
    </section>
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
  options: readonly {
    id: T
    label: string
    detail?: string
    color: string
  }[]
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

function VersionPins({ databaseIds }: { databaseIds: readonly DatabaseId[] }) {
  return (
    <div className="space-y-2">
      {databaseIds.map((databaseId) => {
        const database = getItem(DATABASES, databaseId)
        return (
          <div
            key={database.id}
            className="flex min-w-0 items-center justify-between gap-2 rounded-lg border border-border-subtle bg-surface-primary/40 px-2.5 py-2 sm:gap-3 sm:px-3"
          >
            <span className="flex min-w-0 items-center gap-2 text-xs font-medium text-slate-300">
              <span
                className="h-2 w-2 shrink-0 rounded-full"
                style={{ backgroundColor: database.color }}
              />
              <span className="truncate">{database.label}</span>
            </span>
            <span className="shrink-0 text-xs text-slate-400">{database.versions[0]}</span>
          </div>
        )
      })}
    </div>
  )
}

function RankingBars({ rows }: { rows: readonly ComparisonRow[] }) {
  const bestRuntime = Math.min(...rows.map((row) => row.valueMs))

  return (
    <div className="space-y-3">
      {rows.map((row, index) => {
        const speed = Math.round((bestRuntime / row.valueMs) * 100)
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
            <div className="h-2 overflow-hidden rounded-full bg-surface-primary">
              <div
                className="h-full rounded-full"
                style={{
                  width: `${Math.max(8, speed)}%`,
                  backgroundColor: row.color,
                }}
              />
            </div>
            <div className="flex items-center justify-between gap-2 text-[0.7rem] text-slate-500">
              <span className="truncate">{row.detail}</span>
              <span>{speed} speed index</span>
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
  queryRows: readonly { query: QueryName; values: number[] }[]
  selectedQuery: QueryName
  onSelectQuery: (query: QueryName) => void
}) {
  const maxValue = Math.max(...queryRows.flatMap((queryRow) => queryRow.values))

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
            <div key={`${queryRow.query}-label`} className="py-1">
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
              if (row === undefined) return null
              const intensity = Math.round(14 + (value / maxValue) * 42)
              return (
                <div
                  key={`${queryRow.query}-${row.id}`}
                  className="rounded-md border border-border-subtle px-2 py-1.5 font-medium text-slate-100"
                  style={{
                    background: `linear-gradient(90deg, color-mix(in srgb, ${row.color} ${intensity}%, rgba(20, 24, 38, 0.92)), rgba(20, 24, 38, 0.68))`,
                  }}
                >
                  {formatRuntime(value)}
                </div>
              )
            })}
          </Fragment>
        ))}
      </div>
    </div>
  )
}

function DimensionContractList({ contract }: { contract: readonly DimensionContract[] }) {
  return (
    <div className="space-y-2">
      {contract.map((dimension) => (
        <div key={dimension.label} className="grid gap-1">
          <div className="flex items-center justify-between gap-2">
            <span className="text-xs font-medium text-slate-300">{dimension.label}</span>
            <RoleBadge role={dimension.role} />
          </div>
          <p className="truncate text-xs text-slate-500">{dimension.value}</p>
        </div>
      ))}
    </div>
  )
}

function makeComparisonRows({
  mode,
  systemId,
  suiteId,
  databaseId,
  scaleFactor,
  databaseVersion,
  selectedSystems,
  selectedDatabases,
  selectedScaleFactors,
  selectedVersions,
}: {
  mode: ComparisonMode
  systemId: SystemId
  suiteId: SuiteId
  databaseId: DatabaseId
  scaleFactor: number
  databaseVersion: string
  selectedSystems: readonly SystemId[]
  selectedDatabases: readonly DatabaseId[]
  selectedScaleFactors: readonly number[]
  selectedVersions: readonly string[]
}): ComparisonRow[] {
  if (mode === "database") {
    return selectedDatabases.map((selectedDatabaseId) => {
      const database = getItem(DATABASES, selectedDatabaseId)
      const version = database.versions[0]
      return {
        id: database.id,
        label: database.label,
        detail: `${version} · SF ${scaleFactor}`,
        color: database.color,
        valueMs: estimateRuntimeMs({
          systemId,
          suiteId,
          databaseId: selectedDatabaseId,
          scaleFactor,
          databaseVersion: version,
        }),
      }
    })
  }

  if (mode === "scale") {
    return selectedScaleFactors.map((selectedScaleFactor, index) => ({
      id: `sf-${selectedScaleFactor}`,
      label: `SF ${selectedScaleFactor}`,
      detail: `${getItem(DATABASES, databaseId).label} ${databaseVersion}`,
      color: getScaleColor(index),
      valueMs: estimateRuntimeMs({
        systemId,
        suiteId,
        databaseId,
        scaleFactor: selectedScaleFactor,
        databaseVersion,
      }),
    }))
  }

  if (mode === "version") {
    const database = getItem(DATABASES, databaseId)
    return selectedVersions.map((version, index) => ({
      id: version,
      label: version,
      detail: `${database.label} · SF ${scaleFactor}`,
      color: getScaleColor(index),
      valueMs: estimateRuntimeMs({
        systemId,
        suiteId,
        databaseId,
        scaleFactor,
        databaseVersion: version,
      }),
    }))
  }

  return selectedSystems.map((selectedSystemId) => {
    const system = getItem(SYSTEMS, selectedSystemId)
    return {
      id: system.id,
      label: system.label,
      detail: `${getItem(DATABASES, databaseId).label} ${databaseVersion} · SF ${scaleFactor}`,
      color: system.color,
      valueMs: estimateRuntimeMs({
        systemId: selectedSystemId,
        suiteId,
        databaseId,
        scaleFactor,
        databaseVersion,
      }),
    }
  })
}

function makeQueryRows(rows: readonly ComparisonRow[]) {
  return QUERY_NAMES.map((query, queryIndex) => ({
    query,
    values: rows.map((row, rowIndex) => {
      const shape = 0.72 + (((queryIndex + 2) * (rowIndex + 3)) % 6) * 0.085
      return Math.round(row.valueMs * shape)
    }),
  }))
}

function makeDimensionContract({
  mode,
  selectedSystem,
  selectedSuite,
  selectedDatabase,
  normalizedScaleFactor,
  normalizedVersion,
  selectedSystems,
  selectedSuites,
  selectedDatabases,
  selectedScaleFactors,
  selectedVersions,
}: {
  mode: ComparisonMode
  selectedSystem: (typeof SYSTEMS)[number]
  selectedSuite: (typeof SUITES)[number]
  selectedDatabase: (typeof DATABASES)[number]
  normalizedScaleFactor: number
  normalizedVersion: string
  selectedSystems: readonly SystemId[]
  selectedSuites: readonly SuiteId[]
  selectedDatabases: readonly DatabaseId[]
  selectedScaleFactors: readonly number[]
  selectedVersions: readonly string[]
}): DimensionContract[] {
  return [
    {
      label: "System",
      role: mode === "system" ? "varies" : "fixed",
      value:
        mode === "system"
          ? selectedSystems.map((id) => getItem(SYSTEMS, id).label).join(", ")
          : selectedSystem.label,
    },
    {
      label: "Database",
      role: mode === "database" ? "varies" : "fixed",
      value:
        mode === "database"
          ? selectedDatabases.map((id) => getItem(DATABASES, id).label).join(", ")
          : selectedDatabase.label,
    },
    {
      label: "Database version",
      role: mode === "version" ? "varies" : "fixed",
      value:
        mode === "version"
          ? selectedVersions.join(", ")
          : mode === "database"
            ? "latest completed per database"
            : normalizedVersion,
    },
    {
      label: "Suite",
      role: "fixed",
      value:
        selectedSuites.length > 1
          ? `${selectedSuite.label} active · ${selectedSuites.map((id) => getItem(SUITES, id).label).join(", ")} linked`
          : selectedSuite.label,
    },
    {
      label: "Scale factor",
      role: mode === "scale" ? "varies" : "fixed",
      value:
        mode === "scale"
          ? selectedScaleFactors.map((scale) => `SF ${scale}`).join(", ")
          : `SF ${normalizedScaleFactor}`,
    },
  ]
}

function estimateRuntimeMs({
  systemId,
  suiteId,
  databaseId,
  scaleFactor,
  databaseVersion,
}: {
  systemId: SystemId
  suiteId: SuiteId
  databaseId: DatabaseId
  scaleFactor: number
  databaseVersion: string
}) {
  const system = getItem(SYSTEMS, systemId)
  const suite = getItem(SUITES, suiteId)
  const database = getItem(DATABASES, databaseId)
  const versionIndex = Math.max(
    0,
    (database.versions as readonly string[]).indexOf(databaseVersion),
  )
  const scaleMultiplier = Math.log10(scaleFactor + 10) * Math.pow(scaleFactor / 10, 0.34)
  const versionMultiplier = 1 + versionIndex * 0.1
  return Math.round(
    database.baseMs * system.factor * suite.factor * scaleMultiplier * versionMultiplier,
  )
}

function getModeLabel(mode: ComparisonMode) {
  const selectedMode = COMPARISON_MODES.find((comparisonMode) => comparisonMode.id === mode)
  return `${selectedMode?.label ?? "Database"} comparison`
}

function formatRuntime(valueMs: number) {
  if (valueMs >= 1000) {
    return `${(valueMs / 1000).toFixed(2)}s`
  }
  return `${valueMs}ms`
}

function toggleSelection<T>(values: readonly T[], value: T): T[] {
  if (values.includes(value)) {
    return values.length <= 1 ? [...values] : values.filter((candidate) => candidate !== value)
  }
  return [...values, value]
}

function parseComparisonMode(value: string | null): ComparisonMode {
  return COMPARISON_MODES.some((comparisonMode) => comparisonMode.id === value)
    ? (value as ComparisonMode)
    : "database"
}

function parseQueryName(value: string | null): QueryName {
  return QUERY_NAMES.some((queryName) => queryName === value)
    ? (value as QueryName)
    : DEFAULT_QUERY_NAME
}

function parseIdValue<T extends { id: string }>(
  value: string | null,
  items: readonly T[],
  fallback: T["id"],
): T["id"] {
  return value !== null && items.some((item) => item.id === value) ? (value as T["id"]) : fallback
}

function parseIdSubset<T extends { id: string }>(
  value: string | null,
  items: readonly T[],
  fallback: readonly T["id"][],
): T["id"][] {
  const allowed = new Set(items.map((item) => item.id))
  const parsed = splitParam(value).filter((candidate): candidate is T["id"] =>
    allowed.has(candidate),
  )
  return parsed.length > 0 ? unique(parsed) : [...fallback]
}

function parseNumberValue(value: string | null, allowed: readonly number[], fallback: number) {
  if (value === null) return fallback
  const parsed = Number(value)
  return Number.isFinite(parsed) && includesNumber(allowed, parsed) ? parsed : fallback
}

function parseNumberSubset(
  value: string | null,
  allowed: readonly number[],
  fallback: readonly number[],
) {
  const parsed = splitParam(value)
    .map((item) => Number(item))
    .filter((item) => Number.isFinite(item) && includesNumber(allowed, item))
  return parsed.length > 0 ? unique(parsed) : [...fallback]
}

function parseStringValue(value: string | null, allowed: readonly string[], fallback: string) {
  return value !== null && includesString(allowed, value) ? value : fallback
}

function parseStringSubset(
  value: string | null,
  allowed: readonly string[],
  fallback: readonly string[],
) {
  const parsed = splitParam(value).filter((item) => includesString(allowed, item))
  return parsed.length > 0 ? unique(parsed) : [...fallback]
}

function splitParam(value: string | null) {
  return (
    value
      ?.split(",")
      .map((item) => item.trim())
      .filter(Boolean) ?? []
  )
}

function ensureSelected<T>(values: readonly T[], selected: T): T[] {
  return values.includes(selected) ? [...values] : [selected, ...values]
}

function unique<T>(values: readonly T[]) {
  return [...new Set(values)]
}

function normalizeStringSelection(current: readonly string[], allowed: readonly string[]) {
  const next = current.filter((value) => allowed.includes(value))
  return next.length > 0 ? next : [firstValue(allowed)]
}

function normalizeNumericSelection(current: readonly number[], allowed: readonly number[]) {
  const next = current.filter((value) => allowed.includes(value))
  return next.length > 0 ? next : [firstValue(allowed)]
}

function includesString(values: readonly string[], value: string) {
  return values.includes(value)
}

function includesNumber(values: readonly number[], value: number) {
  return values.includes(value)
}

function getScaleColor(index: number) {
  return SCALE_COLORS[index % SCALE_COLORS.length] ?? SCALE_COLORS[0]
}

function firstValue<T>(values: readonly T[]): T {
  const value = values[0]
  if (value === undefined) {
    throw new Error("Expected a non-empty selection")
  }
  return value
}

function getItem<T extends { id: string }>(items: readonly T[], id: T["id"]) {
  const item = items.find((candidate) => candidate.id === id)
  if (!item) {
    throw new Error(`Unknown item: ${id}`)
  }
  return item
}
