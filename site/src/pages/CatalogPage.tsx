import { Database, ExternalLink, Layers3, Server, type LucideIcon } from "lucide-react"
import { useMemo } from "react"
import { Link, useSearchParams } from "react-router-dom"

import { CatalogCoverageMatrix } from "../components/catalog/CatalogCoverageMatrix"
import { CatalogQueryList } from "../components/catalog/CatalogQueryList"
import { CatalogSuiteList } from "../components/catalog/CatalogSuiteList"
import { ControlSelect, ControlSelectSkeleton } from "../components/controls/ControlSelect"
import { PanelCard, PanelHeader } from "../components/layout/Panel"
import { MetaLabel, SectionTitle } from "../components/Typography"
import { useCatalogData } from "../hooks/useCatalogData"
import { isBenchmarkSuiteId } from "../lib/benchmarks"
import { buildCatalogSuiteSummary } from "../lib/catalog"
import { useAppStore } from "../stores/useAppStore"

export function CatalogPage() {
  const [searchParams, setSearchParams] = useSearchParams()
  const benchmarkDefinitions = useAppStore((state) => state.benchmarkDefinitions)
  const suitesLoading = useAppStore((state) => state.suitesLoading)
  const suitesError = useAppStore((state) => state.suitesError)
  const catalog = useCatalogData()
  const summaries = useMemo(
    () =>
      benchmarkDefinitions.map((definition) =>
        buildCatalogSuiteSummary(definition, catalog.dimensions, catalog.queriesManifest),
      ),
    [benchmarkDefinitions, catalog.dimensions, catalog.queriesManifest],
  )
  const requestedSuite = searchParams.get("suite") ?? undefined
  const selectedSuiteId = isBenchmarkSuiteId(requestedSuite, benchmarkDefinitions)
    ? requestedSuite
    : (benchmarkDefinitions[0]?.id ?? null)
  const selectedSummary =
    summaries.find((summary) => summary.definition.id === selectedSuiteId) ?? null
  const loading = suitesLoading || catalog.loading
  const error = suitesError ?? catalog.error
  const systems = unique(catalog.dimensions.map((row) => row.system))
  const databases = unique(catalog.dimensions.map((row) => row.db))
  const explorerScale = selectedSummary?.scales[0] ?? 1

  function handleSuiteSelect(suiteId: string) {
    setSearchParams({ suite: suiteId }, { replace: true })
  }

  return (
    <div className="flex min-h-full w-full min-w-0 flex-col gap-4 pb-6">
      <header className="flex flex-wrap items-end justify-between gap-4">
        <div className="min-w-0">
          <MetaLabel>Inventory</MetaLabel>
          <h2 className="mt-1 text-2xl font-semibold text-slate-50">Benchmark catalog</h2>
        </div>
        <div className="flex flex-wrap items-center gap-x-5 gap-y-2 text-xs text-slate-400">
          <CatalogStat
            icon={Layers3}
            value={benchmarkDefinitions.length}
            label={benchmarkDefinitions.length === 1 ? "suite" : "suites"}
          />
          <CatalogStat
            icon={Database}
            value={databases.length}
            label={databases.length === 1 ? "database" : "databases"}
          />
          <CatalogStat
            icon={Server}
            value={systems.length}
            label={systems.length === 1 ? "system" : "systems"}
          />
        </div>
      </header>

      {error ? (
        <div className="border-y border-red-500/30 bg-red-950/20 px-3 py-2 text-sm text-red-300">
          Failed to load catalog: {error}
        </div>
      ) : null}

      <div className="grid min-w-0 items-start gap-4 xl:grid-cols-[minmax(15rem,18rem)_minmax(0,1fr)]">
        <PanelCard className="p-3 xl:sticky xl:top-4">
          <PanelHeader className="px-1 pb-2">
            <div>
              <SectionTitle as="h3">Suites</SectionTitle>
              <p className="mt-1 text-xs text-slate-500">
                {benchmarkDefinitions.length} configured
              </p>
            </div>
          </PanelHeader>
          <div className="xl:hidden">
            {loading || selectedSuiteId === null ? (
              <ControlSelectSkeleton label="Suite" labelMode="always" className="w-full" />
            ) : (
              <ControlSelect
                ariaLabel="Suite"
                label="Suite"
                value={selectedSuiteId}
                onChange={handleSuiteSelect}
                options={benchmarkDefinitions.map((definition) => ({
                  value: definition.id,
                  label: definition.title,
                }))}
                icon={<Layers3 className="h-3 w-3" strokeWidth={1.8} />}
                labelMode="always"
                className="w-full"
              />
            )}
          </div>
          <div className="hidden xl:block">
            <CatalogSuiteList
              summaries={summaries}
              selectedSuiteId={selectedSuiteId}
              loading={loading}
              onSelect={handleSuiteSelect}
            />
          </div>
        </PanelCard>

        <div className="grid min-w-0 gap-4">
          <PanelCard className="p-3 sm:p-4">
            <PanelHeader className="mb-3 flex-wrap">
              <div className="min-w-0">
                <MetaLabel>Result coverage</MetaLabel>
                <SectionTitle as="h3" className="mt-1 truncate">
                  {selectedSummary?.definition.title ?? "Suite coverage"}
                </SectionTitle>
                <p className="mt-1 text-xs text-slate-500">
                  {selectedSummary?.systems.length ?? 0} completed{" "}
                  {(selectedSummary?.systems.length ?? 0) === 1 ? "system" : "systems"}
                </p>
              </div>
              {selectedSummary ? (
                <Link
                  to={`/explorer/${selectedSummary.definition.id}?scale=${explorerScale}`}
                  className="inline-flex min-h-9 items-center gap-2 rounded-md border border-border-default bg-surface-inset px-3 text-xs font-medium text-slate-300 transition-colors outline-none hover:border-slate-500 hover:bg-surface-raised hover:text-slate-50 focus-visible:border-slate-300 focus-visible:ring-2 focus-visible:ring-slate-300/20"
                >
                  Open explorer
                  <ExternalLink className="h-3.5 w-3.5" strokeWidth={1.8} />
                </Link>
              ) : null}
            </PanelHeader>
            <CatalogCoverageMatrix summary={selectedSummary} loading={loading} />
          </PanelCard>

          <PanelCard className="p-3 sm:p-4">
            <PanelHeader className="mb-3">
              <div>
                <MetaLabel>Query inventory</MetaLabel>
                <SectionTitle as="h3" className="mt-1">
                  {selectedSummary?.queryNames.length ?? 0} queries
                </SectionTitle>
              </div>
            </PanelHeader>
            <CatalogQueryList
              key={selectedSuiteId}
              queryNames={selectedSummary?.queryNames ?? []}
              loading={loading}
            />
          </PanelCard>
        </div>
      </div>
    </div>
  )
}

function CatalogStat({
  icon: Icon,
  value,
  label,
}: {
  icon: LucideIcon
  value: number
  label: string
}) {
  return (
    <span className="inline-flex items-center gap-1.5">
      <Icon className="h-3.5 w-3.5 text-slate-500" strokeWidth={1.8} />
      <strong className="font-semibold text-slate-200 tabular-nums">{value}</strong>
      {label}
    </span>
  )
}

function unique(values: readonly string[]) {
  return [...new Set(values)]
}
