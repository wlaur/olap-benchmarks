import { expect, test } from "@playwright/test"

import type { Publication } from "../../src/lib/publication"

test("published cohorts reconcile with fixed-family eligibility", async ({ page }, testInfo) => {
  const response = await page.request.get("data/manifest.json")
  const manifest = await response.json()
  await page.goto("#/")
  const evidence = await page.evaluate(async () => {
    const readerPath = "/olap-benchmarks/src/lib/observationReader.ts"
    const comparisonPath = "/olap-benchmarks/src/lib/observations.ts"
    const { fetchComparisonData } = (await import(
      readerPath
    )) as typeof import("../../src/lib/observationReader")
    const { compareFamily } = (await import(
      comparisonPath
    )) as typeof import("../../src/lib/observations")
    const { manifest: comparison, runs } = await fetchComparisonData()
    const systems = [...new Set(runs.map((run) => run.system))]
    return {
      selected: runs.length,
      completed: runs.filter((run) => run.status === "completed").length,
      failed: runs.filter((run) => run.status === "failed").length,
      families: systems.flatMap((system) =>
        comparison.families.flatMap((family) =>
          family.scale_factors.map((scale) => ({
            system,
            family: family.id,
            scale,
            queries: family.query_names.length,
            rows: compareFamily(family, scale, system, runs, comparison)
              .filter((row) => row.run !== null)
              .map((row) => ({
                db: row.db,
                run: row.run!.runId,
                eligible: row.eligible,
                reasons: row.reasons,
                score: row.score,
                wins: row.wins,
                peerHashes: row.queries.filter((query) => query.evidence === "peer hash").length,
              })),
          })),
        ),
      ),
    }
  })
  await testInfo.attach("published-family-eligibility", {
    body: JSON.stringify(evidence, null, 2),
    contentType: "application/json",
  })
  expect(evidence.selected).toBe(manifest.publication.select_cohorts)
  expect(evidence.completed).toBe(manifest.publication.completed_select_cohorts)
  expect(evidence.failed).toBe(manifest.publication.failed_select_cohorts)
  const job = evidence.families.filter((family) => family.family === "join_order")
  expect(job).toHaveLength(1)
  expect(job[0]!.rows).toHaveLength(5)
  for (const row of job[0]!.rows) {
    expect(row.eligible).toBe(true)
    expect(row.peerHashes).toBe(113)
  }
  const telemetry = evidence.families.filter((family) => family.family === "device_telemetry")
  expect(telemetry.map((family) => family.scale)).toEqual([1, 10])
  for (const family of telemetry) {
    expect(family.rows.find((row) => row.db === "duckdb")?.wins).toBe(18)
  }
})

test("publication scope remains accessible across the results pages", async ({ page }) => {
  const response = await page.request.get("data/manifest.json")
  expect(response.ok()).toBe(true)
  const { publication } = (await response.json()) as { publication: Publication }

  for (const route of ["#/", "#/explorer/tpc_h?scale=50", "#/catalog?suite=tpc_h"]) {
    await page.goto(route)
    if (route.startsWith("#/catalog"))
      await page.getByText("Workload and publication context", { exact: true }).click()
    else await page.getByRole("button", { name: "Measurement context", exact: true }).click()
    const report = page.getByRole("link", { name: "Coverage & exclusions", exact: true })
    if (publication.kind === "matrix") {
      await expect(report).toHaveCount(0)
      continue
    }

    await expect(report).toBeVisible()
    await expect(report).toHaveAttribute("href", publication.report_url)
    const notice = page.getByRole("note").filter({ has: report })
    await expect(notice).toContainText(`${publication.select_cohorts} query runs`)
    await expect(notice).toContainText(`${publication.partial_select_cohorts} are partial`)
    await expect(notice).toContainText("Coverage and comparison rules are shown with each result")
  }
})
