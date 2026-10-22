import { expect, test } from "@playwright/test"

import { makeAnalyticalFixture } from "../fixtures/analytical"
import { makePublicationFixture, usePublicationFixture } from "../fixtures/publication"

let fixture: Awaited<ReturnType<typeof makeAnalyticalFixture>>
let empty: Awaited<ReturnType<typeof makePublicationFixture>>
test.beforeAll(async () => {
  fixture = await makeAnalyticalFixture()
  empty = await makePublicationFixture()
})
test.afterAll(async () => {
  await fixture.cleanup()
  await empty.cleanup()
})

test("published analytical evidence keeps warmup failures unknown", async ({ page }, testInfo) => {
  const response = await page.request.get("data/manifest.json")
  const manifest = await response.json()
  const errors: string[] = []
  page.on("pageerror", (error) => errors.push(error.message))
  await page.goto("#/")
  const section = page.getByRole("region", { name: "Analytical concurrency evidence", exact: true })
  await expect(section.locator("[data-analytical-run]")).toHaveCount(
    manifest.publication.analytical_runs,
  )
  const runs = await page.evaluate(async () => {
    const modulePath = "/olap-benchmarks/src/lib/analyticalReader.ts"
    const { fetchAnalyticalRuns } = (await import(
      modulePath
    )) as typeof import("../../src/lib/analyticalReader")
    return (await fetchAnalyticalRuns()).map((run) => ({
      id: run.row.id,
      db: run.row.db,
      clients: run.clients,
      status: run.row.status,
      manifest: run.manifestStatus,
      selected: run.selectedCount,
      canonical: run.canonicalCount,
      overall: run.overall,
      elapsed: run.elapsedSeconds,
    }))
  })
  await testInfo.attach("published-analytical-evidence", {
    body: JSON.stringify(runs, null, 2),
    contentType: "application/json",
  })
  expect(runs).toHaveLength(manifest.publication.analytical_runs)
  const postgres = runs.filter((run) => run.db === "postgres")
  expect(postgres.map((run) => run.clients).sort((a, b) => a! - b!)).toEqual([1, 4, 16])
  for (const run of runs) {
    expect(run.manifest).toBe("full")
    expect(run.selected).toBe(run.canonical)
    const row = section.locator(`[data-analytical-run="${run.id}"]`)
    if (run.db === "postgres") {
      expect(run.status).toBe("failed")
      expect(run.elapsed).toBeNull()
      expect(run.overall.qps).toBeNull()
      expect(run.overall.completed ?? 0).toBe(0)
      expect(run.overall.percentiles.every((percentile) => percentile.valueMs === null)).toBe(true)
      await expect(row.locator("td").nth(6)).toHaveText("Unknown")
      await expect(row).toContainText("p50 Unknown")
      const evidence = section
        .getByText(`Run ${run.id} · per-query, manifest & budget evidence`, { exact: true })
        .locator("..")
      await evidence.locator("summary").first().click()
      await expect(evidence.getByText(/Recorded run error:/)).toBeVisible()
    } else {
      expect(run.status).toBe("completed")
      expect(run.overall.qps).toBeGreaterThan(0)
      expect(run.overall.errors).toBe(0)
      expect(run.overall.timeouts).toBe(0)
    }
  }
  for (const width of [1024, 390]) {
    await page.setViewportSize({ width, height: 900 })
    expect(
      await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth),
    ).toBe(true)
  }
  expect(errors).toEqual([])
})

test("analytical runs retain qualified latency, failures and drilldown without affecting SELECT scores", async ({
  page,
}, testInfo) => {
  await usePublicationFixture(page, fixture.buffer)
  await page.goto("#/")
  const section = page.getByRole("region", { name: "Analytical concurrency evidence", exact: true })
  await expect(section).toBeVisible()
  const rows = section.locator("[data-analytical-run]")
  await expect(rows).toHaveCount(4)
  const polars = section.locator('[data-analytical-run="101"]')
  await expect(polars).toContainText("polars")
  await expect(polars).toContainText("1.35.0")
  await expect(polars).toContainText("65.00s / 60.00s")
  await expect(polars).toContainText("1.846")
  await expect(polars).toContainText("p99 Unknown (<1000)")
  await expect(section.locator('[data-analytical-run="100"]')).toContainText("p50 Unknown (<5)")
  await expect(section.locator('[data-analytical-run="102"]')).toContainText("failed")
  await expect(section.locator('[data-analytical-run="103"]')).toContainText("Unknown / Unknown")
  await expect(section.getByText(/achieved query mix/)).toBeVisible()
  const evidence = section
    .getByText("Run 101 · per-query, manifest & budget evidence", { exact: true })
    .locator("..")
  await evidence.locator("summary").first().click()
  await expect(evidence.getByText(/Full canonical manifest required: 3 queries/)).toBeVisible()
  await expect(evidence.getByText(/unsupported: Documented analytical fixture skip/)).toBeVisible()
  const queryTable = section.getByRole("table", { name: "Run 101 per-query evidence" })
  await expect(queryTable.getByRole("row").filter({ hasText: "Q1" })).toContainText("120")
  await expect(queryTable.getByRole("row", { name: /^Q2 ·/ })).toContainText("0 / 0")
  await expect(evidence.getByText(/Shared across clients: Yes \(recorded\)/)).toBeVisible()
  await evidence.getByText("Per-worker runtime & cache evidence", { exact: true }).click()
  await expect(evidence.getByText(/Cache hits are not instrumented/).first()).toBeVisible()
  await section
    .getByText("Run 103 · per-query, manifest & budget evidence", { exact: true })
    .click()
  await expect(
    section.getByText(/Analytical metadata missing, invalid or unsupported/),
  ).toBeVisible()
  await expect(
    page
      .getByRole("listitem", { name: "fast comparison", exact: true })
      .locator("[data-publication-score]"),
  ).toHaveText("1.00×")
  for (const width of [1440, 390]) {
    await page.setViewportSize({ width, height: 900 })
    expect(
      await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth),
    ).toBe(true)
  }
  await evidence.locator("summary").first().click()
  await section
    .getByText("Run 103 · per-query, manifest & budget evidence", { exact: true })
    .click()
  for (const width of [1440, 390]) {
    await page.setViewportSize({ width, height: 900 })
    await section.screenshot({ path: testInfo.outputPath(`analytical-${width}.png`) })
  }
})

test("publications with no analytical runs have no visible analytical section", async ({
  page,
}) => {
  await usePublicationFixture(page, empty.buffer)
  await page.goto("#/")
  await expect(page.getByRole("listitem", { name: "fast comparison", exact: true })).toBeVisible()
  await expect(page.getByRole("heading", { name: "Analytical concurrency evidence" })).toHaveCount(
    0,
  )
  await expect(page.getByRole("region", { name: "Analytical concurrency evidence" })).toHaveCount(0)
})
