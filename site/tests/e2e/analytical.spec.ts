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
  await section
    .getByRole("heading", { name: "Analytical concurrency evidence", exact: true })
    .click()
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
      const expected = {
        670: ["Cancelled", "Cancelled during warmup", "KeyboardInterrupt"],
        671: ["Timed out", "Timed out during warmup", "TimeoutError"],
        672: ["Cancelled", "Cancelled", "KeyboardInterrupt"],
      }[run.id]!
      await expect(row.locator("[data-analytical-qps]")).toHaveText("Unknown")
      await expect(row).toContainText(expected[0])
      await expect(row).not.toContainText(expected[2])
      await row.getByRole("button", { name: `Run ${run.id} details` }).click()
      const dialog = page.getByRole("dialog", {
        name: `Run ${run.id} · PostgreSQL 18.6 · ${run.clients} client${run.clients === 1 ? "" : "s"}`,
      })
      await expect(dialog.locator("[data-analytical-outcome] > p").first()).toHaveText(expected[1])
      await expect(dialog.locator("[data-analytical-outcome]")).toContainText(
        "QPS unknown: no measurement start recorded",
      )
      await expect(dialog).toContainText("p50 Unknown")
      await expect(dialog.getByText(expected[2])).toBeHidden()
      await dialog.getByText("Recorded run error", { exact: true }).click()
      await expect(dialog.getByText(new RegExp(`^${expected[2]}`))).toBeVisible()
      await expect(
        dialog.getByRole("table", { name: `Run ${run.id} per-query evidence` }),
      ).toBeHidden()
      await page.keyboard.press("Escape")
      await expect(dialog).toBeHidden()
    } else {
      expect(run.status).toBe("completed")
      expect(run.overall.qps).toBeGreaterThan(0)
      expect(run.overall.errors).toBe(0)
      expect(run.overall.timeouts).toBe(0)
    }
  }
  await expect(section.getByText("9/12 runs completed", { exact: true })).toBeVisible()
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
  await page.getByRole("button", { name: "By workload", exact: true }).click()
  const section = page.getByRole("region", { name: "Analytical concurrency evidence", exact: true })
  await section
    .getByRole("heading", { name: "Analytical concurrency evidence", exact: true })
    .click()
  await expect(section).toBeVisible()
  const rows = section.locator("[data-analytical-run]")
  await expect(rows).toHaveCount(4)
  await expect(section.getByText(/achieved different query mixes/)).toBeVisible()
  await expect(section.getByText("2/4 runs completed", { exact: true })).toBeVisible()
  const polars = section.locator('[data-analytical-run="101"]')
  await expect(polars).toContainText("Polars 1.35.0")
  await expect(polars).toContainText("1.85")
  await expect(polars).toContainText("Completed")
  await expect(section.locator('[data-analytical-run="102"]')).toContainText("Cancelled")
  await expect(section.locator('[data-analytical-run="103"]')).toContainText("Failed")
  await expect(section.locator('[data-analytical-run="103"] [data-analytical-qps]')).toHaveText(
    "Unknown",
  )
  await section.getByRole("button", { name: "Run 100 details" }).click()
  await expect(
    page.getByRole("dialog", { name: "Run 100 · DuckDB 1.5.6 · 1 client" }),
  ).toContainText("p50 Unknown (<5)")
  await page.keyboard.press("Escape")
  await expect(page.getByRole("dialog")).toHaveCount(0)
  const trigger = polars.getByRole("button", { name: "Run 101 details" })
  await trigger.focus()
  await page.keyboard.press("Enter")
  const evidence = page.getByRole("dialog", { name: /^Run 101/ })
  await expect(evidence).toContainText("65.00s / 60.00s")
  await expect(evidence).toContainText("p99 Unknown (<1000)")
  await expect(evidence.getByText(/Full canonical manifest required: 3 queries/)).toBeVisible()
  const queryTable = evidence.getByRole("table", { name: "Run 101 per-query evidence" })
  await expect(queryTable).toBeHidden()
  await evidence.getByText("Per-query attempts", { exact: true }).click()
  await expect(evidence.getByText(/unsupported: Documented analytical fixture skip/)).toBeVisible()
  await expect(queryTable.getByRole("row").filter({ hasText: "Q1" })).toContainText("120")
  await expect(queryTable.getByRole("row", { name: /^Q2 ·/ })).toContainText("0 / 0")
  await evidence.getByText("Recorded budget scope", { exact: true }).click()
  await expect(evidence.getByText(/Shared across clients: Yes \(recorded\)/)).toBeVisible()
  await evidence.getByText("Per-worker runtime & cache evidence", { exact: true }).click()
  await expect(evidence.getByText(/Cache hits are not instrumented/).first()).toBeVisible()
  await page.keyboard.press("Escape")
  await expect(evidence).toBeHidden()
  await expect(trigger).toBeFocused()
  await section.getByRole("button", { name: "Run 103 details" }).click()
  const failed = page.getByRole("dialog", { name: /^Run 103/ })
  await expect(failed).toContainText("Unknown / Unknown")
  await expect(
    failed.getByText(/Analytical metadata missing, invalid or unsupported/),
  ).toBeVisible()
  await page.keyboard.press("Escape")
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
  for (const width of [1440, 390]) {
    await page.setViewportSize({ width, height: 900 })
    await section.screenshot({ path: testInfo.outputPath(`analytical-${width}.png`) })
  }
})

test("publications with no analytical runs show an explicit empty disclosure", async ({ page }) => {
  await usePublicationFixture(page, empty.buffer)
  await page.goto("#/")
  await page.getByRole("button", { name: "By workload", exact: true }).click()
  await expect(page.getByRole("listitem", { name: "fast comparison", exact: true })).toBeVisible()
  await page.getByRole("heading", { name: "Analytical concurrency evidence" }).click()
  await expect(
    page.getByText("No analytical runs recorded for this publication.", { exact: true }),
  ).toBeVisible()
})
