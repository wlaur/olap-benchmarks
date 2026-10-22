import { expect, test } from "@playwright/test"

import pinned from "../fixtures/overall.json" with { type: "json" }
import {
  comparisonFixture,
  makePublicationFixture,
  usePublicationFixture,
} from "../fixtures/publication"

test("pinned overall index reproduces the reviewed scores without workload order inversions", async ({
  page,
}, testInfo) => {
  const response = await page.request.get("data/manifest.json")
  expect((await response.json()).results_db_sha256).toBe(pinned.results_db_sha256)
  await page.goto("#/")
  const evidence = await page.evaluate(async () => {
    const readerPath = "/olap-benchmarks/src/lib/observationReader.ts"
    const comparisonPath = "/olap-benchmarks/src/lib/observations.ts"
    const overallPath = "/olap-benchmarks/src/lib/overall.ts"
    const { fetchComparisonData } = (await import(
      readerPath
    )) as typeof import("../../src/lib/observationReader")
    const { compareFamily } = (await import(
      comparisonPath
    )) as typeof import("../../src/lib/observations")
    const { aggregateOverview } = (await import(
      overallPath
    )) as typeof import("../../src/lib/overall")
    const { manifest, runs } = await fetchComparisonData()
    const systems = [...new Set(runs.map((run) => run.system))]
    const families = manifest.families.map((family) => ({
      family,
      scales: family.scale_factors.map((scale) => ({
        scale,
        rows: compareFamily(family, scale, systems[0]!, runs, manifest),
      })),
    }))
    const result = aggregateOverview(families)
    let pairs = 0
    const inversions: string[] = []
    for (const entry of families.filter(
      ({ family }) => family.weight > 0 && family.track === "raw",
    )) {
      for (const cell of entry.scales) {
        const single = aggregateOverview([
          { family: { ...entry.family, scale_factors: [cell.scale] }, scales: [cell] },
        ])
        const eligible = cell.rows.filter((row) => row.eligible)
        for (let a = 0; a < eligible.length; a++) {
          for (let b = a + 1; b < eligible.length; b++) {
            pairs += 1
            const x = eligible[a]!,
              y = eligible[b]!
            const sx = single.rows.find((row) => row.db === x.db)!.score
            const sy = single.rows.find((row) => row.db === y.db)!.score
            if ((x.score! - y.score!) * (sx - sy) > 1e-9)
              inversions.push(`${entry.family.id}/${cell.scale}/${x.db}/${y.db}`)
          }
        }
      }
    }
    return { ...result, systems, pairs, inversions }
  })
  await testInfo.attach("overall-index-evidence", {
    body: JSON.stringify(evidence, null, 2),
    contentType: "application/json",
  })
  expect(evidence.policy).toBe(pinned.policy)
  expect(evidence.systems).toHaveLength(1)
  expect(evidence.familyCount).toBe(pinned.families)
  expect(evidence.queryCount).toBe(pinned.queries)
  expect(evidence.pairs).toBe(pinned.within_workload_pairs)
  expect(evidence.inversions).toEqual([])
  expect(evidence.rows.map((row) => row.db)).toEqual(pinned.rows.map((row) => row.db))
  for (const [index, expected] of pinned.rows.entries()) {
    const row = evidence.rows[index]!
    expect(row.score).toBeCloseTo(expected.score, 1)
    expect(row.coverage).toBeCloseTo(expected.coverage, 1)
    expect(row.rankRange).toEqual(expected.band)
    expect(row.families.reduce((sum, family) => sum + family.cappedQueries, 0)).toBe(
      expected.capped,
    )
    expect(row.coverage + row.recordedGapCoverage + row.unmeasuredCoverage).toBeCloseTo(100, 10)
    const rendered = page.locator(`[data-overall-engine="${row.db}"]`)
    await expect(rendered.locator("[data-overall-score]")).toHaveText(expected.score.toFixed(1))
    await expect(rendered.locator("[data-overall-band]")).toHaveAttribute(
      "data-overall-band",
      expected.band.join("–"),
    )
  }
  expect(evidence.rows.find((row) => row.db === "tdengine")!.unrankedOmissions).toBe(1)
  const list = page.getByRole("list", { name: "Overall database ranking", exact: true })
  await expect(list.getByRole("listitem")).toHaveCount(11)
  await expect(list).not.toContainText("Not eligible")
  await page.getByText("Visible engines", { exact: true }).click()
  await page.getByRole("button", { name: "duckdb", exact: true }).click()
  await expect(list.getByRole("listitem")).toHaveCount(10)
  for (const expected of pinned.rows.slice(1)) {
    const row = page.locator(`[data-overall-engine="${expected.db}"]`)
    await expect(row.locator("[data-overall-score]")).toHaveText(expected.score.toFixed(1))
    await expect(row.locator("[data-overall-rank]")).toHaveText(
      String(pinned.rows.indexOf(expected) + 1),
    )
  }
})

test("overall rows and contributions fit desktop, iPad and mobile widths", async ({
  page,
}, testInfo) => {
  await page.goto("#/")
  const row = page.locator('[data-overall-engine="postgres"]')
  await expect(row.locator("[data-overall-score]")).toHaveText("27.7")
  for (const width of [1440, 1024, 834, 768, 640, 390]) {
    await page.setViewportSize({ width, height: 1000 })
    await expect(row.locator("[data-overall-band]")).toBeVisible()
    await row.locator("summary").click()
    await expect(row).toContainText("partial completed runs")
    await expect(row).toContainText("run failed")
    await expect(row).toContainText("Scored run IDs")
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth - document.documentElement.clientWidth,
      ),
    ).toBeLessThanOrEqual(0)
    const overflows = await row.evaluate((element) =>
      [...element.querySelectorAll("summary, li")]
        .filter((child) => child.scrollWidth > child.clientWidth + 1)
        .map((child) => child.textContent),
    )
    expect(overflows).toEqual([])
    if ([1440, 1024, 390].includes(width))
      await page.screenshot({ path: testInfo.outputPath(`overall-${width}.png`) })
    await row.locator("summary").click()
  }
  await row.locator("summary").click()
  await row.getByRole("button", { name: "Compare TPC-DS by workload", exact: true }).click()
  await expect(
    page.getByRole("heading", { name: "Compare by workload family", exact: true }),
  ).toBeVisible()
  await expect(page.getByRole("button", { name: "TPC-DS", exact: true })).toHaveAttribute(
    "aria-pressed",
    "true",
  )
})

test("partial results earn fixed-denominator credit and blocked evidence stays distinct", async ({
  page,
}) => {
  const fixture = await makePublicationFixture()
  try {
    await usePublicationFixture(page, fixture.buffer)
    await page.goto("#/")
    const row = (db: string) => page.locator(`[data-overall-engine="${db}"]`)
    await expect(row("fast").locator("[data-overall-score]")).toHaveText("100.0")
    await expect(row("slow").locator("[data-overall-score]")).toHaveText("69.9")
    await expect(row("partial").locator("[data-overall-score]")).toHaveText("50.0")
    await expect(row("partial").locator("[data-overall-recorded-gaps]")).toHaveText("50.0%")
    await expect(row("latestfailed").locator("[data-overall-score]")).toHaveText("0.0")
    await expect(row("latestfailed").locator("[data-overall-recorded-gaps]")).toHaveText("100.0%")
    await expect(row("specialist").locator("[data-overall-no-run]")).toHaveText("100.0%")
    await row("latestfailed").locator("summary").click()
    await expect(row("latestfailed")).toContainText("run failed")
    await row("specialist").locator("summary").click()
    await expect(row("specialist")).toContainText("Adapter unimplemented")
    await page.getByText("Overall ranking method", { exact: true }).click()
    await expect(page.getByText(/logarithmically from 100/)).toBeVisible()
    await expect(page.getByRole("link", { name: "Full ranking methodology" })).toBeVisible()
  } finally {
    await fixture.cleanup()
  }
})

test("overall verification errors are visible while workload evidence remains accessible", async ({
  page,
}) => {
  const fixture = await makePublicationFixture()
  try {
    await usePublicationFixture(page, fixture.buffer, {
      ...comparisonFixture,
      registrations: comparisonFixture.registrations.map((registration) => ({
        ...registration,
        scale_factors: registration.db === "fast" ? [2] : registration.scale_factors,
      })),
    })
    await page.goto("#/")
    await expect(
      page.getByRole("alert").filter({ hasText: "Failed to load comparison" }),
    ).toContainText("Run exists for an unregistered scale")
    await expect(page.locator("[data-overall-score]")).toHaveCount(0)
    await page.getByRole("button", { name: "By workload", exact: true }).click()
    await expect(page.getByRole("list", { name: "Family engine comparison" })).toBeVisible()
  } finally {
    await fixture.cleanup()
  }
})
