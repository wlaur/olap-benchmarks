import { expect, test, type Page } from "@playwright/test"

test("home starts with scores and reveals supporting information on demand", async ({ page }) => {
  await page.goto("#/")
  const row = page.locator('[data-overall-engine="duckdb"]')
  await expect(row.locator("[data-overall-score]")).toBeVisible()
  await expect(row.locator("summary")).toContainText("871/871 queries scored")
  await expect(row.locator("[data-overall-coverage]")).toBeHidden()
  const concurrency = page.getByRole("table", {
    name: "Analytical stream measurements",
    includeHidden: true,
  })
  await expect(concurrency).toBeHidden()
  await expect(page.getByText("October 2026 · M4 Pro", { exact: true })).toBeHidden()
  await page.getByRole("heading", { name: "Analytical concurrency evidence", exact: true }).click()
  await expect(concurrency).toBeVisible()
  await page.getByRole("heading", { name: "About this campaign", exact: true }).click()
  await expect(page.getByText("October 2026 · M4 Pro", { exact: true })).toBeVisible()
  await row.locator("summary").focus()
  await page.keyboard.press("Enter")
  await expect(row.locator("[data-overall-coverage]")).toBeVisible()
})

test("home compares fixed families and opens the selected explorer", async ({ page }) => {
  const errors: string[] = []
  page.on("pageerror", (error) => errors.push(error.message))
  await page.goto("#/")
  await expect(page.getByRole("heading", { name: "Overall database ranking" })).toBeVisible()
  await page.getByRole("button", { name: "By workload", exact: true }).click()
  await expect(page.getByRole("heading", { name: "Compare by workload family" })).toBeVisible()
  await page.getByRole("button", { name: "ClickBench", exact: true }).click()
  await expect(
    page.getByText("42 fixed performance queries · 43 coverage positions · raw track"),
  ).toBeVisible()
  await expect(
    page.getByRole("listitem", { name: "postgres comparison", exact: true }),
  ).toContainText("documented incomplete preparation")
  await page.getByRole("button", { name: "Open explorer", exact: true }).click()
  await expect(page).toHaveURL(/\/explorer\/clickbench\?.*scale=1/)
  await expect(page.getByRole("heading", { name: "Database comparison" })).toBeVisible()
  await expect(
    page.getByText(/42 fixed performance queries · 43 coverage positions\./),
  ).toBeVisible()
  await expect(page.getByRole("combobox", { name: "Query" })).toBeVisible()
  expect(errors).toEqual([])
})

test("home publication scores remain fixed when engines are hidden", async ({ page }) => {
  await page.goto("#/")
  await page.getByRole("button", { name: "By workload", exact: true }).click()
  await page.getByRole("button", { name: "ClickBench", exact: true }).click()
  const scores = page
    .locator("[data-comparison-engine]")
    .filter({ has: page.locator("[data-publication-score]") })
  await expect(scores.first()).toBeVisible()
  const original = await scores.evaluateAll((rows) =>
    rows.map((row) => ({
      key: row.getAttribute("data-comparison-engine")!,
      score: row.querySelector("[data-publication-score]")!.getAttribute("data-publication-score"),
    })),
  )
  expect(original.length).toBeGreaterThan(1)
  await page.getByText("Visible engines", { exact: true }).click()
  const hiddenDb = JSON.parse(original[0]!.key)[0]
  await page.getByRole("button", { name: hiddenDb, exact: true }).click()
  for (const row of original.slice(1)) {
    await expect(
      page
        .locator("[data-comparison-engine]")
        .filter({ has: page.locator(`[data-publication-score="${row.score}"]`) })
        .first(),
    ).toBeVisible()
  }
  expect(
    await scores.evaluateAll((rows) =>
      rows.map((row) => ({
        key: row.getAttribute("data-comparison-engine"),
        score: row
          .querySelector("[data-publication-score]")!
          .getAttribute("data-publication-score"),
      })),
    ),
  ).toEqual(original.slice(1))
})

test("score headers and evidence fit desktop, iPad and mobile widths", async ({
  page,
}, testInfo) => {
  await page.goto("#/")
  await page.getByRole("button", { name: "By workload", exact: true }).click()
  await page.getByRole("button", { name: "RTABench", exact: true }).click()
  await expect(
    page.getByText("31 fixed performance queries · 31 coverage positions · raw track"),
  ).toBeVisible()
  await page.getByRole("button", { name: /RTABench prepared aggregates/ }).click()
  await expect(
    page.getByText("10 fixed performance queries · 10 coverage positions · prepared track"),
  ).toBeVisible()
  await page.getByRole("button", { name: "ClickBench", exact: true }).click()
  for (const width of [1920, 1440, 1194, 1024, 834, 768, 390]) {
    await page.setViewportSize({ width, height: 900 })
    await expect(page.getByRole("list", { name: "Family engine comparison" })).toBeVisible()
    await expectNoHorizontalPageOverflow(page)
    const card = page.getByRole("listitem", { name: "duckdb comparison", exact: true })
    await card.getByText(/Run context & preparation/).click()
    await expect(card.getByText("Input fingerprint", { exact: true })).toBeVisible()
    await expectNoHorizontalPageOverflow(page)
    await card.getByText(/Run context & preparation/).click()
    if ([1440, 1024, 390].includes(width)) {
      await page.locator("main").evaluate((main) => {
        main.scrollTop = 0
      })
      await page.screenshot({ path: testInfo.outputPath(`family-${width}.png`) })
    }
  }
})

test("workload drilldown retains strict family eligibility and selected scale", async ({
  page,
}) => {
  await page.goto("#/")
  await page.getByRole("button", { name: "By workload", exact: true }).click()
  await page.getByText("Comparison policy", { exact: true }).click()
  await expect(
    page.getByText(
      /Strict family eligibility and latency ratios remain separate from the overall index/,
    ),
  ).toBeVisible()
  await page.getByRole("button", { name: "Time Series", exact: true }).click()
  await page.getByRole("button", { name: "SF 10", exact: true }).click()
  await expect(page.getByRole("button", { name: "SF 10", exact: true })).toHaveAttribute(
    "aria-pressed",
    "true",
  )
  await page.getByRole("button", { name: "Open explorer", exact: true }).click()
  await expect(page).toHaveURL(/scale=10/)
})

async function expectNoHorizontalPageOverflow(page: Page) {
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth - document.documentElement.clientWidth,
    ),
  ).toBeLessThanOrEqual(0)
}
