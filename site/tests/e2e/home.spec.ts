import { expect, test, type Page } from "@playwright/test"

test("home compares fixed families and opens the selected explorer", async ({ page }) => {
  const errors: string[] = []
  page.on("pageerror", (error) => errors.push(error.message))
  await page.goto("#/")
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

test("family weights do not create a query-count-weighted overall ranking", async ({ page }) => {
  await page.goto("#/")
  await page.getByText("Comparison policy", { exact: true }).click()
  await expect(page.getByText(/no overall ranking is published/)).toBeVisible()
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
