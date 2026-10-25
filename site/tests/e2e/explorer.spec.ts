import { expect, test, type Page } from "@playwright/test"

import { makePublicationFixture, usePublicationFixture } from "../fixtures/publication"

test("explorer leads with the workload result and keeps query details closed", async ({ page }) => {
  const errors: string[] = []
  page.on("pageerror", (error) => errors.push(error.message))
  await page.goto("#/explorer/clickbench")
  await expect(page.getByRole("heading", { name: "ClickBench", exact: true })).toBeVisible()
  const lead = page.locator("[data-comparison-lead]")
  await expect(lead).toContainText("Lowest latency ratio: DuckDB")
  expect((await lead.boundingBox())!.y).toBeLessThan(800)
  await expect(
    page.getByRole("button", { name: "Analyze one database", exact: true }),
  ).toBeVisible()
  await expect(page.getByLabel("SQL query viewer")).toHaveCount(0)
  await expect(page.locator("[data-heatmap-block]").first()).toBeHidden()
  await page.getByRole("button", { name: "Inspect a query", exact: true }).click()
  const dialog = page.getByRole("dialog", { name: "Query performance and SQL", exact: true })
  await expect(dialog).toBeVisible()
  await dialog.getByRole("combobox", { name: "Query to inspect", exact: true }).click()
  await dialog.getByRole("searchbox", { name: "Filter queries" }).fill("Q3")
  await dialog.getByRole("option", { name: "Q3", exact: true }).click()
  await expect(page).toHaveURL(/query=Q3/)
  await expect(dialog.getByLabel("SQL query viewer")).toContainText(/SELECT/i)
  const clickHouse = dialog.getByRole("button", { name: "ClickHouse SQL", exact: true })
  await clickHouse.click()
  await expect(clickHouse).toHaveAttribute("aria-pressed", "true")
  await page.keyboard.press("Escape")
  await expect(dialog).toBeHidden()
  await expect(page.getByRole("button", { name: "Inspect a query", exact: true })).toBeFocused()
  expect(errors).toEqual([])
})

test("single-database comparisons show selected measurements without singleton winners", async ({
  page,
}) => {
  await page.goto("#/explorer/time_series?mode=scale&database=duckdb&scales=1,10")
  await expect(page.getByRole("list", { name: "Recorded query outcomes" })).toBeVisible()
  await expect(page.getByRole("combobox", { name: "Query", exact: true })).toBeVisible()
  await expect(page.locator('[data-query-engine="duckdb"]')).toHaveCount(2)
  await expect(page.getByText("Fastest", { exact: true })).toHaveCount(0)
  for (const mode of ["scale", "version", "system"]) {
    await page.goto(`#/explorer/time_series?mode=${mode}&database=duckdb&scales=1`)
    await expect(
      page.getByText(/1 selected variant .* no comparison/, { exact: true }),
    ).toBeVisible()
    await expect(page.locator('[data-query-engine="duckdb"]')).toHaveCount(1)
    await expect(page.getByText("Fastest", { exact: true })).toHaveCount(0)
  }
})

test("database selection survives a round trip through single-database analysis", async ({
  page,
}) => {
  await page.goto("#/explorer/time_series?mode=database&databases=duckdb,clickhouse,monetdb")
  await expect(page.locator("[data-comparison-engine]")).toHaveCount(3)
  await page.getByRole("button", { name: "Analyze one database", exact: true }).click()
  await expect(page.getByRole("list", { name: "Recorded query outcomes" })).toBeVisible()
  await expect(page.locator('[data-query-engine="duckdb"]')).toHaveCount(2)
  await page.getByRole("button", { name: "Compare databases", exact: true }).click()
  await expect(page.locator("[data-comparison-engine]")).toHaveCount(3)
  await expect(page).toHaveURL(/databases=clickhouse%2Cduckdb%2Cmonetdb/)
  await page.goto("#/explorer/time_series?mode=database&databases=duckdb")
  await expect(page.getByText(/^1 database ·/)).toBeVisible()
})

test("the Explorer navigation restores the last query and comparison", async ({ page }) => {
  await page.goto("#/explorer/clickbench?mode=database&query=Q9&databases=duckdb,monetdb")
  await expect(page.locator("[data-comparison-engine]")).toHaveCount(2)
  await page
    .getByRole("navigation", { name: "Primary" })
    .getByRole("link", { name: "Home", exact: true })
    .click()
  await page
    .getByRole("navigation", { name: "Primary" })
    .getByRole("link", { name: "Explorer", exact: true })
    .click()
  await expect(page).toHaveURL(/explorer\/clickbench.*query=Q9/)
  await expect(page.locator("[data-comparison-engine]")).toHaveCount(2)
})

test("same-route deep links restore the requested query and comparison state", async ({ page }) => {
  await page.goto("#/explorer/clickbench?mode=database&query=Q3&databases=duckdb,clickhouse")
  await expect(page.locator("[data-comparison-engine]")).toHaveCount(2)
  await page.evaluate(() => {
    location.hash = "/explorer/clickbench?mode=database&query=Q9&databases=duckdb,monetdb"
  })
  await expect(
    page.locator("[data-comparison-engine]").filter({ hasText: "MonetDB" }),
  ).toBeVisible()
  await expect(page.locator("[data-comparison-engine]")).toHaveCount(2)
  await page.getByRole("button", { name: "Inspect a query", exact: true }).click()
  await expect(page.getByRole("combobox", { name: "Query to inspect", exact: true })).toContainText(
    "Q9",
  )
  await page.keyboard.press("Escape")
  await page.evaluate(() => {
    location.hash = "/explorer/time_series?mode=scale&database=duckdb&scales=1,10"
  })
  await expect(page.getByRole("heading", { name: "Time Series", exact: true })).toBeVisible()
  await expect(page.locator('[data-query-engine="duckdb"]')).toHaveCount(2)
})

test("query matrices sort semantically and open the selected query inspector", async ({ page }) => {
  await page.setViewportSize({ width: 1600, height: 1000 })
  await page.goto("#/explorer/rtabench?mode=database&databases=clickhouse,duckdb,monetdb")
  await page.getByRole("heading", { name: "All query results", exact: true }).click()
  await expect(page.locator("[data-heatmap-block]")).toHaveCount(2)
  const order = () =>
    page
      .locator("[data-query-name]")
      .evaluateAll((elements) => elements.map((el) => el.getAttribute("data-query-name")))
  const original = await order()
  const button = page
    .getByRole("button", { name: "ClickHouse: sort by best relative performance", exact: true })
    .first()
  const series = await button.getAttribute("data-sort-series")
  const buttons = page.locator(`[data-sort-series="${series}"]`)
  await button.click()
  await expect(buttons.first()).toHaveAttribute("data-sort-direction", "best")
  await expect(buttons.first().locator("..")).toHaveAttribute("aria-sort", "ascending")
  expect(await order()).not.toEqual(original)
  await buttons.first().click()
  await expect(buttons.first().locator("..")).toHaveAttribute("aria-sort", "descending")
  await buttons.first().click()
  expect(await order()).toEqual(original)
  const query = page.locator("[data-query-name]").first()
  const name = await query.getAttribute("data-query-name")
  await query.click()
  await expect(
    page.getByRole("dialog", { name: "Query performance and SQL", exact: true }),
  ).toBeVisible()
  await expect(page.getByRole("combobox", { name: "Query to inspect", exact: true })).toContainText(
    name!,
  )
  await page.keyboard.press("Escape")
  await expect(query).toBeFocused()
})

test("failed, partial and unsupported query observations remain distinct from absence", async ({
  page,
}) => {
  const fixture = await makePublicationFixture()
  try {
    await usePublicationFixture(page, fixture.buffer)
    await page.goto(
      "#/explorer/clickbench?mode=database&query=Q2&databases=fast,partial,latestfailed",
    )
    await page.getByRole("button", { name: "Inspect a query", exact: true }).click()
    const dialog = page.getByRole("dialog", { name: "Query performance and SQL", exact: true })
    await expect(dialog.locator('[data-query-engine="fast"]')).toHaveAttribute(
      "data-query-outcome",
      "measured",
    )
    await expect(dialog.locator('[data-query-engine="partial"]')).toHaveAttribute(
      "data-query-outcome",
      "partial",
    )
    await expect(dialog.locator('[data-query-engine="latestfailed"]')).toHaveAttribute(
      "data-query-outcome",
      "not-run",
    )
    await expect(dialog.locator('[data-query-engine="latestfailed"]')).not.toContainText("0 ms")
    await page.keyboard.press("Escape")
    await page.evaluate(() => {
      location.hash =
        "/explorer/clickbench?mode=database&query=Q28&databases=fast,partial,latestfailed"
    })
    await page.getByRole("button", { name: "Inspect a query", exact: true }).click()
    await expect(dialog.locator('[data-query-engine="fast"]')).toHaveAttribute(
      "data-query-outcome",
      "unsupported",
    )
    await expect(dialog.getByLabel("SQL query viewer")).toContainText(/SELECT/i)
  } finally {
    await fixture.cleanup()
  }
})

test("primary results reflow at mobile widths and enlarged text", async ({ page }) => {
  await page.goto("#/explorer/clickbench")
  await expect(page.locator("[data-comparison-lead]")).toBeVisible()
  for (const width of [1440, 768, 390, 320]) {
    await page.setViewportSize({ width, height: 1000 })
    await expectNoHorizontalPageOverflow(page)
    await expect(page.getByLabel("SQL query viewer")).toHaveCount(0)
  }
  await page.addStyleTag({ content: "html { font-size: 200% !important; }" })
  await expectNoHorizontalPageOverflow(page)
  const bounds = await page
    .locator("nav a, [data-publication-score], [data-comparison-engine]")
    .evaluateAll((elements) =>
      elements.map((element) => {
        const box = element.getBoundingClientRect()
        return {
          text: element.textContent,
          left: box.left,
          right: box.right,
          viewport: innerWidth,
          width: element.clientWidth,
          scroll: element.scrollWidth,
        }
      }),
    )
  for (const bound of bounds) {
    expect(bound.left, bound.text ?? "result").toBeGreaterThanOrEqual(0)
    expect(bound.right, bound.text ?? "result").toBeLessThanOrEqual(bound.viewport)
    expect(bound.scroll, bound.text ?? "result").toBeLessThanOrEqual(bound.width + 1)
  }
})

async function expectNoHorizontalPageOverflow(page: Page) {
  const dimensions = await page.evaluate(() => ({
    clientWidth: document.documentElement.clientWidth,
    scrollWidth: document.documentElement.scrollWidth,
    mainWidth: document.querySelector("main")!.clientWidth,
    mainScroll: document.querySelector("main")!.scrollWidth,
  }))
  expect(dimensions.scrollWidth).toBeLessThanOrEqual(dimensions.clientWidth)
  expect(dimensions.mainScroll).toBeLessThanOrEqual(dimensions.mainWidth)
}
