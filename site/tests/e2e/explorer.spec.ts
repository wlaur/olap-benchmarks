import { expect, test } from "@playwright/test"

test("explorer switches comparison modes and keeps query state in the URL", async ({ page }) => {
  const consoleErrors: string[] = []
  page.on("console", (message) => {
    if (message.type() === "error") consoleErrors.push(message.text())
  })

  await page.goto("#/explorer/clickbench?mode=database")

  const suiteSelect = page.getByRole("combobox", { name: "Benchmark suite" })
  await expect(suiteSelect).toContainText("ClickBench")
  await suiteSelect.click()
  await page.getByRole("option", { name: "RTABench", exact: true }).click()
  await expect(page).toHaveURL(/\/explorer\/rtabench/)
  await expect(page.getByRole("heading", { name: "Benchmark results" })).toBeVisible()
  await expect(page.getByRole("combobox", { name: "Benchmark suite" })).toContainText("RTABench")
  await page.getByRole("combobox", { name: "Benchmark suite" }).click()
  await page.getByRole("option", { name: "ClickBench", exact: true }).click()

  await expect(page.getByRole("heading", { name: "Database comparison" })).toBeVisible()
  await expect(page.getByText("Median across 43 shared queries")).toBeVisible()
  await expect(page.getByText("Fastest overall")).toBeVisible()
  await expect(page.getByText("Shorter bars are faster").first()).toBeVisible()

  const overallBarWidths = await page
    .getByRole("meter")
    .evaluateAll((meters) =>
      meters
        .slice(0, 7)
        .map((meter) => meter.firstElementChild?.getBoundingClientRect().width ?? 0),
    )
  expect(overallBarWidths[0]).toBeLessThan(overallBarWidths.at(-1) ?? 0)

  await page.getByRole("button", { name: "Adjust comparison" }).click()
  await expect(page.getByText("Scale factor", { exact: true }).first()).toBeVisible()
  await page.getByRole("button", { name: "Done" }).click()

  await page.getByRole("combobox", { name: "Query" }).click()
  await expect(page.getByRole("option")).toHaveCount(10)
  await page.getByRole("searchbox", { name: "Filter queries" }).fill("Q22")
  await expect(page.getByRole("option", { name: "Q22", exact: true }).locator("mark")).toHaveText(
    "Q22",
  )
  await page.getByRole("option", { name: "Q22", exact: true }).click()
  await expect(page.getByRole("combobox", { name: "Query" })).toContainText("Q22")
  await expect(page).toHaveURL(/query=Q22/)
  await expect(page.getByRole("button", { name: "QuestDB", exact: true })).toHaveAttribute(
    "data-sql-variant",
    "different",
  )
  await expect(page.getByRole("button", { name: "DuckDB", exact: true })).toHaveAttribute(
    "data-sql-variant",
    "default",
  )
  await expect(page.getByText("Differs from default SQL")).toBeVisible()

  const questDbSqlButton = page.getByRole("button", { name: "QuestDB", exact: true })
  await questDbSqlButton.scrollIntoViewIfNeeded()
  const sqlTabsTop = await questDbSqlButton.evaluate((button) => button.getBoundingClientRect().top)
  await questDbSqlButton.click()
  await expect(questDbSqlButton).toHaveAttribute("aria-pressed", "true")
  expect(await questDbSqlButton.evaluate((button) => button.getBoundingClientRect().top)).toBe(
    sqlTabsTop,
  )

  await page.getByRole("button", { name: "Show all 43 query results" }).click()
  const queryNameCell = page.locator('button[data-query-name="Q22"]')
  const firstHeatCell = page.locator('[data-query="Q22"]').first()
  const [queryNameBox, heatCellBox] = await Promise.all([
    queryNameCell.boundingBox(),
    firstHeatCell.boundingBox(),
  ])
  expect(queryNameBox).not.toBeNull()
  expect(heatCellBox).not.toBeNull()
  expect(queryNameBox!.x + queryNameBox!.width).toBeLessThanOrEqual(heatCellBox!.x)
  expect(heatCellBox!.width).toBeLessThanOrEqual(88)

  await page.getByRole("button", { name: "Version", exact: true }).click()
  await expect(page.getByRole("heading", { name: "Version comparison" })).toBeVisible()
  await expect(page).toHaveURL(/mode=version/)
  await expect(page).toHaveURL(/query=Q22/)
  await expectNoHorizontalPageOverflow(page)

  await page.setViewportSize({ width: 390, height: 844 })
  await page.reload()
  await expect(page.getByRole("heading", { name: "Version comparison" })).toBeVisible()
  await expectNoHorizontalPageOverflow(page)
  expect(consoleErrors).toEqual([])
})

test("query heatmap sorts by database and keeps headers aligned", async ({ page }) => {
  await page.setViewportSize({ width: 1600, height: 1000 })
  await page.goto("#/explorer/rtabench?mode=database&databases=clickhouse%2Cduckdb%2Cmonetdb")
  await page.getByRole("button", { name: /Show all \d+ query results/ }).click()

  const blocks = page.locator("[data-heatmap-block]")
  const headerBlocks = page.locator("[data-heatmap-header-block]")
  await expect(blocks).toHaveCount(2)
  await expect(headerBlocks).toHaveCount(2)
  const firstHeaderCount = await headerBlocks.nth(0).locator("[data-heatmap-series-header]").count()
  const secondHeaderCount = await headerBlocks
    .nth(1)
    .locator("[data-heatmap-series-header]")
    .count()
  expect(firstHeaderCount).toBeGreaterThan(0)
  expect(secondHeaderCount).toBe(firstHeaderCount)

  const originalQueryOrder = await page
    .locator("[data-query-name]")
    .evaluateAll((elements) => elements.map((element) => element.getAttribute("data-query-name")))
  const firstClickHouseSort = page
    .getByRole("button", {
      name: /^ClickHouse: sort by best relative performance$/,
    })
    .first()
  const clickHouseSeries = await firstClickHouseSort.getAttribute("data-sort-series")
  expect(clickHouseSeries).not.toBeNull()
  const sortButtons = page.locator(`[data-sort-series="${clickHouseSeries}"]`)
  await sortButtons.first().click()
  await expect(sortButtons).toHaveCount(2)
  await expect(sortButtons.first()).toHaveAttribute("data-sort-direction", "best")
  await expect(sortButtons.last()).toHaveAttribute("data-sort-direction", "best")
  const bestFirstQueryOrder = await page
    .locator("[data-query-name]")
    .evaluateAll((elements) => elements.map((element) => element.getAttribute("data-query-name")))
  expect(bestFirstQueryOrder).not.toEqual(originalQueryOrder)
  await sortButtons.first().click()
  await expect(sortButtons.first()).toHaveAttribute("data-sort-direction", "worst")
  await sortButtons.first().click()
  await expect(sortButtons.first()).toHaveAttribute("data-sort-direction", "none")
  await expect
    .poll(() =>
      page
        .locator("[data-query-name]")
        .evaluateAll((elements) =>
          elements.map((element) => element.getAttribute("data-query-name")),
        ),
    )
    .toEqual(originalQueryOrder)

  await page.setViewportSize({ width: 1024, height: 900 })
  await expect(blocks).toHaveCount(1)
  await expectNoHorizontalPageOverflow(page)

  const stickyPosition = await page.evaluate(async () => {
    const main = document.querySelector("main")
    const header = document.querySelector<HTMLElement>("[data-heatmap-header]")
    if (!(main instanceof HTMLElement) || !header) return null
    header.scrollIntoView({ block: "start" })
    await new Promise(requestAnimationFrame)
    main.scrollTop += 120
    await new Promise(requestAnimationFrame)
    return {
      headerTop: header.getBoundingClientRect().top,
      mainTop: main.getBoundingClientRect().top,
    }
  })
  expect(stickyPosition).not.toBeNull()
  expect(Math.abs(stickyPosition!.headerTop - stickyPosition!.mainTop)).toBeLessThanOrEqual(1)

  await page.setViewportSize({ width: 390, height: 844 })
  const heatmapScroll = page.locator("[data-heatmap-scroll]")
  const heatmapHeader = page.locator("[data-heatmap-header]")
  const verticalScroll = await heatmapScroll.evaluate((element) => ({
    clientHeight: element.clientHeight,
    overflowY: getComputedStyle(element).overflowY,
    scrollHeight: element.scrollHeight,
  }))
  expect(verticalScroll.overflowY).toBe("hidden")
  expect(verticalScroll.scrollHeight).toBe(verticalScroll.clientHeight)
  await heatmapScroll.evaluate((element) => {
    element.scrollLeft = 120
    element.dispatchEvent(new Event("scroll"))
  })
  await expect.poll(() => heatmapHeader.evaluate((element) => element.scrollLeft)).toBe(120)
  await expectNoHorizontalPageOverflow(page)
})

async function expectNoHorizontalPageOverflow(page: import("@playwright/test").Page) {
  const dimensions = await page.evaluate(() => ({
    clientWidth: document.documentElement.clientWidth,
    scrollWidth: document.documentElement.scrollWidth,
  }))
  expect(dimensions.scrollWidth).toBeLessThanOrEqual(dimensions.clientWidth)
}
