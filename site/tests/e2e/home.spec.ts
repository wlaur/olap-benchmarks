import { expect, test } from "@playwright/test"

test("home scores load and link into the production explorer", async ({ page }) => {
  const consoleErrors: string[] = []
  page.on("console", (message) => {
    if (message.type() === "error") consoleErrors.push(message.text())
  })

  await page.goto("#/")

  await expect(page.getByRole("heading", { name: "Database ranking" })).toBeVisible()
  await expect(page.getByRole("cell", { name: /duckdb/i }).first()).toBeVisible()

  await page.getByRole("button", { name: "ClickBench SF1" }).click()
  await expect(page).toHaveURL(/\/explorer\/clickbench\?.*scale=1/)
  await expect(page).toHaveURL(/system=macbook-m4-pro/)
  await expect(page.getByRole("heading", { name: "Database ranking" })).toBeVisible()
  await expect(page.getByText("Score across 43 queries")).toBeVisible()
  expect(consoleErrors).toEqual([])
})

test("findings open the intended databases, scale, query and SQL", async ({ page }) => {
  await page.goto("#/")
  const links = await page.locator('a[href^="#/explorer/"]').evaluateAll((anchors) =>
    anchors.map((anchor) => ({
      text: anchor.textContent ?? "",
      href: anchor.getAttribute("href") ?? "",
    })),
  )
  expect(links.length).toBeGreaterThan(0)

  for (const link of links) {
    await test.step(link.text, async () => {
      await page.goto("#/")
      await page.getByRole("link", { name: link.text, exact: true }).click()
      const [path, search] = link.href.split("?")
      const expected = new URLSearchParams(search)
      await expect(page.getByRole("heading", { name: "Database ranking" })).toBeVisible()
      await expect(page).toHaveURL((url) => {
        const [actualPath, actualSearch] = url.hash.split("?")
        const actual = new URLSearchParams(actualSearch)
        return (
          actualPath === path &&
          [...expected].every(([key, value]) =>
            key === "databases"
              ? actual.get(key)?.split(",").sort().join(",") === value.split(",").sort().join(",")
              : actual.get(key) === value,
          )
        )
      })
      const databases = expected.get("databases")?.split(",")
      if (databases) {
        await expect(page.locator("[data-score-ranking-row]")).toHaveCount(databases.length)
      }
      const query = expected.get("query")
      if (query) {
        await expect(page.getByRole("combobox", { name: "Query" })).toContainText(query)
      }
      const sqlDatabase = expected.get("sql_database")
      if (sqlDatabase) {
        await expect(page.locator('button[data-sql-variant][aria-pressed="true"]')).toHaveText(
          new RegExp(sqlDatabase, "i"),
        )
      }
    })
  }
})

test("home scores fit desktop and switch to cards on smaller screens", async ({ page }) => {
  const consoleErrors: string[] = []
  page.on("console", (message) => {
    if (message.type() === "error") consoleErrors.push(message.text())
  })

  await page.setViewportSize({ width: 1440, height: 900 })
  await page.goto("#/")
  await expect(page.getByRole("heading", { name: "Database ranking" })).toBeVisible()
  await expectTableToFit(page)
  await expectNoHorizontalPageOverflow(page)

  await page.setViewportSize({ width: 1024, height: 768 })
  await page.reload()
  await expect(page.locator("table")).toBeVisible()
  await expectTableToFit(page)
  await expectNoHorizontalPageOverflow(page)

  await page.setViewportSize({ width: 768, height: 900 })
  await page.reload()
  await expect(page.locator("table")).toBeHidden()
  await expect(page.getByRole("list", { name: "Suite scores by database" })).toBeVisible()
  await expect(page.getByRole("listitem", { name: /duckdb scores/i })).toBeVisible()
  await expectNoHorizontalPageOverflow(page)

  await page.setViewportSize({ width: 390, height: 844 })
  await page.reload()
  await expect(page.getByRole("list", { name: "Suite scores by database" })).toBeVisible()
  await expectNoHorizontalPageOverflow(page)
  expect(consoleErrors).toEqual([])
})

async function expectTableToFit(page: import("@playwright/test").Page) {
  const dimensions = await page.locator("table").evaluate((table) => {
    const scroller = table.parentElement
    return {
      clientWidth: scroller?.clientWidth ?? 0,
      scrollWidth: scroller?.scrollWidth ?? 0,
    }
  })
  expect(dimensions.scrollWidth).toBeLessThanOrEqual(dimensions.clientWidth)
}

async function expectNoHorizontalPageOverflow(page: import("@playwright/test").Page) {
  const dimensions = await page.evaluate(() => ({
    clientWidth: document.documentElement.clientWidth,
    scrollWidth: document.documentElement.scrollWidth,
  }))
  expect(dimensions.scrollWidth).toBeLessThanOrEqual(dimensions.clientWidth)
}
