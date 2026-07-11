import { expect, test } from "@playwright/test"

test("catalog loads real coverage and keeps selected results visible on mobile", async ({
  page,
}) => {
  const consoleErrors: string[] = []
  page.on("console", (message) => {
    if (message.type() === "error") consoleErrors.push(message.text())
  })

  await page.goto("#/catalog?suite=clickbench")

  await expect(page.getByRole("heading", { name: "Benchmark catalog" })).toBeVisible()
  await expect(page.getByRole("heading", { name: "ClickBench", exact: true })).toBeVisible()
  await expect(page.getByRole("rowheader", { name: "ClickHouse" })).toBeVisible()
  await expect(page.getByText("43/43")).toBeVisible()
  await expectNoHorizontalPageOverflow(page)

  await page.getByRole("button", { name: "Q22", exact: true }).click()
  await page.getByRole("button", { name: "QuestDB", exact: true }).click()
  await expect(page).toHaveURL(/query=Q22/)
  await expect(page).toHaveURL(/sql_database=questdb/)
  await expect(page.getByLabel("SQL query viewer")).toContainText("count_distinct")

  await page.setViewportSize({ width: 390, height: 844 })
  await page.reload()

  await expect(page.getByRole("combobox", { name: "Suite" })).toBeVisible()
  await expect(page.getByRole("heading", { name: "ClickBench", exact: true })).toBeVisible()
  await expect(page.getByRole("link", { name: "Open explorer" })).toBeVisible()
  await expect(page.getByRole("listitem", { name: "ClickHouse coverage" })).toBeVisible()
  await expect(page.getByRole("button", { name: "QuestDB", exact: true })).toHaveAttribute(
    "aria-pressed",
    "true",
  )
  await expectNoHorizontalPageOverflow(page)
  expect(consoleErrors).toEqual([])
})

async function expectNoHorizontalPageOverflow(page: import("@playwright/test").Page) {
  const dimensions = await page.evaluate(() => ({
    clientWidth: document.documentElement.clientWidth,
    scrollWidth: document.documentElement.scrollWidth,
  }))
  expect(dimensions.scrollWidth).toBeLessThanOrEqual(dimensions.clientWidth)
}
