import { expect, test } from "@playwright/test"

test("explorer switches comparison modes and keeps query state in the URL", async ({ page }) => {
  const consoleErrors: string[] = []
  page.on("console", (message) => {
    if (message.type() === "error") consoleErrors.push(message.text())
  })

  await page.goto("#/explorer/clickbench?mode=database")

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

  await page.getByRole("button", { name: "Version", exact: true }).click()
  await expect(page.getByRole("heading", { name: "Version comparison" })).toBeVisible()
  await expect(page).toHaveURL(/mode=version/)

  await page.getByRole("combobox", { name: "Query" }).click()
  await page.getByRole("option", { name: "Q22", exact: true }).click()
  await expect(page.getByRole("combobox", { name: "Query" })).toContainText("Q22")
  await expect(page).toHaveURL(/query=Q22/)
  await expectNoHorizontalPageOverflow(page)

  await page.setViewportSize({ width: 390, height: 844 })
  await page.reload()
  await expect(page.getByRole("heading", { name: "Version comparison" })).toBeVisible()
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
