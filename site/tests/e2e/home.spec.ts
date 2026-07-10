import { expect, test } from "@playwright/test"

test("home scores load and link into the production explorer", async ({ page }) => {
  const consoleErrors: string[] = []
  page.on("console", (message) => {
    if (message.type() === "error") consoleErrors.push(message.text())
  })

  await page.goto("#/")

  await expect(page.getByRole("heading", { name: "Suite scores" })).toBeVisible()
  await expect(page.getByRole("cell", { name: /duckdb/i }).first()).toBeVisible()

  await page.getByRole("button", { name: "ClickBench SF1" }).click()
  await expect(page).toHaveURL(/\/explorer\/clickbench\?scale=1/)
  await expect(page.getByRole("heading", { name: "Database comparison" })).toBeVisible()
  await expect(page.getByText("Median across 43 shared queries")).toBeVisible()
  expect(consoleErrors).toEqual([])
})
