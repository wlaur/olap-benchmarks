import { expect, test } from "@playwright/test"

import type { Publication } from "../../src/lib/publication"

test("publication scope stays visible across the results pages", async ({ page }) => {
  const response = await page.request.get("data/manifest.json")
  expect(response.ok()).toBe(true)
  const { publication } = (await response.json()) as { publication: Publication }

  for (const route of ["#/", "#/explorer/tpc_h?scale=50", "#/catalog?suite=tpc_h"]) {
    await page.goto(route)
    const report = page.getByRole("link", { name: "Coverage & exclusions", exact: true })
    if (publication.kind === "matrix") {
      await expect(report).toHaveCount(0)
      continue
    }

    await expect(report).toBeVisible()
    await expect(report).toHaveAttribute("href", publication.report_url)
    const notice = page.getByRole("alert").filter({ has: report })
    await expect(notice).toContainText(`${publication.select_cohorts} query runs`)
    await expect(notice).toContainText(`${publication.partial_select_cohorts} are partial`)
    await expect(notice).toContainText("penalties for missing results")
  }
})
