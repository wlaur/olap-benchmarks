import { expect, test } from "@playwright/test"

import {
  comparisonFixture,
  makePublicationFixture,
  usePublicationFixture,
} from "../fixtures/publication"

let fixture: Awaited<ReturnType<typeof makePublicationFixture>>
test.beforeAll(async () => {
  fixture = await makePublicationFixture()
})
test.afterAll(async () => {
  await fixture.cleanup()
})

test("rendered eligibility retains fixed coverage, exclusions and execution unknowns", async ({
  page,
}) => {
  await usePublicationFixture(page, fixture.buffer)
  await page.goto("#/")
  const engine = (db: string) =>
    page.getByRole("listitem", { name: `${db} comparison`, exact: true })
  await expect(engine("fast").locator("[data-publication-score]")).toHaveText("1.00×")
  await expect(engine("slow").locator("[data-publication-score]")).toHaveText("4.00×")
  await expect(engine("fast")).toContainText("2/3")
  await expect(engine("partial")).toContainText("Not eligible")
  await expect(engine("wrong")).toContainText("Not eligible")
  await expect(engine("invalidated")).toContainText("run invalidated")
  await expect(engine("latestfailed")).toContainText("run 8")
  await expect(engine("latestfailed")).toContainText("run failed")
  await expect(engine("preparation")).toContainText("documented incomplete preparation")
  await expect(engine("missing")).toContainText("Execution unknown")
  await expect(engine("missing")).not.toContainText("unsupported")
  await expect(engine("specialist")).toContainText("Adapter unimplemented")
  await engine("fast").getByText("Query execution & validation evidence", { exact: true }).click()
  await expect(engine("fast").getByText("Regex semantics differ", { exact: false })).toBeVisible()
  const excludedQuery = engine("fast").getByText("Q28", { exact: true }).locator("..").locator("..")
  await excludedQuery.getByText("Iterations, statuses & hashes", { exact: true }).click()
  await expect(
    engine("fast")
      .getByText(/recorded exclusion: Adapter has no implementation/)
      .first(),
  ).toBeVisible()
  await engine("missing")
    .getByText(/Run context & preparation/)
    .click()
  await expect(
    engine("missing").getByText("Unknown — not recorded", { exact: true }).first(),
  ).toBeVisible()
  await expect(
    engine("missing").getByText("Preparation unknown; tuning unknown.", { exact: false }),
  ).toBeVisible()
  await page.getByText("Visible engines", { exact: true }).click()
  await page.getByRole("button", { name: "fast", exact: true }).click()
  await expect(engine("slow").locator("[data-publication-score]")).toHaveText("4.00×")
})

test("mismatched comparison metadata fails visibly without publishing scores", async ({ page }) => {
  await usePublicationFixture(page, fixture.buffer, {
    ...comparisonFixture,
    results_db_sha256: "b".repeat(64),
  })
  await page.goto("#/")
  await expect(
    page.getByRole("alert").filter({ hasText: "Failed to load comparison" }),
  ).toContainText("does not match manifest.json")
  await expect(page.locator("[data-publication-score]")).toHaveCount(0)
})

test("explorer keeps Q28 SQL and coverage while excluding it from comparable performance", async ({
  page,
}) => {
  await usePublicationFixture(page, fixture.buffer)
  await page.goto("#/explorer/clickbench?query=Q28")
  await expect(page.getByRole("heading", { name: "Database comparison" })).toBeVisible()
  await expect(page.getByText(/2 fixed performance queries · 3 coverage positions/)).toBeVisible()
  await expect(page.getByRole("combobox", { name: "Query" })).toContainText("Q28")
  await expect(page.getByRole("heading", { name: "Selected query evidence · Q28" })).toBeVisible()
  await expect(page.getByLabel("SQL query viewer")).toContainText(/SELECT/i)
  await expect(
    page
      .getByRole("listitem", { name: "fast comparison", exact: true })
      .locator("[data-publication-score]"),
  ).toHaveText("1.00×")
})

test("explorer visibility preserves the publication baseline and catalog uses registration", async ({
  page,
}) => {
  await usePublicationFixture(page, fixture.buffer)
  await page.goto("#/explorer/clickbench?databases=fast,slow")
  const slow = page.getByRole("listitem", { name: "slow comparison", exact: true })
  await expect(slow.locator("[data-publication-score]")).toHaveText("4.00×")
  await page.getByText("Change analysis").click()
  await page
    .locator("button:not([data-sql-variant])")
    .filter({ hasText: /^fast$/ })
    .click()
  await expect(slow.locator("[data-publication-score]")).toHaveText("4.00×")
  await page.goto("#/catalog?suite=clickbench")
  await page.getByRole("button", { name: "Show coverage", exact: true }).click()
  const missing = page
    .getByRole("row")
    .filter({ has: page.getByRole("rowheader", { name: "missing", exact: true }) })
  await expect(missing).toContainText("Execution unknown")
  const specialist = page
    .getByRole("row")
    .filter({ has: page.getByRole("rowheader", { name: "specialist", exact: true }) })
  await expect(specialist).toContainText("Adapter unimplemented")
})
