import { expect, test, type Page } from "@playwright/test"

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

test("catalog leads with availability and keeps SQL, coverage and context closed", async ({
  page,
}) => {
  await usePublicationFixture(page, fixture.buffer)
  await page.goto("#/catalog?suite=clickbench&query=Q28")

  await expect(page.getByRole("heading", { name: "Benchmark catalog" })).toBeVisible()
  await expect(
    page.getByRole("heading", { name: "Results recorded for 7 of 9 databases" }),
  ).toBeVisible()
  await expect(page.getByText(/web-traffic hits dataset/)).toBeVisible()
  await expect(page.getByRole("link", { name: "Explore results" })).toHaveAttribute(
    "href",
    /query=Q28/,
  )
  await expect(page.locator("details[open]")).toHaveCount(0)
  await expect(page.getByRole("searchbox", { name: "Filter queries" })).not.toBeVisible()
  await expect(page.getByLabel("SQL query viewer")).toHaveCount(0)
  await expect(page.getByRole("table")).not.toBeVisible()
  await expect(page.getByRole("link", { name: "Workload source" })).not.toBeVisible()

  await expect(page.locator('[data-catalog-engine="partial"]')).toContainText(
    "Partial execution: 1",
  )
  await expect(page.locator('[data-catalog-engine="latestfailed"]')).toContainText("Failed run: 1")
  await expect(page.locator('[data-catalog-engine="invalidated"]')).toContainText(
    "Invalidated run: 1",
  )
  await expect(page.locator('[data-catalog-engine="missing"]')).toContainText("No recorded run")
  await expect(page.locator('[data-catalog-engine="missing"]')).not.toContainText("unsupported")
  await expect(page.locator('[data-catalog-engine="specialist"]')).toContainText(
    "Adapter unimplemented",
  )

  await disclosure(page, "Coverage by scale and system").click()
  const coverage = page.getByRole("region", { name: "Coverage by database and scale" })
  const fast = coverage
    .getByRole("row")
    .filter({ has: page.getByRole("rowheader", { name: "fast", exact: true }) })
  await expect(fast).toContainText("Partial execution · 2/3 queries")
  await expect(fast).toContainText("1 unsupported")
  await expect(coverage.locator('[data-catalog-run="8"]')).toContainText("Failed run · 0/3 queries")
  await expect(coverage.locator('[data-catalog-run="6"]')).toHaveCount(0)
  await expectNoHorizontalPageOverflow(page)
})

test("catalog recognizes complete execution without turning it into correctness", async ({
  page,
}) => {
  await usePublicationFixture(page, fixture.buffer, {
    ...comparisonFixture,
    families: comparisonFixture.families.map((family) => ({
      ...family,
      coverage_query_names: ["Q1", "Q2"],
    })),
  })
  await page.goto("#/catalog?suite=clickbench")
  await expect(page.locator('[data-catalog-engine="fast"]')).toContainText("Complete execution: 1")
  await expect(page.locator('[data-catalog-engine="partial"]')).toContainText(
    "Partial execution: 1",
  )
  await expect(
    page.getByText(/Complete execution does not establish correctness or ranking eligibility/),
  ).toBeVisible()
})

test("catalog SQL requires a request and preserves query and dialect deep links", async ({
  page,
}) => {
  await page.goto("#/catalog?suite=clickbench")
  await expect(page.getByRole("heading", { name: "ClickBench", exact: true })).toBeVisible()
  await disclosure(page, "Queries and SQL").click()
  await page.getByRole("searchbox", { name: "Filter queries" }).fill("Q22")
  await page.getByRole("button", { name: "Q22", exact: true }).click()
  await expect(page.getByLabel("SQL query viewer")).toHaveCount(0)
  await page.getByRole("button", { name: "View SQL", exact: true }).click()
  const dialog = page.getByRole("dialog", { name: "SQL · Q22" })
  await dialog.getByRole("button", { name: "QuestDB", exact: true }).click()
  await expect(page).toHaveURL(/query=Q22/)
  await expect(page).toHaveURL(/sql_database=questdb/)
  await expect(dialog.getByLabel("SQL query viewer")).toContainText("count_distinct")
  await page.keyboard.press("Escape")
  await expect(page.getByRole("button", { name: "View SQL", exact: true })).toBeFocused()
  await expect(page.getByRole("link", { name: "Explore results" })).toHaveAttribute(
    "href",
    /sql_database=questdb/,
  )

  await page.reload()
  await expect(page.getByRole("heading", { name: "ClickBench", exact: true })).toBeVisible()
  await expect(page.locator("details[open]")).toHaveCount(0)
  await expect(page.getByLabel("SQL query viewer")).toHaveCount(0)
  await disclosure(page, "Queries and SQL").click()
  await page.getByRole("button", { name: "View SQL", exact: true }).click()
  await expect(
    page.getByRole("dialog").getByRole("button", { name: "QuestDB", exact: true }),
  ).toHaveAttribute("aria-pressed", "true")
})

test("mobile catalog exposes its conclusion and supports keyboard query inspection", async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 844 })
  await usePublicationFixture(page, fixture.buffer)
  await page.goto("#/catalog?suite=clickbench")
  await expect(page.getByRole("combobox", { name: "Suite" })).toBeVisible()
  await expect(
    page.getByRole("heading", { name: "Results recorded for 7 of 9 databases" }),
  ).toBeInViewport()
  await expect(page.getByRole("link", { name: "Explore results" })).toBeInViewport()
  await expectNoHorizontalPageOverflow(page)

  const queries = disclosure(page, "Queries and SQL")
  await queries.focus()
  await page.keyboard.press("Enter")
  const filter = page.getByRole("searchbox", { name: "Filter queries" })
  await filter.fill("Q2")
  const selected = page.getByRole("button", { name: "Q2", exact: true })
  await selected.focus()
  await page.keyboard.press("Enter")
  await expect(selected).toHaveAttribute("aria-pressed", "true")
  const viewSql = page.getByRole("button", { name: "View SQL", exact: true })
  await viewSql.focus()
  await page.keyboard.press("Enter")
  await expect(page.getByRole("dialog", { name: "SQL · Q2" })).toBeVisible()
  await expect(page.getByLabel("SQL query viewer")).toContainText("SELECT 1")
  await page.keyboard.press("Escape")
  await expect(viewSql).toBeFocused()
  await filter.fill("not-a-query")
  await expect(page.getByText("No matching queries.")).toBeVisible()
  await page.getByRole("button", { name: "Clear query filter" }).click()
  await expect(filter).toHaveValue("")
  await page.setViewportSize({ width: 320, height: 700 })
  await expectNoHorizontalPageOverflow(page)
})

test("changing suites resets details and prepared query links retain their track", async ({
  page,
}) => {
  await page.goto("#/catalog?suite=clickbench&query=Q22&sql_database=questdb")
  await disclosure(page, "Queries and SQL").click()
  await page.getByRole("combobox", { name: "Suite" }).click()
  await page.getByRole("option", { name: "JSON Representation", exact: true }).click()
  await expect(page).toHaveURL(/suite=json_representation$/)
  await expect(page.locator("details[open]")).toHaveCount(0)
  await expect(page.getByText("Not included", { exact: true })).toBeVisible()
  await disclosure(page, "Queries and SQL").click()
  await page.getByRole("button", { name: "prepared_01_events_by_collection", exact: true }).click()
  const explore = page.getByRole("link", { name: "Explore results" })
  await expect(explore).toHaveAttribute("href", /family=json_representation_prepared/)
  await expect(explore).toHaveAttribute("href", /query=prepared_01_events_by_collection/)
  await expect(page.getByLabel("SQL query viewer")).toHaveCount(0)
})

test("catalog keeps its overview shell during loading", async ({ page }) => {
  await usePublicationFixture(page, fixture.buffer)
  let release: () => void = () => {}
  const gate = new Promise<void>((resolve) => {
    release = resolve
  })
  await page.route("**/data/comparison.json", async (route) => {
    await gate
    await route.fulfill({ json: comparisonFixture })
  })
  await page.goto("#/catalog?suite=clickbench")
  const overview = page.getByRole("region", { name: "Workload overview" })
  await expect(overview).toHaveAttribute("aria-busy", "true")
  await expect(overview.getByRole("status")).toContainText("Loading workload results")
  await expect(page.getByText("No recorded query runs for this workload")).not.toBeVisible()
  release()
  await expect(overview).toHaveAttribute("aria-busy", "false")
  await expect(overview.getByRole("list", { name: "Database availability" })).toBeVisible()
})

test("catalog has a distinct empty state for no configured suites", async ({ page }) => {
  await usePublicationFixture(page, fixture.buffer)
  await page.route("**/data/suites.json", (route) => route.fulfill({ json: { suites: [] } }))
  await page.goto("#/catalog")
  await expect(page.getByText("No benchmark suites are configured.")).toBeVisible()
  await expect(page.getByText("Loading workload results…")).not.toBeVisible()
  await expect(page.getByRole("list", { name: "Database availability" })).not.toBeVisible()
})

test("catalog load errors do not masquerade as missing measurements", async ({ page }) => {
  await usePublicationFixture(page, fixture.buffer, {
    ...comparisonFixture,
    results_db_sha256: "b".repeat(64),
  })
  await page.goto("#/catalog?suite=clickbench")
  await expect(
    page.getByRole("alert").filter({ hasText: "Catalog results could not be loaded" }),
  ).toBeVisible()
  await expect(page.getByRole("button", { name: "Retry loading catalog" })).toBeVisible()
  await expect(page.getByRole("list", { name: "Database availability" })).not.toBeVisible()
  await expect(page.getByText(/does not match manifest.json/)).not.toBeVisible()
  await disclosure(page, "Load error details").click()
  await expect(page.getByText(/does not match manifest.json/)).toBeVisible()
})

function disclosure(page: Page, title: string) {
  return page.locator("summary").filter({ hasText: title })
}

async function expectNoHorizontalPageOverflow(page: Page) {
  const dimensions = await page.evaluate(() => ({
    clientWidth: document.documentElement.clientWidth,
    scrollWidth: document.documentElement.scrollWidth,
  }))
  expect(dimensions.scrollWidth).toBeLessThanOrEqual(dimensions.clientWidth)
}
