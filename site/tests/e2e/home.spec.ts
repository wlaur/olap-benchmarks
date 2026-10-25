import { expect, test, type Locator, type Page } from "@playwright/test"

async function selectWorkload(page: Page, name: string | RegExp) {
  await page.getByRole("combobox", { name: "Workload" }).click()
  await page
    .getByRole("option", typeof name === "string" ? { name, exact: true } : { name })
    .click()
}

test("home leads with a data-derived ranking and keeps supporting details closed", async ({
  page,
}) => {
  await page.goto("#/")
  const lead = page.locator("[data-overall-lead]")
  await expect(lead).toContainText("Top score: DuckDB · 79.9/100 · 100% weighted coverage")
  await expect(lead).toContainText("score 0")
  await expect(page.locator("[data-weighted-coverage-definition]")).toHaveText(
    /^Weighted coverage: share of performance queries with a usable result/,
  )
  const row = page.locator('[data-overall-engine="duckdb"]')
  await expect(row.locator("[data-overall-score]")).toBeVisible()
  await expect(row.locator("summary").first()).toContainText("100% weighted coverage")
  await expect(row.locator("[data-overall-coverage]")).toBeHidden()
  await expect(row.locator("[data-overall-queries]")).toBeHidden()
  await expect(page.getByRole("dialog")).toHaveCount(0)
  await expect(page.getByRole("alert")).toHaveCount(0)
  const concurrency = page.getByRole("table", { name: /^Analytical stream measurements/ }).first()
  await expect(concurrency).toBeHidden()
  await expect(page.getByText("October 2026 · M4 Pro", { exact: true })).toBeHidden()

  await row.locator("summary").first().focus()
  await page.keyboard.press("Enter")
  await expect(row.locator("[data-overall-coverage]")).toBeVisible()
  await expect(row.locator("[data-overall-queries]")).toHaveText("871 of 871")
  await expect(row.getByText(/Scored run IDs/).first()).toBeHidden()

  const method = page.getByRole("button", { name: "How the score works" })
  await method.focus()
  await page.keyboard.press("Enter")
  const dialog = page.getByRole("dialog", { name: "Overall ranking method" })
  await expect(dialog).toBeVisible()
  await expect(dialog.getByText(/^Each query earns 100 × fastest median/)).toBeVisible()
  await expect(
    dialog.getByText(/^Weighted coverage is the share of performance queries/),
  ).toBeVisible()
  await page.keyboard.press("Escape")
  await expect(dialog).toBeHidden()
  await expect(method).toBeFocused()

  await page.getByRole("heading", { name: "Analytical concurrency evidence", exact: true }).click()
  await expect(concurrency).toBeVisible()
  await page.getByRole("heading", { name: "About this campaign", exact: true }).click()
  await expect(page.getByText("October 2026 · M4 Pro", { exact: true })).toBeVisible()
  const about = page.locator("section", { has: page.getByText("October 2026 · M4 Pro") }).last()
  const notes = await about.locator(".prose").innerText()
  expect(notes.split(/\s+/).length).toBeLessThanOrEqual(220)
  expect(notes).not.toMatch(/\b(has the lowest|leads|regressed)\b/)
  await expect(about.getByRole("link", { name: "Full method" })).toBeVisible()
  await expect(about.getByRole("link", { name: "follow-up report" })).toBeVisible()
})

test("home compares fixed families and opens the selected explorer", async ({ page }) => {
  const errors: string[] = []
  page.on("pageerror", (error) => errors.push(error.message))
  await page.goto("#/")
  await expect(page.getByRole("heading", { name: "Overall database ranking" })).toBeVisible()
  await page.getByRole("button", { name: "By workload", exact: true }).click()
  await expect(page.getByRole("heading", { name: "Compare one workload" })).toBeVisible()
  await page.getByRole("combobox", { name: "Workload" }).click()
  const clickbench = page.getByRole("option", { name: "ClickBench", exact: true })
  await expect(clickbench).toHaveAccessibleDescription(/web-traffic hits dataset/)
  await clickbench.click()
  await expect(page.getByRole("combobox", { name: "Workload" })).not.toContainText("web-traffic")
  await expect(
    page.getByText("42 performance queries · 43 execution queries · raw track"),
  ).toBeVisible()
  await expect(page.locator("[data-comparison-lead]")).toContainText("Lowest latency ratio:")
  await expect(
    page.getByRole("listitem", { name: "PostgreSQL comparison", exact: true }),
  ).toContainText("documented incomplete preparation")
  await page.getByRole("button", { name: "Open in Explorer", exact: true }).click()
  await expect(page).toHaveURL(/\/explorer\/clickbench\?.*scale=1/)
  expect(errors).toEqual([])
})

test("workload evidence opens in a keyboard-accessible dialog", async ({ page }) => {
  await page.goto("#/")
  await page.getByRole("button", { name: "By workload", exact: true }).click()
  await selectWorkload(page, "ClickBench")
  const engine = page.getByRole("listitem", { name: "DuckDB comparison", exact: true })
  await expect(engine.locator("[data-publication-score]")).toBeVisible()
  await expect(engine.getByText(/Run context & preparation/)).toHaveCount(0)
  const trigger = engine.getByRole("button", { name: "DuckDB evidence details" })
  await trigger.focus()
  await page.keyboard.press("Enter")
  const dialog = page.getByRole("dialog", { name: "DuckDB · ClickBench evidence" })
  await expect(dialog).toBeVisible()
  await expect(dialog.getByRole("button", { name: "Close" })).toBeFocused()
  await dialog.getByText(/Run context & preparation/).click()
  await expect(dialog.getByText("Input fingerprint", { exact: true })).toBeVisible()
  await page.keyboard.press("Escape")
  await expect(dialog).toBeHidden()
  await expect(trigger).toBeFocused()
})

test("score headers and evidence fit desktop, iPad and mobile widths", async ({
  page,
}, testInfo) => {
  await page.goto("#/")
  await page.getByRole("button", { name: "By workload", exact: true }).click()
  await selectWorkload(page, "RTABench")
  await expect(
    page.getByText("31 performance queries · 31 execution queries · raw track"),
  ).toBeVisible()
  await selectWorkload(page, /RTABench prepared aggregates/)
  await expect(
    page.getByText("10 performance queries · 10 execution queries · prepared track"),
  ).toBeVisible()
  await selectWorkload(page, "ClickBench")
  for (const width of [1920, 1440, 1194, 1024, 834, 768, 390]) {
    await page.setViewportSize({ width, height: 900 })
    await expect(page.getByRole("list", { name: "Family engine comparison" })).toBeVisible()
    await expectNoHorizontalPageOverflow(page)
    await page.getByRole("button", { name: "DuckDB evidence details" }).click()
    const dialog = page.getByRole("dialog", { name: "DuckDB · ClickBench evidence" })
    await dialog.getByText(/Run context & preparation/).click()
    await expect(dialog.getByText("Input fingerprint", { exact: true })).toBeVisible()
    await expectNoHorizontalPageOverflow(page)
    await dialog.getByRole("button", { name: "Close" }).click()
    await expect(dialog).toBeHidden()
    if ([1440, 1024, 390].includes(width)) {
      await page.locator("main").evaluate((main) => {
        main.scrollTop = 0
      })
      await page.screenshot({ path: testInfo.outputPath(`family-${width}.png`) })
    }
  }
})

test("workload drilldown retains strict family eligibility and selected scale", async ({
  page,
}) => {
  await page.goto("#/")
  await page.getByRole("button", { name: "By workload", exact: true }).click()
  await page.getByRole("button", { name: "How workload ratios work" }).click()
  await expect(page.getByRole("dialog", { name: "Workload comparison method" })).toBeVisible()
  await page.keyboard.press("Escape")
  await selectWorkload(page, "Time Series")
  await page.getByRole("button", { name: "SF 10", exact: true }).click()
  await expect(page.getByRole("button", { name: "SF 10", exact: true })).toHaveAttribute(
    "aria-pressed",
    "true",
  )
  await page.getByRole("button", { name: "Open in Explorer", exact: true }).click()
  await expect(page).toHaveURL(/scale=10/)
})

test("home view, workload and scale survive reload, Back and shared links", async ({ page }) => {
  await page.goto("#/")
  await expect(page.getByRole("button", { name: "Overall", exact: true })).toHaveAttribute(
    "aria-pressed",
    "true",
  )
  await page.getByRole("button", { name: "By workload", exact: true }).click()
  await expect(page).toHaveURL(/view=workload/)
  await selectWorkload(page, "ClickBench")
  await expect(page).toHaveURL(/family=clickbench/)
  await selectWorkload(page, "Time Series")
  await expect(page).toHaveURL(/family=time_series(&|$)/)
  await page.getByRole("button", { name: "SF 10", exact: true }).click()
  await expect(page).toHaveURL(/view=workload&family=time_series&scale=10/)
  await page.reload()
  await expect(page.getByRole("combobox", { name: "Workload" })).toContainText("Time Series")
  await expect(page.getByRole("button", { name: "SF 10", exact: true })).toHaveAttribute(
    "aria-pressed",
    "true",
  )
  await page.goBack()
  await expect(page.getByRole("button", { name: "SF 1", exact: true })).toHaveAttribute(
    "aria-pressed",
    "true",
  )
  await page.getByRole("button", { name: "Overall", exact: true }).click()
  await expect(page).not.toHaveURL(/view=/)
  await expect(page.getByRole("heading", { name: "Overall database ranking" })).toBeVisible()
  await page.goBack()
  await expect(page.getByRole("heading", { name: "Compare one workload" })).toBeVisible()

  await page.goto("#/?view=workload&family=clickbench")
  await expect(page.getByRole("combobox", { name: "Workload" })).toContainText("ClickBench")
  await page.goto("#/")
  await page.locator('[data-overall-engine="monetdb"] summary').first().click()
  await page.getByRole("button", { name: "Compare ClickBench by workload", exact: true }).click()
  await expect(page).toHaveURL(/view=workload&family=clickbench/)
  await page.goBack()
  await expect(page.getByRole("heading", { name: "Overall database ranking" })).toBeVisible()
})

async function expectNoHorizontalPageOverflow(page: Page) {
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth - document.documentElement.clientWidth,
    ),
  ).toBeLessThanOrEqual(0)
}

async function expectWithinViewport(page: Page, locator: Locator, label: string) {
  const width = page.viewportSize()!.width
  const box = await locator.boundingBox()
  expect(box, label).not.toBeNull()
  expect(box!.x, `${label} left edge`).toBeGreaterThanOrEqual(-0.5)
  expect(box!.x + box!.width, `${label} right edge`).toBeLessThanOrEqual(width + 0.5)
}

for (const rootSize of ["100%", "200%"])
  test(`home ranking, navigation and workload menu fit 320 px at ${rootSize} text`, async ({
    page,
  }) => {
    await page.setViewportSize({ width: 320, height: 720 })
    await page.goto("#/")
    await page.addStyleTag({ content: `html { font-size: ${rootSize} }` })
    const navigation = page.getByRole("navigation", { name: "Primary" })
    for (const name of ["Home", "Explorer", "Catalog"])
      await expectWithinViewport(page, navigation.getByRole("link", { name }), `${name} link`)

    const rows = page.locator("[data-overall-engine]")
    await expect(rows).toHaveCount(11)
    if (rootSize === "100%") {
      const first = await rows.first().boundingBox()
      expect(first!.y + first!.height).toBeLessThanOrEqual(720)
    }
    for (const [db, name] of [
      ["duckdb", "DuckDB"],
      ["timescaledb", "TimescaleDB"],
    ] as const) {
      const summary = page.locator(`[data-overall-engine="${db}"] > details > summary`)
      const title = summary.getByText(name, { exact: true })
      await expectWithinViewport(page, title, `${name} name`)
      const titleBox = (await title.boundingBox())!
      const lineHeight = await title.evaluate((element) =>
        Number.parseFloat(getComputedStyle(element).lineHeight),
      )
      expect(titleBox.height, `${name} name wraps at most once`).toBeLessThanOrEqual(
        lineHeight * 2 + 1,
      )
      await expectWithinViewport(page, summary.locator("[data-overall-score]"), `${name} score`)
      await expectWithinViewport(
        page,
        summary.locator("[data-overall-coverage-summary]"),
        `${name} coverage`,
      )
      await expect(summary).toContainText("weighted coverage")
    }

    await page.getByRole("button", { name: "By workload", exact: true }).click()
    const workload = page.getByRole("combobox", { name: "Workload" })
    await expectWithinViewport(page, workload, "workload selector")
    await workload.click()
    const menu = page.getByRole("listbox")
    await expect(menu).toBeVisible()
    await expectWithinViewport(page, menu, "workload menu")
    await page.keyboard.press("Escape")
  })
