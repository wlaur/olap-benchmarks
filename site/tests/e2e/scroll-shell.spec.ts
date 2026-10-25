import { expect, test, type Page } from "@playwright/test"

async function expectFixedShell(page: Page, label: string) {
  const shell = await page.evaluate(() => ({
    scrollY: window.scrollY,
    documentHeight: document.documentElement.scrollHeight,
    bodyHeight: document.body.scrollHeight,
    viewportHeight: window.innerHeight,
    navTop: document.querySelector("header")!.getBoundingClientRect().top,
    mainBottom: document.querySelector("main")!.getBoundingClientRect().bottom,
  }))
  expect(shell.scrollY, `${label}: window scroll`).toBe(0)
  expect(shell.documentHeight, `${label}: document height`).toBe(shell.viewportHeight)
  expect(shell.bodyHeight, `${label}: body height`).toBe(shell.viewportHeight)
  expect(shell.navTop, `${label}: nav top`).toBe(0)
  expect(Math.round(shell.mainBottom), `${label}: main bottom`).toBe(shell.viewportHeight)
}

const mainScrollTop = (page: Page) => page.locator("main").evaluate((main) => main.scrollTop)

for (const [width, height] of [
  [1440, 900],
  [390, 844],
  [320, 700],
] as const)
  test(`only main scrolls at ${width} px`, async ({ page }) => {
    await page.setViewportSize({ width, height })
    await page.goto("#/")
    await expect(page.getByRole("heading", { name: "Overall database ranking" })).toBeVisible()
    await expectFixedShell(page, "initial")

    await page.mouse.move(10, 10)
    await page.mouse.wheel(0, 800)
    await expectFixedShell(page, "wheel over header")

    await page.mouse.move(width / 2, height / 2)
    await page.mouse.wheel(0, 20_000)
    await expect.poll(() => mainScrollTop(page)).toBeGreaterThan(0)
    await expectFixedShell(page, "main scrolled to end")

    const row = page.locator('[data-overall-engine="monetdb"]')
    await row.locator("summary").first().click()
    await row.getByRole("button", { name: "Compare ClickBench by workload", exact: true }).click()
    await expect(page.locator("[data-comparison-lead]")).toBeInViewport()
    await expectFixedShell(page, "workload drilldown")

    await page
      .locator('main button[aria-haspopup="dialog"]', { hasText: /^Details$/ })
      .first()
      .click()
    const dialog = page.getByRole("dialog")
    await expect(dialog).toBeVisible()
    await page.mouse.wheel(0, 800)
    await expectFixedShell(page, "dialog open")
    await page.keyboard.press("Escape")
    await expect(dialog).toBeHidden()

    for (const link of ["Explorer", "Catalog", "Home"]) {
      await page.locator("header").getByRole("link", { name: link, exact: true }).click()
      await expect(page.locator("main")).toBeVisible()
      await expectFixedShell(page, `route ${link}`)
    }
  })

test("route changes reset the main scroller while same-route updates keep it", async ({ page }) => {
  await page.goto("#/?view=workload&family=tpc_h")
  await expect(page.locator("[data-comparison-lead]")).toBeVisible()
  await page.mouse.move(720, 450)
  await page.mouse.wheel(0, 600)
  await expect.poll(() => mainScrollTop(page)).toBeGreaterThan(300)
  const before = await mainScrollTop(page)

  await page.evaluate(() => {
    window.location.hash = "#/?view=workload&family=tpc_h&scale=50"
  })
  await expect(page).toHaveURL(/scale=50/)
  expect(await mainScrollTop(page)).toBe(before)

  await page.locator("header").getByRole("link", { name: "Explorer", exact: true }).click()
  await expect(page).toHaveURL(/#\/explorer\//)
  await expect.poll(() => mainScrollTop(page)).toBe(0)
  await expect(page.getByRole("heading", { level: 1 })).toBeInViewport()

  await page.locator("header").getByRole("link", { name: "Catalog", exact: true }).click()
  await expect(page).toHaveURL(/#\/catalog/)
  await expect.poll(() => mainScrollTop(page)).toBe(0)
  await expect(page.getByRole("heading", { name: /^Results recorded for/ })).toBeVisible()
  await page.mouse.move(720, 450)
  await page.mouse.wheel(0, 600)
  await expect.poll(() => mainScrollTop(page)).toBeGreaterThan(0)
  await page.locator("header").getByRole("link", { name: "Home", exact: true }).click()
  await expect(page.getByRole("heading", { name: "Overall database ranking" })).toBeVisible()
  await expect.poll(() => mainScrollTop(page)).toBe(0)
})

const activeInfo = (page: Page) =>
  page.evaluate(() => {
    const active = document.activeElement
    const dialog = document.querySelector("dialog[open]")
    return {
      inside: Boolean(dialog && active && dialog.contains(active)),
      label: active?.getAttribute("aria-label") ?? active?.textContent?.trim() ?? "",
    }
  })

test("modal Tab and Shift+Tab stay inside the dialog and Escape restores focus", async ({
  page,
}) => {
  await page.goto("#/")
  const trigger = page.getByRole("button", { name: "How the score works" })
  await trigger.focus()
  await page.keyboard.press("Enter")
  const dialog = page.getByRole("dialog", { name: "Overall ranking method" })
  await expect(dialog.getByRole("button", { name: "Close" })).toBeFocused()

  for (let index = 0; index < 4; index++) {
    await page.keyboard.press("Tab")
    expect((await activeInfo(page)).inside, `Tab ${index + 1}`).toBe(true)
  }
  await expect(dialog.getByRole("button", { name: "Close" })).toBeFocused()

  await page.keyboard.press("Shift+Tab")
  await expect(dialog.getByRole("link", { name: "Full ranking methodology" })).toBeFocused()
  await page.keyboard.press("Shift+Tab")
  await expect(dialog.getByRole("button", { name: "Close" })).toBeFocused()

  await page.keyboard.press("Escape")
  await expect(dialog).toBeHidden()
  await expect(trigger).toBeFocused()
})

test("Tab from a query picker inside a dialog continues after the picker", async ({ page }) => {
  await page.goto("#/explorer/time_series")
  await page.getByRole("button", { name: "Inspect a query" }).click()
  const dialog = page.getByRole("dialog", { name: "Query performance and SQL" })
  await expect(dialog).toBeVisible()
  const picker = dialog.getByRole("combobox").first()
  await picker.click()
  const search = dialog.getByRole("searchbox")
  await expect(search).toBeFocused()

  await page.keyboard.press("Tab")
  await expect(picker).toHaveAttribute("aria-expanded", "false")
  expect((await activeInfo(page)).inside).toBe(true)
  const followsPicker = await picker.evaluate(
    (element) =>
      document.activeElement !== element &&
      Boolean(
        element.compareDocumentPosition(document.activeElement!) & Node.DOCUMENT_POSITION_FOLLOWING,
      ),
  )
  expect(followsPicker).toBe(true)
  await expect(dialog.getByRole("button", { name: "Close" })).not.toBeFocused()

  await picker.click()
  await expect(search).toBeFocused()
  await page.keyboard.press("Escape")
  await expect(dialog).toBeVisible()
  await expect(picker).toBeFocused()
})

test("dialogs opened from right-aligned table cells keep left-aligned prose", async ({ page }) => {
  await page.goto("#/")
  await page.getByText("Analytical concurrency evidence", { exact: true }).click()
  const trigger = page
    .locator('td button[aria-haspopup="dialog"]', { hasText: /^Details$/ })
    .first()
  await trigger.click()
  const dialog = page.locator("dialog[open]")
  await expect(dialog).toBeVisible()
  expect(await dialog.evaluate((element) => getComputedStyle(element).textAlign)).toBe("left")
  const paragraph = dialog.locator("p").first()
  expect(await paragraph.evaluate((element) => getComputedStyle(element).textAlign)).toBe("left")
})
