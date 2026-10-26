import { createHash } from "node:crypto"

import { expect, test } from "@playwright/test"

import {
  comparisonFixture,
  makePublicationFixture,
  usePublicationFixture,
} from "../fixtures/publication"

function metadata(buffer: Buffer) {
  const sha = createHash("sha256").update(buffer).digest("hex")
  return {
    ...comparisonFixture,
    version: 2,
    overall_policy: "relative-speed-v2",
    certificate_policy: "certified-terminal-partial-v1",
    results_db_sha256: sha,
    certificates: [
      {
        selected_run_id: 8,
        cell: {
          system: "fixture",
          db: "latestfailed",
          db_version: "1",
          db_driver: null,
          suite: "clickbench",
          scale_factor: 1,
          benchmark_profile: "resident-hot-v1",
        },
        origin: { revision: "raw", results_db_sha256: sha, run_id: 8 },
        run_content_sha256: sha,
        population: {
          selected_run_id: 9,
          origin: { revision: "raw", results_db_sha256: sha, run_id: 9 },
          run_content_sha256: sha,
          audit: { path: "population.json", sha256: sha },
        },
        lineage: { path: "lineage.json", sha256: sha },
        validation: { path: "validation.json", sha256: sha },
      },
    ],
  }
}

test("v2 reader credits certified failed queries while preserving strict and raw outcomes", async ({
  page,
}) => {
  const fixture = await makePublicationFixture(true)
  try {
    await usePublicationFixture(page, fixture.buffer, metadata(fixture.buffer))
    await page.goto("#/")
    const result = await page.evaluate(async () => {
      const reader = "/olap-benchmarks/src/lib/observationReader.ts"
      const observations = "/olap-benchmarks/src/lib/observations.ts"
      const overall = "/olap-benchmarks/src/lib/overall.ts"
      const { fetchComparisonData } = (await import(
        reader
      )) as typeof import("../../src/lib/observationReader")
      const { compareFamily } = (await import(
        observations
      )) as typeof import("../../src/lib/observations")
      const { aggregateOverview } = (await import(
        overall
      )) as typeof import("../../src/lib/overall")
      const { manifest, runs } = await fetchComparisonData()
      const family = manifest.families[0]!
      const rows = compareFamily(family, 1, "fixture", runs, manifest)
      const ranking = aggregateOverview([{ family, scales: [{ scale: 1, rows }] }], manifest)
      const selected = rows.find((row) => row.db === "latestfailed")!
      return {
        policy: ranking.policy,
        overall: ranking.rows.find((row) => row.db === "latestfailed"),
        eligible: selected.eligible,
        strict: selected.score,
        status: selected.run!.status,
        error: selected.run!.error,
      }
    })
    expect(result).toMatchObject({
      policy: "relative-speed-v2",
      overall: { score: 50, scoredQueries: 1, totalQueries: 2 },
      eligible: false,
      strict: null,
      status: "failed",
      error: "latest run failed",
    })
  } finally {
    await fixture.cleanup()
  }
})

for (const defect of ["bytes", "identity"] as const) {
  test(`v2 reader rejects wrong ${defect}`, async ({ page }) => {
    const fixture = await makePublicationFixture(true)
    try {
      const comparison = metadata(fixture.buffer)
      if (defect === "identity") comparison.certificates[0]!.selected_run_id = 999
      await usePublicationFixture(page, fixture.buffer, comparison)
      if (defect === "bytes")
        await page.route("**/data/results.db", (route) =>
          route.fulfill({ body: Buffer.from("wrong bytes") }),
        )
      await page.goto("#/")
      const error = await page.evaluate(async () => {
        const reader = "/olap-benchmarks/src/lib/observationReader.ts"
        const { fetchComparisonData } = (await import(
          reader
        )) as typeof import("../../src/lib/observationReader")
        try {
          await fetchComparisonData()
          return "unexpected success"
        } catch (cause) {
          return String(cause)
        }
      })
      expect(error).toContain(
        defect === "bytes" ? "bytes do not match" : "does not match selected run 999",
      )
    } finally {
      await fixture.cleanup()
    }
  })
}
