import { sql } from "kysely"

import { getKyselyDb } from "./duckdb"
import type { ProfileScope, QueriesManifest } from "./types"

let queriesCache: QueriesManifest | null = null

export async function fetchQueriesManifest(): Promise<QueriesManifest> {
  if (queriesCache) return queriesCache

  const response = await fetch(`${import.meta.env.BASE_URL}data/queries.json`)
  if (!response.ok) {
    throw new Error(`Failed to load queries.json: ${response.status}`)
  }
  queriesCache = (await response.json()) as QueriesManifest
  return queriesCache
}

let profileScopeCache: Promise<ProfileScope> | null = null

export function fetchProfileScope(): Promise<ProfileScope> {
  profileScopeCache ??= loadProfileScope()
  return profileScopeCache
}

async function loadProfileScope(): Promise<ProfileScope> {
  const db = await getKyselyDb()
  const rows = await db
    .selectFrom("run")
    .select((eb) => [
      eb.ref("run.benchmark_profile").as("profile"),
      sql<number>`cast(count(*) as integer)`.as("runs"),
      sql<
        number | null
      >`max(cast(json_extract(${eb.ref("run.metadata")}, '$.benchmark_profile.warmup_iterations') as integer))`.as(
        "warmup_iterations",
      ),
      sql<
        number | null
      >`max(cast(json_extract(${eb.ref("run.metadata")}, '$.benchmark_profile.measured_iterations') as integer))`.as(
        "measured_iterations",
      ),
    ])
    .where("run.operation", "=", "select")
    .groupBy("run.benchmark_profile")
    .orderBy(
      sql`max(coalesce(${sql.ref("run.finished_at")}, ${sql.ref("run.started_at")}))`,
      "desc",
    )
    .execute()

  const [active, ...excluded] = rows
  return {
    active: active
      ? {
          id: active.profile,
          warmupIterations: active.warmup_iterations,
          measuredIterations: active.measured_iterations,
        }
      : null,
    excluded: excluded.map((row) => ({ id: row.profile, runs: row.runs })),
  }
}

async function activeProfileId(): Promise<string> {
  return (await fetchProfileScope()).active?.id ?? ""
}

export async function fetchSystems(): Promise<string[]> {
  const db = await getKyselyDb()
  const profile = await activeProfileId()
  const rows = await db
    .selectFrom("run")
    .select("system")
    .distinct()
    .where("benchmark_profile", "=", profile)
    .where("operation", "=", "select")
    .orderBy("system")
    .execute()

  return rows.map((row) => row.system)
}
