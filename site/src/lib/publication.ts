import { fetchResultsManifest } from "./resultsManifest"

export type Publication =
  | { kind: "matrix" }
  | {
      kind: "campaign"
      title: string
      report_url: string
      select_cohorts: number
      partial_select_cohorts: number
      source_commits: string[]
    }

let publicationCache: Promise<Publication> | null = null

export function fetchPublication(): Promise<Publication> {
  publicationCache ??= loadPublication()
  return publicationCache
}

async function loadPublication(): Promise<Publication> {
  const { publication } = (await fetchResultsManifest()) as { publication: Publication }
  if (!publication || (publication.kind !== "matrix" && publication.kind !== "campaign")) {
    throw new Error("manifest.json has no valid publication context")
  }
  if (
    publication.kind === "campaign" &&
    (typeof publication.title !== "string" ||
      typeof publication.report_url !== "string" ||
      !Number.isSafeInteger(publication.select_cohorts) ||
      publication.select_cohorts <= 0 ||
      !Number.isSafeInteger(publication.partial_select_cohorts) ||
      publication.partial_select_cohorts < 0 ||
      publication.partial_select_cohorts > publication.select_cohorts ||
      !Array.isArray(publication.source_commits) ||
      publication.source_commits.length === 0 ||
      !publication.source_commits.every((commit) => typeof commit === "string"))
  ) {
    throw new Error("manifest.json has invalid campaign publication context")
  }
  return publication
}
