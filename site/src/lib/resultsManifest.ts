let manifestCache: Promise<Record<string, unknown>> | null = null

export function fetchResultsManifest(): Promise<Record<string, unknown>> {
  manifestCache ??= loadManifest()
  return manifestCache
}

async function loadManifest(): Promise<Record<string, unknown>> {
  const response = await fetch(`${import.meta.env.BASE_URL}data/manifest.json`)
  if (!response.ok) {
    throw new Error(`Failed to load manifest.json: ${response.status}`)
  }
  const manifest: unknown = await response.json()
  if (typeof manifest !== "object" || manifest === null || Array.isArray(manifest)) {
    throw new Error("manifest.json has no valid results manifest")
  }
  return manifest as Record<string, unknown>
}
