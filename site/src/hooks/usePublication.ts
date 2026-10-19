import { useEffect, useState } from "react"

import { fetchPublication, type Publication } from "../lib/publication"

export function usePublication(): { publication: Publication | null; error: string | null } {
  const [state, setState] = useState<{ publication: Publication | null; error: string | null }>({
    publication: null,
    error: null,
  })

  useEffect(() => {
    let cancelled = false
    void fetchPublication().then(
      (publication) => {
        if (!cancelled) setState({ publication, error: null })
      },
      (error: unknown) => {
        if (!cancelled) setState({ publication: null, error: String(error) })
      },
    )
    return () => {
      cancelled = true
    }
  }, [])

  return state
}
