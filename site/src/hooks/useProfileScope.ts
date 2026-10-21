import { useEffect, useState } from "react"

import { fetchProfileScope } from "../lib/queries"
import type { ProfileScope } from "../lib/types"

export function useProfileScope(): { scope: ProfileScope | null; error: string | null } {
  const [state, setState] = useState<{ scope: ProfileScope | null; error: string | null }>({
    scope: null,
    error: null,
  })

  useEffect(() => {
    let cancelled = false
    void fetchProfileScope().then(
      (scope) => {
        if (!cancelled) setState({ scope, error: null })
      },
      (error: unknown) => {
        if (!cancelled) setState({ scope: null, error: String(error) })
      },
    )
    return () => {
      cancelled = true
    }
  }, [])

  return state
}
