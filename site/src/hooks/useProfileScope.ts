import { useEffect, useState } from "react"

import { fetchProfileScope } from "../lib/queries"
import type { ProfileScope } from "../lib/types"

export function useProfileScope(): ProfileScope | null {
  const [scope, setScope] = useState<ProfileScope | null>(null)

  useEffect(() => {
    let cancelled = false
    void fetchProfileScope().then((nextScope) => {
      if (!cancelled) setScope(nextScope)
    })
    return () => {
      cancelled = true
    }
  }, [])

  return scope
}
