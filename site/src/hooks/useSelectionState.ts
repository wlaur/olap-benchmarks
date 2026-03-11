import { useCallback, useState } from "react"

export interface SelectionState {
  hoveredQuery: string | null
  selectedQuery: string | null
  setHoveredQuery: (queryName: string | null) => void
  setSelectedQuery: (queryName: string | null) => void
  toggleSelectedQuery: (queryName: string) => void
  isHighlighted: (queryName: string) => boolean
  isAnyActive: boolean
}

export function useSelectionState(): SelectionState {
  const [hoveredQuery, setHoveredQuery] = useState<string | null>(null)
  const [selectedQuery, setSelectedQuery] = useState<string | null>(null)

  const isAnyActive = hoveredQuery !== null || selectedQuery !== null
  const activeQuery = hoveredQuery ?? selectedQuery

  const isHighlighted = useCallback(
    (queryName: string) => {
      if (activeQuery === null) return true
      return queryName === activeQuery
    },
    [activeQuery],
  )

  const toggleSelectedQuery = useCallback((queryName: string) => {
    setSelectedQuery((prev) => (prev === queryName ? null : queryName))
  }, [])

  return {
    hoveredQuery,
    selectedQuery,
    setHoveredQuery,
    setSelectedQuery,
    toggleSelectedQuery,
    isHighlighted,
    isAnyActive,
  }
}
