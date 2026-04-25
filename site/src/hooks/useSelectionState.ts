import { useCallback, useMemo, useState } from "react"

export interface SelectionState {
  hoveredQuery: string | null
  selectedQuery: string | null
  setHoveredQuery: (queryName: string | null) => void
  setSelectedQuery: (queryName: string | null) => void
  resetSelection: () => void
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
  const resetSelection = useCallback(() => {
    setHoveredQuery(null)
    setSelectedQuery(null)
  }, [])

  return useMemo(
    () => ({
      hoveredQuery,
      selectedQuery,
      setHoveredQuery,
      setSelectedQuery,
      resetSelection,
      toggleSelectedQuery,
      isHighlighted,
      isAnyActive,
    }),
    [hoveredQuery, selectedQuery, resetSelection, toggleSelectedQuery, isHighlighted, isAnyActive],
  )
}
