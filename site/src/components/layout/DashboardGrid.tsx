import { GripVertical } from "lucide-react"
import { type ReactNode, useCallback, useMemo, useState } from "react"
import {
  ResponsiveGridLayout,
  useContainerWidth,
  type Layout,
  type LayoutItem,
  type ResponsiveLayouts,
} from "react-grid-layout"

import { useAppStore } from "../../stores/useAppStore"

const GRID_BREAKPOINTS = { lg: 1280, md: 996, sm: 768, xs: 480 }
const GRID_COLS = { lg: 12, md: 10, sm: 6, xs: 4 }
const ROW_HEIGHT = 28
const GRID_MARGIN = {
  lg: [14, 14] as [number, number],
  md: [14, 14] as [number, number],
  sm: [12, 20] as [number, number],
  xs: [10, 20] as [number, number],
}

const DRAG_CONFIG = { enabled: true, handle: ".widget-drag-handle" } as const
const RESIZE_CONFIG = { enabled: true } as const

export interface WidgetConfig {
  id: string
  visible: boolean
  content: ReactNode
}

interface DashboardGridProps {
  widgets: WidgetConfig[]
}

export function DashboardGrid({ widgets }: DashboardGridProps) {
  const layouts = useAppStore((s) => s.layouts)
  const layoutModified = useAppStore((s) => s.layoutModified)
  const setLayouts = useAppStore((s) => s.setLayouts)
  const markLayoutModified = useAppStore((s) => s.markLayoutModified)
  const resetLayouts = useAppStore((s) => s.resetLayouts)
  const [isDragging, setIsDragging] = useState(false)
  const { width, containerRef, mounted } = useContainerWidth()

  const visibleIds = useMemo(
    () => new Set(widgets.filter((w) => w.visible).map((w) => w.id)),
    [widgets],
  )

  const filteredLayouts = useMemo(() => {
    const result: ResponsiveLayouts = {}
    for (const [breakpoint, items] of Object.entries(layouts)) {
      if (items) {
        result[breakpoint] = (items as readonly LayoutItem[]).filter((item) =>
          visibleIds.has(item.i),
        )
      }
    }
    return result
  }, [layouts, visibleIds])

  const handleLayoutChange = useCallback(
    (_currentLayout: Layout, allLayouts: ResponsiveLayouts) => {
      setLayouts(allLayouts)
    },
    [setLayouts],
  )

  const handleDragStop = useCallback(() => {
    setIsDragging(false)
    markLayoutModified()
  }, [markLayoutModified])

  const handleResizeStop = useCallback(() => {
    markLayoutModified()
  }, [markLayoutModified])

  const visibleWidgets = widgets.filter((w) => w.visible)

  return (
    <div ref={containerRef} className="relative w-full">
      {layoutModified ? (
        <div className="mb-1.5 flex justify-end px-4">
          <button
            type="button"
            onClick={resetLayouts}
            className="rounded-full border border-border-default bg-surface-inset px-2.5 py-0.5 text-[0.65rem] font-medium text-slate-500 transition-colors hover:border-slate-600 hover:text-slate-300"
          >
            Reset layout
          </button>
        </div>
      ) : null}
      {mounted ? (
        <ResponsiveGridLayout
          width={width}
          layouts={filteredLayouts}
          breakpoints={GRID_BREAKPOINTS}
          cols={GRID_COLS}
          rowHeight={ROW_HEIGHT}
          margin={GRID_MARGIN}
          containerPadding={[0, 0]}
          onLayoutChange={handleLayoutChange}
          onDragStart={() => setIsDragging(true)}
          onDragStop={handleDragStop}
          onResizeStop={handleResizeStop}
          dragConfig={DRAG_CONFIG}
          resizeConfig={RESIZE_CONFIG}
        >
          {visibleWidgets.map((widget) => (
            <div
              key={widget.id}
              className={`group/widget relative ${isDragging ? "cursor-grabbing" : ""}`}
            >
              <div className="widget-drag-handle absolute top-1.5 left-1.5 z-10 flex cursor-grab items-center justify-center rounded bg-surface-inset/80 p-0.5 opacity-0 backdrop-blur transition-opacity group-hover/widget:opacity-100 active:cursor-grabbing">
                <GripVertical className="size-3 text-slate-400" />
              </div>
              <div className="h-full">{widget.content}</div>
            </div>
          ))}
        </ResponsiveGridLayout>
      ) : null}
    </div>
  )
}
