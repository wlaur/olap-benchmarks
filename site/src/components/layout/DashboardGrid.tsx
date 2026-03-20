import { Grip } from "lucide-react"
import {
  type ReactNode,
  useCallback,
  useDeferredValue,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react"
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
  const [draftLayouts, setDraftLayouts] = useState(layouts)
  const latestLayoutsRef = useRef(layouts)
  const { width, containerRef, mounted } = useContainerWidth({
    measureBeforeMount: true,
    initialWidth: 1280,
  })
  const deferredWidth = useDeferredValue(width)

  useEffect(() => {
    setDraftLayouts(layouts)
    latestLayoutsRef.current = layouts
  }, [layouts])

  const visibleIds = useMemo(
    () => new Set(widgets.filter((w) => w.visible).map((w) => w.id)),
    [widgets],
  )

  const filteredLayouts = useMemo(() => {
    const result: ResponsiveLayouts = {}
    for (const [breakpoint, items] of Object.entries(draftLayouts)) {
      if (items) {
        result[breakpoint] = (items as readonly LayoutItem[]).filter((item) =>
          visibleIds.has(item.i),
        )
      }
    }
    return result
  }, [draftLayouts, visibleIds])

  const handleLayoutChange = useCallback(
    (_currentLayout: Layout, allLayouts: ResponsiveLayouts) => {
      latestLayoutsRef.current = allLayouts
      setDraftLayouts(allLayouts)
    },
    [],
  )

  const handleDragStop = useCallback(() => {
    setIsDragging(false)
    setLayouts(latestLayoutsRef.current)
    markLayoutModified()
  }, [markLayoutModified, setLayouts])

  const handleResizeStop = useCallback(() => {
    setLayouts(latestLayoutsRef.current)
    markLayoutModified()
  }, [markLayoutModified, setLayouts])

  const visibleWidgets = widgets.filter((w) => w.visible)

  return (
    <div ref={containerRef} className="relative min-h-0 w-full">
      {layoutModified ? (
        <div className="mb-1.5 flex justify-end">
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
          width={deferredWidth}
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
              className={`group/widget relative h-full min-h-0 min-w-0 overflow-hidden ${
                isDragging ? "cursor-grabbing" : ""
              }`}
            >
              <div className="widget-drag-handle absolute top-3 right-3 z-10 flex cursor-grab items-center justify-center rounded-full border border-border-default bg-surface-primary/90 px-2 py-1 text-slate-400 opacity-0 shadow-lg shadow-black/25 backdrop-blur transition-all duration-150 group-focus-within/widget:opacity-100 group-hover/widget:opacity-100 hover:border-slate-600 hover:text-slate-200 active:cursor-grabbing">
                <Grip className="size-3.5" />
              </div>
              <div className="h-full min-h-0 min-w-0 overflow-hidden">{widget.content}</div>
            </div>
          ))}
        </ResponsiveGridLayout>
      ) : null}
    </div>
  )
}
