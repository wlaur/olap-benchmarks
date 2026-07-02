import { useEffect, useRef, useState } from "react"

interface UseContainerWidthOptions {
  initialWidth: number
}

export function useContainerWidth<T extends HTMLElement = HTMLDivElement>({
  initialWidth,
}: UseContainerWidthOptions) {
  const containerRef = useRef<T | null>(null)
  const [width, setWidth] = useState(initialWidth)
  const [mounted, setMounted] = useState(false)

  useEffect(() => {
    const element = containerRef.current
    if (!element) {
      return
    }

    setMounted(true)
    setWidth(element.getBoundingClientRect().width)

    const observer = new ResizeObserver((entries) => {
      for (const entry of entries) {
        setWidth(entry.contentRect.width)
      }
    })
    observer.observe(element)

    return () => {
      observer.disconnect()
    }
  }, [])

  return { width, containerRef, mounted }
}
