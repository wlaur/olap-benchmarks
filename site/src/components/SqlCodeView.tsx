import { sql } from "@codemirror/lang-sql"
import { syntaxHighlighting } from "@codemirror/language"
import { EditorState } from "@codemirror/state"
import { oneDarkHighlightStyle } from "@codemirror/theme-one-dark"
import { EditorView, lineNumbers } from "@codemirror/view"
import { useEffect, useRef } from "react"

import { cn } from "../lib/cn"

interface SqlCodeViewProps {
  code: string
  wrapLines?: boolean
  className?: string
  fillHeight?: boolean
}

function createSqlViewerTheme(wrapLines: boolean, fillHeight: boolean) {
  return EditorView.theme(
    {
      "&": {
        backgroundColor: "transparent",
        color: "#cbd5e1",
        fontSize: "0.875rem",
        height: fillHeight ? "100%" : "auto",
        minHeight: 0,
      },
      ".cm-editor": {
        backgroundColor: "transparent",
        display: "flex",
        flexDirection: "column",
        height: fillHeight ? "100%" : "auto",
        minHeight: 0,
        maxWidth: "100%",
      },
      ".cm-content": {
        padding: "1rem",
        fontFamily:
          "ui-monospace, SFMono-Regular, SFMono-Regular, Menlo, Monaco, Consolas, Liberation Mono, Courier New, monospace",
        lineHeight: "1.65",
        minWidth: wrapLines ? "0" : "max-content",
      },
      ".cm-focused": {
        outline: "none",
      },
      ".cm-gutters": {
        minWidth: "2.75rem",
        border: "none",
        backgroundColor: "transparent",
        color: "#475569",
        position: "relative",
        left: "auto",
      },
      ".cm-activeLine, .cm-activeLineGutter": {
        backgroundColor: "transparent",
      },
      ".cm-selectionBackground, ::selection": {
        backgroundColor: "rgba(108, 142, 239, 0.12) !important",
      },
      ".cm-lineNumbers .cm-gutterElement": {
        padding: "0 0.75rem 0 1rem",
      },
      ".cm-scroller": {
        flex: fillHeight ? "1 1 auto" : "0 1 auto",
        height: fillHeight ? "100%" : "auto",
        overflowX: wrapLines ? "hidden" : "auto",
        overflowY: "auto",
        minWidth: 0,
        minHeight: 0,
        maxWidth: "100%",
        overscrollBehavior: "contain",
      },
    },
    { dark: true },
  )
}

function createSqlViewerState(code: string, wrapLines: boolean, fillHeight: boolean): EditorState {
  return EditorState.create({
    doc: code,
    extensions: [
      lineNumbers(),
      sql(),
      createSqlViewerTheme(wrapLines, fillHeight),
      syntaxHighlighting(oneDarkHighlightStyle),
      EditorState.readOnly.of(true),
      EditorView.editable.of(false),
      wrapLines ? EditorView.lineWrapping : [],
    ],
  })
}

export function SqlCodeView({
  code,
  wrapLines = false,
  className,
  fillHeight = true,
}: SqlCodeViewProps) {
  const hostRef = useRef<HTMLDivElement | null>(null)

  useEffect(() => {
    const host = hostRef.current
    if (!host) return

    const view = new EditorView({
      state: createSqlViewerState(code, wrapLines, fillHeight),
      parent: host,
    })

    return () => {
      view.destroy()
    }
  }, [code, wrapLines, fillHeight])

  return (
    <div
      ref={hostRef}
      aria-label="SQL query viewer"
      className={cn(
        wrapLines
          ? "panel-scrollbar min-h-0 w-full min-w-0 overflow-hidden"
          : "panel-scrollbar min-h-0 w-full min-w-0 overflow-hidden",
        fillHeight ? "h-full" : "h-auto",
        className,
      )}
    />
  )
}
