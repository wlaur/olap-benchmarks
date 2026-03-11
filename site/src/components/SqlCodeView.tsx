import { sql } from "@codemirror/lang-sql"
import { syntaxHighlighting } from "@codemirror/language"
import { EditorState } from "@codemirror/state"
import { oneDarkHighlightStyle } from "@codemirror/theme-one-dark"
import { EditorView, lineNumbers } from "@codemirror/view"
import { useEffect, useRef } from "react"

interface SqlCodeViewProps {
  code: string
}

const sqlViewerTheme = EditorView.theme(
  {
    "&": {
      backgroundColor: "transparent",
      color: "#cbd5e1",
      fontSize: "0.875rem",
    },
    ".cm-editor": {
      backgroundColor: "transparent",
      minHeight: "100%",
      minWidth: "100%",
      width: "100%",
    },
    ".cm-content": {
      padding: "1rem",
      minWidth: "100%",
      fontFamily:
        "ui-monospace, SFMono-Regular, SFMono-Regular, Menlo, Monaco, Consolas, Liberation Mono, Courier New, monospace",
      lineHeight: "1.65",
    },
    ".cm-focused": {
      outline: "none",
    },
    ".cm-gutters": {
      minWidth: "2.75rem",
      border: "none",
      backgroundColor: "transparent",
      color: "#64748b",
    },
    ".cm-activeLine, .cm-activeLineGutter": {
      backgroundColor: "transparent",
    },
    ".cm-selectionBackground, ::selection": {
      backgroundColor: "rgba(56, 189, 248, 0.16) !important",
    },
    ".cm-lineNumbers .cm-gutterElement": {
      padding: "0 0.75rem 0 1rem",
    },
    ".cm-scroller": {
      overflow: "visible",
    },
  },
  { dark: true },
)

function createSqlViewerState(code: string): EditorState {
  return EditorState.create({
    doc: code,
    extensions: [
      lineNumbers(),
      sql(),
      sqlViewerTheme,
      syntaxHighlighting(oneDarkHighlightStyle),
      EditorState.readOnly.of(true),
      EditorView.editable.of(false),
    ],
  })
}

export function SqlCodeView({ code }: SqlCodeViewProps) {
  const hostRef = useRef<HTMLDivElement | null>(null)

  useEffect(() => {
    const host = hostRef.current
    if (!host) return

    const view = new EditorView({
      state: createSqlViewerState(code),
      parent: host,
    })

    return () => {
      view.destroy()
    }
  }, [code])

  return (
    <div ref={hostRef} aria-label="SQL query viewer" className="sql-code-view h-full min-h-0" />
  )
}
