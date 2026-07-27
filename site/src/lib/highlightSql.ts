import { sql } from "@codemirror/lang-sql"
import { classHighlighter, highlightCode } from "@lezer/highlight"

const sqlParser = sql().language.parser

interface HighlightedToken {
  text: string
  className: string
}

export function highlightSqlLines(code: string): HighlightedToken[][] {
  const tree = sqlParser.parse(code)
  const lines: HighlightedToken[][] = [[]]

  highlightCode(
    code,
    tree,
    classHighlighter,
    (text, classes) => {
      lines[lines.length - 1]!.push({ text, className: classes })
    },
    () => {
      lines.push([])
    },
  )

  return lines
}
