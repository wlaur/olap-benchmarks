import { sql } from "@codemirror/lang-sql"
import { classHighlighter, highlightCode } from "@lezer/highlight"

const sqlParser = sql().language.parser

export interface HighlightedToken {
  text: string
  className: string
}

export function highlightSqlTokens(code: string): HighlightedToken[] {
  const tree = sqlParser.parse(code)
  const tokens: HighlightedToken[] = []

  highlightCode(
    code,
    tree,
    classHighlighter,
    (text, classes) => {
      tokens.push({ text, className: classes })
    },
    () => {
      tokens.push({ text: "\n", className: "" })
    },
  )

  return tokens
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
