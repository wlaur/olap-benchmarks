import { Fragment, useMemo } from "react"

import { cn } from "../lib/cn"
import { highlightSqlLines } from "../lib/highlightSql"

interface SqlCodeViewProps {
  code: string
  wrapLines?: boolean
  className?: string
  fillHeight?: boolean
}

export function SqlCodeView({
  code,
  wrapLines = false,
  className,
  fillHeight = true,
}: SqlCodeViewProps) {
  const lines = useMemo(() => highlightSqlLines(code), [code])
  const gutterWidth = `calc(${Math.max(2, String(lines.length).length)}ch + 1.75rem)`

  return (
    <div
      aria-label="SQL query viewer"
      className={cn(
        "panel-scrollbar overflow-auto font-mono text-sm leading-[1.65] text-[#cbd5e1]",
        fillHeight ? "h-full" : "h-auto",
        className,
      )}
    >
      <div
        className={cn("grid items-start py-3", wrapLines ? "min-w-full" : "min-w-max")}
        style={{ gridTemplateColumns: `${gutterWidth} minmax(0, 1fr)` }}
      >
        {lines.map((line, index) => (
          <Fragment key={index}>
            <span className="pr-3 pl-4 text-right text-[#475569] tabular-nums select-none">
              {index + 1}
            </span>
            <span
              className={cn(
                "pr-4",
                wrapLines ? "break-words whitespace-pre-wrap" : "whitespace-pre",
              )}
            >
              {line.length === 0
                ? " "
                : line.map((token, tokenIdx) => (
                    <span key={tokenIdx} className={token.className || undefined}>
                      {token.text}
                    </span>
                  ))}
            </span>
          </Fragment>
        ))}
      </div>
    </div>
  )
}
