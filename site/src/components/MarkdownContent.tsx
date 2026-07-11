import type { ComponentPropsWithoutRef } from "react"
import Markdown from "react-markdown"
import { useNavigate } from "react-router-dom"
import remarkGfm from "remark-gfm"

function isInternalHash(href: string): boolean {
  return href.startsWith("#/")
}

function MarkdownLink({ href, children, ...props }: ComponentPropsWithoutRef<"a">) {
  const navigate = useNavigate()

  if (href && isInternalHash(href)) {
    return (
      <a
        {...props}
        href={href}
        onClick={(e) => {
          e.preventDefault()
          navigate(href.slice(1))
        }}
      >
        {children}
      </a>
    )
  }

  return (
    <a {...props} href={href} target="_blank" rel="noopener noreferrer">
      {children}
    </a>
  )
}

interface MarkdownContentProps {
  content: string
}

export function MarkdownContent({ content }: MarkdownContentProps) {
  return (
    <div className="prose max-w-none font-sans prose-invert prose-headings:font-mono prose-headings:text-slate-50 prose-p:text-slate-300 prose-a:text-accent-400 prose-a:no-underline prose-a:hover:text-accent-300 prose-a:hover:underline prose-strong:text-slate-200 prose-code:font-mono prose-code:text-accent-300 prose-th:font-mono prose-th:text-slate-300 prose-td:text-slate-400 prose-hr:border-border-default">
      <Markdown remarkPlugins={[remarkGfm]} components={{ a: MarkdownLink }}>
        {content}
      </Markdown>
    </div>
  )
}
