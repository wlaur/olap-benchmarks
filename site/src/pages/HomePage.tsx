import { MarkdownContent } from "../components/MarkdownContent"
import homeContent from "../content/home.md?raw"

export function HomePage() {
  return (
    <section className="mx-auto max-w-4xl py-10">
      <MarkdownContent content={homeContent} />
    </section>
  )
}
