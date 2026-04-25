import { HomeScoreTable } from "../components/home/HomeScoreTable"
import { MarkdownContent } from "../components/MarkdownContent"
import homeContent from "../content/home.md?raw"

export function HomePage() {
  return (
    <section className="mx-auto max-w-4xl space-y-10 py-10">
      <HomeScoreTable />
      <MarkdownContent content={homeContent} />
    </section>
  )
}
