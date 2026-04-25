import { HomeScoreTable } from "../components/home/HomeScoreTable"
import { MarkdownContent } from "../components/MarkdownContent"
import { BodyText, DisplayTitle } from "../components/Typography"
import homeContent from "../content/home.md?raw"

export function HomePage() {
  return (
    <section className="mx-auto max-w-4xl space-y-8 py-10">
      <header className="space-y-3">
        <DisplayTitle as="h1">OLAP Benchmarks</DisplayTitle>
        <BodyText className="max-w-2xl text-base">
          Run the same SQL queries against different OLAP databases on the same machine and compare
          how they do.
        </BodyText>
      </header>
      <HomeScoreTable />
      <MarkdownContent content={homeContent} />
    </section>
  )
}
