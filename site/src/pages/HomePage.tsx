import { HomeScoreTable } from "../components/home/HomeScoreTable"
import { MarkdownContent } from "../components/MarkdownContent"
import { BodyText, DisplayTitle } from "../components/Typography"
import homeContent from "../content/home.md?raw"

export function HomePage() {
  return (
    <section className="space-y-8 py-8 sm:py-10">
      <header className="mx-auto max-w-4xl space-y-3">
        <DisplayTitle as="h1">OLAP Benchmarks</DisplayTitle>
        <BodyText className="max-w-2xl text-base">
          Identical SQL workloads run against multiple OLAP databases on the same machine, measuring
          query latency, load time, and resource usage.
        </BodyText>
      </header>
      <HomeScoreTable />
      <div className="mx-auto max-w-4xl">
        <MarkdownContent content={homeContent} />
      </div>
    </section>
  )
}
