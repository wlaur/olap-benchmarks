import { AnalyticalEvidence } from "../components/home/AnalyticalEvidence"
import { HomeScoreTable } from "../components/home/HomeScoreTable"
import { PanelDisclosure } from "../components/layout/Panel"
import { MarkdownContent } from "../components/MarkdownContent"
import { BodyText, DisplayTitle } from "../components/Typography"
import homeContent from "../content/home.md?raw"

export function HomePage() {
  return (
    <section className="mx-auto max-w-5xl space-y-5 py-6">
      <header className="space-y-2">
        <DisplayTitle as="h1">OLAP Benchmarks</DisplayTitle>
        <BodyText className="max-w-2xl text-base">
          These benchmarks compare analytical query performance across databases running on the same
          machine.
        </BodyText>
      </header>
      <HomeScoreTable />
      <AnalyticalEvidence />
      <PanelDisclosure title="About this campaign">
        <MarkdownContent content={homeContent} />
      </PanelDisclosure>
    </section>
  )
}
