import { AnalyticalEvidence } from "../components/home/AnalyticalEvidence"
import { HomeScoreTable } from "../components/home/HomeScoreTable"
import { PanelDisclosure } from "../components/layout/Panel"
import { MarkdownContent } from "../components/MarkdownContent"
import { DisplayTitle } from "../components/Typography"
import homeContent from "../content/home.md?raw"

export function HomePage() {
  return (
    <section className="mx-auto max-w-5xl space-y-4 py-4 sm:space-y-6 sm:py-8">
      <DisplayTitle as="h1" className="text-2xl sm:text-3xl">
        Analytical query speed
      </DisplayTitle>
      <HomeScoreTable />
      <AnalyticalEvidence />
      <PanelDisclosure
        title="About this campaign"
        meta="Campaign notes, notable results and how to read them"
      >
        <MarkdownContent content={homeContent} />
      </PanelDisclosure>
    </section>
  )
}
