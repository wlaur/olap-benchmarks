import { PanelCard } from "./layout/Panel"
import { BodyText, MetaLabel } from "./Typography"

export function StatCard({
  label,
  value,
  detail,
}: {
  label: string
  value: string
  detail?: string
}) {
  return (
    <PanelCard className="rounded-2xl">
      <MetaLabel className="tracking-[0.16em]">{label}</MetaLabel>
      <p className="mt-3 text-2xl font-semibold text-slate-50">{value}</p>
      {detail ? <BodyText className="mt-2">{detail}</BodyText> : null}
    </PanelCard>
  )
}
