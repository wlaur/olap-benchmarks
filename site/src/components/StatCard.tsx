import { PanelCard } from "./layout/Panel"

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
      <p className="text-xs font-medium tracking-[0.16em] text-slate-500 uppercase">{label}</p>
      <p className="mt-3 text-2xl font-semibold text-slate-50">{value}</p>
      {detail ? <p className="mt-2 text-sm text-slate-400">{detail}</p> : null}
    </PanelCard>
  )
}
