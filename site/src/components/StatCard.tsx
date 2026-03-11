interface StatCardProps {
  label: string
  value: string
  detail?: string
}

export function StatCard({ label, value, detail }: StatCardProps) {
  return (
    <article className="rounded-2xl border border-slate-800 bg-slate-900/70 p-5">
      <p className="text-xs font-medium tracking-[0.16em] text-slate-500 uppercase">{label}</p>
      <p className="mt-3 text-2xl font-semibold text-slate-50">{value}</p>
      {detail ? <p className="mt-2 text-sm text-slate-400">{detail}</p> : null}
    </article>
  )
}
