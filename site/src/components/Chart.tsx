import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from "recharts"

import type { ChartPoint } from "../lib/types"

interface TimingChartProps {
  data: ChartPoint[]
}

export function TimingChart({ data }: TimingChartProps) {
  return (
    <ResponsiveContainer width="100%" height={400}>
      <BarChart data={data} margin={{ top: 20, right: 30, left: 20, bottom: 60 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="rgba(148, 163, 184, 0.06)" />
        <XAxis
          dataKey="name"
          angle={-45}
          textAnchor="end"
          tick={{ fill: "#64748b", fontSize: 12 }}
        />
        <YAxis
          tick={{ fill: "#64748b" }}
          label={{
            value: "Duration (s)",
            angle: -90,
            position: "insideLeft",
            fill: "#64748b",
          }}
        />
        <Tooltip
          contentStyle={{
            backgroundColor: "#1e2330",
            border: "1px solid rgba(148, 163, 184, 0.12)",
            borderRadius: 12,
          }}
          labelStyle={{ color: "#e2e8f0" }}
          itemStyle={{ color: "#e2e8f0" }}
        />
        <Bar dataKey="duration_s" fill="#4a6edb" radius={[4, 4, 0, 0]} />
      </BarChart>
    </ResponsiveContainer>
  )
}
