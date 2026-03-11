import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from "recharts"
import type { ChartPoint } from "../lib/types"

interface TimingChartProps {
  data: ChartPoint[]
}

export function TimingChart({ data }: TimingChartProps) {
  return (
    <ResponsiveContainer width="100%" height={400}>
      <BarChart
        data={data}
        margin={{ top: 20, right: 30, left: 20, bottom: 60 }}
      >
        <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
        <XAxis
          dataKey="name"
          angle={-45}
          textAnchor="end"
          tick={{ fill: "#9ca3af", fontSize: 12 }}
        />
        <YAxis
          tick={{ fill: "#9ca3af" }}
          label={{
            value: "Duration (s)",
            angle: -90,
            position: "insideLeft",
            fill: "#9ca3af",
          }}
        />
        <Tooltip
          contentStyle={{
            backgroundColor: "#1f2937",
            border: "1px solid #374151",
            borderRadius: 8,
          }}
          labelStyle={{ color: "#f3f4f6" }}
          itemStyle={{ color: "#60a5fa" }}
        />
        <Bar dataKey="duration_s" fill="#3b82f6" radius={[4, 4, 0, 0]} />
      </BarChart>
    </ResponsiveContainer>
  )
}
