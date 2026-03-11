const LOG_FLOOR = 1e-6
const LOG_MIN = Math.log10(LOG_FLOOR)

interface InlineDurationBarsProps {
  byDatabase: Record<string, number | null>
  databases: string[]
  databaseColors: Record<string, string>
  maxDuration: number
}

export function InlineDurationBars({
  byDatabase,
  databases,
  databaseColors,
  maxDuration,
}: InlineDurationBarsProps) {
  const barHeight = 5
  const gap = 2
  const height = databases.length * (barHeight + gap)
  const logMax = Math.log10(Math.max(maxDuration, LOG_FLOOR))
  const range = logMax - LOG_MIN

  return (
    <svg width="100%" height={height} className="min-w-[140px]">
      {databases.map((db, idx) => {
        const val = byDatabase[db]
        if (val === null || val === undefined) return null
        const logVal = Math.log10(Math.max(val, LOG_FLOOR))
        const pct = range <= 0 ? 100 : Math.max(0, (logVal - LOG_MIN) / range) * 100

        return (
          <rect
            key={db}
            x={0}
            y={idx * (barHeight + gap)}
            width={`${pct}%`}
            height={barHeight}
            fill={databaseColors[db]}
            rx={2}
          />
        )
      })}
    </svg>
  )
}
