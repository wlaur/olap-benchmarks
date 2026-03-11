export function formatDurationSeconds(value: number): string {
  if (value >= 10) return `${value.toFixed(1)} s`
  if (value >= 1) return `${value.toFixed(2)} s`

  const milliseconds = value * 1000

  if (milliseconds >= 100) return `${milliseconds.toFixed(0)} ms`
  if (milliseconds >= 10) return `${milliseconds.toFixed(1)} ms`
  return `${milliseconds.toFixed(2)} ms`
}

export function formatMultiplier(value: number): string {
  return `${value.toFixed(2)}x`
}
