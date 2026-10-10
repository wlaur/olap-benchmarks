export function formatLatencyMs(valueMs: number): string {
  if (valueMs >= 1000) return `${(valueMs / 1000).toFixed(2)}s`
  if (valueMs >= 100) return `${Math.round(valueMs)}ms`
  if (valueMs >= 10) return `${valueMs.toFixed(1)}ms`
  return `${valueMs.toFixed(2)}ms`
}
