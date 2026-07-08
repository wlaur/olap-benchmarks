import { FlaskConical } from "lucide-react"

import type { BenchmarkDefinition, BenchmarkSuiteId } from "../../lib/benchmarks"
import { ControlSelect } from "../controls/ControlSelect"

interface SuiteSelectorProps {
  benchmarkDefinitions: BenchmarkDefinition[]
  selected: BenchmarkSuiteId
  onChange: (suite: BenchmarkSuiteId) => void
  disabled?: boolean
}

export function SuiteSelector({
  benchmarkDefinitions,
  selected,
  onChange,
  disabled = false,
}: SuiteSelectorProps) {
  return (
    <ControlSelect
      ariaLabel="Suite"
      label="Suite"
      value={selected}
      onChange={(value) => onChange(value as BenchmarkSuiteId)}
      disabled={disabled}
      options={benchmarkDefinitions.map((benchmark) => ({
        value: benchmark.id,
        label: benchmark.navLabel,
      }))}
      icon={<FlaskConical className="h-3 w-3" strokeWidth={1.8} />}
      className="max-w-[18rem] sm:min-w-[12.5rem]"
    />
  )
}
