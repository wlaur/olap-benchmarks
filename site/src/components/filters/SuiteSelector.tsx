import { FlaskConical } from "lucide-react"

import { benchmarkDefinitions, type BenchmarkSuiteId } from "../../lib/benchmarks"
import { ControlSelect } from "../controls/ControlSelect"

interface SuiteSelectorProps {
  selected: BenchmarkSuiteId
  onChange: (suite: BenchmarkSuiteId) => void
}

export function SuiteSelector({ selected, onChange }: SuiteSelectorProps) {
  return (
    <ControlSelect
      ariaLabel="Suite"
      label="Suite"
      value={selected}
      onChange={(value) => onChange(value as BenchmarkSuiteId)}
      options={benchmarkDefinitions.map((benchmark) => ({
        value: benchmark.id,
        label: benchmark.navLabel,
      }))}
      icon={<FlaskConical className="h-3 w-3" strokeWidth={1.8} />}
      className="max-w-[18rem] sm:min-w-[12.5rem]"
    />
  )
}
