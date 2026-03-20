import { readFile, writeFile } from "node:fs/promises"

const GENERATED_TYPES_PATH = new URL("../src/lib/generated/db.ts", import.meta.url)

const content = await readFile(GENERATED_TYPES_PATH, "utf8")
const exportedTypeNames = Array.from(
  content.matchAll(/^export type ([A-Za-z][A-Za-z ]+) =/gm),
  (match) => match[1],
).filter((name) => name !== undefined)
const nameMap = new Map(
  exportedTypeNames
    .filter((name) => name.includes(" "))
    .map((name) => [name, name.replaceAll(" ", "")]),
)

const normalizedContent = Array.from(nameMap.entries()).reduce(
  (updated, [before, after]) => updated.replaceAll(before, after),
  content,
)

await writeFile(GENERATED_TYPES_PATH, normalizedContent)
