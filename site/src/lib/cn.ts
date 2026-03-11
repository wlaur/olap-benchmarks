export function cn(
  ...classes: Array<string | false | null | undefined>
): string {
  const names: string[] = []

  for (const value of classes) {
    if (value) {
      names.push(value)
    }
  }

  return names.join(" ")
}
