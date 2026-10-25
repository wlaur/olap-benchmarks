import type { CatalogRunCoverage } from "../../lib/catalog"
import { Disclosure } from "../layout/Disclosure"
import { BodyText } from "../Typography"

export function CatalogRunContext({ run }: { run: CatalogRunCoverage }) {
  return (
    <>
      {run.outcome === "blocked" ? (
        <Disclosure title="Preparation details" lazy>
          {run.context.map((note, index) => (
            <BodyText key={index} className="text-sm [overflow-wrap:anywhere]">
              {note.summary}{" "}
              <a
                href={note.evidence_url}
                target="_blank"
                rel="noreferrer"
                className="text-accent-300 underline"
              >
                Source
              </a>
            </BodyText>
          ))}
          {run.context.length === 0 ? (
            <BodyText className="text-sm">No additional preparation explanation recorded.</BodyText>
          ) : null}
        </Disclosure>
      ) : null}
      {run.outcome === "failed" ? (
        <Disclosure title="Failure reason" lazy>
          <BodyText className="text-sm [overflow-wrap:anywhere]">
            {run.failureReason ?? "Failure reason not recorded."}
          </BodyText>
        </Disclosure>
      ) : null}
    </>
  )
}
