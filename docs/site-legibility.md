# Results legibility

The October 25 review used Claude Opus 5.5 and GPT-6 Astra at xhigh reasoning.
Both requested changes to the information hierarchy, not just cosmetic changes.

## Consolidated findings

- The interface used monospace throughout, with important text at 10–12 px.
- Explorer placed about 5,060 px of evidence before the query result on desktop,
  and about 8,142 px on mobile.
- Pages lacked a short, scoped statement of what their results showed.
- Successful-only query bars hid failures and conflated absent observations with
  timeouts, unsupported queries and exclusions.
- Singleton analyses used superlatives despite having no comparison.
- Catalog opened with SQL instead of explaining the workload and available data.

The user's disclosure requirement takes precedence over proposals to show full
heatmaps, a new overview matrix or Catalog SQL by default. Those are supporting
details, and remain closed until requested.

## View contracts

| View | Main point | Initially visible | Supporting detail |
| --- | --- | --- | --- |
| Overall | Which database leads this published workload mix, with what coverage? | Data-derived conclusion, explicit points scale, ranking and coverage | Family contributions, rank sensitivity, gaps, run IDs and scoring method |
| One workload | Which selected engines have comparable results for this family and scale? | Explicit lower-is-better latency ratio, eligible cohort and compact outcome rows | Eligibility reasons, validation, preparation and iteration evidence |
| Explorer: databases | How do the chosen engines compare in this workload and environment? | Scope, compact workload comparison, query inspection action | SQL, query outcomes, run context and complete query matrix |
| Explorer: one database | What does the selected query show across the chosen variants? | Selected query, observed medians, explicit missing/failure states or singleton explanation | SQL, repetitions, configuration and all other queries |
| Catalog | What does this workload measure, and which results exist? | Purpose, query count, scales/tracks, availability conclusion and Explore action | Query browsing, dialect SQL, full coverage matrix and context |
| Concurrency | What throughput and completion were observed as clients changed? | Completion summary and compact throughput/outcome rows after opening | Windows, percentiles, achieved mixes, budgets, manifests and raw attempts |

## Acceptance criteria

- Use readable sans-serif interface text; reserve monospace for code and numbers.
  Body text is approximately 16 px and secondary information approximately 14 px.
- Show the conclusion and main comparison before supporting evidence on desktop
  and mobile. A disclosure must not merely hide another wall of tiny text.
- Keep SQL, hashes, raw iterations, provenance, settings, methods and complete
  matrices closed by default, reachable through named disclosures or dialogs.
- Dialogs have an accessible title, focus containment, Escape dismissal and focus
  restoration. Controls have visible focus and comfortable touch targets.
- Preserve fixed publication references, weights, denominators, exclusions and
  rank behavior. An overall points ratio is not a latency multiplier.
- Retain recorded failure, unsupported, skipped, excluded, incomplete and absent
  outcomes alongside successful medians. Never display missing latency as zero.
- Across scale, version and system comparisons, show observed differences without
  inventing a suite aggregate or implying causality. Never name a singleton winner.
- Use proportional bars and a consistent, labelled heatmap scale. Meaning must
  remain available without color, hover or a clipped label.
- Verify every route and mode, URL restoration, loading, empty, filtered and error
  states; keyboard dialogs, 320 px reflow and enlarged text; Bun validation and
  focused scientific regression tests.

Timeout recovery is a separate measurement change. Its actual limits and outcomes
must come from recorded run metadata; UI conclusions must update from data.

## Second technical review

Opus 5.5 found lost database selections after a mode round trip, undersized
expanded evidence, an overlong query inspector, a buried query action, omitted
no-run engines, and inconsistent desktop/mobile coverage. Astra xhigh additionally
found clipped descendants at 320 px with enlarged text, full failure explanations
in primary query rows, a summary that confused recorded latency with usable
evidence, and missing fixed system scope.

The iteration preserves database-mode selections in the URL, uses 14 px evidence
text and wrapping controls, places query inspection in the result header, and
keeps per-engine evidence behind lazy disclosures. No-run engines have a compact
availability disclosure. Both widths show explicitly weighted coverage, with raw
query counts and categorized gap reasons in expanded rows. Query summaries count
recorded medians separately from usable evidence; primary rows show short states,
while complete reasons remain in the inspector. Fixed systems and versions are
visible. Primary result copy uses factual labels instead of generated sentences.

Regression checks include the mode round trip, real descendant bounds at 320 px
and 200% text, absence versus failure, blocked medians, and dialog focus behavior.
The overall score fixture and publication asset are unchanged by this redesign.

Fresh Opus 5.5 and Astra xhigh sessions perform the separately requested simulated
first-time-user tests from the site home page. They receive goals and the site
address, without source access or a map of controls. Their findings and subsequent
fixes will be recorded after the browser sessions finish.
