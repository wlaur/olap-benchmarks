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

## First simulated user round

Both models tested six ordinary goals through the rendered interface, without
source access or a supplied navigation map. Opus completed four goals and partly
completed two; Astra completed all six. Opus could inspect text and keyboard
behavior but reported hidden-tab screenshot and pointer limitations and used a
labelled iframe for narrow layout. Astra used an isolated browser fallback after
repeated preview timeouts and verified desktop and mobile viewport dimensions.
These are agent simulations, not human usability research.

Both found the timeout explanation too deeply nested, weighted coverage undefined
near its result, and Catalog's combined execution counts confusing beside the
separate raw/prepared result tracks. Opus additionally found internal exception
names in concurrency outcomes, mismatched-looking denominators, and narrative
campaign notes. Astra found that horizontally scrolling gap reasons separated
from their workload names on mobile.

The response exposes sourced explanations in the first engine dialog, labels
campaign skips without converting them into observed timeouts, and names all-query
execution versus performance-query denominators. Coverage has an adjacent
explanation; Catalog separates family/scale execution and comparison blocks.
Concurrency uses plain outcome labels with raw evidence disclosed. Campaign notes
are short factual references. Mobile contributions keep workload names with gap
reasons. Workload menus include purposes; Home selections are URL-backed, and
Explorer navigation restores the previous context.

The user requested two further rounds with both models, with fixes between them.
Those second and third rounds remain required before UI acceptance and deployment.

First-round verification passed 125 unit tests, site type/lint/format/build and
publication identity checks. The final dropdown adjustment removes Radix's
scroll-button focus/automatic scrolling behavior; the viewport retains ordinary
wheel, touch and keyboard scrolling with an options count. All 19 affected Home
and Catalog browser tests, including WebKit pointer selection, passed afterward.
The full browser suite will run again before final acceptance. Fresh second-round
Opus 5.5 and Astra xhigh simulations are active; the third round follows their
findings and any resulting fixes.

## Second simulated user round

Both fresh models completed all six goals and requested a focused refinement.
Opus used rendered text with a labelled mobile iframe after browser preview
failures; Astra used an isolated browser fallback with verified mobile emulation.
Both could explain the score and distinguish prior timeouts from current skips.

Remaining findings were misleading completed-count captions, missing recorded
skip reasons, timeout context absent in overall gap rows, mixed warmup/measured
counts, opaque latency/date labels, and an unexplained ratio baseline. Astra also
found a mobile drilldown that changed workload without returning the viewport to
its result, and zero-completion Catalog rows that hid unsupported versus skipped
outcomes.

The next iteration labels completed queries without claiming every query finished,
shows recorded execution reasons and sourced overall gap context, names the
per-query reference median, and separates warmup from measured windows. Unknown
cancellation causes remain unknown. Catalog names zero-completion outcomes and
has a failure-reason disclosure. Workload drilldown scrolls and moves focus to the
new result; explicit Home workload choices update the Explorer navigation target.
The short formula/help remain below the ranking to keep supporting explanation
out of the initial result; coverage is defined next to the headline.

Validation passed 131 unit tests and all 50 browser tests, including the mobile
navigation regression and WebKit. The third fresh Opus 5.5/Astra xhigh round is
required before acceptance. Measurements and scoring invariants remain unchanged.

## Third simulated user round and final corrections

Both models completed all six goals using real screenshots at desktop, 390 px and
320 px after documented preview failures and isolated browser fallbacks. They
confirmed readable factual results, understandable coverage, scoped Catalog
tracks and discoverable timeout history. Their final findings were addressed
before deployment.

Opus found document scrolling outside the main region, route scroll carryover,
missing preparation explanations, suite-switch selection loss and incomplete
modal focus containment. Absolutely positioned accessibility text lacked a
positioned ancestor; the application and main regions now contain it. The page
stays at viewport height while only main content scrolls. Path changes reset the
main scroller; query changes preserve it. Modal keyboard focus wraps internally,
including query-picker transitions, and restores correctly. Query filters receive
focus on opening. Suite changes keep configuration open and select available
values for the varying dimension; absent-engine availability is next to the result.

Astra found that the short score definition still required scrolling, mobile
concurrency columns separated outcomes from engine names, and dialog prose
inherited right alignment. The short per-query points definition is now beside
the score, while the full method remains disclosed. A single semantic concurrency
table adapts to readable cards at narrow widths and enlarged text; outcomes and
44 px Details controls remain associated with each engine. Dialog prose is left
aligned. Both corrective agents inspected their changes and closed their browsers.

Catalog now exposes sourced preparation explanations on request. Historical
timeout notes in workload rows quote selected-run annotations without relabelling
current skips as observed timeouts. Evidence disclosures share visible chevrons.
All 59 browser tests and 131 unit tests pass, including document bounds, mode and
suite navigation, keyboard focus, mobile/touch behavior, 200% text and the frozen
publication invariants. Type/lint/format/build and schema/checksum checks pass.
The published asset and numeric scoring policy are unchanged by this UI work.

These three paired rounds are agent simulations, not human usability research.
The reviewed UI is ready for publication. Timeout recovery remains a separate,
serial measurement phase; replacement measurements require their own validation.
