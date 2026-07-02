# Development instructions

* Use Python 3.13+ with full typing (the local venv runs 3.14)
* Use uv, never pip
* In `site/`, use Bun for dependency management and script execution (`bun`, `bun run`), not npm
* Test code with pyright, ruff and pytest (Python) and `bun run *` (TypeScript). Never leave code unlinted or unformatted.
* Don't write unnecessary code comments or docstrings (docstrings are OK for cyclopts CLI descriptions)
* Don't add compatibility fallbacks when changing interfaces for external services, e.g. the results database
* Benchmark results are stored in DuckDB database files (with extension .db) that are included in git
* Remove unused code and styles after refactoring and removing parts of the web app
* For ad-hoc results DB inspection, use the read-only CLI entrypoint `./.venv/bin/olap results query --revision <revision> "<sql>"` instead of opening `results/*.db` directly

## Web app architecture (`site/`)

### Core structure

* `site/src/components/` contains reusable UI building blocks. Prefer extending these before adding page-local one-off wrappers.
* `site/src/components/layout/Panel.tsx` is the shared surface layer for card/panel shells. Use `PanelCard`, `PanelHeader`, and `ChartFrame` for standard dashboard surfaces instead of retyping the same Tailwind class lists.
* `site/src/components/Typography.tsx` is the shared typography layer for repeated text treatments such as section titles, eyebrow labels, meta labels, and body copy. Prefer updating these primitives over scattering typography utility changes across pages.
* `site/src/pages/` should primarily compose data + layout. Keep page-specific grid sizing and responsive layout constants there, but avoid putting generic surface or typography abstractions under a single page namespace.
* `site/src/index.css` should stay small and limited to truly global concerns such as keyframes, scrollbar styling, and other app-wide behavior.

### Loading and skeleton guidelines

* Do not create separate skeleton pages for feature pages that already have a stable layout.
* Loading states should live inside the real component tree, so layout shells remain mounted and only data-dependent regions swap between placeholders and loaded content.
* If a page needs skeleton content, the skeleton should reuse the exact same shared panel/layout primitives as the loaded state.
* Avoid duplicate loading paths for the same page, e.g. one skeleton in the route and another inside the page. There should be one authoritative loading tree.

### Styling guidelines

* Prefer shared primitives first:
  * surfaces and inset chart containers: `components/layout/Panel.tsx`
  * repeated text styles: `components/Typography.tsx`
  * dashboard buttons, chips, segmented controls, and select pills: `components/controls/`
* Keep page-specific layout constants in page-local files when they describe a specific dashboard structure, but move anything reusable across pages into `components/`.
* Prefer shared React control primitives over helper functions that return Tailwind class strings. If a control has a selected, inactive, hover, or focus state that appears in more than one place, it should usually live under `components/controls/`.
* Dashboard control states should stay visually restrained and consistent:
  * selected: neutral raised surface, light border, high-contrast text
  * inactive: inset surface, subdued text, modest hover brightening
  * accent color: use as a small dot/icon/detail, not as a separate full button system per panel
* Before introducing new repeated Tailwind class groups, check whether they should become a shared component or typography primitive.
* For future visual refreshes, update shared primitives first so the app moves coherently instead of accumulating page-by-page styling drift.
