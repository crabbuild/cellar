<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Maintain the documentation library

Edit the Markdown guides, keep their source links accurate and run the
documentation checker before submitting changes. `catalog.json` indexes the
library; `index.html` displays it offline with interactive SVG flows. This guide
explains which files to edit, regenerate and verify.

![Documentation in the delivery loop](diagrams/team-delivery.svg)

## Files and generation

| Content | Location | Edit rule |
| --- | --- | --- |
| Usage guides | `docs/` | Edit Markdown directly |
| Designs, delivery plans, and qualification ledgers | `docs/design/` | Keep contracts and recorded status distinct |
| Architecture decisions | `docs/adr/` | Preserve decision provenance |
| Team templates | `docs/templates/` | Keep reusable fields explicit |
| Diagrams and PNG previews | `docs/diagrams/` | Edit generator definitions and regenerate |
| Retained evidence | Beside its design package | Preserve recorded bytes, hashes, and original command paths |

`tools/docs/build.py` generates the catalog, explorer and SVGs from the checked-in
guide metadata and explicit flow definitions. Static concept diagrams and
visual-gallery metadata live in `tools/docs/illustrations.py`. Edit those
sources rather than the generated SVG, catalog or explorer. Edit guide prose directly.
Design documents declare a single-line `Purpose: ...` after their title. The generator uses that line and the title for catalog metadata. Regenerate after adding, moving, or retitling a design, changing its purpose, or editing diagram definitions.

```sh
python3 tools/docs/build.py
python3 tools/docs/check.py
```

The checker validates document/catalog coverage, local link targets/anchors,
SVG XML/IDs and browser asset references. Open `docs/index.html` directly for
offline use, or serve the repository to preserve source links:

```sh
python3 -m http.server 8765 --bind 127.0.0.1
```

Then browse `http://127.0.0.1:8765/docs/index.html`. Scripts in Markdown image
embeds are disabled by renderers; standalone SVG documents or explorer objects
enable interaction. Browsers with scripting disabled still show labeled
static flows and explanatory text. The explorer exposes source-document links;
it is not an application hosting service.

## Change responsibilities

| Change | Update together |
| --- | --- |
| SDK behavior or signature | Builder guide, API map, source links and runnable example/test |
| Ownership/proof/recovery | Architecture, relevant flow, operations, ADR and qualification gates |
| Limit or default | Configuration table, capacity interpretation, effective-policy evidence |
| Manifest/CLI/artifact | Packaging, CLI reference, example configuration and compatibility policy |
| New validation result | Sources/evidence ledger with exact source/image, workload and limitations |
| Team adoption | Fill concrete owners/targets/thresholds in copies of templates |

### Writing standard

- State each page’s purpose in its opening paragraph.
- Use formal, concise language, active voice, and sentence case headings.
- Keep prose paragraphs below approximately 60 words. Use tables for mappings and lists for parallel requirements.
- Define unfamiliar terms or link to the [glossary](24-glossary.md).
- Add focused diagrams and implementation links near the contract they explain.

Introduce code blocks, label fragments as fragments and explain the expected
result of runnable commands. Preserve error meanings, limits, retry rules and
recovery requirements when shortening prose. Each crate README should explain
its purpose, entry points, examples, validation and links to the owning guides.

Keep observed behavior separate from recommended application patterns and
future gates. A result from an older image must retain its provenance. Do not
update a “reviewed at” hash to an uninspected release. Source links remain
relative so they resolve in the checkout and repository hosting.

## Move a design package

1. Move its Markdown, schemas, task inventories, inspectors, and retained evidence together.
2. Rebase repository links and current command paths. Update CI path filters and ignore rules.
3. Adjust inspector repository roots without weakening evidence checks or refreshing historical source hashes.
4. Preserve raw evidence bytes. Paths inside a retained log describe the original run; they are not current instructions.
5. Regenerate the catalog and run documentation and inspector checks.

The fleet inspector requires ignored `.cellar/raft-perf/fleet-operations/` completion records and their logs. A fresh checkout can lack them. Restore those artifacts to audit DONE tasks; missing records remain failures.

## Visual review

After generation, open each flow, step through nodes, toggle the failure branch,
and test keyboard selection. Check the catalog's role/search filters, narrow
layout, focus indicators and reduced-motion behavior. Render SVGs at 2× for
shareable static previews and inspect them for clipping/overlap. The diagram
skill's local `scripts/main.ts` can perform SVG-to-PNG conversion; PNG is a
preview, while SVG remains the editable visual artifact.

When that renderer lacks working system-font support, the optional
`python3 tools/docs/render.py` renders all diagrams in a private local Chromium
session through an already installed `agent-browser`. It produces the checked-in
2× PNG previews with labels, without adding an application dependency.
Pass diagram stems to regenerate selected previews:

```sh
python3 tools/docs/render.py sdk-lifetimes command-retries event-delivery
```

Every new SVG needs a title, descriptive alternative text, a `viewBox`, readable
labels and a caption in its guide. Keep fonts local and include no external
resources. Use a focused diagram at the relevant explanation rather than
repeating an unrelated architecture image.

Documentation-only changes need documentation integrity/browser checks and the
repository checks applicable to changed surfaces. Do not claim the full SDK,
Docker fleet harness or production qualification ran unless it actually did.
Record material limitations in the final handoff.
