# Maintain the documentation library

Markdown is the primary reference. `catalog.json` is the machine-readable index;
`index.html` is an offline searchable catalog and interactive flow explorer.
Standalone SVGs contain their own scripts and static fallback content. No
JavaScript engine is added to the Rust workspace by these browser-only docs.

![Documentation in the delivery loop](diagrams/team-delivery.svg)

## Files and generation

Guides live directly under `docs/`, decisions under `docs/adr/`, and team
templates under `docs/templates/`. Visuals live under `docs/diagrams/`.
`tools/docs/build.py` generates the catalog, explorer and SVGs from the checked-in
guide metadata and explicit flow definitions. Guide prose is edited directly;
generated files are not the source of framework behavior.

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

Keep observed behavior separate from recommended application patterns and
future gates. A result from an older image must retain its provenance. Do not
update a “reviewed at” hash to an uninspected release. Source links remain
relative so they resolve in the checkout and repository hosting.

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

Documentation-only changes need documentation integrity/browser checks and the
repository checks applicable to changed surfaces. Do not claim the full SDK,
Docker fleet harness or production qualification ran unless it actually did.
Record material limitations in the final handoff.
