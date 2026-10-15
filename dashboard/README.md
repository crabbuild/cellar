<img src="public/cellar-icon.png" alt="Cellar logo" width="56" height="56" />

# Cellar fleet console

Vite + React + TypeScript + shadcn/ui (Radix) + Lucide. The frontend uses the
[Cellar management service](../crates/cellar-admin/README.md), never fleet
credentials or a node's private listener. See the
[implemented scope and verification record](../plans/003-fleet-dashboard.md).

```sh
npm ci
npm run dev       # 127.0.0.1:5173; API proxy to 127.0.0.1:4174
npm run build    # production assets in dist/
npm run lint
npm run test:ui  # browser interactions against explicit test fixtures
```

Playwright tests use synthetic fixtures to check UI behavior. They are not live
fleet integration tests. Screenshots are saved under ignored `test-results/`.
The deployed browser application contains no demo provider or synthetic fleet.

Pages are loaded on demand. Query caches are cleared on logout. Resource identity
and permission checks are enforced by the Rust server; hiding a button is not
an authorization boundary. Write requests are not automatically retried.

## Brand assets

The Vault C logo is a C enclosing one durable cell. Download the
[SVG icon and reusable mark](public/cellar.svg) or the
[512 px PNG](public/cellar-icon.png) for repository avatars and other bitmap uses.
The standalone icon uses a dark tile and white mark.

The sidebar and sign-in screen use `CellarMark`, which references the SVG's
`cellar-mark` symbol and inherits the console's theme colors. The favicon uses
the same SVG, so all three surfaces share one source for the mark.

## Theme and design tokens

The console uses a restrained developer-tool style: neutral surfaces, crisp borders,
Geist for interface text, system monospace for identifiers and code, and blue for
links, selection and focus. Status colors always accompany a label or icon.
The semantic color roles take inspiration from [Geist](https://vercel.com/geist/colors)
and [Cloudflare's component system](https://cloudflare.github.io/cf-ui/).

`src/index.css` is the single source for theme values. Its foundation scale feeds
semantic roles, which are exported to Tailwind through `@theme inline`. Both
light and dark modes use the same component classes; avoid page-level hex colors
or separate dark-mode color overrides.

| Role | Tokens / usage |
| --- | --- |
| Surfaces | `background` for the workspace, `card` for panels, `muted` for recessed controls |
| Text | `foreground` for content, `muted-foreground` for supporting text |
| Actions | `primary` / `primary-foreground` / `primary-hover` for the main action; `info` for links and selection |
| Boundaries | `border`, `border-hover`, `input`; `ring` for keyboard focus |
| Status | `success`, `warning`, `destructive`, `info`, each with `-muted` and `-border` companions |
| Typography | 14px body, 12px desktop captions, 28px headings; captions rise to 14px on narrow screens |
| Spacing | `--space-*` on a 4px scale; `--page-gutter` and `--content-width` define the shell |
| Controls | 36px standard, 32px small, 44px large; primary mobile controls are at least 44px |
| Shape & motion | 8px base radius, subtle/floating shadow tokens, 150ms transitions with reduced-motion support |

Use the shared `components/ui` variants for forms, tables, badges and actions.
The theme selector provides light, system and dark modes, persists the preference,
and follows operating-system changes in system mode. There is no unmodified
single-key theme shortcut. Browser coverage verifies theme persistence, semantic
text contrast, mobile control sizes, navigation dismissal and resource filtering.

## SQL and KV workspaces

SQL keeps the database explorer beside ten session query tabs. CodeMirror uses
SQLite syntax and the inspected table/column names for completion. Run selected
SQL with Cmd/Ctrl+Enter; add Shift to run the entire editor. Explain runs
`EXPLAIN QUERY PLAN` in enforced read-only mode. The editor/results divider works
with pointer dragging and arrow keys. History retains the last 40 runs in memory.
Result sorting, column filtering, full-value inspection and CSV/JSON downloads
operate only on the returned rows; they do not fetch more data. SQL parameters,
arbitrary-query continuation and staged row editing remain future work.

KV keeps prefix pages beside the selected value. Edit creates an explicit draft
of the inspected bytes, metadata, storage tag and expiry. Text, JSON and base64
views preserve bytes; invalid UTF-8 stays binary. JSON formatting is explicit.
Save and delete pin the inspected namespace identity and carry its observed
revision to the owner. A conflict retains the
draft; compare the latest observation and review again before saving. Unchanged
fields adopt the latest baseline when resolving a conflict. Preserve expiry sends
an owner-side instruction, never a recalculated TTL. Absolute dates are entered
in local time and reviewed in UTC. Blank metadata is absent; JSON `null` remains
metadata. Values above 1 MiB expose metadata, size, revision and expiry without
loading their contents into the browser. Query tabs, history and drafts are not
persisted when leaving this workspace or reloading.

See [implementation and verification](../plans/004-sql-kv-workspaces.md).

Queue peek results open a read-only message sheet with payload preview, byte
export, age in the current queue, delivery attempts, visibility, lease expiry
and retained dead-letter transfer identity. It never consumes a message. The
source queue's earlier age and attempt history are not retained by transfers.

Workflow execution views can pin a step snapshot while selecting other steps or
generations, search retained history through bounded owner pages, and overlay
recorded step durations. Partial intervals and missing duration evidence are
labeled explicitly; a configured sleep duration is not elapsed execution time.
Compare retained generations or another instance beside one another, with each
run's recorded definition, node/path changes and inputs/outputs. Search fields
that exceed the payload budget are explicitly omitted; use history for full data.

Cron configuration and adoption remain separate from actual execution records.
Load retained attempts to inspect handler failure, retry requests, drained
background failures, timing and adopted configuration. An interrupted attempt
without a completion record stays unknown. Scheduled workflow acceptance links
to its workflow instance and does not claim workflow completion. The owner retains
256 attempts; history is available with data-read permission and starts with this
framework version. Removed schedules retain inspection access without rearming.

See [inspector implementation and verification](../plans/005-resource-inspectors.md).


## Events and delivery workspaces

Outboxes lists candidate root cells without probing every cell. Open a source
scope to inspect up to 25 lanes, exact backlog counts, oldest pending age,
blocked reasons, incarnation and dispatch revision. Retry requires `data_read`
and `operate`, pins the inspected revision, and records a reason. It never skips
or removes events. A lane with no pending payload rows can still retry a pending
or blocked producer seal.

Event streams and Subscriptions list deployed stream bindings. Name selectors
resolve through bounded catalog pages; the server rechecks the selected UUID's
full identity and derives authority from the current deployment. Declarations
and catalog entries are not activation or delivery proof. Statistics, producer
watermarks, subscription inventory and lifecycle history require the binding's
`administer` capability. Browsing events requires its `read` capability. Reads
return up to 25 events / 256 KiB and exclude payloads until explicitly requested;
they never consume or acknowledge events.

Subscription pause/resume requires `data_read` and `operate`; reset/delete also
requires `data_write`. Reviews pin the subscription UUID and generation, require
a reason and describe checkpoint/retention consequences. An active lease can
reject a change. Forced subscription deletion is an explicit additional choice.
The native durable receipt is bound to a server-derived console actor. A lost
reply retains its operation ID for **Check receipt**. Reconciliation matches the exact recorded request,
including its reason and expected identity; the console never replays
the mutation automatically. The Operations page links to receipt lookup, including after a workspace reload.
Receipt lookup does not inspect delivery lanes or repeat a mutation. Operation
history pages are owner observations.

Cells expose root event-state gauges. Nodes expose signed host counters,
cumulative duration histograms and current dispatch/waiter reservations, pinned
to the observed process generation. Host counters reset on restart and cannot
be attributed to individual cells. No cross-fleet aggregate or drain proof is
inferred from these observations. Cold inspection can activate a cell and
progress existing work. All counters and offsets remain decimal strings in the
browser to preserve 64-bit precision. Requested event payloads are delivered as
JSON text and rendered without a browser numeric conversion, preserving large
application integer values too.

See [event console implementation and verification](../plans/006-event-console.md)
and [primitive qualification status](../plans/outbox-implementation-status.md).
