# Queue, workflow and schedule inspectors

Status: IN PROGRESS. Based on `4de09b1` (PR #30) on 2026-10-14.

## Required result

- Queue: each loaded message opens a detail panel with its payload, age in this
  queue, delivery attempts, visibility, lease expiry and dead-letter provenance.
  Preserve non-consuming observations, binary bytes and bounded payload reads.
- Workflow: persistent pinned step details, searchable retained history, duration
  overlays and side-by-side comparisons of execution generations/instances and
  their recorded definition versions. Respect redaction, repeated steps, parallel
  events, incomplete history and per-workspace payload limits.
- Cron: configuration/adoption remains distinct from real execution history.
  Record actual handler attempts and scheduled workflow requests in the owning
  scheduling cell, including failures, retry decisions, background settlement,
  timing and the adopted deployment/schedule identity. Inspect this journal through
  the authenticated owner route; a saved or adopted schedule is not a successful run.

## Contracts

Queue inspection never leases or settles a message. Workflow diagrams remain
non-executable; comparison does not pin executable versions or change replay.
Cron history shares the scheduling cell's SQLite and output gate. Interrupted
attempts remain non-terminal observations; never invent a successful completion.
Preserve cron retry policy and event/background lifetimes. Signed history queries
use an owner-routed read-only RPC; an unsupported older owner must never treat
inspection as a legacy fetch/arm request. Cap retained records,
read pages, searches and browser payloads; missing historical facts stay explicit.

## Verification

Run root AGENTS.md checks in the checkout's stable target directory. Cover queue
snapshots and leases/transfers with native_queue; workflow history/definitions with
native_workflow, native_workflow_cell and native_artifacts; cron attempts, retries,
background work, cancellation and reactivation with native_cron. Exercise owner
routing on SDK nodes and the isolated preview. Run cellar-admin tests and dashboard
install/build/lint/browser tests. Review light/dark and narrow rendered views.

PR #30 is still open. Keep this work on `codex/resource-inspectors`, with #30 as
its dependency until the SQL/KV workspaces merge. Completion requires the entire
queue/workflow/cron objective and verified evidence for every requirement above.
