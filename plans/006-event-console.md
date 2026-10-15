# Outbox, event stream and subscription console

Status: IN PROGRESS. Based on `origin/main` at `bcc7bc0` (PR #27).

## Scope

- Cell outbox lanes, backlog/age, blocked reasons, guarded retry and durable receipts.
- Cell event-state observations; source, stream and catalog identity links.
- Declared event-stream bindings, bounded catalog discovery, stream statistics,
  paged non-consuming events, destination-observed producers and subscriptions.
- Guarded subscription pause/resume/reset/delete with reviewed reasons, exact
  incarnation/generation, durable receipts and explicit unknown outcomes.
- Node host-lifetime event counters, latency histograms and live reservations.
- Navigation from the fleet, application bindings, cells and node-owned cells.

## Contracts

Use existing signed outbox/stream/catalog/inventory/state routes and their owner,
output and discovery gates. The console never opens a cell database. Resolve
authority from the current deployed binding on the server; the browser cannot
submit grants. Pin resource UUIDs and subscription/source revisions. Native
operator labels are bound to the console principal and fleet. Keep decimal
strings exact in the browser. Reads are bounded and do not consume or acknowledge
events; payload inspection is explicit. Mutations are audited before dispatch,
are never automatically replayed, and retain ambiguous outcomes until a matching
owner receipt is inspected and matches the complete recorded request. No provisioning, source destruction, forced stream
retirement, background polling or aggregate-fleet completeness claims are added.

The underlying primitive remains under development. UI fixtures do not qualify
durability, backend recovery, all failure cuts or old-build interoperability.

## Verification

Completed: `npm ci`, dashboard build/lint, 44 browser tests (including ten
new event tests), native dependency policy and documentation reference checks.
Rendered desktop subscription, mobile outbox and dark mobile stream views inspected.
`cargo check -p cellar-admin --locked`, `cargo format` and the repository
formatter check also pass. Exact u64 values,
explicit payload requests, guarded reset offsets, permissions, bounded pagination
lost-response receipt recovery and seal-only retries have browser coverage.

GitHub Actions could not start because of account runner/billing limits. Local
Rust tests are being rerun with debug symbols disabled to reduce build I/O; test
assertions and debug assertions are unchanged.

Pending: Rust management/API tests, the real-node owner-route test, native CLI
regressions and the complete workspace check. Real-node testing uses a
private fleet store and caller-owned executors. It does not qualify object-store
fault recovery or 10,000-cell event throughput; those remain primitive release
gates in `outbox-implementation-status.md`.
