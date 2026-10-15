# Outbox, event stream and subscription console

Status: DONE (implementation). Based on `origin/main` at `826f890` (PR #34),
including the event primitives from PR #27. Verification exceptions are recorded below.

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
owner receipt is inspected and matches the complete recorded request. No
provisioning, source destruction, forced stream retirement, background polling
or aggregate-fleet completeness claims are added.

Catalog reconciliation consumes only still-pending configuration claims. Duplicate
or stale asynchronous completions cannot erase accepted metadata or attach a
late failure to an accepted operation. A deterministic SQLite regression test
covers both success and failure callbacks.

The underlying primitive remains under development. UI fixtures do not qualify
durability, backend recovery, all failure cuts or old-build interoperability.

## Verification

Completed on the merged branch:

- `npm ci`, dashboard build/lint and 47 browser tests, including thirteen new
  event tests. Desktop subscription, mobile outbox and dark mobile stream views
  were rendered and inspected.
- `cargo format --check`: 500 Rust files; native dependency policy: 403 packages,
  no JavaScript engine; documentation: 36 documents, 6 flows, 386 local references.
- Rust 1.98.1 / isolated Linux Docker: 222 Cellar library tests, admin unit and
  event API tests, the real-node console adapter test, `native_event_stream`,
  seven `native_artifacts` and three `native_cli` tests, and
  `cargo check --workspace --all-targets --all-features --locked`.
- Rust 1.97.0 / macOS: the full `cellar-admin` suite (two unit and fifteen API
  tests, including 10,000-cell pagination), plus both existing source/stream
  owner-routing tests with handoff, immutable receipts and revocation checks.
- Rust 1.97.0 / macOS: workspace Clippy with all targets/features and warnings
  denied passes.

The real-node console test uses a private local fleet store, real signed owner
routes and caller-owned executors. It covers source retry/receipt identity,
current named-binding resolution, non-consuming metadata/payload reads,
subscription changes, root state, generation-pinned host metrics and a complete
returned shutdown report. Browser cases additionally cover exact 64-bit offsets,
raw payload JSON, lost-response and post-reload receipt recovery, permissions,
expired cursors, seal-only retries and mobile overflow.

Verification exceptions:

- The existing multi-node source/stream operator tests failed in Linux Docker
  with their one-second lease / three-second operation fixtures. The repeat
  failed during node activation, before console adapter calls; both tests pass
  unchanged on macOS. The console's real-node test passes on Linux using normal
  operation budgets. These results do not qualify the short-budget Linux case.
- Full workspace Clippy on Rust 1.98.1 stops at an unchanged
  `crates/cellar-native/clone.rs:456` use of `chunks_exact`, rejected by the new
  `chunks_exact_to_as_chunks` lint. No lint suppression or assertion changes
  were introduced.
- GitHub Actions could not start because of account runner/billing limits.

Local builds disable debug symbols to reduce build I/O; test and debug
assertions remain enabled. These checks do not qualify object-store fault
recovery, every failure cut, mixed builds or 10,000-cell event throughput. Those
remain primitive release gates in `outbox-implementation-status.md`.
