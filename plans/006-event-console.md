# Outbox, event stream and subscription console

Status: console parity update against `origin/main` at `63d14f8` (PR #63).
The earlier PR #35 verification remains recorded below. Event-delivery release
qualification is tracked independently in `outbox-implementation-status.md`.

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
provisioning, background fleet polling or aggregate-fleet completeness claims
are added. Reviewed source destruction/recreation and coordinated/forced stream
retirement reuse the existing native control protocols.

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
  `crates/cellar-native/clone/mod.rs:456` use of `chunks_exact`, rejected by the new
  `chunks_exact_to_as_chunks` lint. No lint suppression or assertion changes
  were introduced.
- GitHub Actions could not start because of account runner/billing limits.

Local builds disable debug symbols to reduce build I/O; test and debug
assertions remain enabled. These checks do not qualify object-store fault
recovery, every failure cut, mixed builds or 10,000-cell event throughput. Those
remain primitive release gates in `outbox-implementation-status.md`.


## Console parity with PR #46

| Backend surface | Browser surface |
| --- | --- |
| Source inspect/retry/status/audit | Cell Outbox lanes, exact revisions, reasons, receipt recovery and history |
| Source destroy/recreate | Source lifecycle review with incarnation confirmation and loss acknowledgment |
| Exact unfinished-destruction replay | Explicit resume from the retained receipt; server verifies saved original intent and operator |
| Stream describe/read/sources | Retention/storage observations, payload opt-in, non-consuming pages and producer watermarks |
| Subscription list/describe | Bounded tombstone inventory; fresh owner inspector, retained progress, expired history, leases and retry eligibility |
| Subscription pause/resume/reset/delete | Reviewed generation-guarded controls; destructive actions require data-write permission |
| Stream retire/retirement-status | Revision-pinned coordinated/forced retirement; accepted cleanup stays distinct from terminal completion |
| Platform retirement-proof | Exact-resource proof in the workspace and retained catalog inventory, independent of resource grants |
| Root census, descriptors and owner state | Explicit 25-prefix fleet inspection with four concurrent probes, exact page gauges and unavailable coverage |
| Resources/producers/source-history inventory | Bounded metadata sheets, including roots no longer represented by current bindings |
| Host observations | Existing generation-pinned node counters, latency histograms and reservation gauges |

Fleet inspection runs one provider page per request; its cursor binds the fleet,
bucket/prefix and requested root prefix. Duplicate/malformed prefixes count as
unavailable. It does not inspect facets, certify historical completeness or
combine observations across pages into a fictitious fleet snapshot. All 28
owner-state gauges are available; root SQLite bytes exclude WAL/LTX/bucket/facets.

The console stores lifecycle request bodies in its independent audit database
before dispatch so lost-result reconciliation can compare the original intent.
It never accepts browser-supplied grants or operator identities. Terminal audit
proofs survive late acceptance and failures. Missing receipt bodies remain
unknown. Source destruction can report checking/verified/cleaning while closed;
stream retirement can report retiring while cleanup is incomplete.

All owned protocol/schema markers remain 1; mixed participant builds remain
unsupported.

The extended real-node scenario reproduced a source-verification race twice:
request-time retirement proof returned after durable cleanup had advanced the same
closure to `verified`. The snapshot and source identity were unchanged, but the
older work generation reported `ConfigConflict`. Superseded verification now
returns without writing; source identity replacement and a changed snapshot in
the current installation still fail. The scenario retains destruction completion,
recreation, historic replay isolation and full node shutdown assertions.

### Verification for this update

On Rust 1.97.0 / macOS with the isolated checkout target directory:

- Workspace checking passes with all targets/features and no warnings.
- All 19 admin tests and nine management tests pass.
- The extended real-node console adapter scenario passes, including completed
  source destruction/recreation and full returned node shutdown proof. Both
  source-destruction recovery/fencing tests pass unchanged.
- `native_event_stream`, seven `native_artifacts` tests and three `native_cli`
  tests pass.
- Dashboard install/build/lint and the full 56 browser tests pass. The final UI
  edits additionally pass all 22 event cases and the subscription inspector
  case. Desktop and narrow lifecycle reviews were rendered and inspected.
- Rust formatting and documentation checks pass. Dependency policy covers
  403 packages with no JavaScript engine.

Workspace Clippy with all targets/features and warnings denied passes.
Browser fixture coverage and the isolated real-node adapter scenarios are
separate evidence; neither establishes complete event-delivery release
qualification.
