# Plan 008: Implement safe, independently placed read replicas

> Executor instructions: read this plan completely before editing. Implement
> one work package at a time, run its gates, and update its row in this package's
> `README.md` and `tasks.json`. The companion technical design defines the
> protocol; qualification defines required observations. No chat context is needed.
> All proposed symbols, routes, test targets and new files below are future work.
> Do not report a proposed test command as having passed before its target exists.

## Status and scope

- Priority: P1; effort: XL; risk: HIGH; category: direction/architecture.
- Planned at: `0efd8f4d871199542f97f9fe0b74086a38606d02`, 2026-10-19.
- Build base: the pinned source revision or a reviewed successor with passing drift checks.
- Prerequisites: full baseline source, Rust toolchain/dependencies, writable
  checkout-specific target directory, ordinary integration-test environment.
- First releasable behavior: explicit typed command dispatch plus stale and
  minimum-version query calls;
  ordinary cell calls and write durability keep their current semantics.
- No runtime implementation or qualification was performed while writing this plan.

Use [Design 007](007-technical-design.md) for field semantics and failure rules.
Use [Qualification 009](009-qualification.md) for test IDs and release gates.
Use [Guide 010](010-application-integration.md) for the proposed end-to-end
application builder experience; it must become a compiling example in RR-12.
Do not weaken either document to make a test pass; record a design amendment
when evidence requires a different contract.

## Current state: verify these excerpts before implementation

The following are excerpts from the pinned source, with surrounding comments
omitted. They are drift anchors, not suggested code replacements.

`crates/cellar-logic/types.rs`, `OwnerRecord`:

```rust
pub struct OwnerRecord {
    pub lineage: Option<String>,
    pub node: Option<NodeId>,
    pub epoch: Epoch,
    pub etag: String,
}
```

`crates/cellar/durability/replication/backend.rs`, `await_durable`:

```rust
let ticket = handle.req_seq.fetch_add(1, Ordering::SeqCst) + 1;
let source = self
    .wait_for_durability_ticket(&handle, cell, epoch, ticket)
    .await?;
Ok((position, source))
```

`crates/cellar-logic/output_gate.rs`, `durable_reached`:

```rust
if proven && source == ProofSource::Bucket {
    let (cell, epoch) = (gate.cell.clone(), gate.epoch);
    effects.push(Effect::VerifyOwnership { op, cell, epoch });
    return;
}
self.settle_gate(op, proven, effects);
```

`crates/cellar/application/mod.rs`, current mutable cell contract:

```rust
pub trait Cell: Send + Sync + 'static {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output>;
}
```

`crates/cellar-ltx/src/replica.rs` describes continuous follow/apply as deferred;
the current restore interface performs one restore. Normal SQLite
`open_at_epoch` calls writable initialization; it is not a read-replica opener.

Preserve ADR 002: "One authoritative owner and epoch control its state" and
"Recovery accounts for predecessor log history before takeover." The new
read-serving role needs an additive documented decision, not a competing owner
policy in an I/O adapter.

## Repository conventions and ownership

Read root `AGENTS.md` and any more specific guidance in the complete source.
Keep `cellar-logic` dependency-free and sans-I/O. Its interface is events,
effects and data. Actors own state; adapters return versioned completions.
Use execution-domain services for tasks, blocking work, filesystem and clocks.
Keep `Result` failures typed; retain ambiguity of submitted mutations.

Match existing patterns:

- `crates/cellar-logic/tests/routing_cache.rs`: public `on_event`, effect
  assertions and `State::validate()`; no internal-test configuration required.
- `crates/cellar-ltx/tests/checkpoint_scheduling.rs`: injected `LtxHost`, real
  SQLite/LTX, restore and SQL verification.
- `crates/cellar/tests/native_log.rs`: independent log hosts and complete cleanup.
- `crates/cellar/tests/native_node.rs`: real node/start/recovery/shutdown evidence.
- `crates/cellar/tests/native_daemon.rs`: isolated executable copies and process I/O.

For a component with submodules create `feature/mod.rs`; do not combine
`feature.rs` and `feature/*.rs`. Preserve public module paths through `lib.rs`
path mappings. Core and LTX library tests are disabled at baseline: new important
tests belong in normally runnable `tests/*.rs` targets.

## Baseline setup and drift checks

In a full checkout selected for implementation:

```sh
git status --short
git rev-parse HEAD
git merge-base --is-ancestor 0efd8f4d871199542f97f9fe0b74086a38606d02 HEAD
git diff --stat 0efd8f4d871199542f97f9fe0b74086a38606d02..HEAD -- crates/cellar-logic crates/cellar-ltx crates/cellar tools/perf docs/adr .github/workflows/native-sdk.yml
rustc --version
```

Expected: known working-tree state, the pinned base or reviewed successor,
ancestry exit 0, and understood changes to the source map. If in-scope source
changed, compare symbols/excerpts and revise the plan before touching proof or
recovery. The baseline CI uses Rust 1.98.1. Missing toolchain or external harness
is a recorded prerequisite, not evidence that the feature passes.

Choose a unique target directory on the writable workspace volume. Example from
repository convention, with a task-specific name:

```sh
export CARGO_TARGET_DIR="$HOME/Workspace/crabbuild-target/cellar-read-replicas-implementation"
```

Check this name is not used by another checkout before building. Do not clean
or reuse another checkout's artifacts. Run these existing gates before edits:

```sh
cargo format --check
cargo check-layout
python3 tools/check_native_dependencies.py
python3 tools/docs/check.py
cargo test -p cellar-logic --test routing_cache --test event_interleavings --locked -- --test-threads=1
cargo test -p cellar-ltx --test checkpoint_scheduling --locked -- --test-threads=1
cargo test -p cellar --test native_counter --test native_replication --test native_log --test native_node --locked -- --test-threads=1
```

Expected: each exits 0; Cargo test output lists nonzero tests and no failures.
Record exact counts and any pre-existing failures separately. Do not enable
`cellar_internal_tests` without its documented external sources. No prerequisite
allows skipping the new ordinary integration tests.

## Allowed change areas and boundaries

All paths are relative to the complete repository; proposed files are marked
new in the work packages. Shared source is changed only for its owning contract.

| Area | Allowed responsibility |
| --- | --- |
| `crates/cellar-logic/` | Cut, placement, replica, routing and publication decisions |
| `crates/cellar-ltx/` | Exact bounded restore and verified physical apply mechanics |
| `crates/cellar/durability/replication/`, new `durability/read_replica/` | Completed receipts, source manifests, transfer pins and read-copy lifecycle |
| `crates/cellar/actor/`, `runtime/`, `execution/` | Event/effect integration, query execution, budgets and joining |
| `crates/cellar/application/`, `storage/sqlite/`, `sdk.rs` | Read-only context/storage, query registration and versioned call surfaces |
| `crates/cellar/fleet/`, `node/`, selected CLI files | Advisory placement, authenticated peer routes, inspection and explicit options |
| Tests, selected examples, `tools/perf/` | Behavior and five-node qualification |
| ADRs and affected guides | Contract documentation after behavior exists |

Out of scope: replacing ownership CAS, changing follower quorum/ack rules,
making B/C node-log voters, weakening predecessor recovery, introducing a new
crate/dependency without justification, normal GET/RPC rerouting, generalized
SQL endpoints, JavaScript engines, global consistency claims, demand-based
replica-count autoscaling and unrelated layout changes. Automatic policy-based
node placement and repair are required in RR-07A. Do not reformat unrelated source.

No commit, push, issue, PR or deployment is part of executing this document unless
the operator separately requests it. Work packages are suggested review/commit
boundaries, not authorization to publish. Prefer one coherent review per package.

## RR-01 — Define decisions and executable invariants

Depends on: none. Effort: M. Risk: HIGH because later work relies on these types.

Files: new `crates/cellar-logic/read_replica.rs`, existing `lib.rs` and `types.rs`;
new `crates/cellar-logic/tests/read_replica_decisions.rs`; proposed
`docs/adr/004-read-replica-serving.md` (use next free ADR number if it changed).

Steps:

1. Add separate data types for source identity, exact LTX cut, completed capture
   receipt, accepted safe cut, query consistency, placement and lifecycle
   generation. Keep output fingerprint/ticket/TXID/log offset distinct.
2. Define the lifecycle, monotonic same-history advancement, epoch-reconciliation
   outcomes, query eligibility and typed unsupported/freshness refusals.
   Define Command/Query operation IDs and immutable contract definitions;
   expected-kind/digest mismatch refuses dispatch before any callback runs.
   Scope keys by existing application identity, class and operation ID. Support
   collections of operations; reject collisions rather than replacing handlers.
3. Integrate decisions through core event/effect interfaces; extend `validate`
   with invariants I1–I10. Adapters must not mutate this state directly.
4. Add normally runnable tests that enumerate delayed, duplicate, reordered,
   cancelled and stale-generation completions. No network or filesystem.
5. Record the additive ADR: one writer remains; read nodes are serving caches;
   stale and minimum-version contracts only; proof rules unchanged.

Verify:

```sh
cargo test -p cellar-logic --test read_replica_decisions --locked -- --test-threads=1
cargo check -p cellar-logic --locked
cargo check-layout
```

Expected: new target exists and runs named cases `stale_completion_cannot_publish`,
`read_progress_does_not_credit_write_proof`, `cross_epoch_requires_bridge`,
`minimum_version_never_degrades`, `unsupported_consistency_is_explicit` and
`disconnected_read_requires_policy`; all pass. Existing logic targets still pass.
Also require `operation_ids_are_scoped_by_application_and_class` and
`many_operations_keep_distinct_definitions`.

## RR-02 — Bind completed proof to exact captured LTX cuts

Depends on: RR-01. Effort: L. Risk: HIGH.

Files: `crates/cellar/durability/replication/backend.rs`, `contracts.rs`,
`handle.rs`; `crates/cellar/runtime/cells/host.rs`; new private receipt helpers
under the owning replication module with corresponding `lib.rs` mapping only if
needed; new `crates/cellar/tests/native_read_replica_proof.rs`.

Steps:

1. In `sync_cell`, bind pre-capture ticket high-water to resulting immutable LTX
   terminal TXID/checksum and artifact references. Preserve existing upload credit.
2. In ship capture and ordered `apply_round`, retain the equivalent mapping for
   fleet credit. Keep failed/later discarded pipeline rounds uncredited.
3. Return a covering completed receipt from the durability wait. Preserve the
   original output-position coverage result; do not substitute TXID for it.
4. Handle already-covered/no-op capture, concurrent bucket/fleet success and
   same-epoch handle replacement with explicit identities. Bound ledger retention
   by live waiters/publication references and coalesce completed ranges.
5. Instrument deterministic public-boundary test adapters to pause capture and
   credit without depending on the unavailable internal simulation harness.

Verify:

```sh
cargo test -p cellar --test native_read_replica_proof --locked -- --test-threads=1
cargo test -p cellar --test native_replication --test native_log --locked -- --test-threads=1
```

Expected: real SQLite commits racing two captures yield the first completed
receipt's exact cut; later unproven capture is excluded. Named cases include
`ticket_maps_to_completed_capture`, `bucket_and_fleet_receipts_do_not_mix`,
`failed_pipeline_tail_has_no_receipt`, `noop_capture_returns_existing_cut` and
`receipt_ledger_is_bounded`. All existing proof/recovery tests pass.

## RR-03 — Authorize publication at successful core settlement

Depends on: RR-02. Effort: M/L. Risk: HIGH.

Files: `crates/cellar-logic/output_gate.rs`, `types.rs`, `lib.rs`;
`crates/cellar/actor/mod.rs`, `actor/local_request.rs` and affected runtime
proof contracts; new `crates/cellar/durability/read_replica/mod.rs` and
`publication.rs`; new public integration cases in the RR-01/RR-02 targets.

Steps:

1. Carry completed receipt identity through `DurableReached` into the live
   barrier. Bucket path cannot publish before successful `OwnershipVerified`.
2. At successful settlement, emit `PublishSafeCut` for a matching source
   activation. Failed, stale and cancelled gates emit no publication for their
   candidate. An independent successful later proof may publish a covering cut.
3. Add a typed source snapshot barrier using the same core validation for a
   read-enabled active owner when publication has no recent head.
4. Deliver safe-head notices using a bounded coalescing channel. Read publication
   failure/exhaustion affects read availability, not mutation proof success.
5. Preserve non-replica modes and existing response/egress behavior. Do not use
   adapter proof telemetry counters as authority.

Verify:

```sh
cargo test -p cellar-logic --test read_replica_decisions --locked -- --test-threads=1
cargo test -p cellar --test native_read_replica_proof --test native_counter --locked -- --test-threads=1
```

Expected: `bucket_cut_waits_for_owner_verification`, `fenced_gate_does_not_publish`,
`cancelled_gate_does_not_publish` and `blocked_read_delivery_does_not_block_write`
pass. A paused read-delivery worker can retain no unbounded artifacts and cannot
prevent a write's existing proof from completing.

## RR-04 — Implement exact bootstrap and verified LTX materialization

Depends on: RR-01. Effort: L. Risk: HIGH. Can proceed independently of RR-02/03.

Files: new `crates/cellar-ltx/src/follow.rs`, existing `lib.rs`, `replica.rs`,
`client/epochs.rs` only as needed for exact bounded planning;
new `crates/cellar-ltx/tests/read_replica_apply.rs`;
new `crates/cellar/durability/read_replica/materializer.rs`, `snapshots.rs`;
new `crates/cellar/tests/native_read_replica_materializer.rs` for host-owned
generation files, fsync/rename, pins, budgets and restart. RR-03 and RR-04 may
both extend the owning `mod.rs`; land those shared declarations serially.

Steps:

1. Implement checked physical LTX application into an unpublished builder with
   existing host adapters. Validate continuity, page size, growth/truncation,
   complete segment bounds and actual database checksum at the selected cut.
2. Add exact-cut restore inputs: complete pinned artifact plan through the
   requested safe cut. Never include a segment crossing the bound by clipping
   metadata or reconstruct from latest unbounded state.
3. Publish unique immutable generation files and manifests in the crash order
   specified by Design 007. Reader pins retain old files through generation swap.
4. On crash/restart, validate a completed checkpoint, discard partial builder
   work and resume/reseed. Use full materialization; reject sparse paged caches.
5. Enforce configured bytes before copy/apply and generation-count budgets.
   Make checksum or gap failure retain the old image and request reseed.

Verify:

```sh
cargo test -p cellar-ltx --test read_replica_apply --test checkpoint_scheduling --locked -- --test-threads=1
cargo test -p cellar --test native_read_replica_materializer --locked -- --test-threads=1
cargo check -p cellar-ltx --no-default-features --locked
cargo check -p cellar-ltx --all-features --locked
```

Expected: `apply_matches_restore_at_exact_cut`, `gap_requests_reseed`,
`corrupt_segment_never_publishes`, `growth_and_truncation_match_checksum`,
`reader_keeps_old_image_during_swap`, `restart_ignores_partial_publication`,
`segment_crossing_cut_is_rejected` and `disk_cap_preserves_last_generation`
pass. Pure LTX mechanics run in the LTX target; generation/pin/publication crash
cases run in the host materializer target. Fault-inject each fsync/rename boundary;
real SQL must read the expected
old or new coherent image, never a partially updated mixture.

## RR-05 — Add read-only query registration and runtime

Depends on: RR-04. Effort: L. Risk: HIGH.

Files: new `crates/cellar/application/query/mod.rs`, existing `application/mod.rs`,
`application/cells.rs`, `sdk.rs`; new `crates/cellar/storage/sqlite/replica_read.rs`,
existing SQLite module declaration; new `crates/cellar/runtime/read_replica/mod.rs`
and `query.rs`; corresponding private `lib.rs` module mappings;
new `crates/cellar/tests/native_read_replica.rs`.

Steps:

1. Add `CellContract`/typed builder registration for Command and Query operations,
   backed by named callbacks and codecs. Primary command callbacks can use the
   normal owner instance; query callbacks receive only ReadContext and request.
   Add default-safe Application query dispatch. Query execution never calls the
   normal cell constructor or `Cell::invoke` path. No procedural macro required.
   Make registrations additive across multiple commands, queries, cell classes
   and feature modules. Allow distinct request/response types, shared business
   helpers and ordinary Rust dependency injection within context capabilities.
   Provide explicit typed operation handles/names when payload types are reused.
   Accept an explicit primary factory or reference the application's existing
   factory registration. Validate required factories and preserve dependency
   injection without invoking them for queries.
2. Implement `ReadContext`/`ReadStorage` for pinned images, bounded KV/SQL reads,
   owned JSON input/output and event/domain lifetime checks.
3. Open existing images read-only without schema/wake initialization. Install
   an allowlisted authorizer; deny write statements, ATTACH, writable PRAGMAs,
   extension/file functions and direct framework-table reads.
4. Deny ordinary Context/capability conversion and escaping cursors. Register
   no alarms, queues, outbox work, sockets or background event work on replicas.
5. Add SQL interruption, deadlines, query/result limits and generation/digest
   admission. Test native trust limitations honestly; no sandbox claim.

Verify:

```sh
cargo test -p cellar --test native_read_replica --test native_counter --locked -- --test-threads=1
cargo test -p cellar --all-features --doc --locked
```

Expected: `query_does_not_construct_cell`, `sql_write_and_effects_are_denied`,
`all_reads_pin_one_generation`, `retired_query_handle_refuses_access`,
`query_timeout_interrupts_sql` and `incompatible_query_digest_is_refused` pass.
Verify denied operations leave image bytes, schema and framework wake tables
unchanged. Existing mutable application interfaces continue to compile/pass.
Also require `duplicate_operation_registration_is_rejected` and
`typed_query_callback_has_no_primary_instance`.
Also require `registration_modules_compose_without_overwrite` and
`operations_allow_distinct_request_response_types`.
Also require `contract_factory_runs_only_for_primary_commands`.

## RR-06 — Implement authenticated bootstrap and tail sessions

Depends on: RR-03, RR-04. Effort: L. Risk: HIGH.

Files: new `crates/cellar/durability/read_replica/source.rs`, `protocol.rs`,
`sessions.rs`; new `crates/cellar/node/network/read_replica.rs`;
existing `node/mod.rs`, `node/ingress.rs`, `fleet/peer/auth.rs`, host service and
module wiring; new `crates/cellar/tests/native_read_replica_transport.rs`.

Steps:

1. Add the private routes/capability from Design 007 using existing authenticated
   node-request and bounded-body patterns. Authenticate manifest/tail/error
   responses with session/nonce/source-bound envelopes; baseline version headers
   alone do not authenticate a plain-HTTP response. Keep follower append/voting
   unchanged.
   Include successful query/status responses, full value/served-at/fallback
   metadata and readiness observations in the authenticated body.
2. Bootstrap from an exact pinned safe manifest. Stream bounded chunks; the
   receiver checks source identity, destination/session, digest and actual LTX
   continuity before publishing readiness.
3. Serve complete proven tail segments after the requested cut. Add duplicate,
   gap, source-change, expiry and `SnapshotRequired` responses.
4. Bound sessions, bytes, pins, worker tasks and long-poll deadlines. Drop slow
   readers without retaining the durability log indefinitely.
5. Close/join tasks through the execution domain. Unsupported peers return a
   specific capability error without changing ordinary owner transport.

Verify:

```sh
cargo test -p cellar --test native_read_replica_transport --test native_replication --locked -- --test-threads=1
```

Expected: `bootstrap_excludes_later_unproven_state`, `tail_is_idempotent`,
`wrong_target_or_generation_is_refused`, `malformed_frame_is_bounded`,
`expired_pin_requires_new_snapshot`, `source_change_cancels_old_session` and
`transport_shutdown_joins_all_tasks` pass. No public route accepts raw SQL or
raw filesystem paths. Logs redact authentication and payload data.
Also require `manifest_response_requires_authentication`,
`replayed_response_cannot_change_session`, `chunk_digest_matches_authenticated_manifest`
and `tampered_query_result_is_refused`.

## RR-07 — Add independent placement, lifecycle and query routing

Depends on: RR-05, RR-06. Effort: L. Risk: MED/HIGH.

Files: new `crates/cellar/fleet/read_placement.rs`; existing core replica module,
actor config/dispatch, `application/cells.rs`, runtime/query host, execution
services; new/existing transport query route; tests in `native_read_replica.rs`,
`native_read_replica_transport.rs` and `read_replica_decisions.rs`.
For thin HTTP bindings also update the existing `crates/cellar-axum/builder.rs`
and service/cell adapters as needed; add
`crates/cellar-axum/tests/cqrs.rs`. Keep its current completed-router middleware
and native/Axum host parity. Do not introduce another HTTP routing framework.

Steps:

1. Add feature-off policy and the internal placement lifecycle that RR-07A's
   automatic scheduler drives. Explicit B/C assignments are test/debug fixtures,
   not normal operating requirements. Directory CAS remains advisory,
   epoch-monotonic and never a second owner source.
2. Add bootstrap/catch-up/serving/lagging/source-change/drain decisions and
   versioned readiness completions. Expose bounded peer status observations.
3. Route only `Stub::query` to compatible, admitted snapshots. Existing fetch,
   RPC, alarms and other cell events remain owner routed.
   Add the shared CQRS dispatcher: resolve registered operation ID/kind/digest,
   route declared commands to current primary and declared queries to eligible
   snapshots, then invoke the destination callback. Typed handles and ingress
   adapters must share this one decision path. Add a generic ingress adapter
   hook with application-owned authorization/cell resolution; no new router
   framework or manual node addresses are required in business callbacks.
   Dispatch each selected operation independently and enforce its own query
   policy. Request type ambiguity requires an operation handle, not a guess.
   Application orchestrators may call many typed operations through existing
   contexts/workflows; replica ReadContext remains single-cell read-only.
   Provide thin native/Axum command/query ingress bindings over this dispatcher
   with authorized target resolution, typed codecs and version metadata. Existing
   cell_route remains owner-routed; contract registration exposes no public route
   automatically. Shared middleware must still cover generated bindings.
4. Add deadline-bounded retry selection and explicit `WaitThenOwner` fallback
   using the same read callback and safe owner-local immutable image. Never
   substitute ordinary GET/RPC. Bound fallback separately from writer work.
5. Enforce disconnected-snapshot policy, wrong digest/tenant/scope refusal and
   typed errors that do not poison ordinary cell stubs.

Verify:

```sh
cargo test -p cellar-logic --test read_replica_decisions --locked -- --test-threads=1
cargo test -p cellar --test native_read_replica --test native_read_replica_transport --test native_node --locked -- --test-threads=1
cargo test -p cellar-axum --test cqrs --locked -- --test-threads=1
```

Expected: A with durability D/E serves writes while B/C serve queries; stopping
B/C does not change A's ensemble. `ordinary_rpc_stays_owner_routed`,
`query_routes_to_eligible_replica`, `fallback_uses_safe_read_handler`,
`stale_directory_cannot_grant_authority`, `replica_outage_bounds_owner_fallback`
and `query_never_creates_missing_cell` pass.
Also require `declared_command_routes_to_current_primary`,
`query_label_cannot_invoke_registered_command`,
`ingress_and_typed_calls_share_dispatch_policy` and
`query_contract_policy_cannot_be_weakened`.
Also require `multiple_commands_and_queries_dispatch_independently`,
`per_query_policies_remain_independent` and
`ambiguous_payload_type_requires_operation_handle`.
Also require `native_and_axum_share_operation_dispatch` and
`public_query_route_preserves_minimum_version` in the new Axum target.

## RR-07A — Automatically select replicas and reconcile failures

Depends on: RR-07. Effort: L. Risk: HIGH. Required first-release work, replacing
the previous manual placement assumption. Feature stays off until RR-09 provides
the autonomous takeover integration and the repair qualification gates pass.

Files: new `crates/cellar-logic/replica_placement.rs` and
`crates/cellar-logic/tests/replica_placement.rs`; new
`crates/cellar/fleet/replica_repair/mod.rs`, `supervisor.rs`, `inventory.rs`;
existing replica decision/placement modules, actor/node config and execution
services; existing `types.rs`/ownership capacity observations as necessary;
new `crates/cellar/tests/native_read_replica_repair.rs`. Preserve core I/O rules
and existing public paths through `lib.rs` mappings.

Steps:

1. Define policy-selected existing cells, desired additional read-copy count,
   eligible pools, required/preferred domains, observation/repair/cooldown,
   surge, admission and retry budgets. Zero replicas drains read copies only.
2. Implement deterministic capacity-aware candidate ranking with hard constraints,
   sticky healthy assignments and anti-flap hysteresis. Destination admission
   issues expiring generation-bound provisional capacity handles; stale or failed
   reservations cannot consume capacity forever or erase published images.
3. Add bounded ownership-inventory scans and durable advisory enrollment/progress.
   Resume after coordinator restart. Lost enrollment notices are repaired by
   paginated inventory scans. Never create a missing business cell during repair.
   Repair must also wake healthy sticky-but-evicted owners and acquire deliberately
   released existing state when a source is needed. Count source activity pins
   through admitted repair and honor cooldown afterwards. Implement validated
   Quiescent observations for completed normal handoff, not runtime-absence guesses.
   Sticky idle owners refresh source status from verified retained ownership,
   current process authority and clean-stop cut without factory execution, or
   keep counted source work until that path exists. Test multiple observation
   TTLs with default connected-only admission; idle reads must not force churn.
4. Run execution-domain-owned supervisors on periodic jittered ticks and policy,
   membership/readiness events. Healthy primary coordinates its read copies;
   failed-primary candidates stagger attempts through the existing owner/recovery
   interface. Neither ranking nor advisory CAS grants writer authority.
5. Automatically replace lost read copies, complete bootstrap/catch-up before
   counting them ready, and use add-before-remove for planned changes. Rejoining
   nodes reconcile plan generation instead of stealing readiness back.
6. Report UnderReplicated/PolicyUnsatisfied when feasible capacity is absent and
   retry automatically when it returns. Bound repair storms through hard target,
   source and per-cell limits; record the aggregate bound without inventing a
   globally atomic semaphore under partition. Separate repair from write proof.

Verify:

```sh
cargo test -p cellar-logic --test replica_placement --locked -- --test-threads=1
cargo test -p cellar --test native_read_replica_repair --test native_node --locked -- --test-threads=1
```

Expected: `deterministic_replica_ranking_is_stable`,
`placements_enforce_failure_domain_constraints`,
`exhausted_capacity_reports_under_replicated`,
`concurrent_reconcilers_cannot_publish_stale_plan`,
`replica_loss_triggers_bounded_repair`,
`automatic_move_waits_for_replacement_readiness`,
`node_rejoin_does_not_trigger_repair_churn`, `repair_runs_without_incoming_requests`,
`controller_restart_resumes_pending_repair`, `repair_storm_respects_cluster_budgets`,
`stale_reservations_release_target_capacity` and
`policy_change_does_not_admit_retired_replica` pass. Reuse real node/session/file
adapters in the host target; do not prove automatic repair only with a mocked
set of nodes. In this package, writer-loss intents can remain admission-blocked
until RR-09 adds and tests their full safe takeover path.
Also require `idle_release_and_sticky_cells_repair_without_requests` and
`lost_enrollment_is_rediscovered_without_activation_churn`, including correct
Quiescent observation invalidation after new ownership.

## RR-08 — Expose write-version receipts and minimum-version reads

Depends on: RR-03, RR-07A. Effort: M/L. Risk: HIGH.

Files: new `crates/cellar/application/query/version.rs`, existing query module,
`application/cells.rs`, runtime reply contracts, actor local request replies,
peer query serialization, core replica decisions, SDK exports and relevant tests.

Steps:

1. Add explicit `rpc_with_version` or equivalently named versioned command call;
   retain existing RPC result shapes. Attach only its successful gate's SafeCut.
   Use this proof-backed reply carrier for the new typed CQRS command interface,
   which returns CommandReply with value/version. Keep legacy RPC opt-in. Hold
   new command API enrollment disabled until this package supplies exact versions.
   For no-change/idempotent/gate-follower replies, retain an exact accepted
   covering receipt or request a core snapshot barrier. Missing durable base
   gives typed version unavailability; never synthesize TXID zero or sample a
   later adapter head. Preserve ambiguity after a submitted mutation.
2. Encode canonical bounded token payloads, domain-separated MAC and expiry/key
   checks using existing credentials without exposing their values.
3. Implement `AtLeast` eligibility and waiting within caller deadlines. Return
   actual `served_at` for session propagation; do not silently select Stale.
4. Reject wrong cell, wrong fleet, tampered, expired, oversized or unknown-history
   tokens before allocating large wait queues. Key rotation gives typed refusal.
5. Preserve ambiguous mutation handling and command-receipt reconciliation.
   Demonstrate multi-cell application orchestration returning separate cell
   tokens/results without cross-cell atomicity or a global version claim. A
   parent cell token cannot replace the downstream target's version token.

Verify:

```sh
cargo test -p cellar --test native_read_replica_proof --test native_read_replica --locked -- --test-threads=1
cargo test -p cellar-logic --test read_replica_decisions --locked -- --test-threads=1
```

Expected: `versioned_write_then_replica_read_covers_token`,
`minimum_version_times_out_without_downgrade`, `invalid_token_has_bounded_cost`,
`lost_write_reply_remains_ambiguous` and `read_result_propagates_session_minimum`
pass. A fake future token cannot extend a deadline or grow an unbounded queue.
Also require `no_change_rpc_returns_covering_cut`,
`idempotent_reply_retains_exact_receipt`, `gate_follower_reply_has_covering_version`
and `missing_ltx_base_never_fabricates_version`.
Also require `orchestration_returns_versions_per_cell`.

## RR-09 — Reconcile epochs and integrate safe warm takeover

Depends on: RR-08. Effort: L. Risk: HIGH.

Files: core replica decisions; new `durability/read_replica/history.rs`;
existing replication activation/restore contracts and node startup integration;
advisory placement; new `crates/cellar/tests/native_read_replica_failover.rs`;
existing `native_log.rs`, `native_node.rs` regressions unchanged except new fixture
hooks where necessary. Do not change their missing-evidence expectations.

Steps:

1. Generate and verify epoch bridges only from successful existing predecessor
   recovery/restore results. Implement the exact history-link coverage predicate,
   same-epoch ResumeBridge and bounded multihop ancestry traversal from Design
   007. Persist bounded immutable descriptive metadata; LTX compaction alone
   cannot erase token-coverage requirements.
2. On source changes, stop old session publication; verify bridge coverage before
   reusing state, otherwise reseed. Preserve explicitly permitted old safe reads.
3. Prefer B as a candidate only through existing ownership acquisition. Complete
   recovery before admission; being the fastest replica is not authority.
   Connect RR-07A's supervisor to proactively schedule existing protected-cell
   recovery without incoming application traffic. Reschedule read-copy repair
   after promotion, excluding the new primary from the read-copy count.
4. Add optional trusted-cache reuse into normal owner restore. Verify the cut,
   history and checksum; copy to a new owner-managed file and apply missing
   history. If not provable, use standard restore. Do not open serving files RW.
5. Reconcile prior-epoch token reads, removed replicas, old A rejoin and new
   durability ensemble. D/E recovery evidence retires only under existing rules.

Verify:

```sh
cargo test -p cellar --test native_read_replica_failover --test native_log --test native_node --locked -- --test-threads=1
cargo test -p cellar-logic --test read_replica_decisions --locked -- --test-threads=1
```

Expected: `behind_replica_recovers_before_promotion`,
`new_epoch_covers_old_token_only_with_bridge`, `old_primary_cannot_publish`,
`read_replica_cannot_replace_missing_witness`, `warm_reuse_matches_normal_restore`
and `promotion_never_mutates_pinned_read_image` pass. Recovered version 100 is
present even when B was at 98 and C at 99 when A died.
Also require `same_epoch_resume_requires_verified_bridge`,
`multihop_token_coverage_survives_data_compaction` and
`conflicting_or_pruned_history_never_satisfies_token`.
Also require `primary_loss_triggers_safe_automatic_takeover` and
`missing_witness_blocks_automatic_takeover` in `native_read_replica_repair`.
Run `cargo test -p cellar --test native_read_replica_repair --locked -- --test-threads=1`
as an additional package gate. RecoveryBlocked must retry with bounded backoff;
it cannot become an authorized replacement just to restore a target count.

## RR-10 — Bound resources, expose administration and finish rollout controls

Depends on: RR-09. Effort: M/L. Risk: MED/HIGH.

Files: existing execution-domain services/policy, node options/lifecycle/drain,
actor configuration; replica/query modules; `fleet/management/` bounded status;
selected CLI option parsing/inspection; `application/mod.rs`/SDK exports as
required; relevant native tests and `native_cli.rs`.

Steps:

1. Implement every budget in Design 007 with caller-configured policy, explicit
   CLI capture and typed overload results. Reserve space before materialization.
2. Give query/transfer/snapshot work separate admission from durability work;
   test shared-node witness fsync progress under saturated reads.
3. Add actual cut/source/status/fallback metadata and bounded metrics. Report
   missing/unknown progress honestly; never equate recent apply time to freshness.
4. Add replica queries/transfers/materializers/pins to returned shutdown evidence
   and join them. Preserve every pre-existing stop proof.
5. Add feature-off, drain/disable, deployment-digest reconciliation and rollback
   behavior; no read-only requirement can silently route into a mutable handler.

Verify:

```sh
cargo test -p cellar --test native_read_replica --test native_read_replica_transport --test native_read_replica_failover --test native_execution --test native_concurrency --test native_node --test native_cli --locked -- --test-threads=1
```

Expected: hard limits hold under paused transfers/pinned readers, `source_pins_do_not_block_log_gc`,
`saturated_queries_preserve_proof_progress`, `shutdown_releases_replica_pins`,
`feature_off_preserves_existing_paths` and `disable_drains_without_mutable_replay`
pass. Shutdown timeout produces incomplete evidence, not success.

## RR-11 — Build an auditable fault and automatic-repair harness

Depends on: RR-10. Effort: L. Risk: MED. This work produces new commands; do not
attempt to run the five-node command before this package exists.

Files: new `tools/perf/read_replicas.py`, `read_replica_topology.json`,
`test_read_replicas.py`; existing `run.py`, `faults.py`, `peer_partition.py` only
to extract scoped reusable infrastructure; existing `fault_cells.rs` or a focused
replica qualification application; manifest/bin registration if a new fixture
binary is required; `tools/perf/README.md`.

Steps:

1. Generalize node/resource identities and partition edge validation; existing
   harness code assumes three nodes and cannot qualify disjoint sets.
2. Add controlled A,D,E/B,C initial topology with observed actual ensemble and
   automatic placement. Use policy pools and deterministic ranking fixtures,
   not hard-coded production node assignments. Refuse startup if recorded roles
   differ. Extend to a sixth eligible spare F for disjoint automatic replacement;
   a five-node set has no separate spare after B fails when D/E are excluded.
3. Add fsynced client operation/result/token/read ledgers, actual proof-source
   receipts, immutable artifact/source identities and independent audit output.
4. Implement all Q scenarios, including Q13–Q16 automatic repair, with process crash, group
   partition, bucket reachability and missing-witness refusal checks.
5. Add independently scheduled arrivals, workload/load metadata, p95/p99 and
   matched owner-only baselines. Keep correctness tests separate from speed claims.
6. Add cleanup tests that prove only task-owned resources are removed; preserve
   failed-run evidence, mark missing observations incomplete, and expose `--help`.

Verify the new harness itself:

```sh
python3 -m unittest discover -s tools/perf -p 'test_read_replicas.py'
python3 tools/perf/read_replicas.py --help
python3 -m unittest discover -s tools/perf -p 'test_*.py'
```

Expected: tests pass; help includes `--topology`, `--scenario`, `--image`,
`--output` and `--arrival-mode`; safety checks reject production prefixes,
ambiguous ownership of resources and mismatched actual roles. The new runtime
scenario command is specified in Qualification 009 and must return audited JSON.

## RR-12 — Qualify, document and decide release scope

Depends on: RR-11. Effort: L including soak/failure repeats. Risk: HIGH if skipped.

Files: new evidence reports under `tools/perf/results/`; affected
`docs/03-architecture.md`, `05-sdk-and-lifetimes.md`, `06-storage-transactions.md`,
`07-routing-rpc.md`, `11-configuration.md`, `16-capacity-scaling.md`,
`18-recovery-backup.md`, `19-testing-qualification.md`, `21-production-readiness.md`,
`22-api-reference.md`, `23-cli-reference.md`, `24-glossary.md`, ADR and catalog
entries if new guides are created; a focused existing or new example.

Steps:

1. Run full existing CI-equivalent gates below on the exact release candidate.
2. Execute Q1–Q16 using Qualification 009's scenario/posture matrix and all
   targeted rollout modes. Q4/Q5 require the fleet-only acknowledged tail window;
   bucket-posture recovery/refusal uses its separately defined setups. Confirm
   actual fleet proof is exercised rather than bucket fallback alone.
3. Run independently arriving load with matched owner-only/replica configurations,
   repeats and failure recovery. Evaluate the predeclared thresholds; retain failures.
4. Publish measured envelope and unsupported guarantees honestly in docs. Example
   must show explicit registration, stale and version-qualified reads, typed
   failure, source/served-at metadata and complete shutdown.
   Turn Guide 010's proposed builder/handler/HTTP flow into a compiling example
   with several operations. Add `integration_example_supports_many_operations`
   to the existing native replica integration target, exercising the actual
   example application definition instead of a duplicated test-only registry.
5. Complete the release ledger. Feature remains disabled by default; enabling a
   production canary requires evidence and normal operator deployment procedure.

Existing full verification commands from SDK CI:

```sh
cargo format --check
cargo check-layout
python3 tools/check_native_dependencies.py
python3 tools/docs/check.py
cargo check --workspace --all-targets --all-features --locked
cargo test --workspace --all-features --lib --tests --locked -- --test-threads=1
cargo test --workspace --all-features --doc --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test -p cellar-axum --no-default-features --locked
cargo check -p cellar-axum-counter --no-default-features --bins --locked
git diff --check
```

Expected: all exit 0; test reports include the new targets with nonzero tests;
no source/lint checks weakened. Run the remaining packaging/example jobs in
`.github/workflows/native-sdk.yml` on CI. For a changed example manifest, update
and verify its declared build/OpenAPI contract using that job's exact commands.

## Program-level done criteria

- [ ] RR-01 through RR-12, including RR-07A, are DONE with revisions and gate output.
- [ ] I1–I10 and T01–T17 have executable tests, not just assertions in docs.
- [ ] Q1–Q16 pass in the five-node baseline/six-node repair topology, with
      independent ledger audits and complete stop proofs.
- [ ] Desired replicas are selected/repaired automatically without traffic or
      operator reassignment; insufficient capacity/history yields visible bounded
      retries rather than invented availability or weaker recovery guarantees.
- [ ] Acknowledged writes survive supported failures; ambiguous writes reconcile;
      missing recovery evidence produces refusal without an unauthorized successor.
- [ ] Read-serving lifecycle/query execution never enters mutable owner activation
      or mutates a serving file. A separately authorized successor may perform
      ordinary owner activation on a new owner-managed database after recovery.
- [ ] Actual proof source and serving node prove disjoint-role coverage.
- [ ] Resource/latency/freshness thresholds are declared before measuring and met
      inside the recorded envelope; unsupported deployments remain unqualified.
- [ ] Feature-off regression suite and enabled drain/disable behavior pass.
- [ ] All changed paths trace to this scope; docs, task index and manifest agree.

## Stop conditions and maintenance

Stop the affected work package and report concrete evidence if:

1. Current proof/restore code no longer matches the drift anchors.
2. A cut cannot be bound to exact completed artifacts without guessing among
   independent counters; revise the receipt contract before proceeding.
3. Bootstrap needs to copy live mutable SQLite, apply across a gap, mutate a
   serving image, or infer ancestry from larger numbers.
4. Any solution requires bypassing ownership verification, accepting missing
   fleet witness evidence, or counting B/C progress as write proof.
5. Read-serving/query execution requires reusing normal mutable activation or
   constructors. Separately authorized existing-cell recovery may use them.
6. Essential tests exist only in an unavailable internal harness, or a command
   passes with zero matching tests. Add ordinary integration coverage instead.
7. A verification failure repeats after two focused fixes, or the fix expands
   beyond the package's declared scope. Record the failing command and evidence.

Future changes to LTX compaction/checksums, capture pipelining, epoch continuation,
credential rotation, query registration, deployment schema compatibility or node
shutdown must run the relevant replica tests. Reviewers should scrutinize the
proof-to-cut seam and immutable-file ownership before throughput optimizations.
