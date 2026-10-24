# Fleet operations implementation plan

## Delivery rules

Implement the [design](design.md) as ten reviewable packages. `tasks.json` pins
each package's source boundaries, prerequisites, named test cases and oracles.
Run `plan.py show FO-01` for the first concrete assignment. No runtime package is
complete merely because this design or its validator exists.

Keep the capability default-off. Reuse the current active PR when appropriate;
start from updated main after a merge. Review source drift against `baseline.json`
before editing. Preserve existing Fleet durability mode. Do not replace a failing
safety case with a weaker assertion, infer a commit from timeout, or add a
membership path that bypasses the fleet guard.

The earlier requirement to retain only performance improvements still applies
to optional hot-path optimization. This request additionally authorizes new
operating capabilities. Ship their correctness separately from optimization:
measure their idle/steady-state overhead, and admit optional balancing only when
it improves the tested workload or satisfies an explicit placement constraint.
Do not advertise repair as a throughput optimization.

## Work packages

| Package | Deliverable and owning boundaries | Acceptance and dependency |
| --- | --- | --- |
| FO-01 | CAS catalog, immutable indexes, operation identities and bounded root. New private component `crates/cellar/durability/raft/fleet_catalog/mod.rs`; reuse `Bucket::put_cas` and domain-owned tasks. | Concurrent requests converge; ambiguous replies reconcile; malformed/oversized records refuse admission. No prerequisites. |
| FO-02 | Durable membership permits and cancellation fence. Extend `member_state.rs`, `host_membership.rs`, membership codec/snapshots and activation checks. | Replayed permits cannot execute after cancellation or retirement; restart and joint recovery retain the exact action. Requires FO-01. |
| FO-03 | Trusted dynamic node set, empty managed host readiness and complete history enrollment. Extend `node_directory.rs`, `host_directory.rs`, `host.rs`, inspection and binding registration. | New nodes need no dummy business cell; explicit eligibility distinguishes maintenance from evacuation; untracked births and retired-slot rebinding are refused. Requires FO-02. |
| FO-04 | Pure fleet placement, bounded complete feasibility search and reservation planner. New `crates/cellar-logic/raft_fleet.rs` plus sibling tests; integrate `raft_placement.rs`. | Redistribution progresses with tied peaks and small individual moves; retained state, domains and failure headroom fit. Requires FO-03. |
| FO-05 | Workload samples and optional leader balancing. Extend `inspection.rs`, `host_inspection.rs`, `raft_leadership.rs` and performance observations. | Lagging targets and stale/incomplete samples refuse moves; hot-leader movement improves the skew workload under the performance gate. Requires FO-04. |
| FO-06 | Owned automatic reconciliation using root permits and existing repair APIs. Extend `host_controller.rs`, `controller_probe.rs`, `controller_action.rs`, `placement.rs`. | Failed replicas repair without business traffic; paused/restarted controllers preserve work; new optional work yields under pressure. Requires FO-05. |
| FO-07 | Guarded restart/rolling maintenance, resize handoff, evacuation and removal certificates. New `fleet_operation.rs`; integrate `host_drain.rs`, `operations.rs`, node lifecycle persistence and managed inventory. | One disruption slot; a durable local epoch/process fence rejects delayed stop; only a terminal rooted receipt permits removal. Requires FO-06. |
| FO-08 | Public management API, CLI and authorization. New `fleet/management/raft_fleet.rs`, `cli/raft_fleet_cli.rs`; connect SDK, admin and existing audit surfaces. | Every write has idempotency and revision guards, typed blocked reasons and inspectable outcomes; no infrastructure deletion side effects. Requires FO-07. |
| FO-09 | Horizontal scaling, vertical profile reconciliation and cell lifecycle accounting. Extend catalog/planner/management and guarded birth/retirement adapters. | Final keep-set and single-failure feasibility verified; observed limits gate scale-up, retained state can block scale-down; deletion preserves retention. Requires FO-08. |
| FO-10 | Five-node Colima/RustFS qualification, fault runner and release report. Extend `tools/perf/raft_topology.py`, `raft_run.py`, `raft_docker.py`; add `tools/perf/raft_fleet_operations.py`. | All safety scenarios, control-store probes, performance gates and workload verification pass with pinned executable/configuration and retained artifacts. Requires FO-09. |

Shared source ownership is sequential in this first plan. Dependencies are
correctness and integration ordering, not a request to run multiple agents.
Each Rust leaf uses sibling test files or a private substantial-test module
consistent with `cargo check-layout`; do not combine a leaf `.rs` with a same-name
Rust directory. Keep public module paths mapped by `crates/cellar/lib.rs`.

## Internal contracts to implement

The following signatures describe new APIs; align concrete names with existing
domain types during implementation, without weakening the boundaries.

```rust,ignore
// Pure decisions in cellar-logic; no async, storage or transport dependencies.
fn plan_fleet(view: &FleetView, policy: &FleetPolicy, now_ms: u64)
    -> Result<Option<FleetAction>, FleetPlanError>;

// Domain-owned adapter; a conflict differs from an ambiguous result.
async fn reserve_action(expected: FleetRevision, action: FleetAction)
    -> Result<ReservedAction, FleetCoordinationError>;

// Raft guard and the membership change share committed durable state.
async fn begin_managed_repair(permit: MembershipPermit, timeout: Duration)
    -> Result<RepairStatus, CommandFailure>;
async fn fence_unused_permit(permit: MembershipPermit, timeout: Duration)
    -> Result<PermitReceipt, CommandFailure>;

// Management observation is distinct from final completion.
async fn submit_operation(request: FleetOperationRequest)
    -> Result<OperationHandle, FleetOperationError>;
async fn operation_status(id: OperationId)
    -> Result<FleetOperationStatus, FleetOperationError>;
```

`FleetOperationRequest` includes fleet incarnation, stable operation ID, expected
fleet revision, action and canonical payload digest. Authenticate the principal
at the management boundary and retain its audit identity. Supported actions are
`RegisterNode`, `ActivateNode`, `Rebalance`, `PrepareRestart`, `ResumeNode`,
`EvacuateNode`, `RemoveNode`, `SetDesiredCapacity`, `PauseOptionalBalancing`,
`ResumeOptionalBalancing`, `ResizeNode`, `StopForMaintenance`, `RollingMaintenance`,
and `CancelOperation`. `StopForMaintenance` is a child of the admitted maintenance
or resize operation and consumes its existing disruption slot. Cell creation/retirement use
their application lifecycle APIs with the same inventory admission contract.

Operation phases are `Accepted`, `Planning`, `WaitingForCapacity`, `Executing`,
`Verifying`, `WaitingForRestart`, `WaitingForResourceChange`, `WaitingForRejoin`,
`RepairingAfterMaintenance`, `Finalizing`, `Completed`,
`CancelRequested`, `Cancelled`, and `Blocked`. A restart operation exposes its
`RestartReady` receipt in `WaitingForRestart`; it completes only after verified
rejoin and resume. An evacuation operation completes with its `Evacuated` receipt.
`Blocked` carries a retryable reason and preserves the last durable phase. Status
includes progress counts, exact outstanding group IDs through pagination,
configuration revisions, admitted action IDs and the next prerequisite. Counts
never replace exact membership proof. Cancellation can complete with already
committed effects, which remain visible in the terminal receipt.

Typed reasons include `RevisionConflict`, `PayloadConflict`, `UnknownOutcome`,
`QuorumUnavailable`, `InventoryIncomplete`, `InsufficientCapacity`,
`InsufficientDomains`, `StaleObservation`, `ArchiveBehind`, `StoreUnavailable`,
`UnsupportedProtocol`, `RetiredBinding`, and `OperationInProgress`. Map them
consistently through SDK, HTTP and CLI; do not stringify away ambiguity. Add
`RetainedStateCapacity`, `ProfileMismatch`, `PlannerSearchExhausted`, and
`StaleMaintenanceReceipt` for the expanded protocols. Missing quantitative
capacity and an exhausted search budget are distinct outcomes.

The operation steps, readiness/profile fields, concurrency matrix and terminal
receipt protocol live in [capacity and scaling](capacity-and-scaling.md).
The local stop fence, grace handling, rolling sequence, balancing potential and
scheduler live in [maintenance and rebalancing](maintenance-and-rebalancing.md).
Keep those contracts and the named manifest cases synchronized during review.
The [planning details](planning-details.md) additionally require conservation of
materialized resources, a feasible staged path, per-history target eligibility,
exact failed-pair classification, fewer-cell retained-footprint accounting and
separate absence/health proof lifetimes. The manifest assigns these acceptance
cases to FO-04, FO-07, FO-09 and FO-10. Adding their names is not test execution.

## Review sequence inside each package

1. Read the named source, callers and existing public tests. Confirm baseline
   changes do not invalidate the proposed API or invariant.
2. Add a failing behavioral case at the owning boundary. For FO-01/FO-02 include
   delayed completions and process-loss cuts before the happy path is considered
   complete. Do not add tests that merely mirror a score formula.
3. Implement the smallest complete path and keep physical work domain-owned.
4. Run the manifest's test listing and `check-list`, then execute the target
   serially. Run relevant existing native directory, membership, controller,
   snapshot and operation tests for changed contracts.
5. Run the repository checks below and affected feature-mode Clippy/tests from
   `.github/workflows/native-sdk.yml`. Record exact arguments and results.
6. Review crash recovery, cancellation, retained histories and resource bounds.
   Mark DONE only with retained source/executable/environment evidence.

```sh
export CARGO_TARGET_DIR="$HOME/Workspace/crabbuild-target/cellar-fleet-operations-$(basename "$PWD")"
cargo format --check
cargo check-layout
python3 tools/check_native_dependencies.py
python3 tools/docs/check.py
```

Use a unique writable target for this checkout, locked Cargo builds and the
workflow's toolchain. Never reuse/clean another checkout's artifacts or enable
`cellar_internal_tests` without its external harness.

## Stop conditions and completion

Stop a package and preserve its evidence if a stale permit can mutate membership,
a node can be certified absent using incomplete inventory, an acknowledged write
is lost, a minority is activated from S3, or memory/retention bounds are exceeded.
An unavailable quorum blocks that operation without blocking independent healthy
groups. Performance failure rejects optional optimization, not the safety checks.

Completion records use format 1 with `task_id`, full `source_revision`,
`executable_digest`, `environment`, `limitations`, named `tests`, and `commands`
containing exact `argv`, `exit_code`, repository-relative `output` and its SHA-256.
The validator checks declarations, log hashes and required command/case coverage;
reviewers must still inspect the logs and qualification conclusions. It cannot
prove that a manually authored report is truthful. Missing local artifacts make
DONE unverifiable; restore them from retained storage rather than weakening gates.

FO-10 additionally requires the fields and evidence in
[qualification](qualification.md). A complete delivery includes working SDK/API
and CLI, operator runbooks, validated failure recovery, raw retained measurements
and a supported topology/SLO statement. Documentation alone completes none of
those runtime acceptance gates.
