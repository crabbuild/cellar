# Fleet balancing and maintenance delivery package

Status: implementation in progress. The design began at source
`e0035689dd1eaf059fca250f2eaf05b3280899bc`; `baseline.json` tracks the reviewed
successor for source preflight. The [implementation progress](progress.md) records
the catalog, durable membership guard, trusted enrollment and terminal removal,
with local verification. The current retained-return source passes 902 distinct selected
core/SDK cases in 913 executions, with earlier failed probes retained; deployed qualification remains open. FO-01 through FO-03 are complete; FO-04, FO-06, FO-07,
FO-08 and FO-10 are in progress. Healthy assignment and the explicit resumable topology
controller are implemented. The operator SDK/admin/CLI now exposes keep-set
rebalance, status, progress, wait and cancellation. Explicit add, status,
qualification and activation commands pass local end-to-end verification;
graceful evacuation, exact joined stop and terminal removal now have native
SDK/API/CLI coverage too. Audited cancel-and-restart recovery after
post-finalization inventory churn passes native end-to-end verification.
The catalog now implements M1 intact-restart preparation, a reversible physical
admission gate and synced cancellation. Accepted application work drains while
consensus continues. Pending incoming work can now be inventoried, lost birth
capabilities recovered from the admitted journal, and existing exact-boot moves
continued through restart preparation. Resource-only obligations stay charged.
Owned bounded settlement turns now retain birth decisions across controller
loss, block cancellation while their outcomes are unresolved, atomically root
the actual winner, and advance admitted movements with explicit service/debit
blockers. Finish versus Cancel is an explicit immutable action choice.
A bounded runtime inspection walk now checks the complete target physical
inventory and both current survivors per voting history. Retired copies stay
accounted. Owned reconciliation can now materialize missing original voters of
an already-admitted Live birth, one node per turn. It reuses permanent member
claims, verifies physical attachment after the reply, and retains the pending
history across interruption. Claimed blank engines, dynamic replacements and
changed rooted inputs refuse initialization. Actual both-survivor quorum and
publication checks still precede historical candidate completion. Owned
reconciliation now checks both survivors' matched progress at the actual leader,
requests an eligible transfer when needed, and retains the history until a
survivor is observed as leader. Queued work rechecks the synced gate and an
expiry measured by the Raft owner's clock, tightened by actual peer sample age.
The live matched walk can now publish M5 RestartReady after repeating physical
quiescence/inventory and checking every voting sample at the Root CAS. Clock
values stay ephemeral. Exact historical receipts survive lost confirmation and
controller restart; generic catalog writes cannot mint or delete readiness.
The target remains quiesced in RestartPending with placement excluded and its
disruption slot held. Separate Root stop consent now lets the receiving Node renew
physical inventory, both-survivor quorum and matched leadership before syncing
its exact maintenance stop guard. Actual signed joined closure can be confirmed
offline without releasing capacity or the disruption slot. M8 now admits a
separate return intent and boots the retained engine under a fresh process,
syncing quiescence before endpoint publication. Public and SDK writes remain
blocked while consensus catches up. Exact unknown startup confirmation retains
its admitted nonce. M9 verified resume, rolling execution, maintenance operator
surfaces and resize remain delivery targets. Graceful departure now reports typed proof waiting without
consuming a history or releasing the disruption slot. Status retains local progress
only at the same Root; CLI waiting stops on unknown mutation outcomes. Current
native qualification passes, while the underlying intermittent observation cause
and deployed qualification remain open; the [progress log](progress.md) records
their exact limits.
`tasks.json` retains exact package status.
Controller-driven spare selection, admission and settlement after trusted
isolation are implemented as an explicit SDK service. Independent required
repair can use a remaining slot during an admitted action's retry/backoff,
without releasing that action's debt. Automatic failure
classification and a production isolation provider remain unfinished. The next
priorities are verified resume and the complete maintenance operator workflow,
followed by scalable convergence, failure handling and resize. Unconfirmed closure keeps movement
debt charged.

Deliver automatic redundancy repair, healthy-replica redistribution, load-aware
leadership balancing, and operator-controlled node addition, maintenance and
removal. Use conditional object-store writes for durable fleet intent and bounded
work admission. Each cell's Raft group continues to authorize membership,
replication, leadership and write acknowledgement.

The first qualification topology is five Cellar nodes, each limited to one vCPU
and 3 GiB, with 500 logical cells and three voters per cell: initially 100 leaders
and approximately 300 replica copies per node. RustFS is the object store.
Colima qualifies process behavior and local performance, not independent physical
failure domains. Initial implementation bounds are 64 nodes, 10,000 registered
histories and 16 admitted membership actions; these are limits to enforce, not
supported capacity claims.

Read these documents in order:

1. [Technical design](design.md): authority, records, concurrency, planners,
   admission, recovery and node lifecycle.
2. [Capacity and scaling](capacity-and-scaling.md): exact add/remove protocols,
   complete feasibility, scale-out/in, CPU/memory resizing and retained state.
3. [Planning details and worked examples](planning-details.md): explicit resource
   ledgers, feasible move ordering, five-to-six/five-to-four calculations,
   correlated failures, reduced cell counts and removal-proof closure.
4. [Maintenance and rebalancing](maintenance-and-rebalancing.md): guarded stop,
   rolling restart, operation conflicts, movement algorithms and foreground budgets.
5. [Lifecycle execution contract](lifecycle-execution.md): exact authority,
   resumable effects, activation acknowledgements, shrink ceilings, profile
   sequencing and user-visible completion proofs.
6. [Implementation plan](implementation-plan.md): deliverable work packages,
   dependencies, code ownership and acceptance gates.
7. [Operator workflows](operator-workflows.md): delivered keep-set commands, remaining CLI contract, and safe
   add, restart, evacuate, return and remove procedures.
8. [Qualification](qualification.md): failure matrix, performance gates and
   completion evidence.

Run from the repository root:

```sh
python3 plans/multi-raft/fleet-operations/plan.py validate
python3 plans/multi-raft/fleet-operations/plan.py preflight
python3 plans/multi-raft/fleet-operations/plan.py ready
python3 plans/multi-raft/fleet-operations/plan.py show FO-01
python3 -m unittest discover -s plans/multi-raft/fleet-operations -p 'test_*.py'
```

`tasks.json` is the machine-readable acceptance inventory. The tool checks the
dependency graph, source anchors, named tests, local document links and completion
record structure. It prints future Cargo commands without executing them. Source
preflight stops on drift so an implementer reviews affected contracts first.
`check-list` checks that a future Cargo test listing actually contains the named
cases; a listing alone never marks a task done.

Keep run data in ignored `.cellar/raft-perf/fleet-operations/` or an externally
retained artifact directory. Do not add raw results under
`plans/multi-raft/evidence/`. Evidence paths in the manifest are repository-relative
references to retained records; validators require their logs when checking DONE.

This package extends MR-11 and MR-13 through MR-16. It does not mark those broader
release gates complete. Its runtime capability remains default-off until FO-10
passes. Operator-driven scaling is in scope; automatic VM/container provisioning,
RF5, cell splitting, cross-region federation and quorumless disaster restore are
separate work.
