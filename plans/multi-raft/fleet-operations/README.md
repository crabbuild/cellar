# Fleet balancing and maintenance delivery package

Status: implementation in progress. The design began at source
`e0035689dd1eaf059fca250f2eaf05b3280899bc`; `baseline.json` tracks the reviewed
successor for source preflight. The [implementation progress](progress.md) records
the catalog and durable membership guard, with local verification. Complete fleet
management APIs and operator commands remain delivery targets. `tasks.json` retains
exact package status.

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
7. [Operator workflows](operator-workflows.md): future CLI contract and safe
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
