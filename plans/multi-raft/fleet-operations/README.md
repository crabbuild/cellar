# Fleet balancing and maintenance delivery package

Status: proposed implementation, based on source
`e0035689dd1eaf059fca250f2eaf05b3280899bc`. The executable plan tools work now;
the fleet APIs and commands described here are delivery targets, not existing
operator commands. All implementation packages start TODO.

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
3. [Maintenance and rebalancing](maintenance-and-rebalancing.md): guarded stop,
   rolling restart, operation conflicts, movement algorithms and foreground budgets.
4. [Implementation plan](implementation-plan.md): deliverable work packages,
   dependencies, code ownership and acceptance gates.
5. [Operator workflows](operator-workflows.md): future CLI contract and safe
   add, restart, evacuate, return and remove procedures.
6. [Qualification](qualification.md): failure matrix, performance gates and
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
