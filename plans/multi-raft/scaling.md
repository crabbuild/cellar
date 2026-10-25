<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Cellar multi-Raft scaling plan

Multi-Raft distributes many small consensus groups. The first release uses one
cell per group and three voters. A node may lead one cell and follow another;
adding nodes does not enlarge every group's quorum. Aggregate capacity grows
only while storage, network, routing, apply and workload distribution permit it.

## Capacity dimensions

Let N be nodes, G groups, R voters per group, L learners per group and A the
fraction of groups actively receiving application traffic. With balanced placement:

```text
group replicas per node = G * (R + L) / N
leaders per node        = G / N
active groups per node = A * G * (R + L) / N
heartbeat deliveries/s = G * (R - 1 + L) / heartbeat_interval_seconds
```

The heartbeat expression counts leader-to-peer logical deliveries before replies,
batching or a qualified hibernation protocol. Application inactivity does not stop
Raft heartbeats in the first release. No connection count or memory estimate can
be inferred from these counts alone. Placement skew and recovery reserve make
worst-case node load higher than the average.

Run the supplied arithmetic without deploying anything:

```sh
python3 plans/multi-raft/plan.py capacity million-cells
python3 plans/multi-raft/plan.py capacity ten-million-cells
```

[capacity-profiles.json](capacity-profiles.json) contains assumptions, not measured
limits. One million groups with three voters on 1,000 nodes averages 3,000 replicas
and 1,000 leaders per node. Whether those fit depends on active load, database size,
private-image publication cost, cache policy and failure recovery requirements.

## Qualification tiers

| Tier | Proposed experiment | Gate before advertising this scale |
| --- | --- | --- |
| Q0 | 3 voter processes; 1 to 100 cells | All crash and history invariants; no speed claim |
| Q1 | 6 processes; 10,000 cells with skew | Automatic repair, bounded queues, mixed reads/writes and 24-hour soak |
| Q2 | 30 physical nodes; 100,000 cells | Actual independent failure domains, node loss and recovery reserve |
| Q3 | 100 physical nodes; 1,000,000 cells | Measured density, control-plane limits, domain loss and archive backlog recovery |
| Q4 | 1,000 physical nodes; at least 1,000,000 cells | Independent infrastructure run, bounded controller/network fan-out and upgrade drills |
| Q5 | Multiple independent clusters | Cross-cluster routing/migration/restore protocol from MR-17 |

The current native host admits at most 4,096 registered groups per node. Q1's
10,000 cells with three voters require 30,000 replica placements, averaging
5,000 per process across six processes, before learners or recovery reserve.
MR-14 must reconcile that host bound with the Q1 workload through bounded
implementation and measured qualification. Raising a configured limit alone
establishes no capacity, and counting only the busy cells does not remove idle
replicas' consensus, image or recovery costs. The Q1 gate remains unchanged.

These are work targets, not a deployment sizing recommendation. Each report must
state the active fraction, database-size distribution, request mix, skew, payloads,
hardware and SLO. Thousands of processes on one host do not qualify Q4. A namespace
with one million metadata records but no running groups does not qualify one
million live Raft groups.

## First-release runtime requirements

Drive groups with shared workers and timers; batch independent group writes in
one log engine. Keep per-group ordering while ensuring one stalled group cannot
block unrelated groups. Coalesce transport writes between peers, enforce byte
budgets and preserve reserved heartbeat/election capacity. Avoid per-group threads,
connections, unbounded queues, permanent SQLite connections and unbounded labels
in metrics.

Separate resident application state from durable membership state. Closing an
idle SQLite image does not remove its group or permit S3-only replacement. The
baseline keeps ticking groups. A later hibernation protocol must prove safe wakeup,
message routing, current-term reads and crash recovery before disabling ticks.

Placement must bound peer fan-out while distributing replicas across the intended
failure domains. Maintain per-node load summaries and targeted group assignments;
do not broadcast a full million-group map to every node or list all S3 groups for
every command. Hash-partition catalog/control responsibility with fenced plan
versions. Cache routing hints at ingress and verify at the destination.

New groups require S3 birth records, so group-creation throughput is distinct from
steady-state write throughput. Startup, controller reconciliation and archive
scans use paginated, resumable indexes. A control-plane restart cannot synchronously
hydrate every SQLite database before any healthy group can serve.

## Recovery and operational capacity

Budget separately for leader election, committed apply catch-up, replica rebuild
and full redundancy restoration. A leader can be elected quickly while its large
SQLite image remains unavailable. Measure time to successful business reads/writes,
not just time to a leader notification.

Limit concurrent snapshot generation/transfers and archive backfill. Reserve
storage and CPU for losing the largest qualified domain. Admission must reflect
degraded capacity, so repairs do not amplify overload. Record recovery time and
remaining redundancy per group; averages alone hide cells with missing history.

LTX logs replicate changed pages, which may exceed logical command size. Measure
logical bytes, encoded LTX bytes, replicated bytes, SQLite copy/apply bytes and
S3 bytes separately. Full-image publication is expected to constrain larger cells;
do not extrapolate a small-counter benchmark to large databases.

## Later group and storage refinements

MR-16 first measures which resource limits density. It then selects and qualifies
one improvement at a time: page-versioned committed images, safe group hibernation,
or several small cells in one group. Every choice requires an updated protocol
and crash tests before implementation.

Shared groups retain separate SQLite databases and LTX chains. Their log entries
identify a cell; leadership/membership and consensus ordering are shared. This
reduces group count but couples placement, failure recovery and throughput. It
does not create cross-cell atomicity. Large/hot cells may merit dedicated groups,
but an online transfer must fence the source, commit a transfer boundary, install
the destination and atomically change routing without admitting two writers.
Until that protocol is qualified, group assignment is immutable.

## Federation for very large fleets

Millions of physical nodes are a different platform target. Use many independently
qualified clusters with their own placement, consensus and archive budgets. A
partitioned global directory maps a tenant/cell to its home cluster. Steady-state
writes reach that cluster directly after cached discovery; they do not require a
global quorum or a global metadata write.

MR-17 defines home-cluster transfer, directory fencing, authorization, disaster
restore and global-token scope. Regional groups normally remain within their
qualified failure domains. Cross-region synchronous replication, if required,
has a separate latency and quorum contract. A million-node promise cannot be
derived by multiplying a successful thousand-node test by a thousand clusters.

## Primary references

[TiKV multi-Raft](https://tikv.org/deep-dive/scalability/multi-raft/) demonstrates
the shared-runtime pattern. [TiKV region tuning](https://docs.pingcap.com/tidb/stable/tune-region-performance/)
documents overhead from excessive groups and control-plane pressure. These support
the architectural concerns; they are not Cellar capacity measurements.
