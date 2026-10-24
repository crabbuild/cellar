<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Capacity planning and scaling

Size a fleet from measured traffic, active cells, state growth and recovery
cost. Independent cells can distribute work across nodes; one hot cell retains
one owner. Budget admission, memory, disk and proof latency together, then
test the intended workload and failure conditions.

![Fleet capacity boundaries](diagrams/fleet-topology.svg)

## Budget the whole system

| Budget | Includes | Common mistaken assumption |
| --- | --- | --- |
| Request admission | Root events, nested calls and post-response work | A completed response has released every slot |
| Cell admission | Concurrent calls and transaction contention | More nodes split a single hot cell |
| Residency / routes | Residents, idle cache, owned/alarmed/inflight records | Route-cache bound is total memory bound |
| Memory | Workers, buffers, streams, DB pages, logs and filesystem cache | RSS alone describes cgroup pressure |
| Disk | SQLite, WAL/LTX, local logs, restore workspace and caches | Object storage removes local disk requirements |
| Store/network | CAS, proofs, upload, range hydration, peer logs | All S3-shaped backends have identical semantics |

Explicitly set finite stateless admission for production candidates. Library
default is 64 per script pool; CLI admission is unbounded when its request
limit is unset. Pools in old/new generations can coexist. Background work
retains slots, so nested call chains need a realistic saturation test.

## Establish the operating envelope

Record offered arrivals independently of completion, concurrency, queue depth,
success/rejection/ambiguity and per-operation latency. A closed-loop generator
slows arrivals when responses slow and can conceal overload. Test independently
arriving load, bounded bursts, large bodies, slow clients, skewed keys, growing
data and background activity. Report sample duration, warmup, percentile method,
repetitions and failed runs.

For initial estimates, use average in-flight work ≈ arrival rate × average
time in system for a stable interval. This is a planning estimate, not a safety
bound or a p99 sizing formula. Measure memory per active workload shape and
reserve headroom for recovery, rolling generations and dependency outages.
Measure separate hot-cell capacity; use domain-safe partition changes when a
single authority saturates.

Define a maximum sustainable envelope only after the target workload meets its
SLO, correctness ledger, resource headroom and recovery objectives through a
soak and failure run. Scale-out can increase shared-store contention and
handoff activity. Add one dimension at a time and preserve evidence.

## What the repository's reference demonstrates

The retained 10,000-cell run acknowledged 34,230 writes in bucket mode and
23,314 in fleet mode, reconciled all counters, and passed 14 SDK stop proofs.
Three nodes shared one VM, disk and RustFS server; each node had one CPU and
2 GiB RAM. State overwrote small 1-KiB payloads. The earlier image predates
route-cache and typed-fencing changes. Its 82.6–134.8 acknowledged writes/s
observations are workload measurements, not a sizing recommendation. Fleet
proof can fall back to bucket proof, so these timings do not isolate follower
proof performance.

The later zero-cache 1,000-cell run provides narrower routing/recovery evidence.
Neither run establishes arbitrary production fleet size, multi-zone resilience,
hot-key capacity, growing datasets or long-soak stability. Re-run the exact
release image on the proposed infrastructure. See [evidence](sources.md) and
[qualification matrix](19-testing-qualification.md).

Sources: [runtime policy](../crates/cellar/runtime/policy.rs),
[performance harness](../tools/perf/README.md),
[reference report](../tools/perf/results/2026-10-12-embedded-10000-reference.md).
