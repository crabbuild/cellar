<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Local SQL, KV and Queue performance

`primitives.py` measures the ordinary native HTTP bindings over managed shared
Raft groups and real RustFS. It uses the optimized `lab` profile, Rust 1.98.1,
three SDK voters in one process, and a single Linux container with three CPU
quota units, 9 GiB memory and no additional swap. RustFS retains its existing
one-CPU, 2-GiB owned backend contract. The load generator shares the voter process and CPU quota. These voters share a physical
host; results describe this local topology rather than independent node capacity.

Start the [owned RustFS backend](../multi-raft/README.md) first. Build committed
sources with a checkout-specific target cache, including every compiler input
listed by `raft_run.source_manifest`. The runner compares that manifest and the
Git revision against the image before creating workload containers. It records
the measuring executable hash and compiler identity.

```sh
docker --context colima-cellar-multi-raft build \
  -f tools/perf/primitives.Dockerfile \
  --build-arg TARGET_CACHE_ID=cellar-my-checkout-primitives-perf \
  --build-arg SOURCE_REVISION="$(git rev-parse HEAD)" \
  --iidfile /tmp/cellar-primitives-image .
python3 tools/perf/primitives.py \
  --image "$(cat /tmp/cellar-primitives-image)" \
  --backend /absolute/owned-backend --output /absolute/new-results \
  --seconds 30 --repetitions 2
```

Each image mode, materialized and extents, gets fresh local data volumes and a separate
remote prefix. Each repetition measures concurrency one and four; the second
reverses their order. A two-second warmup precedes each cohort. SQL upserts and
reads use prepared statements. KV uses public text put/get. Each worker owns a
key in one database or KV partition, with a sequence marker and a 256-byte base
payload. Returned reads and final acknowledged values must match exactly.
These overwrite workloads deliberately measure contention on a small working
set, rather than database growth or large-object performance.

Queue traffic sends batches of eight messages, each with a unique sequence and
a 256-byte base payload, split across two queues when concurrency is four.
Ordinary consumers acknowledge every message. The fixture checks the exact set
of completed bodies and distinct message identities, then confirms both queues
are empty. Enqueue requests/sec, enqueue messages/sec and verified messages/sec
including drain are distinct metrics. The latter includes final verification
requests and therefore is a conservative full-cycle rate.

Latency includes sending the full HTTP request and reading its complete response.
Raw microsecond samples support independent nearest-rank p50/p95/p99 checks.
Throughput includes final in-flight requests and their time. This is closed-loop
traffic; it does not measure an open-loop overload SLA. Quorum acknowledgements
do not imply synchronous S3 archive durability. Background archives use RustFS.

An error stops that worker, remains in the report and fails the measurement.
The fixture never substitutes new command IDs to retry an uncertain mutation.
The controller retains failed windows, full logs, cgroup CPU/memory/I/O samples,
stopped containers, named data volumes and source metadata. It never removes
foreign resources. Credentials pass through the process environment and are
excluded from reports. Retained resources carry the run's ownership label;
remove them only after inspecting and retaining the desired evidence.

The three ignored manual test entries are benchmark infrastructure. They do not
replace the existing primitive recovery, S3-proof, process-isolation or release
qualification suites.

The [2026-11-14 local run](../../docs/design/multi-raft/cell-groups/primitive-performance-2026-11-14.md) records partial
verified windows and all six failed primitive/image cases. It establishes no
stable capacity or cross-mode speedup.
