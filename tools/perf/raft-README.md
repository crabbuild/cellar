# Native multi-Raft on Colima and RustFS

This fixture records failed attempts and requires complete replica/archive
checks before marking a run complete. Its independent analyzer selects the
highest observed error-free throughput with p99 below 200 ms by default. Use
`--p99-limit-ms 500` to compare a higher latency budget.
The [recorded measurements](raft-results.md) include both latency budgets and
the longer confirmation at three concurrent writers.

This fixture uses three separate native SDK processes with `NamespaceMode::Raft`,
three durable voters per cell and no Fleet durability service. By default each
node leads 500 of 1,500 cells and follows the other 1,000. Leadership is transferred
through the checked SDK API and observed before traffic starts. Followers apply
committed SQLite/LTX images; they do not execute the command callback.

Each Cellar container has one pinned CPU, a 3-GiB memory limit, no extra swap and a
persistent Docker volume on the Colima VM filesystem. RustFS has two separate
CPUs/4 GiB, and the load container has two separate CPUs/1 GiB. These are container
limits inside one 8-CPU/16-GiB VM on a shared physical host, not independent hosts
or failure zones. The performance image uses the repository's optimized `lab`
profile; do not compare it directly with differently built release artifacts.

Writes use the SDK's Quorum durability: acknowledgement follows durable Raft
majority commitment and committed-image publication. RustFS is the S3-compatible
catalog and verified committed-archive layer. Final archive coverage is checked
separately; write latency does not imply synchronous S3 durability. Available
archives cannot replace a lost Raft quorum or authorize reuse of a lost voter disk.

From the repository root:

```sh
colima start cellar-raft-perf --cpu 8 --memory 16 --disk 80 --activate=false
docker --context colima-cellar-raft-perf build -f tools/perf/raft.Dockerfile \
  --build-arg SOURCE_REVISION="$(git rev-parse HEAD)" \
  -t cellar-raft-perf:ios-7da4ddb5 .
python3 tools/perf/raft.py --cells-per-primary 500 --tick-ms 500 --queue-items 4096 --archive-tmpfs-mib 512 \
  --seconds 60 --repeats 2 --concurrency 1 2 3 4 6
```

The density profile explicitly uses a 500-ms tick: two ticks per heartbeat
and nominal randomized elections at 20–39 ticks (about 10–20 seconds), before
additional scheduling delays. This trades failover speed for lower protocol
traffic. The runner retains the SDK-oriented 100-ms default for small smoke
runs. Record the chosen interval with every result.

`--archive-producers` selects one to four concurrent cell publications per node
(default two). Publications for the same cell remain serial, and physical replay
uses one exclusive verification workspace. Each producer has a bounded capture
and recovery set, so aggregate archive memory scales with this setting. The
runner records it and requires the same value when continuing incomplete setup.
Use one and two with the same image to isolate the effect of overlapping I/O.

`--sql-workers` selects one to sixteen fixed SQL owners per node (default two).
`--sql-worker-queue` bounds queued/executing reservations per owner (default 32).
To isolate worker concurrency while retaining 64 total reservations, compare
`--sql-workers 2 --sql-worker-queue 32` with
`--sql-workers 4 --sql-worker-queue 16` using the same image. CPU, memory and
business/archive byte budgets remain fixed. The authenticated `/resources`
fixture endpoint reports actual physical owners and reservation capacity; the
runner checks these before and after timing, and the independent analyzer
rejects mismatches. Setup continuation requires the original worker settings.

The latest [bounded-concurrency comparison](../../plans/multi-raft/tikv-performance.md#bounded-sql-and-archive-concurrency-comparison)
tested `--sql-workers 4 --sql-worker-queue 16 --archive-producers 4` at twelve
writers for two 180-second windows. It averaged 206.88 successful writes/sec,
with worst p99 355.05 ms and zero errors, while verifying all 4,500 replica values
and 1,500 archive positions. It used persistent internal node bind volumes and
external RustFS; storage backing and shared-host contention affect comparisons.
The second window slowed substantially, and the 200-ms gate did not pass.
Treat this as an experimental configuration; defaults remain unchanged.
Four publishers also double aggregate capture allowance from 8 to 16 MiB and
increase concurrent bounded recovery sets within the same 3-GiB container cap.

To find an operating point below the latency limit, pace aggregate requests
while retaining three independent clients:

```sh
python3 tools/perf/raft.py --cells-per-primary 500 --tick-ms 500 --queue-items 4096 --archive-tmpfs-mib 512 \
  --seconds 60 --repeats 2 --concurrency 3 --rates 10 20 30 40 60
```

`--rates 0` means unpaced closed-loop load. Positive rates space requests globally,
with at most the configured concurrency in flight and no catch-up bursts. Actual
TPS can be lower than the requested rate. Latency starts when the request is sent;
client pacing time is excluded. These are service-latency measurements, not an
open-loop overload/queueing SLA. Repeats reverse the settings to expose order and
backlog effects. Build and run heavy tests before measuring; they share the host's
physical storage even when the benchmark CPUs are pinned.

For **500 total cells on one primary and two follower nodes**, use a fresh run:

```sh
python3 tools/perf/raft.py --primary-only --cells-per-primary 500 --tick-ms 1000 \
  --seconds 20 --repeats 2 --concurrency 1 8 32 64
```

This pins all 500 leaders to node 1 and retains voting, durable replicas on nodes
2 and 3. It is a different topology from 500 leaders on **each** node. The explicit
1,000-ms tick gives a nominal 20–40-second election timeout. Both topologies use
the same resource limits, real SDK command path and replica/archive checks.

A small functional run uses `--cells-per-primary 4 --seconds 2 --repeats 1
--concurrency 1 8`. Run only one fixture at a time in this context: the isolated
network subnet and loopback ports 19800–19803 are fixed. The runner uses the
existing RustFS image pin from the Fleet harness. Public repository TLS fixtures
and development keys are restricted to this local test network.

`--archive-tmpfs-mib 512` mounts only `/data/node/raft-cells/archive-verifier`
on a bounded tmpfs. This workspace replays downloaded archives to verify their
contents, exposes no image pins outside its owner, and is discarded on startup.
Its memory counts against the same 3-GiB node limit. Raft logs, authoritative
committed cell images, and RustFS remain on persistent Docker volumes. The
independent analyzer rejects a tmpfs mount covering the durable data directory.
Omit this option to keep the scratch workspace on disk; record this choice when
comparing performance.

The initial write sweep warms every cell and waits for verified archive coverage
before timed traffic starts. Each measured POST increments one SQL
counter in one synchronous command transaction using a unique command identity.
The load generator uses persistent HTTP/1.1 connections and disjoint per-worker
cell sets with uniform selection inside each set. This maintains at most one
in-flight command per cell, matching the current SDK contract. It
connects directly to their assigned leaders and never retries writes. Reported
TPS counts complete successful HTTP responses over the full interval including
the final response drain. Latency p50/p95/p99/max measures each successful HTTP
request through its complete body. Failed requests retain status/body/latency. `Overloaded` is a documented
pre-proposal refusal: it is counted as an admission rejection and makes that
measurement window non-error-free. Other failures or ambiguous responses stop
qualification. Successful TPS always excludes refused requests. The analyzer defaults to a strict p99 < 200 ms target; `--p99-limit-ms` changes
that analysis threshold. A setting must pass in every retained repeat to be selected.

After traffic, the fixture reads every cell on every replica using the latest
acknowledged version token and checks its exact acknowledged count and serving
member. It then requires each leader's verified archive index to cover its last
acknowledged version. Only these gates set `completed: true`; `all_windows_error_free` separately
records whether every offered write succeeded. This does not test
power loss, process failover, member replacement, all-disk-loss restore or soak
behavior. Those require separate qualification.

Reports are saved under `.cellar/raft-perf/`: image ID, executable SHA-256, source
hashes, actual cgroup limits, raw successful response times and receipts, command identities, failed
responses, resource samples, replica values, archive status and node logs. Secrets
are kept in a private run-specific `config/` directory and excluded from reports.
The cluster remains running for further checks. Each run prints its resource
prefix and context. Stop those named containers with Docker when finished;
retain voter volumes and the run config for inspection. This runner creates a
fresh experiment rather than resuming or repairing an existing cluster.

Recompute TPS/quantiles and independently reconcile every retained receipt with
all replica values and archive positions:

```sh
python3 tools/perf/analyze_raft.py RUN_DIRECTORY
python3 tools/perf/analyze_raft.py RUN_DIRECTORY --p99-limit-ms 500
```

A timed-out live setup may be continued before **any** seed or measured write.
Use `--continue-setup RUN_DIRECTORY` with the original context, cell count and
tick interval. `--placement-seconds` controls the new observation budget. This
archives the previous report and retains its failure history; it neither resets
member identities nor restarts voters. A run with any recorded placement or write
phase is refused, because resetting expected counters would invalidate receipts.

The bounded-queue profile sets runtime request/event capacity and each transport
lane to 4,096 items, with 16-MiB peer and 32-MiB node control byte budgets.
External request admission and the SDK's default image, archive-backlog and
payload bounds remain active. The default 256-MiB archive backlog budget includes
an approximately 80-MiB worst-case reservation before each new callback runs.
Preparation reduces that charge to verified image and encoded-entry bytes.
Commands wait within bounded request admission when only temporary reservations
prevent execution; a full retained backlog still refuses fresh callbacks.
Admission rejection rate is therefore part of the result.

Setup seed writes are journaled and may retry the **same** command ID, input and
original retry deadline for explicit backpressure or ambiguity. This is native
receipt reconciliation, not a new increment. `--continue-seed RUN_DIRECTORY`
requires that journal and refuses runs with measured writes. A seed whose unknown
outcome outlives its original retry window is refused without generating a new
identity. The measured load generator never retries writes.
