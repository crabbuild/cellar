# Embedded fleet validation

The [FleetCare application and standard suite](../../examples/fleetcare/README.md)
run 10,000 cells on three nodes, in embedded and daemon modes, using Colima and
RustFS. Run `python3 tools/perf/fleetcare.py` from the repository root. The suite
builds its release images, measures read/write traffic, verifies all receipts
and projections, checks SDK capabilities, injects crash/store failures and
verifies local and bucket recovery. It writes repeatable reports and can fail
on regressions with `--baseline`. Use `--profile smoke --suite smoke` for a
quick functional check. The [original design](field-service-design.md) includes
additional qualification experiments; its [blueprint](field-service-workload.json)
is documentation, not runner configuration.
The [FleetCare qualification report](results/2026-10-13-fleetcare-bringup.md)
records the application checks, full-scale recovery evidence and provisional
traffic measurements, including the admission and local-reload fixes exposed
by the workload.

The [2026-10-12 report](results/2026-10-12-embedded-1000.md) records the completed
1,000-cell run and the earlier admission and shutdown failures it exposed.
The [10,000-cell reference run](results/2026-10-12-embedded-10000-reference.md)
completed both three-node recovery scenarios and all 14 returned shutdown
proofs. Its earlier image predates the routing-cache and typed-outcome changes.
The [current-image zero-cache run](results/2026-10-12-embedded-cache-zero-1000.md)
retained exactly 64 tracked records while visiting 1,000 cells, and recovered
all 9,284 acknowledged writes with both preserving-stop proofs passing.

`cellar-perf-cells` embeds `cellar::sdk::Node`. The application supplies two
Tokio runtimes, listeners, authenticated peer clients, signing identities and
ordinary/conditional/paginated S3 clients. It captures its own configuration
and signals. The node receives explicit policy and services. The application
checks both runtimes after the node has stopped and joined its owned work.

The local harness uses a private Docker network, a fresh RustFS bucket, random
temporary credentials and peer authentication, and three private 5 GiB ext4
loop images in Colima. Cleanup removes only these resources. It requires an
existing Linux Docker/Colima VM with at least eight CPUs. Each node has one CPU
and 2 GiB memory; RustFS and the load generator each have two CPUs. Do not use
the harness against a production store.

Build from the repository root. Choose a private image tag and target-cache ID
for this checkout; the revision label records the source used to build it.

```sh
docker build -f tools/perf/Dockerfile \
  --build-arg TARGET_CACHE_ID=cellar-my-checkout-perf-target \
  --build-arg SOURCE_REVISION="$(git rev-parse HEAD)" \
  -t cellar-embedded-perf:my-checkout .
python3 -m unittest discover -s tools/perf -p 'test_*.py'
python3 tools/perf/run.py --image cellar-embedded-perf:my-checkout \
  --cells 1000 --max-resident 512 --max-cached-routes 32 --warmup-seconds 30 \
  --max-requests 512 \
  --seconds 20 --repeats 2 --concurrency 32 128 \
  --shutdown-ms 60000 --scenario three_node_bucket three_node_fleet
```

Use an unused `--port` base if another local fleet is running. `--output` must
name a new directory. Raw results, image IDs, source hashes, resource samples,
state snapshots and logs stay there; credentials are not saved in the report.
The application selects a finite script-pool request limit with
`--max-requests`. The SDK's default is 64; this benchmark defaults to 512 to
allow root requests to retain their slots while nested cell calls settle under
the offered load. This limit is recorded with each run. It does not disable
admission control. Failed measurements remain in the report, and `completed`
is true only after all selected recovery and shutdown checks pass.
The application captures `--max-cached-routes` through
`ActorPolicy::max_cached_routes`; the default is 2,048, and zero disables idle,
unowned routing history. Each startup reports the limits it actually captured;
an image using different limits is rejected before load starts. Required state
snapshots check the cache bound after warmup, measurements, recovery, each
rolling handoff's subsequent writes, and the final preserving reload. Missing
or invalid observations leave the run incomplete. Analysis independently checks
every required saved snapshot. Owned cells, alarms and unfinished work retain
their records independently of this cache limit; it is not a whole-process
memory limit. Use more cells than the resident and cache limits to exercise
reclamation, and rebuild the measuring image when changing its source.
Summarize them with:

```sh
python3 tools/perf/analyze.py RUN_DIRECTORY OUTPUT_PREFIX
```

The load generator increments a durable per-cell counter and replaces its JSON
payload. It tracks every successful complete response, without retrying writes.
Every verification reads every cell and requires its counter to equal exactly
the acknowledged count. Bucket mode kills all nodes, discards their local data
and verifies restoration. Fleet mode kills one node and verifies recovery on
survivors. Three-node scenarios then hand off and restart each node, verifying
both after takeover and after subsequent writes. After the fleet has stopped,
the final node restarts alone from its preserving-stop state and verifies all
acknowledged counters again.

Shutdown succeeds only when the returned SDK report proves drained ingress,
complete application events, cells and dispatches, a quiesced log, closed local
durability, and surviving caller runtimes. The final node uses Preserve and
must also prepare a clean reload. A zero process exit alone is insufficient.

Capacity runs behind HAProxy start controlled placement near the beginning of
an actual UTC minute, recorded in `*-setup-clock.json`. Cron remains enabled;
its scheduled queue metrics calls can otherwise claim all primary queues before
HTTP prewarm. The original guard still rejects any preclaimed critical root.
This setup policy is part of comparison compatibility.


## Core persistence baseline

Use the existing counter workload alongside FleetCare to separate core durable
HTTP writes from the application's Queue, outbox and SQL projection work. Each
POST makes one storage transaction in one cell; the SDK output gate proves it
before the complete HTTP response is counted. The client changes the payload on
every request and does not retry writes. Keep all cells resident for this first
comparison so activation/eviction does not dominate the persistence measurement:

```sh
python3 tools/perf/run.py --image cellar-embedded-perf:my-checkout \
  --cells 128 --max-resident 128 --max-cached-routes 32 --max-requests 512 \
  --warmup-seconds 30 --seconds 20 --repeats 2 --concurrency 1 8 32 128 \
  --shutdown-ms 60000 --scenario three_node_bucket three_node_fleet
```

Run a separate case with `--log-level info` to save the existing
`log_ship_round` and `log_ship_loop` diagnostics in the node logs. Round records
include credited cells, LTX entry counts/bytes, capture duration and observed
submission-to-application delay. Loop records separate capture wall time from
idle, grouping and pipeline waiting. Credited cells and LTX entries are not
business-command counts; do not interpret them as transactions per commit or
submission-to-application delay as pure follower RTT. These logs span setup,
traffic and recovery; select the relevant traffic intervals before aggregation.
The load report's `started_unix_ms`/`finished_unix_ms` bracket requests including
the response drain; client and node wall clocks must agree for log correlation.
INFO logging can perturb timings, so use the default WARN runs for comparisons.

Change batching and executor isolation independently, compare both throughput
and tail latency on equivalent fresh runs, and retain every recovery and shutdown
gate. A failed handoff or missing durability proof leaves a candidate unqualified
even when its write samples improve. Shared-host measurements remain local
diagnostics, and capture's share of the loop is not a predicted API speedup.

## Writes in flight and ambiguous responses

The [128-cell fault report](results/2026-10-13-embedded-inflight-128.md) records
both completed scenarios, including the receipt checks before any replay.
The [128-cell isolation report](results/2026-10-13-embedded-isolation-128.md)
records the owner's typed lease-expiry fence, survivor verification before
replay, and exact reconciliation and preserving reload.
The [peer-partition report](results/2026-10-13-embedded-peer-partition-128.md)
records retained ownership and renewed bucket authority while peer paths fail,
including receipt verification before replay and after the final reload.

`cellar-perf-fault-cells` is a separate compiled SDK application for correctness
checks during process failures. It records each operation ID, payload SHA-256
and counter ordinal in the same storage transaction. Replaying the same ID and
payload returns the original receipt. Reusing an ID with another payload fails.
This is application idempotency policy; Cellar does not promise exactly-once
request delivery. The normal counter performance workload is unchanged.

Run the fault driver with an image built above:

```sh
python3 tools/perf/faults.py --image cellar-embedded-perf:my-checkout \
  --cells 128 --concurrency 16 --max-resident 16 --max-cached-routes 8 \
  --max-requests 128 --shutdown-ms 60000 --scenario bucket fleet
```

The host-side client journals planned operations, every attempt and every
outcome, flushing and syncing records before it issues writes. The driver
first acknowledges one write to every cell. It then forwards one write through
a local proxy that consumes the complete SDK response and drops the client's
connection. That receipt must survive even though the client never received
an acknowledgement.

Next, concurrent writes commit their local receipt and hold the handler for
ten seconds before returning. A diagnostic marker proves that a handler is
running after its local transaction; it is not an output-gate acknowledgement.
The driver requires that operation to remain pending at the client and kills
its actual owner inside the hold window. Bucket mode kills all three processes,
discards local state and restores from RustFS. Fleet mode kills the owner and
reads through the two survivors.

Before any retry, every acknowledged operation and the response lost at the
proxy must have the original durable receipt. Unacknowledged operations may
be absent or present exactly once. Each cell's counter must equal its distinct
receipts, with unique contiguous ordinals and matching payload hashes. Only
after that check does the driver replay the issued IDs. The final counters
must then equal the number of logical operations, including ambiguous ones,
and remain exact after the final node's preserving reload.

Required cache snapshots, captured startup policies, process kill observations,
returned shutdown reports and stage-specific client ledgers are saved beside
the raw receipts. The final audit rechecks those stage ledgers independently;
replay cannot conceal a missing acknowledged operation. Failures preserve
partial state captures and leave `completed` false. This checks process crashes
and deliberate client response loss.

Use `--scenario isolation` for a network-failure check on the same native SDK
path. The driver holds accepted handlers for thirty seconds and disconnects
the actual owner from its sole private network while its client operation is
pending. That cuts peer, bucket and public network access. The SDK must close
and join its owned work, report either its expired-lease fence or a distinct
replication failure, and leave both caller executors usable. The application
chooses a failing process exit after receiving that outcome; a process exit
alone proves neither fencing nor graceful drain.

The two survivors verify all acknowledged receipts before replay. The failed
node then restarts on the original network and recovers its predecessor before
acquiring a replacement lease. Reconciliation and the final preserving reload
retain the original operation receipts. This tests full node isolation.
Sustained load and production-scale acceptance remain separate work.

Use `--scenario peer-partition` to block traffic between the actual owner and
its two peers while retaining bucket and public access. The driver installs a
private filter chain inside only that owner's container network namespace,
using Colima's `nsenter` and `iptables`; it removes the chain before healing.
TCP probes must confirm both directions are blocked, all three bucket paths
remain reachable, and the other two peers can still reach each other. The
owner accepts new writes for at least two lease lifetimes. Bucket lease reads
and cell reads must retain the process generation and cell epoch while the
lease renews. The test checks receipts during the partition, then heals and
checks every acknowledged receipt before replay. Reconciliation and final
preserving reload remain exact. This checks one owner separated from two
peers; it does not cover arbitrary network topologies or a sustained load SLO.

The counter performance runs are bounded local recovery and saturation checks.
All nodes share one VM, disk and single RustFS server. That workload overwrites small values, uses
closed-loop traffic and injects failures between write phases. It does not
measure production network partitions, sustained independent arrivals,
unbounded data growth, hot-cell skew or long soaks. Fleet acknowledgement may
fall back to a bucket proof. Results do not certify a production fleet size.

## Requested-size HTTP capacity lab

FleetCare's `--profile capacity --node-resources capacity` selects 3,000 roots
on three 2-vCPU/4-GiB containers with private 10-GiB disks. See the
[FleetCare capacity instructions](../../examples/fleetcare/README.md#three-node-3000-cell-capacity-test).
`COLIMA_PROFILE` and a matching `DOCKER_CONTEXT` select an isolated VM without
changing the user's default Docker context. `--store-cpus` and
`--store-memory-gib` vary backend headroom independently of the application
limits. Dead dependency checks stop new load phases and save OOM/exit evidence.
A dedicated, idle physical host is needed for credible capacity comparisons;
container pinning within a shared VM does not isolate competing macOS work.

The independent FleetCare client wakes up to 20 ms before an arrival and spins
until its original deadline. This can use one CPU from the existing two-CPU
client budget; report that cost alongside generator timing. Actual send and
finish timestamps retain schedule debt and failures in latency. Real HTTP windows must still meet generator timing
and complete correctness/shutdown gates.

For private VM startup/shutdown, pass the Colima profile explicitly without
private Docker context environment settings. Capture and preserve the currently
configured global context rather than assuming its name. Docker operations
retain the private context, and fleet creation must verify the actual RustFS
image against the frozen reference identity.

`--ingress haproxy` sends all FleetCare client actions through one private HTTP
round-robin load balancer, with no retries or redispatch. It requires capacity
nodes, a 12-CPU VM and a two-CPU RustFS budget so proxy/client/node CPU sets stay
separate. `--mixes all-read` visits every manifest root cyclically; `core` and
`write` retain their business-operation weights. Save backend counter deltas
and per-window cell coverage alongside latency and resource evidence; equal
ingress counts alone do not establish balanced cell-owner work. Capacity runs
serially initialize critical Queue/DLQ/SQL and Inventory-0 roots through HTTP before workflow
seeding, then enforce the captured owner mapping at every load boundary. Each
node must own one primary queue, one DLQ, six SQL roots and two Inventory-0 cells. These observations
remain outside timed windows, and the setup policy is part of compatibility. See the
[load-balancer instructions](../../examples/fleetcare/README.md#http-load-balancer).

Capacity HAProxy setup aligns its initial cursor with at most two extra manifest
GETs, then requires the thirty critical requests to select node2/node3/node1
in order. This fixes cross-run identity rotation; selected backends and actual
owners are still verified independently outside measured traffic.
