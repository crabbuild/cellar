# Embedded fleet validation

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

## Writes in flight and ambiguous responses

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
and deliberate client response loss. Peer network partitions, sustained load
and production-scale acceptance remain separate work.

These are bounded local recovery and saturation checks. All nodes share one
VM, disk and single RustFS server. The workload overwrites small values, uses
closed-loop traffic and injects failures between write phases. It does not
measure production network partitions, sustained independent arrivals,
unbounded data growth, hot-cell skew or long soaks. Fleet acknowledgement may
fall back to a bucket proof. Results do not certify a production fleet size.
