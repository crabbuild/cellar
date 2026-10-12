# Embedded fleet validation

The [2026-10-12 report](results/2026-10-12-embedded-1000.md) records the completed
1,000-cell run and the earlier admission and shutdown failures it exposed.

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
  --cells 1000 --max-resident 512 --warmup-seconds 30 \
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

These are bounded local recovery and saturation checks. All nodes share one
VM, disk and single RustFS server. The workload overwrites small values, uses
closed-loop traffic and injects failures between write phases. It does not
measure production network partitions, sustained independent arrivals,
unbounded data growth, hot-cell skew or long soaks. Fleet acknowledgement may
fall back to a bucket proof. Results do not certify a production fleet size.
