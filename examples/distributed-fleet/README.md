<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Distributed Rust fleet and recovery

Use this example to verify acknowledged cell writes through owner loss,
handoff and preserving restart, while measuring bounded fleet capacity.

## How it works

The runnable reference is the existing
[Rust application](../../tools/perf/cells.rs) and
[embedded Node host](../../tools/perf/embedded.rs). It supplies storage clients,
disables retries on conditional ownership writes, injects authenticated peer
transports, captures bounded policy and checks shutdown proofs. The
[harness guide](../../tools/perf/README.md) explains prerequisites and results.

## Run end to end

Prerequisites: Docker CLI, Colima CLI with a running Linux VM, Python 3 and at
least eight VM CPUs. The harness uses Colima to create private ext4 loop disks;
a plain Docker installation without Colima is insufficient for this harness.
Allow at least 15 GiB for its three node disks plus images/results. Builds need
network access for base images and Rust dependencies. Rust code is compiled in
the application image; Python orchestrates infrastructure and verification.
Run from the repository root. Use a private tag and target cache:

```sh
docker build -f tools/perf/Dockerfile \
  --build-arg TARGET_CACHE_ID=cellar-example-fleet-target \
  --build-arg SOURCE_REVISION="$(git rev-parse HEAD)" \
  -t cellar-example-fleet:local .
python3 -m unittest discover -s tools/perf -p 'test_*.py'
python3 tools/perf/run.py --image cellar-example-fleet:local \
  --cells 1000 --max-resident 512 --max-cached-routes 32 \
  --max-requests 512 --warmup-seconds 30 --seconds 20 --repeats 2 \
  --concurrency 32 128 --shutdown-ms 60000 \
  --scenario three_node_bucket three_node_fleet \
  --output examples/.cellar/fleet-walkthrough
```

The output directory must not already exist. Choose an unused `--port` base
if ports near 39000 are occupied. Both selected scenarios take several minutes.
The harness creates a private network, fresh RustFS bucket, temporary credentials
and per-node storage, and removes only its own resources. It kills nodes,
verifies acknowledged counters after recovery, rolls handoffs and checks
preserving restart. Do not use a production bucket.

## Expected output and interpretation

The runner prints `Artifacts: <absolute results directory>` and exits zero only
after all selected checks pass. Inspect the machine-readable outcome:

```sh
python3 - <<'PY'
import json
from pathlib import Path
result = json.loads(Path('examples/.cellar/fleet-walkthrough/results.json').read_text())
print('completed:', result['completed'])
print('failures:', result['failures'])
print('recovery checks:', len(result['recovery']))
print('shutdown proofs:', len(result['shutdown_outcomes']))
assert result['completed'] and not result['failures']
PY
python3 tools/perf/analyze.py examples/.cellar/fleet-walkthrough \
  examples/.cellar/fleet-walkthrough/summary
```

Expect `completed: True`, `failures: []` and nonzero recovery/proof counts.
Every recovery read must equal the number of writes that received a complete
successful response; HTTP errors and mismatches fail the run. Bucket mode
discards local data after killing all nodes; fleet mode recovers a lost owner
on surviving nodes. Subsequent checks cover handoff and preserving reload.

`shutdown_outcomes` contains the returned SDK proofs. Successful proofs require
`outcome: stopped`, drained ingress/application/cells, a quiesced log, closed
local durability, live caller executors, zero pending events/dispatches and no
timeout. Preserve also requires `clean_reload_prepared: true`. Exit code alone
is not proof. Required routing snapshots must respect the captured cache bound.

The analyzer writes `summary.json` and prints scenario, concurrency, writes per
second and p99 latency ranges. Throughput and latency vary by machine; compare
them with `metadata.json`, captured limits and resource samples. A passing local run is
bounded evidence; multi-machine partitions, hot-cell skew, independent arrivals
and long soaks remain in the [roadmap](../ROADMAP.md).

## Cleanup

The runner's `finally` block removes only its own containers, private network
and Colima loop disks, including when a check fails. Logs/results remain for
inspection. After the process exits, remove `examples/.cellar/fleet-walkthrough`
to repeat using that output path. Remove `cellar-example-fleet:local` only if
it belongs to this walkthrough; do not prune shared engine resources. The
[harness guide](../../tools/perf/README.md) links previously recorded runs and
explains the evidence and workload limits.
