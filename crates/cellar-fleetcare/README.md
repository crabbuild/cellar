# FleetCare

A native field-service application and repeatable performance suite for Cellar.
The same application runs inside a caller-owned `sdk::Node` and the production
`sdk::Host::local` daemon. The independent client schedules fixed-rate arrivals,
records complete responses and keeps a durable-command receipt ledger.

From the repository root, with Docker and Colima installed:

```sh
colima start --cpu 8 --memory 16 --disk 80
python3 tools/perf/fleetcare.py
```

The runner builds checkout-specific release images, pulls the pinned RustFS
image if necessary, creates fresh credentials and buckets, starts the fleet,
seeds every declared cell, runs the matrix, verifies state and receipts, and
cleans up its containers, networks, fence tables and private ext4 disks.
Cleanup verifies their removal and retries transient host or Docker failures;
an incomplete cleanup keeps the suite unsuccessful.
Results remain under `.cellar/perf/`. No host Rust installation is required.
Use an idle Colima VM; another CPU-heavy workload invalidates comparisons.
Keep at least 25 GiB available on Colima's storage filesystem before starting
a case. The runner checks this before building and again before each seed;
`--minimum-store-free-gib` changes the start threshold. It records bytes and
inodes at measurement, reload, restore and failure checkpoints. Each host/policy
case starts a fresh RustFS process and data directory after the preceding case
has passed every recovery and shutdown gate. This keeps completed cases' data
and scanner work out of the next 10,000-cell comparison. In-case storage failures
fail the suite; the runner never resets the store to repair them. RustFS warning
logs are saved per case. Backend isolation and filesystem size are part of
baseline compatibility, so older shared-store runs are incompatible.
After clearing stopped nodes' private ext4 state, the runner trims their loop
filesystems so freed blocks release sparse backing-image space in Colima.
It refuses disk clearing or backend reset while a benchmark node or client is live.

The default standard run uses **10,000 cells**, both host modes, Bucket and
Fleet durability, five traffic mixes, rates 25/100/400 requests per second,
30-second warmups, 60-second measurements and three repeats. Allow several
hours, plus the first release build. For a quick functional run:

```sh
python3 tools/perf/fleetcare.py --profile smoke --suite smoke
```

Smoke has 100 cells and shorter measurements. `--profile smoke --suite standard`
also runs the crash, store outage and bucket restore cases. To exercise the
full inventory with short measurements while bringing up the lab:

```sh
python3 tools/perf/fleetcare.py --rates 10 --seconds 5 --warmup-seconds 1 --repeats 1
```

Run regression comparisons with the **same options and VM resources**:

```sh
python3 tools/perf/fleetcare.py --output .cellar/perf/baseline
python3 tools/perf/fleetcare.py --baseline .cellar/perf/baseline --output .cellar/perf/candidate
```

The comparison rejects incomplete or incompatible suites and exits nonzero
for median operation p99 increases over 20%, throughput decreases over 10%,
a lower success fraction, or excessive generator scheduling debt. Override
thresholds with `--latency-regression` and `--throughput-regression`.
Use the initial matrix to identify qualifying rates and mixes, then select
those cases with identical options for both baseline and candidate. A case
with generator saturation or scheduling debt above 10 ms cannot serve as a
capacity comparison; including it makes the comparison fail even when both
runs completed their functional gates. Keep overload cases as separate
exploratory runs. Short bring-up samples remain provisional for p99.
`--no-build` requires images whose source labels match the current checkout.
Repeated case options are rejected because they would reuse command IDs.
The `fleetcare-v2` workload contract includes pre-admission classification of
dropped attachment arrivals; comparisons reject older workload contracts.

## Application state

| Root authority | Full | Smoke |
|---|---:|---:|
| Device | 5,999 | 51 |
| WorkOrderBook | 2,400 | 24 |
| Inventory | 1,000 | 10 |
| Durable dispatcher workflow | 500 | 4 |
| SQL database | 60 | 6 |
| KV backing cell | 20 | 2 |
| Queue broker, including DLQ | 20 | 2 |
| Cron | 1 | 1 |
| Total | **10,000** | **100** |

Scopes use SDK identity construction, with 20 tenants in full and two in smoke.
Placement is natural, with equal node weights. The report records exact owner
counts, residency, cached routes and balance; the suite requires every root to
be owned once across the fleet. Facets, Blob objects, assets and containers do
not introduce additional fleet root authorities.

Devices maintain increasing telemetry sequences, health, maintenance state,
history and an offline alarm. Books contain orders with validated transitions:
open → reserving → reserved → dispatched → in progress → completed; failed
participants move through compensation to cancelled or needs review.
Inventory reserves, consumes, releases and expires stock, with a conservation
audit. Every command has an immutable digest, ordinal, version and receipt.
Changed payloads under the same command ID fail; exact replay returns the same
receipt, including business rejections.

Transactions insert state, history, receipts and outbox effects together.
Alarms retry outboxes. Queues build versioned SQL projections. Valid projections
that exhaust primary delivery attempts remain idempotent repairs in the DLQ;
poison and malformed snapshots enter quarantine. DLQ delivery has ten retries
at five-second intervals to survive a transient SQL outage. Durable dispatcher steps use
stable participant IDs scoped to tenant and book, bounded retries, waits and
compensation. The private
technician simulator can lose a reply after accepting a notification or reject
one, so the suite verifies one external effect and the correct terminal state.
Blob uploads verify content before committing an order reference; the client
checks every acknowledged object again during receipt audits.

The bearer token is a private benchmark operator credential with access to all
fixture tenants. This is a test application, with explicit target validation
and tenant identities in storage, queue envelopes and facets.

## Traffic and evidence

| Mix | Offered operations |
|---|---|
| read | 60% Device, 20% Book, 20% Inventory reads |
| write | 70% telemetry, 20% order creation, 10% stock replenishment |
| core | 60% reads, 40% commands/policy writes, including reporting SQL |
| attachments | Equal streamed Blob writes + committed references and verified object reads |
| capabilities | Service fetch/RPC, asset binding, compiled loader, HTML/cache/crypto/compression, TCP, outbound HTTP/WebSocket and TLS rejection probes |

Untimed gates additionally exercise nested/async transactions, cursors,
structured clone, facets, KV TTL/metadata/streaming, SQL migration/session
bookmarks, queue encodings/delays/retries/DLQ, multipart/resume/abort, ordinary
and hibernating subscriptions, attachments, workflow controls and real
cell-owned container start/port/exec/exit/destroy.

The standard suite audits **original acknowledgements before any replay**,
injects SIGKILL during traffic, pauses RustFS for three seconds, reloads each
node's local directory, and restores the entire fleet after discarding all
local state. It requires actual typed shutdown reports and complete drain
phases. The embedding host additionally proves both caller executors still
work after the node joins. The final node preserves because it has no live
peer available for a handoff.
Strong KV snapshots separately check the latest tenant policy value and
metadata after every local reload and the bucket restore. KV overwrites do not
retain a receipt history for intermediate values.

Each node has an aggregate budget of **1 vCPU / 2 GiB**: its application gets
15/16 CPU and 1,792 MiB, reserving 1/16 CPU and 256 MiB for at most one cell
container. Container diagnostics run outside measured core traffic. A transient
fence process is limited to 1/16 CPU and 16 MiB. Nodes share Colima's host
network so the SDK can reach its fenced child bridges, with separate process
cgroups and unique listeners/directories. This simulates three resource-limited
nodes on one VM; it does not simulate availability-zone network latency.
RustFS gets two CPUs/4 GiB, the generator two CPUs/2 GiB and the simulator one
CPU/256 MiB. Every node has a private 10-GiB ext4 disk, rather than a macOS
shared-filesystem bind mount.

Artifacts include `report.md`, `results.json`, `metadata.json`, per-operation
histograms, JSONL receipts, diagnostic assertions, ownership snapshots,
Docker resource samples and shutdown reports. Latency runs from scheduled
arrival to the complete response, including failures. Service latency and
scheduling debt are separate. A full generator records dropped arrivals;
mutations are not retried during measurements. Histograms have bounded relative
precision and round upward. Percentiles with fewer than 10,000 successful
samples per operation are marked provisional. Reports identify qualifying
tested rates for an explicit SLO, without extrapolating peak capacity.
Successful requests/s divides completions by the fixed arrival window. Raw
`elapsed_s` includes draining outstanding responses; use it to calculate
throughput over the entire sample, especially for short or overloaded runs.
Actual Bucket/Fleet output-barrier counts come from the durability adapter;
they are observations, not acknowledgement counts or authority proofs.

Untimed seed and recovery scans use at most eight concurrent requests by default,
controlled by `--verification-concurrency`. Measured traffic retains its
independent `--concurrency` limit (128 by default). Both hosts use separate S3
client pools for lease and data requests, with 15-second request and 3-second
connection timeouts; conditional requests have no automatic retries. These
settings are recorded and must match for regression comparisons.
The default comparison sets proactive memory shedding **off in both hosts**,
matching the explicit embedding policy. Docker still enforces the CPU and
memory quotas; OOM and durability failures remain failures. Use
`--memory-pressure auto` to enable the same 80% working-set and 95% complete-charge
watermarks in both hosts. Every startup verifies the core's actual watermarks,
and the selected policy is part of baseline compatibility. Earlier daemon runs
used automatic pressure while embedding left it unset; a transient hard-cap
latch on file-cache charges closed daemon admission during a binding probe.
Those runs are not presented as equivalent-policy performance comparisons.

Useful controls include `--skews uniform 80-20 hot`, `--bytes`, `--max-resident`,
`--max-cached-routes`, `--concurrency`, `--verification-concurrency`, `--rates`, `--seconds`, `--repeats`,
`--wake-ms` (fleet discovery and cleanup interval),
`--slo-p99-ms`, and `--minimum-success`. For a six-hour soak, select one
host, durability policy, mix and tested sustainable rate, with `--seconds 21600`
and `--repeats 1`; otherwise that duration applies to every matrix case.
Retain its resource series and ledgers.

Coverage limits are explicit: Cache is currently an always-miss compatibility
facade and SQL dump is unsupported. The current SDK supports workflow sensitive
steps, rollback, batch/configuration, history and byte streams; this workload
does not yet probe those extended APIs. The recorded full-scale qualification
predates their implementation and the subsequent rebase onto `main`.
The production TCP service uses public WebPKI roots, so private TLS tests
certificate rejection and STARTTLS rather than trusted TLS throughput. Peer
partition, rolling code-generation adoption and deliberate shutdown-budget exhaustion
remain qualification extensions in the original design; standard tests cover
process crash, store outage, local reload and bucket restore.

Harness guards: `python3 -m unittest discover -s tools/perf -p 'test_*.py'`.
Application checks: `cargo test -p cellar-fleetcare --locked` with the
repository's required checkout-specific `CARGO_TARGET_DIR`.
