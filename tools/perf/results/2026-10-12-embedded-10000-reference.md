# 10,000-cell embedded reference validation

The bucket and fleet scenarios completed with every selected recovery check and all 14 returned SDK shutdown proofs passing. Final counters exactly matched 34,230 acknowledged writes in bucket mode and 23,314 in fleet mode; every one of the 10,000 cells had been written. All reads checked the counter and payload length. The load generator did not retry application writes; no load errors, counter mismatches or verification read retries were observed.

This is the earlier reference image, built from `20fb251d7fb239de57bc712775d6349b5dc0a0b5`. The harness recorded clean checkout `b7f7ce39dcfb5f34b62cc7930289a3c72f68b49b`; its production crates, measuring application, load generator and run script are identical to that image source. The image predates the routing-history bound and typed fencing outcomes. This run does not validate either later change.

The [structured report](2026-10-12-embedded-10000-reference.json) records image IDs, source hashes, measured resource snapshots, recovery checks and returned shutdown reports. The raw run is retained locally at `.cellar/perf/embedded-10000-b7f7ce3` and archived separately. Harness-owned containers, network and loop mounts were removed after completion.

## Workload and resources

- Three application processes each embedded `sdk::Node` on caller-owned host/core Tokio runtimes and supplied their listeners, authenticated peer clients, signing identities and S3 clients.
- Each node had 1 CPU, 2 GiB RAM and a private 5 GiB ext4 data image. RustFS had 2 CPUs and 4 GiB RAM; the load generator had 2 CPUs and 2 GiB RAM. All shared one 8-CPU, 16-GiB Colima VM and its backing disk on an Apple M2 Max.
- Each scenario used a fresh bucket, 10,000 business cells, changing 1-KiB ASCII JSON values, a 512-cell resident limit and a finite 512-request script-pool limit. Warmup ran for 180 seconds, followed by two 20-second repetitions at 32 and 128 concurrent requests.
- Measurements used closed-loop traffic and excluded warmup. Percentile ranges below span individual repetitions; they are not pooled percentiles.

| Mode | Concurrent requests | Acknowledged writes/s | Per-run p99 range |
| --- | ---: | ---: | ---: |
| Bucket | 32 | 122.4 | 845–1345 ms |
| Bucket | 128 | 134.8 | 1574–2451 ms |
| Fleet | 32 | 82.6 | 1114–1281 ms |
| Fleet | 128 | 95.8 | 4094–5624 ms |

## Recovery and process control

- Bucket mode killed all three nodes and discarded their local state. Restoration verified all 32,258 acknowledgements existing at that point, in 523.5 seconds.
- Fleet mode killed one node and verified all 22,537 acknowledgements on the survivors, in 304.1 seconds.
- Both modes handed off and restarted each node, verified all cells after takeover, then wrote more and verified again. Each final node stopped with Preserve, restarted alone, and verified the entire ledger: 34,230 writes in 747.5 seconds for bucket mode and 23,314 in 300.5 seconds for fleet mode.
- All 14 graceful stops returned complete ingress, application-event, cell, dispatch, log and local-durability proofs, with zero pending work, no exhausted shutdown budget and both caller executors still running. Every preserving stop also prepared a clean reload.

## What remains unproven

Measured snapshots stayed at 512 residents per node while retaining 5,394–7,341 total cell records. The image had no `cached_routes` metric or routing-history limit. The current cache fix therefore needs its own release-image run with more cells than both residency and cache limits.

Measured node RSS was 142–159 MiB; peak cgroup memory, including filesystem cache, ranged from 386–765 MiB. Measured memory-event counters showed no OOM or memory-limit events. These observations do not establish a whole-process memory bound.

All containers shared one VM and disk with a local single-server RustFS store. Host-side development and Cargo work overlapped parts of the run, so timing is not a fully isolated performance comparison or production SLO. Failures occurred between write phases. The run does not cover network partitions during writes, ambiguous acknowledgements, independently arriving traffic, growing datasets, hot-cell skew or long soaks. Fleet acknowledgement may fall back to bucket proof.
