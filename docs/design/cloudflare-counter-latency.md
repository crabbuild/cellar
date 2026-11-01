<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Cloudflare counter latency qualification

Purpose: Record resident retirement admission correctness, measured counter latency gains, proxy-path comparisons, and qualification limits.

The [counter example](../../examples/cloudflare-counter/README.md) runs native
Cellar with SQLite state, R2 bucket durability and a browser latency workbench.
This qualification used one `standard-1` container: 0.5 vCPU and 4 GiB RAM.
It does not qualify multi-Raft throughput or production tail latency.

## Resident retirement admission

Ordinary requests previously read the permanent retirement marker from object
storage before routing, even when the target activation was already resident.
Warm requests now reuse a published local activation's admission. This is tied
to residency, rather than a time-based negative cache of a missing marker.

Erasure closes business admission permanently before proving its SQLite intent
and publishing the external marker. The publication witness belongs to the
original activation. Cold activation, takeover and snapshot restore still check
storage; recovery of an erasure intent keeps admission closed. Only owned cleanup
and authenticated, exact-identity child-destroy continuations can finish through
that closure. Ownership, epoch and write/output proofs remain enforced. See the
[erasure contract](../../crates/cellar-container/allocations.md).

## Measured result

Both runs used five warm-up requests followed by 60 sequential reads over a
persistent HTTPS connection from the same Mac. All measured requests succeeded,
were container-warm and matched the expected native revision. No errors or
invalid samples were discarded. Percentiles use the observed nearest rank.
The source base was `8a93ffd7`; the two images used the same lab build profile
and cloud resource limits. Durations are milliseconds.

| Stage | Before p50 | After p50 | Before p95 | After p95 | After max |
| --- | ---: | ---: | ---: | ---: | ---: |
| Native dispatch | 151.142 | 0.812 | 195.577 | 0.971 | 1.006 |
| Native handler | 0.123 | 0.067 | 0.288 | 0.087 | 0.096 |
| HTTP end to end | 245.684 | 80.511 | 345.979 | 106.022 | 491.779 |

All 60 native dispatches meet the 10 ms target. In this initial run the 100 ms
end-to-end p95 target remained unmet: five requests exceeded the budget. The browser's
separate ten-read sample also kept native dispatch below 1 ms, but one request included
a 76 ms readiness stage and took 287.7 ms overall. A warm container does not
establish that its Durable Object activation has completed readiness.

The slowest CLI request had native dispatch 0.875 ms, complete container time
26 ms and edge time 434 ms. Median complete container and edge times were
25 ms and 58 ms. These nested elapsed intervals identify proxy/network work as
the next bottleneck; they are not independent wire-latency measurements.

## Correctness validation

- Three new public-node tests cover zero warm retirement lookups with that
  backend unavailable, fresh checks on restart, refusal of new and suspended
  reads during marker publication, and closed recovery after publication fails.
- Existing public-node tests: 104 passed; four existing dense-capacity/private
  S3 qualification cases remained ignored.
- Native lifecycle/facet checks, retirement identity and partial-publication
  tests, and replica erasure/restore/log-replay checks passed.
- All five child-execution tests and the unchanged 24-child owner-loss fleet
  test passed, including terminal parent acknowledgement and cache collection.
- Strict Clippy for affected crates/all targets/features, formatting, layout,
  native dependency and documentation checks passed.
- Five frontend/Worker tests passed. Live HTTP verification checked concurrent
  duplicate retries, conflicting/stale refusal, compensating writes, readback
  and durability timing. A state fingerprint matched across image rollout.

The admission optimization is retained for its measured gain. Further proxy
experiments must preserve these contracts, report complete latency samples and
be reverted unless they demonstrate a gain. End-to-end qualification requires
measuring the full HTTP path, rather than extrapolating from native dispatch.

## Proxy-path experiments

A follow-up on 2026-10-31 used the same Mac, native image and resource limits.
A fresh 300-read control measured HTTP p50 78.645 ms and p95 94.609 ms before
any proxy changes. The original path can meet the warm p95 target in a favorable
network window; that observation does not demonstrate a new optimization.

Experiments retained the same named Durable Object, native SQLite/R2 state,
health and inactivity gates, response bounds, ownership checks and output
proofs. No business-response cache or mutation replay was introduced.

- Reusing the container port binding did not produce a repeatable HTTP p95 gain.
- Pinning the gateway to `aws:us-west-1` or `aws:us-west-2` measured p95
  109.790 ms and 107.363 ms respectively (200 warm reads each). Default placement
  was restored. A shorter gateway-to-object interval did not imply a shorter
  client round trip.
- Overlapping the required health and inactivity checks measured p95 88.820 ms
  over 300 version-verified warm reads, but did not establish an improvement
  over the control. The sequential readiness checks were restored.
- Read-only RPC initially appeared faster in separate runs. An alternating
  comparison through the same receiving object did not reproduce the gain.
  RPC and its experimental route selector were removed; all requests again use
  the original HTTP forwarding path.

Deployment verification checked both the gateway and the receiving object's
revision, in addition to the native image. An active object initially continued
serving its previous code after a Worker-only deployment. Initial code-change
comparisons that did not verify the receiving revision cannot establish a code
optimization gain. The qualified comparison below used the same object code for
both transports and a response header confirming the selected gateway branch.

Each of 500 pairs contained one HTTP and one RPC read in fixed-seed randomized
order over one persistent HTTPS connection, after ten warm-up pairs. All 1,000
measured reads succeeded, matched the expected revisions, reused readiness and
were container-warm; no measured failures or invalid samples were excluded.
Durations are milliseconds.

| Same-object transport | Reads | HTTP p50 | HTTP p95 | HTTP p99 | HTTP max | Native p95 | Reads >=100 ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Original HTTP | 500 | 77.003 | 84.404 | 141.160 | 201.561 | 0.954 | 12 |
| Read-only RPC | 500 | 76.977 | 84.746 | 150.308 | 201.208 | 0.934 | 12 |

RPC won 50.6% of pairs. The 95% paired block-bootstrap interval for its p95
change was -2.164 to +3.689 ms (20-pair blocks, 2,000 resamples). There is no
measured transport gain to retain. At the median both paths spent 23 ms in the
object-to-container interval and 56 ms in the complete edge interval; native
dispatch was approximately 0.8 ms. These are nested elapsed intervals, not
independent network-hop measurements.

All proxy experiment code and placement overrides were reverted. The resident
retirement optimization remains because its native and HTTP gains were measured
against the original storage lookup. Warm read p95 below 100 ms is an observed
result from this client and workload, not a global latency guarantee. Both
transports still had p99 above 100 ms. First reads after object recreation were
also observed above 300 ms despite the container remaining warm; cold starts,
short idle-separated bursts, concurrent load and other client regions need
separate qualification.


## Restored-path verification

After reverting every proxy experiment, the uploaded Worker module was checked
for the original HTTP path and sequential readiness checks; placement was back
to its default. A separate idle-window run used one readiness probe, ten warm-up
reads and 1,000 sequential measured reads over one persistent HTTPS connection.
All measured requests succeeded, matched the native revision, were
container-warm, contained all required timing stages and had nondecreasing
counter versions. No measured errors or invalid samples were discarded.

| Restored original proxy | p50 ms | p95 ms | p99 ms | Max ms |
| --- | ---: | ---: | ---: | ---: |
| HTTP end to end | 78.216 | 90.194 | 143.375 | 266.281 |
| Native dispatch | 0.809 | 0.951 | 1.136 | 2.330 |

The warmed p95 targets were met in this run. Thirty-two of 1,000 HTTP requests
exceeded 100 ms; none of the measured native dispatches reached 10 ms. These
results verify the restored path under this workload and network condition;
they are not a gain attributable to the reverted proxy experiments.

The initial readiness probe after the idle interval took 1,094.810 ms, with
native dispatch 706.656 ms and object readiness 86 ms, despite the container
reporting warm. It remains in the recorded probe data and is excluded from the
warmed percentiles by the predefined protocol. This also limits any claim that
all warm-container requests meet the native or end-to-end budgets.

Live HTTP verification after restoration passed actual writes/readback, four
concurrent duplicate retries, conflicting and stale identity refusal, durability
timing and compensating writes. Three additional native dispatches measured
0.689, 0.835 and 0.749 ms.

The unchanged Python `bridge --runtime-durability` case passed twice locally
with Rust 1.98.1 and Python 3.14.6. Broader CI reported failures in Python
durability, typed-RPC lifecycle and the JavaScript handler-budget check; the
local result does not resolve those CI failures or establish production
qualification. No assertions or budgets were weakened.
