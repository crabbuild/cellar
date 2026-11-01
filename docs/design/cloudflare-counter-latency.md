<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Cloudflare counter latency qualification

Purpose: Record resident retirement admission correctness, measured counter latency gains, and the remaining proxy-path qualification gap.

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

All 60 native dispatches meet the 10 ms target. The 100 ms end-to-end target
remains unmet: five requests exceeded the budget. The browser's separate
ten-read sample also kept native dispatch below 1 ms, but one request included
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
