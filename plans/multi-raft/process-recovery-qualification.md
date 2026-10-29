<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Separate-process RustFS recovery at 10 GB

The 2026-10-29 UTC trial passed at **9,999,998,976 SQLite bytes** on frozen
commit `79616cbe2769f4527974db68dd0c8c03d1c25b54`. An acknowledged owner was
killed with SIGKILL and its entire worker directory erased. A new native worker
with an absent local directory recovered through RustFS, served local reads,
became leader and acknowledged a quorum-durable write. Receipt reconciliation,
payload verification and joined shutdown all passed.

This qualifies that single-owner-loss scenario with a surviving quorum and
complete local extent hydration. It does not complete the broader
[10 GB requirements](10gb-implementation.md#completion-requirements) or the
[RT-06 production campaign](010-large-cell-recovery-time.md#rt-06-harden-and-qualify-the-targets).

## Proof and measurements

The source-pinned runner selected exactly one ignored native test,
`process_recovery::rustfs_decimal_ten_gb_after_sigkill_restores_empty_process`.
It returned zero, passed all 20 required ledger phases, and confirmed unchanged
source inputs, executable and fixture. No SQL probe, build or additional payload
scan ran alongside this physical trial.

| Observation | Result |
| --- | --- |
| Original worker PIDs | 42723, 42724, 42725 |
| Lost owner | PID 42723, observed exit signal 9; directory erased |
| Replacement | New PID 44935, member 4, new disk lineage, absent directory |
| Object-store body bytes received before serving | 10,000,163,341 |
| Peer recovery bytes | 0 |
| First restored local read | Counter 8 and expected payload hash, served by member 4 |
| New command | Counter 9, quorum durability, version-bound local read |
| Old and new receipt retries | Same results and scope; no repeated application callback |
| Surviving replicas | Both served the new committed value with its version bound |
| Payload audit | 9,953,083,392 payload bytes; SHA-256 samples at rows 1, 4747, 9492 |
| Final payload operation | 7.054408 s, within its unchanged 60 s read deadlines |
| Shutdown | Three survivors closed owners and exited successfully; killed owner joined |

Times below use the controller's monotonic phase ledger. Setup initializes three
independently verified baseline replicas; it is outside the recovery interval.

| Interval | Seconds |
| --- | ---: |
| Native trial including setup and shutdown | 1,375.58 |
| Setup through owner kill | 770.363408 |
| Owner kill through required archive confirmation | 364.105145 |
| Empty replacement spawn through verified seed installation | 229.216020 |
| Seed installation through first verified local read | 0.219656 |
| Owner kill through verified quorum-durable write | **596.258615** |
| Owner kill through final payload verification | 604.386187 |

Archive catch-up dominates the measured recovery interval. An already confirmed
archive is a different starting condition; the 229.22 s restore interval must
not be presented as the full failure-to-write duration. The run is one local
observation, not a controlled comparison with earlier runs or an RTO guarantee.

The same frozen executable first passed the 272 MiB process-kill drill: 21.507531 s
from owner kill to verified write, 4.558906 s from replacement spawn through seed,
285,228,756 object-store bytes and zero peer recovery bytes.

## Query verification fix

The preceding full-size attempt restored the image, read and wrote successfully,
then failed its final payload read with `VersionUnavailable` at the unchanged
60 s deadline. An isolated query on its retained image read 10,301,005,824 bytes:
new descriptors repeatedly rehashed complete 4 MiB extents. Under additional
read contention the same query exceeded 60 s. The contended diagnostic also
overlapped compilation, so its wall time is not a controlled benchmark.

The fix retains bounded, process-local SHA-256 block proofs derived from complete
extent verification. Every returned block is still independently checked.
Reopening the inventory discards these proofs; initial recovery still verifies
the complete destination. Reusing a read proof cannot grant seal admission or
override a concurrent verification failure. Local read corruption revokes
admission. Writable hydration still checks the complete source extent before
copying any prefix.

On the same retained 10 GB image, the revised standalone query passed in
0.884945 s with 275,841,024 bytes read and identical counter, payload length and
sample hashes. Its separate startup verification took 82.319718 s. This probe
preceded the fresh physical trial and receives no recovery qualification credit.
The fresh trial independently passed its final payload operation in 7.054408 s.

The current revision passed 79 LTX extent tests, 70 native recovery tests,
46 Python runner tests, affected release/all-feature Clippy targets with warnings
denied, formatting, layout, native dependency and documentation checks. Safety
regressions cover descriptor closure, restart, late corruption, failed concurrent
verification, and refusal to partially hydrate a corrupt extent.

## Reproduction and evidence

Use the [process recovery instructions](../../tools/cellar-recovery-tests/README.md#empty-process-recovery-after-sigkill)
and `tools/perf/process_recovery.py`. Run the 272 MiB case first, then select
`--profile 10gb` with the native-generated read-only fixture, fresh worker and
evidence directories, and private RustFS credentials. Keep data outside build
cleanup directories and avoid competing validation work during measurement.

The following retained artifacts identify this trial; raw worker data, RustFS
objects, logs and per-read performance observations remain local and are not
published with credentials or opaque receipt tokens.

| Artifact | Identity |
| --- | --- |
| Validation | `process-recovery-validation-8/result.json` |
| Small physical trial | `process-recovery-smoke-5/result.json` |
| Standalone query probe | `process-recovery-proof-probe-1/result.json` |
| Full physical trial | `process-recovery-10gb-4/result.json` |
| Source inputs SHA-256 | `da75f6978e84a879dabb6b5a09b0be4aa97b8d36287fd2e6b9b23eba4999480c` |
| Recovery executable SHA-256 | `649f28f93e3bf0f249c3e857bb4ee31ce79557030374a4d2be9af9b9e0aa0b0b` |
| Read-only fixture SHA-256 | `24a5f2ff65789e9bbc66b4fa69b470b2f98fe45cd7a3262f1434526d25778764` |
| Full result SHA-256 | `52f0c93478740c2ad725b492baaf820958ad7b17e18a91816441852f5c193d4b` |

The quota was decimal 10,000,000,000 bytes, rounded down to 4 KiB SQLite pages.
The data generator repeats a pseudorandom 1 MiB payload with tagged samples and
a zero-filled quota tail; this is not an independently random-row workload.
The object-store byte counter proves that the replacement actually downloaded
the full image despite fixture repetition.

Environment: Apple M2 Max, 32 GiB RAM, macOS 26.5.2; native RustFS 1.0.0-rc.1
and four distinct worker PIDs on one shared development host. Worker data and
RustFS data used the same Workspace volume; Raft used real TLS sockets and
RustFS used loopback HTTP. Build profile was release with Rust 1.98.1.
Source/store filesystem caches and other host workloads were uncontrolled.
The explicit snapshot transfer budget was 3,600,000 ms; ordinary read deadlines
remained 60 s. Defaults were not raised.

Earlier attempts remain failures: the first expired its five-minute transfer
budget; the second lost active data during external cleanup; the third failed
the final payload audit. They are retained separately, not relabeled as passes.

Before claiming production latency or broader fault coverage, the remaining
work includes separate-host/network profiles, controlled cache and interference
campaigns, repeated percentile measurements, process kills at intermediate
restore/write boundaries, and the full integrity/capacity/authority matrix.
All-replica loss, power loss, partial-hydration activation and a fresh restart
after this replacement's write are outside this particular trial. The runner
therefore correctly retains `production_qualified: false`.
