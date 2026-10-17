# FleetCare qualification — 2026-10-13

The separate FleetCare crate and standard suite completed all four **10,000-cell** combinations: embedded and daemon hosts, each with Bucket and Fleet durability. All **632 harness checks, 92 capability cases and 36 actual typed shutdown reports** passed. Resource cleanup was verified. The normal traffic samples completed **1,500/1,500 requests**.

This establishes full-inventory functional and recovery qualification. The traffic windows were three seconds with one-second warmups and one repeat; their latency percentiles are provisional and do not establish production capacity. The default long rate matrix and soak remain unmeasured on this source.

The PR subsequently rebased onto `main` at `6a8e252`, adapting host-service construction, Docker workspace inputs and formatting. The results below remain tied to their original source digest, not the rebased tree. Current SDK workflow sensitive/rollback, batch/configuration, history and byte-stream support is beyond the suite's coverage; the unsupported workflow notes below describe the measured snapshot. The rebased tree passed workspace check/Clippy, repository formatting, all 11 app tests, 64 harness guards, native CLI/artifact tests and five replication tests. The native node group passed 17/18 initially; its existing fleet-drain timing test hit `NodeLeaseExpired` and passed in isolation without assertion changes.

## Reproduce

From the repository root, with an idle Colima VM configured for eight CPUs, 16 GiB memory and an 80-GiB disk:

```sh
python3 tools/perf/fleetcare.py
```

This builds the release images and runs the default 25/100/400 arrivals/s matrix, five mixes, both hosts and both durability policies, with 30-second warmups, 60-second measurements and three repeats. Allow several hours. The completed qualification used:

```sh
python3 tools/perf/fleetcare.py --rates 25 --seconds 3 --warmup-seconds 1 --repeats 1 --output .cellar/perf/fleetcare-full-13-parity
```

Usage, state machines, primitive coverage and regression commands are in the [FleetCare README](../../../examples/fleetcare/README.md). Machine-readable summaries are in [the qualification JSON](2026-10-13-fleetcare-bringup.json). Raw evidence remains in `.cellar/perf/fleetcare-full-13-parity`, including immutable command ledgers, per-operation histograms, stage assertions, resource series, state snapshots, filesystem observations and logs.

## Inventory and resource policy

The inventory is 5,999 Devices, 2,400 WorkOrderBooks, 1,000 Inventory partitions, 500 durable dispatcher workflows, 60 SQL databases, 20 KV cells, 20 Queue/DLQ brokers and one Cron. Facets, objects, connections and containers have separate counts.

Each node has an aggregate application/child budget of 1 vCPU and 2 GiB: the application receives 15/16 CPU and 1,792 MiB, reserving 1/16 CPU and 256 MiB for one cell container. A brief fence helper is separately capped at 1/16 CPU and 16 MiB. Each node has a private 10-GiB ext4 loop disk. RustFS receives two CPUs/4 GiB, the generator two CPUs/2 GiB and the simulator one CPU/256 MiB. All share one aarch64 Colima VM.

Both hosts captured 512 resident slots, 2,048 cached routes, 128 request slots, 30-second handler/core operation budgets and a one-second wake interval. Actual memory watermarks were checked at every startup: proactive pressure off in both hosts, with Docker hard quotas enforced. `--memory-pressure auto` selects matching automatic watermarks; that policy was not full-scale qualified here. RustFS is freshly isolated between completed cases. The runner checks at least 25 GiB free before each seed, records filesystem bytes/inodes and trims only cleared, stopped private loop disks.

| Host / policy | Seed owners (nodes 1/2/3) | Cold-restored owners | Seed seconds | Shutdown reports |
|---|---|---|---:|---:|
| embedded / bucket | 3333 / 3334 / 3333 | 3335 / 3334 / 3331 | 130.8 | 9 |
| embedded / fleet | 3334 / 3334 / 3332 | 3341 / 3308 / 3351 | 202.5 | 9 |
| daemon / bucket | 3333 / 3333 / 3334 | 3369 / 3328 / 3303 | 244.3 | 9 |
| daemon / fleet | 3334 / 3333 / 3333 | 3314 / 3340 / 3346 | 161.9 | 9 |

Every seed, measured, local-reload and cold-restore inventory totaled exactly 10,000 roots; all observed distributions were within 5% of equal placement. Measured residency was 512 per node. Seed time includes fixture creation and is not a sustained application throughput estimate.

## Completed gates

Each case passed full state/conservation audits, all 23 capability/state probes, SQL source-watermark barriers, original acknowledgement checks before every ambiguity replay, SIGKILL during writes, a three-second RustFS outage, all three local preserving reloads, and restoration after discarding every local state directory. Valid DLQ projections were repaired while poison and malformed messages remained quarantined.

The 408 saved receipt-ledger audits preserve the original receipts, canonical payload digests and ordinals. Cold restore rechecked 39,244 acknowledged commands across the four independent cases, including every case’s 9,399 seed receipts and acknowledged attachments. Strong KV checks saved 20 snapshots and made 320 exact tenant value/metadata comparisons after local reloads and bucket restores.

All 36 stop reports proved drained ingress and application events, drained cells, zero pending dispatches, quiesced logs and closed local durability, without deadline exhaustion. Preserving stops also proved clean reload preparation. All 18 embedded reports additionally proved both caller executors remained usable after owned work joined.

Cumulative output-barrier observations at the measured checkpoint include seeding and background work; they are not acknowledgement counts or authority proofs. Fleet policy can fall back to Bucket proof:

| Host / policy | Bucket barriers | Fleet barriers | Adapter errors at checkpoint |
|---|---:|---:|---:|
| embedded / bucket | 62,838 | 0 | 0 |
| embedded / fleet | 145 | 62,839 | 0 |
| daemon / bucket | 62,955 | 0 | 0 |
| daemon / fleet | 400 | 62,564 | 0 |

## Provisional traffic observations

All rows offered 25 arrivals/s for three seconds and completed 75/75 requests. The p99 column is the worst operation’s all-attempt p99 in that mix. Histograms round upward with bounded relative precision. “Whole sample/s” divides completions by elapsed time including response drain; the runner’s successful-rps metric uses the fixed arrival window. Scheduling debt is reported separately.

| Host | Policy | Mix | Whole sample/s | Worst p99 ms | Debt p99 ms | Sample meets SLO and generator checks |
|---|---|---|---:|---:|---:|---|
| embedded | bucket | read | 25.16 | 32.51 | 10.88 | no |
| embedded | bucket | write | 25.24 | 34.81 | 11.39 | no |
| embedded | bucket | core | 25.18 | 32.77 | 7.74 | yes, provisional |
| embedded | bucket | attachments | 25.22 | 35.84 | 10.75 | no |
| embedded | bucket | capabilities | 25.29 | 44.54 | 11.78 | no |
| embedded | fleet | read | 17.15 | 1425.41 | 6.97 | no |
| embedded | fleet | write | 24.99 | 60.93 | 8.19 | yes, provisional |
| embedded | fleet | core | 18.77 | 1556.48 | 10.37 | no |
| embedded | fleet | attachments | 25.21 | 72.70 | 9.86 | yes, provisional |
| embedded | fleet | capabilities | 25.30 | 12.67 | 10.88 | no |
| daemon | bucket | read | 20.06 | 999.42 | 9.98 | yes, provisional |
| daemon | bucket | write | 17.98 | 1982.46 | 14.21 | no |
| daemon | bucket | core | 24.79 | 876.54 | 11.26 | no |
| daemon | bucket | attachments | 25.10 | 62.46 | 6.14 | yes, provisional |
| daemon | bucket | capabilities | 25.29 | 45.05 | 10.24 | no |
| daemon | fleet | read | 25.21 | 27.39 | 8.96 | yes, provisional |
| daemon | fleet | write | 25.19 | 36.35 | 8.06 | yes, provisional |
| daemon | fleet | core | 25.11 | 143.36 | 10.75 | no |
| daemon | fleet | attachments | 25.07 | 31.74 | 8.57 | yes, provisional |
| daemon | fleet | capabilities | 25.29 | 53.25 | 11.13 | no |

The sample SLO is at least 99.9% success, every operation p99 at most 1,000 ms, scheduling debt p99 at most 10 ms, and no generator saturation or protocol errors. Several rows fail the debt limit; some also exceed the latency SLO. Every row has fewer than 10,000 successful samples per operation. These observations cannot rank the hosts or establish sustained throughput. Use the longer matrix and repeated qualifying rates before selecting a regression baseline; include overload cases as separate experiments.

## Corrections exposed by the suite

- **Covered imminent alarms blocked cold admission at full residency.** Keep the alarm residency optimization only while there is spare capacity; retain wake, durability and ownership guards. native_node::full_node_admits_cold_cells_past_a_covered_alarm failed before and passed after.
- **Clean local reload rejected retained evicted databases as live.** The final stale-live inventory includes live records only; retained cached files remain cached. Native replication tests and actual local reloads passed.
- **Gated remote RPC handler failures lost their typed error.** Preserve the bounded gated handler-error body and cancellation; output-gate failures remain opaque. Three-node SDK control test passed.
- **Valid projections exhausted primary retries and were quarantined in DLQ.** Repair valid versioned snapshots idempotently from DLQ; quarantine poison and malformed messages. Real SDK Queue/DLQ/SQL test failed before and passed after; 23rd suite probe exercises repair.
- **Clearing private loop disks did not reclaim sparse backing extents.** Trim only this stopped run's private loop filesystems; isolate RustFS per completed case and require storage headroom.
- **Embedding and daemon used different proactive memory policies.** Explicit off/auto policy in both hosts, validate actual core watermarks at startup, include policy in baseline compatibility.
- **Earlier RustFS conditional ownership writes returned HTTP 500.** Cause remains unestablished. Failures are retained; fresh-case backend, headroom and storage observations reduce interference without recovering a failed case.

The private-disk reproduction retained 41,240 allocated blocks after deletion, then fell to 8,344 after trim. The memory-policy diagnostic completed both daemon cases with unchanged Docker quotas and proactive pressure off; the standard runner now records and verifies this setting directly. Earlier interrupted or failed runs remain marked unsuccessful in the JSON history and raw directories; they are not combined into the current qualification.

## Verification and limits

Formatting, native dependency checks (387 packages, no JavaScript engine), documentation validation, the locked all-target/all-feature workspace check and Clippy passed. All 11 FleetCare tests, 64 harness guards and native CLI/artifact tests passed. The earlier full workspace library/integration groups and doc tests passed after the admission correction; the timing-sensitive native_node group passed all 15 tests separately with one test thread.

The application uses realistic state, receipts, alarms, outboxes, Queue/SQL projections, workflow coordination, KV policies, Blob attachments and subscriptions. Capability probes also cover facets, services, assets, loaders, web/crypto/compression, TCP, outbound HTTP/WebSockets and cell containers. Some are untimed functional probes rather than isolated sustained-throughput measurements.

Cache remains an always-miss facade; SQL dump and workflow sensitive/rollback are unsupported. Private TLS tests rejection and STARTTLS, with trusted TLS throughput unmeasured. Peer partition, rolling code adoption and deliberate shutdown-budget exhaustion remain extensions in the [original design](../field-service-design.md). The shared VM does not simulate availability-zone latency. KV preserves latest policy values, without historical receipts for intermediate overwrites.

Source digest: `7c3eb6d27a2581b880bebd068f59571a7b4396a116b9e410fe292308246fde0a`. Application image: `sha256:472d78c9c712230a46282a0ae05c1110dff79f86b2a7a5b3ccfad43d9035e171`. The checkout was dirty at revision `2f514334c193ee585c3edb90ba8e3ed2be327d41`; the source digest identifies the compiled snapshot. RustFS image and complete resource/policy identities are recorded in the JSON.
