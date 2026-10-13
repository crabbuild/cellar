# Embedded recovery with writes in flight

Both native SDK scenarios passed receipt verification before replay, after reconciliation and after the final preserving reload. Each used 128 business cells and 145 logical operations: 128 initial writes, one response deliberately lost at a proxy, and 16 concurrent writes held before their handlers returned. All eight verification stages, 18 cache snapshots, 12 captured startup policies and eight returned shutdown proofs passed. The driver exited zero and its independent saved-data audit passed.

The [structured report](2026-10-13-embedded-inflight-128.json) records image `cellar-embedded-perf:461754c`, immutable image ID `sha256:b895b752caaf2b7e602f8fdaa58431a470b46c2f02d5372bdc5e453a8e4f0f4f`, source hashes, fault observations, verification totals and returned SDK outcomes. The image and clean checkout use revision `461754c4326308f608cf187266ef2e5c8097efc8`. Source provenance was checked against the image's Git revision before provisioning and again during the artifact audit. Raw receipts and client journals remain at `.cellar/perf/inflight-128-461754c` and in a separately retained archive whose digest is recorded in the report. Private containers, network and loop directory were independently confirmed removed after completion.

## What the test proves

The compiled application embeds `cellar::sdk::Node` on caller-owned host and core executors and supplies its listeners, peer authentication and storage clients. It implements application idempotency by recording an operation ID, payload SHA-256 and original counter ordinal in the same storage transaction. Replaying that ID with the same payload returns the original receipt. This application policy does not imply that Cellar guarantees exactly-once request delivery.

The client flushes and syncs its operation journal before issuing writes. The proxy consumes a complete successful SDK response, records its receipt separately, and drops the client's connection without delivering a response. That operation remains ambiguous to the client but must survive recovery because the SDK already acknowledged it to the proxy.

The 16 concurrent handlers record local receipts and then wait ten seconds. Their diagnostic markers prove local transaction completion; they do not prove output-gate durability. A marker's operation was still pending at the client when its actual owner, node 1, was killed. The bucket case killed all three nodes within 0.698 seconds of starting the burst and discarded all local state. The fleet case killed node 1 within 0.271 seconds and read through the two survivors. Every controlled process exit was observed as 137 with no OOM kill.

Before replay, the verifier required every client-acknowledged receipt and the proxy's acknowledged receipt to exist with its original payload hash and ordinal. Unacknowledged writes were allowed to be absent or present once. Counters had to equal distinct receipts, with unique contiguous ordinals and no receipt for an unissued operation. Reconciliation then replayed the issued IDs and required exactly 145 operations per scenario, including an exact read from node 3 alone after its preserving reload.

| Stage | Mode | Client acknowledgements | Ambiguous receipts present | Ambiguous receipts absent | Durable counter total |
| --- | --- | ---: | ---: | ---: | ---: |
| Before fault | Bucket | 128 | 0 | 0 | 128 |
| Before replay | Bucket | 128 | 1 | 16 | 129 |
| After reconciliation | Bucket | 145 | 0 | 0 | 145 |
| Preserving reload | Bucket | 145 | 0 | 0 | 145 |
| Before fault | Fleet | 128 | 0 | 0 | 128 |
| Before replay | Fleet | 138 | 1 | 6 | 139 |
| After reconciliation | Fleet | 145 | 0 | 0 | 145 |
| Preserving reload | Fleet | 145 | 0 | 0 | 145 |

The sole ambiguous receipt present in each recovery was the proxy's discarded response. All acknowledged operations survived before any retry; replay did not repair or conceal acknowledged data loss. Every state read validated payload hashes, not only counters or payload lengths.

## Residency and shutdown

Each node captured limits of 16 resident cells, eight cached routes and 128 requests per script pool. Every required snapshot reported eight cached routes, 16 owned cells, 16 occupied slots and 24 tracked records while visiting 128 cells. Initial node resource captures confirm one CPU and 2 GiB memory per node; the private harness uses 5 GiB ext4 data images.

All four handoff stops and four preserving stops returned exit code zero, complete ingress/application/cell/log/local-durability proofs, zero pending events and dispatches, and unexhausted shutdown budgets. Both caller-owned executors remained usable after each SDK node joined its owned work. Every preserving stop prepared a clean reload. Process kills claim no graceful drain proof.

The new receipt fixture passed three Rust tests for replay across serialization, conflicting payload reuse and invalid inputs. All 26 Python harness guards passed, including missing acknowledged receipts, wrong payload hashes, changed ordinals, duplicate execution, real response loss and fatal journal-sync failures. The all-target/all-feature workspace check, measuring-binary Clippy, native CLI/artifact tests, formatting, dependency audit and diff checks passed. The all-feature dependency audit found 386 reachable packages and no JavaScript engine.

## Validation limits

This is a bounded correctness run on shared Colima infrastructure with a local single-server RustFS store. It does not establish throughput, a whole-process memory bound or a production fleet size. Fleet durability can fall back to bucket proof; acknowledgement source was not separately instrumented. Peer network partitions, long soaks, growing application data, hot-cell skew and coverage of every SDK primitive remain separate validation work. The application retains receipts without production retention or garbage-collection policy, and caps this fixture at 4,096 receipts per cell.
